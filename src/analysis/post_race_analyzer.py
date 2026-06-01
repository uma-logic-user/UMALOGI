"""src/analysis/post_race_analyzer.py — フェーズA「自己診断と敗因分析」。

EV>=1.0 で勝負しながら的中しなかったレースを抽出し、オッズ・人気・結果・予想根拠を
整形して Claude API に渡し「敗因の言語化」を行う。結果を Discord へ自動投稿する。

設計（既存オートパイロット/watchdog への非干渉を徹底）:
  - DB アクセスは呼び出し側が **読み取り専用接続（mode=ro）** で開いた conn を注入する。
    本モジュールは新規追加であり、稼働中の予想生成・評価ロジックには一切触れない。
  - Claude クライアント・Discord 通知はいずれも注入可能（テスト容易性・副作用分離）。
  - 既存の正準資産を再利用: 通知は src/notification/discord_notifier.DiscordNotifier。
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# claude-api スキル準拠: 最新かつ最も高性能な Opus を既定とする。
_MODEL = "claude-opus-4-8"
_MAX_TOKENS = 4000
_DEFAULT_EV_THRESHOLD = 1.0
_DEFAULT_LIMIT = 40

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "umalogi.db"


# ── 型プロトコル（注入用） ────────────────────────────────────────────────────


class _Notifier(Protocol):
    def send_text(self, text: str) -> None: ...


# ── 接続（読み取り専用） ──────────────────────────────────────────────────────


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """敗因分析用の読み取り専用 DB 接続を返す（稼働中プロセスと非競合）。

    Args:
        db_path: 接続先。None なら環境変数 DB_PATH か既定の data/umalogi.db。

    Returns:
        row_factory=sqlite3.Row を設定した read-only 接続。
    """
    path = db_path or Path(os.environ.get("DB_PATH", str(_DEFAULT_DB_PATH)))
    if not path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. 敗因分析の骨子: 不的中レースの抽出 ────────────────────────────────────


def extract_missed_races(
    conn: sqlite3.Connection,
    *,
    ev_threshold: float = _DEFAULT_EV_THRESHOLD,
    since: str | None = None,
    limit: int = _DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """EV>=ev_threshold で勝負したが的中しなかったレースを抽出する。

    予想した本命馬（predicted_rank 最小／model_score 最大）の実走結果・オッズ・人気と、
    実際の勝ち馬（rank=1）の情報を併せて返す。is_superseded=1 は除外。

    Args:
        conn:         読み取り専用 DB 接続。
        ev_threshold: この期待値以上の予想を対象とする。
        since:        "YYYY-MM-DD"。指定時は races.date >= since のみ。
        limit:        返却する最大行数（新しい順）。

    Returns:
        各行: race_id / date / venue / race_number / model_type / bet_type /
              expected_value / horse_name / notes / pred_win_odds /
              pred_popularity / actual_rank / winner_name / winner_odds /
              winner_popularity。
    """
    params: list[Any] = [ev_threshold]
    date_filter = ""
    if since:
        date_filter = "AND r.date >= ?"
        params.append(since)
    params.append(limit)

    rows = conn.execute(
        f"""
        WITH lead_horse AS (
            -- 各予想の本命馬（model_score 最大・同点なら predicted_rank 最小）を 1 頭抽出
            SELECT prediction_id, horse_name FROM (
                SELECT ph.prediction_id, ph.horse_name,
                       ROW_NUMBER() OVER (
                           PARTITION BY ph.prediction_id
                           ORDER BY ph.model_score DESC, ph.predicted_rank ASC
                       ) AS rn
                  FROM prediction_horses ph
            ) WHERE rn = 1
        ),
        winners AS (
            -- 各レースの 1 着馬（同着は馬番最小を代表）
            SELECT race_id, horse_name, win_odds, popularity FROM (
                SELECT race_id, horse_name, win_odds, popularity,
                       ROW_NUMBER() OVER (
                           PARTITION BY race_id ORDER BY horse_number ASC
                       ) AS rn
                  FROM race_results WHERE rank = 1
            ) WHERE rn = 1
        )
        SELECT
            p.race_id,
            r.date,
            r.venue,
            r.race_number,
            p.model_type,
            p.bet_type,
            p.expected_value,
            lh.horse_name                       AS horse_name,
            p.notes,
            rr.win_odds                         AS pred_win_odds,
            rr.popularity                       AS pred_popularity,
            rr.rank                             AS actual_rank,
            w.horse_name                        AS winner_name,
            w.win_odds                          AS winner_odds,
            w.popularity                        AS winner_popularity
          FROM predictions p
          JOIN prediction_results pr ON pr.prediction_id = p.id
          JOIN races r               ON r.race_id = p.race_id
          JOIN lead_horse lh         ON lh.prediction_id = p.id
          LEFT JOIN race_results rr  ON rr.race_id = p.race_id
                                    AND rr.horse_name = lh.horse_name
          LEFT JOIN winners w        ON w.race_id = p.race_id
         WHERE p.expected_value >= ?
           AND COALESCE(pr.is_hit, 0) = 0
           AND COALESCE(p.is_superseded, 0) = 0
           {date_filter}
         ORDER BY r.date DESC, r.venue, r.race_number
         LIMIT ?
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


# ── 2. Claude API 連携: 敗因の言語化 ─────────────────────────────────────────


def build_analysis_prompt(missed_races: list[dict[str, Any]]) -> str:
    """抽出データを Claude へ渡す敗因分析プロンプトに整形する（純関数）。"""
    lines: list[str] = []
    for i, m in enumerate(missed_races, 1):
        lines.append(
            f"{i}. {m.get('date')} {m.get('venue')}{m.get('race_number')}R "
            f"[{m.get('model_type')}/{m.get('bet_type')}] "
            f"本命=<{m.get('horse_name')}> "
            f"EV={_fmt(m.get('expected_value'))} "
            f"予想オッズ={_fmt(m.get('pred_win_odds'))}倍 "
            f"予想人気={_fmt(m.get('pred_popularity'))}番人気 "
            f"→ 結果{_fmt(m.get('actual_rank'))}着 / "
            f"勝馬=<{m.get('winner_name')}>"
            f"({_fmt(m.get('winner_odds'))}倍 {_fmt(m.get('winner_popularity'))}番人気) "
            f"根拠: {m.get('notes') or '—'}"
        )
    data_block = "\n".join(lines)

    return (
        "あなたは血統・調教・騎手・馬場適性を総合判断し、期待値ベースで買い目を組む"
        "プロの競馬予想家かつ厳格なデータ分析官です。\n"
        "以下は UMALOGI が「期待値(EV) 1.0 以上」と判断して勝負したにもかかわらず"
        "**的中しなかった**レースの一覧です。各レースのオッズ・人気・着順・予想根拠を"
        "踏まえ、**敗因**を冷静に言語化してください。\n\n"
        f"## 不的中レース（全{len(missed_races)}件）\n{data_block}\n\n"
        "## 分析の要件\n"
        "1. 敗因を 3〜5 個のパターンに分類する"
        "（例: 人気との乖離が過大／妙味オッズの罠／馬場・距離適性の見落とし／"
        "本命の取捨ミス／単なる確率収束の範囲内 等）。\n"
        "2. 各パターンに該当するレースを簡潔に紐付ける。\n"
        "3. モデル/買い目ロジックへの **具体的な改善提言** を 2〜3 個挙げる。\n"
        "4. 出力は Discord 投稿用に簡潔な Markdown で、全体 1500 文字以内。"
        "見出しと箇条書きを使い、断定しすぎず根拠を添えること。\n"
    )


def analyze_losses(
    missed_races: list[dict[str, Any]],
    *,
    client: Any | None = None,
    model: str = _MODEL,
) -> str:
    """不的中レース群を Claude に渡し、敗因の言語化テキストを返す。

    Args:
        missed_races: extract_missed_races() の戻り値。
        client:       anthropic.Anthropic 互換クライアント。None なら自動生成。
        model:        使用モデル ID（既定 claude-opus-4-8）。

    Returns:
        敗因分析テキスト。対象ゼロ件なら API を呼ばず既定メッセージを返す。
    """
    if not missed_races:
        return "分析対象（EV1.0以上の不的中レース）はありませんでした。"

    if client is None:
        import anthropic

        client = anthropic.Anthropic()

    prompt = build_analysis_prompt(missed_races)
    msg = client.messages.create(
        model=model,
        max_tokens=_MAX_TOKENS,
        thinking={"type": "adaptive"},  # claude-api スキル準拠（複雑な分析）
        messages=[{"role": "user", "content": prompt}],
    )
    # thinking ブロックを飛ばし最初の text ブロックを採用。
    text = next((b.text for b in msg.content if getattr(b, "type", "") == "text"), "")
    return text.strip()


# ── 3. Discord への報告連携 ──────────────────────────────────────────────────


def post_analysis_to_discord(
    analysis_text: str,
    *,
    notifier: _Notifier | None = None,
) -> bool:
    """敗因分析テキストを Discord へ投稿する。

    Args:
        analysis_text: 投稿本文。空白のみなら送信しない。
        notifier:      send_text を持つ通知器。None なら DiscordNotifier を生成。

    Returns:
        送信した場合 True、空文のためスキップした場合 False。
    """
    if not analysis_text or not analysis_text.strip():
        logger.info("敗因分析テキストが空のため Discord 送信をスキップします。")
        return False

    if notifier is None:
        from src.notification.discord_notifier import DiscordNotifier

        notifier = DiscordNotifier(channel_label="敗因分析")

    header = "🔍 **【敗因分析レポート】EV1.0以上の不的中レース自己診断**\n\n"
    notifier.send_text(header + analysis_text.strip())
    return True


# ── オーケストレーション ──────────────────────────────────────────────────────


def run_post_race_analysis(
    *,
    db_path: Path | None = None,
    ev_threshold: float = _DEFAULT_EV_THRESHOLD,
    since: str | None = None,
    limit: int = _DEFAULT_LIMIT,
    dry_run: bool = False,
) -> str:
    """抽出→Claude 分析→Discord 投稿 を一気通貫で実行する。

    Args:
        db_path:      DB パス（None で既定）。
        ev_threshold: 対象とする最低 EV。
        since:        集計開始日 "YYYY-MM-DD"。
        limit:        抽出上限。
        dry_run:      True なら Discord 送信を行わず分析テキストのみ返す。

    Returns:
        生成した敗因分析テキスト。
    """
    conn = get_connection(db_path)
    try:
        missed = extract_missed_races(
            conn, ev_threshold=ev_threshold, since=since, limit=limit
        )
    finally:
        conn.close()

    logger.info("敗因分析: 対象 %d 件を抽出しました。", len(missed))
    analysis = analyze_losses(missed)

    if not dry_run:
        post_analysis_to_discord(analysis)
    return analysis


def _fmt(value: Any) -> str:
    """None を '—' に、数値を簡潔な文字列に整形する。"""
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def main() -> None:
    """CLI エントリポイント: py -m src.analysis.post_race_analyzer [--dry-run]。"""
    import argparse
    import sys

    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    parser = argparse.ArgumentParser(description="EV1.0以上の不的中レース敗因分析")
    parser.add_argument("--since", help="集計開始日 YYYY-MM-DD", default=None)
    parser.add_argument("--ev", type=float, default=_DEFAULT_EV_THRESHOLD)
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT)
    parser.add_argument(
        "--dry-run", action="store_true", help="Discord 送信せず標準出力のみ"
    )
    args = parser.parse_args()

    text = run_post_race_analysis(
        ev_threshold=args.ev, since=args.since, limit=args.limit, dry_run=args.dry_run
    )
    print(text)


if __name__ == "__main__":
    main()
