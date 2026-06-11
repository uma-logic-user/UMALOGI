"""
src/marketing/sns_generator.py — SNS 集客テキスト・動画台本の自動生成

毎日の予測データ・的中実績から以下の 3 アセットを自動生成する:

  1. 無料公開用「勝率特化予想（盾）」ポスト
     — 的中率の高い本命系予想のみを公開し信頼を獲得する（EV穴馬は有料側に温存）。
  2. 前日的中実績ポスト
     — 確定 ROI・払戻を真コスト基準（pnl_accounting と同一式）で訴求。
  3. CapCut 等での動画化を見据えた台本（シーン/ナレーション/テロップ）。

すべての生成文に、サブスクリプションへ誘導する「キラーフレーズ」
（期待値の重要性・長期投資としての競馬）を論理的に織り込む。

既存 scripts/generate_sns_post.py（レース単位・note誘導）とは役割が異なり、
本モジュールは**日次バッチ**として 1 日分をまとめて生成する。

Usage:
    py -m src.marketing.sns_generator --date 20260614
    py -m src.marketing.sns_generator            # 既定: 今日
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date as dt_date
from pathlib import Path

from src.ml.bet_policy import is_live_bet
from src.utils.text import ensure_clean, is_garbled

_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR = _ROOT / "outputs" / "marketing"

# 無料公開する最大レース数（残りは有料側の価値として温存する「盾」運用）
_FREE_PICK_LIMIT: int = 3
_TAGS_FREE = "#競馬予想 #AI競馬 #無料予想 #UMALOGI"
_TAGS_HIT = "#競馬的中 #回収率 #AI競馬 #UMALOGI"

# ── サブスク導線キラーフレーズ（期待値・長期投資の論理で誘導）────────────────
# 「当てる自慢」ではなく「期待値で勝ち続ける仕組み」を売る。日替わりローテーション。
KILLER_PHRASES: tuple[str, ...] = (
    "競馬で勝ち続ける人は『当たる馬』ではなく『オッズが実力より高い馬』を買っています。"
    "それが期待値（EV）。UMA-Logicは全頭のEVを毎レース自動計算しています。",
    "1レースの的中は運。100レースの収支は数学です。"
    "長期回収率を決めるのは予想力ではなく、期待値1.0超だけを買い続ける規律。",
    "プロ投資家が株でやっていることを、私たちは競馬でやっています。"
    "勝率×オッズ＝期待値。この数字が1を超える馬券だけが『投資』です。",
    "外れた日も資金が守られる。ケリー基準でベット額まで自動算出するから、"
    "1日の負けで退場しない設計になっています。",
    "無料公開しているのは『当てやすい予想』。本当の利益の源泉である"
    "高期待値の穴馬リストは、購読者だけにお届けしています。",
    "回収率100%超を支えるのは閃きではなくデータ。"
    "JRA公式データを機械学習で解析し、市場の歪みだけを狙い撃ちます。",
)


def pick_killer_phrase(date_str: str, offset: int = 0) -> str:
    """日付から決定的にキラーフレーズを選ぶ（再現性あり・日替わり）。"""
    idx = (int(date_str.replace("-", "")[-6:]) + offset) % len(KILLER_PHRASES)
    return KILLER_PHRASES[idx]


# ── データ構造 ───────────────────────────────────────────────────────────
@dataclass
class FreePick:
    """無料公開用の 1 レース予想（勝率特化・本命のみ）。"""

    race_id: str
    venue: str
    race_number: int
    race_name: str
    horse_name: str
    confidence: float  # モデル信頼度（%換算前）


@dataclass
class HitSummary:
    """前日（指定日）の的中実績サマリ。"""

    date: str
    n_bets: int = 0
    n_hits: int = 0
    total_cost: float = 0.0
    total_payout: float = 0.0
    best_hit_payout: float = 0.0
    best_hit_race: str = ""
    best_hit_bet_type: str = ""

    @property
    def roi(self) -> float:
        return 100.0 * self.total_payout / self.total_cost if self.total_cost else 0.0

    @property
    def hit_rate(self) -> float:
        return 100.0 * self.n_hits / self.n_bets if self.n_bets else 0.0


@dataclass
class DailyPack:
    """1 日分の生成アセット。"""

    date: str
    free_pick_post: str = ""
    hit_report_post: str = ""
    video_script: str = ""
    files: list[Path] = field(default_factory=list)


# ── DB 抽出 ──────────────────────────────────────────────────────────────
def fetch_free_picks(
    conn: sqlite3.Connection, date_str: str, limit: int = _FREE_PICK_LIMIT
) -> list[FreePick]:
    """勝率特化（本命系）モデルの予想上位 limit レースを抽出する。

    各レースで predicted_rank=1 の馬（モデルの一番手）を採用し、
    信頼度（confidence・NULL なら model_score=勝率予測値）の高い順に公開する。
    直前版があれば直前版を優先（is_superseded=0 のみ）。
    馬名が空・文字化けの行は公開対象から除外する（CLAUDE.md §16）。
    """
    rows = conn.execute(
        """
        WITH honmei AS (
            SELECT p.id, p.race_id, p.confidence,
                   ROW_NUMBER() OVER (
                       PARTITION BY p.race_id ORDER BY p.created_at DESC
                   ) AS rn
            FROM predictions p
            JOIN races r ON r.race_id = p.race_id
            WHERE r.date = ?
              AND p.model_type LIKE '本命%'
              AND COALESCE(p.is_superseded, 0) = 0
        )
        SELECT h.race_id, r.venue, r.race_number, COALESCE(r.race_name, ''),
               ph.horse_name,
               COALESCE(h.confidence, ph.model_score, 0) AS conf
        FROM honmei h
        JOIN races r ON r.race_id = h.race_id
        JOIN prediction_horses ph
            ON ph.prediction_id = h.id AND ph.predicted_rank = 1
        WHERE h.rn = 1
          AND ph.horse_name IS NOT NULL AND ph.horse_name != ''
        ORDER BY conf DESC, r.race_number DESC
        """,
        (date_str,),
    ).fetchall()
    picks: list[FreePick] = []
    for row in rows:
        horse = ensure_clean(row[4])
        if not horse or is_garbled(horse):
            continue
        picks.append(
            FreePick(
                race_id=row[0],
                venue=ensure_clean(row[1] or ""),
                race_number=int(row[2] or 0),
                race_name=ensure_clean(row[3]),
                horse_name=horse,
                confidence=float(row[5]),
            )
        )
        if len(picks) >= limit:
            break
    return picks


def fetch_hit_summary(conn: sqlite3.Connection, date_str: str) -> HitSummary:
    """指定日の確定実績を真コスト基準（実コスト = payout − profit）で集計する。

    集計対象は実弾ベット（bet_policy.is_live_bet）のみ。
    観賞用モデル・多点買い券種を混ぜると「実際に張った成績」と乖離し、
    SNS で公開する数字の誠実性が崩れるため。
    """
    summary = HitSummary(date=date_str)
    rows = conn.execute(
        """
        SELECT COALESCE(pr.payout, 0), COALESCE(pr.profit, 0),
               COALESCE(pr.is_hit, 0), p.bet_type, p.model_type,
               COALESCE(r.venue, '') || COALESCE(r.race_number, '') || 'R'
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date = ?
          AND pr.payout IS NOT NULL
          AND COALESCE(p.is_superseded, 0) = 0
        """,
        (date_str,),
    ).fetchall()
    for payout, profit, is_hit, bet_type, model_type, race_label in rows:
        if not is_live_bet(str(model_type or ""), str(bet_type or "")):
            continue
        cost = float(payout) - float(profit)
        summary.n_bets += 1
        summary.n_hits += int(is_hit)
        summary.total_cost += cost
        summary.total_payout += float(payout)
        if is_hit and float(payout) > summary.best_hit_payout:
            summary.best_hit_payout = float(payout)
            summary.best_hit_race = str(race_label)
            summary.best_hit_bet_type = str(bet_type or "")
    return summary


# ── テキスト生成 ─────────────────────────────────────────────────────────
def generate_free_pick_post(picks: list[FreePick], date_str: str) -> str:
    """無料公開用「勝率特化予想（盾）」ポストを Markdown で生成する。"""
    if not picks:
        return ""
    lines = [
        f"🐴 本日の無料AI予想（{_format_date_jp(date_str)}）",
        "",
        "AIが「勝率」で選んだ自信レースだけを無料公開👇",
        "",
    ]
    for p in picks:
        conf_pct = p.confidence * 100 if p.confidence <= 1.0 else p.confidence
        race_label = f"{p.venue}{p.race_number}R"
        if p.race_name:
            race_label += f"（{p.race_name}）"
        # 信頼度が取れない予想は数値を出さない（0%表示は信頼を毀損する）
        suffix = f"（AI勝率予測 {conf_pct:.0f}%）" if conf_pct >= 1.0 else ""
        lines.append(f"◎ {race_label}: **{p.horse_name}**{suffix}")
    lines += [
        "",
        f"💡 {pick_killer_phrase(date_str, offset=0)}",
        "",
        "🔑 高期待値の穴馬・買い目・推奨ベット額は購読者限定で配信中。",
        "プロフィールのリンクから登録できます。",
        "",
        _TAGS_FREE,
    ]
    return "\n".join(lines)


def generate_hit_report_post(summary: HitSummary, date_str: str) -> str:
    """前日的中実績ポストを Markdown で生成する（実績なしの日は空文字）。"""
    if summary.n_bets == 0:
        return ""
    lines = [f"📊 昨日の AI 収支報告（{_format_date_jp(summary.date)}）", ""]
    if summary.n_hits > 0:
        lines += [
            f"✅ 的中 {summary.n_hits} 件 / {summary.n_bets} 買い目"
            f"（的中率 {summary.hit_rate:.0f}%）",
            f"💰 回収率 **{summary.roi:.0f}%**"
            f"（投資 ¥{summary.total_cost:,.0f} → 払戻 ¥{summary.total_payout:,.0f}）",
        ]
        if summary.best_hit_payout > 0:
            lines.append(
                f"🎯 最高払戻: {summary.best_hit_race} "
                f"{summary.best_hit_bet_type} ¥{summary.best_hit_payout:,.0f}"
            )
    else:
        # 負けた日も隠さず公開する（誠実性が長期の信頼＝LTV を作る）
        lines += [
            f"❌ 昨日は {summary.n_bets} 買い目で的中なし。",
            "それでも淡々と続けられるのは、1日の結果ではなく"
            "長期の期待値で設計しているからです。",
        ]
    lines += [
        "",
        f"💡 {pick_killer_phrase(date_str, offset=1)}",
        "",
        "📈 全買い目・収支は購読者向けに毎日全公開しています。",
        "",
        _TAGS_HIT,
    ]
    return "\n".join(lines)


def generate_video_script(
    picks: list[FreePick], summary: HitSummary, date_str: str
) -> str:
    """CapCut 等での動画化を見据えたシーン分割台本を生成する。"""
    date_jp = _format_date_jp(date_str)
    scenes: list[tuple[str, str, str]] = []  # (映像指示, ナレーション, テロップ)

    scenes.append(
        (
            "ロゴアニメーション＋競馬場ドローン映像",
            f"{date_jp}、AI競馬予想 UMA-Logic の無料予想です。",
            f"UMA-Logic {date_jp} 無料AI予想",
        )
    )
    if summary.n_hits > 0:
        scenes.append(
            (
                "前日の的中馬券画像をズームイン",
                f"まず昨日の結果から。{summary.n_bets}点購入で{summary.n_hits}点的中、"
                f"回収率は{summary.roi:.0f}パーセントでした。",
                f"昨日の回収率 {summary.roi:.0f}%",
            )
        )
    for i, p in enumerate(picks, start=1):
        conf_pct = p.confidence * 100 if p.confidence <= 1.0 else p.confidence
        conf_phrase = (
            f"AIの勝率予測は{conf_pct:.0f}パーセントです。" if conf_pct >= 1.0 else ""
        )
        scenes.append(
            (
                f"出馬表をスクロール→{p.horse_name} をハイライト",
                f"無料予想その{i}。{p.venue}{p.race_number}レースの本命は"
                f"{p.horse_name}。{conf_phrase}",
                f"◎ {p.venue}{p.race_number}R {p.horse_name}",
            )
        )
    scenes.append(
        (
            "グラフ（期待値曲線）アニメーション",
            pick_killer_phrase(date_str, offset=2),
            "競馬は期待値で勝つ「投資」",
        )
    )
    scenes.append(
        (
            "購読ページのスクリーンキャプチャ＋CTAボタン強調",
            "高期待値の穴馬リストと推奨ベット額は、概要欄のリンクから"
            "購読者限定で配信しています。",
            "高EV穴馬リストは購読者限定 → 概要欄へ",
        )
    )

    lines = [f"# 動画台本 — {date_jp} UMA-Logic デイリー", ""]
    for i, (visual, narration, telop) in enumerate(scenes, start=1):
        lines += [
            f"## シーン{i}",
            f"- 映像: {visual}",
            f"- ナレーション: {narration}",
            f"- テロップ: {telop}",
            "",
        ]
    return "\n".join(lines)


def _format_date_jp(date_str: str) -> str:
    """YYYY-MM-DD / YYYYMMDD → M月D日 表記。"""
    s = date_str.replace("-", "")
    return f"{int(s[4:6])}月{int(s[6:8])}日"


def normalize_date8(value: str | None) -> dt_date | None:
    """任意形式の日付文字列を date に正規化する（失敗時 None・例外を出さない）。

    "20260614" / "2026-06-14" / "2026/06/14" を受理。非実在日（20260632 等）や
    桁不足は None。自動運用バッチが引数不正でトレースバック死しないための門番。
    """
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 8:
        return None
    try:
        return dt_date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        return None


# ── 日次パック生成（エントリポイント）────────────────────────────────────
def generate_daily_pack(
    conn: sqlite3.Connection,
    target_date: str,
    prev_date: str,
    out_dir: Path | None = None,
) -> DailyPack:
    """当日の無料予想＋前日実績＋動画台本を生成しファイル出力する。

    Args:
        conn: DB 接続。
        target_date: 予想対象日（YYYY-MM-DD）。
        prev_date: 実績集計日（YYYY-MM-DD・通常は前開催日）。
        out_dir: 出力先（省略時 outputs/marketing/YYYYMMDD/）。
    """
    pack = DailyPack(date=target_date)
    picks = fetch_free_picks(conn, target_date)
    summary = fetch_hit_summary(conn, prev_date)

    pack.free_pick_post = generate_free_pick_post(picks, target_date)
    pack.hit_report_post = generate_hit_report_post(summary, target_date)
    pack.video_script = generate_video_script(picks, summary, target_date)

    dest = out_dir or (_OUT_DIR / target_date.replace("-", ""))
    dest.mkdir(parents=True, exist_ok=True)
    for name, content in (
        ("free_pick.md", pack.free_pick_post),
        ("hit_report.md", pack.hit_report_post),
        ("video_script.md", pack.video_script),
    ):
        if not content:
            continue
        path = dest / name
        path.write_text(content + "\n", encoding="utf-8")
        pack.files.append(path)
    return pack


def main() -> int:
    """CLI: 指定日（既定=今日）の SNS アセット一式を生成する。"""
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    parser = argparse.ArgumentParser(description="SNS集客アセットの日次自動生成")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（既定: 今日）")
    parser.add_argument(
        "--prev-date", default=None, help="実績集計日 YYYYMMDD（既定: 前日）"
    )
    parser.add_argument("--db", default=str(_DB_PATH), help="DB パス")
    args = parser.parse_args()

    target_d = normalize_date8(args.date) if args.date else dt_date.today()
    if target_d is None:
        print(f"⚠️ 不正な --date: {args.date!r}（YYYYMMDD 形式で指定してください）")
        return 2
    target = target_d.isoformat()
    if args.prev_date:
        prev_d = normalize_date8(args.prev_date)
        if prev_d is None:
            print(f"⚠️ 不正な --prev-date: {args.prev_date!r}")
            return 2
    else:
        prev_d = dt_date.fromordinal(target_d.toordinal() - 1)
    prev = prev_d.isoformat()

    conn = sqlite3.connect(args.db)
    try:
        pack = generate_daily_pack(conn, target, prev)
    finally:
        conn.close()

    print(f"生成完了: {len(pack.files)} ファイル")
    for f in pack.files:
        print(f"  - {f}")
    if not pack.files:
        print("（対象日の予想・実績データなし）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
