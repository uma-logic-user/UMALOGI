"""
X シグナルパーサー  Phase B
============================

x_signals テーブルの未解析 (parsed=0) ポストを Claude Haiku API で
構造化し、以下を抽出・更新する:

  - race_id     : races テーブルとの突合（会場コード + レース番号でマッチ）
  - horse_number: 主シグナル馬番（◎が最優先、以下 ○▲△ の順）
  - signal_type : "honmei" / "ana" / "keshi"
  - confidence  : 0.0〜1.0（Haiku が推定する言語確信度）
  - parsed      : 0 → 1

【設計制約】
  - tweet_id UNIQUE のため 1ツイート1行。複数馬が言及された場合は
    最重要シグナル（◎があれば◎の馬）を優先して更新する。
  - Haiku API は 10 ツイートをバッチ処理してコストを抑える。
  - API キー未設定時はルールベースのフォールバックで処理する。

Usage:
  python src/ml/x_signal_parser.py
  python src/ml/x_signal_parser.py --date 2026-05-17
  python src/ml/x_signal_parser.py --dry-run
  python src/ml/x_signal_parser.py --limit 50

Functions:
  get_x_consensus_score(conn, race_id) -> dict[int, float]
    EV 計算用の会場別コンセンサススコアを返す（FEATURE_COLS 統合用）。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)

_BATCH_SIZE = 10           # 1回の Haiku 呼び出しで処理するツイート数
_HAIKU_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS  = 2048

# 会場コード → 競馬場名（race_id[4:6] と対応）
_JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}
# 逆引き: 競馬場名 → コード
_VENUE_TO_CODE: dict[str, str] = {v: k for k, v in _JYO.items()}

# ルールベースフォールバック用
_RE_HORSE_NUM = re.compile(r"(?<!\d)([1-9][0-9]?)(?:番|枠)")
_RE_RACE_REF  = re.compile(
    r"(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)"
    r"[\s　]*(\d{1,2})[ＲRr]"
)
_SIGNAL_PRIORITY: dict[str, tuple[str, float]] = {
    "◎": ("honmei", 1.0),
    "○": ("honmei", 0.8),
    "▲": ("ana",    0.7),
    "△": ("ana",    0.5),
    "×": ("keshi",  0.6),
    "✕": ("keshi",  0.6),
}


@dataclass
class ParsedSignal:
    """Haiku またはルールベースの解析結果（1ツイート1件）。"""

    signal_id:    int
    tweet_id:     str
    race_id:      str | None      = None
    horse_number: int | None      = None
    signal_type:  str | None      = None
    confidence:   float           = 0.5
    race_name_raw: str | None     = None
    raw_reason:   str             = ""


@dataclass
class _RaceInfo:
    """race_id と表示用文字列のペア。"""

    race_id:      str
    venue_code:   str
    venue_name:   str
    race_number:  int
    display:      str = field(init=False)

    def __post_init__(self) -> None:
        self.display = f"{self.venue_name}{self.race_number}R"


# ── レース一覧取得 ─────────────────────────────────────────────────────

def _get_races_on_date(conn: sqlite3.Connection, target_date: str) -> list[_RaceInfo]:
    """指定日の全レース情報を取得する。"""
    rows = conn.execute(
        "SELECT race_id, race_number FROM races WHERE date = ? ORDER BY race_id",
        (target_date,),
    ).fetchall()
    result = []
    for race_id, race_number in rows:
        code = race_id[4:6]
        name = _JYO.get(code, code)
        result.append(_RaceInfo(
            race_id=race_id,
            venue_code=code,
            venue_name=name,
            race_number=int(race_number or 0),
        ))
    return result


def _match_race_id(
    venue_hint: str | None,
    race_num_hint: int | None,
    races: list[_RaceInfo],
) -> str | None:
    """会場名 + レース番号からレースIDを特定する。"""
    if not venue_hint or race_num_hint is None:
        return None
    code = _VENUE_TO_CODE.get(venue_hint)
    if not code:
        return None
    for r in races:
        if r.venue_code == code and r.race_number == race_num_hint:
            return r.race_id
    return None


# ── ルールベースパーサー（Haiku API 不使用時のフォールバック） ─────────

def _rule_based_parse(
    signal_id: int,
    tweet_id:  str,
    raw_text:  str,
    races:     list[_RaceInfo],
) -> ParsedSignal:
    """正規表現ベースで馬番・シグナルタイプ・レース名を抽出する。"""
    result = ParsedSignal(signal_id=signal_id, tweet_id=tweet_id)

    # レース特定
    m_race = _RE_RACE_REF.search(raw_text)
    if m_race:
        result.race_name_raw = m_race.group(0)
        result.race_id = _match_race_id(
            m_race.group(1), int(m_race.group(2)), races
        )

    # 最優先シグナル記号を探す
    best_mark: str | None = None
    best_pos:  int = len(raw_text) + 1
    for mark in _SIGNAL_PRIORITY:
        idx = raw_text.find(mark)
        if idx != -1 and idx < best_pos:
            best_pos  = idx
            best_mark = mark

    if best_mark:
        sig_type, conf = _SIGNAL_PRIORITY[best_mark]
        result.signal_type = sig_type
        result.confidence  = conf
        # シグナル記号の直後の馬番を探す（番/枠なしの裸数字も許容）
        after = raw_text[best_pos + 1: best_pos + 10]
        # 「◎7番」「◎7枠」「◎7」どれも対応。0番は JRA に存在しないため除外。
        m_num = re.search(r"([1-9][0-9]?)(?:番|枠)?", after)
        if m_num:
            result.horse_number = int(m_num.group(1))
    elif m := _RE_HORSE_NUM.search(raw_text):
        # シグナル記号なしで馬番だけ言及されている場合
        result.horse_number = int(m.group(1))
        result.confidence   = 0.4

    result.raw_reason = "rule-based"
    return result


# ── Haiku API パーサー ────────────────────────────────────────────────

def _build_haiku_prompt(
    tweets: list[dict[str, Any]],
    races:  list[_RaceInfo],
) -> str:
    races_ctx = "\n".join(f"  - {r.display} (race_id: {r.race_id})" for r in races)
    if not races_ctx:
        races_ctx = "  （この日の開催情報なし）"

    lines = []
    for i, t in enumerate(tweets):
        lines.append(f"[{i}] tweet_id={t['tweet_id']}\n    投稿者: @{t['screen_name']}\n    本文: {t['raw_text']}")
    tweets_ctx = "\n\n".join(lines)

    return f"""あなたは競馬予想テキスト解析の専門家です。
以下のX（旧Twitter）投稿を読み、競馬予想シグナルを抽出してください。

【当日の開催レース一覧】
{races_ctx}

【解析対象ポスト ({len(tweets)} 件)】
{tweets_ctx}

【出力ルール】
- 必ず配列 JSON のみを出力してください（コードブロック不要）。
- 配列の要素数は投稿数と一致させてください（[0]〜[{len(tweets)-1}]）。
- 各要素のフィールド:
  - "idx"          : 投稿の番号（0始まり整数）
  - "tweet_id"     : そのままコピー
  - "race_name_raw": テキストから読み取った「東京11R」等の文字列。不明なら null
  - "race_id"      : 上記レース一覧にある race_id と突合した値。不明なら null
  - "horse_number" : 主シグナル馬番（◎が最優先、以下 ○▲△ の順）。なければ null
  - "signal_type"  : "honmei"（◎○）/ "ana"（▲△）/ "keshi"（×✕）のいずれか。なければ null
  - "confidence"   : 確信度 0.0〜1.0（明示的な◎+馬番=1.0、推測主体=0.4 程度）
  - "note"         : 判断根拠（1行・日本語）

例:
[
  {{"idx": 0, "tweet_id": "abc123", "race_name_raw": "東京11R", "race_id": "202605050511",
    "horse_number": 7, "signal_type": "honmei", "confidence": 0.95,
    "note": "◎7番と明記されており東京11Rとの紐付けが確実"}},
  ...
]
"""


def _call_haiku(prompt: str, api_key: str) -> list[dict[str, Any]]:
    """Claude Haiku API を呼び出して JSON リストを返す。"""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=_HAIKU_MODEL,
        max_tokens=_MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    # コードブロックがあれば除去
    text = re.sub(r"^```(?:json)?\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    return json.loads(text)


def _haiku_batch_parse(
    batch: list[dict[str, Any]],
    races: list[_RaceInfo],
    api_key: str,
) -> list[ParsedSignal]:
    """Haiku で 1バッチを解析し ParsedSignal リストを返す。"""
    prompt = _build_haiku_prompt(batch, races)
    try:
        items = _call_haiku(prompt, api_key)
    except Exception as exc:
        logger.error("Haiku API 呼び出し失敗: %s → ルールベースにフォールバック", exc)
        return [
            _rule_based_parse(
                t["signal_id"], t["tweet_id"], t["raw_text"], races
            )
            for t in batch
        ]

    results: list[ParsedSignal] = []
    for item in items:
        idx = int(item.get("idx", 0))
        if idx >= len(batch):
            continue
        orig = batch[idx]
        ps = ParsedSignal(
            signal_id=orig["signal_id"],
            tweet_id=orig["tweet_id"],
            race_id=item.get("race_id"),
            horse_number=item.get("horse_number"),
            signal_type=item.get("signal_type"),
            confidence=float(item.get("confidence", 0.5)),
            race_name_raw=item.get("race_name_raw"),
            raw_reason=f"haiku: {item.get('note', '')}",
        )
        results.append(ps)
    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# メインクラス
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class XSignalParser:
    """
    未解析の x_signals レコードを Haiku API（またはルールベース）で
    構造化し、DB を更新するパーサー。

    Parameters
    ----------
    conn      : sqlite3.Connection
    api_key   : str | None  Claude API キー。None の場合はルールベース。
    dry_run   : bool        True の場合 DB を更新しない。
    """

    def __init__(
        self,
        conn:    sqlite3.Connection,
        *,
        api_key: str | None = None,
        dry_run: bool = False,
    ) -> None:
        self._conn    = conn
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._dry_run = dry_run

    # ── メインエントリ ─────────────────────────────────────────────────

    def parse_unparsed(
        self,
        target_date: str | None = None,
        *,
        limit: int | None = None,
    ) -> dict[str, int]:
        """
        未解析シグナルをバッチ処理で構造化する。

        Parameters
        ----------
        target_date : "YYYY-MM-DD" or None（None=全件）
        limit       : 処理件数上限

        Returns
        -------
        {"parsed": int, "skipped": int, "errors": int}
        """
        rows = self._fetch_unparsed(target_date, limit)
        if not rows:
            logger.info("[XParser] 未解析シグナルなし")
            return {"parsed": 0, "skipped": 0, "errors": 0}

        logger.info("[XParser] 未解析: %d 件 / モード: %s",
                    len(rows), "Haiku" if self._api_key else "ルールベース")

        # 投稿日別にレース一覧をキャッシュ
        races_cache: dict[str, list[_RaceInfo]] = {}

        stats = {"parsed": 0, "skipped": 0, "errors": 0}
        batch: list[dict[str, Any]] = []

        def _flush(b: list[dict[str, Any]], date_key: str) -> None:
            races = races_cache.setdefault(
                date_key, _get_races_on_date(self._conn, date_key)
            )
            if self._api_key:
                results = _haiku_batch_parse(b, races, self._api_key)
            else:
                results = [
                    _rule_based_parse(
                        t["signal_id"], t["tweet_id"], t["raw_text"], races
                    )
                    for t in b
                ]
            for ps in results:
                self._update_signal(ps, stats)

        current_date: str | None = None
        for row in rows:
            posted_at = row["posted_at"]
            row_date  = posted_at[:10]  # "YYYY-MM-DD"

            if row_date != current_date and batch:
                _flush(batch, current_date)  # type: ignore[arg-type]
                batch = []

            current_date = row_date
            batch.append({
                "signal_id":   row["signal_id"],
                "tweet_id":    row["tweet_id"],
                "screen_name": row["screen_name"],
                "raw_text":    row["raw_text"],
            })

            if len(batch) >= _BATCH_SIZE:
                _flush(batch, current_date)  # type: ignore[arg-type]
                batch = []

        if batch and current_date:
            _flush(batch, current_date)

        if not self._dry_run:
            self._conn.commit()

        logger.info("[XParser] 完了: %s", stats)
        return stats

    # ── DB 操作 ────────────────────────────────────────────────────────

    def _fetch_unparsed(
        self,
        target_date: str | None,
        limit: int | None,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT signal_id, tweet_id, screen_name, raw_text, posted_at
            FROM   x_signals
            WHERE  parsed = 0
        """
        params: list[Any] = []
        if target_date:
            sql += " AND date(posted_at) = ?"
            params.append(target_date)
        sql += " ORDER BY posted_at"
        if limit:
            sql += f" LIMIT {int(limit)}"
        self._conn.row_factory = sqlite3.Row
        return self._conn.execute(sql, params).fetchall()

    def _update_signal(
        self,
        ps:    ParsedSignal,
        stats: dict[str, int],
    ) -> None:
        if self._dry_run:
            logger.info(
                "[DRY-RUN] signal_id=%d race_id=%s horse=%s type=%s conf=%.2f  %s",
                ps.signal_id, ps.race_id, ps.horse_number,
                ps.signal_type, ps.confidence, ps.raw_reason,
            )
            stats["parsed"] += 1
            return

        try:
            self._conn.execute(
                """
                UPDATE x_signals
                SET    race_id      = ?,
                       horse_number = ?,
                       signal_type  = ?,
                       confidence   = ?,
                       race_name_raw = COALESCE(?, race_name_raw),
                       parsed       = 1
                WHERE  signal_id    = ?
                """,
                (
                    ps.race_id,
                    ps.horse_number,
                    ps.signal_type,
                    ps.confidence,
                    ps.race_name_raw,
                    ps.signal_id,
                ),
            )
            stats["parsed"] += 1
        except Exception as exc:
            logger.error("signal_id=%d 更新失敗: %s", ps.signal_id, exc)
            stats["errors"] += 1


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 特徴量統合インターフェース（Phase C で FEATURE_COLS に追加）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_x_consensus_score(
    conn:    sqlite3.Connection,
    race_id: str,
) -> dict[int, float]:
    """
    指定レースの X 世論コンセンサススコアを馬番別に返す。

    スコアは x_signals.confidence × x_accounts.weight の加重平均。
    honmei シグナル (+1.0)、ana シグナル (+0.5)、keshi シグナル (−0.3) で
    方向を付ける。

    Parameters
    ----------
    conn    : sqlite3.Connection
    race_id : 12桁レースID

    Returns
    -------
    {horse_number: consensus_score} — スコアなし馬は含まない
    """
    rows = conn.execute(
        """
        SELECT s.horse_number,
               s.signal_type,
               s.confidence,
               COALESCE(a.weight, 1.0) AS weight
        FROM   x_signals  s
        LEFT JOIN x_accounts a ON a.screen_name = s.screen_name
        WHERE  s.race_id      = ?
          AND  s.parsed       = 1
          AND  s.horse_number IS NOT NULL
          AND  s.signal_type  IS NOT NULL
        """,
        (race_id,),
    ).fetchall()

    if not rows:
        return {}

    # horse → (weighted_sum, total_weight)
    agg: dict[int, list[float]] = {}
    for horse_number, signal_type, confidence, weight in rows:
        direction = {"honmei": 1.0, "ana": 0.5, "keshi": -0.3}.get(signal_type, 0.0)
        contribution = direction * confidence * weight
        if horse_number not in agg:
            agg[horse_number] = [0.0, 0.0]
        agg[horse_number][0] += contribution
        agg[horse_number][1] += weight

    return {
        hn: (wsum / wtotal if wtotal > 0 else 0.0)
        for hn, (wsum, wtotal) in agg.items()
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="X シグナル構造化パーサー (Phase B)")
    p.add_argument("--date",    metavar="YYYY-MM-DD", help="対象日（デフォルト: 全件）")
    p.add_argument("--limit",   type=int,             help="処理件数上限")
    p.add_argument("--dry-run", action="store_true",  help="DB 更新をスキップ")
    p.add_argument("--status",  action="store_true",  help="x_signals の件数を表示して終了")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args()

    from src.database.init_db import init_db
    conn = init_db()
    conn.row_factory = sqlite3.Row

    if args.status:
        total   = conn.execute("SELECT COUNT(*) FROM x_signals").fetchone()[0]
        parsed  = conn.execute("SELECT COUNT(*) FROM x_signals WHERE parsed=1").fetchone()[0]
        pending = total - parsed
        accounts = conn.execute(
            "SELECT COUNT(*) FROM x_accounts WHERE is_active=1"
        ).fetchone()[0]
        print(f"x_signals: {total} 件  解析済み: {parsed}  未解析: {pending}")
        print(f"x_accounts: {accounts} 件 (active)")
        conn.close()
        return

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY 未設定 → ルールベースフォールバック")

    parser = XSignalParser(conn, api_key=api_key, dry_run=args.dry_run)
    stats = parser.parse_unparsed(
        target_date=args.date,
        limit=args.limit,
    )
    print(f"解析完了: {stats}")
    conn.close()


if __name__ == "__main__":
    main()
