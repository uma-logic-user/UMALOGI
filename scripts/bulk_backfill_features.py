"""netkeiba「上がり3F(last_3f)」の過去バルク・バックフィルバッチ（冪等）。

`race_results.last_3f` が NULL かつ 確定済み(rank IS NOT NULL) のレースを
指定期間（既定: 直近3年 2023-01-01〜当日）で特定し、netkeiba から結果を
再取得して last_3f を保存する。COALESCE 保存のため**冪等**（再実行で充填済みは
スキップ・既存値非破壊）。レート制限に配慮し各レース間に sleep を挟む。

使い方::

    py scripts/bulk_backfill_features.py --since 2023-01-01            # 直近3年
    py scripts/bulk_backfill_features.py --since 2025-01-01 --limit 50
    py scripts/bulk_backfill_features.py --dry-run                     # 対象列挙のみ

設計: 対象抽出とバックフィル本体を純粋関数に分離し、`fetcher` を注入可能にして
ネットワーク非依存でテストできるようにする。
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3  # noqa: E402

logger = logging.getLogger(__name__)

# netkeiba 負荷配慮の既定 sleep（秒）。http_client の RateLimiter と二重で安全側。
_DEFAULT_SLEEP = 1.2
_DEFAULT_SINCE = "2023-01-01"


def find_backfill_targets(
    conn: sqlite3.Connection,
    *,
    since: str = _DEFAULT_SINCE,
    until: str | None = None,
    limit: int | None = None,
) -> list[str]:
    """last_3f が NULL の確定レース race_id を期間内で抽出する（日付昇順）。

    確定済み(rank IS NOT NULL)のレースのみ対象（未確定は上がりが存在しない）。
    """
    until = until or date.today().isoformat()
    sql = (
        "SELECT DISTINCT rr.race_id FROM race_results rr "
        "JOIN races r ON r.race_id = rr.race_id "
        "WHERE rr.rank IS NOT NULL AND rr.last_3f IS NULL "
        "AND r.date >= ? AND r.date <= ? "
        "ORDER BY r.date, rr.race_id"
    )
    rows = conn.execute(sql, (since, until)).fetchall()
    ids = [r[0] for r in rows]
    return ids[:limit] if limit else ids


def _default_fetcher(race_id: str, conn: sqlite3.Connection) -> int:
    """netkeiba から結果を取得し last_3f を含めて upsert する（既定 fetcher）。"""
    from scripts.fetch_race_result import _upsert_race_results
    from src.scraper.netkeiba import fetch_race_results

    info = fetch_race_results(race_id, fetch_pedigree=False)
    return _upsert_race_results(conn, race_id, info)


def backfill_last_3f(
    conn: sqlite3.Connection,
    race_ids: list[str],
    *,
    sleep_sec: float = _DEFAULT_SLEEP,
    dry_run: bool = False,
    fetcher: Callable[[str, sqlite3.Connection], int] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """race_ids を順に再取得し last_3f を充填する（冪等・レート制限付き）。

    Args:
        conn: DB 接続。
        race_ids: 対象 race_id リスト。
        sleep_sec: 各レース間の待機秒（>=1 推奨・負荷配慮）。
        dry_run: True なら取得せず対象数のみ返す。
        fetcher: (race_id, conn)->保存件数。None で netkeiba 既定 fetcher。
        sleeper: sleep 関数（テスト注入用）。

    Returns:
        {"targets":N, "saved":N, "errors":N, "filled":N}。filled は last_3f が
        実際に NULL→値 になった race 数。
    """
    fetcher = fetcher or _default_fetcher
    stats = {"targets": len(race_ids), "saved": 0, "errors": 0, "filled": 0}
    if dry_run:
        logger.info("[DRY-RUN] バックフィル対象 %d レース", len(race_ids))
        return stats

    for i, rid in enumerate(race_ids, 1):
        try:
            fetcher(rid, conn)
            stats["saved"] += 1
            filled = conn.execute(
                "SELECT COUNT(*) FROM race_results "
                "WHERE race_id = ? AND last_3f IS NOT NULL",
                (rid,),
            ).fetchone()[0]
            if filled:
                stats["filled"] += 1
            logger.info("[%d/%d] %s 充填=%d", i, len(race_ids), rid, filled)
        except Exception as exc:  # noqa: BLE001 — 1レース失敗で全体を止めない
            stats["errors"] += 1
            logger.warning("[%d/%d] %s 取得失敗: %s", i, len(race_ids), rid, exc)
        if i < len(race_ids):
            sleeper(max(sleep_sec, 0.0))
    return stats


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="last_3f 過去バルク・バックフィル（冪等）")
    p.add_argument("--since", default=_DEFAULT_SINCE, help="開始日 YYYY-MM-DD（既定 2023-01-01）")
    p.add_argument("--until", help="終了日 YYYY-MM-DD（既定 当日）")
    p.add_argument("--limit", type=int, help="処理上限レース数")
    p.add_argument("--sleep", type=float, default=_DEFAULT_SLEEP, help="各レース間 sleep 秒")
    p.add_argument("--dry-run", action="store_true", help="対象列挙のみ（取得しない）")
    args = p.parse_args()

    from src.database.init_db import init_db

    conn = init_db()
    try:
        targets = find_backfill_targets(
            conn, since=args.since, until=args.until, limit=args.limit
        )
        logger.info(
            "バックフィル対象: %d レース (since=%s sleep=%.1fs)",
            len(targets),
            args.since,
            args.sleep,
        )
        stats = backfill_last_3f(
            conn, targets, sleep_sec=args.sleep, dry_run=args.dry_run
        )
    finally:
        conn.close()
    logger.info("完了: %s", stats)
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
