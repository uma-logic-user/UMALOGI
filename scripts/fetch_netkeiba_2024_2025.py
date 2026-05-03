"""
2024-2025年 全レースの race_results を netkeiba から一括取得するスクリプト。

実行例:
    python scripts/fetch_netkeiba_2024_2025.py
    python scripts/fetch_netkeiba_2024_2025.py --year 2024
    python scripts/fetch_netkeiba_2024_2025.py --delay 2.0
    python scripts/fetch_netkeiba_2024_2025.py --resume   # 途中再開（既に取得済みはスキップ）
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_netkeiba")


def get_target_dates(year: int | None, date_from: str, date_to: str) -> list[str]:
    from src.database.init_db import init_db
    conn = init_db()
    if year:
        date_from = f"{year}-01-01"
        date_to   = f"{year}-12-31"
    rows = conn.execute(
        """
        SELECT DISTINCT r.date
        FROM races r
        WHERE r.date BETWEEN ? AND ?
        ORDER BY r.date
        """,
        (date_from, date_to),
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


def get_missing_dates(date_from: str, date_to: str) -> list[str]:
    """race_results が 1件も存在しない日付だけ返す（再開用）。"""
    from src.database.init_db import init_db
    conn = init_db()
    rows = conn.execute(
        """
        SELECT DISTINCT r.date
        FROM races r
        WHERE r.date BETWEEN ? AND ?
          AND EXISTS (
            SELECT 1 FROM races r2 WHERE r2.date = r.date
            AND NOT EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r2.race_id)
          )
        ORDER BY r.date
        """,
        (date_from, date_to),
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year",      type=int, help="対象年（例: 2024）")
    ap.add_argument("--date-from", default="2024-01-01")
    ap.add_argument("--date-to",   default="2025-12-31")
    ap.add_argument("--delay",     type=float, default=1.5, help="リクエスト間隔秒")
    ap.add_argument("--resume",    action="store_true", help="取得済み日付をスキップ")
    args = ap.parse_args()

    date_from = f"{args.year}-01-01" if args.year else args.date_from
    date_to   = f"{args.year}-12-31" if args.year else args.date_to

    if args.resume:
        dates = get_missing_dates(date_from, date_to)
        logger.info("再開モード: 未取得 %d日を処理", len(dates))
    else:
        dates = get_target_dates(args.year, date_from, date_to)
        logger.info("全件モード: %d日を処理", len(dates))

    if not dates:
        logger.info("対象なし。終了。")
        return

    from src.ops.data_sync import sync_results_from_netkeiba

    total_saved = 0
    t0 = time.time()

    for i, date_iso in enumerate(dates, 1):
        date_yyyymmdd = date_iso.replace("-", "")
        try:
            saved = sync_results_from_netkeiba(date_yyyymmdd, delay=args.delay)
            total_saved += saved
        except Exception as exc:
            logger.error("[%d/%d] 例外 %s: %s", i, len(dates), date_iso, exc)
            continue

        if i % 10 == 0 or i == len(dates):
            elapsed = time.time() - t0
            rate    = i / elapsed
            remaining = (len(dates) - i) / rate if rate > 0 else 0
            logger.info(
                "進捗: %d/%d日 保存=%d (%.2f日/秒, 残り%.0f分)",
                i, len(dates), total_saved, rate, remaining / 60,
            )

    elapsed_total = time.time() - t0
    logger.info("完了: %d日処理, %d レース保存, 所要%.1f分", len(dates), total_saved, elapsed_total / 60)


if __name__ == "__main__":
    main()
