"""
1分間隔でリアルタイムオッズを odds_timeseries テーブルに記録するスクリプト。
scheduler.py から呼び出される（5:00〜17:30 の毎分）。

使用方法:
    py scripts/record_odds_timeseries.py          # 当日の全レース
    py scripts/record_odds_timeseries.py <race_id> # 特定レース（将来拡張用）
"""
from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import date

sys.stdout.reconfigure(encoding="utf-8")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def record_today(db_path: str = "data/umalogi.db") -> int:
    """当日の realtime_odds を odds_timeseries へコピー記録する。"""
    conn = sqlite3.connect(db_path)
    today = date.today().isoformat()

    rows = conn.execute(
        """
        SELECT ro.race_id, ro.horse_number, ro.win_odds,
               ro.place_odds_min, ro.place_odds_max, ro.popularity
        FROM realtime_odds ro
        JOIN races r ON r.race_id = ro.race_id
        WHERE r.date = ?
        """,
        (today,),
    ).fetchall()

    if not rows:
        logger.debug("当日の realtime_odds なし: %s", today)
        conn.close()
        return 0

    conn.executemany(
        """
        INSERT INTO odds_timeseries
            (race_id, horse_number, win_odds, place_odds_min, place_odds_max, popularity)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.info("odds_timeseries に %d 件記録 (%s)", len(rows), today)
    conn.close()
    return len(rows)


if __name__ == "__main__":
    record_today()
