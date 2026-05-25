"""
entries.horse_weight バックフィルスクリプト

race_results.horse_weight が存在するのに entries.horse_weight が NULL の行を
race_results の値で埋める。

使用方法:
    py scripts/backfill_horse_weight.py            # 過去90日分
    py scripts/backfill_horse_weight.py --days 30  # 直近30日分
    py scripts/backfill_horse_weight.py --all       # 全件
"""

from __future__ import annotations

import argparse
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8")


def backfill(
    db_path: str = "data/umalogi.db", days: int = 90, all_records: bool = False
) -> None:
    conn = sqlite3.connect(db_path)

    if all_records:
        date_filter = ""
        params: list = []
    else:
        date_filter = f"AND r.date >= date('now', '-{days} days')"
        params = []

    # entries に存在するが horse_weight が NULL で、
    # race_results には horse_weight がある行を更新する
    result = conn.execute(
        f"""
        UPDATE entries
        SET
            horse_weight      = (
                SELECT rr.horse_weight
                FROM race_results rr
                WHERE rr.race_id      = entries.race_id
                  AND rr.horse_number = entries.horse_number
                  AND rr.horse_weight IS NOT NULL
                  AND rr.horse_weight > 0
                LIMIT 1
            ),
            horse_weight_diff = (
                SELECT rr.horse_weight_diff
                FROM race_results rr
                WHERE rr.race_id      = entries.race_id
                  AND rr.horse_number = entries.horse_number
                  AND rr.horse_weight IS NOT NULL
                  AND rr.horse_weight > 0
                LIMIT 1
            )
        WHERE entries.horse_weight IS NULL
          AND EXISTS (
              SELECT 1 FROM races r
              WHERE r.race_id = entries.race_id
              {date_filter}
          )
          AND EXISTS (
              SELECT 1 FROM race_results rr
              WHERE rr.race_id      = entries.race_id
                AND rr.horse_number = entries.horse_number
                AND rr.horse_weight IS NOT NULL
                AND rr.horse_weight > 0
          )
        """,
        params,
    )
    updated = result.rowcount
    conn.commit()

    # 結果確認
    total, hw_count = conn.execute(
        "SELECT COUNT(*), COUNT(horse_weight) FROM entries"
    ).fetchone()
    print(f"バックフィル完了: {updated:,} 件更新")
    print(
        f"entries 全体: {hw_count:,} / {total:,} ({hw_count / total * 100:.1f}%) が horse_weight あり"
    )

    # 直近30日の確認
    rows = conn.execute("""
        SELECT r.date, COUNT(e.id) as total, COUNT(e.horse_weight) as with_hw
        FROM entries e
        JOIN races r ON r.race_id = e.race_id
        WHERE r.date >= date('now', '-30 days')
        GROUP BY r.date
        ORDER BY r.date DESC
        LIMIT 10
    """).fetchall()
    print()
    print("直近10日の horse_weight カバレッジ:")
    for row in rows:
        pct = row[2] / row[1] * 100 if row[1] > 0 else 0
        print(f"  {row[0]}: {row[2]:,}/{row[1]:,} ({pct:.0f}%)")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--all", action="store_true", dest="all_records")
    parser.add_argument("--db", default="data/umalogi.db")
    args = parser.parse_args()
    backfill(db_path=args.db, days=args.days, all_records=args.all_records)
