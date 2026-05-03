"""
2025年の不良データを races / race_results / race_payouts から完全削除する。

使用例:
    py scripts/purge_2025_data.py
    py scripts/purge_2025_data.py --dry-run   # 削除件数の確認のみ
    py scripts/purge_2025_data.py --year 2025 # 年を指定（デフォルト=2025）
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


def purge_year(conn: sqlite3.Connection, year: int, dry_run: bool) -> dict[str, int]:
    prefix = f"{year}%"
    counts: dict[str, int] = {}

    for table, join in [
        ("race_payouts", "JOIN races r ON race_payouts.race_id=r.race_id"),
        ("race_results", "JOIN races r ON race_results.race_id=r.race_id"),
        ("races",        ""),
    ]:
        if join:
            cnt = conn.execute(
                f"SELECT COUNT(*) FROM {table} {join} WHERE r.date LIKE ?", (prefix,)
            ).fetchone()[0]
        else:
            cnt = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE date LIKE ?", (prefix,)
            ).fetchone()[0]
        counts[table] = cnt
        print(f"  {table:<20s}: {cnt:>8,} 件 → 削除対象")

    if dry_run:
        print("  [DRY-RUN: 削除しません]")
        return counts

    # FK 無効化してから子テーブル→親テーブルの順で削除
    conn.execute("PRAGMA foreign_keys = OFF")
    for child in ("race_payouts", "race_results", "predictions",
                  "_predictions_old", "entries", "realtime_odds"):
        conn.execute(
            f"DELETE FROM {child} WHERE race_id IN "
            "(SELECT race_id FROM races WHERE date LIKE ?)", (prefix,)
        )
    conn.execute("DELETE FROM races WHERE date LIKE ?", (prefix,))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()

    print("  削除完了 ✓")
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description="指定年の不良レースデータを削除する")
    ap.add_argument("--year",    type=int, default=2025)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    print("=" * 60)
    print(f"  {args.year}年データ削除")
    if args.dry_run:
        print("  [DRY-RUN モード]")
    print("=" * 60)

    purge_year(conn, args.year, args.dry_run)
    conn.close()


if __name__ == "__main__":
    main()
