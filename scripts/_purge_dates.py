"""5/2・5/3 のデータを完全パージして件数を報告する。"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

TARGET_DATES = ("2026-05-02", "2026-05-03")
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "umalogi.db"

conn = sqlite3.connect(str(DB_PATH))
conn.execute("PRAGMA foreign_keys = OFF")
conn.execute("PRAGMA journal_mode = WAL")

# 対象 race_id を先に収集
race_ids = [
    row[0]
    for d in TARGET_DATES
    for row in conn.execute("SELECT race_id FROM races WHERE date = ?", (d,)).fetchall()
]
print(f"対象 race_id: {len(race_ids)} 件")

if not race_ids:
    print("削除対象なし")
    conn.close()
    sys.exit(0)

ph = ",".join("?" * len(race_ids))

totals: dict[str, int] = {}

# prediction_results → prediction_horses → predictions の順で先に削除
pred_ids = [
    r[0]
    for r in conn.execute(
        f"SELECT id FROM predictions WHERE race_id IN ({ph})", race_ids
    ).fetchall()
]
if pred_ids:
    ph2 = ",".join("?" * len(pred_ids))
    c = conn.execute(f"DELETE FROM prediction_results WHERE prediction_id IN ({ph2})", pred_ids)
    totals["prediction_results"] = c.rowcount
    c = conn.execute(f"DELETE FROM prediction_horses WHERE prediction_id IN ({ph2})", pred_ids)
    totals["prediction_horses"] = c.rowcount
    c = conn.execute(f"DELETE FROM predictions WHERE id IN ({ph2})", pred_ids)
    totals["predictions"] = c.rowcount

for table in ("race_payouts", "race_results", "entries"):
    try:
        c = conn.execute(f"DELETE FROM {table} WHERE race_id IN ({ph})", race_ids)
        totals[table] = c.rowcount
    except Exception as e:
        totals[table] = -1
        print(f"  {table}: {e}")

c = conn.execute(
    f"DELETE FROM races WHERE race_id IN ({ph})", race_ids
)
totals["races"] = c.rowcount

conn.commit()
conn.execute("PRAGMA foreign_keys = ON")
conn.close()

print("\n=== 削除完了 ===")
total_rows = 0
for table, cnt in totals.items():
    print(f"  {table}: {cnt} 行削除")
    if cnt > 0:
        total_rows += cnt
print(f"\n  合計: {total_rows} 行削除")
