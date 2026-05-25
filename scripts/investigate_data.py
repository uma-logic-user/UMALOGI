"""データ品質調査スクリプト"""

import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
conn = sqlite3.connect(str(ROOT / "data" / "umalogi.db"))

print("=== 異常EV予測トップ15 ===")
rows = conn.execute("""
    SELECT id, race_id, model_type, bet_type,
           expected_value, confidence, combination_json, notes
    FROM predictions
    WHERE expected_value > 100
    ORDER BY expected_value DESC
    LIMIT 15
""").fetchall()
for r in rows:
    notes = (r[7] or "")[:60]
    print(f"  id={r[0]} race={r[1]} model={r[2]} type={r[3]}")
    print(f"    EV={r[4]:.1f} conf={r[5]:.8f} combo={r[6][:30]} notes={notes}")

print()
print("=== EV>100 の件数分布 ===")
rows2 = conn.execute("""
    SELECT model_type, bet_type, COUNT(*) as cnt,
           MIN(expected_value), MAX(expected_value)
    FROM predictions
    WHERE expected_value > 100
    GROUP BY model_type, bet_type
    ORDER BY MAX(expected_value) DESC
""").fetchall()
for r in rows2:
    print(f"  {r[0]} {r[1]}: {r[2]}件 EV [{r[3]:.1f}, {r[4]:.1f}]")

print()
print("=== EV>100 のrace_id月別分布 ===")
rows3 = conn.execute("""
    SELECT substr(race_id,1,8) as prefix, COUNT(*)
    FROM predictions
    WHERE expected_value > 100
    GROUP BY prefix
    ORDER BY prefix
""").fetchall()
for r in rows3:
    print(f"  {r[0]}: {r[1]}件")

print()
print("=== 単勝 payout>5000 のレコード ===")
rows4 = conn.execute("""
    SELECT race_id, combination, payout, popularity
    FROM race_payouts
    WHERE race_id LIKE '2026%' AND bet_type='単勝' AND payout > 5000
    ORDER BY payout DESC
    LIMIT 20
""").fetchall()
for r in rows4:
    print(f"  race={r[0]} horse={r[1]} payout={r[2]} pop={r[3]}")

print()
print("=== 2026年の bet_type 別集計 ===")
rows5 = conn.execute("""
    SELECT bet_type, COUNT(*) as cnt, MIN(payout), MAX(payout), AVG(payout)
    FROM race_payouts
    WHERE race_id LIKE '2026%'
    GROUP BY bet_type
    ORDER BY cnt DESC
""").fetchall()
for r in rows5:
    print(f"  {r[0]}: {r[1]}件 [{r[2]:.0f}, {r[3]:.0f}] avg={r[4]:.0f}")

print()
print("=== 单勝で horse の rank が 1 でない異常払戻 ===")
rows6 = conn.execute("""
    SELECT rp.race_id, rp.combination, rp.payout,
           rr.rank, rr.horse_name
    FROM race_payouts rp
    LEFT JOIN race_results rr
        ON rp.race_id = rr.race_id
        AND CAST(rr.horse_number AS TEXT) = rp.combination
    WHERE rp.race_id LIKE '2026%'
        AND rp.bet_type = '単勝'
        AND (rr.rank IS NULL OR CAST(rr.rank AS INTEGER) != 1)
    ORDER BY rp.payout DESC
    LIMIT 20
""").fetchall()
for r in rows6:
    print(f"  race={r[0]} combo={r[1]} payout={r[2]} rank={r[3]} name={r[4]}")

print()
print("=== 単複 bet_type の詳細（存在する場合）===")
rows7 = conn.execute("""
    SELECT id, race_id, bet_type, combination, payout, popularity
    FROM race_payouts
    WHERE bet_type = '単複'
    LIMIT 10
""").fetchall()
for r in rows7:
    print(f"  id={r[0]} race={r[1]} type={r[2]} combo={r[3]} payout={r[4]} pop={r[5]}")
if not rows7:
    print("  (単複レコードなし)")

# predictions テーブルの全EV分布
print()
print("=== predictions EV分布（全体）===")
rows8 = conn.execute("""
    SELECT
        CASE
            WHEN expected_value < 0 THEN '<0'
            WHEN expected_value < 1 THEN '0-1'
            WHEN expected_value < 2 THEN '1-2'
            WHEN expected_value < 5 THEN '2-5'
            WHEN expected_value < 10 THEN '5-10'
            WHEN expected_value < 100 THEN '10-100'
            WHEN expected_value < 1000 THEN '100-1000'
            ELSE '>=1000'
        END as range,
        COUNT(*) as cnt
    FROM predictions
    GROUP BY 1
    ORDER BY MIN(expected_value)
""").fetchall()
for r in rows8:
    print(f"  EV {r[0]}: {r[1]:,}件")

conn.close()
print("\n=== 調査完了 ===")
