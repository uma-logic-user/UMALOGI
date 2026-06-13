"""W-088: races.grade 列のマイグレーション適用＋既存レースのバックフィル。

init_db() でマイグレーション #21 を適用後、race_name から `_extract_grade` で
グレード/クラスを導出して races.grade を UPDATE する。
predictions テーブルには一切触れない（条項1）。races の grade 充填のみ。
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db
from src.scraper.jravan_client import _extract_grade

conn = init_db()

# マイグレーション適用確認
cols = [r[1] for r in conn.execute("PRAGMA table_info(races)").fetchall()]
assert "grade" in cols, "grade 列が追加されていない"
print("OK: races.grade 列が存在")

rows = conn.execute("SELECT race_id, race_name, grade FROM races").fetchall()
print(f"対象レース: {len(rows)} 件")

updates: list[tuple[str, str]] = []
for race_id, race_name, cur_grade in rows:
    derived = _extract_grade(race_name or "")
    # 既存値が空の場合のみ充填（既存の正準値は上書きしない）
    if derived and not (cur_grade or "").strip():
        updates.append((derived, race_id))

with conn:
    conn.executemany("UPDATE races SET grade = ? WHERE race_id = ?", updates)

print(f"バックフィル更新: {len(updates)} 件")

# 結果サマリ
import collections

dist = collections.Counter(
    r[0] for r in conn.execute("SELECT grade FROM races").fetchall()
)
print("--- grade 分布 ---")
for g, c in sorted(dist.items(), key=lambda x: -x[1]):
    label = repr(g) if g else "''(空)"
    print(f"  {label}: {c}")
