"""
DB データを Next.js 用 JSON にエクスポートするスクリプト。
実行: python web/generate_data.py  （プロジェクトルートから）
"""
import json
import sqlite3
import sys
from pathlib import Path

DB_PATH  = Path(__file__).parent.parent / "data" / "umalogi.db"
OUT_DIR  = Path(__file__).parent / "src" / "data"
OUT_FILE = OUT_DIR / "races.json"

RACE_ID  = "202506050811"


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: DB が見つかりません: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    race = conn.execute(
        "SELECT * FROM races WHERE race_id = ?", (RACE_ID,)
    ).fetchone()

    if race is None:
        print(f"ERROR: race_id={RACE_ID} が見つかりません", file=sys.stderr)
        sys.exit(1)

    results = conn.execute(
        """
        SELECT
            rr.rank, rr.horse_name, rr.horse_id,
            rr.sex_age, rr.weight_carried, rr.jockey,
            rr.finish_time, rr.margin,
            rr.win_odds, rr.popularity, rr.horse_weight,
            h.sire, h.dam, h.dam_sire
        FROM race_results rr
        LEFT JOIN horses h ON rr.horse_id = h.horse_id
        WHERE rr.race_id = ?
        ORDER BY rr.rank NULLS LAST, rr.id
        """,
        (RACE_ID,),
    ).fetchall()

    conn.close()

    payload = {
        "race": dict(race),
        "results": [dict(r) for r in results],
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"エクスポート完了: {OUT_FILE}  ({len(results)} 頭)")


if __name__ == "__main__":
    main()
