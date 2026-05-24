"""training_times テーブルに training_grade カラムを追加するマイグレーション"""
from __future__ import annotations

import sqlite3
import sys


def migrate(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(training_times)").fetchall()]
    if "training_grade" not in cols:
        conn.execute("ALTER TABLE training_times ADD COLUMN training_grade TEXT DEFAULT ''")
        conn.commit()
        print("training_grade カラムを追加しました")
    else:
        print("training_grade カラムは既に存在します")
    conn.close()


if __name__ == "__main__":
    migrate(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
