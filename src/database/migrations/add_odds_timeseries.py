"""odds_timeseries テーブルを新設するマイグレーション"""

from __future__ import annotations

import sqlite3
import sys

DDL = """
CREATE TABLE IF NOT EXISTS odds_timeseries (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        TEXT    NOT NULL,
    horse_number   INTEGER NOT NULL,
    win_odds       REAL,
    place_odds_min REAL,
    place_odds_max REAL,
    popularity     INTEGER,
    recorded_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_ots_race_horse ON odds_timeseries(race_id, horse_number);
CREATE INDEX IF NOT EXISTS idx_ots_recorded_at ON odds_timeseries(recorded_at)
"""


def migrate(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    print("odds_timeseries テーブルを作成しました")
    conn.close()


if __name__ == "__main__":
    migrate(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
