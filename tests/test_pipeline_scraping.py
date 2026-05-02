"""
src/pipeline/scraping.py のユニット／結合テスト。

JVDataLoader / requests 等の外部 I/O はモックして DB 操作のみ実テストする。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.scraping import (
    ensure_race_record,
    save_entries_to_db,
    fetch_and_save_odds,
)


# ── フィクスチャ ─────────────────────────────────────────────────


@pytest.fixture()
def mem_db() -> sqlite3.Connection:
    """最小限のスキーマを持つインメモリ DB を返す。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT NOT NULL,
            date TEXT NOT NULL,
            venue TEXT NOT NULL DEFAULT '',
            race_number INTEGER NOT NULL DEFAULT 0,
            distance INTEGER NOT NULL DEFAULT 0,
            surface TEXT NOT NULL DEFAULT '',
            weather TEXT NOT NULL DEFAULT '',
            condition TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id TEXT NOT NULL,
            horse_number INTEGER NOT NULL,
            gate_number INTEGER NOT NULL DEFAULT 0,
            horse_id TEXT,
            horse_name TEXT NOT NULL,
            sex_age TEXT NOT NULL DEFAULT '',
            weight_carried REAL NOT NULL DEFAULT 0,
            jockey TEXT NOT NULL DEFAULT '',
            trainer TEXT NOT NULL DEFAULT '',
            horse_weight INTEGER,
            horse_weight_diff INTEGER,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(race_id, horse_number)
        );
        CREATE TABLE realtime_odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id TEXT NOT NULL,
            horse_number INTEGER NOT NULL,
            horse_name TEXT NOT NULL DEFAULT '',
            win_odds REAL,
            place_odds_min REAL,
            place_odds_max REAL,
            popularity INTEGER,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    return conn


RACE_ID = "202506050701"
DATE_STR = "20250605"


# ── ensure_race_record ────────────────────────────────────────────


def test_ensure_race_record_inserts_when_missing(mem_db: sqlite3.Connection) -> None:
    ensure_race_record(mem_db, RACE_ID, DATE_STR)
    row = mem_db.execute("SELECT race_id, race_number FROM races WHERE race_id=?", (RACE_ID,)).fetchone()
    assert row is not None
    assert row[0] == RACE_ID
    assert row[1] == 1  # race_id[-2:] = "01"


def test_ensure_race_record_does_not_overwrite_existing(mem_db: sqlite3.Connection) -> None:
    mem_db.execute(
        "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface, weather, condition)"
        " VALUES (?, '本番レース', '2025-06-05', '東京', 7, 1600, '芝', '晴', '良')",
        (RACE_ID,),
    )
    mem_db.commit()
    ensure_race_record(mem_db, RACE_ID, DATE_STR)
    row = mem_db.execute("SELECT race_name FROM races WHERE race_id=?", (RACE_ID,)).fetchone()
    assert row[0] == "本番レース"  # 上書きされていない


# ── save_entries_to_db ────────────────────────────────────────────


def _make_entry_table(race_id: str, n: int = 3) -> MagicMock:
    tbl = MagicMock()
    tbl.race_id = race_id
    entries = []
    for i in range(1, n + 1):
        h = MagicMock()
        h.horse_number      = i
        h.gate_number       = i
        h.horse_name        = f"馬{i:02d}"
        h.sex_age           = "牡3"
        h.weight_carried    = 55.0
        h.jockey            = f"騎手{i}"
        h.trainer           = f"調教師{i}"
        h.horse_weight      = 450 + i * 2
        h.horse_weight_diff = 0
        entries.append(h)
    tbl.entries = entries
    return tbl


def test_save_entries_to_db_inserts_rows(mem_db: sqlite3.Connection) -> None:
    mem_db.execute(
        "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface, weather, condition)"
        " VALUES (?, 'R', '2025-06-05', '東京', 7, 1600, '芝', '晴', '良')",
        (RACE_ID,),
    )
    tbl = _make_entry_table(RACE_ID, n=5)
    saved = save_entries_to_db(mem_db, tbl)
    assert saved == 5
    count = mem_db.execute("SELECT COUNT(*) FROM entries WHERE race_id=?", (RACE_ID,)).fetchone()[0]
    assert count == 5


def test_save_entries_to_db_returns_0_on_empty(mem_db: sqlite3.Connection) -> None:
    tbl = MagicMock()
    tbl.race_id = RACE_ID
    tbl.entries = []
    assert save_entries_to_db(mem_db, tbl) == 0


# ── fetch_and_save_odds ───────────────────────────────────────────


def test_fetch_and_save_odds_returns_0_when_both_fail(mem_db: sqlite3.Connection) -> None:
    """netkeiba も RTD も例外になるとき 0 を返す（全段失敗）。"""
    with (
        patch("src.database.init_db.insert_realtime_odds", side_effect=Exception("DB error")),
        patch("src.scraper.entry_table.fetch_realtime_odds", side_effect=Exception("net")),
        patch("src.scraper.rtd_reader.read_rtd_for_race", side_effect=Exception("rtd")),
    ):
        result = fetch_and_save_odds(mem_db, RACE_ID)
    assert result == 0
