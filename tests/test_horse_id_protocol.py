"""馬ID紐付けマスタープロトコル（check_integrity / upsert_horses_data）のテスト。"""

from __future__ import annotations

import sqlite3

import pytest

from src.database.check_integrity import (
    IntegrityViolation,
    assert_integrity,
    check_integrity,
)
from src.database.upsert_horses_data import (
    build_name_master,
    resolve_missing_horse_ids,
    upsert_horse,
)


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE horses (
            horse_id TEXT PRIMARY KEY, horse_name TEXT,
            sire TEXT, dam TEXT, dam_sire TEXT,
            created_at TEXT DEFAULT '', updated_at TEXT DEFAULT ''
        );
        CREATE TABLE racehorses (
            horse_id TEXT PRIMARY KEY, horse_name TEXT DEFAULT '',
            sex TEXT DEFAULT '', birth_year INTEGER, birth_date TEXT DEFAULT '',
            coat_color TEXT DEFAULT ''
        );
        CREATE TABLE race_results (
            id INTEGER PRIMARY KEY, race_id TEXT, horse_id TEXT,
            horse_name TEXT, sex_age TEXT
        );
        """
    )
    return conn


def test_upsert_horse_inserts_then_appends_pedigree() -> None:
    conn = _make_db()
    upsert_horse(conn, "2021100001", "テストホース")
    # 2 回目で血統を追記、馬名は維持。
    upsert_horse(conn, "2021100001", "テストホース", sire="父馬", dam="母馬")
    row = conn.execute(
        "SELECT horse_name, sire, dam FROM horses WHERE horse_id='2021100001'"
    ).fetchone()
    assert row == ("テストホース", "父馬", "母馬")
    # 既存の非空 sire を空で上書きしない。
    upsert_horse(conn, "2021100001", "テストホース", sire="")
    assert (
        conn.execute("SELECT sire FROM horses WHERE horse_id='2021100001'").fetchone()[
            0
        ]
        == "父馬"
    )


def test_build_name_master_dedupes_ambiguous() -> None:
    conn = _make_db()
    conn.executemany(
        "INSERT INTO racehorses (horse_id, horse_name, sex, birth_year) VALUES (?,?,?,?)",
        [
            ("2021100001", "ユニーク", "牡", 2021),
            ("2020100002", "ドウメイ", "牡", 2020),
            ("2020100003", "ドウメイ", "牡", 2020),  # 同名同年同性 → 曖昧
        ],
    )
    master = build_name_master(conn)
    assert master[("ユニーク", 2021, "牡")] == "2021100001"
    assert ("ドウメイ", 2020, "牡") not in master  # 曖昧は除外


def test_resolve_missing_horse_ids_links_unique() -> None:
    conn = _make_db()
    conn.execute(
        "INSERT INTO racehorses (horse_id, horse_name, sex, birth_year) "
        "VALUES ('2021100001','ナマヨセウマ','牡',2021)"
    )
    # 2024 開催・牡3 → 生年 2021 で一意特定できる。
    conn.execute(
        "INSERT INTO race_results (id, race_id, horse_id, horse_name, sex_age) "
        "VALUES (1, '202401010101', NULL, 'ナマヨセウマ', '牡3')"
    )
    res = resolve_missing_horse_ids(conn, apply=True)
    assert res.candidates == 1
    assert res.resolved == 1
    assert res.applied == 1
    assert (
        conn.execute("SELECT horse_id FROM race_results WHERE id=1").fetchone()[0]
        == "2021100001"
    )


def test_resolve_skips_when_birth_year_mismatch() -> None:
    conn = _make_db()
    conn.execute(
        "INSERT INTO racehorses (horse_id, horse_name, sex, birth_year) "
        "VALUES ('2019100001','チガウトシ','牡',2019)"
    )
    conn.execute(
        "INSERT INTO race_results (id, race_id, horse_id, horse_name, sex_age) "
        "VALUES (1, '202401010101', NULL, 'チガウトシ', '牡3')"  # 生年2021 → 不一致
    )
    res = resolve_missing_horse_ids(conn, apply=True)
    assert res.resolved == 0


def test_check_integrity_detects_composite_dup() -> None:
    conn = _make_db()
    conn.executemany(
        "INSERT INTO racehorses (horse_id, horse_name, sex, birth_year, birth_date, coat_color) "
        "VALUES (?,?,?,?,?,?)",
        [
            ("2021100001", "オセン", "牡", 2021, "2021/03/01", "鹿毛"),
            (
                "2021100099",
                "オセン",
                "牡",
                2021,
                "2021/03/01",
                "鹿毛",
            ),  # 同composite別ID
        ],
    )
    rep = check_integrity(conn)
    assert rep.composite_dup_groups == 1
    assert rep.has_critical
    with pytest.raises(IntegrityViolation):
        assert_integrity(conn)


def test_check_integrity_clean_passes() -> None:
    conn = _make_db()
    conn.execute(
        "INSERT INTO racehorses (horse_id, horse_name, sex, birth_year, birth_date, coat_color) "
        "VALUES ('2021100001','キレイ','牡',2021,'2021/03/01','鹿毛')"
    )
    rep = assert_integrity(conn)
    assert not rep.has_critical
