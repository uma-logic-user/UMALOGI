"""
multi_odds テーブルの DB 操作テスト。

検証対象:
  1. マイグレーション #17 でテーブル・インデックスが作成されること
  2. insert_multi_odds() が正しく挿入し行数を返すこと
  3. INSERT OR IGNORE が重複をスキップすること
  4. MultiOddsEntry.recorded_at のデフォルト値が設定されること
"""

from __future__ import annotations

import sqlite3

import pytest

from src.database.init_db import MultiOddsEntry, init_db, insert_multi_odds


@pytest.fixture()
def conn(tmp_path):
    """インメモリではなくテンポラリファイル DB で初期化する。"""
    db_file = tmp_path / "test.db"
    c = init_db(db_file)
    yield c
    c.close()


def _entry(
    race_id: str = "202404010101",
    combo: str = "1-2",
    bet_type: str = "馬連",
    odds: float = 15.4,
    recorded_at: str = "2024-04-01 10:00:00",
) -> MultiOddsEntry:
    return MultiOddsEntry(
        race_id=race_id,
        bet_type=bet_type,
        combination=combo,
        odds=odds,
        odds_max=None,
        popularity=1,
        recorded_at=recorded_at,
    )


class TestMigration:
    def test_table_exists(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='multi_odds'"
        ).fetchone()
        assert row is not None, "multi_odds テーブルが存在しない"

    def test_index_exists(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='multi_odds'"
        ).fetchall()
        index_names = {r[0] for r in rows}
        assert "idx_mo_race_bet" in index_names
        assert "idx_mo_race_id" in index_names

    def test_check_constraint_rejects_invalid_bet_type(
        self, conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO multi_odds (race_id, bet_type, combination, recorded_at) "
                "VALUES (?, ?, ?, ?)",
                ("202404010101", "無効券種", "1-2", "2024-04-01 10:00:00"),
            )


class TestInsertMultiOdds:
    def test_insert_single_returns_one(self, conn: sqlite3.Connection) -> None:
        n = insert_multi_odds(conn, [_entry()])
        assert n == 1

    def test_inserted_row_is_readable(self, conn: sqlite3.Connection) -> None:
        e = _entry(combo="3-5", odds=28.0, bet_type="馬連")
        insert_multi_odds(conn, [e])
        row = conn.execute(
            "SELECT combination, odds, bet_type FROM multi_odds WHERE race_id=?",
            (e.race_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "3-5"
        assert row[1] == pytest.approx(28.0)
        assert row[2] == "馬連"

    def test_insert_multiple_entries(self, conn: sqlite3.Connection) -> None:
        entries = [
            _entry(combo="1-2", recorded_at="2024-04-01 10:00:00"),
            _entry(combo="1-3", recorded_at="2024-04-01 10:00:00"),
            _entry(combo="2-3", recorded_at="2024-04-01 10:00:00"),
        ]
        n = insert_multi_odds(conn, entries)
        assert n == 3

    def test_duplicate_ignored(self, conn: sqlite3.Connection) -> None:
        e = _entry()
        n1 = insert_multi_odds(conn, [e])
        n2 = insert_multi_odds(conn, [e])  # 同一 UNIQUE キー
        assert n1 == 1
        assert n2 == 0  # INSERT OR IGNORE でスキップ

    def test_wide_odds_max_stored(self, conn: sqlite3.Connection) -> None:
        e = MultiOddsEntry(
            race_id="202404010101",
            bet_type="ワイド",
            combination="1-2",
            odds=3.5,
            odds_max=5.2,
            popularity=2,
            recorded_at="2024-04-01 10:00:00",
        )
        insert_multi_odds(conn, [e])
        row = conn.execute(
            "SELECT odds, odds_max FROM multi_odds WHERE bet_type='ワイド'",
        ).fetchone()
        assert row[0] == pytest.approx(3.5)
        assert row[1] == pytest.approx(5.2)

    def test_default_recorded_at_is_set(self) -> None:
        e = MultiOddsEntry(race_id="X", bet_type="馬連", combination="1-2")
        assert e.recorded_at != ""
        assert len(e.recorded_at) == 19  # "YYYY-MM-DD HH:MM:SS"

    @pytest.mark.parametrize(
        "bet_type", ["枠連", "馬連", "ワイド", "馬単", "三連複", "三連単"]
    )
    def test_all_valid_bet_types_accepted(
        self, conn: sqlite3.Connection, bet_type: str
    ) -> None:
        e = MultiOddsEntry(
            race_id="202404010101",
            bet_type=bet_type,
            combination="1-2",
            recorded_at="2024-04-01 10:00:00",
        )
        n = insert_multi_odds(conn, [e])
        assert n == 1

    def test_empty_list_returns_zero(self, conn: sqlite3.Connection) -> None:
        # 空リストは挿入なしで 0 を返す
        # insert_multi_odds は空リストに対して entries[0] を参照しないこと
        n = insert_multi_odds(conn, [])
        assert n == 0
