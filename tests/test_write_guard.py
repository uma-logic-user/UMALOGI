"""DB書き込み前 文字化けガード（src/database/write_guard.py）のテスト。"""

from __future__ import annotations

import sqlite3

from src.database.write_guard import (
    GuardedConnection,
    clean_params,
    guard_connection,
    is_write_sql,
)


# ---- is_write_sql -------------------------------------------------------
def test_is_write_sql_detects_insert_update_replace() -> None:
    assert is_write_sql("INSERT INTO t VALUES(?)")
    assert is_write_sql("  update t set a=?")
    assert is_write_sql("INSERT OR REPLACE INTO t VALUES(?)")
    assert is_write_sql("REPLACE INTO t VALUES(?)")


def test_is_write_sql_ignores_reads() -> None:
    assert not is_write_sql("SELECT * FROM t")
    assert not is_write_sql("PRAGMA table_info(t)")
    assert not is_write_sql("CREATE TABLE t(a TEXT)")


# ---- clean_params -------------------------------------------------------
def test_clean_params_sanitizes_control_chars_in_tuple() -> None:
    dirty = ("正常", "ガイア\x01フォース", 5, None, 3.2)
    cleaned = clean_params(dirty)
    assert cleaned[0] == "正常"
    assert cleaned[1] == "ガイアフォース"  # 制御文字除去
    assert cleaned[2] == 5  # 数値は不変
    assert cleaned[3] is None
    assert cleaned[4] == 3.2


def test_clean_params_recovers_or_blanks_garbled_name() -> None:
    # JVLink ?X?X パターンは回復不能なら空文字へ（文字化けのままは絶対書かない）
    cleaned = clean_params(("?A?h?}?C",))
    assert "?A" not in cleaned[0]


def test_clean_params_handles_dict() -> None:
    cleaned = clean_params({"name": "テスト\x7f", "n": 1})
    assert cleaned["name"] == "テスト"
    assert cleaned["n"] == 1


def test_clean_params_none() -> None:
    assert clean_params(None) is None


# ---- GuardedConnection end-to-end --------------------------------------
def test_guarded_connection_blocks_garbled_write() -> None:
    raw = sqlite3.connect(":memory:")
    raw.execute("CREATE TABLE horses(id INTEGER, name TEXT)")
    con = guard_connection(raw)
    assert isinstance(con, GuardedConnection)
    # 制御文字混入の名前を INSERT → 保存時にサニタイズされる
    con.execute("INSERT INTO horses VALUES(?, ?)", (1, "ステレン\x01ボッシュ"))
    con.commit()
    stored = raw.execute("SELECT name FROM horses WHERE id=1").fetchone()[0]
    assert stored == "ステレンボッシュ"


def test_guarded_connection_executemany_sanitizes_each_row() -> None:
    raw = sqlite3.connect(":memory:")
    raw.execute("CREATE TABLE t(id INTEGER, name TEXT)")
    con = guard_connection(raw)
    con.executemany(
        "INSERT INTO t VALUES(?, ?)",
        [(1, "正常"), (2, "汚れ\x02た名")],
    )
    con.commit()
    names = [r[0] for r in raw.execute("SELECT name FROM t ORDER BY id").fetchall()]
    assert names == ["正常", "汚れた名"]


def test_guarded_connection_select_params_untouched() -> None:
    raw = sqlite3.connect(":memory:")
    raw.execute("CREATE TABLE t(id INTEGER, name TEXT)")
    raw.execute("INSERT INTO t VALUES(1, '正常')")
    con = guard_connection(raw)
    # SELECT のパラメータはサニタイズ対象外（検索条件を壊さない）
    row = con.execute("SELECT name FROM t WHERE id=?", (1,)).fetchone()
    assert row[0] == "正常"


def test_guarded_connection_delegates_attributes() -> None:
    raw = sqlite3.connect(":memory:")
    con = guard_connection(raw)
    # commit/close など Connection の属性へ委譲できる
    con.execute("CREATE TABLE t(a INTEGER)")
    con.commit()
    con.close()
