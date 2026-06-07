"""DB 書き込み前 文字化け強制クレンジング・ガード（CLAUDE.md タスク3 / W-072）。

SQLite への INSERT / UPDATE / REPLACE 実行直前に、すべての文字列パラメータを
``src.utils.text.ensure_clean`` で検証・修復する。文字化け（制御文字・JVLink CP932
リードバイト脱落・誤エンコード）を検知した場合は回復を試み、回復不能なら空文字に
落として「文字化けしたままの DB 書き込み」を物理的に不可能にする。

使い方::

    from src.database.write_guard import guard_connection
    conn = guard_connection(sqlite3.connect(path))
    conn.execute("INSERT INTO horses VALUES(?, ?)", (1, name))  # name は自動浄化

SELECT 等の読み取りクエリのパラメータはサニタイズしない（検索条件を壊さないため）。
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Mapping, Sequence

from src.utils.text import ensure_clean

# 先頭の空白・コメントを読み飛ばして書き込み系か判定する。
_WRITE_RE = re.compile(r"^\s*(?:INSERT|UPDATE|REPLACE)\b", re.IGNORECASE)


def is_write_sql(sql: str) -> bool:
    """SQL が INSERT / UPDATE / REPLACE（OR REPLACE 含む）かどうかを判定する。

    Args:
        sql: 判定対象の SQL 文字列。

    Returns:
        書き込み系クエリなら ``True``。
    """
    return bool(_WRITE_RE.match(sql or ""))


def _clean_value(v: Any) -> Any:
    """単一値を浄化する。文字列のみ ``ensure_clean`` を通し、他型はそのまま返す。"""
    if isinstance(v, str):
        # ensure_clean は回復不能な文字化けを fallback("") に落とす。
        # 正常文字列は制御文字除去・strip のみで内容を保持する。
        return ensure_clean(v, fallback="")
    return v


def clean_params(params: Any) -> Any:
    """execute / executemany のパラメータ群を再帰的に浄化する。

    Args:
        params: ``tuple`` / ``list`` / ``dict`` / ``None`` のいずれか。

    Returns:
        文字列要素のみ浄化した同型のパラメータ。``None`` は ``None``。
    """
    if params is None:
        return None
    if isinstance(params, Mapping):
        return {k: _clean_value(v) for k, v in params.items()}
    if isinstance(params, (list, tuple)):
        cleaned = [_clean_value(v) for v in params]
        return type(params)(cleaned) if isinstance(params, tuple) else cleaned
    # スカラー単体（稀）も浄化する。
    return _clean_value(params)


class GuardedConnection:
    """``sqlite3.Connection`` をラップし、書き込み時にパラメータを自動浄化するプロキシ。

    ``execute`` / ``executemany`` のみ介入し、その他の属性・メソッド
    （``commit`` / ``close`` / ``cursor`` / ``row_factory`` 等）は元の接続へ委譲する。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Any = None) -> sqlite3.Cursor:
        """書き込み系なら ``params`` を浄化してから実行する。"""
        if is_write_sql(sql) and params is not None:
            params = clean_params(params)
        if params is None:
            return self._conn.execute(sql)
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Sequence[Any]) -> sqlite3.Cursor:
        """書き込み系なら各行のパラメータを浄化してから実行する。"""
        if is_write_sql(sql):
            seq_of_params = [clean_params(p) for p in seq_of_params]
        return self._conn.executemany(sql, seq_of_params)

    @property
    def raw(self) -> sqlite3.Connection:
        """ラップ元の生 ``sqlite3.Connection`` を返す。"""
        return self._conn

    def __getattr__(self, name: str) -> Any:
        # execute/executemany 以外はすべて元の接続へ委譲。
        return getattr(self._conn, name)

    def __enter__(self) -> "GuardedConnection":
        self._conn.__enter__()
        return self

    def __exit__(self, *exc: Any) -> Any:
        return self._conn.__exit__(*exc)


def guard_connection(conn: sqlite3.Connection) -> GuardedConnection:
    """既存の ``sqlite3.Connection`` を文字化けガード付きプロキシでラップする。

    Args:
        conn: ラップ対象の接続。

    Returns:
        ``GuardedConnection``。``execute``/``executemany`` の書き込みが自動浄化される。
    """
    if isinstance(conn, GuardedConnection):
        return conn
    return GuardedConnection(conn)
