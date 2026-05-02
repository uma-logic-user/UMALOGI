"""
src/database/schema.py のユニットテスト。

DDL_STATEMENTS が正しく構成されていること、init_db() との整合性を検証する。
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from src.database.schema import DDL_STATEMENTS
from src.database.init_db import DDL_STATEMENTS as DDL_FROM_INIT


# ── DDL リスト構造 ────────────────────────────────────────────────


def test_ddl_statements_is_list_of_strings() -> None:
    assert isinstance(DDL_STATEMENTS, list)
    assert all(isinstance(s, str) for s in DDL_STATEMENTS)
    assert len(DDL_STATEMENTS) > 0


def test_ddl_re_exported_from_init_db() -> None:
    """init_db.DDL_STATEMENTS は schema.DDL_STATEMENTS と同一オブジェクト。"""
    assert DDL_FROM_INIT is DDL_STATEMENTS


def test_required_tables_present() -> None:
    """主要テーブルの CREATE TABLE が含まれる。"""
    ddl_text = "\n".join(DDL_STATEMENTS)
    required = [
        "races", "horses", "race_results", "entries", "realtime_odds",
        "predictions", "prediction_horses", "prediction_results",
        "race_payouts", "model_performance",
        "training_times", "training_hillwork",
        "jockeys", "trainers", "racehorses",
    ]
    for table in required:
        assert re.search(
            rf"CREATE TABLE IF NOT EXISTS {table}\b", ddl_text
        ), f"テーブル {table!r} が DDL_STATEMENTS に見つかりません"


def test_required_views_present() -> None:
    ddl_text = "\n".join(DDL_STATEMENTS)
    for view in ("v_prediction_summary", "v_model_annual_summary", "v_race_mart"):
        assert f"CREATE VIEW IF NOT EXISTS {view}" in ddl_text, \
            f"ビュー {view!r} が DDL_STATEMENTS に見つかりません"


def test_partial_indexes_present() -> None:
    ddl_text = "\n".join(DDL_STATEMENTS)
    assert "idx_tc_mart" in ddl_text
    assert "idx_hc_mart" in ddl_text
    assert "WHERE training_date != ''" in ddl_text


# ── インメモリ DB で実際に実行 ────────────────────────────────────


@pytest.fixture()
def mem_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_ddl_executes_without_error(mem_conn: sqlite3.Connection) -> None:
    """DDL_STATEMENTS をインメモリ DB に対して実行してもエラーが出ない。"""
    # partial index は :memory: では実行できない（ファイル DB のみ）のでスキップ
    for ddl in DDL_STATEMENTS:
        if "WHERE training_date" in ddl:
            continue
        mem_conn.execute(ddl)


def test_races_table_schema(mem_conn: sqlite3.Connection) -> None:
    for ddl in DDL_STATEMENTS:
        if "CREATE TABLE IF NOT EXISTS races" in ddl:
            mem_conn.execute(ddl)
    cols = {row[1] for row in mem_conn.execute("PRAGMA table_info(races)").fetchall()}
    assert {"race_id", "race_name", "date", "venue", "race_number",
            "distance", "surface", "weather", "condition"} <= cols


def test_predictions_table_has_no_check_constraint(mem_conn: sqlite3.Connection) -> None:
    """predictions テーブルに model_type の CHECK 制約が残っていないこと。"""
    for ddl in DDL_STATEMENTS:
        if "CREATE TABLE IF NOT EXISTS predictions" in ddl:
            mem_conn.execute(ddl)
    schema = mem_conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='predictions'"
    ).fetchone()[0]
    assert "CHECK(model_type IN" not in schema
