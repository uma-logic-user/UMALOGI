"""tests/test_generate_performance_report.py — generate_performance_report 単体テスト"""
from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "generate_performance_report.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_performance_report", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mem_db():
    """インメモリ SQLite に実際のスキーマと同等のテストデータを投入する。"""
    conn = sqlite3.connect(":memory:")
    # predictions テーブル（bet_type, recommended_bet）
    conn.execute("""
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            bet_type TEXT,
            recommended_bet REAL
        )
    """)
    # prediction_results テーブル（is_hit, payout, recorded_at）
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            is_hit INTEGER,
            payout REAL,
            recorded_at TEXT
        )
    """)
    # 直近28日: 10件中3件的中, 投資3000円, 回収4500円 (ROI=150%)
    for i in range(10):
        conn.execute(
            "INSERT INTO predictions VALUES (?,?,'複勝',300)",
            (i, f"r{i:03d}"),
        )
    for i in range(7):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,0,0,date('now','-'||?||' days'))",
            (i, i, i),
        )
    for i in range(7, 10):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,1,1500,date('now','-'||?||' days'))",
            (i, i, i),
        )
    conn.commit()
    return conn


def test_build_report_returns_string(mem_db):
    """build_performance_report() が文字列を返す。"""
    mod = _load_module()
    report = mod.build_performance_report(mem_db, days=28)
    assert isinstance(report, str)
    assert "ROI" in report
    assert "的中率" in report


def test_build_report_roi_calculation(mem_db):
    """ROI が正しく計算される (4500/3000 = 150%)。"""
    mod = _load_module()
    report = mod.build_performance_report(mem_db, days=28)
    assert "150.0%" in report or "150%" in report


def test_build_report_empty_db():
    """データなし DB でも例外なく空のレポートが返る。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            bet_type TEXT,
            recommended_bet REAL
        )
    """)
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            is_hit INTEGER,
            payout REAL,
            recorded_at TEXT
        )
    """)
    conn.commit()
    mod = _load_module()
    report = mod.build_performance_report(conn, days=28)
    assert isinstance(report, str)
    assert "ROI" in report  # ヘッダーは必ず含まれる
