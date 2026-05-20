"""
scripts/generate_ab_report.py のユニットテスト。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


def _load_mod():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "generate_ab_report",
        Path(__file__).resolve().parents[2] / "scripts" / "generate_ab_report.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_ab_db() -> sqlite3.Connection:
    """V1/V2 両方の予想データを持つインメモリ DB。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE predictions (
            prediction_id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_version TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            race_id TEXT,
            bet_type TEXT,
            model_version TEXT,
            is_hit INTEGER,
            payout REAL,
            invested REAL,
            created_at TEXT
        )
    """)
    # V1: 10件中2件的中, 総投資3000, 総払戻2400 → ROI=80%
    for i in range(8):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v1',0,0,300,date('now','-'||?||' days'))",
            (i, i, f"r{i:03d}", i),
        )
    for i in range(8, 10):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v1',1,1200,300,date('now','-'||?||' days'))",
            (i, i, f"r{i:03d}", i),
        )
    # V2: 10件中4件的中, 総投資3000, 総払戻4800 → ROI=160%
    for i in range(10, 16):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v2',0,0,300,date('now','-'||?||' days'))",
            (100 + i, 100 + i, f"r{i:03d}", i - 10),
        )
    for i in range(16, 20):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v2',1,1200,300,date('now','-'||?||' days'))",
            (100 + i, 100 + i, f"r{i:03d}", i - 10),
        )
    conn.commit()
    return conn


def test_build_ab_report_contains_both_versions() -> None:
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "v1" in report.lower() or "V1" in report
    assert "v2" in report.lower() or "V2" in report
    assert "ROI" in report


def test_build_ab_report_roi_values() -> None:
    """V1=80%, V2=160% の ROI が正しく計算されること。"""
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "80.0%" in report   # V1 ROI
    assert "160.0%" in report  # V2 ROI


def test_build_ab_report_empty_db_no_exception() -> None:
    """データなしでもクラッシュしないこと。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            race_id TEXT,
            bet_type TEXT,
            model_version TEXT,
            is_hit INTEGER,
            payout REAL,
            invested REAL,
            created_at TEXT
        )
    """)
    conn.commit()
    mod = _load_mod()
    report = mod.build_ab_report(conn, days=28)
    assert isinstance(report, str)
