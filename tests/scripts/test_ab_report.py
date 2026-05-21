"""
scripts/generate_ab_report.py のユニットテスト。
本番 DB スキーマ（predictions + prediction_results）に準拠。
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
    """本番スキーマ準拠の V1/V2 両方データを持つインメモリ DB。"""
    conn = sqlite3.connect(":memory:")
    # predictions: id, race_id, model_type, bet_type, expected_value
    conn.execute("""
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_type TEXT,
            bet_type TEXT,
            expected_value REAL DEFAULT 0
        )
    """)
    # prediction_results: 本番スキーマ (payout, profit, roi, recorded_at)
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            is_hit INTEGER DEFAULT 0,
            payout REAL DEFAULT 0,
            profit REAL DEFAULT 0,
            roi REAL,
            recorded_at TEXT
        )
    """)

    # V1: 10件中2件的中, bet=300, payout=1200 → invested=300 (payout-profit)
    # 外れ: payout=0, profit=-300 → invested=300
    # 的中: payout=1200, profit=900 → invested=300
    # total_payout=2400, total_invest=3000 → ROI=80%
    for i in range(8):
        conn.execute(
            "INSERT INTO predictions(id, race_id, model_type, bet_type) VALUES (?, ?, '本命(ロバスト)', '複勝')",
            (i, f"r{i:03d}"),
        )
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,0,0,-300,0,date('now','-1 day'))",
            (i, i),
        )
    for i in range(8, 10):
        conn.execute(
            "INSERT INTO predictions(id, race_id, model_type, bet_type) VALUES (?, ?, '本命(ロバスト)', '複勝')",
            (i, f"r{i:03d}"),
        )
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,1,1200,900,400,date('now','-1 day'))",
            (i, i),
        )

    # V2: 10件中4件的中, bet=300 → ROI=160%
    # 外れ: payout=0, profit=-300 → invested=300
    # 的中: payout=1200, profit=900 → invested=300
    # total_payout=4800, total_invest=3000 → ROI=160%
    for i in range(10, 16):
        conn.execute(
            "INSERT INTO predictions(id, race_id, model_type, bet_type) VALUES (?, ?, '本命V2', '複勝')",
            (i, f"r{i:03d}"),
        )
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,0,0,-300,0,date('now','-1 day'))",
            (100 + i, i),
        )
    for i in range(16, 20):
        conn.execute(
            "INSERT INTO predictions(id, race_id, model_type, bet_type) VALUES (?, ?, '本命V2', '複勝')",
            (i, f"r{i:03d}"),
        )
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,1,1200,900,400,date('now','-1 day'))",
            (100 + i, i),
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
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_type TEXT,
            bet_type TEXT,
            expected_value REAL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            is_hit INTEGER DEFAULT 0,
            payout REAL DEFAULT 0,
            profit REAL DEFAULT 0,
            roi REAL,
            recorded_at TEXT
        )
    """)
    conn.commit()
    mod = _load_mod()
    report = mod.build_ab_report(conn, days=28)
    assert isinstance(report, str)


def test_build_ab_report_summary_section() -> None:
    """総合サマリーと券種別詳細セクションが出力に含まれること。"""
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "総合サマリー" in report
    assert "券種別詳細" in report
    assert "EV乖離" in report
    assert "純利益" in report


def test_build_ab_report_net_profit() -> None:
    """V1=-¥600, V2=+¥1,800 の純利益が表示されること。

    V1: total_payout=2400, total_invest=3000 → net=-600
    V2: total_payout=4800, total_invest=3000 → net=+1800
    """
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "¥-600" in report
    assert "¥1,800" in report


def test_build_ab_report_v2_wins_verdict() -> None:
    """V2 ROI(160%) > V1 ROI(80%) のとき V2優勢 判定が出力されること。"""
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "V2 優勢" in report


def test_build_ab_report_ev_mae() -> None:
    """EV乖離(MAE)が正しく計算されること。

    expected_value=0 (デフォルト), actual_ev=payout/invest の場合:
    - 外れ: actual_ev = 0/300 = 0, |0-0| = 0
    - 的中: actual_ev = 1200/300 = 4.0, |0-4.0| = 4.0
    V1 MAE: (8*0 + 2*4.0) / 10 = 0.800
    V2 MAE: (6*0 + 4*4.0) / 10 = 1.600
    """
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "0.800" in report  # V1 EV MAE
    assert "1.600" in report  # V2 EV MAE


def test_build_ab_report_race_count() -> None:
    """対象レース数が V1=10, V2=10 として出力されること。"""
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "対象レース数" in report
    # V1: r000〜r009 の10レース, V2: r010〜r019 の10レース
    assert "| 10 | 10 |" in report or "10 |" in report


def test_build_ab_report_winner_badge() -> None:
    """優勢バッジ（🔵 V2 / 🟠 V1）が出力に含まれること。"""
    mod = _load_mod()
    report = mod.build_ab_report(_make_ab_db(), days=28)
    assert "🔵 V2" in report
    assert "🟠 V1" in report
