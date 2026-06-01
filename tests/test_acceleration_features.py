"""W-001 加速力スコア（上がり3F）＋ PCI 計算のテスト。

本番モデル(v1.2.0)の入力次元 FEATURE_COLS を破壊しないことも担保する。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.features.acceleration import (
    PCI_BASELINE,
    acceleration_score,
    build_acceleration_features,
    compute_pci,
    parse_time_to_seconds,
)


# ── タイム解析 ──────────────────────────────────────────────────────────────
def test_parse_time_minutes_seconds() -> None:
    assert parse_time_to_seconds("1:11.6") == pytest.approx(71.6)
    assert parse_time_to_seconds("2:31.5") == pytest.approx(151.5)


def test_parse_time_seconds_and_numeric() -> None:
    assert parse_time_to_seconds("34.5") == pytest.approx(34.5)
    assert parse_time_to_seconds(34.5) == pytest.approx(34.5)


def test_parse_time_invalid_returns_none() -> None:
    for bad in (None, "", "  ", "abc", 0, -5, "0:00.0"):
        assert parse_time_to_seconds(bad) is None


# ── PCI ─────────────────────────────────────────────────────────────────────
def test_pci_known_value() -> None:
    # 1600m / 96.0s / 上がり34.0s → 50 × (96/8) / (34/3) = 50 × 12 / 11.333 ≈ 52.94
    pci = compute_pci(34.0, 96.0, 1600)
    assert pci == pytest.approx(52.94, abs=0.1)


def test_pci_even_pace_near_baseline() -> None:
    # 全体平均1F と 後半平均1F が等しい → PCI = 50（基準）
    # 1200m / 72s → 全体1F=12.0。後半3F=36.0 → 後半1F=12.0 → PCI=50
    pci = compute_pci(36.0, 72.0, 1200)
    assert pci == pytest.approx(PCI_BASELINE, abs=0.01)


def test_pci_faster_finish_higher_pci() -> None:
    # 上がりが速い(小さい)ほど PCI は大きい（後傾＝上がり勝負）
    slow_finish = compute_pci(37.0, 96.0, 1600)
    fast_finish = compute_pci(33.0, 96.0, 1600)
    assert fast_finish is not None and slow_finish is not None
    assert fast_finish > slow_finish


def test_pci_invalid_inputs_return_none() -> None:
    assert compute_pci(None, 96.0, 1600) is None
    assert compute_pci(34.0, None, 1600) is None
    assert compute_pci(34.0, 96.0, None) is None
    assert compute_pci(34.0, 96.0, 0) is None
    assert compute_pci(34.0, 96.0, 400) is None  # 距離 < 後半3F(600m)


# ── 加速力スコア ────────────────────────────────────────────────────────────
def test_acceleration_score_faster_is_positive() -> None:
    # [33.0, 35.0, 37.0] → 速い 33.0 が正、遅い 37.0 が負、中央 35.0 が ~0
    scores = acceleration_score([33.0, 35.0, 37.0])
    assert scores[0] > 0 > scores[2]
    assert scores[1] == pytest.approx(0.0, abs=1e-9)


def test_acceleration_score_handles_none_and_degenerate() -> None:
    # None は 0.0、有効値 < 2 は全 0.0
    assert acceleration_score([34.0, None]) == [0.0, 0.0]
    assert acceleration_score([]) == []
    # 全て同値 → std=0 → 全 0.0
    assert acceleration_score([34.0, 34.0, 34.0]) == [0.0, 0.0, 0.0]


# ── build_acceleration_features（DB 並行計算） ──────────────────────────────
def _conn_with_last3f(with_last3f: bool) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    last3f_col = "last_3f REAL," if with_last3f else ""
    conn.executescript(
        f"""
        CREATE TABLE races (race_id TEXT, distance INTEGER);
        CREATE TABLE race_results (
            race_id TEXT, horse_number INTEGER, finish_time TEXT, {last3f_col}
            x INTEGER
        );
        """
    )
    conn.execute("INSERT INTO races VALUES ('R1', 1600)")
    return conn


def test_build_features_with_last3f() -> None:
    conn = _conn_with_last3f(True)
    conn.executemany(
        "INSERT INTO race_results (race_id, horse_number, finish_time, last_3f, x) "
        "VALUES ('R1', ?, ?, ?, 0)",
        [(1, "1:36.0", 34.0), (2, "1:36.5", 33.0), (3, "1:37.0", 36.0)],
    )
    conn.commit()
    df = build_acceleration_features(conn, "R1")
    assert list(df["horse_number"]) == [1, 2, 3]
    assert df["pci"].notna().all()  # last_3f があれば PCI 算出
    # 最速上がり(33.0)の馬2 が最高 acceleration_score
    top = df.sort_values("acceleration_score", ascending=False).iloc[0]
    assert int(top["horse_number"]) == 2


def test_build_features_without_last3f_is_safe() -> None:
    # last_3f 列が無い古いスキーマでも例外を出さず PCI=NaN を返す（非破壊）
    conn = _conn_with_last3f(False)
    conn.executemany(
        "INSERT INTO race_results (race_id, horse_number, finish_time, x) "
        "VALUES ('R1', ?, ?, 0)",
        [(1, "1:36.0"), (2, "1:36.5")],
    )
    conn.commit()
    df = build_acceleration_features(conn, "R1")
    assert len(df) == 2
    assert df["pci"].isna().all()
    assert list(df["acceleration_score"]) == [0.0, 0.0]


def test_build_features_empty_race() -> None:
    conn = _conn_with_last3f(True)
    df = build_acceleration_features(conn, "UNKNOWN")
    assert df.empty


# ── 本番非破壊ガード: FEATURE_COLS は一切変更されていない ─────────────────────
def test_live_feature_cols_unchanged() -> None:
    """W-001 の特徴量は FEATURE_COLS(v1.2.0 入力次元) に混入していない。"""
    from src.ml.models import FEATURE_COLS

    # v1.2.0 時点のスナップショット（69列）。変化したら本番モデル入力次元の破壊。
    assert len(FEATURE_COLS) == 69
    for leaked in ("last_3f", "last_3f_sec", "pci", "acceleration_score"):
        assert leaked not in FEATURE_COLS
