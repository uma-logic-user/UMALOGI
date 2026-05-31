"""src/ml/odds_drift.py のテスト（オッズ歪み・危険馬フィルタ）。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import pytest

from src.ml.odds_drift import (
    ABANDON_THRESHOLD,
    DANGER_EV_FACTOR,
    apply_drift_filter,
    compute_drift_map,
    danger_horses,
    plunge_horses,
)


@dataclass
class _FakeBet:
    bet_type: str
    combinations: list[tuple[int, ...]]
    expected_value: float
    notes: str = ""


def _conn_with_odds(snapshots: list[tuple[int, float, str]]) -> sqlite3.Connection:
    """(horse_number, win_odds, recorded_at) の列から realtime_odds を作る。"""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE realtime_odds (
            race_id TEXT, horse_number INTEGER, win_odds REAL, recorded_at TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO realtime_odds (race_id, horse_number, win_odds, recorded_at) "
        "VALUES ('R1', ?, ?, ?)",
        snapshots,
    )
    conn.commit()
    return conn


def test_compute_drift_detects_plunge_and_abandon() -> None:
    # フィールド相対: 中央値 ≈ 0。馬1のみ突出して急落・馬2のみ突出して急騰。
    # 馬1: 10.0 → 5.0  (drift -0.50 = 中央値より大幅急落 → 大口流入)
    # 馬2: 4.0  → 8.0  (drift +1.00 = 中央値より大幅急騰 → 危険馬)
    # 馬3-5: ほぼ変化なし
    conn = _conn_with_odds(
        [
            (1, 10.0, "2026-05-31 08:00:00"),
            (1, 5.0, "2026-05-31 14:00:00"),
            (2, 4.0, "2026-05-31 08:00:00"),
            (2, 8.0, "2026-05-31 14:00:00"),
            (3, 3.0, "2026-05-31 08:00:00"),
            (3, 3.1, "2026-05-31 14:00:00"),
            (4, 5.0, "2026-05-31 08:00:00"),
            (4, 5.0, "2026-05-31 14:00:00"),
            (5, 6.0, "2026-05-31 08:00:00"),
            (5, 6.0, "2026-05-31 14:00:00"),
        ]
    )
    drift = compute_drift_map(conn, "R1")
    assert drift[1].is_plunge and not drift[1].is_abandoned
    assert drift[2].is_abandoned and not drift[2].is_plunge
    assert not drift[3].is_plunge and not drift[3].is_abandoned
    assert danger_horses(drift) == {2}
    assert plunge_horses(drift) == {1}


def test_compute_drift_ignores_systematic_shift() -> None:
    # 全馬が一様に +50% 上昇（朝暫定 vs 直前で規模が違うケース）→ 誰も危険馬にしない。
    conn = _conn_with_odds(
        [
            row
            for hn, base in [(1, 4.0), (2, 5.0), (3, 6.0), (4, 7.0), (5, 8.0)]
            for row in (
                (hn, base, "2026-05-31 08:00:00"),
                (hn, base * 1.5, "2026-05-31 14:00:00"),
            )
        ]
    )
    drift = compute_drift_map(conn, "R1")
    assert danger_horses(drift) == set()
    assert plunge_horses(drift) == set()


def test_compute_drift_skips_small_field() -> None:
    # 3頭のみ（_MIN_FIELD=4 未満）→ ノイズとみなし空。
    conn = _conn_with_odds(
        [
            (1, 4.0, "2026-05-31 08:00:00"),
            (1, 9.0, "2026-05-31 14:00:00"),
            (2, 5.0, "2026-05-31 08:00:00"),
            (2, 5.0, "2026-05-31 14:00:00"),
            (3, 6.0, "2026-05-31 08:00:00"),
            (3, 6.0, "2026-05-31 14:00:00"),
        ]
    )
    assert compute_drift_map(conn, "R1") == {}


def test_compute_drift_skips_single_snapshot() -> None:
    conn = _conn_with_odds([(1, 5.0, "2026-05-31 14:00:00")])
    assert compute_drift_map(conn, "R1") == {}


def test_apply_drift_filter_penalizes_and_drops_danger_axis() -> None:
    # 馬2のみ突出して急騰（危険馬）、他4頭はフラット → median≈0。
    conn = _conn_with_odds(
        [
            (2, 4.0, "2026-05-31 08:00:00"),
            (2, 4.0 * (1 + ABANDON_THRESHOLD + 0.1), "2026-05-31 14:00:00"),
            (3, 3.0, "2026-05-31 08:00:00"),
            (3, 3.0, "2026-05-31 14:00:00"),
            (4, 5.0, "2026-05-31 08:00:00"),
            (4, 5.0, "2026-05-31 14:00:00"),
            (5, 6.0, "2026-05-31 08:00:00"),
            (5, 6.0, "2026-05-31 14:00:00"),
            (6, 7.0, "2026-05-31 08:00:00"),
            (6, 7.0, "2026-05-31 14:00:00"),
        ]
    )
    drift = compute_drift_map(conn, "R1")
    assert danger_horses(drift) == {2}

    bets = [
        _FakeBet(
            "単勝", [(2,)], expected_value=1.6
        ),  # 危険軸・減衰後1.6*0.5=0.8 → 除外
        _FakeBet("ワイド", [(2, 3), (2, 4)], expected_value=3.0),  # 軸2・減衰1.5 → 残る
        _FakeBet("複勝", [(5,)], expected_value=1.2),  # 危険馬なし → 無変更
    ]
    kept, dropped, penalized = apply_drift_filter(bets, drift, ev_min=1.0)
    assert dropped == 1
    assert penalized == 2
    # 残ったのはワイド(2,3/2,4)と複勝5
    kept_types = sorted(b.bet_type for b in kept)
    assert kept_types == ["ワイド", "複勝"]
    wide = next(b for b in kept if b.bet_type == "ワイド")
    assert wide.expected_value == pytest.approx(3.0 * DANGER_EV_FACTOR, abs=1e-6)
    assert "危険馬" in wide.notes
    fuku = next(b for b in kept if b.bet_type == "複勝")
    assert fuku.expected_value == pytest.approx(1.2)  # 無変更


def test_apply_drift_filter_noop_without_danger() -> None:
    bets = [_FakeBet("単勝", [(1,)], expected_value=2.0)]
    kept, dropped, penalized = apply_drift_filter(bets, {}, ev_min=1.0)
    assert kept is bets and dropped == 0 and penalized == 0
