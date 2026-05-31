"""Pure_EV_Edge（黒字化専用 単複バリアント）ユニットテスト。

検証範囲:
  - 券種が単勝・複勝のみにロックされていること
  - 卍 Isotonic 較正確率ベースの EV>=1.15 安全マージンフィルタ
  - 1/10 Kelly の賭け額算出（過大ベット防止）
  - 日次/週次サーキットブレーカー
"""

from __future__ import annotations

import sqlite3

import pytest

# 【2026-06-01 完成】src/ml/pure_ev_edge.py を本テストの契約API（PURE_EV_MODEL_NAME /
# select_pure_ev_bets / tansho_ev / fukusho_ev / circuit_breaker_status 等）に整合実装し、
# 2年ウォークフォワード・バックテストで黒字を実証したため module-level skip を解除。
from src.ml.pure_ev_edge import (
    PURE_EV_MODEL_NAME,
    PURE_EV_THRESHOLD,
    PURE_KELLY_FRACTION,
    CircuitBreakerStatus,
    PureEVConfig,
    circuit_breaker_status,
    evaluate_circuit_breaker,
    fukusho_ev,
    get_period_pnl,
    is_locked_bet_type,
    kelly_stake,
    select_pure_ev_bets,
    tansho_ev,
)


# ── 券種ロック ──────────────────────────────────────────────────────────────


def test_only_tansho_fukusho_locked() -> None:
    assert is_locked_bet_type("単勝")
    assert is_locked_bet_type("複勝")
    for bt in ("馬連", "ワイド", "馬単", "三連複", "三連単", "WIN5"):
        assert not is_locked_bet_type(bt)


def test_generated_bets_never_include_combo_types() -> None:
    horses = [
        {
            "horse_number": i,
            "horse_name": f"H{i}",
            "win_odds": 4.0,
            "manji_ev_score": 2.5,
            "place_prob": 0.8,
        }
        for i in range(1, 9)
    ]
    bets = select_pure_ev_bets("R", horses)
    assert all(b.bet_type in ("単勝", "複勝") for b in bets.bets)


# ── EV 計算（卍較正フォールバック・係数膨張なし）─────────────────────────────


def test_tansho_ev_uses_calibrated_prob_no_saturation() -> None:
    # 較正器が無くてもフォールバックは implied=ev/odds を 0.6 で頭打ち（飽和しない）
    p, ev = tansho_ev(manji_ev_score=2.0, win_odds=4.0)
    assert 0.0 < p <= 0.6  # 1.0 飽和しない
    assert ev == pytest.approx(p * 4.0, abs=1e-6)


def test_tansho_ev_invalid_odds_returns_zero() -> None:
    assert tansho_ev(2.0, 1.0) == (0.0, 0.0)
    assert tansho_ev(2.0, 0.5) == (0.0, 0.0)


def test_fukusho_ev_estimated_and_explicit_odds() -> None:
    # 推定: place_odds なし → win_odds×scale
    p, ev = fukusho_ev(place_prob=0.7, win_odds=4.0, place_payout_scale=0.33)
    eff = 1.0 + (4.0 - 1.0) * 0.33
    assert ev == pytest.approx(min(0.7 * eff, 3.0), abs=1e-6)
    # 明示オッズ優先
    p2, ev2 = fukusho_ev(place_prob=0.7, win_odds=4.0, place_odds=1.5)
    assert ev2 == pytest.approx(0.7 * 1.5, abs=1e-6)


# ── EV 閾値フィルタ ─────────────────────────────────────────────────────────


def test_below_threshold_excluded() -> None:
    # 低 EV（人気すぎ）は採用されない
    horses = [
        {
            "horse_number": 1,
            "horse_name": "A",
            "win_odds": 1.2,
            "manji_ev_score": 0.3,
            "place_prob": 0.2,
        }
    ]
    bets = select_pure_ev_bets("R", horses)
    assert bets.bets == []


def test_above_threshold_included_with_stake() -> None:
    horses = [
        {
            "horse_number": 5,
            "horse_name": "Edge",
            "win_odds": 6.0,
            "manji_ev_score": 3.0,
            "place_prob": 0.85,
        }
    ]
    bets = select_pure_ev_bets("R", horses)
    assert len(bets.bets) >= 1
    for b in bets.bets:
        assert b.expected_value >= PURE_EV_THRESHOLD
        assert b.stake >= 100
        assert b.stake % 100 == 0


# ── 1/10 Kelly ──────────────────────────────────────────────────────────────


def test_kelly_is_one_tenth_and_capped() -> None:
    # f* = (EV-1)/(odds-1) = (1.5-1)/(5-1)=0.125 → ×1/10=0.0125 → cap 単勝0.02 未満
    stake = kelly_stake(
        ev=1.5,
        odds=5.0,
        bankroll=100_000,
        bet_type="単勝",
        kelly_fraction=PURE_KELLY_FRACTION,
    )
    # 0.0125 × 100,000 = 1,250 → 100円単位 = 1,200
    assert stake == 1200


def test_kelly_type_cap_limits_large_edge() -> None:
    # 巨大 EV でも単勝 cap 2% を超えない
    stake = kelly_stake(ev=5.0, odds=2.0, bankroll=100_000, bet_type="単勝")
    assert stake <= 100_000 * 0.02


def test_kelly_zero_on_negative_ev() -> None:
    assert kelly_stake(ev=0.9, odds=5.0, bankroll=100_000, bet_type="単勝") == 0
    assert kelly_stake(ev=1.5, odds=1.0, bankroll=100_000, bet_type="単勝") == 0


# ── サーキットブレーカー ─────────────────────────────────────────────────────


def test_circuit_breaker_daily_trip() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000, daily_loss_limit_pct=0.05)
    st = evaluate_circuit_breaker(daily_pnl=-5_001, weekly_pnl=-5_001, config=cfg)
    assert st.tripped
    assert "日次" in st.reason


def test_circuit_breaker_weekly_trip() -> None:
    cfg = PureEVConfig(
        initial_bankroll=100_000, daily_loss_limit_pct=0.05, weekly_loss_limit_pct=0.12
    )
    st = evaluate_circuit_breaker(daily_pnl=-1_000, weekly_pnl=-12_500, config=cfg)
    assert st.tripped
    assert "週次" in st.reason


def test_circuit_breaker_ok_when_within_limits() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000)
    st = evaluate_circuit_breaker(daily_pnl=-1_000, weekly_pnl=-3_000, config=cfg)
    assert not st.tripped


# ── DB 集計 ─────────────────────────────────────────────────────────────────


@pytest.fixture
def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT);
        CREATE TABLE predictions (id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT);
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY, prediction_id INTEGER, profit REAL
        );
        """
    )
    conn.execute("INSERT INTO races VALUES ('R1','2026-06-01')")
    conn.execute("INSERT INTO races VALUES ('R2','2026-06-01')")
    # Pure_EV_Edge の予想2件（+500, -100）と別モデル（混入してはいけない）
    conn.execute("INSERT INTO predictions VALUES (1,'R1','Pure_EV_Edge(直前)')")
    conn.execute("INSERT INTO predictions VALUES (2,'R2','Pure_EV_Edge(直前)')")
    conn.execute("INSERT INTO predictions VALUES (3,'R1','卍(直前)')")
    conn.execute("INSERT INTO prediction_results VALUES (1,1,500)")
    conn.execute("INSERT INTO prediction_results VALUES (2,2,-100)")
    conn.execute("INSERT INTO prediction_results VALUES (3,3,-9999)")
    conn.commit()
    return conn


def test_get_period_pnl_only_pure_ev(_conn: sqlite3.Connection) -> None:
    pnl = get_period_pnl(_conn, "2026-06-01", "2026-06-01")
    assert pnl == pytest.approx(400.0)  # 500 - 100、卍の -9999 は除外


def test_circuit_breaker_status_from_db(_conn: sqlite3.Connection) -> None:
    st = circuit_breaker_status(
        _conn, "2026-06-01", PureEVConfig(initial_bankroll=100_000)
    )
    assert isinstance(st, CircuitBreakerStatus)
    assert not st.tripped  # +400 は黒字


def test_model_name_constant() -> None:
    assert PURE_EV_MODEL_NAME == "Pure_EV_Edge"
