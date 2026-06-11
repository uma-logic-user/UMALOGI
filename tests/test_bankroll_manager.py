"""tests/test_bankroll_manager.py — バンクロール管理（金融工学レイヤー）のテスト。"""

from __future__ import annotations

import sqlite3

import pytest

from src.ml.bankroll_manager import (
    BankrollConfig,
    BetCandidate,
    allocate_stakes,
    drawdown_throttle,
    effective_bankroll,
    estimate_ruin_probability,
    full_kelly,
    recommend_kelly_fraction,
)


# ── full_kelly ───────────────────────────────────────────────────────────
class TestFullKelly:
    def test_positive_edge(self) -> None:
        # p=0.5, odds=3.0 → f* = (1.5-1)/2 = 0.25
        assert full_kelly(0.5, 3.0) == pytest.approx(0.25)

    def test_negative_edge_returns_zero(self) -> None:
        assert full_kelly(0.2, 3.0) == 0.0  # EV=0.6 < 1

    def test_invalid_odds(self) -> None:
        assert full_kelly(0.9, 1.0) == 0.0
        assert full_kelly(0.9, 0.0) == 0.0

    def test_zero_prob(self) -> None:
        assert full_kelly(0.0, 10.0) == 0.0


# ── drawdown_throttle ────────────────────────────────────────────────────
class TestDrawdownThrottle:
    def test_no_drawdown_full_speed(self) -> None:
        assert drawdown_throttle(100_000, 100_000) == 1.0

    def test_mild_drawdown_half(self) -> None:
        assert drawdown_throttle(85_000, 100_000) == 0.5  # -15%

    def test_severe_drawdown_quarter(self) -> None:
        assert drawdown_throttle(75_000, 100_000) == 0.25  # -25%

    def test_critical_drawdown_stops(self) -> None:
        assert drawdown_throttle(60_000, 100_000) == 0.0  # -40%

    def test_zero_peak_safe(self) -> None:
        assert drawdown_throttle(50_000, 0) == 1.0


# ── allocate_stakes ──────────────────────────────────────────────────────
def _cand(prob: float, odds: float, num: int = 1) -> BetCandidate:
    return BetCandidate("202601010101", "単勝", num, prob, odds)


class TestAllocateStakes:
    def test_single_bet_basic(self) -> None:
        cfg = BankrollConfig(initial_bankroll=100_000)
        result = allocate_stakes([_cand(0.5, 3.0)], 100_000, config=cfg)
        # f*=0.25 → 1/10 Kelly=0.025 → max_bet_pct=0.02 でキャップ → ¥2,000
        assert result.allocations[0].stake == 2000
        assert result.total_stake == 2000

    def test_negative_edge_zero_stake(self) -> None:
        result = allocate_stakes([_cand(0.1, 2.0)], 100_000)
        assert result.allocations[0].stake == 0

    def test_simultaneous_exposure_scaled(self) -> None:
        # 10点全てキャップ(2%)に達すると合計20% > 上限10% → 比例縮約で合計≦10%
        cands = [_cand(0.5, 3.0, num=i) for i in range(1, 11)]
        result = allocate_stakes(cands, 100_000)
        assert result.exposure_scale == pytest.approx(0.5)
        assert result.total_stake <= 10_000
        assert all(a.stake == 1000 for a in result.allocations)

    def test_drawdown_throttles_stake(self) -> None:
        full = allocate_stakes([_cand(0.5, 3.0)], 100_000, peak_bankroll=100_000)
        throttled = allocate_stakes([_cand(0.5, 3.0)], 85_000, peak_bankroll=100_000)
        assert throttled.throttle == 0.5
        assert 0 < throttled.total_stake < full.total_stake

    def test_critical_drawdown_all_zero(self) -> None:
        result = allocate_stakes([_cand(0.5, 3.0)], 60_000, peak_bankroll=100_000)
        assert result.throttle == 0.0
        assert result.total_stake == 0

    def test_stakes_rounded_to_100(self) -> None:
        cands = [_cand(0.35, 3.4, num=i) for i in range(1, 4)]
        result = allocate_stakes(cands, 123_456)
        for a in result.allocations:
            assert a.stake % 100 == 0

    def test_zero_bankroll_safe(self) -> None:
        result = allocate_stakes([_cand(0.5, 3.0)], 0)
        assert result.total_stake == 0


# ── effective_bankroll（動的バンクロール）─────────────────────────────────
class TestEffectiveBankroll:
    @pytest.fixture
    def conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(":memory:")
        c.executescript(
            """
            CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT);
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY, race_id TEXT, is_superseded INTEGER
            );
            CREATE TABLE prediction_results (
                id INTEGER PRIMARY KEY, prediction_id INTEGER,
                payout REAL, profit REAL
            );
            """
        )
        return c

    def test_empty_db_returns_initial(self, conn: sqlite3.Connection) -> None:
        cfg = BankrollConfig(initial_bankroll=100_000)
        bankroll, peak = effective_bankroll(conn, cfg)
        assert bankroll == 100_000
        assert peak == 100_000

    def test_compound_and_peak(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            INSERT INTO races VALUES ('r1', '2026-06-01'), ('r2', '2026-06-02');
            INSERT INTO predictions VALUES (1, 'r1', 0), (2, 'r2', 0);
            INSERT INTO prediction_results VALUES
                (1, 1, 30000, 20000),   -- +2万 → 12万（ピーク）
                (2, 2, 0, -5000);       -- -5千 → 11.5万
            """
        )
        bankroll, peak = effective_bankroll(conn)
        assert bankroll == 115_000
        assert peak == 120_000

    def test_superseded_excluded(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            INSERT INTO races VALUES ('r1', '2026-06-01');
            INSERT INTO predictions VALUES (1, 'r1', 1);
            INSERT INTO prediction_results VALUES (1, 1, 99999, 99999);
            """
        )
        bankroll, _ = effective_bankroll(conn)
        assert bankroll == 100_000


# ── 破産確率（モンテカルロ）──────────────────────────────────────────────
class TestRuinProbability:
    def test_no_bet_no_ruin(self) -> None:
        assert estimate_ruin_probability(0.5, 3.0, 0.0) == 0.0

    def test_full_kelly_riskier_than_fractional(self) -> None:
        f_star = full_kelly(0.4, 3.0)  # 0.1
        ruin_full = estimate_ruin_probability(0.4, 3.0, f_star, n_paths=2000)
        ruin_tenth = estimate_ruin_probability(0.4, 3.0, f_star * 0.1, n_paths=2000)
        assert ruin_tenth <= ruin_full

    def test_overbetting_ruins(self) -> None:
        # フル Kelly の3倍は長期的にほぼ確実に資金半減
        ruin = estimate_ruin_probability(0.4, 3.0, 0.3, n_bets=2000, n_paths=1000)
        assert ruin > 0.9

    def test_deterministic_with_seed(self) -> None:
        a = estimate_ruin_probability(0.4, 3.0, 0.05, seed=7)
        b = estimate_ruin_probability(0.4, 3.0, 0.05, seed=7)
        assert a == b

    def test_recommend_fraction_within_unit(self) -> None:
        frac = recommend_kelly_fraction(0.4, 3.0, target_ruin=0.05, n_paths=1000)
        assert 0.0 <= frac <= 1.0

    def test_recommend_zero_for_negative_edge(self) -> None:
        assert recommend_kelly_fraction(0.1, 2.0) == 0.0


# ── ポートフォリオ破産シミュレーション（W-078 三連系対応）──────────────────
from src.ml.bankroll_manager import (  # noqa: E402
    PortfolioSimResult,
    recommend_portfolio_stakes,
    simulate_portfolio_ruin,
)


def _sanren_portfolio() -> list[BetCandidate]:
    """三連系を模した低的中率×高配当の同時多点ポートフォリオ。"""
    return [
        BetCandidate("r1", "三連単", 6, prob=0.009, odds=300.0),  # EV 2.7
        BetCandidate("r1", "三連単", 10, prob=0.008, odds=250.0),  # EV 2.0
        BetCandidate("r1", "三連複", 6, prob=0.04, odds=60.0),  # EV 2.4
        BetCandidate("r2", "三連複", 1, prob=0.05, odds=40.0),  # EV 2.0
        BetCandidate("r2", "三連単", 1, prob=0.007, odds=400.0),  # EV 2.8
    ]


class TestSimulatePortfolioRuin:
    def test_empty_candidates_no_ruin(self) -> None:
        result = simulate_portfolio_ruin([])
        assert result.ruin_probability == 0.0
        assert result.total_fraction == 0.0

    def test_negative_edge_portfolio_no_stake(self) -> None:
        cands = [BetCandidate("r1", "三連単", 1, prob=0.001, odds=100.0)]  # EV 0.1
        result = simulate_portfolio_ruin(cands)
        assert result.total_fraction == 0.0
        assert result.ruin_probability == 0.0

    def test_tenth_kelly_is_safe_for_sanren(self) -> None:
        """1/10 Kelly + 上限キャップ構成では三連系でも破産確率 1% 未満。"""
        result = simulate_portfolio_ruin(
            _sanren_portfolio(), kelly_fraction=0.10, n_rounds=300, n_paths=2000
        )
        assert result.total_fraction > 0.0
        assert result.ruin_probability < 0.01

    def test_full_kelly_riskier_than_fractional(self) -> None:
        """Kelly 分数の増加で破産確率が単調増加する（リスクの定量化）。"""
        low = simulate_portfolio_ruin(
            _sanren_portfolio(),
            kelly_fraction=0.10,
            config=BankrollConfig(max_bet_pct=1.0, max_daily_exposure_pct=1.0),
            n_rounds=200,
            n_paths=1500,
        )
        high = simulate_portfolio_ruin(
            _sanren_portfolio(),
            kelly_fraction=1.0,
            config=BankrollConfig(max_bet_pct=1.0, max_daily_exposure_pct=1.0),
            n_rounds=200,
            n_paths=1500,
        )
        assert high.total_fraction > low.total_fraction
        assert high.ruin_probability >= low.ruin_probability

    def test_mutually_exclusive_same_race(self) -> None:
        """同一レース内 Σp>1 でも正規化され確率として破綻しない。"""
        cands = [
            BetCandidate("r1", "三連複", i, prob=0.4, odds=5.0) for i in range(1, 5)
        ]
        result = simulate_portfolio_ruin(cands, n_rounds=50, n_paths=500)
        assert isinstance(result, PortfolioSimResult)
        assert 0.0 <= result.ruin_probability <= 1.0


class TestRecommendPortfolioStakes:
    def test_returns_positive_stakes_within_target(self) -> None:
        alloc, sim = recommend_portfolio_stakes(
            _sanren_portfolio(),
            bankroll=100_000,
            target_ruin=0.01,
            n_rounds=200,
            n_paths=1000,
        )
        assert sim.ruin_probability <= 0.01
        assert sim.kelly_fraction > 0.0
        assert alloc.total_stake > 0
        # 1日合計エクスポージャー上限（10%）を厳守
        assert alloc.total_stake <= 100_000 * 0.10 + 100  # 丸め余裕

    def test_unwinnable_portfolio_returns_zero(self) -> None:
        """負エッジしかない場合は全見送り（ステーク 0）。"""
        cands = [BetCandidate("r1", "三連単", 1, prob=0.001, odds=50.0)]
        alloc, sim = recommend_portfolio_stakes(
            cands, bankroll=100_000, n_rounds=50, n_paths=300
        )
        assert alloc.total_stake == 0
        assert sim.total_fraction == 0.0
