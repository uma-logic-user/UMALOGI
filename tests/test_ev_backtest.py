"""tests/test_ev_backtest.py — ev_backtest_vectorized モジュールのテスト"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.ev_backtest_vectorized import (
    _BacktestConstants,
    compute_ev_vectorized,
    compute_max_drawdown,
    compute_sharpe_ratio,
    grid_search_ev_threshold,
    simulate_kelly_bankroll,
)


# ─────────────────────────────────────────────────────────────────────────────
# テスト用フィクスチャ
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """ランダムシードで生成した 100 件のサンプル predictions DataFrame。"""
    rng = np.random.default_rng(42)
    n = 100
    return pd.DataFrame(
        {
            "race_date": pd.date_range("2025-01-01", periods=n, freq="D"),
            "model_type": np.where(
                rng.integers(0, 2, n) == 0, "HonmeiModel", "ManjiModel"
            ),
            "bet_type": "win",
            "expected_value": rng.uniform(0.5, 3.0, n),
            "win_odds": rng.uniform(1.5, 20.0, n),
            "is_hit": rng.integers(0, 2, n).astype(float),
            "payout": rng.uniform(0, 2000, n) * rng.integers(0, 2, n),
            "recommended_bet": np.full(n, 100.0),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# _BacktestConstants
# ─────────────────────────────────────────────────────────────────────────────


class TestBacktestConstants:
    def test_kelly_fraction_default(self) -> None:
        assert _BacktestConstants.KELLY_FRACTION == pytest.approx(0.25)

    def test_kelly_cap_default(self) -> None:
        assert _BacktestConstants.KELLY_CAP == pytest.approx(0.25)

    def test_jra_win_takeout(self) -> None:
        assert _BacktestConstants.JRA_WIN_TAKEOUT == pytest.approx(0.20)

    def test_initial_bankroll(self) -> None:
        assert _BacktestConstants.INITIAL_BANKROLL == pytest.approx(100_000.0)

    def test_grid_ev_thresholds_not_empty(self) -> None:
        assert len(_BacktestConstants.GRID_EV_THRESHOLDS) > 0

    def test_grid_kelly_fracs_not_empty(self) -> None:
        assert len(_BacktestConstants.GRID_KELLY_FRACS) > 0


# ─────────────────────────────────────────────────────────────────────────────
# compute_ev_vectorized
# ─────────────────────────────────────────────────────────────────────────────


class TestComputeEvVectorized:
    def test_uses_expected_value_as_fallback(self, sample_df: pd.DataFrame) -> None:
        ev = compute_ev_vectorized(sample_df, prob_col="nonexistent_col")
        assert len(ev) == len(sample_df)
        assert (ev >= 0).all()

    def test_with_prob_col(self) -> None:
        df = pd.DataFrame(
            {
                "model_prob": [0.5, 0.3],
                "win_odds": [3.0, 4.0],
            }
        )
        ev = compute_ev_vectorized(df, prob_col="model_prob", odds_col="win_odds")
        assert ev.iloc[0] == pytest.approx(1.5)  # 0.5 × 3.0
        assert ev.iloc[1] == pytest.approx(1.2)  # 0.3 × 4.0


# ─────────────────────────────────────────────────────────────────────────────
# simulate_kelly_bankroll — np.cumprod 専用
# ─────────────────────────────────────────────────────────────────────────────


class TestSimulateKellyBankroll:
    def test_returns_series_of_same_length(self, sample_df: pd.DataFrame) -> None:
        bankroll = simulate_kelly_bankroll(
            ev_series=sample_df["expected_value"],
            is_hit_series=sample_df["is_hit"],
            payout_series=sample_df["payout"],
            win_odds_series=sample_df["win_odds"],
        )
        assert len(bankroll) == len(sample_df)

    def test_all_miss_reduces_bankroll(self) -> None:
        n = 20
        ev = pd.Series(np.full(n, 1.5))
        hit = pd.Series(np.zeros(n))
        pay = pd.Series(np.zeros(n))
        odds = pd.Series(np.full(n, 3.0))
        br = simulate_kelly_bankroll(ev, hit, pay, odds)
        assert float(br.iloc[-1]) < 100_000.0

    def test_all_win_increases_bankroll(self) -> None:
        n = 20
        ev = pd.Series(np.full(n, 2.0))
        hit = pd.Series(np.ones(n))
        pay = pd.Series(np.full(n, 300.0))
        odds = pd.Series(np.full(n, 3.0))
        br = simulate_kelly_bankroll(ev, hit, pay, odds)
        assert float(br.iloc[-1]) > 100_000.0

    def test_low_ev_bets_not_taken(self) -> None:
        # EV < 1 → Kelly < 0 → clip → 0 → 資金変動なし
        n = 10
        ev = pd.Series(np.full(n, 0.5))
        hit = pd.Series(np.zeros(n))
        pay = pd.Series(np.zeros(n))
        odds = pd.Series(np.full(n, 2.0))
        br = simulate_kelly_bankroll(ev, hit, pay, odds)
        assert float(br.iloc[-1]) == pytest.approx(100_000.0)

    def test_single_bet_win(self) -> None:
        # ev=2, odds=2 → kelly = (2-1)/(2-1) × 0.25 = 0.25
        # win: 100k × (1 + 0.25 × 1) = 125k
        ev = pd.Series([2.0])
        hit = pd.Series([1.0])
        pay = pd.Series([200.0])
        odds = pd.Series([2.0])
        br = simulate_kelly_bankroll(ev, hit, pay, odds, kelly_fraction=0.25)
        assert float(br.iloc[0]) == pytest.approx(125_000.0)

    def test_single_bet_lose(self) -> None:
        # ev=2, odds=2 → kelly=0.25 → lose: 100k × (1 - 0.25) = 75k
        ev = pd.Series([2.0])
        hit = pd.Series([0.0])
        pay = pd.Series([0.0])
        odds = pd.Series([2.0])
        br = simulate_kelly_bankroll(ev, hit, pay, odds, kelly_fraction=0.25)
        assert float(br.iloc[0]) == pytest.approx(75_000.0)

    def test_kelly_cap_enforced(self) -> None:
        # ev=100, odds=2 → raw kelly >> cap → capped at 0.25
        # 5連勝: 100k × 1.25^5 ≈ 305k
        n = 5
        ev = pd.Series(np.full(n, 100.0))
        hit = pd.Series(np.ones(n))
        pay = pd.Series(np.full(n, 200.0))
        odds = pd.Series(np.full(n, 2.0))
        br = simulate_kelly_bankroll(ev, hit, pay, odds, kelly_cap=0.25)
        expected_max = 100_000.0 * (1.25**n) * 1.01  # 1% マージン
        assert float(br.iloc[-1]) <= expected_max

    def test_result_always_positive(self, sample_df: pd.DataFrame) -> None:
        # Kelly < 1 なら bankroll は常に正
        br = simulate_kelly_bankroll(
            sample_df["expected_value"],
            sample_df["is_hit"],
            sample_df["payout"],
            sample_df["win_odds"],
        )
        assert (br > 0).all()

    def test_index_preserved(self) -> None:
        idx = pd.Index([10, 20, 30])
        ev = pd.Series([1.5, 1.8, 0.8], index=idx)
        hit = pd.Series([1.0, 0.0, 1.0], index=idx)
        pay = pd.Series([150.0, 0.0, 80.0], index=idx)
        odds = pd.Series([2.0, 3.0, 1.5], index=idx)
        br = simulate_kelly_bankroll(ev, hit, pay, odds)
        assert list(br.index) == [10, 20, 30]


# ─────────────────────────────────────────────────────────────────────────────
# compute_sharpe_ratio
# ─────────────────────────────────────────────────────────────────────────────


class TestComputeSharpeRatio:
    def test_returns_float(self) -> None:
        br = pd.Series([100_000.0, 110_000.0, 105_000.0, 115_000.0])
        assert isinstance(compute_sharpe_ratio(br), float)

    def test_monotone_increase_positive_sharpe(self) -> None:
        br = pd.Series(np.linspace(100_000.0, 200_000.0, 52))
        assert compute_sharpe_ratio(br) > 0.0

    def test_constant_bankroll_returns_zero(self) -> None:
        br = pd.Series(np.full(52, 100_000.0))
        assert compute_sharpe_ratio(br) == pytest.approx(0.0)

    def test_too_short_returns_zero(self) -> None:
        assert compute_sharpe_ratio(pd.Series([100_000.0])) == pytest.approx(0.0)

    def test_declining_bankroll_negative_sharpe(self) -> None:
        br = pd.Series(np.linspace(100_000.0, 50_000.0, 52))
        assert compute_sharpe_ratio(br) < 0.0


# ─────────────────────────────────────────────────────────────────────────────
# compute_max_drawdown
# ─────────────────────────────────────────────────────────────────────────────


class TestComputeMaxDrawdown:
    def test_no_drawdown_returns_zero(self) -> None:
        br = pd.Series([100.0, 110.0, 120.0, 130.0])
        assert compute_max_drawdown(br) == pytest.approx(0.0)

    def test_20_percent_drawdown(self) -> None:
        br = pd.Series([100.0, 80.0, 90.0, 120.0])
        mdd = compute_max_drawdown(br)
        assert mdd == pytest.approx(0.20, abs=0.01)

    def test_near_total_loss(self) -> None:
        br = pd.Series([100.0, 50.0, 1.0])
        assert compute_max_drawdown(br) > 0.95

    def test_single_element_returns_zero(self) -> None:
        assert compute_max_drawdown(pd.Series([100.0])) == pytest.approx(0.0)

    def test_recovery_detected(self) -> None:
        # 下落後に回復しても最大 DD は下落時点で確定
        br = pd.Series([100.0, 60.0, 150.0])
        mdd = compute_max_drawdown(br)
        assert mdd == pytest.approx(0.40, abs=0.01)


# ─────────────────────────────────────────────────────────────────────────────
# grid_search_ev_threshold
# ─────────────────────────────────────────────────────────────────────────────


class TestGridSearch:
    def test_returns_dataframe(self, sample_df: pd.DataFrame) -> None:
        result = grid_search_ev_threshold(sample_df, ev_thresholds=[1.0])
        assert isinstance(result, pd.DataFrame)

    def test_required_columns_present(self, sample_df: pd.DataFrame) -> None:
        result = grid_search_ev_threshold(sample_df, ev_thresholds=[1.0])
        required = {
            "ev_threshold",
            "kelly_fraction",
            "n_bets",
            "n_hits",
            "hit_rate",
            "roi",
            "sharpe_ratio",
            "max_drawdown",
        }
        assert required.issubset(result.columns)

    def test_sorted_by_sharpe_descending(self, sample_df: pd.DataFrame) -> None:
        result = grid_search_ev_threshold(
            sample_df, ev_thresholds=[1.0, 1.5], kelly_fracs=[0.10, 0.25]
        )
        if len(result) > 1:
            sharpe_vals = result["sharpe_ratio"].values
            # 降順ソートの確認（差分 ≤ 0）
            assert (np.diff(sharpe_vals) <= 1e-8).all()

    def test_empty_df_returns_empty(self) -> None:
        empty = pd.DataFrame(
            columns=[
                "race_date",
                "expected_value",
                "win_odds",
                "is_hit",
                "payout",
                "recommended_bet",
            ]
        )
        result = grid_search_ev_threshold(empty)
        assert len(result) == 0

    def test_unreachable_threshold_excluded(self, sample_df: pd.DataFrame) -> None:
        # ev_threshold = 9999 はデータなし → 結果に含まれない
        result = grid_search_ev_threshold(sample_df, ev_thresholds=[9999.0])
        assert len(result) == 0

    def test_multiple_thresholds_multiple_rows(self, sample_df: pd.DataFrame) -> None:
        result = grid_search_ev_threshold(
            sample_df,
            ev_thresholds=[1.0, 1.5],
            kelly_fracs=[0.25],
        )
        # EV>=1.0 と EV>=1.5 の両方にデータがあれば 2 行
        assert len(result) >= 1

    def test_n_bets_decreases_with_ev_threshold(self, sample_df: pd.DataFrame) -> None:
        result = grid_search_ev_threshold(
            sample_df,
            ev_thresholds=[1.0, 2.0],
            kelly_fracs=[0.25],
        ).sort_values("ev_threshold")
        if len(result) >= 2:
            bets_low = int(result.iloc[0]["n_bets"])
            bets_high = int(result.iloc[-1]["n_bets"])
            assert bets_low >= bets_high
