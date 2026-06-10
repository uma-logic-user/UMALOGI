"""tests/test_all_ticket_optimizer.py — 全券種EV最適化エンジンのテスト。"""

from __future__ import annotations

from itertools import permutations

import pytest

from src.ml.all_ticket_optimizer import (
    FinishOrderModel,
    OptimizerConfig,
    build_formation,
    estimate_combo_odds,
    scan_all_tickets,
)

_NUMS = [1, 2, 3, 4, 5, 6]
_PROBS = [0.40, 0.25, 0.15, 0.10, 0.06, 0.04]


@pytest.fixture
def model() -> FinishOrderModel:
    return FinishOrderModel(_NUMS, _PROBS)


# ── FinishOrderModel（割引Harville）──────────────────────────────────────
class TestFinishOrderModel:
    def test_trifecta_probs_sum_to_one(self, model: FinishOrderModel) -> None:
        total = sum(model.prob_trifecta(a, b, c) for a, b, c in permutations(_NUMS, 3))
        assert total == pytest.approx(1.0, abs=1e-9)

    def test_exacta_probs_sum_to_one(self, model: FinishOrderModel) -> None:
        total = sum(model.prob_exacta(a, b) for a, b in permutations(_NUMS, 2))
        assert total == pytest.approx(1.0, abs=1e-9)

    def test_stronger_horse_more_likely_first(self, model: FinishOrderModel) -> None:
        assert model.prob_exacta(1, 2) > model.prob_exacta(2, 1) * 0.9
        assert model.prob_trifecta(1, 2, 3) > model.prob_trifecta(3, 2, 1)

    def test_quinella_is_order_free(self, model: FinishOrderModel) -> None:
        assert model.prob_quinella(1, 2) == pytest.approx(
            model.prob_exacta(1, 2) + model.prob_exacta(2, 1)
        )

    def test_trio_is_sum_of_six_trifectas(self, model: FinishOrderModel) -> None:
        expected = sum(
            model.prob_trifecta(a, b, c) for a, b, c in permutations((1, 2, 3))
        )
        assert model.prob_trio(1, 2, 3) == pytest.approx(expected)

    def test_wide_at_least_trio(self, model: FinishOrderModel) -> None:
        # ワイド{1,2} は 三連複{1,2,z} の和なので任意の単一 trio より大きい
        assert model.prob_wide(1, 2) >= model.prob_trio(1, 2, 3)

    def test_same_horse_zero(self, model: FinishOrderModel) -> None:
        assert model.prob_exacta(1, 1) == 0.0
        assert model.prob_trifecta(1, 1, 2) == 0.0

    def test_discount_flattens_second_stage(self) -> None:
        # 割引(λ<1)は2着段の格差を縮める → 弱い馬の2着確率が素朴Harvilleより上がる
        plain = FinishOrderModel(_NUMS, _PROBS, lambda_2nd=1.0, lambda_3rd=1.0)
        disc = FinishOrderModel(_NUMS, _PROBS, lambda_2nd=0.5, lambda_3rd=0.5)
        assert disc.prob_exacta(1, 6) > plain.prob_exacta(1, 6)

    def test_monte_carlo_agrees_with_analytic(self, model: FinishOrderModel) -> None:
        freq = model.simulate_finish_counts(n_sims=40_000, seed=7)
        analytic = model.prob_trifecta(1, 2, 3)
        assert freq.get((1, 2, 3), 0.0) == pytest.approx(analytic, abs=0.012)


# ── オッズ推定 ───────────────────────────────────────────────────────────
class TestEstimateComboOdds:
    def test_takeout_applied(self) -> None:
        # 三連単: P=0.01 → (1-0.275)/0.01 = 72.5倍
        assert estimate_combo_odds(0.01, "三連単") == pytest.approx(72.5)

    def test_higher_prob_lower_odds(self) -> None:
        assert estimate_combo_odds(0.2, "馬連") < estimate_combo_odds(0.05, "馬連")


# ── scan_all_tickets ─────────────────────────────────────────────────────
class TestScanAllTickets:
    def test_extracts_only_high_ev(self) -> None:
        # 実オッズに歪み（三連複{1,2,3}に確率比で過剰なオッズ）を仕込む
        model = FinishOrderModel(_NUMS, _PROBS)
        fair = 1.0 / model.prob_trio(1, 2, 3)
        odds_map = {"三連複": {(1, 2, 3): fair * 1.5}}  # EV=1.5 の歪み
        out = scan_all_tickets(
            _NUMS,
            _PROBS,
            odds_map=odds_map,
            config=OptimizerConfig(ev_min=1.30),
        )
        assert len(out) == 1
        assert out[0].bet_type == "三連複"
        assert out[0].combo == (1, 2, 3)
        assert out[0].ev == pytest.approx(1.5, rel=1e-6)

    def test_fair_odds_yield_nothing(self) -> None:
        # 歪みゼロ（全組み合わせフェアオッズ）なら EV=1.0 → 抽出なし
        model = FinishOrderModel(_NUMS, _PROBS)
        odds_map = {
            "馬連": {
                tuple(sorted((x, y))): 1.0 / model.prob_quinella(x, y)
                for x in _NUMS
                for y in _NUMS
                if x < y
            }
        }
        out = scan_all_tickets(_NUMS, _PROBS, odds_map=odds_map)
        assert out == []

    def test_market_estimate_finds_model_market_gap(self) -> None:
        # モデルと市場の確率が乖離 → 実オッズ無しでも推定オッズで歪みを抽出
        market = [0.30, 0.30, 0.15, 0.10, 0.08, 0.07]  # 市場は1番を過小評価
        out = scan_all_tickets(
            _NUMS,
            _PROBS,
            market_win_probs=market,
            config=OptimizerConfig(ev_min=1.10),
        )
        assert len(out) > 0
        # 過小評価された1番絡みの組み合わせが上位に来る
        assert any(1 in c.combo for c in out)

    def test_no_odds_no_market_returns_empty(self) -> None:
        assert scan_all_tickets(_NUMS, _PROBS) == []

    def test_max_bets_respected(self) -> None:
        market = [0.30, 0.30, 0.15, 0.10, 0.08, 0.07]
        out = scan_all_tickets(
            _NUMS,
            _PROBS,
            market_win_probs=market,
            config=OptimizerConfig(ev_min=1.01, max_bets=3),
        )
        assert len(out) <= 3

    def test_results_sorted_by_ev_desc(self) -> None:
        market = [0.30, 0.30, 0.15, 0.10, 0.08, 0.07]
        out = scan_all_tickets(
            _NUMS,
            _PROBS,
            market_win_probs=market,
            config=OptimizerConfig(ev_min=1.01, max_bets=10),
        )
        evs = [c.ev for c in out]
        assert evs == sorted(evs, reverse=True)


# ── フォーメーション ─────────────────────────────────────────────────────
class TestBuildFormation:
    def test_axis_is_common_horse(self) -> None:
        market = [0.30, 0.30, 0.15, 0.10, 0.08, 0.07]
        cands = scan_all_tickets(
            _NUMS,
            _PROBS,
            market_win_probs=market,
            config=OptimizerConfig(ev_min=1.05, max_bets=8),
        )
        formations = build_formation(cands)
        assert formations
        for f in formations:
            for c in f.candidates:
                assert set(f.axis).issubset(set(c.combo))
            assert f.n_points == len(f.candidates)

    def test_empty_candidates(self) -> None:
        assert build_formation([]) == []
