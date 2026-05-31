"""tests/test_ev_features.py — EVEnhancedFeatures / MarketProbabilityCalc / OddsAnomalyDetector テスト"""

from __future__ import annotations

from itertools import permutations

import numpy as np
import pandas as pd
import pytest

from src.ml.ev_features import (
    EVEnhancedFeatures,
    JRATakeoutRates,
    MarketProbabilityCalc,
    OddsAnomalyDetector,
)


# ─────────────────────────────────────────────────────────────────────────────
# JRATakeoutRates
# ─────────────────────────────────────────────────────────────────────────────


class TestJRATakeoutRates:
    def test_default_win_takeout(self) -> None:
        rates = JRATakeoutRates()
        assert rates.WIN == pytest.approx(0.200)

    def test_default_trifecta_takeout(self) -> None:
        rates = JRATakeoutRates()
        assert rates.TRIFECTA == pytest.approx(0.275)

    def test_default_trio_takeout(self) -> None:
        assert JRATakeoutRates.TRIO == pytest.approx(0.225)

    def test_custom_win_takeout(self) -> None:
        rates = JRATakeoutRates(win=0.25)
        assert rates.WIN == pytest.approx(0.25)
        # 他の控除率は変更されないこと
        assert rates.TRIFECTA == pytest.approx(0.275)

    def test_custom_all_rates(self) -> None:
        rates = JRATakeoutRates(win=0.15, trifecta=0.30)
        assert rates.WIN == pytest.approx(0.15)
        assert rates.TRIFECTA == pytest.approx(0.30)
        assert rates.QUINELLA == pytest.approx(0.225)  # 変更なし


# ─────────────────────────────────────────────────────────────────────────────
# MarketProbabilityCalc — Shin 確率
# ─────────────────────────────────────────────────────────────────────────────


class TestShinProbabilities:
    def setup_method(self) -> None:
        self.calc = MarketProbabilityCalc()

    def test_sum_to_one(self) -> None:
        odds = np.array([2.0, 4.0, 6.0, 10.0, 20.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert probs.sum() == pytest.approx(1.0, abs=1e-6)

    def test_favorite_has_highest_prob(self) -> None:
        odds = np.array([1.5, 5.0, 10.0, 20.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert int(probs.argmax()) == 0

    def test_all_equal_odds_uniform(self) -> None:
        odds = np.array([4.0, 4.0, 4.0, 4.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert probs == pytest.approx([0.25, 0.25, 0.25, 0.25], abs=1e-5)

    def test_favorite_longshot_bias_correction(self) -> None:
        # 大穴馬（高オッズ）: Shin 確率 >= 市場暗示確率（FLB 補正）
        odds = np.array([1.3, 2.0, 5.0, 20.0, 50.0])
        probs = self.calc.compute_shin_probabilities(odds)
        raw = 1.0 / odds
        raw_norm = raw / raw.sum()
        assert probs[-1] >= raw_norm[-1]

    def test_invalid_odds_handled(self) -> None:
        # 0 や NaN を含む場合もクラッシュせず有限値を返すこと
        odds = np.array([2.0, 0.0, np.nan, 10.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert np.isfinite(probs).all()
        assert probs.sum() == pytest.approx(1.0, abs=1e-6)

    def test_two_horse_race(self) -> None:
        odds = np.array([1.5, 3.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert probs.sum() == pytest.approx(1.0, abs=1e-6)
        assert probs[0] > probs[1]

    def test_single_horse_returns_one(self) -> None:
        odds = np.array([2.0])
        probs = self.calc.compute_shin_probabilities(odds)
        assert probs[0] == pytest.approx(1.0, abs=1e-6)


# ─────────────────────────────────────────────────────────────────────────────
# MarketProbabilityCalc — Harville 法 複勝確率
# ─────────────────────────────────────────────────────────────────────────────


class TestHarvillePlaceProb:
    def setup_method(self) -> None:
        self.calc = MarketProbabilityCalc()

    def test_place_prob_ge_win_prob(self) -> None:
        probs = np.array([0.4, 0.3, 0.2, 0.1])
        place = self.calc.harville_place_prob(probs, 0)
        assert place >= probs[0]

    def test_place_prob_between_0_and_1(self) -> None:
        probs = np.array([0.5, 0.3, 0.2])
        for i in range(3):
            p = self.calc.harville_place_prob(probs, i)
            assert 0.0 <= p <= 1.0

    def test_place_prob_sum_equals_3_for_4horses(self) -> None:
        # 4頭立て: 全馬の複勝確率の和 = 3.0（各着順に1頭ずつ）
        probs = np.array([0.4, 0.3, 0.2, 0.1])
        total = sum(self.calc.harville_place_prob(probs, i) for i in range(4))
        assert total == pytest.approx(3.0, abs=1e-3)

    def test_underdog_has_positive_place_prob(self) -> None:
        probs = np.array([0.7, 0.2, 0.08, 0.02])
        p = self.calc.harville_place_prob(probs, 3)
        assert p > 0.0

    def test_two_horse_race(self) -> None:
        probs = np.array([0.6, 0.4])
        # 2頭立て: 複勝確率 = 単勝確率（1着か2着しかない）
        p0 = self.calc.harville_place_prob(probs, 0)
        p1 = self.calc.harville_place_prob(probs, 1)
        assert p0 + p1 == pytest.approx(2.0, abs=1e-6)


# ─────────────────────────────────────────────────────────────────────────────
# MarketProbabilityCalc — 三連単確率
# ─────────────────────────────────────────────────────────────────────────────


class TestHarvilleTrifectaProb:
    def setup_method(self) -> None:
        self.calc = MarketProbabilityCalc()

    def test_order_matters(self) -> None:
        probs = np.array([0.5, 0.3, 0.2])
        p_abc = self.calc.harville_trifecta_prob(probs, 0, 1, 2)
        p_bac = self.calc.harville_trifecta_prob(probs, 1, 0, 2)
        assert p_abc != pytest.approx(p_bac)

    def test_prob_between_0_and_1(self) -> None:
        probs = np.array([0.4, 0.3, 0.2, 0.1])
        p = self.calc.harville_trifecta_prob(probs, 0, 1, 2)
        assert 0.0 < p <= 1.0

    def test_all_permutations_sum_to_one(self) -> None:
        # 3頭立て: 全 6 通りの三連単確率の和 = 1
        probs = np.array([0.5, 0.3, 0.2])
        total = sum(
            self.calc.harville_trifecta_prob(probs, a, b, c)
            for a, b, c in permutations(range(3))
        )
        assert total == pytest.approx(1.0, abs=1e-5)

    def test_favorite_combo_highest(self) -> None:
        # 1→2→3 番人気の三連単が最も高確率になるべき
        probs = np.array([0.5, 0.3, 0.2])
        p_123 = self.calc.harville_trifecta_prob(probs, 0, 1, 2)
        p_321 = self.calc.harville_trifecta_prob(probs, 2, 1, 0)
        assert p_123 > p_321


# ─────────────────────────────────────────────────────────────────────────────
# OddsAnomalyDetector
# ─────────────────────────────────────────────────────────────────────────────


class TestOddsAnomalyDetector:
    def setup_method(self) -> None:
        self.detector = OddsAnomalyDetector(steam_threshold=0.15)

    def test_steam_flag_detected(self) -> None:
        morning = pd.Series([4.0, 8.0, 12.0])
        final = pd.Series([3.0, 8.5, 11.5])  # 馬0のみ 25% 急落
        flags = self.detector.detect_steam_move(morning, final)
        assert int(flags.iloc[0]) == 1
        assert int(flags.iloc[1]) == 0

    def test_no_steam_when_odds_rise(self) -> None:
        morning = pd.Series([3.0, 5.0])
        final = pd.Series([4.0, 6.0])  # 全馬オッズ上昇
        flags = self.detector.detect_steam_move(morning, final)
        assert int(flags.sum()) == 0

    def test_steam_exactly_at_threshold(self) -> None:
        morning = pd.Series([10.0])
        final = pd.Series([8.5])  # 15% 下落 = ちょうど閾値
        flags = self.detector.detect_steam_move(morning, final)
        assert int(flags.iloc[0]) == 1

    def test_odds_reversal_score_range(self) -> None:
        odds = pd.Series([1.5, 3.0, 6.0, 12.0])
        pop = pd.Series([1, 2, 3, 4])
        scores = self.detector.detect_odds_reversal(odds, pop)
        assert (scores >= 0).all()
        assert (scores <= 1).all()

    def test_odds_reversal_anticorrelated_higher_than_correlated(self) -> None:
        # 逆相関（高オッズなのに低人気番号）の方がスコアが高くなるべき
        odds = pd.Series([1.5, 3.0, 5.0, 10.0])
        pop_corr = pd.Series([1, 2, 3, 4])  # 一致: 低オッズ=高人気
        pop_rev = pd.Series([4, 3, 2, 1])  # 逆行: 低オッズ=低人気（異常）
        score_corr = self.detector.detect_odds_reversal(odds, pop_corr).max()
        score_rev = self.detector.detect_odds_reversal(odds, pop_rev).max()
        assert float(score_rev) >= float(score_corr)

    def test_late_money_ratio_range(self) -> None:
        early = pd.Series([1000.0, 500.0, 200.0])
        late = pd.Series([100.0, 400.0, 50.0])
        ratio = self.detector.compute_late_money_ratio(early, late)
        assert (ratio >= 0).all()
        assert (ratio <= 1).all()

    def test_late_money_all_late(self) -> None:
        early = pd.Series([0.0, 0.0])
        late = pd.Series([100.0, 200.0])
        ratio = self.detector.compute_late_money_ratio(early, late)
        # ≈ 1.0 (late / (late + ε))
        assert (ratio > 0.99).all()


# ─────────────────────────────────────────────────────────────────────────────
# EVEnhancedFeatures
# ─────────────────────────────────────────────────────────────────────────────


class TestEVEnhancedFeatures:
    def _make_race_df(self, n: int = 5) -> pd.DataFrame:
        rng = np.random.default_rng(42)
        return pd.DataFrame(
            {
                "horse_number": range(1, n + 1),
                "win_odds": rng.uniform(1.5, 30.0, n),
                "popularity": range(1, n + 1),
            }
        )

    def test_required_columns_added(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(6)
        result = engine.add_ev_features(df)
        for col in [
            "shin_prob",
            "implied_prob_excess",
            "harville_place_prob",
            "field_strength_ev_adj",
            "ev_rank_in_race",
            "odds_steam_flag",
            "odds_reversal_score",
        ]:
            assert col in result.columns, f"列 '{col}' が結果 DataFrame に存在しない"

    def test_shin_prob_sums_to_one(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(8)
        result = engine.add_ev_features(df)
        assert float(result["shin_prob"].sum()) == pytest.approx(1.0, abs=1e-5)

    def test_original_df_not_modified(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(4)
        original_cols = df.columns.tolist()
        engine.add_ev_features(df)
        assert df.columns.tolist() == original_cols

    def test_ev_rank_min_is_one(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(5)
        result = engine.add_ev_features(df)
        assert float(result["ev_rank_in_race"].min()) == pytest.approx(1.0)

    def test_field_strength_between_0_and_1(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(10)
        result = engine.add_ev_features(df)
        assert (result["field_strength_ev_adj"] >= 0).all()
        assert (result["field_strength_ev_adj"] <= 1).all()

    def test_harville_place_prob_ge_shin_prob(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(6)
        result = engine.add_ev_features(df)
        # 複勝確率 >= 単勝確率 であること（3着以内 > 1着）
        assert (result["harville_place_prob"] >= result["shin_prob"]).all()

    def test_empty_df_returns_empty(self) -> None:
        engine = EVEnhancedFeatures()
        df = pd.DataFrame(columns=["win_odds", "popularity"])
        result = engine.add_ev_features(df)
        assert len(result) == 0

    def test_steam_flag_zero_when_no_morning_odds(self) -> None:
        engine = EVEnhancedFeatures()
        df = self._make_race_df(4)
        # odds_vs_morning 列なし → スチームフラグは全 0
        result = engine.add_ev_features(df, morning_odds_col=None)
        assert int(result["odds_steam_flag"].sum()) == 0

    def test_implied_prob_excess_sums_near_zero(self) -> None:
        # Shin確率と市場確率はどちらも和=1 → 差分の和 ≈ 0
        engine = EVEnhancedFeatures()
        df = self._make_race_df(8)
        result = engine.add_ev_features(df)
        assert float(result["implied_prob_excess"].sum()) == pytest.approx(
            0.0, abs=1e-5
        )
