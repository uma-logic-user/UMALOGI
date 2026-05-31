"""tests/test_calibration.py — src/ml/calibration.py のユニットテスト"""

from __future__ import annotations

import pytest
import pandas as pd

from src.ml.calibration import (
    correct_honmei_score,
    correction_factor_for,
    apply_calibration_to_series,
)


class TestCorrectHonmeiScore:
    """correct_honmei_score() のユニットテスト"""

    def test_zero_score_returns_zero(self) -> None:
        assert correct_honmei_score(0.0) == 0.0

    def test_negative_score_returns_zero(self) -> None:
        assert correct_honmei_score(-0.1) == 0.0

    def test_bin_0_to_005_applies_3081_factor(self) -> None:
        # bin 0.00-0.05: factor=3.081
        result = correct_honmei_score(0.02)
        expected = min(0.02 * 3.081, 1.0)
        assert abs(result - expected) < 1e-9

    def test_bin_005_to_010_applies_2077_factor(self) -> None:
        # bin 0.05-0.10: factor=2.077
        result = correct_honmei_score(0.07)
        expected = min(0.07 * 2.077, 1.0)
        assert abs(result - expected) < 1e-9

    def test_bin_010_to_015_applies_1910_factor(self) -> None:
        # bin 0.10-0.15: factor=1.910
        result = correct_honmei_score(0.12)
        expected = min(0.12 * 1.910, 1.0)
        assert abs(result - expected) < 1e-9

    def test_bin_015_to_020_applies_1357_factor(self) -> None:
        result = correct_honmei_score(0.17)
        expected = min(0.17 * 1.357, 1.0)
        assert abs(result - expected) < 1e-9

    def test_bin_020_to_025_applies_1141_factor(self) -> None:
        result = correct_honmei_score(0.22)
        expected = min(0.22 * 1.141, 1.0)
        assert abs(result - expected) < 1e-9

    def test_score_above_025_no_correction(self) -> None:
        # 0.25 以上は補正倍率 1.0
        result = correct_honmei_score(0.30)
        assert result == pytest.approx(0.30, abs=1e-9)

    def test_result_capped_at_1(self) -> None:
        # 大きなスコア × 補正倍率 > 1.0 でもキャップされる
        result = correct_honmei_score(0.04)  # 0.04 × 3.081 = 0.123 < 1.0 (OK)
        assert result <= 1.0

    def test_boundary_exactly_005(self) -> None:
        # 0.05 は bin 0.05-0.10 に属する（upper_bound=0.05 は exclusive）
        result = correct_honmei_score(0.05)
        expected = min(0.05 * 2.077, 1.0)
        assert abs(result - expected) < 1e-9

    def test_boundary_exactly_010(self) -> None:
        # 0.10 は bin 0.10-0.15 に属する
        result = correct_honmei_score(0.10)
        expected = min(0.10 * 1.910, 1.0)
        assert abs(result - expected) < 1e-9

    def test_correction_increases_score(self) -> None:
        # すべての対象 bin で補正後 > 補正前
        for raw in [0.01, 0.05, 0.10, 0.15, 0.20]:
            corrected = correct_honmei_score(raw)
            assert corrected >= raw, (
                f"raw={raw}: corrected={corrected} should be >= raw"
            )


class TestCorrectionFactorFor:
    """correction_factor_for() のユニットテスト"""

    def test_low_score_returns_high_factor(self) -> None:
        assert correction_factor_for(0.02) == pytest.approx(3.081)

    def test_mid_score_returns_correct_factor(self) -> None:
        assert correction_factor_for(0.12) == pytest.approx(1.910)

    def test_high_score_returns_1(self) -> None:
        assert correction_factor_for(0.50) == pytest.approx(1.0)


class TestApplyCalibrationToSeries:
    """apply_calibration_to_series() のユニットテスト"""

    def test_applies_correction_element_wise(self) -> None:
        raw = pd.Series([0.02, 0.07, 0.12, 0.17, 0.22, 0.30])
        corrected = apply_calibration_to_series(raw)
        assert len(corrected) == len(raw)
        # 各要素が個別補正と一致
        for i, r in enumerate(raw):
            assert corrected.iloc[i] == pytest.approx(correct_honmei_score(r))

    def test_does_not_modify_original(self) -> None:
        raw = pd.Series([0.05, 0.10, 0.15])
        original = raw.copy()
        apply_calibration_to_series(raw)
        pd.testing.assert_series_equal(raw, original)

    def test_corrected_mean_higher_than_raw_mean(self) -> None:
        # 全 bin 平均補正倍率 1.913 → 補正後平均は高くなる
        raw = pd.Series([0.02, 0.07, 0.12, 0.17, 0.22])
        corrected = apply_calibration_to_series(raw)
        assert corrected.mean() > raw.mean()

    def test_empty_series(self) -> None:
        raw = pd.Series([], dtype=float)
        corrected = apply_calibration_to_series(raw)
        assert len(corrected) == 0

    def test_with_log_prefix(self) -> None:
        raw = pd.Series([0.05, 0.10])
        # ログプレフィックス付きでもエラーにならない
        corrected = apply_calibration_to_series(raw, log_prefix="20260601_test_R1")
        assert len(corrected) == 2
