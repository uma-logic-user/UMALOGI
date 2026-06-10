"""tests/test_no_bet_filter.py — 見送り（No-Bet）判定モデルのテスト。"""

from __future__ import annotations

import numpy as np
import pytest

from src.ml.no_bet_filter import (
    NoBetConfig,
    evaluate_no_bet,
    jensen_shannon_distance,
    market_probs_from_odds,
    normalized_entropy,
)

# 本命が強い堅いレース（1.5倍の断然人気・12頭）
_SOLID_ODDS = [1.5, 4.0, 8.0, 15.0, 25.0, 40.0, 60.0, 80.0, 100.0, 120.0, 150.0, 200.0]
# 全馬均衡の大混戦（16頭・全馬10-20倍）
_CHAOS_ODDS = [
    12.0,
    13.0,
    14.0,
    11.0,
    15.0,
    12.5,
    13.5,
    16.0,
    12.0,
    14.5,
    11.5,
    13.0,
    15.5,
    12.8,
    14.2,
    13.8,
]


# ── 基礎関数 ─────────────────────────────────────────────────────────────
class TestPrimitives:
    def test_market_probs_normalized(self) -> None:
        p = market_probs_from_odds(_SOLID_ODDS)
        assert p.sum() == pytest.approx(1.0)
        assert p[0] == p.max()  # 1.5倍が最大確率

    def test_entropy_bounds(self) -> None:
        uniform = np.full(10, 0.1)
        spike = np.array([0.99] + [0.01 / 9] * 9)
        assert normalized_entropy(uniform) == pytest.approx(1.0)
        assert normalized_entropy(spike) < 0.2

    def test_js_distance_identical_zero(self) -> None:
        p = np.array([0.5, 0.3, 0.2])
        assert jensen_shannon_distance(p, p) == pytest.approx(0.0, abs=1e-9)

    def test_js_distance_disjoint_high(self) -> None:
        p = np.array([1.0, 0.0, 0.0])
        q = np.array([0.0, 0.0, 1.0])
        assert jensen_shannon_distance(p, q) > 0.9


# ── 見送り判定 ───────────────────────────────────────────────────────────
class TestEvaluateNoBet:
    def test_solid_race_is_bettable(self) -> None:
        d = evaluate_no_bet("r1", _SOLID_ODDS)
        assert d.skip is False
        assert d.chaos_score < 0.5

    def test_chaotic_race_is_skipped(self) -> None:
        d = evaluate_no_bet("r2", _CHAOS_ODDS)
        assert d.skip is True
        assert any("混戦" in r for r in d.reasons)
        assert any("弱い本命" in r for r in d.reasons)

    def test_chaos_score_monotonic(self) -> None:
        solid = evaluate_no_bet("r1", _SOLID_ODDS).chaos_score
        chaos = evaluate_no_bet("r2", _CHAOS_ODDS).chaos_score
        assert chaos > solid

    def test_overround_anomaly_detected(self) -> None:
        # 異常に低いオーバーラウンド（オッズ供給欠損を模擬: 3頭分しかない等価）
        d = evaluate_no_bet("r3", [10.0, 12.0, 15.0, 20.0, 30.0, 50.0, 80.0, 100.0])
        assert d.components["overround"] > 0
        assert any("オーバーラウンド" in r for r in d.reasons)

    def test_model_market_divergence_flags_anomaly(self) -> None:
        # モデルが市場と全面的に逆 → データ異常疑いとして divergence が立つ
        n = len(_SOLID_ODDS)
        inverted = market_probs_from_odds(_SOLID_ODDS)[::-1]
        d = evaluate_no_bet("r4", _SOLID_ODDS, model_win_probs=list(inverted))
        assert d.components["divergence"] > 0

    def test_agreeing_model_no_divergence(self) -> None:
        agree = market_probs_from_odds(_SOLID_ODDS)
        d = evaluate_no_bet("r5", _SOLID_ODDS, model_win_probs=list(agree))
        assert d.components["divergence"] == 0.0

    def test_small_field_and_heavy_going(self) -> None:
        d = evaluate_no_bet("r6", [2.0, 3.5, 6.0, 10.0, 20.0], condition="不良")
        assert any("少頭数" in r for r in d.reasons)
        assert any("馬場" in r for r in d.reasons)
        assert d.components["structure"] == 1.0

    def test_empty_odds_always_skip(self) -> None:
        d = evaluate_no_bet("r7", [])
        assert d.skip is True

    def test_threshold_configurable(self) -> None:
        strict = NoBetConfig(chaos_threshold=0.0)
        d = evaluate_no_bet("r8", _SOLID_ODDS, config=strict)
        assert d.skip is True  # 閾値0なら堅いレース(score 0.0)も見送り

        lax = NoBetConfig(chaos_threshold=0.99)
        d2 = evaluate_no_bet("r8b", _CHAOS_ODDS, config=lax)
        assert d2.skip is False  # 閾値を上げれば大混戦も通す

    def test_decision_never_modifies_probs(self) -> None:
        # ゲートのみで確率・EVへの係数操作はしない（W-071遵守の構造確認）
        d = evaluate_no_bet("r9", _SOLID_ODDS)
        assert not hasattr(d, "adjusted_probs")
        assert not hasattr(d, "ev_multiplier")
