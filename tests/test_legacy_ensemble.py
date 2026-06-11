"""legacy_ensemble（過去モデル昇華・卍EV回帰アンサンブル）のユニットテスト。

検証の柱:
  1. w=0 恒等性 — 従来パイプライン（honmei 単独）と完全一致すること。
  2. スケール保存 — 融合後も Σp が Σp_honmei に一致すること
     （後段 blend_with_market / scan_all_tickets の挙動を変えないための不変条件）。
  3. 失敗安全 — 卍が無情報・异常値でも必ず honmei 単独へフォールバックすること。
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd
import pytest

from src.ml.legacy_ensemble import (
    MANJI_ENSEMBLE_BET_TYPES,
    MANJI_ENSEMBLE_WEIGHT,
    ManjiScoreSource,
    ensemble_win_probs,
    predict_manji_ev,
)


# ── ensemble_win_probs ───────────────────────────────────────────────────────
class TestEnsembleWinProbs:
    def setup_method(self) -> None:
        self.p_h = np.array([0.30, 0.20, 0.10, 0.05])
        self.ev = np.array([0.8, 1.2, 0.5, 2.0])
        self.odds = np.array([2.5, 6.0, 12.0, 40.0])

    def test_weight_zero_is_identity(self) -> None:
        """w=0 は honmei 確率の完全コピー（従来パイプラインと恒等）。"""
        out = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=0.0)
        np.testing.assert_array_equal(out, self.p_h)

    def test_negative_weight_is_identity(self) -> None:
        out = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=-1.0)
        np.testing.assert_array_equal(out, self.p_h)

    def test_sum_preserved(self) -> None:
        """融合後も総和 Σp が Σp_honmei に一致する（スケール保存の不変条件）。"""
        for w in (0.1, 0.4, 0.7, 1.0):
            out = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=w)
            assert out.sum() == pytest.approx(self.p_h.sum())

    def test_weight_one_uses_manji_only(self) -> None:
        """w=1 は卍暗黙勝率（スケール調整済み）のみになる。"""
        out = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=1.0)
        implied = self.ev / self.odds
        expected = implied / implied.sum() * self.p_h.sum()
        np.testing.assert_allclose(out, expected)

    def test_weight_above_one_clamped(self) -> None:
        out_1 = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=1.0)
        out_2 = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=5.0)
        np.testing.assert_allclose(out_1, out_2)

    def test_zero_manji_ev_falls_back_to_identity(self) -> None:
        """卍EVが全0（無情報）なら honmei 単独へフォールバック。"""
        out = ensemble_win_probs(self.p_h, np.zeros(4), self.odds, weight=0.4)
        np.testing.assert_array_equal(out, self.p_h)

    def test_negative_manji_ev_clipped(self) -> None:
        """負EVは0クリップされ、混合に負の確率が混入しない。"""
        ev = np.array([-5.0, 1.0, -1.0, 0.5])
        out = ensemble_win_probs(self.p_h, ev, self.odds, weight=0.4)
        assert (out >= 0).all()
        assert out.sum() == pytest.approx(self.p_h.sum())

    def test_nan_inputs_are_safe(self) -> None:
        ev = np.array([np.nan, 1.0, 0.5, np.nan])
        odds = np.array([2.5, np.nan, 12.0, 40.0])
        out = ensemble_win_probs(self.p_h, ev, odds, weight=0.4)
        assert np.isfinite(out).all()
        assert out.sum() == pytest.approx(self.p_h.sum())

    def test_zero_honmei_falls_back(self) -> None:
        """honmei 側が全0でも例外なく恒等コピーを返す。"""
        out = ensemble_win_probs(np.zeros(4), self.ev, self.odds, weight=0.4)
        np.testing.assert_array_equal(out, np.zeros(4))

    def test_blend_changes_ranking_toward_manji(self) -> None:
        """卍が強く支持する馬（高EV×低オッズ比）の確率が w に応じて単調増加する。"""
        idx = 3  # ev=2.0 / odds=40 → implied 比率が p_h 比率より高い馬
        prev = -1.0
        for w in (0.0, 0.2, 0.4, 0.6):
            out = ensemble_win_probs(self.p_h, self.ev, self.odds, weight=w)
            assert out[idx] > prev
            prev = out[idx]


# ── 定数（本番ポリシーのロック） ─────────────────────────────────────────────
class TestProductionConstants:
    def test_weight_is_oos_validated_value(self) -> None:
        """本番ウェイトは OOS 検証済みの 0.4。変更時は OOS 再検証必須。"""
        assert MANJI_ENSEMBLE_WEIGHT == pytest.approx(0.4)

    def test_bet_types_restricted_to_sanrenpuku(self) -> None:
        """適用券種は三連複のみ（三連単は OOS で劣化したため恒等を維持）。"""
        assert MANJI_ENSEMBLE_BET_TYPES == frozenset({"三連複"})


# ── predict_manji_ev ─────────────────────────────────────────────────────────
class _FakeBooster:
    """feature_name_ を持ち、列順を検証して定数を返すスタブ。"""

    feature_name_ = ["f1", "f2", "f3"]

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        assert list(x.columns) == self.feature_name_
        assert not x.isna().any().any()
        return np.full(len(x), 1.5)


class TestPredictManjiEv:
    def test_reindexes_and_fills_missing_columns(self) -> None:
        df = pd.DataFrame({"f2": [1.0, 2.0], "f1": [3.0, np.nan], "extra": [9, 9]})
        out = predict_manji_ev(_FakeBooster(), df)
        np.testing.assert_allclose(out, [1.5, 1.5])


# ── ManjiScoreSource（失敗安全） ─────────────────────────────────────────────
class TestManjiScoreSource:
    def test_load_failure_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """pkl ロード失敗時は None を返し続け、例外を漏らさない。"""
        import src.ml.legacy_ensemble as mod

        monkeypatch.setattr(
            mod, "_MANJI_PKL", mod._ROOT / "data" / "models" / "__missing__.pkl"
        )
        conn = sqlite3.connect(":memory:")
        src = ManjiScoreSource(conn)
        assert src.scores_for("202401010101") is None
        assert src.scores_for("202401010102") is None  # 2回目も安全（キャッシュ）
        conn.close()

    def test_inference_failure_returns_none(self) -> None:
        """FeatureBuilder 例外時も None（呼び出し側は従来動作へフォールバック）。"""
        conn = sqlite3.connect(":memory:")
        src = ManjiScoreSource(conn)
        src._model = _FakeBooster()

        class _Boom:
            def build_race_features(self, rid: str) -> None:
                raise RuntimeError("boom")

        src._builder = _Boom()
        assert src.scores_for("202401010101") is None
        conn.close()

    def test_scores_mapped_by_horse_number(self) -> None:
        conn = sqlite3.connect(":memory:")
        src = ManjiScoreSource(conn)
        src._model = _FakeBooster()

        class _Builder:
            def build_race_features(self, rid: str) -> pd.DataFrame:
                return pd.DataFrame(
                    {
                        "horse_number": [3, 7],
                        "f1": [0.1, 0.2],
                        "f2": [0, 0],
                        "f3": [0, 0],
                    }
                )

        src._builder = _Builder()
        out = src.scores_for("202401010101")
        assert out == {3: 1.5, 7: 1.5}
        conn.close()
