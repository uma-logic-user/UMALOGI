"""買い目精度向上フィルタのテスト（2026-05-31 オーナー特別承認 スコープA）。

検証対象:
  #2 単勝オッズ帯フィルタ（1.5倍以下・100倍以上の足切り）
  #3 レース選定フィルタ（新馬戦・障害戦の見送り判定）
  #4 ワイド専用EVゲート（EV < 1.2 の除外）
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.ml.bet_generator import (
    BetGenerator,
    BetRecommendation,
    RaceBets,
    TANSHO_EV_MIN,
    TANSHO_ODDS_CEIL,
    TANSHO_ODDS_FLOOR,
    WIDE_EV_MIN,
    WIDE_MAX_POINTS,
    _build_odds_map,
    should_skip_race_for_betting,
)


# ── #3 レース選定フィルタ ───────────────────────────────────────────
class TestShouldSkipRace:
    @pytest.mark.parametrize(
        "race_name",
        [
            "第23回中山グランドジャンプ(JGI) 障",
            "第144回中山大障害(JGI) 障",
            "障害4歳以上オープン",
        ],
    )
    def test_shogai_is_skipped(self, race_name: str) -> None:
        skip, reason = should_skip_race_for_betting(race_name)
        assert skip is True
        assert "障害" in reason

    @pytest.mark.parametrize(
        "race_name",
        ["2歳新馬", "サラ系2歳メイクデビュー東京"],
    )
    def test_shinba_is_skipped(self, race_name: str) -> None:
        skip, reason = should_skip_race_for_betting(race_name)
        assert skip is True
        assert "新馬" in reason

    @pytest.mark.parametrize(
        "race_name",
        ["3歳未勝利", "3歳以上1勝クラス", "第81回皐月賞(GI)"],
    )
    def test_flat_race_is_kept(self, race_name: str) -> None:
        skip, reason = should_skip_race_for_betting(race_name)
        assert skip is False
        assert reason == ""

    def test_surface_shogai_fallback(self) -> None:
        # race_name に手掛かりが無くても surface で障害を弾く
        skip, _ = should_skip_race_for_betting("第10R", surface="障")
        assert skip is True

    def test_none_race_name_is_kept(self) -> None:
        skip, _ = should_skip_race_for_betting(None)
        assert skip is False


# ── _build_odds_map ────────────────────────────────────────────────
class TestBuildOddsMap:
    def test_basic(self) -> None:
        df = pd.DataFrame({"horse_number": [1, 2, 3], "win_odds": [2.5, 10.0, 50.0]})
        assert _build_odds_map(df) == {1: 2.5, 2: 10.0, 3: 50.0}

    def test_nan_excluded(self) -> None:
        df = pd.DataFrame({"horse_number": [1, 2], "win_odds": [float("nan"), 8.0]})
        assert _build_odds_map(df) == {2: 8.0}

    def test_missing_columns(self) -> None:
        assert _build_odds_map(pd.DataFrame({"horse_number": [1]})) == {}


# ── #2/#4 オッズ帯フィルタ + ワイドEVゲート ─────────────────────────
def _tansho(num: int, ev: float = 2.0) -> BetRecommendation:
    return BetRecommendation(
        bet_type="単勝",
        combinations=[(num,)],
        horse_names=[str(num)],
        expected_value=ev,
        model_score=0.3,
        recommended_bet=500.0,
        confidence=0.3,
    )


def _wide(ev: float) -> BetRecommendation:
    return BetRecommendation(
        bet_type="ワイド",
        combinations=[(1, 2)],
        horse_names=["1", "2"],
        expected_value=ev,
        model_score=0.2,
        recommended_bet=500.0,
        confidence=0.4,
    )


class TestOddsBandFilter:
    def _filter(
        self, bets: list[BetRecommendation], odds_map: dict[int, float]
    ) -> list[str]:
        gen = BetGenerator()  # conn 不要（OddsEstimator はデフォルトスケール）
        rb = RaceBets(race_id="202601010101", model_type="本命")
        rb.bets = list(bets)
        gen._apply_odds_band_filter(rb, odds_map)
        return [b.bet_type for b in rb.bets] + [
            f"{b.bet_type}:{b.combinations[0][0]}"
            for b in rb.bets
            if b.bet_type == "単勝"
        ]

    def test_tansho_overfavorite_excluded(self) -> None:
        # オッズ 1.5 以下は除外
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1)]
        BetGenerator()._apply_odds_band_filter(rb, {1: TANSHO_ODDS_FLOOR})
        assert rb.bets == []

    def test_tansho_longshot_excluded(self) -> None:
        # オッズ 100 以上は除外
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1)]
        BetGenerator()._apply_odds_band_filter(rb, {1: TANSHO_ODDS_CEIL})
        assert rb.bets == []

    def test_tansho_in_band_kept(self) -> None:
        # ボリュームゾーン（5.0〜30.0）は残す
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1)]
        BetGenerator()._apply_odds_band_filter(rb, {1: 12.0})
        assert len(rb.bets) == 1

    def test_tansho_no_odds_kept(self) -> None:
        # オッズ未取得（マップが空）の単勝は足切りしない
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert len(rb.bets) == 1

    def test_wide_low_ev_excluded(self) -> None:
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_wide(WIDE_EV_MIN - 0.01)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert rb.bets == []

    def test_wide_high_ev_kept(self) -> None:
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_wide(WIDE_EV_MIN)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert len(rb.bets) == 1


# ── W-049 #1 単勝EVゲート ───────────────────────────────────────────
class TestTanshoEvGate:
    def test_tansho_low_ev_excluded(self) -> None:
        # EV < 1.2 はオッズが帯内でも除外
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1, ev=TANSHO_EV_MIN - 0.01)]
        BetGenerator()._apply_odds_band_filter(rb, {1: 12.0})
        assert rb.bets == []

    def test_tansho_ev_excluded_even_without_odds(self) -> None:
        # オッズ未取得でも EV ゲートは適用される
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1, ev=0.8)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert rb.bets == []

    def test_tansho_ev_boundary_kept(self) -> None:
        # EV == 1.2 ちょうどは採用
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_tansho(1, ev=TANSHO_EV_MIN)]
        BetGenerator()._apply_odds_band_filter(rb, {1: 12.0})
        assert len(rb.bets) == 1


# ── W-049 #2 ワイド多点絞り込み ─────────────────────────────────────
def _wide_multi(n_points: int, ev: float = 2.0) -> BetRecommendation:
    """n_points 組（EV降順前提・horse_names は1組2頭でフラット）のワイドを作る。"""
    combos = [(1, i + 2) for i in range(n_points)]
    names: list[str] = []
    for c in combos:
        names.extend([str(c[0]), str(c[1])])
    return BetRecommendation(
        bet_type="ワイド",
        combinations=combos,
        horse_names=names,
        expected_value=ev,
        model_score=0.2,
        recommended_bet=1000.0,
        confidence=0.5,
    )


class TestWidePointLimit:
    def test_trim_to_max_points(self) -> None:
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_wide_multi(5)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert len(rb.bets) == 1
        wide = rb.bets[0]
        assert len(wide.combinations) == WIDE_MAX_POINTS
        # horse_names は 1組=2頭で同期して切り詰められる
        assert len(wide.horse_names) == WIDE_MAX_POINTS * 2
        # 先頭（高EV側）が保持される
        assert wide.combinations[0] == (1, 2)

    def test_no_trim_when_within_limit(self) -> None:
        rb = RaceBets(race_id="r", model_type="本命")
        rb.bets = [_wide_multi(WIDE_MAX_POINTS)]
        BetGenerator()._apply_odds_band_filter(rb, {})
        assert len(rb.bets[0].combinations) == WIDE_MAX_POINTS
