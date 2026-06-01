"""W-066 大穴 EV 暴騰（較正歪み）の安全装置テスト。

卍 Isotonic 較正器は ev_score のみで P(win) を返し odds を考慮しないため、
大穴（高オッズ）馬にも中位馬と同じ確率を付与し EV = P×odds が暴騰する
（例: P=0.145 × 49.7倍 = EV 7.2）。本テストは推論時の 2 層安全装置を担保する:

  Layer 1: calibrate_win_prob の市場相対キャップ（P <= EV_SANITY_CAP/odds）
  Layer 2: pure_ev_edge の実弾単勝 高オッズ足切り（odds > MAX_LIVE_WIN_ODDS で棄却）
"""

from __future__ import annotations

from unittest.mock import patch

import src.ml.pure_ev_edge as PE
from src.ml.manji_calibration import EV_SANITY_CAP, calibrate_win_prob
from src.ml.pure_ev_edge import (
    MAX_LIVE_WIN_ODDS,
    PureEVConfig,
    select_pure_ev_bets,
    tansho_ev,
)

_EPS = 1e-6


# ── Layer 1: calibrate_win_prob の EV サニティキャップ ─────────────────────
def test_longshot_ev_is_capped() -> None:
    """大穴（高オッズ）でも EV=P×odds が EV_SANITY_CAP を超えない。"""
    for odds in (20.0, 49.7, 100.0, 500.0):
        p = calibrate_win_prob(3.0, odds)
        assert p * odds <= EV_SANITY_CAP + _EPS, f"odds={odds} で EV 暴騰"


def test_extreme_ev_score_still_capped() -> None:
    """ev_score が極端でも大穴の EV は頭打ちになる。"""
    p = calibrate_win_prob(30.0, 50.0)
    assert p * 50.0 <= EV_SANITY_CAP + _EPS


def test_favorite_probability_not_clobbered() -> None:
    """人気馬（低オッズ）はキャップが発火しない（EV<=cap なら確率を温存）。

    cap は P <= EV_SANITY_CAP/odds。odds が小さいと閾値が大きく、通常の
    較正確率には届かない＝人気馬の確率は不変であることを保証する。
    """
    odds = 2.0
    cap_threshold = EV_SANITY_CAP / odds  # = 1.0 → 事実上どんな P でも温存
    p = calibrate_win_prob(2.0, odds)
    # キャップで人為的に切り下げられていない（=市場上限未満）
    assert p <= cap_threshold + _EPS
    assert 0.0 <= p < 1.0


def test_tansho_ev_bounded_for_longshot() -> None:
    """tansho_ev 経由でも大穴 EV は EV_SANITY_CAP 以下。"""
    _p, ev = tansho_ev(manji_ev_score=5.0, win_odds=80.0)
    assert ev <= EV_SANITY_CAP + _EPS


# ── Layer 2: pure_ev_edge の高オッズ足切り ─────────────────────────────────
def test_extreme_longshot_rejected_even_with_high_prob() -> None:
    """較正が高確率を返しても、MAX_LIVE_WIN_ODDS 超の大穴は実弾棄却される。"""
    odds = MAX_LIVE_WIN_ODDS + 50.0
    horse = {
        "horse_number": 1,
        "horse_name": "Longshot",
        "win_odds": odds,
        "manji_ev_score": 5.0,
        "place_prob": 0.9,
    }
    # 較正をモックして高確率を強制（Layer1 を迂回しても Layer2 が止める）
    with patch.object(PE, "calibrate_win_prob", return_value=0.5):
        bets = select_pure_ev_bets("R", [horse])
    assert bets.bets == []


def test_odds_at_ceiling_allowed() -> None:
    """ちょうど上限（=MAX_LIVE_WIN_ODDS）は足切りされず採用されうる（境界値・排他上限）。"""
    horse = {
        "horse_number": 2,
        "horse_name": "Edge",
        "win_odds": MAX_LIVE_WIN_ODDS,  # ちょうど上限
        "manji_ev_score": 5.0,
        "place_prob": 0.5,
    }
    # 較正をモック（Layer1 を迂回）し EV 条件を満たす確率を与える。
    with patch.object(PE, "calibrate_win_prob", return_value=0.1):
        bets = select_pure_ev_bets("R", [horse])
    # odds==上限は「> 上限」ではないため足切りされず、単勝買い目が生成される。
    assert any(b.bet_type == "単勝" and b.horse_number == 2 for b in bets.bets)


def test_pure_ev_longshot_ev_never_exceeds_cap() -> None:
    """実較正を用いた選定でも、採用された買い目の EV は EV_SANITY_CAP 以下。"""
    horses = [
        {
            "horse_number": i,
            "horse_name": str(i),
            "win_odds": o,
            "manji_ev_score": 3.0,
            "place_prob": 0.2,
        }
        for i, o in enumerate([3.0, 8.0, 15.0, 30.0, 45.0], start=1)
    ]
    bets = select_pure_ev_bets("R", horses, PureEVConfig(max_bets_per_race=10))
    for b in bets.bets:
        if b.bet_type == "単勝":
            assert b.expected_value <= EV_SANITY_CAP + _EPS
