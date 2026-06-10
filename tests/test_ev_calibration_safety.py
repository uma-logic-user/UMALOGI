"""W-066 大穴 EV 暴騰（較正歪み）の安全装置テスト（blend_with_market 移行版）。

旧 EV_SANITY_CAP=2.0 は「EV を 2.0 に揃えてゲートを素通りさせる」逆効果があった。
新ロジックは blend_with_market により大穴ほど市場確率(0.80/odds)へ収縮させ、
EV 暴騰を構造的に根絶する。本テストは移行後の 2 層安全装置を担保する:

  Layer 1: calibrate_win_prob の blend_with_market（P <= P_market * MAX_RELATIVE_EDGE）
  Layer 2: pure_ev_edge の実弾単勝 高オッズ足切り（odds > MAX_LIVE_WIN_ODDS で棄却）

EV の新しい上限:
  blend 後の理論最大 EV = (1 − MARKET_TAKE_RATE) × MAX_RELATIVE_EDGE = 0.80 × 1.50 = 1.20
  旧 EV_SANITY_CAP=2.0 よりさらに厳しく抑制される。
"""

from __future__ import annotations

from unittest.mock import patch

import src.ml.pure_ev_edge as PE
from src.ml.manji_calibration import calibrate_win_prob
from src.ml.market_blend_calibration import (
    MARKET_TAKE_RATE,
    MAX_RELATIVE_EDGE,
    blend_with_market,
)
from src.ml.pure_ev_edge import (
    MAX_LIVE_WIN_ODDS,
    PureEVConfig,
    select_pure_ev_bets,
    tansho_ev,
)

_EPS = 1e-6
# 新ロジックでの理論上限 EV（旧 EV_SANITY_CAP=2.0 より厳しい 1.20）
_BLEND_EV_MAX: float = (1.0 - MARKET_TAKE_RATE) * MAX_RELATIVE_EDGE  # = 1.20


# ── 複勝特化 Platt 較正器（2026-06-02 卍複勝昇格）──────────────────────────
def test_place_calibrator_returns_probability() -> None:
    """calibrate_place_prob は 0〜1 の確率を返し、飽和(=1.0)しない。"""
    from src.ml.manji_calibration import calibrate_place_prob

    for ev in (0.0, 0.5, 1.0, 2.0, 5.0, 30.0):
        p = calibrate_place_prob(ev)
        assert 0.0 <= p <= 0.999, f"ev={ev} で確率が範囲外: {p}"


def test_place_calibrator_monotonic_nondecreasing() -> None:
    """ev_score が大きいほど P(複勝圏) は概ね単調非減少（学習済み/フォールバック双方）。"""
    from src.ml.manji_calibration import calibrate_place_prob

    probs = [calibrate_place_prob(ev) for ev in (0.5, 1.0, 2.0, 3.0, 5.0)]
    assert all(b >= a - _EPS for a, b in zip(probs, probs[1:])), probs


def test_place_calibrator_is_independent_from_win() -> None:
    """複勝較正器は単勝(Isotonic)とは独立（別パス・odds 引数を取らない）。"""
    from src.ml.manji_calibration import _PLACE_CAL_PATH, _WIN_CAL_PATH

    assert _PLACE_CAL_PATH != _WIN_CAL_PATH
    assert _PLACE_CAL_PATH.name == "manji_place_calibrator.pkl"


# ── Layer 1: calibrate_win_prob の blend_with_market キャップ ─────────────────
def test_longshot_ev_is_capped() -> None:
    """大穴（高オッズ）でも EV=P×odds が _BLEND_EV_MAX(=1.20) を超えない（旧 2.0 から強化）。"""
    for odds in (20.0, 49.7, 100.0, 500.0):
        p = calibrate_win_prob(3.0, odds)
        assert p * odds <= _BLEND_EV_MAX + _EPS, f"odds={odds} で EV 暴騰: {p * odds:.3f}"


def test_extreme_ev_score_still_capped() -> None:
    """ev_score が極端でも大穴の EV は blend 後の理論上限(_BLEND_EV_MAX)を超えない。"""
    p = calibrate_win_prob(30.0, 50.0)
    assert p * 50.0 <= _BLEND_EV_MAX + _EPS


def test_favorite_probability_not_clobbered() -> None:
    """人気馬（低オッズ）は blend ウェイト w=1.0 のためモデル確率が保持される。

    10倍以下では w=1.0（モデル完全信頼）。P_cap = P_market × MAX_RELATIVE_EDGE が
    十分に大きいため、通常の較正確率は切り下げられない。
    """
    odds = 2.0
    p = calibrate_win_prob(2.0, odds)
    # blend 後も確率が 0〜1 の範囲内
    assert 0.0 <= p < 1.0
    # EV が blend 理論上限を超えない
    assert p * odds <= _BLEND_EV_MAX + _EPS


def test_tansho_ev_bounded_for_longshot() -> None:
    """tansho_ev 経由でも大穴 EV は blend 理論上限(_BLEND_EV_MAX=1.20)以下。"""
    _p, ev = tansho_ev(manji_ev_score=5.0, win_odds=80.0)
    assert ev <= _BLEND_EV_MAX + _EPS


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
    """実較正を用いた選定でも、採用された買い目の EV は blend 理論上限(_BLEND_EV_MAX)以下。"""
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
            assert b.expected_value <= _BLEND_EV_MAX + _EPS
