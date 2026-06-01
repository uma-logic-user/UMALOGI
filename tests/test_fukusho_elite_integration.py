"""W-020 FukushoElite 本番統合（EV 最優先ゲート）のテスト。

FukushoElite は複勝特化の実弾モデル。segment+edge フィルターに加え、
統計的複勝 EV（P(place)×推定複勝オッズ）が FUKUSHO_ELITE_EV_MIN 以上の
レース/馬のみ買い目を生成する（勝率・複勝率単独のベットは禁止）。
"""

from __future__ import annotations

from src.ml.bet_generator import (
    FUKUSHO_ELITE_EV_MIN,
    generate_elite_fukusho_bets,
)
from src.ml.bet_policy import LIVE_MODELS, is_live_bet

# 16頭・東京（収益セグメント）・上位3頭に高 ev_score / 低 implied → edge 通過
_N = 16
_HORSES = list(range(1, _N + 1))
_NAMES = [f"H{i}" for i in _HORSES]
# 上位3頭(1,2,3) ev_score=3.0、残りは 0.1 → 正規化 model_prob 上位 ~0.29
_EV_SCORES = [3.0, 3.0, 3.0] + [0.1] * (_N - 3)
# 上位3頭の implied を低く（0.05）→ edge = 0.29/0.05 ≈ 5.8 >= 1.1
_IMPLIED = [0.05, 0.05, 0.05] + [(1.0 - 0.15) / (_N - 3)] * (_N - 3)
# 単勝オッズ（複勝EV推定に使用）
_WIN_ODDS = [4.0, 4.0, 4.0] + [20.0] * (_N - 3)


def _gen(place_top: float):
    """上位3頭の place_prob を place_top にして買い目生成を試みる。"""
    place_probs = [place_top, place_top, place_top] + [0.1] * (_N - 3)
    return generate_elite_fukusho_bets(
        race_id="202605010101",
        venue="東京",
        n_horses=_N,
        horse_numbers=_HORSES,
        horse_names=_NAMES,
        ev_scores=_EV_SCORES,
        implied_probs=_IMPLIED,
        win_odds=_WIN_ODDS,
        place_probs=place_probs,
    )


# ── 配線: 実弾モデル登録 ───────────────────────────────────────────────────
def test_fukusho_elite_is_live_model() -> None:
    assert "FukushoElite" in LIVE_MODELS
    assert is_live_bet("FukushoElite", "複勝") is True
    assert is_live_bet("FukushoElite(直前)", "複勝") is True


# ── EV 最優先ゲート ─────────────────────────────────────────────────────────
def test_high_ev_generates_bet_labeled_fukusho_elite() -> None:
    """複勝EVが基準超なら FukushoElite ラベルで複勝買い目を生成する。"""
    # place_prob=0.6, win_odds=4.0 → eff=1+(4-1)*0.33=1.99 → EV=1.194 >= 1.05
    rec = _gen(place_top=0.6)
    assert rec is not None
    assert rec.model_type == "FukushoElite"  # 卍 への誤ラベルが直っている
    assert len(rec.bets) == 1
    b = rec.bets[0]
    assert b.bet_type == "複勝"
    assert b.expected_value >= FUKUSHO_ELITE_EV_MIN
    assert all(len(c) == 1 for c in b.combinations)  # 単頭複勝


def test_low_ev_is_skipped_even_if_segment_passes() -> None:
    """segment+edge を通過しても複勝EVが基準未満なら見送る（資金流出防止）。"""
    # place_prob=0.4, win_odds=4.0 → EV=0.4*1.99=0.796 < 1.05 → 全馬見送り
    rec = _gen(place_top=0.4)
    assert rec is None


def test_ev_threshold_boundary() -> None:
    """境界近傍: EV>=1.05 で採用、<1.05 で棄却される。"""
    # place_prob を上げ下げして境界の符号が反転することを確認
    assert _gen(place_top=0.6) is not None  # EV 1.19
    assert _gen(place_top=0.45) is None  # EV 0.90


def test_non_segment_venue_skipped() -> None:
    """収益セグメント外の競馬場は EV を見るまでもなく見送る。"""
    rec = generate_elite_fukusho_bets(
        race_id="R",
        venue="中山",  # セグメント外
        n_horses=_N,
        horse_numbers=_HORSES,
        horse_names=_NAMES,
        ev_scores=_EV_SCORES,
        implied_probs=_IMPLIED,
        win_odds=_WIN_ODDS,
        place_probs=[0.6] * _N,
    )
    assert rec is None


def test_few_horses_skipped() -> None:
    """多頭数条件（>=13頭）未満は見送る。"""
    rec = generate_elite_fukusho_bets(
        race_id="R",
        venue="東京",
        n_horses=10,
        horse_numbers=_HORSES[:10],
        horse_names=_NAMES[:10],
        ev_scores=_EV_SCORES[:10],
        implied_probs=[1.0 / 10] * 10,
        win_odds=_WIN_ODDS[:10],
        place_probs=[0.6] * 10,
    )
    assert rec is None
