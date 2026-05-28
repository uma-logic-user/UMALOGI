"""scripts/backtest_all_models.py の StrategyStats ユニットテスト"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from scripts.backtest_all_models import StrategyStats, _BET_AMOUNT


def test_strategy_stats_initial_state():
    s = StrategyStats(label="テスト", bet_type="単勝")
    assert s.races == 0
    assert s.hits == 0
    assert s.roi == 0.0
    assert s.hit_rate == 0.0
    assert s.profit == 0.0


def test_strategy_stats_add_hit():
    s = StrategyStats(label="テスト", bet_type="単勝")
    s.add(hit=True, payout=300.0)  # 100円賭けて300円回収
    assert s.races == 1
    assert s.hits == 1
    assert s.invested == _BET_AMOUNT
    assert s.payout == 300.0
    assert abs(s.roi - 300.0) < 0.01
    assert s.profit == 200.0


def test_strategy_stats_add_miss():
    s = StrategyStats(label="テスト", bet_type="単勝")
    s.add(hit=False, payout=0.0)
    assert s.races == 1
    assert s.hits == 0
    assert s.roi == 0.0
    assert s.profit == -_BET_AMOUNT


def test_strategy_stats_roi_multiple():
    s = StrategyStats(label="テスト", bet_type="複勝")
    s.add(True, 200.0)
    s.add(False, 0.0)
    s.add(True, 300.0)
    # invested = 300, payout = 500
    assert s.races == 3
    assert s.hits == 2
    assert abs(s.hit_rate - 2 / 3 * 100) < 0.01
    assert abs(s.roi - 500.0 / 300.0 * 100) < 0.01


def test_strategy_stats_summary_row_profit():
    """ROI >= 100% のとき summary_row は "○" を返す。"""
    s = StrategyStats(label="本命・単勝(Top1)", bet_type="単勝")
    s.add(True, 500.0)   # invested=100, payout=500, roi=500%
    row = s.summary_row()
    assert row[0] == "本命・単勝(Top1)"  # label
    assert row[1] == "1"                # races
    assert row[2] == "1"                # hits
    assert row[3] == "100.0%"           # hit_rate
    assert row[4] == "100"              # invested
    assert row[5] == "500"              # payout
    assert row[6] == "500.0%"           # roi
    assert row[7] == "○"               # 黒字フラグ


def test_strategy_stats_summary_row_loss():
    """ROI < 100% のとき summary_row は "×" を返す。"""
    s = StrategyStats(label="テスト戦略", bet_type="複勝")
    s.add(False, 0.0)   # miss: invested=100, payout=0, roi=0%
    row = s.summary_row()
    assert row[0] == "テスト戦略"
    assert row[1] == "1"
    assert row[2] == "0"
    assert row[3] == "0.0%"
    assert row[4] == "100"
    assert row[5] == "0"
    assert row[6] == "0.0%"
    assert row[7] == "×"              # 赤字フラグ


# ── _select_horses テスト ────────────────────────────────────
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from scripts.backtest_all_models import _select_horses, STRATEGIES


def _make_df(n: int = 6) -> pd.DataFrame:
    """FeatureBuilder が返す形式を模したダミーDataFrame。"""
    return pd.DataFrame({
        "horse_name": [f"馬{i}" for i in range(n)],
        "win_odds":   [float(i * 2 + 1) for i in range(n)],
        "popularity": list(range(1, n + 1)),
    })


def _make_honmei(scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.predict.return_value = pd.Series(scores)
    return m


def _make_place(scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.predict.return_value = pd.Series(scores)
    return m


def _make_manji(ev_scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.ev_score.return_value = pd.Series(ev_scores)
    return m


def test_select_horses_honmei_top1():
    df     = _make_df(6)
    scores = [0.1, 0.5, 0.3, 0.8, 0.2, 0.6]
    honmei = _make_honmei(scores)
    place  = _make_place([0.0] * 6)
    manji  = _make_manji([0.0] * 6)
    picks  = _select_horses(df, STRATEGIES["honmei_tansho"], honmei, place, manji)
    assert picks == ["馬3"]   # index 3 のスコア 0.8 が最高


def test_select_horses_honmei_top3_insufficient():
    """頭数不足（3頭未満）の場合は空リストを返す。"""
    df     = _make_df(2)
    honmei = _make_honmei([0.5, 0.8])
    place  = _make_place([0.0, 0.0])
    manji  = _make_manji([0.0, 0.0])
    picks  = _select_horses(df, STRATEGIES["honmei_sanrenpuku"], honmei, place, manji)
    assert picks == []


def test_select_horses_manji_ev_filter_pass():
    """EV > 1.0 の馬が1頭以上なら picks を返す。"""
    df    = _make_df(4)
    manji = _make_manji([0.8, 1.2, 0.9, 1.5])  # index 1 と 3 が EV>1
    picks = _select_horses(df, STRATEGIES["manji_tansho"],
                           _make_honmei([0.0]*4), _make_place([0.0]*4), manji)
    assert "馬3" in picks    # 最高EV=1.5


def test_select_horses_manji_ev_filter_no_pass():
    """EV>1.0 が0頭のとき空リストを返す。"""
    df    = _make_df(4)
    manji = _make_manji([0.5, 0.6, 0.7, 0.8])  # 全員 EV<1
    picks = _select_horses(df, STRATEGIES["manji_tansho"],
                           _make_honmei([0.0]*4), _make_place([0.0]*4), manji)
    assert picks == []


def test_select_horses_place_top3():
    df    = _make_df(6)
    place = _make_place([0.1, 0.6, 0.9, 0.4, 0.7, 0.3])
    picks = _select_horses(df, STRATEGIES["place_fukusho_top3"],
                           _make_honmei([0.0]*6), place, _make_manji([0.0]*6))
    assert len(picks) == 3
    assert "馬2" in picks  # score 0.9 が最高


def test_strategies_all_keys_present():
    expected = {
        "honmei_tansho", "honmei_umaren", "honmei_sanrenpuku",
        "manji_tansho", "manji_fukusho",
        "place_fukusho", "place_fukusho_top3",
    }
    assert expected.issubset(set(STRATEGIES.keys()))
