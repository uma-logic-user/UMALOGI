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
    assert abs(s.hit_rate - 200.0 / 3) < 0.01
    assert abs(s.roi - 500.0 / 300.0 * 100) < 0.01


def test_strategy_stats_summary_row_format():
    s = StrategyStats(label="本命・単勝(Top1)", bet_type="単勝")
    s.add(True, 500.0)
    row = s.summary_row()
    assert row[0] == "本命・単勝(Top1)"
    assert "1" in row[1]       # races
    assert "100.0%" in row[3]  # hit_rate
    assert "○" in row[7]       # 黒字フラグ
