"""未来資産曲線シミュレータ（scripts/simulate_max_revenue.py）のテスト。

ゼロ除算・無限ループ・再現性・出力の健全性を保証する。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# scripts/ をパスに通してモジュールを読み込む
_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts" / "simulate_max_revenue.py"

spec = importlib.util.spec_from_file_location("simulate_max_revenue", _SCRIPT)
assert spec is not None and spec.loader is not None
smr = importlib.util.module_from_spec(spec)
sys.modules["simulate_max_revenue"] = smr
spec.loader.exec_module(smr)


# --------------------------------------------------------------------------- #
# kelly_stake
# --------------------------------------------------------------------------- #


def test_kelly_stake_no_zero_division_when_odds_one() -> None:
    """オッズ1.0（denom=0）でもゼロ除算せず0を返す。"""
    assert smr.kelly_stake(1_000_000, 0.5, 1.0, 0.1, 50_000) == 0.0


def test_kelly_stake_zero_on_negative_edge() -> None:
    """負のエッジ（p*o < 1）ではベットしない。"""
    # p=0.1, odds=2.0 -> p*o=0.2 < 1
    assert smr.kelly_stake(1_000_000, 0.1, 2.0, 0.1, 50_000) == 0.0


def test_kelly_stake_capped() -> None:
    """ステークは cap を超えない。"""
    stake = smr.kelly_stake(10_000_000, 0.6, 5.0, 1.0, 50_000)
    assert 0.0 <= stake <= 50_000


def test_kelly_stake_not_exceed_bankroll() -> None:
    """ステークは bankroll を超えない。"""
    stake = smr.kelly_stake(100.0, 0.9, 10.0, 1.0, 1_000_000)
    assert stake <= 100.0


# --------------------------------------------------------------------------- #
# percentile / drawdown ヘルパ
# --------------------------------------------------------------------------- #


def test_percentile_bounds() -> None:
    vals = [float(i) for i in range(101)]  # 0..100
    assert smr._percentile(vals, 0.0) == 0.0
    assert smr._percentile(vals, 1.0) == 100.0
    assert smr._percentile(vals, 0.5) == pytest.approx(50.0)


def test_percentile_empty_and_single() -> None:
    assert smr._percentile([], 0.5) == 0.0
    assert smr._percentile([42.0], 0.9) == 42.0


def test_max_drawdown_monotonic_increasing_is_zero() -> None:
    assert smr._max_drawdown([1, 2, 3, 4, 5]) == 0.0


def test_max_drawdown_half() -> None:
    # ピーク100 -> 50 で 50% DD
    assert smr._max_drawdown([100.0, 50.0, 80.0]) == pytest.approx(0.5)


# --------------------------------------------------------------------------- #
# シミュレーション本体
# --------------------------------------------------------------------------- #


def test_single_trial_curve_length() -> None:
    """1試行の曲線は 53点（開始 + 52週末）で、有限値・無限ループしない。"""
    import random

    sc = smr.build_scenarios()[0]
    curve = smr.simulate_one_trial(sc, random.Random(1))
    assert len(curve) == smr.WEEKS_PER_YEAR + 1
    assert all(isinstance(v, float) for v in curve)
    assert all(v == v for v in curve)  # NaN でない


def test_run_scenario_reproducible() -> None:
    """同一シードなら結果は完全再現される。"""
    sc = smr.build_scenarios()[0]
    r1 = smr.run_scenario(sc, trials=30, seed=123)
    r2 = smr.run_scenario(sc, trials=30, seed=123)
    assert r1.final_median == r2.final_median
    assert r1.median_curve == r2.median_curve


def test_run_scenario_curves_aligned() -> None:
    sc = smr.build_scenarios()[1]
    r = smr.run_scenario(sc, trials=20, seed=7)
    n = smr.WEEKS_PER_YEAR + 1
    assert len(r.median_curve) == n
    assert len(r.p10_curve) == n
    assert len(r.p90_curve) == n
    # P10 <= P50 <= P90 が各点で成り立つ
    for lo, mid, hi in zip(r.p10_curve, r.median_curve, r.p90_curve):
        assert lo <= mid <= hi


def test_run_scenario_invalid_trials() -> None:
    sc = smr.build_scenarios()[0]
    with pytest.raises(ValueError):
        smr.run_scenario(sc, trials=0, seed=1)


def test_confirmed_scenario_loses_vs_backtest() -> None:
    """確定実績シナリオC(ROI80%)はバックテストA(ROI222%)より中央値が低い。"""
    results = {r.scenario.name: r for r in smr.run_all(trials=50, seed=42)}
    assert results["C"].final_median < results["A"].final_median
    # C は負けエッジなので初期割れ確率が高い
    assert results["C"].ruin_probability > 0.5


def test_backtest_scenario_profitable_median() -> None:
    """バックテスト前提Aは中央値で初期原資を上回る。"""
    results = {r.scenario.name: r for r in smr.run_all(trials=50, seed=42)}
    sc_a = results["A"].scenario
    assert results["A"].final_median > sc_a.initial_bankroll


# --------------------------------------------------------------------------- #
# レポート生成
# --------------------------------------------------------------------------- #


def test_build_report_contains_key_sections() -> None:
    results = smr.run_all(trials=20, seed=5)
    report = smr.build_report(results, trials=20, seed=5)
    assert "未来資産曲線シミュレーション" in report
    assert "前提の健全性" in report
    assert "更新履歴" in report
    assert "シナリオ A" in report or "シナリオ A:" in report
    # 文字化けの混入がない（? の連続は無いこと）
    assert "????" not in report


def test_fmt_yen_units() -> None:
    assert "円" in smr._fmt_yen(500)
    assert "万円" in smr._fmt_yen(50_000)
    assert "億円" in smr._fmt_yen(200_000_000)


def test_sparkline_no_crash_on_flat() -> None:
    """全点同値（span=0）でもスパークラインが落ちない。"""
    line = smr._sparkline([100.0] * 53)
    assert len(line) > 0


def test_ascii_chart_handles_empty() -> None:
    assert smr._ascii_line_chart([]) == "(データなし)"


def test_main_writes_report(tmp_path: Path) -> None:
    """main() がレポートとJSONを書き出し、終了コード0を返す。"""
    report = tmp_path / "report.md"
    js = tmp_path / "out.json"
    rc = smr.main(
        [
            "--trials",
            "10",
            "--seed",
            "1",
            "--report",
            str(report),
            "--json",
            str(js),
        ]
    )
    assert rc == 0
    assert report.exists()
    assert js.exists()
    assert "前提の健全性" in report.read_text(encoding="utf-8")
