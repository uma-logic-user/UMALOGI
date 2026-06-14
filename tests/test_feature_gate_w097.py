"""
W-097 / 検証プロトコル標準化 — 新特徴量ゲートキーパーの回帰テスト

ゲート判定（summarize_gate）が「複数 cutoff で一貫した ROI 改善のみ PASS」を
正しく実装していることを検証する。prev_trouble_proxy の実測（平均 -8.15pp・
改善 2/6）が確実に FAIL になることを固定する。
"""

from __future__ import annotations

from src.ml.feature_gate import (
    GatePolicy,
    add_months,
    evaluate_roi_auc,
    summarize_gate,
)


def _rows(deltas_roi: list[float], deltas_auc: list[float] | None = None) -> list[dict]:
    aucs = deltas_auc if deltas_auc is not None else [0.0] * len(deltas_roi)
    return [
        {"cutoff": f"c{i}", "d_roi": dr, "d_auc": da}
        for i, (dr, da) in enumerate(zip(deltas_roi, deltas_auc and aucs or aucs))
    ]


def test_consistent_improvement_passes() -> None:
    # 6 cutoff すべてで +、平均 +8pp、AUC 微増 → PASS
    res = summarize_gate(_rows([5, 8, 6, 12, 7, 10], [0.001] * 6), "good")
    assert res.passed is True
    assert res.win_rate == 1.0
    assert res.mean_delta_roi > 2.0


def test_prev_trouble_proxy_actual_result_fails() -> None:
    """W-096 実測（2/6改善・平均 -8.15pp）は必ず FAIL する。"""
    res = summarize_gate(
        _rows(
            [-15.4, -8.4, 5.9, -26.0, 10.5, -15.6],
            [-0.0034, -0.0027, -0.0010, -0.0028, -0.0034, -0.0027],
        ),
        "prev_trouble_proxy",
    )
    assert res.passed is False
    assert round(res.mean_delta_roi, 1) == -8.2 or round(res.mean_delta_roi, 2) == -8.17
    assert res.win_rate < 0.5
    assert "FAIL" in res.reason


def test_positive_mean_but_inconsistent_fails() -> None:
    # 平均は +だが半分以上の cutoff で負 → win_rate ゲートで FAIL
    res = summarize_gate(_rows([30, -2, -3, -1, 40, -2]), "lucky_spike")
    assert res.mean_delta_roi > 2.0  # 平均は正
    assert res.passed is False  # だが一貫性が無い
    assert "win_rate" in res.reason


def test_auc_degradation_blocks_pass() -> None:
    # ROI は改善・一貫だが AUC が大きく悪化 → AUC ゲートで FAIL
    res = summarize_gate(_rows([5, 6, 7, 8, 9, 10], [-0.01] * 6), "roi_up_auc_down")
    assert res.passed is False
    assert "auc" in res.reason


def test_empty_rows_fail_safe() -> None:
    res = summarize_gate([], "nodata")
    assert res.passed is False
    assert res.n_cutoffs == 0


def test_policy_thresholds_configurable() -> None:
    rows = _rows([1.0, 1.5, 1.2, 1.1, 1.3, 1.0])  # 全部正だが平均 ~1.2pp
    strict = summarize_gate(rows, "marginal", GatePolicy(min_mean_delta_roi=2.0))
    lenient = summarize_gate(rows, "marginal", GatePolicy(min_mean_delta_roi=1.0))
    assert strict.passed is False  # 平均が閾値2.0未満
    assert lenient.passed is True


def test_add_months() -> None:
    assert add_months("2026-01-01", 2) == "2026-03-01"
    assert add_months("2025-11-01", 2) == "2026-01-01"
    assert add_months("2026-12-01", 1) == "2027-01-01"


def test_evaluate_roi_auc_basic() -> None:
    import numpy as np
    import pandas as pd

    test = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r2", "r2"],
            "is_win": [1, 0, 0, 1],
            "win_odds": [2.0, 5.0, 3.0, 4.0],
        }
    )
    prob = np.array([0.9, 0.1, 0.4, 0.6])
    out = evaluate_roi_auc(test, prob, ev_threshold=1.0)
    assert "roi" in out and "auc" in out and "n_bets" in out
    assert out["n_bets"] >= 1
