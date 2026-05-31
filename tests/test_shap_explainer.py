"""tests/test_shap_explainer.py

src/ml/shap_explainer.py のユニットテスト。
shap パッケージは存在しても存在しなくても動作することを検証する。
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.ml.shap_explainer import (
    FEATURE_LABEL_JP,
    build_shap_map,
    compute_shap_top,
    feature_label,
    shap_to_json,
)


# ── ヘルパー ────────────────────────────────────────────────────

FEAT_COLS = ["feat_a", "feat_b", "feat_c", "feat_d"]


def _make_X(n_horses: int = 3) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame(rng.random((n_horses, len(FEAT_COLS))), columns=FEAT_COLS)


def _make_model(shap_values: np.ndarray) -> MagicMock:
    """_model.booster_ が shap で使われるモックモデルを返す。"""
    booster = MagicMock()
    inner = MagicMock()
    inner.booster_ = booster
    model = MagicMock()
    model._model = inner

    # shap.TreeExplainer(booster).shap_values(X) → shap_values を返す
    explainer_instance = MagicMock()
    explainer_instance.shap_values.return_value = shap_values
    return model, booster, explainer_instance


# ── feature_label ───────────────────────────────────────────────


def test_feature_label_known() -> None:
    assert feature_label("jockey_win_rate_90d") == "騎手直近90日勝率"


def test_feature_label_unknown_returns_name() -> None:
    assert feature_label("unknown_feat_xyz") == "unknown_feat_xyz"


def test_feature_label_jp_covers_common_features() -> None:
    for key in ("weight_carried", "u_score", "tc_4f", "uf_crowd_bias"):
        assert key in FEATURE_LABEL_JP, f"{key} が FEATURE_LABEL_JP に未登録"


# ── shap_to_json ─────────────────────────────────────────────────


def test_shap_to_json_normal() -> None:
    d = {"feat_a": 0.3, "feat_b": -0.1}
    result = shap_to_json(d)
    assert result is not None
    parsed = json.loads(result)
    assert parsed["feat_a"] == pytest.approx(0.3)
    assert parsed["feat_b"] == pytest.approx(-0.1)


def test_shap_to_json_empty_returns_none() -> None:
    assert shap_to_json({}) is None


# ── compute_shap_top ─────────────────────────────────────────────


def test_compute_shap_top_shap_not_installed() -> None:
    """shap がインポートできない環境では空辞書リストを返す。"""
    X = _make_X(3)
    model = MagicMock()
    model._model = MagicMock()
    model._model.booster_ = MagicMock()

    with patch("builtins.__import__", side_effect=ImportError("no module named shap")):
        result = compute_shap_top(model, X, top_n=3)

    assert len(result) == 3
    assert all(r == {} for r in result)


def test_compute_shap_top_no_inner_model() -> None:
    """_model 属性が存在しないモデルは空辞書を返す。"""
    X = _make_X(2)
    model = MagicMock(spec=[])  # _model 属性を持たない
    result = compute_shap_top(model, X, top_n=3)
    assert len(result) == 2
    assert all(r == {} for r in result)


def test_compute_shap_top_no_booster() -> None:
    """booster_ が存在しない場合も空辞書を返す。"""
    X = _make_X(2)
    model = MagicMock()
    model._model = MagicMock(spec=[])  # booster_ を持たない
    result = compute_shap_top(model, X, top_n=3)
    assert len(result) == 2
    assert all(r == {} for r in result)


def test_compute_shap_top_2d_array() -> None:
    """shap_values が 2D ndarray の場合（回帰モデル等）に正しく動作する。"""
    n, f = 3, 4
    X = _make_X(n)
    # 固定 SHAP 値（行 0: feat_b=0.5 が最大）
    sv = np.array(
        [
            [0.1, 0.5, -0.3, 0.05],
            [0.2, -0.1, 0.4, 0.0],
            [-0.4, 0.3, 0.2, 0.1],
        ]
    )
    model = MagicMock()
    model._model = MagicMock()
    model._model.booster_ = MagicMock()

    mock_explainer = MagicMock()
    mock_explainer.shap_values.return_value = sv

    with patch.dict(
        "sys.modules", {"shap": MagicMock(TreeExplainer=lambda b: mock_explainer)}
    ):
        result = compute_shap_top(model, X, top_n=2)

    assert len(result) == 3
    # 行 0: |0.5| > |−0.3| → feat_b が top-1
    assert "feat_b" in result[0]
    assert result[0]["feat_b"] == pytest.approx(0.5)
    # 行 0: top-2 は feat_c (-0.3)
    assert "feat_c" in result[0]
    assert result[0]["feat_c"] == pytest.approx(-0.3)


def test_compute_shap_top_binary_classification_list() -> None:
    """LightGBM 2値分類が list[(n,f),(n,f)] を返す場合、class=1 を使用する。"""
    n, f = 2, 4
    X = _make_X(n)
    sv_class0 = np.zeros((n, f))
    sv_class1 = np.array(
        [
            [0.9, 0.1, -0.05, 0.02],
            [0.0, -0.8, 0.6, 0.3],
        ]
    )
    model = MagicMock()
    model._model = MagicMock()
    model._model.booster_ = MagicMock()

    mock_explainer = MagicMock()
    mock_explainer.shap_values.return_value = [sv_class0, sv_class1]

    with patch.dict(
        "sys.modules", {"shap": MagicMock(TreeExplainer=lambda b: mock_explainer)}
    ):
        result = compute_shap_top(model, X, top_n=2)

    assert len(result) == 2
    # 行 0: class1 の最大は feat_a=0.9
    assert "feat_a" in result[0]
    assert result[0]["feat_a"] == pytest.approx(0.9)


def test_compute_shap_top_exception_returns_empty() -> None:
    """計算中に例外が発生しても空辞書を返す（予測は続行）。"""
    X = _make_X(2)
    model = MagicMock()
    model._model = MagicMock()
    model._model.booster_ = MagicMock()

    broken_explainer = MagicMock()
    broken_explainer.shap_values.side_effect = RuntimeError("boom")

    with patch.dict(
        "sys.modules", {"shap": MagicMock(TreeExplainer=lambda b: broken_explainer)}
    ):
        result = compute_shap_top(model, X, top_n=2)

    assert len(result) == 2
    assert all(r == {} for r in result)


# ── build_shap_map ───────────────────────────────────────────────


def test_build_shap_map_maps_horse_number_to_json() -> None:
    """horse_number をキーにした shap_json 辞書が返る。"""
    n = 3
    X = _make_X(n)
    df = pd.DataFrame(
        {
            "horse_number": [3, 7, 11],
            "horse_name": ["馬A", "馬B", "馬C"],
        }
    )

    sv = np.array(
        [
            [0.5, -0.2, 0.1, 0.0],
            [0.0, 0.3, 0.4, 0.05],
            [-0.6, 0.1, 0.2, 0.05],
        ]
    )
    model = MagicMock()
    model._model = MagicMock()
    model._model.booster_ = MagicMock()

    mock_explainer = MagicMock()
    mock_explainer.shap_values.return_value = sv

    with patch.dict(
        "sys.modules", {"shap": MagicMock(TreeExplainer=lambda b: mock_explainer)}
    ):
        result = build_shap_map(model, X, df, top_n=2)

    assert set(result.keys()) == {3, 7, 11}
    # 馬番 3 の shap_json は有効な JSON 文字列
    assert result[3] is not None
    parsed = json.loads(result[3])  # type: ignore[arg-type]
    assert "feat_a" in parsed  # |0.5| が最大


def test_build_shap_map_empty_shap_gives_none() -> None:
    """SHAP 計算が空辞書を返した場合、shap_json は None になる。"""
    X = _make_X(2)
    df = pd.DataFrame({"horse_number": [1, 2], "horse_name": ["馬A", "馬B"]})
    model = MagicMock(spec=[])  # _model なし → 常に空辞書

    result = build_shap_map(model, X, df, top_n=3)
    assert result[1] is None
    assert result[2] is None
