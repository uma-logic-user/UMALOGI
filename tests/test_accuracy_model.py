"""Accuracy Model（勝率特化 Classifier・タスク2.1）のユニットテスト。

DB を介さず、FEATURE_COLS を持つ合成 DataFrame を ``df=`` で渡して
モデルの学習・予測・保存/読込・リークフリー前提を高速に検証する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ml.accuracy_model import AccuracyModel
from src.ml.models import FEATURE_COLS


def _synthetic_df(n_races: int = 50, n_horses: int = 12, seed: int = 0) -> pd.DataFrame:
    """FEATURE_COLS + is_winner + race_id を持つ合成学習データを生成する。"""
    rng = np.random.default_rng(seed)
    rows = []
    for r in range(n_races):
        # 各馬に「強さ」を与え、強い馬ほど勝ちやすい signal を win_rate_all に込める
        strength = rng.random(n_horses)
        winner = int(np.argmax(strength + rng.normal(0, 0.1, n_horses)))
        for h in range(n_horses):
            row = {c: float(rng.normal()) for c in FEATURE_COLS}
            row["win_rate_all"] = float(strength[h])  # 予測可能な signal
            row["race_id"] = f"2025{r:08d}"
            row["is_winner"] = 1 if h == winner else 0
            rows.append(row)
    return pd.DataFrame(rows)


def test_leak_free_feature_cols_exclude_results() -> None:
    """リークフリー大原則: 結果系(rank/finish_time/margin)は特徴量に含まれない。"""
    for banned in ("rank", "finish_time", "margin"):
        assert banned not in FEATURE_COLS


def test_train_and_predict_proba() -> None:
    df = _synthetic_df()
    model = AccuracyModel()
    metrics = model.train(None, df=df)  # conn 不要（df 直渡し）
    assert metrics["trained"] is True
    assert metrics["n_races"] == 50
    assert model.is_trained
    proba = model.predict_proba(df)
    assert len(proba) == len(df)
    assert proba.min() >= 0.0 and proba.max() <= 1.0
    # 学習可能な signal があるため train AUC は 0.5 を上回るはず
    assert metrics["train_auc"] > 0.5


def test_predict_untrained_returns_uniform() -> None:
    df = _synthetic_df(n_races=3)
    model = AccuracyModel()
    proba = model.predict_proba(df)
    assert len(proba) == len(df)
    assert np.allclose(proba, proba[0])  # 一様


def test_train_skips_when_too_few_races() -> None:
    df = _synthetic_df(n_races=5)  # _MIN_TRAIN_RACES(30) 未満
    model = AccuracyModel()
    metrics = model.train(None, df=df)
    assert metrics.get("trained") is False
    assert not model.is_trained


def test_save_and_load_roundtrip(tmp_path) -> None:
    df = _synthetic_df()
    model = AccuracyModel()
    model.train(None, df=df)
    p = tmp_path / "acc.pkl"
    model.save(p)
    loaded = AccuracyModel().load(p)
    assert loaded.is_trained
    np.testing.assert_allclose(
        model.predict_proba(df), loaded.predict_proba(df), rtol=1e-6
    )


def test_predict_returns_series_aligned() -> None:
    df = _synthetic_df()
    model = AccuracyModel()
    model.train(None, df=df)
    s = model.predict(df)
    assert isinstance(s, pd.Series)
    assert list(s.index) == list(df.index)


def test_save_untrained_raises() -> None:
    with pytest.raises(RuntimeError):
        AccuracyModel().save()
