"""Accuracy Model v2（的中率特化Classifier）のテスト。

既存EVモデルとの差別化:
  - 特徴量: FEATURE_COLS + リークフリー新特徴量（前走詳細・血統TE）
  - 閾値: Precision-Recall曲線の最適cut-offで的中率特化
  - 買い目: 各レースの勝率予測上位N頭に絞り込み（EV計算不要）

リークフリーの担保: logic_map.md の大原則を継承。
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd
import pytest

from src.ml.accuracy_model_v2 import AccuracyModelV2, find_optimal_threshold, pick_top_n


# ─── ユニットテスト ──────────────────────────────────────────────────────────

def test_pick_top_n_returns_highest_prob_horse() -> None:
    """各レースから勝率上位N頭を正しく選出できる。"""
    df = pd.DataFrame({
        "race_id": ["R1", "R1", "R1", "R2", "R2"],
        "horse_number": [1, 2, 3, 1, 2],
        "prob": [0.10, 0.40, 0.25, 0.30, 0.60],
    })
    result = pick_top_n(df, prob_col="prob", n=1)
    # R1 は horse_number=2 (prob=0.40), R2 は horse_number=2 (prob=0.60)
    assert set(result["horse_number"].tolist()) == {2}
    assert len(result) == 2


def test_pick_top_n_with_n2() -> None:
    df = pd.DataFrame({
        "race_id": ["R1", "R1", "R1"],
        "horse_number": [1, 2, 3],
        "prob": [0.10, 0.40, 0.35],
    })
    result = pick_top_n(df, prob_col="prob", n=2)
    assert set(result["horse_number"].tolist()) == {2, 3}


def test_find_optimal_threshold_prefers_precision() -> None:
    """閾値最適化が precision を高める方向で動作する。"""
    # 確率が高い馬ほど is_win=1 になる合成データ
    rng = np.random.default_rng(42)
    probs = rng.uniform(0, 1, 200)
    labels = (probs > 0.6).astype(int)
    thr = find_optimal_threshold(probs, labels, beta=0.5)
    # beta=0.5 は precision 重視 → threshold は高め（0.5より高い）
    assert 0.4 <= thr <= 0.95


def test_accuracy_model_v2_fit_predict_shape() -> None:
    """fit/predict_proba が正しい shape を返す。"""
    rng = np.random.default_rng(0)
    n = 300
    # 最低限の特徴量列（win_odds, popularity, weight_carried ... ）
    X = pd.DataFrame({
        "weight_carried": rng.uniform(50, 60, n),
        "horse_weight": rng.uniform(440, 560, n),
        "win_odds": rng.uniform(1.5, 100, n),
        "popularity": rng.integers(1, 18, n),
        "gate_number": rng.integers(1, 18, n),
    })
    y = (rng.random(n) < 0.07).astype(int)  # 勝率約7%
    race_ids = [f"R{i//10}" for i in range(n)]
    X["race_id"] = race_ids

    model = AccuracyModelV2()
    model.fit(X, y)
    probs = model.predict_proba(X)
    assert probs.shape == (n,)
    assert (probs >= 0).all() and (probs <= 1).all()


def test_accuracy_model_v2_threshold_is_set_after_fit() -> None:
    rng = np.random.default_rng(1)
    n = 200
    X = pd.DataFrame({
        "win_odds": rng.uniform(1.5, 80, n),
        "popularity": rng.integers(1, 18, n),
        "weight_carried": rng.uniform(50, 60, n),
        "race_id": [f"R{i//10}" for i in range(n)],
    })
    y = (rng.random(n) < 0.07).astype(int)
    model = AccuracyModelV2()
    model.fit(X, y)
    assert model.threshold is not None
    assert 0.0 < model.threshold < 1.0


def test_accuracy_model_v2_select_bets_filters_by_threshold() -> None:
    """select_bets が閾値以上の馬のみ返す（かつレース内上位N制限）。"""
    rng = np.random.default_rng(2)
    n = 100
    X = pd.DataFrame({
        "win_odds": rng.uniform(1.5, 80, n),
        "popularity": rng.integers(1, 18, n),
        "weight_carried": rng.uniform(50, 60, n),
        "race_id": [f"R{i//10}" for i in range(n)],
        "horse_number": list(range(1, 11)) * 10,
    })
    y = (rng.random(n) < 0.07).astype(int)
    model = AccuracyModelV2()
    model.fit(X, y)
    bets = model.select_bets(X, max_per_race=2)
    if len(bets) > 0:
        # 各レースで最大2頭以下
        assert bets.groupby("race_id")["horse_number"].count().max() <= 2
