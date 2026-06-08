"""Accuracy Model — 勝率特化 LightGBM Classifier（タスク2.1 / 独立モジュール）。

本命モデル(HonmeiModel)とは独立した、的中率（Accuracy）最適化に特化した
勝率予測専用 Classifier。並行セッションのコードと競合しないよう
``src/ml/models.py`` は **import のみ・非改変** で再利用する。

リークフリーの大原則（``logic_map.md`` §0 厳守）:
  - 特徴量は ``FeatureBuilder.build_race_features_for_simulate()`` 由来の ``FEATURE_COLS``
    のみ。rank / finish_time / margin など「結果」は特徴量に一切含めない。
  - 馬成績系（win_rate 等）は対象馬の過去レースからのみ集計済み（当該レース除外・
    日付フィルタ）。目的変数 ``is_winner``(rank==1) はラベルであり特徴量には混入しない。
  - 時系列分割（train_from / train_until）で未来データのリークを防ぐ。

使い方::
    from src.ml.accuracy_model import AccuracyModel
    m = AccuracyModel()
    m.train(conn, train_from=2025, train_until=2025)
    proba = m.predict_proba(feature_df)   # 各馬の1着確率
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.metrics import log_loss, roc_auc_score

# models.py は import のみ（非改変）で再利用 → 並行セッションと非競合。
from src.ml.models import FEATURE_COLS, _build_train_df, _safe_feature_matrix

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models"
_MIN_TRAIN_RACES = 30


class AccuracyModel:
    """勝率特化 LightGBM 二値分類器（目的変数 is_winner）。"""

    FILENAME = "accuracy_model"

    def __init__(self) -> None:
        self._clf: LGBMClassifier | None = None
        self._feature_cols: list[str] = list(FEATURE_COLS)

    @property
    def is_trained(self) -> bool:
        return self._clf is not None

    def train(
        self,
        conn: sqlite3.Connection,
        train_from: int | None = None,
        train_until: int | None = None,
        df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """``is_winner`` を目的変数に勝率モデルを訓練する。

        Args:
            conn:        DB 接続。
            train_from:  学習開始年（例: 2025 → 2025年以降のクリーンデータ）。
            train_until: 学習最終年。
            df:          事前生成済みの学習 DataFrame（省略時は内部生成）。

        Returns:
            学習メトリクス（n_races / n_samples / train_logloss / train_auc / base_rate）。
        """
        if df is None:
            df = _build_train_df(conn, train_until=train_until, train_from=train_from)
        else:
            df = df.copy()

        n_races = int(df["race_id"].nunique()) if "race_id" in df.columns else 0
        if n_races < _MIN_TRAIN_RACES:
            logger.warning(
                "Accuracy Model: 学習レース数 %d < %d のため訓練をスキップ",
                n_races,
                _MIN_TRAIN_RACES,
            )
            return {"n_races": n_races, "n_samples": len(df), "trained": False}

        X = _safe_feature_matrix(df)
        y = df["is_winner"].astype(int).to_numpy()

        # 不均衡（1着は 1/出走頭数）に対応。is_unbalance で内部調整。
        self._clf = LGBMClassifier(
            objective="binary",
            n_estimators=400,
            learning_rate=0.03,
            num_leaves=31,
            min_child_samples=40,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            reg_lambda=1.0,
            is_unbalance=True,
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )
        self._clf.fit(X, y)

        proba = self._clf.predict_proba(X)[:, 1]
        metrics = {
            "n_races": n_races,
            "n_samples": int(len(df)),
            "base_rate": float(y.mean()),
            "train_logloss": float(log_loss(y, proba, labels=[0, 1])),
            "train_auc": float(roc_auc_score(y, proba))
            if y.min() != y.max()
            else float("nan"),
            "trained": True,
        }
        logger.info(
            "Accuracy Model 訓練完了: %d レース / %d サンプル / train AUC %.4f / logloss %.4f",
            metrics["n_races"],
            metrics["n_samples"],
            metrics["train_auc"],
            metrics["train_logloss"],
        )
        return metrics

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        """各馬の1着確率（0〜1）を返す。

        Args:
            df: ``FEATURE_COLS`` を含む（不足列は -1 補填される）特徴量 DataFrame。

        Returns:
            行ごとの1着確率 ndarray。未訓練時はオッズ非依存の一様確率を返す。
        """
        if self._clf is None:
            n = len(df)
            return np.full(n, 1.0 / max(n, 1), dtype=float)
        X = _safe_feature_matrix(df)
        return self._clf.predict_proba(X)[:, 1]

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """予測1着確率を ``pd.Series``（df の index に整合）で返す。"""
        return pd.Series(self.predict_proba(df), index=df.index, name="win_proba")

    def save(self, path: Path | None = None) -> Path:
        if self._clf is None:
            raise RuntimeError("未訓練のモデルは保存できません")
        save_path = path or (_MODEL_DIR / f"{self.FILENAME}.pkl")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            pickle.dump({"clf": self._clf, "feature_cols": self._feature_cols}, f)
        logger.info("Accuracy Model 保存: %s", save_path)
        return save_path

    def load(self, path: Path | None = None) -> "AccuracyModel":
        load_path = path or (_MODEL_DIR / f"{self.FILENAME}.pkl")
        with open(load_path, "rb") as f:
            obj = pickle.load(f)
        self._clf = obj["clf"]
        self._feature_cols = obj.get("feature_cols", list(FEATURE_COLS))
        return self
