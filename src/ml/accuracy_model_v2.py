"""Accuracy Model v2 — 的中率特化 LightGBM Classifier。

既存EVモデル（honmei / 卍）との差別化:
  - 目的変数: is_win（rank == 1）  ← honmei と同じ
  - 特徴量:   FEATURE_COLS(69) + リークフリー新特徴量（前走詳細・血統TE）
  - 最適化:   AUC ではなく **Precision-Recall 最適 cut-off**（的中率特化）
  - 買い目:   EV 計算なし。「各レースの勝率上位 N 頭」のみ購入

リークフリーの担保 (logic_map.md 大原則):
  - 対象レースの rank / finish_time / margin / last_3f は使用しない
  - 前走系特徴量は現レース日より厳密に過去のみ参照（prerun.py）
  - 血統 TE は cutoff（学習期間開始より前）でのみ fit（pedigree_te.py）

⚠️ 本番 EV パイプライン（prediction.py）には直接結線しない。
   独立モジュールとして OOS 検証完了後にブリッジ経由で接続する。
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 定義: beta=0.5 → precision を recall の 2 倍重視（的中率特化）
_DEFAULT_BETA: float = 0.5
_DEFAULT_MAX_PER_RACE: int = 2


def pick_top_n(df: pd.DataFrame, prob_col: str, n: int) -> pd.DataFrame:
    """各レース内で勝率上位 n 頭を選出して返す。

    Args:
        df: "race_id" 列と prob_col を含む DataFrame。
        prob_col: 勝率予測列名。
        n: レースあたりの最大選出頭数。

    Returns:
        各レースの上位 n 頭だけを含む DataFrame（インデックスはリセット）。
    """
    return (
        df.sort_values(prob_col, ascending=False)
        .groupby("race_id", group_keys=False)
        .head(n)
        .reset_index(drop=True)
    )


def find_optimal_threshold(
    probs: np.ndarray,
    labels: np.ndarray,
    beta: float = _DEFAULT_BETA,
) -> float:
    """Fbeta スコアを最大化する確率閾値を返す。

    beta < 1.0 で precision（的中率）を重視。
    beta=0.5 は precision を recall の 2 倍重視。

    Args:
        probs: モデル出力の勝率確率（0-1）。
        labels: 実際の is_win ラベル（0 or 1）。
        beta: F スコアの beta 値。小さいほど precision 重視。

    Returns:
        Fbeta 最大となる閾値（float）。
    """
    thresholds = np.linspace(0.01, 0.99, 200)
    best_thr, best_f = 0.5, -1.0
    for thr in thresholds:
        pred = (probs >= thr).astype(int)
        tp = int(((pred == 1) & (labels == 1)).sum())
        fp = int(((pred == 1) & (labels == 0)).sum())
        fn = int(((pred == 0) & (labels == 1)).sum())
        if tp == 0:
            continue
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        denom = (beta**2 * precision) + recall
        if denom == 0:
            continue
        f = (1 + beta**2) * precision * recall / denom
        if f > best_f:
            best_f, best_thr = f, float(thr)
    return best_thr


class AccuracyModelV2:
    """的中率特化 LightGBM Classifier。

    使い方::

        model = AccuracyModelV2()
        model.fit(train_X, train_y)            # train_X に race_id 列を含む
        probs = model.predict_proba(test_X)
        bets  = model.select_bets(test_X, max_per_race=2)
    """

    def __init__(
        self,
        n_estimators: int = 400,
        max_depth: int = 5,
        learning_rate: float = 0.04,
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        class_weight: str = "balanced",
        beta: float = _DEFAULT_BETA,
        random_state: int = 42,
    ) -> None:
        self._params: dict[str, Any] = dict(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            class_weight=class_weight,
            verbose=-1,
            random_state=random_state,
        )
        self._beta = beta
        self._model: Any = None
        self._feature_cols: list[str] = []
        self.threshold: float | None = None

    # ── 内部 ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _drop_meta(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """race_id / horse_number 等のメタ列と文字列列を除いた特徴量 DataFrame を返す。"""
        skip = {"race_id", "horse_number", "is_win", "rank", "win_odds"}
        # object(文字列)列も除外（LightGBMはint/float/boolのみ受け付ける）
        feat_cols = [
            c
            for c in df.columns
            if c not in skip and df[c].dtype.kind in ("i", "f", "b", "u")
        ]
        return df[feat_cols].copy(), feat_cols

    # ── 公開 API ──────────────────────────────────────────────────────────────

    def fit(
        self,
        X: pd.DataFrame,
        y: np.ndarray | pd.Series,
        val_X: pd.DataFrame | None = None,
        val_y: np.ndarray | pd.Series | None = None,
        val_fraction: float = 0.2,
    ) -> "AccuracyModelV2":
        """学習し、Fbeta 最適閾値を設定する。

        ⚠️ 閾値最適化は必ず「学習に使っていないデータ」で行う。
        訓練データで閾値を決めると過学習した閾値になり、OOS での的中率が
        実際より大幅に誇張される（訓練AUC≒1.0, 的中率100%の症状）。

        Args:
            X: 特徴量 DataFrame（"race_id" 列を含んでよい）。
            y: is_win ラベル（0/1）。
            val_X: 検証セット。省略時は X を時系列分割して末尾 val_fraction を使用。
            val_y: 検証ラベル。
            val_fraction: val_X 省略時に X の末尾から切り出す比率（0.2 = 20%）。
        """
        import lightgbm as lgb

        y_arr = np.asarray(y, dtype=int)

        if val_X is None:
            # 時系列順に split（random shuffle は禁止・時系列リーク防止）
            n_val = max(1, int(len(X) * val_fraction))
            n_train = len(X) - n_val
            X_tr, X_val = X.iloc[:n_train], X.iloc[n_train:]
            y_tr, y_val = y_arr[:n_train], y_arr[n_train:]
        else:
            X_tr, X_val = X, val_X
            y_tr, y_val = y_arr, np.asarray(val_y, dtype=int)

        Xf_tr, feat_cols = self._drop_meta(X_tr)
        self._feature_cols = feat_cols

        self._model = lgb.LGBMClassifier(**self._params)
        self._model.fit(Xf_tr.fillna(0.0), y_tr)

        # 閾値最適化は必ず検証データで（訓練データ禁止）
        probs_val = self.predict_proba(X_val)
        self.threshold = find_optimal_threshold(probs_val, y_val, beta=self._beta)
        logger.info(
            "AccuracyModelV2 fit: train=%d val=%d features=%d threshold=%.4f",
            len(X_tr),
            len(X_val),
            len(feat_cols),
            self.threshold,
        )
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """各馬の is_win 確率（0-1）を返す。"""
        if self._model is None:
            raise RuntimeError("fit() を先に呼んでください")
        Xf, _ = self._drop_meta(X)
        # 訓練時にない列を 0 埋め、余分な列は除外
        for c in self._feature_cols:
            if c not in Xf.columns:
                Xf[c] = 0.0
        Xf = Xf[self._feature_cols].fillna(0.0)
        return self._model.predict_proba(Xf)[:, 1]

    def select_bets(
        self,
        X: pd.DataFrame,
        max_per_race: int = _DEFAULT_MAX_PER_RACE,
    ) -> pd.DataFrame:
        """閾値以上かつレース内上位 max_per_race 頭の買い目を返す。

        Returns:
            race_id / horse_number / prob を含む DataFrame（X の全列を保持）。
            インデックスは X の元インデックスを保持するため、
            ``X.loc[result.index]`` で元データの行を正しく取得できる。
        """
        if self.threshold is None:
            raise RuntimeError("fit() を先に呼んでください")
        probs = self.predict_proba(X)
        # 元のインデックスを保持したまま prob 列を追加
        work = X.copy()
        work["_prob"] = probs
        # レース内上位 N 頭を元インデックスを保ちながら選出
        top_n = (
            work.sort_values("_prob", ascending=False)
            .groupby("race_id", group_keys=False)
            .head(max_per_race)
        )
        result = top_n[top_n["_prob"] >= self.threshold].copy()
        result = result.rename(columns={"_prob": "prob"})
        return result

    def feature_importance(self) -> pd.DataFrame:
        """gain 重要度を降順で返す。"""
        if self._model is None:
            raise RuntimeError("fit() を先に呼んでください")
        imp = self._model.feature_importances_
        return (
            pd.DataFrame({"feature": self._feature_cols, "importance": imp})
            .sort_values("importance", ascending=False)
            .reset_index(drop=True)
        )

    def save(self, path: str) -> None:
        import pickle

        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str) -> "AccuracyModelV2":
        import pickle

        with open(path, "rb") as f:
            return pickle.load(f)


# ── DB から学習データを組み立てるファクトリ ──────────────────────────────────


def build_training_data(
    conn: sqlite3.Connection,
    train_lo: str = "2024-01-01",
    train_hi: str = "2025-10-01",
    race_cap: int = 2000,
) -> tuple[pd.DataFrame, pd.Series]:
    """DB からリークフリーな学習データ（is_win ターゲット）を組み立てる。

    本番 FeatureBuilder(69列) ＋ prerun（前走詳細）を結合。
    血統TE は cutoff(train_hi)で fit した SireEncoder を使用。

    Args:
        conn: umalogi.db 接続。
        train_lo: 学習開始日（含む）。
        train_hi: 学習終了日（含まない・OOS の cutoff）。
        race_cap: 最大レース数（均等間引き）。

    Returns:
        (X: DataFrame, y: Series)
    """
    from src.features.prerun import build_prerun_features
    from src.features.pedigree_te import SireEncoder, build_pedigree_features
    from src.ml.features import FeatureBuilder

    # レース一覧取得
    rows = conn.execute(
        """
        SELECT DISTINCT r.race_id FROM races r
        WHERE r.date >= ? AND r.date < ?
          AND EXISTS (
            SELECT 1 FROM race_results rr
            WHERE rr.race_id=r.race_id AND rr.rank>0 AND rr.win_odds>0
          )
        ORDER BY r.date
        """,
        (train_lo, train_hi),
    ).fetchall()
    ids = [r[0] for r in rows]
    if len(ids) > race_cap:
        step = len(ids) / race_cap
        ids = [ids[int(i * step)] for i in range(race_cap)]

    # 血統エンコーダは train_hi より前でのみ fit（リークフリー）
    enc = SireEncoder().fit(conn, cutoff_date=train_hi)

    fb = FeatureBuilder(conn)
    frames: list[pd.DataFrame] = []

    for rid in ids:
        try:
            base = fb.build_race_features(rid)
        except Exception:
            continue
        if base.empty:
            continue

        # ターゲット（is_win）を race_results から安全に取得
        labels = conn.execute(
            "SELECT horse_number, rank FROM race_results WHERE race_id=? AND rank>0",
            (rid,),
        ).fetchall()
        if not labels:
            continue
        label_map = {hn: 1 if rk == 1 else 0 for hn, rk in labels}
        if 1 not in label_map.values():
            continue  # 着順なし

        base["is_win"] = base["horse_number"].map(label_map)
        base = base.dropna(subset=["is_win"])
        if base.empty:
            continue

        # prerun 特徴量を結合（リークフリー: 現レース日より過去のみ）
        pre = build_prerun_features(conn, rid)
        if not pre.empty:
            base = base.merge(pre, on="horse_number", how="left")

        # 血統 TE を結合
        ped = build_pedigree_features(conn, rid, enc)
        if not ped.empty:
            base = base.merge(ped, on="horse_number", how="left")

        base["race_id"] = rid
        frames.append(base)

    if not frames:
        return pd.DataFrame(), pd.Series(dtype=int)

    df = pd.concat(frames, ignore_index=True)
    y = df.pop("is_win").astype(int)
    return df, y
