"""
増分学習 (Incremental Learning) モジュール

LightGBM の init_model パラメータを使い、既存モデルに新しいレースデータを
追加学習する「ウォームスタート」方式を実装する。

ハイブリッド戦略:
  ─ 増分更新 (online update)  : レース終了直後。直近 N レースのみで追加 boosting。
                                  速度優先。毎レース後自動実行。
  ─ 定期全件再学習 (full retrain): 毎週月曜 or 累積新規 >= 閾値。
                                  全データで一から訓練。精度優先。

Usage:
    trainer = IncrementalTrainer()

    # レース直後の増分更新
    trainer.incremental_update(conn, new_race_ids=["202401010101"])

    # 週次全件再学習
    trainer.full_retrain(conn)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold

from .models import (
    FEATURE_COLS,
    HonmeiModel,
    ManjiModel,
    _build_train_df,
    _MIN_TRAIN_RACES,
)

logger = logging.getLogger(__name__)

_MODEL_DIR   = Path(__file__).resolve().parents[2] / "data" / "models"
_HISTORY_DIR = _MODEL_DIR / "history"

# 増分学習の追加 boosting ラウンド数
_INCREMENTAL_ROUNDS = 50
# 全件再学習の最大ラウンド数
_FULL_RETRAIN_ROUNDS = 500
# 全件再学習トリガーとなる新規レース累積数
_FULL_RETRAIN_THRESHOLD = 100


class ModelVersion:
    """モデルバージョン情報。"""

    def __init__(
        self,
        model_type: str,
        n_races: int,
        n_samples: int,
        trained_at: str,
        cv_auc: float | None = None,
    ) -> None:
        self.model_type = model_type
        self.n_races    = n_races
        self.n_samples  = n_samples
        self.trained_at = trained_at
        self.cv_auc     = cv_auc

    def __str__(self) -> str:
        auc_str = f" AUC={self.cv_auc:.4f}" if self.cv_auc else ""
        return (
            f"{self.model_type} v{self.trained_at}"
            f"  races={self.n_races} samples={self.n_samples}{auc_str}"
        )


def _cross_validate_auc(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    params: dict[str, Any],
    n_splits: int = 5,
) -> float:
    """GroupKFold で CV AUC を計算して返す（本命モデル用）。"""
    gkf = GroupKFold(n_splits=n_splits)
    aucs: list[float] = []
    for train_idx, val_idx in gkf.split(X, y, groups=groups):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
        ds_tr  = lgb.Dataset(X_tr, label=y_tr)
        ds_val = lgb.Dataset(X_val, label=y_val, reference=ds_tr)
        booster = lgb.train(
            params,
            ds_tr,
            num_boost_round=200,
            valid_sets=[ds_val],
            callbacks=[lgb.early_stopping(20, verbose=False), lgb.log_evaluation(-1)],
        )
        pred = booster.predict(X_val)
        from sklearn.metrics import roc_auc_score
        try:
            aucs.append(roc_auc_score(y_val, pred))
        except ValueError:
            pass
    return float(np.mean(aucs)) if aucs else 0.0


class IncrementalTrainer:
    """
    ハイブリッド増分学習トレーナー。

    モデルファイルは data/models/ に保存され、履歴は data/models/history/ に
    タイムスタンプ付きで残す（ロールバック可能）。
    """

    def __init__(self) -> None:
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        _HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        self._new_race_count = 0  # 前回全件再学習からの累積新規レース数

    # ── 増分更新 ─────────────────────────────────────────────────

    def incremental_update(
        self,
        conn: sqlite3.Connection,
        new_race_ids: list[str],
        *,
        force_full: bool = False,
    ) -> dict[str, ModelVersion]:
        """
        新規レースデータで既存モデルを追加学習する。

        Args:
            conn:         DB コネクション
            new_race_ids: 追加学習するレース ID リスト
            force_full:   True の場合、閾値未満でも全件再学習を強制

        Returns:
            {"honmei": ModelVersion, "manji": ModelVersion}
        """
        self._new_race_count += len(new_race_ids)

        # 閾値超過または強制フラグで全件再学習
        if force_full or self._new_race_count >= _FULL_RETRAIN_THRESHOLD:
            logger.info(
                "全件再学習トリガー: 累積新規レース=%d (閾値=%d)",
                self._new_race_count, _FULL_RETRAIN_THRESHOLD,
            )
            result = self.full_retrain(conn)
            self._new_race_count = 0
            return result

        # 増分データのみで追加 boosting
        df_new = self._build_partial_df(conn, new_race_ids)
        if df_new.empty:
            logger.warning("増分データなし: %s", new_race_ids)
            return {}

        results: dict[str, ModelVersion] = {}
        for cls, name in [(HonmeiModel, "honmei"), (ManjiModel, "manji")]:
            version = self._incremental_fit(cls, df_new, name)
            if version:
                results[name] = version
        return results

    def _incremental_fit(
        self,
        model_cls: type[HonmeiModel] | type[ManjiModel],
        df_new: pd.DataFrame,
        name: str,
    ) -> ModelVersion | None:
        model = model_cls()
        try:
            model.load()
        except FileNotFoundError:
            logger.info("%s: 既存モデルなし → スキップ（先に full_retrain を実行してください）", name)
            return None

        is_honmei = name == "honmei"
        target_col = "is_winner" if is_honmei else "ev_target"
        X_new = df_new[FEATURE_COLS].fillna(-1)
        y_new = df_new[target_col] if target_col in df_new.columns else pd.Series(dtype=float)

        if y_new.empty or len(y_new) < 5:
            logger.warning("%s: 増分データが少なすぎます (%d 件)", name, len(y_new))
            return None

        # 既存モデルを init_model として追加 boosting
        init_booster = model._model.booster_
        params = {
            "objective":   "binary" if is_honmei else "regression",
            "metric":      "auc"    if is_honmei else "rmse",
            "learning_rate": 0.05,
            "num_leaves":  31,
            "verbose":     -1,
        }
        ds = lgb.Dataset(X_new, label=y_new)

        try:
            new_booster = lgb.train(
                params,
                ds,
                num_boost_round=_INCREMENTAL_ROUNDS,
                init_model=init_booster,
                callbacks=[lgb.log_evaluation(-1)],
            )
        except Exception as e:
            logger.error("%s 増分学習失敗: %s", name, e)
            return None

        # モデル置き換えと保存
        model._model.set_params(n_estimators=new_booster.num_trees())
        model._model._Booster = new_booster
        model._trained = True
        self._archive_and_save(model, name)

        version = ModelVersion(
            model_type=name,
            n_races=df_new["race_id"].nunique(),
            n_samples=len(df_new),
            trained_at=datetime.now().strftime("%Y%m%d_%H%M%S"),
        )
        logger.info("[増分] %s 更新完了: %s", name, version)
        return version

    # ── 全件再学習 ───────────────────────────────────────────────

    def full_retrain(
        self,
        conn: sqlite3.Connection,
        *,
        validate: bool = True,
    ) -> dict[str, ModelVersion]:
        """
        全データで本命・卍モデルを一から再学習する。

        Args:
            validate: True の場合 GroupKFold CV で AUC を計算する（本命のみ）
        """
        df_all = _build_train_df(conn)
        if df_all.empty:
            logger.warning("学習データなし")
            return {}

        n_races = df_all["race_id"].nunique()
        if n_races < _MIN_TRAIN_RACES:
            logger.warning("学習レース数が少なすぎます: %d < %d", n_races, _MIN_TRAIN_RACES)
            return {}

        results: dict[str, ModelVersion] = {}

        # ── 本命モデル ────────────────────────────────────────────
        honmei = HonmeiModel()
        X = df_all[FEATURE_COLS].fillna(-1)
        y = df_all["is_winner"]
        groups = df_all["race_id"]

        cv_auc: float | None = None
        if validate:
            params = {
                "objective": "binary", "metric": "auc",
                "num_leaves": 31, "learning_rate": 0.05, "verbose": -1,
            }
            try:
                cv_auc = _cross_validate_auc(X, y, groups, params)
                logger.info("[全件再学習] 本命 CV AUC=%.4f", cv_auc)
            except Exception as e:
                logger.warning("CV AUC 計算失敗: %s", e)

        honmei.train(conn)
        if honmei.is_trained:
            self._archive_and_save(honmei, "honmei")
            results["honmei"] = ModelVersion(
                model_type="honmei",
                n_races=n_races,
                n_samples=len(df_all),
                trained_at=datetime.now().strftime("%Y%m%d_%H%M%S"),
                cv_auc=cv_auc,
            )

        # ── 卍モデル ──────────────────────────────────────────────
        manji = ManjiModel()
        manji.train(conn)
        if manji.is_trained:
            self._archive_and_save(manji, "manji")
            results["manji"] = ModelVersion(
                model_type="manji",
                n_races=n_races,
                n_samples=len(df_all),
                trained_at=datetime.now().strftime("%Y%m%d_%H%M%S"),
            )

        logger.info(
            "[全件再学習] 完了: %d レース / 本命=%s 卍=%s",
            n_races,
            results.get("honmei"),
            results.get("manji"),
        )
        return results

    # ── ユーティリティ ────────────────────────────────────────────

    def _build_partial_df(
        self, conn: sqlite3.Connection, race_ids: list[str]
    ) -> pd.DataFrame:
        """指定 race_id のみの学習データを構築する。"""
        if not race_ids:
            return pd.DataFrame()
        placeholders = ",".join("?" * len(race_ids))
        df_all = _build_train_df(conn)
        if df_all.empty:
            return df_all
        return df_all[df_all["race_id"].isin(race_ids)].copy()

    def _archive_and_save(
        self,
        model: HonmeiModel | ManjiModel,
        name: str,
    ) -> None:
        """現在のモデルファイルを history にコピーしてから上書き保存する。"""
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        src  = _MODEL_DIR / f"{name}_model.pkl"
        if src.exists():
            import shutil
            dst = _HISTORY_DIR / f"{name}_model_{ts}.pkl"
            shutil.copy2(str(src), str(dst))
            # 直近10件を超える old バックアップを削除
            old = sorted(_HISTORY_DIR.glob(f"{name}_model_*.pkl"))
            for f in old[:-10]:
                f.unlink()
        model.save()
