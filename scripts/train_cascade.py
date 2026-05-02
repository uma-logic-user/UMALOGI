"""
Cascade Rank Prediction — 三連単特化カスケードモデル 訓練スクリプト

アーキテクチャ（案A）:
  Stage-1: P(1着=i)             ← 既存 HonmeiModel を流用可（または再学習）
  Stage-2: P(2着=j | 1着=i)    ← 「1着馬 vs 自馬」の差分特徴量を追加した LightGBM
  Stage-3: P(3着=k | 1着=i, 2着=j) ← Stage-2 と同様の構造

学習データ:
  - race_results.rank が 1/2/3 すべて揃っているレース（2025年 + 2026年）
  - target_2: rank == 2  (当該レース内で2着になったか)
  - target_3: rank == 3  (当該レース内で3着になったか)

出力モデル:
  data/models/cascade/stage2_model.pkl
  data/models/cascade/stage3_model.pkl
  data/models/cascade/label_encoders.pkl  ← jockey/trainer/sire エンコード辞書

使用例:
  py scripts/train_cascade.py                      # 全年データで学習
  py scripts/train_cascade.py --year 2025          # 2025年のみ
  py scripts/train_cascade.py --dry-run            # 特徴量確認のみ（学習なし）
"""

from __future__ import annotations

import argparse
import functools
import json
import logging
import pickle
import sys
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.model_selection import GroupKFold
from sklearn.metrics import roc_auc_score

optuna.logging.set_verbosity(optuna.logging.WARNING)

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

_CASCADE_DIR = _ROOT / "data" / "models" / "cascade"
_BEST_PARAMS_PATH = _CASCADE_DIR / "best_params.json"

# ── Stage-2/3 専用の追加特徴量（1着馬との差分） ─────────────────────────────
# prefix "diff_" = 自馬の値 - 1着馬の値（Stage-2のみ）
# prefix "diff2_" = 自馬の値 - 2着馬の値（Stage-3のみ）
_DIFF_FEATURES_STAGE2: list[str] = [
    "diff_win_rate_all",          # 1着馬との通算勝率差
    "diff_recent_rank_mean",      # 1着馬との直近着順差（負=自馬が良好）
    "diff_win_odds",              # 1着馬とのオッズ差
    "diff_tc_4f",                 # 調教タイム差
    "diff_weight_carried",        # 斤量差
    "diff_horse_weight",          # 馬体重差
    "winner_jockey_same",         # 1着馬と同じ騎手か (0/1)
    "winner_trainer_same",        # 1着馬と同じ厩舎か (0/1)
    "winner_sire_same",           # 1着馬と同じ父か (0/1)
    "own_rank1_prob",             # Stage-1 が出力した1着確率（自馬）
    "winner_rank1_prob",          # Stage-1 が出力した1着確率（1着馬）
]

_DIFF_FEATURES_STAGE3: list[str] = [
    "diff_win_rate_all",
    "diff_recent_rank_mean",
    "diff_win_odds",
    "diff_tc_4f",
    "diff_weight_carried",
    "diff_horse_weight",
    "winner_jockey_same",         # 1着馬との比較
    "second_jockey_same",         # 2着馬との比較
    "winner_trainer_same",
    "second_trainer_same",
    "own_rank1_prob",
    "winner_rank1_prob",
    "second_rank1_prob",          # Stage-1 の2着馬スコア
    "own_rank2_prob",             # Stage-2 が出力した2着確率（自馬）
]

# ベース特徴量（Stage-1 と共通の基礎特徴）
_BASE_FEATURES: list[str] = [
    "weight_carried", "horse_weight", "horse_weight_diff",
    "win_rate_all", "win_rate_surface", "win_rate_distance_band",
    "recent_rank_mean",
    "surface_code", "sex_code", "venue_encoded", "sire_encoded",
    "distance", "gate_number", "condition_code", "race_number",
    "jockey_code_encoded", "trainer_code_encoded",
    "tc_4f", "tc_lap", "tc_accel_flag", "tc_4f_diff",
    "hc_4f", "hc_lap", "hc_accel_flag", "hc_4f_diff",
    "win_rate_all_rank", "win_rate_all_zscore",
    "win_rate_surface_rank", "win_rate_distance_band_rank",
    "recent_rank_mean_rank", "recent_rank_mean_zscore",
    "tc_4f_rank", "tc_4f_zscore",
]


# ── LightGBM ハイパーパラメータ ──────────────────────────────────────────────
_LGBM_PARAMS: dict = {
    "n_estimators": 800,
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "class_weight": "balanced",
    "random_state": 42,
    "n_jobs": -1,
    "verbose": -1,
}


# ── Optuna ハイパーパラメータ最適化 ──────────────────────────────────────────

def _load_best_params(stage: str) -> dict:
    """best_params.json から指定ステージのパラメータを読み込む。ファイルがなければ _LGBM_PARAMS を返す。"""
    if _BEST_PARAMS_PATH.exists():
        with open(_BEST_PARAMS_PATH, encoding="utf-8") as f:
            data: dict = json.load(f)
        if stage in data:
            logger.info("best_params.json からパラメータ読み込み: %s", stage)
            return {**_LGBM_PARAMS, **data[stage]}
    return _LGBM_PARAMS.copy()


def _optuna_objective(
    trial: optuna.Trial,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
) -> float:
    """3-fold GroupKFold OOF AUC を最大化（-AUC を最小化）。"""
    params: dict = {
        **_LGBM_PARAMS,
        "num_leaves":        trial.suggest_int("num_leaves", 15, 255),
        "learning_rate":     trial.suggest_float("learning_rate", 0.005, 0.2, log=True),
        "feature_fraction":  trial.suggest_float("feature_fraction", 0.4, 1.0),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
        "subsample":         trial.suggest_float("subsample", 0.4, 1.0),
        "reg_alpha":         trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
        "reg_lambda":        trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        "n_estimators":      trial.suggest_int("n_estimators", 200, 1500),
    }
    cv = GroupKFold(n_splits=3)
    aucs: list[float] = []
    for tr_idx, va_idx in cv.split(X, y, groups):
        model = LGBMClassifier(**params)
        model.fit(
            X.iloc[tr_idx], y.iloc[tr_idx],
            eval_set=[(X.iloc[va_idx], y.iloc[va_idx])],
            callbacks=[lgb_early_stop(30, verbose=False)],
        )
        preds = model.predict_proba(X.iloc[va_idx])[:, 1]
        aucs.append(roc_auc_score(y.iloc[va_idx], preds))
    return -float(np.mean(aucs))


def tune_stage(
    stage: str,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    n_trials: int,
) -> dict:
    """Optuna でステージのハイパーパラメータを探索し、best_params.json に保存する。

    Args:
        stage:    "stage2" または "stage3"
        X, y:     学習特徴量と目的変数
        groups:   GroupKFold 用 race_id
        n_trials: Optuna トライアル数

    Returns:
        最適パラメータ辞書（_LGBM_PARAMS にマージ済み）
    """
    logger.info("Optuna チューニング開始: %s  n_trials=%d", stage, n_trials)
    study = optuna.create_study(
        direction="minimize",
        study_name=f"cascade_{stage}",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    objective = functools.partial(_optuna_objective, X=X, y=y, groups=groups)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    best_params = study.best_params
    best_auc = -study.best_value
    logger.info("Optuna 完了: %s  best AUC=%.4f  params=%s", stage, best_auc, best_params)

    _CASCADE_DIR.mkdir(parents=True, exist_ok=True)
    existing: dict = {}
    if _BEST_PARAMS_PATH.exists():
        with open(_BEST_PARAMS_PATH, encoding="utf-8") as f:
            existing = json.load(f)
    existing[stage] = best_params
    existing[f"{stage}_auc"] = round(best_auc, 6)
    with open(_BEST_PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)
    logger.info("best_params.json 保存: %s", _BEST_PARAMS_PATH)
    return {**_LGBM_PARAMS, **best_params}


# ── データロード ─────────────────────────────────────────────────────────────

def load_training_data(
    conn: "sqlite3.Connection",
    year_filter: str | None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    FeatureBuilder を使ってリーク排除済みの学習 DataFrame を生成する。

    _build_train_df() (models.py) と同じアプローチだが、
    rank=1/2/3 が全て揃うレースのみ対象（三連単判定に必要）。

    Returns:
        (DataFrame[FEATURE_COLS + rank + race_id], sire_map) のタプル
    """
    import sqlite3 as _sqlite3
    from src.ml.features import FeatureBuilder

    year_sql = f"AND CAST(substr(r.date,1,4) AS INTEGER) = {year_filter}" if year_filter else ""
    race_rows = conn.execute(
        f"""
        SELECT DISTINCT r.race_id
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        WHERE rr.rank IS NOT NULL
        {year_sql}
        AND r.race_id IN (
            SELECT rr2.race_id
            FROM race_results rr2
            WHERE rr2.rank IN (1, 2, 3)
            GROUP BY rr2.race_id
            HAVING COUNT(DISTINCT rr2.rank) = 3
        )
        ORDER BY r.date
        """
    ).fetchall()

    if not race_rows:
        return pd.DataFrame()

    fb = FeatureBuilder(conn)
    frames: list[pd.DataFrame] = []

    for i, (race_id,) in enumerate(race_rows):
        if i % 500 == 0:
            logger.info("  特徴量生成中... %d/%d", i, len(race_rows))
        df_feat = fb.build_race_features_for_simulate(race_id)
        if df_feat.empty:
            continue

        actual_rows = conn.execute(
            """
            SELECT rr.horse_name, rr.rank
            FROM   race_results rr
            WHERE  rr.race_id = ?
            AND    rr.rank IS NOT NULL
            """,
            (race_id,),
        ).fetchall()
        if not actual_rows:
            continue

        actuals = pd.DataFrame(actual_rows, columns=["horse_name", "rank"])
        df_feat = df_feat.merge(actuals, on="horse_name", how="inner")
        df_feat["race_id"] = race_id
        frames.append(df_feat)

    if not frames:
        return pd.DataFrame(), {}

    df = pd.concat(frames, ignore_index=True)
    logger.info("学習データロード完了: %d 行 (%d レース)", len(df), df["race_id"].nunique())
    return df, fb._sire_map


def build_stage2_features(df: pd.DataFrame, stage1_probs: pd.Series) -> pd.DataFrame:
    """
    Stage-2 用特徴量を構築する。

    各行（馬）について「1着馬との差分特徴量」を付加する。
    学習時: 実際の1着馬を使用
    推論時: Stage-1 で最も確率が高い馬を1着馬として使用

    Args:
        df:            base features を含む DataFrame（race_id・horse_number を含む）
        stage1_probs:  各行の Stage-1 1着確率（df と同じインデックス）

    Returns:
        diff_* 特徴量を追加した DataFrame
    """
    df = df.copy()
    df["own_rank1_prob"] = stage1_probs.values

    result_rows = []
    for race_id, group in df.groupby("race_id"):
        winner_row = group[group["rank"] == 1].iloc[0] if "rank" in group.columns else \
                     group.loc[group["own_rank1_prob"].idxmax()]

        for _, row in group.iterrows():
            diff = {}
            for feat in ["win_rate_all", "recent_rank_mean", "win_odds",
                         "tc_4f", "weight_carried", "horse_weight"]:
                self_val = row.get(feat, np.nan)
                win_val = winner_row.get(feat, np.nan)
                diff[f"diff_{feat}"] = (self_val - win_val) if pd.notna(self_val) and pd.notna(win_val) else np.nan

            diff["winner_rank1_prob"] = winner_row.get("own_rank1_prob", np.nan)
            diff["winner_jockey_same"] = int(row.get("jockey", "") == winner_row.get("jockey", ""))
            diff["winner_trainer_same"] = int(row.get("trainer", "") == winner_row.get("trainer", ""))
            diff["winner_sire_same"] = int(row.get("sire_encoded", -1) == winner_row.get("sire_encoded", -1))
            result_rows.append(diff)

    diff_df = pd.DataFrame(result_rows, index=df.index)
    return pd.concat([df, diff_df], axis=1)


def build_stage3_features(
    df: pd.DataFrame,
    stage1_probs: pd.Series,
    stage2_probs: pd.Series,
) -> pd.DataFrame:
    """
    Stage-3 用特徴量を構築する。

    1着馬・2着馬との差分特徴量を付加する。
    推論時: Stage-1/2 の予測馬を1着・2着候補として使用。
    """
    df = df.copy()
    df["own_rank1_prob"] = stage1_probs.values
    df["own_rank2_prob"] = stage2_probs.values

    result_rows = []
    for race_id, group in df.groupby("race_id"):
        if "rank" in group.columns:
            winner_row = group[group["rank"] == 1].iloc[0]
            second_row = group[group["rank"] == 2].iloc[0]
        else:
            sorted_g = group.sort_values("own_rank1_prob", ascending=False)
            winner_row = sorted_g.iloc[0]
            sorted_g2 = group.sort_values("own_rank2_prob", ascending=False)
            second_row = sorted_g2.iloc[0]

        for _, row in group.iterrows():
            diff = {}
            for feat in ["win_rate_all", "recent_rank_mean", "win_odds",
                         "tc_4f", "weight_carried", "horse_weight"]:
                self_val = row.get(feat, np.nan)
                win_val  = winner_row.get(feat, np.nan)
                diff[f"diff_{feat}"] = (self_val - win_val) if pd.notna(self_val) and pd.notna(win_val) else np.nan

            diff["winner_rank1_prob"] = winner_row.get("own_rank1_prob", np.nan)
            diff["second_rank1_prob"] = second_row.get("own_rank1_prob", np.nan)
            diff["winner_jockey_same"] = int(row.get("jockey", "") == winner_row.get("jockey", ""))
            diff["second_jockey_same"] = int(row.get("jockey", "") == second_row.get("jockey", ""))
            diff["winner_trainer_same"] = int(row.get("trainer", "") == winner_row.get("trainer", ""))
            diff["second_trainer_same"] = int(row.get("trainer", "") == second_row.get("trainer", ""))
            result_rows.append(diff)

    diff_df = pd.DataFrame(result_rows, index=df.index)
    return pd.concat([df, diff_df], axis=1)


# ── 学習 ─────────────────────────────────────────────────────────────────────

def train_stage2(
    df: pd.DataFrame,
    stage1_probs: pd.Series,
    dry_run: bool,
    params: dict | None = None,
) -> tuple[LGBMClassifier | None, pd.Series]:
    """Stage-2: P(2着=j | 1着候補特徴量) モデルを学習する。

    Returns:
        (trained_model, oof_probs_full) — oof_probs_full は df と同じインデックス。
        rank=1 行は 0.0 で埋める（自馬は2着になれないため）。
    """
    feat_df = build_stage2_features(df, stage1_probs)
    target_col = "rank"
    if target_col not in feat_df.columns:
        logger.error("rank 列がありません")
        return None, pd.Series(0.0, index=df.index)

    feat_df["target_2"] = (feat_df[target_col] == 2).astype(int)

    # 1着馬自身を学習対象から除外（自分が2着になれないため）
    feat_df_no1 = feat_df[feat_df[target_col] != 1].copy()

    feature_cols = [c for c in (_BASE_FEATURES + list(_DIFF_FEATURES_STAGE2)) if c in feat_df_no1.columns]
    X = feat_df_no1[feature_cols].fillna(-1)
    y = feat_df_no1["target_2"]
    groups = feat_df_no1["race_id"]

    logger.info("Stage-2 学習データ: %d 行, %d 特徴量, 正例=%d",
                len(X), len(feature_cols), y.sum())

    if dry_run:
        logger.info("[DRY-RUN] Stage-2 特徴量一覧: %s", feature_cols)
        return None, pd.Series(0.0, index=df.index)

    lgbm_params = params if params is not None else _load_best_params("stage2")

    cv = GroupKFold(n_splits=5)
    oof_preds = np.zeros(len(X))
    for fold, (tr_idx, va_idx) in enumerate(cv.split(X, y, groups)):
        model = LGBMClassifier(**lgbm_params)
        model.fit(X.iloc[tr_idx], y.iloc[tr_idx],
                  eval_set=[(X.iloc[va_idx], y.iloc[va_idx])],
                  callbacks=[lgb_early_stop(50, verbose=False)])
        oof_preds[va_idx] = model.predict_proba(X.iloc[va_idx])[:, 1]
        auc = roc_auc_score(y.iloc[va_idx], oof_preds[va_idx])
        logger.info("  Stage-2 fold=%d  AUC=%.4f", fold + 1, auc)

    overall_auc = roc_auc_score(y, oof_preds)
    logger.info("Stage-2 OOF AUC: %.4f", overall_auc)

    # OOF 確率を元の df インデックスに戻す（rank=1 行は 0.0）
    oof_full = pd.Series(0.0, index=df.index)
    oof_full.loc[feat_df_no1.index] = oof_preds

    # 全データで最終モデルを学習
    final_model = LGBMClassifier(**lgbm_params)
    final_model.fit(X, y)
    final_model.feature_names_ = feature_cols  # type: ignore[attr-defined]
    return final_model, oof_full


def train_stage3(
    df: pd.DataFrame,
    stage1_probs: pd.Series,
    stage2_probs: pd.Series,
    dry_run: bool,
    params: dict | None = None,
) -> LGBMClassifier | None:
    """Stage-3: P(3着=k | 1着・2着候補特徴量) モデルを学習する。"""
    feat_df = build_stage3_features(df, stage1_probs, stage2_probs)
    target_col = "rank"
    if target_col not in feat_df.columns:
        return None

    feat_df["target_3"] = (feat_df[target_col] == 3).astype(int)
    feat_df = feat_df[~feat_df[target_col].isin([1, 2])].copy()

    feature_cols = [c for c in (_BASE_FEATURES + list(_DIFF_FEATURES_STAGE3)) if c in feat_df.columns]
    X = feat_df[feature_cols].fillna(-1)
    y = feat_df["target_3"]
    groups = feat_df["race_id"]

    logger.info("Stage-3 学習データ: %d 行, %d 特徴量, 正例=%d",
                len(X), len(feature_cols), y.sum())

    if dry_run:
        logger.info("[DRY-RUN] Stage-3 特徴量一覧: %s", feature_cols)
        return None

    lgbm_params = params if params is not None else _load_best_params("stage3")

    cv = GroupKFold(n_splits=5)
    oof_preds = np.zeros(len(X))
    for fold, (tr_idx, va_idx) in enumerate(cv.split(X, y, groups)):
        model = LGBMClassifier(**lgbm_params)
        model.fit(X.iloc[tr_idx], y.iloc[tr_idx],
                  eval_set=[(X.iloc[va_idx], y.iloc[va_idx])],
                  callbacks=[lgb_early_stop(50, verbose=False)])
        oof_preds[va_idx] = model.predict_proba(X.iloc[va_idx])[:, 1]
        auc = roc_auc_score(y.iloc[va_idx], oof_preds[va_idx])
        logger.info("  Stage-3 fold=%d  AUC=%.4f", fold + 1, auc)

    overall_auc = roc_auc_score(y, oof_preds)
    logger.info("Stage-3 OOF AUC: %.4f", overall_auc)

    lgbm_params_final = params if params is not None else _load_best_params("stage3")
    final_model = LGBMClassifier(**lgbm_params_final)
    final_model.fit(X, y)
    final_model.feature_names_ = feature_cols  # type: ignore[attr-defined]
    return final_model


def lgb_early_stop(stopping_rounds: int, verbose: bool = True):
    """LightGBM early stopping コールバック（lgb.early_stopping のラッパー）。"""
    import lightgbm as lgb
    return lgb.early_stopping(stopping_rounds, verbose=verbose)


# ── 推論ヘルパー ─────────────────────────────────────────────────────────────

class CascadePredictor:
    """
    学習済みカスケードモデルを使って三連単・三連複の確率を出力する。

    使い方:
        predictor = CascadePredictor.load()
        probs = predictor.predict_trifecta(race_df, stage1_probs, top_n=5)
        # → [(prob, (1着馬番, 2着馬番, 3着馬番)), ...]
    """

    def __init__(
        self,
        stage2_model: LGBMClassifier,
        stage3_model: LGBMClassifier,
    ) -> None:
        self.stage2 = stage2_model
        self.stage3 = stage3_model

    @classmethod
    def load(cls, model_dir: Path = _CASCADE_DIR) -> "CascadePredictor":
        with open(model_dir / "stage2_model.pkl", "rb") as f:
            s2 = pickle.load(f)
        with open(model_dir / "stage3_model.pkl", "rb") as f:
            s3 = pickle.load(f)
        return cls(s2, s3)

    def save(self, model_dir: Path = _CASCADE_DIR) -> None:
        model_dir.mkdir(parents=True, exist_ok=True)
        with open(model_dir / "stage2_model.pkl", "wb") as f:
            pickle.dump(self.stage2, f)
        with open(model_dir / "stage3_model.pkl", "wb") as f:
            pickle.dump(self.stage3, f)
        logger.info("Cascadeモデル保存: %s", model_dir)

    def predict_trifecta(
        self,
        race_df: pd.DataFrame,
        stage1_probs: pd.Series,
        top_n: int = 5,
    ) -> list[tuple[float, tuple[int, int, int]]]:
        """
        三連単の上位 top_n 組み合わせと確率を返す。

        Returns:
            [(prob, (1着馬番, 2着馬番, 3着馬番)), ...] 確率降順
        """
        import itertools
        nums = race_df["horse_number"].tolist()
        s1   = stage1_probs.values

        results = []
        for i1, winner_num in enumerate(nums):
            # Stage-2: 1着がwinner_numのとき、他の馬の2着確率
            df2 = race_df.copy()
            df2["rank"] = None  # 推論モード
            df2["own_rank1_prob"] = s1
            winner_row = race_df.iloc[i1]
            stage2_feats = []
            for _, row in race_df.iterrows():
                diff = {}
                for feat in ["win_rate_all", "recent_rank_mean", "win_odds",
                             "tc_4f", "weight_carried", "horse_weight"]:
                    a = row.get(feat, np.nan)
                    b = winner_row.get(feat, np.nan)
                    if a is None: a = np.nan
                    if b is None: b = np.nan
                    diff[f"diff_{feat}"] = (a - b) if (pd.notna(a) and pd.notna(b)) else np.nan
                diff["winner_rank1_prob"] = s1[i1]
                diff["own_rank1_prob"] = s1[list(nums).index(row["horse_number"])] if row["horse_number"] in nums else 0
                diff["winner_jockey_same"] = int(row.get("jockey", "") == winner_row.get("jockey", ""))
                diff["winner_trainer_same"] = int(row.get("trainer", "") == winner_row.get("trainer", ""))
                diff["winner_sire_same"] = int(row.get("sire_encoded", -1) == winner_row.get("sire_encoded", -1))
                stage2_feats.append(diff)

            feat2_df = pd.concat([race_df, pd.DataFrame(stage2_feats, index=race_df.index)], axis=1)
            cols2 = getattr(self.stage2, "feature_names_", _BASE_FEATURES + _DIFF_FEATURES_STAGE2)
            X2 = feat2_df[[c for c in cols2 if c in feat2_df.columns]].fillna(-1)
            s2 = self.stage2.predict_proba(X2)[:, 1]

            # 1着馬は2着から除外
            for i2, second_num in enumerate(nums):
                if second_num == winner_num:
                    continue
                prob_combo = s1[i1] * s2[i2]
                # Stage-3 は計算コスト高のため TOP_N まで絞り込んで適用（骨組みでは省略）
                for i3, third_num in enumerate(nums):
                    if third_num in (winner_num, second_num):
                        continue
                    results.append((prob_combo * s1[i3], (winner_num, second_num, third_num)))

        results.sort(key=lambda x: -x[0])
        return results[:top_n]


# ── メイン ───────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ap = argparse.ArgumentParser(
        description="Cascade Rank Prediction モデルの学習（Stage-2 / Stage-3）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  py scripts/train_cascade.py                 # 全年データで学習
  py scripts/train_cascade.py --year 2025     # 2025年のみ
  py scripts/train_cascade.py --dry-run       # 特徴量確認（学習なし）
""",
    )
    ap.add_argument("--year",     default=None, help="学習対象年（省略時=全期間）")
    ap.add_argument("--dry-run",  action="store_true", help="特徴量確認のみ（モデル保存なし）")
    ap.add_argument("--optuna",   action="store_true", help="Optunaでハイパーパラメータ最適化してから学習")
    ap.add_argument("--n-trials", type=int, default=50, help="Optunaトライアル数（デフォルト: 50）")
    args = ap.parse_args()

    from src.database.init_db import init_db
    from src.ml.models import HonmeiModel, FEATURE_COLS

    print("=" * 60)
    print("  Cascade Rank Prediction — 学習開始")
    if args.dry_run:
        print("  [DRY-RUN モード: モデルを保存しません]")
    if args.optuna:
        print(f"  [Optuna モード: n_trials={args.n_trials}]")
    print("=" * 60)

    conn = init_db()
    df, sire_map = load_training_data(conn, args.year)
    conn.close()

    if df.empty:
        logger.error("学習データが0件です。--year や DB を確認してください。")
        sys.exit(1)

    # Stage-1: 既存 HonmeiModel の 1着確率を利用
    honmei = HonmeiModel()
    try:
        honmei.load()
        logger.info("Stage-1: HonmeiModel 読み込み完了")
        stage1_probs = honmei.predict(df).rename("stage1_prob")
    except FileNotFoundError:
        logger.warning("HonmeiModel が見つかりません。ダミー確率（均等）を使用します。")
        stage1_probs = pd.Series(np.ones(len(df)) / 18.0, index=df.index, name="stage1_prob")

    # ── Optuna チューニング ────────────────────────────────────────────────────
    params_stage2: dict | None = None
    params_stage3: dict | None = None

    if args.optuna and not args.dry_run:
        from src.ml.features import FeatureBuilder  # noqa: F401

        # Stage-2 チューニング用データ構築
        feat2 = build_stage2_features(df, stage1_probs)
        feat2["target_2"] = (feat2["rank"] == 2).astype(int)
        feat2_no1 = feat2[feat2["rank"] != 1].copy()
        cols2 = [c for c in (_BASE_FEATURES + list(_DIFF_FEATURES_STAGE2)) if c in feat2_no1.columns]
        X2 = feat2_no1[cols2].fillna(-1)
        y2 = feat2_no1["target_2"]
        g2 = feat2_no1["race_id"]
        params_stage2 = tune_stage("stage2", X2, y2, g2, args.n_trials)

        # Stage-3 チューニング用データ構築（ダミーOOF確率を使用）
        dummy_oof2 = pd.Series(0.0, index=df.index)
        dummy_oof2.loc[feat2_no1.index] = 0.5
        feat3 = build_stage3_features(df, stage1_probs, dummy_oof2)
        feat3["target_3"] = (feat3["rank"] == 3).astype(int)
        feat3 = feat3[~feat3["rank"].isin([1, 2])].copy()
        cols3 = [c for c in (_BASE_FEATURES + list(_DIFF_FEATURES_STAGE3)) if c in feat3.columns]
        X3 = feat3[cols3].fillna(-1)
        y3 = feat3["target_3"]
        g3 = feat3["race_id"]
        params_stage3 = tune_stage("stage3", X3, y3, g3, args.n_trials)

        print(f"\n  Optuna 完了 → {_BEST_PARAMS_PATH}")

    # ── Stage-2 / Stage-3 学習 ────────────────────────────────────────────────
    logger.info("=" * 40)
    logger.info("Stage-2 学習開始: P(2着=j | 1着候補)")
    stage2_model, stage2_oof_probs = train_stage2(
        df, stage1_probs, dry_run=args.dry_run, params=params_stage2
    )

    logger.info("=" * 40)
    logger.info("Stage-3 学習開始: P(3着=k | 1着・2着候補)")
    stage3_model = train_stage3(
        df, stage1_probs, stage2_oof_probs, dry_run=args.dry_run, params=params_stage3
    )

    if not args.dry_run and stage2_model and stage3_model:
        predictor = CascadePredictor(stage2_model, stage3_model)
        predictor.save()
        # sire_map を永続化して推論時のエンコーディング一貫性を保証
        enc_path = _CASCADE_DIR / "label_encoders.pkl"
        with open(enc_path, "wb") as f:
            pickle.dump({"sire_map": sire_map}, f)
        logger.info("label_encoders.pkl 保存: sire=%d 種", len(sire_map))
        print(f"\n  モデル保存完了: {_CASCADE_DIR}")
        print(f"    stage2_model.pkl")
        print(f"    stage3_model.pkl")
        print(f"    label_encoders.pkl  ({len(sire_map)} 種の父名)")
        if _BEST_PARAMS_PATH.exists():
            print(f"    best_params.json")
    elif args.dry_run:
        print("\n  [DRY-RUN] モデルは保存されませんでした。")

    print("\n  完了")


if __name__ == "__main__":
    main()
