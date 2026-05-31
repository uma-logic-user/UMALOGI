"""
Alpha-Place Model — 複勝特化型 AI（ROI 110% 突破専用機）
==========================================================

ALPHA モデルから完全分離した複勝専用モデル。

改良点:
  1. 目的変数: is_place (rank<=3 バイナリ分類) に特化
  2. Optuna による LightGBM 超パラメータ最適化
  3. Isotonic Regression による確率キャリブレーション
     「AIが80%と言えば実際も80%」の信頼性を確保
  4. EV = P_model(place) / P_market(place) × 払戻率
     (win_odds × P_win ではなく正しい複勝EV式)
  5. 会場別動的EV閾値（過去データで最適化した勝ちパターン）

モデル保存先: data/models/alpha_place/alpha_place_model.pkl
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier

logger = logging.getLogger(__name__)

# ── 設定定数 ─────────────────────────────────────────────────────────

_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models" / "alpha_place"
_MODEL_PATH = _MODEL_DIR / "alpha_place_model.pkl"

# JRA 複勝 テイクアウト率 22.5% → 払戻率 77.5%
FUKUSHO_PAYOUT_RATE: float = 0.775

# Kelly Criterion の fraction（複勝は低配当なので Kelly × 0.20）
KELLY_FRACTION: float = 0.20
MIN_BET: int = 100
MAX_BET: int = 3000

# デフォルト EV 閾値（全会場共通。会場別は venue_thresholds で上書き）
# EV = P_model/P_market × 0.775 の損益分岐 = 1.0 → 1.3以上で選択的に購入
DEFAULT_EV_THRESHOLD: float = 1.30


# ── Harville 複勝確率 ────────────────────────────────────────────────


def harville_place_probs(win_probs: np.ndarray) -> np.ndarray:
    """
    各馬の P(top 3 finish) を Harville 公式で計算する。

    P(h 1着) + P(h 2着) + P(h 3着) を正規化された勝率から導出。

    Args:
        win_probs: 正規化された勝率ベクトル（sum=1, len>=3 推奨）

    Returns:
        place_probs: 各馬の top-3 入着確率ベクトル
    """
    n = len(win_probs)
    p = np.asarray(win_probs, dtype=float)
    p = p / max(p.sum(), 1e-8)

    if n < 3:
        return np.minimum(p * 3.0, 1.0)  # 頭数不足: 全員入着に近い

    one_minus_p = np.maximum(1.0 - p, 1e-8)

    # P(1着)
    result = p.copy()

    # P(2着): P(h 2着) = sum_{j≠h} p_j × p_h/(1-p_j)
    #                  = p_h × sum_{j≠h} p_j/(1-p_j)
    contrib = p / one_minus_p  # p_j / (1-p_j) for all j
    sum_contrib = contrib.sum()
    # sum_{j≠h} = total - contrib[h]
    result += p * (sum_contrib - contrib)

    # P(3着): P(h 3着) = sum_{j≠h} sum_{k≠h,j} p_j × p_k/(1-p_j) × p_h/(1-p_j-p_k)
    for h in range(n):
        p3_h = 0.0
        for j in range(n):
            if j == h:
                continue
            for k in range(n):
                if k == h or k == j:
                    continue
                p_jk = p[j] * p[k] / one_minus_p[j]
                denom = max(1.0 - p[j] - p[k], 1e-8)
                p3_h += p_jk * p[h] / denom
        result[h] += p3_h

    return np.minimum(result, 1.0)


# ── 特徴量定義 ───────────────────────────────────────────────────────

BASE_FEATURES: list[str] = [
    "win_odds",
    "popularity",
    "distance",
    "gate_number",
    "weight_carried",
    "horse_weight",
    "horse_weight_diff",
    "race_number",
    "surface_code",
    "sex_code",
    "venue_encoded",
    "condition_code",
    "jockey_encoded",
    "trainer_encoded",
]

PLACE_EXTRA_FEATURES: list[str] = [
    "log_win_odds",
    "inv_odds",
    "odds_popularity_ratio",
    "field_size",
    "mean_field_odds",
    "odds_vs_field",
    "market_prob",  # 単勝市場確率
    "log_market_prob",
    # 複勝特化特徴量
    "place_market_prob",  # Harville P(top3) — 複勝市場確率
    "log_place_market_prob",
    "win_to_place_ratio",  # 単勝市場確率 / 複勝市場確率 (高=穴馬的)
    # ハイブリッド特徴量
    "nb_win_odds",
    "log_nb_win_odds",
    "nb_market_prob",
    "odds_discrepancy_ratio",
    "nb_vs_field",
]

ALL_PLACE_FEATURES: list[str] = BASE_FEATURES + PLACE_EXTRA_FEATURES


# ── 結果コンテナ ─────────────────────────────────────────────────────


@dataclass
class PlaceBacktestResult:
    year: str | int
    total_investment: int
    total_payout: float
    profit: float
    roi: float
    num_bets: int
    num_hits: int
    hit_rate: float
    max_drawdown: float
    bet_type: str = "複勝"
    ev_threshold: float = DEFAULT_EV_THRESHOLD
    notes: list[str] = field(default_factory=list)


# ── メインモデルクラス ────────────────────────────────────────────────


class AlphaPlaceModel:
    """
    複勝特化型 AI モデル。

    アーキテクチャ:
      LGBMClassifier → Isotonic calibration → EV = P_model/P_market × 0.775
    """

    def __init__(self) -> None:
        self._model: Optional[LGBMClassifier] = None
        self._calibrator = None  # sklearn calibrator
        self._label_encoders: dict = {}
        self._best_params: dict = {}
        self._venue_thresholds: dict[str, float] = {}
        self._is_trained: bool = False

    # ── データ準備 ────────────────────────────────────────────────────

    def load_training_data(
        self,
        conn: sqlite3.Connection,
        years: list[int] | None = None,
        research_db_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        race_results + race_payouts から複勝学習データを生成する。

        is_place = 1: 複勝払戻対象の馬（top-3 確定済みレースのみ）
        """
        logger.info("Alpha-Place 学習データ生成: years=%s", years)

        params: list = []
        cte_filters: list[str] = []

        if years:
            year_strs = [str(y) for y in years]
            placeholders = ",".join("?" * len(years))
            cte_filters.append(f"strftime('%Y', date) IN ({placeholders})")
            params.extend(year_strs)

        cte_date_where = " AND ".join(cte_filters) if cte_filters else "1=1"

        sql = f"""
        WITH confirmed_races AS (
            SELECT DISTINCT race_id
            FROM race_payouts
            WHERE bet_type = '複勝'
              AND race_id IN (
                  SELECT race_id FROM races WHERE {cte_date_where}
              )
        )
        SELECT
            rr.race_id,
            rr.horse_number,
            rr.win_odds,
            rr.popularity,
            rr.rank,
            rr.gate_number,
            rr.weight_carried,
            rr.horse_weight,
            rr.horse_weight_diff,
            rr.jockey,
            rr.trainer,
            rr.sex_age,
            r.date,
            r.venue,
            r.race_number,
            r.distance,
            r.surface,
            r.condition,
            r.weather,
            CASE WHEN rp.combination IS NOT NULL THEN 1 ELSE 0 END AS is_place,
            rp.payout AS actual_payout
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        JOIN confirmed_races cr ON rr.race_id = cr.race_id
        LEFT JOIN race_payouts rp
            ON rp.race_id = rr.race_id
            AND rp.bet_type = '複勝'
            AND CAST(rp.combination AS INTEGER) = rr.horse_number
        WHERE rr.horse_number IS NOT NULL
          AND rr.horse_number > 0
        ORDER BY r.date, rr.race_id, rr.horse_number
        """

        df = pd.read_sql_query(sql, conn, params=params)
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        if research_db_path is not None and Path(research_db_path).exists():
            df = self._merge_research_odds(df, Path(research_db_path))
        else:
            df = df[df["win_odds"].notna() & (df["win_odds"] > 0)]

        logger.info(
            "ロード完了: %d行 is_place=%d (%.1f%%)",
            len(df),
            int(df["is_place"].sum()),
            float(df["is_place"].mean()) * 100,
        )
        return df

    @staticmethod
    def _merge_research_odds(df: pd.DataFrame, rdb: Path) -> pd.DataFrame:
        import sqlite3 as _sql

        rconn = _sql.connect(str(rdb))
        odds_df = pd.read_sql_query(
            "SELECT race_id, horse_number, win_odds AS nb_win_odds FROM horse_odds",
            rconn,
        )
        rconn.close()

        merged = df.merge(odds_df, on=["race_id", "horse_number"], how="left")
        merged["win_odds"] = pd.to_numeric(merged["win_odds"], errors="coerce")
        merged["nb_win_odds"] = pd.to_numeric(merged["nb_win_odds"], errors="coerce")

        mask_jv = merged["win_odds"].isna() | (merged["win_odds"] <= 0)
        merged.loc[mask_jv, "win_odds"] = merged.loc[mask_jv, "nb_win_odds"]
        mask_nb = merged["nb_win_odds"].isna() | (merged["nb_win_odds"] <= 0)
        merged.loc[mask_nb, "nb_win_odds"] = merged.loc[mask_nb, "win_odds"]
        merged = merged[merged["win_odds"].notna() & (merged["win_odds"] > 0)]
        return merged

    # ── 特徴量エンジニアリング ────────────────────────────────────────

    def _add_place_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # カテゴリエンコーディング
        surface_map = {"芝": 0, "ダート": 1, "障害": 2}
        if "surface" in df.columns:
            df["surface_code"] = df["surface"].map(surface_map).fillna(-1).astype(int)

        sex_map = {"牡": 0, "牝": 1, "セ": 2}
        if "sex_age" in df.columns:
            df["sex_code"] = df["sex_age"].str[0].map(sex_map).fillna(-1).astype(int)

        venue_map = {
            "札幌": 0,
            "函館": 1,
            "福島": 2,
            "新潟": 3,
            "東京": 4,
            "中山": 5,
            "中京": 6,
            "京都": 7,
            "阪神": 8,
            "小倉": 9,
        }
        if "venue" in df.columns:
            df["venue_encoded"] = df["venue"].map(venue_map).fillna(-1).astype(int)

        cond_map = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
        if "condition" in df.columns:
            df["condition_code"] = df["condition"].map(cond_map).fillna(-1).astype(int)

        # 騎手・調教師ラベルエンコード（fit=Trueなら new; fit=Falseなら既存LE使用）
        for src, out in [("jockey", "jockey_encoded"), ("trainer", "trainer_encoded")]:
            if src in df.columns and out not in df.columns:
                df[out] = -1

        # 基本オッズ特徴量
        df["win_odds"] = (
            pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        )
        df["log_win_odds"] = np.log(df["win_odds"])
        df["inv_odds"] = 1.0 / df["win_odds"]

        pop = (
            pd.to_numeric(df.get("popularity"), errors="coerce").fillna(9).clip(lower=1)
        )
        df["odds_popularity_ratio"] = df["win_odds"] / pop

        grp = df.groupby("race_id", group_keys=False)
        df["field_size"] = grp["horse_number"].transform("count")
        df["mean_field_odds"] = grp["win_odds"].transform("mean")
        df["odds_vs_field"] = df["win_odds"] / df["mean_field_odds"].clip(lower=1.0)

        inv_sum = grp["inv_odds"].transform("sum").clip(lower=1e-8)
        df["market_prob"] = df["inv_odds"] / inv_sum
        df["log_market_prob"] = np.log(df["market_prob"].clip(lower=1e-8))

        # ── 複勝特化: Harville P(top3) ──────────────────────────────
        place_probs_arr = np.empty(len(df), dtype=float)
        df_reset = df.reset_index(drop=True)
        for _, g in df_reset.groupby("race_id"):
            mp = g["market_prob"].values
            hprobs = harville_place_probs(mp)
            place_probs_arr[g.index] = hprobs
        df["place_market_prob"] = place_probs_arr
        df["place_market_prob"] = df["place_market_prob"].clip(lower=1e-4, upper=0.9999)
        df["log_place_market_prob"] = np.log(df["place_market_prob"])
        df["win_to_place_ratio"] = df["market_prob"] / df["place_market_prob"].clip(
            lower=1e-4
        )

        # ── ハイブリッド特徴量 ───────────────────────────────────────
        if "nb_win_odds" in df.columns:
            df["nb_win_odds"] = (
                pd.to_numeric(df["nb_win_odds"], errors="coerce")
                .fillna(df["win_odds"])
                .clip(lower=1.01)
            )
        else:
            df["nb_win_odds"] = df["win_odds"]

        df["log_nb_win_odds"] = np.log(df["nb_win_odds"])
        df["nb_market_prob"] = 1.0 / df["nb_win_odds"]

        nb_inv_sum = grp["nb_market_prob"].transform("sum").clip(lower=1e-8)
        df["nb_market_prob"] = df["nb_market_prob"] / nb_inv_sum

        df["odds_discrepancy_ratio"] = (
            df["nb_win_odds"] / df["win_odds"].clip(lower=1.01)
        ).clip(0.1, 10.0)
        nb_mean = grp["nb_win_odds"].transform("mean").clip(lower=1.0)
        df["nb_vs_field"] = df["nb_win_odds"] / nb_mean

        return df

    def _encode_categoricals(self, df: pd.DataFrame, fit: bool = True) -> pd.DataFrame:
        from sklearn.preprocessing import LabelEncoder

        df = df.copy()
        for src, out in [("jockey", "jockey_encoded"), ("trainer", "trainer_encoded")]:
            if src not in df.columns:
                df[out] = -1
                continue
            if fit:
                le = LabelEncoder()
                df[out] = le.fit_transform(df[src].astype(str))
                self._label_encoders[out] = le
            else:
                le = self._label_encoders.get(out)
                if le:
                    known = set(le.classes_)
                    df[out] = (
                        df[src]
                        .astype(str)
                        .apply(
                            lambda x: int(le.transform([x])[0]) if x in known else -1
                        )
                    )
                else:
                    df[out] = -1
        return df

    def prepare_features(
        self, df: pd.DataFrame, fit: bool = True
    ) -> tuple[pd.DataFrame, pd.Series | None]:
        df = self._add_place_features(df)
        df = self._encode_categoricals(df, fit=fit)

        for col in ALL_PLACE_FEATURES:
            if col not in df.columns:
                df[col] = -1.0

        X = df[ALL_PLACE_FEATURES].copy()
        X = X.apply(pd.to_numeric, errors="coerce").fillna(-1.0)
        y = df["is_place"].astype(int) if "is_place" in df.columns else None
        return X, y

    # ── 学習 ──────────────────────────────────────────────────────────

    def train(
        self,
        df: pd.DataFrame,
        n_optuna_trials: int = 30,
        calibrate: bool = True,
    ) -> dict[str, float]:
        """
        Optuna で超パラメータ最適化 → LightGBM 学習 → 確率キャリブレーション。

        Args:
            df:              load_training_data() の出力
            n_optuna_trials: Optuna トライアル数
            calibrate:       True なら Isotonic calibration を適用

        Returns:
            metrics dict
        """
        import optuna

        optuna.logging.set_verbosity(optuna.logging.WARNING)

        X, y = self.prepare_features(df, fit=True)
        assert y is not None

        pos_rate = float(y.mean())
        scale_pos_weight = (1.0 - pos_rate) / pos_rate if pos_rate > 0 else 1.0

        logger.info(
            "Alpha-Place Optuna 最適化開始: %d試行 %d行", n_optuna_trials, len(X)
        )
        print(
            f"  [Optuna] {n_optuna_trials}試行 開始 ({len(X):,}行, pos={pos_rate:.1%})",
            flush=True,
        )

        def objective(trial: "optuna.Trial") -> float:
            from sklearn.model_selection import TimeSeriesSplit
            from sklearn.metrics import roc_auc_score

            params: dict[str, Any] = {
                "objective": "binary",
                "n_estimators": trial.suggest_int("n_estimators", 500, 2000),
                "learning_rate": trial.suggest_float(
                    "learning_rate", 0.01, 0.08, log=True
                ),
                "num_leaves": trial.suggest_int("num_leaves", 31, 255),
                "max_depth": trial.suggest_int("max_depth", 4, 10),
                "min_child_samples": trial.suggest_int("min_child_samples", 10, 60),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 1.0, log=True),
                "scale_pos_weight": scale_pos_weight,
                "random_state": 42,
                "n_jobs": -1,
                "verbose": -1,
            }

            tscv = TimeSeriesSplit(n_splits=3)
            auc_scores: list[float] = []
            for train_idx, val_idx in tscv.split(X):
                X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
                if y_val.nunique() < 2:
                    continue
                m = LGBMClassifier(**params)
                m.fit(
                    X_tr,
                    y_tr,
                    eval_set=[(X_val, y_val)],
                    callbacks=[_lgb_early_stopping(50)],
                )
                preds = m.predict_proba(X_val)[:, 1]
                auc_scores.append(roc_auc_score(y_val, preds))

            return float(np.mean(auc_scores)) if auc_scores else 0.0

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=n_optuna_trials, show_progress_bar=False)
        self._best_params = study.best_params
        best_auc = study.best_value
        print(
            f"  [Optuna] 完了 → best AUC={best_auc:.4f}  "
            f"num_leaves={self._best_params.get('num_leaves')} "
            f"lr={self._best_params.get('learning_rate', 0):.4f}",
            flush=True,
        )

        # ── ベストパラメータで最終学習 ──────────────────────────────
        final_params: dict[str, Any] = {
            "objective": "binary",
            "scale_pos_weight": scale_pos_weight,
            "random_state": 42,
            "n_jobs": -1,
            "verbose": -1,
            **self._best_params,
        }
        # 70% 学習 / 15% キャリブレーション / 15% 検証（時系列順）
        n = len(X)
        train_end = int(n * 0.70)
        calib_end = int(n * 0.85)
        X_tr = X.iloc[:train_end]
        X_cal = X.iloc[train_end:calib_end]
        X_val = X.iloc[calib_end:]
        y_tr = y.iloc[:train_end]
        y_cal = y.iloc[train_end:calib_end]
        y_val = y.iloc[calib_end:]

        self._model = LGBMClassifier(**final_params)
        self._model.fit(
            X_tr,
            y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[_lgb_early_stopping(50)],
        )

        from sklearn.metrics import roc_auc_score

        raw_probs = self._model.predict_proba(X_val)[:, 1]
        val_auc = float(roc_auc_score(y_val, raw_probs))

        # ── キャリブレーション（X_cal のみ使用 — 時系列リーク排除）─────
        if calibrate:
            from sklearn.isotonic import IsotonicRegression

            # 既学習モデルの生確率を X_cal で Isotonic 変換（cv="prefit" 相当）
            cal_raw = self._model.predict_proba(X_cal)[:, 1]
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(cal_raw, y_cal)
            self._calibrator = iso

            val_raw = self._model.predict_proba(X_val)[:, 1]
            calib_probs = iso.predict(val_raw)
            calib_auc = float(roc_auc_score(y_val, calib_probs))
            print(
                f"  [校正] Isotonic calibration完了 val AUC: {val_auc:.4f} → {calib_auc:.4f}",
                flush=True,
            )
        else:
            self._calibrator = None
            calib_auc = val_auc

        self._is_trained = True

        return {
            "optuna_auc": best_auc,
            "val_auc": val_auc,
            "calib_auc": calib_auc,
            "n_train": len(X_tr),
            "n_calib": len(X_cal),
            "n_val": len(X_val),
            "pos_rate": pos_rate,
        }

    # ── 推論 ──────────────────────────────────────────────────────────

    def predict_place_prob(self, df: pd.DataFrame) -> pd.Series:
        """キャリブレーション済みの P(top-3 finish) を返す。"""
        if not self._is_trained or self._model is None:
            raise RuntimeError("モデル未学習。train() を先に呼んでください。")
        X, _ = self.prepare_features(df, fit=False)
        raw_probs = self._model.predict_proba(X)[:, 1]
        if self._calibrator is not None:
            # IsotonicRegression の predict() で補正
            probs = self._calibrator.predict(raw_probs)
        else:
            probs = raw_probs
        return pd.Series(probs, index=df.index)

    def compute_ev(self, df: pd.DataFrame, place_probs: pd.Series) -> pd.Series:
        """
        EV = P_model(place) / P_market(place) × FUKUSHO_PAYOUT_RATE

        P_market(place) = Harville P(top3) from win_odds
        """
        df = df.copy()
        df["_model_prob"] = place_probs.values
        df["win_odds"] = (
            pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        )

        market_probs_arr = np.empty(len(df), dtype=float)
        df_reset = df.reset_index(drop=True)
        for _, g in df_reset.groupby("race_id"):
            inv = 1.0 / g["win_odds"].values
            mprobs = inv / max(inv.sum(), 1e-8)
            hprobs = harville_place_probs(mprobs)
            market_probs_arr[g.index] = hprobs
        df["_market_prob"] = market_probs_arr

        df["_market_prob"] = df["_market_prob"].clip(lower=1e-4)
        ev = df["_model_prob"] / df["_market_prob"] * FUKUSHO_PAYOUT_RATE
        return ev.clip(lower=0.0)

    # ── 会場別動的EV閾値 ─────────────────────────────────────────────

    def optimize_venue_thresholds(
        self,
        df: pd.DataFrame,
        ev_range: tuple[float, float] = (1.00, 2.00),
        ev_step: float = 0.05,
        min_bets: int = 30,
    ) -> dict[str, float]:
        """
        会場ごとに ROI を最大化する EV 閾値を探索する。

        探索範囲: ev_range で指定した区間を ev_step 刻みでスキャン。
        min_bets: 最低ベット数。満たさない場合は DEFAULT_EV_THRESHOLD を使用。

        Returns:
            dict[venue_name, optimal_ev_threshold]
        """
        place_probs = self.predict_place_prob(df)
        ev_series = self.compute_ev(df, place_probs)

        work = df.copy()
        work["ev"] = ev_series.values
        work["actual_payout"] = pd.to_numeric(
            work["actual_payout"], errors="coerce"
        ).fillna(0)
        work["is_place"] = work["is_place"].astype(int)

        thresholds = [
            round(ev_range[0] + i * ev_step, 3)
            for i in range(int((ev_range[1] - ev_range[0]) / ev_step) + 1)
        ]

        venue_thresholds: dict[str, float] = {}

        for venue, vdf in work.groupby("venue"):
            best_roi = -np.inf
            best_thresh = DEFAULT_EV_THRESHOLD

            for t in thresholds:
                mask = vdf["ev"] >= t
                n = mask.sum()
                if n < min_bets:
                    continue
                invest = n * 100
                payout = (
                    vdf.loc[mask, "actual_payout"] * vdf.loc[mask, "is_place"]
                ).sum()
                roi = payout / invest * 100
                if roi > best_roi:
                    best_roi = roi
                    best_thresh = t

            venue_thresholds[str(venue)] = best_thresh

        self._venue_thresholds = venue_thresholds
        return venue_thresholds

    def get_ev_threshold(self, venue: str) -> float:
        """会場名から動的EV閾値を返す。未登録は DEFAULT_EV_THRESHOLD。"""
        return self._venue_thresholds.get(venue, DEFAULT_EV_THRESHOLD)

    def generate_buy_signals(
        self,
        df: pd.DataFrame,
        use_venue_thresholds: bool = True,
        bankroll: float = 100_000.0,
    ) -> pd.DataFrame:
        """
        複勝買いシグナルを生成する。

        Returns:
            race_id / horse_number / place_prob / ev / kelly_bet / win_odds
        """
        place_probs = self.predict_place_prob(df)
        ev_series = self.compute_ev(df, place_probs)

        result = df[["race_id", "horse_number", "win_odds"]].copy()
        if "venue" in df.columns:
            result["venue"] = df["venue"].values
        result["place_prob"] = place_probs.values
        result["ev"] = ev_series.values

        if use_venue_thresholds and self._venue_thresholds:
            venue_col = (
                df["venue"].astype(str)
                if "venue" in df.columns
                else pd.Series([""] * len(df), index=df.index)
            )
            thresholds = venue_col.map(
                lambda v: self._venue_thresholds.get(v, DEFAULT_EV_THRESHOLD)
            )
            buy_mask = result["ev"] >= thresholds.values
        else:
            buy_mask = result["ev"] >= DEFAULT_EV_THRESHOLD

        result["kelly_bet"] = result.apply(
            lambda row: self._kelly_bet(row["place_prob"], bankroll), axis=1
        )
        return result[buy_mask].reset_index(drop=True)

    @staticmethod
    def _kelly_bet(p_place: float, bankroll: float) -> int:
        """
        複勝向け Kelly Criterion (1/4 Kelly)。

        複勝の net odds b ≈ (estimated_payout/100) - 1 を
        P_model/P_market × payout_rate で推定するが、
        簡易版として b = 1.5 (150円払戻 on 100円ベットの推定) を使用。
        """
        b = 1.5  # 推定 net odds (150円払戻 - 1)
        p = float(p_place)
        q = 1.0 - p
        f = (b * p - q) / b
        if f <= 0:
            return MIN_BET
        raw = int(bankroll * f * KELLY_FRACTION)
        raw = (raw // 100) * 100
        return int(np.clip(raw, MIN_BET, MAX_BET))

    # ── 保存・ロード ─────────────────────────────────────────────────

    def save(self, path: Path | None = None) -> None:
        path = path or _MODEL_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(
                {
                    "model": self._model,
                    "calibrator": self._calibrator,
                    "label_encoders": self._label_encoders,
                    "best_params": self._best_params,
                    "venue_thresholds": self._venue_thresholds,
                },
                f,
            )
        logger.info("Alpha-Place 保存: %s", path)

    @classmethod
    def load(cls, path: Path | None = None) -> "AlphaPlaceModel":
        path = path or _MODEL_PATH
        obj = cls()
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj._model = data["model"]
        obj._calibrator = data.get("calibrator")
        obj._label_encoders = data.get("label_encoders", {})
        obj._best_params = data.get("best_params", {})
        obj._venue_thresholds = data.get("venue_thresholds", {})
        obj._is_trained = True
        logger.info("Alpha-Place ロード: %s", path)
        return obj


# ── バックテスト ─────────────────────────────────────────────────────


def run_place_backtest(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_years: list[int],
    n_optuna_trials: int = 30,
    bankroll: int = 100_000,
    verbose: bool = True,
    research_db_path: Path | None = None,
    use_venue_opt: bool = True,
    calibrate: bool = True,
    ev_threshold: float = DEFAULT_EV_THRESHOLD,
) -> PlaceBacktestResult:
    """
    Alpha-Place モデルの時系列バックテストを実行する。

    手順:
      1. train_years で Optuna + 学習 (+ Isotonic 校正)
      2. use_venue_opt=True なら train_years で会場別EV閾値を最適化
      3. test_years でシグナル生成・損益計算
    """
    model = AlphaPlaceModel()

    print(f"\n  [学習] {train_years} → [テスト] {test_years}", flush=True)

    # 学習
    train_df = model.load_training_data(conn, train_years, research_db_path)
    if len(train_df) < 500:
        raise ValueError(f"学習データ不足: {len(train_df)}行")

    metrics = model.train(
        train_df, n_optuna_trials=n_optuna_trials, calibrate=calibrate
    )
    print(
        f"  [学習完了] n_train={metrics['n_train']} n_calib={metrics.get('n_calib', '?')} n_val={metrics['n_val']} "
        f"Optuna AUC={metrics['optuna_auc']:.4f} "
        f"Calib AUC={metrics['calib_auc']:.4f}",
        flush=True,
    )

    # 会場別閾値最適化（train データで）
    if use_venue_opt:
        venue_thresh = model.optimize_venue_thresholds(train_df)
        if verbose:
            for v, t in sorted(venue_thresh.items(), key=lambda x: x[1]):
                print(f"  [会場閾値] {v}: EV>{t:.2f}", flush=True)
    else:
        venue_thresh = {}
        print(f"  [固定EV閾値] EV>{ev_threshold:.2f} (会場最適化無効)", flush=True)

    # テストデータ
    test_df = model.load_training_data(conn, test_years, research_db_path)
    if len(test_df) < 50:
        raise ValueError(f"テストデータ不足: {len(test_df)}行")

    place_probs = model.predict_place_prob(test_df)
    ev_series = model.compute_ev(test_df, place_probs)

    test_df = test_df.copy()
    test_df["ev"] = ev_series.values
    test_df["place_prob"] = place_probs.values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0)

    # EV 閾値でフィルタ（会場最適化 or 固定）
    if use_venue_opt and venue_thresh and "venue" in test_df.columns:
        test_df["_threshold"] = (
            test_df["venue"]
            .astype(str)
            .map(lambda v: venue_thresh.get(v, ev_threshold))
        )
    else:
        test_df["_threshold"] = ev_threshold

    bets_df = test_df[test_df["ev"] >= test_df["_threshold"]].copy()
    bets_df["kelly_bet"] = bets_df["place_prob"].apply(
        lambda p: AlphaPlaceModel._kelly_bet(p, bankroll)
    )
    bets_df["payout"] = (
        bets_df["is_place"] * bets_df["actual_payout"] * bets_df["kelly_bet"] / 100
    )

    total_investment = int(bets_df["kelly_bet"].sum())
    total_payout = float(bets_df["payout"].sum())
    profit = total_payout - total_investment
    roi = total_payout / total_investment * 100 if total_investment > 0 else 0.0
    num_hits = int(bets_df["is_place"].sum())
    num_bets = len(bets_df)
    hit_rate = num_hits / num_bets * 100 if num_bets > 0 else 0.0

    # 最大ドローダウン
    bets_sorted = bets_df.sort_values(["date", "race_id", "horse_number"])
    cum_pnl = (bets_sorted["payout"] - bets_sorted["kelly_bet"]).cumsum()
    max_dd = float((cum_pnl.cummax() - cum_pnl).max()) if len(cum_pnl) > 0 else 0.0

    result = PlaceBacktestResult(
        year=str(test_years[0]) if len(test_years) == 1 else str(test_years),
        total_investment=total_investment,
        total_payout=total_payout,
        profit=profit,
        roi=roi,
        num_bets=num_bets,
        num_hits=num_hits,
        hit_rate=hit_rate,
        max_drawdown=max_dd,
    )

    if verbose:
        mark = "✅" if roi >= 110 else ("⚠️" if roi >= 100 else "❌")
        print(f"\n  [複勝バックテスト] {test_years}")
        print(f"    買いシグナル : {num_bets:,}件")
        print(f"    的中率       : {hit_rate:.1f}% ({num_hits}件)")
        print(f"    総投資       : ¥{total_investment:,}")
        print(f"    総払戻       : ¥{total_payout:,.0f}")
        print(f"    損益         : ¥{profit:+,.0f}")
        print(f"    ROI          : {roi:.1f}% {mark}")
        print(f"    最大DD       : ¥{max_dd:,.0f}")

    return result


# ── ユーティリティ ────────────────────────────────────────────────────


def _lgb_early_stopping(rounds: int):
    try:
        from lightgbm import early_stopping as _es

        return _es(rounds, verbose=False)
    except ImportError:
        return None
