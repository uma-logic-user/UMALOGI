"""
Alpha-Payout Model — 複勝ペイアウト直接回帰（EV 最大化専用機）
==============================================================

is_place バイナリの限界を破る「原点回帰」モデル。

設計哲学:
  目的変数 = ev_target_place = actual_place_payout / 100
  （着内なら実払戻/100、着外なら 0.0）

  モデルが直接「この馬の複勝を買ったら何倍になるか」を学習する。
  オッズ（nb_win_odds）が目的変数に自然に組み込まれるため、
  「高配当×着内」という市場の歪みを直接捉えられる。

改善点（vs. is_place バイナリ）:
  1. Huber 損失: 高配当の外れ値に耐性（L2→L1 自動切替）
  2. nb_win_odds 完全統合: 期待ペイアウトを特徴量として学習
  3. Optuna 最適化指標: AUC ではなく検証 ROI を直接最大化
  4. 閾値最適化: 訓練データ ROI 最大化で最適 pred_ev > threshold を学習
  5. ゼロ過多分布対応: Huber が着外(0)と着内(1.1-30)の混在を安定処理

モデル保存先: data/models/alpha_payout/alpha_payout_model.pkl
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

logger = logging.getLogger(__name__)

# ── 設定定数 ─────────────────────────────────────────────────────────

_MODEL_DIR  = Path(__file__).resolve().parents[2] / "data" / "models" / "alpha_payout"
_MODEL_PATH = _MODEL_DIR / "alpha_payout_model.pkl"

# JRA 複勝 テイクアウト率 22.5% → 払戻率 77.5%
FUKUSHO_PAYOUT_RATE: float = 0.775

# Kelly Criterion の fraction
KELLY_FRACTION: float = 0.25
MIN_BET: int = 100
MAX_BET: int = 5_000

# デフォルト EV 閾値（pred_ev > threshold で購入）
DEFAULT_EV_THRESHOLD: float = 1.30

# Huber 損失の delta（残差 > delta で L1 に切替）
# ev_target_place の典型値 2.5 付近で切替
HUBER_DELTA: float = 2.5


# ── Harville 複勝確率 ────────────────────────────────────────────────

def harville_place_probs(win_probs: np.ndarray) -> np.ndarray:
    """P(top-3 finish) を Harville 公式で計算する。"""
    n = len(win_probs)
    p = np.asarray(win_probs, dtype=float)
    p = p / max(p.sum(), 1e-8)

    if n < 3:
        return np.minimum(p * 3.0, 1.0)

    one_minus_p = np.maximum(1.0 - p, 1e-8)
    result = p.copy()
    contrib = p / one_minus_p
    sum_contrib = contrib.sum()
    result += p * (sum_contrib - contrib)

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
    "win_odds", "popularity",
    "distance", "gate_number", "weight_carried",
    "horse_weight", "horse_weight_diff", "race_number",
    "surface_code", "sex_code", "venue_encoded", "condition_code",
    "jockey_encoded", "trainer_encoded",
]

PAYOUT_FEATURES: list[str] = [
    # 基本オッズ変換
    "log_win_odds", "inv_odds", "odds_popularity_ratio",
    # フィールド相対
    "field_size", "mean_field_odds", "odds_vs_field",
    # 市場確率
    "market_prob", "log_market_prob",
    # Harville 複勝確率（市場）
    "place_market_prob", "log_place_market_prob",
    "win_to_place_ratio",
    # 期待ペイアウト（市場 Harville から計算）
    "expected_place_payout",       # 77.5 / place_market_prob
    "log_expected_place_payout",
    # nb_win_odds 完全統合
    "nb_win_odds", "log_nb_win_odds", "inv_nb_odds",
    "nb_market_prob", "log_nb_market_prob",
    "nb_place_market_prob",        # nb_win_odds から Harville P(top3)
    "log_nb_place_market_prob",
    "nb_expected_place_payout",    # 77.5 / nb_place_market_prob
    "log_nb_expected_place_payout",
    # nb vs JV 乖離
    "odds_discrepancy_ratio",      # nb_win_odds / win_odds
    "nb_vs_field",
    "payout_discrepancy",          # nb_expected_payout / jv_expected_payout
]

ALL_PAYOUT_FEATURES: list[str] = BASE_FEATURES + PAYOUT_FEATURES


# ── 結果コンテナ ─────────────────────────────────────────────────────

@dataclass
class PayoutBacktestResult:
    year: str | int
    total_investment: int
    total_payout: float
    profit: float
    roi: float
    num_bets: int
    num_hits: int
    hit_rate: float
    max_drawdown: float
    ev_threshold: float = DEFAULT_EV_THRESHOLD
    bet_type: str = "複勝"
    notes: list[str] = field(default_factory=list)


# ── メインモデルクラス ────────────────────────────────────────────────

class AlphaPayoutModel:
    """
    複勝ペイアウト直接回帰モデル。

    アーキテクチャ:
      LGBMRegressor(huber) → pred_ev = predict(X) → buy when pred_ev > threshold
    """

    def __init__(self) -> None:
        self._model: Optional[LGBMRegressor] = None
        self._label_encoders: dict = {}
        self._best_params: dict = {}
        self._ev_threshold: float = DEFAULT_EV_THRESHOLD
        self._is_trained: bool = False

    # ── データ準備 ────────────────────────────────────────────────────

    def load_training_data(
        self,
        conn: sqlite3.Connection,
        years: list[int] | None = None,
        research_db_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        ev_target_place = actual_place_payout / 100 を目的変数とした学習データを生成する。

        着内馬: ev_target_place = payout/100 (例: 2.50)
        着外馬: ev_target_place = 0.0
        """
        logger.info("Alpha-Payout 学習データ生成: years=%s", years)

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
            COALESCE(rp.payout, 0.0) / 100.0 AS ev_target_place,
            COALESCE(rp.payout, 0.0)           AS actual_payout,
            CASE WHEN rp.combination IS NOT NULL THEN 1 ELSE 0 END AS is_place
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

        pos_rate = float((df["ev_target_place"] > 0).mean())
        mean_ev  = float(df.loc[df["ev_target_place"] > 0, "ev_target_place"].mean())
        logger.info(
            "ロード完了: %d行 着内=%d (%.1f%%) 平均EV=%.2f",
            len(df), int((df["ev_target_place"] > 0).sum()), pos_rate * 100, mean_ev,
        )
        return df

    @staticmethod
    def _merge_research_odds(df: pd.DataFrame, rdb: Path) -> pd.DataFrame:
        import sqlite3 as _sql
        rconn = _sql.connect(str(rdb))
        odds_df = pd.read_sql_query(
            "SELECT race_id, horse_number, win_odds AS nb_win_odds FROM horse_odds", rconn
        )
        rconn.close()

        merged = df.merge(odds_df, on=["race_id", "horse_number"], how="left")
        merged["win_odds"]    = pd.to_numeric(merged["win_odds"],    errors="coerce")
        merged["nb_win_odds"] = pd.to_numeric(merged["nb_win_odds"], errors="coerce")

        mask_jv = merged["win_odds"].isna() | (merged["win_odds"] <= 0)
        merged.loc[mask_jv, "win_odds"] = merged.loc[mask_jv, "nb_win_odds"]
        mask_nb = merged["nb_win_odds"].isna() | (merged["nb_win_odds"] <= 0)
        merged.loc[mask_nb, "nb_win_odds"] = merged.loc[mask_nb, "win_odds"]
        merged = merged[merged["win_odds"].notna() & (merged["win_odds"] > 0)]
        return merged

    # ── 特徴量エンジニアリング ────────────────────────────────────────

    def _add_payout_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # カテゴリエンコーディング
        df["surface_code"] = df.get("surface", pd.Series(dtype=str)).map(
            {"芝": 0, "ダート": 1, "障害": 2}
        ).fillna(-1).astype(int)
        df["sex_code"] = df.get("sex_age", pd.Series(dtype=str)).str[0].map(
            {"牡": 0, "牝": 1, "セ": 2}
        ).fillna(-1).astype(int)
        df["venue_encoded"] = df.get("venue", pd.Series(dtype=str)).map(
            {"札幌": 0, "函館": 1, "福島": 2, "新潟": 3, "東京": 4,
             "中山": 5, "中京": 6, "京都": 7, "阪神": 8, "小倉": 9}
        ).fillna(-1).astype(int)
        df["condition_code"] = df.get("condition", pd.Series(dtype=str)).map(
            {"良": 0, "稍重": 1, "重": 2, "不良": 3}
        ).fillna(-1).astype(int)

        # 基本オッズ
        df["win_odds"] = pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        df["log_win_odds"] = np.log(df["win_odds"])
        df["inv_odds"] = 1.0 / df["win_odds"]

        pop = pd.to_numeric(df.get("popularity"), errors="coerce").fillna(9).clip(lower=1)
        df["odds_popularity_ratio"] = df["win_odds"] / pop

        grp = df.groupby("race_id", group_keys=False)
        df["field_size"]     = grp["horse_number"].transform("count")
        df["mean_field_odds"] = grp["win_odds"].transform("mean")
        df["odds_vs_field"]  = df["win_odds"] / df["mean_field_odds"].clip(lower=1.0)

        inv_sum = grp["inv_odds"].transform("sum").clip(lower=1e-8)
        df["market_prob"]     = df["inv_odds"] / inv_sum
        df["log_market_prob"] = np.log(df["market_prob"].clip(lower=1e-8))

        # JV Harville 複勝確率
        place_arr = np.empty(len(df), dtype=float)
        df_r = df.reset_index(drop=True)
        for _, g in df_r.groupby("race_id"):
            mp = g["market_prob"].values
            place_arr[g.index] = harville_place_probs(mp)
        df["place_market_prob"] = place_arr.clip(1e-4, 0.9999)
        df["log_place_market_prob"] = np.log(df["place_market_prob"])
        df["win_to_place_ratio"] = df["market_prob"] / df["place_market_prob"].clip(lower=1e-4)

        # 期待ペイアウト（JV）
        df["expected_place_payout"] = FUKUSHO_PAYOUT_RATE * 100 / df["place_market_prob"]
        df["log_expected_place_payout"] = np.log(df["expected_place_payout"].clip(lower=1.0))

        # ── nb_win_odds 完全統合 ────────────────────────────────────
        if "nb_win_odds" in df.columns:
            df["nb_win_odds"] = (
                pd.to_numeric(df["nb_win_odds"], errors="coerce")
                .fillna(df["win_odds"]).clip(lower=1.01)
            )
        else:
            df["nb_win_odds"] = df["win_odds"]

        df["log_nb_win_odds"] = np.log(df["nb_win_odds"])
        df["inv_nb_odds"] = 1.0 / df["nb_win_odds"]

        nb_inv_sum = grp["inv_nb_odds"].transform("sum").clip(lower=1e-8)
        df["nb_market_prob"] = df["inv_nb_odds"] / nb_inv_sum
        df["log_nb_market_prob"] = np.log(df["nb_market_prob"].clip(lower=1e-8))

        # nb Harville 複勝確率（最重要特徴量）
        nb_place_arr = np.empty(len(df), dtype=float)
        df_nb = df.reset_index(drop=True)  # nb_market_prob 追加後に再reset
        for _, g in df_nb.groupby("race_id"):
            nb_mp = g["nb_market_prob"].values
            nb_place_arr[g.index] = harville_place_probs(nb_mp)
        df["nb_place_market_prob"] = nb_place_arr.clip(1e-4, 0.9999)
        df["log_nb_place_market_prob"] = np.log(df["nb_place_market_prob"])

        df["nb_expected_place_payout"] = FUKUSHO_PAYOUT_RATE * 100 / df["nb_place_market_prob"]
        df["log_nb_expected_place_payout"] = np.log(df["nb_expected_place_payout"].clip(lower=1.0))

        # nb vs JV 乖離（市場の歪みシグナル）
        df["odds_discrepancy_ratio"] = (
            df["nb_win_odds"] / df["win_odds"].clip(lower=1.01)
        ).clip(0.1, 10.0)
        nb_mean = grp["nb_win_odds"].transform("mean").clip(lower=1.0)
        df["nb_vs_field"] = df["nb_win_odds"] / nb_mean

        # payout 乖離（nb vs JV の期待ペイアウト比）
        df["payout_discrepancy"] = (
            df["nb_expected_place_payout"] / df["expected_place_payout"].clip(lower=1.0)
        ).clip(0.1, 10.0)

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
                    df[out] = df[src].astype(str).apply(
                        lambda x: int(le.transform([x])[0]) if x in known else -1
                    )
                else:
                    df[out] = -1
        return df

    def prepare_features(
        self, df: pd.DataFrame, fit: bool = True
    ) -> tuple[pd.DataFrame, pd.Series | None]:
        df = self._add_payout_features(df)
        df = self._encode_categoricals(df, fit=fit)

        for col in ALL_PAYOUT_FEATURES:
            if col not in df.columns:
                df[col] = -1.0

        X = df[ALL_PAYOUT_FEATURES].copy()
        X = X.apply(pd.to_numeric, errors="coerce").fillna(-1.0)
        y = df["ev_target_place"].astype(float) if "ev_target_place" in df.columns else None
        return X, y

    # ── 学習 ──────────────────────────────────────────────────────────

    def train(
        self,
        df: pd.DataFrame,
        n_optuna_trials: int = 30,
    ) -> dict[str, float]:
        """
        Optuna で超パラメータ最適化（指標: 検証ROI）→ LightGBM Huber 回帰学習。

        検証ROI を直接最大化することで、AUC ではなく利益を直接最適化する。
        """
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        X, y = self.prepare_features(df, fit=True)
        assert y is not None

        pos_rate = float((y > 0).mean())
        mean_ev  = float(y[y > 0].mean()) if (y > 0).any() else 0.0
        print(f"  [Optuna] {n_optuna_trials}試行 開始 ({len(X):,}行, pos={pos_rate:.1%}, mean_ev={mean_ev:.2f})", flush=True)

        n = len(X)
        train_end = int(n * 0.70)
        calib_end = int(n * 0.85)
        X_tr  = X.iloc[:train_end];   y_tr  = y.iloc[:train_end]
        X_val = X.iloc[calib_end:];   y_val = y.iloc[calib_end:]

        # バリデーション用: 実際のペイアウトと is_place
        df_val = df.iloc[calib_end:].copy()

        def _roi_from_preds(preds: np.ndarray, threshold: float) -> float:
            mask = preds >= threshold
            if mask.sum() == 0:
                return 0.0
            invest = mask.sum() * 100
            payout = (
                df_val.loc[mask, "actual_payout"].values
                * df_val.loc[mask, "is_place"].values
            ).sum()
            return float(payout / invest * 100) if invest > 0 else 0.0

        def objective(trial: "optuna.Trial") -> float:
            params = {
                "objective": "huber",
                "alpha": trial.suggest_float("huber_alpha", 0.5, 4.0),
                "n_estimators": trial.suggest_int("n_estimators", 300, 2000),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.10, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 31, 256),
                "max_depth": trial.suggest_int("max_depth", 4, 10),
                "min_child_samples": trial.suggest_int("min_child_samples", 10, 60),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 1.0, log=True),
                "random_state": 42,
                "n_jobs": -1,
                "verbose": -1,
            }
            m = LGBMRegressor(**params)
            m.fit(X_tr, y_tr,
                  eval_set=[(X_val, y_val)],
                  callbacks=[_lgb_early_stopping(50)])
            preds = m.predict(X_val)

            # 最適閾値でROIを計算してOptuna指標とする
            best_roi = 0.0
            for t in np.arange(1.0, 2.5, 0.1):
                roi = _roi_from_preds(preds, t)
                if roi > best_roi:
                    best_roi = roi

            return best_roi

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=n_optuna_trials, show_progress_bar=False)
        self._best_params = study.best_params
        best_roi = study.best_value

        print(f"  [Optuna] 完了 → best 検証ROI={best_roi:.1f}%  "
              f"num_leaves={self._best_params.get('num_leaves')} "
              f"lr={self._best_params.get('learning_rate', 0):.4f}", flush=True)

        # ── ベストパラメータで全データ学習 ──────────────────────────
        final_params = {
            "objective": "huber",
            "random_state": 42,
            "n_jobs": -1,
            "verbose": -1,
            **self._best_params,
        }
        self._model = LGBMRegressor(**final_params)
        self._model.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[_lgb_early_stopping(50)],
        )

        # ── 閾値最適化（訓練+検証データで最適化） ───────────────────
        X_opt = X.iloc[:calib_end]
        y_opt = y.iloc[:calib_end]
        df_opt = df.iloc[:calib_end].copy()
        preds_opt = self._model.predict(X_opt)

        best_t   = DEFAULT_EV_THRESHOLD
        best_opt = 0.0
        for t in np.arange(1.0, 3.0, 0.05):
            mask = preds_opt >= t
            if mask.sum() < 100:
                continue
            invest = mask.sum() * 100
            payout = (
                df_opt.loc[mask, "actual_payout"].values
                * df_opt.loc[mask, "is_place"].values
            ).sum()
            roi = payout / invest * 100
            if roi > best_opt:
                best_opt = roi
                best_t   = round(t, 2)

        self._ev_threshold = best_t
        val_roi = _roi_from_preds(self._model.predict(X_val), best_t)

        print(f"  [閾値] 最適閾値 pred_ev>{best_t:.2f} "
              f"訓練ROI={best_opt:.1f}% 検証ROI={val_roi:.1f}%", flush=True)
        self._is_trained = True

        return {
            "optuna_best_roi": best_roi,
            "threshold":       best_t,
            "train_roi":       best_opt,
            "val_roi":         val_roi,
            "n_train":         train_end,
            "n_val":           n - calib_end,
            "pos_rate":        pos_rate,
            "mean_ev":         mean_ev,
        }

    # ── 推論 ──────────────────────────────────────────────────────────

    def predict_payout_ev(self, df: pd.DataFrame) -> pd.Series:
        """予測 ev_target_place（pred_ev）を返す。"""
        if not self._is_trained or self._model is None:
            raise RuntimeError("モデル未学習。train() を先に呼んでください。")
        X, _ = self.prepare_features(df, fit=False)
        preds = self._model.predict(X)
        return pd.Series(np.maximum(preds, 0.0), index=df.index)

    def generate_buy_signals(
        self,
        df: pd.DataFrame,
        ev_threshold: float | None = None,
        bankroll: float = 100_000.0,
    ) -> pd.DataFrame:
        """買いシグナルを生成する。"""
        threshold = ev_threshold if ev_threshold is not None else self._ev_threshold
        pred_ev = self.predict_payout_ev(df)

        result = df[["race_id", "horse_number", "win_odds"]].copy()
        if "venue" in df.columns:
            result["venue"] = df["venue"].values
        result["pred_ev"] = pred_ev.values

        buy_mask = result["pred_ev"] >= threshold
        result["kelly_bet"] = result["pred_ev"].apply(
            lambda ev: self._kelly_bet(ev, bankroll)
        )
        return result[buy_mask].reset_index(drop=True)

    @staticmethod
    def _kelly_bet(pred_ev: float, bankroll: float) -> int:
        """Kelly Criterion: b = pred_ev - 1 (net odds)"""
        b = max(pred_ev - 1.0, 0.1)  # net odds = expected return - stake
        p = FUKUSHO_PAYOUT_RATE       # 払戻率 = 胴元なしのなら的中確率に近い
        f = (b * p - (1.0 - p)) / b
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
            pickle.dump({
                "model":          self._model,
                "label_encoders": self._label_encoders,
                "best_params":    self._best_params,
                "ev_threshold":   self._ev_threshold,
            }, f)
        logger.info("Alpha-Payout 保存: %s", path)

    @classmethod
    def load(cls, path: Path | None = None) -> "AlphaPayoutModel":
        path = path or _MODEL_PATH
        obj = cls()
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj._model          = data["model"]
        obj._label_encoders = data.get("label_encoders", {})
        obj._best_params    = data.get("best_params", {})
        obj._ev_threshold   = data.get("ev_threshold", DEFAULT_EV_THRESHOLD)
        obj._is_trained     = True
        return obj


# ── バックテスト ─────────────────────────────────────────────────────

def run_payout_backtest(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_years: list[int],
    n_optuna_trials: int = 30,
    bankroll: int = 100_000,
    verbose: bool = True,
    research_db_path: Path | None = None,
    ev_threshold: float | None = None,
) -> PayoutBacktestResult:
    """
    Alpha-Payout モデルの時系列バックテストを実行する。

    手順:
      1. train_years で Optuna + Huber 回帰学習 + 閾値最適化
      2. test_years で buy シグナル生成・損益計算
    """
    model = AlphaPayoutModel()
    print(f"\n  [学習] {train_years} → [テスト] {test_years}", flush=True)

    train_df = model.load_training_data(conn, train_years, research_db_path)
    if len(train_df) < 500:
        raise ValueError(f"学習データ不足: {len(train_df)}行")

    metrics = model.train(train_df, n_optuna_trials=n_optuna_trials)
    print(f"  [学習完了] n_train={metrics['n_train']} val_ROI={metrics['val_roi']:.1f}% "
          f"閾値={metrics['threshold']:.2f}", flush=True)

    threshold = ev_threshold if ev_threshold is not None else model._ev_threshold

    test_df = model.load_training_data(conn, test_years, research_db_path)
    if len(test_df) < 50:
        raise ValueError(f"テストデータ不足: {len(test_df)}行")

    pred_ev = model.predict_payout_ev(test_df)
    test_df = test_df.copy()
    test_df["pred_ev"]      = pred_ev.values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0)

    bets_df = test_df[test_df["pred_ev"] >= threshold].copy()
    bets_df["kelly_bet"] = bets_df["pred_ev"].apply(
        lambda ev: AlphaPayoutModel._kelly_bet(ev, bankroll)
    )
    bets_df["payout"] = (
        bets_df["is_place"] * bets_df["actual_payout"] * bets_df["kelly_bet"] / 100
    )

    total_investment = int(bets_df["kelly_bet"].sum())
    total_payout     = float(bets_df["payout"].sum())
    profit           = total_payout - total_investment
    roi              = total_payout / total_investment * 100 if total_investment > 0 else 0.0
    num_hits         = int(bets_df["is_place"].sum())
    num_bets         = len(bets_df)
    hit_rate         = num_hits / num_bets * 100 if num_bets > 0 else 0.0

    bets_sorted = bets_df.sort_values(["date", "race_id", "horse_number"])
    cum_pnl = (bets_sorted["payout"] - bets_sorted["kelly_bet"]).cumsum()
    max_dd  = float((cum_pnl.cummax() - cum_pnl).max()) if len(cum_pnl) > 0 else 0.0

    result = PayoutBacktestResult(
        year=str(test_years[0]) if len(test_years) == 1 else str(test_years),
        total_investment=total_investment,
        total_payout=total_payout,
        profit=profit,
        roi=roi,
        num_bets=num_bets,
        num_hits=num_hits,
        hit_rate=hit_rate,
        max_drawdown=max_dd,
        ev_threshold=threshold,
    )

    if verbose:
        mark = "✅" if roi >= 110 else ("⚠️ " if roi >= 100 else "❌")
        print(f"\n  [複勝バックテスト] {test_years}")
        print(f"    買いシグナル : {num_bets:,}件 (pred_ev>{threshold:.2f})")
        print(f"    的中率       : {hit_rate:.1f}% ({num_hits}件)")
        print(f"    総投資       : ¥{total_investment:,}")
        print(f"    総払戻       : ¥{total_payout:,.0f}")
        print(f"    損益         : ¥{profit:+,.0f}")
        print(f"    ROI          : {roi:.1f}% {mark}")
        print(f"    最大DD       : ¥{max_dd:,.0f}", flush=True)

    return result


# ── ユーティリティ ────────────────────────────────────────────────────

def _lgb_early_stopping(rounds: int):
    try:
        from lightgbm import early_stopping as _es
        return _es(rounds, verbose=False)
    except ImportError:
        return None
