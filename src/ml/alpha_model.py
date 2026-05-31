"""
ALPHA モデル — 収益特化型 AI（資産形成用）
==========================================

既存モデル（本命・卍）とは完全分離した投資特化モデル。

設計思想:
  - 目的変数: 的中率ではなく「回収期待値 (EV)」最大化
  - オッズの歪み（市場が過小評価している馬）を検知
  - Kelly 基準による資金管理
  - 年間 ROI 110% 超えを至上命題

学習フロー:
  1. v_race_mart + race_payouts から EV ターゲット生成
  2. 時系列分割（Train: 2024 / Test: 2025）でリーク防止
  3. LightGBM Regressor で EV を回帰
  4. EV > ALPHA_THRESHOLD の馬のみ buy シグナル
  5. Kelly Criterion で賭け金算出

モデル保存先: data/models/alpha/alpha_model.pkl
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
from lightgbm import LGBMClassifier

logger = logging.getLogger(__name__)

# ── 設定定数 ─────────────────────────────────────────────────────────

_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models" / "alpha"
_MODEL_PATH = _MODEL_DIR / "alpha_model.pkl"

# ALPHAモデルが賭けるEV閾値（1.5 = 100円で150円以上の期待値）
ALPHA_EV_THRESHOLD: float = 1.5

# Kelly Criterion の fraction（過剰ベット防止のため 1/4 Kelly を使用）
KELLY_FRACTION: float = 0.25

# 最低掛け金・最大掛け金（円）
MIN_BET: int = 100
MAX_BET: int = 5000

# ── EV計算対象の馬券種 ───────────────────────────────────────────────

# 単勝 EV: 最もシンプル＆データが豊富
BET_TYPE_TANSHO = "単勝"
# 複勝 EV: 的中率が高くドローダウンが低い
BET_TYPE_FUKUSHO = "複勝"

# ── 特徴量定義 ───────────────────────────────────────────────────────
# 既存 FEATURE_COLS + オッズ歪み検知特徴量

BASE_FEATURES: list[str] = [
    # オッズ・人気（市場情報 = ALPHA のコア特徴量）
    "win_odds",
    "popularity",
    # レース条件
    "distance",
    "gate_number",
    "weight_carried",
    "horse_weight",
    "horse_weight_diff",
    "race_number",
    # カテゴリ（数値エンコード後）
    "surface_code",
    "sex_code",
    "venue_encoded",
    "condition_code",
    # 騎手・調教師（ラベルエンコード）
    "jockey_encoded",
    "trainer_encoded",
]

# ALPHA 追加特徴量（オッズ歪み検知）— _add_alpha_features() で計算
ALPHA_EXTRA_FEATURES: list[str] = [
    "log_win_odds",  # log(単勝オッズ) — 対数スケールで歪み検知
    "inv_odds",  # 1/単勝オッズ = 市場の implied 確率
    "odds_popularity_ratio",  # オッズ / 人気順位 (高=人気薄のオッズが高い = 歪みポテンシャル)
    "field_size",  # 出走頭数（多頭数ほど高配当チャンス）
    "mean_field_odds",  # レース内平均オッズ
    "odds_vs_field",  # win_odds / mean_field_odds (1より大=人気薄)
    "market_prob",  # 市場確率 = inv_odds / sum(inv_odds) in race
    "log_market_prob",  # log(市場確率)
    # ── ハイブリッド特徴量（netkeiba × JVLink 乖離）─────────────────
    "nb_win_odds",  # netkeiba 単勝オッズ（独立ソース）
    "log_nb_win_odds",  # log(nb_win_odds)
    "nb_market_prob",  # 市場確率（netkeiba 基準）
    "nb_log_market_prob",  # log(nb 市場確率)
    "odds_discrepancy_ratio",  # nb_win_odds / jvlink_win_odds（乖離度: 1=一致）
    "nb_vs_field",  # nb_win_odds / レース内平均 nb_win_odds
]

ALL_ALPHA_FEATURES: list[str] = BASE_FEATURES + ALPHA_EXTRA_FEATURES


@dataclass
class AlphaBacktestResult:
    """バックテスト結果コンテナ"""

    year: str | int
    total_investment: int
    total_payout: float
    profit: float
    roi: float
    num_bets: int
    num_hits: int
    hit_rate: float
    max_drawdown: float
    best_single_payout: float
    bet_type: str
    ev_threshold: float
    notes: list[str] = field(default_factory=list)


class AlphaModel:
    """
    収益特化型 AI モデル。

    アーキテクチャ: 勝率分類 + EV = P(win) × win_odds
      - 直接 EV 回帰はゼロ過剰（93% が非勝者）で機能しない
      - LGBMClassifier で P(win) を推定し、EV = P(win) × win_odds で算出
      - 市場オッズとの差（EV > 1.0 = 期待値プラス）を検知
      - EV > ALPHA_EV_THRESHOLD の馬のみ buy シグナル
      - Kelly Criterion で賭け金サイジング
    """

    def __init__(self) -> None:
        self._model: Optional[LGBMClassifier] = None
        self._feature_cols: list[str] = ALL_ALPHA_FEATURES
        self._label_encoders: dict = {}
        self._is_trained: bool = False

    # ── データ準備 ────────────────────────────────────────────────────

    def load_training_data(
        self,
        conn: sqlite3.Connection,
        years: list[int] | None = None,
        bet_type: str = BET_TYPE_TANSHO,
        min_date: str | None = None,
        max_date: str | None = None,
        research_db_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        race_results + race_payouts から EV 学習データを生成する。

        【設計方針】
        - race_payouts の確定払戻があるレースのみ対象（結果確定済みレース）
        - 勝者は race_payouts の単勝/複勝 combination から特定
        - win_odds がある全馬を正/負サンプルとして使用

        時系列分割のため years または min_date/max_date で絞る。
        """
        logger.info(
            "EV学習データ生成: years=%s bet_type=%s min_date=%s max_date=%s research_db=%s",
            years,
            bet_type,
            min_date,
            max_date,
            research_db_path,
        )

        if bet_type == BET_TYPE_TANSHO:
            payout_bet_type = "単勝"
        else:
            payout_bet_type = "複勝"
        hit_expr = "CASE WHEN CAST(rp_hit.combination AS INTEGER) = rr.horse_number THEN 1 ELSE 0 END"

        # 日付フィルタ構築（CTE内部は無エイリアス、外部クエリはエイリアス r を使用）
        cte_filters: list[str] = []  # CTE内 races テーブル（エイリアスなし）
        params: list = []

        if years:
            year_placeholders = ",".join("?" * len(years))
            year_strs = [str(y) for y in years]
            cte_filters.append(f"strftime('%Y', date) IN ({year_placeholders})")
            params.extend(year_strs)
        if min_date:
            cte_filters.append("date >= ?")
            params.append(min_date)
        if max_date:
            cte_filters.append("date <= ?")
            params.append(max_date)

        cte_date_where = " AND ".join(cte_filters) if cte_filters else "1=1"

        sql = f"""
        WITH confirmed_races AS (
            SELECT DISTINCT race_id
            FROM race_payouts
            WHERE bet_type = '{payout_bet_type}'
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
            r.track_direction,
            {hit_expr} as is_hit,
            CASE WHEN CAST(rp_hit.combination AS INTEGER) = rr.horse_number
                 THEN CAST(rp_hit.payout AS REAL) / 100.0
                 ELSE 0.0 END as ev_target,
            rp_hit.payout as actual_payout
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        JOIN confirmed_races cr ON rr.race_id = cr.race_id
        LEFT JOIN race_payouts rp_hit
            ON rp_hit.race_id = rr.race_id
            AND rp_hit.bet_type = '{payout_bet_type}'
            AND CAST(rp_hit.combination AS INTEGER) = rr.horse_number
        WHERE rr.horse_number IS NOT NULL
          AND rr.horse_number > 0
        ORDER BY r.date, rr.race_id, rr.horse_number
        """

        df = pd.read_sql_query(sql, conn, params=params)
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        # research_db がある場合: in-memory JOIN で win_odds を補完
        if research_db_path is not None and Path(research_db_path).exists():
            df = self._merge_research_odds(df, Path(research_db_path))
        else:
            # research_db なし: 従来通り win_odds IS NOT NULL のみ
            df = df[df["win_odds"].notna() & (df["win_odds"] > 0)]

        logger.info(
            "ロード完了: %d 行, is_hit=%d (%.1f%%)",
            len(df),
            int(df["is_hit"].sum()) if "is_hit" in df.columns else 0,
            float(df["is_hit"].mean()) * 100
            if "is_hit" in df.columns and len(df) > 0
            else 0,
        )
        return df

    @staticmethod
    def _merge_research_odds(df: pd.DataFrame, research_db_path: Path) -> pd.DataFrame:
        """
        research DB の horse_odds テーブルと in-memory JOIN して
        win_odds を補完し、nb_win_odds を独立特徴量として保持する。

        ハイブリッド設計:
          - nb_win_odds: netkeiba 由来の単勝オッズ（JVLink と独立した市場信号）
          - win_odds:    JVLink 由来（優先）、NULL の場合のみ nb_win_odds で補完
          - 両方が存在する場合: odds_discrepancy_ratio = nb/jvlink で乖離度を算出
        """
        import sqlite3 as _sqlite3

        rconn = _sqlite3.connect(str(research_db_path))
        odds_df = pd.read_sql_query(
            "SELECT race_id, horse_number, win_odds AS nb_win_odds FROM horse_odds",
            rconn,
        )
        rconn.close()

        merged = df.merge(odds_df, on=["race_id", "horse_number"], how="left")

        # dtype を float64 に統一してから補完（FutureWarning 回避）
        merged["win_odds"] = pd.to_numeric(merged["win_odds"], errors="coerce")
        merged["nb_win_odds"] = pd.to_numeric(merged["nb_win_odds"], errors="coerce")

        # JVLink の win_odds が NULL → nb_win_odds で補完
        jvlink_null = merged["win_odds"].isna() | (merged["win_odds"] <= 0)
        merged.loc[jvlink_null, "win_odds"] = merged.loc[jvlink_null, "nb_win_odds"]

        # nb_win_odds が NULL → JVLink で補完（双方向）
        nb_null = merged["nb_win_odds"].isna() | (merged["nb_win_odds"] <= 0)
        merged.loc[nb_null, "nb_win_odds"] = merged.loc[nb_null, "win_odds"]

        # それでも win_odds が NULL の行は除外
        merged = merged[merged["win_odds"].notna() & (merged["win_odds"] > 0)]

        logger.info(
            "research_db 補完後: %d 行 (JVLink補完: %d 行, nb補完: %d 行)",
            len(merged),
            int(jvlink_null.sum()),
            int(nb_null.sum()),
        )
        return merged

    def _add_alpha_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """オッズ歪み検知特徴量を追加する。"""
        df = df.copy()

        # ── 基本エンコーディング ─────────────────────────────────────
        # surface → surface_code
        surface_map = {"芝": 0, "ダート": 1, "障害": 2}
        if "surface" in df.columns:
            df["surface_code"] = df["surface"].map(surface_map).fillna(-1).astype(int)

        # 性別コード
        sex_map = {"牡": 0, "牝": 1, "セ": 2}
        if "sex_age" in df.columns:
            df["sex_code"] = df["sex_age"].str[0].map(sex_map).fillna(-1).astype(int)

        # 会場コード
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

        # 馬場状態コード
        cond_map = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
        if "condition" in df.columns:
            df["condition_code"] = df["condition"].map(cond_map).fillna(-1).astype(int)

        # 騎手・調教師ラベルエンコード
        for col, out_col in [
            ("jockey", "jockey_encoded"),
            ("trainer", "trainer_encoded"),
        ]:
            if col in df.columns and out_col not in df.columns:
                if self._label_encoders.get(out_col) is not None:
                    le = self._label_encoders[out_col]
                    known = set(le.classes_)
                    df[out_col] = (
                        df[col]
                        .astype(str)
                        .apply(
                            lambda x: int(le.transform([x])[0]) if x in known else -1
                        )
                    )
                else:
                    # fit時はここでエンコード（_encode_categoricals で対応）
                    df[out_col] = -1

        # ── オッズ歪み特徴量 ─────────────────────────────────────────
        df["win_odds"] = pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0)
        df["win_odds"] = df["win_odds"].clip(lower=1.01)

        df["log_win_odds"] = np.log(df["win_odds"])
        df["inv_odds"] = 1.0 / df["win_odds"]

        pop = (
            pd.to_numeric(df.get("popularity"), errors="coerce").fillna(9).clip(lower=1)
        )
        df["odds_popularity_ratio"] = df["win_odds"] / pop

        # レース内の統計量（グループ集計）
        grp = df.groupby("race_id", group_keys=False)
        df["field_size"] = grp["horse_number"].transform("count")
        df["mean_field_odds"] = grp["win_odds"].transform("mean")
        df["odds_vs_field"] = df["win_odds"] / df["mean_field_odds"].clip(lower=1.0)

        # 市場確率 (bookmaker-implied prob)
        inv_sum = grp["inv_odds"].transform("sum").clip(lower=1e-8)
        df["market_prob"] = df["inv_odds"] / inv_sum
        df["log_market_prob"] = np.log(df["market_prob"].clip(lower=1e-8))

        # ── ハイブリッド特徴量（netkeiba × JVLink 乖離）─────────────────
        # nb_win_odds が存在しない場合は JVLink の win_odds で代替
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
        df["nb_log_market_prob"] = np.log(
            (df["nb_market_prob"] / nb_inv_sum).clip(lower=1e-8)
        )

        # 乖離度: nb_win_odds / jvlink_win_odds（1.0=完全一致、>1=nbが割高）
        df["odds_discrepancy_ratio"] = (
            df["nb_win_odds"] / df["win_odds"].clip(lower=1.01)
        ).clip(0.1, 10.0)

        nb_mean_field = grp["nb_win_odds"].transform("mean").clip(lower=1.0)
        df["nb_vs_field"] = df["nb_win_odds"] / nb_mean_field

        return df

    def _encode_categoricals(self, df: pd.DataFrame, fit: bool = True) -> pd.DataFrame:
        """騎手・調教師をラベルエンコードする。"""
        from sklearn.preprocessing import LabelEncoder

        df = df.copy()

        for src_col, out_col in [
            ("jockey", "jockey_encoded"),
            ("trainer", "trainer_encoded"),
        ]:
            if src_col not in df.columns:
                df[out_col] = -1
                continue
            if fit:
                le = LabelEncoder()
                df[out_col] = le.fit_transform(df[src_col].astype(str))
                self._label_encoders[out_col] = le
            else:
                le = self._label_encoders.get(out_col)
                if le:
                    known = set(le.classes_)
                    df[out_col] = (
                        df[src_col]
                        .astype(str)
                        .apply(
                            lambda x: int(le.transform([x])[0]) if x in known else -1
                        )
                    )
                else:
                    df[out_col] = -1

        return df

    def prepare_features(
        self,
        df: pd.DataFrame,
        fit: bool = True,
    ) -> tuple[pd.DataFrame, pd.Series | None]:
        """
        特徴量行列と EV ターゲットを生成する。

        Returns:
            (X, y) — y は訓練時のみ、推論時は None
        """
        df = self._add_alpha_features(df)
        df = self._encode_categoricals(df, fit=fit)

        [c for c in ALL_ALPHA_FEATURES if c in df.columns]
        missing = [c for c in ALL_ALPHA_FEATURES if c not in df.columns]
        if missing:
            logger.debug("特徴量欠損 (%d列): %s", len(missing), missing[:5])
            for col in missing:
                df[col] = -1.0

        X = df[ALL_ALPHA_FEATURES].copy()
        X = X.apply(pd.to_numeric, errors="coerce").fillna(-1.0)

        # 分類ターゲット（勝率推定用）
        y = df["is_hit"].astype(int) if "is_hit" in df.columns else None
        return X, y

    # ── 学習 ──────────────────────────────────────────────────────────

    def train(self, df: pd.DataFrame) -> dict[str, float]:
        """
        勝率分類モデルを学習する。EV = P(win) × win_odds で算出。

        Args:
            df: load_training_data() の出力

        Returns:
            metrics dict (logloss, auc, n_train, n_val)
        """
        X, y = self.prepare_features(df, fit=True)
        assert y is not None

        logger.info("ALPHA 学習開始: %d サンプル, %d 特徴量", len(X), len(X.columns))

        pos_rate = float(y.mean())
        scale_pos_weight = (1.0 - pos_rate) / pos_rate if pos_rate > 0 else 1.0

        self._model = LGBMClassifier(
            objective="binary",
            n_estimators=1500,
            learning_rate=0.02,
            num_leaves=127,
            max_depth=8,
            min_child_samples=15,
            subsample=0.75,
            colsample_bytree=0.75,
            reg_alpha=0.05,
            reg_lambda=0.5,
            scale_pos_weight=scale_pos_weight,  # クラス不均衡補正
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )

        # 時系列を守るためシャッフルなし
        split_idx = int(len(X) * 0.85)
        X_tr, X_val = X.iloc[:split_idx], X.iloc[split_idx:]
        y_tr, y_val = y.iloc[:split_idx], y.iloc[split_idx:]

        callbacks = []
        cb = lgb_early_stopping(50, verbose=False)
        if cb is not None:
            callbacks.append(cb)

        self._model.fit(
            X_tr,
            y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=callbacks or None,
        )

        win_prob_val = self._model.predict_proba(X_val)[:, 1]
        from sklearn.metrics import log_loss, roc_auc_score

        try:
            logloss = float(log_loss(y_val, win_prob_val))
            auc = float(roc_auc_score(y_val, win_prob_val))
        except Exception:
            logloss, auc = 0.0, 0.0

        self._is_trained = True
        metrics = {
            "logloss": logloss,
            "auc": auc,
            "n_train": len(X_tr),
            "n_val": len(X_val),
        }
        logger.info("学習完了: %s", metrics)
        return metrics

    # ── 推論・賭け金計算 ─────────────────────────────────────────────

    def predict_win_prob(self, df: pd.DataFrame) -> pd.Series:
        """各馬の推定勝率（0〜1）を返す。"""
        if not self._is_trained or self._model is None:
            raise RuntimeError("モデル未学習。train() を先に呼んでください。")
        X, _ = self.prepare_features(df, fit=False)
        win_prob = self._model.predict_proba(X)[:, 1]
        return pd.Series(win_prob, index=df.index)

    def predict_ev(self, df: pd.DataFrame) -> pd.Series:
        """
        各馬の予測 EV = P(win) × win_odds を返す。

        EV > 1.0: 100円賭けて期待回収 > 100円（プラス期待値）
        EV > 1.5: 50%以上の価値超過（ALPHA のデフォルト閾値）
        """
        win_prob = self.predict_win_prob(df)
        odds = (
            pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        )
        ev = win_prob * odds
        return ev.clip(lower=0.0)

    def calc_kelly_bet(
        self,
        ev_pred: float,
        win_odds: float,
        bankroll: int = 100_000,
    ) -> int:
        """
        Kelly Criterion (fractional) で賭け金を算出する。

        Kelly 比率 f = (b·p - q) / b
          b = net odds = win_odds - 1
          p = 推定的中確率 = ev_pred / win_odds
          q = 1 - p
        """
        if ev_pred <= 0 or win_odds <= 1.1:
            return 0
        b = win_odds - 1.0
        p = ev_pred / win_odds  # EV から implied prob を逆算
        p = min(p, 0.95)  # 上限 95%
        q = 1.0 - p
        f = (b * p - q) / b
        if f <= 0:
            return 0
        raw_bet = int(bankroll * f * KELLY_FRACTION)
        raw_bet = (raw_bet // 100) * 100
        return int(np.clip(raw_bet, MIN_BET, MAX_BET))

    def generate_buy_signals(
        self,
        df: pd.DataFrame,
        ev_threshold: float = ALPHA_EV_THRESHOLD,
        bankroll: int = 100_000,
    ) -> pd.DataFrame:
        """
        EV > ev_threshold の馬のみ buy シグナルを生成する。

        Returns:
            race_id / horse_number / ev_pred / kelly_bet / win_odds の DataFrame
        """
        ev_pred = self.predict_ev(df)
        results = df[["race_id", "horse_number", "win_odds", "popularity"]].copy()
        results["ev_pred"] = ev_pred.values
        results["kelly_bet"] = results.apply(
            lambda row: self.calc_kelly_bet(
                row["ev_pred"],
                float(row["win_odds"]) if row["win_odds"] else 50,
                bankroll,
            ),
            axis=1,
        )
        buy_mask = results["ev_pred"] >= ev_threshold
        return results[buy_mask].reset_index(drop=True)

    # ── 保存・ロード ─────────────────────────────────────────────────

    def save(self, path: Path | None = None) -> None:
        path = path or _MODEL_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(
                {"model": self._model, "label_encoders": self._label_encoders}, f
            )
        logger.info("ALPHA モデル保存: %s", path)

    @classmethod
    def load(cls, path: Path | None = None) -> "AlphaModel":
        path = path or _MODEL_PATH
        obj = cls()
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj._model = data["model"]
        obj._label_encoders = data.get("label_encoders", {})
        obj._is_trained = True
        logger.info("ALPHA モデルロード: %s", path)
        return obj


# ── バックテスト ─────────────────────────────────────────────────────


def run_backtest(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_years: list[int],
    bet_type: str = BET_TYPE_TANSHO,
    ev_threshold: float = ALPHA_EV_THRESHOLD,
    bankroll: int = 100_000,
    verbose: bool = True,
    holdout_ratio: float = 0.20,
    research_db_path: Path | None = None,
) -> AlphaBacktestResult:
    """
    時系列バックテストを実行する。

    カンニング防止:
      - train_years のデータのみで学習
      - test_years のデータのみで評価（データ不足時は holdout_ratio で代替）
      - 時系列順序を守り、未来情報を使わない

    holdout_ratio:
      test_years のデータが 100 行未満の場合、train_years データの末尾
      holdout_ratio 割合をテスト用に使う（疑似ウォークフォワード）。
    """
    model = AlphaModel()

    # ── 学習データロード ────────────────────────────────────────────
    logger.info("=== バックテスト 学習フェーズ: %s ===", train_years)
    all_train_df = model.load_training_data(
        conn, train_years, bet_type, research_db_path=research_db_path
    )
    if len(all_train_df) < 500:
        raise ValueError(f"学習データ不足: {len(all_train_df)} 行 (最小 500 行)")

    # ── テストデータ確認（フォールバック判定） ─────────────────────
    logger.info("=== バックテスト テストフェーズ: %s ===", test_years)
    test_df = model.load_training_data(
        conn, test_years, bet_type, research_db_path=research_db_path
    )

    if len(test_df) < 100:
        # テストデータ不足 → 訓練データの末尾をホールドアウトに転用
        split_idx = int(len(all_train_df) * (1.0 - holdout_ratio))
        train_df = all_train_df.iloc[:split_idx].copy()
        test_df = all_train_df.iloc[split_idx:].copy()
        test_label = f"{train_years[0]}(holdout={int(holdout_ratio * 100)}%)"
        logger.warning(
            "テストデータ不足 → %s末尾%d%%をホールドアウトとして使用 (%d行)",
            train_years,
            int(holdout_ratio * 100),
            len(test_df),
        )
        if verbose:
            print(
                f"[警告] {test_years}のテストデータが不足。"
                f"{train_years}データの末尾{int(holdout_ratio * 100)}%({len(test_df)}行)を代用。"
            )
    else:
        train_df = all_train_df
        test_label = str(test_years[0]) if len(test_years) == 1 else str(test_years[-1])

    if len(test_df) < 50:
        raise ValueError(f"テストデータ不足: {len(test_df)} 行")

    metrics = model.train(train_df)
    if verbose:
        print(
            f"[学習] n={metrics['n_train']} LogLoss={metrics['logloss']:.4f} AUC={metrics['auc']:.3f}"
        )

    test_df["ev_pred"] = model.predict_ev(test_df).values

    # ── 投資シミュレーション ────────────────────────────────────────
    # EV > threshold の馬のみ bet
    buy_mask = test_df["ev_pred"] >= ev_threshold
    bets_df = test_df[buy_mask].copy()

    if len(bets_df) == 0:
        logger.warning("買いシグナルなし (threshold=%.2f)", ev_threshold)
        return AlphaBacktestResult(
            year=test_label,
            total_investment=0,
            total_payout=0,
            profit=0,
            roi=0,
            num_bets=0,
            num_hits=0,
            hit_rate=0,
            max_drawdown=0,
            best_single_payout=0,
            bet_type=bet_type,
            ev_threshold=ev_threshold,
            notes=["買いシグナルなし"],
        )

    # Kelly ベット金額
    bets_df["kelly_bet"] = bets_df.apply(
        lambda row: model.calc_kelly_bet(
            row["ev_pred"],
            float(row["win_odds"]) if pd.notna(row["win_odds"]) else 50,
            bankroll,
        ),
        axis=1,
    )
    # 固定 Y100 でも計算（比較用）
    bets_df["fixed_bet"] = 100

    # 払戻計算
    bets_df["payout_kelly"] = (
        bets_df["is_hit"]
        * bets_df["actual_payout"].fillna(0)
        * bets_df["kelly_bet"]
        / 100
    )
    bets_df["payout_fixed"] = bets_df["is_hit"] * bets_df["actual_payout"].fillna(0)

    total_investment = int(bets_df["kelly_bet"].sum())
    total_payout = float(bets_df["payout_kelly"].sum())
    profit = total_payout - total_investment
    roi = total_payout / total_investment * 100 if total_investment > 0 else 0

    num_hits = int(bets_df["is_hit"].sum())
    num_bets = len(bets_df)
    hit_rate = num_hits / num_bets * 100 if num_bets > 0 else 0

    # 最大ドローダウン計算
    bets_df_sorted = bets_df.sort_values(["date", "race_id", "horse_number"])
    cumulative_pnl = (
        bets_df_sorted["payout_kelly"] - bets_df_sorted["kelly_bet"]
    ).cumsum()
    running_max = cumulative_pnl.cummax()
    drawdown_series = running_max - cumulative_pnl
    max_drawdown = float(drawdown_series.max()) if len(drawdown_series) > 0 else 0

    best_payout = float(bets_df["payout_kelly"].max()) if len(bets_df) > 0 else 0

    result = AlphaBacktestResult(
        year=test_label,
        total_investment=total_investment,
        total_payout=total_payout,
        profit=profit,
        roi=roi,
        num_bets=num_bets,
        num_hits=num_hits,
        hit_rate=hit_rate,
        max_drawdown=max_drawdown,
        best_single_payout=best_payout,
        bet_type=bet_type,
        ev_threshold=ev_threshold,
    )

    if verbose:
        print(f"\n[バックテスト結果] {test_years}")
        print(f"  馬券種: {bet_type}")
        print(f"  買いシグナル: {num_bets} 件 (EV>{ev_threshold})")
        print(f"  的中: {num_hits} 件 ({hit_rate:.1f}%)")
        print(f"  総投資: Y{total_investment:,}")
        print(f"  総払戻: Y{total_payout:,.0f}")
        print(f"  損益: Y{profit:+,.0f}")
        print(f"  ROI: {roi:.1f}%")
        print(f"  最大ドローダウン: Y{max_drawdown:,.0f}")
        print(f"  最高単発払戻: Y{best_payout:,.0f}")

    return result


# ── ユーティリティ ────────────────────────────────────────────────────


def lgb_early_stopping(stopping_rounds: int, verbose: bool = True):
    """LightGBM の early stopping コールバック（バージョン差異吸収）。"""
    try:
        from lightgbm import early_stopping as _es

        return _es(stopping_rounds, verbose=verbose)
    except ImportError:
        return None
