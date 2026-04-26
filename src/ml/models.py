"""
競馬予想 AI モデル

HonmeiModel: 本命モデル（LightGBM 2値分類 — 1着確率）
ManjiModel:  卍モデル  （LightGBM 回帰     — 期待回収率）

どちらも train() / predict() / save() / load() インターフェイスを持つ。
学習データが不足している場合はオッズ・人気ベースのフォールバック予測を返す。
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

# ── 特徴量列定義 ───────────────────────────────────────────────────
# features.py の build_race_features_for_simulate() と一致させること
FEATURE_COLS: list[str] = [
    # ── 馬能力・レース条件特徴量 ──────────────────────────────────
    # オッズ系 (win_odds / market_prob / popularity) は除外:
    #   「市場の予想を覚える」だけになり真の予測力が測れないため。
    "weight_carried",           # 斤量
    "horse_weight",             # 馬体重
    "win_rate_all",             # 通算勝率
    "win_rate_surface",         # 馬場別勝率
    "win_rate_distance_band",   # 距離帯別勝率
    "recent_rank_mean",         # 直近5走平均着順
    "surface_code",             # 馬場コード (芝=0, ダート=1, 障害=2)
    "sex_code",                 # 性別コード (牡=0, 牝=1, セ=2)
    "venue_encoded",            # 開催場コード
    "sire_encoded",             # 父馬エンコード
    "distance",                 # 距離
    "horse_weight_diff",        # 前走比体重増減
    "gate_number",              # 枠番
    "condition_code",           # 馬場状態コード (良=0, 稍重=1, 重=2, 不良=3)
    "race_number",              # レース番号（新馬戦 vs 条件戦の識別）
    # ── 人的要素特徴量（真の予測力） ────────────────────────────
    "jockey_code_encoded",      # 騎手コードエンコード（jockeys マスタ）
    "trainer_code_encoded",     # 調教師コードエンコード（trainers マスタ）
    # ── 調教特徴量（WOOD:TC ウッド調教） ────────────────────────
    # データ未取得時は fillna(-1) で -1 に統一されるため学習は正常続行。
    # WOOD データ取得後に再学習すると有効化される。
    "tc_4f",            # ウッド直近4Fタイム（秒）— 小さいほど好時計
    "tc_lap",           # ウッド直近ラスト1Fタイム（秒）
    "tc_accel_flag",    # ウッド加速ラップ (1=ラスト加速=好調サイン, 0=失速)
    "tc_4f_diff",       # ウッド前回比4Fタイム差（秒, 負=好転=状態上向き）
    # ── 調教特徴量（WOOD:HC 坂路調教） ──────────────────────────
    "hc_4f",            # 坂路直近4Fタイム（秒）
    "hc_lap",           # 坂路直近ラスト1Fタイム（秒）
    "hc_accel_flag",    # 坂路加速ラップフラグ
    "hc_4f_diff",       # 坂路前回比4Fタイム差（秒, 負=好転）
    # ── レース内相対特徴量（groupby 的中率・調教強度の相対比較） ─
    # 1 = レース内最良。全馬 NaN の場合は欠損扱い（LightGBM がハンドル）。
    "win_rate_all_rank",         # レース内通算勝率ランク (1=最高勝率)
    "win_rate_all_zscore",       # レース内通算勝率偏差 (高=良)
    "win_rate_surface_rank",     # レース内馬場別勝率ランク
    "win_rate_distance_band_rank",  # レース内距離帯別勝率ランク
    "recent_rank_mean_rank",     # レース内直近着順ランク (1=最好調)
    "recent_rank_mean_zscore",   # レース内直近着順偏差 (高=良)
    "tc_4f_rank",                # レース内調教4Fタイムランク (1=最速)
    "tc_4f_zscore",              # レース内調教4F偏差 (高=良)
    # ── 当日バイアス特徴量 ──────────────────────────────────────
    # レース当日の確定済み結果から算出（リーク防止: 当日レース番号より前のみ）。
    # 1Rは全て None → LightGBM の欠損扱い。完了レース数が増えるほど信頼度向上。
    "today_inner_bias",   # 内枠(1-4)勝率 - 外枠(5-8)勝率（正=内枠有利）
    "today_front_bias",   # 当日の人気1-3馬勝率（高=展開安定=先行バイアス代理変数）
    "today_race_count",   # 集計対象レース数（モデルへの信頼度シグナル）
    "today_gate_match",   # today_inner_bias × (内枠:+1 / 外枠:-1) = この馬の枠番との相性
    # ── オッズ時系列特徴量（大口投票シグナル） ──────────────────
    # realtime_odds に複数スナップショットが記録されている場合のみ有効。
    # 訓練データ（シミュレーション）では常に NaN → 実際の prerace データで再学習後に有効化。
    "odds_vs_morning",    # 直前オッズ / 朝一オッズ（1未満=短縮=大口流入の疑い）
    "odds_velocity",      # 直近1時間のオッズ下落速度（オッズ/分、正値=資金流入中）
]

# 訓練に最低限必要なレース数
_MIN_TRAIN_RACES = 30

# デフォルトモデル保存先
_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models"


# ── 学習データ構築 ─────────────────────────────────────────────────

def _build_train_df(
    conn: sqlite3.Connection,
    train_until: int | None = None,
) -> pd.DataFrame:
    """
    FeatureBuilder を使ってリーク排除済みの学習 DataFrame を生成する。

    **リーク排除の仕組み**
    FeatureBuilder.build_race_features_for_simulate() 内で
    _get_horse_stats(exclude_race_id=race_id) を呼ぶため、
    各馬の通算勝率・直近着順は「そのレース自身を除いた過去成績」に基づく。
    これにより将来の着順が特徴量に混入するデータリークを完全に防ぐ。

    Args:
        conn:        DB 接続
        train_until: 学習に使う最終年（例: 2023 → 2023年以前のみ）。
                     None の場合は全期間を使用。

    **目的変数**
    - is_winner  : 1着 = 1 (本命モデル)
    - is_placed  : 3着以内 = 1 (複勝モデル: サンプルが3倍になり学習安定)
    - ev_target  : 実単勝払戻 (卍モデル)。payout_tansho が取得済みならその値を、
                   未取得 (NULL) かつ 1着の場合は win_odds × 100 で近似する。
    """
    from src.ml.features import FeatureBuilder

    # 着順が確定しているレース ID を日付昇順で取得
    # train_until 指定時はその年以前のみに絞る（時系列分割・アウト・オブ・サンプル評価用）
    if train_until is not None:
        race_rows = conn.execute(
            """
            SELECT DISTINCT r.race_id
            FROM   races r
            JOIN   race_results rr ON rr.race_id = r.race_id
            WHERE  rr.rank IS NOT NULL
            AND    CAST(substr(r.date, 1, 4) AS INTEGER) <= ?
            ORDER  BY r.date
            """,
            (train_until,),
        ).fetchall()
    else:
        race_rows = conn.execute(
            """
            SELECT DISTINCT r.race_id
            FROM   races r
            JOIN   race_results rr ON rr.race_id = r.race_id
            WHERE  rr.rank IS NOT NULL
            ORDER  BY r.date
            """
        ).fetchall()

    if not race_rows:
        return pd.DataFrame()

    fb = FeatureBuilder(conn)
    frames: list[pd.DataFrame] = []

    for (race_id,) in race_rows:
        df_feat = fb.build_race_features_for_simulate(race_id)
        if df_feat.empty:
            continue

        # 着順・実払戻を horse_name キーで結合
        # rr.horse_number (実際の馬番) で race_payouts と照合する
        actual_rows = conn.execute(
            """
            SELECT
                rr.horse_name,
                rr.rank,
                rp.payout AS payout_tansho
            FROM   race_results rr
            LEFT JOIN race_payouts rp
                   ON  rp.race_id     = rr.race_id
                   AND rp.bet_type    = '単勝'
                   AND rp.combination = CAST(rr.horse_number AS TEXT)
            WHERE  rr.race_id = ?
            AND    rr.rank    IS NOT NULL
            """,
            (race_id,),
        ).fetchall()

        if not actual_rows:
            continue

        actuals = pd.DataFrame(actual_rows, columns=["horse_name", "rank", "payout_tansho"])
        df_feat = df_feat.merge(actuals, on="horse_name", how="inner")
        df_feat["race_id"] = race_id
        frames.append(df_feat)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # ── 目的変数 ──────────────────────────────────────────────────
    df["is_winner"] = (df["rank"] == 1).astype(int)
    df["is_placed"]  = (df["rank"] <= 3).astype(int)
    # ev_target: 実払戻が利用可能ならそれを優先し、未取得(NULL)かつ1着なら win_odds×100 で近似
    # np.where は mixed-type で object になるため明示的に float へキャスト
    df["ev_target"] = np.where(
        df["payout_tansho"].notna(),
        df["payout_tansho"].astype(float),
        np.where(
            df["rank"] == 1,
            df["win_odds"].fillna(0).astype(float) * 100.0,
            0.0,
        ),
    ).astype(float)

    return df


@dataclass
class _IsotonicModel:
    """
    LGBMClassifier + Isotonic Regression（OOF キャリブレーション）の複合モデル。

    Phase 2.5 実験 (scripts/experiments/test_calibration.py) で
    Platt Scaling より Brier Score が +3.49% 改善することを確認済み。
    OOF 予測に対して IsotonicRegression(out_of_bounds="clip") を適用し
    確率を実際の勝率に近づける。
    pickle 可能なデータクラスとして実装し、save/load と完全互換。
    """

    base: LGBMClassifier
    iso: IsotonicRegression

    def predict_proba(self, X: Any) -> np.ndarray:
        """Isotonic calibrated P(win) を返す。shape=(n,2) — [:, 1] が P(win)。"""
        raw = self.base.predict_proba(X)[:, 1]
        calibrated = self.iso.predict(raw).astype(float)
        calibrated = np.clip(calibrated, 0.0, 1.0)
        return np.column_stack([1.0 - calibrated, calibrated])

    @property
    def feature_importances_(self) -> np.ndarray:
        """LightGBM の特徴量重要度（base モデルから取得）。"""
        return self.base.feature_importances_


@dataclass
class _PlattModel:
    """
    LGBMClassifier + Platt Scaling (LogisticRegression) の複合モデル。

    後方互換のために保持。新規訓練は _IsotonicModel を使用。
    既存の pkl ファイルが _PlattModel 型のまま読み込まれた場合でも
    predict_proba インターフェイスは同一のため predict() は正常動作する。
    """

    base: LGBMClassifier
    platt: LogisticRegression

    def predict_proba(self, X: Any) -> np.ndarray:
        """calibrated P(win) を返す。shape=(n,2) — [:, 1] が P(win)。"""
        scores = self.base.predict_proba(X)[:, 1].reshape(-1, 1)
        return self.platt.predict_proba(scores)

    @property
    def feature_importances_(self) -> np.ndarray:
        return self.base.feature_importances_


# ── ベースクラス ──────────────────────────────────────────────────

class _BaseModel:
    """本命・卍共通の基底クラス。"""

    _model: Any
    _trained: bool = False

    def save(self, path: Path | None = None) -> Path:
        """モデルを pickle で保存する。"""
        save_path = path or (_MODEL_DIR / f"{self._filename}.pkl")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            pickle.dump(self._model, f)
        logger.info("モデル保存: %s", save_path)
        return save_path

    def load(self, path: Path | None = None) -> None:
        """保存済みモデルを読み込む。"""
        load_path = path or (_MODEL_DIR / f"{self._filename}.pkl")
        if not load_path.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {load_path}")
        with open(load_path, "rb") as f:
            self._model = pickle.load(f)
        self._trained = True
        logger.info("モデル読み込み: %s", load_path)

    @property
    def is_trained(self) -> bool:
        return self._trained

    def _fallback_predict(self, df: pd.DataFrame) -> pd.Series:
        """
        訓練済みモデルがない場合のフォールバック。
        win_odds を反転して確率風スコアを返す（低オッズ馬 → 最高スコア）。
        """
        odds = df["win_odds"].fillna(100.0).clip(lower=1.0)
        score = 1.0 / odds
        return score / score.sum() if score.sum() > 0 else score


# ── 本命モデル ────────────────────────────────────────────────────

class HonmeiModel(_BaseModel):
    """
    本命モデル（的中率特化）。

    LightGBM 2値分類 + Isotonic Regression（OOF キャリブレーション）で
    各馬の 1着確率 P(rank=1) を推定する。

    Phase 2.5 実験でキャリブレーション手法を比較した結果、
    Isotonic Regression が Platt Scaling より Brier Score +3.49% 優位なため採用。

    学習フロー:
      1. GroupKFold(5分割) CV を実施し ROC-AUC 計算 + OOF 予測を収集
      2. OOF 予測を使った IsotonicRegression でキャリブレーション適用
      3. 全データで LGBMClassifier を本訓練（推論・特徴量重要度算出用）
      4. Champion/Challenger: 末尾 20% ホールドアウトで既存保存モデルと AUC を比較
    """

    _filename = "honmei_model"

    # LGBMClassifier に渡すパラメータ（clone で再利用するため定数として保持）
    _LGBM_PARAMS: dict[str, Any] = dict(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=10,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )

    def __init__(self) -> None:
        # _model: 推論で使う _PlattModel（None = 未訓練）
        self._model: Any = None
        # _base_lgbm: 特徴量重要度取得用の raw LGBMClassifier
        self._base_lgbm: LGBMClassifier = LGBMClassifier(**self._LGBM_PARAMS)
        self._trained = False

    def train(
        self,
        conn: sqlite3.Connection,
        train_until: int | None = None,
    ) -> dict[str, Any]:
        """
        DB の race_results から学習データを構築して訓練する。

        学習フロー:
          1. 全データを race_id でソートし末尾 20% を Champion 比較用ホールドアウトに分割
          2. 全データで GroupKFold(5分割) CV を実施し ROC-AUC を計算
          3. GroupKFold の OOF 予測を使って Platt Scaling (LogisticRegression) を適用
             → 追加 LGBM 訓練なしにキャリブレーション済み確率を取得
          4. 全データで base LGBMClassifier を本訓練（推論用）
          5. Champion/Challenger 比較: ホールドアウトセットで既存保存モデルと AUC を比較

        Args:
            conn:        DB 接続
            train_until: 学習に使う最終年（例: 2023 → 2023年以前のみ）。
                         None の場合は全期間を使用。

        Returns:
            {
              "n_races":        学習レース数,
              "n_samples":      学習サンプル数,
              "cv_auc_mean":    5-fold CV の平均 ROC-AUC,
              "cv_auc_std":     同 標準偏差,
              "challenger_auc": ホールドアウトセット上の新モデル AUC,
              "champion_auc":   同セット上の既存保存モデル AUC（なければ NaN）,
              "train_until":    使用した最終年（None なら全期間）,
            }
        """
        df = _build_train_df(conn, train_until=train_until)
        if df.empty:
            logger.warning("学習データが0件のため訓練をスキップします")
            return {
                "n_races": 0, "n_samples": 0,
                "cv_auc_mean": float("nan"), "cv_auc_std": float("nan"),
                "challenger_auc": float("nan"), "champion_auc": float("nan"),
            }

        n_races = df["race_id"].nunique()
        if n_races < _MIN_TRAIN_RACES:
            logger.warning(
                "学習レース数が少ないです (%d 件、推奨 %d 件以上)。精度が低い可能性があります。",
                n_races, _MIN_TRAIN_RACES,
            )

        # ── データ準備 ──────────────────────────────────────────
        # 時系列順にソートし末尾 20% を Champion 比較用ホールドアウトとして確保
        df_sorted = df.sort_values("race_id").reset_index(drop=True)
        n = len(df_sorted)
        cal_n = max(1, int(n * 0.2))
        df_cal = df_sorted.iloc[n - cal_n :]          # ホールドアウト (20%)

        X_cal  = df_cal[FEATURE_COLS].astype(float).fillna(-1)
        y_cal  = df_cal["is_winner"]
        X_all  = df_sorted[FEATURE_COLS].astype(float).fillna(-1)
        y_all  = df_sorted["is_winner"]
        groups = df_sorted["race_id"]

        # ── GroupKFold CV + OOF 予測収集 ────────────────────────
        # race_id でグループを切るため同一レースの馬が train/val に分かれない（リーク防止）
        # OOF 予測は Platt Scaling のキャリブレーションデータとして再利用する
        # → 追加 LGBM 訓練なしに信頼性の高い確率補正が可能
        n_splits = min(5, n_races)
        aucs: list[float] = []
        oof_preds = np.zeros(len(X_all), dtype=float)

        if n_splits >= 2:
            gkf = GroupKFold(n_splits=n_splits)
            for tr_idx, val_idx in gkf.split(X_all, y_all, groups=groups):
                clone = LGBMClassifier(**self._LGBM_PARAMS)
                clone.fit(X_all.iloc[tr_idx], y_all.iloc[tr_idx])
                proba = clone.predict_proba(X_all.iloc[val_idx])[:, 1]
                oof_preds[val_idx] = proba
                try:
                    aucs.append(roc_auc_score(y_all.iloc[val_idx], proba))
                except ValueError:
                    # fold に正例（1着馬）がない稀なケースをスキップ
                    pass

        cv_auc_mean = float(np.mean(aucs)) if aucs else float("nan")
        cv_auc_std  = float(np.std(aucs))  if aucs else float("nan")

        # ── Isotonic Regression: OOF 予測 → 確率キャリブレーション ──
        # Phase 2.5 実験で Platt Scaling より Brier Score +3.49% 改善を確認。
        # CalibratedClassifierCV + GroupKFold は新 sklearn で groups 受け渡し不可のため
        # 手動 OOF 実装（Platt と同じ oof_preds を再利用）。
        iso = IsotonicRegression(out_of_bounds="clip")
        if np.any(oof_preds != 0):
            iso.fit(oof_preds, y_all)
        else:
            # OOF 収集不可（n_splits < 2）: 全サンプルで単純 fit
            iso.fit(np.zeros(len(y_all)), y_all)

        # ── 全データで base LGBMClassifier を本訓練 ─────────────
        self._base_lgbm = LGBMClassifier(**self._LGBM_PARAMS)
        self._base_lgbm.fit(X_all, y_all)
        self._model = _IsotonicModel(base=self._base_lgbm, iso=iso)
        self._trained = True

        # ── 特徴量重要度 Top10 ───────────────────────────────────
        importances: np.ndarray = self._base_lgbm.feature_importances_
        feat_imp_pairs = sorted(
            zip(FEATURE_COLS, importances.tolist()),
            key=lambda t: t[1],
            reverse=True,
        )
        logger.info("【特徴量重要度 Top10 — 本命モデル】")
        for rank_i, (feat, imp) in enumerate(feat_imp_pairs[:10], 1):
            logger.info("  %2d. %-30s  gain=%d", rank_i, feat, int(imp))

        # ── Challenger AUC（ホールドアウトセットでの新モデル AUC） ─
        challenger_auc = float("nan")
        try:
            chal_proba = self._model.predict_proba(X_cal)[:, 1]
            challenger_auc = float(roc_auc_score(y_cal, chal_proba))
        except ValueError:
            pass

        # ── Champion AUC（既存保存モデルの同セット上の AUC） ─────
        champion_auc = float("nan")
        champion_path = _MODEL_DIR / f"{self._filename}.pkl"
        if champion_path.exists():
            try:
                with open(champion_path, "rb") as f:
                    champion_model = pickle.load(f)
                champ_proba = champion_model.predict_proba(X_cal)[:, 1]
                champion_auc = float(roc_auc_score(y_cal, champ_proba))
                logger.info(
                    "Champion/Challenger: champion AUC=%.4f / challenger AUC=%.4f",
                    champion_auc, challenger_auc,
                )
            except Exception as e:
                logger.warning("チャンピオンモデルの評価に失敗（スキップ）: %s", e)

        logger.info(
            "本命モデル訓練完了: %d レース / %d サンプル / CV AUC %.4f ±%.4f",
            n_races, len(df), cv_auc_mean, cv_auc_std,
        )
        return {
            "n_races":        n_races,
            "n_samples":      len(df),
            "cv_auc_mean":    cv_auc_mean,
            "cv_auc_std":     cv_auc_std,
            "challenger_auc": challenger_auc,
            "champion_auc":   champion_auc,
            "train_until":    train_until,
        }

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """
        特徴量 DataFrame を受け取り、各馬の Isotonic-calibrated 1着確率スコアを返す。

        Args:
            df: FeatureBuilder.build_race_features() の出力（horse_number インデックス）

        Returns:
            pd.Series (index=df.index, values=P(win) 0〜1, Isotonic Regression 補正済み)
        """
        if not self._trained:
            logger.debug("未訓練モデル — フォールバック予測を使用")
            return self._fallback_predict(df)

        X = df[FEATURE_COLS].astype(float).fillna(-1)
        proba = self._model.predict_proba(X)[:, 1]
        return pd.Series(proba, index=df.index, name="honmei_score")

    def ev_predict(self, df: pd.DataFrame) -> pd.Series:
        """
        EV = P(win) × win_odds を算出して返す。

        EV >= 1.0 → 期待値プラス（購入推奨）。
        win_odds が欠損の馬は EV = 0.0 を返す。

        Returns:
            pd.Series (index=df.index, values=EV 値)
        """
        p_win = self.predict(df)
        odds  = df["win_odds"].fillna(0.0).astype(float)
        return (p_win * odds).rename("ev_score")


# ── 卍モデル ──────────────────────────────────────────────────────

class ManjiModel(_BaseModel):
    """
    卍モデル（回収率・期待値特化）。

    LightGBM 回帰で「期待回収額（100円ベット時の払戻期待値）」を推定する。
    EV_score = predicted_payout / 100 が 1.0 超 → 期待値プラスの判断基準。
    """

    _filename = "manji_model"

    def __init__(self) -> None:
        self._model = LGBMRegressor(
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=10,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbose=-1,
        )
        self._trained = False

    def train(
        self,
        conn: sqlite3.Connection,
        train_until: int | None = None,
    ) -> dict[str, float]:
        """
        DB の race_results から学習データを構築して訓練する。

        Args:
            conn:        DB 接続
            train_until: 学習に使う最終年（例: 2023 → 2023年以前のみ）。
                         None の場合は全期間を使用。

        Returns:
            {"n_races": 学習レース数, "n_samples": 学習サンプル数}
        """
        df = _build_train_df(conn, train_until=train_until)
        if df.empty:
            logger.warning("学習データが0件のため訓練をスキップします")
            return {"n_races": 0, "n_samples": 0}

        n_races = df["race_id"].nunique()
        X = df[FEATURE_COLS].astype(float).fillna(-1)
        y = df["ev_target"]

        self._model.fit(X, y)
        self._trained = True

        logger.info("卍モデル訓練完了: %d レース / %d サンプル", n_races, len(df))
        return {"n_races": n_races, "n_samples": len(df), "train_until": train_until}

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """
        特徴量 DataFrame を受け取り、各馬の期待回収スコアを返す。

        Returns:
            pd.Series (index=df.index, values=expected_payout per 100 yen)
            EV_score = values / 100 → 1.0 超が期待値プラスの目安
        """
        if not self._trained:
            logger.debug("未訓練モデル — フォールバック予測を使用")
            # フォールバック: win_odds が高い（穴馬）ほど期待回収率が高いと仮定
            odds = df["win_odds"].fillna(1.0)
            return pd.Series(odds, index=df.index, name="manji_score")

        X = df[FEATURE_COLS].astype(float).fillna(-1)
        pred = self._model.predict(X)
        return pd.Series(pred.clip(min=0), index=df.index, name="manji_score")

    def ev_score(self, df: pd.DataFrame) -> pd.Series:
        """predict() の値を 100 で割って EV 比率（1.0 基準）に変換する。"""
        return self.predict(df) / 100.0


# ── 学習エントリポイント ──────────────────────────────────────────

def train_all(
    conn: sqlite3.Connection,
    train_until: int | None = None,
) -> dict[str, dict]:
    """
    本命・卍モデルを両方訓練して data/models/ に保存する。

    本命モデルは Champion/Challenger 比較を行い、
    challenger_auc >= champion_auc - 0.005 の場合のみ保存（世代交代）。
    champion が存在しない場合・AUC 比較不能の場合は無条件で保存する。

    Args:
        conn:        DB 接続
        train_until: 学習に使う最終年（None なら全期間）

    Usage:
        conn = init_db()
        result = train_all(conn, train_until=2023)
    """
    honmei = HonmeiModel()
    manji  = ManjiModel()

    h_result = honmei.train(conn, train_until=train_until)
    m_result = manji.train(conn, train_until=train_until)

    # ── 本命モデル: Champion/Challenger 判定 ─────────────────────
    if honmei.is_trained:
        challenger_auc: float = h_result.get("challenger_auc", float("nan"))
        champion_auc:   float = h_result.get("champion_auc",   float("nan"))

        # champion が存在しないか AUC 比較不能 → 無条件保存
        if np.isnan(champion_auc) or np.isnan(challenger_auc):
            honmei.save()
            h_result["promoted"] = True
        # challenger が champion を下回る場合（許容誤差 0.005）は却下
        elif challenger_auc >= champion_auc - 0.005:
            honmei.save()
            h_result["promoted"] = True
            logger.info(
                "世代交代: challenger AUC=%.4f >= champion AUC=%.4f",
                challenger_auc, champion_auc,
            )
        else:
            h_result["promoted"] = False
            logger.warning(
                "世代交代却下: challenger AUC=%.4f < champion AUC=%.4f (差=%.4f) — 既存モデルを維持",
                challenger_auc, champion_auc, champion_auc - challenger_auc,
            )

    # ── 卍モデル: 無条件保存 ──────────────────────────────────────
    if manji.is_trained:
        manji.save()

    return {"honmei": h_result, "manji": m_result}


def load_models() -> tuple[HonmeiModel, ManjiModel]:
    """
    保存済みモデルを読み込んで返す。
    存在しない場合は未訓練の新規インスタンスを返す（フォールバック動作）。
    """
    honmei = HonmeiModel()
    manji  = ManjiModel()

    try:
        honmei.load()
    except FileNotFoundError:
        logger.info("本命モデルが見つかりません — フォールバックモードで動作します")

    try:
        manji.load()
    except FileNotFoundError:
        logger.info("卍モデルが見つかりません — フォールバックモードで動作します")

    return honmei, manji
