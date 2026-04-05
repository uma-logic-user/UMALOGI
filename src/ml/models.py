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

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

# ── 特徴量列定義 ───────────────────────────────────────────────────
# features.py の build_race_features() と一致させること
FEATURE_COLS: list[str] = [
    "weight_carried",
    "horse_weight",
    "win_odds",
    "popularity",
    "win_rate_all",
    "win_rate_surface",
    "win_rate_distance_band",
    "recent_rank_mean",
    "surface_code",
    "sex_code",
    "venue_encoded",
    "sire_encoded",
    "distance",
]

# 訓練に最低限必要なレース数
_MIN_TRAIN_RACES = 30

# デフォルトモデル保存先
_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models"


# ── 学習データ構築 ─────────────────────────────────────────────────

def _build_train_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    race_results × races × horses を結合して学習用 DataFrame を生成。

    entries テーブルがなくても race_results から直接特徴量を組み立てる。
    (entries ベースの FeatureBuilder はリアルタイム予測用)
    """
    rows = conn.execute(
        """
        SELECT
            rr.race_id,
            rr.horse_id,
            rr.horse_name,
            rr.rank,
            rr.weight_carried,
            rr.horse_weight,
            rr.win_odds,
            rr.popularity,
            rr.sex_age,
            r.distance,
            r.surface,
            r.venue,
            r.date,
            h.sire
        FROM race_results rr
        JOIN  races r  ON rr.race_id  = r.race_id
        LEFT JOIN horses h ON rr.horse_id = h.horse_id
        WHERE rr.rank IS NOT NULL
        ORDER BY r.date, rr.race_id, rr.rank
        """,
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "race_id", "horse_id", "horse_name", "rank",
        "weight_carried", "horse_weight", "win_odds", "popularity",
        "sex_age", "distance", "surface", "venue", "date", "sire",
    ])

    # ── カテゴリエンコード ─────────────────────────────
    from src.ml.features import _SURFACE_CODE, _SEX_CODE, _VENUE_CODE, _parse_sex

    df["surface_code"]   = df["surface"].map(_SURFACE_CODE).fillna(-1).astype(int)
    df["sex_code"]       = df["sex_age"].map(lambda s: _SEX_CODE.get(_parse_sex(s), -1))
    df["venue_encoded"]  = df["venue"].map(_VENUE_CODE).fillna(len(_VENUE_CODE)).astype(int)

    le = LabelEncoder()
    df["sire_encoded"] = le.fit_transform(df["sire"].fillna("unknown"))

    # ── 馬成績特徴量（各行＝そのレース時点の過去成績）─
    # 簡略版: 全期間の集計（リーク防止は後続バージョンで対応）
    stats = (
        df.groupby("horse_id")["rank"]
        .agg(
            win_rate_all=lambda x: (x == 1).mean(),
            recent_rank_mean="mean",
        )
        .reset_index()
    )
    df = df.merge(stats, on="horse_id", how="left")

    # 馬場・距離帯成績（簡略: 馬場×horse_id）
    sf_stats = (
        df.groupby(["horse_id", "surface"])["rank"]
        .agg(win_rate_surface=lambda x: (x == 1).mean())
        .reset_index()
    )
    df = df.merge(sf_stats, on=["horse_id", "surface"], how="left")

    from src.ml.features import _distance_band
    df["dist_band"] = df["distance"].apply(_distance_band)
    db_stats = (
        df.groupby(["horse_id", "dist_band"])["rank"]
        .agg(win_rate_distance_band=lambda x: (x == 1).mean())
        .reset_index()
    )
    df = df.merge(db_stats, on=["horse_id", "dist_band"], how="left")

    # ── ターゲット ──────────────────────────────────────
    df["is_winner"] = (df["rank"] == 1).astype(int)
    # 卍ターゲット: 1着なら win_odds、外れなら 0（期待回収率の代理指標）
    df["ev_target"] = np.where(
        df["rank"] == 1,
        df["win_odds"].fillna(0) * 100,  # 100円ベットの払戻
        0.0,
    )

    return df


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
        人気順を反転して確率風スコアを返す（人気1位 → 最高スコア）。
        """
        pop = df["popularity"].fillna(df["popularity"].max() + 1)
        score = 1.0 / pop
        return score / score.sum() if score.sum() > 0 else score


# ── 本命モデル ────────────────────────────────────────────────────

class HonmeiModel(_BaseModel):
    """
    本命モデル（的中率特化）。

    LightGBM 2値分類で各馬の 1着確率 P(rank=1) を推定する。
    出力 predict() は各馬のスコア（高いほど有力）を pd.Series で返す。
    """

    _filename = "honmei_model"

    def __init__(self) -> None:
        self._model = LGBMClassifier(
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

    def train(self, conn: sqlite3.Connection) -> dict[str, float]:
        """
        DB の race_results から学習データを構築して訓練する。

        Returns:
            {"n_races": 学習レース数, "n_samples": 学習サンプル数}
        """
        df = _build_train_df(conn)
        if df.empty:
            logger.warning("学習データが0件のため訓練をスキップします")
            return {"n_races": 0, "n_samples": 0}

        n_races = df["race_id"].nunique()
        if n_races < _MIN_TRAIN_RACES:
            logger.warning(
                "学習レース数が少ないです (%d 件、推奨 %d 件以上)。"
                "精度が低い可能性があります。",
                n_races, _MIN_TRAIN_RACES,
            )

        X = df[FEATURE_COLS].fillna(-1)
        y = df["is_winner"]
        groups = df["race_id"]

        self._model.fit(X, y)
        self._trained = True

        logger.info("本命モデル訓練完了: %d レース / %d サンプル", n_races, len(df))
        return {"n_races": n_races, "n_samples": len(df)}

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """
        特徴量 DataFrame を受け取り、各馬の 1着確率スコアを返す。

        Args:
            df: FeatureBuilder.build_race_features() の出力（horse_number インデックス）

        Returns:
            pd.Series (index=df.index, values=P(win) 0〜1)
        """
        if not self._trained:
            logger.debug("未訓練モデル — フォールバック予測を使用")
            return self._fallback_predict(df)

        X = df[FEATURE_COLS].fillna(-1)
        proba = self._model.predict_proba(X)[:, 1]
        return pd.Series(proba, index=df.index, name="honmei_score")


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

    def train(self, conn: sqlite3.Connection) -> dict[str, float]:
        """
        DB の race_results から学習データを構築して訓練する。

        Returns:
            {"n_races": 学習レース数, "n_samples": 学習サンプル数}
        """
        df = _build_train_df(conn)
        if df.empty:
            logger.warning("学習データが0件のため訓練をスキップします")
            return {"n_races": 0, "n_samples": 0}

        n_races = df["race_id"].nunique()
        X = df[FEATURE_COLS].fillna(-1)
        y = df["ev_target"]

        self._model.fit(X, y)
        self._trained = True

        logger.info("卍モデル訓練完了: %d レース / %d サンプル", n_races, len(df))
        return {"n_races": n_races, "n_samples": len(df)}

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """
        特徴量 DataFrame を受け取り、各馬の期待回収スコアを返す。

        Returns:
            pd.Series (index=df.index, values=expected_payout per 100 yen)
            EV_score = values / 100 → 1.0 超が期待値プラスの目安
        """
        if not self._trained:
            logger.debug("未訓練モデル — フォールバック予測を使用")
            # フォールバック: win_odds が高い（穴馬）×人気が低い ほどスコア高
            odds = df["win_odds"].fillna(1.0)
            pop  = df["popularity"].fillna(df["popularity"].max() + 1)
            score = odds / pop
            return pd.Series(score, index=df.index, name="manji_score")

        X = df[FEATURE_COLS].fillna(-1)
        pred = self._model.predict(X)
        return pd.Series(pred.clip(min=0), index=df.index, name="manji_score")

    def ev_score(self, df: pd.DataFrame) -> pd.Series:
        """predict() の値を 100 で割って EV 比率（1.0 基準）に変換する。"""
        return self.predict(df) / 100.0


# ── 学習エントリポイント ──────────────────────────────────────────

def train_all(conn: sqlite3.Connection) -> dict[str, dict]:
    """
    本命・卍モデルを両方訓練して data/models/ に保存する。

    Usage:
        conn = init_db()
        result = train_all(conn)
    """
    honmei = HonmeiModel()
    manji  = ManjiModel()

    h_result = honmei.train(conn)
    m_result = manji.train(conn)

    if honmei.is_trained:
        honmei.save()
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
