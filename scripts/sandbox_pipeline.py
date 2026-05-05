"""
scripts/sandbox_pipeline.py — 特例サンドボックス全券種シミュレーション

【特例】netkeiba_research.db × umalogi.db の統合学習を隔離環境で実行。
本番 DB 設計には一切変更を加えない。

【アーキテクチャ】
  データ基盤: horse_odds (全馬オッズ) を基底テーブルとして使用。
  v_race_mart は rank=1-3 しか返さないため不使用。
  学習ラベル: race_results rank=1 = 勝ち馬
  払戻突合: race_payouts（全券種）

  訓練: 2024年 (horse_odds×races×race_results JOIN)
  テスト: 2025年 (カンニングなし、純粋 OOS)

  特徴量:
    - netkeiba 単勝オッズ・人気・implied_prob (特例追加)
    - 会場・距離・馬場・コース・レース番号 (公開情報)
    - フィールドサイズ (頭数による難易度調整)

Phase 1: 統合特徴量構築
Phase 2: サンドボックスモデル学習 (honmei/place/ev)
Phase 3: 全8券種バックテスト
Phase 4: ROI マトリクスレポート生成
Phase 5: ROI>120% 券種を bet_generator.py に自動実装

使い方:
  py scripts/sandbox_pipeline.py                # フル実行
  py scripts/sandbox_pipeline.py --skip-train   # 学習スキップ
  py scripts/sandbox_pipeline.py --report-only  # レポートのみ
"""

from __future__ import annotations

import argparse
import itertools
import logging
import pickle
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.preprocessing import LabelEncoder

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

ROOT        = Path(__file__).resolve().parent.parent
MAIN_DB     = ROOT / "data" / "umalogi.db"
RESEARCH_DB = ROOT / "data" / "netkeiba_research.db"
SANDBOX_DIR = ROOT / "data" / "models" / "sandbox"
DOCS_DIR    = ROOT / "docs"

SANDBOX_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_YEAR = "2024"
TEST_YEAR  = "2025"

ALL_BET_TYPES = ["単勝", "複勝", "枠連", "馬連", "ワイド", "馬単", "三連複", "三連単"]

AUTO_IMPLEMENT_ROI_THRESHOLD = 120.0
MIN_BETS_FOR_IMPLEMENT = 200

# ── 静的エンコーダー ──────────────────────────────────────────────
_SURFACE_MAP   = {"芝": 0, "ダート": 1, "障害": 2}
_CONDITION_MAP = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
_VENUE_MAP     = {
    "札幌": 0, "函館": 1, "福島": 2, "新潟": 3,
    "東京": 4, "中山": 5, "中京": 6, "京都": 7,
    "阪神": 8, "小倉": 9,
}

# LightGBM 共通設定
_LGB_BASE: dict[str, Any] = {
    "n_estimators": 600,
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 30,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "random_state": 42,
    "n_jobs": -1,
    "verbose": -1,
}

# ── 特徴量（全馬に共通で取得できるもの） ─────────────────────────
# netkeiba オッズ特徴量（特例追加）
# + レース公開情報（馬場・会場・距離など）
SANDBOX_FEATURE_COLS = [
    # ★ netkeiba オッズ特徴量（今回の特例: 通常は除外）
    # NOTE: horse_odds.rank は着順（カラーデータリーク）のため除外済み
    "nb_win_odds",       # 単勝オッズ（レース前公開情報）
    "nb_implied_prob",   # implied probability = (1/odds) / Σ(1/odds)
    "nb_log_odds",       # log1p(odds)  — 非線形性キャプチャ
    # レース公開情報
    "venue_code",        # 開催場コード
    "surface_code",      # 馬場コード (芝/ダート/障害)
    "condition_code",    # 馬場状態コード (良〜不良)
    "distance",          # 距離 (m)
    "race_number",       # レース番号 (1-12)
    "month",             # 開催月 (季節性)
    "race_n_horses",     # 頭数 (フィールドサイズ)
]


# ─────────────────────────────────────────────────────────────────
#  Phase 1: 統合特徴量構築
# ─────────────────────────────────────────────────────────────────

def build_sandbox_df(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    year_prefix: str,
    encoders: dict[str, LabelEncoder] | None = None,
    fit_encoders: bool = False,
) -> tuple[pd.DataFrame, dict[str, LabelEncoder]]:
    """
    horse_odds (全馬) + races + race_results を結合して sandbox DataFrame を生成。

    各レースの全出走馬 (horse_odds) を基底とし、
    race_results rank=1 で勝ち馬を特定してラベルを付与。

    Returns:
        (DataFrame, encoders)
    """
    logger.info("特徴量構築: year=%s", year_prefix)

    # ── 1. horse_odds 全馬データ ──────────────────────────────────
    odds_rows = res_conn.execute(
        """
        SELECT ho.race_id, ho.horse_number, ho.win_odds, ho.rank AS popularity
        FROM horse_odds ho
        WHERE substr(ho.race_id, 1, 4) = ?
        """,
        (year_prefix,),
    ).fetchall()

    if not odds_rows:
        logger.warning("horse_odds にデータがありません: year=%s", year_prefix)
        return pd.DataFrame(), encoders or {}

    df = pd.DataFrame(odds_rows, columns=["race_id", "horse_number", "nb_win_odds", "nb_popularity"])
    logger.info("  horse_odds: %d行 / %d レース", len(df), df["race_id"].nunique())

    # ── 2. races テーブルからレース情報を JOIN ────────────────────
    race_rows = conn.execute(
        """
        SELECT race_id, date, venue, surface, condition, distance, race_number
        FROM races
        WHERE date LIKE ?
        """,
        (f"{year_prefix}%",),
    ).fetchall()
    race_df = pd.DataFrame(
        race_rows,
        columns=["race_id", "date", "venue", "surface", "condition", "distance", "race_number"],
    )
    df = df.merge(race_df, on="race_id", how="inner")

    # ── 3. race_results から勝ち馬 / 2着 / 3着 を特定 ─────────────
    result_rows = conn.execute(
        """
        SELECT rr.race_id, rr.horse_number, rr.rank,
               rr.gate_number, rr.payout_is_win, rr.payout_is_place
        FROM (
          SELECT race_id, horse_number, rank, gate_number,
                 NULL as payout_is_win, NULL as payout_is_place
          FROM race_results
          WHERE race_id LIKE ?
            AND rank IN (1, 2, 3)
        ) rr
        """,
        (f"{year_prefix}%",),
    ).fetchall()

    result_df = pd.DataFrame(
        result_rows,
        columns=["race_id", "horse_number", "rank", "gate_number",
                 "payout_is_win", "payout_is_place"],
    )
    # race_results に payout_is_win/place カラムがない場合の fallback
    result_rows_simple = conn.execute(
        """
        SELECT race_id, horse_number, rank, gate_number
        FROM race_results
        WHERE race_id LIKE ?
          AND rank IN (1, 2, 3)
        """,
        (f"{year_prefix}%",),
    ).fetchall()
    result_df = pd.DataFrame(
        result_rows_simple,
        columns=["race_id", "horse_number", "rank", "gate_number"],
    )

    # payout_tansho / payout_fukusho を race_payouts から取得
    payout_rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        WHERE rp.race_id LIKE ?
          AND rp.bet_type IN ('単勝', '複勝')
        """,
        (f"{year_prefix}%",),
    ).fetchall()
    payout_df = pd.DataFrame(payout_rows, columns=["race_id", "bet_type", "combination", "payout"])

    # 単勝払戻 (race_id → payout)
    tansho = (
        payout_df[payout_df["bet_type"] == "単勝"]
        .set_index("race_id")["payout"]
        .to_dict()
    )

    # ── 4. ラベル付与 ─────────────────────────────────────────────
    # is_winner: race_results rank=1 の horse_number と一致するか
    winner_map: dict[str, int] = {}  # race_id → winning horse_number
    gate_map: dict[tuple[str, int], int] = {}  # (race_id, horse_number) → gate_number
    rank_map: dict[tuple[str, int], int] = {}

    for _, row in result_df.iterrows():
        rid = row["race_id"]
        hn  = int(row["horse_number"])
        rk  = int(row["rank"])
        gn  = int(row["gate_number"]) if pd.notna(row["gate_number"]) else 0
        gate_map[(rid, hn)] = gn
        rank_map[(rid, hn)] = rk
        if rk == 1:
            winner_map[rid] = hn

    df["is_winner"] = df.apply(
        lambda r: 1 if winner_map.get(r["race_id"]) == int(r["horse_number"]) else 0,
        axis=1,
    )
    df["is_placed"] = df.apply(
        lambda r: 1 if rank_map.get((r["race_id"], int(r["horse_number"])), 99) <= 3 else 0,
        axis=1,
    )
    df["gate_number"] = df.apply(
        lambda r: gate_map.get((r["race_id"], int(r["horse_number"])), 0),
        axis=1,
    )
    df["payout_tansho"] = df["race_id"].map(tansho).fillna(0.0)
    df["ev_target"] = df["is_winner"] * df["payout_tansho"]

    # ── 5. 特徴量エンジニアリング ─────────────────────────────────
    df["nb_win_odds"]   = pd.to_numeric(df["nb_win_odds"], errors="coerce")
    df["nb_popularity"] = pd.to_numeric(df["nb_popularity"], errors="coerce")

    # implied_prob (per race 正規化)
    df["nb_implied_prob"] = np.nan
    df["nb_log_odds"]     = np.nan
    df["nb_inv_pop"]      = np.nan
    df["race_n_horses"]   = 0

    for race_id, grp in df.groupby("race_id"):
        idx = grp.index
        df.loc[idx, "race_n_horses"] = len(grp)

        valid_odds = grp["nb_win_odds"].dropna()
        if len(valid_odds) > 0:
            inv = 1.0 / valid_odds.clip(lower=1.0)
            df.loc[idx[grp["nb_win_odds"].notna()], "nb_implied_prob"] = inv / inv.sum()
            df.loc[idx, "nb_log_odds"] = np.log1p(grp["nb_win_odds"])

        valid_pop = grp["nb_popularity"].dropna()
        df.loc[idx[grp["nb_popularity"].notna()], "nb_inv_pop"] = (
            1.0 / grp["nb_popularity"].clip(lower=1.0)
        )

    # カテゴリエンコード
    df["surface_code"]   = df["surface"].map(_SURFACE_MAP).fillna(-1).astype(int)
    df["condition_code"] = df["condition"].map(_CONDITION_MAP).fillna(-1).astype(int)
    df["venue_code"]     = df["venue"].map(_VENUE_MAP).fillna(-1).astype(int)
    df["distance"]       = pd.to_numeric(df["distance"], errors="coerce").fillna(1600)
    df["race_number"]    = pd.to_numeric(df["race_number"], errors="coerce").fillna(6)
    df["month"]          = df["date"].str[5:7].astype(int, errors="ignore")

    logger.info(
        "  勝ち馬ラベル付与: is_winner=%d / %d行 / %d レース",
        df["is_winner"].sum(), len(df), df["race_id"].nunique(),
    )
    logger.info(
        "  オッズカバレッジ: %.1f%%",
        df["nb_win_odds"].notna().mean() * 100,
    )
    return df, encoders or {}


# ─────────────────────────────────────────────────────────────────
#  Phase 2: サンドボックスモデル学習
# ─────────────────────────────────────────────────────────────────

class SandboxModels:
    """honmei (勝ち確率) / place (連対確率) / ev (期待払戻) 3モデルのコンテナ。"""

    def __init__(self) -> None:
        self.honmei: LGBMClassifier | None = None
        self.place:  LGBMClassifier | None = None
        self.ev:     LGBMRegressor  | None = None

    def train(self, train_df: pd.DataFrame) -> None:
        avail_feats = [c for c in SANDBOX_FEATURE_COLS if c in train_df.columns]
        X       = train_df[avail_feats].copy()
        y_win   = train_df["is_winner"].values
        y_place = train_df["is_placed"].values
        y_ev    = train_df["ev_target"].values

        logger.info(
            "  学習: %d行 / %d レース  特徴量=%d",
            len(X), train_df["race_id"].nunique(), len(avail_feats),
        )
        logger.info(
            "  正例: 単勝=%d / 複勝=%d", y_win.sum(), y_place.sum()
        )

        self.honmei = LGBMClassifier(**_LGB_BASE)
        self.honmei.fit(X, y_win)

        self.place = LGBMClassifier(**_LGB_BASE)
        self.place.fit(X, y_place)

        # EVモデル: 全馬でゼロが多い → GBDT Regression はそのまま学習
        self.ev = LGBMRegressor(**_LGB_BASE)
        self.ev.fit(X, y_ev)

        logger.info("  学習完了")

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        avail_feats = [c for c in SANDBOX_FEATURE_COLS if c in df.columns]
        X = df[avail_feats].copy()

        out = df[["race_id", "horse_number", "gate_number",
                   "is_winner", "is_placed"]].copy()
        out["win_prob"]   = self.honmei.predict_proba(X)[:, 1] if self.honmei else 0.0
        out["place_prob"] = self.place.predict_proba(X)[:, 1]  if self.place  else 0.0
        out["ev_score"]   = np.clip(self.ev.predict(X), 0, None) if self.ev else 0.0
        return out

    def save(self) -> None:
        path = SANDBOX_DIR / "sandbox_models.pkl"
        with open(path, "wb") as f:
            pickle.dump({"honmei": self.honmei, "place": self.place, "ev": self.ev}, f)
        logger.info("  モデル保存: %s", path)

    def load(self) -> bool:
        path = SANDBOX_DIR / "sandbox_models.pkl"
        if not path.exists():
            return False
        with open(path, "rb") as f:
            data = pickle.load(f)
        self.honmei = data.get("honmei")
        self.place  = data.get("place")
        self.ev     = data.get("ev")
        logger.info("  モデルロード: %s", path)
        return True


# ─────────────────────────────────────────────────────────────────
#  Phase 3: 全8券種バックテスト
# ─────────────────────────────────────────────────────────────────

def _parse_combo(bet_type: str, combination: str) -> tuple[int, ...]:
    """race_payouts の combination 文字列を int tuple に変換。"""
    try:
        if "→" in combination:
            return tuple(int(x) for x in combination.split("→"))
        elif "-" in combination:
            nums = [int(x) for x in combination.split("-")]
            if bet_type in ("馬単", "三連単"):
                return tuple(nums)
            return tuple(sorted(nums))
        else:
            return (int(combination),)
    except ValueError:
        return ()


def _lookup_payout(
    payouts: list[dict],
    bet_type: str,
    selected: tuple[int, ...],
) -> int:
    """選択した組み合わせが的中した場合の払戻額を返す（外れなら 0）。"""
    is_ordered = bet_type in ("馬単", "三連単")
    for p in payouts:
        if p["bet_type"] != bet_type:
            continue
        payout_combo = _parse_combo(bet_type, p["combination"])
        if not payout_combo:
            continue
        if is_ordered:
            if selected == payout_combo:
                return int(p["payout"])
        else:
            if tuple(sorted(selected)) == payout_combo:
                return int(p["payout"])
    return 0


def _simulate_one_race(
    race_pred: pd.DataFrame,
    payouts: list[dict],
) -> dict[str, tuple[int, int]]:
    """
    1レースの全8券種シミュレーション。

    各モデルスコア別に呼ばれるため、race_pred の "score" 列でソートする。

    Returns:
        {bet_type: (invested_yen, returned_yen)}
    """
    # スコア降順でソート
    sp = race_pred.sort_values("score", ascending=False)
    nums = [int(n) for n in sp["horse_number"].tolist()]

    if len(nums) < 2:
        return {}

    gate_map: dict[int, int] = dict(
        zip(race_pred["horse_number"].astype(int), race_pred["gate_number"].astype(int))
    )

    results: dict[str, tuple[int, int]] = {}

    # ── 単勝: スコア1位 ─────────────────────────────────────────
    ret = _lookup_payout(payouts, "単勝", (nums[0],))
    results["単勝"] = (100, ret)

    # ── 複勝: スコア上位3頭 (3点) ───────────────────────────────
    plc3 = nums[:3]
    plc_ret = sum(_lookup_payout(payouts, "複勝", (n,)) for n in plc3)
    results["複勝"] = (300, plc_ret)

    if len(nums) < 2:
        return results

    # ── 枠連: 上位2頭の枠番 ─────────────────────────────────────
    g1 = gate_map.get(nums[0], 0)
    g2 = gate_map.get(nums[1], 0)
    if g1 > 0 and g2 > 0:
        frames = tuple(sorted([g1, g2]))
        ret = _lookup_payout(payouts, "枠連", frames)
        results["枠連"] = (100, ret)

    # ── 馬連: 上位2頭 (順不問) ──────────────────────────────────
    combo = tuple(sorted(nums[:2]))
    ret = _lookup_payout(payouts, "馬連", combo)
    results["馬連"] = (100, ret)

    # ── 馬単: 上位2頭 (着順固定) ────────────────────────────────
    ret = _lookup_payout(payouts, "馬単", (nums[0], nums[1]))
    results["馬単"] = (100, ret)

    if len(nums) < 3:
        return results

    # ── ワイド: 上位3頭から C(3,2)=3点 ─────────────────────────
    top3 = nums[:3]
    wide_ret = sum(
        _lookup_payout(payouts, "ワイド", tuple(sorted([a, b])))
        for a, b in itertools.combinations(top3, 2)
    )
    results["ワイド"] = (300, wide_ret)

    # ── 三連複: 上位3頭 (順不問) ────────────────────────────────
    combo = tuple(sorted(nums[:3]))
    ret = _lookup_payout(payouts, "三連複", combo)
    results["三連複"] = (100, ret)

    # ── 三連単: 上位3頭 (着順固定) ──────────────────────────────
    ret = _lookup_payout(payouts, "三連単", (nums[0], nums[1], nums[2]))
    results["三連単"] = (100, ret)

    return results


def run_backtest(
    models: SandboxModels,
    test_df: pd.DataFrame,
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    """
    2025年全レース・全8券種・全3モデルのバックテストを実行する。

    gate_number は race_results から取得し、予測時に付与する。

    Returns:
        DataFrame: [race_id, bet_type, model, invested, returned]
    """
    logger.info("バックテスト開始: %d レース", test_df["race_id"].nunique())

    # 2025年全払戻を一括取得
    payout_rows = conn.execute(
        """
        SELECT race_id, bet_type, combination, payout
        FROM race_payouts
        WHERE race_id LIKE ?
        """,
        (f"{TEST_YEAR}%",),
    ).fetchall()
    payouts_by_race: dict[str, list[dict]] = {}
    for race_id, bet_type, combination, payout in payout_rows:
        payouts_by_race.setdefault(race_id, []).append(
            {"bet_type": bet_type, "combination": combination, "payout": payout}
        )

    # gate_number を race_results から取得（全馬分）
    gate_rows = conn.execute(
        """
        SELECT race_id, horse_number, gate_number
        FROM race_results
        WHERE race_id LIKE ?
        """,
        (f"{TEST_YEAR}%",),
    ).fetchall()
    gate_by_race: dict[str, dict[int, int]] = {}
    for race_id, hn, gn in gate_rows:
        gate_by_race.setdefault(race_id, {})[int(hn)] = int(gn) if gn else 0

    # 全モデル一括予測
    all_pred = models.predict(test_df)

    records: list[dict] = []
    race_ids = test_df["race_id"].unique()

    for i, race_id in enumerate(race_ids):
        if i % 500 == 0:
            logger.info("  %d / %d レース", i, len(race_ids))

        payouts = payouts_by_race.get(race_id, [])
        if not payouts:
            continue

        race_pred = all_pred[all_pred["race_id"] == race_id].copy()
        if len(race_pred) < 2:
            continue

        # gate_number をより広い情報源から補完
        gate_map = gate_by_race.get(race_id, {})
        race_pred["gate_number"] = race_pred["horse_number"].astype(int).map(
            lambda x: gate_map.get(x, 0)
        )

        # ── 3モデル × 8券種 ─────────────────────────────────────
        for model_key, score_col in [
            ("sandbox_honmei", "win_prob"),
            ("sandbox_place",  "place_prob"),
            ("sandbox_ev",     "ev_score"),
        ]:
            rp = race_pred.copy()
            rp["score"] = rp[score_col]
            sim = _simulate_one_race(rp, payouts)
            for bet_type, (inv, ret) in sim.items():
                records.append({
                    "race_id": race_id,
                    "bet_type": bet_type,
                    "model": model_key,
                    "invested": inv,
                    "returned": ret,
                })

    logger.info("バックテスト完了: %d 件のレコード", len(records))
    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────
#  Phase 4: ROI マトリクス集計
# ─────────────────────────────────────────────────────────────────

def compute_roi_matrix(bt: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (model, bet_type), grp in bt.groupby(["model", "bet_type"]):
        n_races  = len(grp)
        invested = grp["invested"].sum()
        returned = grp["returned"].sum()
        n_hits   = (grp["returned"] > 0).sum()
        roi      = returned / invested * 100 if invested > 0 else 0.0
        hit_rate = n_hits / n_races * 100 if n_races > 0 else 0.0
        avg_ret  = returned / n_hits if n_hits > 0 else 0.0
        rows.append({
            "model": model,
            "bet_type": bet_type,
            "n_races": n_races,
            "invested": int(invested),
            "returned": int(returned),
            "roi_pct": round(roi, 1),
            "hit_rate": round(hit_rate, 1),
            "avg_return": round(avg_ret, 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("roi_pct", ascending=False)


# ─────────────────────────────────────────────────────────────────
#  Phase 5: 黒字券種の自動実装
# ─────────────────────────────────────────────────────────────────

_BET_GENERATOR_PATH = ROOT / "src" / "ml" / "bet_generator.py"


def auto_implement(roi_df: pd.DataFrame) -> list[str]:
    """ROI > 閾値 の券種を bet_generator.py に追記する。"""
    profitable = roi_df[
        (roi_df["roi_pct"] >= AUTO_IMPLEMENT_ROI_THRESHOLD)
        & (roi_df["n_races"] >= MIN_BETS_FOR_IMPLEMENT)
    ].copy()

    if profitable.empty:
        logger.info(
            "ROI≥%.0f%% 且つ %d件以上の券種なし — 本番実装スキップ",
            AUTO_IMPLEMENT_ROI_THRESHOLD, MIN_BETS_FOR_IMPLEMENT,
        )
        return []

    existing = _BET_GENERATOR_PATH.read_text(encoding="utf-8")
    marker = "# ── SandboxDerived Strategies ──"
    if marker in existing:
        existing = existing[: existing.index(marker)]

    implemented: list[str] = []
    lines: list[str] = [
        f"\n\n{marker}",
        f"# 【特例サンドボックス由来】 train={TRAIN_YEAR} → test={TEST_YEAR}",
        f"# 生成日: {datetime.now().strftime('%Y-%m-%d')}",
        f"# ROI≥{AUTO_IMPLEMENT_ROI_THRESHOLD:.0f}% 且つ {MIN_BETS_FOR_IMPLEMENT}レース以上で実装対象。",
        "# CAUTION: 研究環境の結果。本番前に live データで再検証すること。",
        "",
        "from dataclasses import dataclass as _dc",
        "",
        "@_dc",
        "class SandboxBetResult:",
        '    """サンドボックス由来の有望買い目情報。"""',
        "    model: str",
        "    bet_type: str",
        "    roi_pct: float",
        "    hit_rate: float",
        "    n_test_races: int",
        "    note: str",
        "",
        "SANDBOX_PROFITABLE_STRATEGIES: list[SandboxBetResult] = [",
    ]

    strategies = {
        "単勝":  "score上位1頭を単勝購入（100円）",
        "複勝":  "score上位3頭を複勝購入（各100円/計300円）",
        "馬連":  "score上位2頭の馬連（100円）",
        "ワイド": "score上位3頭からC(3,2)=3点ワイド（各100円/計300円）",
        "枠連":  "score上位2頭の枠番組み合わせ（100円）",
        "馬単":  "score1位→2位の馬単（100円）",
        "三連複": "score上位3頭の三連複（100円）",
        "三連単": "score1位→2位→3位の三連単（100円）",
    }

    for _, row in profitable.iterrows():
        model    = row["model"]
        bet_type = row["bet_type"]
        roi      = row["roi_pct"]
        hit      = row["hit_rate"]
        n_r      = int(row["n_races"])
        inv      = int(row["invested"])
        ret      = int(row["returned"])
        profit   = ret - inv
        strat    = strategies.get(bet_type, "")
        note_str = (
            f"ROI={roi}% 的中率={hit}% n={n_r}R "
            f"投={inv:,}円 回={ret:,}円 P&L=+{profit:,}円 // {strat}"
        )
        lines.append(
            f"    SandboxBetResult("
            f'model="{model}", bet_type="{bet_type}", '
            f"roi_pct={roi}, hit_rate={hit}, n_test_races={n_r}, "
            f"note={note_str!r}),"
        )
        implemented.append(f"{model}/{bet_type}")

    lines += [
        "]",
        "",
        "",
        "def get_sandbox_strategy_note(model: str, bet_type: str) -> str:",
        '    """実装済みサンドボックス戦略の note を返す。未登録なら空文字。"""',
        "    for s in SANDBOX_PROFITABLE_STRATEGIES:",
        "        if s.model == model and s.bet_type == bet_type:",
        "            return s.note",
        '    return ""',
        "",
    ]

    new_content = existing.rstrip() + "\n" + "\n".join(lines)
    _BET_GENERATOR_PATH.write_text(new_content, encoding="utf-8")
    logger.info(
        "bet_generator.py に %d 件の戦略を追記しました",
        len(implemented),
    )
    return implemented


# ─────────────────────────────────────────────────────────────────
#  レポート生成
# ─────────────────────────────────────────────────────────────────

def generate_report(roi_df: pd.DataFrame, implemented: list[str]) -> Path:
    today = datetime.now().strftime("%Y%m%d")
    report_path = DOCS_DIR / f"sandbox_report_{today}.md"
    jra_take = {"単勝": 20, "複勝": 20, "枠連": 22.5, "馬連": 22.5,
                "ワイド": 22.5, "馬単": 25, "三連複": 25, "三連単": 27.5}

    lines: list[str] = [
        "# UMALOGI サンドボックス全券種シミュレーションレポート",
        "",
        f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**学習期間**: {TRAIN_YEAR}年 (JVLink + netkeiba オッズ)",
        f"**テスト期間**: {TEST_YEAR}年 (完全 OOS — カンニングなし)",
        "**データ**: `umalogi.db` races/race_results/race_payouts × `netkeiba_research.db` horse_odds",
        "**特徴量**: netkeiba 単勝オッズ・人気・implied prob + 会場・距離・馬場・頭数",
        "",
        "## 【重要】バイアス排除設計",
        "",
        "| 項目 | 設計 |",
        "|------|------|",
        "| データリーク | 学習/テスト年を完全分離（2024学習→2025テスト） |",
        "| 馬選択 | horse_odds の**全出走馬**を対象（top-3 バイアスなし） |",
        "| 枠番 | テスト時は race_results から取得（事前情報不使用） |",
        "| 予測単位 | 固定 100円/点（Kelly 最適化なし、比較のため均等張り） |",
        "",
        "## JRA 控除率参考",
        "",
        "| 券種 | JRA控除率 | 理論ROI上限 |",
        "|------|-----------|------------|",
    ]
    for bt, take in jra_take.items():
        lines.append(f"| {bt} | {take}% | {100-take}% |")

    lines += [
        "",
        "## ROI マトリクス（モデル × 券種）",
        "",
        "> **ROI が JRA 控除後理論上限を超えている場合、その券種・モデルに統計的優位性がある**",
        "> ✅ = ROI≥{}% (実装対象)  🟡 = ROI≥JRA理論上限  ❌ = ROI<100%".format(
            int(AUTO_IMPLEMENT_ROI_THRESHOLD)
        ),
        "",
    ]

    # モデル × 券種のピボット
    if not roi_df.empty:
        models_sorted = sorted(roi_df["model"].unique().tolist())
        header = ["券種"] + [m.replace("sandbox_", "") for m in models_sorted]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|------|" + "------|" * len(models_sorted))
        pivot = roi_df.pivot_table(
            index="bet_type", columns="model", values="roi_pct", aggfunc="first"
        )
        for bt in ALL_BET_TYPES:
            if bt not in pivot.index:
                continue
            take = jra_take.get(bt, 20)
            row_parts = [bt]
            for m in models_sorted:
                val = pivot.loc[bt, m] if m in pivot.columns else None
                if val is None:
                    row_parts.append("—")
                elif val >= AUTO_IMPLEMENT_ROI_THRESHOLD:
                    row_parts.append(f"**{val:.1f}%** ✅")
                elif val >= 100 - take:
                    row_parts.append(f"{val:.1f}% 🟡")
                else:
                    row_parts.append(f"{val:.1f}% ❌")
            lines.append("| " + " | ".join(row_parts) + " |")

    lines += [
        "",
        "## 詳細結果（ROI降順）",
        "",
        "| モデル | 券種 | ROI | 的中率 | レース数 | 投資額 | 回収額 | P&L |",
        "|--------|------|-----|--------|----------|--------|--------|-----|",
    ]
    if not roi_df.empty:
        for _, row in roi_df.iterrows():
            pl = int(row["returned"]) - int(row["invested"])
            flag = " ✅" if row["roi_pct"] >= AUTO_IMPLEMENT_ROI_THRESHOLD else ""
            m_name = row["model"].replace("sandbox_", "")
            lines.append(
                f"| {m_name} | {row['bet_type']} | {row['roi_pct']:.1f}%{flag} "
                f"| {row['hit_rate']:.1f}% | {row['n_races']:,} "
                f"| ¥{row['invested']:,} | ¥{row['returned']:,} | ¥{pl:+,} |"
            )

    if implemented:
        lines += [
            "",
            "## 自動実装済み戦略",
            "",
            f"以下の (モデル, 券種) を `src/ml/bet_generator.py` に追記しました。",
            "",
        ]
        for key in implemented:
            lines.append(f"- ✅ **{key}**")
    else:
        lines += [
            "",
            "## 自動実装",
            "",
            f"ROI≥{AUTO_IMPLEMENT_ROI_THRESHOLD:.0f}% 且つ {MIN_BETS_FOR_IMPLEMENT}レース以上の",
            "券種がなかったため、自動実装はスキップしました。",
        ]

    lines += [
        "",
        "## 注意事項",
        "",
        "- 本シミュレーションは**特例サンドボックス**。本番環境とは隔離された研究環境です",
        "- netkeiba オッズ特徴量は通常 CLAUDE.md 0条で禁止。今回は1回限りの特例です",
        "- 均等 100円/点の固定張り。実際の Kelly 推奨サイズでは ROI が変動します",
        "- 1年間 (2024) の学習データは量的に少ない。追加年で再検証推奨",
        "- バックテスト ROI は将来の利益を保証しません",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("レポート: %s", report_path)
    return report_path


# ─────────────────────────────────────────────────────────────────
#  main
# ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="特例サンドボックス — netkeiba×JVLink統合・全8券種シミュレーション"
    )
    parser.add_argument("--skip-train",  action="store_true", help="学習スキップ（保存済みモデル使用）")
    parser.add_argument("--report-only", action="store_true", help="既存結果からレポートのみ")
    args = parser.parse_args()

    start = datetime.now()
    print(f"\n{'='*65}")
    print("  UMALOGI サンドボックスパイプライン — 特例全券種シミュレーション")
    print(f"  開始: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  学習: {TRAIN_YEAR}  テスト: {TEST_YEAR}")
    print(f"  特徴量: netkeiba win_odds + JVLink 公開情報 (特例)")
    print(f"{'='*65}\n")

    conn     = sqlite3.connect(str(MAIN_DB))
    res_conn = sqlite3.connect(str(RESEARCH_DB))

    bt_cache = SANDBOX_DIR / "backtest_results.pkl"

    if args.report_only and bt_cache.exists():
        bt_df = pd.read_pickle(bt_cache)
        logger.info("既存バックテスト結果ロード: %d 件", len(bt_df))
    else:
        models = SandboxModels()

        if args.skip_train and models.load():
            logger.info("保存済みサンドボックスモデルをロード")
        else:
            print(f"\n{'─'*55}")
            print(f"  Phase 1: 特徴量構築 (学習 {TRAIN_YEAR})")
            print(f"{'─'*55}")
            train_df, _ = build_sandbox_df(conn, res_conn, TRAIN_YEAR, fit_encoders=True)
            if train_df.empty:
                print("[ERROR] 学習データが空です"); return

            print(f"\n{'─'*55}")
            print("  Phase 2: サンドボックスモデル学習")
            print(f"{'─'*55}")
            models.train(train_df)
            models.save()

        print(f"\n{'─'*55}")
        print(f"  Phase 1 (test): 特徴量構築 (テスト {TEST_YEAR})")
        print(f"{'─'*55}")
        test_df, _ = build_sandbox_df(conn, res_conn, TEST_YEAR, fit_encoders=False)
        if test_df.empty:
            print("[ERROR] テストデータが空です"); return

        print(f"\n{'─'*55}")
        print("  Phase 3: 全8券種バックテスト")
        print(f"{'─'*55}")
        bt_df = run_backtest(models, test_df, conn)
        bt_df.to_pickle(bt_cache)

    print(f"\n{'─'*55}")
    print("  Phase 4: ROI マトリクス集計")
    print(f"{'─'*55}")
    roi_df = compute_roi_matrix(bt_df)
    print("\n■ ROI マトリクス（全モデル × 全8券種）\n")
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.width", 120)
    print(roi_df.to_string(index=False))

    print(f"\n{'─'*55}")
    print("  Phase 5: 黒字券種の自動実装")
    print(f"{'─'*55}")
    implemented = auto_implement(roi_df)

    print(f"\n{'─'*55}")
    print("  レポート生成")
    print(f"{'─'*55}")
    report_path = generate_report(roi_df, implemented)

    elapsed = (datetime.now() - start).total_seconds() / 60
    print(f"\n{'='*65}")
    print(f"  完了  経過={elapsed:.1f}分")
    print(f"  レポート: {report_path}")
    if implemented:
        print(f"  実装済み: {len(implemented)}件")
        for key in implemented[:10]:
            print(f"    ✅ {key}")
    print(f"{'='*65}\n")

    conn.close()
    res_conn.close()


if __name__ == "__main__":
    main()
