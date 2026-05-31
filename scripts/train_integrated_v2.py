"""
scripts/train_integrated_v2.py — Sprint 1 統合ウォークフォワード検証

【特徴量セット (v2)】
  netkeiba  : nb_win_odds, nb_implied_prob, nb_log_odds
  JVLink    : venue_code, surface_code, condition_code, distance,
              race_number, month, race_n_horses
  JVLink v2 : weight_carried (担当体重 2024+)
              jockey_win_rate_90d (騎手90日勝率 2024+)
              trainer_win_rate_90d (調教師90日勝率 2024+)

【Edge-based 選択】
  model_prob = per-race 正規化 ev_score
  edge = model_prob / nb_implied_prob
  edge > EDGE_THRESHOLD (1.05) の馬のみ購入

【Walk-forward フォールド】
  F3: 学習=2021+2022+2023  → テスト=2024  (全8券種 実払戻)
  F4: 学習=2021+...+2024   → テスト=2025  (全8券種 実払戻)

【リーク排除】
  - horse_odds.rank は特徴量から除外 (事後情報)
  - rolling 統計は race_date より前のデータのみ参照
  - テストデータは学習に含まない (完全 OOS)

使い方:
  py scripts/train_integrated_v2.py             # F3 + F4 (デフォルト)
  py scripts/train_integrated_v2.py --folds 4  # F4 のみ
  py scripts/train_integrated_v2.py --no-edge  # Edge フィルター無効
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
MAIN_DB = ROOT / "data" / "umalogi.db"
RESEARCH_DB = ROOT / "data" / "netkeiba_research.db"

EDGE_THRESHOLD = 1.05  # model_prob / market_prob の閾値
MIN_PERSON_STARTS = 3  # 最低出走数 (これ未満は NaN)

# ── 静的エンコーダー ──────────────────────────────────────────────────────────
_SURFACE_MAP = {"芝": 0, "ダート": 1, "障害": 2}
_CONDITION_MAP = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
_VENUE_MAP = {
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

# v2 統合特徴量 (LightGBM は NaN を自動処理)
FEATURE_COLS: list[str] = [
    # ── netkeiba オッズ ──
    "nb_win_odds",
    "nb_implied_prob",
    "nb_log_odds",
    # ── JVLink レース情報 ──
    "venue_code",
    "surface_code",
    "condition_code",
    "distance",
    "race_number",
    "month",
    "race_n_horses",
    # ── JVLink 騎手・調教師 (2024+: 実データ、2021-2023: NaN) ──
    "weight_carried",
    "jockey_win_rate_90d",
    "trainer_win_rate_90d",
]

# LightGBM EV リグレッサ
_LGB_PARAMS: dict[str, Any] = {
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

ALL_BET_TYPES = ["単勝", "複勝", "枠連", "馬連", "ワイド", "馬単", "三連複", "三連単"]


# ─────────────────────────────────────────────────────────────────────────────
#  Step 1: 騎手・調教師 90 日勝率ルックアップテーブル構築
# ─────────────────────────────────────────────────────────────────────────────


def build_person_lookup(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    umalogi.db の race_results から騎手・調教師の点在時点勝率を計算する。

    各行 (race_id, horse_number) について:
      jockey_win_rate_90d  : レース日 [D-90, D) の騎手勝率
      trainer_win_rate_90d : レース日 [D-90, D) の調教師勝率

    2024+ のみデータが存在するため、2021-2023 行は NaN → LightGBM が自動処理。
    """
    # rank フィルターなし: rank=NULL (3着外) の馬も含めることで
    # 「non-NULL → 着内」というデータリークを防ぐ
    query = """
    SELECT rr.race_id, rr.horse_number,
           rr.jockey, rr.trainer, rr.rank, r.date
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.jockey != ''
    ORDER BY r.date
    """
    rows = conn.execute(query).fetchall()
    if not rows:
        logger.warning("race_results: データなし (2021-2023 は空が正常)")
        return pd.DataFrame(
            columns=[
                "race_id",
                "horse_number",
                "jockey_win_rate_90d",
                "trainer_win_rate_90d",
                "jockey_starts_90d",
                "trainer_starts_90d",
            ]
        )

    df = pd.DataFrame(
        rows, columns=["race_id", "horse_number", "jockey", "trainer", "rank", "date"]
    )
    df["date"] = pd.to_datetime(df["date"])
    # rank=NULL(3着外) は 0 扱い — NaN安全
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["is_winner"] = (df["rank"] == 1).astype(int)
    df = df.sort_values("date").reset_index(drop=True)

    logger.info("rolling 統計計算: %d行 / %d レース", len(df), df["race_id"].nunique())

    def _rolling_rate(person_col: str) -> pd.DataFrame:
        """1人ずつ sorted array + searchsorted で O(n log n) 計算"""
        # 各人物の (date_array, is_winner_array) を事前構築
        person_records: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for person, grp in df.groupby(person_col):
            grp_s = grp.sort_values("date")
            dates_ns = grp_s["date"].values.astype("datetime64[D]")
            wins_arr = grp_s["is_winner"].values.astype(int)
            person_records[person] = (dates_ns, wins_arr)

        target = df[["race_id", "horse_number", person_col, "date"]].copy()
        col_rate = f"{person_col}_win_rate_90d"
        col_starts = f"{person_col}_starts_90d"

        rates: list[float | None] = []
        starts_list: list[int] = []

        for _, row in target.iterrows():
            person = row[person_col]
            race_date = row["date"]
            window_start = race_date - pd.Timedelta(days=90)

            if person in person_records:
                dates_arr, wins_arr = person_records[person]
                lo = int(
                    np.searchsorted(
                        dates_arr, np.datetime64(window_start, "D"), side="left"
                    )
                )
                hi = int(
                    np.searchsorted(
                        dates_arr, np.datetime64(race_date, "D"), side="left"
                    )
                )
                n_starts = hi - lo
                n_wins = int(wins_arr[lo:hi].sum())
                win_rate = n_wins / n_starts if n_starts >= MIN_PERSON_STARTS else None
            else:
                n_starts, win_rate = 0, None

            rates.append(win_rate)
            starts_list.append(n_starts)

        result = target[["race_id", "horse_number"]].copy()
        result[col_rate] = rates
        result[col_starts] = starts_list
        return result

    logger.info("  jockey 勝率計算中...")
    jockey_df = _rolling_rate("jockey")

    logger.info("  trainer 勝率計算中...")
    trainer_df = _rolling_rate("trainer")

    merged = jockey_df.merge(
        trainer_df[
            ["race_id", "horse_number", "trainer_win_rate_90d", "trainer_starts_90d"]
        ],
        on=["race_id", "horse_number"],
        how="outer",
    )
    logger.info("rolling 統計完了: %d行", len(merged))
    return merged


# ─────────────────────────────────────────────────────────────────────────────
#  Step 2: 統合 DataFrame 構築
# ─────────────────────────────────────────────────────────────────────────────


def build_integrated_df(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    person_lookup: pd.DataFrame,
    years: list[str],
) -> pd.DataFrame:
    """
    horse_odds (netkeiba) × races (JVLink) × race_results (JVLink, 2024+) を統合。
    EV ターゲット = nb_win_odds × is_winner (payout 不要、全年対応)
    """
    dfs: list[pd.DataFrame] = []

    for yr in years:
        # ── A. horse_odds (netkeiba_research.db) ─────────────────────────────
        rows = res_conn.execute(
            """
            SELECT race_id, horse_number,
                   CAST(win_odds AS REAL) AS nb_win_odds,
                   CAST(rank      AS INTEGER) AS finish_rank
            FROM horse_odds
            WHERE substr(race_id, 1, 4) = ?
            """,
            (yr,),
        ).fetchall()
        if not rows:
            logger.warning("horse_odds なし: %s", yr)
            continue
        df = pd.DataFrame(
            rows, columns=["race_id", "horse_number", "nb_win_odds", "finish_rank"]
        )

        # ── B. races (JVLink umalogi.db) ─────────────────────────────────────
        race_rows = conn.execute(
            """
            SELECT race_id, date, venue, surface, condition, distance, race_number
            FROM races WHERE date LIKE ?
            """,
            (f"{yr}%",),
        ).fetchall()
        if not race_rows:
            logger.warning("races なし: %s", yr)
            continue
        race_df = pd.DataFrame(
            race_rows,
            columns=[
                "race_id",
                "date",
                "venue",
                "surface",
                "condition",
                "distance",
                "race_number",
            ],
        )
        df = df.merge(race_df, on="race_id", how="inner")

        # ── C. race_results (JVLink, 2024+) — gate_number + weight_carried ──
        rr_rows = conn.execute(
            """
            SELECT race_id, horse_number, gate_number, weight_carried
            FROM race_results
            WHERE race_id LIKE ? AND gate_number IS NOT NULL AND gate_number > 0
            """,
            (f"{yr}%",),
        ).fetchall()
        if rr_rows:
            rr_df = pd.DataFrame(
                rr_rows,
                columns=["race_id", "horse_number", "gate_number", "weight_carried"],
            )
            df = df.merge(rr_df, on=["race_id", "horse_number"], how="left")
        else:
            df["gate_number"] = 0
            df["weight_carried"] = np.nan

        df["gate_number"] = df["gate_number"].fillna(0).astype(int)

        # ── D. 騎手・調教師 rolling stats (person_lookup から) ───────────────
        if not person_lookup.empty:
            df = df.merge(
                person_lookup[
                    [
                        "race_id",
                        "horse_number",
                        "jockey_win_rate_90d",
                        "trainer_win_rate_90d",
                    ]
                ],
                on=["race_id", "horse_number"],
                how="left",
            )
        else:
            df["jockey_win_rate_90d"] = np.nan
            df["trainer_win_rate_90d"] = np.nan

        # ── E. ラベル付与 ────────────────────────────────────────────────────
        df["finish_rank"] = pd.to_numeric(df["finish_rank"], errors="coerce")
        df["is_winner"] = (df["finish_rank"] == 1).astype(int)
        df["is_placed"] = (df["finish_rank"] <= 3).astype(int)

        # EV ターゲット (全年で計算可能)
        df["ev_target"] = df["nb_win_odds"] * df["is_winner"]

        # ── F. 特徴量エンジニアリング ─────────────────────────────────────────
        df["nb_win_odds"] = pd.to_numeric(df["nb_win_odds"], errors="coerce")

        df["nb_implied_prob"] = np.nan
        df["nb_log_odds"] = np.nan
        df["race_n_horses"] = 0

        for race_id, grp in df.groupby("race_id"):
            idx = grp.index
            df.loc[idx, "race_n_horses"] = len(grp)
            valid = grp["nb_win_odds"].dropna()
            if len(valid) > 0:
                inv = 1.0 / valid.clip(lower=1.0)
                norm = inv / inv.sum()
                df.loc[valid.index, "nb_implied_prob"] = norm
            df.loc[idx, "nb_log_odds"] = np.log1p(grp["nb_win_odds"])

        df["surface_code"] = df["surface"].map(_SURFACE_MAP).fillna(-1).astype(int)
        df["condition_code"] = (
            df["condition"].map(_CONDITION_MAP).fillna(-1).astype(int)
        )
        df["venue_code"] = df["venue"].map(_VENUE_MAP).fillna(-1).astype(int)
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce").fillna(1600)
        df["race_number"] = pd.to_numeric(df["race_number"], errors="coerce").fillna(6)
        df["month"] = (
            pd.to_numeric(df["date"].str[5:7], errors="coerce").fillna(6).astype(int)
        )
        df["weight_carried"] = pd.to_numeric(df["weight_carried"], errors="coerce")

        dfs.append(df)
        logger.info(
            "  %s: %d行 / %d レース / winner=%d / wc_ok=%d / jwr_ok=%d",
            yr,
            len(df),
            df["race_id"].nunique(),
            df["is_winner"].sum(),
            int(df["weight_carried"].notna().sum()),
            int(df["jockey_win_rate_90d"].notna().sum()),
        )

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Step 3: Edge 正規化 (per-race model_prob)
# ─────────────────────────────────────────────────────────────────────────────


def add_edge(df: pd.DataFrame) -> pd.DataFrame:
    """
    ev_score を per-race 正規化 → model_prob_norm
    edge = model_prob_norm / nb_implied_prob
    """
    df = df.copy()
    df["model_prob_norm"] = np.nan
    df["edge"] = np.nan

    for race_id, grp in df.groupby("race_id"):
        scores = grp["ev_score"].clip(lower=0)
        total = scores.sum()
        if total > 0:
            norm = scores / total
        else:
            norm = pd.Series(1.0 / len(grp), index=grp.index)
        df.loc[grp.index, "model_prob_norm"] = norm

        impl = grp["nb_implied_prob"].fillna(1.0 / len(grp))
        df.loc[grp.index, "edge"] = norm / impl.replace(0, np.nan)

    return df


# ─────────────────────────────────────────────────────────────────────────────
#  Step 4: 払戻突合ユーティリティ
# ─────────────────────────────────────────────────────────────────────────────


def _parse_combo(bet_type: str, combination: str) -> tuple[int, ...]:
    combo = str(combination).strip()
    sep = "→" if "→" in combo else ("-" if "-" in combo else None)
    if sep:
        parts = [int(x.strip()) for x in combo.split(sep) if x.strip().isdigit()]
    else:
        parts = [int(combo)] if combo.isdigit() else []
    return tuple(parts) if bet_type in ("馬単", "三連単") else tuple(sorted(parts))


def _lookup(payout_df: pd.DataFrame, bet_type: str, selected: tuple[int, ...]) -> float:
    for _, row in payout_df[payout_df["bet_type"] == bet_type].iterrows():
        try:
            if _parse_combo(bet_type, str(row["combination"])) == selected:
                return float(row["payout"])
        except (ValueError, TypeError):
            pass
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  Step 5: 1 レースシミュレーション
# ─────────────────────────────────────────────────────────────────────────────


def simulate_race(
    pred_df: pd.DataFrame,
    payout_df: pd.DataFrame | None,
    use_edge: bool,
) -> dict[str, tuple[float, float]]:
    """
    EV スコア上位選択 + Edge フィルターで全8券種シミュレート。
    返り値: {bet_type: (投資額, 払戻額)}
    """
    pred = pred_df.sort_values("ev_score", ascending=False).reset_index(drop=True)

    # Top-3 候補 (ev_score 上位)
    top_n = min(3, len(pred))
    top = pred.head(top_n)

    top1 = int(top.iloc[0]["horse_number"])
    top2 = int(top.iloc[1]["horse_number"]) if top_n >= 2 else -1
    top3 = int(top.iloc[2]["horse_number"]) if top_n >= 3 else -1

    gate_map = dict(
        zip(pred["horse_number"].astype(int), pred["gate_number"].astype(int))
    )
    edge_map = dict(zip(pred["horse_number"].astype(int), pred["edge"].fillna(0)))

    def has_edge(h: int) -> bool:
        return (not use_edge) or (edge_map.get(h, 0) >= EDGE_THRESHOLD)

    if payout_df is None or payout_df.empty:
        return {}

    results: dict[str, tuple[float, float]] = {}

    # 単勝
    if has_edge(top1):
        ret = _lookup(payout_df, "単勝", (top1,))
        results["単勝"] = (100, ret)

    # 複勝 (top3 それぞれ edge チェック)
    placed_bets = [(h,) for h in [top1, top2, top3] if h > 0 and has_edge(h)]
    if placed_bets:
        total_inv = len(placed_bets) * 100
        total_ret = sum(_lookup(payout_df, "複勝", c) for c in placed_bets)
        results["複勝"] = (total_inv, total_ret)

    # 枠連
    if top_n >= 2 and has_edge(top1) and has_edge(top2):
        g0, g1 = gate_map.get(top1, 0), gate_map.get(top2, 0)
        if g0 > 0 and g1 > 0:
            ret = _lookup(payout_df, "枠連", tuple(sorted([g0, g1])))
            results["枠連"] = (100, ret)

    # 馬連
    if top_n >= 2 and has_edge(top1) and has_edge(top2):
        ret = _lookup(payout_df, "馬連", tuple(sorted([top1, top2])))
        results["馬連"] = (100, ret)

    # ワイド (3点、各ペア edge チェック)
    if top_n >= 3:
        wide_bets = []
        for a, b in [(top1, top2), (top1, top3), (top2, top3)]:
            if a > 0 and b > 0 and has_edge(a) and has_edge(b):
                wide_bets.append(tuple(sorted([a, b])))
        if wide_bets:
            total_inv = len(wide_bets) * 100
            total_ret = sum(_lookup(payout_df, "ワイド", c) for c in wide_bets)
            results["ワイド"] = (total_inv, total_ret)

    # 馬単
    if top_n >= 2 and has_edge(top1) and has_edge(top2):
        ret = _lookup(payout_df, "馬単", (top1, top2))
        results["馬単"] = (100, ret)

    # 三連複
    if top_n >= 3 and has_edge(top1) and has_edge(top2) and has_edge(top3):
        ret = _lookup(payout_df, "三連複", tuple(sorted([top1, top2, top3])))
        results["三連複"] = (100, ret)

    # 三連単
    if top_n >= 3 and has_edge(top1) and has_edge(top2) and has_edge(top3):
        ret = _lookup(payout_df, "三連単", (top1, top2, top3))
        results["三連単"] = (100, ret)

    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Step 6: 統計集計
# ─────────────────────────────────────────────────────────────────────────────


def calc_stats(records: list[dict], year: str, bet_type: str) -> dict:
    rows = [r for r in records if r["year"] == year and r["bet_type"] == bet_type]
    if not rows:
        return {}
    df = pd.DataFrame(rows).sort_values("race_id")
    n = len(df)
    invested = float(df["invested"].sum())
    returned = float(df["returned"].sum())
    hits = int((df["returned"] > 0).sum())
    roi = returned / invested * 100 if invested else 0.0

    pnl = (df["returned"] - df["invested"]).cumsum()
    max_dd = float((pnl - pnl.cummax()).min())

    top1_ret = float(df["returned"].max())
    top1_race = str(df.loc[df["returned"].idxmax(), "race_id"])
    ret_ex = returned - top1_ret
    roi_ex = ret_ex / invested * 100 if invested else 0.0
    top1_pct = top1_ret / returned * 100 if returned > 0 else 0.0

    return {
        "year": year,
        "bet_type": bet_type,
        "n": n,
        "hits": hits,
        "hit_rate": round(hits / n * 100, 1),
        "invested": int(invested),
        "returned": int(returned),
        "pnl": int(returned - invested),
        "roi": round(roi, 1),
        "max_dd": round(max_dd, 0),
        "top1_ret": int(top1_ret),
        "top1_race": top1_race,
        "roi_ex": round(roi_ex, 1),
        "top1_pct": round(top1_pct, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--folds", nargs="*", type=int, help="実行フォールド番号 (3,4)")
    parser.add_argument("--no-edge", action="store_true", help="Edge フィルター無効")
    args = parser.parse_args()

    run_folds = set(args.folds) if args.folds else {3, 4}
    use_edge = not args.no_edge

    conn = sqlite3.connect(str(MAIN_DB))
    res_conn = sqlite3.connect(str(RESEARCH_DB))

    print()
    print("=" * 70)
    print("  Sprint 1 統合ウォークフォワード検証")
    print("  特徴量: nb_win_odds + JVLink + 騎手/調教師90日勝率")
    print(
        f"  Edge フィルター: {'有効 (threshold=' + str(EDGE_THRESHOLD) + ')' if use_edge else '無効'}"
    )
    print("=" * 70)

    # ── 騎手・調教師 rolling 統計を一括計算 (2024+ のみ実データあり) ──────────
    logger.info("")
    logger.info("── 騎手・調教師 90日勝率 計算中 ──")
    person_lookup = build_person_lookup(conn)

    folds = [
        (3, ["2021", "2022", "2023"], "2024"),
        (4, ["2021", "2022", "2023", "2024"], "2025"),
    ]

    all_records: list[dict] = []

    for fold_no, train_years, test_year in folds:
        if fold_no not in run_folds:
            continue

        label = "+".join(train_years)
        logger.info("")
        logger.info("═" * 65)
        logger.info("フォールド %d: 学習=%s → テスト=%s", fold_no, label, test_year)
        logger.info("═" * 65)

        # 学習データ
        logger.info("学習データ構築中 (%s)...", label)
        train_df = build_integrated_df(conn, res_conn, person_lookup, train_years)
        if train_df.empty:
            logger.error("学習データなし: スキップ")
            continue

        # 最低限 nb_win_odds があれば学習可能 (rolling stats は NaN → LightGBM が自動処理)
        train_clean = train_df.dropna(subset=["nb_win_odds", "nb_implied_prob"]).copy()

        # テストデータ
        logger.info("テストデータ構築中 (%s)...", test_year)
        test_df = build_integrated_df(conn, res_conn, person_lookup, [test_year])
        if test_df.empty:
            logger.error("テストデータなし: スキップ")
            continue
        test_clean = test_df.dropna(subset=["nb_win_odds", "nb_implied_prob"]).copy()

        logger.info(
            "学習: %d行 / %d レース  ev_target非ゼロ=%d行",
            len(train_clean),
            train_clean["race_id"].nunique(),
            (train_clean["ev_target"] > 0).sum(),
        )
        logger.info(
            "  jockey_win_rate 非NaN: %d行 / %d行",
            int(train_clean["jockey_win_rate_90d"].notna().sum()),
            len(train_clean),
        )
        logger.info(
            "テスト: %d行 / %d レース", len(test_clean), test_clean["race_id"].nunique()
        )
        logger.info(
            "  jockey_win_rate 非NaN: %d行 / %d行",
            int(test_clean["jockey_win_rate_90d"].notna().sum()),
            len(test_clean),
        )

        # EV リグレッサ学習
        model = LGBMRegressor(**_LGB_PARAMS)
        model.fit(train_clean[FEATURE_COLS], train_clean["ev_target"])
        logger.info("モデル学習完了")

        # 特徴量重要度 Top10
        feat_imp = pd.Series(
            model.feature_importances_, index=FEATURE_COLS
        ).sort_values(ascending=False)
        logger.info("特徴量重要度 Top10:")
        for feat, imp in feat_imp.head(10).items():
            logger.info("  %-30s %.1f", feat, imp)

        # テスト予測
        test_clean["ev_score"] = model.predict(test_clean[FEATURE_COLS])

        # Edge 計算
        test_clean = add_edge(test_clean)

        # 払戻データ (実データのみ)
        payout_rows = conn.execute(
            "SELECT race_id, bet_type, combination, payout FROM race_payouts WHERE race_id LIKE ?",
            (f"{test_year}%",),
        ).fetchall()
        if not payout_rows:
            logger.error("払戻データなし: スキップ")
            continue
        payout_global = pd.DataFrame(
            payout_rows, columns=["race_id", "bet_type", "combination", "payout"]
        )
        logger.info("払戻データ: %d行", len(payout_global))

        # Edge 統計確認
        if use_edge:
            edge_ok = (test_clean["edge"] >= EDGE_THRESHOLD).sum()
            edge_total = test_clean["edge"].notna().sum()
            logger.info(
                "Edge >= %.2f: %d / %d 馬 (%.1f%%)",
                EDGE_THRESHOLD,
                edge_ok,
                edge_total,
                edge_ok / edge_total * 100 if edge_total > 0 else 0,
            )

        # バックテスト
        race_ids = test_clean["race_id"].unique()
        logger.info("バックテスト開始: %d レース", len(race_ids))
        skipped = 0

        for i, race_id in enumerate(race_ids):
            pred_race = test_clean[test_clean["race_id"] == race_id]
            payout_race = payout_global[payout_global["race_id"] == race_id]

            if pred_race.empty or payout_race.empty:
                skipped += 1
                continue

            for bt, (inv, ret) in simulate_race(
                pred_race, payout_race, use_edge
            ).items():
                all_records.append(
                    {
                        "year": test_year,
                        "race_id": race_id,
                        "bet_type": bt,
                        "invested": inv,
                        "returned": ret,
                    }
                )

            if (i + 1) % 500 == 0:
                logger.info("  %d / %d", i + 1, len(race_ids))

        logger.info(
            "フォールド %d 完了 (%s) — スキップ=%d", fold_no, test_year, skipped
        )

    conn.close()
    res_conn.close()

    if not all_records:
        logger.error("記録なし")
        return

    # ─── レポート生成 ──────────────────────────────────────────────────────────
    tested_years = sorted({r["year"] for r in all_records})

    all_stats: dict[tuple[str, str], dict] = {}
    for y in tested_years:
        for bt in ALL_BET_TYPES:
            s = calc_stats(all_records, y, bt)
            if s:
                all_stats[(y, bt)] = s

    print("\n" + "═" * 95)
    print("  Sprint 1 統合ウォークフォワード — 最終レポート")
    edge_label = f"Edge > {EDGE_THRESHOLD}" if use_edge else "Edge フィルターなし"
    print(
        f"  特徴量: nb_win_odds + JVLink + jockey/trainer 90日勝率  |  選択: {edge_label}"
    )
    print("═" * 95)

    print("\n■ 【1】年別 ROI マトリクス\n")
    header = f"{'年度':<6} {'券種':<6} {'件数':>7} {'的中':>6} {'投資':>10} {'払戻':>12} {'損益':>11} {'ROI':>8} {'MaxDD':>10}"
    print(header)
    print("-" * 95)

    summary_rows: list[dict] = []
    for y in tested_years:
        for bt in ALL_BET_TYPES:
            s = all_stats.get((y, bt))
            if not s:
                continue
            flag = "✅" if s["roi"] >= 100 else ("🔶" if s["roi"] >= 80 else "❌")
            print(
                f"{y:<6} {bt:<6} {s['n']:>7} {s['hits']:>5}件 "
                f"{s['invested']:>10,} {s['returned']:>12,} "
                f"{s['pnl']:>+11,} {s['roi']:>7.1f}% "
                f"{s['max_dd']:>+10,.0f} {flag}"
            )
            summary_rows.append(s)
        print()

    print("\n■ 【2】外れ値除外 ROI（TOP1的中払戻を除いたROI）\n")
    print(
        f"{'年度':<6} {'券種':<6} {'TOP1払戻':>12} {'TOP1寄与':>10} {'除外ROI':>10} {'通常ROI':>9}"
    )
    print("-" * 65)
    for y in tested_years:
        for bt in ALL_BET_TYPES:
            s = all_stats.get((y, bt))
            if not s:
                continue
            flag_ex = (
                "✅" if s["roi_ex"] >= 100 else ("🔶" if s["roi_ex"] >= 80 else "❌")
            )
            print(
                f"{y:<6} {bt:<6} {s['top1_ret']:>12,} {s['top1_pct']:>9.1f}% "
                f"{s['roi_ex']:>9.1f}% {s['roi']:>8.1f}% {flag_ex}"
            )
        print()

    # ── 総括 ──────────────────────────────────────────────────────────────────
    print("\n■ 【3】総括\n")
    print(f"  検証期間: {', '.join(tested_years)}")
    print(f"  特徴量 v2: {len(FEATURE_COLS)} 列 (rolling stats 含む)")
    print(f"  Edge フィルター: {edge_label}")
    print()

    profitable = [
        (s["year"], s["bet_type"], s["roi"]) for s in summary_rows if s["roi"] >= 100
    ]
    if profitable:
        print("  ✅ ROI > 100% 達成:")
        for y, bt, roi in sorted(profitable, key=lambda x: -x[2]):
            print(f"     {y} / {bt}: {roi:.1f}%")
    else:
        best = sorted(summary_rows, key=lambda x: -x["roi"])[:5]
        print("  ❌ ROI > 100% 達成なし")
        print("  上位5件:")
        for s in best:
            print(f"     {s['year']} / {s['bet_type']}: {s['roi']:.1f}%")

    print()
    print("  完了。")


if __name__ == "__main__":
    main()
