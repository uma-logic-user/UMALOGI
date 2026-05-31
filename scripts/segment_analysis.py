"""
scripts/segment_analysis.py — 複勝 ROI セグメント分析

【目的】
  Sprint 1 で複勝 ROI 97.6% (2025) を達成したモデルにおいて、
  「安定して ROI > 100% を超える勝ちパターン」を自動抽出する。

【方法】
  1. F4 WF (学習 2021-2024, テスト 2025) を実行 → 各レースの複勝結果を記録
  2. 競馬場 / コース種別 / 距離帯 / 頭数帯 / 馬場状態 の1D ROI を算出
  3. 収益セグメントを発見し 2D 組合せも探索
  4. 同じセグメントフィルターを F3 (学習 2021-2023, テスト 2024) に適用して OOS 検証
  5. 最終的な「推奨フィルター条件」をプリント

【重要な注意】
  セグメント発見は 2025 テストデータ上で行われる (in-sample) 。
  2024 (F3, 部分 OOS) でも同じ結果が出るか必ず確認する。

使い方:
  py scripts/segment_analysis.py
"""

from __future__ import annotations

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

EDGE_THRESHOLD = 1.1  # Sprint 1 仕様: > 1.1
MIN_SEG_RACES = 30  # セグメント有効の最低レース数
MIN_SEG_ROI = 100.0  # 収益セグメントの閾値 (%)

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

FEATURE_COLS: list[str] = [
    "nb_win_odds",
    "nb_implied_prob",
    "nb_log_odds",
    "venue_code",
    "surface_code",
    "condition_code",
    "distance",
    "race_number",
    "month",
    "race_n_horses",
    "weight_carried",
    "jockey_win_rate_90d",
    "trainer_win_rate_90d",
]

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


# ─────────────────────────────────────────────────────────────────────────────
#  特徴量・ローリング統計構築 (train_integrated_v2.py から流用)
# ─────────────────────────────────────────────────────────────────────────────


def build_person_lookup(conn: sqlite3.Connection) -> pd.DataFrame:
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
        return pd.DataFrame(
            columns=[
                "race_id",
                "horse_number",
                "jockey_win_rate_90d",
                "trainer_win_rate_90d",
            ]
        )
    df = pd.DataFrame(
        rows, columns=["race_id", "horse_number", "jockey", "trainer", "rank", "date"]
    )
    df["date"] = pd.to_datetime(df["date"])
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["is_winner"] = (df["rank"] == 1).astype(int)

    def _rolling_rate(person_col: str) -> pd.DataFrame:
        person_records: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for person, grp in df.groupby(person_col):
            grp_s = grp.sort_values("date")
            person_records[person] = (
                grp_s["date"].values.astype("datetime64[D]"),
                grp_s["is_winner"].values.astype(int),
            )
        target = df[["race_id", "horse_number", person_col, "date"]].copy()
        col_rate = f"{person_col}_win_rate_90d"
        rates: list[float | None] = []
        for _, row in target.iterrows():
            person, race_date = row[person_col], row["date"]
            ws = race_date - pd.Timedelta(days=90)
            if person in person_records:
                da, wa = person_records[person]
                lo = int(np.searchsorted(da, np.datetime64(ws, "D"), side="left"))
                hi = int(
                    np.searchsorted(da, np.datetime64(race_date, "D"), side="left")
                )
                n = hi - lo
                rates.append(int(wa[lo:hi].sum()) / n if n >= 3 else None)
            else:
                rates.append(None)
        result = target[["race_id", "horse_number"]].copy()
        result[col_rate] = rates
        return result

    logger.info("  jockey rolling...")
    jdf = _rolling_rate("jockey")
    logger.info("  trainer rolling...")
    tdf = _rolling_rate("trainer")
    return jdf.merge(
        tdf[["race_id", "horse_number", "trainer_win_rate_90d"]],
        on=["race_id", "horse_number"],
        how="outer",
    )


def build_df(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    person_lookup: pd.DataFrame,
    years: list[str],
) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for yr in years:
        rows = res_conn.execute(
            "SELECT race_id, horse_number, CAST(win_odds AS REAL), CAST(rank AS INTEGER) "
            "FROM horse_odds WHERE substr(race_id,1,4)=?",
            (yr,),
        ).fetchall()
        if not rows:
            continue
        df = pd.DataFrame(
            rows, columns=["race_id", "horse_number", "nb_win_odds", "finish_rank"]
        )

        race_rows = conn.execute(
            "SELECT race_id, date, venue, surface, condition, distance, race_number "
            "FROM races WHERE date LIKE ?",
            (f"{yr}%",),
        ).fetchall()
        if not race_rows:
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

        rr_rows = conn.execute(
            "SELECT race_id, horse_number, gate_number, weight_carried FROM race_results "
            "WHERE race_id LIKE ? AND gate_number IS NOT NULL AND gate_number > 0",
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

        df["finish_rank"] = pd.to_numeric(df["finish_rank"], errors="coerce")
        df["is_winner"] = (df["finish_rank"] == 1).astype(int)
        df["ev_target"] = df["nb_win_odds"] * df["is_winner"]
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
                df.loc[valid.index, "nb_implied_prob"] = inv / inv.sum()
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
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  セグメントラベル付与
# ─────────────────────────────────────────────────────────────────────────────


def add_segment_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 距離帯 (200m 刻み)
    bins = [0, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 9999]
    labels = [
        "~1200",
        "1201~1400",
        "1401~1600",
        "1601~1800",
        "1801~2000",
        "2001~2200",
        "2201~2400",
        "2401~",
    ]
    df["dist_band"] = pd.cut(df["distance"], bins=bins, labels=labels, right=True)
    # 頭数帯
    df["n_horses_band"] = pd.cut(
        df["race_n_horses"],
        bins=[0, 8, 12, 99],
        labels=["少頭数(≤8)", "中頭数(9-12)", "多頭数(13+)"],
        right=True,
    )
    # 馬場グループ
    cond_map = {"良": "良", "稍重": "稍重", "重": "重・不良", "不良": "重・不良"}
    df["cond_group"] = df["condition"].map(cond_map).fillna("良")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  払戻突合
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
#  per-race 複勝シミュレーション + メタデータ記録
# ─────────────────────────────────────────────────────────────────────────────


def simulate_fukusho(
    pred_df: pd.DataFrame,
    payout_df: pd.DataFrame,
    use_edge: bool,
) -> dict | None:
    """
    Returns None if no bet placed (edge filter kills all picks).
    Returns dict: {invested, returned, meta columns}
    """
    pred = pred_df.sort_values("ev_score", ascending=False).reset_index(drop=True)
    if len(pred) < 3:
        return None

    top3 = pred.head(3)
    edge_vals = top3["edge"].fillna(0).tolist()

    picks = [
        int(r["horse_number"])
        for _, r in top3.iterrows()
        if (not use_edge) or edge_vals[int(r.name)] >= EDGE_THRESHOLD
    ]

    if not picks:
        return None

    inv = len(picks) * 100
    ret = sum(_lookup(payout_df, "複勝", (h,)) for h in picks)

    # メタデータ: レース情報 (1行目から取得)
    meta_row = pred.iloc[0]
    return {
        "invested": inv,
        "returned": ret,
        "venue": meta_row.get("venue", ""),
        "surface": meta_row.get("surface", ""),
        "dist_band": str(meta_row.get("dist_band", "")),
        "n_horses_band": str(meta_row.get("n_horses_band", "")),
        "cond_group": str(meta_row.get("cond_group", "")),
        "distance": float(meta_row.get("distance", 0)),
        "race_n_horses": int(meta_row.get("race_n_horses", 0)),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Edge 付与
# ─────────────────────────────────────────────────────────────────────────────


def add_edge(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["model_prob_norm"] = np.nan
    df["edge"] = np.nan
    for race_id, grp in df.groupby("race_id"):
        scores = grp["ev_score"].clip(lower=0)
        total = scores.sum()
        norm = (
            scores / total if total > 0 else pd.Series(1.0 / len(grp), index=grp.index)
        )
        df.loc[grp.index, "model_prob_norm"] = norm
        impl = grp["nb_implied_prob"].fillna(1.0 / len(grp))
        df.loc[grp.index, "edge"] = norm / impl.replace(0, np.nan)
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  1 フォールド実行
# ─────────────────────────────────────────────────────────────────────────────


def run_fold(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    person_lookup: pd.DataFrame,
    train_years: list[str],
    test_year: str,
    use_edge: bool,
) -> list[dict]:
    logger.info("学習データ構築: %s", "+".join(train_years))
    train_df = build_df(conn, res_conn, person_lookup, train_years)
    if train_df.empty:
        return []
    train_clean = train_df.dropna(subset=["nb_win_odds", "nb_implied_prob"]).copy()

    logger.info("テストデータ構築: %s", test_year)
    test_df = build_df(conn, res_conn, person_lookup, [test_year])
    if test_df.empty:
        return []
    test_clean = add_segment_cols(
        test_df.dropna(subset=["nb_win_odds", "nb_implied_prob"]).copy()
    )

    model = LGBMRegressor(**_LGB_PARAMS)
    model.fit(train_clean[FEATURE_COLS], train_clean["ev_target"])
    logger.info("モデル学習完了")

    test_clean["ev_score"] = model.predict(test_clean[FEATURE_COLS])
    test_clean = add_edge(test_clean)

    payout_rows = conn.execute(
        "SELECT race_id, bet_type, combination, payout FROM race_payouts WHERE race_id LIKE ?",
        (f"{test_year}%",),
    ).fetchall()
    if not payout_rows:
        logger.error("払戻データなし")
        return []
    payout_global = pd.DataFrame(
        payout_rows, columns=["race_id", "bet_type", "combination", "payout"]
    )

    records: list[dict] = []
    race_ids = test_clean["race_id"].unique()
    logger.info("バックテスト: %d レース", len(race_ids))

    for i, race_id in enumerate(race_ids):
        pred = test_clean[test_clean["race_id"] == race_id]
        payout = payout_global[payout_global["race_id"] == race_id]
        if pred.empty or payout.empty:
            continue
        result = simulate_fukusho(pred, payout, use_edge)
        if result:
            result["race_id"] = race_id
            result["year"] = test_year
            records.append(result)

        if (i + 1) % 500 == 0:
            logger.info("  %d / %d", i + 1, len(race_ids))

    logger.info("フォールド完了: %d 件", len(records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
#  ROI 集計ユーティリティ
# ─────────────────────────────────────────────────────────────────────────────


def roi_stats(df: pd.DataFrame, label: str = "") -> dict:
    if df.empty:
        return {}
    n = len(df)
    invested = float(df["invested"].sum())
    returned = float(df["returned"].sum())
    hits = int((df["returned"] > 0).sum())
    roi = returned / invested * 100 if invested else 0.0
    return {
        "label": label,
        "n": n,
        "invested": int(invested),
        "returned": int(returned),
        "pnl": int(returned - invested),
        "roi": round(roi, 1),
        "hits": hits,
        "hit_rate": round(hits / n * 100, 1) if n else 0.0,
    }


def print_seg_table(title: str, rows: list[dict], min_n: int = MIN_SEG_RACES) -> None:
    rows = [r for r in rows if r.get("n", 0) >= min_n]
    rows = sorted(rows, key=lambda x: -x.get("roi", 0))
    print(f"\n  {title}")
    print(f"  {'条件':<35} {'件数':>6} {'ROI':>8} {'損益':>10}")
    print("  " + "-" * 65)
    for r in rows:
        flag = "✅" if r["roi"] >= MIN_SEG_ROI else ("🔶" if r["roi"] >= 90 else "")
        print(
            f"  {r['label']:<35} {r['n']:>6} {r['roi']:>7.1f}% {r['pnl']:>+10,} {flag}"
        )


# ─────────────────────────────────────────────────────────────────────────────
#  セグメント分析
# ─────────────────────────────────────────────────────────────────────────────


def analyze_segments(df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """
    1D と 2D セグメントの ROI を算出して返す。
    Returns: (1d_rows, 2d_rows)
    """
    seg_dims = {
        "venue": df["venue"].unique(),
        "surface": df["surface"].unique(),
        "dist_band": df["dist_band"].unique(),
        "n_horses_band": df["n_horses_band"].unique(),
        "cond_group": df["cond_group"].unique(),
    }

    rows_1d: list[dict] = []
    for dim, vals in seg_dims.items():
        for val in vals:
            mask = df[dim] == val
            s = roi_stats(df[mask], label=f"{dim}={val}")
            if s:
                rows_1d.append(s)

    rows_2d: list[dict] = []
    dim_list = list(seg_dims.keys())
    for i in range(len(dim_list)):
        for j in range(i + 1, len(dim_list)):
            d1, d2 = dim_list[i], dim_list[j]
            for v1 in seg_dims[d1]:
                for v2 in seg_dims[d2]:
                    mask = (df[d1] == v1) & (df[d2] == v2)
                    s = roi_stats(df[mask], label=f"{d1}={v1} & {d2}={v2}")
                    if s:
                        rows_2d.append(s)

    return rows_1d, rows_2d


# ─────────────────────────────────────────────────────────────────────────────
#  良いセグメントに絞ったROI計算
# ─────────────────────────────────────────────────────────────────────────────


def apply_filter(df: pd.DataFrame, rules: dict[str, list]) -> pd.DataFrame:
    """
    rules: {dim: [valid_values, ...]}  → AND 条件でフィルタリング
    """
    mask = pd.Series(True, index=df.index)
    for dim, vals in rules.items():
        mask = mask & df[dim].isin(vals)
    return df[mask]


# ─────────────────────────────────────────────────────────────────────────────
#  main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    conn = sqlite3.connect(str(MAIN_DB))
    res_conn = sqlite3.connect(str(RESEARCH_DB))

    print()
    print("=" * 75)
    print("  複勝 ROI セグメント分析  (Sprint 1 / Edge > 1.1)")
    print("=" * 75)

    logger.info("騎手・調教師 rolling 統計計算中...")
    person_lookup = build_person_lookup(conn)
    logger.info("rolling 統計完了: %d行", len(person_lookup))

    # ── F4: 学習 2021-2024 → テスト 2025 ─────────────────────────────────────
    print("\n【フォールド4】学習: 2021+2022+2023+2024 → テスト: 2025\n")
    recs_2025 = run_fold(
        conn,
        res_conn,
        person_lookup,
        train_years=["2021", "2022", "2023", "2024"],
        test_year="2025",
        use_edge=False,  # edge なし → 全件集計してセグメント発見
    )
    df25 = pd.DataFrame(recs_2025)

    if df25.empty:
        logger.error("2025データなし")
        return

    # 全体 ROI
    overall = roi_stats(df25, "全体")
    print(f"  ▶ 全体: n={overall['n']}, ROI={overall['roi']}%, pnl={overall['pnl']:+,}")

    # 1D セグメント分析
    rows_1d, rows_2d = analyze_segments(df25)

    print_seg_table("1D セグメント別 ROI (n≥30)", rows_1d, min_n=MIN_SEG_RACES)

    print_seg_table(
        "2D セグメント別 ROI (n≥30) — 上位30件のみ",
        sorted(rows_2d, key=lambda x: -x.get("roi", 0))[:30],
        min_n=MIN_SEG_RACES,
    )

    # ── 収益セグメント自動抽出 ──────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  収益セグメント候補 (ROI ≥ 100%, n ≥ 30)")
    print("=" * 75)

    profitable_1d = [
        r for r in rows_1d if r["roi"] >= MIN_SEG_ROI and r["n"] >= MIN_SEG_RACES
    ]
    profitable_2d = [
        r for r in rows_2d if r["roi"] >= MIN_SEG_ROI and r["n"] >= MIN_SEG_RACES
    ]

    for r in sorted(profitable_1d, key=lambda x: -x["roi"]):
        print(f"  ✅ [1D] {r['label']}: ROI={r['roi']}%  n={r['n']}")

    for r in sorted(profitable_2d, key=lambda x: -x["roi"])[:20]:
        print(f"  ✅ [2D] {r['label']}: ROI={r['roi']}%  n={r['n']}")

    # ── 最も有望な複合フィルターを構築 ─────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  フィルター組合せ検証 (2025 in-sample)")
    print("=" * 75)

    # ROI > 100% の 1D 条件をそれぞれ適用してどう変わるか確認
    # まず各 dim の勝ち値をリストアップ
    winning_vals: dict[str, list] = {}
    for r in profitable_1d:
        label = r["label"]
        for dim in ["venue", "surface", "dist_band", "n_horses_band", "cond_group"]:
            if label.startswith(f"{dim}="):
                val = label[len(f"{dim}=") :]
                winning_vals.setdefault(dim, []).append(val)

    print("\n  発見した収益条件:")
    for dim, vals in winning_vals.items():
        print(f"    {dim}: {vals}")

    if winning_vals:
        filtered_df = apply_filter(df25, winning_vals)
        s = roi_stats(filtered_df, "全収益条件AND")
        print(
            f"\n  ▶ 全収益条件 AND フィルター: n={s.get('n', 0)},"
            f" ROI={s.get('roi', 0)}%, pnl={s.get('pnl', 0):+,}"
        )
    else:
        print("  収益条件なし")
        # 上位 1D 条件でフィルター作成 (ROI > 95% かつ n >= 30)
        near_1d = [r for r in rows_1d if r["roi"] >= 95 and r["n"] >= MIN_SEG_RACES]
        for r in sorted(near_1d, key=lambda x: -x["roi"])[:5]:
            print(f"  近接候補: {r['label']}: ROI={r['roi']}%  n={r['n']}")
        for r in near_1d:
            for dim in ["venue", "surface", "dist_band", "n_horses_band", "cond_group"]:
                if r["label"].startswith(f"{dim}="):
                    val = r["label"][len(f"{dim}=") :]
                    winning_vals.setdefault(dim, []).append(val)

    # ── F3: 学習 2021-2023 → テスト 2024 (OOS 検証) ─────────────────────────
    print("\n" + "=" * 75)
    print("  OOS 検証: フォールド3 (学習 2021-2023, テスト 2024)")
    print("=" * 75)

    recs_2024 = run_fold(
        conn,
        res_conn,
        person_lookup,
        train_years=["2021", "2022", "2023"],
        test_year="2024",
        use_edge=False,
    )
    df24 = pd.DataFrame(recs_2024)

    if not df24.empty:
        o24 = roi_stats(df24, "全体 2024")
        print(f"\n  ▶ 2024 全体: n={o24['n']}, ROI={o24['roi']}%")

        if winning_vals:
            f24 = apply_filter(df24, winning_vals)
            s24 = roi_stats(f24, "2024 フィルター")
            print(
                f"  ▶ 2024 フィルター適用: n={s24.get('n', 0)},"
                f" ROI={s24.get('roi', 0)}%,"
                f" pnl={s24.get('pnl', 0):+,}"
            )

        # 2024 セグメント詳細
        rows_1d_24, _ = analyze_segments(df24)
        print_seg_table("2024 1D セグメント ROI (n≥10)", rows_1d_24, min_n=10)

    # ── Edge フィルター込みの最終結果 ─────────────────────────────────────────
    print("\n" + "=" * 75)
    print(f"  Edge > {EDGE_THRESHOLD} フィルター + セグメントフィルター (2025)")
    print("=" * 75)

    recs_2025_edge = run_fold(
        conn,
        res_conn,
        person_lookup,
        train_years=["2021", "2022", "2023", "2024"],
        test_year="2025",
        use_edge=True,
    )
    df25e = pd.DataFrame(recs_2025_edge)
    if not df25e.empty:
        oe = roi_stats(df25e, "全体(Edge有)")
        print(f"\n  ▶ 全体 (Edge > {EDGE_THRESHOLD}): n={oe['n']}, ROI={oe['roi']}%")

        if winning_vals:
            fe = apply_filter(df25e, winning_vals)
            se = roi_stats(fe, "フィルター+Edge(2025)")
            print(
                f"  ▶ セグメント + Edge: n={se.get('n', 0)},"
                f" ROI={se.get('roi', 0)}%,"
                f" pnl={se.get('pnl', 0):+,}"
            )

    # ── 最終推奨フィルター条件のプリント ──────────────────────────────────────
    print("\n" + "=" * 75)
    print("  最終推奨フィルター (bet_generator.py 実装用)")
    print("=" * 75)
    print()
    print("  FUKUSHO_ELITE_FILTER = {")
    for dim, vals in winning_vals.items():
        print(f'    "{dim}": {vals},')
    print("  }")
    print(f"  EDGE_THRESHOLD = {EDGE_THRESHOLD}")
    print()

    conn.close()
    res_conn.close()


if __name__ == "__main__":
    main()


# ─────────────────────────────────────────────────────────────────────────────
#  predictions × prediction_results × races ベースのセグメント分析 API
#  （券種 / 会場 / 馬場状態 / コース種別 / モデル / 距離帯 で ROI 集計）
# ─────────────────────────────────────────────────────────────────────────────

_GROUP_SQL: dict[str, str] = {
    "bet_type": "p.bet_type",
    "venue": "r.venue",
    "surface": "r.surface",
    "condition": "r.condition",
    "model": "p.model_type",
    "distance": """CASE
                     WHEN r.distance <= 1400 THEN '短距離(~1400)'
                     WHEN r.distance <= 1800 THEN '中距離(1401-1800)'
                     WHEN r.distance <= 2200 THEN '中長距離(1801-2200)'
                     ELSE '長距離(2201~)'
                   END""",
}

_SEGMENT_BASE_QUERY = """
SELECT
    {group_expr}           AS segment,
    COUNT(*)               AS n_bets,
    SUM(CASE WHEN pr.is_hit=1 THEN 1 ELSE 0 END) AS hits,
    SUM(COALESCE(pr.payout,0) - COALESCE(pr.profit,0)) AS total_invest,
    SUM(COALESCE(pr.payout,0))                          AS total_payout,
    ROUND(
        100.0 * SUM(COALESCE(pr.payout,0))
        / NULLIF(SUM(COALESCE(pr.payout,0) - COALESCE(pr.profit,0)), 0),
        1
    ) AS roi
FROM predictions p
JOIN prediction_results pr ON pr.prediction_id = p.id
JOIN races r ON r.race_id = p.race_id
GROUP BY {group_expr}
ORDER BY roi ASC
"""


def analyze_by_segment(
    db_path: str = "data/umalogi.db",
    group_by: str = "bet_type",
    min_bets: int = 1,
) -> list[dict]:
    """指定セグメントで ROI を集計して返す。

    predictions / prediction_results / races テーブルを結合し、
    指定した集計軸ごとの ROI・ベット数・投資額・回収額を算出する。

    Args:
        db_path:  SQLite パス
        group_by: "bet_type" | "venue" | "surface" | "condition" | "model" | "distance"
        min_bets: この件数未満のセグメントは結果から除外

    Returns:
        {"segment", "n_bets", "hits", "total_invest", "total_payout", "roi"} のリスト
    """
    if group_by not in _GROUP_SQL:
        raise ValueError(f"group_by は {list(_GROUP_SQL)} のいずれかを指定してください")

    group_expr = _GROUP_SQL[group_by]
    sql = _SEGMENT_BASE_QUERY.format(group_expr=group_expr)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows if (r["n_bets"] or 0) >= min_bets]


def _print_segment_table(rows: list[dict], group_by: str) -> None:
    """結果をマークダウンテーブルで標準出力に出力する。"""
    print(f"\n## セグメント分析: {group_by}\n")
    print("| セグメント | ベット数 | 的中数 | 投資額 | 回収額 | ROI% |")
    print("|---|---|---|---|---|---|")
    for r in rows:
        invest = r["total_invest"] or 0
        payout = r["total_payout"] or 0
        roi_val = r["roi"] if r["roi"] is not None else 0.0
        print(
            f"| {r['segment']} | {r['n_bets']} | {r['hits']} "
            f"| ¥{invest:,} | ¥{payout:,} | {roi_val:.1f}% |"
        )


def main_segment() -> None:
    """CLI エントリポイント: 全セグメントまたは指定セグメントを出力する。"""
    import argparse

    parser = argparse.ArgumentParser(description="UMALOGI セグメント別 ROI 分析")
    parser.add_argument(
        "--group",
        choices=list(_GROUP_SQL.keys()),
        default=None,
        help="集計軸（省略時は全軸を出力）",
    )
    parser.add_argument(
        "--min-bets",
        type=int,
        default=20,
        help="最小ベット数フィルター（デフォルト20）",
    )
    parser.add_argument("--db", default="data/umalogi.db", help="SQLite パス")
    args = parser.parse_args()

    groups = [args.group] if args.group else list(_GROUP_SQL.keys())
    for g in groups:
        rows = analyze_by_segment(db_path=args.db, group_by=g, min_bets=args.min_bets)
        _print_segment_table(rows, g)
