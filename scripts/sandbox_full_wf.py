"""
scripts/sandbox_full_wf.py — netkeiba全面解禁・5カ年ウォークフォワードシミュレーション

【データ設計】
  特徴量 : horse_odds.win_odds (netkeiba) + races 公開情報 (JVLink)
  ラベル : horse_odds.rank=1 → is_winner (netkeiba, 事後情報だがラベル用途は正当)
  EVターゲット: is_winner × win_odds (全5カ年で計算可能。payout不要)
  払戻突合:
    2022-2023 テスト → 単勝: win_odds×100 (近似), 複勝: ×100/3 (近似)
    2024-2025 テスト → 全8券種: umalogi.db race_payouts (実データ)

【フォールド】
  F1: 2021           → 2022
  F2: 2021+2022      → 2023
  F3: 2021+2022+2023 → 2024
  F4: 2021+...+2024  → 2025

【リーク排除】
  - horse_odds.rank は特徴量から除外（事後情報）
  - テストデータは学習に一切含めない（完全OOS）

使い方:
  py scripts/sandbox_full_wf.py
  py scripts/sandbox_full_wf.py --folds 3 4   # 2024・2025のみ
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

# ── 静的エンコーダー ─────────────────────────────────────────────────
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

# 特徴量（全て事前公開情報 + オッズ）
FEATURE_COLS = [
    "nb_win_odds",  # 単勝オッズ
    "nb_implied_prob",  # implied probability (正規化オーバーラウンド補正)
    "nb_log_odds",  # log1p(win_odds) — 非線形変換
    "venue_code",
    "surface_code",
    "condition_code",
    "distance",
    "race_number",
    "month",
    "race_n_horses",
]

# LightGBM (EVリグレッサ)
_LGB: dict[str, Any] = {
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
#  特徴量・ラベル構築
# ─────────────────────────────────────────────────────────────────────────────


def build_df(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    years: list[str],
) -> pd.DataFrame:
    """
    horse_odds (netkeiba) × races (JVLink) を結合して学習/テスト DataFrame を生成。

    EVターゲット = is_winner × win_odds
    (win_odds ≈ 単勝払戻/100 なので、勝ち馬の期待値を近似)

    NOTE: horse_odds.rank = 実際の着順 (事後情報)
          → is_winner / is_placed のラベル付けにのみ使用
          → 特徴量には一切使用しない
    """
    dfs: list[pd.DataFrame] = []

    for yr in years:
        # 1. horse_odds 全馬 (win_odds + rank=着順ラベル用)
        rows = res_conn.execute(
            """
            SELECT race_id, horse_number,
                   CAST(win_odds AS REAL) AS nb_win_odds,
                   CAST(rank AS INTEGER)  AS finish_rank
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

        # 2. races (会場・馬場・距離 etc.)
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

        # 3. gate_number (枠連計算用: race_results 2024+ のみ存在)
        # rank フィルターなし — rank=None (出走前エントリー) も gate_number を持つ
        gate_rows = conn.execute(
            """
            SELECT race_id, horse_number, gate_number
            FROM race_results
            WHERE race_id LIKE ? AND gate_number IS NOT NULL AND gate_number > 0
            """,
            (f"{yr}%",),
        ).fetchall()
        gate_map: dict[tuple[str, int], int] = {
            (r, int(h)): int(g or 0) for r, h, g in gate_rows
        }
        df["gate_number"] = df.apply(
            lambda r: gate_map.get((r["race_id"], int(r["horse_number"])), 0),
            axis=1,
        )

        # 4. ラベル付与 (horse_odds.rank から)
        df["finish_rank"] = pd.to_numeric(df["finish_rank"], errors="coerce")
        df["is_winner"] = (df["finish_rank"] == 1).astype(int)
        df["is_placed"] = (df["finish_rank"] <= 3).astype(int)

        # 5. EVターゲット = win_odds × is_winner (payout 不要、全年対応)
        df["ev_target"] = df["nb_win_odds"] * df["is_winner"]

        # 6. 特徴量エンジニアリング
        df["nb_win_odds"] = pd.to_numeric(df["nb_win_odds"], errors="coerce")

        # implied_prob (過剰率補正あり)
        df["nb_implied_prob"] = np.nan
        df["nb_log_odds"] = np.nan
        df["race_n_horses"] = 0

        for race_id, grp in df.groupby("race_id"):
            idx = grp.index
            df.loc[idx, "race_n_horses"] = len(grp)
            valid = grp["nb_win_odds"].dropna()
            if len(valid) > 0:
                inv = 1.0 / valid.clip(lower=1.0)
                # per-race 正規化
                df.loc[idx[grp.index.isin(valid.index)], "nb_implied_prob"] = (
                    inv / inv.sum()
                )
                df.loc[idx, "nb_log_odds"] = np.log1p(grp["nb_win_odds"])

        df["surface_code"] = df["surface"].map(_SURFACE_MAP).fillna(-1).astype(int)
        df["condition_code"] = (
            df["condition"].map(_CONDITION_MAP).fillna(-1).astype(int)
        )
        df["venue_code"] = df["venue"].map(_VENUE_MAP).fillna(-1).astype(int)
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce").fillna(1600)
        df["race_number"] = pd.to_numeric(df["race_number"], errors="coerce").fillna(6)
        df["month"] = df["date"].str[5:7].astype(int, errors="ignore")

        dfs.append(df)
        logger.info(
            "  %s: %d行 / %d レース / winner=%d",
            yr,
            len(df),
            df["race_id"].nunique(),
            df["is_winner"].sum(),
        )

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


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
#  1レース シミュレーション
# ─────────────────────────────────────────────────────────────────────────────


def simulate_race(
    pred_df: pd.DataFrame,
    payout_df: pd.DataFrame | None,
    test_year: str,
) -> dict[str, tuple[float, float]]:
    """
    EV スコア上位選択で全対象券種をシミュレート。
    payout_df=None or test_year in (2022,2023) → 単勝のみ (win_odds から近似)
    """
    pred_sorted = pred_df.sort_values("ev_score", ascending=False)
    top1 = int(pred_sorted.iloc[0]["horse_number"])
    top3_rows = pred_sorted.head(3)
    top3 = [int(r["horse_number"]) for _, r in top3_rows.iterrows()]
    gate_map = dict(
        zip(pred_df["horse_number"].astype(int), pred_df["gate_number"].astype(int))
    )

    # 勝ち馬の win_odds (払戻近似用)
    win_odds_map = dict(
        zip(pred_df["horse_number"].astype(int), pred_df["nb_win_odds"])
    )

    results: dict[str, tuple[float, float]] = {}

    use_actual = (payout_df is not None) and (test_year in ("2024", "2025", "2026"))

    if use_actual and payout_df is not None:
        # ── 実払戻データ使用 (2024-2025) ─────────────────────────────────
        # 単勝
        ret = _lookup(payout_df, "単勝", (top1,))
        results["単勝"] = (100, ret)

        # 複勝 (top3 × 3点)
        if len(top3) >= 3:
            total = sum(_lookup(payout_df, "複勝", (h,)) for h in top3)
            results["複勝"] = (300, total)

        # 枠連
        if len(top3) >= 2:
            g0, g1 = gate_map.get(top3[0], 0), gate_map.get(top3[1], 0)
            if g0 > 0 and g1 > 0:
                ret = _lookup(payout_df, "枠連", tuple(sorted([g0, g1])))
                results["枠連"] = (100, ret)

        # 馬連
        if len(top3) >= 2:
            ret = _lookup(payout_df, "馬連", tuple(sorted([top3[0], top3[1]])))
            results["馬連"] = (100, ret)

        # ワイド (3点)
        if len(top3) >= 3:
            pairs = [(top3[0], top3[1]), (top3[0], top3[2]), (top3[1], top3[2])]
            total = sum(_lookup(payout_df, "ワイド", tuple(sorted(p))) for p in pairs)
            results["ワイド"] = (300, total)

        # 馬単
        if len(top3) >= 2:
            ret = _lookup(payout_df, "馬単", (top3[0], top3[1]))
            results["馬単"] = (100, ret)

        # 三連複
        if len(top3) >= 3:
            ret = _lookup(payout_df, "三連複", tuple(sorted(top3)))
            results["三連複"] = (100, ret)

        # 三連単
        if len(top3) >= 3:
            ret = _lookup(payout_df, "三連単", (top3[0], top3[1], top3[2]))
            results["三連単"] = (100, ret)

    else:
        # ── 近似払戻 (2022-2023): 単勝のみ ──────────────────────────────
        # 勝ち馬の is_winner フラグ
        winner_horse = pred_df.loc[pred_df["is_winner"] == 1, "horse_number"]
        actual_winner = int(winner_horse.iloc[0]) if not winner_horse.empty else -1

        if top1 == actual_winner and actual_winner > 0:
            # 的中: win_odds × 100 円
            payout = float(win_odds_map.get(actual_winner, 1.0)) * 100.0
            results["単勝"] = (100, payout)
        else:
            results["単勝"] = (100, 0.0)

    return results


# ─────────────────────────────────────────────────────────────────────────────
#  集計・分析
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
    parser.add_argument(
        "--folds", nargs="*", type=int, help="実行するフォールド番号 (1-4)"
    )
    args = parser.parse_args()
    run_folds = set(args.folds) if args.folds else {1, 2, 3, 4}

    conn = sqlite3.connect(str(MAIN_DB))
    res_conn = sqlite3.connect(str(RESEARCH_DB))

    folds = [
        (1, ["2021"], "2022"),
        (2, ["2021", "2022"], "2023"),
        (3, ["2021", "2022", "2023"], "2024"),
        (4, ["2021", "2022", "2023", "2024"], "2025"),
    ]

    all_records: list[dict] = []

    for fold_no, train_years, test_year in folds:
        if fold_no not in run_folds:
            continue

        label = "+".join(train_years)
        logger.info("")
        logger.info("═" * 60)
        logger.info("フォールド %d: 学習=%s → テスト=%s", fold_no, label, test_year)
        logger.info("═" * 60)

        # 学習データ
        logger.info("学習データ構築中 (%s)...", label)
        train_df = build_df(conn, res_conn, train_years)
        if train_df.empty:
            logger.error("学習データなし: スキップ")
            continue
        train_clean = train_df.dropna(subset=FEATURE_COLS).copy()

        # テストデータ
        logger.info("テストデータ構築中 (%s)...", test_year)
        test_df = build_df(conn, res_conn, [test_year])
        if test_df.empty:
            logger.error("テストデータなし: スキップ")
            continue
        test_clean = test_df.dropna(subset=FEATURE_COLS).copy()

        logger.info(
            "学習: %d行 / %d レース  ev_target非ゼロ=%d行",
            len(train_clean),
            train_clean["race_id"].nunique(),
            (train_clean["ev_target"] > 0).sum(),
        )
        logger.info(
            "テスト: %d行 / %d レース", len(test_clean), test_clean["race_id"].nunique()
        )

        # EV リグレッサ学習
        model = LGBMRegressor(**_LGB)
        model.fit(train_clean[FEATURE_COLS], train_clean["ev_target"])
        logger.info("モデル学習完了")

        # テスト予測
        test_clean["ev_score"] = model.predict(test_clean[FEATURE_COLS])

        # 払戻データ (2024-2025 のみ実データ)
        payout_rows = conn.execute(
            "SELECT race_id, bet_type, combination, payout FROM race_payouts WHERE race_id LIKE ?",
            (f"{test_year}%",),
        ).fetchall()
        payout_global: pd.DataFrame | None = None
        if payout_rows:
            payout_global = pd.DataFrame(
                payout_rows, columns=["race_id", "bet_type", "combination", "payout"]
            )
            logger.info("払戻データ: %d行", len(payout_global))
        else:
            logger.info("払戻データなし → 単勝近似を使用")

        # バックテスト
        race_ids = test_clean["race_id"].unique()
        logger.info("バックテスト開始: %d レース", len(race_ids))

        for i, race_id in enumerate(race_ids):
            pred = test_clean[test_clean["race_id"] == race_id]
            payout = (
                payout_global[payout_global["race_id"] == race_id]
                if payout_global is not None
                else None
            )

            if pred.empty:
                continue
            if (payout is None or payout.empty) and test_year in ("2024", "2025"):
                continue  # 実払戻なしのレースはスキップ

            for bt, (inv, ret) in simulate_race(pred, payout, test_year).items():
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

        logger.info("フォールド %d 完了 (%s)", fold_no, test_year)

    conn.close()
    res_conn.close()

    if not all_records:
        logger.error("記録なし")
        return

    # ─── レポート生成 ──────────────────────────────────────────────────────────
    tested_years_full = ["2024", "2025"]  # 全8券種
    tested_years_approx = ["2022", "2023"]  # 単勝のみ
    tested_years = [
        y
        for y in ["2022", "2023", "2024", "2025"]
        if any(r["year"] == y for r in all_records)
    ]

    all_stats: dict[tuple[str, str], dict] = {}
    for y in tested_years:
        bts = ["単勝"] if y in tested_years_approx else ALL_BET_TYPES
        for bt in bts:
            all_stats[(y, bt)] = calc_stats(all_records, y, bt)

    # ── 表1: 年別ROI ──────────────────────────────────────────────────────────
    print("\n" + "═" * 90)
    print("  5カ年ウォークフォワードシミュレーション最終レポート")
    print("  netkeiba 全面解禁 / EV = win_odds × is_winner / リーク排除済み")
    print("═" * 90)

    print("\n■ 【1】年別 ROI マトリクス\n")
    print(
        f"{'年度':<6} {'券種':<6} {'件数':>7} {'的中':>6} {'投資':>9} {'払戻':>12} {'損益':>10} {'ROI':>8} {'MaxDD':>10}"
    )
    print("-" * 90)

    for y in tested_years:
        bts_show = ["単勝"] if y in tested_years_approx else ALL_BET_TYPES
        for bt in bts_show:
            s = all_stats.get((y, bt), {})
            if not s:
                continue
            flag = "✅" if s["roi"] >= 100 else ("🔶" if s["roi"] >= 80 else "❌")
            print(
                f"{y:<6} {bt:<6} {s['n']:>7} {s['hits']:>5}件 "
                f"{s['invested']:>9,} {s['returned']:>12,} "
                f"{s['pnl']:>+10,} {s['roi']:>7.1f}% "
                f"{s['max_dd']:>+10,.0f} {flag}"
            )
        print()

    # ── 表2: 外れ値分析 ──────────────────────────────────────────────────────
    print("\n■ 【2】TOP1的中払戻 除外時の ROI（外れ値依存度）\n")
    print(
        f"{'年度':<6} {'券種':<6} {'TOP1払戻':>12} {'TOP1寄与':>10} {'除外ROI':>10} {'通常ROI':>9}"
    )
    print("-" * 65)
    for y in tested_years:
        bts_show = ["単勝"] if y in tested_years_approx else ALL_BET_TYPES
        for bt in bts_show:
            s = all_stats.get((y, bt), {})
            if not s or s["returned"] == 0:
                continue
            warn = " ⚠️" if s["top1_pct"] >= 50 else ""
            print(
                f"{y:<6} {bt:<6} {s['top1_ret']:>12,} "
                f"{s['top1_pct']:>9.1f}% "
                f"{s['roi_ex']:>9.1f}% "
                f"{s['roi']:>8.1f}%{warn}"
            )
        print()

    # ── 表3: 通算サマリー (2024-2025) ────────────────────────────────────────
    print("\n■ 【3】通算サマリー（2024+2025 実払戻データ）\n")
    print(
        f"{'券種':<6} {'件数':>7} {'的中':>6} {'的中率':>8} {'投資計':>12} {'払戻計':>12} {'損益':>10} {'通算ROI':>9} {'評価'}"
    )
    print("-" * 90)

    summary_rows = []
    for bt in ALL_BET_TYPES:
        total_n = total_invested = total_returned = total_hits = 0
        for y in tested_years_full:
            s = all_stats.get((y, bt), {})
            if s:
                total_n += s["n"]
                total_hits += s["hits"]
                total_invested += s["invested"]
                total_returned += s["returned"]
        if total_n == 0:
            continue
        roi = total_returned / total_invested * 100 if total_invested else 0
        hit = total_hits / total_n * 100
        flag = "✅" if roi >= 120 else ("🔶" if roi >= 100 else "❌")
        summary_rows.append(
            (
                roi,
                bt,
                total_n,
                total_hits,
                hit,
                total_invested,
                total_returned,
                roi,
                flag,
            )
        )
        print(
            f"{bt:<6} {total_n:>7} {total_hits:>5}件 {hit:>7.1f}% "
            f"{total_invested:>12,} {total_returned:>12,} "
            f"{total_returned - total_invested:>+10,} {roi:>8.1f}% {flag}"
        )

    # ── 客観評価 ─────────────────────────────────────────────────────────────
    print("\n" + "═" * 90)
    print("■ 【4】AIデータサイエンティストによる最終判定（忖度なし）")
    print("═" * 90)

    print("\n── 年別推移 ──")
    for bt in ALL_BET_TYPES:
        rois = [all_stats.get((y, bt), {}).get("roi") for y in tested_years_full]
        ex_rois = [all_stats.get((y, bt), {}).get("roi_ex") for y in tested_years_full]
        valid = [
            (y, r, e)
            for y, r, e in zip(tested_years_full, rois, ex_rois)
            if r is not None
        ]
        if not valid:
            continue
        avg_roi = np.mean([r for _, r, _ in valid])
        avg_ex = np.mean([e for _, _, e in valid])
        std_roi = np.std([r for _, r, _ in valid])
        black = sum(1 for _, r, _ in valid if r >= 100)
        line_parts = [f"{y}={r:.1f}%" for y, r, _ in valid]
        verdict = (
            "✅ 商用水準"
            if black == len(valid) and avg_ex >= 120
            else "🔶 条件付き"
            if black >= 1 and avg_roi >= 100
            else "❌ 赤字"
        )
        print(
            f"  {bt:<6}: {' / '.join(line_parts)}  avg={avg_roi:.1f}% std={std_roi:.1f}%  {verdict}"
        )

    print("""
── 総合所見 ──

【改善点（前回との差異）】
  EVターゲット = win_odds × is_winner に変更。
  2022-2023の学習データでも正しくEV信号が学習できるようになった。

【解釈の注意点】
  ① 2022-2023テストは単勝のみ（win_odds近似）。
     多馬連系券種の2022-2023の結果は非表示。
  ② 2024-2025のみ全8券種の実払戻データが存在し、最も信頼性が高い。
  ③ 有効フォールドは2年分(2024, 2025)。3年以上での確認が商用判断の最低ライン。

【ROI > 120% 券種が存在した場合の実運用方針】
  - 最初の1カ月は100円単位の最小ロットで実損益を記録
  - 2レース以上連続の最大ドローダウンに耐えられる資金準備
  - 的中率が理論値から±50%以上乖離したら戦略を一時停止
""")


if __name__ == "__main__":
    main()
