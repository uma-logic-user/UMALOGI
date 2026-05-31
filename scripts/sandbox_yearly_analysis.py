"""
scripts/sandbox_yearly_analysis.py — サンドボックス年別ウォークフォワード分析

【データ制約】
  ラベル  : horse_odds.rank (netkeiba_research.db) — rank=1が勝ち馬、≤3が複勝圏
  払戻突合: race_payouts (umalogi.db) — 2024・2025のみ存在
  → 有効フォールド: 2022+2023→2024, 2022+2023+2024→2025

使い方:
  py scripts/sandbox_yearly_analysis.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

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

SANDBOX_FEATURE_COLS = [
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
]

_LGB_BASE: dict = {
    "n_estimators": 400,
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

TARGET_BET_TYPES = ["三連単", "馬単", "馬連", "三連複"]


# ─── 特徴量構築（horse_odds.rankをラベルとして使用） ───────────────────────


def build_df(
    conn: sqlite3.Connection,
    res_conn: sqlite3.Connection,
    years: list[str],
) -> pd.DataFrame:
    """
    horse_odds (全馬) を基底とし、horse_odds.rank を勝ち馬ラベルとして使う。
    horse_odds.rank は着順を格納（netkeiba R&Dデータ特例）。
    特徴量としては使用しない（除外済み）。
    """
    all_dfs: list[pd.DataFrame] = []

    for year_prefix in years:
        # horse_odds: win_odds + rank(=着順、ラベル用)
        rows = res_conn.execute(
            """
            SELECT race_id, horse_number, win_odds, rank AS finish_rank
            FROM horse_odds
            WHERE substr(race_id,1,4) = ?
            """,
            (year_prefix,),
        ).fetchall()
        if not rows:
            logger.warning("horse_odds なし: year=%s", year_prefix)
            continue

        df = pd.DataFrame(
            rows, columns=["race_id", "horse_number", "nb_win_odds", "finish_rank"]
        )

        # races テーブルからレース情報
        race_rows = conn.execute(
            "SELECT race_id, date, venue, surface, condition, distance, race_number FROM races WHERE date LIKE ?",
            (f"{year_prefix}%",),
        ).fetchall()
        if not race_rows:
            logger.warning("races なし: year=%s", year_prefix)
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

        # gate_number は race_results から取得（2024以降のみ存在）
        result_rows = conn.execute(
            "SELECT race_id, horse_number, gate_number FROM race_results WHERE race_id LIKE ? AND rank IN (1,2,3)",
            (f"{year_prefix}%",),
        ).fetchall()
        gate_lookup: dict[tuple[str, int], int] = {}
        for rid, hn, gn in result_rows:
            gate_lookup[(rid, int(hn))] = int(gn or 0)

        # ラベル付与（horse_odds.rank から）
        df["finish_rank"] = pd.to_numeric(df["finish_rank"], errors="coerce")
        df["is_winner"] = (df["finish_rank"] == 1).astype(int)
        df["is_placed"] = (df["finish_rank"] <= 3).astype(int)

        # 単勝払戻 (EV target 用) — 存在する場合のみ
        payout_rows = conn.execute(
            "SELECT race_id, combination, payout FROM race_payouts WHERE race_id LIKE ? AND bet_type='単勝'",
            (f"{year_prefix}%",),
        ).fetchall()
        tansho = {rid: payout for rid, combo, payout in payout_rows}
        df["payout_tansho"] = df["race_id"].map(tansho).fillna(0.0)
        df["ev_target"] = df["is_winner"] * df["payout_tansho"]

        # gate_number (inference用: テスト年のみ有効)
        df["gate_number"] = df.apply(
            lambda r: gate_lookup.get((r["race_id"], int(r["horse_number"])), 0),
            axis=1,
        )

        # 特徴量エンジニアリング
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
                df.loc[idx[grp["nb_win_odds"].notna()], "nb_implied_prob"] = (
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

        logger.info(
            "  %s: %d行 / %d レース / winner=%d",
            year_prefix,
            len(df),
            df["race_id"].nunique(),
            df["is_winner"].sum(),
        )
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


# ─── payout lookup ────────────────────────────────────────────────────────────


def _parse_combo(bet_type: str, combination: str) -> tuple[int, ...]:
    combination = str(combination).strip()
    sep = "→" if "→" in combination else ("-" if "-" in combination else None)
    if sep:
        parts = [int(x.strip()) for x in combination.split(sep) if x.strip().isdigit()]
    else:
        parts = [int(combination)] if combination.isdigit() else []
    if bet_type in ("馬単", "三連単"):
        return tuple(parts)
    return tuple(sorted(parts))


def _lookup_payout(
    payout_df: pd.DataFrame, bet_type: str, selected: tuple[int, ...]
) -> float:
    rows = payout_df[payout_df["bet_type"] == bet_type]
    for _, row in rows.iterrows():
        try:
            if _parse_combo(bet_type, str(row["combination"])) == selected:
                return float(row["payout"])
        except (ValueError, TypeError):
            continue
    return 0.0


def simulate_one_race(
    pred_df: pd.DataFrame,
    payout_df: pd.DataFrame,
) -> dict[str, tuple[float, float]]:
    """EV上位予測で全TARGET券種をシミュレート。{bet_type: (invested, returned)}"""
    pred_sorted = pred_df.sort_values("ev_score", ascending=False)
    top3 = [int(r["horse_number"]) for _, r in pred_sorted.head(3).iterrows()]
    gate_map = dict(
        zip(pred_df["horse_number"].astype(int), pred_df["gate_number"].astype(int))
    )

    results: dict[str, tuple[float, float]] = {}

    if len(top3) >= 2:
        # 馬連: sorted top2
        sel = tuple(sorted([top3[0], top3[1]]))
        results["馬連"] = (100, _lookup_payout(payout_df, "馬連", sel))

        # 馬単: ordered top2
        sel2 = (top3[0], top3[1])
        results["馬単"] = (100, _lookup_payout(payout_df, "馬単", sel2))

    if len(top3) >= 3:
        # 三連複: sorted top3
        sel3c = tuple(sorted(top3))
        results["三連複"] = (100, _lookup_payout(payout_df, "三連複", sel3c))

        # 三連単: ordered top3
        sel3t = (top3[0], top3[1], top3[2])
        results["三連単"] = (100, _lookup_payout(payout_df, "三連単", sel3t))

    return results


# ─── 年別集計 ──────────────────────────────────────────────────────────────


def calc_max_drawdown(invested: pd.Series, returned: pd.Series) -> float:
    pnl_series = (returned - invested).cumsum()
    running_max = pnl_series.cummax()
    return float((pnl_series - running_max).min())


def year_stats(records: list[dict], year: str, bet_type: str) -> dict:
    rows = [r for r in records if r["year"] == year and r["bet_type"] == bet_type]
    if not rows:
        return {}

    df = pd.DataFrame(rows).sort_values("race_id")
    n = len(df)
    invested = df["invested"].sum()
    returned = df["returned"].sum()
    hits = (df["returned"] > 0).sum()
    roi = returned / invested * 100 if invested else 0.0
    hit_rate = hits / n * 100
    max_dd = calc_max_drawdown(df["invested"], df["returned"])

    # 外れ値分析
    top1_ret = float(df["returned"].max())
    top1_race = df.loc[df["returned"].idxmax(), "race_id"]
    ret_ex = returned - top1_ret
    roi_ex = ret_ex / invested * 100 if invested else 0.0
    top1_pct = top1_ret / returned * 100 if returned else 0.0

    return {
        "year": year,
        "bet_type": bet_type,
        "n_races": n,
        "n_hits": int(hits),
        "hit_rate": round(hit_rate, 1),
        "invested": int(invested),
        "returned": int(returned),
        "pnl": int(returned - invested),
        "roi": round(roi, 1),
        "max_dd": round(max_dd, 0),
        "top1_ret": int(top1_ret),
        "top1_race": top1_race,
        "roi_ex_top1": round(roi_ex, 1),
        "top1_contribution_pct": round(top1_pct, 1),
    }


# ─── main ─────────────────────────────────────────────────────────────────


def main() -> None:
    conn = sqlite3.connect(str(MAIN_DB))
    res_conn = sqlite3.connect(str(RESEARCH_DB))

    # 有効フォールド（race_payoutsが存在する2024・2025のみ）
    folds = [
        (["2022", "2023"], "2024"),
        (["2022", "2023", "2024"], "2025"),
    ]

    all_records: list[dict] = []

    for train_years, test_year in folds:
        label = "+".join(train_years)
        logger.info("=" * 60)
        logger.info("フォールド: 学習=%s → テスト=%s", label, test_year)

        train_df = build_df(conn, res_conn, train_years)
        test_df = build_df(conn, res_conn, [test_year])

        if train_df.empty or test_df.empty:
            logger.warning("データ不足: スキップ")
            continue

        train_clean = train_df.dropna(subset=SANDBOX_FEATURE_COLS)
        test_clean = test_df.dropna(subset=SANDBOX_FEATURE_COLS)

        # EV target が全0の場合は複勝払戻 × is_placed に fallback
        if train_clean["ev_target"].sum() == 0:
            logger.warning(
                "  %s: 単勝払戻なし → is_winner のみでEVを代替",
                label,
            )
            train_clean = train_clean.copy()
            train_clean["ev_target"] = train_clean["is_winner"].astype(float)

        logger.info(
            "  学習: %d行 / %d レース",
            len(train_clean),
            train_clean["race_id"].nunique(),
        )
        logger.info(
            "  テスト: %d行 / %d レース",
            len(test_clean),
            test_clean["race_id"].nunique(),
        )

        X_tr = train_clean[SANDBOX_FEATURE_COLS]
        mdl_ev = LGBMRegressor(**_LGB_BASE)
        mdl_ev.fit(X_tr, train_clean["ev_target"])
        logger.info("  学習完了")

        X_te = test_clean[SANDBOX_FEATURE_COLS]
        test_clean = test_clean.copy()
        test_clean["ev_score"] = mdl_ev.predict(X_te)

        # テスト年の払戻データ
        payout_rows = conn.execute(
            "SELECT race_id, bet_type, combination, payout FROM race_payouts WHERE race_id LIKE ?",
            (f"{test_year}%",),
        ).fetchall()
        payout_global = pd.DataFrame(
            payout_rows, columns=["race_id", "bet_type", "combination", "payout"]
        )

        race_ids = test_clean["race_id"].unique()
        logger.info("  バックテスト: %d レース", len(race_ids))

        for i, race_id in enumerate(race_ids):
            race_pred = test_clean[test_clean["race_id"] == race_id]
            race_payout = payout_global[payout_global["race_id"] == race_id]
            if race_pred.empty or race_payout.empty:
                continue
            for bt, (inv, ret) in simulate_one_race(race_pred, race_payout).items():
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
                logger.info("    %d / %d", i + 1, len(race_ids))

        logger.info("  フォールド%s完了", test_year)

    conn.close()
    res_conn.close()

    if not all_records:
        logger.error("結果なし")
        return

    # ─── レポート ────────────────────────────────────────────────────────────

    tested_years = ["2024", "2025"]

    print("\n" + "=" * 80)
    print("  サンドボックス 年別ウォークフォワード分析")
    print("  ラベル: horse_odds.rank (netkeiba R&D) | 払戻: umalogi.db")
    print("=" * 80)

    all_stats: dict[tuple[str, str], dict] = {}
    for y in tested_years:
        for bt in TARGET_BET_TYPES:
            s = year_stats(all_records, y, bt)
            all_stats[(y, bt)] = s

    # ── 1. 年別ROIマトリクス ─────────────────────────────────────────────────
    print("\n■ 【1】年別 ROI（学習→テスト: 2022+2023→2024, +2024→2025）\n")
    hdr = f"{'年度':<6} {'券種':<6} {'レース':>7} {'的中率':>8} {'投資':>9} {'払戻':>12} {'損益':>10} {'ROI':>8} {'MaxDD':>10}"
    print(hdr)
    print("-" * 80)
    for y in tested_years:
        for bt in TARGET_BET_TYPES:
            s = all_stats.get((y, bt), {})
            if not s:
                print(f"{y:<6} {bt:<6}  データなし")
                continue
            flag = "✅" if s["roi"] >= 100 else "❌"
            print(
                f"{y:<6} {bt:<6} {s['n_races']:>7} "
                f"{s['hit_rate']:>7.1f}% "
                f"{s['invested']:>9,} "
                f"{s['returned']:>12,} "
                f"{s['pnl']:>+10,} "
                f"{s['roi']:>7.1f}% "
                f"{s['max_dd']:>+10,.0f} {flag}"
            )
        print()

    # ── 2. 外れ値分析 ─────────────────────────────────────────────────────────
    print("\n■ 【2】外れ値（年間TOP1的中払戻）除外時のROI\n")
    print(
        f"{'年度':<6} {'券種':<6} {'TOP1払戻':>12} {'TOP1寄与':>10} {'除外ROI':>10} {'通常ROI':>10}"
    )
    print("-" * 60)
    for y in tested_years:
        for bt in TARGET_BET_TYPES:
            s = all_stats.get((y, bt), {})
            if not s:
                continue
            danger = " ⚠️" if s["top1_contribution_pct"] >= 50 else ""
            print(
                f"{y:<6} {bt:<6} "
                f"{s['top1_ret']:>12,} "
                f"{s['top1_contribution_pct']:>9.1f}% "
                f"{s['roi_ex_top1']:>9.1f}% "
                f"{s['roi']:>9.1f}%{danger}"
            )
        print()

    # ── 3. 客観評価 ────────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("■ 【3】AIデータサイエンティストによる客観的堅牢性評価（忖度なし）")
    print("=" * 80)

    for bt in TARGET_BET_TYPES:
        stats_list = [all_stats.get((y, bt), {}) for y in tested_years]
        rois = [s.get("roi", None) for s in stats_list if s]
        ex_rois = [s.get("roi_ex_top1", None) for s in stats_list if s]
        contribs = [s.get("top1_contribution_pct", 0) for s in stats_list if s]

        print(f"\n── {bt} ──")
        for y in tested_years:
            s = all_stats.get((y, bt), {})
            if s:
                flag = "✅" if s["roi"] >= 100 else "❌"
                print(
                    f"  {y}: ROI={s['roi']:8.1f}%  除外ROI={s['roi_ex_top1']:8.1f}%  TOP1寄与={s['top1_contribution_pct']:5.1f}%  {flag}"
                )

        valid_rois = [r for r in rois if r is not None]
        if not valid_rois:
            continue

        avg_roi = np.mean(valid_rois)
        std_roi = np.std(valid_rois)
        avg_ex = np.mean([r for r in ex_rois if r is not None]) if ex_rois else 0
        profit_years = sum(1 for r in valid_rois if r >= 100)
        max_contrib = max(contribs) if contribs else 0

        print(
            f"\n  集計: 平均ROI={avg_roi:.1f}%  標準偏差={std_roi:.1f}%  黒字={profit_years}/{len(valid_rois)}年"
        )

        issues: list[str] = []
        if profit_years < len(valid_rois):
            issues.append(f"赤字年あり ({len(valid_rois) - profit_years}年)")
        if avg_ex < 100:
            issues.append(f"外れ値除外後の平均ROI {avg_ex:.1f}%＜100%（特定大穴依存）")
        if std_roi > 200:
            issues.append(f"年間ROI標準偏差 {std_roi:.1f}%（高ボラ）")
        if max_contrib >= 50:
            issues.append(f"最大TOP1寄与 {max_contrib:.1f}%（フロック警戒）")

        if not issues and avg_ex >= 100:
            verdict = "✅ 堅牢: 全年黒字 + 外れ値除外後も100%超。商用化水準。"
        elif not issues:
            verdict = "🔶 条件付: 全年黒字だが外れ値除外後は100%未満。安定性に留意。"
        elif profit_years == len(valid_rois):
            verdict = "🔶 条件付: " + " / ".join(issues)
        else:
            verdict = "❌ 要注意: " + " / ".join(issues)

        print(f"  判定: {verdict}")

    print("""
================================================================================
■ 総合所見（忖度なし）
================================================================================

【有効なデータ点数】
  走-フォワード2フォールド (2024・2025) のみ。
  サンプルが2年分では「再現性あり」と断言するには不足。
  3年以上の検証が商用判断の最低ライン。

【三連単・馬単について】
  これらは本質的に高ボラ券種。1回の的中で年間ROIが大きく動く。
  外れ値除外後ROIが100%を超えるかどうかが実用性の判断基準。

【horse_odds.rank ラベルについて】
  2022-2023の学習ラベルはnetkeiba_research.dbのhorse_odds.rankを使用。
  umalogi.db(JVLink)のrace_resultsとは別ソース。ラベル品質は良好だが
  両ソースの着順整合性は未検証（影響は軽微と推定）。

【推奨アクション】
  ① 本番運用前に2026年分も蓄積後、3フォールド以上で再検証。
  ② 三連単は的中率1-3%のためドローダウン期間が長い。
     1レースあたりの実投資は最大損失を考慮したサイジングを。
  ③ 今週末の note 予想には「参考指標」として活用し、
     実損益トラッキングで実データを積み上げることを推奨。
""")


if __name__ == "__main__":
    main()
