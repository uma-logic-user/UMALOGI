# -*- coding: utf-8 -*-
"""
カンニングゼロ バックテスト 2025 年版

設計:
  Train  : 2024-01-01 ～ 2024-12-31 (2024年全レース)
  Test   : 2025-01-01 ～ 2025-12-31 (2025年全レース、実結果+払戻が揃うレース)

処理フロー:
  1. Train セットで LightGBM + Isotonic を新規訓練（既存pkl を上書きしない）
  2. Test セット全レースをモデルで予測 → Harville 確率で全券種 EV 計算
  3. 実際の race_payouts と照合して ROI / 的中率 を券種・EV閾値・月別に集計
  4. 最終勧告レポートを出力
"""

from __future__ import annotations

import io
import itertools
import json
import logging
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(level=logging.WARNING)

# ───────────────────────────────────────────────────────────────────
TRAIN_FROM = "2024-01-01"
TRAIN_TO   = "2024-12-31"
TEST_FROM  = "2025-01-01"
TEST_TO    = "2025-12-31"

BASE_BET   = 100   # 1点あたり購入単位（円）

# JRA 控除率（払戻倍率スケール用）
_TRACK_TAKE: dict[str, float] = {
    "単勝": 0.200, "複勝": 0.200,
    "馬連": 0.225, "ワイド": 0.225, "馬単": 0.250,
    "三連複": 0.250, "三連単": 0.275,
}
# ───────────────────────────────────────────────────────────────────


def _get_conn() -> sqlite3.Connection:
    db_path = _ROOT / "data" / "umalogi.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_race_ids(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[str]:
    """結果＋払戻が揃っているレース ID を返す。"""
    rows = conn.execute(
        """
        SELECT DISTINCT r.race_id
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        JOIN race_payouts  rp ON rp.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
        ORDER BY r.race_id
        """,
        (date_from, date_to),
    ).fetchall()
    return [row[0] for row in rows]


# ───────────────────────────────────────────────────────────────────
# 特徴量（v_race_mart ベース — FeatureBuilder を呼ばず直接 SQL）
# ───────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "weight_carried", "horse_weight", "horse_weight_diff",
    "win_odds", "gate_number", "distance",
    "surface_code", "sex_code", "venue_code", "condition_code",
    "win_rate_all", "win_rate_surface", "win_rate_distance_band",
    "recent_rank_mean", "sire_code",
]


def _compute_horse_stats(
    conn: sqlite3.Connection,
    horse_id: str,
    race_id: str,
    surface: str,
    distance: int,
) -> dict[str, float]:
    """指定レース以前の馬成績統計（リーク排除）。"""
    _BANDS = [(0, 1400), (1400, 1800), (1800, 2200), (2200, 9999)]
    band = next((i for i, (lo, hi) in enumerate(_BANDS) if lo <= distance < hi), 3)

    rows = conn.execute(
        """
        SELECT rr.rank, r.surface, r.distance
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.horse_id = ?
          AND r.race_id < ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY r.race_id DESC
        """,
        (horse_id, race_id),
    ).fetchall()

    if not rows:
        return {"win_rate_all": 0.0, "win_rate_surface": 0.0,
                "win_rate_distance_band": 0.0, "recent_rank_mean": 10.0}

    ranks_all  = [r[0] for r in rows]
    win_all    = sum(1 for r in ranks_all if r == 1) / len(ranks_all)
    surf_rows  = [r for r in rows if r[1] == surface]
    win_surf   = (sum(1 for r in surf_rows if r[0] == 1) / len(surf_rows)) if surf_rows else 0.0
    dist_rows  = [r for r in rows if _BANDS[band][0] <= r[2] < _BANDS[band][1]]
    win_dist   = (sum(1 for r in dist_rows if r[0] == 1) / len(dist_rows)) if dist_rows else 0.0
    recent5    = ranks_all[:5]
    rank_mean  = float(np.mean(recent5))

    return {
        "win_rate_all": win_all,
        "win_rate_surface": win_surf,
        "win_rate_distance_band": win_dist,
        "recent_rank_mean": rank_mean,
    }


_SIRE_MAP: dict[str, int] = {}
_SURFACE_CODE = {"芝": 0, "ダート": 1, "障害": 2}
_SEX_CODE = {"牡": 0, "牝": 1, "セ": 2}
_VENUE_CODE = {
    "札幌": 0, "函館": 1, "福島": 2, "新潟": 3,
    "東京": 4, "中山": 5, "中京": 6, "京都": 7,
    "阪神": 8, "小倉": 9,
}
_COND_CODE = {"良": 0, "稍重": 1, "重": 2, "不良": 3}


def _encode_sire(sire: str | None) -> int:
    if not sire:
        return -1
    if sire not in _SIRE_MAP:
        _SIRE_MAP[sire] = len(_SIRE_MAP)
    return _SIRE_MAP[sire]


def build_race_df(
    conn: sqlite3.Connection,
    race_id: str,
    include_rank: bool = False,
) -> pd.DataFrame | None:
    """1レース分の特徴量 DataFrame を構築する。"""
    race_row = conn.execute(
        "SELECT distance, surface, venue, condition FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if not race_row:
        return None
    distance, surface, venue, condition = race_row

    rr_rows = conn.execute(
        """
        SELECT rr.horse_id, rr.horse_name, rr.rank,
               rr.weight_carried, rr.horse_weight, rr.horse_weight_diff,
               rr.win_odds, rr.gate_number, rr.horse_number,
               rr.sex_age,
               h.sire
        FROM race_results rr
        LEFT JOIN horses h ON h.horse_id = rr.horse_id
        WHERE rr.race_id = ? AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY rr.rank
        """,
        (race_id,),
    ).fetchall()

    if not rr_rows:
        return None

    records = []
    for (horse_id, horse_name, rank,
         weight_carried, horse_weight, hw_diff,
         win_odds, gate_number, horse_number, sex_age, sire) in rr_rows:

        stats = _compute_horse_stats(conn, horse_id or "", race_id, surface, distance)

        sex_str = sex_age[:1] if sex_age else ""
        records.append({
            "race_id":       race_id,
            "horse_id":      horse_id or "",
            "horse_name":    horse_name,
            "horse_number":  horse_number or 0,
            "rank":          rank,
            "weight_carried": weight_carried or 55.0,
            "horse_weight":  horse_weight or 480.0,
            "horse_weight_diff": hw_diff or 0.0,
            "win_odds":      win_odds or 10.0,
            "gate_number":   gate_number or 0,
            "distance":      distance or 1600,
            "surface_code":  _SURFACE_CODE.get(surface, -1),
            "sex_code":      _SEX_CODE.get(sex_str, -1),
            "venue_code":    _VENUE_CODE.get(venue, 10),
            "condition_code": _COND_CODE.get(condition or "", 0),
            "sire_code":     _encode_sire(sire),
            **stats,
        })

    return pd.DataFrame(records) if records else None


# ───────────────────────────────────────────────────────────────────
# モデル（スタンドアロン LightGBM + Isotonic）
# ───────────────────────────────────────────────────────────────────

def train_model(train_df: pd.DataFrame) -> tuple:
    """学習 DF から LightGBM + Isotonic モデルを訓練して返す。"""
    from lightgbm import LGBMClassifier

    feat_cols = [c for c in FEATURE_COLS if c in train_df.columns]
    X = train_df[feat_cols].astype(float).fillna(-1)
    y = (train_df["rank"] == 1).astype(int)
    groups = train_df["race_id"]

    n_races = groups.nunique()
    n_splits = min(5, n_races)

    LGBM_PARAMS = dict(
        n_estimators=400, learning_rate=0.05, max_depth=6,
        num_leaves=63, min_child_samples=20, subsample=0.8,
        colsample_bytree=0.8, class_weight="balanced",
        random_state=42, verbose=-1,
    )

    oof_preds = np.zeros(len(X), dtype=float)
    aucs: list[float] = []
    gkf = GroupKFold(n_splits=n_splits)

    print(f"  LightGBM 訓練: {n_races} レース / {len(X)} サンプル / {n_splits}-fold CV")

    for tr_idx, val_idx in gkf.split(X, y, groups=groups):
        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X.iloc[tr_idx], y.iloc[tr_idx])
        proba = clf.predict_proba(X.iloc[val_idx])[:, 1]
        oof_preds[val_idx] = proba
        try:
            aucs.append(roc_auc_score(y.iloc[val_idx], proba))
        except ValueError:
            pass

    cv_auc = float(np.mean(aucs)) if aucs else float("nan")
    print(f"  CV AUC: {cv_auc:.4f}")

    # Isotonic キャリブレーション
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oof_preds, y)

    # 全データで本訓練
    final_clf = LGBMClassifier(**LGBM_PARAMS)
    final_clf.fit(X, y)

    feat_cols_used = feat_cols
    return final_clf, iso, feat_cols_used, cv_auc


def predict_proba(model, iso, feat_cols: list[str], df: pd.DataFrame) -> np.ndarray:
    X = df[feat_cols].astype(float).fillna(-1)
    raw = model.predict_proba(X)[:, 1]
    return iso.predict(raw)


# ───────────────────────────────────────────────────────────────────
# 払戻データ取得
# ───────────────────────────────────────────────────────────────────

def get_payouts(conn: sqlite3.Connection, race_id: str) -> dict[str, dict]:
    """race_payouts → {bet_type: {combination_str: payout}}"""
    rows = conn.execute(
        "SELECT bet_type, combination, payout FROM race_payouts WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    result: dict[str, dict] = {}
    for bt, combo, payout in rows:
        result.setdefault(bt, {})[combo] = payout
    return result


# ───────────────────────────────────────────────────────────────────
# Harville 確率
# ───────────────────────────────────────────────────────────────────

def _harville_exacta(probs: list[float], i: int, j: int) -> float:
    pi, pj = probs[i], probs[j]
    return pi * pj / (1 - pi) if pi < 1 else 0.0


def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    return _harville_exacta(probs, i, j) + _harville_exacta(probs, j, i)


def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    pi, pj, pk = probs[i], probs[j], probs[k]
    denom_ij = (1 - pi)
    denom_ijk = (1 - pi - pj)
    if denom_ij <= 0 or denom_ijk <= 0:
        return 0.0
    return pi * pj / denom_ij * pk / denom_ijk


# ───────────────────────────────────────────────────────────────────
# 買い目シミュレーション（1レース）
# ───────────────────────────────────────────────────────────────────

BET_TYPES = ["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"]


def simulate_race_bets(
    race_df: pd.DataFrame,
    probs: np.ndarray,
    payouts: dict[str, dict],
    month: str,
) -> list[dict]:
    """
    1レース分の全買い目をシミュレートして行リストを返す。

    Returns:
        list of {bet_type, ev, is_hit, invested, payout_amount, profit, month}
    """
    records: list[dict] = []
    n = len(race_df)
    if n < 2:
        return records

    horse_nums = list(race_df["horse_number"].astype(int))
    ranks      = list(race_df["rank"].astype(int))
    prob_list  = list(probs)

    # 馬番→インデックス
    num2idx = {h: i for i, h in enumerate(horse_nums)}
    # 着順→馬番
    rank2num = {r: h for h, r in zip(horse_nums, ranks)}

    # EV 計算用のスケール（OddsEstimator の平均スケールを固定値で近似）
    _DEFAULT_SCALE = {
        "単勝": 1.0, "複勝": 0.33,
        "馬連": 6.0, "ワイド": 2.5,
        "馬単": 12.0, "三連複": 30.0, "三連単": 150.0,
    }

    # 軸馬（最高スコア）の単勝オッズ
    top_idx    = int(np.argmax(prob_list))
    top_odds   = float(race_df.iloc[top_idx]["win_odds"] or 10.0)

    def ev_est(prob: float, bet_type: str) -> float:
        scale = _DEFAULT_SCALE.get(bet_type, 1.0)
        return prob * top_odds * scale

    def make_record(bt: str, combo_str: str, prob: float,
                    invested: float = BASE_BET) -> dict:
        """払戻テーブルと照合して的中判定。"""
        pmap = payouts.get(bt, {})
        payout_amt = float(pmap.get(combo_str, 0))
        is_hit = payout_amt > 0
        profit = payout_amt - invested if is_hit else -invested
        ev = ev_est(prob, bt)
        return {
            "bet_type": bt,
            "ev": ev,
            "is_hit": int(is_hit),
            "invested": invested,
            "payout_amount": payout_amt,
            "profit": profit,
            "month": month,
        }

    # ── 単勝（1位予想馬） ──────────────────────────────────────
    top_num = horse_nums[top_idx]
    prob_top = prob_list[top_idx]
    records.append(make_record("単勝", str(top_num), prob_top))

    # ── 複勝（上位3頭） ────────────────────────────────────────
    top3_idx = sorted(range(n), key=lambda i: -prob_list[i])[:3]
    for idx in top3_idx:
        hnum = horse_nums[idx]
        pr   = prob_list[idx]
        records.append(make_record("複勝", str(hnum), pr * 0.33))

    # ── 馬連・ワイド・馬単（上位2頭） ─────────────────────────
    if n >= 2:
        top2_idx = sorted(range(n), key=lambda i: -prob_list[i])[:2]
        i0, i1 = top2_idx[0], top2_idx[1]
        n0, n1 = horse_nums[i0], horse_nums[i1]
        p0, p1 = prob_list[i0], prob_list[i1]
        q_prob  = _harville_quinella(prob_list, i0, i1)

        # 馬連 (小さい馬番 - 大きい馬番)
        low, high = (n0, n1) if n0 < n1 else (n1, n0)
        records.append(make_record("馬連", f"{low}-{high}", q_prob))

        # ワイド
        records.append(make_record("ワイド", f"{low}-{high}", q_prob))

        # 馬単（高確率が1着）
        records.append(make_record("馬単", f"{n0}-{n1}",
                                   _harville_exacta(prob_list, i0, i1)))

    # ── 三連複・三連単（上位3頭） ──────────────────────────────
    if n >= 3:
        top3_idx_s = sorted(range(n), key=lambda i: -prob_list[i])[:3]
        i0, i1, i2 = top3_idx_s
        n0, n1, n2 = horse_nums[i0], horse_nums[i1], horse_nums[i2]

        # 三連複（昇順）
        trio_sorted = sorted([n0, n1, n2])
        trio_str    = f"{trio_sorted[0]}-{trio_sorted[1]}-{trio_sorted[2]}"
        trio_prob   = sum(
            _harville_trio(prob_list, a, b, c)
            for a, b, c in itertools.permutations([i0, i1, i2])
        )
        records.append(make_record("三連複", trio_str, trio_prob))

        # 三連単（最高確率順）
        tan3_str = f"{n0}-{n1}-{n2}"
        records.append(make_record("三連単", tan3_str,
                                   _harville_trio(prob_list, i0, i1, i2)))

    return records


# ───────────────────────────────────────────────────────────────────
# レポート出力
# ───────────────────────────────────────────────────────────────────

def print_report(df: pd.DataFrame, cv_auc: float) -> None:
    sep = "=" * 72

    print(sep)
    print("【バックテスト 2025 カンニングゼロ】")
    print(f"  Train : {TRAIN_FROM} ～ {TRAIN_TO}")
    print(f"  Test  : {TEST_FROM}  ～ {TEST_TO}")
    print(f"  CV AUC: {cv_auc:.4f}")
    print(sep)

    # ── 全体 ──
    total_inv = df["invested"].sum()
    total_pay = df["payout_amount"].sum()
    total_hit = df["is_hit"].sum()
    roi_all   = total_pay / total_inv * 100 if total_inv else 0
    print(f"\n■ 全体サマリー")
    print(f"  件数={len(df):,}  的中={total_hit:,}({total_hit/len(df)*100:.1f}%)"
          f"  投資¥{total_inv:,.0f}  払戻¥{total_pay:,.0f}  ROI={roi_all:.1f}%"
          f"  損益¥{total_pay-total_inv:,.0f}")

    # ── 券種別 ──
    print(f"\n■ 券種別 ROI")
    print(f"{'券種':<8}{'件数':>5}{'的中':>5}{'的中率':>7}{'投資':>9}{'払戻':>9}{'ROI':>7}{'損益':>10}")
    print("-" * 60)
    bet_types_order = ["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"]
    for bt in bet_types_order:
        sub = df[df["bet_type"] == bt]
        if sub.empty:
            continue
        inv = sub["invested"].sum()
        pay = sub["payout_amount"].sum()
        hit = sub["is_hit"].sum()
        roi = pay / inv * 100 if inv else 0
        mark = " ★" if roi >= 100 else ("  ▲" if roi >= 70 else "")
        print(f"{bt:<8}{len(sub):>5,}{hit:>5,}{hit/len(sub)*100:>6.1f}%"
              f"{inv:>9,.0f}{pay:>9,.0f}{roi:>6.1f}%{pay-inv:>10,.0f}{mark}")

    # ── EV 閾値別（全体） ──
    print(f"\n■ EV 閾値別 ROI（全券種）")
    print(f"{'EV≥':>6}{'件数':>6}{'的中':>5}{'投資':>10}{'払戻':>10}{'ROI':>7}{'損益':>10}")
    print("-" * 58)
    for thr in [0.0, 0.5, 0.8, 1.0, 1.1, 1.2, 1.5, 2.0, 3.0]:
        sub = df[df["ev"] >= thr]
        if sub.empty:
            continue
        inv = sub["invested"].sum()
        pay = sub["payout_amount"].sum()
        roi = pay / inv * 100 if inv else 0
        hit = sub["is_hit"].sum()
        mark = " ★" if roi >= 100 else ("  ▲" if roi >= 70 else "")
        print(f"{thr:>6.1f}{len(sub):>6,}{hit:>5,}{inv:>10,.0f}{pay:>10,.0f}{roi:>6.1f}%{pay-inv:>10,.0f}{mark}")

    # ── 券種 × EV 閾値 クロス ──
    print(f"\n■ 券種 × EV 閾値クロス ROI")
    thrs = [0.0, 1.0, 1.2, 1.5, 2.0]
    print(f"{'券種':<8}" + "".join(f"  EV≥{t:.1f}      " for t in thrs))
    print("-" * 72)
    for bt in bet_types_order:
        grp = df[df["bet_type"] == bt]
        if grp.empty:
            continue
        row_str = f"{bt:<8}"
        for thr in thrs:
            sub = grp[grp["ev"] >= thr]
            if sub.empty:
                row_str += f"{'---':>14}"
                continue
            inv = sub["invested"].sum()
            pay = sub["payout_amount"].sum()
            roi = pay / inv * 100 if inv else 0
            mark = "★" if roi >= 100 else ("▲" if roi >= 70 else " ")
            row_str += f"{roi:>6.1f}%{mark}({len(sub):>3})"
        print(row_str)

    # ── 月別資産推移 ──
    print(f"\n■ 月別 ROI 推移（全券種）")
    print(f"{'月':>8}{'件数':>6}{'的中':>5}{'投資':>9}{'払戻':>9}{'ROI':>7}{'損益':>10}")
    print("-" * 58)
    cumulative = 0.0
    for month, grp in df.groupby("month"):
        inv = grp["invested"].sum()
        pay = grp["payout_amount"].sum()
        hit = grp["is_hit"].sum()
        roi = pay / inv * 100 if inv else 0
        cumulative += pay - inv
        print(f"{month:>8}{len(grp):>6,}{hit:>5,}{inv:>9,.0f}{pay:>9,.0f}{roi:>6.1f}%"
              f"{pay-inv:>10,.0f}  累計損益¥{cumulative:,.0f}")

    # ── 月別・券種別 ──
    print(f"\n■ 月別・券種別 ROI")
    pivot_data: dict[str, dict[str, str]] = {}
    months_list = sorted(df["month"].unique())
    for bt in bet_types_order:
        pivot_data[bt] = {}
        grp_bt = df[df["bet_type"] == bt]
        for month in months_list:
            sub = grp_bt[grp_bt["month"] == month]
            if sub.empty:
                pivot_data[bt][month] = "---"
                continue
            inv = sub["invested"].sum()
            pay = sub["payout_amount"].sum()
            roi = pay / inv * 100 if inv else 0
            mark = "★" if roi >= 100 else ("▲" if roi >= 70 else " ")
            pivot_data[bt][month] = f"{roi:5.1f}%{mark}"

    hdr = f"{'券種':<8}" + "".join(f"  {m}  " for m in months_list)
    print(hdr)
    print("-" * (8 + 10 * len(months_list)))
    for bt in bet_types_order:
        row = f"{bt:<8}" + "".join(f" {pivot_data[bt].get(m,'---'):>9}" for m in months_list)
        print(row)

    # ── EV 分布（的中馬券と非的中馬券の EV 比較） ──
    print(f"\n■ EV 分布（的中 vs 非的中）")
    print(f"{'券種':<8}{'非的中EV平均':>13}{'的中EV平均':>12}{'全EV中央':>10}{'EV最大':>10}")
    print("-" * 55)
    for bt in bet_types_order:
        grp = df[df["bet_type"] == bt]
        if grp.empty:
            continue
        miss = grp[grp["is_hit"] == 0]["ev"]
        hits = grp[grp["is_hit"] == 1]["ev"]
        print(f"{bt:<8}{miss.mean():>13.3f}{hits.mean() if len(hits) else float('nan'):>12.3f}"
              f"{grp['ev'].median():>10.3f}{grp['ev'].max():>10.3f}")

    # ── 最適戦略シミュレーション ──
    print(f"\n■ 最適戦略シミュレーション")
    print(f"{'戦略':<32}{'件数':>5}{'的中':>5}{'投資':>9}{'払戻':>9}{'ROI':>7}{'損益':>10}")
    print("-" * 78)

    strategies = [
        ("現状（全件）",             df,                                            ""),
        ("単勝+複勝 全件",           df[df["bet_type"].isin(["単勝","複勝"])],      ""),
        ("複勝 全件",                df[df["bet_type"]=="複勝"],                    ""),
        ("単勝 EV≥1.0",              df[(df["bet_type"]=="単勝")&(df["ev"]>=1.0)],  ""),
        ("単勝 EV≥1.5",              df[(df["bet_type"]=="単勝")&(df["ev"]>=1.5)],  ""),
        ("複勝 EV≥1.0",              df[(df["bet_type"]=="複勝")&(df["ev"]>=1.0)],  ""),
        ("単勝+複勝 EV≥1.0",         df[df["bet_type"].isin(["単勝","複勝"])&(df["ev"]>=1.0)], ""),
        ("単勝+複勝+ワイド EV≥1.0",  df[df["bet_type"].isin(["単勝","複勝","ワイド"])&(df["ev"]>=1.0)], ""),
        ("馬連+三連複 EV≥1.0",       df[df["bet_type"].isin(["馬連","三連複"])&(df["ev"]>=1.0)], ""),
    ]
    for name, sub, _ in strategies:
        if sub.empty:
            print(f"{name:<32}{'---':>5}")
            continue
        inv = sub["invested"].sum()
        pay = sub["payout_amount"].sum()
        roi = pay / inv * 100 if inv else 0
        hit = sub["is_hit"].sum()
        mark = " ★" if roi >= 100 else ("  ▲" if roi >= 70 else "")
        print(f"{name:<32}{len(sub):>5,}{hit:>5,}{inv:>9,.0f}{pay:>9,.0f}{roi:>6.1f}%{pay-inv:>10,.0f}{mark}")

    print(f"\n★ = ROI 100%超え  ▲ = ROI 70%超え")

    # ── 月別 JSON データ（グラフ用） ──
    print(f"\n■ 月別資産推移 JSON（グラフ用）")
    monthly_json = []
    cum = 0.0
    for month, grp in df.groupby("month"):
        inv = grp["invested"].sum()
        pay = grp["payout_amount"].sum()
        roi = pay / inv * 100 if inv else 0
        cum += pay - inv
        monthly_json.append({
            "month": month,
            "roi": round(roi, 1),
            "profit": round(pay - inv, 0),
            "cumulative_profit": round(cum, 0),
            "invested": round(inv, 0),
            "payout": round(pay, 0),
            "hits": int(grp["is_hit"].sum()),
            "total": len(grp),
        })

    def _to_py(v):
        if isinstance(v, (np.integer,)): return int(v)
        if isinstance(v, (np.floating,)): return float(v)
        return v

    safe_json = [{k: _to_py(v) for k, v in row.items()} for row in monthly_json]
    print(json.dumps(safe_json, ensure_ascii=False, indent=2))


# ───────────────────────────────────────────────────────────────────
# メイン
# ───────────────────────────────────────────────────────────────────

def main() -> None:
    conn = _get_conn()

    print("=" * 72)
    print("STEP 1: 訓練データ構築")
    print(f"  期間: {TRAIN_FROM} ～ {TRAIN_TO}")

    train_race_ids = _get_race_ids(conn, TRAIN_FROM, TRAIN_TO)
    print(f"  対象レース: {len(train_race_ids):,}")

    train_frames: list[pd.DataFrame] = []
    for i, race_id in enumerate(train_race_ids):
        df = build_race_df(conn, race_id, include_rank=True)
        if df is not None and not df.empty:
            train_frames.append(df)
        if (i + 1) % 100 == 0:
            print(f"  ... {i+1}/{len(train_race_ids)} レース処理中", end="\r")

    if not train_frames:
        print("ERROR: 訓練データが空です")
        conn.close()
        return

    train_df = pd.concat(train_frames, ignore_index=True)
    print(f"\n  訓練データ: {len(train_df):,} サンプル  "
          f"({train_df['race_id'].nunique():,} レース)")

    print("\nSTEP 2: モデル訓練")
    model, iso, feat_cols, cv_auc = train_model(train_df)

    print("\nSTEP 3: テストデータで予測・買い目シミュレーション")
    print(f"  期間: {TEST_FROM} ～ {TEST_TO}")

    test_race_ids = _get_race_ids(conn, TEST_FROM, TEST_TO)
    print(f"  対象レース: {len(test_race_ids):,}")

    if not test_race_ids:
        print("ERROR: テスト対象レースが0件です（race_results + race_payouts が揃っているレースなし）")
        print("  → JV-Link RACE データ（SEレコード + HRレコード）のインポートを確認してください")
        conn.close()
        return

    all_records: list[dict] = []
    ok_races = 0

    for i, race_id in enumerate(test_race_ids):
        race_df = build_race_df(conn, race_id)
        if race_df is None or race_df.empty:
            continue

        probs = predict_proba(model, iso, feat_cols, race_df)
        payouts = get_payouts(conn, race_id)
        if not payouts:
            continue

        # 月を取得（races テーブルの date カラムから）
        date_row = conn.execute("SELECT date FROM races WHERE race_id=?", (race_id,)).fetchone()
        month_str = (date_row[0][:7] if date_row else "unknown")

        records = simulate_race_bets(race_df, probs, payouts, month_str)
        all_records.extend(records)
        ok_races += 1

        if (i + 1) % 50 == 0:
            print(f"  ... {i+1}/{len(test_race_ids)} レース処理中", end="\r")

    print(f"\n  シミュレーション完了: {ok_races:,} レース / {len(all_records):,} 買い目")

    if not all_records:
        print("ERROR: テスト結果が空です")
        conn.close()
        return

    conn.close()

    result_df = pd.DataFrame(all_records)
    print_report(result_df, cv_auc)


if __name__ == "__main__":
    main()
