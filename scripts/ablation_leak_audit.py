"""ハイブリッドアンサンブル AUC 0.944 のリーク監査（特徴量 Ablation Study）。

手順:
  1. evaluate_hybrid_ensemble と同一経路で train/test フレームを構築し pickle キャッシュ
     （2回目以降は再構築せず高速にグループ別 ablation を回せる）。
  2. 単一特徴量 AUC スキャン: 各数値列単独の test AUC を算出し「単独で異常に強い列」
     ＝リーク源を直接特定する。
  3. グループ別 ablation: 特徴量グループを除外して AccuracyModelV2 を再学習し、
     test AUC / Top2 単勝フラット ROI の変化からリーク寄与を定量化する。

Usage:
    py scripts/ablation_leak_audit.py [--rebuild] [--train-cap 300] [--test-cap 150]
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"
_CACHE = _ROOT / "data" / "ablation_cache.pkl"

# ── 特徴量グループ定義（仮説別）─────────────────────────────────────────────
GROUPS: dict[str, list[str]] = {
    # 最終確定オッズ由来（タイミングリーク仮説）: FEATURE_COLS 外で AccV2 だけが食う
    "odds_ev_extra": [
        "shin_prob",
        "implied_prob_excess",
        "harville_place_prob",
        "odds_reversal_score",
        "odds_steam_flag",
        "field_strength_ev_adj",
        "ev_rank_in_race",
    ],
    # FEATURE_COLS 内のオッズ動態（honmei も使用）
    "odds_dynamics_core": ["odds_vs_morning", "odds_velocity"],
    # 前走詳細（W-070 実装・リークフリー検証済のはず）
    "prerun": [
        "prev_last_3f",
        "prev_rank",
        "prev_margin",
        "days_since_prev",
        "avg_last_3f_3",
        "same_course_runs",
        "same_course_place_rate",
    ],
    # 血統 TE
    "pedigree_te": ["sire_te", "dam_sire_te", "sire_place_te", "bms_te"],
    # レース内相対化の追加列
    "zscore_extra": [
        "win_rate_surface_zscore",
        "win_rate_distance_band_zscore",
        "hc_4f_rank",
        "hc_4f_zscore",
    ],
}


def _race_ids(conn: sqlite3.Connection, lo: str, hi: str, cap: int) -> list[str]:
    rows = conn.execute(
        """SELECT DISTINCT r.race_id FROM races r
           WHERE r.date >= ? AND r.date < ?
             AND EXISTS (SELECT 1 FROM race_results rr
                         WHERE rr.race_id=r.race_id AND rr.rank>0 AND rr.win_odds>0)
           ORDER BY r.date""",
        (lo, hi),
    ).fetchall()
    ids = [r[0] for r in rows]
    if len(ids) > cap:
        step = len(ids) / cap
        ids = [ids[int(i * step)] for i in range(cap)]
    return ids


def _build_frame(conn: sqlite3.Connection, ids: list[str], cutoff: str):
    """evaluate_hybrid_ensemble と同一の特徴量フレーム（is_win/win_odds 付き）。"""
    import pandas as pd

    from src.features.pedigree_te import SireEncoder, build_pedigree_features
    from src.features.prerun import build_prerun_features
    from src.ml.features import FeatureBuilder

    enc = SireEncoder().fit(conn, cutoff_date=cutoff)
    fb = FeatureBuilder(conn)
    frames: list[pd.DataFrame] = []
    for rid in ids:
        try:
            base = fb.build_race_features(rid)
        except Exception:
            continue
        if base.empty:
            continue
        labels = conn.execute(
            "SELECT horse_number, rank, win_odds FROM race_results "
            "WHERE race_id=? AND rank>0 AND win_odds>0",
            (rid,),
        ).fetchall()
        if not labels:
            continue
        lmap = {hn: (1 if rk == 1 else 0, wo) for hn, rk, wo in labels}
        base["is_win"] = base["horse_number"].map(
            lambda x: lmap.get(x, (None, None))[0]
        )
        base["win_odds"] = base["horse_number"].map(
            lambda x: lmap.get(x, (None, None))[1]
        )
        base = base.dropna(subset=["is_win", "win_odds"])
        if base.empty:
            continue
        pre = build_prerun_features(conn, rid)
        if not pre.empty:
            base = base.merge(pre, on="horse_number", how="left")
        ped = build_pedigree_features(conn, rid, enc)
        if not ped.empty:
            base = base.merge(ped, on="horse_number", how="left")
        base["race_id"] = rid
        frames.append(base)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _eval_roi(bets) -> tuple[int, float, float]:
    if len(bets) == 0:
        return 0, 0.0, 0.0
    payout = float((bets["is_win"] * bets["win_odds"] * 100).sum())
    return (
        len(bets),
        float(bets["is_win"].mean() * 100),
        payout / (len(bets) * 100) * 100,
    )


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    import warnings

    warnings.filterwarnings("ignore")

    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true")
    ap.add_argument("--cutoff", default="2025-10-01")
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-hi", default="2026-06-01")
    ap.add_argument("--train-cap", type=int, default=300)
    ap.add_argument("--test-cap", type=int, default=150)
    args = ap.parse_args()

    import numpy as np
    import pandas as pd
    from sklearn.metrics import roc_auc_score

    from src.ml.accuracy_model_v2 import AccuracyModelV2

    if _CACHE.exists() and not args.rebuild:
        with open(_CACHE, "rb") as f:
            train_df, test_df = pickle.load(f)
        print(f"[cache] {len(train_df)}行(train) / {len(test_df)}行(test) をロード")
    else:
        conn = sqlite3.connect(str(_DB_PATH))
        try:
            print("[build] train フレーム構築中...")
            train_ids = _race_ids(conn, args.train_lo, args.cutoff, args.train_cap)
            train_df = _build_frame(conn, train_ids, args.cutoff)
            print(f"  {len(train_df)}行 / {len(train_ids)}レース")
            print("[build] test フレーム構築中...")
            test_ids = _race_ids(conn, args.cutoff, args.test_hi, args.test_cap)
            test_df = _build_frame(conn, test_ids, args.cutoff)
            print(f"  {len(test_df)}行 / {len(test_ids)}レース")
        finally:
            conn.close()
        with open(_CACHE, "wb") as f:
            pickle.dump((train_df, test_df), f)
        print(f"[cache] 保存: {_CACHE}")

    y_tr = train_df["is_win"].astype(int).to_numpy()
    y_te = test_df["is_win"].astype(int).to_numpy()

    # ── ① 単一特徴量 AUC スキャン（リーク源の直接特定）─────────────────────
    print("\n" + "=" * 76)
    print(" ① 単一特徴量 AUC スキャン（test・単独AUC降順 上位20）")
    print("=" * 76)
    skip = {"race_id", "horse_number", "is_win", "rank", "win_odds"}
    numeric_cols = [
        c
        for c in test_df.columns
        if c not in skip and test_df[c].dtype.kind in ("i", "f", "b", "u")
    ]
    singles: list[tuple[str, float]] = []
    for c in numeric_cols:
        v = test_df[c].fillna(0.0).to_numpy()
        if np.unique(v).size < 2:
            continue
        try:
            auc = roc_auc_score(y_te, v)
        except ValueError:
            continue
        singles.append((c, max(auc, 1 - auc)))  # 逆相関も検知
    singles.sort(key=lambda t: -t[1])
    for name, auc in singles[:20]:
        flag = " 🚨" if auc >= 0.90 else (" ⚠️" if auc >= 0.85 else "")
        print(f"  {name:<36} AUC={auc:.4f}{flag}")

    # ── ② グループ別 ablation ───────────────────────────────────────────────
    print("\n" + "=" * 76)
    print(" ② グループ別 Ablation（除外して再学習 → test AUC / Top2 ROI）")
    print("=" * 76)

    def run(drop_cols: list[str], label: str) -> None:
        tr = train_df.drop(columns=[c for c in drop_cols if c in train_df.columns])
        te = test_df.drop(columns=[c for c in drop_cols if c in test_df.columns])
        m = AccuracyModelV2()
        m.fit(tr, y_tr)
        probs = m.predict_proba(te)
        auc = roc_auc_score(y_te, probs)
        work = te[["race_id", "is_win", "win_odds"]].copy()
        work["prob"] = probs
        top2 = (
            work.sort_values("prob", ascending=False)
            .groupby("race_id", group_keys=False)
            .head(2)
        )
        n, hit, roi = _eval_roi(top2)
        print(
            f"  {label:<44} AUC={auc:.4f}  Top2: n={n} 的中{hit:5.1f}% ROI{roi:7.1f}%"
        )

    run([], "G0: 全特徴量（ベースライン）")
    for gname, cols in GROUPS.items():
        run(cols, f"G-: {gname} を除外")
    # 累積除外（疑惑グループを全部抜いた保守ケース）
    all_suspect = GROUPS["odds_ev_extra"] + GROUPS["odds_dynamics_core"]
    run(all_suspect, "G*: オッズ由来を全除外")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
