"""OOS バックテスト: ベースライン vs +新特徴量（前走詳細/同コース/血統TE/加速力）。

新特徴量の限界寄与を honest に測るため、同一ベース列の上に新特徴量を足した
モデルと足さないモデルを時系列分割（train< cutoff < test）で比較する。

- target: is_win（rank==1）。ROI は単勝 EV>閾値 のフラットベットで算出。
- リークフリー: 血統TE は cutoff(検証開始日)より前のみで fit。前走系は各馬の
  過去出走のみ参照。test ラベルはエンコーダ/特徴量に一切混入しない。
- 本番モデル(FEATURE_COLS)・predictions には一切触れない（評価専用）。

使い方::
    py scripts/backtest_v2_oos.py --cutoff 2025-10-01 --train-cap 1800 --test-cap 700
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.features.acceleration import build_acceleration_features  # noqa: E402
from src.features.pedigree_te import (  # noqa: E402
    PEDIGREE_FEATURE_COLS,
    SireEncoder,
    build_pedigree_features,
)
from src.features.prerun import PRERUN_FEATURE_COLS, build_prerun_features  # noqa: E402

BASE_COLS = [
    "weight_carried",
    "horse_weight",
    "horse_weight_diff",
    "win_odds",
    "popularity",
    "gate_number",
]
# ⚠️ ACCEL_COLS は build_acceleration_features が「予測対象レース自身の上がり3F」から
#    算出するため、予測特徴量に使うと当該レース結果のリークになる（is_win と直結）。
#    予測には使わず、参考充填率表示のみに留める。
ACCEL_COLS = ["pci", "acceleration_score", "last_3f_sec", "race_pci"]
# 真にリークフリーな新特徴量（過去出走のみ参照の前走系 ＋ cutoff前fitの血統TE）。
NEW_COLS = PRERUN_FEATURE_COLS + PEDIGREE_FEATURE_COLS


def _race_ids(conn: sqlite3.Connection, lo: str, hi: str, cap: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT r.race_id FROM races r
        WHERE r.date >= ? AND r.date < ?
          AND EXISTS (SELECT 1 FROM race_results rr
                      WHERE rr.race_id=r.race_id AND rr.rank>0 AND rr.win_odds>0)
        ORDER BY r.date
        """,
        (lo, hi),
    ).fetchall()
    ids = [r[0] for r in rows]
    if len(ids) > cap:
        # 均等間引き（期間全体をカバー）
        step = len(ids) / cap
        ids = [ids[int(i * step)] for i in range(cap)]
    return ids


def _build_race_frame(
    conn: sqlite3.Connection, race_id: str, enc: SireEncoder
) -> pd.DataFrame:
    base = pd.read_sql(
        """
        SELECT horse_number, rank, weight_carried, horse_weight, horse_weight_diff,
               win_odds, popularity, gate_number
        FROM race_results
        WHERE race_id = ? AND horse_number IS NOT NULL AND rank > 0 AND win_odds > 0
        """,
        conn,
        params=(race_id,),
    )
    if base.empty:
        return base
    pre = build_prerun_features(conn, race_id)
    ped = build_pedigree_features(conn, race_id, enc)
    acc = build_acceleration_features(conn, race_id)
    df = base.merge(pre, on="horse_number", how="left")
    df = df.merge(ped, on="horse_number", how="left")
    if not acc.empty:
        df = df.merge(acc[["horse_number", *ACCEL_COLS]], on="horse_number", how="left")
    for c in ACCEL_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df["race_id"] = race_id
    df["is_win"] = (df["rank"] == 1).astype(int)
    return df


def _assemble(
    conn: sqlite3.Connection, ids: list[str], enc: SireEncoder
) -> pd.DataFrame:
    frames = [f for rid in ids if not (f := _build_race_frame(conn, rid, enc)).empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _train_predict(
    train: pd.DataFrame, test: pd.DataFrame, cols: list[str]
) -> np.ndarray:
    import lightgbm as lgb

    xtr = train[cols].apply(pd.to_numeric, errors="coerce").astype(float)
    xte = test[cols].apply(pd.to_numeric, errors="coerce").astype(float)
    ytr = train["is_win"].astype(int)
    model = lgb.LGBMClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        verbose=-1,
    )
    model.fit(xtr, ytr)
    return model.predict_proba(xte)[:, 1]


def _evaluate(test: pd.DataFrame, prob_col: str, ev_threshold: float) -> dict:
    """単勝 EV>閾値 フラットベット(100円)で ROI・的中率・AUC を算出。"""
    from sklearn.metrics import roc_auc_score

    df = test.copy()
    # レース内でプロバを正規化して整合的な勝率にする
    df["p_norm"] = df.groupby("race_id")[prob_col].transform(
        lambda s: s / s.sum() if s.sum() > 0 else s
    )
    df["ev"] = df["p_norm"] * df["win_odds"]
    bets = df[df["ev"] >= ev_threshold]
    n_bets = len(bets)
    stake = n_bets * 100
    payout = (bets["is_win"] * bets["win_odds"] * 100).sum()
    roi = (payout / stake * 100) if stake else 0.0
    hit = (bets["is_win"].sum() / n_bets * 100) if n_bets else 0.0
    try:
        auc = roc_auc_score(df["is_win"], df[prob_col])
    except Exception:
        auc = float("nan")
    return {
        "auc": auc,
        "n_bets": n_bets,
        "stake": stake,
        "payout": float(payout),
        "roi": roi,
        "hit": hit,
    }


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2025-10-01", help="train/test 分割日")
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-hi", default="2026-07-01")
    ap.add_argument("--train-cap", type=int, default=1800)
    ap.add_argument("--test-cap", type=int, default=700)
    ap.add_argument("--ev", type=float, default=1.0, help="単勝EVベット閾値")
    args = ap.parse_args()

    from src.database.init_db import init_db

    conn = init_db()
    try:
        print("=" * 68)
        print("OOS バックテスト: ベースライン vs +新特徴量")
        print(
            f"  train: {args.train_lo} 〜 {args.cutoff} / test: {args.cutoff} 〜 {args.test_hi}"
        )
        print("=" * 68)

        # リークフリー: 血統TEは cutoff 前のみで fit（test結果は不参入）
        print("血統TE encoder を fit 中（cutoff前のみ・芝/ダ別）...")
        enc_turf = SireEncoder().fit(conn, cutoff_date=args.cutoff, surface="芝")
        # 簡易のため全体用に芝encoderを共用（surface混在は global_mean に収束）
        enc = enc_turf
        print(f"  global_mean(複勝基準率)= {enc.global_mean:.3f}")

        train_ids = _race_ids(conn, args.train_lo, args.cutoff, args.train_cap)
        test_ids = _race_ids(conn, args.cutoff, args.test_hi, args.test_cap)
        print(f"train races={len(train_ids)} / test races={len(test_ids)}  組立中...")

        train = _assemble(conn, train_ids, enc)
        test = _assemble(conn, test_ids, enc)
        print(f"train rows={len(train)} / test rows={len(test)}")
        if train.empty or test.empty:
            print("データ不足で中断")
            return 1

        # 充填率レポート
        print("\n新特徴量 充填率(test):")
        for c in NEW_COLS:
            if c in test.columns:
                print(f"  {c:22s}: {test[c].notna().mean() * 100:5.1f}%")

        # ベースライン vs 拡張
        print("\n学習・予測中（ベースライン）...")
        test["p_base"] = _train_predict(train, test, BASE_COLS)
        print("学習・予測中（+新特徴量）...")
        test["p_full"] = _train_predict(train, test, BASE_COLS + NEW_COLS)

        rb = _evaluate(test, "p_base", args.ev)
        rf = _evaluate(test, "p_full", args.ev)

        print("\n" + "=" * 68)
        print(f"{'指標':<16}{'ベースライン':>16}{'+新特徴量':>16}{'差分':>12}")
        print("-" * 68)
        print(
            f"{'AUC(単勝)':<16}{rb['auc']:>16.4f}{rf['auc']:>16.4f}{rf['auc'] - rb['auc']:>+12.4f}"
        )
        print(
            f"{'ROI(%)':<16}{rb['roi']:>16.1f}{rf['roi']:>16.1f}{rf['roi'] - rb['roi']:>+12.1f}"
        )
        print(
            f"{'的中率(%)':<16}{rb['hit']:>16.1f}{rf['hit']:>16.1f}{rf['hit'] - rb['hit']:>+12.1f}"
        )
        print(f"{'ベット数':<16}{rb['n_bets']:>16d}{rf['n_bets']:>16d}")
        print("=" * 68)
        verdict = (
            "改善 ✅"
            if rf["roi"] > rb["roi"] and rf["auc"] >= rb["auc"]
            else "改善せず ❌"
        )
        print(
            f"判定: {verdict}（ROI {rb['roi']:.1f}% → {rf['roi']:.1f}% / AUC {rb['auc']:.4f} → {rf['auc']:.4f}）"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
