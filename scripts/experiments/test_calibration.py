"""
確率校正（キャリブレーション）検証スクリプト  Phase 2.5

既存の過去データを使って以下を比較する:
  - Raw LGBMClassifier（キャリブレーションなし）
  - Platt Scaling（Sigmoid: LogisticRegression）
  - Isotonic Regression

評価指標:
  - Brier Score（予測確率と実勝否の二乗誤差平均）
  - Reliability Diagram（キャリブレーション曲線の目視確認用データ）
  - EV ≥ threshold フィルタ適用後の単勝回収率シミュレーション

Usage:
    py scripts/experiments/test_calibration.py
    py scripts/experiments/test_calibration.py --ev-threshold 1.1 --bins 8
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.model_selection import GroupKFold

# Windows CP932 端末でも日本語が文字化けしないよう UTF-8 強制
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db
from src.ml.models import FEATURE_COLS, _build_train_df  # noqa: WPS450


# ── ユーティリティ ────────────────────────────────────────────────────────

def _ev_filter_return_rate(
    proba: np.ndarray,
    win_odds: np.ndarray,
    is_winner: np.ndarray,
    ev_threshold: float,
) -> dict[str, float]:
    """EV フィルタ後の単勝回収率と各種統計を返す。"""
    ev = proba * win_odds
    mask = ev >= ev_threshold
    n_bet = int(mask.sum())
    if n_bet == 0:
        return {"n_bet": 0, "return_rate": 0.0, "hit_rate": 0.0}

    hits = is_winner[mask]
    payouts = np.where(hits == 1, win_odds[mask] * 100, 0)
    spent = n_bet * 100
    return {
        "n_bet": n_bet,
        "return_rate": float(payouts.sum() / spent * 100),
        "hit_rate": float(hits.mean() * 100),
    }


def _reliability_table(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10,
) -> pd.DataFrame:
    """キャリブレーション曲線のビン集計テーブルを返す。"""
    frac_pos, mean_pred = calibration_curve(y_true, y_prob, n_bins=n_bins, strategy="quantile")
    return pd.DataFrame({"pred_mean": mean_pred, "actual_frac": frac_pos})


# ── メイン ────────────────────────────────────────────────────────────────

def run(ev_threshold: float = 1.0, n_bins: int = 10) -> None:
    print("=" * 60)
    print("  UMALOGI Phase 2.5 — 確率校正（キャリブレーション）検証")
    print("=" * 60)

    conn = init_db()
    df_all = _build_train_df(conn)
    conn.close()

    if df_all.empty:
        print("[ERROR] 学習データが 0 件です。DB を確認してください。")
        sys.exit(1)

    n_races = df_all["race_id"].nunique()
    n_samples = len(df_all)
    print(f"\n学習データ: {n_races} レース / {n_samples} サンプル\n")

    if n_races < 50:
        print(f"[WARNING] レース数が少ないため結果の信頼性が低い可能性があります（{n_races} レース）")

    # ── 時系列分割（後半 20% をテストセット）────────────────────────────
    df_sorted = df_all.sort_values("race_id").reset_index(drop=True)
    n = len(df_sorted)
    split_idx = int(n * 0.8)

    df_train = df_sorted.iloc[:split_idx].copy()
    df_test  = df_sorted.iloc[split_idx:].copy()

    X_train = df_train[FEATURE_COLS].astype(float).fillna(-1).values
    y_train = df_train["is_winner"].values
    groups_train = df_train["race_id"].values

    X_test  = df_test[FEATURE_COLS].astype(float).fillna(-1).values
    y_test  = df_test["is_winner"].values
    odds_test = df_test["win_odds"].fillna(0.0).astype(float).values

    test_races = df_test["race_id"].nunique()
    print(f"Train: {len(df_train)} サンプル ({df_train['race_id'].nunique()} レース)")
    print(f"Test : {len(df_test)} サンプル ({test_races} レース)\n")

    # ── LGBMClassifier 訓練（Raw） ──────────────────────────────────────
    from lightgbm import LGBMClassifier

    lgbm_params = dict(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=10,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )
    raw_clf = LGBMClassifier(**lgbm_params)
    raw_clf.fit(X_train, y_train)
    raw_proba = raw_clf.predict_proba(X_test)[:, 1]
    raw_auc = roc_auc_score(y_test, raw_proba)

    # ── Platt Scaling ────────────────────────────────────────────────────
    # GroupKFold OOF で Platt 用訓練データを生成
    n_splits = min(5, df_train["race_id"].nunique())
    oof_preds = np.zeros(len(X_train), dtype=float)
    if n_splits >= 2:
        gkf = GroupKFold(n_splits=n_splits)
        for tr_idx, val_idx in gkf.split(X_train, y_train, groups=groups_train):
            clone = LGBMClassifier(**lgbm_params)
            clone.fit(X_train[tr_idx], y_train[tr_idx])
            oof_preds[val_idx] = clone.predict_proba(X_train[val_idx])[:, 1]
    else:
        oof_preds = raw_clf.predict_proba(X_train)[:, 1]

    platt = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
    platt.fit(oof_preds.reshape(-1, 1), y_train)
    platt_proba_test = platt.predict_proba(
        raw_proba.reshape(-1, 1)
    )[:, 1]

    # ── Isotonic Regression ──────────────────────────────────────────────
    # OOF 予測（Platt と共有）に対して IsotonicRegression を適用
    # CalibratedClassifierCV は GroupKFold + groups の受け渡しに sklearn バージョン依存の問題があるため
    # 手動 OOF 実装に統一する
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oof_preds, y_train)
    iso_proba_test = iso.predict(raw_proba).astype(float)

    # ── Brier Score 比較 ─────────────────────────────────────────────────
    brier_raw   = brier_score_loss(y_test, raw_proba)
    brier_platt = brier_score_loss(y_test, platt_proba_test)
    brier_iso   = brier_score_loss(y_test, iso_proba_test)

    print("─" * 60)
    print(f"{'手法':<22} {'Brier Score':>12} {'改善率':>10} {'ROC-AUC':>10}")
    print("─" * 60)
    print(f"{'Raw LGBMClassifier':<22} {brier_raw:>12.5f} {'(基準)':>10} {raw_auc:>10.4f}")
    auc_platt = roc_auc_score(y_test, platt_proba_test)
    impr_platt = (brier_raw - brier_platt) / brier_raw * 100
    print(f"{'Platt Scaling':<22} {brier_platt:>12.5f} {impr_platt:>+9.2f}% {auc_platt:>10.4f}")
    auc_iso = roc_auc_score(y_test, iso_proba_test)
    impr_iso = (brier_raw - brier_iso) / brier_raw * 100
    print(f"{'Isotonic Regression':<22} {brier_iso:>12.5f} {impr_iso:>+9.2f}% {auc_iso:>10.4f}")
    print("─" * 60)

    best = min(
        [("Raw", brier_raw), ("Platt", brier_platt), ("Isotonic", brier_iso)],
        key=lambda x: x[1],
    )
    print(f"\n✅ Brier Score 最良手法: {best[0]} ({best[1]:.5f})\n")

    # ── EV フィルタ後の単勝回収率シミュレーション ────────────────────────
    print(f"─ EV ≥ {ev_threshold} フィルタ後 単勝回収率シミュレーション ─")
    print(f"{'手法':<22} {'ベット数':>8} {'回収率(%)':>12} {'的中率(%)':>12}")
    print("─" * 60)

    for label, proba in [
        ("Raw", raw_proba),
        ("Platt Scaling", platt_proba_test),
        ("Isotonic Regression", iso_proba_test),
    ]:
        stats = _ev_filter_return_rate(proba, odds_test, y_test, ev_threshold)
        print(
            f"{label:<22} {stats['n_bet']:>8} "
            f"{stats['return_rate']:>11.1f}% {stats['hit_rate']:>11.1f}%"
        )
    print("─" * 60)

    # ── キャリブレーション曲線（テキスト表示）───────────────────────────
    print("\n─ キャリブレーション曲線（Platt Scaling） ─")
    try:
        cal_df = _reliability_table(y_test, platt_proba_test, n_bins=n_bins)
        print(f"  {'予測確率':>10} │ {'実勝率':>10} │ {'乖離':>8}")
        print("  " + "─" * 35)
        for _, row in cal_df.iterrows():
            diff = row["actual_frac"] - row["pred_mean"]
            print(f"  {row['pred_mean']:>10.3f} │ {row['actual_frac']:>10.3f} │ {diff:>+8.3f}")
    except Exception as e:
        print(f"  [スキップ] {e}")

    print("\n─ キャリブレーション曲線（Isotonic Regression） ─")
    try:
        cal_df2 = _reliability_table(y_test, iso_proba_test, n_bins=n_bins)
        print(f"  {'予測確率':>10} │ {'実勝率':>10} │ {'乖離':>8}")
        print("  " + "─" * 35)
        for _, row in cal_df2.iterrows():
            diff = row["actual_frac"] - row["pred_mean"]
            print(f"  {row['pred_mean']:>10.3f} │ {row['actual_frac']:>10.3f} │ {diff:>+8.3f}")
    except Exception as e:
        print(f"  [スキップ] {e}")

    print("\n" + "=" * 60)
    print("  Phase 2.5 推奨事項")
    print("=" * 60)
    if brier_platt <= brier_iso:
        print("  → Platt Scaling が最良。HonmeiModel の現行実装（_PlattModel）を維持推奨。")
    else:
        print("  → Isotonic Regression が優勢。HonmeiModel に CalibratedClassifierCV(isotonic) 導入を検討。")

    if impr_platt < 1.0 and impr_iso < 1.0:
        print("  → Brier 改善幅が < 1%。現データ量ではキャリブレーション効果は限定的。")
        print("    データが蓄積された後（>500 レース）に再検証することを推奨。")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="UMALOGI Phase 2.5 確率校正検証")
    parser.add_argument("--ev-threshold", type=float, default=1.0, help="EV フィルタ閾値 (default: 1.0)")
    parser.add_argument("--bins", type=int, default=10, help="キャリブレーション曲線のビン数 (default: 10)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(ev_threshold=args.ev_threshold, n_bins=args.bins)
