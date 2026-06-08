"""Accuracy Model の OOS 評価（タスク2.1）。

2025年学習 → 2026年テストのアウト・オブ・サンプルで、勝率特化 Classifier の
的中率（Top-1 Accuracy）・対数損失（LogLoss）・AUC を算出する。

リークフリー: 学習は train_from/train_until で 2025 のみ、テストは 2026 のみ。
特徴量は FeatureBuilder（rank/finish_time/margin 排除済）由来。

使い方::
    py scripts/evaluate_accuracy_model.py
    py scripts/evaluate_accuracy_model.py --train-year 2025 --test-year 2026
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np  # noqa: E402
from sklearn.metrics import log_loss, roc_auc_score  # noqa: E402

from src.database.init_db import init_db  # noqa: E402
from src.ml.accuracy_model import AccuracyModel  # noqa: E402
from src.ml.models import _build_train_df  # noqa: E402


def _top1_accuracy(df, proba: np.ndarray) -> tuple[int, int]:
    """各レースで予測確率最大の馬が実際の1着か判定し (的中数, レース数) を返す。"""
    work = df[["race_id", "is_winner"]].copy()
    work["_p"] = proba
    hits = races = 0
    for _race_id, grp in work.groupby("race_id"):
        if grp["is_winner"].sum() == 0:  # 1着不明レースは除外
            continue
        races += 1
        idx = grp["_p"].idxmax()
        if int(grp.loc[idx, "is_winner"]) == 1:
            hits += 1
    return hits, races


def main() -> int:
    ap = argparse.ArgumentParser(description="Accuracy Model OOS 評価")
    ap.add_argument("--train-year", type=int, default=2025)
    ap.add_argument("--test-year", type=int, default=2026)
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    conn = init_db()
    print(
        f"=== Accuracy Model OOS 評価  学習{args.train_year} → テスト{args.test_year} ==="
    )

    train_df = _build_train_df(
        conn, train_from=args.train_year, train_until=args.train_year
    )
    test_df = _build_train_df(
        conn, train_from=args.test_year, train_until=args.test_year
    )
    print(
        f"  学習: {train_df['race_id'].nunique():,} レース / {len(train_df):,} サンプル"
    )
    print(
        f"  テスト: {test_df['race_id'].nunique():,} レース / {len(test_df):,} サンプル"
    )

    model = AccuracyModel()
    train_metrics = model.train(conn, df=train_df)

    proba = model.predict_proba(test_df)
    y_test = test_df["is_winner"].astype(int).to_numpy()

    ll = log_loss(y_test, proba, labels=[0, 1])
    auc = roc_auc_score(y_test, proba) if y_test.min() != y_test.max() else float("nan")
    hits, races = _top1_accuracy(test_df, proba)
    acc = hits / races if races else 0.0

    print("\n--- OOS 結果（テスト年・カンニングなし） ---")
    print(f"  train AUC          : {train_metrics.get('train_auc', float('nan')):.4f}")
    print(f"  OOS LogLoss        : {ll:.4f}")
    print(f"  OOS AUC            : {auc:.4f}")
    print(f"  OOS Top-1 的中率    : {acc:.1%}  ({hits:,}/{races:,} レース)")
    print(f"  ベースレート(1着率)  : {y_test.mean():.1%}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
