"""
単勝・複勝・卍モデルを 2024-2025 クリーンデータ含む全期間で再学習するスクリプト。

修正後の _build_train_df（リーク排除・全馬含む）を使って
HonmeiModel / PlaceModel / ManjiModel を再訓練し data/models/ に保存する。

使用例:
    py scripts/retrain_win_place.py
    py scripts/retrain_win_place.py --train-until 2025
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("retrain")


def main() -> None:
    ap = argparse.ArgumentParser(description="単複特化モデル再学習")
    ap.add_argument("--train-until", type=int, default=None,
                    help="学習最終年 (例: 2025 → 2025年以前)")
    args = ap.parse_args()

    from src.database.init_db import init_db
    from src.ml.models import train_all

    conn = init_db()
    print("=" * 60)
    print("  単勝・複勝・卍モデル 再学習")
    print(f"  train_until={args.train_until or '全期間'}")
    print("=" * 60)

    results = train_all(conn, train_until=args.train_until)
    conn.close()

    print()
    h = results["honmei"]
    p = results["place"]
    m = results["manji"]

    print("  [本命モデル]")
    print(f"    レース数: {h['n_races']:,}  サンプル数: {h['n_samples']:,}")
    print(f"    CV AUC:   {h.get('cv_auc_mean', float('nan')):.4f} ±{h.get('cv_auc_std', float('nan')):.4f}")
    print(f"    Challenger AUC: {h.get('challenger_auc', float('nan')):.4f}")
    print(f"    世代交代: {h.get('promoted', '?')}")
    print()
    print("  [複勝モデル]")
    print(f"    レース数: {p['n_races']:,}  サンプル数: {p['n_samples']:,}")
    print(f"    CV AUC:   {p.get('cv_auc_mean', float('nan')):.4f} ±{p.get('cv_auc_std', float('nan')):.4f}")
    print()
    print("  [卍モデル]")
    print(f"    レース数: {m['n_races']:,}  サンプル数: {m['n_samples']:,}")
    print()
    print("  完了")


if __name__ == "__main__":
    main()
