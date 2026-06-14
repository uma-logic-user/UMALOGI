"""
scripts/walk_forward_trouble.py — prev_trouble_proxy のウォークフォワード頑健性検証（W-096）

2 cutoff だけでは偶然と区別できないため、複数 cutoff で BASE vs BASE+prev_trouble_proxy を
時系列 OOS 比較し、ROI 改善が**一貫して**現れるか（=本物の信号か）を判定する。

各 cutoff: train=[train_lo, cutoff) / test=[cutoff, cutoff+test_months)。
出力: cutoff 別 ROI/AUC と、全 cutoff の平均・勝率（改善した cutoff の割合）。
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402

from scripts.backtest_v2_oos import (  # noqa: E402
    BASE_COLS,
    _assemble,
    _evaluate,
    _race_ids,
)
from scripts.model_status_report import _predict_into, _train  # noqa: E402
from src.features.pedigree_te import SireEncoder  # noqa: E402


def _add_months(ym: str, months: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    total = (y * 12 + (m - 1)) + months
    return f"{total // 12:04d}-{total % 12 + 1:02d}-01"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--cutoffs",
        default="2025-10-01,2025-11-01,2025-12-01,2026-01-01,2026-02-01,2026-03-01",
    )
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-months", type=int, default=2)
    ap.add_argument("--train-cap", type=int, default=1500)
    ap.add_argument("--test-cap", type=int, default=600)
    ap.add_argument("--ev", type=float, default=1.0)
    args = ap.parse_args()

    cutoffs = [c.strip() for c in args.cutoffs.split(",") if c.strip()]
    conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
    rows: list[dict] = []
    try:
        for cutoff in cutoffs:
            test_hi = _add_months(cutoff, args.test_months)
            enc = SireEncoder().fit(conn, cutoff_date=cutoff, surface="芝")
            tr_ids = _race_ids(conn, args.train_lo, cutoff, args.train_cap)
            te_ids = _race_ids(conn, cutoff, test_hi, args.test_cap)
            train = _assemble(conn, tr_ids, enc)
            test = _assemble(conn, te_ids, enc)
            if train.empty or test.empty:
                print(f"{cutoff}: データ不足スキップ")
                continue

            m_base = _train(train, BASE_COLS)
            _predict_into(m_base, test, BASE_COLS, "p_base")
            cols_t = BASE_COLS + ["prev_trouble_proxy"]
            m_tro = _train(train, cols_t)
            _predict_into(m_tro, test, cols_t, "p_tro")

            r_base = _evaluate(test, "p_base", args.ev)
            r_tro = _evaluate(test, "p_tro", args.ev)
            d = {
                "cutoff": cutoff,
                "test_races": len(te_ids),
                "auc_base": r_base["auc"],
                "auc_tro": r_tro["auc"],
                "roi_base": r_base["roi"],
                "roi_tro": r_tro["roi"],
                "d_roi": r_tro["roi"] - r_base["roi"],
                "d_auc": r_tro["auc"] - r_base["auc"],
            }
            rows.append(d)
            print(
                f"{cutoff}: ROI {r_base['roi']:6.1f}% -> {r_tro['roi']:6.1f}% "
                f"(d={d['d_roi']:+5.1f}pp) | AUC {r_base['auc']:.4f}->{r_tro['auc']:.4f} "
                f"(d={d['d_auc']:+.4f}) | test_races={len(te_ids)}"
            )

        if rows:
            df = pd.DataFrame(rows)
            n = len(df)
            wins = int((df["d_roi"] > 0).sum())
            print("\n" + "=" * 60)
            print(f"cutoff 数={n}  ROI改善した cutoff={wins}/{n} ({wins / n * 100:.0f}%)")
            print(f"平均 ΔROI = {df['d_roi'].mean():+.2f}pp  (中央値 {df['d_roi'].median():+.2f}pp)")
            print(f"平均 ΔAUC = {df['d_auc'].mean():+.5f}")
            print("=" * 60)
            consistent = wins >= n - 1 and df["d_roi"].mean() > 1.0
            print(
                "判定: 一貫した改善 ✅（本番投入を検討可）"
                if consistent
                else "判定: 一貫した改善は無い ❌（偶然/ノイズ・本番投入しない）"
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    print(f"walk-forward 開始 {date.today()}")
    raise SystemExit(main())
