"""スマートマネー（異常投票）代替プロキシの検証 — Shin (1993) z 値アプローチ。

背景:
  朝一→直前のオッズ変動率（スチームムーブ）が理想の特徴量だが、
  歴史データが存在しない（odds_timeseries=0行・realtime_odds=単一時点48レース）。
  代替として、**単一スナップショットの最終オッズ曲線からインサイダー資金比率を
  推定する Shin (1993) の z 値**を検証する。z はブックメーカー理論で
  「インサイダー取引者の存在がオッズの歪み（favorite-longshot bias）を生む」
  というモデルから導出される学術的に正当な推定量。

検証する仮説:
  H1: レース z 値が高い（インサイダー資金が濃い）レースでは、
      Shin 補正で確率が上方修正される馬（=資金が向かった先）の単勝が市場含意を超える。
  H2: 馬レベルの shin_uplift = shin_prob / implied_prob 上位馬は
      フラット単勝 ROI がベースライン（全馬≈控除率後80%）を上回る。

Usage:
    py scripts/validate_smart_money.py [--lo 2025-10-01] [--hi 2026-06-01] [--cap 400]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    ap = argparse.ArgumentParser()
    ap.add_argument("--lo", default="2025-10-01")
    ap.add_argument("--hi", default="2026-06-01")
    ap.add_argument("--cap", type=int, default=400)
    args = ap.parse_args()

    import numpy as np

    from src.ml.ev_features import MarketProbabilityCalc

    calc = MarketProbabilityCalc()
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        rids = [
            r[0]
            for r in conn.execute(
                """SELECT DISTINCT r.race_id FROM races r
                   WHERE r.date >= ? AND r.date < ?
                     AND EXISTS (SELECT 1 FROM race_results rr
                                 WHERE rr.race_id=r.race_id AND rr.rank>0
                                   AND rr.win_odds>1.0)
                   ORDER BY r.date""",
                (args.lo, args.hi),
            ).fetchall()
        ]
        if len(rids) > args.cap:
            step = len(rids) / args.cap
            rids = [rids[int(i * step)] for i in range(args.cap)]

        # 馬行: (race_z, shin_uplift, is_win, win_odds)
        rows_out: list[tuple[float, float, int, float]] = []
        for rid in rids:
            rows = conn.execute(
                "SELECT win_odds, rank FROM race_results "
                "WHERE race_id=? AND rank>0 AND win_odds>1.0",
                (rid,),
            ).fetchall()
            if len(rows) < 8:
                continue
            odds = np.array([float(r[0]) for r in rows])
            ranks = [int(r[1]) for r in rows]
            inv = 1.0 / odds
            k = float(inv.sum())
            n = len(odds)
            z = max((k - 1.0) / (n - 1.0), 0.0)  # Shin z（インサイダー比率）
            shin = calc.compute_shin_probabilities(odds)
            implied = inv / k
            uplift = shin / np.maximum(implied, 1e-12)
            for i in range(n):
                rows_out.append(
                    (z, float(uplift[i]), 1 if ranks[i] == 1 else 0, float(odds[i]))
                )

        data = np.array(rows_out)
        z_arr, up_arr, win_arr, odds_arr = (
            data[:, 0],
            data[:, 1],
            data[:, 2],
            data[:, 3],
        )
        print(
            f"対象: {len(rids)}レース / {len(data)}馬行 "
            f"(z 中央値 {np.median(z_arr):.4f})"
        )

        def _roi(mask: np.ndarray) -> tuple[int, float, float]:
            nn = int(mask.sum())
            if nn == 0:
                return 0, 0.0, 0.0
            pay = float((win_arr[mask] * odds_arr[mask] * 100).sum())
            return nn, float(win_arr[mask].mean() * 100), pay / (nn * 100) * 100

        print(
            "\n■ H2: shin_uplift（Shin補正による確率上方修正率）十分位 × 単勝フラットROI"
        )
        print(f" {'分位':<14} {'n':>6} {'的中率%':>7} {'ROI%':>7}")
        deciles = np.quantile(up_arr, np.linspace(0, 1, 11))
        for d in range(10):
            mask = (up_arr >= deciles[d]) & (
                up_arr <= deciles[d + 1] if d == 9 else up_arr < deciles[d + 1]
            )
            nn, hr, roi = _roi(mask)
            mark = " ←" if d >= 8 else ""
            print(
                f" D{d + 1:<2}[{deciles[d]:.3f}-{deciles[d + 1]:.3f}] "
                f"{nn:>6} {hr:>7.1f} {roi:>7.1f}{mark}"
            )
        nn, hr, roi = _roi(np.ones(len(data), dtype=bool))
        print(f" {'全馬(基準)':<14} {nn:>6} {hr:>7.1f} {roi:>7.1f}")

        print("\n■ H1: レース z 値（インサイダー濃度）四分位 × uplift上位3頭の単勝ROI")
        zq = np.quantile(z_arr, [0, 0.25, 0.5, 0.75, 1.0])
        # uplift がレース内上位3頭か（簡易: 全体の上位30%で近似）
        up_hi = up_arr >= np.quantile(up_arr, 0.7)
        for q in range(4):
            zmask = (z_arr >= zq[q]) & (
                z_arr <= zq[q + 1] if q == 3 else z_arr < zq[q + 1]
            )
            nn, hr, roi = _roi(zmask & up_hi)
            nn_all, hr_all, roi_all = _roi(zmask)
            print(
                f" Z{q + 1}[{zq[q]:.4f}-{zq[q + 1]:.4f}] "
                f"uplift上位: n={nn:>5} ROI{roi:>7.1f}% / 全体: ROI{roi_all:>6.1f}%"
            )

        print("\n■ 判定")
        top_mask = up_arr >= np.quantile(up_arr, 0.9)
        nn, hr, roi_top = _roi(top_mask)
        base_roi = _roi(np.ones(len(data), dtype=bool))[2]
        verdict = (
            "✅ 採用候補（特徴量化→再学習→OOSへ進む）"
            if roi_top > base_roi + 10
            else "❌ 単独ではエッジ不十分（特徴量化は保留）"
        )
        print(
            f" uplift上位10%: n={nn} ROI {roi_top:.1f}% vs 基準 {base_roi:.1f}% → {verdict}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
