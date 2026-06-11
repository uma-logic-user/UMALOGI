"""見送り（No-Bet）判定フィルターの有効性検証 — 実弾確定 P&L 履歴への遡及適用。

実際に投票した実弾ベット（bet_policy.is_live_bet・確定 prediction_results）を
レース単位の chaos_score で「買うべきだった/見送るべきだった」に二分し、

  - 防げた不的中（見送りレースで実際に外れたベット数・損失額）
  - 機会損失（見送りレースで実際に当たっていたベット数・逸失利益）
  - フィルタ適用後 ROI vs 適用前 ROI

を定量評価する。chaos_score の入力オッズは race_results.win_odds（確定オッズ）を
使用する＝実運用では直前オッズになるため近似であることに注意（保守的検証）。

Usage:
    py scripts/validate_no_bet_filter.py [--threshold 0.42] [--since 2026-01-01]
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
    ap.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="chaos閾値（既定: モジュール既定）",
    )
    ap.add_argument("--since", default=None, help="集計開始日 YYYY-MM-DD")
    args = ap.parse_args()

    from src.ml.bet_policy import is_live_bet
    from src.ml.no_bet_filter import NoBetConfig, evaluate_no_bet

    cfg = (
        NoBetConfig(chaos_threshold=args.threshold) if args.threshold else NoBetConfig()
    )

    conn = sqlite3.connect(str(_DB_PATH))
    try:
        # 実弾ベット（確定）をレース付きで取得
        where = "WHERE pr.payout IS NOT NULL AND COALESCE(p.is_superseded,0)=0"
        params: list[object] = []
        if args.since:
            where += " AND r.date >= ?"
            params.append(args.since)
        bets = conn.execute(
            f"""
            SELECT p.race_id, p.model_type, p.bet_type,
                   COALESCE(pr.payout,0), COALESCE(pr.profit,0), COALESCE(pr.is_hit,0),
                   r.condition
            FROM predictions p
            JOIN prediction_results pr ON pr.prediction_id = p.id
            JOIN races r ON r.race_id = p.race_id
            {where}
            """,
            params,
        ).fetchall()

        live = [b for b in bets if is_live_bet(str(b[1] or ""), str(b[2] or ""))]
        race_ids = sorted({b[0] for b in live})
        print(f"実弾確定ベット: {len(live)}件 / {len(race_ids)}レース")
        if not live:
            print("対象なし")
            return 0

        # レースごとの chaos_score（確定オッズから算出）
        decisions: dict[str, object] = {}
        for rid in race_ids:
            rows = conn.execute(
                "SELECT win_odds FROM race_results "
                "WHERE race_id=? AND win_odds IS NOT NULL AND win_odds>1.0",
                (rid,),
            ).fetchall()
            cond_row = conn.execute(
                "SELECT condition FROM races WHERE race_id=?", (rid,)
            ).fetchone()
            odds = [float(r[0]) for r in rows]
            decisions[rid] = evaluate_no_bet(
                rid, odds, condition=cond_row[0] if cond_row else None, config=cfg
            )

        # 集計
        def _agg(items: list[tuple]) -> dict[str, float]:
            n = len(items)
            cost = sum(p - pf for _, _, _, p, pf, _, _ in items)
            payout = sum(p for _, _, _, p, _, _, _ in items)
            hits = sum(
                h
                for *_, h, _ in [
                    (b[0], b[1], b[2], b[3], b[4], b[5], b[6]) for b in items
                ]
            )
            return {
                "n": n,
                "cost": cost,
                "payout": payout,
                "profit": payout - cost,
                "hits": sum(b[5] for b in items),
                "roi": payout / cost * 100 if cost > 0 else 0.0,
            }

        kept = [b for b in live if not decisions[b[0]].skip]  # type: ignore[union-attr]
        skipped = [b for b in live if decisions[b[0]].skip]  # type: ignore[union-attr]
        a_all, a_kept, a_skip = _agg(live), _agg(kept), _agg(skipped)
        n_skip_races = sum(1 for d in decisions.values() if d.skip)  # type: ignore[union-attr]

        print("\n" + "=" * 72)
        print(
            f" No-Bet フィルタ検証（chaos閾値 {cfg.chaos_threshold}・確定オッズ近似）"
        )
        print("=" * 72)
        print(
            f" 見送り判定レース: {n_skip_races}/{len(race_ids)} "
            f"({n_skip_races / len(race_ids) * 100:.1f}%)"
        )
        for label, a in (("適用前(全実弾)", a_all), ("適用後(買うレースのみ)", a_kept)):
            print(
                f" {label:<22} n={a['n']:>5} 的中{a['hits']:>4} "
                f"投資¥{a['cost']:>9,.0f} 払戻¥{a['payout']:>9,.0f} "
                f"ROI {a['roi']:6.1f}%"
            )
        print("-" * 72)
        miss_in_skip = a_skip["n"] - a_skip["hits"]
        print(
            f" 🛡️ 防げた不的中: {miss_in_skip}件（見送りレースの損失 "
            f"¥{max(-a_skip['profit'], 0):,.0f} を回避）"
            if a_skip["profit"] < 0
            else f" 🛡️ 防げた不的中: {miss_in_skip}件"
        )
        print(
            f" 💸 機会損失: 的中{a_skip['hits']}件・逸失払戻 ¥{a_skip['payout']:,.0f}"
            f"（見送りレース純損益 ¥{a_skip['profit']:+,.0f}）"
        )
        print(
            f" 📈 ROI 改善: {a_all['roi']:.1f}% → {a_kept['roi']:.1f}% "
            f"({a_kept['roi'] - a_all['roi']:+.1f}pt)"
        )
        verdict = (
            "✅ フィルタ有効（見送りレースは負け越し＝切って正解）"
            if a_skip["profit"] < 0
            else "❌ フィルタ逆効果（見送りレースが勝ち越し＝閾値要再調整）"
        )
        print(f" 判定: {verdict}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
