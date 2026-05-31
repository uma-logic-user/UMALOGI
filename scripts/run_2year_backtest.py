"""
scripts/run_2year_backtest.py — 2カ年厳選黒字化シミュレーター

Pattern A: EV閾値のみ (EV >= threshold)
Pattern B: オッズ範囲 + EV閾値 (odds_lo <= odds <= odds_hi AND EV >= threshold)
Pattern C: 1日購入上限 + EV閾値 (daily top-N by EV, EV >= threshold)

使い方:
    py scripts/run_2year_backtest.py
    py scripts/run_2year_backtest.py --output results/backtest_grid.csv
    py scripts/run_2year_backtest.py --model honmei --bet-type win
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_DB_PATH = _ROOT / "data" / "umalogi.db"

_EV_THRESHOLDS = [1.0, 1.2, 1.5, 2.0]
_ODDS_RANGES = [(3.0, 10.0), (5.0, 25.0), (5.0, 50.0)]
_DAILY_LIMITS = [3, 5, 10]


@dataclass
class _BacktestRow:
    pattern: str
    params: str
    model_type: str
    bet_type: str
    n_bets: int
    n_hits: int
    total_cost: int
    total_payout: int
    roi: float
    hit_rate: float


def _load_data(
    conn: sqlite3.Connection,
    model_type: str,
    bet_type: str,
) -> list[dict[str, Any]]:
    """predictions + prediction_results を JOIN して返す。"""
    sql = """
    SELECT
        p.prediction_id,
        p.race_id,
        p.expected_value,
        p.horse_number,
        r.race_date,
        COALESCE(pr.is_hit, 0)  AS is_hit,
        COALESCE(pr.payout, 0)  AS payout
    FROM predictions p
    LEFT JOIN prediction_results pr ON pr.prediction_id = p.prediction_id
    LEFT JOIN races r ON r.race_id = p.race_id
    WHERE p.model_type = ?
      AND p.bet_type   = ?
    ORDER BY r.race_date, p.race_id
    """
    rows = conn.execute(sql, (model_type, bet_type)).fetchall()
    return [dict(row) for row in rows]


def _run_pattern_a(
    rows: list[dict], ev_thresh: float, model_type: str, bet_type: str
) -> _BacktestRow:
    filtered = [r for r in rows if (r["expected_value"] or 0.0) >= ev_thresh]
    n_bets = len(filtered)
    n_hits = sum(r["is_hit"] for r in filtered)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in filtered)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="A",
        params=f"ev>={ev_thresh:.1f}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _run_pattern_b(
    rows: list[dict],
    ev_thresh: float,
    odds_lo: float,
    odds_hi: float,
    model_type: str,
    bet_type: str,
) -> _BacktestRow:
    filtered = [
        r
        for r in rows
        if (r["expected_value"] or 0.0) >= ev_thresh
        and odds_lo <= (r["payout"] / 100.0 if r["payout"] else 0.0) <= odds_hi
    ]
    n_bets = len(filtered)
    n_hits = sum(r["is_hit"] for r in filtered)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in filtered)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="B",
        params=f"ev>={ev_thresh:.1f},odds={odds_lo:.0f}-{odds_hi:.0f}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _run_pattern_c(
    rows: list[dict],
    ev_thresh: float,
    daily_limit: int,
    model_type: str,
    bet_type: str,
) -> _BacktestRow:
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if (r["expected_value"] or 0.0) >= ev_thresh:
            by_date[r["race_date"] or "unknown"].append(r)
    selected: list[dict] = []
    for date_rows in by_date.values():
        sorted_rows = sorted(
            date_rows, key=lambda x: x["expected_value"] or 0.0, reverse=True
        )
        selected.extend(sorted_rows[:daily_limit])
    n_bets = len(selected)
    n_hits = sum(r["is_hit"] for r in selected)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in selected)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="C",
        params=f"ev>={ev_thresh:.1f},daily<={daily_limit}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _print_summary(results: list[_BacktestRow]) -> None:
    print()
    print("=" * 90)
    print(
        f"{'Pattern':<8} {'Params':<30} {'Model':<8} {'BetType':<8} "
        f"{'Bets':>6} {'Hits':>5} {'ROI':>7} {'HitRate':>8}"
    )
    print("-" * 90)
    for r in sorted(results, key=lambda x: x.roi, reverse=True):
        flag = "★" if r.roi >= 1.0 and r.n_bets >= 10 else " "
        print(
            f"{flag}{r.pattern:<7} {r.params:<30} {r.model_type:<8} {r.bet_type:<8} "
            f"{r.n_bets:>6} {r.n_hits:>5} {r.roi:>7.1%} {r.hit_rate:>8.1%}"
        )
    print("=" * 90)
    winners = [r for r in results if r.roi >= 1.0 and r.n_bets >= 10]
    print(f"\n★ ROI >= 100% かつ Bets >= 10 の組み合わせ: {len(winners)} 件")


def _save_csv(results: list[_BacktestRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "pattern",
                "params",
                "model_type",
                "bet_type",
                "n_bets",
                "n_hits",
                "total_cost",
                "total_payout",
                "roi",
                "hit_rate",
            ]
        )
        for r in results:
            w.writerow(
                [
                    r.pattern,
                    r.params,
                    r.model_type,
                    r.bet_type,
                    r.n_bets,
                    r.n_hits,
                    r.total_cost,
                    r.total_payout,
                    f"{r.roi:.4f}",
                    f"{r.hit_rate:.4f}",
                ]
            )
    print(f"\n📊 CSV 保存: {path}")


def main() -> None:
    p = argparse.ArgumentParser(description="2カ年厳選黒字化シミュレーター")
    p.add_argument(
        "--model", default="honmei", help="モデルタイプ (honmei/manji/alpha)"
    )
    p.add_argument("--bet-type", default="win", help="券種 (win/place/quinella/trio)")
    p.add_argument("--output", default="", help="CSV 出力先パス（省略時は出力なし）")
    args = p.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = _load_data(conn, args.model, args.bet_type)
    finally:
        conn.close()

    if not rows:
        print(f"⚠️  データなし: model={args.model}, bet_type={args.bet_type}")
        sys.exit(0)

    print(
        f"🔍 対象データ: {len(rows)} 件 (model={args.model}, bet_type={args.bet_type})"
    )

    results: list[_BacktestRow] = []

    # Pattern A
    for ev in _EV_THRESHOLDS:
        results.append(_run_pattern_a(rows, ev, args.model, args.bet_type))

    # Pattern B
    for ev in _EV_THRESHOLDS:
        for lo, hi in _ODDS_RANGES:
            results.append(_run_pattern_b(rows, ev, lo, hi, args.model, args.bet_type))

    # Pattern C
    for ev in _EV_THRESHOLDS:
        for limit in _DAILY_LIMITS:
            results.append(_run_pattern_c(rows, ev, limit, args.model, args.bet_type))

    _print_summary(results)

    if args.output:
        _save_csv(results, Path(args.output))


if __name__ == "__main__":
    main()
