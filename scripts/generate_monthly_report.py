"""
月次 ROI レポート生成スクリプト

実行方法:
  python scripts/generate_monthly_report.py
  python scripts/generate_monthly_report.py --months 6   # 直近6ヶ月

出力内容:
  1. 月別トータル ROI（モデル別・券種別内訳つき）
  2. 会場別的中率（ROI 降順）
  3. ケリー基準での資金増減シミュレーション（シャープレシオ・最大ドローダウン付き）
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db

# ── 会場コード → 名称 ────────────────────────────────────────────
_JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


# ================================================================
# 1. 月別 ROI
# ================================================================

def _monthly_roi(conn, months: int) -> list[dict[str, Any]]:
    """
    月別・モデル別・券種別の ROI を集計する。

    Returns:
        [{"month": "2025-01", "model_type": "本命", "bet_type": "単勝",
          "bets": 120, "hits": 30, "invested": 12000, "payout": 13500,
          "roi": 1.125, "hit_rate": 0.25}, ...]
    """
    sql = """
        WITH recent AS (
            -- 直近 N ヶ月分に絞る（predictions.created_at を基準）
            SELECT
                pr.is_hit,
                pr.payout,
                p.model_type,
                p.bet_type,
                p.recommended_bet,
                strftime('%Y-%m', p.created_at) AS month
            FROM prediction_results pr
            JOIN predictions p ON p.id = pr.prediction_id
            WHERE p.created_at >= date('now', :offset)
              AND p.recommended_bet > 0
        )
        SELECT
            month,
            model_type,
            bet_type,
            COUNT(*)                        AS bets,
            SUM(is_hit)                     AS hits,
            SUM(recommended_bet)            AS invested,
            SUM(CASE WHEN is_hit = 1 THEN payout ELSE 0 END) AS payout
        FROM recent
        GROUP BY month, model_type, bet_type
        ORDER BY month DESC, model_type, bet_type
    """
    offset = f"-{months} months"
    rows = conn.execute(sql, {"offset": offset}).fetchall()

    results = []
    for r in rows:
        month, model_type, bet_type, bets, hits, invested, payout = r
        roi      = payout / invested if invested > 0 else 0.0
        hit_rate = hits / bets if bets > 0 else 0.0
        results.append({
            "month":      month,
            "model_type": model_type,
            "bet_type":   bet_type,
            "bets":       bets,
            "hits":       hits,
            "invested":   invested,
            "payout":     payout,
            "roi":        roi,
            "hit_rate":   hit_rate,
        })
    return results


def _print_monthly_roi(rows: list[dict[str, Any]]) -> None:
    print("=" * 72)
    print("【1. 月別 ROI レポート】")
    print("=" * 72)
    if not rows:
        print("  データなし")
        return

    # 月ごとに集計サマリーを先に出力
    from collections import defaultdict
    monthly: dict[str, dict[str, int | float]] = defaultdict(
        lambda: {"bets": 0, "hits": 0, "invested": 0, "payout": 0.0}
    )
    for r in rows:
        m = monthly[r["month"]]
        m["bets"]     += r["bets"]
        m["hits"]     += r["hits"]
        m["invested"]  = int(m["invested"]) + r["invested"]
        m["payout"]   += r["payout"]

    # ヘッダー
    print(f"{'月':^8} {'総投資':>10} {'総回収':>10} {'ROI':>7} {'的中率':>8} {'件数':>6}")
    print("-" * 55)
    for month in sorted(monthly.keys(), reverse=True):
        m = monthly[month]
        inv = m["invested"]
        pay = m["payout"]
        roi = pay / inv if inv > 0 else 0.0
        hr  = m["hits"] / m["bets"] if m["bets"] > 0 else 0.0
        flag = " <<" if roi >= 1.0 else ""
        print(
            f"{month:^8} {inv:>10,} {pay:>10,.0f} {roi:>7.3f} {hr:>8.1%} {m['bets']:>6}{flag}"
        )

    # 詳細（モデル別・券種別）
    print()
    print("  ── 詳細（モデル別・券種別）──")
    print(f"  {'月':^8} {'モデル':^6} {'券種':^6} {'投資':>8} {'回収':>8} {'ROI':>7} {'的中率':>8}")
    print("  " + "-" * 54)
    for r in rows:
        flag = " <<" if r["roi"] >= 1.0 else ""
        print(
            f"  {r['month']:^8} {r['model_type']:^6} {r['bet_type']:^6} "
            f"{r['invested']:>8,} {r['payout']:>8,.0f} "
            f"{r['roi']:>7.3f} {r['hit_rate']:>8.1%}{flag}"
        )


# ================================================================
# 2. 会場別的中率
# ================================================================

def _venue_stats(conn, months: int) -> list[dict[str, Any]]:
    sql = """
        SELECT
            substr(p.race_id, 5, 2)         AS venue_code,
            COUNT(*)                         AS bets,
            SUM(pr.is_hit)                   AS hits,
            SUM(p.recommended_bet)           AS invested,
            SUM(CASE WHEN pr.is_hit = 1 THEN pr.payout ELSE 0 END) AS payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.created_at >= date('now', :offset)
          AND p.recommended_bet > 0
        GROUP BY venue_code
        ORDER BY (SUM(CASE WHEN pr.is_hit = 1 THEN pr.payout ELSE 0 END) * 1.0
                  / NULLIF(SUM(p.recommended_bet), 0)) DESC
    """
    offset = f"-{months} months"
    rows = conn.execute(sql, {"offset": offset}).fetchall()

    results = []
    for r in rows:
        venue_code, bets, hits, invested, payout = r
        roi      = payout / invested if invested > 0 else 0.0
        hit_rate = hits / bets if bets > 0 else 0.0
        results.append({
            "venue":    _JYO.get(venue_code, venue_code),
            "bets":     bets,
            "hits":     hits,
            "invested": invested,
            "payout":   payout,
            "roi":      roi,
            "hit_rate": hit_rate,
        })
    return results


def _print_venue_stats(rows: list[dict[str, Any]]) -> None:
    print()
    print("=" * 72)
    print("【2. 会場別的中率（ROI 降順）】")
    print("=" * 72)
    if not rows:
        print("  データなし")
        return

    print(f"{'会場':^6} {'投資':>10} {'回収':>10} {'ROI':>7} {'的中率':>8} {'件数':>6}")
    print("-" * 50)
    for r in rows:
        flag = " <<" if r["roi"] >= 1.0 else ""
        print(
            f"{r['venue']:^6} {r['invested']:>10,} {r['payout']:>10,.0f} "
            f"{r['roi']:>7.3f} {r['hit_rate']:>8.1%} {r['bets']:>6}{flag}"
        )


# ================================================================
# 3. ケリー基準 資金増減シミュレーション
# ================================================================

def _kelly_simulation(conn, months: int, initial_bankroll: int = 1_000_000) -> dict[str, Any]:
    """
    1/10 ケリー基準で各レースに賭けた場合の資金推移をシミュレートする。

    predictions.recommended_bet をそのまま使用（モデルが算出した推奨額）。
    EV > 1.0 の予想のみを対象とする。

    Returns:
        {
            "final_bankroll": float,
            "total_return":   float,   # 最終/初期 -1
            "sharpe":         float,
            "max_drawdown":   float,   # ピークからの最大下落率
            "win_rate":       float,
            "monthly_returns": [(month, return_rate), ...]
        }
    """
    sql = """
        SELECT
            p.created_at,
            strftime('%Y-%m', p.created_at) AS month,
            pr.is_hit,
            pr.payout,
            p.recommended_bet,
            p.expected_value
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.created_at >= date('now', :offset)
          AND p.recommended_bet > 0
          AND p.expected_value >= 1.0
        ORDER BY p.created_at, pr.id
    """
    offset = f"-{months} months"
    rows = conn.execute(sql, {"offset": offset}).fetchall()

    if not rows:
        return {
            "final_bankroll": initial_bankroll,
            "total_return":   0.0,
            "sharpe":         0.0,
            "max_drawdown":   0.0,
            "win_rate":       0.0,
            "monthly_returns": [],
            "total_bets":     0,
        }

    bankroll = float(initial_bankroll)
    peak     = bankroll
    max_dd   = 0.0
    wins     = 0
    total    = 0

    # 月ごとに月初資金を記録し、月次損益を累積する
    monthly_start:  dict[str, float] = {}
    monthly_profit: dict[str, float] = {}

    for row in rows:
        _created_at, month, is_hit, payout, bet_amount, ev = row
        # 賭け金は recommended_bet を使用
        # ただし bankroll の 10% 超は制限（ドローダウン防止）
        actual_bet = min(float(bet_amount), bankroll * 0.10)
        if actual_bet <= 0 or bankroll <= 0:
            continue

        if month not in monthly_start:
            monthly_start[month]  = bankroll
            monthly_profit[month] = 0.0

        if is_hit:
            profit = payout - actual_bet
            wins += 1
        else:
            profit = -actual_bet

        bankroll                += profit
        monthly_profit[month]  += profit

        if bankroll > peak:
            peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

        total += 1

    # 月次リターン率 = 月次損益 / 月初資金
    monthly_returns: list[tuple[str, float]] = []
    for month in sorted(monthly_start.keys()):
        start = monthly_start[month]
        ret   = monthly_profit[month] / start if start > 0 else 0.0
        monthly_returns.append((month, ret))

    # シャープレシオ（月次リターンの平均/標準偏差 × √12）
    if len(monthly_returns) >= 2:
        rets = [r for _, r in monthly_returns]
        avg_r = sum(rets) / len(rets)
        var_r = sum((r - avg_r) ** 2 for r in rets) / len(rets)
        std_r = math.sqrt(var_r) if var_r > 0 else 0.0
        sharpe = (avg_r / std_r * math.sqrt(12)) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    return {
        "final_bankroll":  bankroll,
        "total_return":    (bankroll - initial_bankroll) / initial_bankroll,
        "sharpe":          sharpe,
        "max_drawdown":    max_dd,
        "win_rate":        wins / total if total > 0 else 0.0,
        "monthly_returns": monthly_returns,
        "total_bets":      total,
        "initial_bankroll": initial_bankroll,
    }


def _print_kelly_simulation(result: dict[str, Any]) -> None:
    print()
    print("=" * 72)
    print("【3. ケリー基準 資金増減シミュレーション（EV >= 1.0 対象）】")
    print("=" * 72)

    if result["total_bets"] == 0:
        print("  シミュレーション対象のベット履歴なし (EV >= 1.0 の予想が必要)")
        return

    init = result["initial_bankroll"]
    final = result["final_bankroll"]
    total_ret = result["total_return"]
    sign = "+" if total_ret >= 0 else ""

    print(f"  初期資金      : {init:>12,} 円")
    print(f"  最終資金      : {final:>12,.0f} 円")
    print(f"  総リターン    : {sign}{total_ret:>+.2%}")
    print(f"  シャープレシオ: {result['sharpe']:>8.3f}")
    print(f"  最大ドローダウン: {result['max_drawdown']:>6.1%}")
    print(f"  的中率        : {result['win_rate']:>8.1%}  ({result['total_bets']} ベット)")

    # 月次推移
    monthly = result["monthly_returns"]
    if monthly:
        print()
        print("  ── 月次資金推移 ──")
        print(f"  {'月':^8} {'月次リターン':>12}")
        print("  " + "-" * 22)
        for month, r in monthly:
            sign = "+" if r >= 0 else ""
            bar = "#" * min(int(abs(r) * 20), 20) if r != 0 else "."
            direction = "+" if r >= 0 else "-"
            print(f"  {month:^8} {sign}{r:>+.2%}  {direction}{bar}")


# ================================================================
# メイン
# ================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="月次 ROI レポート生成")
    parser.add_argument(
        "--months", type=int, default=3,
        help="集計対象期間（月数）。デフォルト: 3ヶ月",
    )
    parser.add_argument(
        "--bankroll", type=int, default=1_000_000,
        help="ケリーシミュレーションの初期資金（円）。デフォルト: 1,000,000",
    )
    args = parser.parse_args()

    conn = init_db()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    print()
    print(f"UMALOGI 月次 ROI レポート  (生成日時: {now_str})")
    print(f"集計期間: 直近 {args.months} ヶ月")

    monthly = _monthly_roi(conn, args.months)
    _print_monthly_roi(monthly)

    venue = _venue_stats(conn, args.months)
    _print_venue_stats(venue)

    kelly = _kelly_simulation(conn, args.months, args.bankroll)
    _print_kelly_simulation(kelly)

    conn.close()
    print()
    print("=" * 72)
    print("レポート生成完了")
    print("=" * 72)


if __name__ == "__main__":
    main()
