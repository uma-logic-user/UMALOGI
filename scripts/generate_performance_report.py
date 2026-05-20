"""
scripts/generate_performance_report.py — 実績サマリーを自動集計して Discord へ通知

直近 N 日間の prediction_results を bet_type 別に集計し、
的中率・ROI・純利益を Markdown 表形式で生成して ab_test チャンネルへ送信する。

Usage:
    py scripts/generate_performance_report.py              # 直近28日
    py scripts/generate_performance_report.py --days 7     # 直近7日
    py scripts/generate_performance_report.py --dry-run    # Discord 送信なし
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def build_performance_report(conn: sqlite3.Connection, days: int = 28) -> str:
    """
    直近 days 日間の bet_type 別実績を集計し Markdown 文字列で返す。

    predictions テーブル (bet_type, recommended_bet) と
    prediction_results テーブル (is_hit, payout, recorded_at) を JOIN して集計する。

    集計項目:
      - 的中率 (hit_rate): is_hit=1 の割合
      - ROI: SUM(payout) / SUM(invested) * 100
      - 純利益: SUM(payout) - SUM(invested)
      - ベット数
    """
    sql = """
        WITH base AS (
            SELECT
                p.bet_type,
                COUNT(*) AS n_bets,
                SUM(COALESCE(r.is_hit, 0)) AS n_hits,
                SUM(COALESCE(r.payout, 0))            AS total_payout,
                SUM(COALESCE(p.recommended_bet, 0))   AS total_invest
            FROM prediction_results r
            JOIN predictions p ON r.prediction_id = p.id
            WHERE date(r.recorded_at) >= date('now', :offset)
            GROUP BY p.bet_type
        )
        SELECT
            bet_type,
            n_bets,
            n_hits,
            ROUND(CAST(n_hits AS REAL) / NULLIF(n_bets, 0) * 100, 1)   AS hit_rate,
            ROUND(total_payout / NULLIF(total_invest, 0) * 100, 1)      AS roi,
            ROUND(total_payout - total_invest, 0)                        AS net_profit,
            total_payout,
            total_invest
        FROM base
        ORDER BY roi DESC
    """
    rows = conn.execute(sql, {"offset": f"-{days} days"}).fetchall()

    lines = [
        f"## 📊 UMALOGI 実績サマリー（直近 {days} 日間）  {date.today().isoformat()}",
        "",
        "| 券種 | ベット数 | 的中数 | 的中率 | ROI | 純利益 |",
        "|------|---------|-------|-------|-----|-------|",
    ]
    total_invest_all = 0.0
    total_payout_all = 0.0

    for row in rows:
        bet_type, n_bets, n_hits, hit_rate, roi, net_profit, total_payout, total_invest = row
        hit_rate_s = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
        roi_s      = f"{roi:.1f}%" if roi is not None else "-"
        profit_s   = f"¥{int(net_profit):+,}" if net_profit is not None else "-"
        lines.append(f"| {bet_type} | {n_bets} | {n_hits} | {hit_rate_s} | {roi_s} | {profit_s} |")
        total_invest_all += total_invest or 0
        total_payout_all += total_payout or 0

    overall_roi = (total_payout_all / total_invest_all * 100) if total_invest_all > 0 else 0.0
    overall_profit = total_payout_all - total_invest_all
    lines += [
        "",
        f"**総合 ROI: {overall_roi:.1f}%  純利益: ¥{int(overall_profit):+,}**",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="実績サマリーを Discord へ通知")
    parser.add_argument("--days",    type=int, default=28, help="集計日数（デフォルト28日）")
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信なし")
    args = parser.parse_args()

    if not _DB_PATH.exists():
        print(f"[ERROR] DB が見つかりません: {_DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        report = build_performance_report(conn, days=args.days)
    finally:
        conn.close()

    print(report)

    if args.dry_run:
        print("\n[DRY-RUN] Discord 送信をスキップしました")
        return

    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    router.send_ab_report(report)
    print("\n✅ Discord 送信完了")


if __name__ == "__main__":
    main()
