"""
scripts/generate_ab_report.py — V1 vs V2 週次 A/B テスト成績比較レポート生成

Usage:
    py scripts/generate_ab_report.py              # 直近7日
    py scripts/generate_ab_report.py --days 28
    py scripts/generate_ab_report.py --dry-run    # Discord 送信なし
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


def build_ab_report(conn: sqlite3.Connection, days: int = 7) -> str:
    """
    V1 vs V2 週次 A/B 成績比較 Markdown を生成して返す。

    prediction_results テーブルの model_version カラムで V1/V2 を識別する。
    model_version が NULL の行は v1 として扱う（後方互換）。
    """
    sql = """
        WITH base AS (
            SELECT
                COALESCE(model_version, 'v1') AS ver,
                bet_type,
                COUNT(*) AS n_bets,
                SUM(is_hit) AS n_hits,
                SUM(COALESCE(payout, 0))   AS total_payout,
                SUM(COALESCE(invested, 0)) AS total_invest
            FROM prediction_results
            WHERE date(created_at) >= date('now', :offset)
            GROUP BY ver, bet_type
        )
        SELECT
            ver,
            bet_type,
            n_bets,
            n_hits,
            ROUND(CAST(n_hits AS REAL) / NULLIF(n_bets, 0) * 100, 1) AS hit_rate,
            ROUND(total_payout / NULLIF(total_invest, 0) * 100, 1)   AS roi
        FROM base
        ORDER BY ver, roi DESC
    """
    rows = conn.execute(sql, {"offset": f"-{days} days"}).fetchall()

    lines = [
        f"## 📊 V1 vs V2 A/B テストレポート（直近 {days} 日）  {date.today().isoformat()}",
        "",
        "| バージョン | 券種 | ベット数 | 的中率 | ROI |",
        "|-----------|------|---------|-------|-----|",
    ]
    for ver, bet_type, n_bets, n_hits, hit_rate, roi in rows:
        hr_s  = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
        roi_s = f"{roi:.1f}%" if roi is not None else "-"
        lines.append(f"| {ver.upper()} | {bet_type} | {n_bets} | {hr_s} | {roi_s} |")

    def _overall_roi(ver: str) -> float:
        r = conn.execute(
            """
            SELECT SUM(COALESCE(payout,0)), SUM(COALESCE(invested,0))
            FROM prediction_results
            WHERE COALESCE(model_version,'v1') = ? AND date(created_at) >= date('now', ?)
            """,
            (ver, f"-{days} days"),
        ).fetchone()
        payout, invest = (r[0] or 0.0), (r[1] or 0.0)
        return (payout / invest * 100) if invest > 0 else 0.0

    roi_v1 = _overall_roi("v1")
    roi_v2 = _overall_roi("v2")
    winner = "🏆 **V2 優勢**" if roi_v2 > roi_v1 else "📌 V1 優勢（V2 改善余地あり）"
    lines += [
        "",
        f"**V1 総合 ROI: {roi_v1:.1f}%** vs **V2 総合 ROI: {roi_v2:.1f}%**  →  {winner}",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="V1 vs V2 A/B テスト比較レポートを Discord へ送信")
    parser.add_argument("--days",    type=int, default=7, help="集計日数（デフォルト7日）")
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信なし")
    args = parser.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        report = build_ab_report(conn, days=args.days)
    finally:
        conn.close()

    print(report)
    if args.dry_run:
        print("\n[DRY-RUN] Discord 送信スキップ")
        return

    from src.notification.router import NotificationRouter
    NotificationRouter().send_ab_report(report)
    print("\n✅ Discord 送信完了")


if __name__ == "__main__":
    main()
