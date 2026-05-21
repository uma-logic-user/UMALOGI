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


def _ver_expr() -> str:
    """model_type から V1/V2 を判定する CASE 式（再利用）。"""
    return "CASE WHEN p.model_type LIKE '%V2%' OR p.model_type LIKE '%v2%' THEN 'v2' ELSE 'v1' END"


def _summary_row(conn: sqlite3.Connection, ver: str, days: int) -> dict:
    """ver ('v1'|'v2') の集計行を返す。"""
    sql = f"""
        SELECT
            COUNT(DISTINCT p.race_id)                            AS n_races,
            COUNT(*)                                              AS n_bets,
            SUM(pr.is_hit)                                        AS n_hits,
            SUM(COALESCE(pr.payout, 0))                           AS total_payout,
            SUM(COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0)) AS total_invest,
            AVG(
                ABS(
                    COALESCE(p.expected_value, 0) -
                    CASE
                        WHEN (COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0)) > 0
                        THEN COALESCE(pr.payout, 0) /
                             (COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0))
                        ELSE 0.0
                    END
                )
            )                                                     AS ev_mae
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE ({_ver_expr()}) = ?
          AND date(pr.recorded_at) >= date('now', ?)
    """
    row = conn.execute(sql, (ver, f"-{days} days")).fetchone()
    n_races, n_bets, n_hits, total_payout, total_invest, ev_mae = row
    n_bets = n_bets or 0
    n_hits = n_hits or 0
    total_payout = total_payout or 0.0
    total_invest = total_invest or 0.0
    hit_rate = (n_hits / n_bets * 100) if n_bets else 0.0
    roi = (total_payout / total_invest * 100) if total_invest > 0 else 0.0
    net_profit = total_payout - total_invest
    return {
        "n_races": n_races or 0,
        "n_bets": n_bets,
        "n_hits": n_hits,
        "hit_rate": hit_rate,
        "roi": roi,
        "net_profit": net_profit,
        "ev_mae": ev_mae or 0.0,
    }


def _detail_rows(conn: sqlite3.Connection, days: int) -> list[tuple]:
    """券種別 V1/V2 明細行を返す。"""
    sql = f"""
        WITH base AS (
            SELECT
                ({_ver_expr()})                                       AS ver,
                p.bet_type,
                COUNT(*)                                              AS n_bets,
                SUM(pr.is_hit)                                        AS n_hits,
                SUM(COALESCE(pr.payout, 0))                           AS total_payout,
                SUM(COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0)) AS total_invest
            FROM prediction_results pr
            JOIN predictions p ON p.id = pr.prediction_id
            WHERE date(pr.recorded_at) >= date('now', ?)
            GROUP BY ver, p.bet_type
        )
        SELECT
            ver,
            bet_type,
            n_bets,
            n_hits,
            ROUND(CAST(n_hits AS REAL) / NULLIF(n_bets, 0) * 100, 1) AS hit_rate,
            ROUND((total_payout - total_invest), 0)                   AS net_profit,
            ROUND(total_payout / NULLIF(total_invest, 0) * 100, 1)   AS roi
        FROM base
        ORDER BY ver, roi DESC
    """
    return conn.execute(sql, (f"-{days} days",)).fetchall()


def build_ab_report(conn: sqlite3.Connection, days: int = 7) -> str:
    """
    V1 vs V2 週次 A/B 成績比較 Markdown を生成して返す。

    比較項目:
    - 対象レース数・ベット数・的中率・ROI・純利益・EV乖離精度(MAE)

    EV乖離精度 = |predictions.expected_value - actual_ev| の平均
    actual_ev  = payout / invested (的中時), 0 (外れ時)
    """
    today = date.today().isoformat()
    v1 = _summary_row(conn, "v1", days)
    v2 = _summary_row(conn, "v2", days)

    def _fmt_sign(v: float, fmt: str = "+.1f") -> str:
        return f"{v:{fmt}}" if v >= 0 else f"{v:.1f}"

    def _winner_badge(v1_val: float, v2_val: float, higher_is_better: bool = True) -> str:
        if higher_is_better:
            return "🔵 V2" if v2_val > v1_val else "🟠 V1"
        return "🔵 V2" if v2_val < v1_val else "🟠 V1"

    lines: list[str] = [
        f"## 📊 V1 vs V2 A/B テストレポート（直近 {days} 日） {today}",
        "",
        "### 🔢 総合サマリー",
        "| 指標 | V1 | V2 | 差分 | 優勢 |",
        "|-----|-----|-----|-----|------|",
        f"| 対象レース数 | {v1['n_races']} | {v2['n_races']} | {v2['n_races'] - v1['n_races']:+d} | — |",
        f"| ベット数 | {v1['n_bets']} | {v2['n_bets']} | {v2['n_bets'] - v1['n_bets']:+d} | — |",
        f"| 的中率 | {v1['hit_rate']:.1f}% | {v2['hit_rate']:.1f}% | {v2['hit_rate']-v1['hit_rate']:+.1f}% | {_winner_badge(v1['hit_rate'], v2['hit_rate'])} |",
        f"| 回収率 (ROI) | {v1['roi']:.1f}% | {v2['roi']:.1f}% | {v2['roi']-v1['roi']:+.1f}% | {_winner_badge(v1['roi'], v2['roi'])} |",
        f"| 純利益 | ¥{v1['net_profit']:,.0f} | ¥{v2['net_profit']:,.0f} | ¥{v2['net_profit']-v1['net_profit']:+,.0f} | {_winner_badge(v1['net_profit'], v2['net_profit'])} |",
        f"| EV乖離 (MAE) | {v1['ev_mae']:.3f} | {v2['ev_mae']:.3f} | {v2['ev_mae']-v1['ev_mae']:+.3f} | {_winner_badge(v1['ev_mae'], v2['ev_mae'], higher_is_better=False)} |",
        "",
    ]

    detail = _detail_rows(conn, days)
    if detail:
        lines += [
            "### 🗂 券種別詳細",
            "| バージョン | 券種 | ベット数 | 的中率 | 純利益 | ROI |",
            "|-----------|------|---------|-------|-------|-----|",
        ]
        for ver, bet_type, n_bets, n_hits, hit_rate, net_profit, roi in detail:
            hr_s  = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
            roi_s = f"{roi:.1f}%" if roi is not None else "-"
            np_s  = f"¥{net_profit:,.0f}" if net_profit is not None else "-"
            lines.append(f"| {ver.upper()} | {bet_type} | {n_bets} | {hr_s} | {np_s} | {roi_s} |")
        lines.append("")

    if v2["roi"] > v1["roi"]:
        verdict = f"🏆 **V2 優勢** (ROI +{v2['roi']-v1['roi']:.1f}pt)"
    elif v1["roi"] > v2["roi"]:
        verdict = f"📌 **V1 優勢** — V2 に改善余地あり (ROI −{v1['roi']-v2['roi']:.1f}pt)"
    else:
        verdict = "⚖️ V1/V2 同等"

    lines += [
        f"**V1 総合 ROI: {v1['roi']:.1f}%** vs **V2 総合 ROI: {v2['roi']:.1f}%**  →  {verdict}",
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
