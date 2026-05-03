"""
2024-2025年 バックテスト マークダウンレポート生成

使用例:
    python scripts/generate_backtest_report.py
    python scripts/generate_backtest_report.py --out docs/backtest_2024_2025_report.md
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ─────────────────────────────────────────────────────────────────
# 統計
# ─────────────────────────────────────────────────────────────────

class Stats(NamedTuple):
    n_bets:   int
    n_hits:   int
    hit_rate: float
    invested: float
    payout:   float
    profit:   float
    roi:      float
    max_hit:  float


def _stats(rows: list[dict]) -> Stats:
    n        = len(rows)
    hits     = sum(1 for r in rows if r["is_hit"])
    invested = sum(r["invested"] or 100 for r in rows)
    payout   = sum(r["payout"]   or 0   for r in rows)
    profit   = payout - invested
    roi      = payout / invested * 100 if invested > 0 else 0.0
    max_hit  = max((r["payout"] or 0 for r in rows), default=0)
    return Stats(
        n_bets=n, n_hits=hits,
        hit_rate=hits / n * 100 if n > 0 else 0.0,
        invested=invested, payout=payout,
        profit=profit, roi=roi, max_hit=max_hit,
    )


def _roi_emoji(roi: float) -> str:
    if roi >= 120: return "🏆"
    if roi >= 100: return "🟢"
    if roi >= 75:  return "🟡"
    return "🔴"


# ─────────────────────────────────────────────────────────────────
# データ読み込み
# ─────────────────────────────────────────────────────────────────

def load_rows(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[dict]:
    sql = """
    SELECT
        p.model_type,
        p.bet_type,
        r.date                       AS race_date,
        r.venue,
        r.surface,
        r.distance,
        COALESCE(p.recommended_bet, 100) AS invested,
        COALESCE(pr.payout, 0)       AS payout,
        COALESCE(pr.is_hit, 0)       AS is_hit,
        p.combination_json
    FROM predictions p
    JOIN races r              ON p.race_id = r.race_id
    LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
    WHERE r.date BETWEEN ? AND ?
      AND pr.id IS NOT NULL
      AND p.bet_type NOT IN ('馬分析', 'WIN5')
    ORDER BY r.date, p.model_type, p.bet_type
    """
    cur = conn.execute(sql, (date_from, date_to))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def data_quality(conn: sqlite3.Connection, year: int) -> dict:
    cur = conn.cursor()
    prefix = f"{year}%"
    cur.execute("SELECT COUNT(*) FROM races WHERE date LIKE ?", (prefix,))
    n_races = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM race_results WHERE race_id LIKE ? AND rank=1", (prefix,))
    n_rank1 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM race_results WHERE race_id LIKE ? AND rank=2", (prefix,))
    n_rank2 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM race_payouts WHERE race_id LIKE ?", (prefix,))
    n_pay = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM predictions p JOIN races r ON p.race_id=r.race_id WHERE r.date LIKE ?", (prefix,))
    n_pred = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM prediction_results pr
        JOIN predictions p ON p.id=pr.prediction_id
        JOIN races r ON p.race_id=r.race_id
        WHERE r.date LIKE ?
    """, (prefix,))
    n_res = cur.fetchone()[0]
    return {
        "year": year,
        "races": n_races,
        "rank1": n_rank1,
        "rank2": n_rank2,
        "payouts": n_pay,
        "predictions": n_pred,
        "results": n_res,
    }


# ─────────────────────────────────────────────────────────────────
# マークダウン生成
# ─────────────────────────────────────────────────────────────────

BET_ORDER = ["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"]
MODEL_ORDER = ["卍(暫定)", "卍(直前)", "本命(暫定)", "本命(直前)", "Oracle(暫定)", "Oracle(直前)"]


def generate_report(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    out_path: Path,
) -> None:
    rows = load_rows(conn, date_from, date_to)
    q24  = data_quality(conn, 2024)
    q25  = data_quality(conn, 2025)

    lines: list[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines += [
        f"# UMALOGI バックテストレポート 2024-2025",
        f"",
        f"> 生成日時: {now}  ",
        f"> 対象期間: {date_from} 〜 {date_to}",
        f"",
        "---",
        "",
    ]

    # ── データ品質 ────────────────────────────────
    lines += [
        "## データ品質",
        "",
        "| 項目 | 2024年 | 2025年 |",
        "|------|--------|--------|",
        f"| レース数 | {q24['races']:,} | {q25['races']:,} |",
        f"| rank=1（勝ち馬）| {q24['rank1']:,} | {q25['rank1']:,} |",
        f"| rank=2（2着）| {q24['rank2']:,} | {q25['rank2']:,} |",
        f"| 払戻データ件数 | {q24['payouts']:,} | {q25['payouts']:,} |",
        f"| 予想件数 | {q24['predictions']:,} | {q25['predictions']:,} |",
        f"| 精算済み件数 | {q24['results']:,} | {q25['results']:,} |",
        "",
    ]

    if not rows:
        lines += [
            "> ⚠️ 対象期間の prediction_results が0件です。",
            "> `simulate_year.py` でシミュレーションを実行してから再実行してください。",
            "",
        ]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"レポート出力: {out_path}")
        return

    # ── モデル×券種 集計 ─────────────────────────
    by_model_bet: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (r["model_type"], r["bet_type"])
        by_model_bet[key].append(r)

    # モデル×券種のサマリー
    lines += [
        "## モデル別・券種別 成績",
        "",
        "| モデル | 券種 | 予想 | 的中 | 的中率 | 投資額 | 払戻額 | 損益 | ROI | 最大払戻 |",
        "|--------|------|-----:|-----:|-------:|-------:|-------:|-----:|----:|---------:|",
    ]

    all_model_stats: dict[str, list] = defaultdict(list)
    for model in MODEL_ORDER:
        for bet in BET_ORDER:
            key = (model, bet)
            if key not in by_model_bet:
                continue
            s = _stats(by_model_bet[key])
            emoji = _roi_emoji(s.roi)
            sign = "+" if s.profit >= 0 else ""
            lines.append(
                f"| {emoji} {model} | {bet} | {s.n_bets:,} | {s.n_hits:,} | "
                f"{s.hit_rate:.1f}% | ¥{s.invested:,.0f} | ¥{s.payout:,.0f} | "
                f"{sign}¥{s.profit:,.0f} | **{s.roi:.1f}%** | ¥{s.max_hit:,.0f} |"
            )
            all_model_stats[model].append(s)

    # ── モデル合計 ───────────────────────────────
    lines += [
        "",
        "## モデル合計",
        "",
        "| モデル | 予想合計 | 的中 | 的中率 | 投資計 | 払戻計 | 損益 | ROI |",
        "|--------|---------:|-----:|-------:|-------:|-------:|-----:|----:|",
    ]

    grand_rows: list[dict] = []
    for model in MODEL_ORDER:
        model_rows = [r for r in rows if r["model_type"] == model]
        if not model_rows:
            continue
        s = _stats(model_rows)
        emoji = _roi_emoji(s.roi)
        sign = "+" if s.profit >= 0 else ""
        lines.append(
            f"| {emoji} {model} | {s.n_bets:,} | {s.n_hits:,} | {s.hit_rate:.1f}% | "
            f"¥{s.invested:,.0f} | ¥{s.payout:,.0f} | {sign}¥{s.profit:,.0f} | **{s.roi:.1f}%** |"
        )
        grand_rows.extend(model_rows)

    if grand_rows:
        s_all = _stats(grand_rows)
        sign = "+" if s_all.profit >= 0 else ""
        lines += [
            "",
            f"**全モデル合計**: {s_all.n_bets:,}件 / 的中{s_all.n_hits:,}件 / "
            f"ROI **{s_all.roi:.1f}%** / 損益 {sign}¥{s_all.profit:,.0f}",
        ]

    # ── 年別サマリー ─────────────────────────────
    for yr in [2024, 2025]:
        yr_rows = [r for r in rows if r["race_date"].startswith(str(yr))]
        if not yr_rows:
            continue
        s = _stats(yr_rows)
        emoji = _roi_emoji(s.roi)
        sign = "+" if s.profit >= 0 else ""
        lines += [
            "",
            f"### {yr}年 サマリー",
            "",
            f"- 予想件数: **{s.n_bets:,}件**",
            f"- 的中数: **{s.n_hits:,}件** ({s.hit_rate:.1f}%)",
            f"- 投資額: ¥{s.invested:,.0f}",
            f"- 払戻額: ¥{s.payout:,.0f}",
            f"- 損益: {sign}¥{s.profit:,.0f}",
            f"- ROI: {emoji} **{s.roi:.1f}%**",
            f"- 最大的中: ¥{s.max_hit:,.0f}",
        ]

    # ── 高額的中 ─────────────────────────────────
    big_hits = sorted(
        [r for r in rows if r["is_hit"] and r["payout"] >= 10000],
        key=lambda r: r["payout"], reverse=True
    )[:20]

    if big_hits:
        lines += [
            "",
            "## 高額的中トップ20（¥10,000以上）",
            "",
            "| 日付 | 場所 | モデル | 券種 | 払戻 |",
            "|------|------|--------|------|-----:|",
        ]
        for r in big_hits:
            lines.append(
                f"| {r['race_date']} | {r['venue']} | {r['model_type']} | "
                f"{r['bet_type']} | ¥{r['payout']:,.0f} |"
            )

    # ── ROI TOP ─────────────────────────────────
    lines += [
        "",
        "## ROI ランキング（最低10件以上）",
        "",
        "| # | モデル | 券種 | 件数 | ROI | 損益 |",
        "|---|--------|------|-----:|----:|-----:|",
    ]

    ranked: list[tuple[float, str, str, Stats]] = []
    for (model, bet), r_list in by_model_bet.items():
        s = _stats(r_list)
        if s.n_bets >= 10:
            ranked.append((s.roi, model, bet, s))
    ranked.sort(key=lambda x: x[0], reverse=True)
    for i, (roi, model, bet, s) in enumerate(ranked[:15], 1):
        emoji = _roi_emoji(roi)
        sign = "+" if s.profit >= 0 else ""
        lines.append(
            f"| {i} | {emoji} {model} | {bet} | {s.n_bets:,} | **{roi:.1f}%** | {sign}¥{s.profit:,.0f} |"
        )

    lines += ["", "---", "", f"*Generated by UMALOGI backtest engine at {now}*", ""]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"レポート出力: {out_path} ({len(rows):,}件のデータを集計)")


# ─────────────────────────────────────────────────────────────────
# エントリポイント
# ─────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date-from", default="2024-01-01")
    ap.add_argument("--date-to",   default="2025-12-31")
    ap.add_argument("--out", default="docs/backtest_2024_2025_report.md")
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()
    try:
        generate_report(conn, args.date_from, args.date_to, Path(args.out))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
