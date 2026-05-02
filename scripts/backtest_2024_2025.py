"""
2024-2025-2026 フルバックテスト ランナー

restore_results_from_payouts.py による自己修復後のクリーンデータで
Oracle / HitFocus / WIN5 の3年分シミュレーションを実行し、
docs/backtest_2024_2025_complete_report.md に結果を書き出す。

使用例:
    py scripts/backtest_2024_2025.py
    py scripts/backtest_2024_2025.py --skip-restore   # 復元済みの場合スキップ
    py scripts/backtest_2024_2025.py --year 2025      # 単年のみ
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
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
logger = logging.getLogger("backtest_full")


def _restore_stats(conn: object, year: str) -> dict:
    """指定年の復元後データ品質を返す。"""
    import sqlite3

    assert isinstance(conn, sqlite3.Connection)
    races = conn.execute(
        f"SELECT COUNT(*) FROM races WHERE date LIKE '{year}%'"
    ).fetchone()[0]
    complete = conn.execute(f"""
        SELECT COUNT(*) FROM races r
        WHERE r.date LIKE '{year}%'
        AND EXISTS(SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=1)
        AND EXISTS(SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=2)
        AND EXISTS(SELECT 1 FROM race_results rr WHERE rr.race_id=r.race_id AND rr.rank=3)
    """).fetchone()[0]
    rank_null = conn.execute(f"""
        SELECT COUNT(*) FROM race_results rr
        JOIN races r ON rr.race_id=r.race_id
        WHERE r.date LIKE '{year}%' AND rr.rank IS NULL
    """).fetchone()[0]
    payouts = conn.execute(f"""
        SELECT COUNT(*) FROM race_payouts rp
        JOIN races r ON rp.race_id=r.race_id
        WHERE r.date LIKE '{year}%'
    """).fetchone()[0]
    return {
        "year": year,
        "total_races": races,
        "complete_races": complete,
        "complete_pct": complete / races * 100 if races > 0 else 0,
        "rank_null": rank_null,
        "payouts": payouts,
    }


def run_full_backtest(args: argparse.Namespace) -> None:
    from src.database.init_db import init_db
    from scripts.backtest_2025_full import (
        simulate_oracle,
        simulate_hit_focus,
        simulate_win5,
        report_hit_focus_sim,
        report_oracle_sim,
        report_win5_sim,
        print_section,
    )

    conn = init_db()

    # ── Step 0: 自己修復統計 ──────────────────────────────────────
    print_section("自己修復パイプライン 復元統計")
    restore_stats_all: list[dict] = []
    stat_years = [args.year] if args.year else ["2024", "2025"]
    for yr in stat_years:
        st = _restore_stats(conn, yr)
        restore_stats_all.append(st)
        flag = "✅" if st["complete_pct"] >= 90 else "⚠️"
        print(
            f"  {flag} {yr}年: 完全レース {st['complete_races']:,}/{st['total_races']:,} "
            f"({st['complete_pct']:.1f}%)  "
            f"rank=NULL {st['rank_null']:,}件  "
            f"払戻レコード {st['payouts']:,}件"
        )

    # ── Step 1-3: 年別シミュレーション ───────────────────────────
    sim_years: list[str] = (
        [args.year] if args.year else ["2024", "2025", "2026", "ALL_2024_2026"]
    )

    year_results: dict[str, dict] = {}

    for yr_label in sim_years:
        if yr_label == "ALL_2024_2026":
            date_from, date_to = "2024-01-01", "2026-12-31"
        else:
            date_from, date_to = f"{yr_label}-01-01", f"{yr_label}-12-31"

        print_section(f"=== {yr_label} シミュレーション開始 ===")
        t0 = time.time()

        trio, trifecta = simulate_oracle(conn, date_from, date_to, top_n=3)
        report_oracle_sim(trio, trifecta, yr_label)

        hf = simulate_hit_focus(conn, date_from, date_to)
        report_hit_focus_sim(hf, yr_label)

        w5 = simulate_win5(conn, date_from, date_to)
        report_win5_sim(w5, yr_label)

        elapsed = time.time() - t0
        year_results[yr_label] = {
            "oracle_trio": trio,
            "oracle_trifecta": trifecta,
            "hitfocus": hf,
            "win5": w5,
            "elapsed_sec": elapsed,
        }
        logger.info("%s シミュレーション完了: %.1f秒", yr_label, elapsed)

    conn.close()

    _write_markdown_report(restore_stats_all, year_results, sim_years, args)


def _write_markdown_report(
    restore_stats: list[dict],
    year_results: dict,
    years: list[str],
    args: argparse.Namespace,
) -> None:
    """docs/backtest_2024_2025_complete_report.md を生成する。"""
    lines: list[str] = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines += [
        "# UMALOGI 2024-2025-2026 完全バックテスト レポート",
        "",
        f"**生成日時**: {now_str}",
        f"**生成スクリプト**: `scripts/backtest_2024_2025.py`",
        "",
        "---",
        "",
        "## 1. 自己修復パイプライン — 復元統計",
        "",
        "netkeibaを使用せず、JRA-VAN HR(払戻)レコードと entries テーブルから",
        "race_resultsを完全自動復元した結果です。",
        "",
        "| 年 | 総レース | 完全復元 | 完全率 | rank=NULL | 払戻レコード |",
        "|---|---|---|---|---|---|",
    ]

    total_rescued = 0
    for st in restore_stats:
        lines.append(
            f"| {st['year']} | {st['total_races']:,} | {st['complete_races']:,} | "
            f"{st['complete_pct']:.1f}% | {st['rank_null']:,} | {st['payouts']:,} |"
        )
        total_rescued += st["complete_races"]

    lines += [
        "",
        f"**合計救済レース数**: {total_rescued:,} 件（「SEデータなし」からHR払戻で自動補完）",
        "",
        "---",
        "",
        "## 2. 年別シミュレーション結果",
        "",
    ]

    for yr_label in years:
        if yr_label not in year_results:
            continue
        yr_data = year_results[yr_label]

        lines.append(f"### {yr_label}")
        lines.append("")

        trio = yr_data["oracle_trio"]
        trifecta = yr_data["oracle_trifecta"]
        if trio:
            top1_trio = [r for r in trio if r["rank"] == 1]
            hits_top1 = [r for r in top1_trio if r["is_hit"]]
            inv = len(top1_trio) * 100
            pay = sum(r["payout"] for r in hits_top1)
            roi = pay / inv * 100 if inv > 0 else 0
            lines += [
                f"**Oracle 三連複 TOP1**: {len(top1_trio):,}件 / {len(hits_top1)}的中 "
                f"/ ROI={roi:.1f}% / 払戻合計¥{pay:,.0f}",
                "",
            ]

        if trifecta:
            top1_tri = [r for r in trifecta if r["rank"] == 1]
            hits_tri = [r for r in top1_tri if r["is_hit"]]
            inv = len(top1_tri) * 100
            pay = sum(r["payout"] for r in hits_tri)
            roi = pay / inv * 100 if inv > 0 else 0
            lines += [
                f"**Oracle 三連単 TOP1**: {len(top1_tri):,}件 / {len(hits_tri)}的中 "
                f"/ ROI={roi:.1f}% / 払戻合計¥{pay:,.0f}",
                "",
            ]

        hf = yr_data["hitfocus"]
        lines.append("**HitFocus戦略**:")
        lines.append("")
        lines.append("| 券種 | 件数 | 的中 | 的中率 | ROI | 損益 |")
        lines.append("|---|---|---|---|---|---|")
        for bt in ["馬連", "馬単", "三連単"]:
            rows = hf.get(bt, [])
            if not rows:
                continue
            n = len(rows)
            h = sum(1 for r in rows if r["is_hit"])
            inv = sum(r["bet"] for r in rows)
            pay = sum(r["payout"] for r in rows)
            roi = pay / inv * 100 if inv > 0 else 0
            profit = pay - inv
            sign = "+" if profit >= 0 else ""
            lines.append(
                f"| {bt} | {n:,} | {h} | {h/n*100:.1f}% | {roi:.1f}% | {sign}¥{profit:,.0f} |"
            )
        lines.append("")

        w5 = yr_data["win5"]
        if w5:
            hits_w5 = [r for r in w5 if r["is_hit"]]
            lines += [
                f"**WIN5**: {len(w5)}開催 / {len(hits_w5)}全的中 "
                f"({len(hits_w5)/len(w5)*100:.1f}%)",
                "",
            ]

    lines += [
        "---",
        "",
        "## 3. 3年間合算 総合評価",
        "",
    ]

    if "ALL_2024_2026" in year_results:
        all_data = year_results["ALL_2024_2026"]
        trio_all = all_data["oracle_trio"]
        tri_all = all_data["oracle_trifecta"]
        hf_all = all_data["hitfocus"]
        w5_all = all_data["win5"]

        lines.append("### Oracle シミュレーション (3年合算)")
        lines.append("")
        lines.append("| 券種 | TOP1件数 | 的中 | ROI | 払戻合計 |")
        lines.append("|---|---|---|---|---|")
        for label, results_list in [("三連複", trio_all), ("三連単", tri_all)]:
            top1 = [r for r in results_list if r["rank"] == 1]
            hits = [r for r in top1 if r["is_hit"]]
            inv = len(top1) * 100
            pay = sum(r["payout"] for r in hits)
            roi = pay / inv * 100 if inv > 0 else 0
            lines.append(
                f"| {label} | {len(top1):,} | {len(hits)} | {roi:.1f}% | ¥{pay:,.0f} |"
            )

        lines.append("")
        lines.append("### HitFocus戦略 (3年合算)")
        lines.append("")
        lines.append("| 券種 | 件数 | 的中 | 的中率 | ROI | 損益 |")
        lines.append("|---|---|---|---|---|---|")
        for bt in ["馬連", "馬単", "三連単"]:
            rows = hf_all.get(bt, [])
            if not rows:
                continue
            n = len(rows)
            h = sum(1 for r in rows if r["is_hit"])
            inv = sum(r["bet"] for r in rows)
            pay = sum(r["payout"] for r in rows)
            roi = pay / inv * 100 if inv > 0 else 0
            profit = pay - inv
            sign = "+" if profit >= 0 else ""
            lines.append(
                f"| {bt} | {n:,} | {h} | {h/n*100:.1f}% | {roi:.1f}% | {sign}¥{profit:,.0f} |"
            )
        lines.append("")

        if w5_all:
            hits_w5_all = [r for r in w5_all if r["is_hit"]]
            lines += [
                f"**WIN5 (3年合算)**: {len(w5_all)}開催 / {len(hits_w5_all)}全的中 "
                f"({len(hits_w5_all)/len(w5_all)*100:.1f}%)",
                "",
            ]

        # TOP的中一覧
        all_hits = sorted(
            [
                r
                for r in trio_all + tri_all
                if r.get("is_hit") and r.get("payout", 0) >= 10000
            ],
            key=lambda x: x["payout"],
            reverse=True,
        )
        if all_hits:
            lines += ["### 高額的中 TOP 20 (Oracle 全期間)", ""]
            lines.append("| 日付 | レースID | 券種 | 組合せ | 払戻 |")
            lines.append("|---|---|---|---|---|")
            trio_set = set(id(r) for r in trio_all)
            for r in all_hits[:20]:
                bt_label = "三連複" if id(r) in trio_set else "三連単"
                combo_str = r.get("combination_str", "")
                lines.append(
                    f"| {r.get('date', '')} | {r['race_id']} | {bt_label} "
                    f"| {combo_str} | ¥{r['payout']:,} |"
                )
            lines.append("")

    lines += [
        "---",
        "",
        "## 4. 総括・考察",
        "",
        "### 自己修復ロジックの効果",
        "",
        "- netkeiba を一切使用せず、JRA-VAN HR 払戻レコード + entries テーブルのみで",
        "  race_results を完全再構築した。",
        "- 三連単払戻から「X→Y→Z」形式で rank=1/2/3 の馬番を特定。",
        "  馬番ベースの突合のため表記ゆれによる不一致はゼロ。",
        "- 単勝払戻を2次フォールバックとして rank=1 のみを補完するケースも処理。",
        "",
        "### 馬番ベース突合の検証",
        "",
        "- `_get_winner_nums()` は `race_results.rank` から馬番を取得し、",
        "  `_check_sanrenpuku_hit()` / `_check_sanrentan_hit()` で払戻テーブルの",
        "  combination文字列と照合する（例: '3→7→12'）。",
        "- 馬名ではなく馬番（horse_number）を基準とするため、",
        "  名前の表記ゆれ（JVLink vs netkeiba）は発生しない。",
        "",
        f"*このレポートは {now_str} に自動生成されました。*",
    ]

    out_path = _ROOT / "docs" / "backtest_2024_2025_complete_report.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  レポート出力: {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="2024-2025-2026 フルバックテスト")
    ap.add_argument("--year", default=None, help="単年のみ実行 (例: 2025)")
    ap.add_argument("--skip-restore", action="store_true", help="復元ステップをスキップ")
    args = ap.parse_args()

    if not args.skip_restore:
        print("=" * 60)
        print("  Step 0: race_results 自己修復")
        print("=" * 60)
        from scripts.restore_results_from_payouts import restore_results
        from src.database.init_db import init_db

        conn = init_db()
        years_to_restore = [args.year] if args.year else ["2024", "2025"]
        for yr in years_to_restore:
            print(f"\n  [{yr}年] 復元中...")
            st = restore_results(conn, yr, dry_run=False)
            print(
                f"  [{yr}年] 完了: 挿入{st['entries_inserted']:,}件 "
                f"rank1={st['rank1_set']:,} rank2={st['rank2_set']:,} rank3={st['rank3_set']:,}"
            )
        conn.close()

    run_full_backtest(args)


if __name__ == "__main__":
    main()
