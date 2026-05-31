"""
Alpha-Payout ウォークフォワードバックテスト
==========================================

複勝ペイアウト直接回帰（AlphaPayoutModel）による全カ年通算 ROI 検証。

設計哲学:
  目的変数 = ev_target_place = actual_place_payout / 100
  LightGBM Huber 損失 + Optuna ROI 直接最大化 + nb_win_odds 完全統合

実行例:
    python scripts/run_alpha_payout.py
    python scripts/run_alpha_payout.py --trials 50
    python scripts/run_alpha_payout.py --no-research-db --ev-threshold 1.5
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from src.ml.alpha_payout_model import (
    PayoutBacktestResult,
    run_payout_backtest,
)

_DB_PATH = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"

# 旧 ALPHA 複勝 ROI（ベースライン: is_place バイナリ最良結果）
_BASELINE_ROI = 96.8


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _sep(char: str = "=", width: int = 72) -> None:
    print(char * width, flush=True)


def _check_year_data(conn: sqlite3.Connection, year: int) -> int:
    """指定年の race_results 行数を返す。"""
    cur = conn.execute(
        "SELECT COUNT(*) FROM race_results rr "
        "JOIN races r ON rr.race_id = r.race_id "
        "WHERE strftime('%Y', r.date) = ?",
        (str(year),),
    )
    return cur.fetchone()[0]


def _aggregate(results: list[PayoutBacktestResult]) -> tuple[float, float, float]:
    inv = sum(r.total_investment for r in results)
    pay = sum(r.total_payout for r in results)
    roi = pay / inv * 100 if inv > 0 else 0.0
    return inv, pay, roi


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpha-Payout ウォークフォワードバックテスト"
    )
    parser.add_argument("--trials", type=int, default=30, help="Optuna トライアル数")
    parser.add_argument(
        "--no-research-db", action="store_true", help="netkeiba_research.db を使わない"
    )
    parser.add_argument(
        "--ev-threshold",
        type=float,
        default=None,
        help="固定EV閾値（省略時: 学習データで自動最適化）",
    )
    parser.add_argument(
        "--all-folds",
        action="store_true",
        help="3フォールド実行（デフォルト: 2フォールド）",
    )
    args = parser.parse_args()

    research_db: Path | None = None
    if not args.no_research_db and _RESEARCH_DB.exists():
        research_db = _RESEARCH_DB
        print(f"[INFO] research DB 使用: {research_db}", flush=True)
    else:
        print("[INFO] research DB なし — JVLink win_odds のみ使用", flush=True)

    conn = _connect()

    # ── 利用可能年次チェック ────────────────────────────────────────────
    available_years: list[int] = []
    for y in range(2021, 2028):
        n = _check_year_data(conn, y)
        if n > 0:
            available_years.append(y)
            print(f"  {y}: {n:,}行 ✓", flush=True)
        else:
            print(f"  {y}: データなし", flush=True)

    if len(available_years) < 2:
        print("[ERROR] ウォークフォワードには最低2年分のデータが必要です。", flush=True)
        conn.close()
        sys.exit(1)

    # ── ウォークフォワード定義 ──────────────────────────────────────────
    # 基本: 直近2フォールド (n-1年→n年)
    # --all-folds: 全年次ペア + 複数年学習フォールドを追加
    walk_forward_pairs: list[tuple[list[int], list[int]]] = []

    for i in range(len(available_years) - 1):
        train = [available_years[i]]
        test = [available_years[i + 1]]
        walk_forward_pairs.append((train, test))

    if args.all_folds and len(available_years) >= 3:
        # 複数年学習フォールドを末尾に追加
        last = available_years[-1]
        prev_two = available_years[-3:-1]
        walk_forward_pairs.append((prev_two, [last]))

    _sep()
    print("  Alpha-Payout ウォークフォワードバックテスト（複勝ペイアウト回帰）")
    print(
        f"  Optuna {args.trials}試行  /  EV閾値: {'自動最適化' if args.ev_threshold is None else f'固定>{args.ev_threshold:.2f}'}"
    )
    print(f"  フォールド数: {len(walk_forward_pairs)}")
    print(f"  ベースライン（旧 ALPHA 複勝）: {_BASELINE_ROI:.1f}%")
    _sep()

    all_results: list[PayoutBacktestResult] = []

    for train_years, test_years in walk_forward_pairs:
        print(f"\n{'─' * 72}", flush=True)
        print(f"  [WF] 学習: {train_years}  →  テスト: {test_years}", flush=True)
        print(f"{'─' * 72}", flush=True)

        try:
            result = run_payout_backtest(
                conn=conn,
                train_years=train_years,
                test_years=test_years,
                n_optuna_trials=args.trials,
                bankroll=100_000,
                verbose=True,
                research_db_path=research_db,
                ev_threshold=args.ev_threshold,
            )
            all_results.append(result)
        except Exception as exc:
            print(f"\n  [ERROR] 年次 {test_years}: {exc}", flush=True)
            import traceback

            traceback.print_exc()
            continue

    conn.close()

    if not all_results:
        print("\n[ERROR] 有効なバックテスト結果がありません。", flush=True)
        sys.exit(1)

    # ── 年別レポート ────────────────────────────────────────────────────
    _sep()
    print("  Alpha-Payout ウォークフォワード 年別レポート")
    _sep()

    header = (
        f"  {'年度':<10} {'買い点':>8} {'的中率':>8} {'投資':>14} {'払戻':>14} "
        f"{'ROI':>8} {'閾値':>7} {'MaxDD':>12}"
    )
    print(header, flush=True)
    print("  " + "─" * (len(header) - 2), flush=True)

    for r in all_results:
        mark = "✅" if r.roi >= 110 else ("⚠️ " if r.roi >= 100 else "❌")
        diff = r.roi - _BASELINE_ROI
        diff_str = f"({diff:+.1f}pp)" if diff != 0 else ""
        print(
            f"  {r.year!s:<10} "
            f"{r.num_bets:>8,} "
            f"{r.hit_rate:>7.1f}% "
            f"¥{r.total_investment:>12,} "
            f"¥{r.total_payout:>12,.0f} "
            f"{r.roi:>7.1f}% {mark} {diff_str}"
            f"  ev>{r.ev_threshold:.2f}"
            f"  DD¥{r.max_drawdown:>9,.0f}",
            flush=True,
        )

    # ── 通算サマリー ────────────────────────────────────────────────────
    total_inv, total_pay, total_roi = _aggregate(all_results)
    years_above_110 = sum(1 for r in all_results if r.roi >= 110)
    years_above_100 = sum(1 for r in all_results if r.roi >= 100)
    diff_vs_baseline = total_roi - _BASELINE_ROI

    _sep()
    print("  通算サマリー（vs 旧 ALPHA ベースライン）")
    _sep()

    mark = "✅" if total_roi >= 110 else ("⚠️ " if total_roi >= 100 else "❌")
    print(
        f"\n  複勝 通算ROI = {total_roi:.1f}% {mark}"
        f"  ({diff_vs_baseline:+.1f}pp vs ベースライン {_BASELINE_ROI:.1f}%)"
    )
    print(
        f"  ROI≥110%: {years_above_110}/{len(all_results)}年  "
        f"ROI≥100%: {years_above_100}/{len(all_results)}年"
    )
    print(
        f"  総投資: ¥{total_inv:,}  総払戻: ¥{total_pay:,.0f}  損益: ¥{total_pay - total_inv:+,.0f}"
    )

    # ── 推奨判定 ────────────────────────────────────────────────────────
    _sep()
    print("  Alpha-Payout 採用判定")
    _sep()

    if total_roi >= 110 and diff_vs_baseline > 0:
        print(
            f"\n  【採用】ROI={total_roi:.1f}% ≥ 110% かつ 旧ALPHA比 {diff_vs_baseline:+.1f}pp ✅"
        )
        print("\n  次のアクション:")
        print("   1. weekend_batch.py に AlphaPayoutModel を統合")
        print("   2. prediction_results テーブルで実績追跡を開始")
        print("   3. 月次で EV 閾値を再最適化")
    elif total_roi >= 100 and diff_vs_baseline > 0:
        print(
            f"\n  【条件付き採用】ROI={total_roi:.1f}% ≥ 100%  旧ALPHA比 {diff_vs_baseline:+.1f}pp ⚠️"
        )
        print(f"  目標ROI 110% まで残り {110 - total_roi:.1f}pp。")
        print("  EV 閾値を上げるか --trials を増やして精度向上を試みてください。")
    elif total_roi >= 100:
        print(
            f"\n  【条件付き採用】ROI={total_roi:.1f}% ≥ 100%  ただしベースライン比 {diff_vs_baseline:+.1f}pp ⚠️"
        )
        print("  旧 ALPHA を下回っているため改善が必要です。")
    else:
        print(f"\n  【不採用】ROI={total_roi:.1f}% < 100% ❌")
        print(f"  旧 ALPHA 比 {diff_vs_baseline:+.1f}pp。さらなる改善が必要。")
        print("\n  改善候補:")
        print("   - --trials 50 以上で Optuna を増やす")
        print("   - 特徴量追加: 近走複勝率・調教師複勝率・血統複勝適性")
        print("   - --all-folds で複数年学習フォールドを追加")
        print("   - EV 閾値を EV>1.5 以上に絞り込む")

    _sep()


if __name__ == "__main__":
    main()
