"""
ALPHA モデル — 2年間バックテストスクリプト
==========================================

実行例:
  python scripts/alpha_backtest.py
  python scripts/alpha_backtest.py --train 2024 --test 2025 --bet-type 複勝
  python scripts/alpha_backtest.py --threshold 1.3 --sweep

時系列分割の厳格な実施:
  - 2024 学習 → 2025 テスト（順方向）
  - 2025 学習 → 2026 テスト（最新評価）
  - カンニング（リーク）なし: テストデータの結果を学習に使わない
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

import sqlite3
import pandas as pd
from src.database.init_db import init_db
from src.ml.alpha_model import (
    AlphaModel,
    AlphaBacktestResult,
    run_backtest,
    ALPHA_EV_THRESHOLD,
    BET_TYPE_TANSHO,
    BET_TYPE_FUKUSHO,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def print_report(results: list[AlphaBacktestResult]) -> None:
    """バックテスト結果をレポート形式で出力する。"""
    print("\n" + "=" * 65)
    print("  ALPHA モデル バックテストレポート")
    print("=" * 65)

    total_investment = sum(r.total_investment for r in results)
    total_payout = sum(r.total_payout for r in results)
    total_profit = total_payout - total_investment
    overall_roi = total_payout / total_investment * 100 if total_investment > 0 else 0

    for r in results:
        print(f"\n── {r.year}年 テスト ({r.bet_type}, EV>{r.ev_threshold:.1f}) ──")
        print(f"  買いシグナル : {r.num_bets:>6,} 件")
        print(f"  的中数       : {r.num_hits:>6,} 件 ({r.hit_rate:.1f}%)")
        print(f"  総投資額     : Y{r.total_investment:>10,}")
        print(f"  総払戻額     : Y{r.total_payout:>10,.0f}")
        print(f"  損益         : Y{r.profit:>+10,.0f}")
        print(f"  ROI          : {r.roi:>8.1f}%")
        print(f"  最大DD       : Y{r.max_drawdown:>10,.0f}")
        print(f"  最高払戻     : Y{r.best_single_payout:>10,.0f}")
        if r.notes:
            for note in r.notes:
                print(f"  NOTE: {note}")

    print("\n" + "─" * 65)
    print(f"  【通算】投資 Y{total_investment:,} → 払戻 Y{total_payout:,.0f}")
    print(f"         損益 Y{total_profit:+,.0f}   ROI {overall_roi:.1f}%")

    if overall_roi >= 110:
        print("\n  [OK] 年間 ROI 110% 超え達成！ALPHA モデル目標クリア")
    elif overall_roi >= 100:
        print("\n  [!!] ROI 100%超え（プラス収支）。目標 110% まで改善余地あり")
    else:
        print("\n  [NG] ROI 100%未満。モデル・戦略の見直しが必要")
    print("=" * 65)


def sweep_thresholds(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_years: list[int],
    bet_type: str,
) -> None:
    """EV 閾値を変えてスイープし、最適閾値を探す。"""
    print("\n=== EV閾値スイープ ===")
    print(f"{'閾値':>6} | {'件数':>6} | {'的中率':>7} | {'ROI':>8} | {'損益':>12}")
    print("-" * 50)

    best_roi = 0.0
    best_threshold = ALPHA_EV_THRESHOLD

    model = AlphaModel()
    all_train_df = model.load_training_data(conn, train_years, bet_type)

    test_df = model.load_training_data(conn, test_years, bet_type)
    if len(test_df) < 100:
        # ホールドアウトモード
        split_idx = int(len(all_train_df) * 0.80)
        train_df = all_train_df.iloc[:split_idx]
        test_df = all_train_df.iloc[split_idx:].copy()
        print(f"[警告] テストデータ不足 → {train_years}末尾20%({len(test_df)}行)を代用")
    else:
        train_df = all_train_df

    _ = model.train(train_df)

    test_df["ev_pred"] = model.predict_ev(test_df).values

    for threshold in [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8, 2.0, 2.5]:
        buy = test_df[test_df["ev_pred"] >= threshold]
        if len(buy) == 0:
            continue
        invest = len(buy) * 100  # 固定 Y100
        payout = float((buy["is_hit"] * buy["actual_payout"].fillna(0)).sum())
        roi = payout / invest * 100
        hit_r = buy["is_hit"].mean() * 100
        profit = payout - invest
        if roi > best_roi:
            best_roi = roi
            best_threshold = threshold
        print(f"  {threshold:>4.1f} | {len(buy):>6,} | {hit_r:>6.1f}% | {roi:>7.1f}% | Y{profit:>+10,.0f}")

    print(f"\n→ 最適閾値: {best_threshold:.1f} (ROI {best_roi:.1f}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description="ALPHA モデル バックテスト")
    parser.add_argument("--train", type=int, nargs="+", default=[2024],
                        help="学習年（デフォルト: 2024）")
    parser.add_argument("--test", type=int, nargs="+", default=[2025],
                        help="テスト年（デフォルト: 2025）")
    parser.add_argument("--bet-type", choices=["単勝", "複勝"], default="単勝",
                        help="馬券種（デフォルト: 単勝）")
    parser.add_argument("--threshold", type=float, default=ALPHA_EV_THRESHOLD,
                        help=f"EV閾値（デフォルト: {ALPHA_EV_THRESHOLD}）")
    parser.add_argument("--bankroll", type=int, default=100_000,
                        help="仮想資金（デフォルト: 100,000円）")
    parser.add_argument("--sweep", action="store_true",
                        help="EV閾値スイープで最適値を探す")
    parser.add_argument("--save", action="store_true",
                        help="学習済みモデルを保存する")
    parser.add_argument("--both-ways", action="store_true",
                        help="順逆両方向でバックテスト（2024→2025 and 2025→2024）")
    parser.add_argument("--research-db", default=None,
                        help="Research DB パス（netkeiba win_odds 補完用）")
    args = parser.parse_args()

    conn = init_db()
    research_db: Optional[Path] = None
    if args.research_db:
        p = Path(args.research_db)
        # 相対パスの場合はプロジェクトルート基準で解決
        if not p.is_absolute():
            p = _ROOT / p
        if p.exists():
            research_db = p
            print(f"[INFO] Research DB: {research_db}")
        else:
            logger.warning("Research DB が見つかりません: %s", p)


    # ── メインバックテスト ───────────────────────────────────────────
    results = []

    # 順方向: 2024 → 2025
    try:
        print(f"\n[バックテスト] 学習={args.train} テスト={args.test} 馬券={args.bet_type}")
        r = run_backtest(
            conn=conn,
            train_years=args.train,
            test_years=args.test,
            bet_type=args.bet_type,
            ev_threshold=args.threshold,
            bankroll=args.bankroll,
            verbose=True,
            research_db_path=research_db,
        )
        results.append(r)
    except ValueError as e:
        logger.error("バックテスト失敗: %s", e)

    # 逆方向（--both-ways 指定時）
    if args.both_ways and len(args.train) == 1 and len(args.test) == 1:
        rev_train = args.test
        rev_test = args.train
        print(f"\n[逆方向バックテスト] 学習={rev_train} テスト={rev_test}")
        try:
            r2 = run_backtest(
                conn=conn,
                train_years=rev_train,
                test_years=rev_test,
                bet_type=args.bet_type,
                ev_threshold=args.threshold,
                bankroll=args.bankroll,
                verbose=True,
                research_db_path=research_db,
            )
            results.append(r2)
        except ValueError as e:
            logger.error("逆方向バックテスト失敗: %s", e)

    # 単勝・複勝を両方テスト
    if args.bet_type == "単勝":
        print(f"\n[複勝バックテスト] 学習={args.train} テスト={args.test}")
        try:
            r3 = run_backtest(
                conn=conn,
                train_years=args.train,
                test_years=args.test,
                bet_type="複勝",
                ev_threshold=args.threshold,
                bankroll=args.bankroll,
                verbose=True,
                research_db_path=research_db,
            )
            results.append(r3)
        except ValueError as e:
            logger.error("複勝バックテスト失敗: %s", e)

    # ── 閾値スイープ ─────────────────────────────────────────────────
    if args.sweep:
        sweep_thresholds(conn, args.train, args.test, args.bet_type)

    # ── 総合レポート ─────────────────────────────────────────────────
    if results:
        print_report(results)

    # ── モデル保存 ───────────────────────────────────────────────────
    if args.save:
        model = AlphaModel()
        train_df = model.load_training_data(
            conn, args.train, args.bet_type, research_db_path=research_db
        )
        model.train(train_df)
        model.save()
        print(f"\nモデル保存完了: data/models/alpha/alpha_model.pkl")

    conn.close()


if __name__ == "__main__":
    main()
