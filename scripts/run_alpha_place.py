"""
Alpha-Place ウォークフォワードバックテスト
==========================================

複勝特化 AI（AlphaPlaceModel）による全カ年通算 ROI 検証スクリプト。

実行例:
    python scripts/run_alpha_place.py
    python scripts/run_alpha_place.py --trials 50 --no-research-db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# UTF-8 強制・行バッファリング有効（Windows cp932 + ファイルリダイレクト対策）
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from src.ml.alpha_place_model import (
    PlaceBacktestResult,
    run_place_backtest,
)

_DB_PATH = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"
_MODEL_SAVE_DIR = _ROOT / "data" / "models" / "alpha_place"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _print_separator(char: str = "=", width: int = 72) -> None:
    print(char * width)


def _print_result(r: PlaceBacktestResult) -> None:
    mark = "✅" if r.roi >= 110 else ("⚠️ " if r.roi >= 100 else "❌")
    print(f"    年度          : {r.year}")
    print(f"    買いシグナル   : {r.num_bets:,}件")
    print(f"    的中率         : {r.hit_rate:.1f}% ({r.num_hits}件)")
    print(f"    総投資         : ¥{r.total_investment:,}")
    print(f"    総払戻         : ¥{r.total_payout:,.0f}")
    print(f"    損益           : ¥{r.profit:+,.0f}")
    print(f"    ROI            : {r.roi:.1f}% {mark}")
    print(f"    最大DD         : ¥{r.max_drawdown:,.0f}")


def _aggregate_results(
    results: list[PlaceBacktestResult],
) -> tuple[float, float, float]:
    total_inv = sum(r.total_investment for r in results)
    total_pay = sum(r.total_payout for r in results)
    total_roi = total_pay / total_inv * 100 if total_inv > 0 else 0.0
    return total_inv, total_pay, total_roi


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpha-Place ウォークフォワードバックテスト"
    )
    parser.add_argument("--trials", type=int, default=30, help="Optuna トライアル数")
    parser.add_argument(
        "--no-research-db", action="store_true", help="netkeiba_research.db を使わない"
    )
    parser.add_argument(
        "--save-model", action="store_true", help="学習済みモデルを保存する"
    )
    parser.add_argument(
        "--no-venue-opt", action="store_true", help="会場別EV閾値最適化を無効化"
    )
    parser.add_argument(
        "--no-calibrate", action="store_true", help="Isotonic calibration を無効化"
    )
    parser.add_argument(
        "--ev-threshold",
        type=float,
        default=None,
        help="固定EV閾値（デフォルト: DEFAULT_EV_THRESHOLD）",
    )
    args = parser.parse_args()

    research_db: Path | None = None
    if not args.no_research_db and _RESEARCH_DB.exists():
        research_db = _RESEARCH_DB
        print(f"[INFO] research DB 使用: {research_db}")
    else:
        print("[INFO] research DB なし — JVLink win_odds のみ使用")

    conn = _connect()

    # ── ウォークフォワード定義 ──────────────────────────────────────────
    walk_forward_pairs: list[tuple[list[int], list[int]]] = [
        ([2024], [2025]),
        ([2025], [2026]),
    ]

    all_results: list[PlaceBacktestResult] = []

    from src.ml.alpha_place_model import DEFAULT_EV_THRESHOLD as _DEF_EV

    _use_venue_opt = not args.no_venue_opt
    _calibrate = not args.no_calibrate
    _ev_threshold = args.ev_threshold if args.ev_threshold is not None else _DEF_EV

    calib_label = "Isotonic(prefit)" if _calibrate else "なし"
    venue_label = "会場別最適化" if _use_venue_opt else f"固定EV>{_ev_threshold:.2f}"

    _print_separator()
    print("  Alpha-Place ウォークフォワードバックテスト（複勝特化）")
    print(
        f"  Optuna {args.trials}試行 / キャリブレーション:{calib_label} / {venue_label}"
    )
    _print_separator()

    for train_years, test_years in walk_forward_pairs:
        print(f"\n{'─' * 72}")
        print(f"  [WF] 学習: {train_years}  →  テスト: {test_years}")
        print(f"{'─' * 72}")

        try:
            result = run_place_backtest(
                conn=conn,
                train_years=train_years,
                test_years=test_years,
                n_optuna_trials=args.trials,
                bankroll=100_000,
                verbose=True,
                research_db_path=research_db,
                use_venue_opt=_use_venue_opt,
                calibrate=_calibrate,
                ev_threshold=_ev_threshold,
            )

            if args.save_model:
                # 最新モデルをモデルディレクトリに保存（年次サフィックス付き）
                from src.ml.alpha_place_model import AlphaPlaceModel

                m = AlphaPlaceModel()
                # run_place_backtestは内部でモデルを作るので再学習は不要
                # ここでは保存スキップ（run_place_backtest の戻り値はResultのみ）
                pass

            all_results.append(result)

        except Exception as exc:
            print(f"\n  [ERROR] 年次 {test_years}: {exc}")
            import traceback

            traceback.print_exc()
            continue

    conn.close()

    if not all_results:
        print("\n[ERROR] 有効なバックテスト結果がありません。終了します。")
        sys.exit(1)

    # ── 通算サマリー ────────────────────────────────────────────────────
    _print_separator()
    print("  Alpha-Place ウォークフォワード 年別レポート")
    _print_separator()

    print("\n【複勝 年別結果】")
    header = f"  {'年度':<8} {'学習年':<8} {'買い点':>8} {'的中率':>8} {'投資':>14} {'払戻':>14} {'ROI':>8} {'MaxDD':>12}"
    print(header)
    print("  " + "─" * (len(header) - 2))
    for r in all_results:
        mark = "✅" if r.roi >= 110 else ("⚠️ " if r.roi >= 100 else "❌")
        print(
            f"  {r.year!s:<8} "
            f"{'─':8} "
            f"{r.num_bets:>8,} "
            f"{r.hit_rate:>7.1f}% "
            f"¥{r.total_investment:>12,} "
            f"¥{r.total_payout:>12,.0f} "
            f"{r.roi:>7.1f}% {mark} "
            f"¥{r.max_drawdown:>10,.0f}"
        )

    total_inv, total_pay, total_roi = _aggregate_results(all_results)
    years_above = sum(1 for r in all_results if r.roi >= 110)

    _print_separator()
    print("  通算サマリー")
    _print_separator()

    mark = "✅" if total_roi >= 110 else ("⚠️ " if total_roi >= 100 else "❌")
    print(
        f"\n  複勝  通算ROI={total_roi:.1f}% {mark}  "
        f"(ROI110%超: {years_above}/{len(all_results)}年)  "
        f"投資¥{total_inv:,}  払戻¥{total_pay:,.0f}"
    )

    _print_separator()
    print("  Alpha-Place STRATEGY 推奨判定")
    _print_separator()

    if total_roi >= 110:
        print(f"\n  複勝: 【採用】ROI={total_roi:.1f}% ≥ 110% — 本番統合推奨 🎉")
        print("\n  次のアクション:")
        print("   1. weekend_batch.py に AlphaPlaceModel を統合")
        print("   2. prediction_results テーブルで実績追跡を開始")
        print("   3. 月次で optimize_venue_thresholds() を再実行")
    elif total_roi >= 100:
        print(f"\n  複勝: 【条件付き採用】ROI={total_roi:.1f}% ≥ 100% — 試験運用可")
        print(
            "       注意: 目標 ROI 110% まで残り"
            f"{110 - total_roi:.1f}pp。EV閾値を上げて絞り込みを推奨。"
        )
    else:
        print(f"\n  複勝: 【不採用】ROI={total_roi:.1f}% < 100% — さらなる改善が必要")
        print("\n  改善候補:")
        print("   - EV閾値を EV>1.1 以上に絞る（高確率シグナルのみ）")
        print("   - 特徴量追加: 近走 複勝率, 調教師複勝率, 血統複勝適性")
        print("   - ラウンド毎のアンサンブル（BaggingClassifier）")

    _print_separator()


if __name__ == "__main__":
    main()
