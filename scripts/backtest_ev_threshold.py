"""
UMALOGI EV閾値・Harville最適化バックテスト

predictions + prediction_results を使い、EV閾値・Harville最小確率を
グリッドサーチして回収率・的中率・シャープレシオを算出する。

使用例:
  python scripts/backtest_ev_threshold.py
  python scripts/backtest_ev_threshold.py --bet-type 単勝
  python scripts/backtest_ev_threshold.py --model "卍(直前)" --date-from 2026-01-01
  python scripts/backtest_ev_threshold.py --ev-min 0.5 --ev-max 3.0 --ev-step 0.1
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db


# ================================================================
# データ読み込み
# ================================================================

def load_results(
    conn,
    date_from: str | None = None,
    date_to:   str | None = None,
) -> pd.DataFrame:
    """
    predictions と prediction_results を結合して返す。

    Returns:
        DataFrame with columns:
          race_id, model_type, bet_type, expected_value, recommended_bet,
          is_hit, payout, race_date
    """
    params: list[str] = []
    where_clauses: list[str] = ["pr.is_hit IS NOT NULL"]

    if date_from:
        where_clauses.append("r.date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("r.date <= ?")
        params.append(date_to)

    where_sql = " AND ".join(where_clauses)

    df = pd.read_sql_query(
        f"""
        SELECT
            p.id               AS prediction_id,
            p.race_id,
            r.date             AS race_date,
            p.model_type,
            p.bet_type,
            p.expected_value,
            p.recommended_bet,
            COALESCE(pr.is_hit, 0)  AS is_hit,
            COALESCE(pr.payout, 0)  AS payout
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE {where_sql}
          AND p.bet_type NOT IN ('馬分析', 'WIN5')
          AND p.expected_value IS NOT NULL
        ORDER BY r.date, p.race_id
        """,
        conn,
        params=params,
    )
    return df


# ================================================================
# メトリクス計算
# ================================================================

class BetStats(NamedTuple):
    n_bets:       int
    n_hits:       int
    hit_rate:     float   # %
    total_bet:    float   # 円
    total_payout: float   # 円
    roi:          float   # 回収率 %
    profit:       float   # 純損益
    sharpe:       float   # シャープレシオ（レース単位）


def _calc_stats(subset: pd.DataFrame, unit_bet: int = 100) -> BetStats:
    """サブセットの賭け統計を計算する。"""
    if subset.empty:
        return BetStats(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    n     = len(subset)
    hits  = int(subset["is_hit"].sum())
    bet   = n * unit_bet
    # payout は払戻総額（既に円建て）
    pay   = float(subset.loc[subset["is_hit"] == 1, "payout"].sum())
    roi   = pay / bet * 100 if bet > 0 else 0.0

    # シャープレシオ: レースごとの損益の平均/標準偏差
    per_race = subset["payout"].where(subset["is_hit"] == 1, 0) - unit_bet
    sharpe = (per_race.mean() / per_race.std()) if per_race.std() > 0 else 0.0

    return BetStats(
        n_bets=n,
        n_hits=hits,
        hit_rate=hits / n * 100,
        total_bet=float(bet),
        total_payout=pay,
        roi=roi,
        profit=pay - bet,
        sharpe=float(sharpe),
    )


# ================================================================
# グリッドサーチ
# ================================================================

def grid_search(
    df: pd.DataFrame,
    ev_thresholds:    list[float],
    model_filter:     str | None = None,
    bet_type_filter:  str | None = None,
    unit_bet:         int = 100,
) -> pd.DataFrame:
    """
    EV閾値のグリッドサーチを実行して結果 DataFrame を返す。

    Args:
        df:              load_results() の出力
        ev_thresholds:   試すEV閾値のリスト
        model_filter:    モデル名フィルタ（部分一致）
        bet_type_filter: 券種フィルタ（完全一致）
        unit_bet:        1点あたりの賭け金（円）

    Returns:
        DataFrame: ev_threshold × model_type × bet_type ごとのメトリクス
    """
    if model_filter:
        df = df[df["model_type"].str.contains(model_filter, na=False)]
    if bet_type_filter:
        df = df[df["bet_type"] == bet_type_filter]

    if df.empty:
        print("WARNING: フィルタ後のデータが0件です。条件を見直してください。")
        return pd.DataFrame()

    records = []
    groups = df.groupby(["model_type", "bet_type"])

    for (model, bet_type), grp in groups:
        for ev_thr in ev_thresholds:
            subset = grp[grp["expected_value"] >= ev_thr]
            stats = _calc_stats(subset, unit_bet)
            records.append({
                "ev_threshold": ev_thr,
                "model_type":   model,
                "bet_type":     bet_type,
                "n_bets":       stats.n_bets,
                "n_hits":       stats.n_hits,
                "hit_rate":     round(stats.hit_rate, 2),
                "total_bet":    stats.total_bet,
                "total_payout": stats.total_payout,
                "roi":          round(stats.roi, 2),
                "profit":       round(stats.profit, 0),
                "sharpe":       round(stats.sharpe, 3),
            })

    return pd.DataFrame(records)


# ================================================================
# 最適点の抽出
# ================================================================

def find_optimal(
    result: pd.DataFrame,
    min_bets: int = 10,
    metric: str = "roi",
) -> pd.DataFrame:
    """
    各 model_type × bet_type の最適 EV 閾値を抽出する。

    Args:
        result:   grid_search() の出力
        min_bets: 統計的信頼性のための最低賭け件数
        metric:   最適化指標 ("roi" / "sharpe" / "profit")

    Returns:
        model_type × bet_type ごとに最優 EV 閾値の行を返す DataFrame
    """
    filtered = result[result["n_bets"] >= min_bets].copy()
    if filtered.empty:
        return filtered

    idx = filtered.groupby(["model_type", "bet_type"])[metric].idxmax()
    return filtered.loc[idx].reset_index(drop=True).sort_values(
        ["model_type", "bet_type", metric], ascending=[True, True, False]
    )


# ================================================================
# レポート表示
# ================================================================

def print_report(result: pd.DataFrame, optimal: pd.DataFrame) -> None:
    """グリッドサーチ結果と最適点をコンソールに表示する。"""
    if result.empty:
        print("結果なし")
        return

    print("\n" + "=" * 80)
    print("  EV閾値グリッドサーチ結果（全組み合わせ）")
    print("=" * 80)

    for (model, bet_type), grp in result.groupby(["model_type", "bet_type"]):
        print(f"\n  [{model}] {bet_type}")
        print(f"  {'EV>=':>6}  {'N件':>5}  {'的中率%':>7}  {'回収率%':>7}  {'純損益':>10}  {'Sharpe':>7}")
        print("  " + "-" * 55)
        for _, row in grp.iterrows():
            marker = " ★" if row["roi"] >= 100 else ""
            print(
                f"  {row['ev_threshold']:>6.2f}  "
                f"{row['n_bets']:>5}  "
                f"{row['hit_rate']:>7.1f}  "
                f"{row['roi']:>7.1f}  "
                f"{row['profit']:>10,.0f}  "
                f"{row['sharpe']:>7.3f}"
                f"{marker}"
            )

    if not optimal.empty:
        print("\n" + "=" * 80)
        print("  最適 EV 閾値サマリー（ROI最大・N>=10）")
        print("=" * 80)
        print(f"  {'モデル':<14}  {'券種':<8}  {'最適EV':>6}  {'N件':>5}  {'回収率%':>7}  {'Sharpe':>7}")
        print("  " + "-" * 60)
        for _, row in optimal.iterrows():
            print(
                f"  {row['model_type']:<14}  "
                f"{row['bet_type']:<8}  "
                f"{row['ev_threshold']:>6.2f}  "
                f"{row['n_bets']:>5}  "
                f"{row['roi']:>7.1f}  "
                f"{row['sharpe']:>7.3f}"
            )
    print()


# ================================================================
# CLI
# ================================================================

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EV閾値・Harville最適化バックテスト",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python scripts/backtest_ev_threshold.py
  python scripts/backtest_ev_threshold.py --bet-type 単勝
  python scripts/backtest_ev_threshold.py --model "卍(直前)" --date-from 2026-01-01
  python scripts/backtest_ev_threshold.py --ev-min 0.5 --ev-max 3.0 --ev-step 0.1
  python scripts/backtest_ev_threshold.py --metric sharpe --min-bets 20
  python scripts/backtest_ev_threshold.py --output results/ev_backtest.csv
""",
    )
    parser.add_argument("--date-from",  help="集計開始日 YYYY-MM-DD")
    parser.add_argument("--date-to",    help="集計終了日 YYYY-MM-DD")
    parser.add_argument("--model",      help="モデル名フィルタ（部分一致）例: 卍(直前)")
    parser.add_argument("--bet-type",   help="券種フィルタ 例: 単勝")
    parser.add_argument("--ev-min",     type=float, default=0.5, help="EV閾値 最小 (default: 0.5)")
    parser.add_argument("--ev-max",     type=float, default=3.0, help="EV閾値 最大 (default: 3.0)")
    parser.add_argument("--ev-step",    type=float, default=0.1, help="EV閾値 ステップ (default: 0.1)")
    parser.add_argument("--unit-bet",   type=int, default=100, help="1点あたり賭け金円 (default: 100)")
    parser.add_argument("--min-bets",   type=int, default=10,  help="最適点抽出の最低賭け件数 (default: 10)")
    parser.add_argument("--metric",     choices=["roi", "sharpe", "profit"], default="roi",
                        help="最適化指標 (default: roi)")
    parser.add_argument("--output",     help="CSV 出力先パス（省略時はコンソールのみ）")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    ev_thresholds = [
        round(v, 4)
        for v in np.arange(args.ev_min, args.ev_max + args.ev_step / 2, args.ev_step)
    ]

    conn = init_db()
    print(f"データ読み込み中... (date_from={args.date_from}, date_to={args.date_to})")
    df = load_results(conn, date_from=args.date_from, date_to=args.date_to)
    conn.close()

    if df.empty:
        print("ERROR: prediction_results が 0 件です。reconcile を先に実行してください。")
        print("  py -m src.ml.reconcile")
        sys.exit(1)

    print(f"読み込み完了: {len(df)} 件 ({df['race_date'].min()} 〜 {df['race_date'].max()})")
    print(f"EV閾値: {ev_thresholds[0]:.2f} 〜 {ev_thresholds[-1]:.2f} ({len(ev_thresholds)} 通り)")
    print(f"モデル: {sorted(df['model_type'].unique())}")
    print(f"券種: {sorted(df['bet_type'].unique())}")

    result = grid_search(
        df,
        ev_thresholds=ev_thresholds,
        model_filter=args.model,
        bet_type_filter=args.bet_type,
        unit_bet=args.unit_bet,
    )

    if result.empty:
        sys.exit(1)

    optimal = find_optimal(result, min_bets=args.min_bets, metric=args.metric)
    print_report(result, optimal)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"CSV 出力: {out_path}")

    # 最適値をJSON形式でも出力（他スクリプトからの呼び出し用）
    if not optimal.empty:
        print("\n最適閾値 (JSON):")
        print(json.dumps(
            optimal[["model_type", "bet_type", "ev_threshold", "roi", "n_bets", "sharpe"]]
            .to_dict(orient="records"),
            ensure_ascii=False, indent=2
        ))


if __name__ == "__main__":
    main()
