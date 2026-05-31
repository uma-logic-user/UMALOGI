"""
scripts/ev_backtest_vectorized.py — EV 特化ベクトル化バックテストシミュレーター

設計思想:
  - Python for ループ禁止: 全計算は pandas/numpy のベクトル演算
  - Kelly シミュレーションは np.cumprod のみ使用（ループなし）
  - JRA 控除率はクラス定数で管理（ハードコード禁止）
  - Sharpe レシオ・最大ドローダウン・グリッドサーチを包括

使い方:
    py scripts/ev_backtest_vectorized.py
    py scripts/ev_backtest_vectorized.py --ev-min 1.2 --kelly-fraction 0.25
    py scripts/ev_backtest_vectorized.py --grid-search
    py scripts/ev_backtest_vectorized.py --grid-search --top-n 5
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_DB_PATH = _ROOT / "data" / "umalogi.db"


# ─────────────────────────────────────────────────────────────────────────────
# バックテスト定数（変更はここだけ）
# ─────────────────────────────────────────────────────────────────────────────


class _BacktestConstants:
    """バックテストパラメータ定数（クラス定数で一元管理）。"""

    KELLY_FRACTION: float = 0.25  # フラクショナル Kelly の係数
    KELLY_CAP: float = 0.25  # Kelly 比率の上限
    INITIAL_BANKROLL: float = 100_000.0  # 初期資金（円）
    MIN_BET: float = 100.0  # 最小賭け金（円）
    MAX_BET: float = 10_000.0  # 最大賭け金（円）

    # JRA 控除率（単勝ベース）
    JRA_WIN_TAKEOUT: float = 0.20  # 単勝控除率 20%

    # グリッドサーチ範囲
    GRID_EV_THRESHOLDS: list[float] = [1.0, 1.2, 1.5, 2.0]
    GRID_KELLY_FRACS: list[float] = [0.10, 0.25, 0.50]


_BC = _BacktestConstants()


# ─────────────────────────────────────────────────────────────────────────────
# データロード
# ─────────────────────────────────────────────────────────────────────────────


def load_ev_matrix(
    conn: sqlite3.Connection,
    model_type: str | None = None,
    since: str | None = None,
) -> pd.DataFrame:
    """predictions + prediction_results を単一 SQL JOIN で読み込む。

    Python ループなし: 1 回の SQL で全データを一括取得する。

    Args:
        conn:       SQLite 接続
        model_type: モデルタイプフィルタ（None = 全モデル）
        since:      開始日フィルタ (YYYY-MM-DD)。None = 全期間。

    Returns:
        DataFrame (columns: race_id, race_date, model_type, bet_type,
        expected_value, win_odds, is_hit, payout, recommended_bet)
    """
    where_clauses: list[str] = ["pr.is_hit IS NOT NULL"]
    params: list[Any] = []

    if model_type:
        where_clauses.append("p.model_type = ?")
        params.append(model_type)

    if since:
        where_clauses.append("r.date >= ?")
        params.append(since)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
    SELECT
        p.race_id,
        r.date                            AS race_date,
        p.model_type,
        p.bet_type,
        COALESCE(p.expected_value, 0.0)   AS expected_value,
        COALESCE(rr.win_odds, 0.0)        AS win_odds,
        COALESCE(pr.is_hit, 0)            AS is_hit,
        COALESCE(pr.payout, 0)            AS payout,
        COALESCE(p.recommended_bet, 100)  AS recommended_bet
    FROM predictions p
    JOIN prediction_results pr ON pr.prediction_id = p.prediction_id
    LEFT JOIN races r           ON r.race_id = p.race_id
    LEFT JOIN race_results rr   ON rr.race_id = p.race_id
                               AND rr.horse_number = p.horse_number
    WHERE {where_sql}
    ORDER BY r.date, p.race_id
    """
    df = pd.read_sql(sql, conn, params=params)

    df["race_date"] = pd.to_datetime(df.get("race_date"), errors="coerce")
    df["expected_value"] = pd.to_numeric(df["expected_value"], errors="coerce").fillna(
        0.0
    )
    df["win_odds"] = pd.to_numeric(df["win_odds"], errors="coerce").fillna(1.01)
    df["payout"] = pd.to_numeric(df["payout"], errors="coerce").fillna(0.0)
    df["recommended_bet"] = pd.to_numeric(
        df["recommended_bet"], errors="coerce"
    ).fillna(100.0)
    df["is_hit"] = pd.to_numeric(df["is_hit"], errors="coerce").fillna(0.0)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# EV ベクトル計算
# ─────────────────────────────────────────────────────────────────────────────


def compute_ev_vectorized(
    df: pd.DataFrame,
    prob_col: str = "model_prob",
    odds_col: str = "win_odds",
) -> pd.Series:
    """EV = P(win) × odds をベクトル演算で計算する。

    Python ループなし: numpy の elementwise 乗算のみ。
    prob_col が存在しない場合は expected_value 列をそのまま EV として扱う。

    Args:
        df:       predictions DataFrame
        prob_col: モデル確率列名
        odds_col: オッズ列名

    Returns:
        EV Series（index は df.index と一致）
    """
    odds = pd.to_numeric(df[odds_col], errors="coerce").fillna(1.01).clip(lower=1.01)

    if prob_col in df.columns:
        prob = pd.to_numeric(df[prob_col], errors="coerce").fillna(0.0)
        return prob * odds
    else:
        # expected_value が EV として保存済みのケース
        return pd.to_numeric(
            df.get("expected_value", pd.Series([0.0] * len(df))), errors="coerce"
        ).fillna(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# Kelly シミュレーション（np.cumprod 専用実装・ループ禁止）
# ─────────────────────────────────────────────────────────────────────────────


def simulate_kelly_bankroll(
    ev_series: pd.Series,
    is_hit_series: pd.Series,
    payout_series: pd.Series,
    win_odds_series: pd.Series,
    initial_bankroll: float = _BC.INITIAL_BANKROLL,
    kelly_fraction: float = _BC.KELLY_FRACTION,
    kelly_cap: float = _BC.KELLY_CAP,
) -> pd.Series:
    """Kelly 基準に基づく資金推移を np.cumprod で計算する。

    **ループ禁止**: 全計算は numpy ベクトル演算 + np.cumprod のみ。

    Kelly 比率:
        b = odds - 1
        kelly_raw = (EV - 1) / (odds - 1) = (EV - 1) / b
        kelly = clip(kelly_raw × kelly_fraction, 0, kelly_cap)

    資金倍率:
        的中: bankroll × (1 + kelly × b)
        外れ: bankroll × (1 - kelly)

    Args:
        ev_series:        EV 値 Series
        is_hit_series:    的中フラグ (0/1) Series
        payout_series:    払戻金 Series（円 / 100円賭け）
        win_odds_series:  単勝オッズ Series
        initial_bankroll: 初期資金
        kelly_fraction:   フラクショナル Kelly 係数
        kelly_cap:        Kelly 比率上限

    Returns:
        資金推移 Series（index は入力と同一、値は絶対資金額）
    """
    ev = np.asarray(ev_series, dtype=float)
    hit = np.asarray(is_hit_series, dtype=float)
    odds = np.asarray(win_odds_series, dtype=float)
    odds = np.where(odds < 1.01, 1.01, odds)

    b = odds - 1.0
    # Kelly 比率: EV > 1 のときのみ正、EV <= 1 は 0（賭けない）
    kelly_raw = np.where(b > 1e-10, (ev - 1.0) / b, 0.0)
    kelly = np.clip(kelly_raw * kelly_fraction, 0.0, kelly_cap)

    # 各ベットの資金倍率
    win_multiplier = 1.0 + kelly * b  # 的中時
    lose_multiplier = 1.0 - kelly  # 外れ時

    # np.cumprod で資金倍率を累積（ループなし）
    multiplier = np.where(hit > 0.5, win_multiplier, lose_multiplier)
    cumulative_multiplier = np.cumprod(multiplier)

    bankroll_values = initial_bankroll * cumulative_multiplier
    return pd.Series(bankroll_values, index=ev_series.index)


# ─────────────────────────────────────────────────────────────────────────────
# Sharpe レシオ・最大ドローダウン
# ─────────────────────────────────────────────────────────────────────────────


def compute_sharpe_ratio(
    bankroll: pd.Series,
    risk_free_rate: float = 0.001,
) -> float:
    """年率換算 Sharpe レシオを計算する。

    JRA 開催日ベース（週 2 日×52 週 = 年 104 日相当）を仮定して年率換算する。

    Args:
        bankroll:       資金推移 Series（時系列順）
        risk_free_rate: 無リスク金利（デフォルト 0.1%）

    Returns:
        Sharpe レシオ（float）。データ不足・分散ゼロの場合は 0.0。
    """
    if len(bankroll) < 2:
        return 0.0
    returns = bankroll.pct_change().dropna()
    std = float(returns.std())
    if std < 1e-10:
        return 0.0
    # JRA 週次換算（週 2 日→年 52 週）
    annual_factor = np.sqrt(52.0)
    excess = returns - risk_free_rate / 52.0
    return float(excess.mean() / std * annual_factor)


def compute_max_drawdown(bankroll: pd.Series) -> float:
    """最大ドローダウン率を計算する。

    Args:
        bankroll: 資金推移 Series

    Returns:
        最大ドローダウン率（0-1）。大きいほど損失大。
    """
    if len(bankroll) < 2:
        return 0.0
    rolling_max = bankroll.cummax()
    drawdown = (rolling_max - bankroll) / rolling_max.clip(lower=1e-8)
    return float(drawdown.max())


# ─────────────────────────────────────────────────────────────────────────────
# グリッドサーチ
# ─────────────────────────────────────────────────────────────────────────────


def grid_search_ev_threshold(
    df: pd.DataFrame,
    ev_thresholds: list[float] | None = None,
    kelly_fracs: list[float] | None = None,
) -> pd.DataFrame:
    """EV 閾値 × Kelly 係数 グリッドサーチを実行する。

    各セルの Kelly シミュレーションは simulate_kelly_bankroll（np.cumprod）で実行。
    EV フィルタは pandas ベクトル操作のみ（行ループなし）。

    Args:
        df:            load_ev_matrix() の出力 DataFrame
        ev_thresholds: 試行する EV 閾値リスト
        kelly_fracs:   試行する Kelly 係数リスト

    Returns:
        グリッドサーチ結果 DataFrame（Sharpe レシオ降順）
    """
    ev_thresholds = ev_thresholds or _BC.GRID_EV_THRESHOLDS
    kelly_fracs = kelly_fracs or _BC.GRID_KELLY_FRACS

    results: list[dict] = []

    for ev_thr in ev_thresholds:
        subset = df[df["expected_value"] >= ev_thr].copy()
        if len(subset) == 0:
            continue

        for kf in kelly_fracs:
            bankroll = simulate_kelly_bankroll(
                ev_series=subset["expected_value"],
                is_hit_series=subset["is_hit"],
                payout_series=subset["payout"],
                win_odds_series=subset["win_odds"].clip(lower=1.01),
                kelly_fraction=kf,
            )

            n_bets = len(subset)
            n_hits = int(subset["is_hit"].sum())
            total_bet = float(subset["recommended_bet"].clip(lower=100.0).sum())
            total_payout = float(subset.loc[subset["is_hit"] > 0.5, "payout"].sum())
            roi = total_payout / total_bet if total_bet > 0 else 0.0

            sharpe = compute_sharpe_ratio(bankroll)
            mdd = compute_max_drawdown(bankroll)
            final = float(bankroll.iloc[-1])

            n_days = 365
            if "race_date" in subset.columns:
                date_range = subset["race_date"].dropna()
                if len(date_range) > 1:
                    n_days = max(1, (date_range.max() - date_range.min()).days)

            ann_roi = (roi ** (365.0 / n_days)) - 1.0 if roi > 0 else 0.0

            results.append(
                {
                    "ev_threshold": ev_thr,
                    "kelly_fraction": kf,
                    "n_bets": n_bets,
                    "n_hits": n_hits,
                    "hit_rate": n_hits / n_bets if n_bets > 0 else 0.0,
                    "total_profit": total_payout - total_bet,
                    "roi": roi,
                    "sharpe_ratio": sharpe,
                    "max_drawdown": mdd,
                    "final_bankroll": final,
                    "annualized_roi": ann_roi,
                }
            )

    if not results:
        return pd.DataFrame(
            columns=[
                "ev_threshold",
                "kelly_fraction",
                "n_bets",
                "n_hits",
                "hit_rate",
                "total_profit",
                "roi",
                "sharpe_ratio",
                "max_drawdown",
                "final_bankroll",
                "annualized_roi",
            ]
        )

    return pd.DataFrame(results).sort_values("sharpe_ratio", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(description="EV ベクトル化バックテストシミュレーター")
    p.add_argument(
        "--ev-min", type=float, default=1.0, help="最小 EV 閾値（デフォルト: 1.0）"
    )
    p.add_argument(
        "--kelly-fraction",
        type=float,
        default=_BC.KELLY_FRACTION,
        help="Kelly 係数（デフォルト: 0.25）",
    )
    p.add_argument("--model", type=str, default=None, help="モデルタイプフィルタ")
    p.add_argument("--since", type=str, default=None, help="開始日 YYYY-MM-DD")
    p.add_argument("--grid-search", action="store_true", help="グリッドサーチモード")
    p.add_argument(
        "--top-n", type=int, default=10, help="グリッドサーチ表示件数（デフォルト: 10）"
    )
    args = p.parse_args()

    if not _DB_PATH.exists():
        print(f"⚠️  DB が見つかりません: {_DB_PATH}", file=sys.stderr)
        sys.exit(0)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        df = load_ev_matrix(conn, model_type=args.model, since=args.since)
    finally:
        conn.close()

    print(f"📊 ロード完了: {len(df):,} 件")

    if len(df) == 0:
        print("⚠️  データなし — バックテスト終了。")
        return

    if args.grid_search:
        print("\n🔍 グリッドサーチ実行中...")
        grid_result = grid_search_ev_threshold(df)

        if len(grid_result) == 0:
            print("⚠️  グリッドサーチ結果なし（全 EV 閾値で対象ゼロ）")
            return

        header = (
            f"{'EV閾値':>8} {'Kelly':>6} {'件数':>6} {'的中':>6} "
            f"{'的中率':>7} {'ROI':>8} {'Sharpe':>8} {'最大DD':>8}"
        )
        print(f"\n{'=' * 70}")
        print(header)
        print(f"{'=' * 70}")
        for _, row in grid_result.head(args.top_n).iterrows():
            print(
                f"{row['ev_threshold']:>8.2f} "
                f"{row['kelly_fraction']:>6.2f} "
                f"{int(row['n_bets']):>6} "
                f"{int(row['n_hits']):>6} "
                f"{row['hit_rate']:>7.1%} "
                f"{row['roi']:>8.1%} "
                f"{row['sharpe_ratio']:>8.3f} "
                f"{row['max_drawdown']:>8.1%}"
            )
    else:
        filtered = df[df["expected_value"] >= args.ev_min]
        print(f"⚡ EV >= {args.ev_min} フィルタ後: {len(filtered):,} 件")

        if len(filtered) == 0:
            print("⚠️  対象ゼロ — EV 閾値を下げるか --grid-search を試してください。")
            return

        bankroll = simulate_kelly_bankroll(
            ev_series=filtered["expected_value"],
            is_hit_series=filtered["is_hit"],
            payout_series=filtered["payout"],
            win_odds_series=filtered["win_odds"].clip(lower=1.01),
            kelly_fraction=args.kelly_fraction,
        )

        n_hits = int(filtered["is_hit"].sum())
        total_bet = float(filtered["recommended_bet"].clip(lower=100.0).sum())
        total_payout = float(filtered.loc[filtered["is_hit"] > 0.5, "payout"].sum())
        roi = total_payout / total_bet if total_bet > 0 else 0.0

        print(f"\n{'=' * 50}")
        print(f"  件数:             {len(filtered):,} 件")
        print(f"  的中数:           {n_hits:,} 件")
        print(f"  的中率:           {n_hits / len(filtered):.1%}")
        print(f"  合計投資:         ¥{total_bet:,.0f}")
        print(f"  合計払戻:         ¥{total_payout:,.0f}")
        print(f"  ROI:              {roi:.1%}")
        print(f"  Sharpe レシオ:    {compute_sharpe_ratio(bankroll):.3f}")
        print(f"  最大ドローダウン: {compute_max_drawdown(bankroll):.1%}")
        print(f"  最終資金:         ¥{bankroll.iloc[-1]:,.0f}")
        print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
