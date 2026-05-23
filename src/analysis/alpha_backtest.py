"""
src/analysis/alpha_backtest.py — ALPHA-Payout ガチ投資シミュレーション
=======================================================================

本番稼働モデルと同じ AlphaPayoutModel（複勝ペイアウト直接回帰）を使い
2024 学習 → 2025 テスト の walk-forward（カンニング排除）で検証する。

初期資金: ¥50,000
Pattern 1: 単利固定ベット ¥1,000/bet
Pattern 2: 複利ベット     残高の 2%/bet（初期 = ¥1,000）

Usage:
    py src/analysis/alpha_backtest.py
    py src/analysis/alpha_backtest.py --bet-type 複勝 --optuna-trials 20
    py src/analysis/alpha_backtest.py --no-optuna    # 速度優先（Optuna なし）
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)

_DB_PATH     = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"

_INITIAL   = 50_000      # 初期資金 (円)
_FIXED     = 1_000       # Pattern 1: 固定賭け金 (円)
_FRAC      = 0.02        # Pattern 2: 残高の何割を賭けるか
_MIN_BET   = 100         # 最低賭け金 (円)


# ── ベットデータ取得 ───────────────────────────────────────────────────

def _load_bet_rows(
    conn: sqlite3.Connection,
    n_optuna: int,
    research_db: Path | None,
) -> tuple[pd.DataFrame, float, float]:
    """
    AlphaPayoutModel を 2024 で学習 → 2025 でシグナル生成する。
    戻り値: (bets_df, ev_threshold, auc)
    """
    from src.ml.alpha_payout_model import AlphaPayoutModel

    model = AlphaPayoutModel()

    print("  [学習] 2024年データをロード中...", flush=True)
    train_df = model.load_training_data(
        conn, [2024], research_db_path=research_db
    )
    print(f"  [学習] {len(train_df):,} 行ロード完了", flush=True)

    print(f"  [学習] Optuna {n_optuna}試行でハイパーパラ最適化中...", flush=True)
    metrics = model.train(train_df, n_optuna_trials=n_optuna)
    threshold = model._ev_threshold
    print(
        f"  [学習完了] n_train={metrics['n_train']:,}  "
        f"val_ROI={metrics['val_roi']:.1f}%  閾値={threshold:.2f}",
        flush=True,
    )

    print("  [テスト] 2025年データをロード中...", flush=True)
    test_df = model.load_training_data(
        conn, [2025], research_db_path=research_db
    )
    print(f"  [テスト] {len(test_df):,} 行ロード完了", flush=True)

    pred_ev = model.predict_payout_ev(test_df)
    test_df = test_df.copy()
    test_df["pred_ev"]       = pred_ev.values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0.0)

    bets_df = test_df[test_df["pred_ev"] >= threshold].copy()
    bets_df = bets_df.sort_values(["date", "race_id", "horse_number"]).reset_index(drop=True)

    n_total = len(test_df)
    n_bets  = len(bets_df)
    n_days  = test_df["date"].nunique()
    print(
        f"  [シグナル] pred_ev>{threshold:.2f}: {n_bets:,} / {n_total:,} 件  "
        f"({n_bets / n_total * 100:.1f}%)  日平均 {n_bets / max(n_days, 1):.1f}件",
        flush=True,
    )
    return bets_df, threshold, float(metrics.get("auc", 0.0))


# ── シミュレーション ──────────────────────────────────────────────────

class SimResult(NamedTuple):
    label: str
    n_bets: int
    n_hits: int
    hit_rate: float
    total_stake: float
    total_payout: float
    roi: float
    net_profit: float
    final_balance: float
    max_drawdown_pct: float
    max_consec_loss: int
    monthly: pd.DataFrame


def _simulate(
    bets_df: pd.DataFrame,
    pattern: str,           # "fixed" | "compound"
    initial: int = _INITIAL,
    fixed: int = _FIXED,
    frac: float = _FRAC,
) -> SimResult:
    if len(bets_df) == 0:
        empty = pd.DataFrame(
            columns=["month", "bets", "hits", "stake", "payout", "profit", "balance"]
        )
        return SimResult(
            label=pattern, n_bets=0, n_hits=0, hit_rate=0,
            total_stake=0, total_payout=0, roi=0, net_profit=0,
            final_balance=initial, max_drawdown_pct=0, max_consec_loss=0,
            monthly=empty,
        )

    balance  = float(initial)
    peak     = float(initial)
    max_dd   = 0.0
    consec   = 0
    max_cl   = 0
    records: list[dict] = []

    for _, row in bets_df.iterrows():
        if balance < _MIN_BET:
            break  # 実質破産

        if pattern == "fixed":
            stake = float(fixed)
        else:
            # 残高の frac 割、100 円単位切り上げ、上限なし
            raw = balance * frac
            stake = max(_MIN_BET, round(raw / 100) * 100)

        stake = min(stake, balance)  # 残高を超えないよう

        is_hit = int(row.get("is_place", row.get("is_hit", 0)))
        apy    = float(row["actual_payout"])

        payout = is_hit * apy * stake / 100.0 if apy > 0 else 0.0

        balance += payout - stake
        peak  = max(peak, balance)
        dd    = (peak - balance) / peak * 100 if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

        if payout > 0:
            consec = 0
        else:
            consec += 1
            max_cl = max(max_cl, consec)

        records.append({
            "date":    row["date"],
            "stake":   stake,
            "payout":  payout,
            "is_hit":  is_hit,
            "balance": balance,
        })

    df = pd.DataFrame(records)
    n_b  = len(df)
    n_h  = int(df["is_hit"].sum())
    ts   = float(df["stake"].sum())
    tp   = float(df["payout"].sum())
    roi  = tp / ts * 100 if ts > 0 else 0.0
    prof = tp - ts

    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    monthly = (
        df.groupby("month")
        .agg(
            bets=("stake", "count"),
            hits=("is_hit", "sum"),
            stake=("stake", "sum"),
            payout=("payout", "sum"),
            balance=("balance", "last"),
        )
        .reset_index()
    )
    monthly["profit"] = monthly["payout"] - monthly["stake"]

    return SimResult(
        label=pattern,
        n_bets=n_b,
        n_hits=n_h,
        hit_rate=n_h / n_b * 100 if n_b > 0 else 0.0,
        total_stake=ts,
        total_payout=tp,
        roi=roi,
        net_profit=prof,
        final_balance=balance,
        max_drawdown_pct=max_dd,
        max_consec_loss=max_cl,
        monthly=monthly,
    )


# ── レポート出力 ──────────────────────────────────────────────────────

def _pct_bar(val: float, good: float, bad: float, width: int = 10) -> str:
    ratio = max(0, min(1, (val - bad) / (good - bad))) if good != bad else 0
    filled = round(ratio * width)
    return "▓" * filled + "░" * (width - filled)


def _print_detail(r: SimResult, initial: int) -> None:
    tag = "Pattern 1: 単利固定ベット (¥1,000/bet)" if r.label == "fixed" \
          else "Pattern 2: 複利ベット (残高の2%/bet)"
    print()
    print(f"{'═'*68}")
    print(f"  {tag}")
    print(f"{'═'*68}")
    print(f"  初期資金    : ¥{initial:>12,}")
    print(f"  最終残高    : ¥{r.final_balance:>12,.0f}  ({r.final_balance/initial*100 - 100:+.1f}%)")
    print(f"  {'─'*64}")
    print(f"  ベット件数  : {r.n_bets:>8,}")
    print(f"  的中数      : {r.n_hits:>8,}  (的中率 {r.hit_rate:.1f}%)")
    print(f"  総投資額    : ¥{r.total_stake:>12,.0f}")
    print(f"  総払戻額    : ¥{r.total_payout:>12,.0f}")
    print(f"  純損益      : ¥{r.net_profit:>+12,.0f}")
    print(f"  ROI         : {r.roi:>10.1f}%")
    print(f"  {'─'*64}")
    print(f"  最大DD      : {r.max_drawdown_pct:>8.1f}%")
    print(f"  最大連負    : {r.max_consec_loss:>8} 連敗")
    print()

    if r.monthly.empty:
        print("  月別データなし")
        return

    print(f"  ── 月別成績 ──")
    print(f"  {'月':>7}  {'件数':>5}  {'的中':>4}  {'投資':>9}  {'払戻':>9}  {'損益':>10}  {'残高':>10}")
    print(f"  {'─'*68}")
    for _, row in r.monthly.iterrows():
        sign = "+" if row["profit"] >= 0 else ""
        print(
            f"  {row['month']:>7}  {int(row['bets']):>5,}  {int(row['hits']):>4}  "
            f"¥{int(row['stake']):>8,}  ¥{int(row['payout']):>8,}  "
            f"{sign}¥{int(row['profit']):>8,}  ¥{int(row['balance']):>9,}"
        )


def _print_summary(p1: SimResult, p2: SimResult, initial: int,
                   threshold: float, n_signals: int, n_days: int) -> None:
    avg_sig = n_signals / max(n_days, 1)
    print()
    print("╔" + "═" * 68 + "╗")
    print("║" + " UMALOGI Alpha-Payout ガチ投資シミュレーション 2025年 ".center(68) + "║")
    print("╠" + "═" * 68 + "╣")
    rows = [
        ("モデル",             "AlphaPayoutModel (複勝EV直接回帰)"),
        ("学習期間",           "2024年 (カンニング排除)"),
        ("テスト期間",         "2025年全52週"),
        ("EV閾値",             f"pred_ev > {threshold:.2f}"),
        ("日平均シグナル数",   f"{avg_sig:.1f} 件/日"),
        ("初期資金",           f"¥{initial:,}"),
    ]
    for lbl, val in rows:
        print(f"║  {lbl:<22} {val:<44}║")
    print("╠" + "═" * 68 + "╣")
    hdr = f"{'指標':<24} {'Pattern 1: 単利固定':>19}  {'Pattern 2: 複利2%':>18}"
    print(f"║  {hdr}  ║")
    print("╠" + "═" * 68 + "╣")
    rows2 = [
        ("最終残高",         f"¥{p1.final_balance:>12,.0f}", f"¥{p2.final_balance:>12,.0f}"),
        ("損益",             f"¥{p1.net_profit:>+12,.0f}", f"¥{p2.net_profit:>+12,.0f}"),
        ("ROI",              f"{p1.roi:>14.1f}%",            f"{p2.roi:>14.1f}%"),
        ("ベット件数",       f"{p1.n_bets:>15,}",            f"{p2.n_bets:>15,}"),
        ("的中率",           f"{p1.hit_rate:>14.1f}%",       f"{p2.hit_rate:>14.1f}%"),
        ("最大ドローダウン", f"{p1.max_drawdown_pct:>14.1f}%", f"{p2.max_drawdown_pct:>14.1f}%"),
        ("最大連続負け",     f"{p1.max_consec_loss:>13}連敗", f"{p2.max_consec_loss:>13}連敗"),
    ]
    for lbl, v1, v2 in rows2:
        print(f"║  {lbl:<24} {v1:>19}  {v2:>18}  ║")
    print("╠" + "═" * 68 + "╣")
    v1 = "✅ 黒字" if p1.net_profit > 0 else "❌ 赤字"
    v2 = "✅ 黒字" if p2.net_profit > 0 else "❌ 赤字"
    print(f"║  {'評価':<24} {v1:>19}  {v2:>18}  ║")

    # 必要資金推定（ROIが1未満の場合のみ）
    if p1.roi < 100 and p1.n_bets > 0:
        loss_per_bet = (100 - p1.roi) / 100 * _FIXED
        needed_days = n_days
        daily_bets  = n_signals / max(n_days, 1)
        # 1年間の総損失 = 日損失 × 日数
        daily_loss = loss_per_bet * daily_bets
        needed_capital = int(daily_loss * needed_days + 1)
        print(f"║  {'必要資金(単利全期間)':24} ¥{needed_capital:>18,}{'':>20}║")

    print("╚" + "═" * 68 + "╝")
    print()


# ── main ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Alpha-Payout ガチ投資シミュレーション")
    ap.add_argument("--optuna-trials", type=int, default=20,
                    help="Optuna 試行回数 (デフォルト20)")
    ap.add_argument("--no-optuna",    action="store_true",
                    help="Optuna を使わず高速実行 (デフォルト閾値使用)")
    args = ap.parse_args()

    n_optuna = 1 if args.no_optuna else args.optuna_trials

    print()
    print("=" * 68)
    print("  UMALOGI Alpha-Payout バックテスト — ガチ投資シミュレーション")
    print(f"  初期資金: ¥{_INITIAL:,}  |  Optuna: {n_optuna}試行")
    print("  walk-forward: 2024学習 → 2025テスト  (カンニング排除)")
    print("=" * 68)

    conn = sqlite3.connect(str(_DB_PATH))
    research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None
    if research_db:
        print(f"  Research DB: {research_db.name}")

    print()
    print("[Phase 1] AlphaPayoutModel 学習・予測...", flush=True)
    try:
        bets_df, threshold, _ = _load_bet_rows(conn, n_optuna, research_db)
    finally:
        conn.close()

    if bets_df.empty:
        print(f"  買いシグナルが0件 (pred_ev > {threshold:.2f})")
        return

    n_signals = len(bets_df)
    n_days    = bets_df["date"].nunique()
    d_min     = bets_df["date"].min()
    d_max     = bets_df["date"].max()
    overall_hit  = int(bets_df.get("is_place", bets_df.get("is_hit", pd.Series([0]*len(bets_df)))).sum())
    overall_rate = overall_hit / n_signals * 100

    print()
    print(f"  シグナル期間 : {d_min} 〜 {d_max}  ({n_days}日間)")
    print(f"  総シグナル数 : {n_signals:,} 件  日平均: {n_signals/n_days:.1f} 件/日")
    print(f"  全シグナル的中率: {overall_rate:.1f}%  ({overall_hit}/{n_signals})")

    # Pattern 1/2 資金が続く限り全シグナルにベット
    print()
    print("[Phase 2] Pattern 1: 単利固定 ¥1,000/bet...", flush=True)
    p1 = _simulate(bets_df, "fixed", _INITIAL, _FIXED)

    print("[Phase 3] Pattern 2: 複利 残高2%/bet...", flush=True)
    p2 = _simulate(bets_df, "compound", _INITIAL, frac=_FRAC)

    _print_detail(p1, _INITIAL)
    _print_detail(p2, _INITIAL)
    _print_summary(p1, p2, _INITIAL, threshold, n_signals, n_days)

    # --- 補足: 全シグナルに100円ずつ張った場合のROI（資金制約なし参考値）
    print("  【参考】資金制約なし ¥100固定ベット (全シグナル消化した場合の理論ROI)")
    total_100 = n_signals * 100
    payout_col = "is_place" if "is_place" in bets_df.columns else "is_hit"
    payout_100 = float(
        (bets_df[payout_col] * bets_df["actual_payout"]).sum()
    )
    roi_100 = payout_100 / total_100 * 100 if total_100 > 0 else 0
    print(f"  全{n_signals:,}件 ×¥100 → 投資¥{total_100:,}  払戻¥{payout_100:,.0f}  ROI {roi_100:.1f}%")
    print()


if __name__ == "__main__":
    main()
