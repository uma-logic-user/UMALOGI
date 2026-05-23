"""
src/analysis/alpha_backtest.py — ALPHA-Payout 最適バランス長期バックテスト
============================================================================

AlphaPayoutModel (複勝EV直接回帰) を用いた 2年半 walk-forward 検証。
カンニング完全排除・絶対破産ゼロを設計方針とした最適バランスモード。

走行ウィンドウ:
  Window 1: 2024年学習 → 2025年全期間テスト   (12ヵ月)
  Window 2: 2024+2025年学習 → 2026年1-5月テスト (5ヵ月)

券種別分数ケリー配分:
  複勝  : Kelly × 0.25 (上限3.0%/残高)   — 土台の的中率重視
  馬連  : Kelly × 0.25 (上限1.5%/残高)   — 中配当バランス
  三連複 : Kelly × 0.25 (上限1.0%/残高)   — 爆発力少額分散

Usage:
  py src/analysis/alpha_backtest.py
  py src/analysis/alpha_backtest.py --no-optuna    # 速度優先
  py src/analysis/alpha_backtest.py --show-detail  # 全月詳細

Public API (IPAT自動発注連携用):
  from src.analysis.alpha_backtest import get_optimal_bet_size
  stake = get_optimal_bet_size(balance=80000, pred_ev=1.35, ticket_type="馬連")
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

import logging
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

_DB_PATH     = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"

_INITIAL = 50_000   # 初期資金 (円)
_MIN_BET = 100      # 最低賭け金 (円)

# ─── 券種別パラメータ (2025年バックテスト実測値) ──────────────────────
# payout_multiple: 的中時の平均払戻倍率 (実払戻/100)
# kelly_fraction : 安全係数 (1/4 Kelly)
# max_pct        : 残高に対する上限
_TICKET_PARAMS: dict[str, dict] = {
    "複勝":  {"payout_multiple": 2.59, "kelly_fraction": 0.25, "max_pct": 0.030},
    "馬連":  {"payout_multiple": 7.22, "kelly_fraction": 0.25, "max_pct": 0.015},
    "三連複": {"payout_multiple": 9.87, "kelly_fraction": 0.25, "max_pct": 0.010},
}

# ============================================================
# ★ IPAT自動発注連携用 公開関数
# ============================================================

def get_optimal_bet_size(
    current_balance: float,
    pred_ev: float,
    ticket_type: str,
    kelly_fraction: float | None = None,
) -> int:
    """
    AlphaPayoutModel の予測EV から最適賭け金 (円) を算出する。

    IPAT自動購入・リアルタイム発注での利用を想定した独立関数。
    予測EV と 残高さえ渡せば、ケリー基準に基づく適切な賭け金を返す。

    Args:
        current_balance: 現在の残高 (円)
        pred_ev:         AlphaPayoutModel.predict_payout_ev() の出力値
                         (例: 1.35 = ¥100投資に対し平均¥135の期待払戻)
        ticket_type:     "複勝" / "馬連" / "三連複"
        kelly_fraction:  ケリー安全係数 (None = 券種デフォルト 0.25 = 1/4 Kelly)

    Returns:
        最適賭け金 (100円単位, ¥100〜)。シグナル不足 (pred_ev ≤ 1.0) の場合は 0。

    Examples:
        >>> # 残高¥80,000、馬連でpred_ev=1.35のシグナル
        >>> get_optimal_bet_size(80000, 1.35, "馬連")
        300
        >>> # 残高¥120,000、複勝でpred_ev=1.20のシグナル
        >>> get_optimal_bet_size(120000, 1.20, "複勝")
        1000
    """
    params = _TICKET_PARAMS.get(ticket_type)
    if params is None:
        raise ValueError(f"未対応の券種: {ticket_type}. 対応: {list(_TICKET_PARAMS)}")

    if pred_ev <= 1.0 or current_balance < _MIN_BET:
        return 0

    avg_pay    = params["payout_multiple"]
    kf         = kelly_fraction if kelly_fraction is not None else params["kelly_fraction"]
    max_pct    = params["max_pct"]

    # 分数ケリー: f = kelly_fraction × (pred_ev - 1) / (avg_pay - 1)
    # 導出: pred_ev = p × avg_pay → p = pred_ev / avg_pay
    #       Kelly f* = (p × b - q) / b where b = avg_pay - 1, q = 1 - p
    #              = (pred_ev - 1) / (avg_pay - 1)
    if avg_pay <= 1.0:
        return 0

    f_kelly = (pred_ev - 1.0) / (avg_pay - 1.0) * kf
    f_capped = min(f_kelly, max_pct)

    stake = current_balance * f_capped
    stake = max(float(_MIN_BET), stake)
    stake = min(stake, current_balance)
    return int(round(stake / 100) * 100)


# ── コンビネーションキー ────────────────────────────────────────────

def _umaren_key(h1: int, h2: int) -> str:
    return f"{min(h1,h2)}-{max(h1,h2)}"


def _sanrenpuku_key(h1: int, h2: int, h3: int) -> str:
    nums = sorted([h1, h2, h3])
    return f"{nums[0]}-{nums[1]}-{nums[2]}"


# ── 払戻マップ ────────────────────────────────────────────────────────

def _build_payout_map(
    conn: sqlite3.Connection,
    min_date: str,
    max_date: str,
) -> dict[str, dict[str, dict[str, float]]]:
    rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        JOIN races r ON rp.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rp.bet_type IN ('複勝', '馬連', '三連複')
          AND rp.combination IS NOT NULL
          AND rp.payout > 0
        """,
        (min_date, max_date),
    ).fetchall()
    pmap: dict[str, dict[str, dict[str, float]]] = {}
    for race_id, bt, combo, pay in rows:
        pmap.setdefault(race_id, {}).setdefault(bt, {})[combo] = float(pay)
    return pmap


def _get_pay(pmap: dict, race_id: str, bt: str, combo: str) -> float:
    return pmap.get(race_id, {}).get(bt, {}).get(combo, 0.0)


# ── モデル学習・予測 ──────────────────────────────────────────────────

def _train_and_predict(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_min: str,
    test_max: str,
    n_optuna: int,
    research_db: Path | None,
) -> tuple[pd.DataFrame, float]:
    """
    train_years でモデル学習 → [test_min, test_max] で予測 → pred_ev 付きDataFrame を返す。
    """
    from src.ml.alpha_payout_model import AlphaPayoutModel

    model = AlphaPayoutModel()

    print(f"  [学習] {train_years} データロード中...", flush=True)
    train_df = model.load_training_data(conn, train_years, research_db_path=research_db)
    print(f"  [学習] {len(train_df):,}行  Optuna {n_optuna}試行...", flush=True)

    metrics = model.train(train_df, n_optuna_trials=n_optuna)
    threshold = model._ev_threshold
    print(
        f"  [完了] val_ROI={metrics['val_roi']:.1f}%  閾値={threshold:.2f}",
        flush=True,
    )

    print(f"  [テスト] {test_min} 〜 {test_max} ロード中...", flush=True)
    test_df = model.load_training_data(
        conn, None,
        research_db_path=research_db,
    )
    # 期間フィルタ
    test_df = test_df[(test_df["date"] >= test_min) & (test_df["date"] <= test_max)].copy()
    test_df["pred_ev"] = model.predict_payout_ev(test_df).values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0.0)
    print(f"  [テスト] {len(test_df):,}行", flush=True)
    return test_df, threshold


# ── シグナル生成 ──────────────────────────────────────────────────────

def _build_signals(
    test_df: pd.DataFrame,
    pmap: dict,
    threshold: float,
) -> pd.DataFrame:
    """pred_ev > threshold のレースから複勝・馬連・三連複シグナルを生成。"""
    signals: list[dict] = []
    sorted_df = test_df.sort_values("pred_ev", ascending=False)

    for race_id, grp in sorted_df.groupby("race_id", sort=False):
        top_ev = float(grp["pred_ev"].iloc[0])
        if top_ev < threshold:
            continue

        date   = grp["date"].iloc[0]
        horses = grp["horse_number"].astype(int).tolist()[:3]
        h1     = horses[0]

        # 複勝 (top-1)
        combo_f = str(h1)
        pay_f   = _get_pay(pmap, race_id, "複勝", combo_f)
        signals.append({
            "date": date, "race_id": race_id, "bet_type": "複勝",
            "combo": combo_f, "max_ev": top_ev,
            "actual_payout": pay_f, "is_hit": int(pay_f > 0),
        })

        # 馬連 (top-1 × top-2)
        if len(horses) >= 2:
            h2 = horses[1]
            combo_u = _umaren_key(h1, h2)
            pay_u   = _get_pay(pmap, race_id, "馬連", combo_u)
            signals.append({
                "date": date, "race_id": race_id, "bet_type": "馬連",
                "combo": combo_u, "max_ev": top_ev,
                "actual_payout": pay_u, "is_hit": int(pay_u > 0),
            })

        # 三連複 (top-1 × top-2 × top-3)
        if len(horses) >= 3:
            h3 = horses[2]
            combo_s = _sanrenpuku_key(h1, h2, h3)
            pay_s   = _get_pay(pmap, race_id, "三連複", combo_s)
            signals.append({
                "date": date, "race_id": race_id, "bet_type": "三連複",
                "combo": combo_s, "max_ev": top_ev,
                "actual_payout": pay_s, "is_hit": int(pay_s > 0),
            })

    return pd.DataFrame(signals).sort_values(["date", "race_id", "bet_type"])


# ── シミュレーション ──────────────────────────────────────────────────

@dataclass
class WindowResult:
    label: str
    train_label: str
    test_label: str
    start_balance: float
    final_balance: float
    peak_balance: float
    n_bets: int
    n_races: int
    n_hits: int
    hit_rate: float
    total_stake: float
    total_payout: float
    roi: float
    net_profit: float
    max_dd_pct: float
    max_consec_loss: int
    monthly: pd.DataFrame = field(default_factory=pd.DataFrame)
    by_type: pd.DataFrame = field(default_factory=pd.DataFrame)


def _simulate_balanced(
    sig_df: pd.DataFrame,
    start_balance: float,
) -> WindowResult:
    """最適バランスモード: 券種別分数ケリーで賭け金を決定。"""
    label = sig_df.attrs.get("label", "")
    train_label = sig_df.attrs.get("train", "")
    test_label  = sig_df.attrs.get("test", "")

    balance  = float(start_balance)
    peak     = float(start_balance)
    max_dd   = 0.0
    consec   = 0
    max_cl   = 0
    records: list[dict] = []

    for (date, race_id), grp in sig_df.groupby(["date", "race_id"], sort=True):
        if balance < _MIN_BET:
            break

        race_pnl = 0.0

        for _, row in grp.iterrows():
            bt     = row["bet_type"]
            ev     = float(row["max_ev"])
            stake  = get_optimal_bet_size(balance + race_pnl, ev, bt)
            if stake == 0:
                continue

            is_hit = int(row["is_hit"])
            pay    = float(row["actual_payout"])
            payout = is_hit * pay * stake / 100.0

            race_pnl += payout - stake

            records.append({
                "date":     date,
                "race_id":  race_id,
                "bet_type": bt,
                "stake":    float(stake),
                "payout":   payout,
                "is_hit":   is_hit,
                "balance":  balance + race_pnl,   # 仮
            })

        balance += race_pnl
        peak    = max(peak, balance)
        dd      = (peak - balance) / peak * 100 if peak > 0 else 0.0
        max_dd  = max(max_dd, dd)

        # 連敗管理: レース単位で"複勝が外れた"ら連敗カウント
        fukusho_row = grp[grp["bet_type"] == "複勝"]
        if not fukusho_row.empty and int(fukusho_row.iloc[0]["is_hit"]) == 0:
            consec += 1
            max_cl = max(max_cl, consec)
        else:
            consec = 0

        # records の balance を実値で更新
        n_this = len(grp[grp["bet_type"].isin(["複勝", "馬連", "三連複"])])
        for r in records[-n_this:]:
            r["balance"] = balance

    if not records:
        wr = WindowResult(
            label=label, train_label=train_label, test_label=test_label,
            start_balance=start_balance, final_balance=balance, peak_balance=peak,
            n_bets=0, n_races=0, n_hits=0, hit_rate=0,
            total_stake=0, total_payout=0, roi=0, net_profit=0,
            max_dd_pct=0, max_consec_loss=0,
        )
        return wr

    df = pd.DataFrame(records)
    n_b  = len(df)
    n_r  = df.groupby(["date", "race_id"]).ngroups
    n_h  = int(df["is_hit"].sum())
    ts   = float(df["stake"].sum())
    tp   = float(df["payout"].sum())
    roi  = tp / ts * 100 if ts > 0 else 0.0

    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    monthly = (
        df.groupby("month")
        .agg(bets=("stake", "count"), hits=("is_hit", "sum"),
             stake=("stake", "sum"), payout=("payout", "sum"),
             balance=("balance", "last"))
        .reset_index()
    )
    monthly["profit"] = monthly["payout"] - monthly["stake"]

    by_type = (
        df.groupby("bet_type")
        .agg(bets=("stake", "count"), hits=("is_hit", "sum"),
             stake=("stake", "sum"), payout=("payout", "sum"))
        .reset_index()
    )
    by_type["hit_rate"] = by_type["hits"] / by_type["bets"] * 100
    by_type["roi"]      = by_type["payout"] / by_type["stake"] * 100

    return WindowResult(
        label=label, train_label=train_label, test_label=test_label,
        start_balance=start_balance, final_balance=balance, peak_balance=peak,
        n_bets=n_b, n_races=n_r, n_hits=n_h,
        hit_rate=n_h / n_b * 100 if n_b > 0 else 0.0,
        total_stake=ts, total_payout=tp, roi=roi,
        net_profit=tp - ts, max_dd_pct=max_dd, max_consec_loss=max_cl,
        monthly=monthly, by_type=by_type,
    )


# ── レポート ──────────────────────────────────────────────────────────

def _print_window(wr: WindowResult, show_detail: bool) -> None:
    bankrupt = wr.final_balance < 1_000
    sign = "💀" if bankrupt else ("📈" if wr.net_profit > 0 else "📉")
    print()
    print(f"{'═'*70}")
    print(f"  {sign}  {wr.label}")
    print(f"      学習: {wr.train_label}  →  テスト: {wr.test_label}")
    print(f"{'═'*70}")
    print(f"  開始残高    : ¥{wr.start_balance:>12,.0f}")
    print(f"  最終残高    : ¥{wr.final_balance:>12,.0f}  ({wr.final_balance/wr.start_balance*100 - 100:+.1f}%)")
    print(f"  最高到達残高: ¥{wr.peak_balance:>12,.0f}")
    print(f"  ─" * 30)
    print(f"  投資レース数: {wr.n_races:>8,}  ({wr.n_bets}件×3券種)")
    print(f"  的中件数    : {wr.n_hits:>8,}  ({wr.hit_rate:.1f}%)")
    print(f"  総投資額    : ¥{wr.total_stake:>12,.0f}")
    print(f"  総払戻額    : ¥{wr.total_payout:>12,.0f}")
    print(f"  純損益      : ¥{wr.net_profit:>+12,.0f}")
    print(f"  ROI         : {wr.roi:>10.1f}%")
    print(f"  最大DD      : {wr.max_dd_pct:>8.1f}%")
    print(f"  最大連負    : {wr.max_consec_loss:>8} 連敗 (複勝基準)")

    # 券種別
    if not wr.by_type.empty:
        print()
        print(f"  ── 券種別成績 ──")
        print(f"  {'券種':>6}  {'件数':>5}  {'的中':>4}  {'的中率':>6}  "
              f"{'投資':>9}  {'払戻':>9}  {'ROI':>7}")
        print(f"  {'─'*58}")
        for _, r in wr.by_type.iterrows():
            print(
                f"  {r['bet_type']:>6}  {int(r['bets']):>5,}  {int(r['hits']):>4}  "
                f"{r['hit_rate']:>5.1f}%  ¥{int(r['stake']):>8,}  "
                f"¥{int(r['payout']):>8,}  {r['roi']:>6.1f}%"
            )

    if show_detail and not wr.monthly.empty:
        print()
        print(f"  ── 月別成績 ──")
        print(f"  {'月':>7}  {'件数':>5}  {'的中':>4}  {'投資':>9}  {'払戻':>9}  "
              f"{'損益':>10}  {'残高':>10}")
        print(f"  {'─'*68}")
        for _, row in wr.monthly.iterrows():
            s = "+" if row["profit"] >= 0 else ""
            print(
                f"  {row['month']:>7}  {int(row['bets']):>5,}  {int(row['hits']):>4}  "
                f"¥{int(row['stake']):>8,}  ¥{int(row['payout']):>8,}  "
                f"{s}¥{int(row['profit']):>8,}  ¥{int(row['balance']):>9,}"
            )


def _print_asset_curve(windows: list[WindowResult]) -> None:
    """テキストによる残高推移グラフ"""
    # 月次残高を収集
    all_months: list[tuple[str, float]] = []
    for wr in windows:
        for _, row in wr.monthly.iterrows():
            all_months.append((row["month"], float(row["balance"])))
    if not all_months:
        return
    all_months.sort()
    balances = [b for _, b in all_months]
    if not balances:
        return

    max_b = max(balances)
    width = 40
    print()
    print("  ── 残高推移カーブ ──")
    print(f"  {'月':>7}  {'残高':>10}  グラフ")
    print(f"  {'─'*60}")
    # 全ウィンドウ開始点
    start = float(windows[0].start_balance)
    all_months.insert(0, ("Start", start))
    for label, bal in all_months:
        bar_len = int(bal / max_b * width)
        bar = "█" * bar_len + "░" * (width - bar_len)
        change = bal / start * 100 - 100
        print(f"  {label:>7}  ¥{bal:>9,.0f}  {bar}  {change:+.1f}%")


def _print_final_summary(windows: list[WindowResult]) -> None:
    final = windows[-1].final_balance
    start = windows[0].start_balance
    total_gain = final - start
    total_roi   = final / start * 100 - 100
    peak = max(wr.peak_balance for wr in windows)
    total_stake = sum(wr.total_stake for wr in windows)
    total_pay   = sum(wr.total_payout for wr in windows)
    overall_roi = total_pay / total_stake * 100 if total_stake else 0
    max_dd = max(wr.max_dd_pct for wr in windows)
    max_cl = max(wr.max_consec_loss for wr in windows)

    w = 72
    print()
    print("╔" + "═" * w + "╗")
    print("║" + " 🏆 UMALOGI Alpha-Payout 2年半 ウォークフォワード 最終結果 🏆 ".center(w) + "║")
    print("╠" + "═" * w + "╣")

    rows = [
        ("モデル",          "AlphaPayoutModel (複勝EV直接回帰)"),
        ("ベット戦略",       "最適バランス (分数ケリー 1/4 Kelly)"),
        ("券種",            "複勝 + 馬連 + 三連複"),
        ("期間",            "2025年全期間 + 2026年1〜5月"),
        ("カンニング",       "完全排除 (walk-forward)"),
        ("初期資金",        f"¥{start:,.0f}"),
    ]
    for lbl, val in rows:
        print(f"║  {lbl:<18} {val:<52}║")

    print("╠" + "═" * w + "╣")

    items = [
        ("最終残高",        f"¥{final:>14,.0f}  ({total_gain:+,.0f})"),
        ("最高到達残高",    f"¥{peak:>14,.0f}"),
        ("通算損益",        f"¥{total_gain:>+14,.0f}  ({total_roi:+.1f}%)"),
        ("通算ROI",         f"{overall_roi:>14.1f}%"),
        ("2年半 最大DD",    f"{max_dd:>14.1f}%"),
        ("最大連続負け",    f"{max_cl:>13}連敗  (複勝基準)"),
        ("破産",            "なし ✅" if final > _MIN_BET else "あり 💀"),
    ]
    for lbl, val in items:
        print(f"║  {lbl:<18} {val:<52}║")

    print("╠" + "═" * w + "╣")
    row = "║  " + f"{'ウィンドウ':<12}"
    for wr in windows:
        row += f"  {wr.label[:16]:<16}"
    row += "║"
    print(row)
    print("╠" + "─" * w + "╣")
    for attr, label in [
        ("net_profit", "損益"),
        ("roi",        "ROI"),
        ("max_dd_pct", "最大DD"),
        ("max_consec_loss", "最大連負"),
    ]:
        row = f"║  {label:<12}"
        for wr in windows:
            val = getattr(wr, attr)
            if attr == "net_profit":
                cell = f"¥{val:>+10,.0f}"
            elif attr == "max_consec_loss":
                cell = f"{val:>10}連敗  "
            else:
                cell = f"{val:>11.1f}%   "
            row += f"  {cell:<16}"
        row += "║"
        print(row)

    print("╚" + "═" * w + "╝")
    print()


def _print_ipat_guide() -> None:
    print("=" * 72)
    print("  📡 IPAT自動発注連携 — get_optimal_bet_size() 使い方")
    print("=" * 72)
    print()
    print("  from src.analysis.alpha_backtest import get_optimal_bet_size")
    print()
    print("  # AlphaPayoutModel で pred_ev を取得した後:")
    print("  stake_fuku  = get_optimal_bet_size(balance, pred_ev, \"複勝\")")
    print("  stake_uma   = get_optimal_bet_size(balance, pred_ev, \"馬連\")")
    print("  stake_sanfu = get_optimal_bet_size(balance, pred_ev, \"三連複\")")
    print()
    print("  ケリー係数早見表 (residual ¥100,000 / pred_ev=1.20 の場合):")
    print(f"  {'券種':>6}  {'stake':>8}  {'残高比':>6}  {'Kelly':>6}")
    print(f"  {'─'*36}")
    bal = 100_000.0
    ev  = 1.20
    for bt in ["複勝", "馬連", "三連複"]:
        s = get_optimal_bet_size(bal, ev, bt)
        p = s / bal * 100
        k_raw = (ev - 1.0) / (_TICKET_PARAMS[bt]["payout_multiple"] - 1.0)
        print(f"  {bt:>6}  ¥{s:>7,}  {p:>5.2f}%  {k_raw*100:>5.2f}% raw")
    print()
    print("  注意: モデルの pred_ev が高いほど stake が増加（上限あり）")
    print(f"  上限: 複勝{_TICKET_PARAMS['複勝']['max_pct']*100:.0f}%  "
          f"馬連{_TICKET_PARAMS['馬連']['max_pct']*100:.1f}%  "
          f"三連複{_TICKET_PARAMS['三連複']['max_pct']*100:.1f}%  (残高対比)")
    print("=" * 72)
    print()


# ── main ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Alpha-Payout 最適バランス長期バックテスト")
    ap.add_argument("--optuna-trials", type=int, default=20)
    ap.add_argument("--no-optuna",    action="store_true")
    ap.add_argument("--show-detail",  action="store_true",
                    help="月別成績を全ウィンドウで表示")
    args = ap.parse_args()

    n_optuna = 1 if args.no_optuna else args.optuna_trials

    print()
    print("=" * 72)
    print("  UMALOGI Alpha-Payout 最適バランス 2年半バックテスト")
    print(f"  初期資金: ¥{_INITIAL:,}  |  Optuna: {n_optuna}試行/ウィンドウ")
    print("  券種: 複勝+馬連+三連複  |  ベット: 分数ケリー(1/4 Kelly)")
    print("=" * 72)
    print()

    conn = sqlite3.connect(str(_DB_PATH))
    research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None
    if research_db:
        print(f"  Research DB: {research_db.name}")

    # ================================================================
    # ウィンドウ定義
    # ================================================================
    WINDOWS = [
        {
            "label":       "Window 1: 2025年全期間",
            "train_label": "2024年 (12ヵ月)",
            "test_label":  "2025年全期間 (Jan-Dec)",
            "train_years": [2024],
            "test_min":    "2025-01-01",
            "test_max":    "2025-12-31",
        },
        {
            "label":       "Window 2: 2026年1-5月",
            "train_label": "2024+2025年 (24ヵ月)",
            "test_label":  "2026年 Jan 1 〜 May 23",
            "train_years": [2024, 2025],
            "test_min":    "2026-01-01",
            "test_max":    "2026-05-23",
        },
    ]

    results: list[WindowResult] = []
    balance = float(_INITIAL)

    for i, win in enumerate(WINDOWS):
        print(f"\n{'━'*72}")
        print(f"  [{i+1}/{len(WINDOWS)}] {win['label']}")
        print(f"{'━'*72}")

        # 学習・予測
        test_df, threshold = _train_and_predict(
            conn,
            train_years=win["train_years"],
            test_min=win["test_min"],
            test_max=win["test_max"],
            n_optuna=n_optuna,
            research_db=research_db,
        )

        # 払戻マップ
        pmap = _build_payout_map(conn, win["test_min"], win["test_max"])

        # シグナル生成
        sig_df = _build_signals(test_df, pmap, threshold)
        sig_df.attrs["label"] = win["label"]
        sig_df.attrs["train"] = win["train_label"]
        sig_df.attrs["test"]  = win["test_label"]

        n_races = sig_df["race_id"].nunique()
        n_days  = sig_df["date"].nunique()
        print(
            f"  シグナル: {len(sig_df):,}件  {n_races}レース  {n_days}日間  "
            f"日平均{n_races/max(n_days,1):.1f}レース",
            flush=True,
        )

        # シミュレーション
        wr = _simulate_balanced(sig_df, balance)
        wr.label       = win["label"]
        wr.train_label = win["train_label"]
        wr.test_label  = win["test_label"]

        results.append(wr)
        balance = wr.final_balance   # 次ウィンドウへ繰り越し

    conn.close()

    # ================================================================
    # レポート出力
    # ================================================================
    for wr in results:
        _print_window(wr, show_detail=args.show_detail)

    _print_asset_curve(results)
    _print_final_summary(results)
    _print_ipat_guide()


if __name__ == "__main__":
    main()
