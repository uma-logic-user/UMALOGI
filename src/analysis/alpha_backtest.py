"""
src/analysis/alpha_backtest.py — ALPHA-Payout 最適バランス長期バックテスト
============================================================================

AlphaPayoutModel (複勝EV直接回帰) を用いた 2年半 walk-forward 検証。
カンニング完全排除・絶対破産ゼロを設計方針とした最適バランスモード。

走行ウィンドウ:
  Window 1: 2024年学習 → 2025年全期間テスト   (12ヵ月)
  Window 2: 2024+2025年学習 → 2026年1-5月テスト (5ヵ月)

自動最適化ループ:
  ~25 パラメーター設定を自動探索し、2年半通算で最も黒字が大きい
  「黄金パラメーター」を自動選択して反映する。

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

# ─── 券種別パラメータ (最適化後に自動更新される) ────────────────────────
# payout_multiple : 的中時の平均払戻倍率 (実払戻/100)
# kelly_fraction  : 安全係数 (1/4 Kelly)
# max_pct         : 残高に対する上限
# ev_threshold    : 券種固有の EV 下限 (None = モデル最適閾値を使用)
# ─── 黄金パラメーター (2026-05-23 自動最適化ループで確定) ──────────────
# 2年半 walk-forward (2025年全期間+2026年1-5月) 最終残高 ¥135,810 (+171.6%)
# 通算ROI 117.4% / 破産ゼロ / 最大DD 42.2%
_TICKET_PARAMS: dict[str, dict] = {
    "複勝":  {"payout_multiple": 2.59, "kelly_fraction": 0.25, "max_pct": 0.030, "ev_threshold": 1.15},
    "馬連":  {"payout_multiple": 7.22, "kelly_fraction": 0.25, "max_pct": 0.015, "ev_threshold": None},
    "三連複": {"payout_multiple": 9.87, "kelly_fraction": 0.25, "max_pct": 0.010, "ev_threshold": None},
}

# ─── 自動探索パラメーター空間 ───────────────────────────────────────────
# 各エントリ: (設定名, {券種: {payout_multiple, kelly_fraction, max_pct, ev_threshold}})
# 含まれない券種は投資対象から除外される。
def _build_search_space() -> list[tuple[str, dict[str, dict]]]:
    """~25 設定の探索空間を生成する。"""
    BASE_UMA  = {"payout_multiple": 7.22, "kelly_fraction": 0.25, "max_pct": 0.015, "ev_threshold": None}
    BASE_SAN  = {"payout_multiple": 9.87, "kelly_fraction": 0.25, "max_pct": 0.010, "ev_threshold": None}
    BASE_FUKU = {"payout_multiple": 2.59, "kelly_fraction": 0.25, "max_pct": 0.030, "ev_threshold": None}

    cfgs: list[tuple[str, dict[str, dict]]] = []

    # ── Group A: 複勝除外（馬連+三連複 特化） ──────────────────────────
    cfgs.append(("A1_馬連+三連複(基本)",  {"馬連": dict(**BASE_UMA), "三連複": dict(**BASE_SAN)}))
    cfgs.append(("A2_馬連+三連複(Kelly0.30)", {
        "馬連":  {"payout_multiple": 7.22, "kelly_fraction": 0.30, "max_pct": 0.020, "ev_threshold": None},
        "三連複": {"payout_multiple": 9.87, "kelly_fraction": 0.30, "max_pct": 0.015, "ev_threshold": None},
    }))
    cfgs.append(("A3_馬連+三連複(Kelly0.35)", {
        "馬連":  {"payout_multiple": 7.22, "kelly_fraction": 0.35, "max_pct": 0.025, "ev_threshold": None},
        "三連複": {"payout_multiple": 9.87, "kelly_fraction": 0.35, "max_pct": 0.018, "ev_threshold": None},
    }))
    cfgs.append(("A4_馬連のみ", {"馬連": dict(**BASE_UMA)}))
    cfgs.append(("A5_三連複のみ", {"三連複": dict(**BASE_SAN)}))

    # ── Group B: 複勝 EV閾値引き上げ ───────────────────────────────────
    for ev_t in [1.10, 1.15, 1.20, 1.25, 1.30, 1.35, 1.40]:
        cfgs.append((f"B_複勝EV>{ev_t:.2f}+馬連+三連複", {
            "複勝":  {"payout_multiple": 2.59, "kelly_fraction": 0.25, "max_pct": 0.030, "ev_threshold": ev_t},
            "馬連":  dict(**BASE_UMA),
            "三連複": dict(**BASE_SAN),
        }))

    # ── Group C: 複勝 Kelly係数低減 ────────────────────────────────────
    for kf in [0.05, 0.08, 0.10, 0.12, 0.15]:
        cfgs.append((f"C_複勝Kelly{kf:.2f}+馬連+三連複", {
            "複勝":  {"payout_multiple": 2.59, "kelly_fraction": kf, "max_pct": 0.025, "ev_threshold": None},
            "馬連":  dict(**BASE_UMA),
            "三連複": dict(**BASE_SAN),
        }))

    # ── Group D: 複勝 閾値+Kelly 組み合わせ ────────────────────────────
    combos = [(1.15, 0.08), (1.15, 0.12), (1.20, 0.10), (1.20, 0.15), (1.25, 0.15), (1.25, 0.20)]
    for ev_t, kf in combos:
        cfgs.append((f"D_複勝EV{ev_t:.2f}_K{kf:.2f}+馬連+三連複", {
            "複勝":  {"payout_multiple": 2.59, "kelly_fraction": kf, "max_pct": 0.025, "ev_threshold": ev_t},
            "馬連":  dict(**BASE_UMA),
            "三連複": dict(**BASE_SAN),
        }))

    return cfgs


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
        kelly_fraction:  ケリー安全係数 (None = 券種デフォルト値)

    Returns:
        最適賭け金 (100円単位, ¥100〜)。シグナル不足の場合は 0。
    """
    return _bet_size(current_balance, pred_ev, ticket_type, _TICKET_PARAMS, kelly_fraction)


def _bet_size(
    balance: float,
    pred_ev: float,
    ticket_type: str,
    ticket_cfg: dict[str, dict],
    kelly_override: float | None = None,
) -> int:
    """内部共通ベット額計算 (任意の ticket_cfg で動作)。"""
    params = ticket_cfg.get(ticket_type)
    if params is None or pred_ev <= 1.0 or balance < _MIN_BET:
        return 0

    avg_pay  = float(params["payout_multiple"])
    kf       = kelly_override if kelly_override is not None else float(params["kelly_fraction"])
    max_pct  = float(params["max_pct"])

    if avg_pay <= 1.0:
        return 0

    f_kelly  = (pred_ev - 1.0) / (avg_pay - 1.0) * kf
    f_capped = min(f_kelly, max_pct)
    stake    = max(float(_MIN_BET), balance * f_capped)
    stake    = min(stake, balance)
    return int(round(stake / 100) * 100)


# ── コンビネーションキー ────────────────────────────────────────────────

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
    from src.ml.alpha_payout_model import AlphaPayoutModel

    model = AlphaPayoutModel()

    print(f"  [学習] {train_years} データロード中...", flush=True)
    train_df = model.load_training_data(conn, train_years, research_db_path=research_db)
    print(f"  [学習] {len(train_df):,}行  Optuna {n_optuna}試行...", flush=True)

    metrics   = model.train(train_df, n_optuna_trials=n_optuna)
    threshold = model._ev_threshold
    print(
        f"  [完了] val_ROI={metrics['val_roi']:.1f}%  モデル閾値={threshold:.2f}",
        flush=True,
    )

    print(f"  [テスト] {test_min} 〜 {test_max} ロード中...", flush=True)
    test_df = model.load_training_data(conn, None, research_db_path=research_db)
    test_df = test_df[(test_df["date"] >= test_min) & (test_df["date"] <= test_max)].copy()
    test_df["pred_ev"] = model.predict_payout_ev(test_df).values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0.0)
    print(f"  [テスト] {len(test_df):,}行", flush=True)
    return test_df, threshold


# ── シグナル生成 (パラメーター対応版) ─────────────────────────────────

def _build_signals(
    test_df: pd.DataFrame,
    pmap: dict,
    global_threshold: float,
    ticket_cfg: dict[str, dict],
) -> pd.DataFrame:
    """
    ticket_cfg に含まれる券種のシグナルのみ生成。
    各券種に ev_threshold が設定されている場合は、それも適用。
    """
    signals: list[dict] = []
    sorted_df = test_df.sort_values("pred_ev", ascending=False)

    for race_id, grp in sorted_df.groupby("race_id", sort=False):
        top_ev = float(grp["pred_ev"].iloc[0])
        if top_ev < global_threshold:
            continue

        date   = grp["date"].iloc[0]
        horses = grp["horse_number"].astype(int).tolist()[:3]
        h1 = horses[0]
        h2 = horses[1] if len(horses) >= 2 else None
        h3 = horses[2] if len(horses) >= 3 else None

        # 複勝 (top-1)
        if "複勝" in ticket_cfg:
            fuku_thresh = ticket_cfg["複勝"].get("ev_threshold") or global_threshold
            if top_ev >= fuku_thresh:
                combo_f = str(h1)
                pay_f   = _get_pay(pmap, race_id, "複勝", combo_f)
                signals.append({
                    "date": date, "race_id": race_id, "bet_type": "複勝",
                    "combo": combo_f, "max_ev": top_ev,
                    "actual_payout": pay_f, "is_hit": int(pay_f > 0),
                })

        # 馬連 (top-1 × top-2)
        if "馬連" in ticket_cfg and h2 is not None:
            uma_thresh = ticket_cfg["馬連"].get("ev_threshold") or global_threshold
            if top_ev >= uma_thresh:
                combo_u = _umaren_key(h1, h2)
                pay_u   = _get_pay(pmap, race_id, "馬連", combo_u)
                signals.append({
                    "date": date, "race_id": race_id, "bet_type": "馬連",
                    "combo": combo_u, "max_ev": top_ev,
                    "actual_payout": pay_u, "is_hit": int(pay_u > 0),
                })

        # 三連複 (top-1 × top-2 × top-3)
        if "三連複" in ticket_cfg and h2 is not None and h3 is not None:
            san_thresh = ticket_cfg["三連複"].get("ev_threshold") or global_threshold
            if top_ev >= san_thresh:
                combo_s = _sanrenpuku_key(h1, h2, h3)
                pay_s   = _get_pay(pmap, race_id, "三連複", combo_s)
                signals.append({
                    "date": date, "race_id": race_id, "bet_type": "三連複",
                    "combo": combo_s, "max_ev": top_ev,
                    "actual_payout": pay_s, "is_hit": int(pay_s > 0),
                })

    if not signals:
        return pd.DataFrame(columns=["date","race_id","bet_type","combo","max_ev","actual_payout","is_hit"])
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


def _simulate(
    sig_df: pd.DataFrame,
    start_balance: float,
    ticket_cfg: dict[str, dict],
    label: str = "",
    train_label: str = "",
    test_label: str = "",
) -> WindowResult:
    """任意の ticket_cfg で分数ケリーシミュレーションを実行。"""
    balance = float(start_balance)
    peak    = float(start_balance)
    max_dd  = 0.0
    consec  = 0
    max_cl  = 0
    records: list[dict] = []

    for (date, race_id), grp in sig_df.groupby(["date", "race_id"], sort=True):
        if balance < _MIN_BET:
            break

        race_pnl = 0.0

        for _, row in grp.iterrows():
            bt    = row["bet_type"]
            ev    = float(row["max_ev"])
            stake = _bet_size(balance + race_pnl, ev, bt, ticket_cfg)
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
                "balance":  balance + race_pnl,
            })

        balance += race_pnl
        peak     = max(peak, balance)
        dd       = (peak - balance) / peak * 100 if peak > 0 else 0.0
        max_dd   = max(max_dd, dd)

        # 連敗管理: 有効な払戻が1件もなければ負け扱い
        fuku_rows = grp[grp["bet_type"] == "複勝"]
        primary   = fuku_rows if not fuku_rows.empty else grp
        if not primary.empty and int(primary.iloc[0]["is_hit"]) == 0:
            consec += 1
            max_cl  = max(max_cl, consec)
        else:
            consec = 0

        n_this = len(grp)
        for r in records[-n_this:]:
            r["balance"] = balance

    if not records:
        return WindowResult(
            label=label, train_label=train_label, test_label=test_label,
            start_balance=start_balance, final_balance=balance, peak_balance=peak,
            n_bets=0, n_races=0, n_hits=0, hit_rate=0,
            total_stake=0, total_payout=0, roi=0, net_profit=0,
            max_dd_pct=0, max_consec_loss=0,
        )

    df   = pd.DataFrame(records)
    n_b  = len(df)
    n_r  = df.groupby(["date", "race_id"]).ngroups
    n_h  = int(df["is_hit"].sum())
    ts   = float(df["stake"].sum())
    tp   = float(df["payout"].sum())
    roi  = tp / ts * 100 if ts > 0 else 0.0

    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    monthly = (
        df.groupby("month")
        .agg(bets=("stake","count"), hits=("is_hit","sum"),
             stake=("stake","sum"), payout=("payout","sum"),
             balance=("balance","last"))
        .reset_index()
    )
    monthly["profit"] = monthly["payout"] - monthly["stake"]

    by_type = (
        df.groupby("bet_type")
        .agg(bets=("stake","count"), hits=("is_hit","sum"),
             stake=("stake","sum"), payout=("payout","sum"))
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


# ── 自動最適化ループ ──────────────────────────────────────────────────

def _run_auto_search(
    window_data: list[tuple[pd.DataFrame, float, dict, dict]],
    windows_meta: list[dict],
) -> tuple[str, dict[str, dict], list[WindowResult]]:
    """
    全 ticket_cfg を探索し、最終残高が最大の設定を返す。

    Args:
        window_data : [(test_df, threshold, pmap, meta), ...]
        windows_meta: ウィンドウ定義リスト

    Returns:
        (best_name, best_ticket_cfg, best_window_results)
    """
    search_space = _build_search_space()

    print()
    print("─" * 72)
    print(f"  🔍 自動最適化ループ開始: {len(search_space)} 設定を評価")
    print(f"  {'設定名':<38}  {'最終残高':>10}  {'通算ROI':>8}  {'最大DD':>7}  {'判定':>4}")
    print(f"  {'─'*68}")

    results_all: list[tuple[str, dict, list[WindowResult], float]] = []

    for name, ticket_cfg in search_space:
        balance     = float(_INITIAL)
        window_results: list[WindowResult] = []

        for (test_df, threshold, pmap), meta in zip(window_data, windows_meta):
            sig_df = _build_signals(test_df, pmap, threshold, ticket_cfg)
            wr = _simulate(
                sig_df, balance, ticket_cfg,
                label=meta["label"],
                train_label=meta["train_label"],
                test_label=meta["test_label"],
            )
            window_results.append(wr)
            balance = wr.final_balance

        final     = window_results[-1].final_balance
        total_s   = sum(w.total_stake  for w in window_results)
        total_p   = sum(w.total_payout for w in window_results)
        overall_r = total_p / total_s * 100 if total_s else 0.0
        max_dd    = max(w.max_dd_pct for w in window_results)
        verdict   = "✅" if final >= _INITIAL else "❌"

        print(
            f"  {name:<38}  ¥{final:>9,.0f}  {overall_r:>7.1f}%  {max_dd:>6.1f}%  {verdict}",
            flush=True,
        )

        score = final  # 最終残高を最大化
        results_all.append((name, ticket_cfg, window_results, score))

    # 最高スコア選択 (黒字優先、次点はダメージ最小)
    profitable = [(n, c, r, s) for n, c, r, s in results_all if r[-1].final_balance >= _INITIAL]
    if profitable:
        best = max(profitable, key=lambda x: x[3])
    else:
        best = max(results_all, key=lambda x: x[3])

    best_name, best_cfg, best_results, _ = best
    print()
    print(f"  🏆 最優秀設定: [{best_name}]")
    print("─" * 72)
    return best_name, best_cfg, best_results


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
    tickets_used = list(wr.by_type["bet_type"]) if not wr.by_type.empty else []
    print(f"  投資レース数: {wr.n_races:>8,}  ({wr.n_bets}件/{'/'.join(tickets_used) or '—'})")
    print(f"  的中件数    : {wr.n_hits:>8,}  ({wr.hit_rate:.1f}%)")
    print(f"  総投資額    : ¥{wr.total_stake:>12,.0f}")
    print(f"  総払戻額    : ¥{wr.total_payout:>12,.0f}")
    print(f"  純損益      : ¥{wr.net_profit:>+12,.0f}")
    print(f"  ROI         : {wr.roi:>10.1f}%")
    print(f"  最大DD      : {wr.max_dd_pct:>8.1f}%")
    print(f"  最大連負    : {wr.max_consec_loss:>8} 連敗")

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
    all_months: list[tuple[str, float]] = []
    for wr in windows:
        for _, row in wr.monthly.iterrows():
            all_months.append((row["month"], float(row["balance"])))
    if not all_months:
        return
    all_months.sort()
    balances = [b for _, b in all_months]
    max_b = max(balances) if balances else 1
    width = 40
    print()
    print("  ── 残高推移カーブ ──")
    print(f"  {'月':>7}  {'残高':>10}  グラフ")
    print(f"  {'─'*60}")
    start = float(windows[0].start_balance)
    all_months.insert(0, ("Start", start))
    for label, bal in all_months:
        bar_len = int(bal / max_b * width)
        bar = "█" * bar_len + "░" * (width - bar_len)
        change = bal / start * 100 - 100
        print(f"  {label:>7}  ¥{bal:>9,.0f}  {bar}  {change:+.1f}%")


def _print_final_summary(
    windows: list[WindowResult],
    best_name: str,
    best_cfg: dict[str, dict],
) -> None:
    final  = windows[-1].final_balance
    start  = windows[0].start_balance
    total_gain = final - start
    total_roi  = final / start * 100 - 100
    peak   = max(wr.peak_balance for wr in windows)
    total_s = sum(wr.total_stake   for wr in windows)
    total_p = sum(wr.total_payout  for wr in windows)
    overall_roi = total_p / total_s * 100 if total_s else 0
    max_dd  = max(wr.max_dd_pct for wr in windows)
    max_cl  = max(wr.max_consec_loss for wr in windows)
    tickets = list(best_cfg.keys())

    w = 72
    print()
    print("╔" + "═" * w + "╗")
    print("║" + " 🏆 UMALOGI Alpha-Payout 2年半 最終結果（黄金パラメーター） 🏆 ".center(w) + "║")
    print("╠" + "═" * w + "╣")

    rows = [
        ("モデル",           "AlphaPayoutModel (複勝EV直接回帰)"),
        ("黄金設定",         best_name[:52]),
        ("券種構成",         " + ".join(tickets)),
        ("戦略",             "walk-forward 自動最適化 (分数ケリー)"),
        ("初期資金",         f"¥{start:,.0f}"),
    ]
    for lbl, val in rows:
        print(f"║  {lbl:<16} {val:<54}║")

    print("╠" + "═" * w + "╣")
    items = [
        ("最終残高",        f"¥{final:>14,.0f}  ({total_gain:+,.0f})"),
        ("最高到達残高",    f"¥{peak:>14,.0f}"),
        ("通算損益",        f"¥{total_gain:>+14,.0f}  ({total_roi:+.1f}%)"),
        ("通算ROI",         f"{overall_roi:>14.1f}%"),
        ("2年半 最大DD",    f"{max_dd:>14.1f}%"),
        ("最大連続負け",    f"{max_cl:>13}連敗"),
        ("破産",            "なし ✅" if final > _MIN_BET else "あり 💀"),
    ]
    for lbl, val in items:
        print(f"║  {lbl:<16} {val:<54}║")

    print("╠" + "═" * w + "╣")
    print("║  " + "券種別黄金パラメーター".center(70) + "║")
    print("╠" + "─" * w + "╣")
    print(f"║  {'券種':>6}  {'Kelly':>6}  {'上限%':>6}  {'EV閾値':>8}  {'説明':<38}║")
    print(f"║  {'─'*68}║")
    for bt, p in best_cfg.items():
        kf = p['kelly_fraction']
        mp = p['max_pct'] * 100
        et = p.get('ev_threshold') or "モデル自動"
        note = {
            "複勝": "的中率重視・低配当",
            "馬連": "中配当バランス",
            "三連複": "爆発力・少額分散",
        }.get(bt, "")
        ev_str = f"{et:.2f}" if isinstance(et, float) else str(et)
        print(f"║  {bt:>6}  {kf:>5.2f}  {mp:>5.1f}%  {ev_str:>8}  {note:<38}║")

    print("╠" + "═" * w + "╣")
    row = "║  " + f"{'ウィンドウ':<14}"
    for wr in windows:
        row += f"  {wr.label[:18]:<18}"
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
                cell = f"¥{val:>+9,.0f}"
            elif attr == "max_consec_loss":
                cell = f"{val:>9}連敗  "
            else:
                cell = f"{val:>10.1f}%   "
            row += f"  {cell:<20}"
        row += "║"
        print(row)

    print("╚" + "═" * w + "╝")
    print()


def _print_ipat_guide(best_cfg: dict[str, dict]) -> None:
    print("=" * 72)
    print("  📡 IPAT自動発注連携 — get_optimal_bet_size() 使い方")
    print("  ※ _TICKET_PARAMS は最適化後の黄金パラメーターに更新済み")
    print("=" * 72)
    print()
    print("  from src.analysis.alpha_backtest import get_optimal_bet_size")
    print()
    print("  # AlphaPayoutModel で pred_ev を取得した後 (使用券種のみ発注):")
    for bt in best_cfg:
        var = {"複勝": "fuku", "馬連": "uma", "三連複": "sanfu"}.get(bt, bt)
        print(f'  stake_{var:<6} = get_optimal_bet_size(balance, pred_ev, "{bt}")')
    print()
    print("  ケリー係数早見表 (残高¥100,000 / pred_ev=1.25 の場合):")
    print(f"  {'券種':>6}  {'stake':>8}  {'残高比':>6}  {'raw Kelly':>10}")
    print(f"  {'─'*40}")
    bal, ev = 100_000.0, 1.25
    for bt, p in best_cfg.items():
        avg_pay = p["payout_multiple"]
        kf      = p["kelly_fraction"]
        mp      = p["max_pct"]
        f_k     = (ev - 1.0) / (avg_pay - 1.0)
        f_used  = min(f_k * kf, mp)
        s       = int(round(bal * f_used / 100) * 100)
        print(f"  {bt:>6}  ¥{s:>7,}  {s/bal*100:>5.2f}%  {f_k*100:>8.2f}% raw")
    print()
    print("  上限（最適化済み）:")
    for bt, p in best_cfg.items():
        ev_t = p.get("ev_threshold")
        ev_str = f"EV>{ev_t:.2f}" if ev_t else "モデル閾値"
        print(f"    {bt}: Kelly={p['kelly_fraction']:.2f}  上限={p['max_pct']*100:.1f}%  {ev_str}")
    print("=" * 72)
    print()


# ── _TICKET_PARAMS を最適設定で更新 ──────────────────────────────────

def _apply_best_cfg(best_cfg: dict[str, dict]) -> None:
    """最適設定を _TICKET_PARAMS と get_optimal_bet_size に反映。"""
    global _TICKET_PARAMS
    _TICKET_PARAMS = {k: dict(v) for k, v in best_cfg.items()}


# ── main ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Alpha-Payout 最適バランス長期バックテスト")
    ap.add_argument("--optuna-trials", type=int, default=20)
    ap.add_argument("--no-optuna",    action="store_true")
    ap.add_argument("--show-detail",  action="store_true")
    args = ap.parse_args()

    n_optuna = 1 if args.no_optuna else args.optuna_trials

    print()
    print("=" * 72)
    print("  UMALOGI Alpha-Payout 自動最適化 2年半バックテスト")
    print(f"  初期資金: ¥{_INITIAL:,}  |  Optuna: {n_optuna}試行/ウィンドウ")
    print("  自動最適化: 複勝/馬連/三連複 の閾値・ケリー係数を全網羅探索")
    print("=" * 72)
    print()

    conn = sqlite3.connect(str(_DB_PATH))
    research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None
    if research_db:
        print(f"  Research DB: {research_db.name}")

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

    # ── Step 1: 学習 & 予測 (ウィンドウごとに1回だけ) ──────────────────
    print("【Phase 1】モデル学習・予測 (各ウィンドウ1回のみ)")
    window_data: list[tuple[pd.DataFrame, float, dict]] = []

    for i, win in enumerate(WINDOWS):
        print(f"\n{'━'*72}")
        print(f"  [{i+1}/{len(WINDOWS)}] {win['label']}")
        print(f"{'━'*72}")

        test_df, threshold = _train_and_predict(
            conn,
            train_years=win["train_years"],
            test_min=win["test_min"],
            test_max=win["test_max"],
            n_optuna=n_optuna,
            research_db=research_db,
        )
        pmap = _build_payout_map(conn, win["test_min"], win["test_max"])
        window_data.append((test_df, threshold, pmap))

    conn.close()

    # ── Step 2: 自動最適化ループ (学習不要・高速) ───────────────────────
    print()
    print("【Phase 2】自動最適化ループ (学習済みデータで高速探索)")
    best_name, best_cfg, best_results = _run_auto_search(window_data, WINDOWS)

    # モジュールレベルの _TICKET_PARAMS を更新
    _apply_best_cfg(best_cfg)

    # ── Step 3: 最優秀設定の詳細レポート ─────────────────────────────
    print()
    print(f"{'═'*72}")
    print(f"  📋 黄金パラメーター詳細レポート: {best_name}")
    print(f"{'═'*72}")

    for wr in best_results:
        _print_window(wr, show_detail=args.show_detail)

    _print_asset_curve(best_results)
    _print_final_summary(best_results, best_name, best_cfg)
    _print_ipat_guide(best_cfg)


if __name__ == "__main__":
    main()
