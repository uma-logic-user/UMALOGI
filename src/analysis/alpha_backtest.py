"""
src/analysis/alpha_backtest.py — ALPHA-Payout 超攻撃型投資シミュレーション
==========================================================================

AlphaPayoutModel (複勝EV直接回帰) の予測を使い、
複勝 / 馬連 / 三連複 の全3券種を組み合わせた超攻撃型複利シミュレーション。

walk-forward: 2024学習 → 2025テスト (カンニング排除)

券種別ポジション配分（全体投資額の割合）:
  複勝  : 40%  ← 的中率が高く土台を支える
  馬連  : 35%  ← 中配当でリターンを積み上げ
  三連複 : 25%  ← 高配当爆発狙い

モード:
  --mode aggressive : 1レースあたり残高の20%（攻撃型）
  --mode ultra      : 1レースあたり残高の50%（超攻撃型）
  --mode yolo       : EV比例全ツッパ (min(pred_ev/2, 1.0) * 残高)

Usage:
  py src/analysis/alpha_backtest.py
  py src/analysis/alpha_backtest.py --mode ultra
  py src/analysis/alpha_backtest.py --mode yolo --no-optuna
  py src/analysis/alpha_backtest.py --mode ultra --compare   # 全モード比較
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
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

_INITIAL   = 50_000    # 初期資金 (円)
_MIN_BET   = 100       # 最低賭け金 (円)
_EV_THR    = 1.05      # AlphaPayoutModel デフォルト閾値

# 券種別投資配分
_SPLIT = {"複勝": 0.40, "馬連": 0.35, "三連複": 0.25}


# ── 払戻マップ構築 ─────────────────────────────────────────────────────

def _build_payout_map(
    conn: sqlite3.Connection, year: int
) -> dict[str, dict[str, dict[str, float]]]:
    """
    {race_id: {bet_type: {combination_str: payout}}} を返す。
    payouts は「100円あたりの払い戻し額」ではなく「実払戻額」。
    """
    rows = conn.execute(
        """
        SELECT rp.race_id, rp.bet_type, rp.combination, rp.payout
        FROM race_payouts rp
        JOIN races r ON rp.race_id = r.race_id
        WHERE strftime('%Y', r.date) = ?
          AND rp.bet_type IN ('複勝', '馬連', '三連複')
          AND rp.combination IS NOT NULL
          AND rp.payout IS NOT NULL AND rp.payout > 0
        """,
        (str(year),),
    ).fetchall()

    pmap: dict[str, dict[str, dict[str, float]]] = {}
    for race_id, bet_type, combo, payout in rows:
        pmap.setdefault(race_id, {}).setdefault(bet_type, {})[combo] = float(payout)
    return pmap


def _combo_key_umaren(h1: int, h2: int) -> str:
    """馬連キー: 小さい馬番-大きい馬番"""
    return f"{min(h1, h2)}-{max(h1, h2)}"


def _combo_key_sanrenpuku(h1: int, h2: int, h3: int) -> str:
    """三連複キー: 数値昇順でハイフン連結"""
    nums = sorted([h1, h2, h3])
    return f"{nums[0]}-{nums[1]}-{nums[2]}"


def _get_payout(
    pmap: dict[str, dict[str, dict[str, float]]],
    race_id: str,
    bet_type: str,
    combo_key: str,
) -> float:
    """払戻額 (0 = 外れ)"""
    return pmap.get(race_id, {}).get(bet_type, {}).get(combo_key, 0.0)


# ── モデル予測ロード ──────────────────────────────────────────────────

def _load_predictions(
    conn: sqlite3.Connection, n_optuna: int, research_db: Path | None
) -> tuple[pd.DataFrame, float]:
    """
    AlphaPayoutModel を 2024 で学習 → 2025 で pred_ev を付与した全馬データを返す。
    戻り値: (test_df_with_pred_ev, ev_threshold)
    """
    from src.ml.alpha_payout_model import AlphaPayoutModel

    model = AlphaPayoutModel()

    print("  [学習] 2024年データをロード中...", flush=True)
    train_df = model.load_training_data(conn, [2024], research_db_path=research_db)
    print(f"  [学習] {len(train_df):,} 行", flush=True)

    print(f"  [学習] Optuna {n_optuna}試行...", flush=True)
    metrics = model.train(train_df, n_optuna_trials=n_optuna)
    threshold = model._ev_threshold
    print(
        f"  [学習完了] val_ROI={metrics['val_roi']:.1f}%  閾値={threshold:.2f}",
        flush=True,
    )

    print("  [テスト] 2025年データをロード中...", flush=True)
    test_df = model.load_training_data(conn, [2025], research_db_path=research_db)
    test_df["pred_ev"] = model.predict_payout_ev(test_df).values
    test_df["actual_payout"] = pd.to_numeric(
        test_df["actual_payout"], errors="coerce"
    ).fillna(0.0)
    print(f"  [テスト] {len(test_df):,} 行", flush=True)
    return test_df, threshold


# ── レース単位シグナル生成 ───────────────────────────────────────────

def _build_race_signals(
    test_df: pd.DataFrame,
    pmap: dict,
    threshold: float,
) -> pd.DataFrame:
    """
    pred_ev > threshold の馬が1頭以上いるレースを「エントリーレース」として選択。
    各エントリーレースで pred_ev 上位3頭を使い、複勝・馬連・三連複を生成。

    ※ 閾値はレース選択のフィルタのみに使用。
      2番手・3番手の馬は閾値を超えなくてもモデルのトップピックとして使う。
    """
    signals: list[dict] = []
    # pred_ev 降順でソートしてグループ化
    sorted_df = test_df.sort_values("pred_ev", ascending=False)
    grouped = sorted_df.groupby("race_id", sort=False)

    for race_id, grp in grouped:
        # レース選択: 最高EVが閾値を超えるか
        if float(grp["pred_ev"].iloc[0]) < threshold:
            continue

        date   = grp["date"].iloc[0]
        max_ev = float(grp["pred_ev"].iloc[0])
        # 上位3頭（EV順。3頭未満なら可能な分だけ）
        top_horses = grp["horse_number"].astype(int).tolist()[:3]

        h1 = top_horses[0]

        # ── 複勝 (top-1) ──────────────────────────────────────────────
        combo_f = str(h1)
        pay_f   = _get_payout(pmap, race_id, "複勝", combo_f)
        signals.append({
            "date": date, "race_id": race_id, "bet_type": "複勝",
            "combo": combo_f, "max_ev": max_ev,
            "actual_payout": pay_f, "is_hit": int(pay_f > 0),
        })

        # ── 馬連 (top-1 × top-2) ─────────────────────────────────────
        if len(top_horses) >= 2:
            h2 = top_horses[1]
            combo_u = _combo_key_umaren(h1, h2)
            pay_u   = _get_payout(pmap, race_id, "馬連", combo_u)
            signals.append({
                "date": date, "race_id": race_id, "bet_type": "馬連",
                "combo": combo_u, "max_ev": max_ev,
                "actual_payout": pay_u, "is_hit": int(pay_u > 0),
            })

        # ── 三連複 (top-1 × top-2 × top-3) ────────────────────────────
        if len(top_horses) >= 3:
            h3 = top_horses[2]
            combo_s = _combo_key_sanrenpuku(h1, h2, h3)
            pay_s   = _get_payout(pmap, race_id, "三連複", combo_s)
            signals.append({
                "date": date, "race_id": race_id, "bet_type": "三連複",
                "combo": combo_s, "max_ev": max_ev,
                "actual_payout": pay_s, "is_hit": int(pay_s > 0),
            })

    return pd.DataFrame(signals).sort_values(["date", "race_id", "bet_type"])


# ── シミュレーション ──────────────────────────────────────────────────

class SimResult(NamedTuple):
    mode: str
    final_balance: float
    peak_balance: float
    n_bets: int
    n_races: int
    n_hits: int
    hit_rate: float
    total_stake: float
    total_payout: float
    roi: float
    max_dd_pct: float
    max_consec_loss: int
    bankruptcy_month: str | None
    monthly: pd.DataFrame


def _compute_stake(balance: float, mode: str, max_ev: float, frac: float = 0.20) -> float:
    """
    モード別賭け金算出（残高に対する割合）。
    mode:
      fixed     : 固定1000円（旧パターン1）
      compound  : 残高の2%（旧パターン2）
      aggressive: 残高の20%（攻撃型）
      ultra     : 残高の50%（超攻撃型）
      yolo      : min(pred_ev/2, 1.0) × 残高（EV全ツッパ）
    """
    if balance < _MIN_BET:
        return 0.0
    if mode == "fixed":
        raw = 1_000.0
    elif mode == "compound":
        raw = balance * 0.02
    elif mode == "aggressive":
        raw = balance * 0.20
    elif mode == "ultra":
        raw = balance * 0.50
    elif mode == "yolo":
        ratio = min(max_ev / 2.0, 1.0)
        raw = balance * ratio
    else:
        raw = balance * frac

    raw = max(_MIN_BET, raw)
    raw = min(raw, balance)              # 残高上限
    return round(raw / 100) * 100        # 100円単位


def _simulate(sig_df: pd.DataFrame, mode: str) -> SimResult:
    balance = float(_INITIAL)
    peak    = float(_INITIAL)
    max_dd  = 0.0
    consec  = 0
    max_cl  = 0
    bankrupt_month: str | None = None
    records: list[dict] = []

    # レースごとにグループ化して投資額を決定
    for (date, race_id), grp in sig_df.groupby(["date", "race_id"], sort=True):
        if balance < _MIN_BET:
            bankrupt_month = pd.to_datetime(date).strftime("%Y-%m")
            break

        # このレースの最大EV (全券種共通で投資総額を決める)
        max_ev = float(grp["max_ev"].iloc[0])

        # 全体割当（残高の何%をこのレースに使うか）
        total_alloc = _compute_stake(balance, mode, max_ev)
        total_alloc = min(total_alloc, balance)

        race_profit = 0.0
        race_stake  = 0.0
        race_hit    = False

        for _, row in grp.iterrows():
            frac = _SPLIT.get(row["bet_type"], 0.0)
            stake = round(total_alloc * frac / 100) * 100
            stake = max(_MIN_BET, min(stake, balance - race_stake))
            if stake < _MIN_BET:
                continue

            pay_x100 = float(row["actual_payout"])    # 100円あたり払戻
            is_hit   = int(row["is_hit"])
            payout   = is_hit * pay_x100 * stake / 100.0

            race_stake  += stake
            race_profit += payout - stake
            if is_hit:
                race_hit = True

            records.append({
                "date":     date,
                "race_id":  race_id,
                "bet_type": row["bet_type"],
                "stake":    stake,
                "payout":   payout,
                "is_hit":   is_hit,
                "balance":  balance + race_profit,   # 仮（後で更新）
            })

        balance += race_profit
        peak = max(peak, balance)
        dd   = (peak - balance) / peak * 100 if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

        # records 末尾を正しい balance で上書き
        for r in records[-len(grp):]:
            r["balance"] = balance

        if race_hit:
            consec = 0
        else:
            consec += 1
            max_cl = max(max_cl, consec)

    if not records:
        empty = pd.DataFrame(
            columns=["month","bets","hits","stake","payout","profit","balance"]
        )
        return SimResult(
            mode=mode, final_balance=balance, peak_balance=peak,
            n_bets=0, n_races=0, n_hits=0, hit_rate=0,
            total_stake=0, total_payout=0, roi=0, max_dd_pct=0,
            max_consec_loss=0, bankruptcy_month=bankrupt_month, monthly=empty,
        )

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

    return SimResult(
        mode=mode, final_balance=balance, peak_balance=peak,
        n_bets=n_b, n_races=n_r, n_hits=n_h,
        hit_rate=n_h / n_b * 100 if n_b > 0 else 0.0,
        total_stake=ts, total_payout=tp, roi=roi,
        max_dd_pct=max_dd, max_consec_loss=max_cl,
        bankruptcy_month=bankrupt_month, monthly=monthly,
    )


# ── レポート出力 ──────────────────────────────────────────────────────

_MODE_LABEL = {
    "fixed":      "旧 Pattern 1: 単利固定 ¥1,000/bet",
    "compound":   "旧 Pattern 2: 複利 2%/bet",
    "aggressive": "攻撃型: 1レース残高の 20%",
    "ultra":      "超攻撃型: 1レース残高の 50%",
    "yolo":       "全ツッパ: EV比例 (min(EV/2, 100%))",
}


def _print_detail(r: SimResult) -> None:
    label = _MODE_LABEL.get(r.mode, r.mode)
    bankrupt = r.bankruptcy_month is not None
    print()
    print(f"{'═'*72}")
    print(f"  {label}")
    print(f"{'═'*72}")
    if bankrupt:
        print(f"  ⚠️  破産: {r.bankruptcy_month}  最大連敗 {r.max_consec_loss}連敗")
    print(f"  初期資金    : ¥{_INITIAL:>12,}")
    print(f"  最終残高    : ¥{r.final_balance:>12,.0f}"
          + (f"  (破産)" if bankrupt else f"  ({r.final_balance/_INITIAL*100 - 100:+.1f}%)"))
    print(f"  最高到達残高: ¥{r.peak_balance:>12,.0f}")
    print(f"  ─"*36)
    print(f"  ベット件数  : {r.n_bets:>8,}  ({r.n_races}レース×複数券種)")
    print(f"  的中数      : {r.n_hits:>8,}  (的中率 {r.hit_rate:.1f}%)")
    print(f"  総投資額    : ¥{r.total_stake:>12,.0f}")
    print(f"  総払戻額    : ¥{r.total_payout:>12,.0f}")
    print(f"  純損益      : ¥{r.total_payout - r.total_stake:>+12,.0f}")
    print(f"  ROI         : {r.roi:>10.1f}%")
    print(f"  最大DD      : {r.max_dd_pct:>8.1f}%")
    print(f"  最大連負    : {r.max_consec_loss:>8} 連敗")

    if not r.monthly.empty:
        print()
        print(f"  ── 月別成績 ──")
        print(f"  {'月':>7}  {'件数':>5}  {'的中':>4}  {'投資':>10}  {'払戻':>10}  "
              f"{'損益':>11}  {'残高':>11}")
        print(f"  {'─'*72}")
        for _, row in r.monthly.iterrows():
            sign = "+" if row["profit"] >= 0 else ""
            print(
                f"  {row['month']:>7}  {int(row['bets']):>5,}  {int(row['hits']):>4}  "
                f"¥{int(row['stake']):>9,}  ¥{int(row['payout']):>9,}  "
                f"{sign}¥{int(row['profit']):>9,}  ¥{int(row['balance']):>10,}"
            )


def _print_comparison_table(results: list[SimResult]) -> None:
    """全モード比較サマリーテーブル"""
    print()
    w = 76
    print("╔" + "═" * w + "╗")
    print("║" + " UMALOGI Alpha-Payout 超攻撃型シミュレーション 2025年 全モード比較 ".center(w) + "║")
    print("╠" + "═" * w + "╣")

    cols = [
        ("モード",       lambda r: _MODE_LABEL.get(r.mode, r.mode)[:28]),
        ("最終残高",     lambda r: f"¥{r.final_balance:>12,.0f}" + ("💀" if r.bankruptcy_month else "")),
        ("最高到達残高", lambda r: f"¥{r.peak_balance:>12,.0f}"),
        ("ROI",          lambda r: f"{r.roi:>8.1f}%"),
        ("件数",         lambda r: f"{r.n_bets:>6,}"),
        ("的中率",       lambda r: f"{r.hit_rate:>6.1f}%"),
        ("最大DD",       lambda r: f"{r.max_dd_pct:>6.1f}%"),
        ("最大連負",     lambda r: f"{r.max_consec_loss:>5}連敗"),
        ("破産",         lambda r: r.bankruptcy_month or "なし"),
    ]

    for label, fn in cols:
        row = f"║  {label:<14}"
        for r in results:
            cell = fn(r)
            row += f"  {cell:<18}"
        row += "  ║"
        print(row)

    print("╠" + "═" * w + "╣")
    row = "║  " + "評価".ljust(14)
    for r in results:
        if r.bankruptcy_month:
            cell = "💀 破産"
        elif r.roi >= 150:
            cell = "🚀 爆益"
        elif r.roi >= 120:
            cell = "✅ 大黒字"
        elif r.roi > 100:
            cell = "✅ 黒字"
        else:
            cell = "❌ 赤字"
        row += f"  {cell:<18}"
    row += "  ║"
    print(row)
    print("╚" + "═" * w + "╝")
    print()


def _print_bet_type_breakdown(sig_df: pd.DataFrame) -> None:
    """券種別成績内訳"""
    print()
    print("  ── 券種別シグナル内訳 (全モード共通) ──")
    print(f"  {'券種':>6}  {'件数':>6}  {'的中':>5}  {'的中率':>7}  {'平均払戻':>9}  {'最高払戻':>9}")
    print(f"  {'─'*56}")
    for bt in ["複勝", "馬連", "三連複"]:
        sub = sig_df[sig_df["bet_type"] == bt]
        n   = len(sub)
        h   = int(sub["is_hit"].sum())
        hr  = h / n * 100 if n > 0 else 0
        payouts_hit = sub[sub["is_hit"] == 1]["actual_payout"]
        avg_p = float(payouts_hit.mean()) if len(payouts_hit) > 0 else 0
        max_p = float(payouts_hit.max())  if len(payouts_hit) > 0 else 0
        print(
            f"  {bt:>6}  {n:>6,}  {h:>5}  {hr:>6.1f}%  "
            f"¥{avg_p:>8,.0f}  ¥{max_p:>8,.0f}"
        )


# ── main ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Alpha-Payout 超攻撃型投資シミュレーション")
    ap.add_argument("--mode",   default="ultra",
                    choices=["fixed","compound","aggressive","ultra","yolo","all"],
                    help="投資モード (default: ultra / all: 全モード比較)")
    ap.add_argument("--optuna-trials", type=int, default=20)
    ap.add_argument("--no-optuna",    action="store_true")
    ap.add_argument("--compare",      action="store_true",
                    help="全5モードを一括比較")
    args = ap.parse_args()

    n_optuna = 1 if args.no_optuna else args.optuna_trials
    modes    = ["fixed","compound","aggressive","ultra","yolo"] \
               if (args.mode == "all" or args.compare) else [args.mode]

    print()
    print("=" * 72)
    print("  UMALOGI Alpha-Payout 超攻撃型バックテスト 2025年全期間")
    print(f"  初期資金: ¥{_INITIAL:,}  |  券種: 複勝+馬連+三連複")
    print(f"  Optuna: {n_optuna}試行  |  モード: {', '.join(modes)}")
    print("  walk-forward: 2024学習 → 2025テスト (カンニング排除)")
    print("=" * 72)

    conn = sqlite3.connect(str(_DB_PATH))
    research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None

    print()
    print("[Phase 1] AlphaPayoutModel 学習・予測...", flush=True)
    test_df, threshold = _load_predictions(conn, n_optuna, research_db)

    print("[Phase 2] 払戻マップ構築 (2025年 複勝/馬連/三連複)...", flush=True)
    pmap = _build_payout_map(conn, 2025)
    conn.close()
    print(f"  払戻マップ: {sum(len(v) for v in pmap.values())} レース×券種", flush=True)

    print("[Phase 3] レース単位シグナル生成...", flush=True)
    sig_df = _build_race_signals(test_df, pmap, threshold)
    n_races  = sig_df.groupby("race_id").ngroups
    n_days   = sig_df["date"].nunique()
    print(
        f"  シグナル: {len(sig_df):,}件  {n_races}レース  {n_days}日間  "
        f"日平均{n_races/n_days:.1f}レース",
        flush=True,
    )

    _print_bet_type_breakdown(sig_df)

    print()
    print("[Phase 4] シミュレーション実行...", flush=True)
    results: list[SimResult] = []
    for m in modes:
        print(f"  → {_MODE_LABEL.get(m, m)}...", flush=True)
        results.append(_simulate(sig_df, m))

    # 詳細レポート（ultraとyoloは必ず表示、他は--compare時のみ）
    for r in results:
        if r.mode in ("ultra", "yolo") or len(modes) == 1:
            _print_detail(r)

    if len(results) > 1:
        _print_comparison_table(results)
    else:
        r = results[0]
        bankrupt = r.bankruptcy_month is not None
        print()
        print("╔" + "═" * 65 + "╗")
        print("║" + f" 結果サマリー [{_MODE_LABEL.get(r.mode,r.mode)}] ".center(65) + "║")
        print("╠" + "═" * 65 + "╣")
        items = [
            ("初期資金",     f"¥{_INITIAL:,}"),
            ("最終残高",     f"¥{r.final_balance:,.0f}" + (" 💀破産" if bankrupt else f" ({r.final_balance/_INITIAL*100-100:+.1f}%)")),
            ("最高到達残高", f"¥{r.peak_balance:,.0f}"),
            ("総投資件数",   f"{r.n_bets:,}件 ({r.n_races}レース)"),
            ("ROI",          f"{r.roi:.1f}%"),
            ("最大DD",       f"{r.max_dd_pct:.1f}%"),
            ("最大連続負け", f"{r.max_consec_loss}連敗"),
            ("破産",         r.bankruptcy_month or "なし"),
        ]
        for lbl, val in items:
            print(f"║  {lbl:<22} {val:<41}║")
        print("╚" + "═" * 65 + "╝")
        print()


if __name__ == "__main__":
    main()
