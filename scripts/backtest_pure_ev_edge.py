"""
Pure_EV_Edge 2年ウォークフォワード・バックテスト（2024-01〜2025-12）

データソース（単一真実源）:
  - 馬別オッズ・着順 : data/netkeiba_research.db の horse_odds（2021-2026・250k行）
  - 特徴量           : data/umalogi.db（FeatureBuilder）
  - 複勝の確定払戻   : data/umalogi.db race_payouts（無ければ推定場oddsで補完）

バイアス統制:
  - 前方リーク排除   : Isotonic 較正器は **学習窓(train)** のみで fit し、検証窓(test)で適用。
  - Kelly 複利リーク排除: Kelly は **固定バンクロール** で算出（勝ち分を再投資しない）。
  - survivorship排除 : 全出走馬を母集団とし、敗者も必ずコストに計上。
  - 残る注意         : 卍モデル本体の学習期間に検証期間が含まれる可能性があり、
                       モデルレベルの楽観バイアスは残存しうる（ログに明記）。

出力: フラット¥100/点 と 1/10 Kelly それぞれの 真の累計ROI・月次勝率・最大DD。

使用方法:
  py scripts/backtest_pure_ev_edge.py                 # フル(2024-2025)
  py scripts/backtest_pure_ev_edge.py --limit 300     # 動作確認用
  py scripts/backtest_pure_ev_edge.py --train-end 2024-04-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("pure_ev_backtest")

_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"
_FIXED_BANKROLL = 100_000.0  # Kelly 用固定バンクロール（複利なし）
_FLAT_STAKE = 100  # フラット¥100/点


def _iso_week(d: str) -> str:
    y, m, dd = (int(x) for x in d[:10].split("-"))
    iso = date(y, m, dd).isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _load_races(conn, train_end: str, test_end: str) -> list[tuple[str, str]]:
    """horse_odds に存在し umalogi races にも在る race を (race_id, date) で返す。"""
    rows = conn.execute(
        """
        SELECT r.race_id, r.date
        FROM races r
        WHERE r.date >= '2024-01-01' AND r.date <= ?
          AND EXISTS (SELECT 1 FROM ro.horse_odds h WHERE h.race_id = r.race_id)
        ORDER BY r.date, r.race_id
        """,
        (test_end,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _odds_rank_map(conn, race_id: str) -> dict[int, tuple[float, int]]:
    """horse_odds から {馬番: (win_odds, rank)} を返す（SSoT）。"""
    rows = conn.execute(
        "SELECT horse_number, win_odds, rank FROM ro.horse_odds "
        "WHERE race_id = ? AND win_odds IS NOT NULL AND win_odds > 0",
        (race_id,),
    ).fetchall()
    return {
        int(r[0]): (float(r[1]), int(r[2]) if r[2] is not None else 0) for r in rows
    }


def _place_payout(conn, race_id: str, horse_number: int, est_odds: float) -> float:
    """複勝の確定払戻（per 100円）。race_payouts に無ければ推定場oddsで補完。"""
    row = conn.execute(
        "SELECT payout FROM race_payouts WHERE race_id=? AND bet_type='複勝' AND combination=?",
        (race_id, str(horse_number)),
    ).fetchone()
    if row and row[0]:
        return float(row[0])  # per 100 円
    return est_odds * 100.0


def _fit_calibrator(train_rows, conn, manji):
    """学習窓の (ev_score, is_win) で Isotonic 較正器を fit（前方リーク排除）。"""
    from sklearn.isotonic import IsotonicRegression
    from src.ml.features import FeatureBuilder

    xs: list[float] = []
    ys: list[int] = []
    for rid, _d in train_rows:
        omap = _odds_rank_map(conn, rid)
        if not omap:
            continue
        try:
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is None or df.empty:
                continue
            ev = list(manji.ev_score(df))
        except Exception:
            continue
        for i, (_, row) in enumerate(df.iterrows()):
            if i >= len(ev):
                break
            num = int(row.get("horse_number", 0))
            if num not in omap:
                continue
            xs.append(float(ev[i]))
            ys.append(1 if omap[num][1] == 1 else 0)
    if len(xs) < 100 or sum(ys) == 0:
        return None
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(xs, ys)
    return iso


def _max_drawdown(cum: list[float]) -> float:
    peak = 0.0
    mdd = 0.0
    for v in cum:
        peak = max(peak, v)
        mdd = min(mdd, v - peak)
    return mdd


def run(limit: int | None, train_end: str, test_end: str) -> None:
    import src.ml.manji_calibration as mc
    from src.database.init_db import init_db
    from src.ml.features import FeatureBuilder
    from src.ml.models import load_models
    from src.ml.manji_calibration import calibrate_win_prob
    from src.ml.pure_ev_edge import (
        PureEVConfig,
        evaluate_circuit_breaker,
        select_pure_ev_bets,
    )

    conn = init_db()
    conn.execute("ATTACH DATABASE ? AS ro", (str(_RESEARCH_DB),))
    _honmei, _place, manji = load_models()

    all_rows = _load_races(conn, train_end, test_end)
    train_rows = [r for r in all_rows if r[1] < train_end]
    test_rows = [r for r in all_rows if r[1] >= train_end]
    if limit:
        test_rows = test_rows[:limit]

    print(
        f"[Pure_EV_Edge BT] 学習窓 {len(train_rows)}R (< {train_end}) / 検証窓 {len(test_rows)}R"
    )
    print("[Pure_EV_Edge BT] Isotonic 較正器を学習窓のみで fit 中（前方リーク排除）...")
    iso = _fit_calibrator(train_rows, conn, manji)
    if iso is None:
        print("[警告] 学習窓のサンプル不足 → 保守フォールバック較正で続行")
    else:
        mc._win_cal_cache = iso  # バックテスト中はこの較正器を強制使用

    # 集計コンテナ: mode -> {cost, payout, profit, n_bets, n_hits}
    stats = {m: defaultdict(float) for m in ("flat", "kelly")}
    monthly = {m: defaultdict(float) for m in ("flat", "kelly")}  # 月 -> profit
    cum = {m: [] for m in ("flat", "kelly")}
    running = {m: 0.0 for m in ("flat", "kelly")}
    cb_cfg = PureEVConfig(initial_bankroll=_FIXED_BANKROLL)
    # サーキットブレーカー用の 日次/週次 損益（mode ごと）
    day_pnl = {m: defaultdict(float) for m in ("flat", "kelly")}
    week_pnl = {m: defaultdict(float) for m in ("flat", "kelly")}
    tripped_days = {m: set() for m in ("flat", "kelly")}

    n_proc = 0
    for rid, d in test_rows:
        omap = _odds_rank_map(conn, rid)
        if not omap:
            continue
        try:
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is None or df.empty:
                continue
            # win_odds を horse_odds(SSoT)で上書き
            df = df.copy()
            df["win_odds"] = df["horse_number"].map(
                lambda n: omap.get(int(n), (None, 0))[0]
            )
            ev_scores = list(manji.ev_score(df))
        except Exception:
            continue

        # 出走馬を select_pure_ev_bets の入力 dict に変換（place_prob は卍較正から派生）
        horses: list[dict] = []
        for i, (_, row) in enumerate(df.iterrows()):
            if i >= len(ev_scores):
                break
            try:
                hn = int(row["horse_number"])
                o = float(row.get("win_odds"))
            except (TypeError, ValueError):
                continue
            if o <= 1.0:
                continue
            mev = float(ev_scores[i])
            place_prob = min(0.99, calibrate_win_prob(mev, o) * 2.8)
            horses.append(
                {
                    "horse_number": hn,
                    "horse_name": str(row.get("horse_name", hn)),
                    "win_odds": o,
                    "manji_ev_score": mev,
                    "place_prob": place_prob,
                }
            )

        bets = select_pure_ev_bets(rid, horses, cb_cfg).bets
        n_proc += 1
        month = d[:7]
        day_key, week_key = d[:10], _iso_week(d)

        for b in bets:
            if b.horse_number not in omap:
                continue
            _odds, rank = omap[b.horse_number]
            hit = (rank == 1) if b.bet_type == "単勝" else (1 <= rank <= 3)
            for m in ("flat", "kelly"):
                # サーキットブレーカー: 当日/当週の損益が上限到達ならスキップ
                st = evaluate_circuit_breaker(
                    day_pnl[m][day_key], week_pnl[m][week_key], cb_cfg
                )
                if st.tripped:
                    tripped_days[m].add(day_key)
                    continue
                cost = _FLAT_STAKE if m == "flat" else b.stake
                if cost < 100:
                    continue
                if hit:
                    if b.bet_type == "単勝":
                        payout = b.odds * (cost / 100.0) * 100.0  # = odds*cost
                    else:
                        pp = _place_payout(conn, rid, b.horse_number, b.odds)
                        payout = pp * (cost / 100.0)
                else:
                    payout = 0.0
                profit = payout - cost
                stats[m]["cost"] += cost
                stats[m]["payout"] += payout
                stats[m]["profit"] += profit
                stats[m]["n_bets"] += 1
                stats[m]["n_hits"] += 1 if hit else 0
                monthly[m][month] += profit
                running[m] += profit
                cum[m].append(running[m])
                day_pnl[m][day_key] += profit
                week_pnl[m][week_key] += profit

        if n_proc % 300 == 0:
            print(f"  ... {n_proc}/{len(test_rows)} レース処理")

    conn.close()

    print("\n" + "=" * 64)
    print(f"Pure_EV_Edge バックテスト結果（検証 {n_proc} レース・EV>=1.15・単複のみ）")
    print("=" * 64)
    for m, label in (("flat", "フラット¥100/点"), ("kelly", "1/10 Kelly(固定bank)")):
        s = stats[m]
        cost = s["cost"] or 1.0
        roi = s["payout"] / cost * 100.0
        hit_rate = s["n_hits"] / (s["n_bets"] or 1) * 100.0
        months = monthly[m]
        win_months = sum(1 for v in months.values() if v > 0)
        n_months = len(months) or 1
        mdd = _max_drawdown(cum[m])
        print(f"\n■ {label}")
        print(
            f"  買い目数      : {int(s['n_bets'])}（的中 {int(s['n_hits'])} / 的中率 {hit_rate:.1f}%）"
        )
        print(f"  投資/払戻     : ¥{int(s['cost']):,} → ¥{int(s['payout']):,}")
        print(f"  真の累計損益  : ¥{int(s['profit']):,}")
        print(f"  真の累計ROI   : {roi:.1f}%")
        print(
            f"  月次勝率      : {win_months}/{n_months}ヶ月 ({win_months / n_months * 100:.0f}%)"
        )
        print(f"  最大DD        : ¥{int(mdd):,}")
        if tripped_days[m]:
            print(f"  サーキットブレーカー作動日: {len(tripped_days[m])}日")
    print("\n[注意] 卍モデル本体の学習期間に検証期間が含まれうるためモデルレベルの")
    print(
        "       楽観バイアスは残存。較正/EV/損益は本BT内で out-of-sample に統制済み。"
    )
    print("[注意] 複勝oddsは horse_odds に無く推定/実払戻で補完（単勝が主エッジ）。")


def main() -> int:
    p = argparse.ArgumentParser(description="Pure_EV_Edge 2年WFバックテスト")
    p.add_argument(
        "--limit", type=int, default=None, help="検証レース数の上限（動作確認用）"
    )
    p.add_argument(
        "--train-end", default="2024-04-01", help="学習窓の終端（これ未満で fit）"
    )
    p.add_argument("--test-end", default="2025-12-31", help="検証窓の終端")
    args = p.parse_args()
    run(args.limit, args.train_end, args.test_end)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
