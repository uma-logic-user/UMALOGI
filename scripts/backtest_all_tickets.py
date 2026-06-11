"""全券種 EV オプティマイザの OOS バックテスト。

リーク修正済みキャッシュ（scripts/ablation_leak_audit.py 生成）の test フレームに対し、
  勝率推計 = honmei Booster（本番モデル）→ blend_with_market 較正
  市場確率 = 最終単勝オッズ → Shin (1993) 真確率
で all_ticket_optimizer.scan_all_tickets を実行し、券種ごとの
「真の ROI・的中率・最大ドローダウン」を実払戻（race_payouts）で清算する。

誠実性の設計:
  - 買い目選定に使う組み合わせオッズは win オッズからの**推定**（実オッズ未保存のため）
  - 清算は race_payouts の**実払戻**のみ（推定オッズで利益を計上しない）
  - 1点 100 円フラット・テスト期間は cutoff 以降の純 OOS

アンサンブル検証（過去モデル昇華・legacy_ensemble）:
    --manji-weight W : 卍(EV回帰)の暗黙勝率を w=W で honmei に融合してから較正する。
    --frame train    : cutoff前の train フレームで実行（重みのグリッド探索専用）。
                       OOS(test) で重みを選ぶことはリークのため、探索は必ず train で行う。

Usage:
    py scripts/backtest_all_tickets.py [--ev-min 1.30] [--max-bets 6]
    py scripts/backtest_all_tickets.py --frame train --manji-weight 0.3   # 重み探索
    py scripts/backtest_all_tickets.py --manji-weight 0.3                 # OOS 検証
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"
_CACHE = _ROOT / "data" / "ablation_cache.pkl"
_HONMEI_PKL = _ROOT / "data" / "models" / "honmei_model.pkl"
_MANJI_PKL = _ROOT / "data" / "models" / "manji_model.pkl"


def _combo_key(bet_type: str, combo: tuple[int, ...]) -> str:
    """race_payouts.combination と同一の表記に変換する。"""
    if bet_type in ("馬単", "三連単"):
        return "→".join(str(n) for n in combo)
    return "-".join(str(n) for n in sorted(combo))


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    import warnings

    warnings.filterwarnings("ignore")

    ap = argparse.ArgumentParser()
    ap.add_argument("--ev-min", type=float, default=1.30)
    ap.add_argument("--max-bets", type=int, default=6)
    ap.add_argument("--top-k", type=int, default=8)
    ap.add_argument("--show-hits", action="store_true", help="的中の内訳を表示")
    ap.add_argument(
        "--manji-weight",
        type=float,
        default=0.0,
        help="卍EV回帰の暗黙勝率の混合比 w（0=従来と恒等）",
    )
    ap.add_argument(
        "--ensemble-bet-types",
        type=str,
        default="",
        help="卍アンサンブルを適用する券種（カンマ区切り・空=全券種に適用）。"
        "例: 三連複 → 三連複のみ w 適用、他券種は w=0（従来と恒等）",
    )
    ap.add_argument(
        "--frame",
        choices=("train", "test"),
        default="test",
        help="train=cutoff前（重み探索用）/ test=純OOS（最終検証）",
    )
    args = ap.parse_args()

    import numpy as np

    from src.ml.all_ticket_optimizer import OptimizerConfig, scan_all_tickets
    from src.ml.ev_features import MarketProbabilityCalc
    from src.ml.legacy_ensemble import ensemble_win_probs, predict_manji_ev
    from src.ml.market_blend_calibration import blend_with_market
    from src.ml.models import FEATURE_COLS

    if not _CACHE.exists():
        print(
            "⚠️ 先に scripts/ablation_leak_audit.py を実行してキャッシュを生成してください"
        )
        return 1
    with open(_CACHE, "rb") as f:
        train_df, test_df = pickle.load(f)
    if args.frame == "train":
        test_df = train_df
    with open(_HONMEI_PKL, "rb") as f:
        honmei = pickle.load(f)
    manji = None
    if args.manji_weight > 0.0:
        with open(_MANJI_PKL, "rb") as f:
            manji = pickle.load(f)

    conn = sqlite3.connect(str(_DB_PATH))
    calc = MarketProbabilityCalc()
    cfg = OptimizerConfig(ev_min=args.ev_min, max_bets=args.max_bets, top_k=args.top_k)

    # レース日付（ドローダウンの時系列順序用）
    date_map = dict(
        conn.execute(
            "SELECT race_id, date FROM races WHERE race_id IN ({})".format(
                ",".join("?" * test_df["race_id"].nunique())
            ),
            list(test_df["race_id"].unique()),
        ).fetchall()
    )

    # 払戻マップ {(race_id, bet_type, combination): payout}
    payout_map: dict[tuple[str, str, str], float] = {}
    for rid, bt, comb, pay in conn.execute(
        "SELECT race_id, bet_type, combination, payout FROM race_payouts "
        "WHERE race_id IN ({})".format(",".join("?" * test_df["race_id"].nunique())),
        list(test_df["race_id"].unique()),
    ).fetchall():
        payout_map[(rid, str(bt), str(comb))] = float(pay or 0)

    # 券種別集計
    stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"n": 0, "hits": 0, "cost": 0.0, "payout": 0.0}
    )
    pnl_series: dict[str, list[tuple[str, float]]] = defaultdict(list)  # (date, profit)
    hit_log: list[tuple[str, str, str, float]] = []  # (date, bet_type, combo, payout)
    n_races_with_bets = 0

    for rid, g in test_df.groupby("race_id"):
        numbers = [int(n) for n in g["horse_number"]]
        odds = np.array([float(o) for o in g["win_odds"]])
        if len(numbers) < 6:
            continue
        # honmei 本番モデルの勝率（69列整列）→（卍アンサンブル）→ 市場ブレンド較正
        x = g.reindex(columns=FEATURE_COLS).fillna(0.0)
        if getattr(honmei, "_Booster", None) is None:
            continue
        raw = np.asarray(honmei._Booster.predict(x.values), dtype=float)
        market_p = calc.compute_shin_probabilities(odds)

        ens_types = {t.strip() for t in args.ensemble_bet_types.split(",") if t.strip()}

        def _scan(probs: "np.ndarray") -> list:
            model_p = np.array(
                [blend_with_market(float(p), float(o)) for p, o in zip(probs, odds)]
            )
            return scan_all_tickets(
                numbers, model_p, market_win_probs=market_p, config=cfg
            )

        if manji is None:
            cands = _scan(raw)
        else:
            manji_ev = predict_manji_ev(manji, g)
            raw_ens = ensemble_win_probs(raw, manji_ev, odds, weight=args.manji_weight)
            if not ens_types:
                cands = _scan(raw_ens)
            else:
                # 券種別適用: 指定券種はアンサンブル確率、他は従来確率（恒等）
                cands = [c for c in _scan(raw) if c.bet_type not in ens_types] + [
                    c for c in _scan(raw_ens) if c.bet_type in ens_types
                ]
        if not cands:
            continue
        n_races_with_bets += 1
        date = str(date_map.get(rid, ""))
        for c in cands:
            key = _combo_key(c.bet_type, c.combo)
            pay = payout_map.get((rid, c.bet_type, key), 0.0)
            s = stats[c.bet_type]
            s["n"] += 1
            s["cost"] += 100.0
            s["payout"] += pay
            s["hits"] += 1 if pay > 0 else 0
            if pay > 0:
                hit_log.append((date, c.bet_type, key, pay))
            pnl_series[c.bet_type].append((date, pay - 100.0))
            pnl_series["合計"].append((date, pay - 100.0))
            st = stats["合計"]
            st["n"] += 1
            st["cost"] += 100.0
            st["payout"] += pay
            st["hits"] += 1 if pay > 0 else 0

    conn.close()

    def _max_drawdown(series: list[tuple[str, float]]) -> float:
        """日付順の累積損益からの最大ドローダウン（円）。"""
        cum = peak = 0.0
        mdd = 0.0
        for _, profit in sorted(series, key=lambda t: t[0]):
            cum += profit
            peak = max(peak, cum)
            mdd = min(mdd, cum - peak)
        return -mdd

    n_races = test_df["race_id"].nunique()
    print("=" * 78)
    print(
        f" 全券種EVオプティマイザ OOS バックテスト"
        f"（EV≥{args.ev_min} / max{args.max_bets}点 / 実払戻清算）"
    )
    period = (
        "2025-10〜2026-06 (純OOS)"
        if args.frame == "test"
        else "cutoff前 (train・重み探索用)"
    )
    ens = f" / 卍w={args.manji_weight}" if args.manji_weight > 0 else ""
    print(
        f" 対象: {n_races}レース（うちベット発生 {n_races_with_bets}）"
        f" / 期間: {period}{ens}"
    )
    print("=" * 78)
    print(
        f" {'券種':<8} {'点数':>6} {'的中':>5} {'的中率%':>7} "
        f"{'投資¥':>9} {'払戻¥':>9} {'ROI%':>7} {'最大DD¥':>9}"
    )
    print("-" * 78)
    order = ["馬連", "ワイド", "馬単", "三連複", "三連単", "合計"]
    for bt in order:
        if bt not in stats:
            continue
        s = stats[bt]
        roi = s["payout"] / s["cost"] * 100 if s["cost"] else 0.0
        hr = s["hits"] / s["n"] * 100 if s["n"] else 0.0
        mdd = _max_drawdown(pnl_series[bt])
        print(
            f" {bt:<8} {s['n']:>6.0f} {s['hits']:>5.0f} {hr:>7.1f} "
            f"{s['cost']:>9,.0f} {s['payout']:>9,.0f} {roi:>7.1f} {mdd:>9,.0f}"
        )
    print("=" * 78)
    if args.show_hits and hit_log:
        total_pay = sum(h[3] for h in hit_log)
        print(" 的中内訳（払戻降順・分散リスク評価用）:")
        for date, bt, key, pay in sorted(hit_log, key=lambda t: -t[3]):
            print(
                f"   {date} {bt:<5} {key:<12} ¥{pay:>8,.0f} "
                f"(総払戻の{pay / total_pay * 100:4.1f}%)"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
