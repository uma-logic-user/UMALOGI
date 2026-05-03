"""
単複限定バックテスト — 初期所持金100,000円 / 1,000円固定
対象: 2024-2025 (payout存在レースのみ)
"""
from __future__ import annotations
import sys, sqlite3
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd
import numpy as np
from src.ml.models import load_models, FEATURE_COLS, _build_train_df
from src.database.init_db import init_db

INIT  = 100_000
UNIT  = 1_000

def main() -> None:
    print("=== 単複限定バックテスト (2024-2025) ===", flush=True)

    conn = init_db()
    print("訓練データ構築中...", flush=True)
    df_all = _build_train_df(conn)

    payouts_raw = pd.read_sql_query(
        """
        SELECT race_id, bet_type,
               CAST(combination AS INTEGER) AS combo_int,
               payout
        FROM race_payouts
        WHERE bet_type IN ('単勝','複勝')
          AND race_id >= '202400000000'
          AND race_id <  '202600000000'
          AND CAST(combination AS INTEGER) > 0
        """,
        conn,
    )
    conn.close()

    print(f"  payouts: {len(payouts_raw):,}行 / {payouts_raw['race_id'].nunique():,}レース", flush=True)

    honmei_model, _place, manji_model = load_models()
    print("モデル予測中...", flush=True)
    X = df_all[FEATURE_COLS].fillna(0)
    df_all = df_all.copy()
    df_all["honmei_score"] = honmei_model.predict(X)
    df_all["manji_ev"]     = manji_model.ev_score(X)
    df_all["is_win"]       = (df_all["rank"] == 1).astype(int)
    df_all["is_place"]     = ((df_all["rank"] >= 1) & (df_all["rank"] <= 3)).astype(int)

    pay_tansho  = {
        (r.race_id, int(r.combo_int)): r.payout
        for _, r in payouts_raw[payouts_raw.bet_type == "単勝"].iterrows()
    }
    pay_fukusho = {
        (r.race_id, int(r.combo_int)): r.payout
        for _, r in payouts_raw[payouts_raw.bet_type == "複勝"].iterrows()
    }

    payout_races = set(payouts_raw["race_id"].unique())
    df = df_all[
        df_all["race_id"].str[:4].isin(["2024", "2025"])
        & df_all["race_id"].isin(payout_races)
    ].copy()
    n_races = df["race_id"].nunique()
    print(f"  対象レース: {n_races:,} / 行数: {len(df):,}", flush=True)

    # honmei_ev = honmei_score × win_odds
    if "win_odds" in df.columns:
        df["honmei_ev"] = df["honmei_score"] * df["win_odds"].fillna(0)
    else:
        df["honmei_ev"] = df["honmei_score"]

    # ─── シミュレーション (2パターン) ───
    rows: list[dict] = []
    for race_id, grp in df.groupby("race_id"):
        year = race_id[:4]
        for model_label, score_col, ev_col in [
            ("本命", "honmei_score", "honmei_ev"),
            ("卍",  "manji_ev",     "manji_ev"),
        ]:
            # -- 戦略A: 毎レース top-1 に無条件購入 --
            top1 = grp.nlargest(1, score_col).iloc[0]
            hn   = int(top1["horse_number"])
            hit1 = bool(top1["is_win"])
            pr1  = pay_tansho.get((race_id, hn), 0)
            rows.append({
                "strategy": "全賭け",
                "year": year, "model": model_label, "bet_type": "単勝",
                "is_hit": hit1 and pr1 > 0,
                "invested": UNIT,
                "payout_yen": pr1 * UNIT / 100 if hit1 and pr1 > 0 else 0,
            })
            # 複勝 top-3 (全賭け)
            for _, r3 in grp.nlargest(3, score_col).iterrows():
                hn3  = int(r3["horse_number"])
                hit3 = bool(r3["is_place"])
                pr3  = pay_fukusho.get((race_id, hn3), 0)
                rows.append({
                    "strategy": "全賭け",
                    "year": year, "model": model_label, "bet_type": "複勝",
                    "is_hit": hit3 and pr3 > 0,
                    "invested": UNIT,
                    "payout_yen": pr3 * UNIT / 100 if hit3 and pr3 > 0 else 0,
                })

            # -- 戦略B: EV>1.0 の馬のみ購入 --
            ev_picks = grp[grp[ev_col] > 1.0].nlargest(3, ev_col)
            for _, r_ev in ev_picks.iterrows():
                hn_ev  = int(r_ev["horse_number"])
                # 単勝
                hit_ev1 = bool(r_ev["is_win"])
                pr_ev1  = pay_tansho.get((race_id, hn_ev), 0)
                rows.append({
                    "strategy": "EV>1",
                    "year": year, "model": model_label, "bet_type": "単勝",
                    "is_hit": hit_ev1 and pr_ev1 > 0,
                    "invested": UNIT,
                    "payout_yen": pr_ev1 * UNIT / 100 if hit_ev1 and pr_ev1 > 0 else 0,
                })
                # 複勝
                hit_ev3 = bool(r_ev["is_place"])
                pr_ev3  = pay_fukusho.get((race_id, hn_ev), 0)
                rows.append({
                    "strategy": "EV>1",
                    "year": year, "model": model_label, "bet_type": "複勝",
                    "is_hit": hit_ev3 and pr_ev3 > 0,
                    "invested": UNIT,
                    "payout_yen": pr_ev3 * UNIT / 100 if hit_ev3 and pr_ev3 > 0 else 0,
                })

    res = pd.DataFrame(rows)
    print()

    # ─── 全期間集計 ───
    for strategy in ["全賭け", "EV>1"]:
        sub = res[res["strategy"] == strategy]
        if sub.empty:
            continue
        print(f"\n=== [{strategy}] 全期間(2024-2025) サマリ ===")
        print(f"{'モデル':<4} {'券種':<4} {'賭数':>6} {'的中':>5} {'的中率':>7} {'投資額':>10} {'払戻':>10} {'ROI':>7} {'損益':>10}")
        for (mdl, bt), g in sub.groupby(["model", "bet_type"]):
            n   = len(g);  h   = g["is_hit"].sum()
            inv = g["invested"].sum();  pay = g["payout_yen"].sum()
            roi = pay / inv * 100 if inv > 0 else 0.0
            print(f"{mdl:<4} {bt:<4} {n:>6,} {h:>5,} {h/n*100:>6.1f}% {inv:>10,.0f} {pay:>10,.0f} {roi:>6.1f}% {pay-inv:>+10,.0f}")

    # ─── 所持金推移・最大ドローダウン ───
    print()
    print("=== 所持金推移・最大ドローダウン (初期100,000円 / 1,000円固定) ===")

    for strategy in ["全賭け", "EV>1"]:
        print(f"\n--- {strategy} ---")
        for mdl in ["本命", "卍"]:
            for bt in ["単勝", "複勝"]:
                sub = res[(res["strategy"] == strategy) & (res["model"] == mdl) & (res["bet_type"] == bt)].sort_values("year")
                if sub.empty:
                    print(f"  {mdl}/{bt}: 賭け対象なし")
                    continue
                balance  = INIT
                peak     = INIT
                max_dd   = 0.0
                balances = []
                for _, row in sub.iterrows():
                    balance += row["payout_yen"] - row["invested"]
                    balances.append(balance)
                    if balance > peak:
                        peak = balance
                    if peak > 0:
                        dd = (peak - balance) / peak * 100
                        if dd > max_dd:
                            max_dd = dd
                final = balances[-1] if balances else INIT
                total_inv  = sub["invested"].sum()
                total_pay  = sub["payout_yen"].sum()
                roi_total  = total_pay / total_inv * 100 if total_inv > 0 else 0
                print(f"  {mdl}/{bt}: 賭数={len(sub):,} 最終残高={final:,.0f}円 "
                      f"最大DD={max_dd:.1f}% ROI={roi_total:.1f}% 総損益={final-INIT:+,.0f}円")


if __name__ == "__main__":
    main()
