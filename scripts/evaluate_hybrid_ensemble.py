"""二階層アンサンブルフィルタリング: Accuracy Model × EVモデル (honmei)。

Layer1: Accuracy Model で「勝率が高い馬」を選別（閾値でフィルタ）
Layer2: EVモデル (honmei) の期待値フィルタ（モデル勝率 × オッズ ≥ ev_threshold）

仮説: 「統計的に強い馬」かつ「市場が過小評価している馬」の積集合を狙えば
       単体運用より高いエッジが生まれる。

使い方:
    py scripts/evaluate_hybrid_ensemble.py
    py scripts/evaluate_hybrid_ensemble.py --ev-threshold 1.0 --train-cap 800 --test-cap 350
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"
_HONMEI_PKL = _ROOT / "data" / "models" / "honmei_model.pkl"


def _race_ids(conn: sqlite3.Connection, lo: str, hi: str, cap: int) -> list[str]:
    rows = conn.execute(
        """SELECT DISTINCT r.race_id FROM races r
           WHERE r.date >= ? AND r.date < ?
             AND EXISTS (SELECT 1 FROM race_results rr
                         WHERE rr.race_id=r.race_id AND rr.rank>0 AND rr.win_odds>0)
           ORDER BY r.date""",
        (lo, hi),
    ).fetchall()
    ids = [r[0] for r in rows]
    if len(ids) > cap:
        step = len(ids) / cap
        ids = [ids[int(i * step)] for i in range(cap)]
    return ids


def _eval_bets(bets: object) -> dict:  # type: ignore[type-arg]
    """単勝フラットベット(100円)のROI・的中率を算出。"""
    import pandas as pd

    if isinstance(bets, pd.DataFrame) and bets.empty:
        return {"n": 0, "hit": 0.0, "roi": 0.0}
    n = len(bets)
    stake = n * 100
    payout = float((bets["is_win"] * bets["win_odds"] * 100).sum())
    hit = float(bets["is_win"].mean() * 100)
    roi = payout / stake * 100 if stake else 0.0
    return {"n": n, "hit": hit, "roi": roi}


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass
    import warnings

    warnings.filterwarnings("ignore")

    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2025-10-01")
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-hi", default="2026-06-01")
    ap.add_argument("--train-cap", type=int, default=800)
    ap.add_argument("--test-cap", type=int, default=350)
    ap.add_argument("--max-per-race", type=int, default=2)
    ap.add_argument(
        "--ev-threshold",
        type=float,
        default=1.0,
        help="Layer2: EVモデルの期待値閾値（デフォルト1.0）",
    )
    args = ap.parse_args()

    import pandas as pd
    from sklearn.metrics import roc_auc_score

    from src.ml.accuracy_model_v2 import AccuracyModelV2, build_training_data
    from src.features.prerun import build_prerun_features
    from src.features.pedigree_te import SireEncoder, build_pedigree_features
    from src.ml.features import FeatureBuilder
    from src.ml.models import FEATURE_COLS

    conn = sqlite3.connect(str(_DB_PATH))
    try:
        print("=" * 72)
        print(" 二階層アンサンブル: Accuracy Model × EVモデル (honmei)")
        print(f" Layer1: Accuracy閾値(自動最適化) / Layer2: EV≥{args.ev_threshold}")
        print(
            f" train:{args.train_lo}〜{args.cutoff} / test:{args.cutoff}〜{args.test_hi}"
        )
        print("=" * 72)

        # ── 学習 ─────────────────────────────────────────────────────────
        print("\n[Step1] Accuracy Model 学習中...")
        train_X, train_y = build_training_data(
            conn, train_lo=args.train_lo, train_hi=args.cutoff, race_cap=args.train_cap
        )
        acc_model = AccuracyModelV2()
        acc_model.fit(train_X, train_y)
        print(
            f"  閾値={acc_model.threshold:.4f} / 特徴量={len(acc_model._feature_cols)}列"
        )

        # ── EVモデルロード ───────────────────────────────────────────────
        print("[Step2] EVモデル (honmei) ロード...")
        with open(_HONMEI_PKL, "rb") as f:
            honmei = pickle.load(f)
        ev_feat_cols = [c for c in FEATURE_COLS if True]  # 69列全部使う

        # ── テストデータ構築 ─────────────────────────────────────────────
        print("[Step3] テストデータ構築中...")
        enc = SireEncoder().fit(conn, cutoff_date=args.cutoff)
        fb = FeatureBuilder(conn)
        test_ids = _race_ids(conn, args.cutoff, args.test_hi, args.test_cap)
        frames: list[pd.DataFrame] = []
        for rid in test_ids:
            try:
                base = fb.build_race_features(rid)
            except Exception:
                continue
            if base.empty:
                continue
            labels = conn.execute(
                "SELECT horse_number, rank, win_odds FROM race_results "
                "WHERE race_id=? AND rank>0 AND win_odds>0",
                (rid,),
            ).fetchall()
            if not labels:
                continue
            lmap = {hn: (1 if rk == 1 else 0, wo) for hn, rk, wo in labels}
            base["is_win"] = base["horse_number"].map(
                lambda x: lmap.get(x, (0, None))[0]
            )
            base["win_odds"] = base["horse_number"].map(
                lambda x: lmap.get(x, (0, None))[1]
            )
            base = base.dropna(subset=["is_win", "win_odds"])
            if base.empty:
                continue
            pre = build_prerun_features(conn, rid)
            if not pre.empty:
                base = base.merge(pre, on="horse_number", how="left")
            ped = build_pedigree_features(conn, rid, enc)
            if not ped.empty:
                base = base.merge(ped, on="horse_number", how="left")
            base["race_id"] = rid
            frames.append(base)

        if not frames:
            print("テストデータなし")
            return 1

        test_df = pd.concat(frames, ignore_index=True)
        print(f"  {len(test_df)}行 / {len(test_ids)}レース")

        # ── 予測スコア付与 ───────────────────────────────────────────────
        print("[Step4] 予測スコア算出...")
        test_df["prob_acc"] = acc_model.predict_proba(test_df)

        # EVモデル予測（69列 FEATURE_COLS に正確に整列・欠損列は0埋め）
        xte = test_df.reindex(columns=ev_feat_cols).fillna(0.0)
        if hasattr(honmei, "_Booster") and honmei._Booster is not None:
            test_df["prob_ev"] = honmei._Booster.predict(xte.values)
        else:
            test_df["prob_ev"] = 0.07  # フォールバック

        # レース内正規化（EV計算のため）
        test_df["prob_ev_norm"] = test_df.groupby("race_id")["prob_ev"].transform(
            lambda s: s / s.sum() if s.sum() > 0 else s
        )
        test_df["ev_score"] = test_df["prob_ev_norm"] * test_df["win_odds"]

        # ── 各戦略でのベット選定 ────────────────────────────────────────
        # [A] Accuracy Model 単体（Layer1のみ）
        bets_acc = acc_model.select_bets(test_df, max_per_race=args.max_per_race)

        # [B] EVモデル単体（全レースでEV≥threshold の馬）
        bets_ev = test_df[test_df["ev_score"] >= args.ev_threshold].copy()

        # [C] 二階層アンサンブル（Layer1 AND Layer2）
        # Layer1: Accuracy Model が選んだ馬
        # Layer2: その中でさらに EV≥threshold の馬
        if len(bets_acc) > 0:
            ensemble_mask = (
                bets_acc["ev_score"] >= args.ev_threshold
                if "ev_score" in bets_acc.columns
                else pd.Series(False, index=bets_acc.index)
            )
            # bets_accにev_scoreを付与
            bets_acc_with_ev = bets_acc.copy()
            bets_acc_with_ev["ev_score"] = test_df.loc[
                bets_acc.index, "ev_score"
            ].values
            bets_ensemble = bets_acc_with_ev[
                bets_acc_with_ev["ev_score"] >= args.ev_threshold
            ]
        else:
            bets_ensemble = pd.DataFrame()

        # [D] 全買い（ベースライン: 常に1着候補上位2頭を全レースで購入）
        bets_all = (
            test_df.sort_values("prob_ev", ascending=False)
            .groupby("race_id", group_keys=False)
            .head(args.max_per_race)
            .copy()
        )

        # ── 評価 ─────────────────────────────────────────────────────────
        r_acc = _eval_bets(bets_acc)
        r_ev = _eval_bets(bets_ev)
        r_ens = _eval_bets(bets_ensemble)
        r_all = _eval_bets(bets_all)

        # AUC
        try:
            auc_acc = roc_auc_score(test_df["is_win"], test_df["prob_acc"])
            auc_ev = roc_auc_score(test_df["is_win"], test_df["prob_ev"])
        except Exception:
            auc_acc = auc_ev = float("nan")

        print("\n" + "=" * 72)
        print(f" {'戦略':<24} {'ベット数':>8} {'的中率%':>8} {'ROI%':>8} {'AUC':>8}")
        print("-" * 72)

        rows = [
            ("全買い(ベースライン)", r_all, auc_ev),
            ("EVモデル単体(EV≥threshold)", r_ev, auc_ev),
            ("AccuracyModel単体(L1のみ)", r_acc, auc_acc),
            ("二階層アンサンブル(L1∩L2)", r_ens, float("nan")),
        ]
        for name, r, auc in rows:
            auc_str = (
                f"{auc:.4f}" if not (isinstance(auc, float) and auc != auc) else "  n/a"
            )
            print(
                f" {name:<24} {r['n']:>8d} {r['hit']:>8.1f} {r['roi']:>8.1f} {auc_str:>8}"
            )

        print("=" * 72)

        # ── アンサンブルの判定 ───────────────────────────────────────────
        print("\n■ 二階層アンサンブル（Layer1∩Layer2）の評価")
        print(
            f"  Accuracy単体: {r_acc['n']}件 → アンサンブル後: {r_ens['n']}件 "
            f"(絞り込み率 {r_ens['n'] / max(1, r_acc['n']) * 100:.0f}%)"
        )

        if r_ens["n"] > 0:
            acc_beats_ev = r_ens["roi"] > r_ev["roi"]
            acc_beats_acc = r_ens["roi"] > r_acc["roi"]
            ens_verdict = (
                "単体を上回る✅"
                if (acc_beats_ev and acc_beats_acc)
                else "EVは上回る✅"
                if acc_beats_ev
                else "AccuracyModelは上回る✅"
                if acc_beats_acc
                else "単体を下回る❌"
            )
            print(f"  ROI判定: {ens_verdict}")
            print(
                f"  二階層ROI {r_ens['roi']:.1f}% vs "
                f"EV単体{r_ev['roi']:.1f}% / Accuracy単体{r_acc['roi']:.1f}%"
            )

            print("\n■ オッズ分布（アンサンブル選別馬）")
            odds = bets_ensemble["win_odds"].dropna()
            print(
                f"  平均: {odds.mean():.1f}倍 / 中央値: {odds.median():.1f}倍 "
                f"/ 最小: {odds.min():.1f}倍 / 最大: {odds.max():.1f}倍"
            )
        else:
            print("  ⚠️ 二階層の積集合が0件 — EV閾値が高すぎる可能性")
            print(
                f"     --ev-threshold を下げて再試行してください（現在: {args.ev_threshold}）"
            )

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
