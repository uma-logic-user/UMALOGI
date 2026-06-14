"""
scripts/research_cross_features.py — 前走不利クロス特徴量の平日研究 WF 検証（W-097 Task3）

docs/research/cross_feature_ideas.md の上位3クロス特徴量を実装し、
src/ml/feature_gate のウォークフォワード・ゲートで本番採否を検証する。
平日バックグラウンドで実行し、結果を data/cross_feature_wf_results.json に保存する。

上位3クロス（前走不利＝prev_trouble_proxy を起点）:
  x_trouble_inner       = 前走不利 × 今回内枠(gate<=4)
  x_trouble_samecourse  = 前走不利 × 同コース複勝実績
  x_trouble_value       = 前走不利 × 人気妙味(log1p(win_odds))

効率化: cutoff ごとに研究データを1回だけ組み立て、3候補を同一 train/test で同時評価する。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from scripts.backtest_v2_oos import BASE_COLS, _assemble, _race_ids  # noqa: E402
from src.features.pedigree_te import SireEncoder  # noqa: E402
from src.ml.feature_gate import (  # noqa: E402
    GatePolicy,
    _predict,
    _train_lgbm,
    add_months,
    evaluate_roi_auc,
    summarize_gate,
)

_OUT = _ROOT / "data" / "cross_feature_wf_results.json"
_CUTOFFS = ["2025-10-01", "2025-11-01", "2025-12-01", "2026-01-01", "2026-02-01", "2026-03-01"]

# クロス特徴量名 → 計算関数（_assemble 出力 df から算出）
CROSS_FEATURES: dict[str, str] = {
    "x_trouble_inner": "前走不利 × 今回内枠(gate<=4)",
    "x_trouble_samecourse": "前走不利 × 同コース複勝実績",
    "x_trouble_value": "前走不利 × 人気妙味 log1p(win_odds)",
}


def _add_cross_columns(df: pd.DataFrame) -> pd.DataFrame:
    """_assemble 出力に上位3クロス特徴量を付与する（リークフリー: 前走＋当日エントリーのみ）。"""
    trouble = pd.to_numeric(df.get("prev_trouble_proxy"), errors="coerce").fillna(0.0)
    gate = pd.to_numeric(df.get("gate_number"), errors="coerce")
    same = pd.to_numeric(df.get("same_course_place_rate"), errors="coerce").fillna(0.0)
    odds = pd.to_numeric(df.get("win_odds"), errors="coerce").fillna(0.0)

    inner = (gate <= 4).astype(float).fillna(0.0)
    df["x_trouble_inner"] = trouble * inner
    df["x_trouble_samecourse"] = trouble * same
    df["x_trouble_value"] = trouble * np.log1p(odds.clip(lower=0))
    return df


def _assemble_with_cross(conn, ids, enc):
    return _add_cross_columns(_assemble(conn, ids, enc))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-lo", default="2024-01-01")
    ap.add_argument("--test-months", type=int, default=2)
    ap.add_argument("--train-cap", type=int, default=1500)
    ap.add_argument("--test-cap", type=int, default=600)
    ap.add_argument("--cutoffs", default=",".join(_CUTOFFS))
    args = ap.parse_args()

    cutoffs = [c.strip() for c in args.cutoffs.split(",") if c.strip()]
    conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
    # candidate -> list of per-cutoff rows
    per: dict[str, list[dict]] = {k: [] for k in CROSS_FEATURES}
    try:
        for cutoff in cutoffs:
            test_hi = add_months(cutoff, args.test_months)
            enc = SireEncoder().fit(conn, cutoff_date=cutoff, surface="芝")
            tr = _assemble_with_cross(
                conn, _race_ids(conn, args.train_lo, cutoff, args.train_cap), enc
            )
            te = _assemble_with_cross(
                conn, _race_ids(conn, cutoff, test_hi, args.test_cap), enc
            )
            if tr.empty or te.empty:
                print(f"{cutoff}: データ不足スキップ", flush=True)
                continue

            m_base = _train_lgbm(tr, BASE_COLS)
            r_base = evaluate_roi_auc(te, _predict(m_base, te, BASE_COLS), 1.0)
            print(f"\n[{cutoff}] BASE ROI={r_base['roi']:.1f}% AUC={r_base['auc']:.4f}", flush=True)

            for col in CROSS_FEATURES:
                cols = BASE_COLS + [col]
                m = _train_lgbm(tr, cols)
                r = evaluate_roi_auc(te, _predict(m, te, cols), 1.0)
                row = {
                    "cutoff": cutoff,
                    "test_races": len(te["race_id"].unique()),
                    "roi_base": round(r_base["roi"], 2),
                    "roi_full": round(r["roi"], 2),
                    "d_roi": round(r["roi"] - r_base["roi"], 2),
                    "d_auc": round(r["auc"] - r_base["auc"], 5),
                }
                per[col].append(row)
                print(
                    f"    {col:22s} ROI->{r['roi']:6.1f}% (d={row['d_roi']:+5.1f}pp) "
                    f"dAUC={row['d_auc']:+.4f}",
                    flush=True,
                )

        # ゲート判定
        results = []
        policy = GatePolicy()
        print("\n" + "=" * 64, flush=True)
        print("ゲート判定（複数 cutoff ウォークフォワード）", flush=True)
        print("=" * 64, flush=True)
        for col, desc in CROSS_FEATURES.items():
            res = summarize_gate(per[col], col, policy)
            d = res.to_dict()
            d["description"] = desc
            d["validated_at"] = datetime.now().isoformat(timespec="seconds")
            results.append(d)
            verdict = "✅ PASS" if res.passed else "❌ FAIL"
            print(f"{col} ({desc})", flush=True)
            print(f"  {verdict}: {res.reason}", flush=True)

        _OUT.write_text(
            json.dumps(
                {"generated_at": datetime.now().isoformat(), "policy": policy.__dict__, "results": results},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\n結果保存: {_OUT}", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    print(f"=== クロス特徴量 WF 研究 開始 {datetime.now():%Y-%m-%d %H:%M} ===", flush=True)
    raise SystemExit(main())
