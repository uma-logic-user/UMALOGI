"""
scripts/validate_feature.py — 新特徴量の本番統合ゲート CLI（W-097）

`src/ml/feature_gate.py` の複数 cutoff ウォークフォワード検証を、研究アセンブラ
（backtest_v2_oos の _assemble＝本番 6 列ベース＋前走系＋血統TE）に対して実行する
標準窓口。**新特徴量はこのゲートを PASS しない限り本番統合してはならない**（条項8）。

判定結果は data/feature_gate_results.json に追記され、検証履歴の単一台帳となる。

使い方::
    # prerun が出力する任意の列（例 prev_trouble_proxy）を検証
    py scripts/validate_feature.py --candidate prev_trouble_proxy
    # 複数列をまとめて1候補として
    py scripts/validate_feature.py --candidate colA,colB --name comboAB
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

from scripts.backtest_v2_oos import BASE_COLS, _assemble, _race_ids  # noqa: E402
from src.features.pedigree_te import SireEncoder  # noqa: E402
from src.ml.feature_gate import GatePolicy, walk_forward_gate  # noqa: E402

_MANIFEST = _ROOT / "data" / "feature_gate_results.json"
_DEFAULT_CUTOFFS = "2025-10-01,2025-11-01,2025-12-01,2026-01-01,2026-02-01,2026-03-01"


def _encoder_fn(conn: sqlite3.Connection, cutoff: str):
    return SireEncoder().fit(conn, cutoff_date=cutoff, surface="芝")


def _record(result_dict: dict) -> None:
    """検証台帳へ追記（単一真実源）。"""
    history: list[dict] = []
    if _MANIFEST.exists():
        try:
            history = json.loads(_MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history.insert(0, result_dict)
    _MANIFEST.write_text(
        json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run_gate(
    candidate_cols: list[str],
    name: str,
    cutoffs: list[str],
    *,
    train_cap: int = 1500,
    test_cap: int = 600,
    policy: GatePolicy | None = None,
) -> dict:
    conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
    try:
        res = walk_forward_gate(
            conn=conn,
            feature_name=name,
            base_cols=BASE_COLS,
            candidate_cols=candidate_cols,
            race_ids_fn=_race_ids,
            assemble_fn=_assemble,
            encoder_fn=_encoder_fn,
            cutoffs=cutoffs,
            train_cap=train_cap,
            test_cap=test_cap,
            policy=policy,
        )
    finally:
        conn.close()
    out = res.to_dict()
    out["validated_at"] = datetime.now().isoformat(timespec="seconds")
    out["candidate_cols"] = candidate_cols
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="新特徴量の本番統合ゲート検証")
    ap.add_argument("--candidate", required=True, help="候補列（カンマ区切り）")
    ap.add_argument("--name", default=None, help="候補名（省略時は列名）")
    ap.add_argument("--cutoffs", default=_DEFAULT_CUTOFFS)
    ap.add_argument("--train-cap", type=int, default=1500)
    ap.add_argument("--test-cap", type=int, default=600)
    args = ap.parse_args()

    cols = [c.strip() for c in args.candidate.split(",") if c.strip()]
    name = args.name or "+".join(cols)
    cutoffs = [c.strip() for c in args.cutoffs.split(",") if c.strip()]

    print(f"=== 特徴量ゲート検証: {name}  ({len(cutoffs)} cutoff) ===")
    out = run_gate(cols, name, cutoffs, train_cap=args.train_cap, test_cap=args.test_cap)
    _record(out)

    print("\n" + "=" * 60)
    verdict = "✅ PASS（本番統合の検討可）" if out["passed"] else "❌ FAIL（本番統合 不可）"
    print(f"{name}: {verdict}")
    print(f"  {out['reason']}")
    print(f"  台帳: {_MANIFEST}")
    print("=" * 60)
    return 0 if out["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
