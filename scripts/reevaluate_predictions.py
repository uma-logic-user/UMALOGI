"""
scripts/reevaluate_predictions.py — 全 prediction_results を再評価して ROI を正規化する

修正内容:
  - combination_json の払戻キー重複を排除（三連複・馬連・ワイドで発生）
  - n_tickets = len(unique_combos) → invested = n_tickets × 100
  - ROI = payout / (n_tickets × 100) × 100 で統一

Usage:
    py -3 scripts/reevaluate_predictions.py --dry-run  # 変化量確認のみ
    py -3 scripts/reevaluate_predictions.py            # 実際に更新
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator


def _get_evaluatable_races(conn) -> list[str]:
    """prediction_results が存在 かつ race_payouts が揃っているレースを返す。"""
    rows = conn.execute("""
        SELECT DISTINCT p.race_id
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN race_payouts rp ON rp.race_id = p.race_id
        JOIN races r ON r.race_id = p.race_id
        ORDER BY r.date
    """).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="全 prediction_results を再評価する")
    parser.add_argument(
        "--dry-run", action="store_true", help="変化量を表示するのみ（DB 更新なし）"
    )
    args = parser.parse_args()

    conn = init_db()
    evaluator = Evaluator()

    race_ids = _get_evaluatable_races(conn)
    print(f"[再評価] 対象レース数: {len(race_ids)}")

    total_preds = 0
    roi_improved = 0
    errors = 0

    for i, race_id in enumerate(race_ids, 1):
        try:
            result = evaluator.evaluate_race(conn, race_id, dry_run=args.dry_run)
            for detail in result.hits:
                total_preds += 1
                if detail.is_hit and detail.roi > 0:
                    roi_improved += 1
            if result.errors:
                for e in result.errors:
                    print(f"  [{race_id}] WARN: {e}")
        except Exception as e:
            print(f"  [{race_id}] ERROR: {e}")
            errors += 1
            conn.rollback()
            continue

        if i % 50 == 0:
            print(f"  [{i}/{len(race_ids)}] 処理中...")

    print()
    print(
        f"[完了] レース={len(race_ids)} 予想={total_preds} 的中再計算={roi_improved} エラー={errors}"
    )
    if args.dry_run:
        print("  --dry-run: DB は更新されていません")
    conn.close()


if __name__ == "__main__":
    main()
