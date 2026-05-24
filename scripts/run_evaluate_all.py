"""
全過去レース 一括的中評価バッチ
race_results.rank が存在し race_payouts が揃っているレースに対して
Evaluator.evaluate_race() を実行し prediction_results を更新する。

Usage:
    py scripts/run_evaluate_all.py [--dry-run] [--date YYYY-MM-DD] [--force]

Options:
    --dry-run   : DB への書き込みをスキップ（動作確認用）
    --date      : 指定日のみ評価（省略時は全件）
    --force     : 既に is_hit が記録済みの予想も再評価（上書き）
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _fetch_target_race_ids(conn, date_filter: str | None, force: bool) -> list[str]:
    """評価対象レース ID を返す。

    条件:
      - race_results に rank IS NOT NULL のデータが存在する
      - race_payouts にデータが存在する
      - predictions が存在する
      - force=False の場合、prediction_results.is_hit が NULL のもののみ
    """
    base_sql = """
        SELECT DISTINCT r.race_id
        FROM races r
        WHERE EXISTS (
            SELECT 1 FROM race_results rr
            WHERE rr.race_id = r.race_id AND rr.rank IS NOT NULL AND rr.rank > 0
        )
        AND EXISTS (
            SELECT 1 FROM race_payouts rp
            WHERE rp.race_id = r.race_id
        )
        AND EXISTS (
            SELECT 1 FROM predictions p
            WHERE p.race_id = r.race_id AND p.bet_type != '馬分析'
        )
    """
    params: list[str] = []

    if date_filter:
        base_sql += " AND r.date = ?"
        params.append(date_filter)

    if not force:
        # prediction_results が未記録の予想が1件以上あるレースのみ
        base_sql += """
            AND EXISTS (
                SELECT 1 FROM predictions p2
                LEFT JOIN prediction_results pr ON pr.prediction_id = p2.id
                WHERE p2.race_id = r.race_id
                  AND p2.bet_type != '馬分析'
                  AND pr.id IS NULL
            )
        """

    base_sql += " ORDER BY r.race_id"
    rows = conn.execute(base_sql, params).fetchall()
    return [r[0] for r in rows]


def run(
    date_filter: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    conn = init_db()
    evaluator = Evaluator()

    race_ids = _fetch_target_race_ids(conn, date_filter, force)
    logger.info(
        "評価対象: %d レース (date=%s, force=%s, dry_run=%s)",
        len(race_ids), date_filter or "全件", force, dry_run,
    )

    if not race_ids:
        logger.info("評価対象のレースがありません。")
        conn.close()
        return

    total_hit = 0
    total_pred = 0
    total_payout = 0.0
    total_invest = 0.0
    error_races: list[str] = []

    for i, race_id in enumerate(race_ids, 1):
        try:
            result = evaluator.evaluate_race(conn, race_id, dry_run=dry_run)
        except Exception as exc:
            logger.error("[%d/%d] %s 評価失敗: %s", i, len(race_ids), race_id, exc)
            error_races.append(race_id)
            continue

        n_hit = result.hit_count
        n_pred = len(result.hits)
        total_hit += n_hit
        total_pred += n_pred
        total_payout += result.total_payout
        total_invest += result.total_invested

        status = "✅" if n_hit > 0 else "  "
        roi_str = f"ROI={result.roi:.0f}%" if result.total_invested > 0 else "ROI=N/A"
        logger.info(
            "%s [%d/%d] %s %s %d/%d的中 %s",
            status, i, len(race_ids),
            race_id, result.race_name[:12],
            n_hit, n_pred, roi_str,
        )
        if result.errors:
            for err in result.errors:
                logger.warning("  %s", err)

    # ── サマリ ──
    overall_roi = (total_payout / total_invest * 100.0) if total_invest > 0 else 0.0
    logger.info("=" * 60)
    logger.info(
        "完了: %d レース評価 | 的中 %d/%d | 投資¥%s | 払戻¥%s | ROI=%.1f%%",
        len(race_ids), total_hit, total_pred,
        f"{total_invest:,.0f}", f"{total_payout:,.0f}", overall_roi,
    )
    if error_races:
        logger.warning("評価失敗 %d レース: %s", len(error_races), error_races[:10])
    if dry_run:
        logger.info("[DRY-RUN] DB への書き込みはスキップされました")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="全過去レース 一括的中評価バッチ")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD 指定日のみ評価")
    parser.add_argument("--dry-run", action="store_true", help="DB書き込みをスキップ")
    parser.add_argument("--force", action="store_true", help="既に記録済みの予想も再評価")
    args = parser.parse_args()

    run(date_filter=args.date, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
