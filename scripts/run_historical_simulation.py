"""
2024-2025年 ヒストリカルシミュレーション一括実行スクリプト

処理フロー:
  1. 対象年・期間の全レースIDを取得
  2. simulate_pipeline() で各レースのAI予想を生成（predictions / prediction_horses に保存）
  3. post_race_pipeline() で払戻データと突合して prediction_results を生成

使用例:
    python scripts/run_historical_simulation.py               # 2024-2025年全て
    python scripts/run_historical_simulation.py --year 2024   # 2024年のみ
    python scripts/run_historical_simulation.py --date-from 2024-01-01 --date-to 2024-06-30
    python scripts/run_historical_simulation.py --force       # 既存予想を上書き
    python scripts/run_historical_simulation.py --workers 4   # 並列実行（実験的）
"""

from __future__ import annotations

import argparse
import io
import logging
import sqlite3
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("hist_sim")


# ─────────────────────────────────────────────────────────────────
# DB ヘルパー
# ─────────────────────────────────────────────────────────────────

def get_race_ids(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[str]:
    """対象期間の全レースIDを取得（race_results が存在するものだけ）。"""
    cur = conn.execute(
        """
        SELECT DISTINCT r.race_id
        FROM races r
        WHERE r.date BETWEEN ? AND ?
          AND EXISTS (
            SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id
          )
          AND EXISTS (
            SELECT 1 FROM race_payouts rp WHERE rp.race_id = r.race_id
          )
        ORDER BY r.race_id
        """,
        (date_from, date_to),
    )
    return [row[0] for row in cur.fetchall()]


def get_simulated_ids(conn: sqlite3.Connection, date_from: str, date_to: str) -> set[str]:
    """既に予想が存在するレースIDのセット。"""
    cur = conn.execute(
        """
        SELECT DISTINCT p.race_id
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
          AND (p.notes LIKE '%SIMULATE%' OR p.notes LIKE '%simulate%')
        """,
        (date_from, date_to),
    )
    return {row[0] for row in cur.fetchall()}


def get_evaluated_ids(conn: sqlite3.Connection, date_from: str, date_to: str) -> set[str]:
    """既に prediction_results が存在するレースIDのセット。"""
    cur = conn.execute(
        """
        SELECT DISTINCT p.race_id
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON p.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
        """,
        (date_from, date_to),
    )
    return {row[0] for row in cur.fetchall()}


# ─────────────────────────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────────────────────────

def run_simulation(
    date_from: str,
    date_to: str,
    force: bool = False,
) -> dict:
    """シミュレーション実行。"""
    from src.database.init_db import init_db
    from src.pipeline.simulation import simulate_pipeline

    conn = init_db()
    race_ids = get_race_ids(conn, date_from, date_to)
    already_done = set() if force else get_simulated_ids(conn, date_from, date_to)
    conn.close()

    todo = [rid for rid in race_ids if rid not in already_done]
    total   = len(race_ids)
    skipped = total - len(todo)

    logger.info("シミュレーション対象: %d件 / スキップ済み: %d件 / 新規実行: %d件",
                total, skipped, len(todo))

    stats = {"simulated": 0, "errors": 0, "skipped": skipped}
    t0 = time.time()

    for i, race_id in enumerate(todo, 1):
        try:
            result = simulate_pipeline(race_id)
            if "error" in result:
                logger.warning("[%d/%d] エラー %s: %s", i, len(todo), race_id, result["error"])
                stats["errors"] += 1
            else:
                stats["simulated"] += 1
        except Exception as exc:
            logger.error("[%d/%d] 例外 %s: %s", i, len(todo), race_id, exc)
            stats["errors"] += 1

        if i % 100 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed
            remaining = (len(todo) - i) / rate if rate > 0 else 0
            logger.info(
                "進捗: %d/%d (%.1f件/秒) 残り約%.0f分",
                i, len(todo), rate, remaining / 60,
            )

    elapsed_total = time.time() - t0
    logger.info(
        "シミュレーション完了: 実行=%d 스킵=%d エラー=%d 所要%.0f分",
        stats["simulated"], stats["skipped"], stats["errors"], elapsed_total / 60,
    )
    return stats


def run_evaluation(
    date_from: str,
    date_to: str,
    force: bool = False,
) -> dict:
    """全レースの払戻突合（精算）を実行。"""
    from src.database.init_db import init_db
    from src.ops.retrain_trigger import batch_evaluate_date

    conn = init_db()

    # 対象日付リストを取得
    cur = conn.execute(
        """
        SELECT DISTINCT r.date
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE r.date BETWEEN ? AND ?
          AND pr.id IS NULL
        ORDER BY r.date
        """,
        (date_from, date_to),
    )
    dates = [row[0] for row in cur.fetchall()]
    conn.close()

    logger.info("精算対象日数: %d日", len(dates))
    total_hits = 0

    for i, date in enumerate(dates, 1):
        try:
            conn2 = init_db()
            results = batch_evaluate_date(conn2, date, notify=False)
            hits = sum(r["evaluation"].hit_count for r in results if "evaluation" in r)
            total_hits += hits
            conn2.close()
            if i % 50 == 0:
                logger.info("精算進捗: %d/%d日 累計的中=%d", i, len(dates), total_hits)
        except Exception as exc:
            logger.error("精算エラー %s: %s", date, exc)

    logger.info("精算完了: %d日 累計的中=%d", len(dates), total_hits)
    return {"dates": len(dates), "total_hits": total_hits}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year",       type=int, help="対象年（--date-from/to より優先）")
    ap.add_argument("--date-from",  default="2024-01-01")
    ap.add_argument("--date-to",    default="2025-12-31")
    ap.add_argument("--force",      action="store_true", help="既存予想を上書き")
    ap.add_argument("--sim-only",   action="store_true", help="シミュレーションのみ（精算スキップ）")
    ap.add_argument("--eval-only",  action="store_true", help="精算のみ（シミュレーションスキップ）")
    args = ap.parse_args()

    date_from = f"{args.year}-01-01" if args.year else args.date_from
    date_to   = f"{args.year}-12-31" if args.year else args.date_to

    logger.info("=" * 60)
    logger.info("ヒストリカルシミュレーション開始: %s 〜 %s", date_from, date_to)
    logger.info("=" * 60)

    if not args.eval_only:
        run_simulation(date_from, date_to, force=args.force)

    if not args.sim_only:
        run_evaluation(date_from, date_to, force=args.force)

    logger.info("=" * 60)
    logger.info("全処理完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
