"""
自動再学習トリガー

レース確定後に呼び出されることを想定したモジュール。
  1. post_race_pipeline() を呼ぶと評価 → 通知 → 増分学習 を一括実行する。
  2. 週次の full_retrain も scheduler.py からここを呼び出す。

Usage (post-race):
    from src.ops.retrain_trigger import post_race_pipeline
    post_race_pipeline(conn, race_id="202401010101")

Usage (weekly full retrain):
    from src.ops.retrain_trigger import weekly_retrain
    weekly_retrain(conn)
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def post_race_pipeline(
    conn: sqlite3.Connection,
    race_id: str,
    *,
    notify: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    レース確定後の一連処理を実行する。

    Pipeline:
      1. Evaluator でレース結果を評価 (prediction_results に保存)
      2. NotificationDispatcher で SNS 通知
      3. IncrementalTrainer で増分学習

    Args:
        conn:     DB コネクション
        race_id:  確定済みレース ID
        notify:   SNS 通知を送信するか
        dry_run:  DB 書き込み・SNS 送信をスキップするか

    Returns:
        {"evaluation": EvaluationResult, "notified": [...], "model": {...}}
    """
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[3]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from src.evaluation.evaluator import Evaluator
    from src.notification.dispatcher import NotificationDispatcher
    from src.ml.incremental import IncrementalTrainer

    result: dict = {}

    # ── Step 1: 的中評価 ─────────────────────────────────────────
    evaluator = Evaluator()
    eval_result = evaluator.evaluate_race(conn, race_id, dry_run=dry_run)
    result["evaluation"] = eval_result
    logger.info(
        "評価完了: race=%s 的中=%d/全%d ROI=%.1f%%",
        race_id,
        eval_result.hit_count,
        len(eval_result.hits),
        eval_result.roi,
    )

    # ── Step 2: SNS 通知 ──────────────────────────────────────────
    if notify and not dry_run:
        dispatcher = NotificationDispatcher()
        notified = dispatcher.dispatch(eval_result)
        result["notified"] = [h.bet_type for h in notified]
    else:
        result["notified"] = []

    # ── Step 3: 増分学習 ──────────────────────────────────────────
    if not dry_run:
        try:
            trainer = IncrementalTrainer()
            versions = trainer.incremental_update(conn, new_race_ids=[race_id])
            result["model"] = {k: str(v) for k, v in versions.items()}
        except Exception as e:
            logger.error("増分学習失敗: %s", e)
            result["model"] = {"error": str(e)}
    else:
        result["model"] = {}

    return result


def weekly_retrain(
    conn: sqlite3.Connection,
    *,
    validate: bool = True,
) -> dict:
    """
    週次全件再学習を実行する。

    通常は毎週月曜の早朝（競馬がない時間帯）に scheduler.py から呼び出す。

    Returns:
        {"honmei": version_str, "manji": version_str}
    """
    from src.ml.incremental import IncrementalTrainer

    trainer = IncrementalTrainer()
    versions = trainer.full_retrain(conn, validate=validate)
    logger.info("週次再学習完了: %s", {k: str(v) for k, v in versions.items()})
    return {k: str(v) for k, v in versions.items()}


def batch_evaluate_date(
    conn: sqlite3.Connection,
    date: str,
    *,
    notify: bool = True,
    dry_run: bool = False,
) -> list[dict]:
    """
    指定日の全確定レースを一括評価・通知する。

    Args:
        date: "YYYY-MM-DD" 形式 (ISO 8601)
    """
    race_ids = [
        r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (date,),
        ).fetchall()
    ]
    logger.info("一括評価: %s の %d レース", date, len(race_ids))
    return [post_race_pipeline(conn, rid, notify=notify, dry_run=dry_run) for rid in race_ids]
