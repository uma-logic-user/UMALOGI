"""
UMA-LOGI AI — 自律スケジューラー

競馬週次サイクルに合わせた自動実行スクリプト。
`schedule` ライブラリを使って各タスクを登録し、常駐プロセスとして動作する。

スケジュール一覧:
  金曜 20:00   : JRA-VAN RACE 同期 + 出馬表取得
  土曜 07:30   : JRA-VAN WOOD 同期（調教タイム）
  日曜 07:30   : 同上
  土曜 16:00   : レース確定後 払戻同期 + 評価 + 通知 + 増分学習
  日曜 16:00   : 同上
  月曜 06:00   : マスタ差分更新 (DIFN/BLOD)
  月曜 07:00   : 週次全件再学習
  月曜 08:00   : GitHub 自動コミット・プッシュ

Usage:
    python scripts/scheduler.py           # デーモン起動
    python scripts/scheduler.py --run-now friday   # 即時実行（テスト用）
    python scripts/scheduler.py --run-now post_race --date 2024/01/06
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_ROOT / "data" / "scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")

try:
    import schedule  # type: ignore[import-untyped]
    _SCHEDULE_AVAILABLE = True
except ImportError:
    logger.warning("schedule がインストールされていません: pip install schedule")
    _SCHEDULE_AVAILABLE = False


# ================================================================
# 各ジョブ定義
# ================================================================

def job_friday_sync() -> None:
    """金曜夜: 出馬表 + JRA-VAN RACE 同期"""
    logger.info("=== [金曜バッチ] 開始 ===")
    try:
        from src.ops.data_sync import sync_friday, sync_race_results
        sync_friday()
        sync_race_results()
        logger.info("=== [金曜バッチ] 完了 ===")
    except Exception as e:
        logger.error("[金曜バッチ] 失敗: %s", e, exc_info=True)


def job_morning_wood() -> None:
    """土日朝: 調教タイム同期"""
    logger.info("=== [朝調教同期] 開始 ===")
    try:
        from src.ops.data_sync import sync_wood
        sync_wood()
        logger.info("=== [朝調教同期] 完了 ===")
    except Exception as e:
        logger.error("[朝調教同期] 失敗: %s", e, exc_info=True)


def job_post_race(target_date: str | None = None) -> None:
    """土日夕方: レース確定後の払戻同期 + 評価 + 通知 + 増分学習"""
    if target_date is None:
        target_date = date.today().strftime("%Y/%m/%d")
    logger.info("=== [レース後処理] %s 開始 ===", target_date)
    try:
        from src.database.init_db import init_db
        from src.ops.data_sync import sync_race_results, sync_payouts_from_netkeiba
        from src.ops.retrain_trigger import batch_evaluate_date

        conn = init_db()
        try:
            # 払戻データを確実に取得
            sync_race_results()
            sync_payouts_from_netkeiba(target_date.replace("/", ""))

            # 評価 + 通知 + 増分学習
            results = batch_evaluate_date(conn, target_date, notify=True)
            hit_count = sum(r["evaluation"].hit_count for r in results if "evaluation" in r)
            logger.info("[レース後処理] 完了: %d レース 合計的中=%d", len(results), hit_count)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[レース後処理] 失敗: %s", e, exc_info=True)


def job_monday_masters() -> None:
    """月曜: マスタデータ差分更新"""
    logger.info("=== [マスタ更新] 開始 ===")
    try:
        from src.ops.data_sync import sync_masters
        sync_masters(full=False)
        logger.info("=== [マスタ更新] 完了 ===")
    except Exception as e:
        logger.error("[マスタ更新] 失敗: %s", e, exc_info=True)


def job_weekly_retrain() -> None:
    """月曜: 全件再学習"""
    logger.info("=== [週次再学習] 開始 ===")
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import weekly_retrain
        conn = init_db()
        try:
            result = weekly_retrain(conn)
            logger.info("[週次再学習] 完了: %s", result)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[週次再学習] 失敗: %s", e, exc_info=True)


def job_git_push() -> None:
    """月曜: GitHub 自動プッシュ"""
    logger.info("=== [Git プッシュ] 開始 ===")
    try:
        from src.ops.git_ops import weekly_auto_commit
        success = weekly_auto_commit()
        logger.info("[Git プッシュ] %s", "成功" if success else "失敗")
    except Exception as e:
        logger.error("[Git プッシュ] 失敗: %s", e, exc_info=True)


# ================================================================
# スケジューラー本体
# ================================================================

def register_schedules() -> None:
    """全ジョブをスケジュールに登録する。"""
    if not _SCHEDULE_AVAILABLE:
        raise RuntimeError("schedule ライブラリが必要です: pip install schedule")

    schedule.every().friday.at("20:00").do(job_friday_sync)
    schedule.every().saturday.at("07:30").do(job_morning_wood)
    schedule.every().sunday.at("07:30").do(job_morning_wood)
    schedule.every().saturday.at("16:00").do(job_post_race)
    schedule.every().sunday.at("16:00").do(job_post_race)
    schedule.every().monday.at("06:00").do(job_monday_masters)
    schedule.every().monday.at("07:00").do(job_weekly_retrain)
    schedule.every().monday.at("08:00").do(job_git_push)

    logger.info("スケジュール登録完了: %d ジョブ", len(schedule.jobs))
    for job in schedule.jobs:
        logger.info("  %s", job)


def run_daemon() -> None:
    """スケジューラーをデーモンとして常駐させる。Ctrl+C で終了。"""
    register_schedules()
    logger.info("UMA-LOGI AI スケジューラー起動 — Ctrl+C で終了")
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("スケジューラー停止")


# ================================================================
# CLI
# ================================================================

_JOB_MAP = {
    "friday":      job_friday_sync,
    "wood":        job_morning_wood,
    "post_race":   job_post_race,
    "masters":     job_monday_masters,
    "retrain":     job_weekly_retrain,
    "git":         job_git_push,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="UMA-LOGI AI スケジューラー")
    parser.add_argument(
        "--run-now",
        metavar="JOB",
        choices=list(_JOB_MAP.keys()),
        help=f"即時実行するジョブ: {list(_JOB_MAP.keys())}",
    )
    parser.add_argument("--date", help="post_race ジョブの対象日 YYYY/MM/DD")
    args = parser.parse_args()

    if args.run_now:
        logger.info("即時実行: %s", args.run_now)
        fn = _JOB_MAP[args.run_now]
        if args.run_now == "post_race" and args.date:
            fn(args.date)
        else:
            fn()
    else:
        run_daemon()


if __name__ == "__main__":
    main()
