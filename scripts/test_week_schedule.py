"""
scripts/test_week_schedule.py — 1週間スケジュール貫通テスト

本番 umalogi.db を umalogi_test.db として一時コピーし、
全サブプロセス・外部API をモック化した状態で1週間分ジョブを連続実行する。

モック戦略（C案ハイブリッド）:
  - _run / _run_with_retry           : サブプロセスを実行せず rc=0 を返す
  - _send_discord*                   : 標準出力のみ（Webhook 非送信）
  - _notify_provisional_summary      : 標準出力のみ
  - backup_db                        : no-op（バックアップファイル未生成）
  - weekly_retrain                   : no-op（学習なし）
  - batch_evaluate_date              : 空リスト返却
  - weekly_auto_commit               : no-op（git push なし）
  - win5_batch                       : スキップ応答
  - infer_ranks                      : no-op
  - subprocess.run                   : no-op（generate_data.py 呼び出し用）
  - DB接続                           : DB_PATH 環境変数で umalogi_test.db を向く

テスト完了後（または例外時）は finally で umalogi_test.db を自動削除する。

Usage:
    python scripts/test_week_schedule.py
    python scripts/test_week_schedule.py --keep-db
    python scripts/test_week_schedule.py --jobs friday,sat_post,mon_retrain
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import shutil
import subprocess
import sys
import threading
import traceback
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("test_week_schedule")

_DB_SRC = _ROOT / "data" / "umalogi.db"
_DB_TEST = _ROOT / "data" / "umalogi_test.db"


# ================================================================
# モック関数
# ================================================================

def _mock_run(cmd: list[str], label: str, timeout: int = 3600) -> int:
    logger.info("[MOCK _run] label=%s", label)
    return 0


def _mock_run_with_retry(
    cmd: list[str],
    label: str,
    timeout: int = 3600,
    max_retries: int = 3,
    base_delay: float = 60.0,
) -> int:
    logger.info("[MOCK _run_with_retry] label=%s", label)
    return 0


def _mock_send_discord(text: str) -> None:
    logger.info("[MOCK Discord] %s", text[:100])


def _mock_send_discord_embed(embeds: list[dict]) -> None:
    title = embeds[0].get("title", "—") if embeds else "—"
    logger.info("[MOCK Discord Embed] %s", title)


def _mock_notify_provisional_summary(target_date: str) -> None:
    logger.info("[MOCK notify_provisional_summary] date=%s", target_date)


def _mock_backup_db(*args: Any, **kwargs: Any) -> Path:
    logger.info("[MOCK backup_db] スキップ")
    return _ROOT / "data" / "backups" / "mock_backup.db"


def _mock_weekly_retrain(conn: Any, *, validate: bool = True) -> dict:
    logger.info("[MOCK weekly_retrain] スキップ")
    return {"honmei": "mock-v0", "manji": "mock-v0"}


def _mock_batch_evaluate_date(
    conn: Any, date: str, *, notify: bool = True, dry_run: bool = False
) -> list:
    logger.info("[MOCK batch_evaluate_date] date=%s → 空リスト", date)
    return []


def _mock_weekly_auto_commit() -> bool:
    logger.info("[MOCK weekly_auto_commit] スキップ")
    return True


def _mock_win5_batch(*args: Any, **kwargs: Any) -> dict:
    logger.info("[MOCK win5_batch] スキップ")
    return {"skipped": True, "reason": "mock"}


def _mock_infer_ranks(conn: Any, year_filter: Any, dry_run: bool) -> dict:
    logger.info("[MOCK infer_ranks] スキップ")
    return {"rank1_set": 0, "rank2_set": 0, "rank3_set": 0, "skipped": 0}


def _mock_subprocess_run(*args: Any, **kwargs: Any) -> CompletedProcess:
    cmd_str = " ".join(str(a) for a in (args[0] if args else []))
    logger.info("[MOCK subprocess.run] %s", cmd_str[:80])
    return CompletedProcess(args=[], returncode=0)


# ================================================================
# 1週間シナリオ定義（時系列順）
# ================================================================

# (キー, 表示ラベル, 関数名, kwargs)
_WEEK_SCENARIO: list[tuple[str, str, str, dict]] = [
    ("friday",      "金 20:00  金曜バッチ（JVLink同期→暫定予想→Discord通知）", "job_friday_sync",       {}),
    ("sat_wood",    "土 07:30  朝調教同期",                                     "job_morning_wood",      {}),
    ("sat_runner",  "土 08:30  直前予想ループ起動",                              "job_today_auto_runner", {}),
    ("sat_win5",    "土 09:00  WIN5予測",                                        "job_win5_prediction",   {}),
    ("sat_umanity", "土 13:00  Umanity投稿",                                     "job_umanity_upload",    {}),
    ("sat_intra1",  "土 13:00  中間結果同期",                                    "job_intraday_sync",     {"target_date": "2026/05/02"}),
    ("sat_intra2",  "土 15:30  中間結果同期",                                    "job_intraday_sync",     {"target_date": "2026/05/02"}),
    ("sat_post",    "土 17:30  レース後処理（払戻同期→評価→増分学習→backup）",   "job_post_race",         {"target_date": "2026/05/02"}),
    ("sun_wood",    "日 07:30  朝調教同期",                                      "job_morning_wood",      {}),
    ("sun_runner",  "日 08:30  直前予想ループ起動",                               "job_today_auto_runner", {}),
    ("sun_win5",    "日 09:00  WIN5予測",                                         "job_win5_prediction",   {}),
    ("sun_umanity", "日 13:00  Umanity投稿",                                      "job_umanity_upload",    {}),
    ("sun_intra1",  "日 13:00  中間結果同期",                                     "job_intraday_sync",     {"target_date": "2026/05/03"}),
    ("sun_intra2",  "日 15:30  中間結果同期",                                     "job_intraday_sync",     {"target_date": "2026/05/03"}),
    ("sun_post",    "日 17:30  レース後処理（払戻同期→評価→増分学習→backup）",    "job_post_race",         {"target_date": "2026/05/03"}),
    ("mon_masters", "月 06:00  マスタデータ差分更新",                             "job_monday_masters",    {}),
    ("mon_retrain", "月 07:00  週次全件再学習",                                   "job_weekly_retrain",    {}),
    ("mon_git",     "月 08:00  GitHub 自動プッシュ",                              "job_git_push",          {}),
]


# ================================================================
# テスト実行
# ================================================================

def _wait_auto_runner_thread(timeout: float = 10.0) -> None:
    """today_auto_runner バックグラウンドスレッドの終了を待つ。"""
    for t in threading.enumerate():
        if t.name == "today_auto_runner":
            logger.info("[テスト] auto_runner スレッド待機中（最大 %.0f 秒）…", timeout)
            t.join(timeout=timeout)
            break


def run_week_test(jobs_filter: set[str] | None = None) -> bool:
    """
    1週間シナリオを実行する。

    Returns:
        True: 全ジョブ PASS / False: 1件以上 FAIL
    """
    import scripts.scheduler as sched

    os.environ["DB_PATH"] = str(_DB_TEST)

    results: list[tuple[str, str, str | None]] = []

    with contextlib.ExitStack() as stack:
        # ── サブプロセス・通知モック ───────────────────────────
        stack.enter_context(patch.object(sched, "_run",                    side_effect=_mock_run))
        stack.enter_context(patch.object(sched, "_run_with_retry",         side_effect=_mock_run_with_retry))
        stack.enter_context(patch.object(sched, "_send_discord",           side_effect=_mock_send_discord))
        stack.enter_context(patch.object(sched, "_send_discord_embed",     side_effect=_mock_send_discord_embed))
        stack.enter_context(patch.object(sched, "_notify_provisional_summary", side_effect=_mock_notify_provisional_summary))

        # ── 外部サービス・ファイル操作モック ──────────────────
        stack.enter_context(patch("src.ops.backup.backup_db",                       side_effect=_mock_backup_db))
        stack.enter_context(patch("src.ops.retrain_trigger.weekly_retrain",          side_effect=_mock_weekly_retrain))
        stack.enter_context(patch("src.ops.retrain_trigger.batch_evaluate_date",     side_effect=_mock_batch_evaluate_date))
        stack.enter_context(patch("src.ops.git_ops.weekly_auto_commit",              side_effect=_mock_weekly_auto_commit))
        stack.enter_context(patch("src.main_pipeline.win5_batch",                    side_effect=_mock_win5_batch))
        stack.enter_context(patch("scripts.infer_ranks_from_payouts.infer_ranks",    side_effect=_mock_infer_ranks))

        # ── generate_data.py などの直接 subprocess.run 呼び出し ─
        stack.enter_context(patch("subprocess.run", side_effect=_mock_subprocess_run))

        for key, label, fn_name, kwargs in _WEEK_SCENARIO:
            if jobs_filter and key not in jobs_filter:
                logger.info("⏩ SKIP  %s", label)
                continue

            fn = getattr(sched, fn_name)
            logger.info("")
            logger.info("=" * 68)
            logger.info("▶  %s", label)
            logger.info("=" * 68)

            try:
                fn(**kwargs)
                if fn_name == "job_today_auto_runner":
                    _wait_auto_runner_thread()
                results.append((key, "PASS", None))
                logger.info("✅ PASS  %s", label)
            except Exception:
                err = traceback.format_exc()
                logger.error("❌ FAIL  %s\n%s", label, err)
                results.append((key, "FAIL", err.splitlines()[-2] if err.splitlines() else ""))

    # ── サマリー出力 ──────────────────────────────────────────
    logger.info("")
    logger.info("=" * 68)
    logger.info("  テスト結果サマリー（%d ジョブ実行）", len(results))
    logger.info("=" * 68)
    for key, status, err_line in results:
        lbl = next(l for k, l, _, _ in _WEEK_SCENARIO if k == key)
        icon = "✅" if status == "PASS" else "❌"
        logger.info("%s %-6s  %s", icon, status, lbl)
        if err_line:
            logger.info("         ↳ %s", err_line)

    pass_count = sum(1 for _, s, _ in results if s == "PASS")
    fail_count = sum(1 for _, s, _ in results if s == "FAIL")
    logger.info("")
    logger.info("PASS: %d  /  FAIL: %d", pass_count, fail_count)
    return fail_count == 0


# ================================================================
# CLI エントリーポイント
# ================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="1週間スケジュール貫通テスト（ハイブリッドモック）"
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="テスト完了後も umalogi_test.db を削除しない",
    )
    parser.add_argument(
        "--jobs",
        metavar="KEY[,KEY...]",
        help=(
            "実行するジョブキーをカンマ区切りで指定\n"
            f"利用可能: {', '.join(k for k, *_ in _WEEK_SCENARIO)}"
        ),
    )
    args = parser.parse_args()

    jobs_filter: set[str] | None = None
    if args.jobs:
        jobs_filter = set(args.jobs.split(","))

    # ── テストDB 準備 ──────────────────────────────────────────
    if not _DB_SRC.exists():
        logger.error("本番 DB が見つかりません: %s", _DB_SRC)
        sys.exit(1)

    logger.info("テストDB を準備中: %s → %s", _DB_SRC.name, _DB_TEST.name)
    shutil.copy2(str(_DB_SRC), str(_DB_TEST))
    size_mb = _DB_TEST.stat().st_size / 1024 / 1024
    logger.info("テストDB コピー完了（%.1f MB）", size_mb)

    # ── テスト実行 ────────────────────────────────────────────
    success = False
    try:
        success = run_week_test(jobs_filter=jobs_filter)
    except Exception:
        logger.critical("テストスクリプト自体で致命的例外が発生しました", exc_info=True)
    finally:
        if not args.keep_db and _DB_TEST.exists():
            _DB_TEST.unlink()
            logger.info("テストDB を削除しました: %s", _DB_TEST.name)
        elif _DB_TEST.exists():
            logger.info("テストDB を保持しています: %s", _DB_TEST)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
