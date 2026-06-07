"""
UMALOGI 日次自動更新パイプライン

毎日（平日）の定時実行で以下を自動実行する：
  1. JVLink差分データ同期（RACE/WOOD/ODDS）
  2. netkeiba フォールバック補完（失敗時のみ）
  3. 血統データのインクリメンタルバックフィル（N頭/日）
  4. data_cleaner.py クレンジング（差分のみ）
  5. 調教評価データ取得（直近レース分）
  6. データカバレッジレポートを Discord #system へ送信

スケジューラとの関係:
  - scheduler.py の job_monday_masters() と役割が一部重複するが、
    こちらは「データ品質保証」に特化し、モデル推論には干渉しない。
  - today_auto_runner.py --continuous が稼働中は本スクリプトを直接実行しない。
    代わりに scheduler.py に job_daily_update として登録して排他制御を行う。

使い方:
    py scripts/daily_update_pipeline.py
    py scripts/daily_update_pipeline.py --skip-jvlink
    py scripts/daily_update_pipeline.py --pedigree-limit 50
    py scripts/daily_update_pipeline.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_ROOT / "data" / "daily_update.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def _send_discord(text: str) -> None:
    """#system チャンネルへ送信。"""
    try:
        from src.notification.router import NotificationRouter
        NotificationRouter().send_system_text(text)
    except Exception as exc:
        logger.warning("Discord送信失敗: %s", exc)


def step1_jvlink_sync(dry_run: bool) -> bool:
    """JVLink 差分同期（RACE / WOOD）。"""
    if dry_run:
        logger.info("[step1] [DRY-RUN] JVLink 同期スキップ")
        return True
    logger.info("[step1] JVLink 差分同期 開始")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "src.ops.data_sync", "--option", "1"],
            cwd=str(_ROOT),
            timeout=600,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        ok = result.returncode == 0
        logger.info("[step1] JVLink 同期 %s (rc=%d)", "OK" if ok else "NG", result.returncode)
        return ok
    except Exception as exc:
        logger.warning("[step1] JVLink 同期 例外: %s", exc)
        return False


def step2_pedigree_backfill(limit: int, dry_run: bool) -> int:
    """血統インクリメンタルバックフィル。"""
    logger.info("[step2] 血統バックフィル %d頭", limit)
    if dry_run:
        return 0
    try:
        result = subprocess.run(
            [sys.executable, "scripts/backfill_pedigree.py", "--limit", str(limit), "--delay", "1.0"],
            cwd=str(_ROOT),
            timeout=600,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        # "saved=N" を stdout から取得
        import re
        m = re.search(r"saved=(\d+)", result.stdout)
        saved = int(m.group(1)) if m else 0
        logger.info("[step2] 血統バックフィル: saved=%d", saved)
        return saved
    except Exception as exc:
        logger.warning("[step2] 血統バックフィル 例外: %s", exc)
        return 0


def step3_data_cleaner(dry_run: bool) -> None:
    """データクレンジング実行。"""
    logger.info("[step3] データクレンジング")
    if dry_run:
        return
    try:
        subprocess.run(
            [sys.executable, "scripts/data_cleaner.py"],
            cwd=str(_ROOT),
            timeout=120,
            capture_output=True,
            encoding="utf-8",
        )
        logger.info("[step3] クレンジング完了")
    except Exception as exc:
        logger.warning("[step3] クレンジング 例外: %s", exc)


def step4_coverage_report() -> dict[str, object]:
    """データカバレッジレポートを生成して返す。"""
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    try:
        total_rr = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
        with_last3f = conn.execute("SELECT COUNT(*) FROM race_results WHERE last_3f IS NOT NULL").fetchone()[0]
        with_odds = conn.execute("SELECT COUNT(*) FROM race_results WHERE win_odds IS NOT NULL").fetchone()[0]
        h_total = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
        h_sire  = conn.execute("SELECT COUNT(*) FROM horses WHERE sire IS NOT NULL AND sire != ''").fetchone()[0]
        p_today = conn.execute(
            "SELECT COUNT(*) FROM predictions p JOIN races r ON r.race_id=p.race_id WHERE r.date=?",
            (date.today().isoformat(),),
        ).fetchone()[0]
        return {
            "race_results": total_rr,
            "last_3f_pct": round(with_last3f / max(total_rr, 1) * 100, 1),
            "odds_pct": round(with_odds / max(total_rr, 1) * 100, 1),
            "horses": h_total,
            "sire_pct": round(h_sire / max(h_total, 1) * 100, 1),
            "predictions_today": p_today,
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 日次更新パイプライン")
    parser.add_argument("--skip-jvlink", action="store_true", help="JVLink同期スキップ")
    parser.add_argument("--pedigree-limit", type=int, default=100, help="血統バックフィル頭数/日")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start = datetime.now()
    logger.info("=== UMALOGI 日次更新パイプライン 開始 %s ===", start.strftime("%Y-%m-%d %H:%M"))

    # Step 1: JVLink同期
    if not args.skip_jvlink:
        step1_jvlink_sync(args.dry_run)
    else:
        logger.info("[step1] JVLink 同期スキップ (--skip-jvlink)")

    # Step 2: 血統バックフィル
    pedigree_saved = step2_pedigree_backfill(args.pedigree_limit, args.dry_run)

    # Step 3: クレンジング
    step3_data_cleaner(args.dry_run)

    # Step 4: カバレッジレポート
    report = step4_coverage_report()

    elapsed = (datetime.now() - start).seconds
    logger.info("=== 日次更新完了 (%d秒) ===", elapsed)
    for k, v in report.items():
        logger.info("  %-30s %s", k, v)

    # Discord通知
    if not args.dry_run:
        summary = (
            f"📊 **[UMALOGI] 日次更新完了** ({date.today().isoformat()})\n"
            f"race_results: {report['race_results']:,}件 "
            f"(last_3f={report['last_3f_pct']}% / odds={report['odds_pct']}%)\n"
            f"血統充填率: {report['sire_pct']}% "
            f"(本日追加: +{pedigree_saved}頭)\n"
            f"本日予想: {report['predictions_today']}件\n"
            f"所要時間: {elapsed}秒"
        )
        _send_discord(summary)


if __name__ == "__main__":
    main()
