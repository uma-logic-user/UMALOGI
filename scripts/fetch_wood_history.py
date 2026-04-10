"""
WOOD データ一括取得スクリプト（2021-01-01 〜 2025-04-01）
==========================================================

JV-Link は 32bit COM サーバーのため、subprocess で py -3.14-32 を呼び出します。
JVOpen(fromtime, option=2/SETUP) は全量再取得モード。
option=1（差分）では古いデータが取得できないため option=2 を使用。

既取得データは training_times/training_hillwork の UPSERT で安全に上書きされます。

使い方:
  python scripts/fetch_wood_history.py
  python scripts/fetch_wood_history.py --fromtime 20230101  # 途中再開
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────────────────────

DEFAULT_FROMTIME = "20210101"
MAX_RETRIES      = 5
BACKOFF_BASE     = 60   # 秒（指数: 60, 120, 240, 480, 960）
TIMEOUT_SEC      = 28800  # 8時間（SETUP モードは全量取得のため長めに設定）

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/fetch_wood_history.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def fetch_with_retry(fromtime: str) -> bool:
    """WOOD データを fromtime から取得。エラー時は MAX_RETRIES 回リトライ。"""
    cmd = [
        "py", "-3.14-32",
        "-m", "src.scraper.jravan_client",
        "--fromtime", fromtime,
        "--dataspec", "WOOD",
        "--option",   "1",  # WOOD は option=2(SETUP)非対応。option=1 で取得できる範囲が上限
    ]

    logger.info("=" * 60)
    logger.info("WOOD 一括取得開始: fromtime=%s", fromtime)
    logger.info("コマンド: %s", " ".join(cmd))
    logger.info("=" * 60)

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info("試行 %d/%d ...", attempt, MAX_RETRIES)
        try:
            result = subprocess.run(
                cmd,
                cwd=str(ROOT),
                timeout=TIMEOUT_SEC,
            )
            if result.returncode == 0:
                logger.info("完了 (試行 %d)", attempt)
                return True
            logger.warning("returncode=%d", result.returncode)

        except subprocess.TimeoutExpired:
            logger.error("タイムアウト (%d 秒)", TIMEOUT_SEC)
        except Exception as exc:
            logger.error("例外: %s", exc)

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            logger.info("  → %d 秒後にリトライ...", wait)
            time.sleep(wait)

    logger.error("最大リトライ回数 (%d) を超過。中断します。", MAX_RETRIES)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="WOOD データ一括取得（リトライ付き）")
    parser.add_argument(
        "--fromtime", default=DEFAULT_FROMTIME,
        help="取得開始日 YYYYMMDD（デフォルト: %(default)s）",
    )
    args = parser.parse_args()

    ok = fetch_with_retry(args.fromtime)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
