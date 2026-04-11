"""
当日の全レースに対してレース直前予想を自動実行するスクリプト。

GitHub Actions の prerace-predict ジョブから呼び出される。
race_id を指定しない場合（スケジュール実行）にここに処理を集約することで、
YAML 内のインラインスクリプトを排除し、構文エラーを防ぐ。

使用方法:
    python scripts/run_prerace_auto.py
    python scripts/run_prerace_auto.py --date 20260412
"""

from __future__ import annotations

import argparse
import datetime
import logging
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _fetch_race_ids(target_date: str) -> list[str]:
    """races テーブルから指定日のレース ID を取得する。"""
    from src.database.init_db import init_db

    formatted = f"{target_date[:4]}/{target_date[4:6]}/{target_date[6:8]}"
    conn = init_db()
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
        (formatted,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="当日全レース直前予想バッチ")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（省略時=当日）")
    args = parser.parse_args()

    target_date = args.date or datetime.date.today().strftime("%Y%m%d")

    race_ids = _fetch_race_ids(target_date)
    if not race_ids:
        logger.warning("対象日 %s のレースが見つかりません", target_date)
        sys.exit(0)

    logger.info("本日のレース: %d 件 (date=%s)", len(race_ids), target_date)

    failed = 0
    for rid in race_ids:
        logger.info("  予想中: %s", rid)
        result = subprocess.run(
            [sys.executable, "-m", "src.main_pipeline", "prerace", rid],
            capture_output=False,
        )
        if result.returncode != 0:
            failed += 1
            logger.error("  [ERROR] %s 失敗", rid)

    logger.info("完了: 成功=%d 失敗=%d", len(race_ids) - failed, failed)
    # 全件失敗時のみ非ゼロ終了（一部失敗はワークフロー継続）
    sys.exit(1 if failed == len(race_ids) else 0)


if __name__ == "__main__":
    main()
