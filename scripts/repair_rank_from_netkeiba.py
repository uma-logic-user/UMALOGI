"""
2025年 race_results 着順補完スクリプト

race_results で rank が NULL（着順未記録）の行を netkeiba からの
スクレイピング結果で補完する。

対象: 2025年レースで ranked_horses < total_horses * 0.8 のもの
戦略: fetch_race_results(fetch_pedigree=False) → UPDATE rank only

v2 改善点:
- 取得失敗レースはチェックポイントに追加しない（再実行で再試行）
- リトライ間に適切な delay を付与（レートリミット回避）
- 連続失敗 N 件で長時間クールダウン
"""

import sqlite3
import sys
import time
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scraper.netkeiba import fetch_race_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            ROOT / "data" / "repair_rank_log.txt", encoding="utf-8", mode="a"
        ),
    ],
)
logger = logging.getLogger(__name__)

DB_PATH = ROOT / "data" / "umalogi.db"
CHECKPOINT_PATH = ROOT / "data" / "repair_rank_checkpoint.txt"

DELAY_SEC = 2.0  # レース間インターバル
RETRY_DELAY_SEC = 1.5  # fetch_race_results 内リトライ間隔
COOLDOWN_THRESHOLD = 5  # 連続失敗 N 件でクールダウン
COOLDOWN_SEC = 60  # クールダウン待機秒数


def _load_checkpoint() -> set[str]:
    if CHECKPOINT_PATH.exists():
        lines = CHECKPOINT_PATH.read_text(encoding="utf-8").splitlines()
        return set(l for l in lines if l.strip())
    return set()


def _save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_PATH.write_text("\n".join(sorted(done)), encoding="utf-8")


def _get_target_races(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """着順補完が必要なレースを返す (race_id, date)"""
    rows = conn.execute("""
        WITH stats AS (
            SELECT
                rr.race_id,
                r.date,
                COUNT(rr.id) as total_h,
                SUM(CASE WHEN rr.rank BETWEEN 1 AND 18 THEN 1 ELSE 0 END) as ranked_h
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.date BETWEEN '2025-01-01' AND '2025-12-31'
              AND r.date < date('now')
            GROUP BY rr.race_id
        )
        SELECT race_id, date
        FROM stats
        WHERE ranked_h < total_h * 0.8
        ORDER BY date, race_id
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def repair_race(conn: sqlite3.Connection, race_id: str, race_date: str) -> int | None:
    """
    1レースの着順を netkeiba から取得して UPDATE する。
    成功: 更新行数（0以上）
    失敗: None（チェックポイントに追加しない）
    """
    try:
        info = fetch_race_results(
            race_id,
            race_date=race_date,
            fetch_pedigree=False,
            delay=RETRY_DELAY_SEC,  # リトライ間隔を確保
        )
    except Exception as e:
        logger.warning("取得失敗 %s: %s", race_id, e)
        return None

    if not info.results:
        logger.warning("結果なし %s", race_id)
        return None

    updated = 0
    for hr in info.results:
        if hr.rank is None or hr.horse_number is None:
            continue
        cur = conn.execute(
            "UPDATE race_results SET rank=? "
            "WHERE race_id=? AND horse_number=? "
            "AND (rank IS NULL OR rank NOT BETWEEN 1 AND 18)",
            (hr.rank, race_id, hr.horse_number),
        )
        updated += cur.rowcount

    conn.commit()
    return updated


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    races = _get_target_races(conn)
    logger.info("修復対象レース: %d 件", len(races))

    done = _load_checkpoint()
    todo = [(rid, dt) for rid, dt in races if rid not in done]
    logger.info("未処理: %d 件（済: %d 件）", len(todo), len(done))

    total_updated = 0
    consecutive_failures = 0

    for i, (race_id, race_date) in enumerate(todo, 1):
        result = repair_race(conn, race_id, race_date)

        if result is None:
            consecutive_failures += 1
            if consecutive_failures >= COOLDOWN_THRESHOLD:
                logger.warning(
                    "連続失敗 %d 件 → %d 秒クールダウン",
                    consecutive_failures,
                    COOLDOWN_SEC,
                )
                time.sleep(COOLDOWN_SEC)
                consecutive_failures = 0
        else:
            total_updated += result
            consecutive_failures = 0
            done.add(race_id)  # 成功時のみチェックポイントへ追加

        if i % 20 == 0:
            _save_checkpoint(done)
            logger.info("[%d/%d] 累計更新 %d 行", i, len(todo), total_updated)

        time.sleep(DELAY_SEC)

    _save_checkpoint(done)
    conn.close()

    logger.info("=== 完了 ===")
    logger.info(
        "処理レース: %d / 成功チェックポイント: %d / 更新行数: %d",
        len(todo),
        len(done),
        total_updated,
    )


if __name__ == "__main__":
    main()
