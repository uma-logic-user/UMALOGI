"""
文字化けentries再取得スクリプト
netkeiba から正常なエントリーデータを取得してDBに保存する
"""

import sys
import time
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.init_db import init_db
from src.scraper.entry_table import fetch_entry_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def refetch_entries(dates: list[str]) -> None:
    conn = init_db()

    # 対象race_idを取得
    date_ph = ",".join(["?"] * len(dates))
    race_ids: list[str] = [
        r[0]
        for r in conn.execute(
            f"SELECT race_id FROM races WHERE date IN ({date_ph}) ORDER BY race_id",
            dates,
        ).fetchall()
    ]
    logger.info("対象レース数: %d", len(race_ids))

    ok_count = 0
    skip_count = 0
    error_count = 0

    for i, race_id in enumerate(race_ids):
        # 既存entries確認
        existing = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE race_id=?", (race_id,)
        ).fetchone()[0]
        if existing > 0:
            logger.info(
                "[%d/%d] %s: skip (既存%d件)", i + 1, len(race_ids), race_id, existing
            )
            skip_count += 1
            continue

        try:
            logger.info("[%d/%d] %s: netkeiba取得中...", i + 1, len(race_ids), race_id)
            et = fetch_entry_table(race_id, delay=1.5)

            if et is None or not et.entries:
                logger.warning("  → エントリーなし (skip)")
                skip_count += 1
                continue

            # DBに保存
            for entry in et.entries:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO entries
                        (race_id, horse_number, gate_number, horse_id, horse_name,
                         sex_age, weight_carried, jockey, trainer,
                         horse_weight, horse_weight_diff)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        race_id,
                        entry.horse_number,
                        entry.gate_number,
                        entry.horse_id or "",
                        entry.horse_name or "",
                        getattr(entry, "sex_age", "") or "",
                        getattr(entry, "weight_carried", None),
                        getattr(entry, "jockey", "") or "",
                        getattr(entry, "trainer", "") or "",
                        getattr(entry, "horse_weight", None),
                        getattr(entry, "horse_weight_diff", None),
                    ),
                )
            conn.commit()
            logger.info("  → %d頭保存完了", len(et.entries))
            ok_count += 1

        except Exception as e:
            logger.error("  → エラー: %s", e)
            error_count += 1

        # レート制限対策
        if (i + 1) % 10 == 0:
            logger.info("--- 10件完了 / クールダウン5秒 ---")
            time.sleep(5)
        else:
            time.sleep(1.5)

    logger.info(
        "=== 完了: 成功=%d, スキップ=%d, エラー=%d ===",
        ok_count,
        skip_count,
        error_count,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="netkeiba からエントリー再取得")
    parser.add_argument(
        "--date",
        nargs="+",
        metavar="YYYYMMDD_OR_YYYY-MM-DD",
        help="対象日 (複数指定可). 省略時は2026-05-16, 2026-05-17",
    )
    args = parser.parse_args()

    if args.date:
        # YYYYMMDD → YYYY-MM-DD 正規化
        def _normalize(d: str) -> str:
            d = d.strip()
            if len(d) == 8 and d.isdigit():
                return f"{d[:4]}-{d[4:6]}-{d[6:]}"
            return d

        target_dates = [_normalize(d) for d in args.date]
    else:
        target_dates = ["2026-05-16", "2026-05-17"]

    refetch_entries(target_dates)
