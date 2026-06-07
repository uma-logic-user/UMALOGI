"""
血統（sire/dam/dam_sire）バックフィルバッチ

horses テーブルで sire が欠損しているが race_results に出走実績のある馬を対象に
netkeiba から父馬・母馬・母父を取得して保存する（冪等・中断再開可能）。

優先度: 直近2年の出走が多い馬から処理する（Feature Importance 2位の sire_encoded を最速で改善）。

使い方:
    py scripts/backfill_pedigree.py --limit 500   # 500頭処理
    py scripts/backfill_pedigree.py --dry-run     # 対象のみ表示
    py scripts/backfill_pedigree.py               # 全件（数時間）
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

_DB_PATH = _ROOT / "data" / "umalogi.db"
_DEFAULT_DELAY = 1.5  # netkeiba レート制限


def find_targets(conn: sqlite3.Connection, limit: int | None = None) -> list[tuple[str, str]]:
    """sire 欠損かつ出走実績あり の馬を優先度順（直近出走数降順）で返す。"""
    sql = """
        SELECT h.horse_id, h.horse_name, COUNT(rr.id) AS n_races
        FROM horses h
        JOIN race_results rr ON rr.horse_id = h.horse_id
        JOIN races r ON r.race_id = rr.race_id
        WHERE (h.sire IS NULL OR h.sire = '')
          AND r.date >= '2022-01-01'
          AND rr.rank IS NOT NULL
        GROUP BY h.horse_id, h.horse_name
        ORDER BY n_races DESC, h.horse_id
    """
    if limit:
        sql += f" LIMIT {limit}"
    return [(r[0], r[1]) for r in conn.execute(sql).fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description="血統バックフィル")
    parser.add_argument("--limit", type=int, default=None, help="処理馬数上限")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=_DEFAULT_DELAY, help="リクエスト間隔(秒)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(_DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    targets = find_targets(conn, args.limit)
    logger.info("血統バックフィル対象: %d頭", len(targets))

    if args.dry_run:
        for hid, hname in targets[:20]:
            logger.info("  %s  %s", hid, hname)
        if len(targets) > 20:
            logger.info("  ... (他 %d頭)", len(targets) - 20)
        conn.close()
        return

    from src.scraper.netkeiba import _fetch_pedigree
    from src.utils.text import ensure_clean

    saved = 0
    failed = 0
    for i, (horse_id, horse_name) in enumerate(targets, 1):
        try:
            ped = _fetch_pedigree(horse_id, delay=args.delay)
            if ped.sire or ped.dam:
                sire_clean = ensure_clean(ped.sire or "")
                dam_clean = ensure_clean(ped.dam or "")
                dam_sire_clean = ensure_clean(ped.dam_sire or "")
                conn.execute(
                    "UPDATE horses SET sire=?, dam=?, dam_sire=?, updated_at=datetime('now','localtime') "
                    "WHERE horse_id=?",
                    (sire_clean or None, dam_clean or None, dam_sire_clean or None, horse_id),
                )
                conn.commit()
                logger.info("[%d/%d] %s %s → sire=%s", i, len(targets), horse_id, horse_name, sire_clean)
                saved += 1
            else:
                logger.debug("[%d/%d] %s %s → 血統なし(スキップ)", i, len(targets), horse_id, horse_name)
                failed += 1
        except Exception as exc:
            logger.warning("[%d/%d] %s 失敗: %s", i, len(targets), horse_id, exc)
            failed += 1
            time.sleep(args.delay * 2)

        # 10頭ごとに進捗報告
        if i % 10 == 0:
            logger.info("  進捗: %d/%d 完了 (saved=%d, failed=%d)", i, len(targets), saved, failed)

    logger.info("=== 完了: saved=%d, failed=%d, total=%d ===", saved, failed, len(targets))
    conn.close()


if __name__ == "__main__":
    main()
