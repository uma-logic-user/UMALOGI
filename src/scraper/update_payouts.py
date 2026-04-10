"""
払戻データ一括取得バッチ

race_payouts テーブルにデータがない過去レースの払戻を
netkeiba から取得して保存する。

使用例:
  python -m src.scraper.update_payouts               # 未取得レースを全取得
  python -m src.scraper.update_payouts --year 2024   # 2024年分のみ
  python -m src.scraper.update_payouts --limit 50    # 最大50レース
  python -m src.scraper.update_payouts --dry-run     # DB 書き込みなし（確認用）
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db, insert_race_payouts
from src.scraper.netkeiba import fetch_race_payouts


def _get_races_without_payouts(conn, year: int | None) -> list[str]:
    """
    race_results は存在するが race_payouts が未取得のレース ID を返す。
    """
    year_filter = "AND substr(r.date,1,4) = ?" if year else ""
    params = [str(year)] if year else []

    rows = conn.execute(
        f"""
        SELECT DISTINCT r.race_id
        FROM races r
        JOIN race_results rr ON r.race_id = rr.race_id
        WHERE NOT EXISTS (
            SELECT 1 FROM race_payouts rp WHERE rp.race_id = r.race_id
        )
        {year_filter}
        ORDER BY r.date, r.race_id
        """,
        params,
    ).fetchall()
    return [r[0] for r in rows]


def update_payouts(
    *,
    year: int | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    delay: float = 2.0,
) -> dict[str, int]:
    """
    未取得レースの払戻を一括取得して保存する。

    Returns:
        {"total": 対象レース数, "saved": 保存数, "empty": 払戻なし数, "errors": エラー数}
    """
    conn = init_db()
    race_ids = _get_races_without_payouts(conn, year)

    if limit:
        race_ids = race_ids[:limit]

    stats = {"total": len(race_ids), "saved": 0, "empty": 0, "errors": 0}
    logger.info("払戻未取得レース: %d 件 (year=%s)", len(race_ids), year or "all")

    bar = tqdm(race_ids, desc="払戻取得", unit="race", dynamic_ncols=True)

    for race_id in bar:
        try:
            payouts = fetch_race_payouts(race_id, delay=delay)
            if not payouts:
                stats["empty"] += 1
            else:
                if not dry_run:
                    insert_race_payouts(conn, race_id, payouts)
                stats["saved"] += 1
        except Exception as exc:
            logger.warning("払戻取得失敗 race_id=%s: %s", race_id, exc)
            stats["errors"] += 1

        bar.set_postfix(
            saved=stats["saved"],
            empty=stats["empty"],
            err=stats["errors"],
            refresh=False,
        )

    conn.close()
    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="払戻データ一括取得バッチ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.scraper.update_payouts               # 全未取得レースを処理
  python -m src.scraper.update_payouts --year 2024   # 2024年分のみ
  python -m src.scraper.update_payouts --limit 100   # 最大100レース
  python -m src.scraper.update_payouts --dry-run     # 書き込みなし確認
""",
    )
    parser.add_argument("--year",    type=int, help="対象年（省略時=全期間）")
    parser.add_argument("--limit",   type=int, help="最大処理レース数")
    parser.add_argument("--delay",   type=float, default=2.0, help="リクエスト間隔（秒）")
    parser.add_argument("--dry-run", action="store_true", help="DB 書き込みなし")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("src.scraper.netkeiba").setLevel(logging.WARNING)

    args = _parse_args()
    stats = update_payouts(
        year=args.year,
        limit=args.limit,
        dry_run=args.dry_run,
        delay=args.delay,
    )

    mode = "[DRY-RUN]" if args.dry_run else ""
    print(f"\n{'='*50} {mode}")
    print(f"  払戻取得結果 (year={args.year or 'all'})")
    print(f"{'='*50}")
    print(f"  対象レース : {stats['total']:5d}")
    print(f"  保存成功  : {stats['saved']:5d}")
    print(f"  払戻なし  : {stats['empty']:5d}")
    print(f"  エラー    : {stats['errors']:5d}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
