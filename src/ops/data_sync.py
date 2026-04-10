"""
JRA-VAN データ自動同期スクリプト

スケジュール:
  金曜 20:00  : RACE (翌週末分出馬表)
  土日 8:00   : WOOD (当日調教タイム)
  土日 レース後: RACE (確定成績 / 払戻)
  月曜 6:00   : DIFN (騎手・調教師・競走馬マスタ更新)
  月1回       : SETUP (全マスタ初期化)

Usage:
    python -m src.ops.data_sync friday
    python -m src.ops.data_sync race_results
    python -m src.ops.data_sync wood
    python -m src.ops.data_sync masters
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _get_conn() -> sqlite3.Connection:
    from src.database.init_db import init_db
    return init_db()


def sync_friday(target_date: str | None = None) -> int:
    """
    金曜夜バッチ: 翌週末の出馬表を取得して DB に保存する。

    Returns:
        保存したレース数
    """
    from src.main_pipeline import friday_batch
    conn = _get_conn()
    try:
        race_ids = friday_batch(target_date)
        logger.info("金曜バッチ完了: %d レース保存", len(race_ids))
        return len(race_ids)
    finally:
        conn.close()


def sync_race_results(from_date: str | None = None) -> int:
    """
    JRA-VAN RACE dataspec からレース結果・払戻を取得して保存する。

    Returns:
        保存したレース数
    """
    try:
        from src.scraper.jravan_client import (
            JVLinkClient,
            JVDataLoader,
            DATASPEC_RACE,
        )
    except ImportError as e:
        logger.error("JV-Link インポート失敗（32bit Python 環境が必要）: %s", e)
        return 0

    conn = _get_conn()
    try:
        from_dt = from_date or (datetime.now() - timedelta(days=2)).strftime("%Y%m%d000000")
        with JVLinkClient() as client:
            loader = JVDataLoader(client)
            stats = loader.load(
                dataspec=DATASPEC_RACE,
                from_date=from_dt,
                conn=conn,
            )
        logger.info("RACE 同期完了: %s", stats)
        return stats.get("saved", 0)
    finally:
        conn.close()


def sync_wood() -> int:
    """
    JRA-VAN WOOD dataspec から調教タイムを取得して保存する。
    """
    try:
        from src.scraper.jravan_client import (
            JVLinkClient,
            JVDataLoader,
            DATASPEC_WOOD,
        )
    except ImportError as e:
        logger.error("JV-Link インポート失敗: %s", e)
        return 0

    conn = _get_conn()
    try:
        from_dt = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d000000")
        with JVLinkClient() as client:
            loader = JVDataLoader(client)
            stats = loader.load(
                dataspec=DATASPEC_WOOD,
                from_date=from_dt,
                conn=conn,
            )
        logger.info("WOOD 同期完了: %s", stats)
        return stats.get("saved", 0)
    finally:
        conn.close()


def sync_masters(full: bool = False) -> int:
    """
    マスタデータを同期する。

    Args:
        full: True の場合 SETUP (全件)、False の場合 DIFN + BLOD (差分)
    """
    try:
        from src.scraper.jravan_client import (
            JVLinkClient,
            JVDataLoader,
            DATASPEC_DIFN,
            DATASPEC_BLOD,
            DATASPEC_SETUP,
        )
    except ImportError as e:
        logger.error("JV-Link インポート失敗: %s", e)
        return 0

    conn = _get_conn()
    total = 0
    try:
        with JVLinkClient() as client:
            loader = JVDataLoader(client)
            if full:
                stats = loader.load(dataspec=DATASPEC_SETUP, from_date="", conn=conn)
                total += stats.get("saved", 0)
                logger.info("SETUP 完了: %s", stats)
            else:
                from_dt = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d000000")
                for spec in (DATASPEC_DIFN, DATASPEC_BLOD):
                    stats = loader.load(dataspec=spec, from_date=from_dt, conn=conn)
                    total += stats.get("saved", 0)
                    logger.info("%s 完了: %s", spec, stats)
    finally:
        conn.close()
    return total


def sync_payouts_from_netkeiba(target_date: str | None = None) -> int:
    """
    netkeiba から確定払戻を取得して DB に補完する（JRA-VAN が遅延している場合の代替）。
    """
    conn = _get_conn()
    try:
        if target_date is None:
            target_date = date.today().strftime("%Y%m%d")
        from src.scraper.update_payouts import update_payouts_for_date
        count = update_payouts_for_date(conn, target_date)
        logger.info("netkeiba 払戻補完: %d 件", count)
        return count
    except Exception as e:
        logger.error("払戻補完失敗: %s", e)
        return 0
    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="JRA-VAN データ同期")
    parser.add_argument(
        "command",
        choices=["friday", "race_results", "wood", "masters", "masters_full", "payouts"],
        help="実行するデータ同期コマンド",
    )
    parser.add_argument("--date", help="対象日 YYYYMMDD", default=None)
    args = parser.parse_args()

    if args.command == "friday":
        sync_friday(args.date)
    elif args.command == "race_results":
        sync_race_results(args.date)
    elif args.command == "wood":
        sync_wood()
    elif args.command == "masters":
        sync_masters(full=False)
    elif args.command == "masters_full":
        sync_masters(full=True)
    elif args.command == "payouts":
        sync_payouts_from_netkeiba(args.date)


if __name__ == "__main__":
    main()
