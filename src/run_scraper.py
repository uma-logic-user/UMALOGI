"""
実データ取得スクリプト

対象: 2025年 有馬記念（レースID: 202506050811）
処理: netkeiba からレース結果・血統情報を取得 → umalogi.db へ保存 → 件数表示
"""

import logging
import sqlite3
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加（直接実行時用）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.init_db import get_db_path, init_db, insert_race
from src.scraper.netkeiba import RaceInfo, fetch_race_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# 対象レース
# -----------------------------------------------------------------------
TARGET_RACE_ID = "202506050811"  # 2025年 有馬記念（中山11R）


# -----------------------------------------------------------------------
# 件数表示
# -----------------------------------------------------------------------
def print_table_counts(conn: sqlite3.Connection) -> None:
    """races / horses / race_results の件数をターミナルに表示する。"""
    tables = ["races", "horses", "race_results"]
    print("\n" + "=" * 50)
    print("  DB 件数サマリー")
    print("=" * 50)
    for table in tables:
        count: int = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<20}: {count:>5} 件")
    print("=" * 50 + "\n")


def print_race_summary(race: RaceInfo) -> None:
    """取得したレース結果をターミナルに表示する。"""
    print("\n" + "=" * 60)
    print(f"  {race.race_name}  ({race.date}  {race.venue})")
    print(f"  {race.surface}{race.distance}m  天候:{race.weather}  馬場:{race.condition}")
    print("=" * 60)
    print(f"  {'着順':>4}  {'馬名':<16}  {'父':<16}  {'タイム':>8}  {'単勝':>6}")
    print("-" * 60)
    for r in race.results:
        rank_str = str(r.rank) if r.rank else "---"
        sire = r.pedigree.sire or "-"
        time_str = r.finish_time or "---"
        odds_str = f"{r.win_odds:.1f}" if r.win_odds else "---"
        print(
            f"  {rank_str:>4}  {r.horse_name:<16}  {sire:<16}  {time_str:>8}  {odds_str:>6}"
        )
    print("=" * 60 + "\n")


# -----------------------------------------------------------------------
# メイン処理
# -----------------------------------------------------------------------
def main() -> None:
    db_path = get_db_path()
    logger.info("DB パス: %s", db_path)

    # --- DB 初期化 ---
    conn = init_db(db_path)

    try:
        # --- レース結果取得 ---
        logger.info("レース取得開始: race_id=%s", TARGET_RACE_ID)
        race: RaceInfo = fetch_race_results(
            TARGET_RACE_ID,
            fetch_pedigree=True,   # 血統情報も取得
            delay=1.5,             # サーバー負荷軽減のため各リクエストに 1.5 秒待機
            max_retries=3,
        )

        # --- 結果表示 ---
        print_race_summary(race)

        # --- DB 保存 ---
        logger.info("DB 保存開始: %d 頭分", len(race.results))
        insert_race(conn, race)
        logger.info("DB 保存完了")

        # --- 件数表示 ---
        print_table_counts(conn)

    except Exception as exc:
        logger.error("エラーが発生しました: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
