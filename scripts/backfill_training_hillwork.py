"""
坂路調教データ バックフィルスクリプト

netkeiba の調教ページから坂路データを取得し、
training_hillwork テーブルに保存する。

使用方法:
    py scripts/backfill_training_hillwork.py              # 過去30日分
    py scripts/backfill_training_hillwork.py --days 14    # 直近14日分
    py scripts/backfill_training_hillwork.py --race-id 202604010601  # 単体テスト
    py scripts/backfill_training_hillwork.py --dry-run    # 保存なしでパースのみ
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")

from src.scraper.training_scraper import fetch_training_page, save_training_records

DB_PATH = "data/umalogi.db"
REQUEST_DELAY = 2.0  # 秒 (レート制限)


def get_recent_race_ids(conn: sqlite3.Connection, days: int) -> list[str]:
    """過去 N 日のレース ID を取得する。"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date >= ? ORDER BY date DESC",
        (cutoff,),
    ).fetchall()
    return [r[0] for r in rows]


def check_already_fetched(conn: sqlite3.Connection, race_id: str) -> bool:
    """該当 race_id の馬が training_hillwork に既に存在するか確認する。"""
    # race_id の year+place+kai+day 部分 (先頭10桁) を使って horse_id との突合は難しいため、
    # training_hillwork の data_date = race の date で簡易チェック
    race_date = conn.execute(
        "SELECT date FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if not race_date:
        return False
    cnt = conn.execute(
        "SELECT COUNT(*) FROM training_hillwork WHERE data_date = ?",
        (race_date[0],),
    ).fetchone()[0]
    return cnt > 0


def run_backfill(
    db_path: str,
    days: int = 30,
    race_id_single: str = "",
    dry_run: bool = False,
) -> None:
    conn = sqlite3.connect(db_path)

    if race_id_single:
        race_ids = [race_id_single]
    else:
        race_ids = get_recent_race_ids(conn, days)

    print(f"対象レース数: {len(race_ids)} 件")

    total_hillwork = 0
    total_times = 0
    skipped = 0
    errors = 0

    for i, race_id in enumerate(race_ids, 1):
        # 既取得チェック (重複リクエスト回避)
        if not race_id_single and check_already_fetched(conn, race_id):
            skipped += 1
            continue

        print(f"[{i}/{len(race_ids)}] race_id={race_id} 取得中...", end=" ", flush=True)

        try:
            records = fetch_training_page(race_id, delay=REQUEST_DELAY)
        except Exception as exc:
            print(f"ERROR: {exc}")
            errors += 1
            continue

        if not records:
            print("0件 (データなし or パース失敗)")
            continue

        hillwork_cnt = sum(1 for r in records if r.is_hillwork)
        times_cnt = sum(1 for r in records if not r.is_hillwork)
        print(f"坂路={hillwork_cnt}件, コース={times_cnt}件", end="")

        if dry_run:
            print(" [DRY-RUN: 保存スキップ]")
        else:
            hw_saved, tc_saved = save_training_records(conn, records)
            print(f" → 保存: 坂路={hw_saved}, コース={tc_saved}")
            total_hillwork += hw_saved
            total_times += tc_saved

        # リクエスト間隔 (fetch_training_page 内の delay に追加)
        if i < len(race_ids):
            time.sleep(0.5)

    conn.close()

    print()
    print("=" * 50)
    print(f"完了: 坂路保存={total_hillwork:,}件, コース保存={total_times:,}件")
    print(f"スキップ(既取得)={skipped}件, エラー={errors}件")

    # 最終カウント確認
    conn2 = sqlite3.connect(db_path)
    hc = conn2.execute("SELECT COUNT(*) FROM training_hillwork").fetchone()[0]
    tc = conn2.execute("SELECT COUNT(*) FROM training_times").fetchone()[0]
    conn2.close()
    print(f"DB合計: training_hillwork={hc:,}件, training_times={tc:,}件")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--days", type=int, default=30, help="取得対象日数 (デフォルト30)"
    )
    parser.add_argument("--race-id", default="", help="単体テスト用 race_id")
    parser.add_argument("--dry-run", action="store_true", help="パースのみ(DB保存なし)")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    run_backfill(
        db_path=args.db,
        days=args.days,
        race_id_single=args.race_id,
        dry_run=args.dry_run,
    )
