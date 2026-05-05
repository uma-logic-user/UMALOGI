"""
scripts/repair_race_data.py — netkeiba から欠損レースデータを補完して umalogi.db を修復する

ユーザー許可事項:
  CLAUDE.md の netkeiba 禁止規則について、以下の用途のみ例外として明示的に許可:
  「JRA-VAN 仕様で補完できない馬場状態・距離・着順・タイム・斤量・馬体重の
  本番 DB（umalogi.db）補完のための netkeiba スクレイピング」

対象:
  - races.distance = 0 または races.surface = '' の全レース
  - 合わせて race_results の rank / finish_time / horse_weight も更新

更新テーブル:
  races:        distance, surface, condition, weather, track_direction
  race_results: rank, finish_time, margin, horse_weight, horse_weight_diff,
                win_odds, popularity （horse_number で突合）

Usage:
    py -3 scripts/repair_race_data.py --dry-run          # 対象確認のみ
    py -3 scripts/repair_race_data.py --limit 20         # 最大 20 レース
    py -3 scripts/repair_race_data.py --date 2026-05-03  # 特定日のみ
    py -3 scripts/repair_race_data.py --all              # 全欠損レース
    py -3 scripts/repair_race_data.py --all --skip-results  # races のみ更新
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

import requests

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.database.init_db import init_db
from src.scraper.netkeiba import fetch_race_results, RaceInfo, HorseResult


# ── 定数 ──────────────────────────────────────────────────────────────
_REQUEST_DELAY = 2.0   # 各レース間のウェイト（秒）
_MAX_RETRIES   = 2


# ── DB ヘルパー ──────────────────────────────────────────────────────

def _get_missing_races(
    conn: sqlite3.Connection,
    target_date: str | None = None,
    limit: int | None = None,
) -> list[tuple[str, str, str, int]]:
    """
    distance=0 または surface='' のレースを (race_id, date, venue, race_number) で返す。
    """
    where_clauses = ["(r.distance = 0 OR r.distance IS NULL OR r.surface = '' OR r.surface IS NULL)"]
    params: list = []

    if target_date:
        where_clauses.append("r.date = ?")
        params.append(target_date)

    where = " AND ".join(where_clauses)
    sql = f"""
        SELECT r.race_id, r.date, r.venue, r.race_number
        FROM races r
        WHERE {where}
        ORDER BY r.date DESC, r.race_number
    """
    if limit:
        sql += f" LIMIT {limit}"

    return conn.execute(sql, params).fetchall()


def _update_race(
    conn: sqlite3.Connection,
    race_id: str,
    info: RaceInfo,
) -> None:
    """races テーブルを更新する。"""
    conn.execute(
        """
        UPDATE races
        SET distance        = ?,
            surface         = ?,
            condition       = ?,
            weather         = ?,
            track_direction = ?
        WHERE race_id = ?
        """,
        (
            info.distance,
            info.surface,
            info.condition,
            info.weather,
            info.track_direction,
            race_id,
        ),
    )


def _update_race_results(
    conn: sqlite3.Connection,
    race_id: str,
    results: list[HorseResult],
) -> int:
    """
    race_results を horse_number で突合して更新する。

    Returns:
        更新できた行数
    """
    # 既存の horse_number 一覧を取得（race_id 内で unique であるはず）
    existing = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT horse_number, id FROM race_results WHERE race_id = ?",
            (race_id,),
        ).fetchall()
    }

    updated = 0
    for h in results:
        if h.horse_number is None:
            continue
        row_id = existing.get(h.horse_number)
        if row_id is None:
            continue

        conn.execute(
            """
            UPDATE race_results
            SET rank              = COALESCE(?, rank),
                finish_time       = COALESCE(?, finish_time),
                margin            = COALESCE(?, margin),
                win_odds          = COALESCE(?, win_odds),
                popularity        = COALESCE(?, popularity),
                horse_weight      = COALESCE(?, horse_weight),
                horse_weight_diff = COALESCE(?, horse_weight_diff)
            WHERE id = ?
            """,
            (
                h.rank,
                h.finish_time,
                h.margin,
                h.win_odds,
                h.popularity,
                h.horse_weight,
                h.horse_weight_diff,
                row_id,
            ),
        )
        updated += 1

    return updated


# ── メイン処理 ────────────────────────────────────────────────────────

def repair(
    *,
    target_date: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    skip_results: bool = False,
    verbose: bool = False,
) -> dict:
    """
    欠損レースデータを netkeiba から取得して DB を修復する。

    Returns:
        {"total": N, "success": M, "failed": K}
    """
    conn = init_db()
    session = requests.Session()

    missing = _get_missing_races(conn, target_date=target_date, limit=limit)

    print(f"[REPAIR] 対象レース数: {len(missing)}")
    if dry_run:
        print("[REPAIR] --dry-run: 実際の更新は行いません")
        for race_id, date, venue, rno in missing[:20]:
            print(f"  {date} {venue} {rno}R  race_id={race_id}")
        if len(missing) > 20:
            print(f"  ... 他 {len(missing)-20} 件")
        conn.close()
        return {"total": len(missing), "success": 0, "failed": 0}

    success = 0
    failed  = 0

    for i, (race_id, date, venue, rno) in enumerate(missing):
        print(f"[{i+1}/{len(missing)}] {date} {venue} {rno}R  race_id={race_id}", end=" ... ", flush=True)

        try:
            info = fetch_race_results(
                race_id,
                race_date=date,
                fetch_pedigree=False,  # 血統は別途取得
                delay=_REQUEST_DELAY,
                max_retries=_MAX_RETRIES,
                session=session,
            )
        except Exception as e:
            print(f"ERROR: {e}")
            failed += 1
            time.sleep(_REQUEST_DELAY)
            continue

        if dry_run:
            print(f"距離={info.distance}m 馬場={info.surface} 状態={info.condition}")
            continue

        try:
            _update_race(conn, race_id, info)

            n_updated = 0
            if not skip_results and info.results:
                n_updated = _update_race_results(conn, race_id, info.results)

            conn.commit()
            success += 1
            print(f"OK 距離={info.distance}m 馬場={info.surface} 状態={info.condition} 出走馬更新={n_updated}頭")

        except Exception as e:
            conn.rollback()
            print(f"DB ERROR: {e}")
            failed += 1
            continue

    conn.close()

    print()
    print(f"[REPAIR 完了] 総数={len(missing)} / 成功={success} / 失敗={failed}")
    return {"total": len(missing), "success": success, "failed": failed}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="netkeiba から欠損レースデータを補完する")
    p.add_argument("--dry-run",       action="store_true", help="対象確認のみ（DB 更新なし）")
    p.add_argument("--all",           action="store_true", help="全欠損レースを処理")
    p.add_argument("--date",          default=None,        help="特定日 YYYY-MM-DD")
    p.add_argument("--limit",         type=int, default=50, help="最大処理レース数（デフォルト 50）")
    p.add_argument("--skip-results",  action="store_true", help="races のみ更新（race_results はスキップ）")
    p.add_argument("--verbose",  "-v", action="store_true", help="詳細ログ")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    limit = None if args.all else args.limit

    repair(
        target_date=args.date,
        limit=limit,
        dry_run=args.dry_run,
        skip_results=args.skip_results,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
