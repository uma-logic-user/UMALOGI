"""
スクレイピング・データ取得パイプライン

責務:
  - 金曜バッチ（JRA-VAN RACE dataspec → DB 保存）
  - 出馬表フォールバック取得（netkeiba）
  - リアルタイムオッズ取得（netkeiba API → RTD キャッシュ）
  - races テーブル仮登録
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]

logger = logging.getLogger(__name__)


def friday_batch(target_date: str | None = None) -> list[str]:
    """翌日（または指定日）の全レース出馬表を JRA-VAN から取得して DB に保存する。

    Args:
        target_date: 対象日 "YYYYMMDD"。None なら翌日。

    Returns:
        保存したレース ID のリスト
    """
    from src.scraper.jravan_client import JVDataLoader, DATASPEC_RACE
    from src.database.init_db import init_db

    if target_date is None:
        target_date = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

    logger.info("金曜バッチ開始: 対象日=%s (JRA-VAN RACE)", target_date)

    from_dt = f"{target_date}000000"
    sid = os.environ.get("JRAVAN_SID", "")

    try:
        loader = JVDataLoader(sid=sid)
        stats  = loader.load(dataspec=DATASPEC_RACE, fromtime=from_dt)
        logger.info("金曜バッチ JVLink 完了: %s", stats)
    except Exception as exc:
        logger.error("JVLink 接続失敗 (JRAVAN_SID=%r): %s", sid[:4] + "..." if sid else "", exc)
        raise

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = init_db()
    race_ids: list[str] = [
        r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (formatted,),
        ).fetchall()
    ]
    conn.close()

    logger.info("金曜バッチ完了: %d レース保存 (対象日=%s)", len(race_ids), target_date)
    return race_ids


def ensure_race_record(conn: sqlite3.Connection, race_id: str, date_str: str) -> None:
    """races テーブルにレコードがなければ仮登録する。"""
    exists = conn.execute(
        "SELECT 1 FROM races WHERE race_id=?", (race_id,)
    ).fetchone()
    if not exists:
        try:
            race_num = int(race_id[-2:])
        except ValueError:
            race_num = 0
        formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        with conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO races
                    (race_id, race_name, date, venue, race_number,
                     distance, surface, weather, condition)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (race_id, f"レース{race_num}", formatted, "未定",
                 race_num, 0, "未定", "", ""),
            )


def save_entries_to_db(conn: sqlite3.Connection, tbl: object) -> int:
    """EntryTable を entries テーブルに保存して保存件数を返す。

    horse_id は JVLink と netkeiba で形式が異なるため NULL で保存し、
    後続の JVLink 同期で上書きされることを想定する。
    """
    saved = 0
    for h in tbl.entries:  # type: ignore[attr-defined]
        try:
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO entries
                        (race_id, horse_number, gate_number, horse_id,
                         horse_name, sex_age, weight_carried,
                         jockey, trainer, horse_weight, horse_weight_diff)
                    VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tbl.race_id,           # type: ignore[attr-defined]
                        h.horse_number,
                        h.gate_number,
                        h.horse_name,
                        h.sex_age,
                        h.weight_carried,
                        h.jockey,
                        h.trainer,
                        h.horse_weight,
                        h.horse_weight_diff,
                    ),
                )
            saved += 1
        except Exception as exc:
            logger.warning(
                "entries 保存失敗 %s #%d: %s",
                tbl.race_id, h.horse_number, exc,  # type: ignore[attr-defined]
            )
    return saved


def fetch_and_save_odds(conn: sqlite3.Connection, race_id: str) -> int:
    """realtime_odds が空のとき、netkeiba API → RTD の順でオッズを取得して DB に保存する。

    フォールバック戦略（2段階）:
      1. netkeiba オッズ API — 最新オッズ（推奨）
      2. JRA-VAN ローカル RTD キャッシュ — API 失敗時の予備

    Returns:
        保存した頭数（0 なら全段失敗）
    """
    from src.database.init_db import insert_realtime_odds

    name_map: dict[int, str] = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT horse_number, horse_name FROM entries WHERE race_id=?", (race_id,)
        ).fetchall()
    }

    # Stage 1: netkeiba オッズ API
    try:
        from src.scraper.entry_table import fetch_realtime_odds
        odds_list = fetch_realtime_odds(race_id, delay=1.0)
        if odds_list and any(o.win_odds for o in odds_list):
            n = insert_realtime_odds(conn, race_id, odds_list, name_map)
            logger.info("オッズ取得 [netkeiba API]: %d 頭保存 (race_id=%s)", n, race_id)
            return n
        logger.warning(
            "netkeiba API: オッズ取得なし or 全 NaN (race_id=%s) → RTD にフォールバック",
            race_id,
        )
    except Exception as exc:
        logger.warning(
            "netkeiba オッズ API 失敗 (race_id=%s): %s — RTD にフォールバック",
            race_id, exc,
        )

    # Stage 2: JRA-VAN ローカル RTD キャッシュ
    try:
        from src.scraper.rtd_reader import read_rtd_for_race, rtd_odds_to_horse_odds
        rtd_info = read_rtd_for_race(race_id)
        if rtd_info and rtd_info.odds:
            odds_list = rtd_odds_to_horse_odds(rtd_info)
            if odds_list and any(o.win_odds for o in odds_list):
                n = insert_realtime_odds(conn, race_id, odds_list, name_map)
                logger.info("オッズ取得 [RTD キャッシュ]: %d 頭保存 (race_id=%s)", n, race_id)
                return n
        logger.warning("RTD: オッズ取得なし or ファイル未存在 (race_id=%s)", race_id)
    except Exception as exc:
        logger.warning("RTD 読み込み失敗 (race_id=%s): %s", race_id, exc)

    logger.error(
        "🚨 オッズ全段取得失敗 (race_id=%s) — netkeiba API / RTD 両方が使用不可です。",
        race_id,
    )
    return 0
