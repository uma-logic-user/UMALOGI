# -*- coding: utf-8 -*-
"""
レース結果速報取得スクリプト

発走後15分で today_auto_runner から呼び出す。
netkeiba からレース結果・払戻を取得して DB に保存し、
予想評価（prediction_results）を更新してダッシュボード JSON を再生成する。

Usage:
    python scripts/fetch_race_result.py --race-id 202603010501
    python scripts/fetch_race_result.py --date 20260425   # 指定日の全未取得レース
    python scripts/fetch_race_result.py --date 20260425 --all   # 既取得も上書き
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)
        ),
    ],
)
logger = logging.getLogger("fetch_result")


def _get_target_race_ids(conn, date_iso: str, force_all: bool) -> list[str]:
    """取得対象の race_id リストを返す。"""
    if force_all:
        rows = conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (date_iso,),
        ).fetchall()
    else:
        # race_results が存在しないレースのみ
        rows = conn.execute(
            """
            SELECT r.race_id FROM races r
            WHERE r.date = ?
              AND NOT EXISTS (
                  SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id
              )
            ORDER BY r.race_id
            """,
            (date_iso,),
        ).fetchall()
    return [r[0] for r in rows]


def fetch_single_race(race_id: str, delay: float = 1.5) -> bool:
    """
    指定レースの結果を netkeiba から取得し、DB に保存して評価する。

    Returns:
        True = 結果あり保存成功 / False = まだ結果なし or エラー
    """
    from src.ops.data_sync import _get_conn
    from src.scraper.netkeiba import fetch_race_results, fetch_race_payouts
    from src.database.init_db import insert_race_payouts
    from src.evaluation.evaluator import Evaluator
    import sqlite3

    conn = _get_conn()

    # ── 結果取得 ──────────────────────────────────────────────────
    try:
        race_info = fetch_race_results(race_id, fetch_pedigree=False, delay=delay)
    except Exception as exc:
        logger.warning("結果ページ取得失敗 race_id=%s: %s", race_id, exc)
        conn.close()
        return False

    if not race_info.results:
        logger.info("結果なし (未発走か取消): race_id=%s", race_id)
        conn.close()
        return False

    if not any(h.rank == 1 for h in race_info.results):
        logger.info("1着馬なし (レース未確定?): race_id=%s", race_id)
        conn.close()
        return False

    # ── races テーブルを正しい日本語で更新 ──────────────────────────
    # JVLink 由来の賞金コード ("20000" 等) を netkeiba の正しいレース名で上書き
    _valid_name = race_info.race_name.replace('\x00', '').strip()
    if _valid_name and not _valid_name.isdigit():
        with conn:
            conn.execute(
                """
                UPDATE races SET
                    race_name       = ?,
                    surface         = CASE WHEN ? != '' THEN ? ELSE surface END,
                    distance        = CASE WHEN ? > 0    THEN ? ELSE distance END,
                    weather         = CASE WHEN ? != '' THEN ? ELSE weather END,
                    condition       = CASE WHEN ? != '' THEN ? ELSE condition END,
                    track_direction = CASE WHEN ? != '' THEN ? ELSE track_direction END
                WHERE race_id = ?
                """,
                (
                    _valid_name,
                    race_info.surface,  race_info.surface,
                    race_info.distance, race_info.distance,
                    race_info.weather,  race_info.weather,
                    race_info.condition, race_info.condition,
                    getattr(race_info, "track_direction", ""),
                    getattr(race_info, "track_direction", ""),
                    race_id,
                ),
            )
        logger.info("races 更新: race_id=%s name=%s surf=%s dist=%d",
                    race_id, _valid_name, race_info.surface, race_info.distance)

    # ── race_results 保存 ─────────────────────────────────────────
    # horse_id は JRA-VAN フォーマットと異なるため NULL で保存（FK 違反回避）
    saved_count = 0
    for h in race_info.results:
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO race_results
                        (race_id, horse_name, rank,
                         gate_number, horse_number,
                         sex_age, weight_carried, jockey, trainer,
                         finish_time, margin, popularity, win_odds,
                         horse_weight, horse_weight_diff)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(race_id, horse_name) DO UPDATE SET
                        rank              = COALESCE(excluded.rank,           race_results.rank),
                        finish_time       = COALESCE(excluded.finish_time,    race_results.finish_time),
                        margin            = COALESCE(excluded.margin,         race_results.margin),
                        popularity        = COALESCE(excluded.popularity,     race_results.popularity),
                        win_odds          = COALESCE(excluded.win_odds,       race_results.win_odds),
                        horse_weight      = COALESCE(excluded.horse_weight,   race_results.horse_weight),
                        horse_weight_diff = COALESCE(excluded.horse_weight_diff, race_results.horse_weight_diff)
                    """,
                    (
                        race_id, h.horse_name, h.rank,
                        h.gate_number, h.horse_number,
                        h.sex_age, h.weight_carried, h.jockey, h.trainer,
                        h.finish_time, h.margin, h.popularity, h.win_odds,
                        h.horse_weight, h.horse_weight_diff,
                    ),
                )
                saved_count += 1
        except sqlite3.IntegrityError as e:
            logger.warning("race_results 保存失敗 %s %s: %s", race_id, h.horse_name, e)

    logger.info("race_results 保存: race_id=%s (%d/%d頭)", race_id, saved_count, len(race_info.results))

    # ── race_payouts 取得・保存 ───────────────────────────────────
    try:
        time.sleep(delay)
        payouts = fetch_race_payouts(race_id, delay=delay)
        if payouts:
            insert_race_payouts(conn, race_id, payouts)
            logger.info("race_payouts 保存: race_id=%s (%d件)", race_id, len(payouts))
        else:
            logger.warning("払戻なし: race_id=%s (確定前?)", race_id)
    except Exception as pe:
        logger.warning("払戻取得失敗 race_id=%s: %s", race_id, pe)

    # ── 予想評価 ──────────────────────────────────────────────────
    try:
        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, race_id)
        hits = result.hit_count
        roi  = result.roi
        logger.info(
            "評価完了: race_id=%s  的中=%d件  投資¥%.0f  払戻¥%.0f  ROI=%.1f%%",
            race_id, hits, result.total_invested, result.total_payout, roi,
        )
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True


def fetch_for_date(date_str: str, force_all: bool = False, delay: float = 1.5) -> int:
    """
    指定日の全(未取得)レースの結果を取得する。

    Args:
        date_str: YYYYMMDD 形式
        force_all: True なら既存データも上書き
        delay:     リクエスト間隔(秒)

    Returns:
        取得成功レース数
    """
    from src.ops.data_sync import _get_conn
    date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    conn = _get_conn()
    race_ids = _get_target_race_ids(conn, date_iso, force_all)
    conn.close()

    if not race_ids:
        logger.info("取得対象レースなし (date=%s, all=%s)", date_iso, force_all)
        return 0

    logger.info("取得対象: %d レース (date=%s)", len(race_ids), date_iso)
    saved = 0
    for race_id in race_ids:
        ok = fetch_single_race(race_id, delay=delay)
        if ok:
            saved += 1
        time.sleep(0.5)   # レース間インターバル

    return saved


def _run_generate_data() -> None:
    """web/generate_data.py を実行してダッシュボード JSON を再生成する。"""
    cmd = [sys.executable, str(_ROOT / "web" / "generate_data.py")]
    try:
        result = subprocess.run(cmd, cwd=str(_ROOT), timeout=120)
        if result.returncode == 0:
            logger.info("ダッシュボード更新完了")
        else:
            logger.warning("generate_data.py 失敗 (rc=%d)", result.returncode)
    except Exception as e:
        logger.warning("generate_data.py 実行エラー: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="レース結果速報取得・評価・ダッシュボード更新")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--race-id",  help="対象レース ID (例: 202603010501)")
    group.add_argument("--date",     help="対象日 YYYYMMDD (全未取得レース)")
    parser.add_argument("--all",     action="store_true",
                        help="--date 指定時: 既存データも上書き取得")
    parser.add_argument("--delay",   type=float, default=1.5,
                        help="リクエスト間隔秒 (デフォルト 1.5)")
    parser.add_argument("--no-dashboard", action="store_true",
                        help="generate_data.py を実行しない")
    args = parser.parse_args()

    if args.race_id:
        ok = fetch_single_race(args.race_id, delay=args.delay)
        if ok and not args.no_dashboard:
            _run_generate_data()
        sys.exit(0 if ok else 1)

    else:  # --date
        saved = fetch_for_date(args.date, force_all=args.all, delay=args.delay)
        logger.info("完了: %d レース取得", saved)
        if saved > 0 and not args.no_dashboard:
            _run_generate_data()


if __name__ == "__main__":
    main()
