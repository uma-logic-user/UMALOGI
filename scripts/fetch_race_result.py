# -*- coding: utf-8 -*-
"""
レース結果速報取得スクリプト

発走後15分で today_auto_runner から呼び出す。
JRA-VAN (JVLink) 経由でレース結果・払戻を取得して DB に保存し、
予想評価（prediction_results）を更新してダッシュボード JSON を再生成する。

【注意】netkeiba.com へのアクセスは CLAUDE.md により永久禁止。
全データは JVLink 経由で取得する。

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


def _run_jvlink_race_sync(race_date: str) -> bool:
    """
    JVLink RACE TODAY 同期を実行して当日の SE/HR レコードを DB に取り込む。

    Args:
        race_date: YYYYMMDD 形式の日付

    Returns:
        True = 成功 / False = 失敗
    """
    logger.info("JVLink RACE TODAY 同期開始: date=%s", race_date)
    try:
        proc = subprocess.run(
            ["py", "-3.14-32",
             str(_ROOT / "scripts" / "_jvlink_force_worker.py"),
             "--dataspec", "RACE",
             "--fromtime", race_date,
             "--option", "3"],  # OPT_TODAY: 当日データ
            cwd=str(_ROOT),
            timeout=180,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        last_line = proc.stdout.splitlines()[-1] if proc.stdout else ""
        if proc.returncode != 0:
            logger.warning("JVLink ワーカー rc=%d stderr=%s", proc.returncode, proc.stderr[:300])
            return False
        logger.info("JVLink 同期完了: %s", last_line)
        return True
    except subprocess.TimeoutExpired:
        logger.error("JVLink ワーカー タイムアウト (180s): date=%s", race_date)
        return False
    except Exception as exc:
        logger.error("JVLink ワーカー 実行失敗: %s", exc)
        return False


def _get_target_race_ids(conn, date_iso: str, force_all: bool) -> list[str]:
    """取得対象の race_id リストを返す。"""
    if force_all:
        rows = conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (date_iso,),
        ).fetchall()
    else:
        # rank が存在しないレースのみ
        rows = conn.execute(
            """
            SELECT r.race_id FROM races r
            WHERE r.date = ?
              AND NOT EXISTS (
                  SELECT 1 FROM race_results rr
                  WHERE rr.race_id = r.race_id AND rr.rank IS NOT NULL AND rr.rank > 0
              )
            ORDER BY r.race_id
            """,
            (date_iso,),
        ).fetchall()
    return [r[0] for r in rows]


def fetch_single_race(race_id: str, delay: float = 1.5) -> bool:
    """
    指定レースの結果を JVLink から取得し DB に保存して評価する。

    Args:
        race_id: 12桁のレースID
        delay:   使用しない（後方互換のため残す）

    Returns:
        True = 結果あり保存成功 / False = まだ結果なし or エラー
    """
    from src.database.init_db import init_db
    from src.evaluation.evaluator import Evaluator

    race_date = f"{race_id[0:4]}{race_id[4:6]}{race_id[6:8]}"  # YYYYMMDD

    # JVLink RACE TODAY 同期（当日全レースを取込む）
    ok = _run_jvlink_race_sync(race_date)
    if not ok:
        logger.warning("JVLink 同期失敗 — 結果を確認できません: race_id=%s", race_id)
        return False

    conn = init_db()

    # 着順確認
    with_rank = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
        (race_id,),
    ).fetchone()[0]
    if with_rank == 0:
        logger.info("結果なし (未発走か取消): race_id=%s", race_id)
        conn.close()
        return False

    rank1 = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank = 1",
        (race_id,),
    ).fetchone()[0]
    if rank1 == 0:
        logger.info("1着馬なし (レース未確定?): race_id=%s", race_id)
        conn.close()
        return False

    logger.info("race_results 確認: race_id=%s (%d頭 rank有)", race_id, with_rank)

    # 予想評価
    try:
        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, race_id)
        logger.info(
            "評価完了: race_id=%s  的中=%d件  投資¥%.0f  払戻¥%.0f  ROI=%.1f%%",
            race_id, result.hit_count,
            result.total_invested, result.total_payout, result.roi,
        )
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True


def fetch_for_date(date_str: str, force_all: bool = False, delay: float = 1.5) -> int:
    """
    指定日の全(未取得)レースの結果を JVLink から取得する。

    Args:
        date_str: YYYYMMDD 形式
        force_all: True なら既存データも上書き
        delay:     使用しない（後方互換のため残す）

    Returns:
        取得成功レース数
    """
    from src.database.init_db import init_db
    from src.evaluation.evaluator import Evaluator

    date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    # JVLink 一括同期（日付分を一度だけ実行）
    ok = _run_jvlink_race_sync(date_str)
    if not ok:
        logger.warning("JVLink 同期失敗: date=%s", date_str)

    conn = init_db()
    race_ids = _get_target_race_ids(conn, date_iso, force_all)

    if not race_ids:
        logger.info("結果取得済みレースなし (date=%s)", date_iso)
        conn.close()
        return 0

    logger.info("評価対象: %d レース (date=%s)", len(race_ids), date_iso)
    evaluator = Evaluator()
    saved = 0
    for race_id in race_ids:
        with_rank = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
            (race_id,),
        ).fetchone()[0]
        if with_rank == 0:
            continue
        try:
            result = evaluator.evaluate_race(conn, race_id)
            saved += 1
            logger.info(
                "評価完了: %s  的中=%d  ROI=%.1f%%",
                race_id, result.hit_count, result.roi,
            )
        except Exception as e:
            logger.warning("評価失敗 %s: %s", race_id, e)

    conn.close()
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
    parser = argparse.ArgumentParser(description="レース結果速報取得・評価・ダッシュボード更新 (JVLink経由)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--race-id",  help="対象レース ID (例: 202603010501)")
    group.add_argument("--date",     help="対象日 YYYYMMDD (全未取得レース)")
    parser.add_argument("--all",     action="store_true",
                        help="--date 指定時: 既存データも上書き取得")
    parser.add_argument("--delay",   type=float, default=1.5,
                        help="（後方互換: 無効）")
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
        logger.info("完了: %d レース評価", saved)
        if saved > 0 and not args.no_dashboard:
            _run_generate_data()


if __name__ == "__main__":
    main()
