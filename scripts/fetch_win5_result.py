# -*- coding: utf-8 -*-
"""
WIN5 確定結果取得スクリプト

当日の WIN5 確定結果（各レースの1着馬番5つ＋払戻金額）を
netkeiba から取得して win5_results テーブルに保存する。

スケジューラーから job_win5_result_fetch() 経由で
土日 17:30 前後（全レース確定後）に呼ばれる。

Usage:
    python scripts/fetch_win5_result.py --date 20260511
    python scripts/fetch_win5_result.py           # 当日
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

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
logger = logging.getLogger("fetch_win5_result")


def _get_win5_race_ids(conn, formatted_date: str) -> list[str]:
    """WIN5 予想レコードの race_ids、なければ当日レース先頭5件を返す。"""
    row = conn.execute(
        """
        SELECT p.combination_json
        FROM predictions p
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date = ? AND p.model_type = 'WIN5' AND p.bet_type = 'WIN5'
        ORDER BY p.created_at DESC
        LIMIT 1
        """,
        (formatted_date,),
    ).fetchone()

    if row:
        try:
            combo = json.loads(row[0] or "{}")
            race_ids = combo.get("race_ids") or []
            if race_ids:
                return race_ids
        except Exception:
            pass

    # 予想レコードがない場合は当日レース先頭5件を使用
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY race_id LIMIT 5",
        (formatted_date,),
    ).fetchall()
    return [r[0] for r in rows]


def fetch_win5_result(target_date: str | None = None) -> dict:
    """
    指定日の WIN5 確定結果を netkeiba から取得して win5_results に保存する。

    Args:
        target_date: "YYYYMMDD" 形式。None なら当日。

    Returns:
        {"date": ..., "winning_numbers": [...], "payout": ..., "skipped": bool}
    """
    if target_date is None:
        target_date = date.today().strftime("%Y%m%d")

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    logger.info("WIN5結果取得開始: 対象日=%s", formatted)

    from src.database.init_db import init_db
    from src.scraper.netkeiba import fetch_race_payouts

    conn = init_db()

    # 既に結果が保存済みか確認
    existing = conn.execute(
        "SELECT COUNT(*) FROM win5_results WHERE race_date = ?",
        (formatted,),
    ).fetchone()[0]
    if existing > 0:
        logger.info("WIN5結果取得スキップ: 既保存 date=%s", formatted)
        conn.close()
        return {"date": target_date, "skipped": True, "reason": "already saved"}

    # WIN5 対象レース ID を取得
    win5_race_ids = _get_win5_race_ids(conn, formatted)
    if len(win5_race_ids) < 5:
        logger.warning("WIN5結果取得スキップ: レース不足 %d 件", len(win5_race_ids))
        conn.close()
        return {"date": target_date, "skipped": True, "reason": f"only {len(win5_race_ids)} races"}

    logger.info("WIN5対象レース: %s", win5_race_ids)

    # 各 WIN5 レースページから WIN5 払戻セクションを探す（通常は最後のレースに掲載）
    winning_numbers: list[int] = []
    payout_amount = 0

    for race_id in reversed(win5_race_ids):
        try:
            logger.info("払戻取得試行: race_id=%s", race_id)
            payouts = fetch_race_payouts(race_id, delay=2.0)
            win5_entry = next((p for p in payouts if p["bet_type"] == "WIN5"), None)
            if win5_entry:
                combo_str = win5_entry["combination"]
                # "3-7-12-1-9" → [3, 7, 12, 1, 9]
                nums = [
                    int(n.strip())
                    for n in combo_str.replace("→", "-").split("-")
                    if n.strip().isdigit()
                ]
                if len(nums) == 5:
                    winning_numbers = nums
                    payout_amount = win5_entry["payout"]
                    logger.info(
                        "WIN5結果発見: race_id=%s combination=%s payout=¥%d",
                        race_id, combo_str, payout_amount,
                    )
                    break
                else:
                    logger.warning("WIN5組み合わせ馬番数異常: %s → %s", combo_str, nums)
        except Exception as exc:
            logger.warning("WIN5結果取得失敗: race_id=%s: %s", race_id, exc)

    if not winning_numbers:
        logger.warning("WIN5確定結果が見つかりません: date=%s", formatted)
        conn.close()
        return {
            "date": target_date,
            "skipped": False,
            "reason": "no win5 payout found on any of the 5 race pages",
            "winning_numbers": [],
        }

    # win5_results に保存
    with conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO win5_results (race_date, race_ids, winning_numbers, payout)
            VALUES (?, ?, ?, ?)
            """,
            (
                formatted,
                json.dumps(win5_race_ids),
                json.dumps(winning_numbers),
                payout_amount,
            ),
        )
    logger.info(
        "WIN5結果保存完了: date=%s 的中馬番=%s 払戻=¥%d",
        formatted, winning_numbers, payout_amount,
    )

    conn.close()
    return {
        "date": target_date,
        "race_ids": win5_race_ids,
        "winning_numbers": winning_numbers,
        "payout": payout_amount,
        "skipped": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="WIN5確定結果取得")
    parser.add_argument("--date", help="対象日 YYYYMMDD（省略時=当日）")
    args = parser.parse_args()
    result = fetch_win5_result(args.date)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
