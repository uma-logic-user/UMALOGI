"""
2026-05-03 欠損払戻データ再取得スクリプト

JVLink OPT_STORED=4 を使用して JRA-VAN ローカルキャッシュから
当日分の SE(成績) および HR(払戻) レコードを再取得する。

実行コマンド (32bit Python 必須):
    py -3.14-32 scripts/refetch_20260503.py
"""
from __future__ import annotations

import os
import sys
import sqlite3
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from src.scraper.jravan_client import JVDataLoader, OPT_STORED, DATASPEC_RACE
from src.database.init_db import init_db


TARGET_DATE = "20260503"
TARGET_RACES = [
    # 新潟 R10-R12
    "202604010210", "202604010211", "202604010212",
    # 東京 R05-R12
    "202605020405", "202605020406", "202605020407", "202605020408",
    "202605020409", "202605020410", "202605020411", "202605020412",
    # 京都 R05-R12
    "202608030405", "202608030406", "202608030407", "202608030408",
    "202608030409", "202608030410", "202608030411", "202608030412",
]


def check_payouts(conn: sqlite3.Connection) -> dict[str, int]:
    """欠損レースの払戻件数を返す。"""
    result: dict[str, int] = {}
    for race_id in TARGET_RACES:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM race_payouts WHERE race_id = ?", (race_id,)
        ).fetchone()[0]
        result[race_id] = cnt
    return result


def main() -> None:
    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: 環境変数 JRAVAN_SID が未設定です。.env を確認してください。", flush=True)
        sys.exit(1)

    conn = init_db()
    print("=== 取得前 払戻件数 ===", flush=True)
    before = check_payouts(conn)
    missing_before = sum(1 for v in before.values() if v == 0)
    for race_id, cnt in before.items():
        status = "  欠損" if cnt == 0 else f"  OK({cnt}件)"
        print(f"  {race_id}: {status}", flush=True)
    print(f"欠損レース数: {missing_before} / {len(TARGET_RACES)}", flush=True)
    conn.close()

    print(f"\n=== JVLink OPT_STORED 再取得 (fromtime={TARGET_DATE}) ===", flush=True)
    loader = JVDataLoader(sid=sid)
    stats = loader.load_race(fromtime=TARGET_DATE, option=OPT_STORED)
    print(f"取得結果: SE={stats.get('se', 0)} payout={stats.get('payout', 0)} "
          f"read={stats.get('total_read', 0)} open_code={stats.get('open_code', '?')}", flush=True)

    conn = init_db()
    print("\n=== 取得後 払戻件数 ===", flush=True)
    after = check_payouts(conn)
    missing_after = sum(1 for v in after.values() if v == 0)
    for race_id, cnt in after.items():
        before_cnt = before[race_id]
        if before_cnt == 0 and cnt > 0:
            status = f"  FIXED ({cnt}件)"
        elif cnt == 0:
            status = "  まだ欠損"
        else:
            status = f"  OK({cnt}件)"
        print(f"  {race_id}: {status}", flush=True)

    print(f"\n補完結果: {missing_before - missing_after} レース修復 "
          f"/ 残欠損 {missing_after} レース", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
