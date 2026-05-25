"""
OPT_NORMAL で差分 SE レコードを取得して rank を補完するスクリプト

py -3.14-32 scripts/fetch_normal_se.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from src.scraper.jravan_client import JVDataLoader, OPT_NORMAL
from src.database.init_db import init_db


def count_nulls(conn) -> int:
    return conn.execute(
        """SELECT COUNT(*) FROM race_results
           WHERE rank IS NULL
             AND (race_id LIKE '202604010%'
                  OR race_id LIKE '202605020%'
                  OR race_id LIKE '202608030%')"""
    ).fetchone()[0]


def main() -> None:
    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: JRAVAN_SID 未設定", flush=True); sys.exit(1)

    conn = init_db()
    before = count_nulls(conn)
    conn.close()
    print(f"=== 前: rank=NULL {before}件 ===", flush=True)

    loader = JVDataLoader(sid=sid)
    # OPT_NORMAL: 前回フェッチ以降の差分を取得
    stats = loader.load_race(fromtime="20260501000000", option=OPT_NORMAL)
    print(
        f"OPT_NORMAL 結果: RA={stats.get('ra',0)} SE={stats.get('se',0)} "
        f"payout={stats.get('payout',0)} read={stats.get('total_read',0)}",
        flush=True,
    )

    conn = init_db()
    after = count_nulls(conn)
    conn.close()
    print(f"=== 後: rank=NULL {after}件 (改善: {before-after}件) ===", flush=True)


if __name__ == "__main__":
    main()
