# -*- coding: utf-8 -*-
"""
5/3 未確定19レース 払戻速報回収スクリプト (32bit Python 専用)

JVLink OPT_NORMAL で配信される 速報払戻(data_cat='2') を一時的に受け入れ、
確定データが未配信の19レースの払戻・着順を補完する。

Usage:
    py -3.14-32 scripts/recover_503_payouts.py

確認後:
    py scripts/infer_ranks_from_payouts.py --year 2026
    py web/generate_data.py
"""

from __future__ import annotations

import io
import os
import sqlite3
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

_MISSING_RACES = {
    "202604010210", "202604010211", "202604010212",
    "202605020405", "202605020406", "202605020407", "202605020408",
    "202605020409", "202605020410", "202605020411", "202605020412",
    "202608030405", "202608030406", "202608030407", "202608030408",
    "202608030409", "202608030410", "202608030411", "202608030412",
}

# 明らかなプレースホルダー値を除外 (16000は特異値として既知)
_PLACEHOLDER_AMOUNTS = {16000, 0}
_MAX_SANSHO_PAYOUT   = 10_000_000  # 三連単上限

from src.scraper.jravan_client import (
    JVREAD_DOWNLOADING, JVREAD_EOF, JVREAD_FILECHANGE,
    JVLinkClient, OPT_NORMAL,
    _parse_payout,   # type: ignore[attr-defined]
    _PAYOUT_SPECS,   # type: ignore[attr-defined]
)
from src.database.init_db import init_db


def _parse_payout_guarded(raw: bytes, rec_type: str) -> dict | None:
    """_parse_payout のラッパー — プレースホルダー値を除外する。"""
    result = _parse_payout(raw, rec_type)
    if result is None:
        return None
    # race_id が対象外なら捨てる
    if result.get("race_id") not in _MISSING_RACES:
        return None
    # 払戻額が明らかなプレースホルダーなら捨てる
    entries = result.get("payouts", [])
    valid = [
        e for e in entries
        if e.get("payout") not in _PLACEHOLDER_AMOUNTS
        and (e.get("payout") or 0) <= _MAX_SANSHO_PAYOUT
    ]
    if not valid:
        return None
    result["payouts"] = valid
    return result


def _save_payouts(conn: sqlite3.Connection, rec: dict) -> int:
    """race_payouts に INSERT OR REPLACE して保存件数を返す。"""
    saved = 0
    race_id = rec["race_id"]
    for e in rec["payouts"]:
        with conn:
            conn.execute(
                """
                INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(race_id, bet_type, combination) DO UPDATE SET
                    payout     = excluded.payout,
                    popularity = excluded.popularity
                """,
                (race_id, e["bet_type"], e.get("combination", ""),
                 e["payout"], e.get("popularity")),
            )
            saved += 1
    return saved


def main() -> int:
    sid = os.getenv("JRAVAN_SID", "UMALOGI00")
    print(f"[recover] JRAVAN_SID={'*' * len(sid)}")
    print(f"[recover] 対象: {len(_MISSING_RACES)}レース")

    conn = init_db()
    total_saved = 0
    found_races: set[str] = set()

    try:
        with JVLinkClient(sid) as client:
            code = client.open("RACE", "20260503", OPT_NORMAL)
            print(f"[recover] JVOpen code={code}")
            if code < 0:
                print("[recover] データなし")
                conn.close()
                return 0

            read_count = 0
            while True:
                rc, data = client.read_record()

                if rc == JVREAD_EOF:
                    break
                if rc == JVREAD_FILECHANGE:
                    continue
                if rc == JVREAD_DOWNLOADING:
                    time.sleep(1)
                    continue
                if rc < 0:
                    print(f"[recover] JVRead エラー rc={rc}")
                    break

                read_count += 1

                if not data or len(data) < 3:
                    continue

                rec_type = data[:2].decode("ascii", errors="replace")
                data_cat  = chr(data[2])

                # HR/WH系のみ、速報(data_cat='2')も許可
                if rec_type not in _PAYOUT_SPECS:
                    continue
                if data_cat not in ("1", "2"):
                    continue

                rec = _parse_payout_guarded(data, rec_type)
                if rec is None:
                    continue

                race_id = rec["race_id"]
                n = _save_payouts(conn, rec)
                if n > 0:
                    total_saved += n
                    found_races.add(race_id)

    except Exception as exc:
        print(f"[recover] 例外: {exc}", file=sys.stderr)
        conn.close()
        return 1

    print(f"\n[recover] 完了: 払戻{total_saved}件保存 / {len(found_races)}レース補完")
    for rid in sorted(found_races):
        print(f"  {rid}")

    still_missing = _MISSING_RACES - found_races
    if still_missing:
        print(f"\n[recover] 払戻未取得: {len(still_missing)}レース")
        for rid in sorted(still_missing):
            print(f"  {rid}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
