# -*- coding: utf-8 -*-
"""
JVRTOpen 速報単勝オッズ取得ワーカー  (32bit Python 専用)

【重要】JV-Link は 32bit COM サーバーのため、64bit 側からは subprocess で呼ぶ:
  py -3.14-32 scripts/_jvrt_odds_worker.py --race-id 202605021201 --date 20260531

JRA-VAN 速報オッズ（JVRTOpen 0B30）を COM 経由で直接取得し、
単勝オッズ・人気を JSON で stdout に出力する（DB は変更しない）。

出力（最終行に1行 JSON）:
  {"race_id": "...", "head_count": 16, "odds": [{"horse_number":1,"win_odds":4.5,"popularity":2}, ...]}

exit code: 0=正常（オッズ0件でも0）, 1=致命的エラー
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

# Windows CP932 端末対策
if hasattr(sys.stdout, "buffer") and (sys.stdout.encoding or "").lower() not in (
    "utf-8",
    "utf8",
):
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

from src.scraper.jravan_client import (
    JVLinkClient,
    JVREAD_DOWNLOADING,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
)
from src.scraper.rtd_reader import build_rt_race_key, parse_o1_realtime

_DATASPEC_WIN_ODDS = "0B30"  # 速報オッズ（単複枠）
_MAX_READ = 50  # O1 を見つけるまでの最大読み取り回数
_DOWNLOAD_WAITS = 5  # JVREAD_DOWNLOADING の最大待機回数


def fetch(race_id: str, date8: str, sid: str) -> dict:
    """速報単勝オッズを取得して dict を返す。失敗時は odds=[]。"""
    key = build_rt_race_key(race_id, date8)
    if not key:
        print(f"[worker] 不正な race_id/date: {race_id} {date8}", file=sys.stderr)
        return {"race_id": race_id, "head_count": 0, "odds": []}

    with JVLinkClient(sid) as client:
        code = client.rt_open(_DATASPEC_WIN_ODDS, key)
        print(
            f"[worker] JVRTOpen({_DATASPEC_WIN_ODDS}, {key}) code={code}",
            file=sys.stderr,
        )
        if code != 0:
            return {"race_id": race_id, "head_count": 0, "odds": []}

        waits = 0
        for _ in range(_MAX_READ):
            rc, data = client.read_record()
            if rc == JVREAD_EOF:
                break
            if rc == JVREAD_FILECHANGE:
                continue
            if rc == JVREAD_DOWNLOADING:
                waits += 1
                if waits > _DOWNLOAD_WAITS:
                    break
                time.sleep(1)
                continue
            if rc < 0:
                print(f"[worker] JVRead エラー code={rc}", file=sys.stderr)
                break
            if not data:
                continue
            text = data.decode("cp932", errors="replace")
            if not text.startswith("O1"):
                continue
            info = parse_o1_realtime(text, race_id)
            if info is None:
                continue
            return {
                "race_id": race_id,
                "head_count": info.head_count,
                "odds": [
                    {
                        "horse_number": o.horse_number,
                        "win_odds": o.win_odds,
                        "popularity": o.popularity,
                    }
                    for o in info.odds
                ],
            }

    return {"race_id": race_id, "head_count": 0, "odds": []}


def main() -> int:
    p = argparse.ArgumentParser(
        description="JVRTOpen 速報単勝オッズ取得ワーカー (32bit)"
    )
    p.add_argument("--race-id", required=True, help="12桁 race_id")
    p.add_argument("--date", required=True, metavar="YYYYMMDD", help="開催日")
    p.add_argument("--sid", default=os.getenv("JRAVAN_SID", ""))
    args = p.parse_args()

    if not args.sid:
        print("[worker] JRAVAN_SID 未設定", file=sys.stderr)
        return 1

    try:
        result = fetch(args.race_id, args.date, args.sid)
    except Exception as exc:  # noqa: BLE001 — ワーカーは常に安全に終了する
        print(f"[worker] 例外: {exc}", file=sys.stderr)
        return 1

    # 最終行に JSON を出力（呼び出し側が最終 JSON 行をパースする）
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
