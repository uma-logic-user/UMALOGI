# -*- coding: utf-8 -*-
"""
JVRTOpen 速報リアルタイム取得ワーカー  (32bit Python 専用)

【重要】JV-Link は 32bit COM サーバーのため、64bit 側からは subprocess で呼ぶ:
  py -3.14-32 scripts/_jvrt_odds_worker.py --race-id 202605021201 --date 20260531

JRA-VAN 速報データを COM 経由で直接取得し JSON で stdout に出力する（DB は変更しない）:
  - 速報単勝オッズ (JVRTOpen 0B30 / O1)
  - 速報馬体重     (JVRTOpen 0B11 / WH)
  - 速報天候馬場   (JVRTOpen 0B12 / RA)  ※0B42 はオッズを返すため不可

出力（最終行に1行 JSON）:
  {"race_id":"...","head_count":16,"odds":[...],
   "weights":{"1":{"weight":482,"weight_diff":-2},...},
   "weather":"晴","condition":"良"}

exit code: 0=正常（取得0件でも0）, 1=致命的エラー
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
    parse_record,
)
from src.scraper.rtd_reader import (
    build_rt_race_key,
    parse_o1_realtime,
    parse_wh_realtime,
)

_DS_WIN_ODDS = "0B30"  # 速報オッズ（単複枠）→ O1
_DS_WEIGHT = "0B11"  # 速報馬体重 → WH
_DS_RACE = "0B12"  # 速報出馬表 → RA/SE（RA に天候馬場）
_MAX_READ = 60  # 1ストリームあたりの最大読み取り回数
_DOWNLOAD_WAITS = 5  # JVREAD_DOWNLOADING の最大待機回数


def _read_records(client, prefix: bytes):
    """指定レコード種別の生バイトを順次 yield する（DOWNLOADING/FILECHANGE を吸収）。"""
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
        if data and data.startswith(prefix):
            yield data


def _fetch_odds(client, key: str, race_id: str) -> dict:
    code = client.rt_open(_DS_WIN_ODDS, key)
    print(f"[worker] JVRTOpen({_DS_WIN_ODDS}) code={code}", file=sys.stderr)
    out: dict = {"head_count": 0, "odds": []}
    if code == 0:
        for data in _read_records(client, b"O1"):
            info = parse_o1_realtime(data.decode("cp932", "replace"), race_id)
            if info is not None:
                out = {
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
                break
    client.close()
    return out


def _fetch_weights(client, key: str, race_id: str) -> dict:
    code = client.rt_open(_DS_WEIGHT, key)
    print(f"[worker] JVRTOpen({_DS_WEIGHT}) code={code}", file=sys.stderr)
    weights: dict[str, dict] = {}
    if code == 0:
        for data in _read_records(client, b"WH"):
            parsed = parse_wh_realtime(data, race_id)
            if parsed:
                weights = {str(k): v for k, v in parsed.items()}
                break
    client.close()
    return weights


def _fetch_weather(client, key: str) -> dict:
    code = client.rt_open(_DS_RACE, key)
    print(f"[worker] JVRTOpen({_DS_RACE}) code={code}", file=sys.stderr)
    out = {"weather": "", "condition": ""}
    if code == 0:
        for data in _read_records(client, b"RA"):
            rec = parse_record(data)
            if rec and rec.get("_record_type") == "RA":
                out = {
                    "weather": rec.get("weather") or "",
                    "condition": rec.get("condition") or "",
                }
                break
    client.close()
    return out


def fetch(race_id: str, date8: str, sid: str) -> dict:
    """速報オッズ・馬体重・天候馬場を 1 セッションで取得して dict を返す。"""
    key = build_rt_race_key(race_id, date8)
    base = {
        "race_id": race_id,
        "head_count": 0,
        "odds": [],
        "weights": {},
        "weather": "",
        "condition": "",
    }
    if not key:
        print(f"[worker] 不正な race_id/date: {race_id} {date8}", file=sys.stderr)
        return base

    with JVLinkClient(sid) as client:
        odds = _fetch_odds(client, key, race_id)
        weights = _fetch_weights(client, key, race_id)
        weather = _fetch_weather(client, key)

    base.update(odds)
    base["weights"] = weights
    base.update(weather)
    return base


def main() -> int:
    p = argparse.ArgumentParser(
        description="JVRTOpen 速報リアルタイム取得ワーカー (32bit)"
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
