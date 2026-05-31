# -*- coding: utf-8 -*-
"""
JVLink ライブ realtime 速報プローブ（32bit・読み取り専用・DB 非変更）

本日の実 race_id から正しい速報キー (YYYYMMDD+JYO+KAI+NICHI+RR) を組み立て、
JVRTOpen で速報オッズ(0B30)/速報出馬表(0B11/0B12)/速報天候馬場(0B42) を試行する。

実行: py -3.14-32 scripts/probe_jvlink_realtime.py
"""

from __future__ import annotations

import io
import os
import sqlite3
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
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


def _code_of(result: object) -> int:
    if isinstance(result, (tuple, list)):
        return int(result[0])
    return int(result)  # type: ignore[arg-type]


def _today_keys() -> list[tuple[str, str]]:
    """(race_id, 速報キー16桁) のリストを DB から構築する。"""
    con = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
    rows = con.execute(
        "SELECT race_id, date FROM races WHERE date = ? ORDER BY race_id",
        (time.strftime("%Y-%m-%d"),),
    ).fetchall()
    con.close()
    out: list[tuple[str, str]] = []
    for rid, d in rows:
        if len(rid) != 12 or not d:
            continue
        date8 = d.replace("-", "")  # YYYYMMDD
        jyo, kai, nichi, rr = rid[4:6], rid[6:8], rid[8:10], rid[10:12]
        key = f"{date8}{jyo}{kai}{nichi}{rr}"  # 16桁速報レースキー
        out.append((rid, key))
    return out


def main() -> int:
    sid = os.getenv("JRAVAN_SID", "")
    if not sid:
        print("[FAIL] JRAVAN_SID 未設定")
        return 1
    import win32com.client  # type: ignore[import]

    try:
        from src.ops.jvlink_dialog_handler import start_dialog_handler

        start_dialog_handler(interval=0.3)
    except Exception:
        pass

    jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
    ret = jvl.JVInit(sid)
    print(f"[JVInit] code={ret}")
    if ret != 0:
        return 1

    keys = _today_keys()
    print(f"[info] 本日のレースキー {len(keys)} 件")
    if not keys:
        print("[warn] 本日のレースが DB にない")
        return 0

    def try_rt(tag: str, dataspec: str, key: str, read: bool = False) -> int:
        try:
            r = jvl.JVRTOpen(dataspec, key)
            code = _code_of(r)
        except Exception as e:
            print(f"  [{tag}] JVRTOpen({dataspec},{key}) 例外 {e}")
            return -9999
        print(f"  [{tag}] JVRTOpen({dataspec}, {key}) -> code={code}")
        if code == 0 and read:
            buff = " " * 120000
            for _ in range(4):
                try:
                    rr = jvl.JVRead(buff, len(buff), " " * 256)
                except Exception as e:
                    print(f"        JVRead 例外 {e}")
                    break
                rcode = _code_of(rr)
                rec = rr[1] if isinstance(rr, (tuple, list)) and len(rr) >= 2 else ""
                print(f"        JVRead code={rcode} rec[:48]={str(rec)[:48]!r}")
                if rcode <= 0:
                    break
        try:
            jvl.JVClose()
        except Exception:
            pass
        time.sleep(0.25)
        return code

    # 先頭3レース（東京/京都の早いR）で速報オッズ単複を試す — 最重要
    print("\n=== 速報オッズ単複 0B30（最重要・リアルタイムオッズ）===")
    for rid, key in keys[:3]:
        try_rt(f"odds {rid}", "0B30", key, read=True)

    # 速報出馬表 0B11(馬名表) / 0B12(出馬表) — 馬体重含む
    print("\n=== 速報出馬表 0B11 / 0B12（馬体重）===")
    rid0, key0 = keys[0]
    try_rt(f"name {rid0}", "0B11", key0, read=True)
    try_rt(f"entry {rid0}", "0B12", key0, read=True)

    # 速報天候馬場 0B42 — レースキーで試行
    print("\n=== 速報天候馬場 0B42 ===")
    try_rt(f"weather {rid0}", "0B42", key0, read=True)
    # 0B42 は開催単位キー(YYYYMMDD+JYO)の可能性 → 両形式試す
    try_rt(f"weather-day {rid0}", "0B42", key0[:10], read=True)

    print("\n[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
