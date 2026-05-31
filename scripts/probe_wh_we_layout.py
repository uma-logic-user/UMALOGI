# -*- coding: utf-8 -*-
"""速報 WH(馬体重)/WE(天候馬場) レコードの完全ダンプ（32bit・読み取り専用）。
実行: py -3.14-32 scripts/probe_wh_we_layout.py
"""

from __future__ import annotations
import io
import os
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from src.scraper.jravan_client import (
    JVLinkClient,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING,
)

KEY = "2026053105021201"  # 5/31 東京2回12日1R (16頭)


def dump(client, dataspec: str, key: str, n: int = 6) -> None:
    code = client.rt_open(dataspec, key)
    print(f"\n===== JVRTOpen({dataspec}, {key}) code={code} =====")
    if code != 0:
        return
    seen = 0
    waits = 0
    for _ in range(80):
        rc, data = client.read_record()
        if rc == JVREAD_EOF:
            print("EOF")
            break
        if rc == JVREAD_FILECHANGE:
            continue
        if rc == JVREAD_DOWNLOADING:
            waits += 1
            if waits > 5:
                break
            time.sleep(1)
            continue
        if rc < 0:
            print(f"err {rc}")
            break
        if not data:
            continue
        text = data.decode("cp932", errors="replace")
        print(f"[{text[:2]}] strlen={len(text)} bytelen={len(data)} {text[:90]!r}")
        if text[:2] == "WH":
            # 馬体重レイアウト解析用に生バイトを latin-1 で可視化
            print(
                "  RAW(latin1):",
                data[:140]
                .decode("latin-1", "replace")
                .encode("unicode_escape")
                .decode(),
            )
        seen += 1
        if seen >= n:
            break
    client.close()  # JVClose して次の JVRTOpen に備える


def main() -> int:
    sid = os.getenv("JRAVAN_SID", "")
    with JVLinkClient(sid) as c:
        dump(c, "0B11", KEY)  # 速報馬体重(WH)想定
        dump(c, "0B12", KEY)  # 速報出馬表
        dump(c, "0B42", KEY)  # 速報天候馬場(WE)想定
        dump(c, "0B41", "20260531")  # 速報開催情報(日付キー)
    return 0


if __name__ == "__main__":
    sys.exit(main())
