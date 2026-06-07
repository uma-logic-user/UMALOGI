"""
JVLink DIFN (UM/KS/CH) マスタレコードのバイトオフセット検証スクリプト。

UM(競走馬)/KS(騎手)/CH(調教師) の生バイトをダンプし、現行スライスとの
ズレを実データで確定するための一時診断ツール。

使用例:
  py -3-32 scripts/dump_difn_bytes.py --fromtime 20250101 --option 1
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from src.scraper.jravan_client import (  # noqa: E402
    JVLinkClient,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING,
    DATASPEC_DIFN,
)

logging.basicConfig(level=logging.WARNING)
SID = "UMALOGI00"


def hex_dump(raw: bytes, label: str = "", max_bytes: int = 160) -> None:
    print(f"\n=== {label} ({len(raw)} bytes) ===")
    for i in range(0, min(len(raw), max_bytes), 16):
        chunk = raw[i : i + 16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        asc_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
        print(f"  {i:4d}  {hex_part:<48s}  {asc_part}")


def sj(raw: bytes, a: int, b: int) -> str:
    try:
        return raw[a:b].decode("cp932", errors="replace").replace("\x00", "").strip()
    except Exception:
        return "?"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fromtime", default="20250101")
    ap.add_argument("--option", type=int, default=1, choices=[1, 2, 3, 4])
    ap.add_argument("--max-each", type=int, default=2)
    args = ap.parse_args()

    want = {"UM": args.max_each, "KS": args.max_each, "CH": args.max_each}
    got = {"UM": 0, "KS": 0, "CH": 0}

    print(f"DIFN ダンプ: fromtime={args.fromtime} option={args.option}")
    with JVLinkClient(SID) as client:
        code = client.open(DATASPEC_DIFN, args.fromtime, args.option)
        if code < 0:
            print(f"JVOpen 失敗: code={code}")
            return
        while any(got[k] < want[k] for k in want):
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1)
                continue
            if code < 0:
                print(f"JVRead エラー: {code}")
                break
            if not data:
                continue
            rt = data[:2].decode("ascii", errors="replace")
            if rt in want and got[rt] < want[rt]:
                got[rt] += 1
                print(f"\n{'=' * 64}\n{rt} レコード #{got[rt]}")
                # 現行スライス位置の値と +1 シフト後の値を併記
                if rt == "UM":
                    print(f"  cur [10:20] horse_id = '{sj(data, 10, 20)}'")
                    print(f"  +1  [11:21] horse_id = '{sj(data, 11, 21)}'")
                    print(f"  cur [3:11]  data_date= '{sj(data, 3, 11)}'")
                    print(f"  +1  [4:12]  data_date= '{sj(data, 4, 12)}'")
                elif rt in ("KS", "CH"):
                    print(f"  cur [10:15] code = '{sj(data, 10, 15)}'")
                    print(f"  +1  [11:16] code = '{sj(data, 11, 16)}'")
                hex_dump(data, f"{rt}")
    print(f"\n完了: {got}")


if __name__ == "__main__":
    main()
