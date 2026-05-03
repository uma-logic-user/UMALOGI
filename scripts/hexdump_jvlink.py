"""
JVLink から SE/HR レコードをダンプしてバイトオフセットを確認するスクリプト。
DBには一切書き込まない。

実行: py -3.14-32 scripts/hexdump_jvlink.py
"""
from __future__ import annotations

import os
import sys
import time

# UTF-8 強制
sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)

from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVLinkClient, dump_record,
    OPT_NORMAL, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING,
)

FROM   = "20250101000000"
MAX_SE = 5
MAX_HR = 5


def analyze_se_offsets(raw: bytes) -> None:
    print(f"\n  長さ: {len(raw)} bytes")
    print(f"  [0:2]  rec_type  : {raw[0:2]!r}")
    print(f"  [2:3]  data_cat  : {raw[2:3]!r}")
    print(f"  [3:11] data_date : {raw[3:11]!r}")
    print(f"  [11:19] kaisai_dt: {raw[11:19]!r}")
    print(f"  [19:21] JYO      : {raw[19:21]!r}")
    print(f"  [25:27] RACE_NO  : {raw[25:27]!r}")
    print(f"  [27:28] waku_ban : {raw[27:28]!r}")
    print(f"  [28:30] uma_ban  : {raw[28:30]!r}")
    print(f"  [30:40] horse_id : {raw[30:40]!r}")

    # SJIS馬名探索
    for start in range(36, 80):
        chunk = raw[start:start+36]
        try:
            decoded = chunk.decode('cp932', errors='replace')
            if any(0x4E00 <= ord(c) <= 0x9FFF or 0x3040 <= ord(c) <= 0x30FF for c in decoded):
                print(f"  [{start}:{start+36}] 馬名候補(SJIS): {decoded.rstrip(chr(0)).strip()!r}")
                break
        except Exception:
            pass

    # 着順 1〜18 を探す（2桁ASCII数字）
    print("  --- 着順候補ポジション ---")
    for pos in range(150, 280):
        try:
            s = raw[pos:pos+2].decode('ascii', errors='strict')
            if s.isdigit() and 1 <= int(s) <= 18:
                print(f"    [{pos}:{pos+2}] = {s!r}")
        except Exception:
            pass


def analyze_hr_offsets(raw: bytes) -> None:
    print(f"\n  長さ: {len(raw)} bytes")
    print(f"  [25:27] RACE_NO: {raw[25:27]!r}")

    section = raw[27:90]
    print("  offset27〜 hex: " + " ".join(f"{b:02X}" for b in section))
    print("  offset27〜 asc: " + "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in section))

    for entry_size, combo_len, amount_len, pop_len in [
        (9,  2, 5, 2),
        (12, 2, 8, 2),
    ]:
        print(f"\n  [entry={entry_size}: combo={combo_len}+amount={amount_len}+pop={pop_len}]")
        pos = 27
        for i in range(3):
            if pos + entry_size > len(raw):
                break
            combo  = raw[pos:pos+combo_len]
            amount = raw[pos+combo_len:pos+combo_len+amount_len]
            pop    = raw[pos+combo_len+amount_len:pos+entry_size]
            try:
                amt_int = int(amount.decode('ascii', errors='strict'))
            except Exception:
                amt_int = -1
            print(f"    #{i+1}: combo={combo!r} amt={amount!r}({amt_int:,}) pop={pop!r}")
            pos += entry_size


def main() -> None:
    # .envからSID読み込み
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding='utf-8', errors='replace').splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("エラー: JRAVAN_SID 未設定")
        sys.exit(1)

    print(f"SID: {sid[:4]}****  FROM: {FROM}")

    se_count = 0
    hr_count = 0

    with JVLinkClient(sid) as client:
        open_code = client.open("RACE", FROM, OPT_NORMAL)
        if open_code < 0:
            print(f"JVOpen失敗: {open_code}")
            return

        while se_count < MAX_SE or hr_count < MAX_HR:
            code, data = client.read_record()

            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1)
                continue
            if code < 0:
                print(f"JVRead error: {code}")
                break
            if not data or len(data) < 2:
                continue

            rec_type = data[:2].decode('ascii', errors='replace')

            if rec_type == 'SE' and se_count < MAX_SE:
                se_count += 1
                print(f"\n{'='*60}")
                dump_record(data[:280], f"SE#{se_count}")
                analyze_se_offsets(data)

            elif rec_type == 'HR' and hr_count < MAX_HR:
                hr_count += 1
                print(f"\n{'='*60}")
                dump_record(data[:120], f"HR#{hr_count}")
                analyze_hr_offsets(data)

    print(f"\n完了: SE={se_count}, HR={hr_count}")


if __name__ == "__main__":
    main()
