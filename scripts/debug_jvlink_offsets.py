"""
JVLink SE/HR レコードのバイトオフセット解析スクリプト

使用例:
  py -3.14-32 scripts/debug_jvlink_offsets.py --fromtime 20250101 --option 4
  py -3.14-32 scripts/debug_jvlink_offsets.py --fromtime 20250101 --option 1

SEレコード/HRレコードを最初の数件取得し、全バイトをダンプする。
known-good な race_results (netkeiba 由来) と照合してオフセットを確定する。
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from src.scraper.jravan_client import (
    JVLinkClient, OPT_NORMAL, OPT_STORED, JVREAD_EOF, JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING, _to_bytes, _safe_int_val, _make_race_id,
    _str, _sjis, _RK_KAISAI_DT, _RK_JYO, _RK_KAI, _RK_NICHI, _RK_RACE_NO,
)

import time
import logging
logging.basicConfig(level=logging.WARNING)

DB_PATH = _ROOT / "data" / "umalogi.db"
SID = "UMALOGI00"


def hex_dump(raw: bytes, label: str = "", max_bytes: int = 600) -> None:
    print(f"\n=== {label} ({len(raw)} bytes) ===")
    for i in range(0, min(len(raw), max_bytes), 16):
        chunk = raw[i:i+16]
        hex_part = ' '.join(f'{b:02x}' for b in chunk)
        asc_part = ''.join(chr(b) if 0x20 <= b < 0x7F else '.' for b in chunk)
        print(f"  {i:4d}  {hex_part:<48s}  {asc_part}")


def try_decode_sjis(raw: bytes, sl: slice) -> str:
    """スライスを Shift-JIS でデコード試行。"""
    try:
        return raw[sl].decode('cp932', errors='replace').replace('\x00', '').strip()
    except Exception:
        return '?'


def analyze_se(raw: bytes, conn: sqlite3.Connection) -> None:
    """SEレコードを解析して既知フィールドと照合する。"""
    race_id = _make_race_id(raw)
    print(f"\n  race_id(computed): {race_id}")

    # netkeiba由来のデータで照合
    rows = conn.execute(
        "SELECT horse_name, rank, horse_number FROM race_results "
        "WHERE race_id=? AND horse_number IS NULL ORDER BY rank LIMIT 5",
        (race_id,)
    ).fetchall()
    if rows:
        print(f"  netkeiba照合データ (rank/horse_number):")
        for r in rows:
            print(f"    rank={r[1]} horse_no={r[2]} name={r[0]}")

    # 既知オフセット確認
    print(f"  [27:28] waku_ban = '{try_decode_sjis(raw, slice(27, 28))}'")
    print(f"  [28:30] uma_ban  = '{try_decode_sjis(raw, slice(28, 30))}'")
    print(f"  [30:40] horse_id = '{try_decode_sjis(raw, slice(30, 40))}'")
    print(f"  [40:76] horse_nm = '{try_decode_sjis(raw, slice(40, 76))}'")
    print(f"  [76:77] sex      = '{try_decode_sjis(raw, slice(76, 77))}'")
    print(f"  [77:79] age      = '{try_decode_sjis(raw, slice(77, 79))}'")
    print(f"  [79:84] jkey_cd  = '{try_decode_sjis(raw, slice(79, 84))}'")
    print(f"  [84:104] jkey_nm = '{try_decode_sjis(raw, slice(84, 104))}'")
    print(f"  [105:108] load   = '{try_decode_sjis(raw, slice(105, 108))}'")

    # 着順候補を複数のオフセットで試す
    print(f"\n  -- 着順オフセット候補 --")
    for off in range(100, min(len(raw)-2, 380), 1):
        val = try_decode_sjis(raw, slice(off, off+2))
        try:
            n = int(val)
            if 1 <= n <= 18:
                print(f"    [{off}:{off+2}] = '{val}' -> {n}  ★候補")
        except Exception:
            pass

    hex_dump(raw, f"SE race_id={race_id}")


def analyze_hr(raw: bytes, conn: sqlite3.Connection) -> None:
    """HRレコードを解析して既知払戻と照合する。"""
    race_id = _make_race_id(raw)
    print(f"\n  race_id(computed): {race_id}")

    # 既知払戻データ
    rows = conn.execute(
        "SELECT bet_type, combination, payout FROM race_payouts "
        "WHERE race_id=? AND bet_type='単勝' AND payout < 100000",
        (race_id,)
    ).fetchall()
    if rows:
        print(f"  netkeiba 単勝払戻: {rows}")
    else:
        rows2 = conn.execute(
            "SELECT bet_type, combination, payout FROM race_payouts WHERE race_id=? LIMIT 5",
            (race_id,)
        ).fetchall()
        print(f"  DB払戻サンプル: {rows2}")

    hex_dump(raw, f"HR race_id={race_id}", max_bytes=400)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--fromtime', default='20250101')
    ap.add_argument('--option', type=int, default=4, choices=[1,2,3,4])
    ap.add_argument('--max-se', type=int, default=3, help='SEレコード取得上限')
    ap.add_argument('--max-hr', type=int, default=3, help='HRレコード取得上限')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))

    n_se = n_hr = 0
    target_se = args.max_se
    target_hr = args.max_hr

    print(f"JVLink SE/HR オフセット解析: fromtime={args.fromtime} option={args.option}")

    with JVLinkClient(SID) as client:
        code = client.open("RACE", args.fromtime, args.option)
        if code < 0:
            print(f"JVOpen 失敗: code={code}")
            return

        while True:
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

            rec_type = data[:2].decode('ascii', errors='replace')

            if rec_type == 'SE' and n_se < target_se:
                n_se += 1
                print(f"\n{'='*60}")
                print(f"SE レコード #{n_se}")
                analyze_se(data, conn)
                if n_se >= target_se and n_hr >= target_hr:
                    break

            elif rec_type == 'HR' and n_hr < target_hr:
                n_hr += 1
                print(f"\n{'='*60}")
                print(f"HR レコード #{n_hr}")
                analyze_hr(data, conn)
                if n_se >= target_se and n_hr >= target_hr:
                    break

    conn.close()
    print(f"\n解析完了: SE={n_se}件 HR={n_hr}件")


if __name__ == '__main__':
    main()
