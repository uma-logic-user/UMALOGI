"""
完全確定HRレコード（全馬券種有効）を探して構造を確定するスクリプト。

実行: py -3.14-32 scripts/hexdump_hr_full.py
"""
from __future__ import annotations
import os, sys, time
sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)

from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import JVLinkClient, OPT_NORMAL, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING


def hex_full(data: bytes) -> None:
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_part = " ".join(f"{b:02X}" for b in chunk)
        asc_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
        print(f"  {i:04X}  {hex_part:<47}  {asc_part}")


def is_complete_hr(data: bytes) -> bool:
    """複数馬券種に有効払戻が入ったHRか判定する。"""
    # 単勝offset27の払戻（5バイト）が>0 かつ 後半部分に有効データ（数字）が含まれる
    if len(data) < 200:
        return False
    try:
        tan_amt = int(data[29:34].decode('ascii', 'strict'))
        if tan_amt <= 0:
            return False
    except:
        return False
    # 後半(offset 200以降)にゼロ以外の数字があるか
    later = data[200:400]
    digit_count = sum(1 for b in later if 0x31 <= b <= 0x39)  # '1'-'9'
    return digit_count >= 5


def decode_ascii_safe(data: bytes) -> str:
    return "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in data)


def main() -> None:
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text('utf-8', errors='replace').splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("JRAVAN_SID 未設定"); sys.exit(1)

    found = []
    with JVLinkClient(sid) as client:
        if client.open("RACE", "20250101000000", OPT_NORMAL) < 0:
            print("JVOpen失敗"); return

        scan = 0
        for _ in range(500000):
            code, data = client.read_record()
            if code == JVREAD_EOF: break
            if code == JVREAD_FILECHANGE: continue
            if code == JVREAD_DOWNLOADING: time.sleep(1); continue
            if code < 0: break
            if not data or len(data) < 2: continue
            if data[:2].decode('ascii','replace') != 'HR': continue
            scan += 1
            if is_complete_hr(data):
                found.append(data)
                print(f"完全HR発見 #{len(found)} (scan={scan})")
                if len(found) >= 3:
                    break

    print(f"\n候補件数: {len(found)} / {scan} HR中")

    for idx, data in enumerate(found[:1]):
        print(f"\n{'='*70}")
        print(f"HR #{idx+1}  {len(data)} bytes")
        print(f"race context: {data[11:27].decode('ascii','replace')}")
        hex_full(data)

        # ASCII全体（印字可能部分）
        print("\n=== ASCII全体（ドットは非印字） ===")
        asc = decode_ascii_safe(data)
        for i in range(0, len(asc), 80):
            print(f"  [{i:4d}] {asc[i:i+80]}")

        # 有効エントリを数字のruns（連続数字10桁以上）で探す
        print("\n=== 連続数字ブロック（位置→内容）===")
        i = 0
        while i < len(data):
            if 0x30 <= data[i] <= 0x39:
                j = i
                while j < len(data) and (0x30 <= data[j] <= 0x39):
                    j += 1
                if j - i >= 6:
                    print(f"  [{i:4d}:{j:4d}] ({j-i:3d}桁) {data[i:j].decode('ascii')}")
                i = j
            else:
                i += 1


if __name__ == "__main__":
    main()
