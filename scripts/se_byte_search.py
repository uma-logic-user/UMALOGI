"""
SEレコードの既知文字列サーチ — 騎手/調教師/斤量の正確なバイト位置を特定する。

実行: py -3.14-32 scripts/se_byte_search.py
"""
from __future__ import annotations
import os, sys, time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVLinkClient, OPT_NORMAL,
    JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING,
    _make_race_id,
)


def sjis(s: str) -> bytes:
    return s.encode("cp932")


def find_all(raw: bytes, pattern: bytes) -> list[int]:
    pos, results = 0, []
    while True:
        idx = raw.find(pattern, pos)
        if idx == -1:
            break
        results.append(idx)
        pos = idx + 1
    return results


def hexline(raw: bytes, start: int, end: int, label: str = "") -> str:
    region = raw[start:end]
    hex_str = " ".join(f"{b:02x}" for b in region)
    asc = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in region)
    return f"  [{start:3d}:{end:3d}] {label:<28s} | {hex_str:<50s} | '{asc}'"


def decode(raw: bytes, start: int, length: int, enc: str = "cp932") -> str:
    try:
        return raw[start:start+length].decode(enc, errors="replace").replace("\x00","").strip()
    except Exception:
        return "???"


def analyze(raw: bytes) -> None:
    race_id = _make_race_id(raw)
    cat     = chr(raw[2])
    print(f"\n{'='*80}")
    print(f"race_id={race_id}  cat={cat}  len={len(raw)}")
    print(f"{'='*80}")

    # ── 確定済み領域 ─────────────────────────────────────────────
    horse_name_raw = raw[40:76]
    horse_name = horse_name_raw.decode("cp932", errors="replace").replace("\x00","").strip()
    print(f"  馬名(40-76): {horse_name!r}")

    # ── 馬名直後 76-130 を1バイト単位で全展開 ────────────────────
    print(f"\n  [76..130] バイト単位展開:")
    for i in range(76, min(130, len(raw))):
        b = raw[i]
        # 印刷可能ASCII or 主なコード表示
        char = chr(b) if 0x20 <= b < 0x7F else f"0x{b:02x}"
        print(f"    byte[{i:3d}] = 0x{b:02x}  ({char})")

    # ── 既知文字列を検索 ─────────────────────────────────────────
    print(f"\n  [既知文字列サーチ]")

    # 馬名をSJISで検索してオフセット確認
    nm_sjis = raw[40:76]
    for needle_str, label in [
        ("木村哲也", "trainer:木村哲也"),
        ("手塚貴徳", "trainer:手塚貴徳"),
        ("堀内岳志", "trainer:堀内岳志"),
        ("田村康仁", "trainer:田村康仁"),
        ("杉浦宏昭", "trainer:杉浦宏昭"),
        ("蛯名正義", "trainer:蛯名正義"),
        ("的場均",   "trainer:的場均"),
        # 騎手候補
        ("川田将雅", "jockey:川田将雅"),
        ("福永祐一", "jockey:福永祐一"),
        ("武豊",     "jockey:武豊"),
        ("岩田康誠", "jockey:岩田康誠"),
        ("松山弘平", "jockey:松山弘平"),
        ("浜中俊",   "jockey:浜中俊"),
        ("石橋脩",   "jockey:石橋脩"),
    ]:
        try:
            needle = sjis(needle_str)
        except Exception:
            continue
        positions = find_all(raw, needle)
        if positions:
            print(f"    '{needle_str}' → bytes {positions} (len={len(needle)})")
            for p in positions:
                end = p + len(needle)
                ctx_start = max(0, p-8)
                ctx_end   = min(len(raw), end+8)
                region = raw[ctx_start:ctx_end]
                hex_str = " ".join(f"{b:02x}" for b in region)
                print(f"      context [{ctx_start}:{ctx_end}]: {hex_str}")

    # ── 重量候補（斤量 50-60kg → raw 00-10 × 10 or +50形式）──────
    print(f"\n  [重量候補領域 80-120]")
    for off in range(80, 125):
        b = raw[off]
        # +50 kg 方式: byte=5→55.0, byte=7→57.0 など (1バイト整数)
        if 0 <= b <= 15:
            print(f"    byte[{off}] = {b} → 斤量候補 {b+50}kg(+50) or raw={b}")
        # ×10 kg 方式: 3バイトASCII "550" → 55.0kg
        if off + 3 <= len(raw):
            tri = raw[off:off+3]
            if all(0x30 <= x <= 0x39 for x in tri):
                val = int(tri)
                if 480 <= val <= 620:
                    print(f"    bytes[{off}:{off+3}] = {tri.decode()} → 斤量候補 {val/10:.1f}kg (×10)")
        # 2バイトASCII "05" = 5 → 55kg (+50方式)
        if off + 2 <= len(raw):
            pair = raw[off:off+2]
            if all(0x30 <= x <= 0x39 for x in pair):
                val = int(pair)
                if 0 <= val <= 15:
                    print(f"    bytes[{off}:{off+2}] = {pair.decode()} → 斤量候補 {val+50}.0kg(ASCII+50)")

    # ── 全体概観 ────────────────────────────────────────────────
    print(f"\n  [全体 16バイト刻み 0-400]")
    for s in range(0, min(400, len(raw)), 16):
        chunk = raw[s:s+16]
        h = " ".join(f"{b:02x}" for b in chunk)
        a = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in chunk)
        print(f"    [{s:3d}] {h:<48s} |{a}|")


def main() -> None:
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text("utf-8", errors="replace").splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: JRAVAN_SID 未設定"); sys.exit(1)

    # 最近のSEレコードを収集（複数レース分）
    se_records: list[bytes] = []
    TARGET = 15

    print("JVLink SE レコード収集中 (OPT_NORMAL, RACE)...")
    with JVLinkClient(sid) as client:
        ret = client.open("RACE", "20260101", OPT_NORMAL)
        print(f"JVOpen ret={ret}")
        if ret < 0:
            print("JVOpen失敗"); return

        for _ in range(10_000_000):
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1); continue
            if code < 0:
                break
            if not data or len(data) < 100:
                continue
            if data[:2].decode("ascii", errors="replace") == "SE":
                # ノクターン(202605020306)またはその周辺レースを優先
                rid = _make_race_id(data)
                if rid.startswith("202605"):
                    se_records.append(data)
                    if len(se_records) >= TARGET:
                        break
                elif len(se_records) == 0:
                    se_records.append(data)  # フォールバック

    print(f"\n取得 SE: {len(se_records)} 件")
    for se in se_records:
        analyze(se)


if __name__ == "__main__":
    main()
