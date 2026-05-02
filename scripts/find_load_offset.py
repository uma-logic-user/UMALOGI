"""
SE レコードで斤量(×10)と着順の正確なバイトオフセットを特定する。

実行: py -3.14-32 scripts/find_load_offset.py

出力:
  - 確定済みフィールド周辺のダンプ
  - [76:200] を64バイト単位で表示
  - 斤量らしい "5xx"/"6xx" パターンを自動検出
  - 着順らしい "0x"/"1x" 2バイトパターンを検出
"""
from __future__ import annotations
import os, sys, time, re
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVLinkClient, OPT_NORMAL,
    JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING,
    _make_race_id,
)


def asc(b: bytes) -> str:
    return "".join(chr(x) if 0x20 <= x < 0x7F else "·" for x in b)


def sjis(b: bytes) -> str:
    try:
        return b.decode("cp932", errors="replace").replace("\x00", "").strip()
    except Exception:
        return "?"


def dump(raw: bytes, s: int, e: int, label: str) -> str:
    chunk = raw[s:e]
    return f"  [{s:3d}:{e:3d}] {label:<30s} asc={asc(chunk)!r:20s} hex={' '.join(f'{x:02x}' for x in chunk)}"


def analyze(raw: bytes, idx: int) -> None:
    race_id = _make_race_id(raw)
    uma_ban = int(raw[28:30].decode("ascii", errors="replace").strip() or "0")
    horse_nm = sjis(raw[40:76])
    print(f"\n{'='*72}")
    print(f"SE #{idx}  race={race_id}  uma={uma_ban:02d}  horse='{horse_nm}'  len={len(raw)}")

    # 確定フィールド
    print(dump(raw, 27, 28,  "枠番[確定]"))
    print(dump(raw, 28, 30,  "馬番[確定]"))
    print(dump(raw, 78, 79,  "性別[実測確定]"))
    print(dump(raw, 80, 82,  "馬齢[実測確定]"))
    print(dump(raw, 84, 90,  "騎手CD[実測確定]"))
    print(dump(raw, 90, 98,  "騎手名SJIS[実測確定]"))
    print(dump(raw, 98, 104, "調教師CD[実測確定]"))
    print(dump(raw, 104,124, "調教師名SJIS[実測確定]"))

    # 不明ゾーン (76-84, 82-86)
    print()
    print(dump(raw, 76, 78,  "?? [76:78]"))
    print(dump(raw, 79, 80,  "?? [79:80]"))
    print(dump(raw, 82, 84,  "?? [82:84]"))

    # 斤量候補: 調教師名直後 [124:135]
    print()
    print("  -- 斤量候補 (調教師名直後) --")
    for s in range(124, 135):
        e = s + 3
        if e <= len(raw):
            val_str = asc(raw[s:e])
            try:
                v = int(val_str)
                plausible = "★ 斤量候補" if 480 <= v <= 640 else ""
            except ValueError:
                plausible = ""
            print(f"  [{s}:{e}] {val_str!r:10s} {plausible}")

    # 着順候補: 旧211付近を中心に ±20バイト
    print()
    print("  -- 着順候補 (旧211±20) --")
    for s in range(190, 225, 2):
        e = s + 2
        if e <= len(raw):
            val_str = asc(raw[s:e])
            try:
                v = int(val_str)
                plausible = "★ 着順候補" if 1 <= v <= 18 else ""
            except ValueError:
                plausible = ""
            print(f"  [{s}:{e}] {val_str!r:8s} {plausible}")

    # 全体ビュー: [76:260]
    print()
    print("  -- 全体ビュー [76:260] (ASCII表示) --")
    for s in range(76, min(260, len(raw)), 16):
        e = min(s + 16, len(raw))
        chunk = raw[s:e]
        print(f"  [{s:3d}] {asc(chunk)!r}")


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

    samples: list[bytes] = []
    TARGET = 5

    print("JVLinkからSEレコードを収集中...")
    with JVLinkClient(sid) as client:
        ret = client.open("RACE", "20260501", OPT_NORMAL)
        print(f"JVOpen ret={ret}")
        if ret < 0:
            print("JVOpen失敗"); return

        for _ in range(5_000_000):
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1); continue
            if code < 0:
                break
            if not data or len(data) < 200:
                continue
            if data[:2].decode("ascii", errors="replace") == "SE":
                samples.append(data)
                if len(samples) >= TARGET:
                    break

    print(f"\n取得 SE={len(samples)} 件")
    for i, raw in enumerate(samples):
        analyze(raw, i + 1)


if __name__ == "__main__":
    main()
