"""
SE レコード全フィールドスキャン — rank 候補オフセット特定

cat='7' SE レコードの全バイトを走査し、値 1-18 が出現する ASCII 2 桁
オフセットをリストアップして rank 候補を絞り込む。

py -3.14-32 scripts/probe_se_full.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from src.scraper.jravan_client import (
    JVLinkClient, OPT_SETUP, DATASPEC_RACE,
    JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING,
    _make_race_id, _sjis, _int,
)

def main() -> None:
    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: JRAVAN_SID 未設定", flush=True); sys.exit(1)

    samples: list[tuple[str, int, bytes]] = []

    with JVLinkClient(sid) as client:
        code = client.open(DATASPEC_RACE, "20260426000000", OPT_SETUP)
        print(f"JVOpen code={code}", flush=True)
        if code < 0:
            return
        while len(samples) < 60:
            code, data = client.read_record()
            if code == JVREAD_EOF: break
            if code in (JVREAD_FILECHANGE, JVREAD_DOWNLOADING): continue
            if code < 0 or not data: continue
            raw = data if isinstance(data, bytes) else data.encode("latin-1", errors="replace")
            if raw[:2].decode("ascii", errors="replace") != "SE":
                continue
            rid = _make_race_id(raw)
            uma = _int(raw, slice(28, 30))
            samples.append((rid, uma, raw))

    print(f"取得 SE 件数: {len(samples)}", flush=True)
    if not samples:
        return

    # cat 分布
    cats: dict[str, int] = {}
    for _, _, r in samples:
        c = r[2:3].decode("ascii", errors="replace")
        cats[c] = cats.get(c, 0) + 1
    print(f"cat 分布: {cats}", flush=True)

    # horse_number(馬番) が既知なので、record 内で horse_number と同じ値が
    # rank 候補として出現するオフセットを集計する
    # ただし rank != uma_ban のはず → rank=1 の馬は uma_ban が何かを調べる

    # まず horse_name と uma_ban をリストアップ
    print("\n--- サンプル10件 ---")
    for rid, uma, raw in samples[:10]:
        cat = raw[2:3].decode("ascii", errors="replace")
        nm  = _sjis(raw, slice(40, 76)).strip()
        print(f"  {rid} 馬番{uma:02d} cat={cat} 馬名={nm}")

    # R1のサンプルを拾って全オフセット 1-18 出現位置を列挙
    r1_samples = [(rid, uma, raw) for rid, uma, raw in samples if rid.endswith("01")][:3]
    if not r1_samples:
        r1_samples = samples[:3]

    print("\n--- R1 サンプルの 1-18 出現オフセット (offset+2バイト ASCII) ---")
    for rid, uma, raw in r1_samples:
        nm = _sjis(raw, slice(40, 76)).strip()
        hits = []
        for i in range(27, min(420, len(raw) - 1)):
            chunk = raw[i:i+2]
            try:
                v = int(chunk.decode("ascii", errors="replace").strip())
                if 1 <= v <= 18:
                    hits.append((i, v))
            except (ValueError, UnicodeDecodeError):
                pass
        print(f"  {rid} 馬番{uma:02d} {nm}: {hits[:30]}")

    # 同一レースで全出走馬の「オフセット→値」マップを作り、
    # offset が全馬で 1..n (n=出走頭数) になる場所を rank オフセット候補とする
    print("\n--- 同一レースで rank らしいオフセット候補 ---")
    # 1レースを選んで全出走馬を集める
    first_rid = samples[0][0]
    race_samps = [(uma, raw) for rid, uma, raw in samples if rid == first_rid]

    if len(race_samps) >= 3:
        # 各 offset で全馬の値を確認
        offset_vals: dict[int, list[int]] = {}
        for uma, raw in race_samps:
            for i in range(27, min(420, len(raw) - 1)):
                chunk = raw[i:i+2]
                try:
                    v = int(chunk.decode("ascii", errors="replace").strip())
                    if 0 <= v <= 20:
                        offset_vals.setdefault(i, []).append(v)
                except (ValueError, UnicodeDecodeError):
                    pass

        n_horses = len(race_samps)
        print(f"  レース {first_rid} 出走頭数: {n_horses}")
        for off, vals in sorted(offset_vals.items()):
            if len(vals) == n_horses:
                unique = set(vals)
                if len(unique) >= max(2, n_horses // 2):
                    print(f"  offset={off:3d}: {sorted(vals)}  ← rank候補（ばらつきあり）")


if __name__ == "__main__":
    main()
