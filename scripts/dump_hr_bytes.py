"""
2025年HRレコードの実際のバイト構造をダンプする診断スクリプト。
実行: py -3.14-32 scripts/dump_hr_bytes.py

JVLink OPT_NORMAL で接続し、最初の完全なHRレコード(単勝有効データ付き)を取得。
全セクションのバイト列をASCIIで出力して正しいスペックを確認する。
"""
from __future__ import annotations
import os, sys, time
sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)

from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import JVLinkClient, OPT_NORMAL, OPT_STORED, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING


def hex_line(data: bytes, offset: int = 0, width: int = 16) -> None:
    for i in range(0, len(data), width):
        chunk = data[i:i+width]
        hex_p = " ".join(f"{b:02X}" for b in chunk)
        asc_p = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
        print(f"  [{offset+i:4d}] {hex_p:<{width*3-1}}  {asc_p}")


def try_parse(data: bytes, specs: list, label: str) -> None:
    """各スペックでオフセット27から手動パース。"""
    print(f"\n--- {label} ---")
    offset = 27
    AMOUNT_BYTES = 5
    valid_total = 0
    for (bet_type, n_ent, cb, pb) in specs:
        ent_len = cb + AMOUNT_BYTES + pb
        section_data = data[offset: offset + n_ent * ent_len]
        valid = []
        for i in range(n_ent):
            e = section_data[i*ent_len:(i+1)*ent_len]
            if len(e) < ent_len:
                break
            combo_raw  = e[:cb]
            amount_raw = e[cb:cb+AMOUNT_BYTES]
            pop_raw    = e[cb+AMOUNT_BYTES:ent_len]
            combo_s  = combo_raw.decode("ascii", "replace")
            amount_s = amount_raw.decode("ascii", "replace")
            pop_s    = pop_raw.decode("ascii", "replace")
            try:
                amt = int(amount_s.strip())
            except Exception:
                amt = -1
            if amt > 0:
                valid.append(f"[{combo_s}]=¥{amt:,}(pop={pop_s.strip()})")
        valid_total += len(valid)
        print(f"  {bet_type:5s} n={n_ent} ent={ent_len}b off={offset}: {valid if valid else '(none)'}")
        offset += n_ent * ent_len
    print(f"  有効計={valid_total}  最終offset={offset} record_len={len(data)}")


def main() -> None:
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text("utf-8", errors="replace").splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("JRAVAN_SID 未設定"); sys.exit(1)

    # まず OPT_STORED で試す
    for opt_label, opt in [("OPT_STORED", OPT_STORED), ("OPT_NORMAL", OPT_NORMAL)]:
        print(f"\n{'='*70}")
        print(f"接続オプション: {opt_label}")
        found: list[bytes] = []
        with JVLinkClient(sid) as client:
            rc = client.open("RACE", "20250101000000", opt)
            if rc < 0:
                print(f"JVOpen失敗 code={rc}"); continue

            scan = 0
            for _ in range(1_000_000):
                code, data = client.read_record()
                if code == JVREAD_EOF: break
                if code == JVREAD_FILECHANGE: continue
                if code == JVREAD_DOWNLOADING: time.sleep(1); continue
                if code < 0: break
                if not data or len(data) < 35: continue
                if data[:2].decode("ascii", "replace") != "HR": continue
                scan += 1
                # 単勝に有効払戻があるか (offset 29-33)
                try:
                    tan = int(data[29:34])
                    if tan > 0:
                        found.append(data)
                        race_id = data[11:27].decode("ascii", "replace")
                        cat = chr(data[2])
                        print(f"HR発見 #{len(found)} scan={scan} race={race_id} cat={cat} len={len(data)} TAN_AMT={tan}")
                        if len(found) >= 3:
                            break
                except Exception:
                    pass

        print(f"スキャン: {scan} HR   発見: {len(found)}")
        if not found:
            print("→ このオプションではHRレコードなし")
            continue

        data = found[0]
        race_id = data[11:27].decode("ascii", "replace")
        print(f"\n=== 最初のHRレコード詳細 race={race_id} len={len(data)} ===")

        # オフセット27から全体をASCIIで表示
        print("\n[raw ASCII offset 0-80]")
        hex_line(data[0:80], 0)
        print("\n[raw ASCII offset 80-160]")
        hex_line(data[80:160], 80)
        print("\n[raw ASCII offset 160-240]")
        hex_line(data[160:240], 160)
        print("\n[raw ASCII offset 240-430]")
        hex_line(data[240:430], 240)

        # 4スペックを比較
        SPEC_OFFICIAL = [
            # (label, n_entries, combo_bytes, pop_bytes)
            # AMOUNT_BYTES は常に5 (定数)
            ("単勝",   3, 2, 2),
            ("複勝",   5, 2, 2),
            ("枠連",   3, 2, 2),  # combo_bytes=2 (1桁×2枠)
            ("馬連",   3, 4, 2),
            ("ワイド", 7, 4, 2),
            ("馬単",   6, 4, 2),
            ("三連複", 3, 6, 3),  # pop=3
            ("三連単", 6, 6, 3),  # pop=3
        ]
        SPEC_COMBO4 = [
            ("単勝",   3, 2, 2),
            ("複勝",   5, 2, 2),
            ("枠連",   3, 4, 2),  # combo=4
            ("馬連",   3, 4, 2),
            ("ワイド", 7, 4, 2),
            ("馬単",   6, 4, 2),
            ("三連複", 3, 6, 2),  # pop=2
            ("三連単", 6, 6, 2),  # pop=2
        ]
        SPEC_OFF2 = [  # 2バイトオフセット遅れ (offset 29 から)
            ("単勝",   3, 2, 2),
            ("複勝",   5, 2, 2),
            ("枠連",   3, 2, 2),
            ("馬連",   3, 4, 2),
            ("ワイド", 7, 4, 2),
            ("馬単",   6, 4, 2),
            ("三連複", 3, 6, 3),
            ("三連単", 6, 6, 3),
        ]

        try_parse(data, SPEC_OFFICIAL, "SPEC_OFFICIAL: 枠連combo=2 三連pop=3 offset=27")
        try_parse(data, SPEC_COMBO4,   "SPEC_COMBO4: 枠連combo=4 三連pop=2 offset=27")
        # offset 29 版
        data_shifted = data[2:]  # 2バイト前にシフト → 実質offset=25から
        # just show offset 25-27
        print(f"\n  byte[27]={data[27]:02X}({chr(data[27])}) "
              f"byte[28]={data[28]:02X}({chr(data[28])}) "
              f"byte[29]={data[29]:02X}({chr(data[29])}) "
              f"byte[30]={data[30]:02X}({chr(data[30])}) "
              f"byte[31]={data[31]:02X}({chr(data[31])})")

        break  # 最初の有効オプションのみ処理


if __name__ == "__main__":
    main()
