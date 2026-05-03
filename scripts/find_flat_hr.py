"""
data_cat='1'(平地確定払戻) のHRレコードを探してバイト構造を確定する。
JV-Data 4.5.2の公式仕様と比較検証。

実行: py -3.14-32 scripts/find_flat_hr.py
"""
from __future__ import annotations
import os, sys, time
sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)

from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import JVLinkClient, OPT_NORMAL, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING


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

    found_by_cat: dict[str, list[bytes]] = {}

    with JVLinkClient(sid) as client:
        if client.open("RACE", "20250101000000", OPT_NORMAL) < 0:
            print("JVOpen失敗"); return

        scan = 0
        for _ in range(2000000):
            code, data = client.read_record()
            if code == JVREAD_EOF: break
            if code == JVREAD_FILECHANGE: continue
            if code == JVREAD_DOWNLOADING: time.sleep(1); continue
            if code < 0: break
            if not data or len(data) < 30: continue
            if data[:2].decode('ascii', 'replace') != 'HR': continue

            scan += 1
            cat = chr(data[2])
            race_id = data[11:27].decode('ascii', 'replace')

            # 単勝に有効払戻があるか確認
            try:
                tan_amt = int(data[29:34])
                if tan_amt <= 0: continue
            except Exception:
                continue

            # 複勝セクション (offset 54-99) に有効数字があるか
            fuk_section = data[54:99]
            fuk_digit_nonzero = sum(1 for b in fuk_section if 0x31 <= b <= 0x39)

            # 枠連セクション (offset 99-126) の候補チェック
            wak_section = data[99:162]
            wak_nonzero = sum(1 for b in wak_section if 0x31 <= b <= 0x39)

            if cat not in found_by_cat:
                found_by_cat[cat] = []

            if len(found_by_cat.get(cat, [])) < 3 and fuk_digit_nonzero >= 3:
                found_by_cat.setdefault(cat, []).append(data)
                print(f"cat={cat!r} race={race_id} len={len(data)} "
                      f"TAN=¥{tan_amt:,} FUK非零={fuk_digit_nonzero} WAK非零={wak_nonzero}")

            if all(len(v) >= 3 for v in found_by_cat.values()) and len(found_by_cat) >= 2:
                break

    print(f"\nスキャン: {scan}件  カテゴリ: {list(found_by_cat.keys())}")

    # 各カテゴリの先頭1件を詳しく解析
    for cat, records in found_by_cat.items():
        if not records:
            continue
        data = records[0]
        race_id = data[11:27].decode('ascii', 'replace')
        print(f"\n{'='*70}")
        print(f"data_cat={cat!r} race={race_id} len={len(data)}")

        # 全セクション表示
        asc = "".join(chr(b) if 0x20 <= b < 0x7F else '·' for b in data)
        for start in range(0, len(data), 100):
            print(f"  [{start:4d}] {asc[start:start+100]!r}")

        # パターン: 公式仕様 (枠連combo=2, 三連pop=3)
        specs = [
            ("単勝",   3, 2, 5, 2),
            ("複勝",   5, 2, 5, 2),
            ("枠連",   3, 2, 5, 2),
            ("馬連",   3, 4, 5, 2),
            ("ワイド", 7, 4, 5, 2),
            ("馬単",   6, 4, 5, 2),
            ("三連複", 3, 6, 5, 3),
            ("三連単", 6, 6, 5, 3),
        ]

        offset = 27
        print("\n  --- 公式スペック解析 ---")
        for bet_type, n_entries, cb, ab, pb in specs:
            es = cb + ab + pb
            section_bytes = data[offset:offset + n_entries * es]
            entries = []
            for i in range(n_entries):
                raw = section_bytes[i*es:(i+1)*es]
                if len(raw) < es: break
                c = raw[:cb]
                a = raw[cb:cb+ab]
                p = raw[cb+ab:es]
                try:
                    amt = int(a)
                    pop = int(p)
                    cstr = c.decode('ascii', 'replace').strip()
                except Exception:
                    amt = pop = -1; cstr = '?'
                is_sp = all(b == 0x20 for b in raw)
                is_zero = all(b == 0x30 for b in raw)
                if is_sp:
                    continue  # skip all-space
                if amt > 0 and cstr.strip('0'):
                    entries.append(f"{cstr}=¥{amt:,}(p{pop})")
            print(f"    [{bet_type:4s}] n={n_entries}×{es}b: {entries if entries else '(全ZERO/SPACE)'}")
            offset += n_entries * es
        print(f"  終端offset={offset} / {len(data)}")


if __name__ == "__main__":
    main()
