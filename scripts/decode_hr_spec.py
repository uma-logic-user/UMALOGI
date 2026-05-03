"""
HRレコードの構造をJV-Data 4.5.2仕様に照らして確定する。
hexdump_hr_full.py で取得した実データを使い、全券種のcombo_bytes・entry_sizeを検証。

実行: py -3.14-32 scripts/decode_hr_spec.py
"""
from __future__ import annotations
import os, sys, time
sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)

from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import JVLinkClient, OPT_NORMAL, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING

# JV-Data 4.5.2 公式仕様に基づく候補
# 各賭式の (label, n_entries, combo_bytes, amount_bytes, pop_bytes)
# 枠連は combo=2 (枠番1桁×2) vs combo=4 (2桁×2) を両方試す

# 公式仕様 (JV-Data 4.5.2 HRレコード):
# 単勝: UMABAN(2) + HARAIMODOSHI(5) + NINKI(2) = 9 bytes × 3 = 27
# 複勝: UMABAN(2) + HARAIMODOSHI(5) + NINKI(2) = 9 bytes × 5 = 45
# 枠連: WAKUBAN(2) + HARAIMODOSHI(5) + NINKI(2) = 9 bytes × 3 = 27  ← combo=2
# 馬連: UMABAN(4) + HARAIMODOSHI(5) + NINKI(2) = 11 bytes × 3 = 33
# ワイド: UMABAN(4) + HARAIMODOSHI(5) + NINKI(2) = 11 bytes × 7 = 77
# 馬単: UMABAN(4) + HARAIMODOSHI(5) + NINKI(2) = 11 bytes × 6 = 66
# 三連複: UMABAN(6) + HARAIMODOSHI(5) + NINKI(3) = 14 bytes × 3 = 42
# 三連単: UMABAN(6) + HARAIMODOSHI(5) + NINKI(3) = 14 bytes × 6 = 84

SPEC_OFFICIAL = [
    # (label, n_entries, combo_bytes, amount_bytes, pop_bytes)
    ("単勝",   3, 2, 5, 2),
    ("複勝",   5, 2, 5, 2),
    ("枠連",   3, 2, 5, 2),  # ← 仕様書: 枠番1桁×2
    ("馬連",   3, 4, 5, 2),
    ("ワイド", 7, 4, 5, 2),
    ("馬単",   6, 4, 5, 2),
    ("三連複", 3, 6, 5, 3),  # ← pop=3桁
    ("三連単", 6, 6, 5, 3),  # ← pop=3桁
]

SPEC_CURRENT = [
    ("単勝",   3, 2, 5, 2),
    ("複勝",   5, 2, 5, 2),
    ("枠連",   3, 4, 5, 2),  # ← 現在: combo=4
    ("馬連",   3, 4, 5, 2),
    ("ワイド", 7, 4, 5, 2),
    ("馬単",   6, 4, 5, 2),
    ("三連複", 3, 6, 5, 2),  # ← 現在: pop=2
    ("三連単", 6, 6, 5, 2),
]


def decode_combo(raw: bytes, combo_bytes: int) -> str:
    try:
        s = raw.decode('ascii', 'replace')
    except Exception:
        return '??'
    return s


def parse_with_spec(data: bytes, spec: list, label: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  スペック: {label}")

    total = sum(n * (c+a+p) for _, n, c, a, p in spec)
    print(f"  合計期待バイト数: {total}  (実データ payload={len(data)-27-2} bytes)")

    offset = 27
    for bet_type, n_entries, combo_b, amount_b, pop_b in spec:
        entry_size = combo_b + amount_b + pop_b
        section_start = offset
        valid = []
        for i in range(n_entries):
            if offset + entry_size > len(data):
                break
            raw_combo  = data[offset : offset + combo_b]
            raw_amount = data[offset + combo_b : offset + combo_b + amount_b]
            raw_pop    = data[offset + combo_b + amount_b : offset + entry_size]

            combo_str  = decode_combo(raw_combo, combo_b)
            # Detect if this is a zeros-only entry (empty slot)
            is_digits  = all(0x30 <= b <= 0x39 for b in raw_amount)
            is_spaces  = all(b == 0x20 for b in raw_combo + raw_amount + raw_pop)
            if is_digits:
                amount = int(raw_amount)
            else:
                amount = -1

            if is_spaces:
                entry_desc = "SPACE(未使用)"
            elif amount > 0:
                try:
                    pop_val = int(raw_pop)
                except Exception:
                    pop_val = -1
                valid.append((combo_str, amount, pop_val))
                entry_desc = f"combo={combo_str!r} ¥{amount:,} pop={pop_val}"
            else:
                entry_desc = f"combo={combo_str!r} amount={raw_amount!r}(ZERO/invalid)"

            offset += entry_size

        section_raw = data[section_start:offset]
        hex_preview = " ".join(f"{b:02X}" for b in section_raw[:min(30, len(section_raw))])
        asc_preview = "".join(chr(b) if 0x20 <= b < 0x7F else '.' for b in section_raw[:min(30, len(section_raw))])
        if valid:
            print(f"  [{bet_type:4s}] ({n_entries}件 × {entry_size}bytes={n_entries*entry_size}) 有効{len(valid)}件 →",
                  " | ".join(f"combo={c} ¥{a:,}(pop={p})" for c, a, p in valid))
        else:
            print(f"  [{bet_type:4s}] ({n_entries}件 × {entry_size}bytes={n_entries*entry_size}) 有効なし  hex={hex_preview}")

    print(f"  offset終端: {offset}  (record長={len(data)})")


def is_good_hr(data: bytes) -> bool:
    """複数券種で有効払戻がある確定HRか判定"""
    if len(data) < 200:
        return False
    try:
        tan_amt = int(data[29:34].decode('ascii', 'strict'))
        if tan_amt <= 0:
            return False
    except Exception:
        return False
    later = data[100:400]
    digit_count = sum(1 for b in later if 0x31 <= b <= 0x39)
    return digit_count >= 20  # 複数券種で有効データがある


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
            if data[:2].decode('ascii', 'replace') != 'HR': continue
            scan += 1
            if is_good_hr(data):
                found.append(data)
                print(f"良好HR発見 #{len(found)} (scan={scan}, len={len(data)}, "
                      f"race={data[11:27].decode('ascii','replace')}, data_cat={chr(data[2])})")
                if len(found) >= 5:
                    break

    print(f"\n候補: {len(found)} / {scan}件")

    for idx, data in enumerate(found[:3]):
        print(f"\n{'='*70}")
        print(f"HR #{idx+1}  {len(data)} bytes  race={data[11:27].decode('ascii','replace')}  cat={chr(data[2])}")

        # ASCII全体を表示（有効データ確認）
        asc = "".join(chr(b) if 0x20 <= b < 0x7F else '·' for b in data)
        print(f"  offset 27-130: {asc[27:130]!r}")
        print(f"  offset 130-280: {asc[130:280]!r}")

        # 両スペックで比較
        parse_with_spec(data, SPEC_OFFICIAL, "JV-Data 4.5.2 公式(枠連combo=2,三連pop=3)")
        parse_with_spec(data, SPEC_CURRENT,  "現パーサー(枠連combo=4,三連pop=2)")


if __name__ == "__main__":
    main()
