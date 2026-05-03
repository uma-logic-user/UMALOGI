"""
SEレコードの生バイト構造を解析してフィールド境界を特定する。

戦略:
  1. JVLinkからSEレコードを取得
  2. 確定済みフィールド（騎手コード・調教師名など）の位置を実測
  3. 各フィールドをSJIS/ASCIIデコードして構造を確認
  4. 着順の正しいオフセットを計算

実行: py -3.14-32 scripts/hexdump_se_fields.py
"""
from __future__ import annotations
import os, sys, time
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


def decode_sjis(b: bytes) -> str:
    try:
        return b.decode("cp932", errors="replace").replace("\x00", "").strip()
    except Exception:
        return repr(b)


def decode_ascii(b: bytes) -> str:
    return b.decode("ascii", errors="replace").replace("\x00", "").strip()


def hexdump_region(raw: bytes, start: int, end: int, label: str = "") -> None:
    region = raw[start:end]
    asc = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in region)
    hex_str = " ".join(f"{b:02x}" for b in region)
    print(f"  [{start:3d}:{end:3d}] {label:<25s} hex={hex_str}")
    print(f"           {'':25s} asc={asc!r}")


def analyze_se(raw: bytes, se_index: int) -> None:
    cat = chr(raw[2])
    race_id = _make_race_id(raw)
    print(f"\n{'='*70}")
    print(f"SE #{se_index}  race_id={race_id}  cat={cat}  len={len(raw)}")
    print(f"{'='*70}")

    # ── 確定済みフィールド ──────────────────────────────────────
    hexdump_region(raw, 27, 28,  "枠番 [確定]")
    hexdump_region(raw, 28, 30,  "馬番 [確定]")
    hexdump_region(raw, 30, 40,  "血統ID [確定]")
    hexdump_region(raw, 40, 76,  "馬名SJIS [確定]")
    hexdump_region(raw, 76, 77,  "性別 [確定]")
    hexdump_region(raw, 77, 79,  "馬齢 [確定]")
    hexdump_region(raw, 79, 84,  "騎手コード [確定]")
    hexdump_region(raw, 84, 104, "騎手名SJIS [確定]")

    # ── 推定フィールド ──────────────────────────────────────────
    hexdump_region(raw, 104, 105, "負担重量区分? [推定]")
    hexdump_region(raw, 105, 108, "斤量×10 [推定]")
    hexdump_region(raw, 108, 113, "調教師コード [推定]")
    hexdump_region(raw, 113, 133, "調教師名SJIS [推定]")

    # 133以降: 馬主・生産者情報と推定
    hexdump_region(raw, 133, 138, "馬主コード? [推定]")
    hexdump_region(raw, 138, 178, "馬主名SJIS? [推定]")
    hexdump_region(raw, 178, 183, "生産者コード? [推定]")
    hexdump_region(raw, 183, 223, "生産者名SJIS? [推定]")
    hexdump_region(raw, 223, 225, "産地コード? [推定]")

    # 着順候補領域
    print("\n  -- 着順候補領域 [225+] --")
    for off in [225, 227, 229, 233, 235, 237, 241, 243, 245]:
        if off + 4 <= len(raw):
            hexdump_region(raw, off, off+4, f"offset {off}")

    # 既存オフセット (現在の推定値)
    print("\n  -- 現在の推定値との比較 --")
    hexdump_region(raw, 211, 213, "_SE_RANK (現在)")
    hexdump_region(raw, 213, 218, "_SE_WIN_ODDS (現在)")
    hexdump_region(raw, 218, 220, "_SE_POPULARITY (現在)")
    hexdump_region(raw, 220, 224, "_SE_FINISH_T (現在)")
    hexdump_region(raw, 229, 232, "_SE_HORSE_WT (現在)")

    # 全体概観: 64バイト単位で
    print("\n  -- 全体 (64バイト単位) --")
    total = len(raw)
    for start in range(0, total, 64):
        chunk = raw[start:start+64]
        asc = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in chunk)
        print(f"  [{start:3d}] {asc!r}")


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

    se_samples: list[bytes] = []
    hr_samples: list[bytes] = []
    TARGET_SE = 3

    print("JVLinkからSEレコードを収集中 (OPT_NORMAL)...")

    with JVLinkClient(sid) as client:
        ret = client.open("RACE", "20250101", OPT_NORMAL)
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
            if not data or len(data) < 50:
                continue

            rec_type = data[:2].decode("ascii", errors="replace")
            data_cat = chr(data[2])

            if rec_type == "SE" and len(se_samples) < TARGET_SE:
                se_samples.append(data)
            if rec_type == "HR" and len(hr_samples) < TARGET_SE:
                hr_samples.append(data)

            if len(se_samples) >= TARGET_SE and len(hr_samples) >= TARGET_SE:
                break

    print(f"\n取得: SE={len(se_samples)}, HR={len(hr_samples)}")

    for i, se in enumerate(se_samples):
        analyze_se(se, i + 1)

    if hr_samples:
        print("\n\n=== HR サンプル（単勝1着馬番確認） ===")
        for i, hr in enumerate(hr_samples):
            race_id = _make_race_id(hr)
            cat = chr(hr[2])
            print(f"HR #{i+1} race_id={race_id} cat={cat}")
            hexdump_region(hr, 27, 29, "単勝combo[27:29]")
            hexdump_region(hr, 29, 34, "単勝払戻[29:34]")


if __name__ == "__main__":
    main()
