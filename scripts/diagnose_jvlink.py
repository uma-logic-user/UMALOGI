"""
JVLink から実際に何のレコードが来ているかを診断するスクリプト。
OPT_NORMAL で 20250101 から読み、最初の1000レコードのレコード種別・data_cat を集計する。

実行: py -3.14-32 scripts/diagnose_jvlink.py
"""
from __future__ import annotations
import os, sys, time
from collections import defaultdict
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

MAX_RECORDS = 5000   # 最大読み取りレコード数
HR_SAMPLES  = 3       # HRレコードのサンプル数（バイト構造確認用）


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

    # (rec_type, data_cat) → カウント
    counts: dict[tuple[str, str], int] = defaultdict(int)
    hr_samples: list[bytes] = []
    se_samples: list[bytes] = []
    total = 0

    print(f"JVLink診断: OPT_NORMAL, fromtime=20250101 ...")

    with JVLinkClient(sid) as client:
        ret = client.open("RACE", "20250101", OPT_NORMAL)
        print(f"JVOpen ret={ret}")
        if ret < 0:
            print("JVOpen失敗"); return

        for _ in range(10_000_000):
            code, data = client.read_record()
            if code == JVREAD_EOF:
                print(f"EOF ({total}レコード)")
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1); continue
            if code < 0:
                print(f"エラーコード: {code}"); break
            if not data or len(data) < 3:
                continue

            rec_type = data[:2].decode("ascii", errors="replace")
            data_cat = chr(data[2])
            counts[(rec_type, data_cat)] += 1
            total += 1

            if rec_type == "HR" and len(hr_samples) < HR_SAMPLES:
                hr_samples.append(data)
            if rec_type == "SE" and len(se_samples) < HR_SAMPLES:
                se_samples.append(data)

            if total >= MAX_RECORDS:
                print(f"MAX_RECORDS={MAX_RECORDS}に達して打ち切り")
                break

    print(f"\n== 読み取り結果 ({total}レコード) ==")
    print(f"{'種別':>6}  {'cat':>3}  {'件数':>8}")
    print("-" * 30)
    for (rt, cat), cnt in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {rt:>4}  cat={cat}  {cnt:>8,}")

    if not hr_samples:
        print("\nHRレコードが0件 → 払戻データなし")
        print("原因候補:")
        print("  1) 2025年確定データの配信期限切れ（JRA-VAN）")
        print("  2) TARGET frontier JV にデータが蓄積されていない")
        print("  3) fromtime より古いデータはサーバーに存在しない")
    else:
        print(f"\n== HR サンプル {len(hr_samples)} 件（バイト構造確認）==")
        for i, hr in enumerate(hr_samples):
            race_id = _make_race_id(hr)
            cat = chr(hr[2])
            print(f"\n[HR #{i+1}] race_id={race_id} cat={cat} len={len(hr)}")
            # offset 25-40 周辺を表示（払戻先頭部）
            segment = hr[20:60]
            asc = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in segment)
            print(f"  bytes[20:60] = {asc!r}")
            # 単勝払戻の確認
            try:
                combo = hr[27:29].decode("ascii", "replace").strip()
                amount_str = hr[29:34].decode("ascii", "replace").strip()
                amount = int(amount_str) if amount_str else 0
                print(f"  単勝組合せ[27:29]={combo!r} 払戻[29:34]={amount_str!r} → ¥{amount:,}")
            except Exception as e:
                print(f"  払戻解析エラー: {e}")

    if se_samples:
        print(f"\n== SE サンプル {len(se_samples)} 件（オフセット確認）==")
        for i, se in enumerate(se_samples):
            cat  = chr(se[2])
            race_id = _make_race_id(se)
            uma_ban_raw = se[28:30].decode("ascii", "replace")
            rank_211    = se[211:213].decode("ascii", "replace") if len(se) >= 213 else "??"
            print(f"[SE #{i+1}] cat={cat} race={race_id} len={len(se)}")
            print(f"  馬番[28:30]={uma_ban_raw!r}  rank@211={rank_211!r}")
            # 200-230 周辺のASCIIを表示
            seg = se[200:240] if len(se) >= 240 else se[200:]
            asc = "".join(chr(b) if 0x20 <= b < 0x7F else "·" for b in seg)
            print(f"  bytes[200:240] = {asc!r}")


if __name__ == "__main__":
    main()
