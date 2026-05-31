"""
SE レコードの rank フィールドオフセット診断スクリプト

JVLink から SE レコードを読み取り、実際のランクデータが
どのバイト位置にあるかを特定する。

実行コマンド (32bit Python 必須):
    py -3.14-32 scripts/probe_se_rank.py
"""

from __future__ import annotations

import os
import sys
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
    JVLinkClient,
    OPT_SETUP,
    DATASPEC_RACE,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING,
    _int,
    _make_race_id,
    _sjis,
)

# 検証対象：天皇賞(春) 202608030411
TARGET_RACE = "202608030411"

# 既知の着順付きレース（5/3 R1など早い回は確定済みの可能性）
TARGET_RACES = {
    "202608030101",
    "202608030102",
    "202608030103",
    "202604010101",
    "202604010102",
    "202604010103",
    TARGET_RACE,
}


def main() -> None:
    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: JRAVAN_SID 未設定", flush=True)
        sys.exit(1)

    found: list[tuple[str, int, bytes]] = []

    with JVLinkClient(sid) as client:
        code = client.open(DATASPEC_RACE, "20260426000000", OPT_SETUP)
        print(f"JVOpen code={code}", flush=True)
        if code < 0:
            print("JVOpen 失敗", flush=True)
            return

        while len(found) < 20:
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code in (JVREAD_FILECHANGE, JVREAD_DOWNLOADING):
                continue
            if code < 0 or not data:
                continue

            raw = (
                data
                if isinstance(data, bytes)
                else data.encode("latin-1", errors="replace")
            )
            rec_type = raw[:2].decode("ascii", errors="replace")
            if rec_type != "SE":
                continue

            race_id = _make_race_id(raw)
            uma_ban = _int(raw, slice(28, 30))
            cat = raw[2:3].decode("ascii", errors="replace")

            found.append((race_id, uma_ban, raw))

    print(f"\n取得 SE レコード数: {len(found)}", flush=True)
    if not found:
        print("SE レコードなし", flush=True)
        return

    # 最初の SE レコードを詳細ダンプ
    race_id, uma_ban, raw = found[0]
    cat = raw[2:3].decode("ascii", errors="replace")
    print(
        f"\n--- SE サンプル: race_id={race_id} 馬番={uma_ban} cat={cat} len={len(raw)} ---"
    )
    horse_name = _sjis(raw, slice(40, 76)).strip()
    print(f"馬名: {horse_name}")

    # オフセット 190-230 の値を scan して rank 候補を探す
    print("\n[オフセット 190-230 バイトダンプ]")
    for i in range(190, min(230, len(raw))):
        chunk = raw[i : i + 2]
        try:
            val = int(chunk.decode("ascii", errors="replace").strip())
            if 1 <= val <= 18:
                print(f"  offset {i:3d}: {chunk!r} = {val}  ← rank候補")
            else:
                print(f"  offset {i:3d}: {chunk!r} = {val}")
        except ValueError:
            print(f"  offset {i:3d}: {chunk!r} (非数値)")

    # TARGET_RACE の SE を検索
    target_records = [(rid, ub, r) for rid, ub, r in found if rid == TARGET_RACE]
    if target_records:
        print(f"\n--- 天皇賞(春) {TARGET_RACE} SE ---")
        for race_id, uma_ban, raw in target_records[:3]:
            cat = raw[2:3].decode("ascii", errors="replace")
            horse_name = _sjis(raw, slice(40, 76)).strip()
            print(f"  馬番={uma_ban} {horse_name} cat={cat}")
            # rank 候補オフセット
            for off in [202, 206, 210, 211, 213, 215]:
                chunk = raw[off : off + 2]
                try:
                    v = int(chunk.decode("ascii", errors="replace").strip())
                    print(f"    off={off}: {v}")
                except Exception:
                    print(f"    off={off}: {chunk!r}")

    # cat 分布確認
    cats = {}
    for _, _, r in found:
        c = r[2:3].decode("ascii", errors="replace")
        cats[c] = cats.get(c, 0) + 1
    print(f"\ncat 分布: {cats}")


if __name__ == "__main__":
    main()
