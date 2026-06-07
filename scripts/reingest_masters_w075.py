"""W-075: KS(騎手)/CH(調教師)マスタ パーサ是正後の再取り込み（一時運用）。

DIFN ストリームから KS/CH レコードのみを保存する（SE/RA/UM 等はスキップして高速化）。
UM 再取り込みは W-074 の reingest_um_w074.py で別途実施済み。
"""

from __future__ import annotations

import io
import sqlite3
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from src.scraper.jravan_client import (  # noqa: E402
    JVLinkClient,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING,
    DATASPEC_DIFN,
    _parse_ks,
    _parse_ch,
    _save_ks,
    _save_ch,
)

DB = str(_ROOT / "data" / "umalogi.db")


def _join_rate(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    jk = cur.execute(
        "SELECT COUNT(DISTINCT rr.jockey) FROM race_results rr "
        "JOIN jockeys j ON j.jockey_name = rr.jockey WHERE rr.jockey<>''"
    ).fetchone()[0]
    jk_tot = cur.execute(
        "SELECT COUNT(DISTINCT jockey) FROM race_results WHERE jockey<>''"
    ).fetchone()[0]
    ch = cur.execute(
        "SELECT COUNT(DISTINCT rr.trainer) FROM race_results rr "
        "JOIN trainers t ON t.trainer_name = rr.trainer WHERE rr.trainer<>''"
    ).fetchone()[0]
    ch_tot = cur.execute(
        "SELECT COUNT(DISTINCT trainer) FROM race_results WHERE trainer<>''"
    ).fetchone()[0]
    print(f"  騎手名 結合: {jk}/{jk_tot} distinct")
    print(f"  調教師名 結合: {ch}/{ch_tot} distinct")


def main() -> None:
    fromtime = sys.argv[1] if len(sys.argv) > 1 else "20250101"
    print(f"=== KS/CH 再取り込み開始 fromtime={fromtime} ===")
    conn = sqlite3.connect(DB)
    print("[before]")
    _join_rate(conn)
    t0 = time.time()
    n_ks = n_ch = 0
    with JVLinkClient("UMALOGI00") as client:
        if client.open(DATASPEC_DIFN, fromtime, 1) < 0:
            print("JVOpen 失敗")
            return
        while True:
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(1)
                continue
            if code < 0:
                print(f"JVRead エラー: {code}")
                break
            if not data:
                continue
            rt = data[:2]
            if rt == b"KS":
                rec = _parse_ks(data)
                if rec:
                    _save_ks(conn, rec)
                    n_ks += 1
            elif rt == b"CH":
                rec = _parse_ch(data)
                if rec:
                    _save_ch(conn, rec)
                    n_ch += 1
            # SE/RA/UM 等はスキップ
    print(f"取り込み: KS={n_ks} CH={n_ch}  所要 {time.time() - t0:.1f}s")
    print("[after]")
    _join_rate(conn)
    conn.close()


if __name__ == "__main__":
    main()
