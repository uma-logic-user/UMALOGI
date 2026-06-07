"""W-076: 既存 race_results / entries への騎手・調教師コードのバックフィル（一時運用）。

SE レコード(RACE dataspec)を再読込し、(race_id, horse_number) をキーに
jockey_code / trainer_code のみを UPDATE する（他カラムは触らない＝低リスク・高速）。
氏名は SE 8バイト切り詰め・文字化けでマスタ結合できないため、コードを直接充填する。
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
    _parse_se,
)

DB = str(_ROOT / "data" / "umalogi.db")


def _coverage(conn: sqlite3.Connection, tag: str) -> None:
    cur = conn.cursor()
    tot = cur.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    jc = cur.execute(
        "SELECT COUNT(*) FROM race_results WHERE jockey_code IS NOT NULL"
    ).fetchone()[0]
    tc = cur.execute(
        "SELECT COUNT(*) FROM race_results WHERE trainer_code IS NOT NULL"
    ).fetchone()[0]
    jj = cur.execute(
        "SELECT COUNT(*) FROM race_results rr JOIN jockeys j "
        "ON j.jockey_code = rr.jockey_code WHERE rr.jockey_code IS NOT NULL"
    ).fetchone()[0]
    print(
        f"[{tag}] rows={tot:,} jockey_code充填={jc:,} trainer_code充填={tc:,} "
        f"jockeysマスタ結合={jj:,}"
    )


def main() -> None:
    fromtime = sys.argv[1] if len(sys.argv) > 1 else "20240101"
    # option: 1=NORMAL(差分・古いデータ非配信) / 2=SETUP(サーバー全量) / 4=STORED(ローカルキャッシュ)
    # 歴史データ(2024等)は NORMAL では配信されないため STORED/SETUP を使う。
    option = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    print(f"=== SE コード backfill 開始 fromtime={fromtime} option={option} ===")
    conn = sqlite3.connect(DB)
    _coverage(conn, "before")
    t0 = time.time()
    n_se = n_upd = 0
    with JVLinkClient("UMALOGI00") as client:
        if client.open("RACE", fromtime, option) < 0:
            print("JVOpen 失敗")
            return
        batch = 0
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
            if not data or data[:2] != b"SE":
                continue
            rec = _parse_se(data)
            if not rec or not rec.get("horse_number"):
                continue
            n_se += 1
            jc = rec.get("jockey_code")
            tc = rec.get("trainer_code")
            if jc is None and tc is None:
                continue
            cur = conn.execute(
                "UPDATE race_results SET "
                "jockey_code = COALESCE(?, jockey_code), "
                "trainer_code = COALESCE(?, trainer_code) "
                "WHERE race_id = ? AND horse_number = ?",
                (jc, tc, rec["race_id"], rec["horse_number"]),
            )
            conn.execute(
                "UPDATE entries SET "
                "jockey_code = COALESCE(?, jockey_code), "
                "trainer_code = COALESCE(?, trainer_code) "
                "WHERE race_id = ? AND horse_number = ?",
                (jc, tc, rec["race_id"], rec["horse_number"]),
            )
            n_upd += cur.rowcount
            batch += 1
            if batch >= 500:
                conn.commit()
                batch = 0
    conn.commit()
    print(
        f"SE={n_se:,} 件処理 / race_results更新={n_upd:,}  所要 {time.time() - t0:.1f}s"
    )
    _coverage(conn, "after")
    conn.close()


if __name__ == "__main__":
    main()
