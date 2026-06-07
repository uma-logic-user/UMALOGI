"""W-074: UM パーサ是正後の競走馬マスタ再取り込み（一時運用スクリプト）。

修正済み _parse_um で racehorses を正しいデータ（horse_id 正規化・生年月日・毛色・
血統）に再構築する。KS/CH は本修正対象外（既知の破損を維持・W-075）。
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

from src.scraper.jravan_client import JVDataLoader, OPT_NORMAL  # noqa: E402

DB = str(_ROOT / "data" / "umalogi.db")


def _snapshot(tag: str) -> None:
    con = sqlite3.connect(DB)
    cur = con.cursor()
    tot = cur.execute("SELECT COUNT(*) FROM racehorses").fetchone()[0]
    by = cur.execute(
        "SELECT COUNT(*) FROM racehorses WHERE birth_year IS NOT NULL"
    ).fetchone()[0]
    coat = cur.execute(
        "SELECT COUNT(*) FROM racehorses WHERE coat_color<>''"
    ).fetchone()[0]
    join = cur.execute(
        "SELECT COUNT(DISTINCT rr.horse_id) FROM race_results rr "
        "JOIN racehorses um ON um.horse_id=rr.horse_id WHERE rr.horse_id IS NOT NULL"
    ).fetchone()[0]
    print(
        f"[{tag}] racehorses={tot} birth_year充填={by} coat充填={coat} "
        f"race_results結合={join}"
    )
    con.close()


def main() -> None:
    fromtime = sys.argv[1] if len(sys.argv) > 1 else "20230101"
    print(f"=== UM 再取り込み開始 fromtime={fromtime} option=NORMAL ===")
    _snapshot("before")
    t0 = time.time()
    loader = JVDataLoader(sid="UMALOGI00")
    stats = loader.load_difn(fromtime, OPT_NORMAL)
    print(f"取り込み統計: {stats}")
    print(f"所要: {time.time() - t0:.1f}s")
    _snapshot("after")


if __name__ == "__main__":
    main()
