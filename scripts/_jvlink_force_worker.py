"""
JVLink ストリーミング強制インポート ワーカー  (32bit Python 専用)

【重要】このスクリプトは必ず 32bit Python で実行すること:
  py -3.14-32 scripts/_jvlink_force_worker.py --dataspec RACE --fromtime 20210101 --option 4

【設計思想: OOM 対策】
  通常の JVDataLoader.load() は全レコードをメモリに蓄積してから一括保存するため、
  5年分 (数十万レコード) の処理でメモリ枯渇が起きうる。
  このワーカーは JVREAD_FILECHANGE (-1) のタイミングでバッチ COMMIT することで、
  常にメモリを一定量に抑える。

  JVREAD_FILECHANGE は JV-Data のファイル境界シグナル。同一ファイル内では
  RA (レース) → SE (馬毎) の順序が保証されており FK 制約違反が起きない。
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

# Windows CP932 端末対策
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVREAD_DOWNLOADING,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVLinkClient,
    OPT_NORMAL,
    OPT_SETUP,
    OPT_STORED,
    parse_record,
    save_records_to_db,
)
from src.database.init_db import init_db


# ────────────────────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────────────────────

DEFAULT_SID        = "UMALOGI00"
DEFAULT_BATCH_SIZE = 5000   # この件数を超えたら強制コミット（安全ネット）


# ────────────────────────────────────────────────────────────────────────────
# メイン処理
# ────────────────────────────────────────────────────────────────────────────

def _print_stats(
    label: str,
    total: dict[str, int],
    batch: Optional[dict[str, int]] = None,
) -> None:
    if batch is not None:
        print(
            f"  [{label}] "
            f"RA={batch.get('ra',0)} SE={batch.get('se',0)} "
            f"payout={batch.get('payout',0)} TC={batch.get('tc',0)} "
            f"HC={batch.get('hc',0)} skip={batch.get('skipped',0)}"
        )
    print(
        f"  [累計] RA={total['ra']} SE={total['se']} payout={total['payout']} "
        f"TC={total['tc']} HC={total['hc']} skip={total['skipped']}"
    )


def run(
    dataspec: str,
    fromtime: str,
    option: int,
    batch_size: int,
    sid: str,
) -> int:
    """
    JVLink をストリーミング読み込みし、ファイル境界ごとにバッチ保存する。

    【トランザクション方針】
    save_records_to_db() 内の _save_* 関数はそれぞれ `with conn:` で
    レコード単位にコミットする。isolation_level を変更すると `with conn:`
    の動作が壊れ "cannot commit - no transaction is active" が発生する。
    よって手動 BEGIN/COMMIT は使わず、メモリ上限 (batch_size) に達したら
    save_records_to_db() を呼ぶことでリストを定期的にクリアするだけにする。

    Returns:
        0 = 成功, 1 = 致命的エラー
    """
    print(f"[worker] start dataspec={dataspec} fromtime={fromtime} option={option}")

    conn: sqlite3.Connection = init_db()

    pending:    list[dict] = []
    file_count: int = 0
    read_count: int = 0
    total: dict[str, int] = {
        "ra": 0, "jg": 0, "se": 0, "payout": 0,
        "tc": 0, "hc": 0, "bt": 0, "hn": 0,
        "um": 0, "ks": 0, "ch": 0, "skipped": 0,
    }

    def _flush_pending(label: str) -> None:
        """pending をDBに保存してクリアする。"""
        nonlocal pending
        if not pending:
            return
        batch_stats = save_records_to_db(pending, conn)
        for k in total:
            total[k] += batch_stats.get(k, 0)
        _print_stats(label, total, batch_stats)
        pending = []

    try:
        with JVLinkClient(sid) as client:
            open_code = client.open(dataspec, fromtime, option)
            print(f"[worker] JVOpen code={open_code}")
            if open_code < 0:
                print(f"[worker] JVOpen 失敗 code={open_code} → 終了")
                conn.close()
                return 0  # データなしは正常扱い（上位がスキップ判定）

            while True:
                code, data = client.read_record()

                if code == JVREAD_EOF:
                    # 末尾残分をフラッシュ
                    _flush_pending(f"FILE-{file_count}-EOF")
                    print(
                        f"[worker] EOF: read={read_count} files={file_count} "
                        f"RA={total['ra']} SE={total['se']} payout={total['payout']} "
                        f"TC={total['tc']} HC={total['hc']} skip={total['skipped']}"
                    )
                    break

                if code == JVREAD_FILECHANGE:
                    # ファイル境界: 同ファイル内は RA→SE 順が保証されるので安全にフラッシュ
                    file_count += 1
                    _flush_pending(f"FILE-{file_count}")
                    continue

                if code == JVREAD_DOWNLOADING:
                    time.sleep(1)
                    continue

                if code < 0:
                    print(f"[worker] JVRead エラー code={code} → 中断")
                    conn.close()
                    return 1

                if data:
                    rec = parse_record(data)
                    if rec:
                        pending.append(rec)
                read_count += 1

                # メモリ安全ネット: batch_size を超えたら強制フラッシュ
                if len(pending) >= batch_size:
                    _flush_pending(f"BATCH@{read_count}")

                if read_count % 1000 == 0:
                    print(
                        f"[worker] 読み込み中: {read_count} レコード "
                        f"(pending={len(pending)} file={file_count})"
                    )

    except Exception as exc:
        print(f"[worker] 例外: {exc}", file=sys.stderr)
        conn.close()
        return 1

    conn.close()
    return 0


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="JVLink ストリーミング強制インポートワーカー (32bit Python 専用)"
    )
    p.add_argument("--sid",        default=DEFAULT_SID)
    p.add_argument("--dataspec",   required=True,
                   choices=["RACE", "WOOD", "BLOD", "DIFN", "SETUP"])
    p.add_argument("--fromtime",   required=True,
                   metavar="YYYYMMDD",
                   help="取得開始日 (例: 20210101)")
    p.add_argument("--option",     type=int, default=OPT_NORMAL,
                   choices=[OPT_NORMAL, OPT_SETUP, OPT_STORED],
                   help="1=NORMAL(サーバー直取得) 2=SETUP 4=STORED(キャッシュ) (デフォルト: 1)")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                   help=f"強制コミット件数 (デフォルト: {DEFAULT_BATCH_SIZE})")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        run(
            dataspec   = args.dataspec,
            fromtime   = args.fromtime,
            option     = args.option,
            batch_size = args.batch_size,
            sid        = args.sid,
        )
    )
