"""
DB 最適化ユーティリティ（VACUUM / ANALYZE）

長期稼働による SQLite ファイルの肥大化・フラグメンテーション・統計情報の陳腐化を
解消するための保守処理。

処理の流れ:
    1. WAL チェックポイント (TRUNCATE)  … -wal を本体へ反映し空にする
    2. VACUUM                            … 全ページを再構築しファイルを縮小
    3. ANALYZE                           … クエリプランナー用の統計を更新

VACUUM は DB ファイル全体を書き換える「大規模操作」であるため、CLAUDE.md 条項4 に従い
呼び出し側（scheduler の深夜保守ジョブ）で必ず事前バックアップ（src.ops.backup.backup_db）を
取得してから本処理を実行すること。

Usage:
    python -m src.ops.db_optimize            # data/umalogi.db を最適化
    python -m src.ops.db_optimize --analyze-only  # VACUUM を行わず ANALYZE のみ
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"


def _db_size_bytes(db_path: Path) -> int:
    """DB 本体ファイルのサイズ（バイト）を返す。存在しなければ 0。"""
    return db_path.stat().st_size if db_path.exists() else 0


def _checkpoint_wal(conn: sqlite3.Connection) -> None:
    """WAL を本体へ反映して -wal ファイルを切り詰める。WAL 無効時は無害にスキップ。"""
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error as exc:
        logger.warning("WAL チェックポイント失敗（続行）: %s", exc)


def optimize_db(
    db_path: Path | None = None,
    *,
    vacuum: bool = True,
    analyze: bool = True,
) -> dict[str, object]:
    """SQLite DB に対して VACUUM / ANALYZE を安全に実行する。

    VACUUM は autocommit モードかつオープン中トランザクションが無い状態でのみ実行可能なため、
    ``isolation_level=None`` で接続し明示的に制御する。

    Args:
        db_path: 最適化対象 DB。省略時は data/umalogi.db。
        vacuum:  True で VACUUM を実行（ファイル縮小）。
        analyze: True で ANALYZE を実行（統計更新）。

    Returns:
        実行結果サマリーの dict:
            - ``ok``           (bool): 致命的エラーなく完了したか
            - ``vacuumed``     (bool): VACUUM を実行したか
            - ``analyzed``     (bool): ANALYZE を実行したか
            - ``before_bytes`` (int): 最適化前のファイルサイズ
            - ``after_bytes``  (int): 最適化後のファイルサイズ
            - ``saved_bytes``  (int): 削減バイト数（負値は増加）
            - ``elapsed_sec``  (float): 所要秒数

    Raises:
        FileNotFoundError: db_path が存在しない場合。
    """
    db_path = db_path or _DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"DB が存在しません: {db_path}")

    before = _db_size_bytes(db_path)
    t0 = time.monotonic()
    vacuumed = False
    analyzed = False
    ok = True

    # VACUUM のため autocommit で接続する
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=60)
    try:
        _checkpoint_wal(conn)

        if vacuum:
            try:
                logger.info("VACUUM 実行中 ...")
                conn.execute("VACUUM")
                vacuumed = True
                logger.info("VACUUM 完了")
            except sqlite3.Error as exc:
                ok = False
                logger.error("VACUUM 失敗: %s", exc)

        if analyze:
            try:
                logger.info("ANALYZE 実行中 ...")
                conn.execute("ANALYZE")
                analyzed = True
                logger.info("ANALYZE 完了")
            except sqlite3.Error as exc:
                ok = False
                logger.error("ANALYZE 失敗: %s", exc)
    finally:
        conn.close()

    after = _db_size_bytes(db_path)
    elapsed = time.monotonic() - t0
    saved = before - after

    logger.info(
        "DB 最適化サマリー: %.1f MB → %.1f MB (削減 %.1f MB) / %.1f 秒",
        before / 1_048_576,
        after / 1_048_576,
        saved / 1_048_576,
        elapsed,
    )

    return {
        "ok": ok,
        "vacuumed": vacuumed,
        "analyzed": analyzed,
        "before_bytes": before,
        "after_bytes": after,
        "saved_bytes": saved,
        "elapsed_sec": elapsed,
    }


def main() -> None:
    """CLI エントリーポイント。data/umalogi.db を最適化する。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="DB 最適化（VACUUM / ANALYZE）")
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="VACUUM を行わず ANALYZE のみ実行",
    )
    args = parser.parse_args()

    result = optimize_db(vacuum=not args.analyze_only, analyze=True)
    saved_mb = int(result["saved_bytes"]) / 1_048_576  # type: ignore[call-overload]
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M}] DB 最適化完了: "
        f"VACUUM={result['vacuumed']} ANALYZE={result['analyzed']} "
        f"削減={saved_mb:.1f}MB"
    )


if __name__ == "__main__":
    main()
