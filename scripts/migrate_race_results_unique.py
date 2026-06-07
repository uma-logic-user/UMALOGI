"""
scripts/migrate_race_results_unique.py
race_results の UNIQUE(race_id, horse_name) → UNIQUE(race_id, horse_number) 移行スクリプト。

以下の3フェーズを実行する:
  Phase 1: ゴミデータ DELETE
    - Cat1 (304行): 文字化け行で正常行がすでに存在する重複ゴミ
    - Cat2_no_num (338行): horse_number=NULL の回収不能文字化け行
  Phase 2: スキーマ変更
    - UNIQUE(race_id, horse_name) を DROP
    - UNIQUE INDEX (race_id, horse_number) WHERE horse_number IS NOT NULL を追加
  Phase 3: 残り文字化け行の空文字クリア (Cat2_with_num: ~117行)

Usage:
    py scripts/migrate_race_results_unique.py --dry-run   # 確認のみ
    py scripts/migrate_race_results_unique.py             # 本番実行
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_DEFAULT_DB = _ROOT / "data" / "umalogi.db"
_BACKUP_DIR = _ROOT / "data" / "backups"

_GARBLED = re.compile(r"\?[A-Za-z]|[｡-ﾟ]|�|[\x80-\x9f]")


def _is_garbled(s: str | None) -> bool:
    return bool(s and _GARBLED.search(s))


def _classify(conn: sqlite3.Connection) -> tuple[list[int], list[int], list[int]]:
    """文字化け行を3分類して ID リストを返す。

    Returns:
        (cat1_ids, cat2_no_num_ids, cat2_with_num_ids)
    """
    rows = conn.execute(
        "SELECT id, race_id, horse_number, horse_name FROM race_results "
        "WHERE horse_name != '' AND horse_name IS NOT NULL"
    ).fetchall()

    cat1: list[int] = []
    cat2_no_num: list[int] = []
    cat2_with_num: list[int] = []

    for row_id, race_id, horse_num, name in rows:
        if not _is_garbled(name):
            continue
        if horse_num is not None:
            valid = conn.execute(
                "SELECT COUNT(*) FROM race_results "
                "WHERE race_id=? AND horse_number=? AND id!=?",
                (race_id, horse_num, row_id),
            ).fetchone()[0]
            if valid > 0:
                cat1.append(row_id)
            else:
                cat2_with_num.append(row_id)
        else:
            cat2_no_num.append(row_id)

    return cat1, cat2_no_num, cat2_with_num


# ── スキーマ再構築 DDL ──────────────────────────────────────────────────

_RECREATE_TABLE = """
CREATE TABLE race_results_new (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id           TEXT    NOT NULL REFERENCES races(race_id),
    horse_id          TEXT    REFERENCES horses(horse_id),
    horse_name        TEXT    NOT NULL,
    rank              INTEGER,
    gate_number       INTEGER,
    horse_number      INTEGER,
    sex_age           TEXT    NOT NULL DEFAULT '',
    weight_carried    REAL    NOT NULL DEFAULT 0,
    jockey            TEXT    NOT NULL DEFAULT '',
    trainer           TEXT    NOT NULL DEFAULT '',
    finish_time       TEXT,
    margin            TEXT,
    popularity        INTEGER,
    win_odds          REAL,
    horse_weight      INTEGER,
    horse_weight_diff INTEGER,
    created_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    blood_id          TEXT,
    last_3f           REAL
)
"""

_COPY_DATA = """
INSERT INTO race_results_new
    SELECT id, race_id, horse_id, horse_name, rank,
           gate_number, horse_number, sex_age, weight_carried,
           jockey, trainer, finish_time, margin, popularity,
           win_odds, horse_weight, horse_weight_diff, created_at,
           blood_id, last_3f
    FROM race_results
"""

_INDEXES = [
    "CREATE INDEX idx_race_results_race_id  ON race_results(race_id)",
    "CREATE INDEX idx_race_results_horse_id ON race_results(horse_id)",
    "CREATE INDEX idx_results_race_id       ON race_results(race_id)",
    "CREATE INDEX idx_results_horse_id      ON race_results(horse_id)",
    "CREATE INDEX idx_results_rank          ON race_results(rank)",
    "CREATE INDEX idx_rr_horse_raceid       ON race_results(horse_id, race_id DESC)",
    "CREATE INDEX idx_rr_jockey_raceid      ON race_results(jockey, race_id DESC)",
    "CREATE INDEX idx_rr_trainer_raceid     ON race_results(trainer, race_id DESC)",
    "CREATE INDEX idx_rr_race_rank          ON race_results(race_id, rank)",
    "CREATE INDEX idx_rr_horse_race         ON race_results(horse_id, race_id)",
    # 新規: horse_number による部分ユニークインデックス（NULL は除外）
    "CREATE UNIQUE INDEX idx_rr_unique_horsenum ON race_results(race_id, horse_number) WHERE horse_number IS NOT NULL",
]

_TRIGGERS = [
    """CREATE TRIGGER trg_race_results_rank_ins
BEFORE INSERT ON race_results
BEGIN
    SELECT CASE WHEN NEW.rank IS NOT NULL AND NEW.rank > 18
        THEN RAISE(ABORT,
            'race_results.rank > 18 は不正値。HR払戻レコードをSEと誤認識している可能性があります。')
    END;
END""",
    """CREATE TRIGGER trg_race_results_rank_upd
BEFORE UPDATE OF rank ON race_results
BEGIN
    SELECT CASE WHEN NEW.rank IS NOT NULL AND NEW.rank > 18
        THEN RAISE(ABORT,
            'race_results.rank > 18 は不正値。HR払戻レコードをSEと誤認識している可能性があります。')
    END;
END""",
]


def _backup(db_path: Path) -> Path:
    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = _BACKUP_DIR / f"umalogi_{ts}_pre_migrate_unique.db"
    shutil.copy2(db_path, dest)
    logger.info("バックアップ作成: %s", dest)
    return dest


def run(db_path: Path, *, dry_run: bool) -> None:
    mode = "DRY-RUN" if dry_run else "LIVE"
    logger.info("=== race_results UNIQUE制約移行スクリプト [%s] ===", mode)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")

    # ── 分類 ─────────────────────────────────────────────────────────
    logger.info("文字化け行を分類中...")
    cat1, cat2_no_num, cat2_with_num = _classify(conn)
    total_before = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]

    print()
    print("=" * 60)
    print(f"対象 DB      : {db_path}")
    print(f"総行数        : {total_before:,}")
    print()
    print(f"[Phase 1] DELETE (計 {len(cat1) + len(cat2_no_num)} 行)")
    print(f"  Cat1 - 正常行と重複する文字化け行 : {len(cat1):,}")
    print(f"  Cat2 - horse_number=NULL の文字化け行: {len(cat2_no_num):,}")
    print(f"[Phase 2] UNIQUE(race_id, horse_name) → UNIQUE(race_id, horse_number) WHERE IS NOT NULL")
    print(f"[Phase 3] 残り文字化け行を空文字クリア: {len(cat2_with_num):,}")
    print("=" * 60)

    if dry_run:
        print("[DRY-RUN] DB は変更しません。")
        conn.close()
        return

    if not _backup(db_path):
        logger.error("バックアップ失敗")
        conn.close()
        return

    # ── Phase 1: DELETE ──────────────────────────────────────────────
    all_delete = cat1 + cat2_no_num
    if all_delete:
        logger.info("Phase 1: %d 行を削除...", len(all_delete))
        chunk = 500
        deleted = 0
        for i in range(0, len(all_delete), chunk):
            ids = all_delete[i : i + chunk]
            placeholders = ",".join("?" * len(ids))
            with conn:
                c = conn.execute(
                    f"DELETE FROM race_results WHERE id IN ({placeholders})", ids
                )
                deleted += c.rowcount
        logger.info("Phase 1 完了: %d 行削除", deleted)

    # ── Phase 2: スキーマ移行 ─────────────────────────────────────────
    logger.info("Phase 2: スキーマ移行 (テーブル再構築)...")
    try:
        with conn:
            conn.execute("DROP TABLE IF EXISTS race_results_new")
            conn.execute(_RECREATE_TABLE)
            conn.execute(_COPY_DATA)
            conn.execute("DROP TABLE race_results")
            conn.execute("ALTER TABLE race_results_new RENAME TO race_results")

        logger.info("テーブル再構築完了")

        # インデックス再作成
        for sql in _INDEXES:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError as e:
                logger.warning("Index 作成スキップ: %s / %s", sql[:60], e)

        # トリガー再作成
        for sql in _TRIGGERS:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError as e:
                logger.warning("Trigger 作成スキップ: %s", e)

        logger.info("Phase 2 完了")
    except Exception as exc:
        logger.error("Phase 2 失敗: %s", exc)
        conn.close()
        raise

    # ── Phase 3: 残り文字化け行を空文字クリア ────────────────────────
    # cat2_with_num 行を再分類（DELETE後に残っているはず）
    logger.info("Phase 3: 残り文字化け行 (%d 行) を空文字クリア...", len(cat2_with_num))
    cleared = 0
    failed = 0
    for row_id in cat2_with_num:
        try:
            with conn:
                conn.execute(
                    "UPDATE race_results SET horse_name = '' WHERE id = ?", (row_id,)
                )
            cleared += 1
        except sqlite3.IntegrityError as e:
            logger.warning("Phase 3 UNIQUE 衝突 id=%d: %s", row_id, e)
            failed += 1

    logger.info("Phase 3 完了: クリア=%d 衝突=%d", cleared, failed)

    # ── 最終確認 ─────────────────────────────────────────────────────
    total_after = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    remaining_garbled = sum(
        1
        for (name,) in conn.execute(
            "SELECT horse_name FROM race_results WHERE horse_name != '' AND horse_name IS NOT NULL"
        )
        if _is_garbled(name)
    )

    conn.close()

    print()
    print("=" * 60)
    print("【完了レポート】")
    print(f"  削除前行数     : {total_before:,}")
    print(f"  削除後行数     : {total_after:,}")
    print(f"  削除行数       : {total_before - total_after:,}")
    print(f"  空文字クリア   : {cleared:,}")
    print(f"  Phase3 衝突残  : {failed:,}")
    print(f"  残留文字化け   : {remaining_garbled:,}")
    print("=" * 60)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(_DEFAULT_DB))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("DB が見つかりません: %s", db_path)
        sys.exit(1)

    run(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
