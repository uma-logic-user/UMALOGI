"""
cleanup_encoding.py — DB 全件エンコーディング文字化けクレンジング

対象テーブル・カラム:
  horses       : horse_name, sire, dam, dam_sire
  race_results : horse_name, jockey, trainer
  races        : race_name, venue
  entries      : horse_name, jockey, trainer
  jockeys      : jockey_name
  trainers     : trainer_name
  racehorses   : horse_name, trainer_name

処理フロー:
  1. 各カラムを全件スキャンして is_garbled() で文字化け検知
  2. try_recover_encoding() で回復を試みる
  3. 回復成功 → UPDATE。回復不可 → 空文字にリセット (NULL 許可カラムは NULL)
  4. 修復件数・テーブルを最後に報告する
"""

from __future__ import annotations

import sys
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.utils.text import is_garbled, try_recover_encoding  # noqa: E402
from src.database.init_db import get_db_path  # noqa: E402


# ── 対象テーブル定義 ─────────────────────────────────────────────
# (table_name, pk_column, text_columns, nullable_columns)
_TARGETS: list[tuple[str, str, list[str], set[str]]] = [
    # horses: sire/dam/dam_sire は NULL 可。horse_name は UNIQUE だが単独 PK なので安全
    (
        "horses",
        "horse_id",
        ["horse_name", "sire", "dam", "dam_sire"],
        {"sire", "dam", "dam_sire"},
    ),
    # race_results: horse_name は (race_id, horse_name) UNIQUE のため除外。jockey/trainer は安全
    ("race_results", "rowid", ["jockey", "trainer"], set()),
    ("races", "race_id", ["race_name", "venue"], set()),
    # entries: horse_name は UNIQUE 制約あり → 除外。jockey/trainer のみ
    ("entries", "rowid", ["jockey", "trainer"], set()),
    ("jockeys", "jockey_code", ["jockey_name"], set()),
    ("trainers", "trainer_code", ["trainer_name"], set()),
    ("racehorses", "horse_id", ["horse_name", "trainer_name"], set()),
]


@dataclass
class ColumnReport:
    table: str
    column: str
    scanned: int = 0
    garbled: int = 0
    recovered: int = 0
    cleared: int = 0
    skipped: int = 0

    def __str__(self) -> str:
        return (
            f"  {self.table}.{self.column}: "
            f"スキャン={self.scanned} 文字化け={self.garbled} "
            f"回復={self.recovered} クリア={self.cleared} スキップ={self.skipped}"
        )


def _clean_column(
    conn: sqlite3.Connection,
    table: str,
    pk: str,
    col: str,
    nullable: bool,
) -> ColumnReport:
    report = ColumnReport(table=table, column=col)

    cur = conn.cursor()
    # rowid を使う場合は特殊扱い
    select_pk = "rowid" if pk == "rowid" else pk
    cur.execute(
        f"SELECT {select_pk}, {col} FROM {table} WHERE {col} IS NOT NULL AND {col} != ''"
    )
    rows = cur.fetchall()
    report.scanned = len(rows)

    updates: list[tuple[str | None, object]] = []

    for row_pk, val in rows:
        if not isinstance(val, str) or not is_garbled(val):
            continue
        report.garbled += 1
        recovered = try_recover_encoding(val)
        if recovered:
            updates.append((recovered, row_pk))
            report.recovered += 1
            logger.info(
                "回復 %s.%s [%s]: %r → %r",
                table,
                col,
                row_pk,
                val[:30],
                recovered[:30],
            )
        else:
            new_val: str | None = None if nullable else ""
            updates.append((new_val, row_pk))
            report.cleared += 1
            logger.warning(
                "クリア %s.%s [%s]: %r (回復不可)",
                table,
                col,
                row_pk,
                val[:30],
            )

    if updates:
        conn.executemany(
            f"UPDATE {table} SET {col} = ? WHERE {select_pk} = ?",
            updates,
        )
        conn.commit()

    return report


def run_cleanup(db_path: Path) -> list[ColumnReport]:
    logger.info("クレンジング開始: %s", db_path)
    reports: list[ColumnReport] = []

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # 更新時の FK チェックを一時停止

    try:
        for table, pk, cols, nullable_cols in _TARGETS:
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            if not cur.fetchone():
                logger.debug("テーブルなし: %s → スキップ", table)
                continue

            for col in cols:
                # カラム存在確認
                cur.execute(f"PRAGMA table_info({table})")
                col_names = {r[1] for r in cur.fetchall()}
                if col not in col_names:
                    logger.debug("カラムなし: %s.%s → スキップ", table, col)
                    continue

                report = _clean_column(
                    conn, table, pk, col, nullable=(col in nullable_cols)
                )
                reports.append(report)
    finally:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.close()

    return reports


def main() -> None:
    db_path = get_db_path()
    if not db_path.exists():
        logger.error("DB が見つかりません: %s", db_path)
        sys.exit(1)

    reports = run_cleanup(db_path)

    total_garbled = sum(r.garbled for r in reports)
    total_recovered = sum(r.recovered for r in reports)
    total_cleared = sum(r.cleared for r in reports)

    print("\n" + "=" * 60)
    print("【エンコーディングクレンジング結果】")
    print("=" * 60)
    for r in reports:
        if r.garbled > 0:
            print(str(r))
    print("-" * 60)
    print(f"合計 文字化け検知: {total_garbled}件")
    print(f"     正常回復    : {total_recovered}件")
    print(f"     空文字クリア: {total_cleared}件")
    print("=" * 60)

    if total_garbled == 0:
        logger.info("文字化けは検出されませんでした。")
    else:
        logger.info(
            "クレンジング完了: 文字化け=%d 回復=%d クリア=%d",
            total_garbled,
            total_recovered,
            total_cleared,
        )


if __name__ == "__main__":
    main()
