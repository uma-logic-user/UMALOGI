"""
scripts/clean_mojibake.py — DB 内の文字化けデータを包括的に浄化するスタンドアロンスクリプト。

既存の cleanup_encoding.py より広範な対象と高感度パターンを使用:
  - entries.horse_name (従来は UNIQUE 制約懸念で除外していたが実際は問題なし)
  - 単発 '?X' パターン / 半角カタカナ / カーリークォート を検出
  - レース単位での絞り込み (--race-date) をサポート

Usage:
    py scripts/clean_mojibake.py                            # 全テーブルを対象にライブ実行
    py scripts/clean_mojibake.py --dry-run                  # 確認のみ（DB 変更なし）
    py scripts/clean_mojibake.py --race-date 20260606       # 指定日のレース起因データのみ
    py scripts/clean_mojibake.py --db path/to/other.db      # 別 DB を指定
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, field
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

# ── 文字化け検知パターン ─────────────────────────────────────────────
# 名前フィールド用: 単発 ?X, 半角カタカナ, カーリークォート, U+FFFD
_NAME_GARBLED = re.compile(
    r"\?[A-Za-z]"  # ?X: JVLink CP932 リードバイト脱落パターン
    r"|[｡-ﾟ]"  # 半角カタカナ (U+FF61-U+FF9F)
    r"|[''"
    "‛‟†‡]"  # カーリークォート・ダガー (CP932バイト誤読)
    r"|�"  # Unicode 置換文字
    r"|[\x80-\x9f]"  # C1 制御文字残留
)


def _is_garbled(s: str | None) -> bool:
    if not s:
        return False
    return bool(_NAME_GARBLED.search(s))


# ── スキャン対象テーブル定義 ────────────────────────────────────────
# (table, pk_expr, columns_to_clean, optional_race_id_column)
_TARGETS: list[tuple[str, str, list[str], str | None]] = [
    ("races", "race_id", ["race_name", "venue"], "race_id"),
    (
        "entries",
        "(race_id, horse_number)",
        ["horse_name", "jockey", "trainer"],
        "race_id",
    ),
    (
        "race_results",
        "race_id || ',' || horse_number",
        ["horse_name", "jockey", "trainer"],
        "race_id",
    ),
    ("horses", "horse_id", ["horse_name", "sire", "dam", "dam_sire"], None),
    ("racehorses", "horse_id", ["horse_name", "trainer_name"], None),
]


@dataclass
class CleanStats:
    scanned: int = 0
    garbled: int = 0
    cleared: int = 0
    errors: list[str] = field(default_factory=list)

    def report_line(self, table: str, col: str) -> str:
        return (
            f"  {table}.{col}: スキャン={self.scanned} "
            f"文字化け={self.garbled} クリア={self.cleared}"
        )


def _build_pk_where(table: str, pk_expr: str) -> tuple[str, str]:
    """テーブルに応じた SELECT/UPDATE の WHERE 句と PK リストを返す。"""
    if table == "entries":
        return "race_id, horse_number", "(race_id = ? AND horse_number = ?)"
    if table == "race_results":
        return "race_id, horse_number", "(race_id = ? AND horse_number = ?)"
    # 単一 PK
    pk_col = pk_expr
    return pk_col, f"{pk_col} = ?"


def _clean_column(
    conn: sqlite3.Connection,
    table: str,
    pk_expr: str,
    col: str,
    race_date: str | None,
    race_id_col: str | None,
    dry_run: bool,
) -> CleanStats:
    stats = CleanStats()
    select_pk, where_clause = _build_pk_where(table, pk_expr)

    # race_date フィルタ (指定されていれば)
    date_filter = ""
    date_param: list[str] = []
    if race_date and race_id_col:
        date_filter = f" AND {race_id_col} LIKE ?"
        date_param = [f"{race_date}%"]

    try:
        rows = conn.execute(
            f"SELECT {select_pk}, {col} FROM {table}"
            f" WHERE {col} IS NOT NULL AND {col} != ''"
            f"{date_filter}",
            date_param,
        ).fetchall()
    except sqlite3.OperationalError as e:
        stats.errors.append(str(e))
        return stats

    stats.scanned = len(rows)
    for row in rows:
        *pk_vals, val = row
        if not _is_garbled(val):
            continue
        stats.garbled += 1
        logger.warning("文字化け %s.%s %s: %r", table, col, pk_vals, val[:30])
        if dry_run:
            continue
        try:
            with conn:
                conn.execute(
                    f"UPDATE {table} SET {col} = '' WHERE {where_clause}",
                    pk_vals,
                )
            stats.cleared += 1
        except Exception as exc:
            stats.errors.append(f"{table}.{col} {pk_vals}: {exc}")

    return stats


def run_cleanup(
    db_path: Path,
    *,
    dry_run: bool = False,
    race_date: str | None = None,
) -> dict[str, CleanStats]:
    conn = sqlite3.connect(str(db_path))
    all_stats: dict[str, CleanStats] = {}
    try:
        for table, pk_expr, cols, race_id_col in _TARGETS:
            # テーブル存在確認
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                logger.debug("テーブルなし: %s → スキップ", table)
                continue

            for col in cols:
                key = f"{table}.{col}"
                stats = _clean_column(
                    conn, table, pk_expr, col, race_date, race_id_col, dry_run
                )
                all_stats[key] = stats
                if stats.scanned:
                    logger.info("%s → %s", key, stats.report_line(table, col))
    finally:
        conn.close()
    return all_stats


def _print_summary(stats: dict[str, CleanStats], dry_run: bool) -> None:
    total_garbled = sum(s.garbled for s in stats.values())
    total_cleared = sum(s.cleared for s in stats.values())
    print()
    print("=" * 60)
    print(f"【文字化け浄化 {'DRY-RUN ' if dry_run else ''}結果】")
    print("=" * 60)
    for key, s in stats.items():
        if s.garbled:
            print(f"  {key}: 文字化け={s.garbled} クリア={s.cleared}")
        for err in s.errors:
            print(f"  [ERROR] {err}")
    print("-" * 60)
    print(f"  合計 文字化け検知: {total_garbled}件")
    print(f"  合計 空文字クリア: {total_cleared}件")
    if dry_run:
        print("  ※ --dry-run のため DB は変更されていません")
    print("=" * 60)


def main() -> None:
    ap = argparse.ArgumentParser(description="DB 文字化けデータ浄化スクリプト")
    ap.add_argument("--db", default=str(_DEFAULT_DB), help="対象 DB のパス")
    ap.add_argument("--dry-run", action="store_true", help="確認のみ（DB 変更なし）")
    ap.add_argument(
        "--race-date", default=None, help="YYYYMMDD: 特定日のデータのみ対象"
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("DB が見つかりません: %s", db_path)
        sys.exit(1)

    logger.info(
        "クレンジング開始: %s (dry_run=%s, race_date=%s)",
        db_path,
        args.dry_run,
        args.race_date,
    )
    stats = run_cleanup(db_path, dry_run=args.dry_run, race_date=args.race_date)
    _print_summary(stats, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
