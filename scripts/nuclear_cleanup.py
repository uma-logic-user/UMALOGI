"""
nuclear_cleanup.py — DB 全テキストカラム一括浄化スクリプト

対象テーブル:
  races, race_results, race_payouts, horses, predictions

処理内容:
  1. 制御文字 (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F, 0x7F-0x9F) を除去
  2. predictions.combination_json の JSON 破損チェック + 修復
  3. 実行前に data/backups/ へ自動バックアップ

Usage:
  py scripts/nuclear_cleanup.py           # 本番実行
  py scripts/nuclear_cleanup.py --dry-run # 汚染数の確認のみ（変更なし）
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]

# 除去対象の制御文字（タブ・改行・CR は残す）
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def clean_str(v: str) -> str:
    return _CTRL_RE.sub("", v).strip()


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = db_path.parent / "backups" / f"umalogi_nuclear_{ts}.db"
    dest.parent.mkdir(exist_ok=True)
    shutil.copy2(db_path, dest)
    print(f"  バックアップ完了: {dest}")
    return dest


def scan_and_fix(
    conn: sqlite3.Connection,
    table: str,
    cols: list[str],
    pk_col: str,
    dry_run: bool,
) -> dict[str, int]:
    fixed: dict[str, int] = {}
    for col in cols:
        try:
            rows = conn.execute(
                f"SELECT {pk_col}, {col} FROM {table} WHERE {col} IS NOT NULL"
            ).fetchall()
        except sqlite3.OperationalError as e:
            print(f"  [{table}.{col}] スキップ: {e}")
            continue

        dirty = [(pk, v) for pk, v in rows if isinstance(v, str) and _CTRL_RE.search(v)]
        fixed[col] = len(dirty)
        if not dirty:
            continue

        print(f"  [{table}.{col}] 汚染: {len(dirty)} 行")
        if not dry_run:
            for pk, v in dirty:
                new_v = clean_str(v)
                conn.execute(
                    f"UPDATE {table} SET {col} = ? WHERE {pk_col} = ?",
                    (new_v, pk),
                )
            print(f"  [{table}.{col}] → 修正済み ({len(dirty)} 行)")

    return fixed


def fix_combination_json(conn: sqlite3.Connection, dry_run: bool) -> int:
    rows = conn.execute(
        "SELECT id, combination_json FROM predictions WHERE combination_json IS NOT NULL"
    ).fetchall()
    broken = 0
    for pred_id, cj in rows:
        if not cj:
            continue
        try:
            json.loads(cj)
        except (json.JSONDecodeError, TypeError):
            broken += 1
            cleaned = clean_str(cj)
            try:
                json.loads(cleaned)
                ok = True
            except (json.JSONDecodeError, TypeError):
                cleaned = "[]"
                ok = False
            if not dry_run:
                conn.execute(
                    "UPDATE predictions SET combination_json = ? WHERE id = ?",
                    (cleaned, pred_id),
                )
            print(
                f"  [predictions.combination_json] id={pred_id} "
                f"{'修復済み' if ok else '破損→空配列'}"
            )
    return broken


def main() -> None:
    ap = argparse.ArgumentParser(description="DB 全テキストカラム一括浄化")
    ap.add_argument("--dry-run", action="store_true", help="変更なしで汚染行数のみ報告")
    args = ap.parse_args()

    db_path = _ROOT / "data" / "umalogi.db"
    if not db_path.exists():
        print(f"DB が見つかりません: {db_path}")
        sys.exit(1)

    print("=" * 60)
    print(f"  NUCLEAR CLEANUP  {'[DRY-RUN]' if args.dry_run else '[本番実行]'}")
    print(f"  DB: {db_path}")
    print("=" * 60)

    if not args.dry_run:
        backup_db(db_path)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")  # FK無効化してUPDATE高速化
    conn.execute("PRAGMA journal_mode = WAL")

    targets: list[tuple[str, list[str], str]] = [
        ("races",        ["race_name", "venue", "surface", "weather", "condition"], "race_id"),
        ("race_results", ["horse_name", "jockey", "trainer"],                        "id"),
        ("race_payouts", ["bet_type", "combination"],                                 "id"),
        # horses.horse_name は raw SJIS bytes を latin-1 誤解釈した根本破損(63K行)。
        # strip しても半角カナガラベが残るだけで回復不能。
        # 正しい修正は JV-Link DIFN:UM マスタの完全再インポート（別タスク）。
        # sire/dam/dam_sire は正常(0件汚染)のため除外。
        ("horses",       ["sire", "dam", "dam_sire"],                                "horse_id"),
        ("predictions",  ["model_type", "bet_type"],                                 "id"),
    ]

    total_fixed = 0
    for table, cols, pk in targets:
        print(f"\n--- {table} ---")
        result = scan_and_fix(conn, table, cols, pk, dry_run=args.dry_run)
        total_fixed += sum(result.values())

    print(f"\n--- predictions.combination_json ---")
    broken_json = fix_combination_json(conn, dry_run=args.dry_run)
    total_fixed += broken_json

    if not args.dry_run:
        conn.commit()
        print(f"\n  COMMIT 完了")

    conn.close()

    print("\n" + "=" * 60)
    if args.dry_run:
        print(f"  [DRY-RUN] 汚染合計: {total_fixed} 件（変更なし）")
    else:
        print(f"  浄化完了: {total_fixed} 件を修正しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
