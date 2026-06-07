"""馬名表記揺れの月次自動クレンジング（馬ID紐付けマスタープロトコル運用）。

毎月実行を想定。``horses`` をマスターとする紐付け設計を維持するため、
表記揺れ（全角半角・余分な空白）を正規化し、horse_id 欠損行を名寄せ解決する。

処理順:
  1. 整合性ガード（汚染検知時は即中止）。
  2. race_results.horse_name の表記揺れ正規化（NFKC＋空白除去）。
     ※ racehorses の正規名と一致させることで名寄せ率を上げる。
  3. horse_id 欠損行の名寄せ解決（composite key）。
  4. 整合性再確認＋結果サマリー。

スケジューリング例（Windows タスクスケジューラ / 月次）:
  py scripts/monthly_horse_cleanse.py --apply

dry-run（既定）:
  py scripts/monthly_horse_cleanse.py
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import unicodedata
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.check_integrity import assert_integrity  # noqa: E402
from src.database.upsert_horses_data import resolve_missing_horse_ids  # noqa: E402

_DB = str(_ROOT / "data" / "umalogi.db")


def _norm(name: str) -> str:
    """馬名を NFKC 正規化し前後空白・連続空白を除去する。"""
    return " ".join(unicodedata.normalize("NFKC", name).split())


def normalize_race_result_names(conn: sqlite3.Connection, *, apply: bool) -> int:
    """race_results.horse_name の表記揺れを正規化する。

    Args:
        conn:  SQLite コネクション。
        apply: True で UPDATE 実行、False は件数のみ。

    Returns:
        正規化対象（変化のあった）行数。
    """
    rows = conn.execute(
        "SELECT id, horse_name FROM race_results WHERE horse_name <> ''"
    ).fetchall()
    updates = [(_norm(n), rid) for rid, n in rows if _norm(n) != n]
    if apply and updates:
        conn.executemany("UPDATE race_results SET horse_name = ? WHERE id = ?", updates)
        conn.commit()
    return len(updates)


def main() -> int:
    ap = argparse.ArgumentParser(description="馬名表記揺れ月次クレンジング")
    ap.add_argument("--db", default=_DB)
    ap.add_argument("--apply", action="store_true", help="実際に更新（既定は dry-run）")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    conn = sqlite3.connect(args.db)
    try:
        print("=== 月次馬名クレンジング ===")
        assert_integrity(conn)  # ガード1
        n_norm = normalize_race_result_names(conn, apply=args.apply)
        print(f"表記揺れ正規化対象: {n_norm:,} 行")
        res = resolve_missing_horse_ids(conn, apply=args.apply)
        print(
            f"名寄せ解決: 候補 {res.candidates:,} / 解決可 {res.resolved:,} / "
            f"適用 {res.applied:,}" + ("" if args.apply else "  (dry-run)")
        )
        assert_integrity(conn)  # ガード2
        print("完了。" + ("" if args.apply else " ※ --apply 未指定のため未反映"))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
