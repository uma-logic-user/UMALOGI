"""馬ID紐付けマスタープロトコル — UPSERT と名寄せ解決。

馬情報マスタープロトコルの中核。``horse_id``（JRA-VAN 血統登録番号）を主キーとした
冪等な UPSERT と、composite key（馬名＋生年月日＋毛色）による名寄せマスターの構築、
および horse_id が欠損した ``race_results`` 行の解決を提供する。

設計原則:
  - horse_id は JRA-VAN が一意採番した血統登録番号をそのまま主キーとする
    （我々が採番しない）。JVLink 由来データは原理的に同名馬の取り違えが起きない。
  - composite key は「マスター(racehorses)内の重複検知」と
    「horse_id を持たない外部ソース(netkeiba 等)行の名寄せ解決」に用いる。
  - race_results には生年月日・毛色が無く馬名と sex_age のみ。よって解決時は
    馬名＋(sex_age 由来の)生年・性別で一意特定できる場合のみ紐付け、曖昧時は skip。
  - 全 UPSERT は horse_id PK の ON CONFLICT で「不足情報の追記」を行い、
    既存の非空値を空値で上書きしない（COALESCE 的マージ）。

CLI:
  py -m src.database.upsert_horses_data --resolve            # NULL紐付けをdry-run
  py -m src.database.upsert_horses_data --resolve --apply    # 実際に紐付け
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"

_SEX_AGE_RE = re.compile(r"^([牡牝騸セ])\s*(\d{1,2})$")


def upsert_horse(
    conn: sqlite3.Connection,
    horse_id: str,
    horse_name: str,
    *,
    sire: str = "",
    dam: str = "",
    dam_sire: str = "",
) -> None:
    """``horses`` テーブルへ horse_id を主キーとして UPSERT する。

    存在しなければ新規登録、存在すれば不足している血統情報のみ追記する
    （既存の非空値は空値で上書きしない）。

    Args:
        conn:       SQLite コネクション。
        horse_id:   血統登録番号（JRA-VAN 採番）。
        horse_name: 馬名。
        sire:       父名（任意）。
        dam:        母名（任意）。
        dam_sire:   母父名（任意）。
    """
    if not horse_id:
        raise ValueError("horse_id は必須です")
    conn.execute(
        """
        INSERT INTO horses (horse_id, horse_name, sire, dam, dam_sire)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(horse_id) DO UPDATE SET
            horse_name = CASE WHEN excluded.horse_name <> ''
                              THEN excluded.horse_name ELSE horses.horse_name END,
            sire       = CASE WHEN COALESCE(horses.sire, '') = ''
                              THEN excluded.sire ELSE horses.sire END,
            dam        = CASE WHEN COALESCE(horses.dam, '') = ''
                              THEN excluded.dam ELSE horses.dam END,
            dam_sire   = CASE WHEN COALESCE(horses.dam_sire, '') = ''
                              THEN excluded.dam_sire ELSE horses.dam_sire END,
            updated_at = datetime('now', 'localtime')
        """,
        (horse_id, horse_name, sire, dam, dam_sire),
    )


def build_name_master(conn: sqlite3.Connection) -> dict[tuple[str, int, str], str]:
    """composite key 名寄せマスターを ``racehorses`` から構築する。

    キーは (馬名, 生年, 性別) の組（生年月日のうち生年で集約）。同名馬を生年・性別で
    弁別する。同一キーに複数 horse_id が衝突する場合は曖昧として除外する。

    Args:
        conn: SQLite コネクション。

    Returns:
        {(horse_name, birth_year, sex): horse_id} の一意マッピング。
    """
    rows = conn.execute(
        """
        SELECT horse_name, birth_year, sex, horse_id
        FROM racehorses
        WHERE horse_name <> '' AND birth_year IS NOT NULL AND sex <> ''
        """
    ).fetchall()
    master: dict[tuple[str, int, str], str] = {}
    ambiguous: set[tuple[str, int, str]] = set()
    for name, by, sex, hid in rows:
        key = (name, int(by), sex)
        if key in master and master[key] != hid:
            ambiguous.add(key)
        else:
            master[key] = hid
    for key in ambiguous:
        master.pop(key, None)
    return master


@dataclass
class ResolveResult:
    """名寄せ解決の結果サマリー。"""

    candidates: int = 0  # horse_id 欠損行数
    resolved: int = 0  # 一意に解決できた行数
    applied: int = 0  # 実際に UPDATE した行数（dry-run時 0）


def _derive_birth_year(race_id: str, sex_age: str) -> tuple[int, str] | None:
    """race_id（先頭4桁=開催年）と sex_age("牡3")から (生年, 性別) を推定する。"""
    m = _SEX_AGE_RE.match((sex_age or "").strip())
    if not m or len(race_id) < 4 or not race_id[:4].isdigit():
        return None
    sex_raw, age = m.group(1), int(m.group(2))
    sex = {"セ": "騸"}.get(sex_raw, sex_raw)  # 表記揺れ吸収
    birth_year = int(race_id[:4]) - age
    return birth_year, sex


def resolve_missing_horse_ids(
    conn: sqlite3.Connection, *, apply: bool = False
) -> ResolveResult:
    """horse_id 欠損の ``race_results`` 行を composite key 名寄せで解決する。

    race_results は馬名と sex_age しか持たないため、開催年と sex_age から
    (生年, 性別) を導出し、名寄せマスターで一意に特定できた行のみ紐付ける。

    Args:
        conn:  SQLite コネクション。
        apply: True で実際に UPDATE。False は dry-run（件数のみ）。

    Returns:
        :class:`ResolveResult`。
    """
    master = build_name_master(conn)
    res = ResolveResult()
    rows = conn.execute(
        """
        SELECT id, race_id, horse_name, sex_age
        FROM race_results
        WHERE (horse_id IS NULL OR horse_id = '') AND horse_name <> ''
        """
    ).fetchall()
    res.candidates = len(rows)
    updates: list[tuple[str, int]] = []
    for rid, race_id, name, sex_age in rows:
        derived = _derive_birth_year(race_id, sex_age)
        if derived is None:
            continue
        by, sex = derived
        hid = master.get((name, by, sex))
        if hid:
            res.resolved += 1
            updates.append((hid, rid))
    if apply and updates:
        conn.executemany(
            "UPDATE race_results SET horse_id = ? WHERE id = ? "
            "AND (horse_id IS NULL OR horse_id = '')",
            updates,
        )
        conn.commit()
        res.applied = len(updates)
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description="馬ID紐付け UPSERT / 名寄せ解決")
    ap.add_argument("--db", default=str(_DB_PATH))
    ap.add_argument("--resolve", action="store_true", help="NULL horse_id を名寄せ解決")
    ap.add_argument(
        "--apply", action="store_true", help="実際に UPDATE（既定は dry-run）"
    )
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    # 取り込み前セーフティガード（汚染検知時は中止）。
    from src.database.check_integrity import assert_integrity

    conn = sqlite3.connect(args.db)
    try:
        assert_integrity(conn)
        if args.resolve:
            res = resolve_missing_horse_ids(conn, apply=args.apply)
            master_size = len(build_name_master(conn))
            print(f"名寄せマスター(composite key)エントリ数: {master_size:,}")
            print(f"horse_id 欠損行          : {res.candidates:,}")
            print(f"一意に解決可能           : {res.resolved:,}")
            print(
                f"適用                     : {res.applied:,}"
                + ("" if args.apply else "  (dry-run: --apply で実行)")
            )
        # 解決後にも整合性を再確認。
        assert_integrity(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
