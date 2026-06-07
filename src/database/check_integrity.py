"""馬ID紐付け整合性チェック（セーフティガード）。

馬情報マスタープロトコルの安全装置。``horses`` / ``racehorses`` / ``race_results``
の馬ID紐付けが汚染されていないかを検査し、汚染検知時は処理を停止させる。

検査項目:
  1. composite key 重複: 同一(馬名, 生年月日, 毛色)に異なる horse_id が割り当て
     られていないか（名寄せ汚染の決定的兆候）。
  2. horse_id 名前空間整合: race_results.horse_id が racehorses に存在する割合。
  3. NULL 紐付け率: race_results.horse_id の欠損割合。
  4. 同名異ID（同名馬）: 参考情報として件数を報告。

CLI:
  py -m src.database.check_integrity            # 検査して結果表示
  py -m src.database.check_integrity --strict   # CRITICAL 検出時 exit 1

他スクリプトからの利用:
  from src.database.check_integrity import assert_integrity
  assert_integrity(conn)   # 汚染検知時 IntegrityViolation を送出
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"


class IntegrityViolation(RuntimeError):
    """紐付け汚染を検知した際に送出される例外。"""


@dataclass
class IntegrityReport:
    """整合性チェック結果。"""

    composite_dup_groups: int = 0  # 同(名,生年月日,毛色)で別IDが付いたグループ数
    composite_dup_examples: list[tuple[str, str, str, int]] = field(
        default_factory=list
    )
    rr_total: int = 0
    rr_linked: int = 0
    rr_null: int = 0
    rr_in_racehorses: int = 0  # racehorses に存在する horse_id 件数(distinct)
    rr_distinct_linked: int = 0
    same_name_diff_id: int = 0  # 同名で別IDの馬名数（同名馬・正常な場合もある）

    @property
    def link_rate(self) -> float:
        return self.rr_linked / self.rr_total if self.rr_total else 0.0

    @property
    def namespace_match_rate(self) -> float:
        return (
            self.rr_in_racehorses / self.rr_distinct_linked
            if self.rr_distinct_linked
            else 0.0
        )

    @property
    def has_critical(self) -> bool:
        # composite key 重複は決定的な汚染 → CRITICAL。
        return self.composite_dup_groups > 0

    def render(self) -> str:
        lines = [
            "=== 馬ID紐付け整合性レポート ===",
            f"race_results 総数            : {self.rr_total:,}",
            f"  horse_id 紐付け済          : {self.rr_linked:,} ({self.link_rate:.1%})",
            f"  horse_id NULL              : {self.rr_null:,}",
            f"racehorses 名前空間一致(distinct): "
            f"{self.rr_in_racehorses:,}/{self.rr_distinct_linked:,} "
            f"({self.namespace_match_rate:.1%})",
            f"同名異ID（同名馬）馬名数      : {self.same_name_diff_id:,}",
            f"composite key 重複グループ    : {self.composite_dup_groups:,}"
            + ("  🔴 CRITICAL（紐付け汚染）" if self.composite_dup_groups else "  ✅"),
        ]
        for name, bd, coat, n in self.composite_dup_examples[:10]:
            lines.append(f"    - '{name}' 生{bd} {coat}: {n} 種の horse_id")
        return "\n".join(lines)


def check_integrity(conn: sqlite3.Connection) -> IntegrityReport:
    """DB の馬ID紐付け整合性を検査して :class:`IntegrityReport` を返す。

    Args:
        conn: SQLite コネクション。

    Returns:
        検査結果レポート。
    """
    cur = conn.cursor()
    rep = IntegrityReport()

    rep.rr_total = cur.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    rep.rr_null = cur.execute(
        "SELECT COUNT(*) FROM race_results WHERE horse_id IS NULL OR horse_id=''"
    ).fetchone()[0]
    rep.rr_linked = rep.rr_total - rep.rr_null

    rep.rr_distinct_linked = cur.execute(
        "SELECT COUNT(DISTINCT horse_id) FROM race_results "
        "WHERE horse_id IS NOT NULL AND horse_id<>''"
    ).fetchone()[0]
    rep.rr_in_racehorses = cur.execute(
        "SELECT COUNT(DISTINCT rr.horse_id) FROM race_results rr "
        "JOIN racehorses um ON um.horse_id = rr.horse_id "
        "WHERE rr.horse_id IS NOT NULL AND rr.horse_id<>''"
    ).fetchone()[0]

    # composite key 重複: 同一(馬名, 生年月日, 毛色)で horse_id が複数存在 → 汚染。
    # birth_date/coat が空のレコードは判定材料不足のため除外。
    dup_rows = cur.execute(
        """
        SELECT horse_name, birth_date, coat_color, COUNT(DISTINCT horse_id) AS n
        FROM racehorses
        WHERE horse_name <> '' AND birth_date <> '' AND coat_color <> ''
        GROUP BY horse_name, birth_date, coat_color
        HAVING COUNT(DISTINCT horse_id) > 1
        ORDER BY n DESC
        """
    ).fetchall()
    rep.composite_dup_groups = len(dup_rows)
    rep.composite_dup_examples = [(r[0], r[1], r[2], r[3]) for r in dup_rows]

    rep.same_name_diff_id = cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT horse_name FROM racehorses
            WHERE horse_name <> ''
            GROUP BY horse_name HAVING COUNT(DISTINCT horse_id) > 1
        )
        """
    ).fetchone()[0]
    return rep


def assert_integrity(conn: sqlite3.Connection) -> IntegrityReport:
    """整合性を検査し、CRITICAL（composite key 重複）検知時に例外を送出する。

    取り込みパイプラインの前後に挟むセーフティガード用。

    Raises:
        IntegrityViolation: composite key 重複（紐付け汚染）を検知した場合。
    """
    rep = check_integrity(conn)
    if rep.has_critical:
        raise IntegrityViolation(
            f"馬ID紐付け汚染を検知: composite key 重複 {rep.composite_dup_groups} 件。"
            f"取り込みを中止します。\n{rep.render()}"
        )
    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="馬ID紐付け整合性チェック")
    ap.add_argument("--db", default=str(_DB_PATH))
    ap.add_argument(
        "--strict",
        action="store_true",
        help="CRITICAL 検出時に exit 1（CI / 取り込みガード用）",
    )
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    conn = sqlite3.connect(args.db)
    try:
        rep = check_integrity(conn)
    finally:
        conn.close()
    print(rep.render())
    if args.strict and rep.has_critical:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
