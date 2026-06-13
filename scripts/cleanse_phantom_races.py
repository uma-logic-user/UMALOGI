"""W-089: 破損・幽霊レース行の論理隔離（条項4 準拠・物理削除しない）。

2021-2024 の失敗インポート残骸が races に多数残存する。これらは
``race_name`` 空・``distance`` 0・``race_results`` 0件・``predictions`` 0件で、
レースの体をなさず特徴量生成・分析クエリのノイズ源になる。
本スクリプトは ``races.status`` を ``'error'`` に更新して論理隔離する。

【隔離条件（厳格・全 AND）】
  1. grade 空（''/NULL）
  2. race_results が 0 件（結果が一切存在しない）
  3. predictions が 0 件（条項1: 予想紐付き行は不変・保護）
  4. distance が 0 / NULL（実 RA データを持たない）
  5. date が本日より前（当日・未来の未実施レースを誤隔離しない）

【安全性】
  - predictions テーブルには一切触れない（条項1）。races.status のみ UPDATE。
  - v_race_mart は INNER JOIN race_results のため結果0件行は元々 ML 学習に到達しない
    （= 隔離は明示的衛生化であり、ML 汚染の即時是正ではない）。
  - 冪等: 既に status='error' の行は再更新しない。再実行で件数は安定する。

Usage:
    py scripts/cleanse_phantom_races.py            # 隔離を適用
    py scripts/cleanse_phantom_races.py --dry-run  # 件数のみ表示（変更しない）
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db  # noqa: E402

# 厳格な隔離条件。UPDATE の対象表 races には別名を付けられないため、別名 r を使う
# 相関サブクエリとして race_id を列挙する形にする（SQLite の UPDATE 制約回避）。
_PHANTOM_SUBQUERY = """
    SELECT r.race_id FROM races r
    WHERE (r.grade IS NULL OR r.grade = '')
      AND (r.distance IS NULL OR r.distance = 0)
      AND r.date < ?
      AND NOT EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id)
      AND NOT EXISTS (SELECT 1 FROM predictions p WHERE p.race_id = r.race_id)
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="破損・幽霊レース行の論理隔離（W-089）"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="件数のみ表示し DB を変更しない"
    )
    args = parser.parse_args()

    conn = init_db()  # マイグレーション #22 で status 列を保証

    today = date.today().isoformat()

    # 対象件数（まだ error になっていないもの）
    target = conn.execute(
        f"SELECT COUNT(*) FROM races WHERE status <> 'error' "
        f"AND race_id IN ({_PHANTOM_SUBQUERY})",
        (today,),
    ).fetchone()[0]

    already = conn.execute(
        "SELECT COUNT(*) FROM races WHERE status = 'error'"
    ).fetchone()[0]

    print(f"基準日: {today}")
    print(f"既に隔離済み (status='error'): {already} 件")
    print(f"新規隔離対象: {target} 件")

    if args.dry_run:
        print("--dry-run: DB は変更しません。")
        _report(conn)
        return

    with conn:
        conn.execute(
            f"UPDATE races SET status = 'error' WHERE status <> 'error' "
            f"AND race_id IN ({_PHANTOM_SUBQUERY})",
            (today,),
        )

    print(f"隔離適用: {target} 件を status='error' に更新しました。")
    _report(conn)


def _report(conn) -> None:  # type: ignore[no-untyped-def]
    total = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    valid = conn.execute(
        "SELECT COUNT(*) FROM races WHERE status = 'valid'"
    ).fetchone()[0]
    error = conn.execute(
        "SELECT COUNT(*) FROM races WHERE status = 'error'"
    ).fetchone()[0]
    print("--- status 分布 ---")
    print(f"  total: {total}")
    print(f"  valid: {valid}")
    print(f"  error: {error}")


if __name__ == "__main__":
    main()
