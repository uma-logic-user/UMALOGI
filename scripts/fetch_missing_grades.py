"""W-088 平日バッチ: 結果ありで grade 空のレースを netkeiba から補完する。

JVLink 生 RA レコードは保存されていないため、既存レースの grade を遡及充填する
決定的ソースは netkeiba 二次ソース（CLAUDE.md 規則11）となる。本スクリプトは
**価値ある対象＝結果ありで grade 空のレース**のみを、安全なレートリミット付きで
順次取得し、競走名からクラスを導出して ``races.grade`` を UPDATE する。
文字化け・空の ``race_name`` も netkeiba 正規名で復元する（規則10/16）。

⚠️ 週末（土・日）は実行しないこと（条項2）。月曜深夜等の凍結解除時間帯に
   ``--run`` 付きで実行する。既定は dry-run（対象列挙のみ・ネットワークアクセス無し）。

【対象条件】
  - grade 空（''/NULL）
  - race_results が 1 件以上存在（結果あり＝実在レース）
  - status = 'valid'（破損・幽霊行 W-089 は除外）

【安全性 / 冪等性】
  - predictions テーブルには一切触れない（条項1）。races.grade / race_name のみ。
  - grade が導出できた行のみ UPDATE。再実行で充填済みは対象から外れ件数が逓減する。
  - 各レース取得前に ``--sleep`` 秒スリープ（既定 3.0s）＋ fetch 内 delay でレート抑制。

Usage:
    py scripts/fetch_missing_grades.py                 # dry-run: 対象件数・先頭を表示
    py scripts/fetch_missing_grades.py --run           # 実取得＋UPDATE（平日のみ）
    py scripts/fetch_missing_grades.py --run --limit 500 --sleep 4.0
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db  # noqa: E402
from src.scraper.jravan_client import _extract_grade  # noqa: E402

_TARGET_SQL = """
    SELECT r.race_id, r.date, r.race_name
    FROM races r
    WHERE (r.grade IS NULL OR r.grade = '')
      AND r.status = 'valid'
      AND EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id)
    ORDER BY r.date DESC
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="grade 空レースの netkeiba 補完（W-088）"
    )
    parser.add_argument(
        "--run", action="store_true", help="実取得＋UPDATE を行う（既定は dry-run）"
    )
    parser.add_argument("--limit", type=int, default=0, help="処理上限件数（0=無制限）")
    parser.add_argument(
        "--sleep", type=float, default=3.0, help="各レース取得前の追加スリープ秒"
    )
    args = parser.parse_args()

    conn = init_db()
    rows = conn.execute(_TARGET_SQL).fetchall()
    if args.limit > 0:
        rows = rows[: args.limit]

    print(f"対象（結果あり・grade 空・status=valid）: {len(rows)} 件")
    for race_id, d, name in rows[:10]:
        print(f"  {race_id} {d} name={name!r}")

    if not args.run:
        print("\n[dry-run] --run 未指定のためネットワークアクセス・更新は行いません。")
        print("平日（月〜金）に `--run` を付けて実行してください（条項2 週末凍結）。")
        return

    # 実取得は遅延 import（dry-run 時に requests 依存を読み込まない）
    from src.scraper.netkeiba import fetch_race_results
    from src.utils.text import is_garbled_name

    filled = 0
    failed = 0
    for i, (race_id, d, cur_name) in enumerate(rows, 1):
        time.sleep(args.sleep)
        try:
            info = fetch_race_results(
                race_id, race_date=d, fetch_pedigree=False, delay=1.5
            )
        except Exception as exc:  # noqa: BLE001 — 個別失敗は握りつぶし次へ
            failed += 1
            print(f"  [{i}/{len(rows)}] {race_id} 取得失敗: {exc}")
            continue

        nk_name = (info.race_name or "").strip()
        grade = _extract_grade(nk_name)

        # race_name が空 or 文字化けなら netkeiba 正規名で復元
        new_name = None
        if nk_name and (not (cur_name or "").strip() or is_garbled_name(cur_name)):
            new_name = nk_name

        if grade or new_name:
            with conn:
                if grade and new_name:
                    conn.execute(
                        "UPDATE races SET grade = ?, race_name = ? WHERE race_id = ?",
                        (grade, new_name, race_id),
                    )
                elif grade:
                    conn.execute(
                        "UPDATE races SET grade = ? WHERE race_id = ?", (grade, race_id)
                    )
                else:
                    conn.execute(
                        "UPDATE races SET race_name = ? WHERE race_id = ?",
                        (new_name, race_id),
                    )
            if grade:
                filled += 1
            print(
                f"  [{i}/{len(rows)}] {race_id} grade={grade!r} "
                f"name={'(復元)' if new_name else '(据置)'}"
            )
        else:
            print(f"  [{i}/{len(rows)}] {race_id} grade 導出不可 name={nk_name!r}")

    print(
        f"\n完了: grade 充填 {filled} 件 / 取得失敗 {failed} 件 / 対象 {len(rows)} 件"
    )


if __name__ == "__main__":
    main()
