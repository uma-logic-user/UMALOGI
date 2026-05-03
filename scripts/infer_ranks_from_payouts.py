"""
払戻データ（三連単・単勝）から race_results.rank を逆引きして更新するスクリプト。

JVLink SE レコードの着順オフセットが不明な場合、確定払戻データから
1着・2着・3着馬番を特定して race_results.rank を補完する。

使用例:
    py scripts/infer_ranks_from_payouts.py
    py scripts/infer_ranks_from_payouts.py --year 2025
    py scripts/infer_ranks_from_payouts.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import sqlite3

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


def _parse_sanrentan(combo: str) -> tuple[int, int, int] | None:
    """三連単 "X→Y→Z" または "X-Y-Z" 形式をパースして (rank1,rank2,rank3) を返す。"""
    for sep in ("→", "-"):
        if sep in combo:
            parts = combo.split(sep)
            if len(parts) == 3:
                try:
                    a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
                    if all(1 <= x <= 18 for x in (a, b, c)):
                        return a, b, c
                except ValueError:
                    pass
    return None


def infer_ranks(conn: sqlite3.Connection, year_filter: str | None, dry_run: bool) -> dict[str, int]:
    stats = {"races_processed": 0, "rank1_set": 0, "rank2_set": 0, "rank3_set": 0, "skipped": 0}

    # 対象レース: rank=2 が未設定 かつ 払戻データがあるレース
    # （旧: rank が全て NULL のレースのみ → rank=1 が入っているレースをスキップするバグ）
    where_year = f"AND r.date LIKE '{year_filter}%'" if year_filter else ""
    races = conn.execute(f"""
        SELECT DISTINCT r.race_id
        FROM races r
        WHERE NOT EXISTS (
            SELECT 1 FROM race_results rr
            WHERE rr.race_id = r.race_id AND rr.rank = 2
        )
        AND EXISTS (
            SELECT 1 FROM race_payouts rp
            WHERE rp.race_id = r.race_id
        )
        {where_year}
        ORDER BY r.race_id
    """).fetchall()

    print(f"  rank=2 未設定レース数: {len(races):,} 件")

    for (race_id,) in races:
        stats["races_processed"] += 1

        rank1_num = rank2_num = rank3_num = None

        # ── 三連単から rank 1/2/3 を確定 ─────────────────────────
        # corrupt データ（2桁など不正 combination）を読み飛ばして最初に parse できた行を使う
        for (combo, _payout) in conn.execute(
            "SELECT combination, payout FROM race_payouts "
            "WHERE race_id=? AND bet_type='三連単' AND payout >= 100 "
            "ORDER BY payout ASC",
            (race_id,),
        ):
            parsed = _parse_sanrentan(combo)
            if parsed:
                rank1_num, rank2_num, rank3_num = parsed
                break

        # ── 三連単がなければ単勝から rank 1 のみ補完 ───────────────
        if rank1_num is None:
            tanshо = conn.execute(
                "SELECT combination FROM race_payouts "
                "WHERE race_id=? AND bet_type='単勝' AND payout >= 100 "
                "ORDER BY payout ASC LIMIT 1",
                (race_id,),
            ).fetchone()
            if tanshо:
                try:
                    n = int(tanshо[0].strip())
                    if 1 <= n <= 18:
                        rank1_num = n
                except ValueError:
                    pass

        if rank1_num is None:
            stats["skipped"] += 1
            continue

        if not dry_run:
            for horse_num, rank_val in [
                (rank1_num, 1),
                (rank2_num, 2),
                (rank3_num, 3),
            ]:
                if horse_num is not None:
                    conn.execute(
                        "UPDATE race_results SET rank=? WHERE race_id=? AND horse_number=? AND rank IS NULL",
                        (rank_val, race_id, horse_num),
                    )

        if rank1_num is not None:
            stats["rank1_set"] += 1
        if rank2_num is not None:
            stats["rank2_set"] += 1
        if rank3_num is not None:
            stats["rank3_set"] += 1

    if not dry_run:
        conn.commit()

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="払戻データから race_results.rank を補完する")
    ap.add_argument("--year",    default=None, help="対象年 例: 2025")
    ap.add_argument("--dry-run", action="store_true", help="DBへの書き込みを行わない")
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    print("=" * 60)
    print("  払戻データから着順補完")
    if args.dry_run:
        print("  [DRY-RUN モード: DBは変更しません]")
    print("=" * 60)

    stats = infer_ranks(conn, args.year, args.dry_run)
    conn.close()

    print(f"\n  処理レース       : {stats['races_processed']:>6,} 件")
    print(f"  rank=1 補完      : {stats['rank1_set']:>6,} 件")
    print(f"  rank=2 補完      : {stats['rank2_set']:>6,} 件")
    print(f"  rank=3 補完      : {stats['rank3_set']:>6,} 件")
    print(f"  払戻なし/スキップ: {stats['skipped']:>6,} 件")
    print()
    print("  完了")


if __name__ == "__main__":
    main()
