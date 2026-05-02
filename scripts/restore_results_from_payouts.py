"""
entries + 払戻データ(三連単/単勝) から race_results を完全再構築するスクリプト。

対象: entries はあるが race_results の rank=1/2/3 が欠損しているレース。
処理:
  1. entries から全出走馬を race_results に INSERT (rank=NULL)
  2. 払戻データ(三連単→単勝の順) から rank=1,2,3 を UPDATE

これにより build_race_features_for_simulate が全レースで機能するようになる。

使用例:
    py scripts/restore_results_from_payouts.py
    py scripts/restore_results_from_payouts.py --year 2025
    py scripts/restore_results_from_payouts.py --dry-run
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
    """三連単 "X→Y→Z" または "X-Y-Z" をパースして (rank1,rank2,rank3) を返す。"""
    for sep in ("→", "-"):
        if sep in combo:
            parts = combo.split(sep)
            if len(parts) == 3:
                try:
                    a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
                    if all(1 <= x <= 18 for x in (a, b, c)) and len({a, b, c}) == 3:
                        return a, b, c
                except ValueError:
                    pass
    return None


def restore_results(
    conn: sqlite3.Connection,
    year_filter: str | None,
    dry_run: bool,
) -> dict[str, int]:
    """
    entries から race_results を再構築し、払戻から rank=1/2/3 を補完する。

    Returns:
        統計辞書: races_inserted, entries_inserted, rank1_set, rank2_set, rank3_set, skipped
    """
    stats: dict[str, int] = {
        "races_targeted": 0,
        "races_inserted": 0,
        "entries_inserted": 0,
        "rank1_set": 0,
        "rank2_set": 0,
        "rank3_set": 0,
        "skipped_no_payout": 0,
    }

    where_year = f"AND r.date LIKE '{year_filter}%'" if year_filter else ""

    # 対象レース: entries があるが rank=1 が race_results にない
    races = conn.execute(f"""
        SELECT DISTINCT r.race_id
        FROM races r
        WHERE EXISTS (
            SELECT 1 FROM entries e WHERE e.race_id = r.race_id AND e.horse_number > 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM race_results rr
            WHERE rr.race_id = r.race_id AND rr.rank = 1
        )
        {where_year}
        ORDER BY r.race_id
    """).fetchall()

    stats["races_targeted"] = len(races)
    print(f"  対象レース数 (rank=1 未設定): {len(races):,} 件")

    for (race_id,) in races:
        # ── entries から全出走馬を取得 ──────────────────────────────
        entries = conn.execute("""
            SELECT horse_number, horse_id, horse_name, sex_age,
                   weight_carried, horse_weight, gate_number,
                   horse_weight_diff, jockey, trainer
            FROM entries
            WHERE race_id = ? AND horse_number > 0
            ORDER BY horse_number
        """, (race_id,)).fetchall()

        if not entries:
            stats["skipped_no_payout"] += 1
            continue

        # ── 払戻から rank 1/2/3 を特定 ──────────────────────────────
        rank1_num = rank2_num = rank3_num = None

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

        # 三連単なし → 単勝で rank1 のみ
        if rank1_num is None:
            tansho = conn.execute(
                "SELECT combination FROM race_payouts "
                "WHERE race_id=? AND bet_type='単勝' AND payout >= 100 "
                "ORDER BY payout ASC LIMIT 1",
                (race_id,),
            ).fetchone()
            if tansho:
                try:
                    n = int(tansho[0].strip())
                    if 1 <= n <= 18:
                        rank1_num = n
                except ValueError:
                    pass

        if rank1_num is None:
            stats["skipped_no_payout"] += 1
            continue

        if dry_run:
            stats["races_inserted"] += 1
            stats["entries_inserted"] += len(entries)
            if rank1_num:
                stats["rank1_set"] += 1
            if rank2_num:
                stats["rank2_set"] += 1
            if rank3_num:
                stats["rank3_set"] += 1
            continue

        # ── race_results に INSERT (既存行はスキップ) ───────────────
        inserted_count = 0
        for (horse_number, horse_id, horse_name, sex_age,
             weight_carried, horse_weight, gate_number,
             horse_weight_diff, jockey, trainer) in entries:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO race_results
                        (race_id, horse_number, horse_id, horse_name,
                         sex_age, weight_carried, gate_number,
                         horse_weight, horse_weight_diff, jockey, trainer)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    race_id, horse_number, horse_id or None, horse_name or "",
                    sex_age or "", weight_carried or 0.0, gate_number or 0,
                    horse_weight or None, horse_weight_diff or None,
                    jockey or "", trainer or "",
                ))
                inserted_count += 1
            except Exception as exc:
                print(f"    [WARN] INSERT失敗 {race_id} #{horse_number}: {exc}")

        stats["races_inserted"] += 1
        stats["entries_inserted"] += inserted_count

        # ── 払戻から rank を SET ────────────────────────────────────
        for horse_num, rank_val in [
            (rank1_num, 1), (rank2_num, 2), (rank3_num, 3),
        ]:
            if horse_num is not None:
                conn.execute(
                    "UPDATE race_results SET rank=? WHERE race_id=? AND horse_number=? AND rank IS NULL",
                    (rank_val, race_id, horse_num),
                )
                if rank_val == 1:
                    stats["rank1_set"] += 1
                elif rank_val == 2:
                    stats["rank2_set"] += 1
                elif rank_val == 3:
                    stats["rank3_set"] += 1

    if not dry_run:
        conn.commit()

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="entries+払戻から race_results を再構築する")
    ap.add_argument("--year",    default=None, help="対象年 例: 2025")
    ap.add_argument("--dry-run", action="store_true", help="DBへの書き込みを行わない")
    args = ap.parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    print("=" * 60)
    print("  race_results 完全自動復元 (entries + 払戻推論)")
    if args.dry_run:
        print("  [DRY-RUN モード: DBは変更しません]")
    print("=" * 60)

    stats = restore_results(conn, args.year, args.dry_run)
    conn.close()

    print(f"\n  対象レース         : {stats['races_targeted']:>6,} 件")
    print(f"  復元レース         : {stats['races_inserted']:>6,} 件")
    print(f"  挿入エントリ       : {stats['entries_inserted']:>6,} 件")
    print(f"  rank=1 補完        : {stats['rank1_set']:>6,} 件")
    print(f"  rank=2 補完        : {stats['rank2_set']:>6,} 件")
    print(f"  rank=3 補完        : {stats['rank3_set']:>6,} 件")
    print(f"  払戻なし/スキップ  : {stats['skipped_no_payout']:>6,} 件")
    print()
    print("  完了")


if __name__ == "__main__":
    main()
