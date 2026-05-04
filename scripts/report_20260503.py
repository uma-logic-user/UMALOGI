"""
2026-05-03 確定版・損益レポート

評価済みの prediction_results から5/3全レースの P&L を集計する。
払戻未取得の22レースは「欠損中」として別途報告する。

Usage:
    py -3 scripts/report_20260503.py
"""
from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")

from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator

# 5/3 の欠損レースID (払戻未配信)
MISSING_RACES = {
    "202604010210", "202604010211", "202604010212",
    "202605020405", "202605020406", "202605020407", "202605020408",
    "202605020409", "202605020410", "202605020411", "202605020412",
    "202608030405", "202608030406", "202608030407", "202608030408",
    "202608030409", "202608030410", "202608030411", "202608030412",
}

# 5/3 の対象レースIDプレフィックス
TARGET_PREFIXES = ("202604010", "202605020", "202608030")


def get_5_3_races(conn: sqlite3.Connection) -> list[str]:
    """5/3 開催レースの一覧を取得する。"""
    rows = conn.execute(
        """
        SELECT DISTINCT race_id FROM predictions
        WHERE race_id LIKE '202604010%'
           OR race_id LIKE '202605020%'
           OR race_id LIKE '202608030%'
        ORDER BY race_id
        """
    ).fetchall()
    return [r[0] for r in rows]


def run_evaluator_for_missing(conn: sqlite3.Connection, race_ids: list[str]) -> None:
    """prediction_results が未評価のレースに評価を実行する。"""
    evaluated = {
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT p.race_id
            FROM prediction_results pr
            JOIN predictions p ON pr.prediction_id = p.id
            WHERE p.race_id LIKE '202604010%'
               OR p.race_id LIKE '202605020%'
               OR p.race_id LIKE '202608030%'
            """
        ).fetchall()
    }
    to_eval = [r for r in race_ids if r not in evaluated and r not in MISSING_RACES]
    if to_eval:
        print(f"未評価レース {len(to_eval)} 件を評価中...", flush=True)
        ev = Evaluator()
        for race_id in to_eval:
            ev.evaluate_race(conn, race_id)
        conn.commit()


def main() -> None:
    conn = init_db()
    races = get_5_3_races(conn)

    # 未評価の対象レースを評価
    run_evaluator_for_missing(conn, races)

    # ── prediction_results から P&L 集計 ──
    rows = conn.execute(
        """
        SELECT
            p.race_id,
            p.model_type,
            p.bet_type,
            pr.is_hit,
            pr.payout,
            p.recommended_bet
        FROM prediction_results pr
        JOIN predictions p ON pr.prediction_id = p.id
        WHERE p.race_id LIKE '202604010%'
           OR p.race_id LIKE '202605020%'
           OR p.race_id LIKE '202608030%'
        """
    ).fetchall()
    conn.close()

    # 集計
    total_invested = 0.0
    total_payout   = 0.0
    total_bets     = 0
    total_hits     = 0
    by_model: dict[str, dict] = {}
    by_venue: dict[str, dict] = {}

    VENUE_MAP = {"202604": "新潟", "202605": "東京", "202608": "京都"}

    for race_id, model_type, bet_type, is_hit, payout_amt, rec_bet in rows:
        invested = float(rec_bet or 100)
        payout   = float(payout_amt or 0)
        key      = model_type or "unknown"
        venue    = VENUE_MAP.get(race_id[:6], race_id[:6])

        total_invested += invested
        total_payout   += payout
        total_bets     += 1
        total_hits     += int(bool(is_hit))

        if key not in by_model:
            by_model[key] = {"bets": 0, "hits": 0, "invested": 0.0, "payout": 0.0}
        by_model[key]["bets"]     += 1
        by_model[key]["hits"]     += int(bool(is_hit))
        by_model[key]["invested"] += invested
        by_model[key]["payout"]   += payout

        if venue not in by_venue:
            by_venue[venue] = {"bets": 0, "hits": 0, "invested": 0.0, "payout": 0.0}
        by_venue[venue]["bets"]     += 1
        by_venue[venue]["hits"]     += int(bool(is_hit))
        by_venue[venue]["invested"] += invested
        by_venue[venue]["payout"]   += payout

    print("=" * 65)
    print("  2026-05-03 確定版・損益レポート")
    print("=" * 65)

    roi_all = total_payout / total_invested * 100 if total_invested else 0
    hit_r   = total_hits / total_bets * 100 if total_bets else 0
    print(f"\n【全体】")
    print(f"  賭数  : {total_bets:,} 件")
    print(f"  的中  : {total_hits:,} 件 ({hit_r:.1f}%)")
    print(f"  投資額: ¥{total_invested:,.0f}")
    print(f"  払戻  : ¥{total_payout:,.0f}")
    print(f"  損益  : ¥{total_payout - total_invested:+,.0f}")
    print(f"  ROI   : {roi_all:.1f}%")

    print(f"\n【モデル別】")
    print(f"  {'モデル':<20} {'賭':>6} {'的中':>5} {'的中%':>7} {'投資':>10} {'払戻':>10} {'ROI':>7} {'損益':>10}")
    for key, d in sorted(by_model.items()):
        n   = d["bets"]; h = d["hits"]
        inv = d["invested"]; pay = d["payout"]
        roi = pay / inv * 100 if inv else 0
        hr  = h / n * 100 if n else 0
        print(f"  {key:<20} {n:>6,} {h:>5,} {hr:>6.1f}% {inv:>10,.0f} {pay:>10,.0f} {roi:>6.1f}% {pay-inv:>+10,.0f}")

    print(f"\n【会場別】")
    print(f"  {'会場':<6} {'賭':>6} {'的中':>5} {'的中%':>7} {'投資':>10} {'払戻':>10} {'ROI':>7} {'損益':>10}")
    for venue, d in sorted(by_venue.items()):
        n   = d["bets"]; h = d["hits"]
        inv = d["invested"]; pay = d["payout"]
        roi = pay / inv * 100 if inv else 0
        hr  = h / n * 100 if n else 0
        print(f"  {venue:<6} {n:>6,} {h:>5,} {hr:>6.1f}% {inv:>10,.0f} {pay:>10,.0f} {roi:>6.1f}% {pay-inv:>+10,.0f}")

    print(f"\n【払戻未取得レース (JVLink未配信) : {len(MISSING_RACES)}件】")
    for rid in sorted(MISSING_RACES):
        vn = VENUE_MAP.get(rid[:6], rid[:6])
        r_no = rid[-2:]
        print(f"  {rid}  {vn} R{r_no}")
    print("  ※ JRA-VAN が払戻データを配信次第、再実行すれば自動反映されます。")
    print()


if __name__ == "__main__":
    main()
