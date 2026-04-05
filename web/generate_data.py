"""
DB データを Next.js 用 JSON にエクスポートするスクリプト。

出力ファイル:
  web/src/data/races.json       — 全レース一覧（結果付き）
  web/src/data/predictions.json — 全予想一覧（買い目・成績付き）
  web/src/data/summary.json     — モデル別年間成績サマリー

実行:
  python web/generate_data.py              # 全データエクスポート
  python web/generate_data.py --year 2025  # 指定年のみ
  python web/generate_data.py --latest 10  # 直近N レースのみ
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "umalogi.db"
OUT_DIR = Path(__file__).parent / "src" / "data"


# ── ユーティリティ ────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"ERROR: DB が見つかりません: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_list(rows: list[sqlite3.Row]) -> list[dict]:
    return [dict(r) for r in rows]


# ── races.json ────────────────────────────────────────────────────

def export_races(
    conn: sqlite3.Connection,
    year: int | None = None,
    latest: int | None = None,
) -> list[dict]:
    """
    全レースと結果を結合してエクスポートする。

    Returns:
        [
          {
            "race_id": "...", "race_name": "...", "date": "...",
            "venue": "...", "surface": "...", "distance": 0,
            "results": [
              {"rank": 1, "horse_name": "...", "win_odds": 3.8, ...},
              ...
            ]
          },
          ...
        ]
    """
    where = ""
    params: list = []
    if year:
        where = "WHERE substr(r.date,1,4) = ?"
        params.append(str(year))

    order = "ORDER BY r.date DESC, r.race_id"
    limit = f"LIMIT {latest}" if latest else ""

    races = conn.execute(
        f"""
        SELECT race_id, race_name, date, venue, race_number,
               distance, surface, weather, condition
        FROM races r
        {where}
        {order}
        {limit}
        """,
        params,
    ).fetchall()

    output: list[dict] = []
    for race in races:
        race_dict = dict(race)
        results = conn.execute(
            """
            SELECT
                rr.rank, rr.horse_name, rr.horse_id,
                rr.sex_age, rr.weight_carried, rr.jockey,
                rr.finish_time, rr.margin,
                rr.win_odds, rr.popularity, rr.horse_weight,
                h.sire, h.dam, h.dam_sire
            FROM race_results rr
            LEFT JOIN horses h ON rr.horse_id = h.horse_id
            WHERE rr.race_id = ?
            ORDER BY rr.rank NULLS LAST, rr.id
            """,
            (race["race_id"],),
        ).fetchall()
        race_dict["results"] = _rows_to_list(results)
        output.append(race_dict)

    return output


# ── predictions.json ──────────────────────────────────────────────

def export_predictions(
    conn: sqlite3.Connection,
    year: int | None = None,
    latest: int | None = None,
) -> list[dict]:
    """
    全予想（買い目・的中実績付き）をエクスポートする。
    """
    where = ""
    params: list = []
    if year:
        where = "AND substr(r.date,1,4) = ?"
        params.append(str(year))

    limit = f"LIMIT {latest}" if latest else ""

    predictions = conn.execute(
        f"""
        SELECT
            p.id AS prediction_id,
            p.race_id,
            r.race_name,
            r.date,
            r.venue,
            r.surface,
            r.distance,
            p.model_type,
            p.bet_type,
            p.confidence,
            p.expected_value,
            p.recommended_bet,
            p.notes,
            p.created_at,
            pr.is_hit,
            pr.payout,
            pr.profit,
            pr.roi
        FROM predictions p
        JOIN  races r ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE 1=1 {where}
        ORDER BY p.created_at DESC
        {limit}
        """,
        params,
    ).fetchall()

    output: list[dict] = []
    for pred in predictions:
        pred_dict = dict(pred)
        horses = conn.execute(
            """
            SELECT horse_name, horse_id, predicted_rank, model_score, ev_score
            FROM prediction_horses
            WHERE prediction_id = ?
            ORDER BY predicted_rank NULLS LAST
            """,
            (pred["prediction_id"],),
        ).fetchall()
        pred_dict["horses"] = _rows_to_list(horses)
        output.append(pred_dict)

    return output


# ── summary.json ──────────────────────────────────────────────────

def export_summary(conn: sqlite3.Connection) -> dict:
    """
    モデル別・年別の成績サマリーをエクスポートする。
    """
    annual = conn.execute(
        """
        SELECT model_type, year, bet_type,
               total_bets, hits, hit_rate,
               total_invested, total_payout, roi,
               updated_at
        FROM model_performance
        WHERE month = 0
        ORDER BY year DESC, model_type
        """,
    ).fetchall()

    overall = conn.execute(
        """
        SELECT
            COUNT(pr.id)           AS total_bets,
            SUM(pr.is_hit)         AS total_hits,
            SUM(p.recommended_bet) AS total_invested,
            SUM(pr.payout)         AS total_payout
        FROM predictions p
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        """,
    ).fetchone()

    recent_races = conn.execute(
        "SELECT COUNT(*) AS cnt FROM races"
    ).fetchone()

    return {
        "annual_performance": _rows_to_list(annual),
        "overall": dict(overall) if overall else {},
        "total_races_in_db": recent_races["cnt"] if recent_races else 0,
    }


# ── メイン ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="DB → Next.js JSON エクスポート")
    parser.add_argument("--year",   type=int, help="エクスポート対象年")
    parser.add_argument("--latest", type=int, help="直近N レースのみ")
    args = parser.parse_args()

    conn = _connect()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── races.json ─────────────────────────────────────────────────
    races = export_races(conn, year=args.year, latest=args.latest)
    (OUT_DIR / "races.json").write_text(
        json.dumps(races, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"races.json:       {len(races):4d} レース")

    # ── predictions.json ───────────────────────────────────────────
    preds = export_predictions(conn, year=args.year, latest=args.latest)
    (OUT_DIR / "predictions.json").write_text(
        json.dumps(preds, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"predictions.json: {len(preds):4d} 件")

    # ── summary.json ───────────────────────────────────────────────
    summary = export_summary(conn)
    (OUT_DIR / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"summary.json:     {len(summary['annual_performance'])} レコード")

    conn.close()
    print(f"\nエクスポート先: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
