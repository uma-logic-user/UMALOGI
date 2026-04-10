"""
DB データを Next.js 用 JSON にエクスポートするスクリプト。

出力ファイル:
  web/src/data/races.json              — 全レース一覧（結果 + 払戻付き）
  web/src/data/races/{race_id}.json    — 個別レース詳細（結果 + 払戻 + 予想）
  web/src/data/predictions.json        — 全予想一覧（買い目・成績付き）
  web/src/data/summary.json            — モデル別年間成績サマリー

【出力フィールド一覧】

races.json / races/{race_id}.json — レース属性:
  race_id, race_name, year, date, venue, race_number,
  surface, distance, weather, condition

results[] — 出走・着順:
  rank, gate_number(*), horse_number(*), horse_name, horse_id,
  sex_age, weight_carried, jockey, trainer(*),
  finish_time, margin, win_odds, popularity,
  horse_weight, horse_weight_diff(*), sire, dam, dam_sire
  (*) entries テーブルから取得。現状 entries=0件のため NULL。

payouts[] — 払戻金（race_payouts テーブルより）:
  bet_type, combination, payout, popularity

predictions[] — レース内予想（個別レース JSON のみ）:
  prediction_id, model_type, bet_type, confidence,
  expected_value, recommended_bet, combination_json,
  is_hit, payout, profit, roi,
  horses[]: horse_name, predicted_rank, model_score, ev_score

predictions.json — 予想フラット一覧:
  prediction_id, race_id, race_name, date, year, venue,
  race_number, surface, distance, weather, condition,
  model_type, bet_type, confidence, expected_value,
  recommended_bet, combination_json, notes, created_at,
  is_hit, payout, profit, roi,
  horses[]

summary.json:
  annual_performance[], overall{}, by_bet_type[], total_races_in_db

【未対応項目】
  track_direction (右/左/直線): DBスキーマ・スクレイパー未対応のため NULL
  gate_number / horse_number / trainer / horse_weight_diff:
    entries テーブルが 0件のため現状 NULL（スクレイパー拡張で将来対応）

実行:
  python web/generate_data.py              # 全データエクスポート
  python web/generate_data.py --year 2025  # 指定年のみ
  python web/generate_data.py --latest 50  # 直近N レースのみ
  python web/generate_data.py --no-detail  # 個別レースJSON を生成しない
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB_PATH           = Path(__file__).parent.parent / "data" / "umalogi.db"
OUT_DIR           = Path(__file__).parent / "src" / "data"
PREDICTIONS_DIR   = Path(__file__).parent.parent / "data" / "predictions"


# ── ユーティリティ ─────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"ERROR: DB が見つかりません: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _rows(rows: list[sqlite3.Row]) -> list[dict]:
    return [dict(r) for r in rows]


def _year_from_date(date_str: str | None) -> str | None:
    """'2024/06/01' → '2024'"""
    if not date_str:
        return None
    return date_str[:4]


# ── 調教評価の取得 ────────────────────────────────────────────────

def _fetch_training_evals(conn: sqlite3.Connection, race_id: str) -> dict[int, dict]:
    """
    training_evaluations テーブルから調教評価を取得し、
    馬番をキーとした辞書を返す。

    Returns:
        {horse_number: {"eval_grade": "A", "eval_text": "一杯に追われ"}, ...}
    """
    rows = conn.execute(
        """
        SELECT horse_number, eval_grade, eval_text
        FROM training_evaluations
        WHERE race_id = ?
        ORDER BY horse_number
        """,
        (race_id,),
    ).fetchall()
    return {
        r[0]: {"eval_grade": r[1], "eval_text": r[2]}
        for r in rows
    }


def _fetch_prerace_snapshot(race_id: str) -> dict | None:
    """
    data/predictions/{race_id}.json が存在する場合に読み込み、
    bias・horses（ev_score, kelly_fraction, odds_vs_morning, odds_velocity）を返す。

    Returns:
        {"bias": {...}, "horses": {horse_number: {...}}} または None
    """
    path = PREDICTIONS_DIR / f"{race_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    horses_map: dict[int, dict] = {}
    for h in data.get("horses", []):
        num = h.get("horse_number")
        if num is None:
            continue
        horses_map[num] = {
            "honmei_score":    h.get("honmei_score"),
            "ev_score":        h.get("ev_score"),
            "kelly_fraction":  h.get("kelly_fraction"),
            "manji_ev":        h.get("manji_ev"),
            "odds_vs_morning": h.get("odds_vs_morning"),
            "odds_velocity":   h.get("odds_velocity"),
        }

    return {
        "bias":            data.get("bias", {}),
        "ev_recommend":    data.get("ev_recommend", []),
        "horses":          horses_map,
        "generated_at":    data.get("generated_at"),
    }


# ── 結果行の取得（entries LEFT JOIN 付き） ─────────────────────────

def _fetch_results(conn: sqlite3.Connection, race_id: str) -> list[dict]:
    """
    race_results から出走・着順データを返す。

    gate_number / horse_number / trainer / horse_weight_diff は
    Step 1 の scraper 改修以降に再スクレイプしたデータに格納される。
    旧データは NULL のまま（entries テーブルとの JOIN は不要）。
    """
    rows = conn.execute(
        """
        SELECT
            rr.rank,
            rr.gate_number,
            rr.horse_number,
            rr.horse_name,
            rr.horse_id,
            rr.sex_age,
            rr.weight_carried,
            rr.jockey,
            rr.trainer,
            rr.finish_time,
            rr.margin,
            rr.win_odds,
            rr.popularity,
            rr.horse_weight,
            rr.horse_weight_diff,
            h.sire,
            h.dam,
            h.dam_sire
        FROM race_results rr
        LEFT JOIN horses h ON rr.horse_id = h.horse_id
        WHERE rr.race_id = ?
        ORDER BY rr.rank NULLS LAST, rr.id
        """,
        (race_id,),
    ).fetchall()
    return _rows(rows)


# ── 払戻の取得 ─────────────────────────────────────────────────────

def _fetch_payouts(conn: sqlite3.Connection, race_id: str) -> list[dict]:
    """
    race_payouts から払戻データを取得する。

    Returns:
        [{"bet_type": "単勝", "combination": "14",
          "payout": 380, "popularity": 1}, ...]

    bet_type の表示順: 単勝 → 複勝 → 枠連 → 馬連 → ワイド → 馬単 →
                       三連複 → 三連単
    """
    _BET_ORDER = {
        "単勝": 1, "複勝": 2, "枠連": 3, "馬連": 4,
        "ワイド": 5, "馬単": 6, "三連複": 7, "三連単": 8,
    }
    rows = conn.execute(
        """
        SELECT bet_type, combination, payout, popularity
        FROM race_payouts
        WHERE race_id = ?
        ORDER BY popularity NULLS LAST
        """,
        (race_id,),
    ).fetchall()
    result = _rows(rows)
    result.sort(key=lambda r: (_BET_ORDER.get(r["bet_type"], 99),
                                r["popularity"] or 999))
    return result


# ── races.json ────────────────────────────────────────────────────

def export_races(
    conn: sqlite3.Connection,
    year: int | None = None,
    latest: int | None = None,
) -> list[dict]:
    """
    全レース（結果 + 払戻付き）をエクスポートする。
    """
    where  = "WHERE substr(r.date,1,4) = ?" if year else ""
    params: list = [str(year)] if year else []
    limit  = f"LIMIT {latest}" if latest else ""

    races = conn.execute(
        f"""
        SELECT race_id, race_name, date, venue, race_number,
               distance, surface, track_direction, weather, condition
        FROM races r
        {where}
        ORDER BY r.date DESC, r.race_id
        {limit}
        """,
        params,
    ).fetchall()

    output: list[dict] = []
    for race in races:
        d = dict(race)
        d["year"]     = _year_from_date(d.get("date"))
        d["results"]  = _fetch_results(conn, d["race_id"])
        d["payouts"]  = _fetch_payouts(conn, d["race_id"])
        output.append(d)

    return output


# ── 個別レース JSON ───────────────────────────────────────────────

def _fetch_race_predictions(
    conn: sqlite3.Connection,
    race_id: str,
) -> list[dict]:
    """指定レースの予想一覧を取得する（個別レース JSON 用）。"""
    preds = conn.execute(
        """
        SELECT
            p.id             AS prediction_id,
            p.model_type,
            p.bet_type,
            p.confidence,
            p.expected_value,
            p.recommended_bet,
            p.combination_json,
            p.notes,
            p.created_at,
            pr.is_hit,
            pr.payout,
            pr.profit,
            pr.roi
        FROM predictions p
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE p.race_id = ?
        ORDER BY p.created_at, p.id
        """,
        (race_id,),
    ).fetchall()

    output: list[dict] = []
    for pred in preds:
        pd = dict(pred)
        horses = conn.execute(
            """
            SELECT horse_name, horse_id, predicted_rank,
                   model_score, ev_score
            FROM prediction_horses
            WHERE prediction_id = ?
            ORDER BY predicted_rank NULLS LAST, id
            """,
            (pd["prediction_id"],),
        ).fetchall()
        pd["horses"] = _rows(horses)
        output.append(pd)

    return output


def export_race_detail(
    conn: sqlite3.Connection,
    race: dict,
) -> dict:
    """
    個別レース JSON を構築する。
    races.json の 1 エントリに predictions + prerace snapshot + 調教評価 を追加。
    """
    race_id = race["race_id"]
    detail  = dict(race)   # year / results / payouts は既に含まれている

    detail["predictions"] = _fetch_race_predictions(conn, race_id)

    # 調教評価（馬番→{eval_grade, eval_text}）
    training_evals = _fetch_training_evals(conn, race_id)
    detail["training_evals"] = training_evals  # {str(num): {...}}

    # 直前スナップショット（AI予想 JSON が存在する場合のみ）
    prerace = _fetch_prerace_snapshot(race_id)
    if prerace:
        detail["prerace"] = {
            "bias":         prerace["bias"],
            "ev_recommend": prerace["ev_recommend"],
            "generated_at": prerace["generated_at"],
        }
        # 各結果行に prerace データをマージ
        for r in detail.get("results", []):
            num = r.get("horse_number")
            if num is not None and num in prerace["horses"]:
                r.update(prerace["horses"][num])
            if num is not None and num in training_evals:
                r["training_eval"] = training_evals[num]
    else:
        # prerace JSON なし → 調教評価だけ結果行にマージ
        for r in detail.get("results", []):
            num = r.get("horse_number")
            if num is not None and num in training_evals:
                r["training_eval"] = training_evals[num]

    return detail


# ── predictions.json ──────────────────────────────────────────────

def export_predictions(
    conn: sqlite3.Connection,
    year: int | None = None,
    latest: int | None = None,
) -> list[dict]:
    """
    全予想（買い目・的中実績・レース属性付き）をエクスポートする。
    """
    where  = "AND substr(r.date,1,4) = ?" if year else ""
    params: list = [str(year)] if year else []
    limit  = f"LIMIT {latest}" if latest else ""

    predictions = conn.execute(
        f"""
        SELECT
            p.id             AS prediction_id,
            p.race_id,
            r.race_name,
            r.date,
            r.venue,
            r.race_number,
            r.surface,
            r.distance,
            r.weather,
            r.condition,
            p.model_type,
            p.bet_type,
            p.confidence,
            p.expected_value,
            p.recommended_bet,
            p.combination_json,
            p.notes,
            p.created_at,
            pr.is_hit,
            pr.payout,
            pr.profit,
            pr.roi
        FROM predictions p
        JOIN  races r             ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE 1=1 {where}
        ORDER BY p.created_at DESC
        {limit}
        """,
        params,
    ).fetchall()

    output: list[dict] = []
    for pred in predictions:
        pd = dict(pred)
        pd["year"] = _year_from_date(pd.get("date"))
        horses = conn.execute(
            """
            SELECT horse_name, horse_id, predicted_rank,
                   model_score, ev_score
            FROM prediction_horses
            WHERE prediction_id = ?
            ORDER BY predicted_rank NULLS LAST, id
            """,
            (pd["prediction_id"],),
        ).fetchall()
        pd["horses"] = _rows(horses)
        output.append(pd)

    return output


# ── summary.json ──────────────────────────────────────────────────

def export_summary(conn: sqlite3.Connection) -> dict:
    """
    モデル別・年別・券種別の成績サマリーをエクスポートする。
    """
    # 年間累計（month=0 が年間集計）
    annual = conn.execute(
        """
        SELECT model_type, year, bet_type, venue,
               total_bets, hits, hit_rate,
               total_invested, total_payout, roi,
               updated_at
        FROM model_performance
        WHERE month = 0
        ORDER BY year DESC, model_type, bet_type
        """,
    ).fetchall()

    # 全体サマリー
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

    # 券種別集計（全期間）
    by_bet_type = conn.execute(
        """
        SELECT
            p.bet_type,
            COUNT(pr.id)            AS total_bets,
            COALESCE(SUM(pr.is_hit), 0)          AS hits,
            ROUND(
                CAST(SUM(pr.is_hit) AS REAL)
                / NULLIF(COUNT(pr.id), 0) * 100, 2
            )                       AS hit_rate,
            COALESCE(SUM(p.recommended_bet), 0)  AS total_invested,
            COALESCE(SUM(pr.payout), 0)          AS total_payout,
            ROUND(
                COALESCE(SUM(pr.payout), 0)
                / NULLIF(SUM(p.recommended_bet), 0) * 100, 2
            )                       AS roi
        FROM predictions p
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE pr.id IS NOT NULL
        GROUP BY p.bet_type
        ORDER BY total_bets DESC
        """,
    ).fetchall()

    # 年別 × モデル別サマリー（ドリルダウン UI 用）
    by_year = conn.execute(
        """
        SELECT
            substr(r.date, 1, 4)   AS year,
            p.model_type,
            COUNT(pr.id)           AS total_bets,
            COALESCE(SUM(pr.is_hit), 0) AS hits,
            ROUND(
                CAST(SUM(pr.is_hit) AS REAL)
                / NULLIF(COUNT(pr.id), 0) * 100, 2
            )                      AS hit_rate,
            COALESCE(SUM(p.recommended_bet), 0) AS total_invested,
            COALESCE(SUM(pr.payout), 0)         AS total_payout,
            ROUND(
                COALESCE(SUM(pr.payout), 0)
                / NULLIF(SUM(p.recommended_bet), 0) * 100, 2
            )                      AS roi
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE pr.id IS NOT NULL
        GROUP BY year, p.model_type
        ORDER BY year DESC, p.model_type
        """,
    ).fetchall()

    total_races = conn.execute("SELECT COUNT(*) AS cnt FROM races").fetchone()

    return {
        "total_races_in_db":  total_races["cnt"] if total_races else 0,
        "annual_performance": _rows(annual),
        "by_bet_type":        _rows(by_bet_type),
        "by_year":            _rows(by_year),
        "overall":            dict(overall) if overall else {},
    }


# ── メイン ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB → Next.js JSON エクスポート",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python web/generate_data.py                  # 全データ
  python web/generate_data.py --year 2024      # 2024年のみ
  python web/generate_data.py --latest 50      # 直近50レース
  python web/generate_data.py --no-detail      # 個別レースJSON を生成しない
""",
    )
    parser.add_argument("--year",      type=int,  help="エクスポート対象年")
    parser.add_argument("--latest",    type=int,  help="直近 N レースのみ")
    parser.add_argument("--no-detail", action="store_true",
                        help="個別レース JSON (races/{id}.json) を生成しない")
    args = parser.parse_args()

    conn = _connect()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── races.json ─────────────────────────────────────────────────
    races = export_races(conn, year=args.year, latest=args.latest)
    (OUT_DIR / "races.json").write_text(
        json.dumps(races, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"races.json:       {len(races):5d} レース")

    # ── races/{race_id}.json ──────────────────────────────────────
    if not args.no_detail:
        races_dir = OUT_DIR / "races"
        races_dir.mkdir(exist_ok=True)
        for race in races:
            detail = export_race_detail(conn, race)
            (races_dir / f"{race['race_id']}.json").write_text(
                json.dumps(detail, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        print(f"races/{{id}}.json: {len(races):5d} ファイル → {races_dir.resolve()}")

    # ── predictions.json ───────────────────────────────────────────
    preds = export_predictions(conn, year=args.year, latest=args.latest)
    (OUT_DIR / "predictions.json").write_text(
        json.dumps(preds, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"predictions.json: {len(preds):5d} 件")

    # ── summary.json ───────────────────────────────────────────────
    summary = export_summary(conn)
    (OUT_DIR / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    n_annual = len(summary["annual_performance"])
    print(f"summary.json:     {n_annual:5d} モデル×年レコード"
          f"  (総レース {summary['total_races_in_db']:,})")

    conn.close()
    print(f"\nエクスポート先: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
