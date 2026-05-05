"""
DB データを Next.js 用 JSON にエクスポートするスクリプト。

出力ファイル:
  web/src/data/races.json              — 全レース一覧（結果 + 払戻付き）
  web/src/data/races/{race_id}.json    — 個別レース詳細（結果 + 払戻 + 予想）
  web/src/data/predictions.json        — 全予想一覧（買い目・成績付き）
  web/src/data/summary.json            — モデル別年間成績サマリー
  web/src/data/financial.json          — 日次×モデル別収支（収支管理ページ用）

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


def _sanitize(v: object) -> object:
    """文字列中のnullバイト・制御文字を除去して返す。"""
    if isinstance(v, str):
        return v.replace('\x00', '').strip()
    return v


def _rows(rows: list[sqlite3.Row]) -> list[dict]:
    return [{k: _sanitize(v) for k, v in dict(r).items()} for r in rows]


def _year_from_date(date_str: str | None) -> str | None:
    """'2024/06/01' → '2024'"""
    if not date_str:
        return None
    return date_str[:4]


# 払戻キー生成（evaluator.py の _combo_to_payout_key と同じロジック）
def _combo_to_payout_key(bet_type: str, combo: list[int]) -> str | None:
    if not combo:
        return None
    if bet_type in ("単勝", "複勝"):
        return str(combo[0])
    elif bet_type in ("馬連", "ワイド", "枠連"):
        if len(combo) < 2:
            return None
        return "-".join(str(n) for n in sorted(combo[:2]))
    elif bet_type == "馬単":
        if len(combo) < 2:
            return None
        return "→".join(str(n) for n in combo[:2])
    elif bet_type == "三連複":
        if len(combo) < 3:
            return None
        return "-".join(str(n) for n in sorted(combo[:3]))
    elif bet_type == "三連単":
        if len(combo) < 3:
            return None
        return "→".join(str(n) for n in combo[:3])
    return None


def _dedup_combination_json(combination_json: str | None, bet_type: str) -> str:
    """
    combination_json から払戻キー重複コンボを除去して JSON 文字列を返す。

    三連複・馬連・ワイドでは [3,14,11] と [14,3,11] が同一キーになるため
    先着を残して除去。これにより n_tickets 表示と投資額が完全一致する。
    """
    if not combination_json:
        return combination_json or "[]"
    try:
        combos: list = json.loads(combination_json)
    except Exception:
        return combination_json or "[]"
    if not combos or not isinstance(combos[0], list):
        return combination_json or "[]"

    seen: set[str] = set()
    result: list[list[int]] = []
    for combo in combos:
        key = _combo_to_payout_key(bet_type, combo)
        if key is None or key not in seen:
            if key is not None:
                seen.add(key)
            result.append(combo)
    return json.dumps(result, ensure_ascii=False)


def _identify_bet_form(combination_json: str | None, bet_type: str) -> tuple[str, int]:
    """
    combination_json から「買い方ラベル」と「点数」を返す。

    Returns:
        (bet_form, n_tickets)
        例: ("2頭軸マルチ", 12), ("ボックス", 6), ("1頭軸マルチ", 6)
    """
    if not combination_json:
        return bet_type, 0
    try:
        combos: list = json.loads(combination_json)
    except Exception:
        return bet_type, 0
    if not combos:
        return bet_type, 0

    n = len(combos)

    if bet_type == "三連単":
        if not isinstance(combos[0], list):
            return bet_type, n
        all_horses: set = set(h for c in combos for h in c)
        firsts: set = set(c[0] for c in combos)
        num = len(all_horses)
        # ボックス: N頭の全順列 N*(N-1)*(N-2)
        if len(firsts) == num and num >= 3 and n == num * (num - 1) * (num - 2):
            return f"{num}頭ボックス", n
        # 軸馬判定: 全コンボに必ず含まれる馬の数で判定（マルチ = 位置不問）
        always_in = [h for h in all_horses if all(h in c for c in combos)]
        if len(always_in) >= 2:
            return "2頭軸マルチ", n
        if len(always_in) == 1:
            return "1頭軸マルチ", n
        # 1着固定パターン（マルチ不使用）
        if len(firsts) == 2:
            return "2頭軸（1着固定）", n
        if len(firsts) == 1:
            return "1頭軸（1着固定）", n
        return "フォーメーション", n

    if bet_type == "三連複":
        if not isinstance(combos[0], list):
            return bet_type, n
        all_horses = set(h for c in combos for h in c)
        num = len(all_horses)
        # ボックス: C(N,3)
        if num >= 3 and n == num * (num - 1) * (num - 2) // 6:
            return f"{num}頭ボックス", n
        # 軸馬: 全組み合わせに含まれる馬
        axes = [h for h in all_horses if all(h in c for c in combos)]
        if len(axes) >= 2:
            return "軸2頭ながし", n
        if len(axes) == 1:
            return "軸1頭ながし", n
        return "フォーメーション", n

    if bet_type in ("馬連", "ワイド", "馬単"):
        if not isinstance(combos[0], list):
            return bet_type, n
        all_horses = set(h for c in combos for h in c)
        num = len(all_horses)
        if num >= 2 and n == num * (num - 1) // 2:
            return f"{num}頭ボックス", n
        axes = [h for h in all_horses if all(h in c for c in combos)]
        if axes:
            return "軸ながし", n
        return "フォーメーション", n

    if bet_type in ("単勝", "複勝"):
        return bet_type, n

    return bet_type, n


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
    未開催レース（race_results が空）の場合は entries テーブルにフォールバック。
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

    if rows:
        return _rows(rows)

    # 未開催レース: entries テーブルから出走馬情報を返す（rank等は NULL）
    rows = conn.execute(
        """
        SELECT
            NULL        AS rank,
            e.gate_number,
            e.horse_number,
            e.horse_name,
            e.horse_id,
            e.sex_age,
            e.weight_carried,
            e.jockey,
            e.trainer,
            NULL        AS finish_time,
            NULL        AS margin,
            NULL        AS win_odds,
            NULL        AS popularity,
            e.horse_weight,
            e.horse_weight_diff,
            h.sire,
            h.dam,
            h.dam_sire
        FROM entries e
        LEFT JOIN horses h ON e.horse_id = h.horse_id
        WHERE e.race_id = ?
        ORDER BY e.horse_number
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

    # 馬番→馬名マップ（このレースのみ）
    horse_num_to_name: dict[str, str] = {
        str(r[0]): r[1]
        for r in conn.execute(
            "SELECT horse_number, horse_name FROM race_results WHERE race_id = ? AND horse_number IS NOT NULL",
            (race_id,),
        ).fetchall()
    }

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
        bet_type = pd.get("bet_type", "")
        # combination_json の払戻キー重複を排除してから点数を算出
        pd["combination_json"] = _dedup_combination_json(pd.get("combination_json"), bet_type)
        bet_form, n_tickets = _identify_bet_form(pd["combination_json"], bet_type)
        pd["bet_form"]  = bet_form
        pd["n_tickets"] = n_tickets
        # 投資額 = n_tickets × 100 で統一（rec_bet は参考値として保持）
        pd["invested"]  = n_tickets * 100
        pd["horse_num_to_name"] = horse_num_to_name
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

    # 馬番→馬名マップを全レース一括取得（N+1 回避）
    horse_name_map: dict[str, dict[str, str]] = {}
    for row in conn.execute(
        "SELECT race_id, horse_number, horse_name FROM race_results WHERE horse_number IS NOT NULL"
    ).fetchall():
        rid, hnum, hname = row
        horse_name_map.setdefault(str(rid), {})[str(hnum)] = hname

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
        # combination_json の払戻キー重複を排除してから点数を算出
        bet_type = pd.get("bet_type", "")
        pd["combination_json"] = _dedup_combination_json(pd.get("combination_json"), bet_type)
        bet_form, n_tickets = _identify_bet_form(pd["combination_json"], bet_type)
        pd["bet_form"]   = bet_form
        pd["n_tickets"]  = n_tickets
        # 投資額 = n_tickets × 100 で統一（UI で一貫した表示のため）
        pd["invested"]   = n_tickets * 100
        # combination_json の馬番から馬名を引くためのマップ
        pd["horse_num_to_name"] = horse_name_map.get(str(pd.get("race_id", "")), {})
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


# ── financial.json ────────────────────────────────────────────────

_BET_ORDER = {
    "単勝": 1, "複勝": 2, "枠連": 3, "馬連": 4,
    "ワイド": 5, "馬単": 6, "三連複": 7, "三連単": 8,
}


def _build_period_aggregates(
    conn: sqlite3.Connection,
    period_expr: str,
    label_fn: "Callable[[str], str]",
) -> "dict[str, list[dict]]":
    """
    指定の period_expr（SQLのSUBSTR式）で集計した月別/年別データを返す。

    Returns:
        { model_type: [ PeriodStats ] }

    PeriodStats:
        period, label, invested, payout, profit, roi,
        total_bets, hits, cumulative_profit,
        by_bet_type: [ BetTypeStatsLight ]
    """
    rows = conn.execute(
        f"""
        SELECT
            {period_expr}                            AS period,
            p.model_type,
            p.bet_type,
            COALESCE(SUM(p.recommended_bet), 0)      AS invested,
            COALESCE(SUM(pr.payout), 0)              AS payout,
            COUNT(pr.id)                             AS total_bets,
            COALESCE(SUM(pr.is_hit), 0)              AS hits
        FROM predictions p
        JOIN  races r              ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE pr.id IS NOT NULL
        GROUP BY {period_expr}, p.model_type, p.bet_type
        ORDER BY period, p.model_type, p.bet_type
        """,
    ).fetchall()

    from collections import defaultdict
    # (period, model) → aggregated dict
    period_map: dict[tuple, dict] = {}
    for r in _rows(rows):
        key = (r["period"], r["model_type"])
        if key not in period_map:
            period_map[key] = {
                "period": r["period"],
                "label":  label_fn(r["period"]),
                "model_type": r["model_type"],
                "invested": 0.0, "payout": 0.0,
                "total_bets": 0, "hits": 0,
                "by_bet_type": [],
            }
        inv = r["invested"] or 0.0
        pay = r["payout"]   or 0.0
        period_map[key]["invested"]   += inv
        period_map[key]["payout"]     += pay
        period_map[key]["total_bets"] += r["total_bets"]
        period_map[key]["hits"]       += r["hits"]
        period_map[key]["by_bet_type"].append({
            "bet_type":   r["bet_type"],
            "invested":   round(inv, 1),
            "payout":     round(pay, 1),
            "profit":     round(pay - inv, 1),
            "roi":        round(pay / inv * 100, 2) if inv > 0 else 0.0,
            "total_bets": r["total_bets"],
            "hits":       r["hits"],
        })

    for v in period_map.values():
        v["by_bet_type"].sort(key=lambda b: _BET_ORDER.get(b["bet_type"], 99))

    result: dict[str, list[dict]] = {}
    for (period, model), v in sorted(period_map.items()):
        inv  = v["invested"];  pay = v["payout"]
        profit = pay - inv
        roi    = round(pay / inv * 100, 2) if inv > 0 else 0.0
        if model not in result:
            result[model] = []
        result[model].append({
            "period":     v["period"],
            "label":      v["label"],
            "invested":   round(inv,    1),
            "payout":     round(pay,    1),
            "profit":     round(profit, 1),
            "roi":        roi,
            "total_bets": v["total_bets"],
            "hits":       v["hits"],
            "by_bet_type": v["by_bet_type"],
        })

    # 累計損益
    for model_data in result.values():
        cum = 0.0
        for p in model_data:
            cum += p["profit"]
            p["cumulative_profit"] = round(cum, 1)

    return result


from typing import Callable


# ── gachi_hits.json（ガチ予想的中実績）──────────────────────────────

def export_gachi_hits(conn: sqlite3.Connection, limit: int = 200) -> list[dict]:
    """
    Oracle モデル（Harville確率最大化ガチ予想）の的中実績を返す。

    出力:
      [
        {
          "race_id", "race_name", "date", "venue", "surface", "distance",
          "model_type",        # "Oracle(直前)" / "Oracle(暫定)"
          "bet_type",          # "三連複" / "三連単"
          "combination_json",  # 買い目
          "payout",            # 払戻金額
          "is_hit",            # 1 = 的中
          "rank",              # "Oracle" 的中ランク S/A/B/C（払戻額ベース）
        },
        ...
      ]
    """
    rows = conn.execute(
        """
        SELECT
            r.race_id,
            r.race_name,
            r.date,
            r.venue,
            r.surface,
            r.distance,
            p.model_type,
            p.bet_type,
            p.combination_json,
            COALESCE(pr.payout, 0)  AS payout,
            COALESCE(pr.is_hit, 0)  AS is_hit,
            p.recommended_bet,
            p.notes
        FROM predictions p
        JOIN races r ON r.race_id = p.race_id
        LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE p.model_type LIKE 'Oracle%'
          AND p.bet_type IN ('三連複', '三連単')
        ORDER BY r.date DESC, payout DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    # 馬番→馬名マップを race_id 単位で一括構築
    unique_race_ids = list(dict.fromkeys(row[0] for row in rows))
    horse_name_maps: dict[str, dict[str, str]] = {}
    for rid in unique_race_ids:
        horse_name_maps[rid] = {
            str(r[0]): r[1]
            for r in conn.execute(
                "SELECT horse_number, horse_name FROM race_results"
                " WHERE race_id = ? AND horse_number IS NOT NULL",
                (rid,),
            ).fetchall()
        }

    result = []
    for row in rows:
        payout = row[9] or 0
        rank = (
            "S" if payout >= 100_000 else
            "A" if payout >= 30_000 else
            "B" if payout >= 10_000 else
            "C"
        )
        combo_json = _sanitize(row[8])
        bet_type_raw = _sanitize(row[7])
        bet_form, n_tickets = _identify_bet_form(combo_json, bet_type_raw)
        result.append({
            "race_id":           _sanitize(row[0]),
            "race_name":         _sanitize(row[1]),
            "date":              _sanitize(row[2]),
            "venue":             _sanitize(row[3]),
            "surface":           _sanitize(row[4]),
            "distance":          row[5],
            "model_type":        _sanitize(row[6]),
            "bet_type":          bet_type_raw,
            "bet_form":          bet_form,
            "n_tickets":         n_tickets,
            "combination_json":  combo_json,
            "payout":            payout,
            "is_hit":            row[10],
            "rank":              rank if row[10] else None,
            "recommended_bet":   row[11],
            "notes":             _sanitize(row[12] or ""),
            "horse_num_to_name": horse_name_maps.get(row[0], {}),
        })
    return result


# ── win5.json（WIN5 SBCランク）──────────────────────────────────────

def export_win5_data(conn: sqlite3.Connection) -> list[dict]:
    """
    WIN5 予想データをUIエクスポート用に整形する。

    出力:
      [
        {
          "date",          # "2026-04-27"
          "race_ids",      # [r1, r2, r3, r4, r5]
          "races",         # [{race_id, race_name, venue, distance, surface}, ...]
          "selections",    # {race_id: [{horse_number, horse_name, rank}, ...]}
          "total_combinations",
          "is_hit",        # 1 = 的中
          "payout",        # 払戻金額
        },
        ...
      ]
    """
    # WIN5 予想を取得（最新30件）
    rows = conn.execute(
        """
        SELECT
            p.race_id, p.combination_json, p.notes,
            COALESCE(pr.payout, 0) AS payout,
            COALESCE(pr.is_hit, 0) AS is_hit,
            r.date
        FROM predictions p
        JOIN races r ON r.race_id = p.race_id
        LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE p.model_type = 'WIN5' AND p.bet_type = 'WIN5'
        ORDER BY r.date DESC
        LIMIT 30
        """,
    ).fetchall()

    result: list[dict] = []
    for row in rows:
        race_id, combo_json, notes, payout, is_hit, date = row
        combo: dict = {}
        try:
            parsed = json.loads(combo_json or "{}")
            if isinstance(parsed, dict):
                combo = parsed
        except Exception:
            pass

        race_ids = combo.get("race_ids", [race_id])
        selections_raw = combo.get("selections", {})
        horse_ranks_raw = combo.get("horse_ranks", {})

        # レース基本情報を取得
        ph = ",".join("?" * len(race_ids))
        race_rows = conn.execute(
            f"SELECT race_id, race_name, venue, distance, surface FROM races WHERE race_id IN ({ph})",
            race_ids,
        ).fetchall() if race_ids else []
        race_info = {r[0]: {"race_id": r[0], "race_name": _sanitize(r[1]),
                             "venue": _sanitize(r[2]), "distance": r[3],
                             "surface": _sanitize(r[4])} for r in race_rows}

        result.append({
            "date":               _sanitize(date),
            "race_ids":           race_ids,
            "races":              [race_info.get(rid, {"race_id": rid}) for rid in race_ids],
            "selections":         selections_raw,
            "horse_ranks":        horse_ranks_raw,
            "total_combinations": combo.get("total_combinations", 1),
            "is_hit":             is_hit,
            "payout":             payout,
            "notes":              _sanitize(notes or ""),
        })
    return result


def export_financial(conn: sqlite3.Connection) -> dict:
    """
    日次×モデル別の収支データをエクスポートする。

    出力構造:
      {
        model_type: {
          "daily":   [ DailyStats ],    # 日次（by_bet_type + races 付き）
          "monthly": [ PeriodStats ],   # 月別（by_bet_type のみ）
          "yearly":  [ PeriodStats ],   # 年別（by_bet_type のみ）
        }
      }
    """
    # ── 日次×券種 集計 ────────────────────────────────────────
    bet_rows = conn.execute(
        """
        SELECT
            substr(r.date, 1, 10)                AS date,
            p.model_type,
            p.bet_type,
            COALESCE(SUM(p.recommended_bet), 0)  AS invested,
            COALESCE(SUM(pr.payout), 0)          AS payout,
            COUNT(pr.id)                         AS total_bets,
            COALESCE(SUM(pr.is_hit), 0)          AS hits
        FROM predictions p
        JOIN  races r              ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE pr.id IS NOT NULL
        GROUP BY substr(r.date, 1, 10), p.model_type, p.bet_type
        ORDER BY date, p.model_type, p.bet_type
        """,
    ).fetchall()

    # ── レース粒度 ─────────────────────────────────────────────
    race_rows = conn.execute(
        """
        SELECT
            substr(r.date, 1, 10) AS date,
            p.model_type,
            p.bet_type,
            p.race_id,
            r.race_name,
            r.venue,
            r.race_number,
            COALESCE(p.recommended_bet, 0) AS invested,
            COALESCE(pr.payout, 0)         AS payout,
            pr.is_hit
        FROM predictions p
        JOIN  races r              ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE pr.id IS NOT NULL
        ORDER BY date, p.model_type, p.bet_type, p.race_id
        """,
    ).fetchall()

    # race lookup: (date, model, bet_type) → [RaceHit]
    from collections import defaultdict
    race_map: dict[tuple, list[dict]] = defaultdict(list)
    for r in _rows(race_rows):
        key = (r["date"], r["model_type"], r["bet_type"])
        race_map[key].append({
            "race_id":     r["race_id"],
            "race_name":   r["race_name"],
            "venue":       r["venue"],
            "race_number": r["race_number"],
            "invested":    round(r["invested"], 1),
            "payout":      round(r["payout"],   1),
            "is_hit":      r["is_hit"] or 0,
        })

    # ── 日次サマリー構築 ────────────────────────────────────────
    # (date, model) → { invested, payout, hits, total_bets, by_bet_type }
    day_map: dict[tuple, dict] = {}
    for r in _rows(bet_rows):
        key = (r["date"], r["model_type"])
        if key not in day_map:
            day_map[key] = {
                "date": r["date"], "model_type": r["model_type"],
                "invested": 0.0, "payout": 0.0,
                "total_bets": 0, "hits": 0,
                "by_bet_type": [],
            }
        invested = r["invested"] or 0.0
        payout   = r["payout"]   or 0.0
        day_map[key]["invested"]   += invested
        day_map[key]["payout"]     += payout
        day_map[key]["total_bets"] += r["total_bets"]
        day_map[key]["hits"]       += r["hits"]

        bt_roi = round(payout / invested * 100, 2) if invested > 0 else 0.0
        day_map[key]["by_bet_type"].append({
            "bet_type":   r["bet_type"],
            "invested":   round(invested, 1),
            "payout":     round(payout,   1),
            "profit":     round(payout - invested, 1),
            "roi":        bt_roi,
            "total_bets": r["total_bets"],
            "hits":       r["hits"],
            "races":      race_map.get((r["date"], r["model_type"], r["bet_type"]), []),
        })

    # bet_type の表示順でソート
    for v in day_map.values():
        v["by_bet_type"].sort(key=lambda b: _BET_ORDER.get(b["bet_type"], 99))

    # ── model別にリスト化 + 累計損益付与 ───────────────────────
    result: dict[str, list[dict]] = {}
    for (date, model), v in sorted(day_map.items()):
        invested = v["invested"]
        payout   = v["payout"]
        profit   = payout - invested
        roi      = round(payout / invested * 100, 2) if invested > 0 else 0.0
        if model not in result:
            result[model] = []
        result[model].append({
            "date":       date,
            "invested":   round(invested, 1),
            "payout":     round(payout,   1),
            "profit":     round(profit,   1),
            "roi":        roi,
            "total_bets": v["total_bets"],
            "hits":       v["hits"],
            "by_bet_type": v["by_bet_type"],
        })

    # 累計損益
    for model_data in result.values():
        cumulative = 0.0
        for day in model_data:
            cumulative += day["profit"]
            day["cumulative_profit"] = round(cumulative, 1)

    # ── 月別・年別集計 ─────────────────────────────────────────
    def _month_label(period: str) -> str:
        y, m = period.split("-")
        return f"{y}年{int(m)}月"

    def _year_label(period: str) -> str:
        return f"{period}年"

    monthly = _build_period_aggregates(
        conn,
        period_expr="substr(r.date, 1, 7)",
        label_fn=_month_label,
    )
    yearly = _build_period_aggregates(
        conn,
        period_expr="substr(r.date, 1, 4)",
        label_fn=_year_label,
    )

    # ── 全モデルを統合して返す ──────────────────────────────────
    all_models = set(result) | set(monthly) | set(yearly)
    output: dict[str, dict] = {}
    for model in sorted(all_models):
        output[model] = {
            "daily":   result.get(model, []),
            "monthly": monthly.get(model, []),
            "yearly":  yearly.get(model, []),
        }
    return output


def export_condition_analysis(conn: sqlite3.Connection) -> dict:
    """
    過去2年分のバックテスト結果から「競馬場×距離×馬場状態×モデル」ごとの
    ROI・的中率を集計して返す。最低3件以上のデータがある条件のみ出力。
    """
    import datetime

    base_from = """
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE r.date >= date('now', '-2 years')
          AND pr.id IS NOT NULL
    """

    dist_case = """
        CASE
            WHEN r.distance IS NULL OR r.distance = 0 THEN '不明'
            WHEN r.distance < 1400  THEN '短距離(<1400m)'
            WHEN r.distance <= 1800 THEN 'マイル(1400-1800m)'
            WHEN r.distance <= 2200 THEN '中距離(1801-2200m)'
            ELSE '長距離(>2200m)'
        END
    """

    def _agg(group_expr: str, group_alias: str) -> list[dict]:
        sql = f"""
        SELECT
            ({group_expr}) AS {group_alias},
            p.model_type,
            COUNT(pr.id)  AS total_bets,
            COALESCE(SUM(pr.is_hit), 0) AS hits,
            ROUND(CAST(SUM(pr.is_hit) AS REAL)
                  / NULLIF(COUNT(pr.id), 0) * 100, 1) AS hit_rate,
            COALESCE(SUM(p.recommended_bet), 0)  AS total_invested,
            COALESCE(SUM(pr.payout), 0)          AS total_payout,
            ROUND(COALESCE(SUM(pr.payout), 0)
                  / NULLIF(SUM(p.recommended_bet), 0) * 100, 1) AS roi
        {base_from}
        GROUP BY ({group_expr}), p.model_type
        HAVING COUNT(pr.id) >= 3
        ORDER BY roi DESC
        """
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    by_venue     = _agg("r.venue",                           "venue")
    by_distance  = _agg(dist_case,                            "distance_cat")
    by_surface   = _agg("COALESCE(r.surface, '不明')",       "surface")
    by_condition = _agg("COALESCE(r.condition, '不明')",     "track_condition")

    combined_sql = f"""
    SELECT
        r.venue,
        ({dist_case})                        AS distance_cat,
        COALESCE(r.surface, '不明')          AS surface,
        COALESCE(r.condition, '不明')        AS track_condition,
        p.model_type,
        COUNT(pr.id)                         AS total_bets,
        COALESCE(SUM(pr.is_hit), 0)          AS hits,
        ROUND(CAST(SUM(pr.is_hit) AS REAL)
              / NULLIF(COUNT(pr.id), 0) * 100, 1) AS hit_rate,
        COALESCE(SUM(p.recommended_bet), 0)  AS total_invested,
        COALESCE(SUM(pr.payout), 0)          AS total_payout,
        ROUND(COALESCE(SUM(pr.payout), 0)
              / NULLIF(SUM(p.recommended_bet), 0) * 100, 1) AS roi
    {base_from}
    GROUP BY r.venue, distance_cat, r.surface, r.condition, p.model_type
    HAVING COUNT(pr.id) >= 3
    ORDER BY roi DESC
    LIMIT 200
    """
    cur = conn.execute(combined_sql)
    cols = [d[0] for d in cur.description]
    combined = [dict(zip(cols, row)) for row in cur.fetchall()]

    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "by_venue":      by_venue,
        "by_distance":   by_distance,
        "by_surface":    by_surface,
        "by_condition":  by_condition,
        "combined":      combined,
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

    # ── financial.json ─────────────────────────────────────────────
    financial = export_financial(conn)
    (OUT_DIR / "financial.json").write_text(
        json.dumps(financial, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    n_models = sum(len(v) for v in financial.values())
    print(f"financial.json:   {n_models:5d} 日×モデルレコード")

    # ── gachi_hits.json（ガチ予想・Oracle的中実績）──────────────────
    gachi = export_gachi_hits(conn)
    (OUT_DIR / "gachi_hits.json").write_text(
        json.dumps(gachi, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"gachi_hits.json:  {len(gachi):5d} 件")

    # ── win5.json（WIN5 SBCランク）──────────────────────────────────
    win5 = export_win5_data(conn)
    (OUT_DIR / "win5.json").write_text(
        json.dumps(win5, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"win5.json:        {len(win5):5d} 日付分")

    # ── condition_analysis.json（得意条件分析）─────────────────────
    condition = export_condition_analysis(conn)
    (OUT_DIR / "condition_analysis.json").write_text(
        json.dumps(condition, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"condition_analysis.json: {len(condition['combined']):3d} 条件")

    conn.close()
    print(f"\nエクスポート先: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
