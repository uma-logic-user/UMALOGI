"""src/web/dashboard_data.py — ダッシュボード描画用の純データ層。

Streamlit / Plotly に依存しない（テスト容易性のため UI と完全分離）。
すべての関数は `sqlite3.Connection` を注入で受け取り、プリミティブな
`list[dict]` を返す。ROI 集計は唯一の正準ロジックである
`src.ml.pnl_accounting.compute_live_roi` を再利用し、会計基準の二重定義を避ける。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from src.ml.pnl_accounting import compute_live_roi


def recent_results(
    conn: sqlite3.Connection, *, limit: int = 20
) -> list[dict[str, Any]]:
    """直近の確定レース（1 着馬が判明済み）を新しい日付順で返す。

    競走中止（rank IS NULL）や未確定レースは 1 着不在のため自動的に除外される。
    同着（rank=1 が複数）の場合は該当馬すべてを別行で返す。

    Args:
        conn:  DB 接続。
        limit: 返却する最大行数。

    Returns:
        各行: race_id / date / venue / race_number / race_name /
              winner / horse_number / win_odds / popularity。
    """
    rows = conn.execute(
        """
        SELECT r.race_id, r.date, r.venue, r.race_number, r.race_name,
               rr.horse_name AS winner, rr.horse_number,
               rr.win_odds, rr.popularity
          FROM races r
          JOIN race_results rr
            ON rr.race_id = r.race_id AND rr.rank = 1
         ORDER BY r.date DESC, r.venue, r.race_number DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def latest_prediction_date(conn: sqlite3.Connection) -> str | None:
    """有効な予想（is_superseded=0）が存在する最新のレース日を返す。

    Args:
        conn: DB 接続。

    Returns:
        "YYYY-MM-DD"。予想が 1 件も無ければ None。
    """
    row = conn.execute(
        """
        SELECT MAX(r.date)
          FROM predictions p
          JOIN races r ON r.race_id = p.race_id
         WHERE COALESCE(p.is_superseded, 0) = 0
        """
    ).fetchone()
    value = row[0] if row else None
    return str(value) if value else None


def top_ev_horses(
    conn: sqlite3.Connection,
    *,
    target_date: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """指定日の予想を期待値（expected_value）の高い順で返す。

    1 予想（1 レース×1 モデル×1 券種）につき、最有力馬（model_score 最大、
    同点なら predicted_rank 最小）の 1 頭を代表として 1 行返す。
    再推論で無効化された予想（is_superseded=1）と expected_value が NULL の
    予想は除外する。

    Args:
        conn:        DB 接続。
        target_date: "YYYY-MM-DD"。None なら有効な予想がある最新日を採用。
        limit:       返却する最大行数。

    Returns:
        各行: race_id / venue / race_number / model_type / bet_type /
              confidence / expected_value / horse_name / ev_score。
              該当日が無ければ空リスト。
    """
    if target_date is None:
        target_date = latest_prediction_date(conn)
    if target_date is None:
        return []

    rows = conn.execute(
        """
        SELECT race_id, venue, race_number, model_type, bet_type,
               confidence, expected_value, horse_name, ev_score
          FROM (
            SELECT p.race_id, r.venue, r.race_number, p.model_type, p.bet_type,
                   p.confidence, p.expected_value, ph.horse_name, ph.ev_score,
                   ROW_NUMBER() OVER (
                       PARTITION BY p.id
                       ORDER BY ph.model_score DESC, ph.predicted_rank ASC
                   ) AS rn
              FROM predictions p
              JOIN races r ON r.race_id = p.race_id
              LEFT JOIN prediction_horses ph ON ph.prediction_id = p.id
             WHERE r.date = ?
               AND COALESCE(p.is_superseded, 0) = 0
               AND p.expected_value IS NOT NULL
          )
         WHERE rn = 1
         ORDER BY expected_value DESC, race_number
         LIMIT ?
        """,
        (target_date, limit),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def model_roi_table(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
    live_only: bool = True,
) -> list[dict[str, Any]]:
    """モデル別の確定 ROI 一覧を返す（正準ロジック compute_live_roi を再利用）。

    Args:
        conn:      DB 接続。
        since:     "YYYY-MM-DD"。指定時は created_at >= since のみ集計。
        live_only: True なら実弾（単複・実弾モデル）のみ。False なら全予想。

    Returns:
        消化数（n）の多い順のモデル別行。各行:
        model_type / n / roi / hit_rate / payout / cost / profit。
        集計対象が無ければ空リスト。
    """
    summary = compute_live_roi(conn, since=since, live_only=live_only)
    by_model: dict[str, dict[str, Any]] = summary.get("by_model", {})
    rows = [{"model_type": model, **stats} for model, stats in by_model.items()]
    rows.sort(key=lambda r: r["n"], reverse=True)
    return rows


def _row_to_dict(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
    """sqlite3.Row（または keys() を持つ行）を通常の dict へ変換する。"""
    if isinstance(row, sqlite3.Row):
        return {k: row[k] for k in row.keys()}
    raise TypeError("row_factory=sqlite3.Row が設定された接続が必要です")
