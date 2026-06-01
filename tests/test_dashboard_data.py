"""src/web/dashboard_data.py のデータ層ユニットテスト。

一時 SQLite に最小スキーマ（DDL_STATEMENTS）を投入し、ダッシュボードが描画に使う
3 つの純データ関数（recent_results / top_ev_horses / model_roi_table）の
観測可能な振る舞いを検証する。Streamlit / Plotly には一切依存しない。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest

from src.database.schema import DDL_STATEMENTS
from src.web import dashboard_data as dd


@pytest.fixture()
def conn() -> Iterator[sqlite3.Connection]:
    """DDL_STATEMENTS で構築した空の一時 DB 接続を返す。"""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    for ddl in DDL_STATEMENTS:
        c.execute(ddl)
    c.commit()
    yield c
    c.close()


def _add_race(
    c: sqlite3.Connection,
    race_id: str,
    date: str,
    venue: str,
    race_number: int,
    race_name: str = "テストレース",
) -> None:
    c.execute(
        "INSERT INTO races(race_id, race_name, date, venue, race_number, "
        "distance, surface) VALUES(?,?,?,?,?,?,?)",
        (race_id, race_name, date, venue, race_number, 1600, "芝"),
    )


def _add_result(
    c: sqlite3.Connection,
    race_id: str,
    horse_name: str,
    rank: int | None,
    horse_number: int,
    win_odds: float | None = None,
    popularity: int | None = None,
) -> None:
    c.execute(
        "INSERT INTO race_results(race_id, horse_name, rank, horse_number, "
        "win_odds, popularity) VALUES(?,?,?,?,?,?)",
        (race_id, horse_name, rank, horse_number, win_odds, popularity),
    )


def _add_prediction(
    c: sqlite3.Connection,
    race_id: str,
    model_type: str,
    bet_type: str,
    expected_value: float,
    horse_name: str,
    *,
    is_superseded: int = 0,
    confidence: float = 0.5,
    model_score: float = 1.0,
) -> int:
    cur = c.execute(
        "INSERT INTO predictions(race_id, model_type, bet_type, confidence, "
        "expected_value, is_superseded) VALUES(?,?,?,?,?,?)",
        (race_id, model_type, bet_type, confidence, expected_value, is_superseded),
    )
    pred_id = int(cur.lastrowid or 0)
    c.execute(
        "INSERT INTO prediction_horses(prediction_id, horse_name, predicted_rank, "
        "model_score, ev_score) VALUES(?,?,?,?,?)",
        (pred_id, horse_name, 1, model_score, expected_value),
    )
    return pred_id


def _add_pred_result(
    c: sqlite3.Connection,
    prediction_id: int,
    is_hit: int,
    payout: float,
    profit: float,
) -> None:
    c.execute(
        "INSERT INTO prediction_results(prediction_id, is_hit, payout, profit) "
        "VALUES(?,?,?,?)",
        (prediction_id, is_hit, payout, profit),
    )


# ── recent_results ───────────────────────────────────────────────────────────


def test_recent_results_returns_winner_newest_first(conn: sqlite3.Connection) -> None:
    """直近の確定レースが新しい日付順で 1 着馬とともに返る。"""
    _add_race(conn, "R_OLD", "2026-05-30", "東京", 11, "古いレース")
    _add_result(conn, "R_OLD", "オールドホース", 1, 5, 3.2, 1)
    _add_race(conn, "R_NEW", "2026-05-31", "中山", 11, "新しいレース")
    _add_result(conn, "R_NEW", "ニューホース", 1, 7, 8.4, 4)
    conn.commit()

    rows = dd.recent_results(conn, limit=10)

    assert len(rows) == 2
    # 新しい日付（2026-05-31）が先頭。
    assert rows[0]["race_id"] == "R_NEW"
    assert rows[0]["winner"] == "ニューホース"
    assert rows[0]["win_odds"] == 8.4
    assert rows[1]["race_id"] == "R_OLD"


def test_recent_results_excludes_races_without_winner(
    conn: sqlite3.Connection,
) -> None:
    """1 着馬が未確定（rank=1 不在）のレースは含めない。"""
    _add_race(conn, "R_DONE", "2026-05-31", "東京", 1)
    _add_result(conn, "R_DONE", "勝ち馬", 1, 3, 2.0, 1)
    _add_race(conn, "R_PENDING", "2026-05-31", "東京", 2)
    _add_result(conn, "R_PENDING", "中止馬", None, 4, None, None)
    conn.commit()

    rows = dd.recent_results(conn, limit=10)

    ids = {r["race_id"] for r in rows}
    assert ids == {"R_DONE"}


def test_recent_results_respects_limit(conn: sqlite3.Connection) -> None:
    """limit を超える件数は返さない。"""
    for i in range(5):
        rid = f"R{i}"
        _add_race(conn, rid, "2026-05-31", "東京", i + 1)
        _add_result(conn, rid, f"馬{i}", 1, i + 1, 2.0, 1)
    conn.commit()

    rows = dd.recent_results(conn, limit=3)

    assert len(rows) == 3


# ── top_ev_horses ────────────────────────────────────────────────────────────


def test_top_ev_horses_sorted_by_ev_desc(conn: sqlite3.Connection) -> None:
    """指定日の予想が期待値の高い順で返る。"""
    _add_race(conn, "RA", "2026-05-31", "東京", 11)
    _add_race(conn, "RB", "2026-05-31", "中山", 11)
    _add_prediction(conn, "RA", "本命(直前)", "単勝", 1.20, "低EV馬")
    _add_prediction(conn, "RB", "Pure_EV_Edge", "複勝", 1.85, "高EV馬")
    conn.commit()

    rows = dd.top_ev_horses(conn, target_date="2026-05-31", limit=10)

    assert [r["horse_name"] for r in rows] == ["高EV馬", "低EV馬"]
    assert rows[0]["expected_value"] == 1.85
    assert rows[0]["venue"] == "中山"


def test_top_ev_horses_excludes_superseded(conn: sqlite3.Connection) -> None:
    """is_superseded=1（再推論で無効化）の予想は除外する。"""
    _add_race(conn, "RA", "2026-05-31", "東京", 11)
    _add_prediction(conn, "RA", "本命(直前)", "単勝", 2.5, "無効馬", is_superseded=1)
    _add_prediction(conn, "RA", "卍(直前)", "複勝", 1.3, "有効馬")
    conn.commit()

    rows = dd.top_ev_horses(conn, target_date="2026-05-31", limit=10)

    assert [r["horse_name"] for r in rows] == ["有効馬"]


def test_top_ev_horses_defaults_to_latest_date(conn: sqlite3.Connection) -> None:
    """target_date 未指定時は予想が存在する最新日を採用する。"""
    _add_race(conn, "ROLD", "2026-05-30", "東京", 11)
    _add_race(conn, "RNEW", "2026-05-31", "中山", 11)
    _add_prediction(conn, "ROLD", "本命(直前)", "単勝", 3.0, "昨日の馬")
    _add_prediction(conn, "RNEW", "本命(直前)", "単勝", 1.1, "今日の馬")
    conn.commit()

    rows = dd.top_ev_horses(conn, limit=10)

    assert [r["horse_name"] for r in rows] == ["今日の馬"]


# ── model_roi_table ──────────────────────────────────────────────────────────


def test_model_roi_table_computes_per_model_roi(conn: sqlite3.Connection) -> None:
    """実弾モデルのモデル別 ROI が確定 P&L から算出される。"""
    _add_race(conn, "RA", "2026-05-31", "東京", 11)
    # 本命(直前)×単勝 は実弾。コスト=payout-profit=100、payout=250 → ROI=250%。
    pid = _add_prediction(conn, "RA", "本命(直前)", "単勝", 1.5, "勝ち馬")
    _add_pred_result(conn, pid, is_hit=1, payout=250.0, profit=150.0)
    conn.commit()

    rows = dd.model_roi_table(conn)

    honmei = [r for r in rows if r["model_type"] == "本命(直前)"]
    assert len(honmei) == 1
    assert honmei[0]["n"] == 1
    assert honmei[0]["roi"] == 250.0
    assert honmei[0]["hit_rate"] == 100.0


def test_model_roi_table_excludes_appreciation_models(
    conn: sqlite3.Connection,
) -> None:
    """観賞用モデル（Oracle 等）は live_only 時に実弾集計から除外される。"""
    _add_race(conn, "RA", "2026-05-31", "東京", 11)
    pid = _add_prediction(conn, "RA", "Oracle(直前)", "単勝", 1.5, "観賞馬")
    _add_pred_result(conn, pid, is_hit=0, payout=0.0, profit=-100.0)
    conn.commit()

    rows = dd.model_roi_table(conn, live_only=True)

    assert all(r["model_type"] != "Oracle(直前)" for r in rows)


def test_model_roi_table_empty_when_no_results(conn: sqlite3.Connection) -> None:
    """確定結果が無ければ空リストを返す（例外を投げない）。"""
    rows = dd.model_roi_table(conn)
    assert rows == []
