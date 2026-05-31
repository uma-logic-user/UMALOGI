"""W-057 シャドーA/B（Pure_EV_Edge vs 従来単複）と health_reporter 統合のテスト。"""

from __future__ import annotations

import sqlite3

import pytest

from src.ml.pnl_accounting import compute_ab_variants
from src.ops.health_reporter import format_ab_field, format_ab_text


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT, bet_type TEXT,
            created_at TEXT, is_superseded INTEGER DEFAULT 0
        );
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout REAL, profit REAL
        );
        """
    )
    return c


def _add(c, pid, model, bet, payout, profit, is_hit, *, superseded=0):
    c.execute(
        "INSERT INTO predictions(id,race_id,model_type,bet_type,created_at,is_superseded)"
        " VALUES(?,?,?,?,?,?)",
        (pid, "R", model, bet, "2026-06-01 10:00", superseded),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id,is_hit,payout,profit) VALUES(?,?,?,?)",
        (pid, is_hit, payout, profit),
    )


def test_ab_splits_pure_ev_vs_legacy(conn) -> None:
    # Pure_EV_Edge: 単勝当たり cost=100 payout=500 profit=400
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1)
    _add(conn, 2, "Pure_EV_Edge(直前)", "複勝", 0, -100, 0)  # cost=100
    # 従来単複: 本命/卍 単複（負け気味）
    _add(conn, 3, "本命(直前)", "単勝", 0, -100, 0)
    _add(conn, 4, "卍(直前)", "複勝", 150, 50, 1)  # cost=100
    # 観賞用/三連系は A/B から除外されるべき
    _add(conn, 5, "Oracle(直前)", "三連単", 0, -5000, 0)
    _add(conn, 6, "本命(直前)", "三連単", 0, -2000, 0)
    conn.commit()

    ab = compute_ab_variants(conn)
    # Pure_EV: cost=200 payout=500 profit=300 roi=250%
    assert ab["pure_ev"]["n"] == 2
    assert ab["pure_ev"]["profit"] == 300
    assert ab["pure_ev"]["roi"] == 250.0
    # legacy: cost=200 payout=150 profit=-50 roi=75%
    assert ab["legacy"]["n"] == 2
    assert ab["legacy"]["profit"] == -50
    assert ab["legacy"]["roi"] == 75.0
    assert ab["winner"] == "Pure_EV_Edge"
    assert ab["both_active"] is True
    assert ab["diff_profit"] == 350.0


def test_ab_excludes_superseded(conn) -> None:
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1)
    _add(conn, 2, "Pure_EV_Edge(直前)", "単勝", 0, -100, 0, superseded=1)  # 除外
    conn.commit()
    ab = compute_ab_variants(conn)
    assert ab["pure_ev"]["n"] == 1
    assert ab["pure_ev"]["profit"] == 400


def test_ab_winner_undetermined_when_one_side_empty(conn) -> None:
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1)
    conn.commit()
    ab = compute_ab_variants(conn)
    assert ab["both_active"] is False
    assert "判定不能" in ab["winner"]


def test_format_ab_field_and_text(conn) -> None:
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1)
    _add(conn, 2, "本命(直前)", "単勝", 0, -100, 0)
    conn.commit()
    ab = compute_ab_variants(conn)
    field = format_ab_field(ab)
    assert "name" in field and "value" in field and field["inline"] is False
    assert "W-057" in field["name"]
    txt = format_ab_text(ab)
    assert "PureEV" in txt and "従来単複" in txt
