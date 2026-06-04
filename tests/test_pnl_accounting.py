"""pnl_accounting.compute_live_roi のテスト（真コスト基準・実弾フィルタ）。"""

from __future__ import annotations

import sqlite3

import pytest

from src.ml.pnl_accounting import compute_live_roi


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
        (pid, "R1", model, bet, "2026-05-31 10:00", superseded),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id,is_hit,payout,profit) VALUES(?,?,?,?)",
        (pid, is_hit, payout, profit),
    )


def test_live_roi_excludes_exotics_and_ornamental(conn) -> None:
    # 実弾: 単勝 当たり(cost=100, payout=400) / 複勝 外れ(cost=300, payout=0)
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 400, 300, 1)  # cost=100
    _add(conn, 2, "Pure_EV_Edge(直前)", "複勝", 0, -300, 0)  # cost=300
    # 観賞用・三連系（除外されるべき大損）
    _add(conn, 3, "Oracle(直前)", "三連単", 0, -5000, 0)
    _add(conn, 4, "Pure_EV_Edge(直前)", "三連単", 0, -2000, 0)
    conn.commit()

    r = compute_live_roi(conn, live_only=True)
    # 実弾のみ: cost=100+300=400, payout=400+0=400, profit=400-400=0, roi=100%
    assert r["n"] == 2
    assert r["cost"] == 400
    assert r["payout"] == 400
    assert r["profit"] == 0
    assert r["roi"] == 100.0
    assert set(r["by_bet_type"].keys()) == {"単勝", "複勝"}


def test_live_roi_excludes_superseded(conn) -> None:
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1)  # 有効 cost=100
    _add(conn, 2, "Pure_EV_Edge(直前)", "単勝", 0, -100, 0, superseded=1)  # 無効化→除外
    conn.commit()
    r = compute_live_roi(conn, live_only=True)
    assert r["n"] == 1
    assert r["profit"] == 400


def test_live_only_false_includes_all(conn) -> None:
    _add(conn, 1, "Pure_EV_Edge(直前)", "単勝", 400, 300, 1)
    _add(conn, 2, "Oracle(直前)", "三連単", 0, -5000, 0)
    conn.commit()
    r = compute_live_roi(conn, live_only=False)
    assert r["n"] == 2
    assert r["profit"] == 300 - 5000
