"""sns_publisher の DB 連携グルー（detect_and_flash / compute_ornamental_weekly_stats 期間版）テスト。"""

from __future__ import annotations

import sqlite3

import pytest

from src.ops import sns_publisher as SP


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT);
        CREATE TABLE predictions (id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT,
            bet_type TEXT, combination_json TEXT, created_at TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE prediction_results (id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout REAL, profit REAL);
        """
    )
    return c


def _row(
    c,
    pid,
    model,
    bet,
    payout,
    profit,
    hit,
    *,
    combo="[[5]]",
    rid="R",
    date="2026-06-01",
):
    c.execute("INSERT OR IGNORE INTO races VALUES (?,?)", (rid, date))
    c.execute(
        "INSERT INTO predictions(id,race_id,model_type,bet_type,combination_json,created_at,is_superseded)"
        " VALUES(?,?,?,?,?,?,0)",
        (pid, rid, model, bet, combo, date + " 10:00"),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id,is_hit,payout,profit) VALUES(?,?,?,?)",
        (pid, hit, payout, profit),
    )


def test_detect_and_flash_ornamental_manbaiken(conn) -> None:
    _row(conn, 1, "Oracle(直前)", "三連単", 38500, 38400, 1, combo="[[3,9,12]]")
    _row(conn, 2, "本命(直前)", "単勝", 90000, 89900, 1)  # 実弾→集客速報外
    conn.commit()
    sent: list[str] = []
    out = SP.detect_and_flash(
        conn,
        "R",
        race_label="日本ダービー",
        venue="東京",
        sender=lambda t, ch: sent.append(t) or True,
    )
    assert len(out) == 1 and "万馬券" in out[0]
    assert len(sent) == 1


def test_detect_and_flash_below_threshold(conn) -> None:
    _row(conn, 1, "Oracle(直前)", "複勝", 900, -100, 1)  # ROI<150%・万馬券でない
    conn.commit()
    assert SP.detect_and_flash(conn, "R", sender=lambda t, ch: True) == []


def test_detect_and_flash_no_hit(conn) -> None:
    _row(conn, 1, "HitFocus(直前)", "三連複", 0, -600, 0)
    conn.commit()
    assert SP.detect_and_flash(conn, "R") == []


def test_weekly_stats_with_date_filter(conn) -> None:
    _row(conn, 1, "Oracle(直前)", "単勝", 4200, 4100, 1, rid="R1", date="2026-06-01")
    _row(
        conn, 2, "Oracle(直前)", "単勝", 0, -100, 0, rid="R2", date="2026-05-01"
    )  # 期間外
    conn.commit()
    stats = SP.compute_ornamental_weekly_stats(
        conn, start_date="2026-05-30", end_date="2026-06-07"
    )
    assert len(stats) == 1
    assert stats[0].n_bets == 1 and stats[0].best_payout == 4200
