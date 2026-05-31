"""src/data/jravan_pipeline.py のテスト（SSOT facade・オッズ空問題検知）。"""

from __future__ import annotations

import sqlite3

import pytest

from src.data.jravan_pipeline import (
    MIN_HEALTHY_SNAPSHOTS,
    coverage_report,
    odds_snapshot_health,
    sync_odds,
)


def _conn(snapshots: list[tuple]) -> sqlite3.Connection:
    """(race_id, horse_number, win_odds, recorded_at) から realtime_odds + races を作る。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT);
        CREATE TABLE realtime_odds (
            race_id TEXT, horse_number INTEGER, win_odds REAL, recorded_at TEXT
        );
        """
    )
    conn.executemany("INSERT INTO realtime_odds VALUES (?,?,?,?)", snapshots)
    conn.commit()
    return conn


# ── odds_snapshot_health ─────────────────────────────────────────────────────


def test_health_empty() -> None:
    conn = _conn([])
    h = odds_snapshot_health(conn, "R1")
    assert h.n_snapshots == 0 and not h.is_healthy and h.status == "empty"


def test_health_single_snapshot_is_unhealthy() -> None:
    conn = _conn([("R1", 1, 5.0, "t1"), ("R1", 2, 3.0, "t1")])
    h = odds_snapshot_health(conn, "R1")
    assert h.n_snapshots == 1 and h.n_horses == 2
    assert not h.is_healthy  # 1点では odds_drift が動かない
    assert h.status == "single"


def test_health_two_snapshots_is_healthy() -> None:
    conn = _conn(
        [("R1", 1, 5.0, "t1"), ("R1", 1, 4.0, "t2"), ("R1", 2, 3.0, "t1")]
    )
    h = odds_snapshot_health(conn, "R1")
    assert h.n_snapshots == 2 and h.is_healthy and h.status == "healthy"
    assert MIN_HEALTHY_SNAPSHOTS == 2


# ── coverage_report（オッズ空問題の自動検知）────────────────────────────────


def test_coverage_detects_empty_and_single() -> None:
    conn = _conn(
        [
            # R1: 2点（健全）
            ("R1", 1, 5.0, "t1"), ("R1", 1, 4.0, "t2"),
            # R2: 1点
            ("R2", 1, 6.0, "t1"),
            # R3: 0点（空）
        ]
    )
    for rid in ("R1", "R2", "R3"):
        conn.execute("INSERT INTO races VALUES (?, '2026-06-01')", (rid,))
    conn.commit()

    rep = coverage_report(conn, "2026-06-01")
    assert rep.n_races == 3
    assert rep.healthy == 1
    assert rep.single == 1
    assert rep.empty == 1
    assert rep.empty_race_ids == ("R3",)
    assert rep.single_race_ids == ("R2",)
    assert not rep.is_ok  # 劣化を検知


def test_coverage_all_healthy_is_ok() -> None:
    conn = _conn(
        [
            ("R1", 1, 5.0, "t1"), ("R1", 1, 4.0, "t2"),
            ("R2", 1, 6.0, "t1"), ("R2", 1, 5.5, "t2"),
        ]
    )
    for rid in ("R1", "R2"):
        conn.execute("INSERT INTO races VALUES (?, '2026-06-01')", (rid,))
    conn.commit()
    rep = coverage_report(conn, "2026-06-01")
    assert rep.is_ok and rep.healthy == 2


# ── sync_odds（取得後検証＝空問題の再発防止）─────────────────────────────────


def test_sync_odds_verify_detects_empty_after_fetch() -> None:
    # fetcher が「成功した」と言っても realtime_odds が空なら ok=False にする
    conn = _conn([])
    conn.execute("INSERT INTO races VALUES ('R1','2026-06-01')")
    conn.commit()

    def _liar_fetcher(_c: sqlite3.Connection, _rid: str) -> int:
        return 5  # 5件取得した、と嘘をつくが realtime_odds は空のまま

    res = sync_odds(conn, "R1", fetcher=_liar_fetcher, verify=True)
    assert not res.ok
    assert "空" in res.detail


def test_sync_odds_success_when_rows_present() -> None:
    conn = _conn([])
    conn.execute("INSERT INTO races VALUES ('R1','2026-06-01')")
    conn.commit()

    def _real_fetcher(c: sqlite3.Connection, rid: str) -> int:
        c.execute("INSERT INTO realtime_odds VALUES (?,1,5.0,'t1')", (rid,))
        c.commit()
        return 1

    res = sync_odds(conn, "R1", fetcher=_real_fetcher, verify=True)
    assert res.ok and res.n_records == 1


def test_sync_odds_handles_fetcher_exception() -> None:
    conn = _conn([])

    def _boom(_c: sqlite3.Connection, _rid: str) -> int:
        raise RuntimeError("JVLink down")

    res = sync_odds(conn, "R1", fetcher=_boom, verify=True)
    assert not res.ok and "JVLink down" in res.detail
