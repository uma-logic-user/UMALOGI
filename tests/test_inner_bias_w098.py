"""
W-098 — 内枠複勝バイアス z スコアのリークフリー回帰テスト

検証:
  - today_inner_bias は対象レースより前(race_number小)の同日確定結果のみ参照（自/後続は不参照）。
  - yesterday_inner_bias は直近前開催日の全レースを参照。
  - 内枠複勝率が基準より高い日は z>0、低い日は z<0。
  - リークが無い: 後続レースの結果を変えても対象レースの today_bias は不変。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.features.inner_bias import build_daily_inner_index


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT, race_number INTEGER);
        CREATE TABLE race_results (
            race_id TEXT, gate_number INTEGER, rank INTEGER
        );
        """
    )
    return c


def _add_race(c, race_id, date, rn, results):
    """results = list[(gate_number, rank)]。"""
    c.execute("INSERT INTO races VALUES (?,?,?)", (race_id, date, rn))
    for g, rk in results:
        c.execute("INSERT INTO race_results VALUES (?,?,?)", (race_id, g, rk))


def _strong_inner_day(c, date):
    """内枠(1-3)が複勝を独占する日（内枠バイアス強）。12R 分。"""
    for rn in range(1, 13):
        # 8頭立て: 枠1-3が1-2-3着、枠4-8は着外
        res = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)]
        _add_race(c, f"{date.replace('-', '')}{rn:02d}", date, rn, res)


def _neutral_day(c, date):
    """内枠が平均的（外枠も来る）な日。"""
    for rn in range(1, 13):
        res = [(1, 5), (2, 2), (3, 8), (4, 1), (5, 3), (6, 6), (7, 4), (8, 7)]
        _add_race(c, f"{date.replace('-', '')}{rn:02d}", date, rn, res)


def test_yesterday_bias_high_when_inner_dominated(conn) -> None:
    # 6/01 内枠独占, 6/02 中立日。基準は中立寄りなので 6/01 は z>0。
    _neutral_day(conn, "2026-05-30")
    _strong_inner_day(conn, "2026-06-01")
    _neutral_day(conn, "2026-06-02")
    conn.commit()
    idx = build_daily_inner_index(conn, "2026-05-01", "2026-07-01")
    # 6/02 の yesterday は 6/01(内枠独占) → 高い z
    z = idx.yesterday_bias_z("2026-06-02")
    assert z > 0.5


def test_today_bias_uses_only_earlier_races(conn) -> None:
    _neutral_day(conn, "2026-06-01")  # 基準用の前日
    _strong_inner_day(conn, "2026-06-02")
    conn.commit()
    idx = build_daily_inner_index(conn, "2026-05-01", "2026-07-01")
    # 6/02 の 12R 時点では 1-11R(内枠独占)が既走 → today_bias 高
    z_late = idx.today_bias_z("2026-06-02", 12)
    assert z_late > 0.5
    # 6/02 の 1R 時点では既走レース0 → neutral(0)
    z_first = idx.today_bias_z("2026-06-02", 1)
    assert z_first == 0.0


def test_leakfree_future_results_do_not_affect_today(conn) -> None:
    """後続レースの結果を変えても、対象レース時点の today_bias は不変（リーク無し）。"""
    _neutral_day(conn, "2026-06-01")
    _strong_inner_day(conn, "2026-06-02")
    conn.commit()
    # 基準は対象日(6/02)より前に限定（研究では reference_hi=cutoff）＝同日のリークを排除。
    idx1 = build_daily_inner_index(
        conn, "2026-05-01", "2026-07-01", reference_hi="2026-06-02"
    )
    z_before = idx1.today_bias_z("2026-06-02", 6)  # 1-5R のみ参照

    # 後続(7-12R)の結果を破壊的に変更（外枠勝ちに）
    conn.execute(
        "UPDATE race_results SET rank = CASE gate_number WHEN 8 THEN 1 WHEN 1 THEN 8 ELSE rank END "
        "WHERE race_id IN (SELECT race_id FROM races WHERE date='2026-06-02' AND race_number>=7)"
    )
    conn.commit()
    idx2 = build_daily_inner_index(
        conn, "2026-05-01", "2026-07-01", reference_hi="2026-06-02"
    )
    z_after = idx2.today_bias_z("2026-06-02", 6)
    assert z_before == z_after  # 6R 時点の値は後続変更の影響を受けない


def test_no_prior_day_returns_zero(conn) -> None:
    _strong_inner_day(conn, "2026-06-01")
    conn.commit()
    idx = build_daily_inner_index(conn, "2026-05-01", "2026-07-01")
    # 6/01 は最初の開催日 → yesterday 無し → 0
    assert idx.yesterday_bias_z("2026-06-01") == 0.0
