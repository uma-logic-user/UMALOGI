"""前走詳細・同コース実績 特徴量（src/features/prerun.py）のテスト。

リークフリー（現レース日より前の出走のみ参照）を最重要に検証する。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.features.prerun import build_prerun_features, PRERUN_FEATURE_COLS


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.execute(
        "CREATE TABLE races(race_id TEXT PRIMARY KEY, date TEXT, venue TEXT, "
        "surface TEXT, distance INTEGER)"
    )
    c.execute(
        "CREATE TABLE race_results(race_id TEXT, horse_number INTEGER, horse_id TEXT, "
        "horse_name TEXT, rank INTEGER, margin TEXT, last_3f REAL, finish_time TEXT)"
    )
    # 対象レース（東京・芝1600・2026-06-07）
    c.execute("INSERT INTO races VALUES('R_TARGET','2026-06-07','東京','芝',1600)")
    # 馬Aの過去2走（東京芝1600で1着・東京芝1600で3着）＋ 対象レース出走
    c.execute("INSERT INTO races VALUES('R_PAST1','2026-05-01','東京','芝',1600)")
    c.execute("INSERT INTO races VALUES('R_PAST2','2026-03-01','東京','芝',1600)")
    # 未来のレース（リークチェック用・対象より後）
    c.execute("INSERT INTO races VALUES('R_FUTURE','2026-07-01','東京','芝',1600)")

    # 馬A (horse_id=H_A)
    c.execute(
        "INSERT INTO race_results VALUES('R_TARGET',1,'H_A','馬A',NULL,NULL,NULL,NULL)"
    )
    c.execute(
        "INSERT INTO race_results VALUES('R_PAST1',5,'H_A','馬A',1,'0.0',33.5,'1:33.0')"
    )
    c.execute(
        "INSERT INTO race_results VALUES('R_PAST2',2,'H_A','馬A',3,'0.3',34.0,'1:33.5')"
    )
    # リーク源: 未来レースで大敗（特徴量に絶対混入してはならない）
    c.execute(
        "INSERT INTO race_results VALUES('R_FUTURE',1,'H_A','馬A',18,'5.0',40.0,'1:40.0')"
    )

    # 馬B (過去走なし=初出走)
    c.execute(
        "INSERT INTO race_results VALUES('R_TARGET',2,'H_B','馬B',NULL,NULL,NULL,NULL)"
    )
    c.commit()
    return c


def test_prerun_feature_cols_defined() -> None:
    assert "prev_last_3f_sec" in PRERUN_FEATURE_COLS
    assert "prev_rank" in PRERUN_FEATURE_COLS
    assert "same_course_place_rate" in PRERUN_FEATURE_COLS


def test_prev_run_uses_most_recent_past_only(conn: sqlite3.Connection) -> None:
    df = build_prerun_features(conn, "R_TARGET")
    a = df[df["horse_number"] == 1].iloc[0]
    # 直近の過去走 = R_PAST1（2026-05-01・1着・上がり33.5）
    assert a["prev_rank"] == 1
    assert abs(a["prev_last_3f_sec"] - 33.5) < 1e-6


def test_no_future_leak(conn: sqlite3.Connection) -> None:
    df = build_prerun_features(conn, "R_TARGET")
    a = df[df["horse_number"] == 1].iloc[0]
    # 未来レース(R_FUTURE・18着・上がり40.0)が混入していないこと
    assert a["prev_rank"] != 18
    assert a["prev_last_3f_sec"] != 40.0


def test_same_course_place_rate(conn: sqlite3.Connection) -> None:
    df = build_prerun_features(conn, "R_TARGET")
    a = df[df["horse_number"] == 1].iloc[0]
    # 東京芝 過去2走とも複勝圏(1着,3着) → place_rate=1.0, runs=2
    assert a["same_course_runs"] == 2
    assert abs(a["same_course_place_rate"] - 1.0) < 1e-6


def test_first_time_starter_safe_defaults(conn: sqlite3.Connection) -> None:
    df = build_prerun_features(conn, "R_TARGET")
    b = df[df["horse_number"] == 2].iloc[0]
    # 初出走馬: 過去走なし → NaN/0 で安全に返す（例外を出さない）
    assert b["same_course_runs"] == 0
    import math

    assert math.isnan(b["prev_last_3f_sec"]) or b["prev_last_3f_sec"] is None


def test_returns_all_runners(conn: sqlite3.Connection) -> None:
    df = build_prerun_features(conn, "R_TARGET")
    assert set(df["horse_number"]) == {1, 2}
