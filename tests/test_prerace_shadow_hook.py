"""
tests/test_prerace_shadow_hook.py — 直前パイプラインの SNS シャドー配線（best-effort）テスト

検証項目:
  1. _maybe_save_shadow が DB 内スコア再利用でシャドー多券種を predictions へ追記する
  2. SHADOW_SNS_ENABLE=0 で完全無効化される（緊急停止弁）
  3. 内部で例外が発生しても再送出せず 0 を返す（実弾予想を絶対に止めない best-effort 隔離）
  4. 既存 live 予想を一切変更しない（条項1）
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.database.init_db import init_db, insert_prediction
from src.pipeline.prediction import _maybe_save_shadow


@pytest.fixture()
def conn(tmp_path: Path) -> sqlite3.Connection:
    db = tmp_path / "shadow_hook.db"
    c = init_db(db)
    c.execute("PRAGMA journal_mode=DELETE")
    yield c
    c.close()


def _insert_race(c: sqlite3.Connection, race_id: str) -> None:
    c.execute(
        "INSERT OR IGNORE INTO races "
        "(race_id, race_name, date, venue, race_number, distance, surface) "
        "VALUES (?, 'テストレース', '2026-06-14', '東京', 11, 1600, '芝')",
        (race_id,),
    )
    c.commit()


def _df(n: int = 8) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "horse_number": list(range(1, n + 1)),
            "horse_name": [f"テスト馬{i}" for i in range(1, n + 1)],
            "win_odds": [2.5, 4.0, 6.0, 9.0, 12.0, 18.0, 25.0, 40.0][:n],
        }
    )


def _scores(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    honmei = pd.Series([0.30, 0.20, 0.15, 0.12, 0.10, 0.06, 0.04, 0.03], index=df.index)
    ev = pd.Series([1.5, 1.3, 1.2, 1.1, 1.05, 0.9, 0.8, 0.7], index=df.index)
    return honmei, ev


def _shadow_count(c: sqlite3.Connection, race_id: str) -> int:
    return c.execute(
        "SELECT COUNT(*) FROM predictions WHERE race_id = ? AND model_type LIKE '%_v0525%'",
        (race_id,),
    ).fetchone()[0]


def test_maybe_save_shadow_appends_multi_tickets(conn: sqlite3.Connection) -> None:
    race_id = "202699060101"
    _insert_race(conn, race_id)
    df = _df()
    honmei, ev = _scores(df)

    n = _maybe_save_shadow(conn, race_id, df, honmei, ev)

    assert n > 0
    assert _shadow_count(conn, race_id) > 0


def test_maybe_save_shadow_disabled_by_env(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHADOW_SNS_ENABLE", "0")
    race_id = "202699060102"
    _insert_race(conn, race_id)
    df = _df()
    honmei, ev = _scores(df)

    n = _maybe_save_shadow(conn, race_id, df, honmei, ev)

    assert n == 0
    assert _shadow_count(conn, race_id) == 0


def test_maybe_save_shadow_is_best_effort(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """生成段で例外が起きても再送出せず 0 を返す（実弾を止めない）。"""
    race_id = "202699060103"
    _insert_race(conn, race_id)
    df = _df()
    honmei, ev = _scores(df)

    def _boom(*_a: object, **_k: object) -> dict:
        raise RuntimeError("shadow generation failed")

    monkeypatch.setattr("src.pipeline.prediction.generate_shadow_bets", _boom)

    # 例外を送出してはならない
    n = _maybe_save_shadow(conn, race_id, df, honmei, ev)
    assert n == 0


def test_maybe_save_shadow_keeps_live_intact(conn: sqlite3.Connection) -> None:
    race_id = "202699060104"
    _insert_race(conn, race_id)
    live_pid = insert_prediction(
        conn,
        race_id=race_id,
        model_type="卍(直前)",
        bet_type="複勝",
        horses=[{"horse_number": 3, "horse_name": "live馬", "predicted_rank": 1}],
        confidence=0.7,
        expected_value=1.4,
        recommended_bet=300,
        combination_json="[[3]]",
    )
    before = conn.execute(
        "SELECT model_type, bet_type, expected_value FROM predictions WHERE id = ?",
        (live_pid,),
    ).fetchone()

    df = _df()
    honmei, ev = _scores(df)
    _maybe_save_shadow(conn, race_id, df, honmei, ev)

    after = conn.execute(
        "SELECT model_type, bet_type, expected_value FROM predictions WHERE id = ?",
        (live_pid,),
    ).fetchone()
    assert before == after
