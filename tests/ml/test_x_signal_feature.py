"""
x_signal_parser.get_x_consensus_score() と
FEATURE_COLS への x_consensus_score 追加の検証。
"""

from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture()
def feature_db() -> sqlite3.Connection:
    """x_signals テーブルを持つインメモリ DB。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE x_signals (
            signal_id    INTEGER PRIMARY KEY,
            tweet_id     TEXT,
            race_id      TEXT,
            screen_name  TEXT,
            horse_number INTEGER,
            signal_type  TEXT,
            confidence   REAL,
            race_name_raw TEXT,
            raw_text     TEXT,
            posted_at    TEXT,
            fetched_at   TEXT,
            parsed       INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE x_accounts (
            id          INTEGER PRIMARY KEY,
            screen_name TEXT UNIQUE,
            weight      REAL DEFAULT 1.0
        )
    """)
    # horse_number=5 に 2件の honmei シグナル (confidence 0.85, 0.70)
    conn.execute(
        "INSERT INTO x_signals VALUES (1,'t1','202606050511','user_a',5,'honmei',0.85,'','',NULL,NULL,1)"
    )
    conn.execute(
        "INSERT INTO x_signals VALUES (2,'t2','202606050511','user_b',5,'honmei',0.70,'','',NULL,NULL,1)"
    )
    # horse_number=9 に 1件の ana シグナル
    conn.execute(
        "INSERT INTO x_signals VALUES (3,'t3','202606050511','user_a',9,'ana',0.60,'','',NULL,NULL,1)"
    )
    conn.commit()
    return conn


def test_x_consensus_score_aggregation(feature_db: sqlite3.Connection) -> None:
    """horse_number=5 の x_consensus_score が honmei 方向込みの加重平均で計算される。

    direction(honmei)=1.0, weight=1.0 のとき:
        score = (1.0*0.85*1.0 + 1.0*0.70*1.0) / (1.0+1.0) = 0.775
    """
    from src.ml.x_signal_parser import get_x_consensus_score

    scores = get_x_consensus_score(feature_db, race_id="202606050511")
    assert 5 in scores
    assert abs(scores[5] - 0.775) < 0.01


def test_x_consensus_score_missing_race(feature_db: sqlite3.Connection) -> None:
    """x_signals にないレースは空 dict を返す。"""
    from src.ml.x_signal_parser import get_x_consensus_score

    scores = get_x_consensus_score(feature_db, race_id="000000000000")
    assert scores == {}


def test_x_consensus_score_in_feature_cols() -> None:
    """x_consensus_score が FEATURE_COLS に含まれること。"""
    from src.ml.models import FEATURE_COLS

    assert "x_consensus_score" in FEATURE_COLS
