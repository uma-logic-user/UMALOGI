import os
import sqlite3
import sys
import tempfile

import pandas as pd

sys.path.insert(0, ".")

from src.umasugi_engine.factors.odds_momentum import calc_odds_momentum_score


def test_neutral_when_no_timeseries():
    """時系列データがない場合は 0.5 中立を返す"""
    conn = sqlite3.connect("data/umalogi.db")
    df = pd.DataFrame([{"race_id": "FAKE_RACE_9999", "horse_number": 1}])
    result = calc_odds_momentum_score(df, conn)
    assert "odds_momentum_score" in result.columns
    assert result["odds_momentum_score"].iloc[0] == 0.5
    conn.close()


def test_falling_odds_gives_high_score():
    """オッズが下落している馬は高スコア（買い圧力 = 好シグナル）"""
    tmp = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(tmp)
    conn.execute("""
        CREATE TABLE realtime_odds (
            id INTEGER PRIMARY KEY, race_id TEXT, horse_number INTEGER,
            win_odds REAL, place_odds_min REAL, place_odds_max REAL,
            popularity INTEGER, recorded_at TEXT
        )
    """)
    # 5分間でオッズが 10.0 → 5.0 に下落（買い圧力）
    for i, odds in enumerate([10.0, 9.0, 8.0, 6.0, 5.0]):
        conn.execute(
            "INSERT INTO realtime_odds VALUES (?,?,?,?,?,?,?,?)",
            (i + 1, "RACE001", 1, odds, None, None, 3, f"2026-05-24 10:0{i}:00"),
        )
    conn.commit()
    df = pd.DataFrame([{"race_id": "RACE001", "horse_number": 1}])
    result = calc_odds_momentum_score(df, conn)
    # 下落 → 高スコア
    assert result["odds_momentum_score"].iloc[0] > 0.5
    conn.close()
    os.unlink(tmp)
