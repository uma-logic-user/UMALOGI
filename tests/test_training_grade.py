import sqlite3
import sys

import pandas as pd

sys.path.insert(0, ".")

from src.umasugi_engine.factors.training_grade import calc_training_grade_score


def test_neutral_when_no_data():
    """horse_id が存在しない場合は 0.5 中立を返す"""
    conn = sqlite3.connect("data/umalogi.db")
    df = pd.DataFrame([{"race_id": "202605020101", "horse_id": "FAKE_HORSE_ID_9999"}])
    result = calc_training_grade_score(df, conn)
    assert "training_grade_score" in result.columns
    assert result["training_grade_score"].iloc[0] == 0.5
    conn.close()


def test_score_range():
    """スコアは [0, 1] の範囲内であること"""
    conn = sqlite3.connect("data/umalogi.db")
    rows = conn.execute(
        "SELECT DISTINCT rr.horse_id, rr.race_id FROM race_results rr "
        "WHERE rr.race_id IN (SELECT race_id FROM races ORDER BY date DESC LIMIT 10) "
        "LIMIT 20"
    ).fetchall()
    if not rows:
        conn.close()
        return
    df = pd.DataFrame(rows, columns=["horse_id", "race_id"])
    result = calc_training_grade_score(df, conn)
    assert result["training_grade_score"].between(0.0, 1.0).all()
    conn.close()
