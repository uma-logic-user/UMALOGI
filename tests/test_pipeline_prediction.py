"""
src/pipeline/prediction.py のユニットテスト。

DB・モデル・外部 I/O はすべてモックし、パイプラインのロジックのみ検証する。
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.prediction import (
    _check_data_quality,
    _estimate_race_start_jst,
    _check_race_deadline,
)


# ── _check_data_quality ───────────────────────────────────────────


def _make_df(n: int = 8, odds_nan: int = 0, weight_nan: int = 0) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "horse_number": i + 1,
            "win_odds":    None if i < odds_nan else float(i + 1) * 3.0,
            "horse_weight": None if i < weight_nan else 450 + i * 2,
        })
    return pd.DataFrame(rows)


def test_check_data_quality_ok() -> None:
    df = _make_df(n=8, odds_nan=0)
    ok, reason = _check_data_quality(df)
    assert ok is True
    assert reason == "OK"


def test_check_data_quality_empty_df() -> None:
    ok, reason = _check_data_quality(pd.DataFrame())
    assert ok is False
    assert "0 頭" in reason


def test_check_data_quality_high_odds_missing() -> None:
    df = _make_df(n=8, odds_nan=7)  # 7/8 = 87.5% 欠損
    ok, reason = _check_data_quality(df)
    assert ok is False
    assert "オッズ" in reason


def test_check_data_quality_weight_warning_but_ok(caplog: pytest.LogCaptureFixture) -> None:
    df = _make_df(n=8, weight_nan=8, odds_nan=0)  # 馬体重全欠損だが続行
    with caplog.at_level("WARNING"):
        ok, _ = _check_data_quality(df)
    assert ok is True
    assert "馬体重" in caplog.text


# ── _estimate_race_start_jst ──────────────────────────────────────


def test_estimate_r1_is_1000() -> None:
    dt = _estimate_race_start_jst(1, "20250605")
    assert dt.hour == 10 and dt.minute == 0


def test_estimate_r11_is_1500() -> None:
    dt = _estimate_race_start_jst(11, "20250605")
    assert dt.hour == 15 and dt.minute == 0


def test_estimate_r6_is_1230() -> None:
    dt = _estimate_race_start_jst(6, "20250605")
    assert dt.hour == 12 and dt.minute == 30


# ── _check_race_deadline ──────────────────────────────────────────


@pytest.fixture()
def mem_db_with_race() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT,
            date TEXT,
            venue TEXT,
            race_number INTEGER,
            distance INTEGER DEFAULT 0,
            surface TEXT DEFAULT '',
            weather TEXT DEFAULT '',
            condition TEXT DEFAULT ''
        )
    """)
    conn.execute(
        "INSERT INTO races VALUES (?, ?, ?, ?, ?, 0, '', '', '')",
        ("202506050701", "テストレース", "2025-06-05", "東京", 7),
    )
    conn.commit()
    return conn


def test_check_race_deadline_logs_warning_when_late(
    mem_db_with_race: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """締め切り後に呼ばれた場合、Discord 通知を送ろうとする（URL 未設定なのでスキップ）。"""
    from datetime import datetime
    fake_now = datetime(2025, 6, 5, 13, 30)  # R7 発走推定 13:00 → 締切 12:45 を超過
    with (
        patch("src.pipeline.prediction.datetime") as mock_dt,
        patch("src.pipeline.prediction._discord.send_text") as mock_send,
    ):
        mock_dt.now.return_value = fake_now
        mock_dt.strptime.side_effect = datetime.strptime
        _check_race_deadline(mem_db_with_race, "202506050701")
    mock_send.assert_called_once()
    assert "遅延警告" in mock_send.call_args[0][0]


def test_check_race_deadline_no_warning_when_early(
    mem_db_with_race: sqlite3.Connection,
) -> None:
    """締め切り前なら Discord には送信されない。"""
    from datetime import datetime
    fake_now = datetime(2025, 6, 5, 10, 0)  # R7=13:00 → 締切 12:45 まで余裕あり
    with (
        patch("src.pipeline.prediction.datetime") as mock_dt,
        patch("src.pipeline.prediction._discord.send_text") as mock_send,
    ):
        mock_dt.now.return_value = fake_now
        mock_dt.strptime.side_effect = datetime.strptime
        _check_race_deadline(mem_db_with_race, "202506050701")
    mock_send.assert_not_called()
