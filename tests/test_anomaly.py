"""src/pipeline/anomaly.py のテスト（取消・騎手変更検知）。"""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

from src.pipeline.anomaly import (
    check_race_anomalies,
    detect_jockey_changes,
    detect_scratches,
)


def test_detect_scratches_basic() -> None:
    entry = {1, 2, 3, 4, 5, 6}
    present = {1, 2, 3, 4, 6}  # 馬5が欠落
    assert detect_scratches(entry, present) == {5}


def test_detect_scratches_skips_when_feed_too_small() -> None:
    # present が min_present(5) 未満 = feed 取得失敗 → 誤検知防止で空
    entry = {1, 2, 3, 4, 5, 6}
    present = {1, 2}
    assert detect_scratches(entry, present) == set()


def test_detect_jockey_changes_normalizes() -> None:
    entry = {1: "武 豊", 2: "ルメール", 3: "川田将雅"}
    fresh = {1: "武豊", 2: "Ｃ．ルメール", 3: "戸崎圭太"}
    changes = detect_jockey_changes(entry, fresh)
    # 馬1: 空白差のみ → 変更なし / 馬2: 全角C付与だがNFKCで差が残るため変更扱い
    assert 1 not in changes
    assert 3 in changes and changes[3] == ("川田将雅", "戸崎圭太")


def _build_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE entries (race_id TEXT, horse_number INTEGER, jockey TEXT)"
    )
    conn.execute(
        "CREATE TABLE realtime_odds "
        "(race_id TEXT, horse_number INTEGER, win_odds REAL, recorded_at TEXT)"
    )
    horses = [(i, f"騎手{i}") for i in range(1, 8)]  # 7頭
    conn.executemany("INSERT INTO entries VALUES ('R1', ?, ?)", horses)
    # 最新スナップショット(14:00)に馬7が居ない → 取消
    for i in range(1, 7):
        conn.execute(
            "INSERT INTO realtime_odds VALUES ('R1', ?, 5.0, '2026-05-31 14:00:00')",
            (i,),
        )
    conn.commit()
    return conn


def test_check_race_anomalies_detects_scratch_no_jockey() -> None:
    conn = _build_conn()
    result = check_race_anomalies(conn, "R1", check_jockey=False)
    assert result.scratched == {7}
    assert result.has_changes


def test_check_race_anomalies_applies_jockey_change() -> None:
    conn = _build_conn()
    fresh = {i: f"騎手{i}" for i in range(1, 7)}
    fresh[2] = "新騎手2"  # 馬2 騎手変更
    with patch("src.pipeline.anomaly._fetch_fresh_jockeys", return_value=fresh):
        result = check_race_anomalies(conn, "R1", check_jockey=True)
    assert result.scratched == {7}
    assert result.jockey_changes == {2: ("騎手2", "新騎手2")}
    # entries が更新されている
    row = conn.execute(
        "SELECT jockey FROM entries WHERE race_id='R1' AND horse_number=2"
    ).fetchone()
    assert row[0] == "新騎手2"
