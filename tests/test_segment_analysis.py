"""segment_analysis.py のテスト。"""

from __future__ import annotations
import sqlite3
from pathlib import Path


def _make_test_db(path: str) -> None:
    """テスト用の最小DBを作成する。"""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY, race_name TEXT, date TEXT,
            venue TEXT, distance INTEGER, surface TEXT,
            weather TEXT, condition TEXT, created_at TEXT, track_direction TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT,
            bet_type TEXT, confidence REAL, expected_value REAL,
            recommended_bet INTEGER, notes TEXT, combination_json TEXT, created_at TEXT
        );
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout INTEGER, profit INTEGER, roi REAL, recorded_at TEXT
        );

        INSERT INTO races VALUES ('20260601010101','R1','2026-06-01','東京',1600,'芝','晴','良','2026-06-01',NULL);
        INSERT INTO races VALUES ('20260601010102','R2','2026-06-01','東京',1600,'芝','晴','稍重','2026-06-01',NULL);

        INSERT INTO predictions VALUES (1,'20260601010101','本命(直前)','単勝',0.8,1.5,1000,NULL,NULL,'2026-06-01');
        INSERT INTO predictions VALUES (2,'20260601010101','本命(直前)','複勝',0.7,1.3,800,NULL,NULL,'2026-06-01');
        INSERT INTO predictions VALUES (3,'20260601010102','卍(直前)','三連複',0.5,0.9,600,NULL,NULL,'2026-06-01');

        INSERT INTO prediction_results VALUES (1,1,1,3000,2000,300.0,'2026-06-01');
        INSERT INTO prediction_results VALUES (2,2,0,0,-800,0.0,'2026-06-01');
        INSERT INTO prediction_results VALUES (3,3,0,0,-600,0.0,'2026-06-01');
    """)
    conn.close()


class TestSegmentAnalysis:
    def test_by_bet_type(self, tmp_path: Path) -> None:
        """券種別 ROI が正しく計算される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment

        rows = analyze_by_segment(db, group_by="bet_type")
        bet_types = {r["segment"]: r for r in rows}
        assert "単勝" in bet_types
        # 単勝: payout=3000, profit=2000 → invest=1000 → ROI=300%
        assert abs(bet_types["単勝"]["roi"] - 300.0) < 1.0

    def test_by_venue(self, tmp_path: Path) -> None:
        """会場別 ROI が出力される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment

        rows = analyze_by_segment(db, group_by="venue")
        assert len(rows) > 0
        assert all("roi" in r and "n_bets" in r and "segment" in r for r in rows)

    def test_by_condition(self, tmp_path: Path) -> None:
        """馬場状態別 ROI が出力される。"""
        db = str(tmp_path / "test.db")
        _make_test_db(db)
        from scripts.segment_analysis import analyze_by_segment

        rows = analyze_by_segment(db, group_by="condition")
        conditions = {r["segment"] for r in rows}
        assert "良" in conditions or "稍重" in conditions
