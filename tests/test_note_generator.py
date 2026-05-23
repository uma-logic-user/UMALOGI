"""
src/ops/note_generator.py のユニットテスト。

DB に依存する関数は sqlite3 のインメモリ DB またはモックで検証する。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import src.ops.note_generator as ng


# ── ヘルパー ─────────────────────────────────────────────────────────

def _make_mem_db() -> sqlite3.Connection:
    """テスト用インメモリ DB を作成し最小テーブルを用意する。"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT,
            date TEXT,
            venue TEXT,
            race_number INTEGER,
            distance INTEGER,
            surface TEXT,
            weather TEXT,
            condition TEXT,
            track_direction TEXT
        );
        CREATE TABLE entries (
            race_id TEXT,
            horse_number INTEGER,
            gate_number INTEGER,
            horse_name TEXT,
            sex_age TEXT,
            weight_carried REAL,
            jockey TEXT,
            trainer TEXT,
            horse_weight REAL,
            horse_weight_diff REAL
        );
        CREATE TABLE realtime_odds (
            race_id TEXT,
            horse_number INTEGER,
            win_odds REAL,
            place_odds_min REAL,
            place_odds_max REAL,
            popularity INTEGER
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id TEXT,
            model_type TEXT,
            bet_type TEXT,
            confidence REAL,
            expected_value REAL,
            recommended_bet TEXT,
            notes TEXT,
            combination_json TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def _insert_race(conn: sqlite3.Connection, race_id: str = "2026050101010101",
                 date: str = "2026-05-01") -> None:
    conn.execute(
        "INSERT INTO races VALUES (?,?,?,?,?,?,?,?,?,?)",
        (race_id, "テストレース", date, "東京", 1, 1600, "芝", "晴", "良", "左"),
    )


def _insert_entry(conn: sqlite3.Connection, race_id: str, hn: int,
                  name: str = "テスト馬") -> None:
    conn.execute(
        "INSERT INTO entries VALUES (?,?,?,?,?,?,?,?,?,?)",
        (race_id, hn, (hn + 1) // 2, name, "牡3", 57.0, "テスト騎手", "テスト調教師", 450.0, 0.0),
    )


def _insert_pred(conn: sqlite3.Connection, race_id: str, model_type: str,
                 bet_type: str, confidence: float, ev: float,
                 combos: list[list[int]]) -> None:
    conn.execute(
        "INSERT INTO predictions (race_id, model_type, bet_type, confidence, "
        "expected_value, recommended_bet, notes, combination_json, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (race_id, model_type, bet_type, confidence, ev, None, None,
         json.dumps(combos), "2026-05-01T10:00:00"),
    )


# ── _fmt_combo ────────────────────────────────────────────────────────

class TestFmtCombo:
    def test_single_horse(self) -> None:
        assert ng._fmt_combo([[5]], "単勝") == "5番"

    def test_pair(self) -> None:
        assert ng._fmt_combo([[3, 7]], "馬連") == "3→7"

    def test_multiple_fuku(self) -> None:
        result = ng._fmt_combo([[1], [2], [3]], "複勝")
        assert "1番" in result and "2番" in result and "3番" in result

    def test_empty_returns_dash(self) -> None:
        assert ng._fmt_combo([], "単勝") == "―"

    def test_overflow_suffix(self) -> None:
        combos = [[i] for i in range(1, 12)]  # 11件
        result = ng._fmt_combo(combos, "複勝", max_show=8)
        assert "他3点" in result

    def test_max_show_respected(self) -> None:
        combos = [[i, i + 1] for i in range(1, 6)]
        result = ng._fmt_combo(combos, "馬連", max_show=2)
        assert "他3点" in result


# ── _mark_horse ───────────────────────────────────────────────────────

class TestMarkHorse:
    def test_first_is_maru(self) -> None:
        assert ng._mark_horse(0) == "◎"

    def test_second_is_circle(self) -> None:
        assert ng._mark_horse(1) == "○"

    def test_fifth_is_x(self) -> None:
        assert ng._mark_horse(4) == "×"

    def test_out_of_range_returns_triangle(self) -> None:
        assert ng._mark_horse(10) == "△"


# ── _ev_bar ───────────────────────────────────────────────────────────

class TestEvBar:
    def test_super_high(self) -> None:
        assert "超高EV" in ng._ev_bar(3.1)

    def test_high(self) -> None:
        assert "高EV" in ng._ev_bar(2.0)

    def test_good(self) -> None:
        assert "EV良好" in ng._ev_bar(1.5)

    def test_ok(self) -> None:
        assert "EV適正" in ng._ev_bar(1.0)

    def test_low(self) -> None:
        assert "参考値" in ng._ev_bar(0.9)


# ── _calc_rec_bet ─────────────────────────────────────────────────────

class TestCalcRecBet:
    def test_minimum_100(self) -> None:
        assert ng._calc_rec_bet(1.01) == 100

    def test_high_ev(self) -> None:
        # Kelly = (3-1)/3 = 0.667 → cap 0.25 → 10000*0.25 = 2500
        assert ng._calc_rec_bet(3.0) == 2500

    def test_result_is_multiple_of_100(self) -> None:
        for ev in [1.2, 1.5, 2.0, 2.7]:
            assert ng._calc_rec_bet(ev) % 100 == 0


# ── _fetch_honmei_scores の馬番重複除去 ──────────────────────────────

class TestFetchHonmeiScores:
    def test_dedup_same_horse(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "R001")
        # 同じ馬番（1番）を持つ予測を2件挿入
        _insert_pred(conn, "R001", "本命(直前)", "単勝", 0.9, 2.5, [[1]])
        _insert_pred(conn, "R001", "本命(直前)", "複勝", 0.8, 1.5, [[1]])
        _insert_pred(conn, "R001", "本命(直前)", "単勝", 0.5, 1.2, [[3]])
        conn.commit()

        result = ng._fetch_honmei_scores(conn, "R001")
        horse_nums = [r["horse_number"] for r in result]
        # 1番は1回だけ、3番も1回
        assert horse_nums.count(1) == 1
        assert horse_nums.count(3) == 1

    def test_highest_confidence_first(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "R002")
        _insert_pred(conn, "R002", "本命(直前)", "複勝", 0.6, 1.2, [[2]])
        _insert_pred(conn, "R002", "本命(直前)", "単勝", 0.9, 2.0, [[5]])
        conn.commit()

        result = ng._fetch_honmei_scores(conn, "R002")
        assert result[0]["horse_number"] == 5  # confidence 0.9 が先頭


# ── _build_bet_summary の馬連 EV フィルタ ────────────────────────────

class TestBuildBetSummary:
    def _make_sc(self, manji_preds: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "honmei_preds": [],
            "manji_preds": manji_preds,
            "alpha_preds": [],
            "manji_ev": max(
                (p["expected_value"] or 0 for p in manji_preds if p["bet_type"] == "複勝"),
                default=0.0,
            ),
            "alpha_ev": 0.0,
        }

    def test_umaren_ev_lt_1_excluded(self) -> None:
        sc = self._make_sc([
            {"bet_type": "複勝", "expected_value": 5.0, "combos": [[1], [2]]},
            {"bet_type": "馬連", "expected_value": 0.8, "combos": [[1, 2]]},
        ])
        lines = ng._build_bet_summary(sc, [])
        assert not any("馬連" in l for l in lines)

    def test_umaren_ev_ge_1_included(self) -> None:
        sc = self._make_sc([
            {"bet_type": "複勝", "expected_value": 3.0, "combos": [[1], [2]]},
            {"bet_type": "馬連", "expected_value": 2.0, "combos": [[1, 2]]},
        ])
        lines = ng._build_bet_summary(sc, [])
        assert any("馬連" in l for l in lines)

    def test_no_signal_returns_note(self) -> None:
        sc = self._make_sc([])
        sc["honmei_preds"] = []
        sc["alpha_preds"] = []
        sc["manji_ev"] = 0.0
        sc["alpha_ev"] = 0.0
        lines = ng._build_bet_summary(sc, [])
        assert any("シグナルはありません" in l for l in lines)


# ── _score_race ───────────────────────────────────────────────────────

class TestScoreRace:
    def test_zero_score_when_no_preds(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "EMPTY", "2026-05-01")
        result = ng._score_race(conn, "EMPTY")
        assert result["score"] == 0.0
        assert result["consensus"] == 0

    def test_score_increases_with_ev(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "SCORED", "2026-05-01")
        _insert_pred(conn, "SCORED", "Alpha-Payout(直前)", "三連複", 0.5, 2.0, [[1, 2, 3]])
        _insert_pred(conn, "SCORED", "卍(直前)", "複勝", 0.6, 1.5, [[1], [2]])
        conn.commit()

        result = ng._score_race(conn, "SCORED")
        assert result["alpha_ev"] == 2.0
        assert result["manji_ev"] == 1.5
        # スコア = 2.0*3 + 1.5*2 + consensus*2.5 ≥ 9.0
        assert result["score"] >= 9.0

    def test_consensus_counts_models(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "CON", "2026-05-01")
        _insert_pred(conn, "CON", "Alpha-Payout(直前)", "単勝", 0.7, 1.5, [[5]])
        _insert_pred(conn, "CON", "卍(直前)", "複勝", 0.8, 1.2, [[5], [6]])
        _insert_pred(conn, "CON", "本命(直前)", "単勝", 0.95, 1.1, [[5]])
        conn.commit()

        result = ng._score_race(conn, "CON")
        assert result["consensus"] == 3


# ── select_recommended_races ─────────────────────────────────────────

class TestSelectRecommendedRaces:
    def test_returns_empty_when_no_races(self) -> None:
        conn = _make_mem_db()
        result = ng.select_recommended_races(conn, "2099-01-01")
        assert result == []

    def test_yyyymmdd_format_accepted(self) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "X001", "2026-06-01")
        result = ng.select_recommended_races(conn, "20260601")
        # レースはあるが予想なし → スコア0 → min_n で3件確保 (1件しかないので1件)
        assert isinstance(result, list)

    def test_top_n_limit(self) -> None:
        conn = _make_mem_db()
        for i in range(1, 8):
            rid = f"202606010101010{i}"
            _insert_race(conn, rid, "2026-06-01")
            _insert_pred(conn, rid, "Alpha-Payout(直前)", "三連複", 0.5, float(i), [[1, 2, 3]])
            _insert_pred(conn, rid, "卍(直前)", "複勝", 0.6, float(i) * 0.8, [[1], [2]])
        conn.commit()

        result = ng.select_recommended_races(conn, "2026-06-01", top_n=4, min_n=2)
        assert len(result) <= 4

    def test_sorted_by_score_desc(self) -> None:
        conn = _make_mem_db()
        for i in range(1, 4):
            rid = f"2026060201010{i:02d}"
            _insert_race(conn, rid, "2026-06-02")
            _insert_pred(conn, rid, "Alpha-Payout(直前)", "三連複", 0.5, float(i), [[1, 2, 3]])
        conn.commit()

        result = ng.select_recommended_races(conn, "2026-06-02", top_n=5, min_n=1)
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)


# ── generate のバリデーション ─────────────────────────────────────────

class TestGenerateValidation:
    def test_invalid_date_raises(self) -> None:
        with pytest.raises(ValueError, match="不正な日付形式"):
            ng.generate("not-a-date")

    def test_no_races_returns_comment(self, tmp_path: Path) -> None:
        with patch.object(ng, "_DB_PATH", ng._DB_PATH), \
             patch.object(ng, "_OUT_DIR", tmp_path), \
             patch("src.ops.note_generator._db") as mock_db:
            conn = _make_mem_db()
            mock_db.return_value = conn
            result = ng.generate("20991231")
        assert "予想データなし" in result

    def test_output_file_created(self, tmp_path: Path) -> None:
        conn = _make_mem_db()
        _insert_race(conn, "202606030101010101", "2026-06-03")
        _insert_pred(conn, "202606030101010101", "Alpha-Payout(直前)", "三連複",
                     0.5, 2.0, [[1, 2, 3]])
        _insert_pred(conn, "202606030101010101", "卍(直前)", "複勝",
                     0.6, 1.5, [[1], [2]])
        conn.commit()

        with patch("src.ops.note_generator._db", return_value=conn), \
             patch.object(ng, "_OUT_DIR", tmp_path):
            ng.generate("20260603", top_n=3)

        out = tmp_path / "20260603_recommendations.md"
        assert out.exists()
        text = out.read_text(encoding="utf-8")
        assert "2026年06月03日" in text
