"""
tests/test_win_report.py — src/ops/win_report.py のユニットテスト

requests.post / save_draft は全てモックし、実際の HTTP / Playwright は呼ばない。
DB は sqlite3 インメモリを使用する。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import src.ops.win_report as wr
from src.ops.win_report import (
    WinReportData,
    build_note_draft,
    build_x_post,
    generate_win_report_file,
    publish_win_report,
)


# ── テスト用フェイクオブジェクト ──────────────────────────────────────

@dataclass
class _FakeHit:
    prediction_id: int
    bet_type: str
    is_hit: bool
    payout: float
    invested: float
    profit: float
    roi: float
    combination: list[str]
    actual_winners: list[str]
    is_refund: bool = False


@dataclass
class _FakeResult:
    race_id: str
    race_name: str
    date: str
    hits: list[Any]
    total_invested: float
    total_payout: float
    roi: float
    has_manbaken: bool = False
    max_single_roi: float = 0.0
    is_refund_race: bool = False

    @property
    def hit_count(self) -> int:
        return sum(1 for h in self.hits if h.is_hit)


def _make_hit(
    pred_id: int = 1,
    bet_type: str = "複勝",
    payout: float = 1200.0,
    invested: float = 500.0,
) -> _FakeHit:
    return _FakeHit(
        prediction_id=pred_id,
        bet_type=bet_type,
        is_hit=True,
        payout=payout,
        invested=invested,
        profit=payout - invested,
        roi=payout / invested * 100,
        combination=["ロンギングセリーヌ"],
        actual_winners=["ロンギングセリーヌ"],
    )


def _make_data(**kwargs: Any) -> WinReportData:
    defaults: dict[str, Any] = dict(
        race_id="202605021011",
        race_name="優駿牝馬",
        venue="東京",
        race_number=11,
        date_str="2026-05-24",
        hit_items=[_make_hit()],
        total_invested=500.0,
        total_payout=1200.0,
        roi=240.0,
        top_ev=10.44,
        ev_vs_odds=[{"horse_number": 6, "odds": 27.1, "ev": 10.44, "gap": 10.40}],
    )
    defaults.update(kwargs)
    return WinReportData(**defaults)


def _make_mem_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT,
            date TEXT,
            venue TEXT,
            race_number INTEGER
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_type TEXT,
            expected_value REAL,
            combination_json TEXT
        );
        CREATE TABLE race_results (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            horse_number INTEGER,
            win_odds REAL
        );
    """)
    return conn


# ── Task 1 テスト ────────────────────────────────────────────────────

def test_generate_win_report_file_creates_correct_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(wr, "_RESULTS_DIR", tmp_path / "results")
    data = _make_data()

    path = generate_win_report_file(data)

    content = path.read_text(encoding="utf-8")
    assert "=== TITLE ===" in content
    assert "=== BODY ===" in content
    assert "=== X_POST ===" in content
    assert "【的中実績】期待値最適化アルゴリズムによる選別成功" in content
    assert "優駿牝馬" in content
    assert "240.0" in content  # ROI


def test_build_x_post_under_280_chars() -> None:
    data = _make_data()
    post = build_x_post(data)
    assert len(post) <= 280


def test_build_x_post_under_280_chars_with_long_race_name() -> None:
    data = _make_data(race_name="非常に長いレース名テスト用サンプルデータABCDEFGHIJKLMNOPQRSTUVWXYZ")
    post = build_x_post(data)
    assert len(post) <= 280


def test_build_x_post_includes_required_hashtags() -> None:
    data = _make_data()
    post = build_x_post(data)
    assert "#競馬予想" in post
    assert "#期待値アルゴリズム" in post
    assert "#的中実績" in post


# ── Task 2 テスト ────────────────────────────────────────────────────

def test_build_note_draft_contains_ev_and_roi() -> None:
    data = _make_data()
    predictions = [{"model_type": "卍", "expected_value": 10.44}]
    title, body = build_note_draft(data, predictions)

    assert "10.44" in body           # EV値
    assert "240.0" in body           # ROI
    assert "【的中実績】" in title
    assert "優駿牝馬" in title


# ── Task 3 テスト ────────────────────────────────────────────────────

def test_ev_vs_odds_table_populated_from_predictions() -> None:
    conn = _make_mem_db()
    conn.execute(
        "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
        (1, "202605021011", "卍", 10.44, json.dumps([[6]])),
    )
    conn.execute(
        "INSERT INTO race_results VALUES (?, ?, ?, ?)",
        (1, "202605021011", 6, 27.1),
    )
    conn.commit()

    hit = _make_hit(pred_id=1)
    result = wr._fetch_ev_vs_odds(conn, "202605021011", [hit])

    assert len(result) == 1
    assert result[0]["horse_number"] == 6
    assert result[0]["odds"] == pytest.approx(27.1)
    assert result[0]["ev"] == pytest.approx(10.44)
    assert result[0]["gap"] == pytest.approx(10.44 - 1.0 / 27.1, abs=0.01)


# ── Task 4 テスト ────────────────────────────────────────────────────

def test_publish_win_report_skips_when_no_hits() -> None:
    conn = _make_mem_db()
    result = _FakeResult(
        race_id="202605021011",
        race_name="優駿牝馬",
        date="2026-05-24",
        hits=[],
        total_invested=500.0,
        total_payout=0.0,
        roi=0.0,
    )

    called: list[str] = []
    with patch("src.ops.win_report.generate_win_report_file", side_effect=lambda d: called.append("file")):
        publish_win_report(result, "202605021011", conn)

    assert called == [], "的中なしなのにファイル生成が呼ばれた"


def test_publish_win_report_handles_playwright_failure_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(wr, "_RESULTS_DIR", tmp_path / "results")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
    monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

    conn = _make_mem_db()
    conn.execute(
        "INSERT INTO races VALUES (?, ?, ?, ?, ?)",
        ("202605021011", "優駿牝馬", "2026-05-24", "東京", 11),
    )
    conn.execute(
        "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
        (1, "202605021011", "卍", 10.44, json.dumps([[6]])),
    )
    conn.commit()

    hit = _make_hit(pred_id=1)
    result = _FakeResult(
        race_id="202605021011",
        race_name="優駿牝馬",
        date="2026-05-24",
        hits=[hit],
        total_invested=500.0,
        total_payout=1200.0,
        roi=240.0,
    )

    with patch("src.ops.win_report._post_note_draft", side_effect=RuntimeError("Playwright 失敗")):
        # 例外が publish_win_report の外に漏れないことを確認
        publish_win_report(result, "202605021011", conn)  # 例外なし
