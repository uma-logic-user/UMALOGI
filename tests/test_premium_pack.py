"""tests/test_premium_pack.py — サブスク向けプレミアムデータ自動生成のテスト。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src.marketing.premium_pack import (
    LEAK_STORY_LINES,
    PREMIUM_EV_MIN,
    RacePremium,
    _fetch_race_inputs,
    generate_premium_html,
    generate_premium_pack,
    generate_premium_text,
    generate_teaser_text,
    scan_premium_races,
)
from src.ml.all_ticket_optimizer import TicketCandidate, build_formation


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY, date TEXT, venue TEXT,
            race_number INTEGER, race_name TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT,
            bet_type TEXT, confidence REAL, is_superseded INTEGER,
            created_at TEXT
        );
        CREATE TABLE prediction_horses (
            id INTEGER PRIMARY KEY, prediction_id INTEGER,
            horse_name TEXT, predicted_rank INTEGER, model_score REAL
        );
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout REAL, profit REAL
        );
        CREATE TABLE entries (
            id INTEGER PRIMARY KEY, race_id TEXT,
            horse_number INTEGER, horse_name TEXT
        );
        CREATE TABLE realtime_odds (
            id INTEGER PRIMARY KEY, race_id TEXT,
            horse_number INTEGER, win_odds REAL, recorded_at TEXT
        );
        INSERT INTO races VALUES
            ('r1', '2026-06-14', '東京', 11, '安田記念'),
            ('r2', '2026-06-14', '京都', 10, ''),
            ('r3', '2026-06-13', '阪神', 9, '');
        INSERT INTO predictions VALUES
            (1, 'r1', '本命(直前)', '単勝', 0.82, 0, '2026-06-14 14:00');
        """
    )
    # r1: 8頭立て。馬名で entries と突合できる model_score を投入。
    names = [f"ホース{i}" for i in range(1, 9)]
    # モデルは 1 番を市場より大幅に強く評価 → 高 EV 歪みが出る構図
    scores = [0.45, 0.10, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03]
    odds = [9.0, 4.0, 6.0, 8.0, 10.0, 14.0, 20.0, 30.0]
    for i, (nm, sc, od) in enumerate(zip(names, scores, odds, strict=True), start=1):
        c.execute(
            "INSERT INTO prediction_horses VALUES (?, 1, ?, ?, ?)", (i, nm, i, sc)
        )
        c.execute(
            "INSERT INTO entries (race_id, horse_number, horse_name) "
            "VALUES ('r1', ?, ?)",
            (i, nm),
        )
        # 古いオッズ→最新オッズの順に 2 本入れ、最新が使われることを確認可能にする
        c.execute(
            "INSERT INTO realtime_odds (race_id, horse_number, win_odds, recorded_at) "
            "VALUES ('r1', ?, ?, '2026-06-14 09:00')",
            (i, od * 2),
        )
        c.execute(
            "INSERT INTO realtime_odds (race_id, horse_number, win_odds, recorded_at) "
            "VALUES ('r1', ?, ?, '2026-06-14 14:30')",
            (i, od),
        )
    return c


def _dummy_candidates() -> list[TicketCandidate]:
    return [
        TicketCandidate("三連複", (1, 2, 3), prob=0.05, odds=40.0, ev=2.0),
        TicketCandidate("三連複", (1, 2, 4), prob=0.04, odds=40.0, ev=1.6),
        TicketCandidate("三連単", (1, 2, 3), prob=0.01, odds=150.0, ev=1.5),
    ]


# ── DB 抽出 ──────────────────────────────────────────────────────────────
class TestFetchRaceInputs:
    def test_returns_aligned_inputs(self, conn: sqlite3.Connection) -> None:
        out = _fetch_race_inputs(conn, "r1")
        assert out is not None
        numbers, probs, odds = out
        assert numbers == list(range(1, 9))
        assert probs[0] == pytest.approx(0.45)
        assert odds[0] == pytest.approx(9.0)  # 最新オッズ（14:30）が使われる

    def test_no_prediction_returns_none(self, conn: sqlite3.Connection) -> None:
        assert _fetch_race_inputs(conn, "r2") is None

    def test_no_odds_returns_none(self, conn: sqlite3.Connection) -> None:
        conn.execute("DELETE FROM realtime_odds")
        assert _fetch_race_inputs(conn, "r1") is None

    def test_low_coverage_returns_none(self, conn: sqlite3.Connection) -> None:
        # 勝率マッチを 3 頭まで削る（3/8 = 37.5% < 80%）
        conn.execute("DELETE FROM prediction_horses WHERE id > 3")
        assert _fetch_race_inputs(conn, "r1") is None


class TestScanPremiumRaces:
    def test_scan_extracts_sanren_only(self, conn: sqlite3.Connection) -> None:
        races = scan_premium_races(conn, "2026-06-14")
        assert len(races) == 1
        r = races[0]
        assert r.race_id == "r1"
        assert r.candidates, "EV1.30超の三連系候補が抽出されるはず"
        assert all(c.bet_type in ("三連複", "三連単") for c in r.candidates)
        assert all(c.ev >= PREMIUM_EV_MIN for c in r.candidates)
        assert r.formations

    def test_scan_failure_isolated_per_race(self, conn: sqlite3.Connection) -> None:
        """1 レースの DB 破損が全体を殺さない（best-effort）。"""
        conn.execute("DROP TABLE entries")
        assert scan_premium_races(conn, "2026-06-14") == []


# ── テキスト生成 ─────────────────────────────────────────────────────────
class TestGeneratePremiumText:
    def test_contains_formation_and_disclaimer(self) -> None:
        cands = _dummy_candidates()
        race = RacePremium(
            race_id="r1",
            venue="東京",
            race_number=11,
            race_name="安田記念",
            candidates=cands,
            formations=build_formation(cands),
        )
        text = generate_premium_text([race], "2026-06-14")
        assert "東京11R" in text
        assert "三連複 フォーメーション" in text
        assert "推定値" in text  # 推定オッズの誠実性注意
        assert "購読者限定" in text
        assert "2.00" in text  # EV 値

    def test_empty_day_keeps_discipline_message(self) -> None:
        text = generate_premium_text([], "2026-06-14")
        assert "ありませんでした" in text
        assert "規律" in text


class TestGenerateTeaserText:
    def test_teaser_hides_combos_and_tells_leak_story(
        self, conn: sqlite3.Connection
    ) -> None:
        cands = _dummy_candidates()
        race = RacePremium(
            race_id="r1",
            venue="東京",
            race_number=11,
            race_name="安田記念",
            candidates=cands,
            formations=build_formation(cands),
        )
        text = generate_teaser_text([race], "2026-06-14", conn, "2026-06-13")
        assert LEAK_STORY_LINES[0] in text
        assert "EV 2.00" in text
        assert "購読者限定" in text
        # チラ見せ: 買い目そのもの（1-2-3 等）は絶対に出さない
        assert "1-2-3" not in text
        assert "1→2→3" not in text

    def test_teaser_no_candidates_day(self, conn: sqlite3.Connection) -> None:
        text = generate_teaser_text([], "2026-06-14", conn, "2026-06-13")
        assert "候補なし" in text


# ── HTML レポート（Tailwind ラグジュアリー版）────────────────────────────
class TestGeneratePremiumHtml:
    def _race(self) -> RacePremium:
        cands = _dummy_candidates()
        return RacePremium(
            race_id="r1",
            venue="東京",
            race_number=11,
            race_name="安田記念",
            candidates=cands,
            formations=build_formation(cands),
        )

    def test_html_is_selfcontained_tailwind_document(self) -> None:
        html = generate_premium_html([self._race()], "2026-06-14")
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert "tailwind" in html.lower()
        assert 'lang="ja"' in html

    def test_html_contains_race_facts_and_integrity_note(self) -> None:
        html = generate_premium_html([self._race()], "2026-06-14")
        assert "東京11R" in html
        assert "安田記念" in html
        assert "2.00" in html  # 最大 EV
        assert "三連複" in html
        assert "推定" in html  # 推定オッズの誠実性注意
        assert "購読者限定" in html

    def test_html_empty_day_keeps_discipline_message(self) -> None:
        html = generate_premium_html([], "2026-06-14")
        assert "規律" in html


# ── E2E ──────────────────────────────────────────────────────────────────
class TestGeneratePremiumPack:
    def test_writes_files(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        pack = generate_premium_pack(conn, "2026-06-14", "2026-06-13", out_dir=tmp_path)
        assert len(pack.files) == 4
        names = {f.name for f in pack.files}
        assert names == {
            "premium_sanren.md",
            "sns_teaser.md",
            "premium_sanren.html",
            "premium_signals.json",
        }
        for f in pack.files:
            content = f.read_text(encoding="utf-8")
            assert content.strip()
        # Web UI（/api/premium-signals）が読む JSON の構造を検証
        signals = json.loads(
            (tmp_path / "premium_signals.json").read_text(encoding="utf-8")
        )
        assert signals["date"] == "2026-06-14"
        assert signals["n_races"] == len(signals["races"])
        if signals["races"]:
            cand = signals["races"][0]["candidates"][0]
            assert {"bet_type", "combo", "prob", "odds", "ev", "stake"} <= set(cand)
