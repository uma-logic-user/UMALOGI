"""tests/test_sns_generator.py — SNS集客アセット自動生成のテスト。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.marketing.sns_generator import (
    KILLER_PHRASES,
    FreePick,
    HitSummary,
    fetch_free_picks,
    fetch_hit_summary,
    generate_daily_pack,
    generate_free_pick_post,
    generate_hit_report_post,
    generate_video_script,
    pick_killer_phrase,
)


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
        INSERT INTO races VALUES
            ('r1', '2026-06-14', '東京', 11, '安田記念'),
            ('r2', '2026-06-14', '京都', 10, ''),
            ('r3', '2026-06-13', '阪神', 9, ''),
            ('r4', '2026-06-14', '東京', 1, ''),
            ('r5', '2026-06-14', '東京', 2, '');
        -- 当日(6/14)の本命予想: r1(高confidence・直前) / r2(confidence NULL→model_score)
        -- r4=空馬名 / r5=文字化け馬名 → 公開対象から除外されること
        INSERT INTO predictions VALUES
            (1, 'r1', '本命(直前)', '単勝', 0.82, 0, '2026-06-14 14:00'),
            (2, 'r1', '本命(暫定)', '単勝', 0.70, 0, '2026-06-13 20:00'),
            (3, 'r2', '本命(直前)', '単勝', NULL, 0, '2026-06-14 13:00'),
            (4, 'r1', '卍(直前)',   '単勝', 0.90, 0, '2026-06-14 14:00'),
            (5, 'r4', '本命(直前)', '単勝', 0.95, 0, '2026-06-14 09:00'),
            (6, 'r5', '本命(直前)', '単勝', 0.94, 0, '2026-06-14 09:00');
        INSERT INTO prediction_horses VALUES
            (1, 1, 'アスコリピチェーノ', 1, 0.30),
            (2, 1, 'ソウルラッシュ', 2, 0.20),
            (3, 2, 'ソウルラッシュ', 1, 0.25),
            (4, 3, 'ジャンタルマンタル', 1, 0.55),
            (5, 4, 'ガイアフォース', 1, 0.40),
            (6, 5, '', 1, 0.50),
            (7, 6, '?A?h?}?C?S', 1, 0.50);
        -- 前日(6/13)の確定実績: 実弾=卍複勝(的中)・Pure_EV単勝(不的中)。
        -- 本命単勝(NON_LIVE_RETIRED)・Oracle三連単(観賞用)は集計除外されること
        INSERT INTO predictions VALUES
            (10, 'r3', '卍(直前)', '複勝', 0.6, 0, '2026-06-13 14:00'),
            (11, 'r3', 'Pure_EV_Edge(直前)', '単勝', 0.5, 0, '2026-06-13 14:00'),
            (12, 'r3', '本命(直前)', '単勝', 0.6, 0, '2026-06-13 14:00'),
            (13, 'r3', 'Oracle(直前)', '三連単', 0.5, 0, '2026-06-13 14:00');
        INSERT INTO prediction_results VALUES
            (1, 10, 1, 1500, 1400),
            (2, 11, 0, 0, -100),
            (3, 12, 1, 99999, 99899),
            (4, 13, 1, 88888, 88788);
        """
    )
    return c


# ── DB 抽出 ──────────────────────────────────────────────────────────────
class TestFetchFreePicks:
    def test_honmei_only_top_rank(self, conn: sqlite3.Connection) -> None:
        picks = fetch_free_picks(conn, "2026-06-14")
        assert len(picks) == 2
        # confidence 降順（r1=0.82 > r2=NULL→model_score 0.55）
        assert picks[0].race_id == "r1"
        assert picks[0].horse_name == "アスコリピチェーノ"  # 直前版が優先
        assert picks[1].horse_name == "ジャンタルマンタル"

    def test_confidence_falls_back_to_model_score(
        self, conn: sqlite3.Connection
    ) -> None:
        picks = fetch_free_picks(conn, "2026-06-14")
        assert picks[1].confidence == pytest.approx(0.55)

    def test_empty_and_garbled_names_excluded(
        self, conn: sqlite3.Connection
    ) -> None:
        names = {p.horse_name for p in fetch_free_picks(conn, "2026-06-14")}
        assert "" not in names
        assert "?A?h?}?C?S" not in names  # CLAUDE.md §16 文字化け公開禁止

    def test_limit(self, conn: sqlite3.Connection) -> None:
        assert len(fetch_free_picks(conn, "2026-06-14", limit=1)) == 1

    def test_no_data_day(self, conn: sqlite3.Connection) -> None:
        assert fetch_free_picks(conn, "2026-01-01") == []


class TestFetchHitSummary:
    def test_live_bets_only_true_cost_basis(self, conn: sqlite3.Connection) -> None:
        # 実弾=卍複勝(的中¥1,500/コスト¥100)+Pure_EV単勝(不的中/コスト¥100)。
        # 本命単勝(退避)とOracle三連単(観賞用)の巨額的中は混入しない。
        s = fetch_hit_summary(conn, "2026-06-13")
        assert s.n_bets == 2
        assert s.n_hits == 1
        assert s.total_cost == 200.0  # (1500-1400) + (0-(-100))
        assert s.total_payout == 1500.0
        assert s.roi == pytest.approx(750.0)
        assert s.best_hit_payout == 1500.0

    def test_empty_day(self, conn: sqlite3.Connection) -> None:
        s = fetch_hit_summary(conn, "2026-01-01")
        assert s.n_bets == 0
        assert s.roi == 0.0


# ── テキスト生成 ─────────────────────────────────────────────────────────
def _picks() -> list[FreePick]:
    return [FreePick("r1", "東京", 11, "安田記念", "テスト馬", 0.8)]


class TestGenerateFreePick:
    def test_contains_pick_and_cta(self) -> None:
        post = generate_free_pick_post(_picks(), "2026-06-14")
        assert "テスト馬" in post
        assert "東京11R" in post
        assert "購読者限定" in post  # サブスク導線
        assert any(phrase in post for phrase in KILLER_PHRASES)

    def test_empty_picks_empty_post(self) -> None:
        assert generate_free_pick_post([], "2026-06-14") == ""


class TestGenerateHitReport:
    def test_winning_day(self) -> None:
        s = HitSummary(
            "2026-06-13",
            n_bets=2,
            n_hits=1,
            total_cost=200,
            total_payout=1500,
            best_hit_payout=1500,
            best_hit_race="阪神9R",
            best_hit_bet_type="単勝",
        )
        post = generate_hit_report_post(s, "2026-06-14")
        assert "750%" in post
        assert "阪神9R" in post
        assert any(phrase in post for phrase in KILLER_PHRASES)

    def test_losing_day_honest(self) -> None:
        s = HitSummary("2026-06-13", n_bets=3, n_hits=0, total_cost=300, total_payout=0)
        post = generate_hit_report_post(s, "2026-06-14")
        assert "的中なし" in post  # 負けも誠実に公開
        assert "期待値" in post

    def test_no_bets_no_post(self) -> None:
        assert generate_hit_report_post(HitSummary("2026-06-13"), "2026-06-14") == ""


class TestVideoScript:
    def test_scene_structure(self) -> None:
        s = HitSummary(
            "2026-06-13", n_bets=2, n_hits=1, total_cost=200, total_payout=1500
        )
        script = generate_video_script(_picks(), s, "2026-06-14")
        assert "シーン1" in script
        assert "ナレーション" in script
        assert "テロップ" in script
        assert "テスト馬" in script
        assert "購読者限定" in script  # CTA シーン


class TestKillerPhrase:
    def test_deterministic(self) -> None:
        assert pick_killer_phrase("2026-06-14") == pick_killer_phrase("2026-06-14")

    def test_offset_varies(self) -> None:
        a = pick_killer_phrase("2026-06-14", offset=0)
        b = pick_killer_phrase("2026-06-14", offset=1)
        assert a != b


# ── E2E（ファイル出力）────────────────────────────────────────────────────
class TestDailyPack:
    def test_generates_files(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        pack = generate_daily_pack(conn, "2026-06-14", "2026-06-13", out_dir=tmp_path)
        names = {f.name for f in pack.files}
        assert names == {"free_pick.md", "hit_report.md", "video_script.md"}
        for f in pack.files:
            assert f.read_text(encoding="utf-8").strip()

    def test_empty_day_skips_empty_assets(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        pack = generate_daily_pack(conn, "2026-01-01", "2026-01-01", out_dir=tmp_path)
        names = {f.name for f in pack.files}
        assert "free_pick.md" not in names
        assert "hit_report.md" not in names
        assert "video_script.md" in names  # 台本は骨格のみでも生成
