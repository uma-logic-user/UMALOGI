"""
tests/test_generate_weekly_note.py
generate_weekly_note.py の単体テスト
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

# プロジェクトルートを sys.path に追加
import sys
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.generate_weekly_note import (
    _fetch_all_model_stats,
    _fetch_manbaiken_hits,
    _fetch_top_hits,
    _fetch_winning_segments,
    _model_base,
    _model_display,
    _is_v2,
    _pnl_str,
    _grade_badge,
    _grade_label,
    _build_manbaiken_section,
    _build_winning_segments_section,
    _build_v2_preview_section,
    generate_weekly_note,
    _MANBAIKEN_THRESHOLD,
    _TOKUDAI_THRESHOLD,
    _WINNER_ROI_THRESHOLD,
)


# ── フィクスチャ ────────────────────────────────────────────────────

@pytest.fixture()
def mem_db() -> sqlite3.Connection:
    """テスト用インメモリ SQLite DB（スキーマ最小限）。"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE races (
            race_id    TEXT PRIMARY KEY,
            date       TEXT,
            venue      TEXT,
            race_number INTEGER,
            race_name  TEXT,
            surface    TEXT,
            distance   INTEGER,
            condition  TEXT
        );
        CREATE TABLE predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id         TEXT,
            model_type      TEXT,
            bet_type        TEXT,
            combination_json TEXT,
            expected_value  REAL,
            recommended_bet INTEGER
        );
        CREATE TABLE prediction_results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER,
            is_hit        INTEGER,
            payout        REAL,
            profit        REAL
        );
        CREATE TABLE race_results (
            race_id      TEXT,
            horse_number INTEGER,
            horse_name   TEXT,
            jockey       TEXT,
            rank         INTEGER,
            win_odds     REAL,
            popularity   INTEGER
        );
    """)
    return conn


def _insert_race(conn: sqlite3.Connection, race_id: str, dt: str, venue: str = "東京", no: int = 1) -> None:
    conn.execute(
        "INSERT INTO races VALUES (?,?,?,?,?,?,?,?)",
        (race_id, dt, venue, no, f"{venue}{no}R", "T", 1600, "良"),
    )


def _insert_prediction(
    conn: sqlite3.Connection,
    race_id: str,
    model_type: str,
    bet_type: str,
    is_hit: int,
    payout: float,
    profit: float,
    ev: float = 1.0,
) -> None:
    conn.execute(
        "INSERT INTO predictions (race_id, model_type, bet_type, combination_json, expected_value, recommended_bet) VALUES (?,?,?,?,?,?)",
        (race_id, model_type, bet_type, "[1]", ev, 300),
    )
    pred_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO prediction_results (prediction_id, is_hit, payout, profit) VALUES (?,?,?,?)",
        (pred_id, is_hit, payout, profit),
    )


# ── _model_base ───────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("本命(直前)",   "本命"),
    ("卍(暫定)",     "卍"),
    ("卍v2(直前)",   "卍v2"),
    ("Alpha-Payout", "Alpha-Payout"),
    ("HitFocus(暫定)", "HitFocus"),
])
def test_model_base(raw: str, expected: str) -> None:
    assert _model_base(raw) == expected


# ── _is_v2 ────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("卍v2(直前)", True),
    ("本命V2",     True),
    ("本命(直前)", False),
    ("Oracle",     False),
])
def test_is_v2(raw: str, expected: bool) -> None:
    assert _is_v2(raw) == expected


# ── _pnl_str ──────────────────────────────────────────────────────

@pytest.mark.parametrize("n,expected", [
    (1000,   "+¥1,000"),
    (0,      "+¥0"),
    (-500,   "-¥500"),
    (-1645970, "-¥1,645,970"),
])
def test_pnl_str(n: int, expected: str) -> None:
    assert _pnl_str(n) == expected


# ── _grade_badge ──────────────────────────────────────────────────

def test_grade_badge() -> None:
    assert "特大万馬券" in _grade_badge("tokudai")
    assert "万馬券" in _grade_badge("manbaiken")
    assert "高配当" in _grade_badge("kodai")


def test_grade_label_tokudai() -> None:
    label = _grade_label("tokudai", 215450)
    assert "215,450" in label
    assert "特大万馬券" in label


# ── _fetch_all_model_stats ────────────────────────────────────────

def test_fetch_all_model_stats_empty(mem_db: sqlite3.Connection) -> None:
    result = _fetch_all_model_stats(mem_db, "2026-05-11", "2026-05-17")
    assert result == []


def test_fetch_all_model_stats_basic(mem_db: sqlite3.Connection) -> None:
    _insert_race(mem_db, "R001", "2026-05-17")
    _insert_prediction(mem_db, "R001", "卍(直前)", "複勝", 1, 300, 200)
    _insert_prediction(mem_db, "R001", "卍(直前)", "複勝", 0, 0, -100)

    result = _fetch_all_model_stats(mem_db, "2026-05-11", "2026-05-17")
    assert len(result) == 1
    row = result[0]
    assert row["model_base"] == "卍"
    assert row["bet_type"] == "複勝"
    assert row["total"] == 2
    assert row["hits"] == 1
    assert row["hit_rate"] == pytest.approx(50.0)


def test_fetch_all_model_stats_roi_calc(mem_db: sqlite3.Connection) -> None:
    """ROI = payout / investment × 100。投資額計算の正確性を検証。"""
    _insert_race(mem_db, "R001", "2026-05-17")
    # 1件: ¥200投資 → ¥300払戻 → 利益¥100
    _insert_prediction(mem_db, "R001", "本命(直前)", "複勝", 1, 300, 100)

    result = _fetch_all_model_stats(mem_db, "2026-05-11", "2026-05-17")
    row = result[0]
    assert row["payout"] == 300
    assert row["profit"] == 100
    assert row["investment"] == 200  # payout - profit
    assert row["roi"] == pytest.approx(150.0)  # 300/200*100


# ── _fetch_winning_segments ───────────────────────────────────────

def test_fetch_winning_segments_true_winner() -> None:
    """ROI100%超のセグメントが is_true_winner=True で返る。"""
    stats = [
        {"model_base": "卍", "model_display": "卍モデル", "bet_type": "三連複",
         "total": 10, "hits": 5, "hit_rate": 50.0, "roi": 150.0, "profit": 5000,
         "payout": 15000, "investment": 10000, "is_v2": False, "is_qf": False},
        {"model_base": "本命", "model_display": "本命モデル", "bet_type": "複勝",
         "total": 10, "hits": 3, "hit_rate": 30.0, "roi": 60.0, "profit": -4000,
         "payout": 6000, "investment": 10000, "is_v2": False, "is_qf": False},
    ]
    segs, is_winner = _fetch_winning_segments(stats)
    assert is_winner is True
    assert len(segs) == 1
    assert segs[0]["bet_type"] == "三連複"


def test_fetch_winning_segments_fallback_no_winner() -> None:
    """ROI100%超なし → フォールバック TOP N を is_true_winner=False で返す。"""
    stats = [
        {"model_base": "卍", "model_display": "卍モデル", "bet_type": "複勝",
         "total": 5, "hits": 2, "hit_rate": 40.0, "roi": 80.0, "profit": -2000,
         "payout": 8000, "investment": 10000, "is_v2": False, "is_qf": False},
        {"model_base": "本命", "model_display": "本命モデル", "bet_type": "単勝",
         "total": 5, "hits": 1, "hit_rate": 20.0, "roi": 50.0, "profit": -5000,
         "payout": 5000, "investment": 10000, "is_v2": False, "is_qf": False},
    ]
    segs, is_winner = _fetch_winning_segments(stats)
    assert is_winner is False
    # ROI 降順: 卍複勝(80%) が先頭
    assert segs[0]["bet_type"] == "複勝"


def test_fetch_winning_segments_minimum_bets_filter() -> None:
    """最低 _MINIMUM_BETS_FOR_SEGMENT 件未満のセグメントは除外される。"""
    stats = [
        {"model_base": "Oracle", "model_display": "Oracleモデル", "bet_type": "三連単",
         "total": 1, "hits": 1, "hit_rate": 100.0, "roi": 5000.0, "profit": 10000,
         "payout": 10100, "investment": 100, "is_v2": False, "is_qf": False},
        {"model_base": "本命", "model_display": "本命モデル", "bet_type": "複勝",
         "total": 10, "hits": 5, "hit_rate": 50.0, "roi": 80.0, "profit": -2000,
         "payout": 8000, "investment": 10000, "is_v2": False, "is_qf": False},
    ]
    segs, is_winner = _fetch_winning_segments(stats)
    # Oracle は 1 件なのでフィルターされ、本命モデルのみ残る
    assert all(s["total"] >= 3 for s in segs)


# ── _fetch_manbaiken_hits ─────────────────────────────────────────

def test_fetch_manbaiken_hits_empty(mem_db: sqlite3.Connection) -> None:
    result = _fetch_manbaiken_hits(mem_db, "2026-05-11", "2026-05-17")
    assert result == []


def test_fetch_manbaiken_hits_grade_classification(mem_db: sqlite3.Connection) -> None:
    """払戻金額による grade 分類を検証。"""
    _insert_race(mem_db, "R001", "2026-05-17", "東京", 5)
    _insert_race(mem_db, "R002", "2026-05-17", "東京", 1)
    _insert_race(mem_db, "R003", "2026-05-17", "新潟", 6)

    _insert_prediction(mem_db, "R001", "本命(直前)", "三連単", 1, 215450, 206450)
    _insert_prediction(mem_db, "R002", "本命(直前)", "三連単", 1, 39240, 30240)
    _insert_prediction(mem_db, "R003", "本命(直前)", "三連複", 1, 7500, 6000)

    result = _fetch_manbaiken_hits(mem_db, "2026-05-11", "2026-05-17", min_payout=5000)
    assert len(result) == 3

    grades = {r["payout"]: r["grade"] for r in result}
    assert grades[215450] == "tokudai"
    assert grades[39240] == "manbaiken"
    assert grades[7500] == "kodai"


def test_fetch_manbaiken_hits_deduplication(mem_db: sqlite3.Connection) -> None:
    """同一レース×券種の重複排除を検証。"""
    _insert_race(mem_db, "R001", "2026-05-17")
    _insert_prediction(mem_db, "R001", "本命(直前)", "三連単", 1, 20000, 15000)
    _insert_prediction(mem_db, "R001", "卍(直前)",   "三連単", 1, 20000, 15000)

    result = _fetch_manbaiken_hits(mem_db, "2026-05-11", "2026-05-17")
    # 同一 race_id × bet_type = 三連単 → 1 件のみ
    assert len(result) == 1


# ── _build_manbaiken_section ──────────────────────────────────────

def test_build_manbaiken_section_empty() -> None:
    assert _build_manbaiken_section([]) == []


def test_build_manbaiken_section_tokudai() -> None:
    hits = [{
        "grade": "tokudai", "date": "2026-05-17", "venue": "東京",
        "race_number": 5, "race_name": "東京5R",
        "model_type": "本命(直前)", "bet_type": "三連単",
        "payout": 215450, "profit": 206450,
        "placed": [
            {"number": 5, "name": "ウルフマン", "rank": 1},
            {"number": 6, "name": "クリスタルドレス", "rank": 2},
        ],
    }]
    lines = _build_manbaiken_section(hits)
    text = "\n".join(lines)
    assert "特大万馬券" in text
    assert "215,450" in text
    assert "ウルフマン" in text


def test_build_manbaiken_section_header_by_grade() -> None:
    """万馬券がある場合と高配当のみの場合でヘッダーが切り替わる。"""
    hits_manbaiken = [{"grade": "manbaiken", "date": "2026-05-17", "venue": "東京",
                        "race_number": 1, "race_name": "東京1R",
                        "model_type": "卍(直前)", "bet_type": "三連単",
                        "payout": 12000, "profit": 9000, "placed": []}]
    hits_kodai = [{"grade": "kodai", "date": "2026-05-17", "venue": "東京",
                    "race_number": 2, "race_name": "東京2R",
                    "model_type": "卍(直前)", "bet_type": "ワイド",
                    "payout": 7000, "profit": 4000, "placed": []}]

    text_man = "\n".join(_build_manbaiken_section(hits_manbaiken))
    text_kod = "\n".join(_build_manbaiken_section(hits_kodai))
    assert "万馬券的中実績" in text_man
    assert "高配当的中実績" in text_kod


# ── _build_winning_segments_section ──────────────────────────────

def test_build_winning_segments_section_true_winner() -> None:
    segs = [{"model_display": "卍モデル", "bet_type": "三連複",
              "total": 10, "hits": 5, "hit_rate": 50.0, "roi": 150.0, "profit": 5000}]
    lines = _build_winning_segments_section(segs, True, "2026-05-11〜2026-05-17")
    text = "\n".join(lines)
    assert "完全勝利" in text
    assert "150.0%" in text
    assert "卍モデル" in text


def test_build_winning_segments_section_fallback() -> None:
    segs = [{"model_display": "本命モデル", "bet_type": "複勝",
              "total": 8, "hits": 3, "hit_rate": 37.5, "roi": 80.0, "profit": -2000}]
    lines = _build_winning_segments_section(segs, False, "2026-05-11〜2026-05-17")
    text = "\n".join(lines)
    assert "最高パフォーマンス" in text or "ベストパフォーマー" in text
    assert "80.0%" in text


def test_build_winning_segments_section_empty() -> None:
    assert _build_winning_segments_section([], False, "2026-05-11〜2026-05-17") == []


# ── _build_v2_preview_section ─────────────────────────────────────

def test_build_v2_preview_section_no_specifics() -> None:
    """V2予告に具体的な数式・アーキテクチャが含まれていないことを確認。"""
    lines = _build_v2_preview_section()
    text = "\n".join(lines)

    # 必須要素
    assert "次世代" in text or "V2" in text
    assert "A/Bテスト" in text

    # 隠蔽すべき具体的な実装詳細が含まれていないこと
    forbidden = ["tanh", "LightGBM", "feature_importance", "SHAP", "pkl", "model.predict"]
    for kw in forbidden:
        assert kw not in text, f"具体的な実装詳細が含まれています: {kw}"


# ── generate_weekly_note 統合テスト ──────────────────────────────

def test_generate_weekly_note_no_data(mem_db: sqlite3.Connection) -> None:
    """データなしでも例外を起こさず記事を生成できる。"""
    article = generate_weekly_note(mem_db, week_offset=1, include_picks=False)
    assert isinstance(article, str)
    assert len(article) > 100


def test_generate_weekly_note_contains_v2_preview(mem_db: sqlite3.Connection) -> None:
    """V2予告セクションが常に含まれている。"""
    article = generate_weekly_note(mem_db, week_offset=1, include_picks=False)
    assert "次世代" in article or "V2" in article


def test_generate_weekly_note_manbaiken_hero_appears_first(mem_db: sqlite3.Connection) -> None:
    """万馬券的中が記事の冒頭に配置されている（購読案内より前）。"""
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(weeks=1)
    last_sat = last_monday + timedelta(days=5)
    dt = last_sat.strftime("%Y-%m-%d")

    _insert_race(mem_db, "R001", dt, "東京", 5)
    _insert_prediction(mem_db, "R001", "本命(直前)", "三連単", 1, 215450, 206450)
    _insert_prediction(mem_db, "R001", "本命(直前)", "三連単", 0, 0, -9000)  # 外れ

    article = generate_weekly_note(mem_db, week_offset=1, include_picks=False)

    idx_manbaiken = article.find("特大万馬券")
    idx_footer    = article.find("UMALOGIを応援")
    assert idx_manbaiken != -1, "万馬券セクションが見つからない"
    assert idx_manbaiken < idx_footer, "万馬券セクションが購読案内より前にあるべき"


def test_generate_weekly_note_no_total_roi_in_top(mem_db: sqlite3.Connection) -> None:
    """
    記事の冒頭部分（最初の1000文字）に全モデル合算ROIの表示がないことを確認。
    赤字合算が記事の印象を悪化させないための仕様。
    """
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(weeks=1)
    last_sat = last_monday + timedelta(days=5)
    dt = last_sat.strftime("%Y-%m-%d")

    _insert_race(mem_db, "R001", dt)
    # 全モデル合算で赤字になるデータを投入
    _insert_prediction(mem_db, "R001", "本命(直前)", "複勝", 0, 0, -100)
    _insert_prediction(mem_db, "R001", "卍(直前)",   "複勝", 0, 0, -100)

    article = generate_weekly_note(mem_db, week_offset=1, include_picks=False)
    top_section = article[:1000]

    # 「全モデル成績サマリー」「総投資額」「総払戻額」が冒頭にないこと
    assert "全モデル成績サマリー" not in top_section
    assert "総投資額" not in top_section
