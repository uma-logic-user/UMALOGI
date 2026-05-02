"""自己修復パイプライン（自動補完・リトライ・GracefulDegradation）の単体テスト"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── ヘルパー ─────────────────────────────────────────────────────────────────


def _make_test_db() -> sqlite3.Connection:
    """インメモリ DB にテスト用テーブルを作成して返す。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE races (
            race_id      TEXT PRIMARY KEY,
            date         TEXT,
            venue        TEXT DEFAULT '',
            race_number  INTEGER DEFAULT 0
        );
        CREATE TABLE race_results (
            race_id      TEXT,
            horse_number INTEGER,
            rank         INTEGER
        );
        CREATE TABLE race_payouts (
            race_id     TEXT,
            bet_type    TEXT,
            combination TEXT,
            payout      INTEGER
        );
    """)
    return conn


# ─── Task 2: infer_ranks 自動補完テスト ──────────────────────────────────────


def test_infer_ranks_fills_missing_rank_from_sanrentan() -> None:
    """三連単払戻から rank=1,2,3 が補完されることを確認する。"""
    from scripts.infer_ranks_from_payouts import infer_ranks

    conn = _make_test_db()
    conn.execute("INSERT INTO races VALUES ('R001', '2026-05-03', '東京', 1)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 3, NULL)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 7, NULL)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 12, NULL)")
    conn.execute("INSERT INTO race_payouts VALUES ('R001', '三連単', '3→7→12', 15000)")
    conn.commit()

    stats = infer_ranks(conn, year_filter=None, dry_run=False)

    rows = conn.execute(
        "SELECT horse_number, rank FROM race_results WHERE race_id='R001' ORDER BY horse_number"
    ).fetchall()
    assert {(3, 1), (7, 2), (12, 3)} == set(rows)
    assert stats["rank1_set"] == 1


def test_infer_ranks_dry_run_does_not_write() -> None:
    """dry_run=True のとき DB を変更しないことを確認する。"""
    from scripts.infer_ranks_from_payouts import infer_ranks

    conn = _make_test_db()
    conn.execute("INSERT INTO races VALUES ('R002', '2026-05-03', '東京', 2)")
    conn.execute("INSERT INTO race_results VALUES ('R002', 5, NULL)")
    conn.execute("INSERT INTO race_payouts VALUES ('R002', '単勝', '5', 350)")
    conn.commit()

    infer_ranks(conn, year_filter=None, dry_run=True)

    rank_val = conn.execute(
        "SELECT rank FROM race_results WHERE race_id='R002'"
    ).fetchone()[0]
    assert rank_val is None


# ─── Task 3: _run_with_retry リトライテスト ──────────────────────────────────


def test_run_with_retry_succeeds_on_second_attempt() -> None:
    """1回失敗した後に2回目で成功する場合、最終的に 0 を返すことを確認する。"""
    from scripts.scheduler import _run_with_retry

    call_count = 0

    def fake_run(cmd: list, label: str, timeout: int = 3600) -> int:
        nonlocal call_count
        call_count += 1
        return 1 if call_count < 2 else 0

    with patch("scripts.scheduler._run", side_effect=fake_run):
        rc = _run_with_retry(["echo", "test"], "テスト", max_retries=3, base_delay=0)

    assert rc == 0
    assert call_count == 2


def test_run_with_retry_exhausts_all_retries() -> None:
    """全リトライ失敗時に非ゼロ rc を返すことを確認する。"""
    from scripts.scheduler import _run_with_retry

    with patch("scripts.scheduler._run", return_value=1):
        rc = _run_with_retry(["echo", "fail"], "テスト", max_retries=2, base_delay=0)

    assert rc != 0


# ─── Task 6: Graceful Degradation — オッズフォールバックテスト ───────────────


def test_fetch_and_save_odds_falls_back_to_db_existing() -> None:
    """RTD 失敗時に DB 既存オッズ件数を返すことを確認する。"""
    from src.pipeline.scraping import fetch_and_save_odds

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER, horse_name TEXT
        );
        CREATE TABLE realtime_odds (
            race_id      TEXT,
            horse_number INTEGER,
            horse_name   TEXT,
            win_odds     REAL,
            fetched_at   TEXT
        );
    """)
    conn.execute("INSERT INTO entries VALUES ('R003', 1, 'テスト馬')")
    conn.execute(
        "INSERT INTO realtime_odds VALUES ('R003', 1, 'テスト馬', 3.5, '2026-05-03 10:00')"
    )
    conn.commit()

    with patch("src.pipeline.scraping._fetch_odds_rtd", return_value=[]):
        result = fetch_and_save_odds(conn, "R003")

    assert result >= 1


def test_fetch_and_save_odds_returns_zero_when_all_fail() -> None:
    """RTD も DB も空のとき 0 を返す（システムを止めない）ことを確認する。"""
    from src.pipeline.scraping import fetch_and_save_odds

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE entries (race_id TEXT, horse_number INTEGER, horse_name TEXT);
        CREATE TABLE realtime_odds (
            race_id TEXT, horse_number INTEGER, horse_name TEXT,
            win_odds REAL, fetched_at TEXT
        );
    """)
    conn.commit()

    with patch("src.pipeline.scraping._fetch_odds_rtd", return_value=None):
        result = fetch_and_save_odds(conn, "R_EMPTY")

    assert result == 0


# ─── Task 7: prerace_pipeline Graceful Degradation テスト ────────────────────


def test_prerace_pipeline_does_not_skip_on_odds_failure() -> None:
    """オッズ取得ゼロでも skipped=True を返さず予測を続行することを確認する。"""
    import pandas as pd
    from src.pipeline.prediction import prerace_pipeline

    with (
        patch("src.pipeline.prediction.init_db") as mock_db,
        patch("src.pipeline.prediction.FeatureBuilder") as mock_fb,
        patch("src.pipeline.prediction.load_models") as mock_lm,
        patch("src.pipeline.prediction.BetGenerator") as mock_bg,
        patch("src.pipeline.prediction.fetch_and_save_odds", return_value=0),
        patch(
            "src.pipeline.prediction._save_predictions",
            return_value={"本命": [], "卍": []},
        ),
        patch("src.pipeline.win5.try_win5"),
        patch("src.pipeline.prediction.save_json"),
        patch("src.pipeline.prediction.build_output_json", return_value={}),
        patch("src.pipeline.prediction._check_race_deadline"),
    ):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        mock_db.return_value = mock_conn

        df = pd.DataFrame(
            {
                "horse_number": [1, 2],
                "win_odds": [None, None],
                "horse_weight": [500, 490],
            }
        )
        mock_fb.return_value.build_race_features.return_value = df

        mock_lm.return_value = (
            MagicMock(
                predict=lambda x: pd.Series([0.6, 0.4]),
                ev_predict=lambda x: pd.Series([1.1, 0.9]),
            ),
            MagicMock(ev_score=lambda x: pd.Series([1.1, 0.9])),
        )
        mock_bg.return_value.generate_honmei.return_value = MagicMock(bets=[])
        mock_bg.return_value.generate_manji.return_value = MagicMock(bets=[])
        mock_bg.return_value.generate_oracle.return_value = MagicMock(bets=[])

        result = prerace_pipeline("202605030501", provisional=False)

    assert result.get("skipped") is not True
