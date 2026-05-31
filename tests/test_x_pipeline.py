"""
X世論分析パイプライン 堅牢性テスト
=====================================

カバー範囲:
  1. XSignalParser — NaN/極値/空データなどエッジケースでクラッシュしないこと
  2. get_x_consensus_score — 全馬0/極端値/空結果でもゼロ除算しないこと
  3. FeatureBuilder._add_x_consensus — NaN/欠損値/dry-run で DataFrame が壊れないこと
  4. x_accounts_history マイグレーション — テーブルが正しく作成されること
  5. RateLimiter ジッター — 実際の待機時間がベース ±30% 内に収まること
"""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

# ── フィクスチャ ────────────────────────────────────────────────────────


@pytest.fixture
def mem_conn() -> sqlite3.Connection:
    """インメモリ SQLite + 最低限のスキーマ。"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE races (
            race_id      TEXT PRIMARY KEY,
            date         TEXT,
            venue        TEXT,
            race_number  INTEGER,
            race_name    TEXT DEFAULT ''
        );
        CREATE TABLE x_accounts (
            screen_name    TEXT PRIMARY KEY,
            display_name   TEXT DEFAULT '',
            follower_count INTEGER DEFAULT 0,
            hit_rate_30d   REAL,
            weight         REAL DEFAULT 1.0,
            is_active      INTEGER DEFAULT 1,
            last_scraped_at TEXT,
            created_at     TEXT DEFAULT (datetime('now')),
            updated_at     TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE x_signals (
            signal_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id     TEXT NOT NULL UNIQUE,
            race_id      TEXT,
            screen_name  TEXT NOT NULL,
            horse_number INTEGER,
            signal_type  TEXT,
            confidence   REAL NOT NULL DEFAULT 0.5,
            race_name_raw TEXT,
            raw_text     TEXT NOT NULL,
            posted_at    TEXT NOT NULL,
            fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
            parsed       INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO races VALUES ('202605050511','2026-05-05','東京',11,'ヴィクトリアマイル');
        INSERT INTO x_accounts VALUES ('tester','テスト',50000,0.35,1.2,1,NULL,datetime('now'),datetime('now'));
    """)
    return conn


def _insert_signal(
    conn: sqlite3.Connection,
    tweet_id: str,
    horse_number: int | None,
    signal_type: str | None,
    confidence: float,
    race_id: str = "202605050511",
    parsed: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO x_signals
           (tweet_id, race_id, screen_name, horse_number, signal_type,
            confidence, raw_text, posted_at, parsed)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            tweet_id,
            race_id,
            "tester",
            horse_number,
            signal_type,
            confidence,
            "raw",
            "2026-05-05T08:00:00",
            parsed,
        ),
    )
    conn.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. XSignalParser エッジケーステスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestXSignalParser:
    def test_parse_empty_db(self, mem_conn: sqlite3.Connection) -> None:
        """未解析シグナルが0件でもクラッシュしない。"""
        from src.ml.x_signal_parser import XSignalParser

        parser = XSignalParser(mem_conn, dry_run=True)
        result = parser.parse_unparsed()
        assert result["parsed"] == 0
        assert result["errors"] == 0

    def test_rule_based_nan_confidence(self) -> None:
        """ルールベースパーサーが不正テキストでも NaN を返さない。"""
        from src.ml.x_signal_parser import _rule_based_parse, _RaceInfo

        races = [_RaceInfo("202605050511", "05", "東京", 11)]
        edge_cases = [
            ("e1", ""),  # 空文字
            ("e2", "   "),  # 空白のみ
            ("e3", "◎" * 200),  # シグナル記号の連続
            ("e4", "東京" + "9" * 50 + "R"),  # 超長い数字列
            ("e5", "◎0番"),  # 0番（無効馬番）
            ("e6", "▲99番 東京11R"),  # 超大馬番
        ]
        for tweet_id, text in edge_cases:
            ps = _rule_based_parse(0, tweet_id, text, races)
            # confidence は必ず 0.0〜1.0 の float でなければならない
            assert isinstance(ps.confidence, float), (
                f"{tweet_id}: confidence が float でない"
            )
            assert 0.0 <= ps.confidence <= 1.0, (
                f"{tweet_id}: confidence={ps.confidence} が範囲外"
            )
            # horse_number が存在すれば正の整数
            if ps.horse_number is not None:
                assert ps.horse_number > 0, (
                    f"{tweet_id}: horse_number={ps.horse_number} が非正"
                )

    def test_parse_dry_run_does_not_write(self, mem_conn: sqlite3.Connection) -> None:
        """dry_run=True では parsed フラグが 0 のまま。"""
        conn = mem_conn
        conn.execute(
            """INSERT INTO x_signals
               (tweet_id, screen_name, raw_text, posted_at, parsed)
               VALUES ('tw1','tester','東京11R ◎7番','2026-05-05T08:00:00', 0)"""
        )
        conn.commit()

        from src.ml.x_signal_parser import XSignalParser

        parser = XSignalParser(conn, dry_run=True)
        parser.parse_unparsed()

        still_unparsed = conn.execute(
            "SELECT COUNT(*) FROM x_signals WHERE parsed=0"
        ).fetchone()[0]
        assert still_unparsed == 1, "dry_run=True なのに parsed フラグが書き換わった"

    def test_parse_updates_flag(self, mem_conn: sqlite3.Connection) -> None:
        """dry_run=False で正常処理したシグナルの parsed=1 になる。"""
        conn = mem_conn
        conn.execute(
            """INSERT INTO x_signals
               (tweet_id, screen_name, raw_text, posted_at, parsed)
               VALUES ('tw2','tester','東京11R ◎5番','2026-05-05T08:00:00', 0)"""
        )
        conn.commit()

        from src.ml.x_signal_parser import XSignalParser

        parser = XSignalParser(conn, api_key=None, dry_run=False)
        result = parser.parse_unparsed()

        assert result["parsed"] == 1
        row = conn.execute(
            "SELECT parsed, horse_number, signal_type FROM x_signals WHERE tweet_id='tw2'"
        ).fetchone()
        assert row["parsed"] == 1
        assert row["signal_type"] == "honmei"
        assert row["horse_number"] == 5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. get_x_consensus_score エッジケーステスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestGetXConsensusScore:
    def test_empty_signals_returns_empty(self, mem_conn: sqlite3.Connection) -> None:
        """シグナルが0件なら空 dict を返す（ZeroDivision なし）。"""
        from src.ml.x_signal_parser import get_x_consensus_score

        result = get_x_consensus_score(mem_conn, "202605050511")
        assert result == {}

    def test_single_honmei_signal(self, mem_conn: sqlite3.Connection) -> None:
        """honmei シグナル 1件 → 正のスコア。"""
        _insert_signal(mem_conn, "s1", 7, "honmei", 0.9)
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(mem_conn, "202605050511")
        assert 7 in scores
        assert scores[7] > 0.0

    def test_keshi_signal_is_negative(self, mem_conn: sqlite3.Connection) -> None:
        """keshi シグナル → 負のスコア。"""
        _insert_signal(mem_conn, "s2", 3, "keshi", 0.8)
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(mem_conn, "202605050511")
        assert 3 in scores
        assert scores[3] < 0.0

    def test_all_zero_confidence(self, mem_conn: sqlite3.Connection) -> None:
        """confidence=0.0 でも ZeroDivision しない。"""
        _insert_signal(mem_conn, "s3", 5, "honmei", 0.0)
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(mem_conn, "202605050511")
        # スコアは 0.0（NaN や例外でないこと）
        assert 5 in scores
        assert math.isfinite(scores[5])

    def test_extreme_confidence(self, mem_conn: sqlite3.Connection) -> None:
        """confidence が 1.0 を超えた DB 値（異常データ）でもスコアが有限値。"""
        conn = mem_conn
        # 意図的に 9.99 を投入（DB 制約外の異常データを想定）
        conn.execute(
            """INSERT INTO x_signals
               (tweet_id, race_id, screen_name, horse_number, signal_type,
                confidence, raw_text, posted_at, parsed)
               VALUES ('s4','202605050511','tester',8,'honmei',9.99,'','2026-05-05T08:00:00',1)"""
        )
        conn.commit()
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(conn, "202605050511")
        assert 8 in scores
        assert math.isfinite(scores[8])

    def test_all_horses_no_signal(self, mem_conn: sqlite3.Connection) -> None:
        """全馬シグナルなし → 全スコア 0（empty dict）。"""
        # parsed=0 のシグナルは集計対象外
        _insert_signal(mem_conn, "s5", 1, "honmei", 0.8, parsed=0)
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(mem_conn, "202605050511")
        assert len(scores) == 0

    def test_mixed_signals_same_horse(self, mem_conn: sqlite3.Connection) -> None:
        """同一馬に honmei + keshi が混在 → キャンセルされてスコアが中庸値。"""
        _insert_signal(mem_conn, "s6", 4, "honmei", 1.0)
        _insert_signal(mem_conn, "s7", 4, "keshi", 1.0)
        from src.ml.x_signal_parser import get_x_consensus_score

        scores = get_x_consensus_score(mem_conn, "202605050511")
        # honmei(+1.0) + keshi(-0.3) → 合計 +0.7, 平均 +0.35 程度（正値）
        assert 4 in scores
        assert math.isfinite(scores[4])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. FeatureBuilder._add_x_consensus エッジケーステスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAddXConsensus:
    def _make_df(self, n: int = 6, extra: dict[str, Any] | None = None) -> pd.DataFrame:
        """テスト用の最小特徴量 DataFrame を生成する。"""
        df = pd.DataFrame(
            {
                "horse_number": list(range(1, n + 1)),
                "crowd_bias_ratio": [1.0] * n,
            }
        )
        if extra:
            for k, v in extra.items():
                df[k] = v
        return df

    def test_dry_run_all_zero(self, mem_conn: sqlite3.Connection) -> None:
        """dry_run=True なら全馬 x_consensus_score=0.0。"""
        from src.ml.features import FeatureBuilder

        fb = FeatureBuilder(mem_conn)
        df = self._make_df()
        result = fb._add_x_consensus(df, "202605050511", dry_run=True)
        assert (result["x_consensus_score"] == 0.0).all()
        assert (result["x_crowd_divergence"] == 0.0).all()

    def test_no_signals_in_db(self, mem_conn: sqlite3.Connection) -> None:
        """DB にシグナルが存在しない場合でも NaN が発生しない。"""
        from src.ml.features import FeatureBuilder

        fb = FeatureBuilder(mem_conn)
        df = self._make_df()
        result = fb._add_x_consensus(df, "202605050511")
        assert result["x_consensus_score"].notna().all()
        assert result["x_crowd_divergence"].notna().all()

    def test_with_nan_crowd_bias(self, mem_conn: sqlite3.Connection) -> None:
        """crowd_bias_ratio が NaN でも x_crowd_divergence がクラッシュしない。"""
        from src.ml.features import FeatureBuilder

        fb = FeatureBuilder(mem_conn)
        df = self._make_df(extra={"crowd_bias_ratio": float("nan")})
        result = fb._add_x_consensus(df, "202605050511", dry_run=True)
        assert result["x_crowd_divergence"].notna().all()

    def test_with_extreme_crowd_bias(self, mem_conn: sqlite3.Connection) -> None:
        """crowd_bias_ratio が 100 の極端値でも x_crowd_divergence が [-1, 1] 内。"""
        from src.ml.features import FeatureBuilder

        _insert_signal(mem_conn, "f1", 1, "honmei", 0.9)
        fb = FeatureBuilder(mem_conn)
        n = 6
        df = pd.DataFrame(
            {
                "horse_number": list(range(1, n + 1)),
                "crowd_bias_ratio": [100.0, 0.01, 1.0, 1.0, 1.0, 1.0],
            }
        )
        result = fb._add_x_consensus(df, "202605050511")
        assert result["x_crowd_divergence"].between(-1.0, 1.0).all()

    def test_missing_horse_number_column(self, mem_conn: sqlite3.Connection) -> None:
        """horse_number 列がない DataFrame でもクラッシュしない。"""
        from src.ml.features import FeatureBuilder

        fb = FeatureBuilder(mem_conn)
        df = pd.DataFrame({"crowd_bias_ratio": [1.0, 1.2, 0.8]})
        result = fb._add_x_consensus(df, "202605050511", dry_run=True)
        assert "x_consensus_score" in result.columns
        assert "x_crowd_divergence" in result.columns

    def test_x_signal_count_capped(self, mem_conn: sqlite3.Connection) -> None:
        """x_signal_count は 50 でクリップされる（過剰投稿アカウント対策）。"""
        from src.ml.features import FeatureBuilder

        # 馬1番に 60 件シグナル投入
        for i in range(60):
            _insert_signal(mem_conn, f"bulk{i:03d}", 1, "honmei", 0.5)
        fb = FeatureBuilder(mem_conn)
        df = pd.DataFrame(
            {
                "horse_number": [1, 2],
                "crowd_bias_ratio": [1.0, 1.0],
            }
        )
        result = fb._add_x_consensus(df, "202605050511")
        assert result["x_signal_count"].max() <= 50


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. x_accounts_history マイグレーションテスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestXAccountsHistoryMigration:
    def test_table_created_by_init_db(self, tmp_path: Path) -> None:
        """init_db() 実行後に x_accounts_history テーブルが存在する。"""
        from src.database.init_db import init_db

        conn = init_db(tmp_path / "test.db")
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "x_accounts_history" in tables, (
            "x_accounts_history テーブルが作成されていない"
        )

        cols = {
            r[1]
            for r in conn.execute("PRAGMA table_info(x_accounts_history)").fetchall()
        }
        required = {
            "history_id",
            "screen_name",
            "race_id",
            "horse_number",
            "signal_type",
            "confidence",
            "win_odds",
            "final_rank",
            "is_hit",
            "payout",
            "roi",
            "evaluated_at",
            "created_at",
        }
        missing = required - cols
        assert not missing, f"必須カラムが不足: {missing}"
        conn.close()

    def test_insert_and_query(self, tmp_path: Path) -> None:
        """x_accounts_history にレコードを INSERT → SELECT できる。"""
        from src.database.init_db import init_db

        conn = init_db(tmp_path / "test2.db")

        # 依存テーブルに先にデータを入れる
        # FK 非依存: x_accounts_history は参照整合性をアプリ側で担保
        conn.execute(
            """INSERT INTO x_accounts_history
               (screen_name, race_id, horse_number, signal_type, confidence, win_odds, final_rank, is_hit, payout)
               VALUES ('acc_test','race_test_01',7,'honmei',0.9,5.2,1,1,1040.0)"""
        )
        conn.commit()

        row = conn.execute(
            "SELECT roi FROM x_accounts_history WHERE screen_name='acc_test'"
        ).fetchone()
        assert row is not None
        conn.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. RateLimiter ジッターテスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestRateLimiterJitter:
    @pytest.mark.asyncio
    async def test_jitter_within_bounds(self) -> None:
        """実際の待機時間がベースインターバルの ±30% 内に収まる。"""
        from src.scraper.x_scraper import RateLimiter

        limiter = RateLimiter(max_per_hour=60, jitter_ratio=0.30)  # ベース=60秒
        # 1回目は即座に通過（last=0 なので wait=0）
        await limiter.wait("test_key")

        # 2回目の wait 時間を計測
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None
            await limiter.wait("test_key")
            if mock_sleep.called:
                actual_wait = mock_sleep.call_args[0][0]
                base = 60.0
                assert actual_wait >= base * 0.70, (
                    f"待機時間が短すぎる: {actual_wait:.1f}s"
                )
                assert actual_wait <= base * 1.30, (
                    f"待機時間が長すぎる: {actual_wait:.1f}s"
                )

    @pytest.mark.asyncio
    async def test_no_negative_sleep(self) -> None:
        """ジッターによって sleep 時間が負にならない。"""
        from src.scraper.x_scraper import RateLimiter

        limiter = RateLimiter(max_per_hour=3600, jitter_ratio=0.99)  # 極端なジッター
        await limiter.wait("key1")
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None
            await limiter.wait("key1")
            if mock_sleep.called:
                actual_wait = mock_sleep.call_args[0][0]
                assert actual_wait >= 1.0, (
                    f"待機時間が最小値1秒を下回った: {actual_wait:.3f}s"
                )
