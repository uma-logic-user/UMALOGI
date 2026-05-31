"""
scripts/scheduler.py — job_note_daily_article() / _notify_note_article_summary()
のユニットテスト。

実際の DB / Discord / Playwright は呼ばず、unittest.mock で全て差し替える。
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch


_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ── scheduler をインポートできるよう schedule スタブを差し込む ─────────────────


def _ensure_schedule_stub() -> None:
    if "schedule" not in sys.modules:
        stub = types.ModuleType("schedule")
        stub.every = MagicMock()
        stub.jobs = []
        stub.next_run = MagicMock(return_value="2099-01-01 00:00:00")
        sys.modules["schedule"] = stub


_ensure_schedule_stub()

import scripts.scheduler as sched


# ── テスト用フィクスチャ ────────────────────────────────────────────────────────


def _make_recommended(n: int = 3) -> list[dict]:
    """ダミーの推奨レースリストを生成する。"""
    result = []
    for i in range(1, n + 1):
        result.append(
            {
                "race_id": f"202605230101010{i}",
                "score": float(40 - i * 2),
                "alpha_ev": float(3 - i * 0.5),
                "manji_ev": float(5 - i),
                "oracle_count": 3,
                "honmei_conf": 0.8,
                "consensus": 4,
                "alpha_preds": [],
                "manji_preds": [],
                "oracle_preds": [],
                "honmei_preds": [],
            }
        )
    return result


# ── _notify_note_article_summary ───────────────────────────────────────────────


class TestNotifyNoteArticleSummary:
    def test_sends_embed_via_send_discord_embed(self) -> None:
        """_send_discord_embed が1回呼ばれること。"""
        recommended = _make_recommended(3)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = {
            "venue": "東京",
            "race_number": 6,
            "race_name": "テストレース",
        }

        with (
            patch("scripts.scheduler._send_discord_embed") as mock_embed,
            patch("sqlite3.connect", return_value=mock_conn),
        ):
            sched._notify_note_article_summary(
                recommended, "20260523", discord_sent=True
            )

        mock_embed.assert_called_once()
        embed_list = mock_embed.call_args[0][0]
        assert len(embed_list) == 1
        embed = embed_list[0]
        assert "2026-05-23" in embed["title"]
        assert embed["color"] == 0x00C8FF
        # fields が3つあること
        assert len(embed["fields"]) == 3

    def test_discord_sent_false_shows_warning_text(self) -> None:
        recommended = _make_recommended(1)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        with (
            patch("scripts.scheduler._send_discord_embed") as mock_embed,
            patch("sqlite3.connect", return_value=mock_conn),
        ):
            sched._notify_note_article_summary(
                recommended, "20260601", discord_sent=False
            )

        embed = mock_embed.call_args[0][0][0]
        discord_field = next(f for f in embed["fields"] if "Discord" in f["name"])
        assert "⚠️" in discord_field["value"]

    def test_empty_recommended_shows_placeholder(self) -> None:
        mock_conn = MagicMock()
        with (
            patch("scripts.scheduler._send_discord_embed") as mock_embed,
            patch("sqlite3.connect", return_value=mock_conn),
        ):
            sched._notify_note_article_summary([], "20260601", discord_sent=True)

        embed = mock_embed.call_args[0][0][0]
        race_field = next(f for f in embed["fields"] if "厳選" in f["name"])
        assert "なし" in race_field["value"]

    def test_sqlite_error_does_not_crash(self) -> None:
        """DB 接続失敗でも例外を出さないこと。"""
        with (
            patch("scripts.scheduler._send_discord_embed"),
            patch("sqlite3.connect", side_effect=OSError("DB error")),
        ):
            sched._notify_note_article_summary(_make_recommended(2), "20260601", True)


# ── job_note_daily_article ─────────────────────────────────────────────────────


class TestJobNoteDailyArticle:
    """job_note_daily_article() の動作を各シナリオで検証する。"""

    _MOCK_MD = "# テスト記事\n\nコンテンツ"

    def _base_patches(self):
        """共通パッチをコンテキストマネージャとして返す。"""
        return [
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(5),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._send_discord_embed"),
        ]

    def test_generates_article_and_marks_done(self) -> None:
        """正常ケース: 記事生成 + Discord 転送 + job 完了マーク。"""
        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(5),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._mark_job_done") as mock_done,
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": ""}, clear=False),
        ):
            sched.job_note_daily_article()

        mock_done.assert_called_once_with("job_note_daily_article")
        mock_router.send_note_draft.assert_called_once()

    def test_skips_note_com_when_auto_post_not_set(self) -> None:
        """NOTE_DRAFT_AUTO_POST 未設定時は save_draft が呼ばれないこと。"""
        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(3),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._mark_job_done"),
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch("src.ops.note_draft_publisher.save_draft") as mock_save,
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": ""}, clear=False),
        ):
            sched.job_note_daily_article()

        mock_save.assert_not_called()

    def test_skips_note_com_when_session_missing(self, tmp_path: Path) -> None:
        """NOTE_DRAFT_AUTO_POST=1 でもセッションファイルなし → save_draft 未呼び出し。"""
        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(3),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._mark_job_done"),
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch("src.ops.note_draft_publisher.save_draft") as mock_save,
            patch.object(sched, "_ROOT", tmp_path),
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": "1"}, clear=False),
        ):
            sched.job_note_daily_article()

        mock_save.assert_not_called()

    def test_calls_save_draft_when_auto_post_and_session_exist(
        self, tmp_path: Path
    ) -> None:
        """NOTE_DRAFT_AUTO_POST=1 かつセッションあり → save_draft が呼ばれること。"""
        session = tmp_path / ".note_session.json"
        session.write_text("{}", encoding="utf-8")

        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(3),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._mark_job_done"),
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch(
                "src.ops.note_draft_publisher.save_draft", return_value=True
            ) as mock_save,
            patch.object(sched, "_ROOT", tmp_path),
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": "1"}, clear=False),
        ):
            sched.job_note_daily_article()

        mock_save.assert_called_once()
        _, kwargs = mock_save.call_args
        assert "title" in kwargs
        assert "body" in kwargs
        assert kwargs["headless"] is True

    def test_save_draft_failure_sends_discord_alert(self, tmp_path: Path) -> None:
        """save_draft が False を返したら Discord アラートが送信されること。"""
        session = tmp_path / ".note_session.json"
        session.write_text("{}", encoding="utf-8")

        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(3),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord") as mock_discord,
            patch("scripts.scheduler._mark_job_done"),
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch("src.ops.note_draft_publisher.save_draft", return_value=False),
            patch.object(sched, "_ROOT", tmp_path),
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": "1"}, clear=False),
        ):
            sched.job_note_daily_article()

        # 失敗時の Discord 通知が呼ばれること
        texts = [str(c) for c in mock_discord.call_args_list]
        assert any("失敗" in t for t in texts)

    def test_generator_error_sends_sos_and_returns_early(self) -> None:
        """generate() が例外を上げたら Discord SOS を送信して return すること。"""
        with (
            patch(
                "src.ops.note_generator.generate", side_effect=RuntimeError("DB locked")
            ),
            patch("scripts.scheduler._send_discord") as mock_discord,
            patch("scripts.scheduler._mark_job_done") as mock_done,
        ):
            sched.job_note_daily_article()

        mock_discord.assert_called_once()
        assert "失敗" in str(mock_discord.call_args)
        mock_done.assert_not_called()

    def test_playwright_not_installed_does_not_crash(self, tmp_path: Path) -> None:
        """playwright がない環境でも例外を出さずに完了すること。"""
        session = tmp_path / ".note_session.json"
        session.write_text("{}", encoding="utf-8")

        mock_router = MagicMock()
        mock_router.send_note_draft.return_value = True

        # save_draft の import が ImportError を上げるシナリオ
        with (
            patch("src.ops.note_generator.generate", return_value=self._MOCK_MD),
            patch("src.ops.note_generator._db", return_value=MagicMock()),
            patch(
                "src.ops.note_generator.select_recommended_races",
                return_value=_make_recommended(3),
            ),
            patch("scripts.scheduler._notify_note_article_summary"),
            patch("scripts.scheduler._send_discord"),
            patch("scripts.scheduler._mark_job_done"),
            patch(
                "src.notification.router.NotificationRouter", return_value=mock_router
            ),
            patch.object(sched, "_ROOT", tmp_path),
            patch.dict(os.environ, {"NOTE_DRAFT_AUTO_POST": "1"}, clear=False),
        ):
            # save_draft をインポートしようとしたら ImportError
            import builtins

            real_import = builtins.__import__

            def _mock_import(name, *args, **kwargs):
                if name == "src.ops.note_draft_publisher":
                    raise ImportError("playwright not installed")
                return real_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=_mock_import):
                sched.job_note_daily_article()  # 例外を出さないこと


# ── スケジュール登録確認 ───────────────────────────────────────────────────────


class TestScheduleRegistration:
    def test_job_note_daily_article_in_catchup_hours(self) -> None:
        assert "job_note_daily_article" in sched._CATCHUP_HOURS
        assert sched._CATCHUP_HOURS["job_note_daily_article"] == 3

    def test_job_note_daily_article_in_job_schedules(self) -> None:
        assert "job_note_daily_article" in sched._JOB_SCHEDULES
        schedules = sched._JOB_SCHEDULES["job_note_daily_article"]
        # 土曜(5) と 日曜(6) の 10:30 が登録されていること
        assert (5, 10, 30) in schedules
        assert (6, 10, 30) in schedules

    def test_job_note_daily_article_in_job_map_full(self) -> None:
        assert "job_note_daily_article" in sched._JOB_MAP_FULL
        assert (
            sched._JOB_MAP_FULL["job_note_daily_article"]
            is sched.job_note_daily_article
        )

    def test_note_article_in_cli_job_map(self) -> None:
        assert "note_article" in sched._JOB_MAP
        assert sched._JOB_MAP["note_article"] is sched.job_note_daily_article
