"""scheduler.py の取りこぼしリカバリーロジックのユニットテスト。"""

from __future__ import annotations

from datetime import datetime, timedelta


def _should_recover(
    last_run: datetime | None,
    scheduled_today: datetime,
    now: datetime,
    catchup_hours: int,
) -> bool:
    from scripts.scheduler import _should_recover as _sr

    return _sr(last_run, scheduled_today, now, catchup_hours)


class TestShouldRecover:
    def _make(self, hour: int, minute: int = 0, date_offset: int = 0) -> datetime:
        base = datetime(2026, 5, 2, hour, minute)
        return base + timedelta(days=date_offset)

    def test_never_run_and_within_window(self) -> None:
        """未実行・許容窓内 → 再実行すべき"""
        scheduled = self._make(7, 30)
        now = self._make(9, 0)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is True

    def test_never_run_but_window_expired(self) -> None:
        """未実行・許容窓超過 → 再実行しない"""
        scheduled = self._make(7, 30)
        now = self._make(14, 0)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False

    def test_already_run_today(self) -> None:
        """当日実行済み → 再実行しない"""
        scheduled = self._make(7, 30)
        now = self._make(9, 0)
        last_run = self._make(7, 31)
        assert _should_recover(last_run, scheduled, now, catchup_hours=4) is False

    def test_ran_yesterday_within_window(self) -> None:
        """前日実行・本日まだ・許容窓内 → 再実行すべき"""
        scheduled = self._make(7, 30)
        now = self._make(9, 0)
        last_run = self._make(7, 31, date_offset=-1)
        assert _should_recover(last_run, scheduled, now, catchup_hours=4) is True

    def test_scheduled_time_not_yet_passed(self) -> None:
        """スケジュール時刻まだ → 実行しない"""
        scheduled = self._make(20, 0)
        now = self._make(10, 0)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False

    def test_exact_boundary_at_window_edge(self) -> None:
        """ちょうど窓の境界（許容時間ちょうど）→ 実行しない（exclusive）"""
        scheduled = self._make(7, 30)
        now = self._make(11, 30)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False


class TestAbReportSchedule:
    """job_ab_report がスケジューラーに正しく登録されているかを検証する。"""

    def test_job_ab_report_in_job_schedules(self) -> None:
        """_JOB_SCHEDULES に job_ab_report が存在すること。"""
        from scripts.scheduler import _JOB_SCHEDULES

        assert "job_ab_report" in _JOB_SCHEDULES

    def test_job_ab_report_scheduled_sunday_1800(self) -> None:
        """日曜 18:00 (weekday=6) にスケジュールされていること。"""
        from scripts.scheduler import _JOB_SCHEDULES

        entries = _JOB_SCHEDULES["job_ab_report"]
        assert (6, 18, 0) in entries, f"日曜18:00エントリーが見つかりません: {entries}"

    def test_job_ab_report_in_recovery_map(self) -> None:
        """_JOB_MAP_FULL（取りこぼしリカバリー対象）に含まれること。"""
        from scripts.scheduler import _JOB_MAP_FULL

        assert "job_ab_report" in _JOB_MAP_FULL

    def test_job_ab_report_cli_alias(self) -> None:
        """--run-now ab_report で即時実行できるように _JOB_MAP に登録されていること。"""
        from scripts.scheduler import _JOB_MAP

        assert "ab_report" in _JOB_MAP
