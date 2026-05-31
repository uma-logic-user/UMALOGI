"""job_fit_manji_calibrator（卍較正器 週次再学習ジョブ）の登録テスト。"""

from __future__ import annotations

import importlib


def test_fit_calibrator_registered_in_schedules_and_maps() -> None:
    sched = importlib.import_module("scripts.scheduler")

    # スケジュール定義（月曜=weekday0・03:00）
    assert "job_fit_manji_calibrator" in sched._JOB_SCHEDULES
    assert sched._JOB_SCHEDULES["job_fit_manji_calibrator"] == [(0, 3, 0)]

    # 取りこぼし許容時間が定義されている
    assert "job_fit_manji_calibrator" in sched._CATCHUP_HOURS
    assert sched._CATCHUP_HOURS["job_fit_manji_calibrator"] > 0

    # catchup 回復マップ・CLI マップに関数が登録されている
    assert (
        sched._JOB_MAP_FULL["job_fit_manji_calibrator"]
        is sched.job_fit_manji_calibrator
    )
    assert sched._JOB_MAP["fit_calibrator"] is sched.job_fit_manji_calibrator

    # ジョブ本体が callable
    assert callable(sched.job_fit_manji_calibrator)


def test_fit_calibrator_job_runs_fit(monkeypatch) -> None:
    """job 本体が fit_manji_win_calibrator を呼ぶ（DB/モデルはスタブ）。"""
    sched = importlib.import_module("scripts.scheduler")
    import src.ml.manji_calibration as MC
    import src.database.init_db as DB

    class _Conn:
        def close(self) -> None:
            pass

    monkeypatch.setattr(DB, "init_db", lambda: _Conn())
    calls = {}
    monkeypatch.setattr(
        MC,
        "fit_manji_win_calibrator",
        lambda conn, **k: calls.setdefault("fit", True) or {"fitted": True},
    )
    monkeypatch.setattr(
        sched, "_mark_job_done", lambda name: calls.setdefault("done", name)
    )

    sched.job_fit_manji_calibrator()
    assert calls.get("fit") is True
    assert calls.get("done") == "job_fit_manji_calibrator"
