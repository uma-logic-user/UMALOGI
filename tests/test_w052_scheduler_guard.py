"""W-052: スケジューラ暴走（post_race の全年度再シミュレーション）根本修正のテスト。

検証:
  1. post_race_pipeline(retrain=False) は増分学習(IncrementalTrainer)を呼ばない
  2. post_race_pipeline(retrain=True) は増分学習を呼ぶ
  3. batch_evaluate_date は既定 retrain=False を post_race_pipeline へ伝播する
  4. weekly_retrain は土日に full_retrain を実行しない（条項2ガード）
  5. weekly_retrain は平日に full_retrain を実行する
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _eval_stub() -> SimpleNamespace:
    return SimpleNamespace(hit_count=0, hits=[], roi=0.0)


def test_post_race_pipeline_skips_retrain_when_false() -> None:
    from src.ops.retrain_trigger import post_race_pipeline

    with (
        patch("src.evaluation.evaluator.Evaluator") as E,
        patch("src.notification.dispatcher.NotificationDispatcher") as D,
        patch("src.ml.incremental.IncrementalTrainer") as T,
    ):
        E.return_value.evaluate_race.return_value = _eval_stub()
        D.return_value.dispatch.return_value = []
        result = post_race_pipeline(
            MagicMock(), "202605021201", notify=False, retrain=False
        )
        T.assert_not_called()
        assert result["model"] == {}


def test_post_race_pipeline_runs_retrain_when_true() -> None:
    from src.ops.retrain_trigger import post_race_pipeline

    with (
        patch("src.evaluation.evaluator.Evaluator") as E,
        patch("src.notification.dispatcher.NotificationDispatcher") as D,
        patch("src.ml.incremental.IncrementalTrainer") as T,
    ):
        E.return_value.evaluate_race.return_value = _eval_stub()
        D.return_value.dispatch.return_value = []
        T.return_value.incremental_update.return_value = {}
        post_race_pipeline(MagicMock(), "202605021201", notify=False, retrain=True)
        T.return_value.incremental_update.assert_called_once()


def test_batch_evaluate_date_defaults_to_no_retrain() -> None:
    import sqlite3

    from src.ops import retrain_trigger

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE races (race_id TEXT, date TEXT)")
    conn.executemany(
        "INSERT INTO races VALUES (?, '2026-05-31')",
        [("202605021201",), ("202605021202",)],
    )
    conn.commit()

    with patch.object(retrain_trigger, "post_race_pipeline", return_value={}) as P:
        retrain_trigger.batch_evaluate_date(conn, "2026-05-31", notify=False)

    assert P.call_count == 2
    for call in P.call_args_list:
        assert call.kwargs["retrain"] is False


def test_weekly_retrain_skips_on_weekend() -> None:
    from src.ops.retrain_trigger import weekly_retrain

    saturday = date(2026, 5, 30)
    with patch("src.ml.incremental.IncrementalTrainer") as T:
        result = weekly_retrain(MagicMock(), today=saturday)
        assert result == {}
        T.assert_not_called()


def test_weekly_retrain_runs_on_weekday() -> None:
    from src.ops.retrain_trigger import weekly_retrain

    monday = date(2026, 6, 1)
    with patch("src.ml.incremental.IncrementalTrainer") as T:
        T.return_value.full_retrain.return_value = {"honmei": "v1"}
        result = weekly_retrain(MagicMock(), validate=False, today=monday)
        T.return_value.full_retrain.assert_called_once()
        assert result == {"honmei": "v1"}


def test_weekly_retrain_weekend_override() -> None:
    from src.ops.retrain_trigger import weekly_retrain

    sunday = date(2026, 5, 31)
    with patch("src.ml.incremental.IncrementalTrainer") as T:
        T.return_value.full_retrain.return_value = {"honmei": "v2"}
        result = weekly_retrain(
            MagicMock(), validate=False, today=sunday, allow_weekend=True
        )
        T.return_value.full_retrain.assert_called_once()
        assert result == {"honmei": "v2"}
