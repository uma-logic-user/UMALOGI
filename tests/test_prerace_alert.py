"""
src/notification/prerace_alert.py のユニットテスト。

SQLite をインメモリ DB で代替し、実際の Discord 送信はモックする。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.notification.prerace_alert import (
    PRERACE_ALERT_EV_THRESHOLD,
    _build_alert_message,
    _estimate_start,
    _format_stake,
    check_and_send_prerace_alerts,
)


# ── フィクスチャ ──────────────────────────────────────────────────────────────


def _make_db(races: list[dict], predictions: list[dict]) -> str:
    """インメモリ SQLite にテストデータを投入して ':memory:' パスを返す。

    実際の関数に渡すため NamedTemporaryFile を使う。
    """
    import tempfile

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute(
        """
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT NOT NULL DEFAULT '',
            date TEXT NOT NULL,
            venue TEXT NOT NULL DEFAULT '',
            race_number INTEGER NOT NULL,
            distance INTEGER NOT NULL DEFAULT 0,
            surface TEXT NOT NULL DEFAULT '',
            weather TEXT NOT NULL DEFAULT '',
            condition TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT '',
            track_direction TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id TEXT NOT NULL,
            model_type TEXT NOT NULL,
            bet_type TEXT NOT NULL,
            confidence REAL,
            expected_value REAL,
            recommended_bet REAL,
            notes TEXT,
            combination_json TEXT,
            created_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    for r in races:
        conn.execute(
            "INSERT INTO races (race_id, race_name, date, venue, race_number) VALUES (?,?,?,?,?)",
            (
                r["race_id"],
                r.get("race_name", ""),
                r["date"],
                r.get("venue", "東京"),
                r["race_number"],
            ),
        )
    for p in predictions:
        conn.execute(
            "INSERT INTO predictions (race_id, model_type, bet_type, expected_value, recommended_bet) VALUES (?,?,?,?,?)",
            (
                p["race_id"],
                p.get("model_type", "本命"),
                p.get("bet_type", "単勝"),
                p["expected_value"],
                p.get("recommended_bet", 5000),
            ),
        )
    conn.commit()
    conn.close()
    return tmp.name


def _cleanup_db(path: str) -> None:
    import os

    try:
        os.unlink(path)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def reset_notified():
    """各テスト前後に _notified_races と _notified_date をリセットする。"""
    import src.notification.prerace_alert as m

    m._notified_races.clear()
    m._notified_date = ""
    yield
    m._notified_races.clear()
    m._notified_date = ""


# ── _estimate_start ────────────────────────────────────────────────────────────


class TestEstimateStart:
    def test_r1_is_10_00(self):
        dt = _estimate_start("2026/05/31", 1)
        assert dt.hour == 10
        assert dt.minute == 0

    def test_r2_is_10_30(self):
        dt = _estimate_start("2026/05/31", 2)
        assert dt.hour == 10
        assert dt.minute == 30

    def test_r12_is_15_30(self):
        dt = _estimate_start("2026/05/31", 12)
        assert dt.hour == 15
        assert dt.minute == 30

    def test_accepts_hyphen_date(self):
        dt = _estimate_start("2026-05-31", 1)
        assert dt.hour == 10

    def test_date_preserved(self):
        dt = _estimate_start("2026/06/07", 5)
        assert dt.year == 2026
        assert dt.month == 6
        assert dt.day == 7


# ── _format_stake ─────────────────────────────────────────────────────────────


class TestFormatStake:
    def test_none_returns_dash(self):
        assert _format_stake(None) == "—"

    def test_zero_returns_dash(self):
        assert _format_stake(0) == "—"

    def test_negative_returns_dash(self):
        assert _format_stake(-100) == "—"

    def test_rounds_down_to_100(self):
        assert _format_stake(5999) == "¥5,900"

    def test_exact_5000(self):
        assert _format_stake(5000) == "¥5,000"

    def test_large_value(self):
        assert _format_stake(20000) == "¥20,000"


# ── _build_alert_message ──────────────────────────────────────────────────────


class TestBuildAlertMessage:
    def test_contains_at_everyone(self):
        race_info = {"venue": "東京", "race_number": 11, "race_name": "東京優駿"}
        preds = [
            {
                "model_type": "本命",
                "bet_type": "単勝",
                "expected_value": 1.5,
                "recommended_bet": 5000,
            }
        ]
        start = datetime(2026, 6, 7, 15, 0)
        msg = _build_alert_message("202606070511", race_info, preds, start)
        assert "@everyone" in msg

    def test_contains_venue_and_race_number(self):
        race_info = {"venue": "中山", "race_number": 9, "race_name": ""}
        preds = [
            {
                "model_type": "ALPHA",
                "bet_type": "複勝",
                "expected_value": 1.3,
                "recommended_bet": 3000,
            }
        ]
        start = datetime(2026, 6, 7, 14, 0)
        msg = _build_alert_message("202606060909", race_info, preds, start)
        assert "中山" in msg
        assert "9R" in msg

    def test_contains_ev_and_stake(self):
        race_info = {"venue": "阪神", "race_number": 6, "race_name": ""}
        preds = [
            {
                "model_type": "卍",
                "bet_type": "馬連",
                "expected_value": 1.85,
                "recommended_bet": 7000,
            }
        ]
        start = datetime(2026, 6, 7, 12, 30)
        msg = _build_alert_message("202606060606", race_info, preds, start)
        assert "1.85" in msg
        assert "¥7,000" in msg

    def test_contains_start_time(self):
        race_info = {"venue": "東京", "race_number": 11, "race_name": ""}
        preds = [
            {
                "model_type": "本命",
                "bet_type": "単勝",
                "expected_value": 2.0,
                "recommended_bet": 5000,
            }
        ]
        start = datetime(2026, 6, 7, 15, 0)
        msg = _build_alert_message("202606070511", race_info, preds, start)
        assert "15:00" in msg

    def test_race_name_shown_when_present(self):
        race_info = {"venue": "東京", "race_number": 11, "race_name": "日本ダービー"}
        preds = [
            {
                "model_type": "本命",
                "bet_type": "単勝",
                "expected_value": 1.5,
                "recommended_bet": 5000,
            }
        ]
        start = datetime(2026, 6, 7, 15, 0)
        msg = _build_alert_message("202606070511", race_info, preds, start)
        assert "日本ダービー" in msg


# ── check_and_send_prerace_alerts ─────────────────────────────────────────────


class TestCheckAndSendPreraceAlerts:
    def _now_14min_before_r11(self, date_str: str = "2026-05-31") -> datetime:
        """R11 (15:00) の14分前 = 14:46"""
        base = datetime.fromisoformat(date_str)
        return base.replace(hour=14, minute=46)

    def test_sends_alert_for_qualifying_race(self):
        """EV >= 閾値 の予想があり 14〜16 分前のレースを1件通知する。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": PRERACE_ALERT_EV_THRESHOLD + 0.1,
                    "recommended_bet": 5000,
                }
            ],
        )
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 1
        mock_dispatch.assert_called_once()
        _cleanup_db(db_path)

    def test_skips_race_below_ev_threshold(self):
        """EV が閾値未満の予想しかないレースは通知しない。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": PRERACE_ALERT_EV_THRESHOLD - 0.1,
                    "recommended_bet": 5000,
                }
            ],
        )
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 0
        mock_dispatch.assert_not_called()
        _cleanup_db(db_path)

    def test_no_duplicate_notification(self):
        """同じレースに対して2回目の呼び出しでは通知しない。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": PRERACE_ALERT_EV_THRESHOLD + 0.1,
                    "recommended_bet": 5000,
                }
            ],
        )
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count1 = check_and_send_prerace_alerts(db_path=db_path, now=now)
            count2 = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count1 == 1
        assert count2 == 0
        assert mock_dispatch.call_count == 1
        _cleanup_db(db_path)

    def test_skips_race_too_far_in_future(self):
        """発走まで17分以上あるレース（ウィンドウ外）は通知しない。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": PRERACE_ALERT_EV_THRESHOLD + 0.5,
                    "recommended_bet": 5000,
                }
            ],
        )
        # R11 = 15:00、17分前 = 14:43 → ウィンドウ外
        now = datetime.fromisoformat(date_str).replace(hour=14, minute=43)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 0
        mock_dispatch.assert_not_called()
        _cleanup_db(db_path)

    def test_skips_race_already_past_window(self):
        """発走まで13分以下（ウィンドウ後）は通知しない。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": PRERACE_ALERT_EV_THRESHOLD + 0.5,
                    "recommended_bet": 5000,
                }
            ],
        )
        # R11 = 15:00、12分前 = 14:48 → ウィンドウ後
        now = datetime.fromisoformat(date_str).replace(hour=14, minute=48)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 0
        mock_dispatch.assert_not_called()
        _cleanup_db(db_path)

    def test_no_races_today_returns_zero(self):
        """当日レースがなければ 0 を返す。"""
        date_str = "2026-05-31"
        db_path = _make_db(races=[], predictions=[])
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 0
        mock_dispatch.assert_not_called()
        _cleanup_db(db_path)

    def test_multiple_qualifying_predictions_sent_once_per_race(self):
        """EV >= 閾値 の予想が複数あっても、レース単位で1回だけ通知する。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                }
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "model_type": "本命",
                    "expected_value": 1.8,
                    "recommended_bet": 5000,
                },
                {
                    "race_id": "202605310511",
                    "model_type": "ALPHA",
                    "expected_value": 1.5,
                    "recommended_bet": 3000,
                },
                {
                    "race_id": "202605310511",
                    "model_type": "卍",
                    "expected_value": 1.3,
                    "recommended_bet": 2000,
                },
            ],
        )
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 1
        mock_dispatch.assert_called_once()
        # メッセージに3件の予想が含まれることを確認（positional 第3引数）
        args, kwargs = mock_dispatch.call_args
        message = kwargs.get("message") or (args[2] if len(args) > 2 else "")
        assert "3件" in message
        _cleanup_db(db_path)

    def test_different_races_each_notified_once(self):
        """同じ時刻帯に複数レースがある場合、各レース1回ずつ通知する。"""
        date_str = "2026-05-31"
        db_path = _make_db(
            races=[
                {
                    "race_id": "202605310511",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "東京",
                },
                {
                    "race_id": "202605310611",
                    "date": date_str,
                    "race_number": 11,
                    "venue": "中山",
                },
            ],
            predictions=[
                {
                    "race_id": "202605310511",
                    "expected_value": 1.5,
                    "recommended_bet": 5000,
                },
                {
                    "race_id": "202605310611",
                    "expected_value": 1.8,
                    "recommended_bet": 6000,
                },
            ],
        )
        now = self._now_14min_before_r11(date_str)

        with patch("src.notification.prerace_alert._dispatch_alert") as mock_dispatch:
            count = check_and_send_prerace_alerts(db_path=db_path, now=now)

        assert count == 2
        assert mock_dispatch.call_count == 2
        _cleanup_db(db_path)


# ── notify_prerace_15min (Router) ─────────────────────────────────────────────


class TestNotifyPrerace15min:
    def test_sends_to_ev_alert_channel(self):
        """ev_alert チャンネルが設定されている場合はそちらへ送信する。"""
        import os
        from src.notification.router import NotificationRouter

        env = {
            "DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/test/predict",
            "DISCORD_WEBHOOK_EV_ALERT": "https://discord.com/api/webhooks/test/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("requests.post") as mock_post:
                mock_post.return_value = MagicMock(status_code=204)
                router = NotificationRouter()
                router.notify_prerace_15min(
                    race_id="202606070511",
                    max_ev=1.85,
                    message="@everyone テストメッセージ",
                )
        mock_post.assert_called()
        call_json = mock_post.call_args[1]["json"]
        assert "テストメッセージ" in call_json.get("content", "")

    def test_fallback_to_prediction_when_ev_alert_unset(self):
        """ev_alert 未設定時は prediction チャンネルへフォールバックする。"""
        import os
        from src.notification.router import NotificationRouter

        env = {
            "DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/test/predict",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("requests.post") as mock_post:
                mock_post.return_value = MagicMock(status_code=204)
                router = NotificationRouter()
                router.notify_prerace_15min(
                    race_id="202606070511",
                    max_ev=1.85,
                    message="@everyone フォールバックテスト",
                )
        mock_post.assert_called()

    def test_no_send_when_no_webhook_configured(self):
        """Webhook が未設定の場合は送信しない（エラーにもならない）。"""
        import os
        from src.notification.router import NotificationRouter

        with patch.dict(os.environ, {}, clear=True):
            with patch("requests.post") as mock_post:
                router = NotificationRouter()
                router.notify_prerace_15min(
                    race_id="202606070511",
                    max_ev=1.85,
                    message="@everyone 未設定テスト",
                )
        mock_post.assert_not_called()
