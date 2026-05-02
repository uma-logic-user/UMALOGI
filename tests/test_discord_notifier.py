"""
src/notification/discord_notifier.py のユニットテスト。

requests.post をモックして実際の HTTP 送信は行わない。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.notification.discord_notifier import DiscordNotifier, _format_race_label


# ── ヘルパー ─────────────────────────────────────────────────────


def _make_notifier(url: str = "https://discord.example/webhook") -> DiscordNotifier:
    return DiscordNotifier(webhook_url=url)


def _mock_response(status: int = 204) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = ""
    resp.raise_for_status = MagicMock()
    return resp


# ── _format_race_label ────────────────────────────────────────────


@pytest.mark.parametrize("race_id,expected", [
    # race_id[4:6] が会場コード、race_id[10:12] がレース番号
    ("202505050701", "東京 1R"),     # 05=東京, race_num=01
    ("202509050911", "阪神 11R"),   # 09=阪神, race_num=11
    ("202501010101", "札幌 1R"),    # 01=札幌, race_num=01
])
def test_format_race_label(race_id: str, expected: str) -> None:
    assert _format_race_label(race_id) == expected


# ── _sanitize ────────────────────────────────────────────────────


def test_sanitize_removes_null_byte() -> None:
    n = _make_notifier()
    assert n._sanitize("hello\x00world") == "helloworld"


def test_sanitize_strips_whitespace() -> None:
    n = _make_notifier()
    assert n._sanitize("  hello  ") == "hello"


# ── send_text ────────────────────────────────────────────────────


def test_send_text_calls_post() -> None:
    n = _make_notifier()
    with patch("src.notification.discord_notifier.requests.post", return_value=_mock_response()) as mock_post:
        n.send_text("テストメッセージ")
    mock_post.assert_called_once()
    payload = mock_post.call_args[1]["json"]
    assert "テストメッセージ" in payload["content"]


def test_send_text_no_url_skips(caplog: pytest.LogCaptureFixture) -> None:
    n = DiscordNotifier(webhook_url="", enabled=True)
    with caplog.at_level("WARNING"):
        n.send_text("this should be skipped")
    assert "スキップ" in caplog.text or "未設定" in caplog.text


# ── notify_skip ──────────────────────────────────────────────────


def test_notify_skip_logs_and_sends() -> None:
    n = _make_notifier()
    with patch("src.notification.discord_notifier.requests.post", return_value=_mock_response()) as mock_post:
        n.notify_skip("202505050701", "オッズ欠損 100%")
    mock_post.assert_called_once()
    content = mock_post.call_args[1]["json"]["content"]
    assert "見送り" in content


# ── notify_scraping_alert ─────────────────────────────────────────


def test_notify_scraping_alert_sends_emergency_text() -> None:
    n = _make_notifier()
    with patch("src.notification.discord_notifier.requests.post", return_value=_mock_response()) as mock_post:
        n.notify_scraping_alert("202505050701", "0頭取得")
    mock_post.assert_called_once()
    content = mock_post.call_args[1]["json"]["content"]
    assert "緊急" in content or "スクレイピング" in content


# ── notify_prerace_result ─────────────────────────────────────────


def _make_mock_bets(model_type: str, ev: float = 0.5) -> MagicMock:
    bet = MagicMock()
    bet.bet_type = "単勝"
    bet.combinations = [[3]]
    bet.horse_names  = ["テスト馬"]
    bet.expected_value = ev
    bet.recommended_bet = 1000
    bet.model_score = 0.3
    bets_obj = MagicMock()
    bets_obj.model_type = model_type
    bets_obj.bets = [bet]
    return bets_obj


def test_notify_prerace_result_all_ev_zero_skips(caplog: pytest.LogCaptureFixture) -> None:
    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=0.0)
    manji  = _make_mock_bets("卍",   ev=0.0)
    with caplog.at_level("INFO"):
        n.notify_prerace_result("202505050701", honmei, manji)
    assert "スキップ" in caplog.text or "skip" in caplog.text.lower()


def test_notify_prerace_result_sends_embed() -> None:
    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=1.5)
    manji  = _make_mock_bets("卍",   ev=2.0)
    with patch("src.notification.discord_notifier.requests.post", return_value=_mock_response()) as mock_post:
        n.notify_prerace_result("202505050701", honmei, manji)
    mock_post.assert_called_once()
    body = mock_post.call_args[1]["json"]
    assert "embeds" in body
    assert len(body["embeds"]) == 1
