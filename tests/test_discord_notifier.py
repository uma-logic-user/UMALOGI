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


@pytest.mark.parametrize(
    "race_id,expected",
    [
        # race_id[4:6] が会場コード、race_id[10:12] がレース番号
        ("202505050701", "東京 1R"),  # 05=東京, race_num=01
        ("202509050911", "阪神 11R"),  # 09=阪神, race_num=11
        ("202501010101", "札幌 1R"),  # 01=札幌, race_num=01
    ],
)
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
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.send_text("テストメッセージ")
    mock_post.assert_called_once()
    payload = mock_post.call_args[1]["json"]
    assert "テストメッセージ" in payload["content"]


def test_send_text_no_url_skips(caplog: pytest.LogCaptureFixture) -> None:
    with patch.dict(
        "os.environ",
        {"DISCORD_WEBHOOK_URL": "", "DISCORD_SYSTEM_WEBHOOK_URL": ""},
        clear=False,
    ):
        n = DiscordNotifier(enabled=False)  # enabled=False でコンストラクタ警告を抑制
    with caplog.at_level("WARNING", logger="src.notification.discord_notifier"):
        n.send_text("this should be skipped")
    assert "スキップ" in caplog.text or "未設定" in caplog.text


# ── notify_skip ──────────────────────────────────────────────────


def test_notify_skip_logs_only(caplog: pytest.LogCaptureFixture) -> None:
    n = _make_notifier()
    with patch("src.notification.discord_notifier.requests.post") as mock_post:
        with caplog.at_level("WARNING", logger="src.notification.discord_notifier"):
            n.notify_skip("202505050701", "オッズ欠損 100%")
    mock_post.assert_not_called()
    assert "見送り" in caplog.text


# ── notify_scraping_alert ─────────────────────────────────────────


def test_notify_scraping_alert_sends_emergency_text() -> None:
    n = _make_notifier()
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.notify_scraping_alert("202505050701", "0頭取得")
    mock_post.assert_called_once()
    payload = mock_post.call_args[1]["json"]
    # notify_scraping_alert は send_system_embed 経由で embeds 形式で送信する
    assert "embeds" in payload
    title = payload["embeds"][0].get("title", "")
    assert "スクレイピング" in title or "異常" in title


# ── notify_prerace_result ─────────────────────────────────────────


def _make_mock_bets(model_type: str, ev: float = 0.5) -> MagicMock:
    bet = MagicMock()
    bet.bet_type = "単勝"
    bet.combinations = [[3]]
    bet.horse_names = ["テスト馬"]
    bet.expected_value = ev
    bet.recommended_bet = 1000
    bet.model_score = 0.3
    bets_obj = MagicMock()
    bets_obj.model_type = model_type
    bets_obj.bets = [bet]
    return bets_obj


def test_notify_prerace_result_all_ev_zero_skips(
    caplog: pytest.LogCaptureFixture,
) -> None:
    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=0.0)
    manji = _make_mock_bets("卍", ev=0.0)
    with caplog.at_level("INFO"):
        n.notify_prerace_result("202505050701", honmei, manji)
    assert "スキップ" in caplog.text or "skip" in caplog.text.lower()


def test_notify_prerace_result_sends_embed() -> None:
    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=1.5)
    manji = _make_mock_bets("卍", ev=2.0)
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.notify_prerace_result("202505050701", honmei, manji)
    mock_post.assert_called_once()
    body = mock_post.call_args[1]["json"]
    assert "embeds" in body
    assert len(body["embeds"]) == 1


# ── notify_prerace_result プレミアム Embed（格付け色・投資比率バー）────────


def test_notify_prerace_result_grade_color_and_stake_bar() -> None:
    from src.notification.embed_builder import COLOR_G1

    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=1.5)
    manji = _make_mock_bets("卍", ev=2.0)
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.notify_prerace_result(
            "202505050701",
            honmei,
            manji,
            race_name="安田記念（GⅠ）",
            bankroll=100_000,
        )
    embed = mock_post.call_args[1]["json"]["embeds"][0]
    assert embed["color"] == COLOR_G1
    assert "G1" in embed["title"]
    field_names = [f["name"] for f in embed["fields"]]
    assert any("推奨投資比率" in fn for fn in field_names)
    joined = " ".join(str(f["value"]) for f in embed["fields"])
    assert "█" in joined or "░" in joined


def test_notify_prerace_result_confidence_gradient_without_grade() -> None:
    from src.notification.embed_builder import confidence_color

    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=1.2)
    manji = _make_mock_bets("卍", ev=1.2)
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.notify_prerace_result(
            "202505050701", honmei, manji, race_name="3歳上1勝クラス", confidence=0.4
        )
    embed = mock_post.call_args[1]["json"]["embeds"][0]
    assert embed["color"] == confidence_color(0.4)


def test_notify_prerace_result_top_signal_grid() -> None:
    n = _make_notifier()
    honmei = _make_mock_bets("本命", ev=1.8)
    honmei.bets[0].bet_type = "三連複"
    honmei.bets[0].combinations = [[5, 3, 9], [5, 3, 12], [5, 9, 12]]
    honmei.bets[0].horse_names = ["アーバンシック", "", ""]
    honmei.bets[0].odds = 48.3
    manji = _make_mock_bets("卍", ev=0.5)
    with patch(
        "src.notification.discord_notifier.requests.post", return_value=_mock_response()
    ) as mock_post:
        n.notify_prerace_result("202505050701", honmei, manji)
    embed = mock_post.call_args[1]["json"]["embeds"][0]
    field_names = [f["name"] for f in embed["fields"]]
    assert any("軸馬" in fn for fn in field_names)
    assert any("相手馬" in fn for fn in field_names)
    grid = [f for f in embed["fields"] if f.get("inline")]
    assert len(grid) >= 3
    joined = " ".join(str(f["value"]) for f in grid)
    assert "1.80" in joined  # EV
    assert "48.3" in joined  # 想定オッズ
