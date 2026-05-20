# tests/test_router.py
"""
src/notification/router.py のユニットテスト。
requests.post をモックして実際の HTTP 送信は行わない。
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


# ── _chunk_text ──────────────────────────────────────────────────────────────

class TestChunkText:
    def test_short_text_returned_as_single_chunk(self):
        from src.notification.router import _chunk_text
        assert _chunk_text("Hello world", max_len=100) == ["Hello world"]

    def test_split_at_double_newline(self):
        from src.notification.router import _chunk_text
        text = "段落1\n\n段落2"
        result = _chunk_text(text, max_len=6)
        assert len(result) == 2
        assert "段落1" in result[0]
        assert "段落2" in result[1]

    def test_split_at_single_newline_when_no_double(self):
        from src.notification.router import _chunk_text
        text = "行1\n行2"
        result = _chunk_text(text, max_len=4)
        assert len(result) == 2

    def test_hard_cut_when_no_newline(self):
        from src.notification.router import _chunk_text
        text = "A" * 200
        result = _chunk_text(text, max_len=100)
        assert len(result) == 2
        for chunk in result:
            assert len(chunk) <= 100

    def test_3600_chars_splits_into_2_chunks(self):
        from src.notification.router import _chunk_text
        text = "A" * 3600
        result = _chunk_text(text, max_len=1800)
        assert len(result) == 2

    def test_empty_string_returns_one_empty_chunk(self):
        from src.notification.router import _chunk_text
        result = _chunk_text("", max_len=100)
        assert result == [""]


# ── _generate_x_post ─────────────────────────────────────────────────────────

class TestGenerateXPost:
    def test_result_under_140_chars(self):
        from src.notification.router import _generate_x_post
        title = "🏇【UMALOGI週次レポート】2026-05-18号 — 全モデル成績公開＆今週のAI厳選予想"
        body = "## 万馬券3本的中！ALPHAモデルROI203%達成\n\n本文コンテンツ..."
        result = _generate_x_post(title, body)
        assert len(result) <= 140

    def test_contains_umalogi_hashtag(self):
        from src.notification.router import _generate_x_post
        result = _generate_x_post("タイトル", "本文")
        assert "#UMALOGI" in result

    def test_contains_keiba_hashtag(self):
        from src.notification.router import _generate_x_post
        result = _generate_x_post("タイトル", "本文")
        assert "#競馬" in result

    def test_subtitle_extracted_from_body(self):
        from src.notification.router import _generate_x_post
        body = "前文\n## サブタイトルです\n本文"
        result = _generate_x_post("タイトル", body)
        assert "サブタイトルです" in result


# ── NotificationRouter 初期化 ─────────────────────────────────────────────────

class TestNotificationRouterInit:
    def test_no_env_vars_no_exception(self):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            assert router._get("prediction") is None

    def test_prediction_channel_configured_when_url_set(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("prediction") is not None

    def test_ev_alert_unset_falls_back_to_prediction(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("ev_alert") is router._get("prediction")

    def test_ev_alert_set_returns_separate_instance(self):
        from src.notification.router import NotificationRouter
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("ev_alert") is not router._get("prediction")

    def test_system_backward_compat_old_env_var(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_SYSTEM_WEBHOOK_URL": "https://example.com/sys"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("system") is not None

    def test_new_system_env_var_takes_precedence(self):
        from src.notification.router import NotificationRouter
        env = {
            "DISCORD_WEBHOOK_SYSTEM":    "https://example.com/new_sys",
            "DISCORD_SYSTEM_WEBHOOK_URL": "https://example.com/old_sys",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            notifier = router._get("system")
            assert notifier is not None
            assert notifier._url == "https://example.com/new_sys"


# ── send_note_draft ──────────────────────────────────────────────────────────

def _mock_post_ok() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 204
    resp.text = ""
    return resp


class TestSendNoteDraft:
    @patch("src.notification.discord_notifier.requests.post")
    def test_returns_false_when_no_url(self, mock_post):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            result = router.send_note_draft(title="テスト", body="コンテンツ")
        assert result is False
        mock_post.assert_not_called()

    @patch("src.notification.discord_notifier.requests.post")
    def test_returns_true_and_sends_when_url_set(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            result = router.send_note_draft(title="テスト", body="短いコンテンツ")
        assert result is True
        assert mock_post.call_count >= 1

    @patch("src.notification.discord_notifier.requests.post")
    def test_pagination_header_in_first_chunk(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "【note下書き (1/2)】" in first_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_pagination_header_in_second_chunk(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        second_content = mock_post.call_args_list[1][1]["json"]["content"]
        assert "【note下書き (2/2)】" in second_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_chunks_wrapped_in_code_block(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="本文コンテンツ")
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "```markdown" in first_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_x_post_sent_as_last_message(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(
                title="テスト", body="短い本文", x_post="カスタムツイート文"
            )
        last_content = mock_post.call_args_list[-1][1]["json"]["content"]
        assert "X（Twitter）告知ポスト" in last_content
        assert "カスタムツイート文" in last_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_3600_char_body_sends_3_messages(self, mock_post):
        # 2 chunks + 1 x_post = 3 messages
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        assert mock_post.call_count == 3

    @patch("src.notification.discord_notifier.requests.post")
    def test_final_chunk_has_end_marker(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="短い本文")
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "（以上）" in first_content


# ── EV 激熱アラート ──────────────────────────────────────────────────────────

class TestEvAlert:
    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_sent_when_max_ev_meets_threshold(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD + 0.1
            bet.recommended_bet = 1000
            bets = MagicMock()
            bets.bets = [bet]
            router.notify_prerace_result("2026051905010911", bets, bets)
        # At least 2 POST calls: prediction embed + ev_alert embed
        assert mock_post.call_count >= 2

    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_not_sent_below_threshold(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD - 0.1
            bet.recommended_bet = 500
            bets = MagicMock()
            bets.bets = [bet]
            router.notify_prerace_result("2026051905010911", bets, bets)
            # ev_alert URL "https://example.com/ev" should NOT be called
            for c in mock_post.call_args_list:
                url_arg = c[0][0] if c[0] else c[1].get("url", "")
                assert "ev" not in url_arg or "pred" in url_arg

    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_not_sent_when_same_url_as_prediction(self, mock_post):
        """ev_alert が独立設定されていない場合は2重送信しない。"""
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        # ev_alert not set → _channels.get("ev_alert") returns None → no extra call
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD + 1.0
            bet.recommended_bet = 1000
            bets = MagicMock()
            bets.bets = [bet]
            count_before = mock_post.call_count
            router.notify_prerace_result("2026051905010911", bets, bets)
            # ev_alert channel not in _channels, so no extra send
            assert "ev_alert" not in router._channels


# ── 全 URL 未設定でも例外が発生しない ────────────────────────────────────────

class TestAllChannelsUnset:
    def test_no_exception_on_all_methods(self):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            router.send_text("テスト")
            router.send_system_text("システムテスト")
            router.send_system_embed("タイトル", "説明")
            router.notify_skip("2026051905010911", "テスト理由")
            router.send_ab_report("## A/B レポート")
            result = router.send_note_draft("テスト", "コンテンツ")
            assert result is False


# ── _format_buying_guide ──────────────────────────────────────────────────────

def test_format_buying_guide_basic() -> None:
    """honmei/manji/alpha の buy_candidates から買い方テンプレートを生成する。"""
    from src.notification.router import _format_buying_guide

    class _Bet:
        def __init__(self, bet_type, numbers, names, ev):
            self.bet_type = bet_type
            self.numbers = numbers
            self.names = names
            self.expected_value = ev

    predictions = {
        "honmei": [_Bet("win", [5], ["アーバンシック"], 1.8),
                   _Bet("place", [5], ["アーバンシック"], 1.6)],
        "manji":  [_Bet("quinella", [[5, 3]], ["アーバンシック", "レガシー"], 2.1)],
        "alpha":  [_Bet("trio", [[5, 3, 7]], ["アーバン", "レガシー", "サクセス"], 3.0)],
    }
    text = _format_buying_guide(predictions)
    assert text is not None
    assert "単勝" in text
    assert "複勝" in text
    assert "馬連" in text
    assert "三連複" in text
    assert "アーバンシック" in text


def test_format_buying_guide_empty_returns_none() -> None:
    """全予想が空の場合 None を返す。"""
    from src.notification.router import _format_buying_guide
    assert _format_buying_guide({}) is None
