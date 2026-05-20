"""NotificationRouter + post_weekly_note_draft の単体テスト。"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ── ヘルパー ──────────────────────────────────────────────────────────────────

def _mock_post(sent: list) -> object:
    """requests.post のモックを返す。呼び出しごとに sent に URL を追加する。"""
    def _post(url, **kwargs):
        sent.append({"url": url, "json": kwargs.get("json", {})})
        m = MagicMock()
        m.status_code = 204
        return m
    return _post


# ── NotificationRouter テスト ─────────────────────────────────────────────────

class TestNotificationRouter:
    def test_fallback_to_prediction(self, monkeypatch):
        """DISCORD_WEBHOOK_SYSTEM 未設定時にシステム通知が予想チャンネルへフォールバックする。"""
        # discord_notifier.py は load_dotenv(override=False) をモジュールレベルで呼ぶため
        # delenv ではなく setenv("") で空文字にする（override=False が再設定しなくなる）
        monkeypatch.setenv("DISCORD_WEBHOOK_SYSTEM", "")
        monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")
        monkeypatch.setenv("DISCORD_WEBHOOK_EV_ALERT", "")
        monkeypatch.setenv("DISCORD_WEBHOOK_AB_TEST", "")
        monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "")
        # DISCORD_WEBHOOK_URL は設定済み（フォールバック先として使用される）

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.send_system_text("system msg")

        # system 未設定 → prediction へ1回フォールバック送信されること（URL値は問わない）
        assert len(sent) == 1, \
            f"prediction フォールバックで1回の送信を期待: sent={[e['url'] for e in sent]}"
        # prediction URL と一致すること
        prediction_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
        if prediction_url:
            assert sent[0]["url"] == prediction_url, \
                f"prediction URL 以外が使われた: actual={sent[0]['url']}"

    def test_ev_alert_routes_separately(self, monkeypatch):
        """max_ev >= 1.5 かつ ev_alert URL 設定済みで ev_alert チャンネルへ別送される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://fake-prediction")
        monkeypatch.setenv("DISCORD_WEBHOOK_EV_ALERT", "http://fake-ev-alert")
        monkeypatch.setenv("DISCORD_WEBHOOK_SYSTEM", "")
        monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.notify_ev_alert("2026050105050101", 2.0, "EV=2.00 テスト")

        urls = [e["url"] for e in sent]
        assert "http://fake-ev-alert" in urls, "ev_alert URL への送信がない"
        assert "http://fake-prediction" not in urls, \
            "prediction チャンネルへ二重送信されている（ev_alert は prediction へ fallback しない）"

    def test_send_note_draft_chunking(self, monkeypatch):
        """3000文字の本文が複数チャンクに分割され、ページング番号付きで送信される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "http://fake-draft")
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
        monkeypatch.setenv("DISCORD_WEBHOOK_SYSTEM", "")
        monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            result = router.send_note_draft("テストタイトル", "あ" * 3000)

        assert result is True, "send_note_draft が False を返した"
        assert len(sent) >= 2, f"チャンク数が1以下: {len(sent)}"
        first_content = sent[0]["json"]["content"]
        assert "(1/" in first_content, f"ページング番号がない: {first_content[:80]}"
        # 終端マーカーは最後のチャンクか、x_post の直前チャンクにある
        all_contents = [e["json"].get("content", "") for e in sent]
        assert any("_（以上）_" in c for c in all_contents), "終端マーカーがない"

    def test_send_note_draft_x_post(self, monkeypatch):
        """x_post が指定されたとき末尾メッセージとして送信される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "http://fake-draft")
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
        monkeypatch.setenv("DISCORD_WEBHOOK_SYSTEM", "")
        monkeypatch.setenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.send_note_draft("タイトル", "短い本文", x_post="X告知テキストXX")

        assert any("X告知テキストXX" in e["json"].get("content", "") for e in sent), \
            "X 告知テキストが送信されていない"

    def test_all_channels_unset(self, monkeypatch):
        """全チャンネル URL 未設定でも例外が発生しない。"""
        for key in [
            "DISCORD_WEBHOOK_URL", "DISCORD_WEBHOOK_SYSTEM",
            "DISCORD_WEBHOOK_EV_ALERT", "DISCORD_WEBHOOK_AB_TEST",
            "DISCORD_WEBHOOK_NOTE_DRAFT", "DISCORD_SYSTEM_WEBHOOK_URL",
        ]:
            monkeypatch.setenv(key, "")

        from src.notification.router import NotificationRouter
        router = NotificationRouter()
        router.send_text("t")
        router.send_system_text("t")
        router.notify_ev_alert("2026050105050101", 2.0, "e")
        router.send_ab_report("r")
        result = router.send_note_draft("title", "body")
        assert result is False


# ── post_weekly_note_draft のトグルテスト ─────────────────────────────────────

class TestPlaywrightToggle:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_PLAYWRIGHT_POST", raising=False)
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_off_with_zero(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "0")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_off_with_false_str(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "False")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_on_with_one(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "1")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is True

    def test_on_with_true_str(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "True")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is True
