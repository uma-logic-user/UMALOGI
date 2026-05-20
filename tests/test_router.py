"""tests/test_router.py — NotificationRouter 単体テスト"""
from __future__ import annotations

import pytest
from unittest.mock import patch


def test_fallback_to_prediction_when_ev_alert_unset(monkeypatch):
    """ev_alert 未設定時に prediction チャンネルへフォールバックする。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/prediction")
    monkeypatch.delenv("DISCORD_WEBHOOK_EV_ALERT", raising=False)
    # モジュールキャッシュをクリアして環境変数を反映させる
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    notifier = router._get("ev_alert")
    pred_notifier = router._get("prediction")
    assert notifier is pred_notifier


def test_all_channels_unset_no_exception(monkeypatch):
    """全 URL 未設定でも例外が発生しない（ログのみで安全スキップ）。"""
    for key in [
        "DISCORD_WEBHOOK_URL",
        "DISCORD_WEBHOOK_SYSTEM",
        "DISCORD_WEBHOOK_EV_ALERT",
        "DISCORD_WEBHOOK_AB_TEST",
        "DISCORD_WEBHOOK_NOTE_DRAFT",
        "DISCORD_SYSTEM_WEBHOOK_URL",
    ]:
        monkeypatch.delenv(key, raising=False)
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    # 例外を投げずに安全に完了すること
    router.send_text("テスト")
    router.send_system_text("テスト")


def test_ev_alert_routes_separately(monkeypatch):
    """max_ev >= 1.5 かつ ev_alert 設定済みで ev_alert チャンネルへ別送する。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/pred")
    monkeypatch.setenv("DISCORD_WEBHOOK_EV_ALERT", "https://discord.test/ev")
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    assert router._notifiers.get("ev_alert") is not None
    assert router._notifiers["ev_alert"] is not router._notifiers["prediction"]


def test_send_note_draft_chunking(monkeypatch):
    """3000文字の本文が複数チャンクに分割されページング付きで送信される。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "https://discord.test/note")
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    sent: list[str] = []
    with patch.object(router._notifiers["note_draft"], "send_text", side_effect=sent.append):
        body = "テスト行テスト行テスト行テスト行テスト行\n" * 200  # 約4200文字 (21文字×200行)
        router.send_note_draft("テストタイトル", body)
    assert len(sent) >= 2
    assert "1/" in sent[0]


def test_send_note_draft_x_post(monkeypatch):
    """x_post が指定されたとき末尾メッセージとして送信される。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "https://discord.test/note")
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    sent: list[str] = []
    with patch.object(router._notifiers["note_draft"], "send_text", side_effect=sent.append):
        router.send_note_draft("タイトル", "本文テスト", x_post="X告知テキスト")
    assert any("X告知ポスト" in m for m in sent)


def test_send_note_draft_no_channel_returns_false(monkeypatch):
    """note_draft チャンネル未設定時は False を返し例外なし。"""
    monkeypatch.delenv("DISCORD_WEBHOOK_NOTE_DRAFT", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    import importlib, sys
    for mod_name in list(sys.modules.keys()):
        if "notification.router" in mod_name:
            del sys.modules[mod_name]
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    result = router.send_note_draft("タイトル", "本文")
    assert result is False
