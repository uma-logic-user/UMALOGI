"""軽量・非同期 Discord アラート送信ユーティリティ。

スクレイパー内部から文字化け検出時にシステム通知を発火するための
ファイアー＆フォーゲット実装。メインスレッドをブロックしない。
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

# 優先順位: DISCORD_SYSTEM_WEBHOOK_URL → DISCORD_WEBHOOK_SNS → 未設定
_SYSTEM_WEBHOOK_ENV_KEYS = (
    "DISCORD_SYSTEM_WEBHOOK_URL",
    "DISCORD_WEBHOOK_SNS",
)


def _get_system_webhook() -> str:
    for key in _SYSTEM_WEBHOOK_ENV_KEYS:
        url = os.environ.get(key, "")
        if url:
            return url
    return ""


def _post(url: str, payload: dict[str, Any]) -> None:
    try:
        import requests

        requests.post(url, json=payload, timeout=5)
    except Exception as exc:
        logger.debug("[discord_alert] 送信失敗（無視）: %s", exc)


def send_mojibake_alert(source: str, field: str, value: str) -> None:
    """文字化け検出時にシステム Discord チャンネルへ非同期アラートを送信する。

    環境変数 ``DISCORD_SYSTEM_WEBHOOK_URL``（優先）または ``DISCORD_WEBHOOK_SNS``
    が設定されていない場合はサイレントスキップする。

    Args:
        source: データソース識別子（例: "JVLink", "netkeiba"）。
        field:  文字化けが検出されたフィールド名（例: "horse_name"）。
        value:  検出された文字化け文字列（先頭30文字）。
    """
    url = _get_system_webhook()
    if not url:
        return

    truncated = value[:30] + ("..." if len(value) > 30 else "")
    payload = {
        "content": (
            f"⚠️ **文字化け検知アラート**\n"
            f"ソース: `{source}`  フィールド: `{field}`\n"
            f"検知値: `{truncated}`\n"
            f"→ 安全のため空文字で保護しました"
        )
    }
    thread = threading.Thread(target=_post, args=(url, payload), daemon=True)
    thread.start()
