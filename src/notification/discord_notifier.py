"""
Discord Webhook ノーティファイア

環境変数:
  DISCORD_WEBHOOK_URL  : Discord Incoming Webhook URL

embed を使った見やすいフォーマットで送信する。
画像がある場合は multipart/form-data でアップロード。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests

from .base import BaseNotifier, NotifyMessage

logger = logging.getLogger(__name__)

# embed カラー（的中レベルに応じて変える）
_COLOR_NORMAL  = 0x00FF88   # 緑
_COLOR_BIG     = 0xFFD700   # 金
_COLOR_JACKPOT = 0xFF4500   # 赤金


class DiscordNotifier(BaseNotifier):
    """Discord Webhook を通じて通知を送る。"""

    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")
        if enabled and not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません")

    def _send(self, message: NotifyMessage) -> bool:
        if not self._url:
            return False

        color = _COLOR_JACKPOT if "万馬券" in message.title or "爆裂" in message.title \
                else _COLOR_BIG if "高配当" in message.title \
                else _COLOR_NORMAL

        embed = {
            "title": message.title,
            "description": message.body,
            "color": color,
        }
        if message.url:
            embed["url"] = message.url

        payload = {"embeds": [embed]}

        if message.image_path and Path(message.image_path).exists():
            # 画像付き: multipart/form-data
            with open(message.image_path, "rb") as fp:
                resp = requests.post(
                    self._url,
                    data={"payload_json": json.dumps(payload)},
                    files={"file": (Path(message.image_path).name, fp, "image/png")},
                    timeout=10,
                )
        else:
            resp = requests.post(
                self._url,
                json=payload,
                timeout=10,
            )

        if resp.status_code in (200, 204):
            logger.info("[Discord] 送信成功: %s", message.title)
            return True
        else:
            logger.warning("[Discord] 送信失敗 status=%d: %s", resp.status_code, resp.text[:200])
            return False
