"""
LINE Notify ノーティファイア

環境変数:
  LINE_NOTIFY_TOKEN : LINE Notify のアクセストークン
                      https://notify-bot.line.me/ja/ で発行

画像は PNG/JPG ファイルパスを渡すと multipart で送信する。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import requests

from .base import BaseNotifier, NotifyMessage

logger = logging.getLogger(__name__)

_LINE_NOTIFY_URL = "https://notify-api.line.me/api/notify"


class LineNotifier(BaseNotifier):
    """LINE Notify を通じて通知を送る。"""

    def __init__(
        self,
        *,
        token: str | None = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._token = token or os.environ.get("LINE_NOTIFY_TOKEN", "")
        if enabled and not self._token:
            logger.warning("LINE_NOTIFY_TOKEN が設定されていません")

    def _send(self, message: NotifyMessage) -> bool:
        if not self._token:
            return False

        text = f"\n{message.title}\n\n{message.body}"
        if message.url:
            text += f"\n\n{message.url}"

        headers = {"Authorization": f"Bearer {self._token}"}

        if message.image_path and Path(message.image_path).exists():
            with open(message.image_path, "rb") as fp:
                resp = requests.post(
                    _LINE_NOTIFY_URL,
                    headers=headers,
                    data={"message": text},
                    files={"imageFile": fp},
                    timeout=10,
                )
        else:
            resp = requests.post(
                _LINE_NOTIFY_URL,
                headers=headers,
                data={"message": text},
                timeout=10,
            )

        if resp.status_code == 200:
            logger.info("[LINE] 送信成功: %s", message.title)
            return True
        else:
            logger.warning("[LINE] 送信失敗 status=%d: %s", resp.status_code, resp.text[:200])
            return False
