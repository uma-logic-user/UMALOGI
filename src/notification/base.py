"""
通知基底クラス

各 SNS ノーティファイアが実装する抽象インターフェイス。
"""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class NotifyMessage:
    """送信するメッセージの共通DTO。"""
    title:    str
    body:     str
    image_path: Path | None = None   # 証拠画像（任意）
    url:      str | None = None      # レース結果 URL（任意）


class BaseNotifier(abc.ABC):
    """SNS通知の抽象基底クラス。"""

    def __init__(self, *, enabled: bool = True) -> None:
        self._enabled = enabled

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def send(self, message: NotifyMessage) -> bool:
        """
        メッセージを送信する。

        Returns:
            True = 送信成功, False = スキップ / 失敗
        """
        if not self._enabled:
            logger.debug("[%s] disabled — skip", self.name)
            return False
        try:
            return self._send(message)
        except Exception as e:
            logger.error("[%s] 送信失敗: %s", self.name, e)
            return False

    @abc.abstractmethod
    def _send(self, message: NotifyMessage) -> bool:
        """サブクラスが実装する実際の送信処理。"""
        ...
