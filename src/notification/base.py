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
    """送信するメッセージの共通DTO。

    Attributes:
        title: メッセージのタイトル。
        body: メッセージ本文。
        image_path: 証拠画像のパス（任意）。
        url: レース結果 URL（任意）。
    """

    title: str
    body: str
    image_path: Path | None = None  # 証拠画像（任意）
    url: str | None = None  # レース結果 URL（任意）


class BaseNotifier(abc.ABC):
    """SNS通知の抽象基底クラス。

    各 SNS ノーティファイアが継承して `_send` を実装する。
    `send` は enabled フラグのチェックと例外ラッパーを提供する。
    """

    def __init__(self, *, enabled: bool = True) -> None:
        """初期化。

        Args:
            enabled: False に設定すると送信処理を全スキップする。
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """ノーティファイアの名前（クラス名）を返す。"""
        return self.__class__.__name__

    def send(self, message: NotifyMessage) -> bool:
        """メッセージを送信する。

        enabled=False のときはスキップし、送信中に例外が発生した場合は
        ログに記録して False を返す。

        Args:
            message: 送信するメッセージ。

        Returns:
            True = 送信成功, False = スキップ / 失敗。
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
        """サブクラスが実装する実際の送信処理。

        Args:
            message: 送信するメッセージ。

        Returns:
            True = 送信成功, False = 失敗。
        """
        ...
