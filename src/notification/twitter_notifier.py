"""
X (Twitter) ノーティファイア

環境変数:
  X_API_KEY             : API Key (Consumer Key)
  X_API_SECRET          : API Secret (Consumer Secret)
  X_ACCESS_TOKEN        : Access Token
  X_ACCESS_TOKEN_SECRET : Access Token Secret

tweepy v4 を使用。画像は media_upload → media_id で添付。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import tweepy  # type: ignore[import-untyped]
    _TWEEPY_AVAILABLE = True
except ImportError:
    _TWEEPY_AVAILABLE = False
    logger.warning("tweepy がインストールされていません: pip install tweepy")

from .base import BaseNotifier, NotifyMessage

# X の最大文字数（半角換算）
_MAX_CHARS = 280


class TwitterNotifier(BaseNotifier):
    """X (旧 Twitter) API v2 を通じて投稿する。"""

    def __init__(
        self,
        *,
        api_key:              str | None = None,
        api_secret:           str | None = None,
        access_token:         str | None = None,
        access_token_secret:  str | None = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._api_key             = api_key             or os.environ.get("X_API_KEY", "")
        self._api_secret          = api_secret          or os.environ.get("X_API_SECRET", "")
        self._access_token        = access_token        or os.environ.get("X_ACCESS_TOKEN", "")
        self._access_token_secret = access_token_secret or os.environ.get("X_ACCESS_TOKEN_SECRET", "")

        self._client:  "tweepy.Client | None"  = None
        self._api_v1:  "tweepy.API | None"     = None  # 画像アップロード用

        if enabled:
            self._init_clients()

    def _init_clients(self) -> None:
        if not _TWEEPY_AVAILABLE:
            return
        if not all([self._api_key, self._api_secret,
                    self._access_token, self._access_token_secret]):
            logger.warning("X API 認証情報が不完全です")
            return
        try:
            # v2 クライアント（ツイート投稿）
            self._client = tweepy.Client(
                consumer_key=self._api_key,
                consumer_secret=self._api_secret,
                access_token=self._access_token,
                access_token_secret=self._access_token_secret,
            )
            # v1.1 API（メディアアップロード — v2 未対応のため）
            auth = tweepy.OAuth1UserHandler(
                self._api_key, self._api_secret,
                self._access_token, self._access_token_secret,
            )
            self._api_v1 = tweepy.API(auth)
            logger.info("[Twitter] クライアント初期化完了")
        except Exception as e:
            logger.error("[Twitter] 初期化失敗: %s", e)

    def _send(self, message: NotifyMessage) -> bool:
        if not _TWEEPY_AVAILABLE or self._client is None:
            return False

        # ツイート本文を組み立て（280文字以内に収める）
        text = f"{message.title}\n\n{message.body}"
        if message.url:
            text += f"\n{message.url}"
        if len(text) > _MAX_CHARS:
            text = text[: _MAX_CHARS - 3] + "..."

        media_ids: list[int] | None = None

        # 画像アップロード
        if message.image_path and Path(message.image_path).exists() and self._api_v1:
            try:
                media = self._api_v1.media_upload(str(message.image_path))
                media_ids = [media.media_id]
            except Exception as e:
                logger.warning("[Twitter] 画像アップロード失敗: %s", e)

        try:
            resp = self._client.create_tweet(
                text=text,
                media_ids=media_ids,
            )
            logger.info("[Twitter] 投稿成功: tweet_id=%s", resp.data.get("id"))
            return True
        except Exception as e:
            logger.error("[Twitter] 投稿失敗: %s", e)
            return False
