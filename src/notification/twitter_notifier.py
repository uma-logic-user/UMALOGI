"""
X (Twitter) ノーティファイア

環境変数:
  X_API_KEY             : API Key (Consumer Key)
  X_API_SECRET          : API Secret (Consumer Secret)
  X_ACCESS_TOKEN        : Access Token
  X_ACCESS_TOKEN_SECRET : Access Token Secret

tweepy v4 を使用。画像は media_upload → media_id で添付。

レート制限:
  X Free Plan: 1,500 ツイート/月。RateLimiter で月次カウントを管理する。
  カウントは data/x_rate_count.json に永続化。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_RATE_FILE = _ROOT / "data" / "x_rate_count.json"
_MONTHLY_LIMIT = 1500


class _RateLimiter:
    """月次ツイート数を管理し、Free Plan の 1,500 件/月 制限を超えないようにする。

    カウントは data/x_rate_count.json に永続化する。
    キーは 'YYYY-MM' 形式で月ごとに独立管理する。
    """

    def __init__(self) -> None:
        """初期化。永続化ファイルからカウントを読み込む。"""
        self._data: dict[str, int] = self._load()

    def _load(self) -> dict[str, int]:
        """永続化ファイルから月次カウントを読み込む。

        Returns:
            月次カウントの辞書。ファイルが存在しない場合は空の辞書。
        """
        try:
            if _RATE_FILE.exists():
                with open(_RATE_FILE, encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save(self) -> None:
        """月次カウントを永続化ファイルに保存する。"""
        try:
            _RATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(_RATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._data, f)
        except Exception as e:
            logger.warning("[Twitter] レートカウント保存失敗: %s", e)

    def _key(self) -> str:
        """現在月のキー文字列（'YYYY-MM' 形式）を返す。

        Returns:
            'YYYY-MM' 形式の文字列。
        """
        d = date.today()
        return f"{d.year}-{d.month:02d}"

    def remaining(self) -> int:
        """今月の残り投稿可能件数を返す。

        Returns:
            残り投稿可能件数（0 以上）。
        """
        return _MONTHLY_LIMIT - self._data.get(self._key(), 0)

    def can_post(self) -> bool:
        """今月まだ投稿できるかを返す。

        Returns:
            True = 月次制限内、False = 制限超過。
        """
        return self.remaining() > 0

    def increment(self) -> None:
        """今月のカウントを 1 増やして永続化する。"""
        key = self._key()
        self._data[key] = self._data.get(key, 0) + 1
        self._save()

    def current_count(self) -> int:
        """今月の投稿済み件数を返す。

        Returns:
            今月の投稿済み件数。
        """
        return self._data.get(self._key(), 0)


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
    """X (旧 Twitter) API v2 を通じて投稿する。

    tweepy v4 を使用。画像は v1.1 API の media_upload → media_id で添付する。
    月次ツイート上限（1,500 件）は _RateLimiter で管理する。
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        access_token: str | None = None,
        access_token_secret: str | None = None,
        enabled: bool = True,
    ) -> None:
        """初期化。

        Args:
            api_key: API Key (Consumer Key)。未指定時は X_API_KEY 環境変数を使用。
            api_secret: API Secret (Consumer Secret)。未指定時は X_API_SECRET 環境変数を使用。
            access_token: Access Token。未指定時は X_ACCESS_TOKEN 環境変数を使用。
            access_token_secret: Access Token Secret。
                                 未指定時は X_ACCESS_TOKEN_SECRET 環境変数を使用。
            enabled: False に設定すると全送信をスキップする。
        """
        super().__init__(enabled=enabled)
        self._api_key = api_key or os.environ.get("X_API_KEY", "")
        self._api_secret = api_secret or os.environ.get("X_API_SECRET", "")
        self._access_token = access_token or os.environ.get("X_ACCESS_TOKEN", "")
        self._access_token_secret = access_token_secret or os.environ.get(
            "X_ACCESS_TOKEN_SECRET", ""
        )

        self._client: "tweepy.Client | None" = None
        self._api_v1: "tweepy.API | None" = None  # 画像アップロード用
        self._rate_limiter = _RateLimiter()

        if enabled:
            self._init_clients()

    def _init_clients(self) -> None:
        """tweepy v2 クライアントと v1.1 API を初期化する。

        認証情報が不完全、または tweepy 未インストールの場合はスキップする。
        """
        if not _TWEEPY_AVAILABLE:
            return
        if not all(
            [
                self._api_key,
                self._api_secret,
                self._access_token,
                self._access_token_secret,
            ]
        ):
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
                self._api_key,
                self._api_secret,
                self._access_token,
                self._access_token_secret,
            )
            self._api_v1 = tweepy.API(auth)
            logger.info("[Twitter] クライアント初期化完了")
        except Exception as e:
            logger.error("[Twitter] 初期化失敗: %s", e)

    def _send(self, message: NotifyMessage) -> bool:
        """BaseNotifier の抽象メソッド実装。X にツイートを投稿する。

        280文字を超える場合は末尾を切り詰める。月次制限に達している場合はスキップする。

        Args:
            message: 送信するメッセージ。image_path が指定された場合は画像も添付。

        Returns:
            True = 投稿成功, False = 失敗またはスキップ。
        """
        if not _TWEEPY_AVAILABLE or self._client is None:
            return False

        if not self._rate_limiter.can_post():
            logger.warning(
                "[Twitter] 月次制限到達 (%d/%d)。投稿をスキップします。",
                self._rate_limiter.current_count(),
                _MONTHLY_LIMIT,
            )
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
            self._rate_limiter.increment()
            logger.info(
                "[Twitter] 投稿成功: tweet_id=%s  月次残り %d/%d",
                resp.data.get("id"),
                self._rate_limiter.remaining(),
                _MONTHLY_LIMIT,
            )
            return True
        except Exception as e:
            logger.error("[Twitter] 投稿失敗: %s", e)
            return False

    def post_text(self, text: str, image_path: str | None = None) -> bool:
        """プレーンテキストを X に投稿する（weekend_batch から直接呼ぶ用）。"""
        from .base import NotifyMessage

        NotifyMessage(
            title="", body=text, image_path=Path(image_path) if image_path else None
        )
        # title+body で重複改行が入るため text を直接使う
        if not _TWEEPY_AVAILABLE or self._client is None:
            return False
        if not self._rate_limiter.can_post():
            logger.warning("[Twitter] 月次制限到達。投稿スキップ。")
            return False
        if len(text) > _MAX_CHARS:
            text = text[: _MAX_CHARS - 3] + "..."

        media_ids: list[int] | None = None
        if image_path and Path(image_path).exists() and self._api_v1:
            try:
                media = self._api_v1.media_upload(image_path)
                media_ids = [media.media_id]
            except Exception as e:
                logger.warning("[Twitter] 画像アップロード失敗: %s", e)

        try:
            resp = self._client.create_tweet(text=text, media_ids=media_ids)
            self._rate_limiter.increment()
            logger.info("[Twitter] post_text 成功: tweet_id=%s", resp.data.get("id"))
            return True
        except Exception as e:
            logger.error("[Twitter] post_text 失敗: %s", e)
            return False

    @property
    def rate_remaining(self) -> int:
        """今月の残り投稿可能件数を返す。"""
        return self._rate_limiter.remaining()
