"""netkeiba スクレイピング共通 HTTP クライアント（堅牢化レイヤー）。

本番ログ（2026-05-31）で netkeiba が 503 / 429 / 403 を多発していたため、
以下の冗長化・ブロック回避ロジックを一元化する:

  1. User-Agent ローテーション + ブラウザ完全ヘッダ（bot 判定回避）
  2. プロセス全体のグローバルレート制限（並列スレッドの自己 DoS 防止）
  3. Retry-After ヘッダの尊重（429 / 503 のサーバー指示に従う）
  4. ステータス別バックオフ（レート制限は長く、4xx 恒久エラーは即中断）

netkeiba.py / entry_table.py の両 HTTP 層が本モジュールのヘルパーを共有する。
各モジュールは `requests.get` 呼び出し自体は自前で保持する（既存テストの
`@patch("requests.get")` 互換のため）。本モジュールはヘッダ生成・待機・
バックオフ計算・リトライ可否判定の「ロジック」のみを提供する。
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ── User-Agent プール（実在する最近のブラウザ） ───────────────────────
# 1 リクエストごとにランダムに選ぶことで単一 UA のフィンガープリントを避ける。
USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
)

# 一時的ブロック・サーバー過負荷を示すステータス（リトライ対象）
# 403 は bot 判定の可能性があり、UA ローテーション + バックオフで回復しうるため含める。
# 404（恒久 Not Found）は含めない（即中断してフォールバックへ）。
_RETRYABLE_STATUS: frozenset[int] = frozenset({403, 429, 500, 502, 503, 504})
# レート制限を示すステータス（バックオフを長めに取る）
_RATE_LIMIT_STATUS: frozenset[int] = frozenset({429, 503})

# Retry-After を尊重する際の上限（秒）。サーバーが極端に長い値を返しても待ちすぎない。
_MAX_RETRY_AFTER = 120.0


def build_headers(referer: str = "https://www.netkeiba.com/") -> dict[str, str]:
    """ブラウザを模した完全な HTTP ヘッダを生成する（UA はランダム選択）。

    Args:
        referer: Referer ヘッダに設定する URL。

    Returns:
        requests.get に渡すヘッダ辞書。
    """
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }


class RateLimiter:
    """プロセス全体で共有するスレッドセーフなレート制限器。

    複数スレッド（auto_runner の ThreadPoolExecutor 等）が同一ホストへ
    同時アクセスして 429/503 を誘発する「自己 DoS」を防ぐため、
    直近リクエストから最低 `min_interval` 秒の間隔を強制する。
    実待機にはランダムジッタを加え、機械的な等間隔アクセスを避ける。
    """

    def __init__(self, min_interval: float, jitter: float = 0.4) -> None:
        self._min_interval = max(0.0, min_interval)
        self._jitter = max(0.0, jitter)
        self._lock = threading.Lock()
        self._last_ts = 0.0

    def wait(self) -> None:
        """前回リクエストから min_interval 秒経過するまで（＋ジッタ）待機する。"""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_ts
            base_wait = self._min_interval - elapsed
            jitter = random.uniform(0.0, self._jitter) if self._jitter else 0.0
            wait = max(0.0, base_wait) + jitter
            # 次リクエストの基準時刻を「待機後」に進めてからロックを解放する
            self._last_ts = now + wait
        if wait > 0:
            time.sleep(wait)


def _env_float(name: str, default: float) -> float:
    """環境変数を float として読む（不正値は default）。"""
    try:
        return float(os.environ.get(name, "").strip() or default)
    except (ValueError, AttributeError):
        return default


# netkeiba 用グローバルレート制限器。
# NETKEIBA_MIN_INTERVAL で間隔（秒）を調整可能（デフォルト 1.2 秒）。
NETKEIBA_LIMITER = RateLimiter(
    min_interval=_env_float("NETKEIBA_MIN_INTERVAL", 1.2),
    jitter=_env_float("NETKEIBA_JITTER", 0.5),
)


def retry_after_seconds(resp: Optional[requests.Response]) -> Optional[float]:
    """Retry-After ヘッダを秒数に変換する（秒数形式 / HTTP-date 形式の両対応）。

    Args:
        resp: HTTP レスポンス（None 可）。

    Returns:
        待機すべき秒数。ヘッダがない・解析不能・負値なら None。
        極端に長い値は _MAX_RETRY_AFTER で上限を設ける。
    """
    if resp is None:
        return None
    raw = resp.headers.get("Retry-After") if getattr(resp, "headers", None) else None
    if not raw:
        return None
    raw = str(raw).strip()
    # 形式 1: 秒数（整数）
    try:
        secs = float(raw)
        return min(max(secs, 0.0), _MAX_RETRY_AFTER) if secs >= 0 else None
    except ValueError:
        pass
    # 形式 2: HTTP-date
    try:
        dt = parsedate_to_datetime(raw)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return min(max(delta, 0.0), _MAX_RETRY_AFTER)
    except (TypeError, ValueError):
        return None


def is_retryable_status(status: Optional[int]) -> bool:
    """このステータスコードでリトライすべきか（恒久 4xx は False）。"""
    return status in _RETRYABLE_STATUS


def backoff_seconds(attempt: int, base: float, status: Optional[int] = None) -> float:
    """指数バックオフ + ジッタの待機秒数を計算する。

    レート制限系（429/503）はサーバー保護のため待機を長めに取る。

    Args:
        attempt: 試行回数（1 始まり）。
        base:    基準待機秒数。
        status:  直前レスポンスのステータスコード（429/503 で延長）。

    Returns:
        待機秒数（ジッタ込み）。
    """
    factor = 2 ** (attempt - 1)
    wait = base * factor
    if status in _RATE_LIMIT_STATUS:
        # レート制限時は最低 5 秒は空ける（base が小さいテスト時を除き実効）
        wait = max(wait, 5.0 * factor)
    jitter = random.uniform(0.0, max(0.5, wait * 0.25))
    return wait + jitter
