"""
X（旧Twitter）競馬予想スクレイパー  Phase A

凄腕競馬予想家のポストを Playwright でスクレイピングし、
x_signals テーブルに raw_text として格納する。

設計原則:
  - stealth-mode Playwright でログイン不要の公開タイムラインを取得
  - Rate limit: 1アカウントあたり 1時間あたり 15 リクエスト以下
  - 取得対象: レース開催日の前夜 18:00 〜 当日 10:00 の投稿
  - 個人情報保護: 収集データは非公開 DB のみに格納・外部公開禁止
  - X 利用規約上のリスク: 非営利・研究目的・低頻度アクセス

【依存パッケージ】
    pip install playwright playwright-stealth
    playwright install chromium

Usage:
    py src/scraper/x_scraper.py
    py src/scraper/x_scraper.py --date 2026-05-17
    py src/scraper/x_scraper.py --account some_user --dry-run
    py src/scraper/x_scraper.py --check-only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)

_TARGETS_JSON = _ROOT / "scripts" / "x_targets.json"
_RATE_LIMIT_PER_HOUR = 15      # X 利用規約上の安全マージン
_REQUEST_INTERVAL_SEC = 3600 / _RATE_LIMIT_PER_HOUR  # 240秒 = 4分/リクエスト
_JITTER_RATIO = 0.30           # ±30% のランダムジッター

# ── ステルス用 User-Agent プール ──────────────────────────────────────
_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── ステルス用 Viewport プール (width × height) ───────────────────────
_VIEWPORTS: list[dict[str, int]] = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1280, "height": 800},
    {"width": 1536, "height": 864},
]

# ── 馬番・印・レース名の正規表現パターン ──────────────────────────────
_RE_HORSE_NUM  = re.compile(r"(?:^|[^\d])([1-9][0-9]?)(?:番|枠|頭)(?:[目号])?")
_RE_SIGNAL     = re.compile(r"[◎○▲△×☆]")
_RE_RACE_NAME  = re.compile(
    r"(?:東京|中山|阪神|京都|中京|小倉|新潟|福島|函館|札幌)"
    r"[\d０-９]{1,2}[RrＲ]"
    r"|[^\s]{2,8}(?:ステークス|特別|賞|カップ|記念|マイルC|G[123]|GI)"
)
_RE_HONMEI     = re.compile(r"[◎本命]")
_RE_ANA        = re.compile(r"[▲△穴]")
_RE_KESHI      = re.compile(r"[×✕消]")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# データクラス
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RawTweet:
    """スクレイピング結果の生ポスト。"""

    __slots__ = ("tweet_id", "screen_name", "raw_text", "posted_at", "fetched_at")

    def __init__(
        self,
        tweet_id:    str,
        screen_name: str,
        raw_text:    str,
        posted_at:   str,
    ) -> None:
        self.tweet_id    = tweet_id
        self.screen_name = screen_name
        self.raw_text    = raw_text
        self.posted_at   = posted_at
        self.fetched_at  = datetime.now().isoformat(timespec="seconds")

    def is_racing_related(self) -> bool:
        """競馬関連ポストかどうかを簡易判定する。"""
        text = self.raw_text
        has_signal  = bool(_RE_SIGNAL.search(text))
        has_race    = bool(_RE_RACE_NAME.search(text))
        has_horse_n = bool(_RE_HORSE_NUM.search(text))
        return has_signal or has_race or has_horse_n

    def extract_signal_type(self) -> str | None:
        """◎/○/▲/△/× から signal_type を推定する。"""
        text = self.raw_text
        if _RE_HONMEI.search(text):
            return "honmei"
        if _RE_ANA.search(text):
            return "ana"
        if _RE_KESHI.search(text):
            return "keshi"
        return None

    def extract_race_name(self) -> str | None:
        """テキストからレース名を抽出する（突合は x_signal_parser で行う）。"""
        m = _RE_RACE_NAME.search(self.raw_text)
        return m.group(0) if m else None

    def __repr__(self) -> str:
        return (
            f"RawTweet(id={self.tweet_id!r}, "
            f"user={self.screen_name!r}, "
            f"at={self.posted_at!r}, "
            f"text={self.raw_text[:40]!r})"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# レートリミッター
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RateLimiter:
    """1アカウントあたり毎時 max_per_hour リクエスト以下に制限する。

    jitter_ratio を設定すると待機時間に ±jitter_ratio のランダムなブレを加える。
    ±30% デフォルト — Bot 検知のパターンマッチを回避する。
    """

    def __init__(
        self,
        max_per_hour: int = _RATE_LIMIT_PER_HOUR,
        jitter_ratio: float = _JITTER_RATIO,
    ) -> None:
        self._interval    = 3600.0 / max_per_hour
        self._jitter      = jitter_ratio
        self._last: dict[str, float] = {}

    async def wait(self, key: str) -> None:
        import random
        now  = time.monotonic()
        last = self._last.get(key, 0.0)
        base_wait = self._interval - (now - last)
        if base_wait > 0:
            # ±jitter_ratio のランダムブレを加算（Bot パターン回避）
            jitter = base_wait * self._jitter * (2 * random.random() - 1)
            actual = max(base_wait + jitter, 1.0)
            logger.debug("[RateLimit] %s: %.1f秒待機 (jitter=%.1f秒)", key, actual, jitter)
            await asyncio.sleep(actual)
        self._last[key] = time.monotonic()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# スクレイパー本体
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class XScraper:
    """
    X（旧Twitter）公開タイムラインスクレイパー。

    Playwright Chromium + stealth ヘッダーで公開プロフィールを巡回する。
    ログイン不要・公開ポストのみ取得。

    Parameters
    ----------
    conn : sqlite3.Connection
        umalogi.db への接続。x_signals / x_accounts テーブルに書き込む。
    targets_path : Path
        x_targets.json のパス。
    dry_run : bool
        True の場合、DB 書き込みをスキップしてログのみ出力する。
    """

    _BASE_URL = "https://x.com"

    def __init__(
        self,
        conn: sqlite3.Connection,
        targets_path: Path = _TARGETS_JSON,
        *,
        dry_run: bool = False,
    ) -> None:
        self._conn        = conn
        self._targets     = self._load_targets(targets_path)
        self._dry_run     = dry_run
        self._rate_limiter = RateLimiter()
        self._saved       = 0
        self._skipped     = 0

    # ── ターゲット読み込み ────────────────────────────────────────────

    @staticmethod
    def _load_targets(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            logger.warning("x_targets.json が存在しません: %s", path)
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        accounts = [a for a in data.get("accounts", []) if a.get("is_active", True)]
        logger.info("監視アカウント: %d 件", len(accounts))
        return accounts

    # ── メインエントリ ─────────────────────────────────────────────────

    async def run(
        self,
        target_date: date | None = None,
        *,
        account_filter: str | None = None,
    ) -> dict[str, int]:
        """
        全ターゲットアカウントのタイムラインをスクレイピングする。

        Parameters
        ----------
        target_date : date | None
            取得対象日（レース開催日）。None の場合は当日。
        account_filter : str | None
            指定した場合はそのアカウントのみ処理する。

        Returns
        -------
        {"saved": int, "skipped": int, "errors": int}
        """
        if target_date is None:
            target_date = date.today()

        # 取得ウィンドウ: 前夜 18:00 〜 当日 10:00
        window_start = datetime(
            target_date.year, target_date.month, target_date.day, 10, 0
        ) - timedelta(hours=16)   # 前夜 18:00
        window_end = datetime(
            target_date.year, target_date.month, target_date.day, 10, 0
        )

        logger.info(
            "スクレイピング対象期間: %s 〜 %s",
            window_start.isoformat(), window_end.isoformat(),
        )

        targets = self._targets
        if account_filter:
            targets = [t for t in targets if t["screen_name"] == account_filter]
            if not targets:
                logger.warning("アカウント %r が x_targets.json に見つかりません", account_filter)
                return {"saved": 0, "skipped": 0, "errors": 0}

        errors = 0
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error(
                "playwright が未インストールです。`pip install playwright` 後に "
                "`playwright install chromium` を実行してください。"
            )
            return {"saved": 0, "skipped": 0, "errors": 1}

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx     = await self._build_context(browser)
            page    = await ctx.new_page()
            await self._apply_stealth(page)

            for account in targets:
                sn = account["screen_name"]
                try:
                    tweets = await self._scrape_timeline(
                        page, sn, window_start, window_end
                    )
                    self._save_tweets(tweets, sn)
                    logger.info(
                        "[%s] 取得: %d件 / 保存: %d件",
                        sn, len(tweets), self._saved,
                    )
                except Exception as exc:
                    logger.error("[%s] スクレイピング失敗: %s", sn, exc)
                    errors += 1

            await browser.close()

        return {
            "saved":   self._saved,
            "skipped": self._skipped,
            "errors":  errors,
        }

    # ── Playwright コンテキスト ───────────────────────────────────────

    async def _build_context(self, browser: Any) -> Any:
        """ステルスヘッダーを付与したブラウザコンテキストを生成する。

        UA・Viewport をセッションごとにランダム選択して Bot フィンガープリントを分散させる。
        """
        import random
        ua       = random.choice(_USER_AGENTS)
        viewport = random.choice(_VIEWPORTS)

        # Chrome バージョンを UA から抽出して Sec-Ch-Ua ヘッダーと同期させる
        m = re.search(r"Chrome/(\d+)", ua)
        chrome_ver = m.group(1) if m else "124"

        # プラットフォーム判定（UA から Windows/Mac/Linux を推定）
        if "Windows" in ua:
            platform = "Windows"
        elif "Macintosh" in ua or "Mac OS" in ua:
            platform = "macOS"
        else:
            platform = "Linux"

        logger.debug("[Stealth] UA=%s  Viewport=%sx%s", ua[:60], viewport["width"], viewport["height"])
        return await browser.new_context(
            user_agent=ua,
            viewport=viewport,
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            extra_http_headers={
                "Accept-Language":     "ja,en-US;q=0.9,en;q=0.8",
                "Accept":              "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding":     "gzip, deflate, br",
                "Sec-Ch-Ua":           f'"Chromium";v="{chrome_ver}", "Google Chrome";v="{chrome_ver}", "Not-A.Brand";v="99"',
                "Sec-Ch-Ua-Mobile":    "?0",
                "Sec-Ch-Ua-Platform":  f'"{platform}"',
                "Sec-Fetch-Dest":      "document",
                "Sec-Fetch-Mode":      "navigate",
                "Sec-Fetch-Site":      "none",
                "Sec-Fetch-User":      "?1",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control":       "max-age=0",
                "DNT":                 "1",
            },
        )

    async def _apply_stealth(self, page: Any) -> None:
        """Playwright の自動検出を回避するスクリプトを注入する。

        navigator.webdriver / plugins / languages を偽装し、
        HeadlessChromium 固有の特徴を隠す。
        playwright-stealth パッケージがあれば優先使用する。
        """
        try:
            from playwright_stealth import stealth_async  # type: ignore[import]
            await stealth_async(page)
            logger.debug("[Stealth] playwright-stealth 適用済み")
            return
        except ImportError:
            pass

        # playwright-stealth 非インストール時のフォールバック
        await page.add_init_script("""
            // webdriver フラグ削除
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

            // プラグイン偽装（実在する Chrome プラグインを模倣）
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const arr = [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                        { name: 'Chrome PDF Viewer',  filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                        { name: 'Native Client',      filename: 'internal-nacl-plugin', description: '' },
                    ];
                    arr.__proto__ = PluginArray.prototype;
                    return arr;
                }
            });

            // 言語設定（日本語環境）
            Object.defineProperty(navigator, 'languages', { get: () => ['ja', 'ja-JP', 'en-US', 'en'] });

            // Chrome オブジェクト（Headless では未定義 → Bot 検知に使われる）
            window.chrome = {
                app: { isInstalled: false, InstallState: {}, RunningState: {} },
                runtime: { id: undefined },
                loadTimes: function() { return {}; },
                csi: function() { return {}; },
            };

            // Permission API 偽装（Headless は denied を返す → 検知される）
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (params) =>
                params.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(params);

            // ハードウェア情報偽装
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
            Object.defineProperty(navigator, 'deviceMemory',        { get: () => 8 });

            // WebGL レンダラー偽装（GPU フィンガープリント対策）
            const getParam = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(param) {
                if (param === 37445) return 'Intel Inc.';
                if (param === 37446) return 'Intel Iris OpenGL Engine';
                return getParam.call(this, param);
            };
        """)

    # ── タイムラインスクレイピング ────────────────────────────────────

    async def _scrape_timeline(
        self,
        page: Any,
        screen_name: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[RawTweet]:
        """
        指定アカウントのタイムラインを巡回して RawTweet リストを返す。

        X の DOM 構造は頻繁に変わるため、データ属性・aria ラベルを優先する。
        ページネーションは「スクロールで追加読み込み」方式。
        """
        await self._rate_limiter.wait(screen_name)

        url = f"{self._BASE_URL}/{screen_name}"
        logger.info("[%s] タイムライン読み込み: %s", screen_name, url)

        try:
            await page.goto(url, wait_until="networkidle", timeout=30_000)
        except Exception as exc:
            logger.warning("[%s] ページ読み込みタイムアウト: %s", screen_name, exc)

        tweets: list[RawTweet] = []
        seen_ids: set[str] = set()
        scroll_attempts = 0
        max_scrolls = 15  # 最大スクロール回数（レート制限対策）

        while scroll_attempts < max_scrolls:
            # DOM からツイートを抽出
            new_tweets = await self._extract_tweets_from_dom(
                page, screen_name, window_start, window_end, seen_ids
            )
            tweets.extend(new_tweets)

            # ウィンドウ外（window_start より古い）ツイートが出たら終了
            if await self._is_past_window(page, window_start):
                logger.debug("[%s] ウィンドウ外に達したためスクロール終了", screen_name)
                break

            # スクロールして次のバッチを読み込む
            prev_height = await page.evaluate("document.documentElement.scrollHeight")
            await page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
            await asyncio.sleep(2.0)  # ロード待機

            new_height = await page.evaluate("document.documentElement.scrollHeight")
            if new_height == prev_height:
                logger.debug("[%s] それ以上スクロールできません", screen_name)
                break

            scroll_attempts += 1

        logger.info("[%s] 取得完了: %d件", screen_name, len(tweets))
        return tweets

    async def _extract_tweets_from_dom(
        self,
        page: Any,
        screen_name: str,
        window_start: datetime,
        window_end: datetime,
        seen_ids: set[str],
    ) -> list[RawTweet]:
        """DOM からツイートテキスト・ID・投稿日時を抽出する。"""
        results: list[RawTweet] = []

        # X の article 要素からツイートを取得
        # data-testid="tweet" または role="article" を目印にする
        tweet_elements = await page.query_selector_all('[data-testid="tweet"]')

        for el in tweet_elements:
            try:
                # ── tweet_id: permalink の末尾 ID ──────────────────
                link = await el.query_selector('a[href*="/status/"]')
                if not link:
                    continue
                href = await link.get_attribute("href") or ""
                m = re.search(r"/status/(\d+)", href)
                if not m:
                    continue
                tweet_id = m.group(1)
                if tweet_id in seen_ids:
                    continue

                # ── posted_at: <time> 要素の datetime 属性 ─────────
                time_el = await el.query_selector("time")
                if not time_el:
                    continue
                dt_str = await time_el.get_attribute("datetime") or ""
                try:
                    posted_dt = datetime.fromisoformat(
                        dt_str.replace("Z", "+00:00")
                    ).astimezone().replace(tzinfo=None)
                except ValueError:
                    continue

                # 取得ウィンドウ外はスキップ
                if not (window_start <= posted_dt <= window_end):
                    continue

                # ── raw_text: ツイート本文 ────────────────────────
                text_el = await el.query_selector('[data-testid="tweetText"]')
                if not text_el:
                    continue
                raw_text = (await text_el.inner_text()).strip()
                if not raw_text:
                    continue

                tweet = RawTweet(
                    tweet_id=tweet_id,
                    screen_name=screen_name,
                    raw_text=raw_text,
                    posted_at=posted_dt.isoformat(timespec="seconds"),
                )

                seen_ids.add(tweet_id)
                results.append(tweet)

            except Exception as exc:
                logger.debug("ツイート要素パース失敗: %s", exc)
                continue

        return results

    async def _is_past_window(self, page: Any, window_start: datetime) -> bool:
        """最後のツイートが window_start より古ければ True を返す。"""
        time_elements = await page.query_selector_all("time")
        if not time_elements:
            return False
        last = time_elements[-1]
        dt_str = await last.get_attribute("datetime") or ""
        try:
            dt = datetime.fromisoformat(
                dt_str.replace("Z", "+00:00")
            ).astimezone().replace(tzinfo=None)
            return dt < window_start
        except ValueError:
            return False

    # ── DB 保存 ────────────────────────────────────────────────────────

    def _save_tweets(self, tweets: list[RawTweet], screen_name: str) -> None:
        """競馬関連ツイートのみ x_signals テーブルに保存する。"""
        if self._dry_run:
            for t in tweets:
                if t.is_racing_related():
                    logger.info("[DRY-RUN] 保存予定: %s", t)
            return

        # x_accounts に upsert（初回追加）
        self._conn.execute(
            """
            INSERT INTO x_accounts (screen_name, last_scraped_at)
            VALUES (?, datetime('now','localtime'))
            ON CONFLICT(screen_name) DO UPDATE SET
                last_scraped_at = excluded.last_scraped_at,
                updated_at      = datetime('now','localtime')
            """,
            (screen_name,),
        )

        for tweet in tweets:
            if not tweet.is_racing_related():
                self._skipped += 1
                continue
            try:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO x_signals
                        (tweet_id, screen_name, raw_text, posted_at,
                         signal_type, race_name_raw)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tweet.tweet_id,
                        tweet.screen_name,
                        tweet.raw_text,
                        tweet.posted_at,
                        tweet.extract_signal_type(),
                        tweet.extract_race_name(),
                    ),
                )
                self._saved += 1
            except sqlite3.IntegrityError:
                self._skipped += 1

        self._conn.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="X（旧Twitter）競馬予想スクレイパー",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "例:\n"
            "  py src/scraper/x_scraper.py                    # 当日分を取得\n"
            "  py src/scraper/x_scraper.py --date 2026-05-17  # 指定日を取得\n"
            "  py src/scraper/x_scraper.py --dry-run          # DB書き込みなし\n"
            "  py src/scraper/x_scraper.py --check-only       # DB件数確認のみ\n"
        ),
    )
    p.add_argument("--date",         help="取得対象日 (YYYY-MM-DD)。省略時は当日")
    p.add_argument("--account",      help="特定アカウントのみ処理 (screen_name)")
    p.add_argument("--dry-run",      action="store_true", help="DB書き込みをスキップ")
    p.add_argument("--check-only",   action="store_true", help="DB の x_signals 件数を表示して終了")
    return p.parse_args()


def check_db_status(conn: sqlite3.Connection) -> None:
    """x_signals / x_accounts の現在の件数を表示する。"""
    try:
        n_signals  = conn.execute("SELECT COUNT(*) FROM x_signals").fetchone()[0]
        n_parsed   = conn.execute("SELECT COUNT(*) FROM x_signals WHERE parsed=1").fetchone()[0]
        n_accounts = conn.execute("SELECT COUNT(*) FROM x_accounts WHERE is_active=1").fetchone()[0]
        print(f"x_signals  : {n_signals} 件 (うち解析済み {n_parsed} 件)")
        print(f"x_accounts : {n_accounts} 件（active）")
    except sqlite3.OperationalError as e:
        print(f"x_signals テーブルが未作成です（init_db を実行してください）: {e}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    try:
        if args.check_only:
            check_db_status(conn)
            return

        target_date: date | None = None
        if args.date:
            target_date = date.fromisoformat(args.date)

        scraper = XScraper(conn, dry_run=args.dry_run)
        result  = asyncio.run(
            scraper.run(target_date=target_date, account_filter=args.account)
        )
        print(
            f"完了: saved={result['saved']} "
            f"skipped={result['skipped']} "
            f"errors={result['errors']}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
