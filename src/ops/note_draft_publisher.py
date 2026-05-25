"""
note.com Playwright 自動下書き保存モジュール

note.com は公式 API を持たないため Playwright で操作する。
「下書き保存」のみ行い、公開（発行）は社長が手動で行う。

必須環境変数 (.env):
  NOTE_EMAIL    : note.com のメールアドレス
  NOTE_PASSWORD : note.com のパスワード

使用例:
  from src.ops.note_draft_publisher import save_draft
  ok = save_draft(title="週末予想 5/10", body="## AI予想まとめ\n...", tags=["競馬", "UMALOGI"])

Bot検知回避:
  playwright-stealth を使用して navigator.webdriver / chrome runtime 等を隠蔽。
  headless=False で実行することで Chromium 特有のフラグも除去。
"""

from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_DEBUG_DIR = _ROOT / "outputs" / "debug"
_DEBUG_DIR.mkdir(parents=True, exist_ok=True)

_LOGIN_URL    = "https://note.com/login"
_NEW_POST_URL = "https://note.com/notes/new"
_SETTINGS_URL = "https://note.com/settings/profile"
_SESSION_FILE = _ROOT / ".note_session.json"

# Playwright 1.58 が同梱する Chromium バージョンに合わせた UA
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)

# ボット検知・WAF の特徴的 HTML パターン
_BOT_SIGNATURES = [
    ("cloudflare",    ["cf-browser-verification", "Checking your browser", "_cf_chl_", "cf_clearance"]),
    ("imperva",       ["_Incapsula_Resource", "visid_incap_", "incap_ses_"]),
    ("akamai",        ["akamai-bot-manager", "__utmzzses"]),
    ("note_bot_page", ["申し訳ありません、ただ今障害が発生しております"]),
]

# なおふみブランドのプロフィール固定値
_PROFILE_NAME = "なおふみ | UMALOGI開発者"
_PROFILE_BIO  = (
    "世界最高峰の Python/SQL エンジニア。17年の実務経験と最新AI（Claude Code 等）を駆使し、"
    "5カ年分の全 JRA データを解析する競馬投資AI『UMALOGI』を開発。"
    "単なる予想ではなく、1円単位の ROI を追求する投資ロジックを提唱。"
)


# ─── 証拠保全・ブロック検知 ─────────────────────────────────────────────────

def _dump_evidence(page: "Page", label: str) -> Path:  # type: ignore[name-defined]
    """スクリーンショット + HTML ソースを outputs/debug/ に保存する。

    Args:
        page:  Playwright の Page オブジェクト。
        label: ファイル名に使うラベル文字列（例: "login_block"）。

    Returns:
        保存したスクリーンショットの Path。
    """
    ss_path = _DEBUG_DIR / f"note_{label}.png"
    html_path = _DEBUG_DIR / f"note_{label}.html"
    try:
        page.screenshot(path=str(ss_path), full_page=True)
        logger.info("[note] スクリーンショット: %s", ss_path)
    except Exception as e:
        logger.warning("[note] スクリーンショット失敗: %s", e)

    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        logger.info("[note] HTML ダンプ: %s (%d bytes)", html_path, len(html))
    except Exception as e:
        logger.warning("[note] HTML ダンプ失敗: %s", e)
        html = ""

    return ss_path


def _detect_block(page: "Page") -> str | None:  # type: ignore[name-defined]
    """ページが WAF / Bot検知 / note障害ページかを判定する。

    Args:
        page: Playwright の Page オブジェクト。

    Returns:
        正常の場合 None。ブロック検知時は "cloudflare" / "imperva" / "akamai" /
        "note_bot_page" のいずれかを返す。
    """
    try:
        html = page.content().lower()
    except Exception:
        return None

    for kind, signatures in _BOT_SIGNATURES:
        if any(sig.lower() in html for sig in signatures):
            return kind
    return None


def _log_block(kind: str, url: str, page: "Page") -> None:  # type: ignore[name-defined]
    """ブロック種別に応じた詳細ログを出力する。

    Args:
        kind: ブロック種別文字列（"cloudflare" / "imperva" / "akamai" / "note_bot_page"）。
        url:  アクセス中だった URL。
        page: Playwright の Page オブジェクト（将来の拡張用）。
    """
    if kind == "cloudflare":
        logger.error(
            "[note] Cloudflare Bot 検知ブロック — url=%s\n"
            "  対策: headless=False は有効。playwright-stealth も適用済み。\n"
            "  次の手: セッションを削除して再ログイン、またはリクエスト間隔を延ばす。",
            url,
        )
    elif kind in ("imperva", "akamai"):
        logger.error("[note] WAF(%s) ブロック — url=%s", kind, url)
    elif kind == "note_bot_page":
        logger.error(
            "[note] note.com ブロックページ — url=%s\n"
            "  通常ブラウザでアクセス可能であれば Playwright のフィンガープリントが検知された可能性あり。\n"
            "  HTML は outputs/debug/ に保存済み。",
            url,
        )
    else:
        logger.error("[note] 不明なブロック: kind=%s url=%s", kind, url)


# ─── ステルス Playwright コンテキスト ───────────────────────────────────────

def _build_stealth() -> "Stealth":  # type: ignore[name-defined]
    """playwright-stealth の Stealth インスタンスを構築する。

    UA・言語・プラットフォーム・ベンダーを Windows Chrome に偽装する。

    Returns:
        設定済みの Stealth インスタンス。
    """
    from playwright_stealth import Stealth
    return Stealth(
        navigator_user_agent_override=_UA,
        navigator_languages_override=("ja-JP", "ja", "en-US", "en"),
        navigator_platform_override="Win32",
        navigator_vendor_override="Google Inc.",
    )


def _browser_opts() -> dict[str, object]:
    """共通の browser launch オプションを返す。常に headless=False。

    Returns:
        Playwright の chromium.launch() に渡すオプション辞書。
    """
    return dict(
        headless=False,
        slow_mo=random.randint(40, 90),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
        ],
    )


def _ctx_opts(session: bool = False) -> dict[str, object]:
    """共通の BrowserContext オプションを返す。

    Args:
        session: True の場合、保存済みセッションファイルを storage_state として読み込む。

    Returns:
        Playwright の new_context() に渡すオプション辞書。
    """
    w = random.choice([1280, 1366, 1440, 1920])
    h = random.choice([768, 900, 1024, 1080])
    opts = dict(
        user_agent=_UA,
        viewport={"width": w, "height": h},
        locale="ja-JP",
        timezone_id="Asia/Tokyo",
        permissions=["clipboard-read", "clipboard-write"],
        extra_http_headers={
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "sec-ch-ua": '"Chromium";v="134","Google Chrome";v="134","Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    if session and _SESSION_FILE.exists():
        opts["storage_state"] = str(_SESSION_FILE)
    return opts


# ─── 認証情報 ────────────────────────────────────────────────────────────────

def _load_credentials() -> tuple[str, str]:
    """NOTE_EMAIL / NOTE_PASSWORD を環境変数（.env）から読み込む。

    Returns:
        (email, password) のタプル。

    Raises:
        EnvironmentError: NOTE_EMAIL または NOTE_PASSWORD が未設定の場合。
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(_ROOT / ".env", override=False)
    except ImportError:
        pass
    email    = os.environ.get("NOTE_EMAIL", "")
    password = os.environ.get("NOTE_PASSWORD", "")
    if not email or not password:
        raise EnvironmentError("NOTE_EMAIL / NOTE_PASSWORD が .env に設定されていません。")
    return email, password


# ─── 操作ヘルパー ────────────────────────────────────────────────────────────

def _human_delay(lo: float = 0.3, hi: float = 0.8) -> None:
    """ランダムな人間らしい遅延をかける（Bot検知回避）。

    Args:
        lo: 最小待機秒数。デフォルト 0.3 秒。
        hi: 最大待機秒数。デフォルト 0.8 秒。
    """
    time.sleep(random.uniform(lo, hi))


def _clipboard_paste(page: "Page", text: str) -> None:  # type: ignore[name-defined]
    """クリップボード経由でエディタにテキストをペーストする。

    navigator.clipboard API でテキストをクリップボードに書き込み、
    Ctrl+A → Ctrl+V でエディタに貼り付ける。

    Args:
        page: Playwright の Page オブジェクト。
        text: ペーストするテキスト。
    """
    escaped = text.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
    page.evaluate(f"async () => {{ await navigator.clipboard.writeText(`{escaped}`); }}")
    _human_delay(0.2, 0.5)
    page.keyboard.press("Control+a")
    page.keyboard.press("Control+v")
    _human_delay(0.3, 0.6)


def _try_autofill_login(page: "Page", email: str, password: str) -> None:  # type: ignore[name-defined]
    """メールアドレス・パスワードをフォームに自動入力してログインを試みる。

    複数のセレクタを順番に試して最初にヒットしたフィールドに入力する。
    フィールドが見つからない場合は警告ログのみ出力して処理を継続する。

    Args:
        page:     Playwright の Page オブジェクト。
        email:    note.com のメールアドレス（または note ID）。
        password: note.com のパスワード。
    """
    # note.com のメール欄は type="text"（email IDと共用）
    email_selectors = [
        'input[type="text"]',
        'input[type="email"]',
        'input[name="email"]',
        'input[placeholder*="mail"]',
        'input[placeholder*="note ID"]',
        'input[placeholder*="メール"]',
    ]
    email_filled = False
    for sel in email_selectors:
        try:
            locs = page.locator(sel)
            for i in range(locs.count()):
                loc = locs.nth(i)
                if loc.is_visible(timeout=2000):
                    loc.click()
                    _human_delay()
                    loc.fill(email)
                    _human_delay()
                    val = loc.input_value()
                    if email in val or val in email:
                        logger.info("[note] メールアドレス自動入力: %s (selector=%s)", email, sel)
                        email_filled = True
                        break
        except Exception:
            continue
        if email_filled:
            break

    if not email_filled:
        logger.warning("[note] メールアドレス入力フィールドが見つかりません — 手動入力してください")

    _human_delay()

    pw_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[placeholder*="パスワード"]',
    ]
    pw_filled = False
    for sel in pw_selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=3000):
                loc.click()
                _human_delay()
                loc.fill(password)
                logger.info("[note] パスワード自動入力完了")
                pw_filled = True
                _human_delay()
                break
        except Exception:
            continue

    if not pw_filled:
        logger.warning("[note] パスワード入力フィールドが見つかりません — 手動入力してください")

    _human_delay()

    btn_selectors = [
        'button[type="submit"]',
        'button:has-text("ログイン")',
        'input[type="submit"]',
    ]
    for sel in btn_selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=3000):
                loc.click()
                logger.info("[note] ログインボタンクリック: %s", sel)
                _human_delay(2.0, 3.5)
                break
        except Exception:
            continue


def _wait_for_login(page: "Page") -> bool:  # type: ignore[name-defined]
    """ログイン完了（/login URL からの離脱）を最大8分ポーリングで検知する。

    reCAPTCHA や二段階認証による手動操作を考慮して長めのタイムアウトを設定している。

    Args:
        page: Playwright の Page オブジェクト。

    Returns:
        ログイン完了を検出できた場合 True、タイムアウトの場合 False。
    """
    print("  ★ブラウザでログインが完了するまで待機中（最大8分）...")
    deadline = time.time() + 480
    while time.time() < deadline:
        try:
            current = page.evaluate("() => window.location.href")
        except Exception:
            current = page.url
        if "/login" not in current and "note.com" in current:
            logger.info("[note] ログイン完了を検出: %s", current)
            return True
        try:
            dom_ok: bool = page.evaluate("""() => {
                var hasPassword = !!document.querySelector('input[type="password"]');
                var hasLoginTitle = document.title.includes('ログイン');
                return window.location.pathname !== '/login' || (!hasPassword && !hasLoginTitle);
            }""")
            if dom_ok:
                logger.info("[note] DOM でログイン完了を検出: %s", page.evaluate("() => window.location.href"))
                return True
        except Exception:
            pass
        time.sleep(2)

    # タイムアウト後に最終確認
    try:
        current = page.evaluate("() => window.location.href")
    except Exception:
        current = page.url
    if "/login" not in current and "note.com" in current:
        logger.info("[note] タイムアウト後にログイン完了を確認: %s", current)
        return True

    logger.error("[note] ログイン完了を検出できませんでした（タイムアウト、url=%s）", current)
    return False


# ─── 公開 API ────────────────────────────────────────────────────────────────

def login_and_save_session() -> bool:
    """
    可視モードでブラウザを開き、メール/パスワードを自動入力してログインし、
    セッションを保存する。playwright-stealth でBot検知を回避。

    Returns:
        True = セッション保存成功, False = 失敗
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    try:
        email, password = _load_credentials()
        logger.info("[note] 認証情報ロード完了: %s", email)
    except EnvironmentError as e:
        logger.error("[note] %s", e)
        email, password = "", ""

    print(f"\n{'='*55}")
    print("  note.com ログイン — ブラウザが開きます (Stealth mode)")
    if email:
        print(f"  メールアドレス・パスワードを自動入力します: {email}")
        print("  reCAPTCHA が出た場合は手動で解決してください。")
    else:
        print("  NOTE_EMAIL/NOTE_PASSWORD 未設定 — 手動でログインしてください。")
    print("  ログイン完了後、このプログラムが自動で続行します。")
    print(f"{'='*55}\n")

    stealth = _build_stealth()
    with stealth.use_sync(sync_playwright()) as pw:
        browser = pw.chromium.launch(**_browser_opts())
        ctx = browser.new_context(**_ctx_opts(session=False))
        page = ctx.new_page()

        # ページロード前にwebdriverフラグを追加で隠蔽
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;
        """)

        page.goto(_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        _human_delay(1.5, 2.5)

        # ブロックチェック
        block = _detect_block(page)
        if block:
            _log_block(block, _LOGIN_URL, page)
            _dump_evidence(page, "login_block")
            browser.close()
            return False

        if email and password:
            _try_autofill_login(page, email, password)

        # reCAPTCHA や二段階認証のために長めに待機
        if not _wait_for_login(page):
            _dump_evidence(page, "login_timeout")
            browser.close()
            return False

        _human_delay(1.0, 2.0)
        ctx.storage_state(path=str(_SESSION_FILE))
        logger.info("[note] セッション保存完了: %s", _SESSION_FILE)
        browser.close()

    print(f"\n✅ セッション保存完了: {_SESSION_FILE}")
    print("  以降の post_weekly_note_draft.py はこのセッションを自動利用します。")
    return True


def save_draft(
    title: str,
    body: str,
    *,
    tags: list[str] | None = None,
    headless: bool = False,  # Bot検知回避のため常に False 推奨
) -> bool:
    """
    note.com に下書き記事を保存する (Stealth mode)。

    Returns:
        True = 下書き保存成功, False = 失敗
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    if not _SESSION_FILE.exists():
        logger.error("[note] セッションファイルが見つかりません。先に --login-only を実行してください。")
        return False

    # headless 引数は受け取るが Bot検知回避のため常に False を推奨
    if headless:
        logger.warning("[note] headless=True はBot検知されやすいため headless=False に強制します")
        headless = False

    stealth = _build_stealth()
    with stealth.use_sync(sync_playwright()) as pw:
        browser = pw.chromium.launch(**_browser_opts())
        ctx = browser.new_context(**_ctx_opts(session=True))
        page = ctx.new_page()

        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;
        """)

        # ── Step 1: セッション有効確認 ────────────────────────────
        try:
            logger.info("[note] セッション確認中 (stealth mode)...")
            page.goto("https://note.com/", wait_until="domcontentloaded", timeout=20000)
            _human_delay(2.0, 3.0)

            block = _detect_block(page)
            if block:
                _log_block(block, "https://note.com/", page)
                _dump_evidence(page, "session_check_block")
                browser.close()
                return False

            ss = _dump_evidence(page, "after_login")
            current_url = page.evaluate("() => window.location.href")
            if "/login" in current_url:
                logger.error("[note] セッション期限切れ — 再ログインが必要: %s", current_url)
                browser.close()
                return False

            logger.info("[note] セッション有効: %s", current_url)

        except Exception as e:
            _dump_evidence(page, "login_error")
            logger.error("[note] セッション確認例外: %s", e)
            browser.close()
            return False

        # ── Step 2: 新規記事作成ページへ遷移 ─────────────────────
        logger.info("[note] 新規記事ページへ遷移: %s", _NEW_POST_URL)
        try:
            page.goto(_NEW_POST_URL, wait_until="domcontentloaded", timeout=30000)
            _human_delay(3.0, 5.0)
        except Exception as e:
            logger.error("[note] 記事作成ページ遷移失敗: %s", e)
            _dump_evidence(page, "new_post_error")
            browser.close()
            return False

        # ブロック・障害ページ検出（証拠保全あり）
        block = _detect_block(page)
        _dump_evidence(page, "new_post")
        if block:
            _log_block(block, _NEW_POST_URL, page)
            browser.close()
            return False

        logger.info("[note] 記事作成ページ正常表示")

        # ── Step 3: タイトル入力 ──────────────────────────────────
        title_selectors = [
            'input[data-placeholder="記事タイトル"]',
            'input[placeholder*="タイトル"]',
            'textarea[placeholder*="タイトル"]',
            '[contenteditable="true"][data-placeholder*="タイトル"]',
            '[contenteditable="true"][aria-label*="タイトル"]',
            'h1[contenteditable="true"]',
            '.title-input',
        ]
        title_filled = False
        for sel in title_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=2000):
                    loc.click()
                    _human_delay()
                    loc.fill(title)
                    title_filled = True
                    logger.info("[note] タイトル入力: %r (selector=%s)", title[:30], sel)
                    break
            except Exception:
                continue

        if not title_filled:
            logger.warning("[note] タイトル入力フィールドが見つかりません — エディタが未ロードの可能性")

        _human_delay(0.8, 1.5)

        # ── Step 4: 本文入力（クリップボード経由）────────────────
        body_selectors = [
            '.ProseMirror',
            '[contenteditable="true"].ProseMirror',
            '[role="textbox"][contenteditable="true"]',
            '[contenteditable="true"][data-placeholder*="本文"]',
            '[contenteditable="true"][class*="editor"]',
            'div[contenteditable="true"]',
        ]
        body_filled = False
        for sel in body_selectors:
            try:
                locs = page.locator(sel)
                if locs.count() == 0:
                    continue
                for i in range(min(locs.count(), 5)):
                    loc = locs.nth(i)
                    if not loc.is_visible(timeout=1000):
                        continue
                    placeholder = loc.get_attribute("data-placeholder") or ""
                    aria = loc.get_attribute("aria-label") or ""
                    if "タイトル" in placeholder or "タイトル" in aria:
                        continue
                    loc.click()
                    _human_delay(0.4, 0.8)
                    page.keyboard.press("Control+End")
                    _human_delay()
                    try:
                        _clipboard_paste(page, body)
                        body_filled = True
                        logger.info("[note] 本文ペースト完了 (%d 文字, selector=%s)", len(body), sel)
                        break
                    except Exception as ce:
                        logger.warning("[note] clipboard_paste 失敗: %s — type() フォールバック", ce)
                        truncated = body[:3000] + "\n\n[以下省略]" if len(body) > 3000 else body
                        page.keyboard.type(truncated, delay=0)
                        body_filled = True
                        break
                if body_filled:
                    break
            except Exception as ex:
                logger.debug("[note] body selector '%s' エラー: %s", sel, ex)

        if not body_filled and not title_filled:
            # エディタ自体が見つからない = ブロック or ページ構造変化
            _dump_evidence(page, "editor_not_found")
            logger.error(
                "[note] タイトル・本文フィールドが見つかりません。\n"
                "  → Bot検知ブロック / note.com ページ構造変化 の可能性。\n"
                "  → outputs/debug/note_editor_not_found.html を確認してください。"
            )
            browser.close()
            return False

        _dump_evidence(page, "body_entered")
        _human_delay(0.8, 1.5)

        # ── Step 5: タグ設定（任意） ──────────────────────────────
        if tags:
            tag_selectors = [
                'input[placeholder*="タグ"]',
                'input[data-placeholder*="タグ"]',
                '[class*="tag"] input',
                'input[name*="tag"]',
            ]
            for tag in tags[:5]:
                for sel in tag_selectors:
                    try:
                        loc = page.locator(sel).first
                        if loc.count() > 0 and loc.is_visible(timeout=2000):
                            loc.click()
                            loc.fill(tag)
                            page.keyboard.press("Enter")
                            _human_delay(0.4, 0.8)
                            logger.info("[note] タグ追加: %s", tag)
                            break
                    except Exception:
                        continue

        # ── Step 6: 下書き保存 ────────────────────────────────────
        _human_delay(0.8, 1.5)
        draft_selectors = [
            'button:has-text("下書き保存")',
            'button:has-text("下書きとして保存")',
            'button[data-testid="save-draft"]',
            '[aria-label="下書き保存"]',
            'a:has-text("下書き保存")',
        ]
        saved = False
        for sel in draft_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=2000):
                    loc.click(timeout=8000)
                    _human_delay(2.0, 3.0)
                    logger.info("[note] 下書き保存ボタンクリック: %s", sel)
                    saved = True
                    break
            except Exception as ex:
                logger.debug("[note] セレクタ '%s' → %s", sel, ex)

        if not saved and body_filled:
            # エディタは開いているが保存ボタンが見つからない場合のみ Ctrl+S
            try:
                page.keyboard.press("Control+s")
                _human_delay(2.0, 3.0)
                logger.info("[note] Ctrl+S 下書き保存")
                saved = True
            except Exception as e:
                logger.warning("[note] Ctrl+S 失敗: %s", e)

        _dump_evidence(page, "draft_saved")

        if not saved:
            _notify_discord("note 下書き保存", "保存ボタンが見つかりません")
            browser.close()
            return False

        browser.close()
        logger.info("[note] 下書き保存完了: %r", title[:40])
        return True


def update_profile(*, headless: bool = False) -> bool:
    """note.com のプロフィールを「なおふみ | UMALOGI開発者」に更新する。

    _PROFILE_NAME と _PROFILE_BIO の定数値でニックネームと自己紹介を上書きする。
    セッションファイルが存在しない場合は即座に False を返す。

    Args:
        headless: ブラウザをヘッドレスモードで起動するか。Bot検知回避のため False 推奨。

    Returns:
        プロフィール更新成功の場合 True、失敗の場合 False。
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    if not _SESSION_FILE.exists():
        logger.error("[note] セッションファイルが見つかりません。先に login_and_save_session() を実行してください。")
        return False

    stealth = _build_stealth()
    with stealth.use_sync(sync_playwright()) as pw:
        browser = pw.chromium.launch(**_browser_opts())
        ctx = browser.new_context(**_ctx_opts(session=True))
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        page.goto("https://note.com/", wait_until="domcontentloaded", timeout=20000)
        _human_delay(2.0, 3.0)
        if "/login" in page.evaluate("() => window.location.href"):
            logger.error("[note] セッション期限切れ。再度 login_and_save_session() を実行してください。")
            browser.close()
            return False

        try:
            page.goto(_SETTINGS_URL, wait_until="domcontentloaded", timeout=40000)
            try:
                page.wait_for_selector('input[name="editNickname"]', timeout=15000)
            except Exception:
                _human_delay(5.0, 6.0)
            _dump_evidence(page, "settings")
        except Exception as e:
            logger.error("[note] 設定ページ遷移失敗: %s", e)
            browser.close()
            return False

        try:
            page.fill('input[name="editNickname"]', _PROFILE_NAME, timeout=8000)
            logger.info("[note] 名前入力完了: %s", _PROFILE_NAME)
        except Exception as e:
            logger.warning("[note] 名前フィールド fill 失敗: %s", e)

        _human_delay()

        try:
            page.fill('textarea[name="editBiography"]', _PROFILE_BIO, timeout=8000)
            logger.info("[note] 自己紹介入力完了")
        except Exception as e:
            logger.warning("[note] 自己紹介フィールド fill 失敗: %s", e)

        _human_delay(0.5, 1.0)

        saved = False
        for sel in ['button:has-text("保存")', 'button:has-text("変更を保存")', 'button[type="submit"]']:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=2000):
                    loc.click(timeout=8000)
                    _human_delay(2.0, 3.0)
                    saved = True
                    logger.info("[note] プロフィール保存ボタンクリック: %s", sel)
                    break
            except Exception:
                continue

        _dump_evidence(page, "profile_saved")
        browser.close()
        if saved:
            logger.info("[note] プロフィール更新完了: %s", _PROFILE_NAME)
            return True
        else:
            logger.warning("[note] 保存ボタンが見つかりませんでした（手動確認推奨）")
            return False


def _notify_discord(step: str, error: str, screenshot_path: Path | None = None) -> None:
    """Discord に手動介入要請の通知を送る（例外は握り潰す）。

    Args:
        step:            エラーが発生したステップ名（例: "note 下書き保存"）。
        error:           エラーメッセージ。
        screenshot_path: 添付するスクリーンショットの Path。省略可。
    """
    try:
        from src.notification.discord_notifier import DiscordNotifier
        DiscordNotifier().notify_intervention_required(
            step=step,
            error=error,
            action=f"note.com を手動で確認してください: {_LOGIN_URL}",
            screenshot_path=screenshot_path,
        )
    except Exception as e:
        logger.warning("[note] Discord 通知失敗: %s", e)
