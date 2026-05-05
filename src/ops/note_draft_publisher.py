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
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_DEBUG_DIR = _ROOT / "outputs" / "debug"
_DEBUG_DIR.mkdir(parents=True, exist_ok=True)

_LOGIN_URL  = "https://note.com/login"
_NEW_POST_URL = "https://note.com/notes/new"


def _load_credentials() -> tuple[str, str]:
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


def save_draft(
    title: str,
    body: str,
    *,
    tags: list[str] | None = None,
    headless: bool = True,
) -> bool:
    """
    note.com に下書き記事を保存する。

    フロー:
      1. ログイン
      2. 新規記事作成ページへ遷移
      3. タイトル・本文を入力
      4. タグを設定（指定時）
      5. 「下書き保存」ボタンをクリック
      6. 保存確認後にスクリーンショット

    Returns:
        True = 下書き保存成功, False = 失敗
    """
    try:
        email, password = _load_credentials()
    except EnvironmentError as e:
        logger.error("[note] %s", e)
        return False

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        # ── Step 1: ログイン ──────────────────────────────────────
        try:
            logger.info("[note] ログイン開始: %s", _LOGIN_URL)
            page.goto(_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            # メールアドレス入力
            page.wait_for_selector('input[type="email"]', timeout=10000)
            page.fill('input[type="email"]', email)

            # パスワード入力
            page.fill('input[type="password"]', password)

            # ログインボタンをクリック
            for sel in [
                'button[type="submit"]',
                'button:has-text("ログイン")',
                'input[type="submit"]',
            ]:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.click(timeout=8000)
                    break

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            time.sleep(2)

            content = page.content()
            logged_in = (
                "ログアウト" in content
                or "note.com" in page.url
                and "/login" not in page.url
            )
            if not logged_in and "/login" in page.url:
                ss = _DEBUG_DIR / "note_login_fail.png"
                page.screenshot(path=str(ss))
                logger.error("[note] ログイン失敗。スクリーンショット: %s", ss)
                _notify_discord("note ログイン", f"ログイン後も /login にとどまる (url={page.url})", ss)
                browser.close()
                return False

            logger.info("[note] ログイン成功: %s", page.url)

        except PWTimeout as e:
            ss = _DEBUG_DIR / "note_login_timeout.png"
            page.screenshot(path=str(ss))
            logger.error("[note] ログインタイムアウト: %s", e)
            _notify_discord("note ログイン（タイムアウト）", str(e), ss)
            browser.close()
            return False

        # ── Step 2: 新規記事作成ページへ遷移 ─────────────────────
        logger.info("[note] 新規記事ページへ遷移: %s", _NEW_POST_URL)
        try:
            page.goto(_NEW_POST_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)
        except Exception as e:
            logger.error("[note] 記事作成ページ遷移失敗: %s", e)
            browser.close()
            return False

        # ── Step 3: タイトル入力 ──────────────────────────────────
        title_selectors = [
            'input[data-placeholder="記事タイトル"]',
            'input[placeholder*="タイトル"]',
            '[contenteditable="true"][data-placeholder*="タイトル"]',
            '.title-input',
            'h1[contenteditable="true"]',
        ]
        title_filled = False
        for sel in title_selectors:
            try:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.click()
                    page.locator(sel).first.fill(title)
                    title_filled = True
                    logger.info("[note] タイトル入力: %r (selector=%s)", title[:30], sel)
                    break
            except Exception:
                continue
        if not title_filled:
            logger.warning("[note] タイトル入力フィールドが見つかりません（DOMが変更された可能性）")

        time.sleep(1)

        # ── Step 4: 本文入力 ──────────────────────────────────────
        body_selectors = [
            '[contenteditable="true"][data-placeholder*="本文"]',
            '[contenteditable="true"][class*="editor"]',
            '.ProseMirror',
            'div[contenteditable="true"]',
            'textarea[name="body"]',
        ]
        body_filled = False
        for sel in body_selectors:
            try:
                locs = page.locator(sel)
                if locs.count() > 0:
                    # タイトル以外の最初の contenteditable に入力
                    for i in range(locs.count()):
                        loc = locs.nth(i)
                        placeholder = loc.get_attribute("data-placeholder") or ""
                        aria = loc.get_attribute("aria-label") or ""
                        if "タイトル" not in placeholder and "タイトル" not in aria:
                            loc.click()
                            loc.fill(body)
                            body_filled = True
                            logger.info("[note] 本文入力完了 (%d 文字)", len(body))
                            break
                    if body_filled:
                        break
            except Exception:
                continue

        if not body_filled:
            logger.warning("[note] 本文入力フィールドが見つかりません")

        time.sleep(1)

        # ── Step 5: タグ設定（任意） ──────────────────────────────
        if tags:
            tag_selectors = [
                'input[placeholder*="タグ"]',
                'input[data-placeholder*="タグ"]',
                '[class*="tag"] input',
            ]
            for tag in tags[:5]:  # 最大5タグ
                for sel in tag_selectors:
                    try:
                        if page.locator(sel).count() > 0:
                            page.fill(sel, tag)
                            page.keyboard.press("Enter")
                            time.sleep(0.5)
                            logger.info("[note] タグ追加: %s", tag)
                            break
                    except Exception:
                        continue

        # ── Step 6: 下書き保存 ────────────────────────────────────
        draft_selectors = [
            'button:has-text("下書き保存")',
            'button:has-text("下書きとして保存")',
            'button[data-testid="save-draft"]',
            'a:has-text("下書き保存")',
        ]
        saved = False
        for sel in draft_selectors:
            try:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.click(timeout=8000)
                    time.sleep(2)
                    logger.info("[note] 下書き保存ボタンをクリック: %s", sel)
                    saved = True
                    break
            except Exception as ex:
                logger.debug("[note] セレクタ '%s' → %s", sel, ex)

        if not saved:
            # Ctrl+S で保存を試みる
            try:
                page.keyboard.press("Control+s")
                time.sleep(2)
                logger.info("[note] Ctrl+S で保存を試みました")
                saved = True
            except Exception as e:
                logger.warning("[note] Ctrl+S 失敗: %s", e)

        ss = _DEBUG_DIR / "note_draft_saved.png"
        page.screenshot(path=str(ss), full_page=True)
        logger.info("[note] スクリーンショット: %s", ss)

        if not saved:
            _notify_discord(
                "note 下書き保存",
                "下書き保存ボタンが見つかりません（DOM変更の可能性）",
                ss,
            )
            browser.close()
            return False

        browser.close()
        logger.info("[note] 下書き保存完了: %r", title[:40])
        return True


def _notify_discord(step: str, error: str, screenshot_path: Path | None = None) -> None:
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
