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

_LOGIN_URL    = "https://note.com/login"
_NEW_POST_URL = "https://note.com/notes/new"
_SETTINGS_URL = "https://note.com/settings/profile"
_SESSION_FILE = _ROOT / ".note_session.json"  # Cookie 永続化ファイル（gitignore 推奨）

# なおふみブランドのプロフィール固定値
_PROFILE_NAME = "なおふみ | UMALOGI開発者"
_PROFILE_BIO  = (
    "世界最高峰の Python/SQL エンジニア。17年の実務経験と最新AI（Claude Code 等）を駆使し、"
    "5カ年分の全 JRA データを解析する競馬投資AI『UMALOGI』を開発。"
    "単なる予想ではなく、1円単位の ROI を追求する投資ロジックを提唱。"
)


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


def _clipboard_paste(page, text: str) -> None:
    """
    テキストをクリップボード経由でエディタにペーストする。
    JavaScript で clipboard.writeText を呼び出してから Ctrl+V。
    """
    # JS 経由でクリップボードに書き込む（pyperclip は不要）
    escaped = text.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
    page.evaluate(f"""
        async () => {{
            await navigator.clipboard.writeText(`{escaped}`);
        }}
    """)
    time.sleep(0.3)
    page.keyboard.press("Control+a")  # 既存テキストを全選択してから
    page.keyboard.press("Control+v")  # ペースト
    time.sleep(0.5)


def login_and_save_session() -> bool:
    """
    可視モードでブラウザを開き、手動ログインを促してセッションを保存する。
    reCAPTCHA が出た場合にユーザーが手動で解決できる。
    保存したセッションは .note_session.json に書き込み、以降の操作で再利用する。

    Returns:
        True = セッション保存成功, False = 失敗
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    print(f"\n{'='*55}")
    print("  note.com 手動ログイン — ブラウザが開きます")
    print("  reCAPTCHA を解決して「ログイン」ボタンを押してください。")
    print("  ログイン完了後、このプログラムが自動で続行します。")
    print(f"{'='*55}\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            permissions=["clipboard-read", "clipboard-write"],
        )
        page = ctx.new_page()
        page.goto(_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

        # ログイン完了を検知するまで待機（最大3分）
        try:
            page.wait_for_url("**/note.com/", timeout=180000)
        except Exception:
            pass

        if "/login" in page.url:
            logger.error("[note] ログイン完了を検出できませんでした")
            browser.close()
            return False

        # セッションを保存
        ctx.storage_state(path=str(_SESSION_FILE))
        logger.info("[note] セッション保存完了: %s", _SESSION_FILE)
        browser.close()

    print(f"\n✅ セッション保存完了: {_SESSION_FILE}")
    print("  以降の publish_note_draft.py はこのセッションを自動利用します。")
    return True


def _new_context_with_session(pw, headless: bool = True):
    """
    保存済みセッションがあればそれを使用、なければ空のコンテキストを返す。
    """
    base_opts = dict(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        permissions=["clipboard-read", "clipboard-write"],
    )
    browser = pw.chromium.launch(headless=headless)
    if _SESSION_FILE.exists():
        logger.info("[note] 保存済みセッションを使用: %s", _SESSION_FILE)
        ctx = browser.new_context(storage_state=str(_SESSION_FILE), **base_opts)
    else:
        logger.warning("[note] セッションファイルなし — 直接ログインを試みます")
        ctx = browser.new_context(**base_opts)
    return browser, ctx


def update_profile(*, headless: bool = True) -> bool:
    """
    note.com のプロフィールを「なおふみ | UMALOGI開発者」に更新する。

    note.com 設定ページ（/settings）で名前・自己紹介を書き換える。

    Returns:
        True = 更新成功, False = 失敗
    """
    try:
        email, password = _load_credentials()
    except EnvironmentError as e:
        logger.error("[note] %s", e)
        return False

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    if not _SESSION_FILE.exists():
        logger.error("[note] セッションファイルが見つかりません。先に login_and_save_session() を実行してください。")
        return False

    with sync_playwright() as pw:
        browser, ctx = _new_context_with_session(pw, headless=headless)
        page = ctx.new_page()

        # ログイン状態を確認
        page.goto("https://note.com/", wait_until="domcontentloaded", timeout=20000)
        time.sleep(2)
        if "/login" in page.url:
            logger.error("[note] セッション期限切れ。再度 login_and_save_session() を実行してください。")
            browser.close()
            return False
        logger.info("[note] セッション有効: %s", page.url)

        # 設定ページへ遷移
        try:
            page.goto(_SETTINGS_URL, wait_until="domcontentloaded", timeout=40000)
            # React/Next.js の SPA レンダリング完了を待つ
            try:
                page.wait_for_selector('input[name="editNickname"]', timeout=15000)
            except Exception:
                time.sleep(5)  # フォールバック: 5秒待機
            ss = _DEBUG_DIR / "note_settings.png"
            try:
                page.screenshot(path=str(ss), timeout=60000)
                logger.info("[note] 設定ページ: %s", ss)
            except Exception:
                logger.warning("[note] 設定ページスクリーンショット失敗（続行）")
        except Exception as e:
            logger.error("[note] 設定ページ遷移失敗: %s", e)
            browser.close()
            return False

        # 名前・自己紹介を page.fill() で入力（Playwright が keyboard type をシミュレート → React state も更新）
        name_filled = False
        bio_filled  = False
        try:
            page.fill('input[name="editNickname"]', _PROFILE_NAME, timeout=8000)
            name_filled = True
            logger.info("[note] 名前入力完了: %s", _PROFILE_NAME)
        except Exception as e:
            logger.warning("[note] 名前フィールド fill 失敗: %s", e)

        time.sleep(0.3)

        try:
            page.fill('textarea[name="editBiography"]', _PROFILE_BIO, timeout=8000)
            bio_filled = True
            logger.info("[note] 自己紹介入力完了")
        except Exception as e:
            logger.warning("[note] 自己紹介フィールド fill 失敗: %s", e)

        time.sleep(0.5)

        time.sleep(0.5)

        # 保存ボタンをクリック（「保存」テキストで絞り込み）
        save_selectors = [
            'button:has-text("保存")',
            'button:has-text("変更を保存")',
            'button[type="submit"]',
            'input[type="submit"]',
        ]
        saved = False
        for sel in save_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=2000):
                    loc.click(timeout=8000)
                    time.sleep(2)
                    saved = True
                    logger.info("[note] プロフィール保存ボタンクリック: %s", sel)
                    break
            except Exception:
                continue

        ss_final = _DEBUG_DIR / "note_profile_saved.png"
        page.screenshot(path=str(ss_final), full_page=True)
        logger.info("[note] プロフィール保存後スクリーンショット: %s", ss_final)

        browser.close()
        if saved:
            logger.info("[note] プロフィール更新完了: %s", _PROFILE_NAME)
            return True
        else:
            logger.warning("[note] 保存ボタンが見つかりませんでした（手動確認推奨）")
            return False


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
      3. タイトル入力
      4. 本文をクリップボード経由でペースト
      5. タグ設定（指定時）
      6. 「下書き保存」ボタンをクリック
      7. 保存確認後にスクリーンショット

    Returns:
        True = 下書き保存成功, False = 失敗
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("[note] Playwright がインストールされていません。pip install playwright")
        return False

    if not _SESSION_FILE.exists():
        logger.error("[note] セッションファイルが見つかりません。先に --login を実行してください。")
        return False

    with sync_playwright() as pw:
        browser, ctx = _new_context_with_session(pw, headless=headless)
        page = ctx.new_page()

        # ── Step 1: セッション有効確認 ────────────────────────────
        try:
            logger.info("[note] セッション確認中...")
            page.goto("https://note.com/", wait_until="domcontentloaded", timeout=20000)
            time.sleep(2)

            # スクリーンショットで状態確認
            ss = _DEBUG_DIR / "note_after_login.png"
            page.screenshot(path=str(ss))
            logger.info("[note] ログイン後スクリーンショット: %s", ss)

            current_url = page.url
            if "/login" in current_url:
                logger.error("[note] ログイン失敗（まだ /login にいる）: %s", current_url)
                _notify_discord("note ログイン", f"ログイン後も /login (url={current_url})", ss)
                browser.close()
                return False

            logger.info("[note] ログイン成功: %s", current_url)

        except Exception as e:
            ss = _DEBUG_DIR / "note_login_error.png"
            page.screenshot(path=str(ss))
            logger.error("[note] ログイン例外: %s", e)
            browser.close()
            return False

        # ── Step 2: 新規記事作成ページへ遷移 ─────────────────────
        logger.info("[note] 新規記事ページへ遷移: %s", _NEW_POST_URL)
        try:
            page.goto(_NEW_POST_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)
            ss = _DEBUG_DIR / "note_new_post.png"
            page.screenshot(path=str(ss))
            logger.info("[note] 記事作成ページ: %s", ss)
        except Exception as e:
            logger.error("[note] 記事作成ページ遷移失敗: %s", e)
            browser.close()
            return False

        # ── Step 3: タイトル入力 ──────────────────────────────────
        title_selectors = [
            'input[data-placeholder="記事タイトル"]',
            'input[placeholder*="タイトル"]',
            'textarea[placeholder*="タイトル"]',
            '[contenteditable="true"][data-placeholder*="タイトル"]',
            '[contenteditable="true"][aria-label*="タイトル"]',
            '.title-input',
            'h1[contenteditable="true"]',
        ]
        title_filled = False
        for sel in title_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=2000):
                    loc.click()
                    time.sleep(0.3)
                    loc.fill(title)
                    title_filled = True
                    logger.info("[note] タイトル入力: %r (selector=%s)", title[:30], sel)
                    break
            except Exception:
                continue

        if not title_filled:
            logger.warning("[note] タイトル入力フィールドが見つかりません（スキップ）")

        time.sleep(1)

        # ── Step 4: 本文入力（クリップボード経由）────────────────
        #
        # note.com は ProseMirror ベースの WYSIWYG エディタ。
        # fill() で流すと Markdown 記法が平文になるため、
        # クリップボード経由で Ctrl+V ペーストを行う。
        # ただし note.com は Markdown をリッチテキストとして解釈しないため、
        # 本文は Markdown 記法のままテキストとして保存される。
        # （note.com 側で有料プラン向けに Markdown モードがあるが、
        #   無料プランは WYSIWYG のみ。テキストとして保存で OK。）
        #
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
                # タイトル入力欄でないものを選ぶ
                for i in range(min(locs.count(), 5)):
                    loc = locs.nth(i)
                    if not loc.is_visible(timeout=1000):
                        continue
                    placeholder = loc.get_attribute("data-placeholder") or ""
                    aria = loc.get_attribute("aria-label") or ""
                    if "タイトル" in placeholder or "タイトル" in aria:
                        continue

                    loc.click()
                    time.sleep(0.5)
                    page.keyboard.press("Control+End")  # 末尾へ移動
                    time.sleep(0.3)

                    # クリップボード経由でペースト
                    try:
                        _clipboard_paste(page, body)
                        body_filled = True
                        logger.info("[note] 本文ペースト完了 (%d 文字, selector=%s)", len(body), sel)
                        break
                    except Exception as ce:
                        logger.warning("[note] clipboard_paste 失敗 (%s): %s — type() にフォールバック", sel, ce)
                        # フォールバック: type() で1文字ずつ（遅いが確実）
                        # 長文は切り詰める
                        truncated = body[:3000] + "\n\n[以下省略 — 全文はファイルを参照]" if len(body) > 3000 else body
                        page.keyboard.type(truncated, delay=0)
                        body_filled = True
                        logger.info("[note] 本文 type() 完了")
                        break

                if body_filled:
                    break
            except Exception as ex:
                logger.debug("[note] body selector '%s' エラー: %s", sel, ex)
                continue

        if not body_filled:
            logger.warning("[note] 本文入力フィールドが見つかりません（スキップ）")

        ss = _DEBUG_DIR / "note_body_entered.png"
        page.screenshot(path=str(ss))
        logger.info("[note] 本文入力後スクリーンショット: %s", ss)
        time.sleep(1)

        # ── Step 5: タグ設定（任意） ──────────────────────────────
        if tags:
            tag_selectors = [
                'input[placeholder*="タグ"]',
                'input[data-placeholder*="タグ"]',
                '[class*="tag"] input',
                'input[name*="tag"]',
            ]
            for tag in tags[:5]:
                added = False
                for sel in tag_selectors:
                    try:
                        loc = page.locator(sel).first
                        if loc.count() > 0 and loc.is_visible(timeout=2000):
                            loc.click()
                            loc.fill(tag)
                            page.keyboard.press("Enter")
                            time.sleep(0.5)
                            logger.info("[note] タグ追加: %s", tag)
                            added = True
                            break
                    except Exception:
                        continue
                if not added:
                    logger.debug("[note] タグ追加スキップ: %s", tag)

        # ── Step 6: 下書き保存 ────────────────────────────────────
        time.sleep(1)
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
                    time.sleep(2)
                    logger.info("[note] 下書き保存ボタンクリック: %s", sel)
                    saved = True
                    break
            except Exception as ex:
                logger.debug("[note] セレクタ '%s' → %s", sel, ex)

        if not saved:
            # Ctrl+S フォールバック
            try:
                page.keyboard.press("Control+s")
                time.sleep(2)
                logger.info("[note] Ctrl+S 下書き保存")
                saved = True
            except Exception as e:
                logger.warning("[note] Ctrl+S 失敗: %s", e)

        ss_final = _DEBUG_DIR / "note_draft_saved.png"
        page.screenshot(path=str(ss_final), full_page=True)
        logger.info("[note] 最終スクリーンショット: %s", ss_final)

        if not saved:
            _notify_discord("note 下書き保存", "保存ボタンが見つかりません", ss_final)
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
