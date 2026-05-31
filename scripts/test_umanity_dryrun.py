"""
scripts/test_umanity_dryrun.py — ウマニティ DOM 動作確認ドライラン

Playwright でウマニティにログインし、予想コロシアムの DOM 構造を確認する。
実際には投稿せず、各ステップのスクリーンショットを outputs/debug/ に保存する。

Steps:
  1. ウマニティにログイン
  2. 予想ページ（race_5.php または race.php）に遷移
  3. 「予想する」ボタンを探す
  4. 予想フォームにダミーの馬番を入力
  5. 最終スクリーンショットを outputs/debug/umanity_test.png に保存
  6. エラー発生時は Discord に手動介入要請を送信

Usage:
    py scripts/test_umanity_dryrun.py
    py scripts/test_umanity_dryrun.py --no-headless
    py scripts/test_umanity_dryrun.py --race-code 2026050108
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("umanity_dryrun")

_OUT_DIR = _ROOT / "outputs" / "debug"
_OUT_DIR.mkdir(parents=True, exist_ok=True)

_LOGIN_URL = "https://umanity.jp/"
_RACE_LIST_URL = "https://umanity.jp/racedata/race.php"
_DUMMY_HORSE = 3  # DOM テスト用ダミー馬番


# ── 認証情報ロード ─────────────────────────────────────────────────────


def _load_credentials() -> tuple[str, str]:
    try:
        from dotenv import load_dotenv

        load_dotenv(_ROOT / ".env", override=False)
    except ImportError:
        pass
    email = os.environ.get("UMANITY_EMAIL", "")
    password = os.environ.get("UMANITY_PASSWORD", "")
    if not email or not password:
        raise EnvironmentError(
            "UMANITY_EMAIL / UMANITY_PASSWORD が設定されていません。.env を確認してください。"
        )
    return email, password


# ── Discord エラー通知 ─────────────────────────────────────────────────


def _notify_discord(step: str, error: str, screenshot_path: Path | None = None) -> None:
    try:
        from src.notification.discord_notifier import DiscordNotifier

        notifier = DiscordNotifier()
        notifier.notify_intervention_required(
            step=step,
            error=error,
            action=f"ウマニティのDOM構造を手動確認してください: {_LOGIN_URL}",
            screenshot_path=screenshot_path,
        )
    except Exception as e:
        logger.warning("Discord 通知失敗（通知システム自体のエラー）: %s", e)


# ── ドライラン本体 ─────────────────────────────────────────────────────


def run_dryrun(
    race_code: str | None = None,
    headless: bool = True,
) -> dict[str, object]:
    """
    ウマニティ DOM ドライランを実行する。

    Returns:
        {
            "login_success": bool,
            "navigation_success": bool,
            "form_found": bool,
            "horse_input_found": bool,
            "screenshot_path": str | None,
            "dom_links": list[dict],  # ページ内リンクのダンプ
            "errors": list[str],
        }
    """
    result: dict[str, object] = {
        "login_success": False,
        "navigation_success": False,
        "form_found": False,
        "horse_input_found": False,
        "screenshot_path": None,
        "dom_links": [],
        "errors": [],
    }

    email, password = _load_credentials()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        # ── Step 1: ログイン ──────────────────────────────────────────

        logger.info("[Step 1] ウマニティ トップページへアクセス: %s", _LOGIN_URL)
        try:
            page.goto(_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            ss1 = _OUT_DIR / f"{ts}_01_top.png"
            page.screenshot(path=str(ss1))
            logger.info("  📸 スクリーンショット: %s", ss1.name)

            # ログインリンク（右上）をクリックしてモーダルを開く
            page.click("text=ログイン", timeout=10000)
            time.sleep(1.0)

            page.wait_for_selector('input[name="userid"]', timeout=8000)
            page.fill('input[name="userid"]', email)
            page.fill('input[name="password"]', password)

            # JS 経由でフォーム送信（#blackmask がクリックをブロックするため）
            with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
                page.evaluate("""() => {
                    const submit = document.querySelector('input[name="submit"]');
                    const mode   = document.querySelector('input[name="mode"]');
                    if (submit && mode) {
                        mode.value = 'login';
                        submit.closest('form').submit();
                    }
                }""")

            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            time.sleep(2)

            content = page.content()
            logged_in = (
                "ログアウト" in content
                or "logout" in content.lower()
                or "mypage" in content.lower()
            )

            if logged_in:
                result["login_success"] = True
                logger.info("  ✅ ログイン成功")
            else:
                err = (
                    f"ログイン後のページに認証成功マーカーがありません (url={page.url})"
                )
                logger.error("  ❌ %s", err)
                ss_fail = _OUT_DIR / f"{ts}_01_login_fail.png"
                page.screenshot(path=str(ss_fail))
                result["errors"].append(err)  # type: ignore[attr-defined]
                _notify_discord("ウマニティ ログイン", err, ss_fail)
                browser.close()
                return result

            ss2 = _OUT_DIR / f"{ts}_02_after_login.png"
            page.screenshot(path=str(ss2))
            logger.info("  📸 スクリーンショット: %s", ss2.name)

        except PWTimeout as e:
            err = f"ログイン中にタイムアウト: {e}"
            logger.error("  ❌ %s", err)
            ss_err = _OUT_DIR / f"{ts}_01_timeout.png"
            page.screenshot(path=str(ss_err))
            result["errors"].append(err)  # type: ignore[attr-defined]
            _notify_discord("ウマニティ ログイン（タイムアウト）", err, ss_err)
            browser.close()
            return result
        except Exception as e:
            err = f"ログイン中に例外: {type(e).__name__}: {e}"
            logger.error("  ❌ %s", err)
            ss_err = _OUT_DIR / f"{ts}_01_exception.png"
            try:
                page.screenshot(path=str(ss_err))
            except Exception:
                pass
            result["errors"].append(err)  # type: ignore[attr-defined]
            _notify_discord("ウマニティ ログイン（例外）", err, ss_err)
            browser.close()
            return result

        # ── Step 2: レース予想ページへ遷移 ───────────────────────────

        target_url = (
            f"https://umanity.jp/racedata/race_5.php?code={race_code}"
            if race_code
            else _RACE_LIST_URL
        )
        logger.info("[Step 2] 予想ページへ遷移: %s", target_url)

        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            ss3 = _OUT_DIR / f"{ts}_03_race_page.png"
            page.screenshot(path=str(ss3))
            logger.info("  📸 スクリーンショット: %s", ss3.name)
            result["navigation_success"] = True
            logger.info("  ✅ ページ到達: %s", page.url)

            # ページ内のリンクをダンプ（DOM 分析用）
            links: list[dict[str, str]] = page.evaluate("""
                () => Array.from(document.querySelectorAll('a'))
                     .map(a => ({text: a.textContent.trim(), href: a.href}))
                     .filter(a => a.text.length > 0 && a.text.length < 40)
                     .slice(0, 50)
            """)
            result["dom_links"] = links
            yosou_links = [
                l for l in links if "予想" in l["text"] or "yosou" in l["href"]
            ]
            logger.info("  予想関連リンク: %s", yosou_links[:10])

        except Exception as e:
            err = f"予想ページ遷移失敗: {type(e).__name__}: {e}"
            logger.error("  ❌ %s", err)
            ss_err = _OUT_DIR / f"{ts}_03_nav_error.png"
            try:
                page.screenshot(path=str(ss_err))
            except Exception:
                pass
            result["errors"].append(err)  # type: ignore[attr-defined]
            _notify_discord("ウマニティ 予想ページ遷移", err, ss_err)
            browser.close()
            return result

        # ── Step 3: 「予想する」ボタンを探す ──────────────────────────

        logger.info("[Step 3] 「予想する」ボタンを探す")

        predict_selectors = [
            'a:has-text("予想する")',
            'button:has-text("予想する")',
            'input[value="予想する"]',
            ".predict_btn",
            'a[href*="yosou"]',
            '[class*="yosou"]',
            'a:has-text("予想を投稿")',
            'a:has-text("登録する")',
        ]

        form_found = False
        clicked_selector = ""
        for sel in predict_selectors:
            try:
                count = page.locator(sel).count()
                if count > 0:
                    logger.info(
                        "  ✅ 予想ボタン発見: selector='%s' count=%d", sel, count
                    )
                    form_found = True
                    clicked_selector = sel
                    page.locator(sel).first.click(timeout=5000)
                    time.sleep(1.5)

                    ss4 = _OUT_DIR / f"{ts}_04_form_open.png"
                    page.screenshot(path=str(ss4))
                    logger.info("  📸 スクリーンショット: %s", ss4.name)
                    break
            except Exception as ex:
                logger.debug("  セレクタ '%s' → 失敗: %s", sel, ex)

        if not form_found:
            logger.warning(
                "  ⚠️ 「予想する」ボタンが見つかりません （平日・レース未開催の可能性が高い）"
            )
            # フォーム要素を収集して DOM 構造を記録する
            all_inputs: list[dict[str, str]] = page.evaluate("""
                () => Array.from(document.querySelectorAll('input, button, select, textarea'))
                     .map(el => ({
                         tag: el.tagName, type: el.type || '',
                         name: el.name || '', id: el.id || '',
                         value: (el.value || '').slice(0, 30),
                         class_: el.className.slice(0, 50)
                     }))
                     .slice(0, 30)
            """)
            logger.info("  フォーム要素ダンプ: %s", all_inputs)

        result["form_found"] = form_found

        # ── Step 4: 馬番入力テスト ────────────────────────────────────

        horse_input_found = False
        if form_found:
            logger.info("[Step 4] ダミー馬番 %d の入力テスト", _DUMMY_HORSE)

            horse_selectors = [
                f'input[type="checkbox"][value="{_DUMMY_HORSE}"]',
                f'input[name="horse_{_DUMMY_HORSE}"]',
                f'[data-horse="{_DUMMY_HORSE}"]',
                f'label:has-text("{_DUMMY_HORSE}番") input',
                f'input[value="{_DUMMY_HORSE}"]',
            ]

            for sel in horse_selectors:
                try:
                    if page.locator(sel).count() > 0:
                        page.locator(sel).first.check()
                        logger.info(
                            "  ✅ 馬番 %d の入力成功: selector='%s'", _DUMMY_HORSE, sel
                        )
                        horse_input_found = True
                        break
                except Exception as ex:
                    logger.debug("  馬番セレクタ '%s' → 失敗: %s", sel, ex)

            if not horse_input_found:
                logger.warning(
                    "  ⚠️ 馬番入力フィールドが見つかりません（セレクタ要修正）"
                )

            result["horse_input_found"] = horse_input_found
        else:
            logger.info("[Step 4] フォーム未発見のためスキップ")

        # ── Step 5: 最終スクリーンショット ───────────────────────────

        final_ss = _OUT_DIR / "umanity_test.png"
        page.screenshot(path=str(final_ss), full_page=True)
        result["screenshot_path"] = str(final_ss)
        logger.info(
            "[Step 5] ✅ 最終スクリーンショット保存: outputs/debug/umanity_test.png"
        )

        browser.close()

    return result


# ── エントリポイント ──────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ウマニティ DOM 動作確認ドライラン")
    p.add_argument("--no-headless", action="store_true", help="ブラウザ画面を表示する")
    p.add_argument("--race-code", help="テスト対象レースコード (例: 2026050108)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logger.info("=" * 60)
    logger.info("ウマニティ DOM ドライラン開始")
    logger.info(
        "headless=%s  race_code=%s",
        not args.no_headless,
        args.race_code or "（なし → race.php）",
    )
    logger.info("=" * 60)

    try:
        result = run_dryrun(
            race_code=args.race_code,
            headless=not args.no_headless,
        )
    except EnvironmentError as e:
        logger.error("環境変数エラー: %s", e)
        sys.exit(1)

    logger.info("")
    logger.info("=" * 60)
    logger.info("ドライラン完了サマリー")
    logger.info(
        "  ① ログイン成功:         %s", "✅" if result["login_success"] else "❌ FAILED"
    )
    logger.info(
        "  ② ページ遷移成功:       %s",
        "✅" if result["navigation_success"] else "❌ FAILED",
    )
    logger.info(
        "  ③ 予想ボタン発見:       %s",
        "✅" if result["form_found"] else "⚠️  平日のためレース未開催（週末に再テスト）",
    )
    logger.info(
        "  ④ 馬番入力フィールド:   %s",
        "✅" if result["horse_input_found"] else "—（フォーム未発見のためスキップ）",
    )
    logger.info("  ⑤ スクリーンショット:   %s", result.get("screenshot_path") or "なし")
    if result["errors"]:
        logger.error("  エラー:")
        for e in result["errors"]:  # type: ignore[union-attr]
            logger.error("    - %s", e)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
