"""
scripts/post_weekly_note_draft.py — 週次まとめ記事を note.com に下書き投稿する

1. セッションファイルがなければブラウザを開いて手動ログインを促す
2. 週次記事 Markdown を生成する
3. note.com に下書き保存する

Usage:
    py scripts/post_weekly_note_draft.py
    py scripts/post_weekly_note_draft.py --week-offset 1   # 先々週を振り返る
    py scripts/post_weekly_note_draft.py --login-only      # ログインのみ（セッション更新）
    py scripts/post_weekly_note_draft.py --no-headless     # ブラウザ画面を表示する
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("post_weekly_note")

_DB_PATH     = _ROOT / "data" / "umalogi.db"
_SESSION_FILE = _ROOT / ".note_session.json"


def _generate_article(week_offset: int) -> tuple[str, str]:
    """記事 (title, body) を生成して返す。"""
    from datetime import date
    from scripts.generate_weekly_note import generate_weekly_note

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        _is_premium = os.environ.get("IS_PREMIUM_NOTE", os.environ.get("NOTE_IS_PREMIUM", "")).lower() in ("1", "true")
        body = generate_weekly_note(conn, week_offset=week_offset, include_picks=True, is_premium=_is_premium)
    finally:
        conn.close()

    issue_date = date.today().strftime("%Y-%m-%d")
    title = f"🏇【UMALOGI週次レポート】{issue_date}号 — 全モデル成績公開＆今週のAI厳選予想"
    return title, body


def _should_publish_playwright() -> bool:
    """ENABLE_PLAYWRIGHT_POST 環境変数が '1' または 'true' なら True を返す。"""
    return os.environ.get("ENABLE_PLAYWRIGHT_POST", "").lower() in ("1", "true")


def _generate_x_post(title: str, body: str) -> str:
    """
    note記事 Markdown から X 告知ポストテキストを生成する。
    140文字以内に収め、固定ハッシュタグを末尾に付与する。
    """
    sub = ""
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            sub = stripped.lstrip("# ").strip()
            break

    hashtags = "#競馬 #AI予想 #UMALOGI #JRA"
    short_title = title[:40]
    base = f"🏇 {short_title}\n{sub}\n\nnoteで全モデル成績公開中📊\n\n{hashtags}"
    if len(base) > 140:
        trim_len = 140 - len(f"\n\nnoteで全モデル成績公開中📊\n\n{hashtags}") - 5
        base = f"🏇 {short_title[:trim_len]}\n\nnoteで全モデル成績公開中📊\n\n{hashtags}"
    return base


def main() -> None:
    p = argparse.ArgumentParser(description="週次まとめ記事を note.com に下書き投稿する")
    p.add_argument("--week-offset", type=int, default=1,
                   help="何週前を振り返るか（デフォルト1=先週）")
    p.add_argument("--login-only", action="store_true",
                   help="ログインのみ（下書き投稿はしない）")
    p.add_argument("--no-headless", action="store_true",
                   help="ブラウザ画面を表示する（デバッグ用）")
    args = p.parse_args()

    headless = not args.no_headless

    from src.ops.note_draft_publisher import login_and_save_session, save_draft

    # ── Step 1: セッション確認 / ログイン ──────────────────────────
    if not _SESSION_FILE.exists():
        print()
        print("=" * 60)
        print("  note.com セッションが見つかりません。")
        print("  ブラウザが開くので、ログインしてください。")
        print("  reCAPTCHA が出た場合は手動で解決してください。")
        print("  ログイン完了後、自動で下書き投稿に進みます。")
        print("=" * 60)
        print()
        ok = login_and_save_session()
        if not ok:
            logger.error("ログイン失敗。スクリプトを終了します。")
            sys.exit(1)
        logger.info("ログイン完了。セッション保存: %s", _SESSION_FILE)
    else:
        logger.info("既存セッションを使用: %s", _SESSION_FILE)

    if args.login_only:
        print("\n✅ ログイン完了（--login-only のため下書き投稿はスキップ）")
        return

    # ── Step 2: 記事生成 ────────────────────────────────────────────
    logger.info("週次記事を生成中 (week_offset=%d)...", args.week_offset)
    title, body = _generate_article(args.week_offset)
    logger.info("記事生成完了: %d 文字", len(body))
    print(f"\n  タイトル: {title}")
    print(f"  本文文字数: {len(body):,} 文字\n")

    # ── Step 3-A: Discord note_draft チャンネルへ転送 ──────────────
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)

    x_post = _generate_x_post(title, body)
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    discord_ok = router.send_note_draft(title, body, x_post=x_post)
    if discord_ok:
        logger.info("Discord note-draft 転送完了")
    else:
        logger.info("Discord note-draft 転送スキップ（DISCORD_WEBHOOK_NOTE_DRAFT 未設定）")

    # ── Step 3-B: Playwright 投稿（ENABLE_PLAYWRIGHT_POST トグル）────
    enable_pw = _should_publish_playwright()
    if not enable_pw:
        logger.info(
            "Playwright投稿: スキップ (ENABLE_PLAYWRIGHT_POST=%s)",
            os.environ.get("ENABLE_PLAYWRIGHT_POST", "未設定"),
        )
        print()
        print("=" * 60)
        print("  ✅ Discord 転送完了（Playwright投稿はスキップ）")
        print(f"  タイトル: {title}")
        print("  ENABLE_PLAYWRIGHT_POST=1 を設定すると note.com にも自動投稿します。")
        print("=" * 60)
        print()
        return

    logger.info("note.com に下書き保存中 (headless=%s, ENABLE_PLAYWRIGHT_POST=True)...", headless)
    ok = save_draft(
        title=title,
        body=body,
        tags=["競馬", "AI予想", "UMALOGI", "JRA", "期待値", "競馬AI"],
        headless=headless,
    )

    print()
    print("=" * 60)
    if ok:
        print("  ✅ note.com 下書き保存 完了!")
        print(f"  タイトル: {title}")
        print("  note.com の下書き一覧を確認してください。")
        print("  公開は手動で行ってください（自動公開は行いません）。")
    else:
        print("  ❌ note.com 下書き保存 失敗")
        print("  outputs/debug/ のスクリーンショットを確認してください。")
        print("  セッション期限切れの場合: --login-only で再ログインしてください。")
    print("=" * 60)
    print()

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
