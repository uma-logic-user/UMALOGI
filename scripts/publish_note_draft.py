"""
scripts/publish_note_draft.py — Markdown ファイルを note.com に下書き保存する CLI

Usage:
    # 初回: 手動ログインしてセッションを保存（reCAPTCHA 対応）
    py scripts/publish_note_draft.py --login

    # 下書き保存
    py scripts/publish_note_draft.py --file outputs/note/20260503_R11_天皇賞(春).md
    py scripts/publish_note_draft.py --file outputs/note/20260503_R11_天皇賞(春).md --tags 競馬 UMALOGI AI予想

    # プロフィール更新
    py scripts/publish_note_draft.py --update-profile --visible

.env に以下を設定してから実行すること:
    NOTE_EMAIL=your@email.com
    NOTE_PASSWORD=yourpassword
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")


def _extract_title(content: str) -> str:
    """Markdown 先頭の # 見出しをタイトルとして返す。"""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line.lstrip("# ").strip()
    return "UMALOGI AI予想"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Markdown ファイルを note.com に下書き保存する"
    )
    parser.add_argument("--file", help="保存する Markdown ファイルパス")
    parser.add_argument(
        "--tags",
        nargs="*",
        default=["競馬", "UMALOGI", "AI予想", "複勝"],
        help="タグ（スペース区切り、最大5個）",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="ブラウザを表示して実行（デバッグ用、デフォルト: ヘッドレス）",
    )
    parser.add_argument(
        "--update-profile",
        action="store_true",
        help="下書き保存の前に「なおふみ | UMALOGI開発者」プロフィールへ更新する",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="可視ブラウザで手動ログイン → .note_session.json に保存（初回・セッション切れ時）",
    )
    args = parser.parse_args()

    if not args.file and not args.update_profile and not args.login:
        print(
            "[ERROR] --file / --update-profile / --login のいずれかを指定してください"
        )
        sys.exit(1)

    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=False)

    # ── セッション保存（初回ログイン）─────────────────────────────
    if args.login:
        from src.ops.note_draft_publisher import login_and_save_session

        print("[INFO] 手動ログインモードを開始します...")
        print("[INFO] ブラウザが開いたら note.com にログインしてください。")
        print("[INFO] reCAPTCHA を解決し「ログイン」ボタンを押すと自動で続行します。")
        print()
        ok_login = login_and_save_session()
        if ok_login:
            print("✅ セッション保存完了 (.note_session.json)")
            print("  以降は --login 不要で自動実行できます。")
        else:
            print("❌ セッション保存失敗（ログインが完了しませんでした）")
            sys.exit(1)
        print()
        if not args.file and not args.update_profile:
            return

    # ── プロフィール更新（オプション）──────────────────────────────
    if args.update_profile:
        from src.ops.note_draft_publisher import update_profile

        print("[INFO] プロフィール更新を開始します...")
        print("[INFO]   名前: なおふみ | UMALOGI開発者")
        print()
        ok_profile = update_profile(headless=not args.visible)
        if ok_profile:
            print("✅ プロフィール更新完了")
            print("  → スクリーンショット: outputs/debug/note_profile_saved.png")
        else:
            print("⚠️  プロフィール更新失敗（手動で確認してください）")
            print("  → outputs/debug/note_settings.png を確認")
        print()
        if not args.file:
            return

    # ── 下書き保存 ─────────────────────────────────────────────────
    md_path = Path(args.file)
    if not md_path.is_absolute():
        md_path = _ROOT / md_path
    if not md_path.exists():
        print(f"[ERROR] ファイルが見つかりません: {md_path}")
        sys.exit(1)

    content = md_path.read_text(encoding="utf-8")
    title = _extract_title(content)

    print(f"[INFO] タイトル : {title}")
    print(f"[INFO] 文字数   : {len(content):,} 文字")
    print(f"[INFO] タグ     : {args.tags}")
    print(f"[INFO] モード   : {'表示あり' if args.visible else 'ヘッドレス'}")
    print()

    from src.ops.note_draft_publisher import save_draft

    ok = save_draft(
        title=title,
        body=content,
        tags=args.tags[:5],
        headless=not args.visible,
    )

    if ok:
        print(f"\n✅ 下書き保存完了: {title[:50]}")
        print("  → note.com を開いて下書き一覧を確認してください")
        print("  → スクリーンショット: outputs/debug/note_draft_saved.png")
    else:
        print("\n❌ 下書き保存失敗")
        print("  → outputs/debug/ のスクリーンショットで原因を確認してください")
        sys.exit(1)


if __name__ == "__main__":
    main()
