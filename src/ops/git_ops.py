"""
Git 自動操作モジュール

週次・月次でコードと DB スナップショットを GitHub にプッシュする。

セキュリティ注意:
  ・.env ファイルや API キーを含むファイルは .gitignore で除外すること。
  ・data/umalogi.db は大きい場合は Git LFS または除外推奨。
  ・このモジュールは「コードと設定」のみコミット対象とする。

環境変数:
  GIT_REMOTE_URL  : プッシュ先リモート URL (省略時は既存 origin を使用)
  GIT_BRANCH      : プッシュ先ブランチ (デフォルト: master)
  GIT_USER_NAME   : コミット用 user.name
  GIT_USER_EMAIL  : コミット用 user.email
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_BRANCH = os.environ.get("GIT_BRANCH", "master")


def _run(cmd: list[str], *, cwd: Path = _ROOT) -> tuple[int, str, str]:
    """サブプロセスを実行して (returncode, stdout, stderr) を返す。

    Args:
        cmd: 実行するコマンドとその引数のリスト。
        cwd: コマンドを実行するカレントディレクトリ。省略時はプロジェクトルート。

    Returns:
        (returncode, stdout 文字列, stderr 文字列) のタプル。
    """
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def _configure_git() -> None:
    """必要に応じて git user.name / user.email を設定する。

    GIT_USER_NAME / GIT_USER_EMAIL 環境変数が設定されている場合に限り
    ローカルの git config を更新する。未設定時は何もしない。
    """
    name = os.environ.get("GIT_USER_NAME")
    email = os.environ.get("GIT_USER_EMAIL")
    if name:
        _run(["git", "config", "user.name", name])
    if email:
        _run(["git", "config", "user.email", email])


def status() -> str:
    """git status の短縮出力を返す。

    Returns:
        `git status --short` の標準出力文字列。変更なしの場合は空文字列。
    """
    _, out, _ = _run(["git", "status", "--short"])
    return out


def has_changes() -> bool:
    """コミットすべき変更があるかどうかを返す。

    Returns:
        ステージング・未ステージングを問わず変更が存在する場合 True。
    """
    return bool(status())


def commit_and_push(
    message: str | None = None,
    *,
    add_patterns: list[str] | None = None,
    push: bool = True,
) -> bool:
    """
    変更をステージング → コミット → プッシュする。

    Args:
        message:      コミットメッセージ。None なら自動生成。
        add_patterns: git add するパターンリスト。None なら ["src/", "tests/", "scripts/"]
        push:         False にするとコミットのみでプッシュしない。

    Returns:
        True = 成功、False = 失敗またはスキップ
    """
    _configure_git()

    # ステージング対象（機密ファイルを除くコードのみ）
    patterns = add_patterns or [
        "src/",
        "tests/",
        "scripts/",
        "requirements.txt",
        "CLAUDE.md",
    ]
    for pattern in patterns:
        rc, _, err = _run(["git", "add", pattern])
        if rc != 0:
            logger.debug("git add %s: %s", pattern, err)

    if not has_changes():
        logger.info("git: コミットすべき変更なし")
        return True

    msg = message or (
        f"auto: weekly sync {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]"
    )
    rc, out, err = _run(["git", "commit", "-m", msg])
    if rc != 0:
        logger.error("git commit 失敗: %s", err)
        return False
    logger.info("git commit: %s", out.splitlines()[0] if out else "ok")

    if not push:
        return True

    rc, out, err = _run(["git", "push", "origin", _BRANCH])
    if rc != 0:
        logger.error("git push 失敗: %s", err)
        return False
    logger.info("git push 成功: %s", out or "ok")
    return True


def auto_tag(tag_prefix: str = "weekly") -> str | None:
    """
    現在日付でタグを作成する。

    Returns:
        作成したタグ名 (例: "weekly-2024-01-07") または None
    """
    tag = f"{tag_prefix}-{datetime.now().strftime('%Y-%m-%d')}"
    rc, _, err = _run(["git", "tag", tag])
    if rc != 0:
        logger.warning("タグ作成失敗 %s: %s", tag, err)
        return None
    rc, _, err = _run(["git", "push", "origin", tag])
    if rc != 0:
        logger.warning("タグプッシュ失敗 %s: %s", tag, err)
        return None
    logger.info("タグ作成・プッシュ: %s", tag)
    return tag


def weekly_auto_commit() -> bool:
    """週次の自動コミット・プッシュ。scheduler.py から呼び出す。

    コミットメッセージに `[skip ci]` を付与して CI を抑制する。
    コミット成功時に日付タグも自動生成する。

    Returns:
        True = コミット・プッシュ成功、False = 失敗またはスキップ。
    """
    msg = f"chore: weekly auto-commit {datetime.now().strftime('%Y-%m-%d')} [skip ci]"
    success = commit_and_push(message=msg)
    if success:
        auto_tag()
    return success
