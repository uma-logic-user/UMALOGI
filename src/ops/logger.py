"""
src/ops/logger.py — 本番常駐プロセス共通のログ設定（時刻ベース自動ローテーション）

長期無人運用でログファイルが肥大化しディスクを食い潰す事故を防ぐため、
日次ローテーション + 保持世代数制限（既定7日）を一元的に提供する。

設計:
  - `TimedRotatingFileHandler(when="midnight", interval=1, backupCount=7)` を使用。
    毎日0時にローテーションし、7世代（=直近7日分）を超えた古いログは自動削除される。
  - ルートロガーのハンドラーを設定するため、各モジュールの `logging.getLogger(__name__)` が
    すべて同一のローテーションファイル + コンソールへ集約される（従来の basicConfig と同等の挙動）。
  - 二重呼び出し時はハンドラーを張り替えて重複出力を防ぐ（冪等）。

Usage:
    from src.ops.logger import setup_logging
    logger = setup_logging("scheduler", "scheduler.log")
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

# ログ出力ディレクトリ（既定: <project_root>/data）。
_ROOT: Path = Path(__file__).resolve().parents[2]
_DEFAULT_LOG_DIR: Path = _ROOT / "data"

# 保持する世代数（日次ローテーションのため = 保持日数）。
DEFAULT_BACKUP_DAYS: int = 7

_DEFAULT_FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def _utf8_console_handler() -> logging.StreamHandler:
    """UTF-8 で stdout に書くコンソールハンドラー（Windows cp932 文字化け回避）。"""
    stream = open(
        sys.stdout.fileno(),
        mode="w",
        encoding="utf-8",
        errors="replace",
        closefd=False,
    )
    return logging.StreamHandler(stream)


def setup_logging(
    name: str,
    log_filename: str,
    *,
    level: int = logging.INFO,
    fmt: str = _DEFAULT_FMT,
    datefmt: str | None = None,
    backup_days: int = DEFAULT_BACKUP_DAYS,
    to_console: bool = True,
    log_dir: Path | None = None,
    use_rich: bool = False,
) -> logging.Logger:
    """ルートロガーに日次ローテーションのファイル + コンソール出力を設定する。

    ログファイルは毎日0時にローテーションし、`backup_days` 世代（=保持日数）を超えた
    古いファイルは自動削除される。これによりログ肥大化でディスクが枯渇する事故を防ぐ。

    Args:
        name:         返却する名前付きロガー（`logging.getLogger(name)`）。
        log_filename: ログファイル名（`log_dir` 配下に作成）。
        level:        ログレベル（既定 INFO）。
        fmt:          フォーマット文字列。
        datefmt:      日付フォーマット（省略時はデフォルト）。
        backup_days:  保持世代数（=保持日数。既定7。これより古い分は自動削除）。
        to_console:   True ならコンソールにも出力する。
        log_dir:      出力ディレクトリ（省略時 <project_root>/data）。
        use_rich:     True なら rich.RichHandler でコンソールをカラー装飾する
                      （rich 未導入時は従来の plain ハンドラーへ自動フォールバック。
                      ファイル出力のフォーマットは一切変わらない）。

    Returns:
        設定済みの名前付きロガー。
    """
    directory = log_dir or _DEFAULT_LOG_DIR
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / log_filename

    formatter = logging.Formatter(fmt, datefmt=datefmt)

    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        interval=1,
        backupCount=backup_days,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [file_handler]
    if to_console:
        console: logging.Handler | None = None
        if use_rich:
            try:
                from src.ui.console import create_rich_log_handler

                console = create_rich_log_handler(level=level)
            except Exception:  # noqa: BLE001 - 装飾失敗で本番を殺さない
                console = None
        if console is None:
            console = _utf8_console_handler()
            console.setFormatter(formatter)
        handlers.append(console)

    root = logging.getLogger()
    # 冪等性: 既存ハンドラーを除去してから張り替える（多重起動時の重複出力防止）。
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass
    root.setLevel(level)
    for h in handlers:
        root.addHandler(h)

    return logging.getLogger(name)
