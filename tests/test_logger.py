"""src/ops/logger.py のユニットテスト（ログ自動ローテーション設定）。

長期無人運用でログ肥大化を防ぐ日次ローテーション + 7日保持を検証する。
"""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from src.ops.logger import DEFAULT_BACKUP_DAYS, setup_logging


def _timed_handlers() -> list[TimedRotatingFileHandler]:
    root = logging.getLogger()
    return [h for h in root.handlers if isinstance(h, TimedRotatingFileHandler)]


def test_default_backup_days_is_seven() -> None:
    """既定の保持世代数は7日。"""
    assert DEFAULT_BACKUP_DAYS == 7


def test_setup_logging_uses_timed_rotation_7days(tmp_path: Path) -> None:
    """日次(midnight)ローテーション・backupCount=7 のハンドラーが設定される。"""
    setup_logging("t1", "t1.log", log_dir=tmp_path)
    handlers = _timed_handlers()
    assert handlers, "TimedRotatingFileHandler が設定されていない"
    h = handlers[0]
    assert h.when.upper() == "MIDNIGHT"
    assert h.backupCount == 7
    assert h.interval == 24 * 60 * 60  # midnight = 1日


def test_setup_logging_creates_logfile(tmp_path: Path) -> None:
    """ログ出力でファイルが生成される。"""
    logger = setup_logging("t2", "t2.log", log_dir=tmp_path)
    logger.info("hello")
    for h in logging.getLogger().handlers:
        h.flush()
    assert (tmp_path / "t2.log").exists()


def test_setup_logging_is_idempotent(tmp_path: Path) -> None:
    """二重呼び出しでハンドラーが重複しない（多重出力防止）。"""
    setup_logging("t3", "t3.log", log_dir=tmp_path)
    n_first = len(_timed_handlers())
    setup_logging("t3", "t3.log", log_dir=tmp_path)
    n_second = len(_timed_handlers())
    assert n_first == 1
    assert n_second == 1


def test_custom_backup_days(tmp_path: Path) -> None:
    """保持日数を変更できる。"""
    setup_logging("t4", "t4.log", log_dir=tmp_path, backup_days=3)
    assert _timed_handlers()[0].backupCount == 3
