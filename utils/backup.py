"""
DB バックアップユーティリティ

使用例:
  from utils.backup import make_backup
  make_backup()

  # CLI
  py -c "from utils.backup import make_backup; make_backup()"
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT       = Path(__file__).resolve().parents[1]
_DB_PATH    = _ROOT / "data" / "umalogi.db"
_BACKUP_DIR = _ROOT / "data" / "backups"
_KEEP       = 5  # 保持する最大件数


def make_backup(
    db_path: Path | None = None,
    backup_dir: Path | None = None,
    keep: int = _KEEP,
) -> Path:
    """
    umalogi.db をタイムスタンプ付きでバックアップする。

    Args:
        db_path:    バックアップ元 DB（デフォルト: data/umalogi.db）
        backup_dir: 保存先ディレクトリ（デフォルト: data/backups/）
        keep:       残す最大件数（古い順に削除。デフォルト: 5）

    Returns:
        作成したバックアップファイルの Path
    """
    db_path    = Path(db_path)    if db_path    else _DB_PATH
    backup_dir = Path(backup_dir) if backup_dir else _BACKUP_DIR

    if not db_path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {db_path}")

    backup_dir.mkdir(parents=True, exist_ok=True)

    ts       = datetime.now().strftime("%Y%m%d_%H%M")
    dst_name = f"umalogi_{ts}.db"
    dst      = backup_dir / dst_name

    shutil.copy2(db_path, dst)
    logger.info("バックアップ作成: %s", dst)

    _prune(backup_dir, keep)

    return dst


def _prune(backup_dir: Path, keep: int) -> None:
    """古いバックアップを削除して最新 keep 件のみ残す。"""
    files = sorted(
        backup_dir.glob("umalogi_*.db"),
        key=lambda p: p.stat().st_mtime,
    )
    to_delete = files[:-keep] if len(files) > keep else []
    for f in to_delete:
        f.unlink()
        logger.info("古いバックアップを削除: %s", f.name)
