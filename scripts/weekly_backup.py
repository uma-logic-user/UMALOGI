# -*- coding: utf-8 -*-
"""
UMALOGI 週次バックアップスクリプト

毎週月曜 06:00 (scheduler.py から job_weekly_backup() 経由) または手動で実行する。
umalogi.db と重要なログファイルを ZIP 圧縮して data/backups/ に退避する。

保持世代: 直近 12 週（それ以前は自動削除）

Usage:
    py scripts/weekly_backup.py            # 通常実行
    py scripts/weekly_backup.py --dry-run  # ファイルリストのみ表示
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT     = Path(__file__).resolve().parents[1]
_BACKUP   = _ROOT / "data" / "backups"
_KEEP_MAX = 12   # 直近12世代を保持

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("weekly_backup")


def _collect_targets() -> list[Path]:
    """バックアップ対象ファイルを収集する。"""
    targets: list[Path] = []

    # ── DB ─────────────────────────────────────────────────────────────────
    db = _ROOT / "data" / "umalogi.db"
    if db.exists():
        targets.append(db)

    # ── ログファイル ────────────────────────────────────────────────────────
    for log_name in [
        "auto_runner.log", "scheduler.log", "nextjs.log",
        "self_healing.log", "tunnel.log",
    ]:
        p = _ROOT / "data" / log_name
        if p.exists():
            targets.append(p)

    # ── モデル ─────────────────────────────────────────────────────────────
    models_dir = _ROOT / "data" / "models"
    if models_dir.exists():
        for pkl in models_dir.glob("*.pkl"):
            targets.append(pkl)

    # ── 設定ファイル ────────────────────────────────────────────────────────
    for cfg in [_ROOT / "CLAUDE.md", _ROOT / ".env"]:
        if cfg.exists():
            targets.append(cfg)

    return targets


def _rotate_old_backups() -> None:
    """_KEEP_MAX 世代を超えた古い ZIP を削除する。"""
    zips = sorted(_BACKUP.glob("umalogi_backup_*.zip"), key=lambda p: p.stat().st_mtime)
    excess = len(zips) - _KEEP_MAX
    for old in zips[:excess]:
        old.unlink()
        logger.info("古いバックアップを削除: %s", old.name)


def run_backup(dry_run: bool = False) -> Path | None:
    """バックアップを実行して ZIP パスを返す。"""
    _BACKUP.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).astimezone()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    zip_path = _BACKUP / f"umalogi_backup_{stamp}.zip"

    targets = _collect_targets()
    if not targets:
        logger.warning("バックアップ対象ファイルなし")
        return None

    # DB の整合性チェック（WAL モードのチェックポイント）
    db = _ROOT / "data" / "umalogi.db"
    if db.exists() and not dry_run:
        try:
            conn = sqlite3.connect(str(db), timeout=10)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()
            logger.info("WAL チェックポイント完了")
        except Exception as e:
            logger.warning("WAL チェックポイント失敗 (続行): %s", e)

    total_bytes = sum(t.stat().st_size for t in targets)
    logger.info(
        "バックアップ開始: %d ファイル / %.1f MB → %s",
        len(targets), total_bytes / 1_048_576, zip_path.name,
    )

    if dry_run:
        for t in targets:
            logger.info("  [dry-run] %s (%.1f KB)", t.name, t.stat().st_size / 1024)
        return None

    # ZIP 圧縮
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for target in targets:
            # ZIP 内パスをリポジトリ相対にする
            try:
                arcname = target.relative_to(_ROOT)
            except ValueError:
                arcname = target.name
            zf.write(target, arcname)
            logger.info("  追加: %s", arcname)

    zip_size = zip_path.stat().st_size
    ratio = (1 - zip_size / max(total_bytes, 1)) * 100
    logger.info(
        "ZIP 作成完了: %.1f MB → %.1f MB (圧縮率 %.0f%%)",
        total_bytes / 1_048_576, zip_size / 1_048_576, ratio,
    )

    _rotate_old_backups()
    logger.info("バックアップ完了: %s", zip_path)
    return zip_path


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 週次バックアップ")
    parser.add_argument("--dry-run", action="store_true", help="実際には書き込まずファイルリストを表示")
    args = parser.parse_args()

    result = run_backup(dry_run=args.dry_run)
    if result:
        print(f"\n✅ バックアップ完了: {result}")
    elif not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
