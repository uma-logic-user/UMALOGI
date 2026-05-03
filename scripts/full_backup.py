"""
UMALOGI 完全バックアップスクリプト

ソースコード一式 + umalogi.db を ZIP 圧縮して data/backups/ に保存する。
スクレイピング完了後・モデル更新後・大規模修正前に実行すること。

バックアップ内容:
  1. umalogi_YYYYMMDD_HHMMSS.db   --- DB ホットバックアップ (WAL 安全)
  2. umalogi_src_YYYYMMDD_HHMMSS.zip --- ソースコード + 設定ファイル一式
  3. umalogi_models_YYYYMMDD_HHMMSS.zip --- 訓練済みモデル (.pkl) 一式

除外対象 (ZIP 対象外):
  - data/umalogi.db, data/backups/ (DB は別途ホットバックアップ)
  - data/models/history/ (モデル履歴は容量が大きいため最新のみ)
  - .git/, node_modules/, __pycache__/, .venv/, *.pyc, *.log, *.png

保持世代:
  ローカル backups/ 内の zip ファイルは最大 MAX_SRC_GEN=3 世代を保持し、
  古いものを自動削除する。DB は既存の src.ops.backup が管理 (5 世代)。

使用例:
    py scripts/full_backup.py                  # DB + src + models を全バックアップ
    py scripts/full_backup.py --src-only       # ソースコードのみ
    py scripts/full_backup.py --models-only    # モデルのみ
    py scripts/full_backup.py --no-cloud       # クラウド同期スキップ
    py scripts/full_backup.py --dry-run        # 対象ファイルの確認のみ
"""

from __future__ import annotations

import argparse
import logging
import sys
import zipfile
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

_BACKUP_DIR   = _ROOT / "data" / "backups"
_MODELS_DIR   = _ROOT / "data" / "models"
_MAX_SRC_GEN  = 3   # ソース ZIP の最大保持世代数
_MAX_MDL_GEN  = 3   # モデル ZIP の最大保持世代数

# ── ZIP 除外パターン ──────────────────────────────────────────────────────
_EXCLUDE_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    ".mypy_cache", ".ruff_cache", ".pytest_cache",
    "backups", "raw", "processed",          # data/ 内の大容量ディレクトリ
    "history",                              # data/models/history/
    ".next", "out", "dist",                 # Next.js ビルド成果物
}

_EXCLUDE_EXTS = {
    ".pyc", ".pyo", ".log", ".png", ".jpg", ".jpeg",
    ".db", ".db-shm", ".db-wal",            # DB 本体は除外 (別途ホットバックアップ)
    ".pkl",                                 # モデルは別 ZIP
    ".tmp", ".bak",
}

_EXCLUDE_NAMES = {
    "nul", "nul`", "powershell",            # 誤作成ファイル
    ".env",                                 # 秘密情報は除外
}


def _should_exclude(path: Path, root: Path) -> bool:
    """バックアップ対象外なら True を返す。"""
    # 親ディレクトリにブラックリストが含まれる場合は除外
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    for part in relative.parts[:-1]:
        if part in _EXCLUDE_DIRS:
            return True
    # ファイル名チェック
    if path.is_dir() and path.name in _EXCLUDE_DIRS:
        return True
    if path.suffix.lower() in _EXCLUDE_EXTS:
        return True
    if path.name.lower() in _EXCLUDE_NAMES:
        return True
    return False


# ── ソースコード ZIP ──────────────────────────────────────────────────────

def backup_src(timestamp: str, dry_run: bool = False) -> Path:
    """
    プロジェクトルート以下のソースコードを ZIP 圧縮して保存する。

    Returns:
        作成した ZIP ファイルのパス
    """
    zip_path = _BACKUP_DIR / f"umalogi_src_{timestamp}.zip"
    logger.info("ソースコード ZIP 開始: %s", zip_path.name)

    targets: list[Path] = []
    for p in sorted(_ROOT.rglob("*")):
        if p.is_file() and not _should_exclude(p, _ROOT):
            targets.append(p)

    total_bytes = sum(p.stat().st_size for p in targets)
    logger.info("  対象ファイル: %d 件 / %.1f MB", len(targets), total_bytes / 1_048_576)

    if dry_run:
        print(f"[DRY-RUN] ZIP 対象: {len(targets)} ファイル / {total_bytes/1_048_576:.1f} MB")
        for p in targets[:20]:
            print(f"  {p.relative_to(_ROOT)}")
        if len(targets) > 20:
            print(f"  ... 他 {len(targets)-20} ファイル")
        return zip_path

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in targets:
            arcname = p.relative_to(_ROOT)
            zf.write(p, arcname)

    size_mb = zip_path.stat().st_size / 1_048_576
    logger.info("  ソース ZIP 完了: %s (%.1f MB → %.1f MB 圧縮後)", zip_path.name, total_bytes / 1_048_576, size_mb)
    return zip_path


# ── モデル ZIP ───────────────────────────────────────────────────────────

def backup_models(timestamp: str, dry_run: bool = False) -> Path | None:
    """
    data/models/*.pkl を ZIP 圧縮して保存する。history/ は除外。

    Returns:
        作成した ZIP ファイルのパス。モデルが存在しない場合は None。
    """
    pkl_files = sorted(_MODELS_DIR.glob("*.pkl"))
    if not pkl_files:
        logger.info("  モデル (.pkl) が存在しないためスキップ")
        return None

    zip_path = _BACKUP_DIR / f"umalogi_models_{timestamp}.zip"
    total_bytes = sum(p.stat().st_size for p in pkl_files)
    logger.info("モデル ZIP 開始: %d ファイル / %.1f MB", len(pkl_files), total_bytes / 1_048_576)

    if dry_run:
        print(f"[DRY-RUN] モデル ZIP 対象: {len(pkl_files)} ファイル / {total_bytes/1_048_576:.1f} MB")
        for p in pkl_files:
            print(f"  {p.name}  ({p.stat().st_size/1_048_576:.1f} MB)")
        return zip_path

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        for p in pkl_files:
            zf.write(p, p.name)

    size_mb = zip_path.stat().st_size / 1_048_576
    logger.info("  モデル ZIP 完了: %s (%.1f MB)", zip_path.name, size_mb)
    return zip_path


# ── 世代ローテーション ────────────────────────────────────────────────────

def _rotate(pattern: str, max_gen: int) -> None:
    """glob パターンで一致する古いファイルを max_gen 世代以外削除する。"""
    files = sorted(_BACKUP_DIR.glob(pattern))
    if len(files) > max_gen:
        for old in files[:-max_gen]:
            old.unlink()
            logger.info("  古いバックアップ削除: %s", old.name)


# ── メイン ──────────────────────────────────────────────────────────────

def run_full_backup(
    *,
    src_only:    bool = False,
    models_only: bool = False,
    no_cloud:    bool = False,
    dry_run:     bool = False,
) -> dict[str, Path | None]:
    """
    完全バックアップを実行する。

    Returns:
        {"db": Path, "src": Path, "models": Path | None}
    """
    from src.ops.backup import backup_db  # noqa: PLC0415

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results: dict[str, Path | None] = {"db": None, "src": None, "models": None}

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    if not src_only and not models_only:
        # ── DB ホットバックアップ ───────────────────────────────────
        logger.info("=" * 60)
        logger.info("DB ホットバックアップ開始")
        if dry_run:
            print(f"[DRY-RUN] DB: {_ROOT / 'data' / 'umalogi.db'}")
            db_size = (_ROOT / "data" / "umalogi.db").stat().st_size / 1_048_576
            print(f"  サイズ: {db_size:.0f} MB → バックアップ後 同程度")
        else:
            from src.ops import backup as _bk_mod
            cloud = None if no_cloud else _bk_mod._AUTO
            results["db"] = backup_db(cloud_dir=cloud)
            logger.info("DB バックアップ完了: %s", results["db"].name)

    if not models_only:
        # ── ソースコード ZIP ─────────────────────────────────────────
        logger.info("=" * 60)
        results["src"] = backup_src(timestamp, dry_run=dry_run)
        if not dry_run:
            _rotate("umalogi_src_*.zip", _MAX_SRC_GEN)

    if not src_only:
        # ── モデル ZIP ───────────────────────────────────────────────
        logger.info("=" * 60)
        results["models"] = backup_models(timestamp, dry_run=dry_run)
        if not dry_run and results["models"]:
            _rotate("umalogi_models_*.zip", _MAX_MDL_GEN)

    return results


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="UMALOGI 完全バックアップ (DB + ソースコード + モデル)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  py scripts/full_backup.py                 # 完全バックアップ
  py scripts/full_backup.py --src-only      # ソースコードのみ
  py scripts/full_backup.py --models-only   # モデルのみ
  py scripts/full_backup.py --dry-run       # 対象確認のみ
  py scripts/full_backup.py --no-cloud      # クラウド同期スキップ
""",
    )
    parser.add_argument("--src-only",    action="store_true", help="ソースコードのみ ZIP")
    parser.add_argument("--models-only", action="store_true", help="モデルのみ ZIP")
    parser.add_argument("--no-cloud",    action="store_true", help="クラウド同期スキップ")
    parser.add_argument("--dry-run",     action="store_true", help="対象ファイル確認のみ (DB/ZIP 作成なし)")
    args = parser.parse_args()

    print("=" * 60)
    print("  UMALOGI 完全バックアップ")
    if args.dry_run:
        print("  [DRY-RUN モード: ファイルを作成しません]")
    print("=" * 60)

    results = run_full_backup(
        src_only=args.src_only,
        models_only=args.models_only,
        no_cloud=args.no_cloud,
        dry_run=args.dry_run,
    )

    print()
    print("=" * 60)
    print("  バックアップ結果")
    print("=" * 60)
    for key, path in results.items():
        if path is None:
            print(f"  {key:<10s}: スキップ")
        else:
            size = path.stat().st_size / 1_048_576 if path.exists() and not args.dry_run else 0
            print(f"  {key:<10s}: {path.name}  ({size:.1f} MB)")

    if not args.dry_run:
        backups = sorted(_BACKUP_DIR.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
        print(f"\n  backups/ 現在: {len(backups)} ファイル")
        for p in backups[:6]:
            size_mb = p.stat().st_size / 1_048_576
            print(f"    {p.name}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
