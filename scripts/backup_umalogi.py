"""
scripts/backup_umalogi.py — UMALOGI 全資産の安全退避バックアップ

マックスプラン終了・開発一時停止に備え、コード（src/scripts/docs/tests 等）と
実弾データ（data/ 配下の .db）を 1 つの ZIP に固めて backups/ へ退避する。

退避方針:
  - 含める: src/ scripts/ docs/ tests/ web_streamlit/ + ルート直下の主要ファイル、
           data/ 配下の .db（実弾データ本体）。
  - 除外:  .db-wal / .db-shm（稼働中プロセスが書き換える揮発ファイル）、
           .venv/ 等の仮想環境、__pycache__・.git・node_modules・.next 等の一時/再生成物、
           backups/ 自身（自己参照防止）。

Usage:
    py scripts/backup_umalogi.py
    py scripts/backup_umalogi.py --output-dir D:/umalogi_archive   # 退避先を変更
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from datetime import datetime
from pathlib import Path

if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

# プロジェクトルート（このファイルの 1 つ上の親）
_ROOT: Path = Path(__file__).resolve().parents[1]

# バックアップ対象に含めるトップレベルのソースディレクトリ。
_INCLUDE_DIRS: tuple[str, ...] = (
    "src",
    "scripts",
    "docs",
    "tests",
    "web_streamlit",
    ".claude",
)

# バックアップ対象に含めるルート直下の個別ファイル。
_INCLUDE_FILES: tuple[str, ...] = (
    "CLAUDE.md",
    "VERSION",
    "README.md",
    "requirements.txt",
    "pyproject.toml",
    "pytest.ini",
    "ruff.toml",
    ".gitignore",
)

# 走査から除外するディレクトリ名（一時・再生成物・仮想環境）。
_EXCLUDE_DIR_NAMES: frozenset[str] = frozenset(
    {
        ".venv",
        "venv",
        "env",
        "__pycache__",
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "node_modules",
        ".next",
        "backups",  # 自己参照防止
        "htmlcov",
    }
)

# 除外するファイル拡張子（揮発・大容量・再生成物）。
_EXCLUDE_SUFFIXES: frozenset[str] = frozenset(
    {".db-wal", ".db-shm", ".pyc", ".pyo", ".log"}
)


def _is_excluded(path: Path) -> bool:
    """除外対象（除外ディレクトリ配下 or 除外拡張子）なら True。"""
    if any(part in _EXCLUDE_DIR_NAMES for part in path.parts):
        return True
    # .db-wal / .db-shm は二重拡張子なので name で判定する。
    name = path.name
    return any(name.endswith(suffix) for suffix in _EXCLUDE_SUFFIXES)


def _iter_files(base: Path) -> "list[Path]":
    """base 配下の全ファイルを再帰列挙（除外フィルタ適用済み）。"""
    files: list[Path] = []
    if not base.exists():
        return files
    for p in base.rglob("*"):
        if p.is_file() and not _is_excluded(p):
            files.append(p)
    return files


def collect_targets(root: Path) -> "list[Path]":
    """バックアップに含める全ファイルの絶対パスを収集する。

    - _INCLUDE_DIRS 配下の全ファイル（除外フィルタ適用）
    - _INCLUDE_FILES のうち存在するもの
    - data/ 配下の .db ファイルのみ（.db-wal / .db-shm は除外フィルタで落ちる）
    """
    targets: list[Path] = []

    for d in _INCLUDE_DIRS:
        targets.extend(_iter_files(root / d))

    for f in _INCLUDE_FILES:
        fp = root / f
        if fp.is_file():
            targets.append(fp)

    # data/ は .db 本体のみを退避（巨大な processed/models 等は含めない）。
    data_dir = root / "data"
    if data_dir.exists():
        for db in data_dir.glob("*.db"):
            if db.is_file() and not _is_excluded(db):
                targets.append(db)

    # 重複排除しつつ順序を安定化。
    return sorted(set(targets))


def create_backup(root: Path, output_dir: Path) -> Path:
    """ZIP バックアップを作成し、生成された ZIP のパスを返す。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = output_dir / f"umalogi_backup_{timestamp}.zip"

    targets = collect_targets(root)
    print(f"バックアップ対象: {len(targets)} ファイル")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in targets:
            # ルートからの相対パスを ZIP 内アーカイブ名にする。
            arcname = path.relative_to(root).as_posix()
            zf.write(path, arcname)

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"✅ バックアップ生成完了: {zip_path}  ({size_mb:.1f} MB)")
    return zip_path


def main() -> int:
    parser = argparse.ArgumentParser(description="UMALOGI 全資産のバックアップを作成する。")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_ROOT / "backups",
        help="ZIP の出力先（既定: <project_root>/backups）。",
    )
    args = parser.parse_args()
    create_backup(_ROOT, args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
