"""
DB バックアップユーティリティ

data/umalogi.db を data/backups/ へ5世代分ローテーションバックアップし、
クラウド同期フォルダ（環境変数 CLOUD_BACKUP_DIR）にも DB と最新モデル (.pkl) を
3世代分コピーする。

Usage:
    python -m src.ops.backup              # 即時バックアップ実行
    python -m src.ops.backup --list       # バックアップ一覧表示
    python -m src.ops.backup --no-cloud   # ローカルのみ（クラウド同期スキップ）

環境変数:
    CLOUD_BACKUP_DIR  クラウド同期先ディレクトリ
                      （デフォルト: G:/マイドライブ/UMALOGI_backup）
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT                  = Path(__file__).resolve().parents[2]
_DB_PATH               = _ROOT / "data" / "umalogi.db"
_BACKUP_DIR            = _ROOT / "data" / "backups"
_MODELS_DIR            = _ROOT / "data" / "models"
_MAX_GENERATIONS       = 5
_MAX_CLOUD_GENERATIONS = 3

# cloud_dir 引数のデフォルト値を「環境変数から取得」にするためのセンチネル
_AUTO = object()


def _resolve_cloud_dir() -> Path | None:
    """環境変数 CLOUD_BACKUP_DIR からクラウドバックアップ先を取得する。未設定はデフォルトを使用。"""
    raw = os.environ.get("CLOUD_BACKUP_DIR", r"G:\マイドライブ\UMALOGI_backup")
    return Path(raw) if raw else None


def _latest_model(models_dir: Path) -> Path | None:
    """data/models/ から更新日時が最も新しい .pkl ファイルを返す。"""
    pkls = sorted(models_dir.glob("*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return pkls[0] if pkls else None


def _cloud_sync(
    local_db_backup: Path,
    models_dir: Path,
    cloud_dir: Path,
    max_gen: int = _MAX_CLOUD_GENERATIONS,
) -> None:
    """
    クラウドフォルダへ DB バックアップと最新モデルをコピーし、古い世代をローテーションする。
    エラーは warning ログのみ（ローカルバックアップには影響させない）。
    """
    try:
        cloud_dir.mkdir(parents=True, exist_ok=True)

        # DB コピー
        cloud_db_path = cloud_dir / local_db_backup.name
        shutil.copy2(local_db_backup, cloud_db_path)
        size_mb = cloud_db_path.stat().st_size / 1024 / 1024
        logger.info("[クラウド] DB コピー完了: %s (%.1f MB)", cloud_db_path.name, size_mb)

        # DB のローテーション（古い世代を削除）
        existing_dbs = sorted(cloud_dir.glob("umalogi_*.db"))
        if len(existing_dbs) > max_gen:
            for old in existing_dbs[:-max_gen]:
                old.unlink()
                logger.info("[クラウド] 古い DB 削除: %s", old.name)

        # 最新モデル (.pkl) をコピー
        latest_pkl = _latest_model(models_dir)
        if latest_pkl:
            cloud_pkl = cloud_dir / latest_pkl.name
            shutil.copy2(latest_pkl, cloud_pkl)
            pkl_mb = cloud_pkl.stat().st_size / 1024 / 1024
            logger.info("[クラウド] モデルコピー完了: %s (%.1f MB)", cloud_pkl.name, pkl_mb)

            # モデルのローテーション
            existing_pkls = sorted(
                cloud_dir.glob("*.pkl"), key=lambda p: p.stat().st_mtime
            )
            if len(existing_pkls) > max_gen:
                for old_pkl in existing_pkls[:-max_gen]:
                    old_pkl.unlink()
                    logger.info("[クラウド] 古いモデル削除: %s", old_pkl.name)
        else:
            logger.info("[クラウド] コピー対象モデルなし（%s）", models_dir)

        remaining = list(cloud_dir.glob("umalogi_*.db"))
        logger.info("[クラウド] DB 世代数: %d / %d  保存先: %s", len(remaining), max_gen, cloud_dir)

    except Exception as exc:
        logger.warning("[クラウド] バックアップ失敗（ローカルは正常）: %s", exc)


def backup_db(
    db_path:    Path | None = None,
    backup_dir: Path | None = None,
    max_gen:    int = _MAX_GENERATIONS,
    cloud_dir:  object = _AUTO,
) -> Path:
    """
    SQLite DB を timestamped ファイル名でバックアップし、古い世代を削除する。
    クラウドフォルダが利用可能な場合は DB と最新モデルを自動的にクラウドへも同期する。

    Args:
        db_path:    バックアップ元 DB ファイルパス（デフォルト: data/umalogi.db）
        backup_dir: バックアップ保存先ディレクトリ（デフォルト: data/backups/）
        max_gen:    ローカルで保持する最大世代数（デフォルト: 5）
        cloud_dir:  クラウド同期先ディレクトリ。
                    デフォルト (_AUTO) は環境変数 CLOUD_BACKUP_DIR を使用。
                    None を渡すとクラウド同期をスキップする。

    Returns:
        作成したバックアップファイルのパス

    Raises:
        FileNotFoundError: db_path が存在しない場合
    """
    db_path    = db_path    or _DB_PATH
    backup_dir = backup_dir or _BACKUP_DIR

    if not db_path.exists():
        raise FileNotFoundError(f"DB ファイルが見つかりません: {db_path}")

    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"umalogi_{timestamp}.db"

    # SQLite のホットバックアップ（shutil.copy2 はファイルメタデータも保持）
    shutil.copy2(db_path, backup_path)
    size_mb = backup_path.stat().st_size / 1024 / 1024
    logger.info("バックアップ作成: %s (%.1f MB)", backup_path.name, size_mb)

    # 古い世代をローテーション削除
    existing = sorted(backup_dir.glob("umalogi_*.db"))
    if len(existing) > max_gen:
        to_delete = existing[:-max_gen]
        for old in to_delete:
            old.unlink()
            logger.info("古いバックアップ削除: %s", old.name)

    remaining = sorted(backup_dir.glob("umalogi_*.db"))
    logger.info("バックアップ世代数: %d / %d", len(remaining), max_gen)

    # クラウド同期
    resolved_cloud: Path | None = _resolve_cloud_dir() if cloud_dir is _AUTO else cloud_dir  # type: ignore[assignment]
    if resolved_cloud is not None:
        _cloud_sync(backup_path, _MODELS_DIR, resolved_cloud)

    return backup_path


def list_backups(backup_dir: Path | None = None) -> list[Path]:
    """バックアップファイルの一覧を返す（新しい順）。"""
    backup_dir = backup_dir or _BACKUP_DIR
    if not backup_dir.exists():
        return []
    return sorted(backup_dir.glob("umalogi_*.db"), reverse=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="DB バックアップユーティリティ")
    parser.add_argument("--list", action="store_true", help="バックアップ一覧を表示")
    parser.add_argument("--max-gen", type=int, default=_MAX_GENERATIONS,
                        help=f"保持する最大世代数（デフォルト: {_MAX_GENERATIONS}）")
    parser.add_argument("--no-cloud", action="store_true", help="クラウド同期をスキップ")
    args = parser.parse_args()

    if args.list:
        backups = list_backups()
        if not backups:
            print("バックアップが存在しません")
        else:
            print(f"バックアップ一覧 ({len(backups)} 件):")
            for p in backups:
                size_mb = p.stat().st_size / 1024 / 1024
                print(f"  {p.name}  ({size_mb:.1f} MB)")
        return

    backup_path = backup_db(
        max_gen=args.max_gen,
        cloud_dir=None if args.no_cloud else _AUTO,
    )
    print(f"バックアップ完了: {backup_path}")


if __name__ == "__main__":
    main()
