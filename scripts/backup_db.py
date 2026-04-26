"""
DB バックアップ実行スクリプト

data/umalogi.db を data/backups/ フォルダに日付付きファイル名で保存し、
5世代ローテーションする。環境変数 CLOUD_BACKUP_DIR が設定されていれば
クラウドフォルダにも同期する。

Usage:
    py scripts/backup_db.py           # バックアップ実行
    py scripts/backup_db.py --list    # バックアップ一覧表示
    py scripts/backup_db.py --no-cloud
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.ops.backup import main

if __name__ == "__main__":
    main()
