"""
モデル学習パイプライン

責務:
  - train_pipeline(): DB 全データでモデルを学習・保存する
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def train_pipeline() -> None:
    """DB の全データでモデルを学習・保存する。

    学習前に data/backups/ へのバックアップを試みる（失敗しても学習は継続）。
    train_all() の結果は標準出力に JSON 形式で出力される。

    Raises:
        Exception: train_all() または init_db() が失敗した場合。
    """
    from src.database.init_db import init_db
    from src.ml.models import train_all

    try:
        from utils.backup import make_backup

        make_backup()
        logger.info("学習前バックアップ完了")
    except Exception as exc:
        logger.warning("バックアップ失敗（学習は継続）: %s", exc)

    conn = init_db()
    result = train_all(conn)
    conn.close()
    logger.info("学習結果: %s", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
