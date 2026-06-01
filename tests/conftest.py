"""pytest 共通フィクスチャ — テスト環境の完全独立化。

目的:
    隔離環境（git worktree 等・実 DB や .env を持たない）でも全テストが GREEN に
    なるよう、以下を autouse フィクスチャで保証する。

    1. 実 DB 依存テストの土台:
       `sqlite3.connect("data/umalogi.db")` をハードコードするテスト
       （test_training_grade / test_odds_momentum 等）が参照するファイルを、
       存在しない場合のみ **完全スキーマを持つ空 DB** として用意する。
       → 実データではなく「テーブルが存在する」ことだけを保証（中立値テストが通る）。
       既に実 DB があれば一切触らない（本番データ保護）。

    2. .env 依存テストの土台:
       Discord webhook 系の環境変数が未設定の隔離環境で、ルーティング系テストが
       URL 未設定により誤って空送信になるのを防ぐため、未設定キーにのみ
       無害なダミー値を注入する（既存値があれば尊重）。

設計原則:
    - 非破壊: 既存の実 DB / 実 .env を上書きしない（あれば使う）。
    - 冪等: 何度実行しても安全。
    - 局所的: 環境変数注入は session スコープで一度きり、テスト挙動を変えない。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture(scope="session", autouse=True)
def _ensure_schema_db() -> None:
    """実 DB が無い隔離環境でのみ、完全スキーマの空 DB を用意する。

    `data/umalogi.db` を直接 connect するレガシーテストの土台。
    既存ファイル（実データ）があれば触らない。
    """
    db_path = _ROOT / "data" / "umalogi.db"
    if db_path.exists() and db_path.stat().st_size > 0:
        # 実 DB（または既存の空 DB）が存在 → 尊重して何もしない
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    # init_db() が全 DDL / マイグレーションを実行し、空でも全テーブルを生成する。
    import sqlite3

    from src.database.init_db import init_db

    conn = init_db(db_path)
    conn.close()

    # init_db() のフローに組み込まれていない独立マイグレーションを追加適用し、
    # 本番 DB と同じスキーマ（後付けカラム）を空 DB にも揃える。
    # training_grade: training_times への ALTER（手動実行前提の独立 migration）。
    from src.database.migrations.add_training_grade import migrate as _add_grade

    _add_grade(str(db_path))

    # WAL を使わず DELETE ジャーナルにする。テスト中に多数のテストが同じ空 DB へ
    # 書き込むと WAL が肥大し DB/WAL 不整合(DatabaseError)を起こすため、
    # 隔離環境のテスト用 DB はチェックポイント不要の DELETE モードで安定させる。
    jconn = sqlite3.connect(db_path)
    jconn.execute("PRAGMA journal_mode=DELETE")
    jconn.commit()
    jconn.close()


@pytest.fixture(scope="session", autouse=True)
def _mock_discord_env() -> None:
    """未設定の Discord webhook 環境変数にのみ無害なダミーを注入する。

    .env を持たない隔離環境でルーティング系テストの「URL 未設定で空送信」を防ぐ。
    既存値は尊重（本番/ローカル設定を壊さない）。個別テストが monkeypatch で
    上書きする場合はそちらが優先される（session autouse は土台のみ）。
    """
    defaults = {
        "DISCORD_WEBHOOK_URL": "http://test-prediction.local",
    }
    for key, dummy in defaults.items():
        if not os.environ.get(key):
            os.environ[key] = dummy
