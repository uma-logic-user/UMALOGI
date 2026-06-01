"""src/web/dashboard.py のスモークテスト。

Streamlit ランタイムを必要としない部分（接続ヘルパー・Plotly 図構築）を検証する。
モジュールの import が副作用なく成功することも併せて担保する（main() は
`if __name__ == "__main__"` でガードされているため import では実行されない）。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import plotly.graph_objects as go
import pytest

from src.database.schema import DDL_STATEMENTS
from src.web import dashboard


def _make_db(path: Path) -> None:
    c = sqlite3.connect(path)
    for ddl in DDL_STATEMENTS:
        c.execute(ddl)
    c.commit()
    c.close()


def test_resolve_db_path_uses_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """環境変数 DB_PATH が設定されていればそれを優先する。"""
    monkeypatch.setenv("DB_PATH", "/tmp/custom.db")
    assert dashboard.resolve_db_path() == Path("/tmp/custom.db")


def test_resolve_db_path_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """DB_PATH 未設定なら既定の data/umalogi.db を指す。"""
    monkeypatch.delenv("DB_PATH", raising=False)
    assert dashboard.resolve_db_path().name == "umalogi.db"


def test_get_connection_is_readonly(tmp_path: Path) -> None:
    """get_connection は読み取り専用接続を返し、書き込みは拒否される。"""
    db = tmp_path / "umalogi.db"
    _make_db(db)

    conn = dashboard.get_connection(db)
    try:
        # 読み取りは可能。
        conn.execute("SELECT COUNT(*) FROM races").fetchone()
        # 書き込みは read-only のため失敗する。
        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                "INSERT INTO races(race_id, race_name, date, venue, "
                "race_number, distance, surface) VALUES('x','n','2026-01-01','東京',1,1600,'芝')"
            )
    finally:
        conn.close()


def test_get_connection_missing_file(tmp_path: Path) -> None:
    """DB が存在しない場合は FileNotFoundError を送出する。"""
    with pytest.raises(FileNotFoundError):
        dashboard.get_connection(tmp_path / "nope.db")


def test_roi_bar_figure_builds() -> None:
    """ROI 棒グラフが各モデルを 1 本のバーとして含む Figure を返す。"""
    rows = [
        {"model_type": "本命(直前)", "roi": 271.7, "hit_rate": 15.2},
        {"model_type": "Oracle(直前)", "roi": 21.7, "hit_rate": 4.0},
    ]
    fig = dashboard._roi_bar_figure(rows)
    assert isinstance(fig, go.Figure)
    bar = fig.data[0]
    assert set(bar.y) == {"本命(直前)", "Oracle(直前)"}
    assert len(bar.x) == 2


def test_hit_rate_bar_figure_builds() -> None:
    """的中率棒グラフが Figure を返す。"""
    rows = [{"model_type": "卍(直前)", "roi": 150.0, "hit_rate": 13.3}]
    fig = dashboard._hit_rate_bar_figure(rows)
    assert isinstance(fig, go.Figure)
    assert list(fig.data[0].x) == [13.3]
