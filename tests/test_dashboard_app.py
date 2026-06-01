"""src/web/dashboard.py の E2E スモークテスト（Streamlit AppTest）。

`streamlit run src/web/dashboard.py` 相当の実行を AppTest で再現し、
一時 DB（実データ風フィクスチャ）に対してアプリ全体が例外なく描画されることを
保証する。「安全に起動できること」の自動検証。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.database.schema import DDL_STATEMENTS

_APP = Path(__file__).resolve().parents[1] / "src" / "web" / "dashboard.py"


def _seed_db(path: Path) -> None:
    """直近結果・予想・確定実績を含む最小フィクスチャ DB を作る。"""
    c = sqlite3.connect(path)
    for ddl in DDL_STATEMENTS:
        c.execute(ddl)
    c.execute(
        "INSERT INTO races(race_id, race_name, date, venue, race_number, "
        "distance, surface) VALUES('R1','テストS','2026-05-31','東京',11,1600,'芝')"
    )
    c.execute(
        "INSERT INTO race_results(race_id, horse_name, rank, horse_number, "
        "win_odds, popularity) VALUES('R1','勝ち馬',1,7,4.2,2)"
    )
    cur = c.execute(
        "INSERT INTO predictions(race_id, model_type, bet_type, confidence, "
        "expected_value, is_superseded) VALUES('R1','本命(直前)','単勝',0.6,1.45,0)"
    )
    pid = cur.lastrowid
    c.execute(
        "INSERT INTO prediction_horses(prediction_id, horse_name, predicted_rank, "
        "model_score, ev_score) VALUES(?,'勝ち馬',1,2.0,1.45)",
        (pid,),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id, is_hit, payout, profit) "
        "VALUES(?,1,420.0,320.0)",
        (pid,),
    )
    c.commit()
    c.close()


def test_dashboard_app_runs_without_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """アプリ全体を実行し、Streamlit 例外が発生しないことを確認する。"""
    db = tmp_path / "umalogi.db"
    _seed_db(db)
    monkeypatch.setenv("DB_PATH", str(db))

    at = AppTest.from_file(str(_APP), default_timeout=30).run()

    assert not at.exception
    # タイトルが描画されている。
    assert any("UMALOGI" in t.value for t in at.title)
    # ROI 集計対象が存在するため警告（集計対象なし）にはならない。
    assert all("集計対象の確定実績がありません" not in w.value for w in at.warning)
