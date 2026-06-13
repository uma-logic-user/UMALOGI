"""W-088 回帰テスト: races.grade 列と U score クラス変化因子。

races.grade 列が存在しないと `src/ml/u_score.py` の相手関係・クラス変化因子
(W-010/011) が `no such column: grade` で例外となり、U score 計算が全予想で
スキップされていた。本テストは:
  1. `_extract_grade` がレース名を正準グレードトークンへ正しく導出すること
  2. `init_db` 後の races に grade 列が存在すること（マイグレーション #21）
  3. `UScoreEngine.calc` が例外を出さず u_score 列を生成すること（全スキップ解消）
を保証する。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.database.init_db import init_db
from src.ml.u_score import UScoreEngine
from src.scraper.jravan_client import _extract_grade


# ── 1. _extract_grade のトークン導出 ───────────────────────────────────────


def test_extract_grade_graded_stakes_iii_ii_i_ordering() -> None:
    """III → II → I の順で照合し GI が GII/GIII に誤マッチしないこと。"""
    assert _extract_grade("第65回大阪杯(GI)") == "G1"
    assert _extract_grade("第29回マーメイドステークス(GIII)") == "G3"
    assert _extract_grade("第72回サンケイスポーツ賞(GII)") == "G2"


def test_extract_grade_jump_grades() -> None:
    """障害 J・GI/GII/GIII もグレードに正規化されること。"""
    assert _extract_grade("第23回中山グランドジャンプ(JGI)") == "G1"
    assert _extract_grade("阪神ジャンプステークス(JGIII)") == "G3"


def test_extract_grade_listed_and_open() -> None:
    assert _extract_grade("ノベンバーステークス(L)") == "L"
    assert _extract_grade("小倉日経オープン") == "OP"


def test_extract_grade_condition_classes() -> None:
    assert _extract_grade("3歳新馬") == "新馬"
    assert _extract_grade("3歳未勝利") == "未勝利"
    assert _extract_grade("3歳1勝クラス") == "1勝"
    assert _extract_grade("2勝クラス") == "2勝"
    assert _extract_grade("3勝クラス") == "3勝"


def test_extract_grade_unknown_returns_empty() -> None:
    assert _extract_grade("") == ""
    assert _extract_grade("謎の特別") == ""


def test_extract_grade_tokens_are_grade_rank_compatible() -> None:
    """導出トークンが u_score._grade_rank で 1〜10 にランク付けされること。"""
    from src.ml.u_score import UScoreEngine as _UE  # noqa: F401

    # _grade_rank は _calc_competition 内のローカル関数のため、語彙の整合性のみ確認。
    canonical = {"G1", "G2", "G3", "OP", "L", "3勝", "2勝", "1勝", "未勝利", "新馬"}
    samples = [
        "第1回テスト(GI)",
        "第1回テスト(GII)",
        "第1回テスト(GIII)",
        "テストオープン",
        "テスト(L)",
        "3勝クラス",
        "2勝クラス",
        "1勝クラス",
        "3歳未勝利",
        "2歳新馬",
    ]
    for name in samples:
        assert _extract_grade(name) in canonical


# ── 2. マイグレーション #21: races.grade 列の存在 ──────────────────────────


def test_races_has_grade_column() -> None:
    conn = init_db(db_path=Path(":memory:"))
    cols = [r[1] for r in conn.execute("PRAGMA table_info(races)").fetchall()]
    assert "grade" in cols, "マイグレーション #21 で grade 列が追加されていない"


# ── 3. U score 全スキップ解消（W-088 本丸の回帰テスト） ─────────────────────


def _seed_race(conn: sqlite3.Connection) -> None:
    """grade 付きレース 1 件と出走馬 2 頭を投入する。"""
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, grade)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "202601010101",
                "テスト記念(GI)",
                "2026-01-01",
                "中山",
                1,
                2000,
                "芝",
                "G1",
            ),
        )
        for num, (hid, name) in enumerate(
            (("2020100001", "テストホースA"), ("2020100002", "テストホースB")), start=1
        ):
            conn.execute(
                "INSERT OR REPLACE INTO horses (horse_id, horse_name) VALUES (?, ?)",
                (hid, name),
            )
            conn.execute(
                """
                INSERT INTO race_results
                    (race_id, horse_id, horse_name, horse_number, rank)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("202601010101", hid, name, num, num),
            )


def test_u_score_calc_does_not_skip_with_grade_column() -> None:
    """grade 列があれば _calc_competition の grade クエリが例外を出さず、
    calc が u_score 列を生成すること（W-088: 全スキップ解消の回帰）。"""
    conn = init_db(db_path=Path(":memory:"))
    _seed_race(conn)

    # FeatureBuilder が供給する数値列を最小限再現する
    # （u_score._clip_norm は欠損 scalar に非対応のため列を明示する）。
    df = pd.DataFrame(
        {
            "race_id": ["202601010101", "202601010101"],
            "horse_id": ["2020100001", "2020100002"],
            "horse_number": [1, 2],
            "win_rate_all": [0.2, 0.1],
            "win_rate_surface": [0.18, 0.09],
            "win_rate_distance_band": [0.15, 0.08],
            "recent_rank_mean": [3.0, 5.0],
            "surface": ["芝", "芝"],
        }
    )

    result = UScoreEngine(conn).calc(df)

    assert "u_score" in result.columns
    assert "uf_class_change" in result.columns
    assert result["u_score"].notna().all()
    assert len(result) == 2


def test_u_score_grade_query_executes_against_schema() -> None:
    """_calc_competition が発行する grade クエリがスキーマ上で成功すること。"""
    conn = init_db(db_path=Path(":memory:"))
    _seed_race(conn)
    rows = conn.execute(
        "SELECT race_id, grade FROM races WHERE race_id = ?", ("202601010101",)
    ).fetchall()
    assert rows == [("202601010101", "G1")]
