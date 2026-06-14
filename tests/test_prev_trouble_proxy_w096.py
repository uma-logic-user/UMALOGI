"""
W-096 / v1.16.0-dev — 前走不利プロキシ特徴量の回帰テスト（Task4）

検証:
  - 速い上がりで凡退した馬に高い不利スコアが付く（巻き返し加点）。
  - 上がりが遅く順当に凡退した馬は低スコア（不利ではない）。
  - 好走馬（3着以内）は不利スコア0。
  - リークフリー: 現レース日より前の出走のみ参照する。
  - FEATURE_COLS に prev_trouble_proxy が含まれる。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.features.prerun import PRERUN_FEATURE_COLS, build_prerun_features


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY, date TEXT, venue TEXT,
            surface TEXT, distance INTEGER, grade TEXT
        );
        CREATE TABLE race_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, race_id TEXT, horse_id TEXT,
            horse_name TEXT, rank INTEGER, horse_number INTEGER,
            weight_carried REAL, margin TEXT, last_3f TEXT
        );
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER, horse_id TEXT,
            horse_name TEXT, weight_carried REAL
        );
        """
    )
    # 対象レース（本日）
    c.execute(
        "INSERT INTO races VALUES ('20260614T','2026-06-14','東京','芝',1600,'G3')"
    )
    # 前走レース（過去）
    c.execute(
        "INSERT INTO races VALUES ('20260501P','2026-05-01','東京','芝',1600,'G3')"
    )
    # 出馬表（本日の3頭）
    for hn, hid, name in [
        (1, "H1", "差し馬"),
        (2, "H2", "凡走馬"),
        (3, "H3", "好走馬"),
    ]:
        c.execute(
            "INSERT INTO entries VALUES ('20260614T',?,?,?,55.0)", (hn, hid, name)
        )
    # 前走結果:
    #   H1: 上がり最速32.8s だが 8着（=不利の巻き返し期待） margin 0.6
    #   H2: 上がり凡庸36.5s で 10着（順当な凡走＝不利ではない）
    #   H3: 3着で好走（不利スコア0）
    c.execute(
        "INSERT INTO race_results (race_id,horse_id,horse_name,rank,horse_number,weight_carried,margin,last_3f)"
        " VALUES ('20260501P','H1','差し馬',8,1,55.0,'0.6','32.8')"
    )
    c.execute(
        "INSERT INTO race_results (race_id,horse_id,horse_name,rank,horse_number,weight_carried,margin,last_3f)"
        " VALUES ('20260501P','H2','凡走馬',10,2,55.0,'1.5','36.5')"
    )
    c.execute(
        "INSERT INTO race_results (race_id,horse_id,horse_name,rank,horse_number,weight_carried,margin,last_3f)"
        " VALUES ('20260501P','H3','好走馬',3,3,55.0,'0.2','34.0')"
    )
    # 対象レースの出走馬は race_results から取得される（rank は現レースでは未確定でも可）。
    for hn, hid, name in [
        (1, "H1", "差し馬"),
        (2, "H2", "凡走馬"),
        (3, "H3", "好走馬"),
    ]:
        c.execute(
            "INSERT INTO race_results (race_id,horse_id,horse_name,rank,horse_number,weight_carried)"
            " VALUES ('20260614T',?,?,NULL,?,55.0)",
            (hid, name, hn),
        )
    c.commit()
    return c


def test_feature_in_prerun_and_leakfree_pipeline() -> None:
    # prerun のリークフリー列として登録される
    assert "prev_trouble_proxy" in PRERUN_FEATURE_COLS
    # V2/accuracy モデルの入力（LEAKFREE_NEW_COLS）に自動合流する
    from src.features.backtest_v2 import LEAKFREE_NEW_COLS

    assert "prev_trouble_proxy" in LEAKFREE_NEW_COLS
    # V1 入力次元（base FEATURE_COLS）には含めない（prerun 非結合のため）
    from src.ml.models import FEATURE_COLS

    assert "prev_trouble_proxy" not in FEATURE_COLS


def test_trouble_proxy_high_for_fast_closer_poor_finish(conn) -> None:
    df = build_prerun_features(conn, "20260614T")
    by_num = {int(r["horse_number"]): r for _, r in df.iterrows()}
    # H1: 速い上がり(32.8s)×8着凡退 → 明確に高い（closing0.9×rank0.5=0.45）
    assert by_num[1]["prev_trouble_proxy"] >= 0.4
    # H2: 遅い上がり(36.5s)で順当凡走 → 低スコア
    assert by_num[2]["prev_trouble_proxy"] <= 0.1
    # H1 は H2 より明確に高い（不利の巻き返し期待）
    assert by_num[1]["prev_trouble_proxy"] > by_num[2]["prev_trouble_proxy"]
    # H3: 好走（3着）→ 0
    assert by_num[3]["prev_trouble_proxy"] == 0.0


def test_leakfree_no_future_reference(conn) -> None:
    """未来（現レース日以降）の結果は参照しない。前走が無ければ nan。"""
    import math

    # 新馬（過去走なし）を対象レースの出走馬として追加
    conn.execute(
        "INSERT INTO race_results (race_id,horse_id,horse_name,rank,horse_number,weight_carried)"
        " VALUES ('20260614T','H9','新馬',NULL,9,54.0)"
    )
    conn.commit()
    df = build_prerun_features(conn, "20260614T")
    by_num = {int(r["horse_number"]): r for _, r in df.iterrows()}
    assert math.isnan(by_num[9]["prev_trouble_proxy"])
