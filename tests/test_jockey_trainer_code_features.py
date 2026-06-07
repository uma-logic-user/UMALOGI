"""W-076: 騎手/調教師コードベース特徴量エンコードのテスト。

race_results/entries の jockey_code/trainer_code を最優先で使い、欠損時のみ
名前→コードマップにフォールバックすることを検証する。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.ml.features import FeatureBuilder


@pytest.fixture
def fb() -> FeatureBuilder:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE jockeys (jockey_code TEXT, jockey_name TEXT);
        CREATE TABLE trainers (trainer_code TEXT, trainer_name TEXT);
        INSERT INTO jockeys VALUES ('00666','武豊');
        INSERT INTO trainers VALUES ('00399','国枝栄');
        """
    )
    return FeatureBuilder(conn)


def test_encode_jockey_prefers_code(fb: FeatureBuilder) -> None:
    # コードがあれば名前マッチを介さず int(code) を返す。
    assert fb._encode_jockey("どんな名前でも", "00666") == 666
    # 先頭ゼロも整数化される。
    assert fb._encode_jockey("", "01196") == 1196


def test_encode_jockey_falls_back_to_name(fb: FeatureBuilder) -> None:
    # コード欠損時は名前→コードマップにフォールバック。
    assert fb._encode_jockey("武豊", None) == 666
    # マップに無い名前は 0、名前も無ければ -1。
    assert fb._encode_jockey("無名騎手", None) == 0
    assert fb._encode_jockey("", None) == -1


def test_encode_jockey_invalid_code_falls_back(fb: FeatureBuilder) -> None:
    # 非数値コードは無効としてフォールバック。
    assert fb._encode_jockey("武豊", "ＸＹＺ") == 666


def test_encode_trainer_prefers_code(fb: FeatureBuilder) -> None:
    assert fb._encode_trainer("名前無関係", "00399") == 399
    assert fb._encode_trainer("国枝栄", None) == 399
    assert fb._encode_trainer("", None) == -1
