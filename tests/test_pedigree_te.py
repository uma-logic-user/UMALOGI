"""血統 Target Encoding（src/features/pedigree_te.py）のテスト。

リークフリー（cutoff 日より前の結果のみで TE を学習）と、未知/欠損サイアーへの
全体平均スムージング・フォールバックを最重要に検証する。
"""

from __future__ import annotations

import sqlite3

import pytest

from src.features.pedigree_te import (
    PEDIGREE_FEATURE_COLS,
    SireEncoder,
    build_pedigree_features,
)


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.execute(
        "CREATE TABLE races(race_id TEXT PRIMARY KEY, date TEXT, venue TEXT, "
        "surface TEXT, distance INTEGER)"
    )
    c.execute(
        "CREATE TABLE race_results(race_id TEXT, horse_number INTEGER, horse_id TEXT, "
        "horse_name TEXT, rank INTEGER)"
    )
    c.execute(
        "CREATE TABLE horses(horse_id TEXT PRIMARY KEY, horse_name TEXT, sire TEXT, "
        "dam TEXT, dam_sire TEXT)"
    )
    # 過去（学習対象）: サイアーS_GOOD産駒は芝で good、S_BAD産駒は凡走
    c.execute("INSERT INTO races VALUES('P1','2025-01-01','東京','芝',1600)")
    c.execute("INSERT INTO races VALUES('P2','2025-02-01','東京','芝',1600)")
    c.execute("INSERT INTO horses VALUES('H1','良駒','S_GOOD','d','ds')")
    c.execute("INSERT INTO horses VALUES('H2','駄駒','S_BAD','d','ds')")
    # H1: 過去2走とも1着 / H2: 過去2走とも10着
    c.execute("INSERT INTO race_results VALUES('P1',1,'H1','良駒',1)")
    c.execute("INSERT INTO race_results VALUES('P2',1,'H1','良駒',1)")
    c.execute("INSERT INTO race_results VALUES('P1',2,'H2','駄駒',10)")
    c.execute("INSERT INTO race_results VALUES('P2',2,'H2','駄駒',10)")
    # 対象レース（未来・2026）: H1きょうだい(同サイアーS_GOOD)とH3(未知サイアー)
    c.execute("INSERT INTO races VALUES('TARGET','2026-06-07','東京','芝',1600)")
    c.execute("INSERT INTO horses VALUES('H3','新駒','S_GOOD','d','ds')")
    c.execute("INSERT INTO horses VALUES('H4','謎駒','S_UNKNOWN','d','ds')")
    c.execute("INSERT INTO race_results VALUES('TARGET',1,'H3','新駒',NULL)")
    c.execute("INSERT INTO race_results VALUES('TARGET',2,'H4','謎駒',NULL)")
    c.commit()
    return c


def test_feature_cols_defined() -> None:
    assert "sire_place_te" in PEDIGREE_FEATURE_COLS


def test_encoder_fit_leakfree_cutoff(conn: sqlite3.Connection) -> None:
    enc = SireEncoder()
    # cutoff=2026-01-01 → 過去(2025)のみで学習。S_GOOD は複勝率高、S_BAD は低い。
    enc.fit(conn, cutoff_date="2026-01-01", surface="芝")
    good = enc.encode("S_GOOD")
    bad = enc.encode("S_BAD")
    assert good > bad


def test_unknown_sire_falls_back_to_global_mean(conn: sqlite3.Connection) -> None:
    enc = SireEncoder()
    enc.fit(conn, cutoff_date="2026-01-01", surface="芝")
    unknown = enc.encode("S_UNKNOWN_NEVER_SEEN")
    # 未知サイアーは全体平均（0〜1）にフォールバック（NaN や例外を出さない）
    assert 0.0 <= unknown <= 1.0
    assert abs(unknown - enc.global_mean) < 1e-9


def test_smoothing_pulls_small_sample_toward_global(conn: sqlite3.Connection) -> None:
    # スムージング m が大きいほど少数サンプルのサイアーは全体平均に近づく
    enc_low = SireEncoder(smoothing=0.0)
    enc_high = SireEncoder(smoothing=100.0)
    enc_low.fit(conn, cutoff_date="2026-01-01", surface="芝")
    enc_high.fit(conn, cutoff_date="2026-01-01", surface="芝")
    # 高スムージングの方が global_mean に近い
    d_low = abs(enc_low.encode("S_GOOD") - enc_low.global_mean)
    d_high = abs(enc_high.encode("S_GOOD") - enc_high.global_mean)
    assert d_high < d_low


def test_build_pedigree_features_for_race(conn: sqlite3.Connection) -> None:
    enc = SireEncoder()
    enc.fit(conn, cutoff_date="2026-01-01", surface="芝")
    df = build_pedigree_features(conn, "TARGET", enc)
    assert set(df["horse_number"]) == {1, 2}
    h3 = df[df["horse_number"] == 1].iloc[0]  # S_GOOD産駒
    h4 = df[df["horse_number"] == 2].iloc[0]  # 未知サイアー
    # S_GOOD産駒は未知サイアー(=全体平均)より高評価
    assert h3["sire_place_te"] >= h4["sire_place_te"]
