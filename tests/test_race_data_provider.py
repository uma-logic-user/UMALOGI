"""src/data/race_data_provider.py — JRA/NAR 統合プロバイダのテスト。

datasource="nar" 経路は NetkeibaNarFetcher をモック注入して検証し、
datasource 列の付与・データ混在ガード・自動判定を確認する。
JRA 経路は in-memory SQLite を注入して読み取りを検証する（ネットワーク非依存）。
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from src.data.race_data_provider import (
    ENTRY_COLUMNS,
    RaceDataProvider,
    assert_single_datasource,
    provider_for_race,
)
from src.nar.data_fetcher import (
    NarDataFetcher,
    NarHorseEntry,
    NarPayout,
    NarRaceMeta,
    NarRaceResult,
    NarResultRow,
)

_NAR_RID = "202644010101"  # 大井
_JRA_RID = "202605010101"  # 東京


class _FakeNarFetcher(NarDataFetcher):
    """ネットワーク無しで決め打ち NAR データを返すモック。"""

    def fetch_race_meta(self, race_id: str) -> NarRaceMeta:
        return NarRaceMeta(race_id, "2026-06-04", "大井", 1, 1600, "ダート", "20:50")

    def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
        return [
            NarHorseEntry(1, "ハクサンリュウ", "牡4", "的場文男", "荒山勝徳", 3.2, 1),
            NarHorseEntry(2, "テンリュウオー", "牝5", "森泰斗", "佐藤賢二", 5.8, 2),
        ]

    def fetch_odds(self, race_id: str) -> dict[int, float]:
        return {1: 3.2, 2: 5.8}

    def fetch_results(self, race_id: str) -> NarRaceResult:
        return NarRaceResult(
            race_id=race_id,
            ranking=[2, 1],
            results=[
                NarResultRow(1, 2, "テンリュウオー"),
                NarResultRow(2, 1, "ハクサンリュウ"),
            ],
            payouts=[
                NarPayout("単勝", "2", 580),
                NarPayout("複勝", "2", 210),
                NarPayout("複勝", "1", 150),
                NarPayout("馬連", "1-2", 1840),
            ],
        )


def _nar_provider() -> RaceDataProvider:
    return RaceDataProvider(datasource="nar", nar_fetcher=_FakeNarFetcher())


# ── datasource バリデーション ───────────────────────────────────────────────


def test_invalid_datasource_raises() -> None:
    with pytest.raises(ValueError):
        RaceDataProvider(datasource="keiba")


def test_default_datasource_is_jra() -> None:
    assert RaceDataProvider().datasource == "jra"


# ── NAR 経路（正常系・モック） ───────────────────────────────────────────────


def test_nar_get_entries_returns_dataframe_tagged_nar() -> None:
    """datasource='nar' の出馬表が datasource 列付き DataFrame で返る。"""
    df = _nar_provider().get_entries(_NAR_RID)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ENTRY_COLUMNS
    assert len(df) == 2
    assert set(df["datasource"]) == {"nar"}
    assert df.iloc[0]["horse_name"] == "ハクサンリュウ"
    assert df.iloc[0]["win_odds"] == 3.2


def test_nar_get_results_returns_ranking_tagged_nar() -> None:
    """datasource='nar' の結果（着順）が datasource 列付きで返る。"""
    df = _nar_provider().get_results(_NAR_RID)
    assert set(df["datasource"]) == {"nar"}
    assert list(df["rank"]) == [1, 2]
    assert df.iloc[0]["horse_number"] == 2
    assert df.iloc[0]["horse_name"] == "テンリュウオー"


def test_nar_get_payouts_returns_payout_rows_tagged_nar() -> None:
    """datasource='nar' の払戻明細（複勝複数含む）が datasource 列付きで返る。"""
    df = _nar_provider().get_payouts(_NAR_RID)
    assert set(df["datasource"]) == {"nar"}
    fukusho = df[df["bet_type"] == "複勝"]
    assert len(fukusho) == 2
    assert set(df["bet_type"]) >= {"単勝", "複勝", "馬連"}


# ── 自動判定 ─────────────────────────────────────────────────────────────────


def test_provider_for_race_autoselects_nar() -> None:
    """NAR race_id からは datasource='nar' のプロバイダが選ばれる。"""
    p = provider_for_race(_NAR_RID, nar_fetcher=_FakeNarFetcher())
    assert p.datasource == "nar"
    assert set(p.get_entries(_NAR_RID)["datasource"]) == {"nar"}


def test_provider_for_race_autoselects_jra() -> None:
    """JRA race_id からは datasource='jra' のプロバイダが選ばれる。"""
    assert provider_for_race(_JRA_RID).datasource == "jra"


# ── 混在ガード ───────────────────────────────────────────────────────────────


def test_assert_single_datasource_rejects_mixed() -> None:
    """jra と nar が混在した DataFrame はガードで拒否される。"""
    mixed = pd.DataFrame(
        {"race_id": ["a", "b"], "datasource": ["jra", "nar"], "horse_number": [1, 2]}
    )
    with pytest.raises(ValueError):
        assert_single_datasource(mixed)


def test_assert_single_datasource_rejects_expected_mismatch() -> None:
    """期待 datasource と異なる DataFrame は拒否される。"""
    df = pd.DataFrame({"datasource": ["nar", "nar"]})
    with pytest.raises(ValueError):
        assert_single_datasource(df, expected="jra")


def test_assert_single_datasource_accepts_uniform() -> None:
    """単一 datasource の DataFrame はそのまま通る。"""
    df = _nar_provider().get_entries(_NAR_RID)
    assert_single_datasource(df, expected="nar")  # 例外が出ないこと


# ── JRA 経路（in-memory DB 注入） ────────────────────────────────────────────


@pytest.fixture
def jra_db(tmp_path) -> str:  # type: ignore[no-untyped-def]
    db = tmp_path / "jra.db"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER, gate_number INTEGER,
            horse_name TEXT, sex_age TEXT, weight_carried REAL,
            jockey TEXT, trainer TEXT, horse_weight INTEGER, horse_weight_diff INTEGER
        );
        CREATE TABLE realtime_odds (
            race_id TEXT, horse_number INTEGER, win_odds REAL,
            place_odds_min REAL, place_odds_max REAL, popularity INTEGER
        );
        CREATE TABLE race_results (
            race_id TEXT, horse_number INTEGER, horse_name TEXT, rank INTEGER
        );
        CREATE TABLE race_payouts (
            race_id TEXT, bet_type TEXT, combination TEXT, payout INTEGER
        );
        INSERT INTO entries(race_id,horse_number,horse_name,sex_age,jockey,trainer)
            VALUES ('202605010101',1,'JRAホースA','牡3','ルメール','国枝栄');
        INSERT INTO realtime_odds(race_id,horse_number,win_odds,popularity)
            VALUES ('202605010101',1,2.1,1);
        INSERT INTO race_results(race_id,horse_number,horse_name,rank)
            VALUES ('202605010101',1,'JRAホースA',1);
        INSERT INTO race_payouts(race_id,bet_type,combination,payout)
            VALUES ('202605010101','単勝','1',210);
        """
    )
    con.commit()
    con.close()
    return str(db)


def test_jra_get_entries_from_db_tagged_jra(jra_db: str) -> None:
    """datasource='jra' は既存 DB（entries+realtime_odds）から読み datasource='jra' を付与する。"""
    df = RaceDataProvider(datasource="jra", db_path=jra_db).get_entries(_JRA_RID)
    assert set(df["datasource"]) == {"jra"}
    assert df.iloc[0]["horse_name"] == "JRAホースA"
    assert df.iloc[0]["win_odds"] == 2.1


def test_jra_get_results_from_db_tagged_jra(jra_db: str) -> None:
    df = RaceDataProvider(datasource="jra", db_path=jra_db).get_results(_JRA_RID)
    assert set(df["datasource"]) == {"jra"}
    assert df.iloc[0]["rank"] == 1
    assert df.iloc[0]["horse_number"] == 1


def test_jra_path_does_not_call_nar_fetcher(jra_db: str) -> None:
    """JRA 経路は NAR フェッチャを呼ばない（経路分離の担保）。"""

    class _BoomFetcher(_FakeNarFetcher):
        def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
            raise AssertionError("JRA 経路で NAR フェッチャが呼ばれた")

    df = RaceDataProvider(
        datasource="jra", db_path=jra_db, nar_fetcher=_BoomFetcher()
    ).get_entries(_JRA_RID)
    assert set(df["datasource"]) == {"jra"}
