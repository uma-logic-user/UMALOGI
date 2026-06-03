"""src/nar/data_fetcher.py — 地方競馬（NAR）データ取得基盤のテスト。

ネットワークには一切アクセスせず、純関数（URL ビルダー・ID 判定）と
DummyNarFetcher（決定的ダミーデータ）のみを検証する。
"""

from __future__ import annotations

from src.nar.data_fetcher import (
    NAR_VENUES,
    DummyNarFetcher,
    NarHorseEntry,
    NarRaceMeta,
    NarRaceResult,
    NetkeibaNarFetcher,
    is_nar_race_id,
)


def test_nar_venues_contains_major_tracks() -> None:
    """主要な地方競馬場（大井・船橋・川崎・浦和・園田・門別）が定義されている。"""
    names = set(NAR_VENUES.values())
    for track in ("大井", "船橋", "川崎", "浦和", "園田", "門別"):
        assert track in names


def test_is_nar_race_id_distinguishes_central_and_local() -> None:
    """JRA(中央) と NAR(地方) の race_id を会場コードで判別できる。"""
    # NAR: 大井(44)・船橋(43) 等の地方会場コードを含む
    assert is_nar_race_id("202644010101") is True
    # JRA: 東京(05)・中山(06) 等の中央会場コード
    assert is_nar_race_id("202605010101") is False


def test_dummy_fetcher_race_meta_uses_given_id_and_nar_venue() -> None:
    """DummyNarFetcher.fetch_race_meta は指定 race_id と NAR 会場を返す。"""
    fetcher = DummyNarFetcher()
    meta = fetcher.fetch_race_meta("202644010101")
    assert isinstance(meta, NarRaceMeta)
    assert meta.race_id == "202644010101"
    assert meta.venue in NAR_VENUES.values()
    assert meta.race_number >= 1
    assert meta.distance > 0


def test_dummy_fetcher_entries_are_valid() -> None:
    """fetch_entries は連番の馬番を持つ NarHorseEntry を複数返す。"""
    fetcher = DummyNarFetcher()
    entries = fetcher.fetch_entries("202644010101")
    assert len(entries) >= 2
    assert all(isinstance(e, NarHorseEntry) for e in entries)
    numbers = [e.horse_number for e in entries]
    assert numbers == list(range(1, len(entries) + 1))
    assert all(e.horse_name for e in entries)


def test_dummy_fetcher_odds_cover_all_entries() -> None:
    """fetch_odds は全出走馬ぶんの単勝オッズ（正値）を返す。"""
    fetcher = DummyNarFetcher()
    entries = fetcher.fetch_entries("202644010101")
    odds = fetcher.fetch_odds("202644010101")
    assert set(odds.keys()) == {e.horse_number for e in entries}
    assert all(v > 0 for v in odds.values())


def test_dummy_fetcher_results_rank_subset_of_entries() -> None:
    """fetch_results の着順は出走馬番の部分集合で、1 着が存在する。"""
    fetcher = DummyNarFetcher()
    entries = fetcher.fetch_entries("202644010101")
    result = fetcher.fetch_results("202644010101")
    assert isinstance(result, NarRaceResult)
    entry_nums = {e.horse_number for e in entries}
    assert set(result.ranking).issubset(entry_nums)
    assert result.ranking[0] in entry_nums  # 1 着馬番


def test_dummy_fetcher_is_deterministic() -> None:
    """同一 race_id に対する出力は決定的（再現可能）である。"""
    a = DummyNarFetcher().fetch_entries("202644010101")
    b = DummyNarFetcher().fetch_entries("202644010101")
    assert [e.horse_name for e in a] == [e.horse_name for e in b]


def test_netkeiba_nar_urls_target_nar_subdomain_and_contain_race_id() -> None:
    """netkeiba NAR フェッチャの URL は nar サブドメインと race_id を含む。"""
    rid = "202644010101"
    entry_url = NetkeibaNarFetcher.build_entry_url(rid)
    odds_url = NetkeibaNarFetcher.build_odds_url(rid)
    result_url = NetkeibaNarFetcher.build_result_url(rid)
    for url in (entry_url, odds_url, result_url):
        assert "nar.netkeiba.com" in url
        assert rid in url


def test_netkeiba_nar_live_fetch_is_prototype_stub() -> None:
    """ライブ取得は未実装の明示スタブ（NotImplementedError）であり、
    検証できないダミー成功を返さない（誠実なプロトタイプ境界）。"""
    fetcher = NetkeibaNarFetcher()
    import pytest

    with pytest.raises(NotImplementedError):
        fetcher.fetch_entries("202644010101")
