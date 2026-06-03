"""src/nar/data_fetcher.py — 地方競馬（NAR）データ取得基盤のテスト。

純関数（URL ビルダー・ID 判定）と DummyNarFetcher（決定的ダミーデータ）に加え、
NetkeibaNarFetcher のライブパーサを **モック HTML 注入**（http_get 差し替え）で検証する。
実通信テストは接続不可・サイト構造変更時に graceful skip する。
"""

from __future__ import annotations

import pytest

from src.nar.data_fetcher import (
    NAR_VENUES,
    DummyNarFetcher,
    NarHorseEntry,
    NarRaceMeta,
    NarRaceResult,
    NetkeibaNarFetcher,
    is_nar_race_id,
    parse_shutuba_entries,
    parse_shutuba_meta,
    parse_shutuba_odds,
)

# 実際の nar.netkeiba.com /race/shutuba.html の構造を模した最小モック HTML。
# 性齢セルは実ページ同様 class 無し（HorseInfo の次 td）で配置する。
MOCK_SHUTUBA_HTML = """
<html><head><title>3歳条件 未勝利 出馬表 | 2026年6月3日 門別1R 地方競馬レース情報 - netkeiba</title></head>
<body>
<div class="RaceList_Item02">
  <div class="RaceName">3歳条件 未勝利</div>
  <div class="RaceData01">14:15発走 / ダ1000m (右) / 天候:晴 / 馬場:良</div>
</div>
<table class="Shutuba_Table">
  <tr class="HorseList">
    <td class="Waku1">1</td>
    <td class="Umaban1">1</td>
    <td class="CheckMark Horse_Select">--</td>
    <td class="HorseInfo"><div class="HorseName"><a href="/horse/2024100001/">トモニミルホープ</a></div></td>
    <td>牝3</td>
    <td class="Txt_C">55.0</td>
    <td class="Jockey"><a href="/jockey/">坂下秀樹</a></td>
    <td class="Trainer"><a href="/trainer/">北海道 沼澤英知</a></td>
    <td class="Weight">434 (-4)</td>
    <td class="Popular Txt_R">229.3</td>
    <td class="Popular Txt_C">7</td>
  </tr>
  <tr class="HorseList">
    <td class="Waku2">2</td>
    <td class="Umaban2">2</td>
    <td class="CheckMark Horse_Select">--</td>
    <td class="HorseInfo"><div class="HorseName"><a href="/horse/2024100002/">サンプルホース</a></div></td>
    <td>牡4</td>
    <td class="Txt_C">56.0</td>
    <td class="Jockey"><a href="/jockey/">御神本訓史</a></td>
    <td class="Trainer"><a href="/trainer/">田中太郎</a></td>
    <td class="Weight">480 (+2)</td>
    <td class="Popular Txt_R">2.1</td>
    <td class="Popular Txt_C">1</td>
  </tr>
</table>
</body></html>
"""


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


# ── ライブパーサ（モック HTML 注入で検証） ──────────────────────────────────


def test_parse_shutuba_meta_extracts_fields() -> None:
    """出馬表 HTML から発走時刻・馬場・距離・会場をパースできる。"""
    meta = parse_shutuba_meta(MOCK_SHUTUBA_HTML, "202630060301")
    assert isinstance(meta, NarRaceMeta)
    assert meta.race_id == "202630060301"
    assert meta.venue == "門別"
    assert meta.race_number == 1
    assert meta.distance == 1000
    assert meta.surface == "ダート"
    assert meta.post_time == "14:15"


def test_parse_shutuba_entries_maps_dto() -> None:
    """出馬表 HTML から NarHorseEntry（馬番/馬名/性齢/騎手/調教師/オッズ/人気）を抽出する。"""
    entries = parse_shutuba_entries(MOCK_SHUTUBA_HTML)
    assert len(entries) == 2
    e0 = entries[0]
    assert isinstance(e0, NarHorseEntry)
    assert e0.horse_number == 1
    assert e0.horse_name == "トモニミルホープ"
    assert e0.sex_age == "牝3"
    assert e0.jockey == "坂下秀樹"
    assert e0.trainer == "沼澤英知"  # 地域接頭辞「北海道」は除去
    assert e0.win_odds == 229.3
    assert e0.popularity == 7


def test_parse_shutuba_odds_maps_number_to_odds() -> None:
    """出馬表 HTML から馬番→単勝オッズの辞書を抽出する。"""
    odds = parse_shutuba_odds(MOCK_SHUTUBA_HTML)
    assert odds == {1: 229.3, 2: 2.1}


def test_fetcher_uses_injected_http_get_without_network() -> None:
    """http_get 注入により、ネットワーク無しで fetch_entries/odds が機能する。"""
    fetcher = NetkeibaNarFetcher(http_get=lambda url: MOCK_SHUTUBA_HTML)
    entries = fetcher.fetch_entries("202630060301")
    odds = fetcher.fetch_odds("202630060301")
    meta = fetcher.fetch_race_meta("202630060301")
    assert [e.horse_name for e in entries] == ["トモニミルホープ", "サンプルホース"]
    assert odds[2] == 2.1
    assert meta.venue == "門別"


def test_parser_is_robust_to_malformed_html() -> None:
    """DOM 要素が見つからない場合もクラッシュせず空・既定値を返す。"""
    assert parse_shutuba_entries("<html><body>no rows</body></html>") == []
    assert parse_shutuba_odds("<html><body>broken") == {}
    meta = parse_shutuba_meta("<html></html>", "202630060301")
    assert meta.race_id == "202630060301"  # 会場は race_id から補完
    assert meta.venue == "門別"


def test_fetch_entries_returns_empty_on_http_failure() -> None:
    """http_get が例外を投げても、クラッシュせず空リストを返す（WARNING ログ）。"""

    def _boom(url: str) -> str:
        raise RuntimeError("network down")

    fetcher = NetkeibaNarFetcher(http_get=_boom)
    assert fetcher.fetch_entries("202630060301") == []


@pytest.mark.parametrize("rid", ["202630060301"])
def test_live_fetch_smoke(rid: str) -> None:
    """実通信スモークテスト（接続不可・構造変更時は graceful skip）。"""
    fetcher = NetkeibaNarFetcher()
    try:
        entries = fetcher.fetch_entries(rid)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"ライブ取得不可のためスキップ: {exc}")
    if not entries:
        pytest.skip("出走馬を取得できず（開催外/構造変更の可能性）スキップ")
    assert all(e.horse_number >= 1 for e in entries)
    assert all(e.horse_name for e in entries)
