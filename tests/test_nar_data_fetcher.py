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
    NarPayout,
    NarRaceMeta,
    NarRaceResult,
    NarResultRow,
    NetkeibaNarFetcher,
    is_nar_race_id,
    parse_result_page,
    parse_result_payouts,
    parse_result_rows,
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


# ── 結果ページ（result）パーサ ──────────────────────────────────────────────

# 実際の nar.netkeiba.com /race/result.html の構造を模した最小モック HTML。
#  - 着順テーブル: table.RaceTable01（td.Result_Num=着順 / 2 つの td.Num=[枠, 馬番] / .Horse_Info=馬名）
#  - 払戻テーブル: table.Payout_Detail_Table（単複は div/span、馬連等は ul/li、Payout は <br> 区切り）
# 1 着の枠(4)と馬番(8)を意図的に変え、馬番側を正しく取得できることを検証する。
MOCK_RESULT_HTML = """
<html><head><title>3歳条件 未勝利 結果 | 2026年6月3日 門別1R 地方競馬レース情報 - netkeiba</title></head>
<body>
<table class="RaceTable01">
  <tr><th class="Result_Num">着 順</th><th class="Num">枠</th><th class="Num">馬番</th><th>馬名</th></tr>
  <tr class="HorseList">
    <td class="Result_Num">1</td>
    <td class="Num Waku4">4</td>
    <td class="Num Waku">8</td>
    <td class="Horse_Info"><span class="Horse_Name"><a href="/horse/1/">サンプルホースA</a></span></td>
    <td class="Horse_Info">牝3</td>
    <td class="Jockey"><a href="/jockey/">安藤洋一</a></td>
  </tr>
  <tr class="HorseList">
    <td class="Result_Num">2</td>
    <td class="Num Waku3">3</td>
    <td class="Num Waku">3</td>
    <td class="Horse_Info"><span class="Horse_Name"><a href="/horse/2/">サンプルホースB</a></span></td>
    <td class="Horse_Info">牡3</td>
    <td class="Jockey"><a href="/jockey/">桑村真章</a></td>
  </tr>
  <tr class="HorseList">
    <td class="Result_Num">3</td>
    <td class="Num Waku2">2</td>
    <td class="Num Waku">2</td>
    <td class="Horse_Info"><a href="/horse/3/">サンプルホースC</a></td>
    <td class="Horse_Info">牝3</td>
  </tr>
</table>
<table class="Payout_Detail_Table">
  <tr><th>単勝</th>
    <td class="Result"><div><span>8</span></div></td>
    <td class="Payout"><span>110円</span></td><td class="Ninki">1人気</td></tr>
  <tr><th>複勝</th>
    <td class="Result"><div><span>8</span></div><div><span>3</span></div><div><span>2</span></div></td>
    <td class="Payout"><span>100円<br>120円<br>530円</span></td>
    <td class="Ninki">2人気 / 1人気 / 6人気</td></tr>
  <tr><th>馬連</th>
    <td class="Result"><ul><li><span>3</span></li><li><span>8</span></li></ul></td>
    <td class="Payout"><span>140円</span></td><td class="Ninki">1人気</td></tr>
</table>
<table class="Payout_Detail_Table">
  <tr><th>ワイド</th>
    <td class="Result"><ul><li><span>3</span></li><li><span>8</span></li></ul><ul><li><span>2</span></li><li><span>8</span></li></ul><ul><li><span>2</span></li><li><span>3</span></li></ul></td>
    <td class="Payout"><span>130円<br/>1,320円<br/>930円</span></td>
    <td class="Ninki">1人気 / 12人気 / 9人気</td></tr>
  <tr><th>三連複</th>
    <td class="Result"><ul><li><span>2</span></li><li><span>3</span></li><li><span>8</span></li></ul></td>
    <td class="Payout"><span>1,410円</span></td><td class="Ninki">4人気</td></tr>
</table>
</body></html>
"""


def test_parse_result_rows_extracts_rank_number_name() -> None:
    """着順テーブルから（着順・馬番・馬名）を抽出し、枠ではなく馬番を採る。"""
    rows = parse_result_rows(MOCK_RESULT_HTML)
    assert len(rows) == 3
    assert all(isinstance(r, NarResultRow) for r in rows)
    assert (rows[0].rank, rows[0].horse_number, rows[0].horse_name) == (
        1,
        8,  # 枠は 4 だが馬番 8 を採る
        "サンプルホースA",
    )
    assert (rows[1].rank, rows[1].horse_number) == (2, 3)
    assert (rows[2].rank, rows[2].horse_number) == (3, 2)


def test_parse_result_payouts_single_value_types() -> None:
    """単勝・馬連など単一払戻をクレンジングして抽出する。"""
    payouts = parse_result_payouts(MOCK_RESULT_HTML)
    tansho = [p for p in payouts if p.bet_type == "単勝"]
    assert len(tansho) == 1
    assert tansho[0].combination == "8"
    assert tansho[0].amount == 110
    umaren = [p for p in payouts if p.bet_type == "馬連"]
    assert umaren[0].combination == "3-8"
    assert umaren[0].amount == 140


def test_parse_result_payouts_multiple_values() -> None:
    """複勝（3 値）・ワイド（3 組）の複数払戻を行ごとに分解する。"""
    payouts = parse_result_payouts(MOCK_RESULT_HTML)
    fukusho = [p for p in payouts if p.bet_type == "複勝"]
    assert {(p.combination, p.amount) for p in fukusho} == {
        ("8", 100),
        ("3", 120),
        ("2", 530),
    }
    wide = [p for p in payouts if p.bet_type == "ワイド"]
    assert {(p.combination, p.amount) for p in wide} == {
        ("3-8", 130),
        ("2-8", 1320),  # カンマ "1,320円" を除去して int 化
        ("2-3", 930),
    }


def test_parse_result_payouts_required_bet_types_present() -> None:
    """要件の券種（単勝・複勝・馬連・ワイド）がすべて抽出されている。"""
    payouts = parse_result_payouts(MOCK_RESULT_HTML)
    kinds = {p.bet_type for p in payouts}
    for required in ("単勝", "複勝", "馬連", "ワイド"):
        assert required in kinds
    assert all(isinstance(p, NarPayout) and p.amount > 0 for p in payouts)


def test_fetch_results_via_injected_http_get() -> None:
    """http_get 注入で fetch_results が完全な NarRaceResult を返す。"""
    fetcher = NetkeibaNarFetcher(http_get=lambda url: MOCK_RESULT_HTML)
    result = fetcher.fetch_results("202630060301")
    assert isinstance(result, NarRaceResult)
    assert result.race_id == "202630060301"
    assert result.ranking == [8, 3, 2]  # 着順順の馬番
    assert result.results[0].horse_name == "サンプルホースA"
    assert any(p.bet_type == "単勝" for p in result.payouts)


def test_parse_result_robust_to_malformed_html() -> None:
    """DOM 欠損時もクラッシュせず空の NarRaceResult を返す。"""
    result = parse_result_page("<html><body>no tables</body></html>", "202630060301")
    assert isinstance(result, NarRaceResult)
    assert result.race_id == "202630060301"
    assert result.ranking == []
    assert result.results == []
    assert result.payouts == []


def test_fetch_results_returns_empty_dto_on_http_failure() -> None:
    """通信失敗時も例外を投げず、空の NarRaceResult を返す（WARNING ログ）。"""

    def _boom(url: str) -> str:
        raise RuntimeError("network down")

    fetcher = NetkeibaNarFetcher(http_get=_boom)
    result = fetcher.fetch_results("202630060301")
    assert result.ranking == [] and result.payouts == []


@pytest.mark.parametrize("rid", ["202630060301"])
def test_live_fetch_results_smoke(rid: str) -> None:
    """結果ページの実通信スモーク（未確定/接続不可/構造変更時は graceful skip）。"""
    fetcher = NetkeibaNarFetcher()
    try:
        result = fetcher.fetch_results(rid)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"結果ライブ取得不可のためスキップ: {exc}")
    if not result.results:
        pytest.skip("結果未確定/取得不可のためスキップ")
    assert result.ranking[0] == result.results[0].horse_number
    assert all(r.horse_name for r in result.results)
    if result.payouts:
        assert all(p.amount > 0 and p.combination for p in result.payouts)
