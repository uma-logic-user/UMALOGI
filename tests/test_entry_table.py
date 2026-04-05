"""
entry_table.py スクレイパーのユニットテスト（ネットワーク不使用）
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.scraper.entry_table import (
    EntryHorse,
    HorseOdds,
    _parse_weight,
    fetch_entry_table,
    fetch_realtime_odds,
)


# ── フィクスチャ HTML ────────────────────────────────────────────

SAMPLE_SHUTUBA_HTML = """
<!DOCTYPE html>
<html>
<body>
<table class="Shutuba_Table">
  <thead><tr><th>枠</th><th>馬番</th><th>印</th><th>馬名</th><th>性齢</th>
  <th>斤量</th><th>騎手</th><th>調教師</th><th>馬体重</th>
  <th>単勝</th><th>人気</th><th>更新</th><th>情報</th><th>メモ</th><th>拡張</th>
  </tr></thead>
  <tbody>
    <tr class="HorseList">
      <td class="Waku">1</td>
      <td class="Umaban">1</td>
      <td></td>
      <td class="HorseInfo"><a href="/horse/2022105081/">ミュージアムマイル</a></td>
      <td>牡3</td>
      <td>56.0</td>
      <td>Ｃ．デム</td>
      <td>国枝栄</td>
      <td>502 (+2)</td>
      <td>3.8</td><td>3</td><td></td><td></td><td></td><td></td>
    </tr>
    <tr class="HorseList">
      <td class="Waku">2</td>
      <td class="Umaban">2</td>
      <td></td>
      <td class="HorseInfo"><a href="/horse/2021105898/">レガレイラ</a></td>
      <td>牝4</td>
      <td>55.0</td>
      <td>横山武史</td>
      <td>木村哲也</td>
      <td>482 (-4)</td>
      <td>3.3</td><td>1</td><td></td><td></td><td></td><td></td>
    </tr>
  </tbody>
</table>
</body>
</html>
"""


# ── _parse_weight ────────────────────────────────────────────────

class TestParseWeight:
    def test_正常な体重とプラス差分(self) -> None:
        assert _parse_weight("502 (+2)") == (502, 2)

    def test_マイナス差分(self) -> None:
        assert _parse_weight("482 (-4)") == (482, -4)

    def test_体重のみ(self) -> None:
        weight, diff = _parse_weight("500")
        assert weight == 500
        assert diff is None

    def test_計不(self) -> None:
        assert _parse_weight("計不") == (None, None)

    def test_空文字(self) -> None:
        assert _parse_weight("") == (None, None)


# ── fetch_entry_table ────────────────────────────────────────────

class TestFetchEntryTable:
    def test_出馬表を正しくパースする(self) -> None:
        with patch("src.scraper.entry_table._fetch", return_value=SAMPLE_SHUTUBA_HTML):
            table = fetch_entry_table("202506050811")

        assert table.race_id == "202506050811"
        assert len(table.entries) == 2

    def test_馬名とhorse_idを取得する(self) -> None:
        with patch("src.scraper.entry_table._fetch", return_value=SAMPLE_SHUTUBA_HTML):
            table = fetch_entry_table("202506050811")

        h1 = table.entries[0]
        assert h1.horse_name == "ミュージアムマイル"
        assert h1.horse_id == "2022105081"
        assert h1.horse_number == 1
        assert h1.gate_number == 1

    def test_斤量と性齢を取得する(self) -> None:
        with patch("src.scraper.entry_table._fetch", return_value=SAMPLE_SHUTUBA_HTML):
            table = fetch_entry_table("202506050811")

        h1 = table.entries[0]
        assert h1.weight_carried == 56.0
        assert h1.sex_age == "牡3"
        assert h1.jockey == "Ｃ．デム"
        assert h1.trainer == "国枝栄"

    def test_馬体重と差分を取得する(self) -> None:
        with patch("src.scraper.entry_table._fetch", return_value=SAMPLE_SHUTUBA_HTML):
            table = fetch_entry_table("202506050811")

        assert table.entries[0].horse_weight == 502
        assert table.entries[0].horse_weight_diff == 2
        assert table.entries[1].horse_weight == 482
        assert table.entries[1].horse_weight_diff == -4

    def test_空テーブルでも例外なし(self) -> None:
        empty_html = "<html><body><table class='Shutuba_Table'></table></body></html>"
        with patch("src.scraper.entry_table._fetch", return_value=empty_html):
            table = fetch_entry_table("000000000000")

        assert table.entries == []


# ── fetch_realtime_odds ──────────────────────────────────────────

WIN_ODDS_JSON = json.dumps({
    "1": {
        "01": ["3.8", "", "3"],
        "02": ["5.1", "", "1"],
    }
})

PLACE_ODDS_JSON = json.dumps({
    "1": {
        "01": ["2.0", "3.5", "3"],
        "02": ["1.5", "2.8", "1"],
    }
})


class TestFetchRealtimeOdds:
    def _fetch_side_effect(self, url: str, params: dict | None = None, **kwargs):
        odds_type = (params or {}).get("type", 1)
        return WIN_ODDS_JSON if odds_type == 1 else PLACE_ODDS_JSON

    def test_単勝オッズを正しく取得する(self) -> None:
        with patch("src.scraper.entry_table._fetch", side_effect=self._fetch_side_effect):
            odds = fetch_realtime_odds("202506050811")

        assert len(odds) == 2
        h1 = next(o for o in odds if o.horse_number == 1)
        assert h1.win_odds == pytest.approx(3.8)
        assert h1.popularity == 3

    def test_複勝オッズを正しく取得する(self) -> None:
        with patch("src.scraper.entry_table._fetch", side_effect=self._fetch_side_effect):
            odds = fetch_realtime_odds("202506050811")

        h1 = next(o for o in odds if o.horse_number == 1)
        assert h1.place_odds_min == pytest.approx(2.0)
        assert h1.place_odds_max == pytest.approx(3.5)

    def test_馬番昇順で返す(self) -> None:
        with patch("src.scraper.entry_table._fetch", side_effect=self._fetch_side_effect):
            odds = fetch_realtime_odds("202506050811")

        numbers = [o.horse_number for o in odds]
        assert numbers == sorted(numbers)

    def test_JSONパース失敗でも例外なし(self) -> None:
        with patch("src.scraper.entry_table._fetch", return_value="invalid json"):
            odds = fetch_realtime_odds("202506050811")

        assert odds == []
