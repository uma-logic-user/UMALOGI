"""
multi_odds_scraper.py の単体テスト。

検証対象:
  1. _format_combination() が unordered / ordered を正しく変換すること
  2. _parse_odds_dict() が MultiOddsEntry リストを正しく生成すること
  3. fetch_multi_odds() がモックモードで全券種を返すこと
  4. ワイドの odds_max が正しく設定されること
  5. 不正なキーがスキップされること
"""

from __future__ import annotations


import pytest

from src.scraper.multi_odds_scraper import (
    _format_combination,
    _parse_odds_dict,
    fetch_multi_odds,
    _MOCK_DATA,
    _BET_TYPES,
)
from src.database.init_db import MultiOddsEntry


class TestFormatCombination:
    def test_unordered_pair(self) -> None:
        assert _format_combination("01-02", "unordered") == "1-2"

    def test_unordered_triple(self) -> None:
        assert _format_combination("01-02-03", "unordered") == "1-2-3"

    def test_ordered_pair(self) -> None:
        assert _format_combination("01-02", "ordered") == "1→2"

    def test_ordered_triple(self) -> None:
        assert _format_combination("01-02-03", "ordered") == "1→2→3"

    def test_large_horse_numbers(self) -> None:
        assert _format_combination("14-16", "unordered") == "14-16"

    def test_leading_zeros_stripped(self) -> None:
        assert _format_combination("07-09", "unordered") == "7-9"


class TestParseOddsDict:
    def _run(
        self, raw: dict, bet_type: str = "馬連", fmt: str = "unordered"
    ) -> list[MultiOddsEntry]:
        return _parse_odds_dict(
            raw, bet_type, fmt, "202404010101", "2024-04-01 10:00:00"
        )

    def test_basic_parse(self) -> None:
        raw = {"01-02": ["15.4", "3"], "01-03": ["8.2", "1"]}
        entries = self._run(raw)
        assert len(entries) == 2
        combos = {e.combination for e in entries}
        assert combos == {"1-2", "1-3"}

    def test_odds_value(self) -> None:
        entries = self._run({"01-02": ["15.4", "3"]})
        assert entries[0].odds == pytest.approx(15.4)

    def test_popularity(self) -> None:
        entries = self._run({"01-02": ["15.4", "3"]})
        assert entries[0].popularity == 3

    def test_wide_odds_max(self) -> None:
        raw = {"01-02": ["3.5", "5.2", "2"]}
        entries = self._run(raw, bet_type="ワイド")
        assert entries[0].odds == pytest.approx(3.5)
        assert entries[0].odds_max == pytest.approx(5.2)
        assert entries[0].popularity == 2

    def test_non_wide_has_no_odds_max(self) -> None:
        entries = self._run({"01-02": ["15.4", "3"]}, bet_type="馬連")
        assert entries[0].odds_max is None

    def test_ordered_combination(self) -> None:
        raw = {"01-02": ["30.1", "4"]}
        entries = self._run(raw, bet_type="馬単", fmt="ordered")
        assert entries[0].combination == "1→2"

    def test_empty_key_skipped(self) -> None:
        raw = {"": ["15.4", "3"], "01-02": ["8.2", "1"]}
        entries = self._run(raw)
        assert len(entries) == 1

    def test_empty_vals_skipped(self) -> None:
        raw = {"01-02": [], "01-03": ["8.2", "1"]}
        entries = self._run(raw)
        assert len(entries) == 1

    def test_dash_odds_returns_none(self) -> None:
        entries = self._run({"01-02": ["---", "3"]})
        assert entries[0].odds is None

    def test_race_id_set_correctly(self) -> None:
        entries = self._run({"01-02": ["15.4", "3"]})
        assert entries[0].race_id == "202404010101"

    def test_recorded_at_set(self) -> None:
        entries = self._run({"01-02": ["15.4", "3"]})
        assert entries[0].recorded_at == "2024-04-01 10:00:00"


class TestFetchMultiOddsMock:
    @pytest.fixture(autouse=True)
    def _set_mock_env(self, monkeypatch):
        monkeypatch.setenv("UMALOGI_MOCK_MULTI_ODDS", "1")

    def test_returns_list(self) -> None:
        result = fetch_multi_odds("202404010101")
        assert isinstance(result, list)

    def test_all_bet_types_present(self) -> None:
        result = fetch_multi_odds("202404010101")
        found_types = {e.bet_type for e in result}
        expected = {bt for _, (bt, _) in _BET_TYPES.items() if bt in _MOCK_DATA or True}
        # モックデータが存在する券種を確認
        mock_bet_types = {_BET_TYPES[t][0] for t in _MOCK_DATA}
        assert found_types >= mock_bet_types

    def test_mock_entry_count(self) -> None:
        result = fetch_multi_odds("202404010101")
        expected_count = sum(len(v) for v in _MOCK_DATA.values())
        assert len(result) == expected_count

    def test_race_id_propagated(self) -> None:
        result = fetch_multi_odds("TEST_RACE_ID")
        assert all(e.race_id == "TEST_RACE_ID" for e in result)

    def test_filter_by_bet_types(self) -> None:
        result = fetch_multi_odds("202404010101", bet_types=[4])  # 馬連のみ
        assert all(e.bet_type == "馬連" for e in result)

    def test_wide_odds_max_in_mock(self) -> None:
        result = fetch_multi_odds("202404010101", bet_types=[5])  # ワイドのみ
        wide = [e for e in result if e.bet_type == "ワイド"]
        assert len(wide) > 0
        assert any(e.odds_max is not None for e in wide)

    def test_ordered_combination_in_mock(self) -> None:
        result = fetch_multi_odds("202404010101", bet_types=[6])  # 馬単
        assert all("→" in e.combination for e in result)

    def test_unordered_combination_in_mock(self) -> None:
        result = fetch_multi_odds("202404010101", bet_types=[4])  # 馬連
        assert all("-" in e.combination and "→" not in e.combination for e in result)

    def test_invalid_bet_type_ignored(self) -> None:
        result = fetch_multi_odds("202404010101", bet_types=[99])
        assert result == []
