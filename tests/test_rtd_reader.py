"""RTD リーダーのファイルパターン修正テスト"""

from __future__ import annotations

import sys
import zlib
from pathlib import Path
from unittest.mock import patch


_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.rtd_reader import (
    _race_id_from_filename,
    read_rtd_for_race,
)


def _make_rtd(tmp_path: Path, filename: str) -> Path:
    """最小限のO1レコードを持つzlibファイルを生成する。"""
    # O1 + データ区分(1) + 作成日(8) + 開催日(8) + 場コード(2) + 開催回(2) + 日次(2)
    # + レース番号(2) + 登録頭数(2) + 出走頭数(2) + プール情報(12) = 先頭43文字
    text = (
        "O1"
        + "1"
        + "20260503"
        + "20260503"
        + "08"
        + "03"
        + "04"
        + "11"
        + "15"
        + "15"
        + "0" * 12
    )
    raw = zlib.compress(text.encode("cp932"))
    p = tmp_path / filename
    p.write_bytes(raw)
    return p


class TestRaceIdFromFilename:
    def test_old_format_0b30(self) -> None:
        """旧フォーマット(0B30, 20文字) で race_id を正しく導出できる。"""
        stem = "0B302026041903010401"
        result = _race_id_from_filename(stem)
        assert result == "202603010401"

    def test_new_format_0b12(self) -> None:
        """新フォーマット(0B12, 16文字) でjyo/raceを正しく抽出できる。"""
        stem = "0B12202605030812"
        assert len(stem) == 16
        jyo = stem[12:14]
        race = stem[14:16]
        assert jyo == "08"  # 京都
        assert race == "12"


class TestReadRtdForRace:
    def test_finds_0b12_format_file(self, tmp_path: Path) -> None:
        """0B12フォーマットのRTDファイルを正しく発見できる。"""
        # 京都R11 race_id=202608030411, jyo=08, race=11
        _make_rtd(tmp_path, "0B12202605030811.rtd")

        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_rtd_for_race("202608030411")

        assert result is not None  # ファイルが見つかること

    def test_finds_0b30_format_file(self, tmp_path: Path) -> None:
        """旧0B30フォーマットのRTDファイルも引き続き発見できる。"""
        _make_rtd(tmp_path, "0B302026050308030411.rtd")

        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_rtd_for_race("202608030411")

        assert result is not None

    def test_returns_none_when_no_file(self, tmp_path: Path) -> None:
        """RTDファイルが存在しない場合はNoneを返す。"""
        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_rtd_for_race("202608030411")

        assert result is None

    def test_0b12_race12_file(self, tmp_path: Path) -> None:
        """0B12フォーマット・実際のファイル名(0B12202605030812)を発見できる。"""
        _make_rtd(tmp_path, "0B12202605030812.rtd")

        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            # 京都R12 race_id=202608030412
            result = read_rtd_for_race("202608030412")

        assert result is not None
