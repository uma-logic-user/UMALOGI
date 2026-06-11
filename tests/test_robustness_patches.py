"""自動運用クラッシュ・エッジケース堅牢化パッチのリグレッションテスト（2026-06-11）。

対象パッチ:
  1. rtd_reader: 発走直後の RTD ファイル削除と stat() の TOCTOU 競合でクラッシュしない
  2. rtd_reader: 想定外ファイル名からゴミ race_id を生成して DB を汚染しない
  3. entry_table.fetch_realtime_odds: netkeiba API の形状変化（list 応答・非 list 値）で
     AttributeError / KeyError 死しない
  4. sns_generator.normalize_date8: 不正日付引数で日次バッチがトレースバック死しない
"""

from __future__ import annotations

import sys
import zlib
from datetime import date as dt_date
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.marketing.sns_generator import normalize_date8
from src.scraper.rtd_reader import (
    _race_id_from_filename,
    read_all_rtd_for_date,
    read_rtd_for_race,
)


def _make_rtd(tmp_path: Path, filename: str) -> Path:
    """最小限の O1 レコードを持つ zlib 圧縮 RTD ファイルを生成する。"""
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


# ── パッチ1: stat() TOCTOU ────────────────────────────────────────────────
class TestRtdStatRaceCondition:
    def test_stat_oserror_does_not_crash(self, tmp_path: Path) -> None:
        """glob 後にファイルが消えて stat が失敗しても例外を投げない。"""
        _make_rtd(tmp_path, "0B12202605030811.rtd")
        with (
            patch("src.scraper.rtd_reader._RTD_DIR", tmp_path),
            patch.object(Path, "stat", side_effect=OSError("deleted")),
        ):
            # stat 全滅でも mtime=0.0 扱いで先頭候補を読みに行き、クラッシュしない
            result = read_rtd_for_race("202608030411")
        assert result is not None

    def test_stat_oserror_with_unreadable_file(self, tmp_path: Path) -> None:
        """stat も read も失敗（完全削除相当）なら None を返す。"""
        _make_rtd(tmp_path, "0B12202605030811.rtd")
        with (
            patch("src.scraper.rtd_reader._RTD_DIR", tmp_path),
            patch.object(Path, "stat", side_effect=OSError("deleted")),
            patch.object(Path, "read_bytes", side_effect=FileNotFoundError),
        ):
            result = read_rtd_for_race("202608030411")
        assert result is None


# ── パッチ2: 想定外ファイル名 ─────────────────────────────────────────────
class TestRaceIdFilenameValidation:
    def test_unexpected_length_returns_empty(self) -> None:
        assert _race_id_from_filename("0B30202604190301") != "0B30"  # 16桁は新扱い
        assert _race_id_from_filename("0B3020260419030104") == ""  # 18桁=不正
        assert _race_id_from_filename("0B30tmp_backup_file1") == ""  # 非数字

    def test_old_format_still_works(self) -> None:
        assert _race_id_from_filename("0B302026041903010401") == "202603010401"

    def test_read_all_skips_garbage_filenames(self, tmp_path: Path) -> None:
        """不正ファイル名はスキップされ、正常ファイルのみ返る。"""
        _make_rtd(tmp_path, "0B302026050308030411.rtd")
        _make_rtd(tmp_path, "0B3020260503080304.rtd")  # 18桁=不正
        with patch("src.scraper.rtd_reader._RTD_DIR", tmp_path):
            result = read_all_rtd_for_date("20260503")
        assert list(result.keys()) == ["202608030411"]


# ── パッチ3: netkeiba オッズ API 形状変化 ─────────────────────────────────
class TestFetchRealtimeOddsShapeGuard:
    def _run(self, payload: str) -> list:
        from src.scraper import entry_table

        with (
            patch.object(entry_table, "_fetch", return_value=payload),
            patch("time.sleep"),
        ):
            return entry_table.fetch_realtime_odds("202608030411", delay=0)

    def test_list_response_returns_empty(self) -> None:
        """JSON ルートが list（メンテナンス応答等）でもクラッシュしない。"""
        assert self._run("[]") == []

    def test_null_data_block(self) -> None:
        """data ブロックが null でもクラッシュしない。"""
        assert self._run('{"status": "NG", "data": null}') == []

    def test_non_list_horse_values_skipped(self) -> None:
        """馬別データが dict / 文字列でも KeyError 死せずスキップする。"""
        payload = '{"1": {"01": {"odd": "3.8"}, "02": ["5.1", "", "1"]}}'
        result = self._run(payload)
        assert [h.horse_number for h in result] == [2]
        assert result[0].win_odds == 5.1

    def test_normal_response_still_parsed(self) -> None:
        payload = '{"1": {"01": ["3.8", "", "3"], "02": ["5.1", "", "1"]}}'
        result = self._run(payload)
        assert len(result) == 2
        assert result[0].win_odds == 3.8


# ── パッチ4: 日付正規化 ──────────────────────────────────────────────────
class TestNormalizeDate8:
    def test_accepts_various_formats(self) -> None:
        expected = dt_date(2026, 6, 14)
        assert normalize_date8("20260614") == expected
        assert normalize_date8("2026-06-14") == expected
        assert normalize_date8("2026/06/14") == expected

    def test_rejects_invalid(self) -> None:
        assert normalize_date8(None) is None
        assert normalize_date8("") is None
        assert normalize_date8("202606") is None  # 桁不足
        assert normalize_date8("20260632") is None  # 非実在日
        assert normalize_date8("abcdefgh") is None
