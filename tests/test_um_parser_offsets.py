"""UM(競走馬マスタ DIFN:UM)パーサのバイトオフセット回帰テスト。

2026-06-07 (W-074) に実 JVLink バイトで全面是正したオフセットを、保存済み
フィクスチャ (tests/fixtures/um_sample_*.bin) に対して固定する。旧実装では
ヘッダー後の全フィールドが誤配置で racehorses が全列ゴミ化し、horse_id 名前空間も
race_results と不一致だった。本テストは再発（スライスのデグレ）を検知する。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.scraper.jravan_client import _parse_um

_FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> bytes:
    p = _FIXTURES / name
    if not p.exists():
        pytest.skip(f"フィクスチャ未配置: {name}")
    return p.read_bytes()


def test_parse_um_sample0_meiner() -> None:
    """マイネルウィルトス(2016100752)の全フィールドが正しく抽出される。"""
    rec = _parse_um(_load("um_sample_0.bin"))
    assert rec is not None
    assert rec["horse_id"] == "2016100752"
    assert rec["horse_name"] == "マイネルウィルトス"
    assert rec["sex"] == "牡"
    assert rec["birth_year"] == 2016
    assert rec["birth_month"] == 3
    assert rec["birth_date"] == "2016/03/17"
    assert rec["coat_color"] == "黒鹿毛"
    assert rec["country"] == "日本"
    assert rec["father_id"] == "1120002202"
    assert rec["father_name"] == "スクリーンヒーロー"
    assert rec["mother_name"] == "マイネボヌール"
    assert rec["grandsire_name"] == "ロージズインメイ"


def test_parse_um_sample1_patrick() -> None:
    """パトリック(2016102133)の血統が正しく抽出される。"""
    rec = _parse_um(_load("um_sample_1.bin"))
    assert rec is not None
    assert rec["horse_id"] == "2016102133"
    assert rec["horse_name"] == "パトリック"
    assert rec["birth_date"] == "2016/04/08"
    assert rec["coat_color"] == "鹿毛"
    assert rec["father_name"] == "ワークフォース"
    assert rec["grandsire_name"] == "ディープインパクト"


def test_parse_um_horse_id_format_matches_race_results() -> None:
    """horse_id は 10 桁数字・先頭ゼロ無し（race_results.horse_id と同名前空間）。"""
    for name in ("um_sample_0.bin", "um_sample_1.bin"):
        rec = _parse_um(_load(name))
        assert rec is not None
        hid = rec["horse_id"]
        assert hid.isdigit() and len(hid) == 10
        assert not hid.startswith("0")  # 旧バグでは "0201610075" 等になっていた


def test_parse_um_rejects_short_record() -> None:
    """82 バイト未満（馬名漢字まで読めない）は None を返す。"""
    assert _parse_um(b"UM4202506052016100752") is None
