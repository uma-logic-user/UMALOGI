"""KS(騎手)/CH(調教師)マスタ パーサのバイトオフセット回帰テスト。

2026-06-08 (W-075) に実 JVLink バイトで是正したオフセットを保存済みフィクスチャ
(tests/fixtures/ks_sample_0.bin / ch_sample_0.bin) に対して固定する。旧実装では
jockeys/trainers の name が数値ゴミ化し race_results.jockey/trainer と結合 0 件だった。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.scraper.jravan_client import _parse_ch, _parse_ks

_FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> bytes:
    p = _FIXTURES / name
    if not p.exists():
        pytest.skip(f"フィクスチャ未配置: {name}")
    return p.read_bytes()


def test_parse_ks_yano() -> None:
    """KS: 矢野貴之(05380) のコード・氏名・生年月日が正しく抽出される。"""
    rec = _parse_ks(_load("ks_sample_0.bin"))
    assert rec is not None
    assert rec["jockey_code"] == "05380"
    # 姓名間の全角空白は除去され race_results.jockey 形式に一致する。
    assert rec["jockey_name"] == "矢野貴之"
    assert "　" not in rec["jockey_name"]
    assert rec["birth_date"] == "1984/08/03"


def test_parse_ch_yamada() -> None:
    """CH: 山田信大(05713) のコード・氏名・生年月日が正しく抽出される。"""
    rec = _parse_ch(_load("ch_sample_0.bin"))
    assert rec is not None
    assert rec["trainer_code"] == "05713"
    assert rec["trainer_name"] == "山田信大"
    assert "　" not in rec["trainer_name"]
    assert rec["birth_date"] == "1974/03/08"


def test_parse_ks_name_is_japanese_not_numeric() -> None:
    """旧バグの再発検知: 氏名が数値ゴミ(例 '80200403...')でないこと。"""
    rec = _parse_ks(_load("ks_sample_0.bin"))
    assert rec is not None
    assert not rec["jockey_name"].isdigit()
    assert any("一" <= ch <= "鿿" for ch in rec["jockey_name"])  # 漢字を含む


def test_parse_ks_rejects_short_record() -> None:
    assert _parse_ks(b"KS2202506050538") is None
