"""JVRTOpen 速報オッズ取得経路のユニットテスト。

- build_rt_race_key: race_id + 日付 → 16桁速報レースキー
- parse_o1_realtime: 速報 O1 レコード（JVRTOpen 由来）→ RtdRaceInfo
- _fetch_odds_jvrt: 32bit ワーカー subprocess の JSON 出力を HorseOdds に変換

速報 O1 のレイアウトは 2026-05-31 のライブ実データ（東京2回12日1R・16頭）で実証済み:
  単勝配列 start=43 / entry=8 (馬番2 + オッズ×10 4 + 人気2) / 出走頭数=[37:39]
"""

from __future__ import annotations

import json
from unittest.mock import patch

from src.scraper.rtd_reader import build_rt_race_key, parse_o1_realtime

# 2026-05-31 東京2回12日1R のライブ速報 O1 生レコード（JVRTOpen 0B30 で実取得）。
# ヘッダ(39) + 発売フラグ等(4) + 単勝配列 16頭×8桁(128) = 先頭171文字。
_RAW = "O14202605312026053105021201053109471616777301434015020057030305500704063008050019010611991107082310084750160900340210272313113309141223521213077409140136051501350416028806"


def test_build_rt_race_key() -> None:
    # race_id(12桁) = YYYY(4)+JYO(2)+KAI(2)+NICHI(2)+RR(2)
    assert build_rt_race_key("202605021201", "20260531") == "2026053105021201"
    assert build_rt_race_key("202608031212", "20260531") == "2026053108031212"


def test_build_rt_race_key_invalid() -> None:
    assert build_rt_race_key("123", "20260531") == ""
    assert build_rt_race_key("202605021201", "") == ""


def test_parse_o1_realtime_live_record() -> None:
    info = parse_o1_realtime(_RAW, "202605021201")
    assert info is not None
    assert info.head_count == 16
    assert len(info.odds) == 16
    by_num = {o.horse_number: o for o in info.odds}
    # 1番人気=5番=1.9倍, 2番人気=9番=3.4倍（ライブ実測）
    assert by_num[5].win_odds == 1.9
    assert by_num[5].popularity == 1
    assert by_num[9].win_odds == 3.4
    assert by_num[9].popularity == 2
    # 人気順は 1..16 の順列
    pops = sorted(o.popularity for o in info.odds)
    assert pops == list(range(1, 17))


def test_parse_o1_realtime_rejects_non_o1() -> None:
    assert parse_o1_realtime("WH1xxxx", "202605021201") is None
    assert parse_o1_realtime("", "202605021201") is None


def test_fetch_odds_jvrt_parses_worker_json() -> None:
    from src.pipeline.scraping import _fetch_odds_jvrt

    payload = {
        "race_id": "202605021201",
        "head_count": 3,
        "odds": [
            {"horse_number": 1, "win_odds": 4.5, "popularity": 2},
            {"horse_number": 2, "win_odds": 1.9, "popularity": 1},
            {"horse_number": 3, "win_odds": None, "popularity": None},
        ],
    }

    class _Proc:
        returncode = 0
        stdout = "[worker] start\n" + json.dumps(payload) + "\n"
        stderr = ""

    with patch("subprocess.run", return_value=_Proc()):
        out = _fetch_odds_jvrt("202605021201", "20260531")
    assert out is not None
    assert len(out) == 3
    assert out[1].win_odds == 1.9
    assert out[1].popularity == 1


def test_fetch_odds_jvrt_returns_none_on_worker_failure() -> None:
    from src.pipeline.scraping import _fetch_odds_jvrt

    class _Proc:
        returncode = 1
        stdout = ""
        stderr = "boom"

    with patch("subprocess.run", return_value=_Proc()):
        assert _fetch_odds_jvrt("202605021201", "20260531") is None


def test_fetch_odds_jvrt_returns_none_on_timeout() -> None:
    import subprocess

    from src.pipeline.scraping import _fetch_odds_jvrt

    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("py", 60)):
        assert _fetch_odds_jvrt("202605021201", "20260531") is None
