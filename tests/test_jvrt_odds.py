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


# 2026-05-31 東京2回12日1R のライブ WH（馬体重）レイアウトを再現した合成バイト列。
# ヘッダ35バイト + 馬体重情報(45バイト×3頭)。馬名は 36バイトのダミー全角で代替。
_WH_HEADER = b"WH1" + b"20260531" + b"2026053105021201" + b"00000000"  # 35 bytes
_WH_NAME = b"\x81\x40" * 18  # 全角空白 18 文字 = 36 バイト
_WH_RAW = (
    _WH_HEADER
    + b"01"
    + _WH_NAME
    + b"482"
    + b"-"
    + b"002"  # 馬番1: 482kg(-2)
    + b"02"
    + _WH_NAME
    + b"492"
    + b"+"
    + b"012"  # 馬番2: 492kg(+12)
    + b"03"
    + _WH_NAME
    + b"000"
    + b" "
    + b"   "  # 馬番3: 未計測(None)
)


def test_parse_wh_realtime_live_layout() -> None:
    from src.scraper.rtd_reader import parse_wh_realtime

    out = parse_wh_realtime(_WH_RAW, "202605021201")
    assert out[1] == {"weight": 482, "weight_diff": -2}
    assert out[2] == {"weight": 492, "weight_diff": 12}
    assert out[3] == {"weight": None, "weight_diff": None}


def test_parse_wh_realtime_rejects_non_wh() -> None:
    from src.scraper.rtd_reader import parse_wh_realtime

    assert parse_wh_realtime(b"O1xxxx", "202605021201") == {}
    assert parse_wh_realtime(b"", "202605021201") == {}


def _make_payload() -> dict:
    return {
        "race_id": "202605021201",
        "head_count": 3,
        "odds": [
            {"horse_number": 1, "win_odds": 4.5, "popularity": 2},
            {"horse_number": 2, "win_odds": 1.9, "popularity": 1},
            {"horse_number": 3, "win_odds": None, "popularity": None},
        ],
        "weights": {
            "1": {"weight": 482, "weight_diff": -2},
            "2": {"weight": 492, "weight_diff": 12},
        },
        "weather": "晴",
        "condition": "良",
    }


def test_run_jvrt_worker_parses_json() -> None:
    from src.pipeline.scraping import _run_jvrt_worker

    class _Proc:
        returncode = 0
        stdout = (
            "[worker] start\n" + json.dumps(_make_payload(), ensure_ascii=False) + "\n"
        )
        stderr = ""

    with patch("subprocess.run", return_value=_Proc()):
        payload = _run_jvrt_worker("202605021201", "20260531")
    assert payload is not None
    assert payload["weather"] == "晴"
    assert payload["weights"]["1"]["weight"] == 482


def test_run_jvrt_worker_none_on_failure_and_timeout() -> None:
    import subprocess

    from src.pipeline.scraping import _run_jvrt_worker

    class _Fail:
        returncode = 1
        stdout = ""
        stderr = "boom"

    with patch("subprocess.run", return_value=_Fail()):
        assert _run_jvrt_worker("202605021201", "20260531") is None
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("py", 90)):
        assert _run_jvrt_worker("202605021201", "20260531") is None


def test_payload_to_horse_odds() -> None:
    from src.pipeline.scraping import _payload_to_horse_odds

    out = _payload_to_horse_odds(_make_payload())
    assert out is not None and len(out) == 3
    assert out[1].win_odds == 1.9
    assert _payload_to_horse_odds(None) is None
    assert _payload_to_horse_odds({"odds": []}) is None


def test_apply_jvrt_weight_weather_updates_db() -> None:
    import sqlite3

    from src.pipeline.scraping import _apply_jvrt_weight_weather

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER,
            horse_weight INTEGER, horse_weight_diff INTEGER
        );
        CREATE TABLE races (race_id TEXT, weather TEXT, condition TEXT);
    """)
    conn.execute("INSERT INTO entries VALUES ('202605021201', 1, NULL, NULL)")
    conn.execute("INSERT INTO entries VALUES ('202605021201', 2, 999, 9)")
    conn.execute("INSERT INTO races VALUES ('202605021201', '', '')")
    conn.commit()

    _apply_jvrt_weight_weather(conn, "202605021201", _make_payload())

    w1 = conn.execute(
        "SELECT horse_weight, horse_weight_diff FROM entries WHERE horse_number=1"
    ).fetchone()
    assert w1 == (482, -2)
    weather, cond = conn.execute(
        "SELECT weather, condition FROM races WHERE race_id='202605021201'"
    ).fetchone()
    assert weather == "晴" and cond == "良"


def test_apply_jvrt_weight_weather_no_overwrite_with_empty() -> None:
    import sqlite3

    from src.pipeline.scraping import _apply_jvrt_weight_weather

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER,
            horse_weight INTEGER, horse_weight_diff INTEGER
        );
        CREATE TABLE races (race_id TEXT, weather TEXT, condition TEXT);
    """)
    conn.execute("INSERT INTO entries VALUES ('R', 1, 500, 4)")
    conn.execute("INSERT INTO races VALUES ('R', '晴', '良')")
    conn.commit()

    # weight=None / 空天候 では既存値を上書きしない
    payload = {
        "weights": {"1": {"weight": None, "weight_diff": None}},
        "weather": "",
        "condition": "",
    }
    _apply_jvrt_weight_weather(conn, "R", payload)
    assert (
        conn.execute(
            "SELECT horse_weight FROM entries WHERE horse_number=1"
        ).fetchone()[0]
        == 500
    )
    assert (
        conn.execute("SELECT weather FROM races WHERE race_id='R'").fetchone()[0]
        == "晴"
    )
