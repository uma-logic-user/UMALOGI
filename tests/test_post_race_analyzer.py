"""src/analysis/post_race_analyzer.py のユニットテスト。

敗因分析の純データ層（extract_missed_races / build_analysis_prompt）と、
Claude API・Discord 連携の注入ポイント（analyze_losses / post_analysis_to_discord）を
検証する。実 API・実 Webhook には一切アクセスしない（フェイクを注入）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from src.analysis import post_race_analyzer as pra
from src.database.schema import DDL_STATEMENTS


@pytest.fixture()
def conn() -> Iterator[sqlite3.Connection]:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    for ddl in DDL_STATEMENTS:
        c.execute(ddl)
    c.commit()
    yield c
    c.close()


def _add_race(c: sqlite3.Connection, race_id: str, date: str = "2026-05-31") -> None:
    c.execute(
        "INSERT INTO races(race_id, race_name, date, venue, race_number, "
        "distance, surface) VALUES(?,?,?,?,?,?,?)",
        (race_id, "テストS", date, "東京", int(race_id[10:12]), 1600, "芝"),
    )


def _add_result(
    c: sqlite3.Connection,
    race_id: str,
    horse_name: str,
    rank: int | None,
    horse_number: int,
    win_odds: float,
    popularity: int,
) -> None:
    c.execute(
        "INSERT INTO race_results(race_id, horse_name, rank, horse_number, "
        "win_odds, popularity) VALUES(?,?,?,?,?,?)",
        (race_id, horse_name, rank, horse_number, win_odds, popularity),
    )


def _add_pred(
    c: sqlite3.Connection,
    race_id: str,
    ev: float,
    horse_name: str,
    *,
    is_hit: int,
    is_superseded: int = 0,
    bet_type: str = "単勝",
    notes: str = "期待値妙味",
) -> int:
    cur = c.execute(
        "INSERT INTO predictions(race_id, model_type, bet_type, confidence, "
        "expected_value, notes, is_superseded) VALUES(?,?,?,?,?,?,?)",
        (race_id, "本命(直前)", bet_type, 0.5, ev, notes, is_superseded),
    )
    pid = int(cur.lastrowid or 0)
    c.execute(
        "INSERT INTO prediction_horses(prediction_id, horse_name, predicted_rank, "
        "model_score, ev_score) VALUES(?,?,1,?,?)",
        (pid, horse_name, 0.4, ev),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id, is_hit, payout, profit) "
        "VALUES(?,?,?,?)",
        (pid, is_hit, 0.0 if not is_hit else 500.0, -100.0 if not is_hit else 400.0),
    )
    return pid


# ── extract_missed_races ─────────────────────────────────────────────────────


def test_extract_only_ev_ge_threshold_and_missed(conn: sqlite3.Connection) -> None:
    """EV>=1.0 かつ 不的中 の予想だけが抽出される。"""
    rid = "202605010111"
    _add_race(conn, rid)
    _add_result(conn, rid, "本命馬", 5, 7, 4.5, 2)  # 予想した本命は5着
    _add_result(conn, rid, "勝った馬", 1, 3, 12.0, 6)  # 実際の勝ち馬
    _add_pred(conn, rid, ev=1.8, horse_name="本命馬", is_hit=0)  # 対象

    # 除外対象たち
    rid2 = "202605010112"
    _add_race(conn, rid2)
    _add_result(conn, rid2, "的中本命", 1, 1, 2.0, 1)
    _add_pred(conn, rid2, ev=1.5, horse_name="的中本命", is_hit=1)  # 的中→除外

    rid3 = "202605010113"
    _add_race(conn, rid3)
    _add_result(conn, rid3, "低EV馬", 8, 2, 3.0, 1)
    _add_pred(conn, rid3, ev=0.7, horse_name="低EV馬", is_hit=0)  # EV<1.0→除外
    conn.commit()

    rows = pra.extract_missed_races(conn, limit=50)

    assert len(rows) == 1
    r = rows[0]
    assert r["race_id"] == rid
    assert r["horse_name"] == "本命馬"
    assert r["expected_value"] == 1.8
    assert r["winner_name"] == "勝った馬"


def test_extract_excludes_superseded(conn: sqlite3.Connection) -> None:
    """is_superseded=1（再推論で無効化）の予想は除外する。"""
    rid = "202605010111"
    _add_race(conn, rid)
    _add_result(conn, rid, "無効本命", 9, 4, 5.0, 3)
    _add_pred(conn, rid, ev=2.0, horse_name="無効本命", is_hit=0, is_superseded=1)
    conn.commit()

    assert pra.extract_missed_races(conn, limit=50) == []


def test_extract_respects_custom_threshold(conn: sqlite3.Connection) -> None:
    """ev_threshold を引き上げると低EVの不的中は除外される。"""
    rid = "202605010111"
    _add_race(conn, rid)
    _add_result(conn, rid, "本命馬", 4, 7, 4.5, 2)
    _add_pred(conn, rid, ev=1.2, horse_name="本命馬", is_hit=0)
    conn.commit()

    assert pra.extract_missed_races(conn, ev_threshold=1.5, limit=50) == []
    assert len(pra.extract_missed_races(conn, ev_threshold=1.0, limit=50)) == 1


# ── build_analysis_prompt ────────────────────────────────────────────────────


def test_build_prompt_contains_key_fields() -> None:
    """プロンプトにオッズ・人気・結果・予想根拠が含まれる。"""
    missed = [
        {
            "race_id": "202605010111",
            "date": "2026-05-31",
            "venue": "東京",
            "race_number": 11,
            "model_type": "本命(直前)",
            "bet_type": "単勝",
            "expected_value": 1.8,
            "horse_name": "本命馬",
            "notes": "期待値妙味",
            "pred_win_odds": 4.5,
            "pred_popularity": 2,
            "actual_rank": 5,
            "winner_name": "勝った馬",
            "winner_odds": 12.0,
            "winner_popularity": 6,
        }
    ]
    prompt = pra.build_analysis_prompt(missed)

    assert "本命馬" in prompt
    assert "4.5" in prompt  # 予想オッズ
    assert "勝った馬" in prompt  # 結果
    assert "期待値妙味" in prompt  # 予想根拠
    assert "敗因" in prompt  # 分析依頼の主旨


# ── analyze_losses（Claude API 注入） ────────────────────────────────────────


class _FakeBlock:
    def __init__(self, type_: str, text: str = "") -> None:
        self.type = type_
        self.text = text


class _FakeMessages:
    def __init__(self, captured: dict[str, Any], reply: str) -> None:
        self._captured = captured
        self._reply = reply

    def create(self, **kwargs: Any) -> Any:
        self._captured.update(kwargs)

        class _Resp:
            content = [_FakeBlock("thinking", ""), _FakeBlock("text", self._reply)]

        return _Resp()


class _FakeClient:
    def __init__(self, reply: str) -> None:
        self.captured: dict[str, Any] = {}
        self.messages = _FakeMessages(self.captured, reply)


def test_analyze_losses_calls_claude_and_returns_text() -> None:
    """注入したクライアントで Claude を呼び、text ブロックを返す。"""
    missed = [
        {
            "race_id": "202605010111", "date": "2026-05-31", "venue": "東京",
            "race_number": 11, "model_type": "本命(直前)", "bet_type": "単勝",
            "expected_value": 1.8, "horse_name": "本命馬", "notes": "妙味",
            "pred_win_odds": 4.5, "pred_popularity": 2, "actual_rank": 5,
            "winner_name": "勝った馬", "winner_odds": 12.0, "winner_popularity": 6,
        }
    ]
    client = _FakeClient(reply="【敗因分析】人気薄の伏兵に足元をすくわれた典型。")

    result = pra.analyze_losses(missed, client=client)

    assert "敗因分析" in result
    assert client.captured["model"] == "claude-opus-4-8"
    # adaptive thinking がオンであること（claude-api スキル準拠）。
    assert client.captured["thinking"]["type"] == "adaptive"


def test_analyze_losses_empty_skips_api() -> None:
    """対象ゼロ件なら API を呼ばずに既定メッセージを返す。"""
    client = _FakeClient(reply="呼ばれないはず")
    result = pra.analyze_losses([], client=client)

    assert "分析対象" in result
    assert client.captured == {}  # API 未呼び出し


# ── post_analysis_to_discord（通知注入） ─────────────────────────────────────


class _FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_text(self, text: str) -> None:
        self.sent.append(text)


def test_post_analysis_to_discord_sends_text() -> None:
    notifier = _FakeNotifier()
    ok = pra.post_analysis_to_discord("敗因レポート本文", notifier=notifier)

    assert ok is True
    assert len(notifier.sent) == 1
    # レポートヘッダー付きで本文が含まれること。
    assert "敗因レポート本文" in notifier.sent[0]
    assert "敗因分析" in notifier.sent[0]


def test_post_analysis_to_discord_skips_empty() -> None:
    notifier = _FakeNotifier()
    ok = pra.post_analysis_to_discord("   ", notifier=notifier)

    assert ok is False
    assert notifier.sent == []
