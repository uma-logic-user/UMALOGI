"""Pure_EV_Edge のメインパイプライン配線テスト（_run_pure_ev_edge / notify）。

旧バグ（PureEVConfig(bankroll=)・PureEVBet.win_prob）の回帰防止を含む。
"""

from __future__ import annotations

import pandas as pd

import src.ml.pure_ev_edge as PE
import src.pipeline.prediction as P
from src.ml.pure_ev_edge import PureEVBet, PureEVRaceBets, CircuitBreakerStatus


def test_run_pure_ev_edge_saves_with_prob_and_flat_cost(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            {"horse_number": 1, "horse_name": "A", "win_odds": 5.0},
            {"horse_number": 2, "horse_name": "B", "win_odds": 3.0},
        ]
    )
    ev = pd.Series([2.0, 1.5])
    place = pd.Series([0.8, 0.7])

    # バンクロール取得をスタブ（PureEVConfig(initial_bankroll=) が通ること=旧bug1の回帰防止）
    monkeypatch.setattr(P, "get_current_bankroll", lambda conn: 100_000)
    # サーキットブレーカーは非発動
    monkeypatch.setattr(
        PE, "circuit_breaker_status", lambda *a, **k: CircuitBreakerStatus(False, "OK")
    )
    # 買い目を固定（単勝1・複勝1）
    monkeypatch.setattr(
        PE,
        "select_pure_ev_bets",
        lambda rid, horses, cfg: PureEVRaceBets(
            rid,
            [
                PureEVBet(rid, "単勝", 1, "A", 5.0, 0.30, 1.50, 1200),
                PureEVBet(rid, "複勝", 1, "A", 2.3, 0.80, 1.60, 900),
            ],
        ),
    )

    captured: list[dict] = []

    def _fake_insert(conn, **kw):
        captured.append(kw)
        return len(captured)

    monkeypatch.setattr(P, "insert_prediction", _fake_insert)

    out = P._run_pure_ev_edge(
        conn=None,
        race_id="202605021201",
        df=df,
        manji_ev_scores=ev,
        place_scores=place,
        suffix="(直前)",
        rdate="2026-05-31",
    )

    assert out is not None and len(out.bets) == 2
    # 単勝・複勝で各1 insert（旧bug2: b.win_prob AttributeError を起こさず prob で保存）
    assert len(captured) == 2
    by_type = {kw["bet_type"]: kw for kw in captured}
    assert set(by_type) == {"単勝", "複勝"}
    for kw in captured:
        assert kw["model_type"] == "Pure_EV_Edge(直前)"
        # 会計真コスト: 1点 → ¥100
        assert kw["recommended_bet"] == 100.0
        # model_score は prob（None でない）
        assert all(h["model_score"] is not None for h in kw["horses"])


def test_run_pure_ev_edge_skips_when_circuit_breaker_tripped(monkeypatch) -> None:
    monkeypatch.setattr(P, "get_current_bankroll", lambda conn: 100_000)
    monkeypatch.setattr(
        PE,
        "circuit_breaker_status",
        lambda *a, **k: CircuitBreakerStatus(True, "日次損失上限 到達"),
    )
    out = P._run_pure_ev_edge(
        conn=None,
        race_id="R",
        df=pd.DataFrame([{"horse_number": 1, "win_odds": 5.0}]),
        manji_ev_scores=pd.Series([2.0]),
        place_scores=pd.Series([0.8]),
        suffix="(直前)",
        rdate="2026-05-31",
    )
    assert out is None  # CB 発動で見送り


def test_notify_pure_ev_edge_sends_to_prediction(monkeypatch) -> None:
    from src.notification.router import NotificationRouter

    router = NotificationRouter()
    sent: list[str] = []

    class _Stub:
        def send_text(self, text: str) -> None:
            sent.append(text)

    monkeypatch.setattr(router, "_get", lambda ch: _Stub())
    monkeypatch.setattr(router, "_channels", {})  # ev_alert なし
    bets = PureEVRaceBets(
        "R", [PureEVBet("R", "単勝", 3, "テスト馬", 6.0, 0.25, 1.5, 1200)]
    )
    router.notify_pure_ev_edge("R", bets)
    assert sent and "Pure_EV_Edge" in sent[0] and "テスト馬" in sent[0]


def test_notify_pure_ev_edge_empty_noop(monkeypatch) -> None:
    from src.notification.router import NotificationRouter

    router = NotificationRouter()
    called = []
    monkeypatch.setattr(router, "_get", lambda ch: called.append(ch))
    router.notify_pure_ev_edge("R", PureEVRaceBets("R", []))
    assert called == []  # 買い目なしは何もしない
