"""グランドスラム Phase2: 異常系・境界値エッジケース大量テスト（50+）。

対象: pure_ev_edge / pnl_accounting / health_reporter / サーキットブレーカー /
      ネットワーク切断・DBロック競合・大ドローダウン等の異常系。

calibrate_win_prob は較正器pklの有無で値が変わるため、決定性確保のためモックする。
"""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

import src.ml.pure_ev_edge as PE
from src.ml.pure_ev_edge import (
    PureEVConfig,
    evaluate_circuit_breaker,
    fukusho_ev,
    is_locked_bet_type,
    kelly_stake,
    select_pure_ev_bets,
    tansho_ev,
)


# ─────────────────────────────────────────────────────────────────────
# 1. tansho_ev / fukusho_ev 境界値
# ─────────────────────────────────────────────────────────────────────
def test_tansho_ev_odds_le_one_returns_zero() -> None:
    assert tansho_ev(2.0, 1.0) == (0.0, 0.0)
    assert tansho_ev(2.0, 0.5) == (0.0, 0.0)
    assert tansho_ev(2.0, 0.0) == (0.0, 0.0)
    assert tansho_ev(2.0, -3.0) == (0.0, 0.0)


def test_tansho_ev_ev_equals_prob_times_odds() -> None:
    with patch.object(PE, "calibrate_win_prob", return_value=0.25):
        p, ev = tansho_ev(2.0, 4.0)
    assert p == 0.25
    assert ev == pytest.approx(1.0)


def test_tansho_ev_zero_prob() -> None:
    with patch.object(PE, "calibrate_win_prob", return_value=0.0):
        p, ev = tansho_ev(0.0, 10.0)
    assert (p, ev) == (0.0, 0.0)


def test_fukusho_ev_estimated_odds_capped() -> None:
    # 高オッズ×高確率 → EV は _FUKUSHO_EV_CAP(3.0) で頭打ち
    p, ev = fukusho_ev(place_prob=0.95, win_odds=50.0, place_payout_scale=0.33)
    assert ev <= PE._FUKUSHO_EV_CAP + 1e-9


def test_fukusho_ev_explicit_odds_takes_precedence() -> None:
    p, ev = fukusho_ev(place_prob=0.5, win_odds=10.0, place_odds=2.0)
    assert ev == pytest.approx(1.0)


def test_fukusho_ev_invalid_place_odds_falls_back_to_estimate() -> None:
    p, ev = fukusho_ev(place_prob=0.5, win_odds=4.0, place_odds=0.0)
    eff = 1.0 + (4.0 - 1.0) * PE._DEFAULT_PLACE_SCALE
    assert ev == pytest.approx(min(0.5 * eff, PE._FUKUSHO_EV_CAP))


def test_fukusho_ev_zero_prob_zero_ev() -> None:
    p, ev = fukusho_ev(place_prob=0.0, win_odds=8.0)
    assert ev == 0.0


# ─────────────────────────────────────────────────────────────────────
# 2. kelly_stake 境界値・cap・丸め
# ─────────────────────────────────────────────────────────────────────
def test_kelly_zero_on_non_positive_edge() -> None:
    assert kelly_stake(ev=1.0, odds=5.0, bankroll=100_000, bet_type="単勝") == 0
    assert kelly_stake(ev=0.8, odds=5.0, bankroll=100_000, bet_type="単勝") == 0


def test_kelly_zero_on_invalid_odds() -> None:
    assert kelly_stake(ev=2.0, odds=1.0, bankroll=100_000, bet_type="単勝") == 0
    assert kelly_stake(ev=2.0, odds=0.0, bankroll=100_000, bet_type="単勝") == 0


def test_kelly_rounds_down_to_100() -> None:
    # f=(1.5-1)/(5-1)=0.125, ×0.1=0.0125, ×100000=1250 → cap 単勝2%=2000 → 1250→1200
    stake = kelly_stake(
        ev=1.5, odds=5.0, bankroll=100_000, bet_type="単勝", kelly_fraction=0.10
    )
    assert stake == 1200


def test_kelly_type_cap_tansho_2pct() -> None:
    stake = kelly_stake(ev=10.0, odds=2.0, bankroll=100_000, bet_type="単勝")
    assert stake <= 100_000 * 0.02


def test_kelly_type_cap_fukusho_3pct() -> None:
    stake = kelly_stake(ev=10.0, odds=2.0, bankroll=100_000, bet_type="複勝")
    assert stake <= 100_000 * 0.03


def test_kelly_unknown_type_default_cap() -> None:
    stake = kelly_stake(ev=10.0, odds=2.0, bankroll=100_000, bet_type="三連単")
    assert stake <= 100_000 * 0.02  # _KELLY_TYPE_CAP.get default 0.02


def test_kelly_below_100_returns_zero() -> None:
    # 極小バンクロールで 100円未満 → 0
    assert kelly_stake(ev=1.05, odds=20.0, bankroll=1_000, bet_type="単勝") == 0


def test_kelly_zero_bankroll() -> None:
    assert kelly_stake(ev=2.0, odds=3.0, bankroll=0, bet_type="単勝") == 0


# ─────────────────────────────────────────────────────────────────────
# 3. select_pure_ev_bets エッジケース
# ─────────────────────────────────────────────────────────────────────
def _horse(num, odds, mev=2.0, pprob=0.8, name=None):
    return {
        "horse_number": num,
        "horse_name": name or f"H{num}",
        "win_odds": odds,
        "manji_ev_score": mev,
        "place_prob": pprob,
    }


def test_select_empty_horses() -> None:
    bets = select_pure_ev_bets("R", [])
    assert bets.bets == []


def test_select_only_tansho_fukusho_types() -> None:
    with patch.object(PE, "calibrate_win_prob", return_value=0.4):
        bets = select_pure_ev_bets("R", [_horse(i, 5.0) for i in range(1, 9)])
    assert all(b.bet_type in ("単勝", "複勝") for b in bets.bets)


def test_select_below_threshold_excluded() -> None:
    # 較正P低 → EV<1.15 で除外
    with patch.object(PE, "calibrate_win_prob", return_value=0.05):
        bets = select_pure_ev_bets("R", [_horse(1, 1.2, pprob=0.1)])
    assert bets.bets == []


def test_select_prob_floor_excludes_longshot() -> None:
    # prob_floor=0.06 未満は大穴として除外（EVが高くても）
    with patch.object(PE, "calibrate_win_prob", return_value=0.01):
        bets = select_pure_ev_bets("R", [_horse(1, 200.0, pprob=0.9)])
    assert bets.bets == []


def test_select_max_bets_cap() -> None:
    cfg = PureEVConfig(max_bets_per_race=2)
    with patch.object(PE, "calibrate_win_prob", return_value=0.5):
        bets = select_pure_ev_bets("R", [_horse(i, 6.0) for i in range(1, 9)], cfg)
    assert len(bets.bets) <= 2


def test_select_invalid_fields_skipped() -> None:
    bad = [{"horse_number": "x", "win_odds": "y"}, {"horse_number": 0, "win_odds": 5.0}]
    with patch.object(PE, "calibrate_win_prob", return_value=0.5):
        bets = select_pure_ev_bets("R", bad)
    assert bets.bets == []


def test_select_sorted_by_ev_desc() -> None:
    with patch.object(
        PE, "calibrate_win_prob", side_effect=lambda ev, o: min(ev / 10, 0.5)
    ):
        bets = select_pure_ev_bets(
            "R",
            [_horse(1, 4.0, mev=2.0), _horse(2, 10.0, mev=5.0)],
            PureEVConfig(max_bets_per_race=10),
        )
    evs = [b.expected_value for b in bets.bets]
    assert evs == sorted(evs, reverse=True)


def test_select_missing_optional_fields_default_zero() -> None:
    h = {"horse_number": 1, "horse_name": "A", "win_odds": 5.0}  # mev/pprob 欠如
    with patch.object(PE, "calibrate_win_prob", return_value=0.0):
        bets = select_pure_ev_bets("R", [h])
    assert bets.bets == []  # mev=0 → EV低 → 除外（例外なし）


# ─────────────────────────────────────────────────────────────────────
# 4. サーキットブレーカー（大ドローダウン）境界値
# ─────────────────────────────────────────────────────────────────────
def test_cb_daily_trip_at_exact_limit() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000, daily_loss_limit_pct=0.05)
    st = evaluate_circuit_breaker(daily_pnl=-5000, weekly_pnl=-5000, config=cfg)
    assert st.tripped and "日次" in st.reason


def test_cb_daily_just_below_limit_ok() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000, daily_loss_limit_pct=0.05)
    st = evaluate_circuit_breaker(daily_pnl=-4999, weekly_pnl=-4999, config=cfg)
    assert not st.tripped


def test_cb_weekly_trip() -> None:
    cfg = PureEVConfig(
        initial_bankroll=100_000, daily_loss_limit_pct=0.05, weekly_loss_limit_pct=0.12
    )
    st = evaluate_circuit_breaker(daily_pnl=-1000, weekly_pnl=-12000, config=cfg)
    assert st.tripped and "週次" in st.reason


def test_cb_daily_takes_precedence_over_weekly() -> None:
    cfg = PureEVConfig(
        initial_bankroll=100_000, daily_loss_limit_pct=0.05, weekly_loss_limit_pct=0.12
    )
    st = evaluate_circuit_breaker(daily_pnl=-9000, weekly_pnl=-20000, config=cfg)
    assert st.tripped and "日次" in st.reason


def test_cb_positive_pnl_no_trip() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000)
    st = evaluate_circuit_breaker(daily_pnl=50_000, weekly_pnl=80_000, config=cfg)
    assert not st.tripped and st.daily_loss == 0.0


def test_cb_extreme_drawdown() -> None:
    cfg = PureEVConfig(initial_bankroll=100_000)
    st = evaluate_circuit_breaker(daily_pnl=-999_999, weekly_pnl=-999_999, config=cfg)
    assert st.tripped


def test_is_locked_bet_type() -> None:
    assert is_locked_bet_type("単勝") and is_locked_bet_type("複勝")
    for bt in ("馬連", "ワイド", "馬単", "三連複", "三連単", "WIN5", ""):
        assert not is_locked_bet_type(bt)


# ─────────────────────────────────────────────────────────────────────
# 5. circuit_breaker_status（DB・期間集計）
# ─────────────────────────────────────────────────────────────────────
@pytest.fixture
def pe_conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT);
        CREATE TABLE predictions (id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT);
        CREATE TABLE prediction_results (id INTEGER PRIMARY KEY, prediction_id INTEGER, profit REAL);
        """
    )
    return c


def _pe_row(c, pid, rid, date, model, profit):
    c.execute("INSERT OR IGNORE INTO races VALUES (?,?)", (rid, date))
    c.execute("INSERT INTO predictions VALUES (?,?,?)", (pid, rid, model))
    c.execute("INSERT INTO prediction_results VALUES (?,?,?)", (pid, pid, profit))


def test_cb_status_no_data_ok(pe_conn) -> None:
    st = PE.circuit_breaker_status(
        pe_conn, "2026-06-01", PureEVConfig(initial_bankroll=100_000)
    )
    assert not st.tripped


def test_cb_status_daily_loss_trips(pe_conn) -> None:
    _pe_row(pe_conn, 1, "R1", "2026-06-01", "Pure_EV_Edge(直前)", -9000)
    pe_conn.commit()
    st = PE.circuit_breaker_status(
        pe_conn,
        "2026-06-01",
        PureEVConfig(initial_bankroll=100_000, daily_loss_limit_pct=0.05),
    )
    assert st.tripped


def test_cb_status_excludes_other_models(pe_conn) -> None:
    # 卍の大損は Pure_EV_Edge のCB集計に含まれない
    _pe_row(pe_conn, 1, "R1", "2026-06-01", "卍(直前)", -50000)
    pe_conn.commit()
    st = PE.circuit_breaker_status(
        pe_conn, "2026-06-01", PureEVConfig(initial_bankroll=100_000)
    )
    assert not st.tripped


# ─────────────────────────────────────────────────────────────────────
# 6. pnl_accounting エッジケース
# ─────────────────────────────────────────────────────────────────────
@pytest.fixture
def acc_conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE predictions (id INTEGER PRIMARY KEY, race_id TEXT, model_type TEXT,
            bet_type TEXT, created_at TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE prediction_results (id INTEGER PRIMARY KEY, prediction_id INTEGER,
            is_hit INTEGER, payout REAL, profit REAL);
        """
    )
    return c


def _acc(c, pid, model, bet, payout, profit, hit=0, sup=0, rid="R"):
    c.execute(
        "INSERT INTO predictions(id,race_id,model_type,bet_type,created_at,is_superseded) VALUES(?,?,?,?,?,?)",
        (pid, rid, model, bet, "2026-06-01 10:00", sup),
    )
    c.execute(
        "INSERT INTO prediction_results(prediction_id,is_hit,payout,profit) VALUES(?,?,?,?)",
        (pid, hit, payout, profit),
    )


def test_live_roi_empty_db(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    r = compute_live_roi(acc_conn)
    assert r["n"] == 0 and r["roi"] == 0.0 and r["cost"] == 0


def test_live_roi_all_superseded_excluded(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    _acc(acc_conn, 1, "本命(直前)", "単勝", 500, 400, 1, sup=1)
    acc_conn.commit()
    assert compute_live_roi(acc_conn)["n"] == 0


def test_live_roi_only_exotics_zero(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    _acc(acc_conn, 1, "本命(直前)", "三連単", 0, -5000, 0)
    acc_conn.commit()
    assert compute_live_roi(acc_conn)["n"] == 0  # 単複でないため実弾0


def test_live_roi_ornamental_excluded(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    _acc(acc_conn, 1, "Oracle(直前)", "単勝", 500, 400, 1)
    acc_conn.commit()
    assert compute_live_roi(acc_conn)["n"] == 0  # 観賞用は実弾外


def test_live_roi_since_filter(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    acc_conn.execute(
        "INSERT INTO predictions(id,race_id,model_type,bet_type,created_at,is_superseded)"
        " VALUES(1,'R','Pure_EV_Edge(直前)','単勝','2026-05-01 10:00',0)"
    )
    acc_conn.execute("INSERT INTO prediction_results VALUES(1,1,1,500,400)")
    acc_conn.commit()
    assert compute_live_roi(acc_conn, since="2026-06-01")["n"] == 0
    assert compute_live_roi(acc_conn, since="2026-04-01")["n"] == 1


def test_live_roi_live_only_false_includes_exotics(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    _acc(acc_conn, 1, "Oracle(直前)", "三連単", 0, -5000, 0)
    acc_conn.commit()
    assert compute_live_roi(acc_conn, live_only=False)["n"] == 1


def test_live_roi_zero_cost_no_div_error(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_live_roi

    # payout==profit → cost=0 → ROI 0.0（ゼロ除算しない）
    _acc(acc_conn, 1, "本命(直前)", "単勝", 0, 0, 0)
    acc_conn.commit()
    r = compute_live_roi(acc_conn)
    assert r["roi"] == 0.0


def test_ab_empty_db(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_ab_variants

    ab = compute_ab_variants(acc_conn)
    assert ab["both_active"] is False
    assert ab["pure_races"] == 0 and ab["promoted"] is False


def test_ab_promotion_exact_boundary(acc_conn) -> None:
    from src.ml.pnl_accounting import (
        AB_MIN_RACES,
        AB_ROI_DIFF_THRESHOLD,
        compute_ab_variants,
    )

    # Pure_EV を ちょうど AB_MIN_RACES レース、ROI差ちょうど閾値以上に設計
    for i in range(AB_MIN_RACES):
        _acc(acc_conn, 1000 + i, "Pure_EV_Edge(直前)", "単勝", 250, 150, 1, rid=f"P{i}")
    _acc(acc_conn, 9000, "本命(直前)", "単勝", 100, 0, 1, rid="L0")  # ROI100%
    acc_conn.commit()
    ab = compute_ab_variants(acc_conn)
    assert ab["pure_races"] == AB_MIN_RACES
    # Pure ROI 250% vs Legacy 100% → 差150pt ≥ 10pt
    assert ab["diff_roi"] >= AB_ROI_DIFF_THRESHOLD
    assert ab["promoted"] is True


def test_ab_not_promoted_below_min_races(acc_conn) -> None:
    from src.ml.pnl_accounting import compute_ab_variants

    _acc(acc_conn, 1, "Pure_EV_Edge(直前)", "単勝", 500, 400, 1, rid="P1")
    _acc(acc_conn, 2, "本命(直前)", "単勝", 0, -100, 0, rid="L1")
    acc_conn.commit()
    ab = compute_ab_variants(acc_conn)
    assert ab["promoted"] is False
    assert ab["races_remaining"] > 0


# ─────────────────────────────────────────────────────────────────────
# 7. health_reporter エッジケース・異常系
# ─────────────────────────────────────────────────────────────────────
def test_health_severity_no_races_ok() -> None:
    from src.ops.health_reporter import HealthReport

    r = HealthReport(
        date="2026-06-02",
        n_races=0,
        n_predicted=0,
        n_chokuzen=0,
        n_odds_ge2=0,
        n_odds_1=0,
        n_odds_0=0,
        n_results=0,
        n_results_missing=0,
        n_discord_errors=0,
    )
    assert r.severity == "ok"  # 非開催日は健全扱い
    assert r.coverage_rate == 0.0


def test_health_rate_no_zero_division() -> None:
    from src.ops.health_reporter import HealthReport

    r = HealthReport(
        date="d",
        n_races=0,
        n_predicted=5,
        n_chokuzen=0,
        n_odds_ge2=0,
        n_odds_1=0,
        n_odds_0=0,
        n_results=0,
        n_results_missing=0,
        n_discord_errors=0,
    )
    assert r.coverage_rate == 0.0  # n_races=0 でも例外なし


def test_health_format_text_contains_icon() -> None:
    from src.ops.health_reporter import HealthReport, format_report_text

    r = HealthReport(
        date="2026-06-01",
        n_races=24,
        n_predicted=24,
        n_chokuzen=24,
        n_odds_ge2=24,
        n_odds_1=0,
        n_odds_0=0,
        n_results=24,
        n_results_missing=0,
        n_discord_errors=0,
    )
    txt = format_report_text(r)
    assert "2026-06-01" in txt and any(i in txt for i in ("✅", "⚠️", "🚨"))


def test_safe_ab_variants_returns_none_on_error() -> None:
    from src.ops.health_reporter import _safe_ab_variants

    class _BadConn:
        def execute(self, *a, **k):
            raise sqlite3.OperationalError("locked")

    # 例外を握りつぶし None（レポート本体を止めない）
    assert _safe_ab_variants(_BadConn()) is None


def test_format_ab_field_not_both_active() -> None:
    from src.ops.health_reporter import format_ab_field

    ab = {
        "pure_ev": {"n": 0, "profit": 0, "roi": 0.0, "hit_rate": 0.0},
        "legacy": {"n": 10, "profit": 500, "roi": 120.0, "hit_rate": 50.0},
        "diff_profit": -500,
        "diff_roi": -120.0,
        "winner": "判定不能",
        "both_active": False,
        "pure_races": 0,
        "min_races": 100,
        "races_remaining": 100,
        "roi_diff_threshold": 10.0,
        "promoted": False,
        "progress_text": "未稼働（消化0R / 基準100R）",
    }
    field = format_ab_field(ab)
    assert "W-057" in field["name"] and "未稼働" in field["value"]


# ─────────────────────────────────────────────────────────────────────
# 8. DBロック競合・ネットワーク異常のシミュレーション
# ─────────────────────────────────────────────────────────────────────
def test_compute_live_roi_propagates_db_lock() -> None:
    """DBロック時は例外伝播（上位 _safe_* で握る設計）。"""
    from src.ml.pnl_accounting import compute_live_roi

    class _LockedConn:
        def execute(self, *a, **k):
            raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        compute_live_roi(_LockedConn())


def test_fetch_odds_jvrt_network_timeout_returns_none() -> None:
    """JRA-VAN速報ワーカーのタイムアウト（ネット断相当）→ None で後続フォールバック。"""
    import subprocess

    from src.pipeline.scraping import _run_jvrt_worker

    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("py", 90)):
        assert _run_jvrt_worker("202605021201", "20260531") is None


def test_fetch_odds_jvrt_worker_crash_returns_none() -> None:
    from src.pipeline.scraping import _run_jvrt_worker

    class _Crash:
        returncode = 1
        stdout = ""
        stderr = "boom"

    with patch("subprocess.run", return_value=_Crash()):
        assert _run_jvrt_worker("202605021201", "20260531") is None


def test_fetch_odds_jvrt_disabled_env_returns_none(monkeypatch) -> None:
    from src.pipeline.scraping import _run_jvrt_worker

    monkeypatch.setenv("JVLINK_DISABLED", "1")
    assert _run_jvrt_worker("R", "20260531") is None
