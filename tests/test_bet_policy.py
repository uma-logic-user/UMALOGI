"""bet_policy: 実弾(ライブ)ベット単一真実源のテスト。"""

from __future__ import annotations

from src.ml.bet_policy import (
    FLAT_UNIT_YEN,
    MODEL_LIVE_BET_TYPES,
    WATCH_ONLY_MODELS,
    base_model,
    flat_cost,
    is_live_bet,
    is_ornamental,
    is_watch_only,
)


def test_flat_cost_is_accounting_basis() -> None:
    # 会計コスト = ¥100 × 点数（実発注額/Kelly とは別概念）
    assert FLAT_UNIT_YEN == 100
    assert flat_cost(1) == 100
    assert flat_cost(3) == 300
    assert flat_cost(0) == 0
    assert flat_cost(-5) == 0


def test_base_model_strips_suffix_and_v2() -> None:
    assert base_model("本命(直前)") == "本命"
    assert base_model("卍(暫定)") == "卍"
    assert base_model("本命V2(直前)") == "本命"
    assert base_model("OracleV2(暫定)") == "Oracle"
    assert base_model("Alpha-Payout(直前)") == "Alpha-Payout"
    assert base_model("HitFocus(直前)") == "HitFocus"


def test_live_bet_only_tansho_fukusho_of_live_models() -> None:
    # 実弾モデル × 許可券種 = True（2026-06-02: 卍は複勝特化・Pure/Fukushoは単複）
    assert is_live_bet("卍(暫定)", "複勝") is True
    assert is_live_bet("Pure_EV_Edge(直前)", "単勝") is True
    assert is_live_bet("Pure_EV_Edge(直前)", "複勝") is True
    assert is_live_bet("FukushoElite(直前)", "複勝") is True


def test_manji_place_promoted_win_watch_only() -> None:
    """卍 複勝特化昇格: 複勝のみ実弾、単勝は WATCH_ONLY（投票しない）。"""
    # 複勝 = 実弾投票対象へ昇格
    assert is_live_bet("卍(直前)", "複勝") is True
    assert is_live_bet("卍(暫定)", "複勝") is True
    # 単勝 = 投票しない（WATCH_ONLY）が観賞用ではない
    assert is_live_bet("卍(直前)", "単勝") is False
    assert is_live_bet("卍(暫定)", "単勝") is False
    assert is_watch_only("卍(直前)", "単勝") is True
    assert is_watch_only("卍(暫定)", "単勝") is True
    assert is_ornamental("卍(直前)") is False
    # 複勝は WATCH_ONLY ではない（実弾）
    assert is_watch_only("卍(直前)", "複勝") is False


def test_gate_split_policy_tables() -> None:
    """ポリシーテーブルの整合: 卍は複勝live・単勝watch、他モデルは単複既定。"""
    assert MODEL_LIVE_BET_TYPES["卍"] == frozenset({"複勝"})
    assert WATCH_ONLY_MODELS["卍"] == frozenset({"単勝"})
    # Pure_EV_Edge/FukushoElite は override 無し（単複両方が既定で実弾）
    assert "Pure_EV_Edge" not in MODEL_LIVE_BET_TYPES
    assert "FukushoElite" not in WATCH_ONLY_MODELS
    # 監視専用でも非観賞用モデルは集客フラグが立たない
    assert is_watch_only("Pure_EV_Edge(直前)", "単勝") is False


def test_retired_models_not_live_after_20260602_shrink() -> None:
    # 本命・Alpha-Payout は確定実績ROI<100%により実弾から退避（非実弾・非観賞用）
    assert is_live_bet("本命(直前)", "単勝") is False
    assert is_live_bet("本命(直前)", "複勝") is False
    assert is_live_bet("本命V2(直前)", "単勝") is False
    assert is_live_bet("Alpha-Payout(直前)", "複勝") is False
    # 退避モデルは観賞用(集客専用)ではない
    assert is_ornamental("本命(直前)") is False
    assert is_ornamental("Alpha-Payout(直前)") is False


def test_exotics_are_not_live() -> None:
    for bt in ("三連単", "三連複", "馬連", "馬単", "ワイド", "WIN5"):
        assert is_live_bet("本命(直前)", bt) is False
        assert is_live_bet("卍(直前)", bt) is False
        assert is_live_bet("Alpha-Payout(直前)", bt) is False


def test_ornamental_models_never_live() -> None:
    assert is_ornamental("Oracle(直前)") is True
    assert is_ornamental("HitFocus(暫定)") is True
    assert is_ornamental("HitFocusV2(直前)") is True
    # 観賞用は単複でも実弾にならない
    assert is_live_bet("Oracle(直前)", "単勝") is False
    assert is_live_bet("HitFocus(直前)", "複勝") is False
    assert is_live_bet("OracleV2(直前)", "三連単") is False


def test_unknown_model_not_live() -> None:
    assert is_live_bet("謎モデル(直前)", "単勝") is False


def test_pure_ev_edge_is_live_tanpuku() -> None:
    # Pure_EV_Edge（黒字化専用枠）は実弾モデル・単複のみ
    assert is_live_bet("Pure_EV_Edge(直前)", "単勝") is True
    assert is_live_bet("Pure_EV_Edge(直前)", "複勝") is True
    assert is_live_bet("Pure_EV_Edge(直前)", "三連単") is False
    assert base_model("Pure_EV_Edge(直前)") == "Pure_EV_Edge"
