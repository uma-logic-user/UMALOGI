"""bet_policy: 実弾(ライブ)ベット単一真実源のテスト。"""

from __future__ import annotations

from src.ml.bet_policy import (
    FLAT_UNIT_YEN,
    base_model,
    flat_cost,
    is_live_bet,
    is_ornamental,
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
    # 実弾モデル × 単複 = True（2026-06-02 縮退後: 卍/Pure_EV_Edge/FukushoElite のみ）
    assert is_live_bet("卍(直前)", "単勝") is True
    assert is_live_bet("卍(暫定)", "複勝") is True
    assert is_live_bet("Pure_EV_Edge(直前)", "単勝") is True
    assert is_live_bet("FukushoElite(直前)", "複勝") is True


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
