"""bet_policy: 実弾(ライブ)ベット単一真実源のテスト。"""

from __future__ import annotations

from src.ml.bet_policy import base_model, is_live_bet, is_ornamental


def test_base_model_strips_suffix_and_v2() -> None:
    assert base_model("本命(直前)") == "本命"
    assert base_model("卍(暫定)") == "卍"
    assert base_model("本命V2(直前)") == "本命"
    assert base_model("OracleV2(暫定)") == "Oracle"
    assert base_model("Alpha-Payout(直前)") == "Alpha-Payout"
    assert base_model("HitFocus(直前)") == "HitFocus"


def test_live_bet_only_tansho_fukusho_of_live_models() -> None:
    # 実弾モデル × 単複 = True
    assert is_live_bet("本命(直前)", "単勝") is True
    assert is_live_bet("本命(直前)", "複勝") is True
    assert is_live_bet("卍(直前)", "単勝") is True
    assert is_live_bet("卍(暫定)", "複勝") is True
    assert is_live_bet("Alpha-Payout(直前)", "複勝") is True
    assert is_live_bet("本命V2(直前)", "単勝") is True


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
