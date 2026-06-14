"""
W-094 / v1.16.0-dev — 暫定予想の能力ベース買い目生成（オッズ非依存）の回帰テスト

検証:
  - オッズ列が全く無くても ◎〇▲△ の印と具体的な買い目（単勝/複勝/ワイド/馬連）が出る。
  - 能力スコア順に印が割り当てられる。
  - 保存用 RaceBets が本命の単勝＋複勝を含む。
"""

from __future__ import annotations

import pandas as pd

from src.ml.provisional_picks import (
    assign_ability_marks,
    build_provisional_display,
    build_provisional_racebets,
    mark_for_rank,
)


def _mock_df_no_odds() -> pd.DataFrame:
    """オッズ列を一切持たない出馬表（暫定の最悪条件）。"""
    return pd.DataFrame(
        {
            "horse_number": [1, 2, 3, 4, 5],
            "horse_name": ["アルファ", "ブラボー", "チャーリー", "デルタ", "エコー"],
        }
    )


def test_mark_for_rank() -> None:
    assert mark_for_rank(0) == "◎"
    assert mark_for_rank(1) == "○"
    assert mark_for_rank(2) == "▲"
    assert mark_for_rank(3) == "△"
    assert mark_for_rank(99) == ""
    assert mark_for_rank(-1) == ""


def test_marks_follow_ability_order_without_odds() -> None:
    df = _mock_df_no_odds()
    # 能力降順: 馬番3(0.40) > 5(0.30) > 1(0.20) > 2(0.05) > 4(0.02)
    scores = pd.Series([0.20, 0.05, 0.40, 0.02, 0.30])
    marks = assign_ability_marks(df, scores)
    # codepoint 揺れ（○ U+25CB vs 〇 U+3007）を避け、モジュール定数を真とする。
    assert marks[3] == mark_for_rank(0)  # ◎
    assert marks[5] == mark_for_rank(1)  # 2番手の印
    assert marks[1] == mark_for_rank(2)  # ▲
    # ◎〇▲△ の4頭のみ（馬番3,5,1,2）。最下位の馬番4(0.02)は無印。
    assert len(marks) == 4
    assert 4 not in marks


def test_display_produces_concrete_bets_without_odds() -> None:
    df = _mock_df_no_odds()
    scores = pd.Series([0.20, 0.05, 0.40, 0.02, 0.30])
    disp = build_provisional_display(df, scores)
    assert disp["basis"] == "ability"
    bet_types = {b["bet_type"] for b in disp["bets"]}
    # 単勝・複勝・ワイド・馬連がすべて具体的に生成される
    assert {"単勝", "複勝", "ワイド", "馬連"}.issubset(bet_types)
    # 単勝の軸は能力1位（馬番3）
    tansho = next(b for b in disp["bets"] if b["bet_type"] == "単勝")
    assert tansho["combination"] == [3]
    # 複勝は能力上位3頭
    fukusho = next(b for b in disp["bets"] if b["bet_type"] == "複勝")
    assert set(fukusho["combination"]) == {3, 1, 5}
    # ranked 先頭は◎
    assert disp["ranked"][0]["mark"] == "◎"
    assert disp["ranked"][0]["horse_number"] == 3


def test_racebets_for_saving_has_tansho_and_fukusho() -> None:
    df = _mock_df_no_odds()
    scores = pd.Series([0.20, 0.05, 0.40, 0.02, 0.30])
    rb = build_provisional_racebets("202699010101", df, scores)
    assert rb.model_type == "本命"
    types = [b.bet_type for b in rb.bets]
    assert types.count("単勝") == 1
    assert types.count("複勝") == 3
    tansho = next(b for b in rb.bets if b.bet_type == "単勝")
    assert tansho.combinations == [(3,)]
    assert tansho.expected_value == 0.0  # オッズ未確定＝EV未知


def test_empty_df_is_safe() -> None:
    df = pd.DataFrame({"horse_number": [], "horse_name": []})
    scores = pd.Series([], dtype=float)
    assert assign_ability_marks(df, scores) == {}
    disp = build_provisional_display(df, scores)
    assert disp["bets"] == []
    rb = build_provisional_racebets("r", df, scores)
    assert rb.bets == []


def test_nan_scores_do_not_crash() -> None:
    df = _mock_df_no_odds()
    scores = pd.Series([float("nan")] * 5)
    marks = assign_ability_marks(df, scores)
    # NaN は 0 扱い、安定ソートで先頭から印が付く（クラッシュしないことが要件）
    assert len(marks) == 4


def test_build_output_json_provisional_emits_marks_and_picks() -> None:
    """暫定モードの UI ペイロードに印（mark）と provisional_picks が必ず載る。"""
    from src.ml.bet_generator import RaceBets
    from src.pipeline._common import build_output_json

    df = _mock_df_no_odds()
    honmei = pd.Series([0.20, 0.05, 0.40, 0.02, 0.30])
    zeros = pd.Series([0.0] * 5)
    empty_honmei = RaceBets(race_id="202699010101", model_type="本命")
    empty_manji = RaceBets(race_id="202699010101", model_type="卍")

    payload = build_output_json(
        "202699010101",
        df,
        honmei,
        zeros,  # honmei_ev_scores（オッズ無→0）
        zeros,  # manji ev
        empty_honmei,
        empty_manji,
        provisional=True,
    )
    # 各馬に印フィールドがある（◎が能力1位の馬番3）
    marks = {h["horse_number"]: h["mark"] for h in payload["horses"]}
    assert marks[3] == "◎" or marks[3] == mark_for_rank(0)
    # provisional_picks が具体的な買い目を含む
    assert "provisional_picks" in payload
    assert len(payload["provisional_picks"]["bets"]) > 0
