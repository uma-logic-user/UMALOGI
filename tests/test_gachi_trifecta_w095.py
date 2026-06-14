"""
W-095 / v1.16.0-dev — 3連系「本気」アンサンブルモデルの回帰テスト（Task3）

検証:
  - 軸は本命×複勝の双方が高い担保馬から選ばれる。
  - 紐は卍EVが高い穴馬から選ばれる。
  - 馬連/馬単/三連複/三連単の具体的な買い目が生成される。
  - オッズ列が無くても（暫定でも）確率ベースで動く。
"""

from __future__ import annotations

import pandas as pd

from src.ml.gachi_trifecta import (
    GachiTrifectaBets,
    build_gachi_trifecta,
    select_axis_and_partners,
)


def _mock_df(n: int = 8, with_odds: bool = True) -> pd.DataFrame:
    data: dict[str, list] = {
        "horse_number": list(range(1, n + 1)),
        "horse_name": [f"ウマ{i}" for i in range(1, n + 1)],
    }
    if with_odds:
        data["win_odds"] = [3.0, 5.0, 8.0, 12.0, 20.0, 30.0, 50.0, 80.0][:n]
    return pd.DataFrame(data)


def test_axis_from_honmei_and_place_partners_from_ev() -> None:
    df = _mock_df(8)
    # 馬番1,2 は勝率・複勝率ともに高い（担保＝軸）
    honmei = pd.Series([0.40, 0.30, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05])
    place = pd.Series([0.80, 0.70, 0.20, 0.20, 0.20, 0.20, 0.20, 0.20])
    # 馬番5,6 は EV が突出（穴＝紐）
    ev = pd.Series([0.9, 0.9, 1.0, 1.0, 2.5, 2.2, 1.0, 1.0])

    axis, partners = select_axis_and_partners(df, honmei, place, ev)
    assert axis == [1, 2]  # 担保馬2頭
    # 紐は EV 上位の穴（5,6 が先頭に来る）
    assert 5 in partners and 6 in partners
    assert 1 not in partners and 2 not in partners


def test_build_produces_all_four_bet_types() -> None:
    df = _mock_df(8)
    honmei = pd.Series([0.40, 0.30, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05])
    place = pd.Series([0.80, 0.70, 0.20, 0.20, 0.20, 0.20, 0.20, 0.20])
    ev = pd.Series([0.9, 0.9, 1.0, 1.0, 2.5, 2.2, 1.0, 1.0])

    bets = build_gachi_trifecta("202699010101", df, honmei, place, ev)
    assert isinstance(bets, GachiTrifectaBets)
    types = {b.bet_type for b in bets.bets}
    assert {"馬連", "馬単", "三連複", "三連単"} == types
    # 三連複/三連単の代表組は軸(1)を含む
    trio = next(b for b in bets.bets if b.bet_type == "三連複")
    assert 1 in trio.combinations[0]
    trifecta = next(b for b in bets.bets if b.bet_type == "三連単")
    assert trifecta.combinations[0][0] == 1  # 軸1着固定


def test_works_without_odds_provisional() -> None:
    """オッズ列が無くても確率ベースで買い目が出る（暫定での本気3連系）。"""
    df = _mock_df(6, with_odds=False)
    honmei = pd.Series([0.35, 0.25, 0.15, 0.10, 0.08, 0.07])
    bets = build_gachi_trifecta("r", df, honmei)  # place/ev 省略
    assert len(bets.bets) == 4
    # axis は勝率上位
    assert bets.axis[0] == 1


def test_to_dict_serializable() -> None:
    df = _mock_df(6)
    honmei = pd.Series([0.35, 0.25, 0.15, 0.10, 0.08, 0.07])
    place = pd.Series([0.7, 0.6, 0.4, 0.3, 0.2, 0.2])
    ev = pd.Series([1.0, 1.0, 1.5, 2.0, 1.0, 1.0])
    d = build_gachi_trifecta("r", df, honmei, place, ev).to_dict()
    assert d["model_type"] == "本気3連系"
    assert isinstance(d["bets"], list)
    assert all("combinations" in b for b in d["bets"])


def test_too_few_horses_returns_empty() -> None:
    df = _mock_df(2)
    honmei = pd.Series([0.6, 0.4])
    bets = build_gachi_trifecta("r", df, honmei)
    assert bets.bets == []


def test_point_caps_respected() -> None:
    df = _mock_df(8)
    honmei = pd.Series([0.4, 0.2, 0.1, 0.08, 0.07, 0.06, 0.05, 0.04])
    place = pd.Series([0.8, 0.6, 0.5, 0.4, 0.3, 0.3, 0.2, 0.2])
    ev = pd.Series([1.0, 1.0, 1.5, 1.4, 1.3, 1.2, 1.1, 1.0])
    bets = build_gachi_trifecta("r", df, honmei, place, ev)
    for b in bets.bets:
        if b.bet_type == "三連単":
            assert len(b.combinations) <= 12
        if b.bet_type == "三連複":
            assert len(b.combinations) <= 8
