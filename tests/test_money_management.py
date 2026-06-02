"""
tests/test_money_management.py — BudgetAllocator (money_management.py) の TDD テスト

テスト対象:
  - allocate_budget(bets, total_budget) -> list[BetAllocation]
  - BetAllocation dataclass

保証する不変条件:
  1. 結果の allocated_yen の合計が total_budget に等しい（bets 非空かつ positive EV あり）
  2. 全 allocated_yen が 100 円単位（UNIT_YEN の倍数）
  3. EV <= 1.0 の保険枠は最大 3 件に制限される
  4. EV <= 1.0 の保険枠は 100 円固定（positive EV 買い目が存在する場合）
  5. EV > 1.0 の正 EV 買い目はエッジ（EV-1.0）比例で配分される（高い EV = 多い配分）
"""

from __future__ import annotations

from src.ops.money_management import BetAllocation, allocate_budget
from src.ops.sns_publisher import NoteBet

# ─────────────────────────────────────────────────────────────────────
# ヘルパー
# ─────────────────────────────────────────────────────────────────────


def _total(result: list[BetAllocation]) -> int:
    return sum(r.allocated_yen for r in result)


def _by_desc(result: list[BetAllocation]) -> dict[str, BetAllocation]:
    return {r.horse_desc: r for r in result}


# ─────────────────────────────────────────────────────────────────────
# ケース 1: 買い目 0 件
# ─────────────────────────────────────────────────────────────────────


def test_empty_bets_returns_empty_list():
    """買い目 0 件 → 空リストを返す。"""
    result = allocate_budget([], total_budget=10_000)
    assert result == []


def test_zero_budget_returns_empty_list():
    """総予算 0 円 → 空リストを返す。"""
    bets = [NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5)]
    assert allocate_budget(bets, total_budget=0) == []


# ─────────────────────────────────────────────────────────────────────
# ケース 2: 買い目 1 件
# ─────────────────────────────────────────────────────────────────────


def test_single_positive_ev_gets_full_budget():
    """EV > 1.0 の買い目 1 件 → 全額が 1 件に割り当てられる。"""
    bets = [NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5)]
    result = allocate_budget(bets, total_budget=10_000)
    assert len(result) == 1
    assert result[0].allocated_yen == 10_000
    assert _total(result) == 10_000


def test_single_positive_ev_100_yen_unit():
    """1 件でも 100 円単位を守る。"""
    bets = [NoteBet(bet_type="単勝", horse_desc="1番", ev=1.3)]
    result = allocate_budget(bets, total_budget=5_000)
    assert result[0].allocated_yen % 100 == 0


# ─────────────────────────────────────────────────────────────────────
# ケース 3: 複数・同一 EV
# ─────────────────────────────────────────────────────────────────────


def test_multiple_same_ev_total_equals_budget():
    """複数同一 EV → 合計が total_budget に一致する。"""
    bets = [
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="5番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="7番", ev=1.5),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    assert _total(result) == 10_000


def test_multiple_same_ev_all_100_yen_unit():
    """複数同一 EV → 全て 100 円単位。"""
    bets = [
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="5番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="7番", ev=1.5),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    for r in result:
        assert r.allocated_yen % 100 == 0
        assert r.allocated_yen >= 100


def test_same_ev_remainder_goes_to_smallest_horse_number():
    """同一 EV の残余は馬番が最も若い買い目に加算される。"""
    # edge=0.5 × 3, budget=10000: raw=[3333.33,...], floored=[3300,3300,3300]
    # remainder=100 → horse_number 3 (最小) が受け取る
    bets = [
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="5番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="7番", ev=1.5),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    d = _by_desc(result)
    # 馬番 3 が最も多い（残余を受け取るため）
    assert d["3番"].allocated_yen >= d["5番"].allocated_yen
    assert d["3番"].allocated_yen >= d["7番"].allocated_yen


# ─────────────────────────────────────────────────────────────────────
# ケース 4: EV 比例配分
# ─────────────────────────────────────────────────────────────────────


def test_higher_ev_gets_more_allocation():
    """EV が高い買い目は低い買い目より多くの配分を受ける。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="1番", ev=2.0),  # edge=1.0
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.5),  # edge=0.5
    ]
    result = allocate_budget(bets, total_budget=10_000)
    d = _by_desc(result)
    assert d["1番"].allocated_yen > d["3番"].allocated_yen
    assert _total(result) == 10_000


def test_proportional_allocation_total_equals_budget():
    """異なる EV の複数買い目でも合計が total_budget に一致する。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="1番", ev=1.7),
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.4),
        NoteBet(bet_type="馬連", horse_desc="1-3", ev=1.2),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    assert _total(result) == 10_000


def test_proportional_allocation_all_100_yen_unit():
    """異なる EV の複数買い目で全て 100 円単位。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="1番", ev=1.7),
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.4),
        NoteBet(bet_type="馬連", horse_desc="1-3", ev=1.2),
    ]
    result = allocate_budget(bets, total_budget=7_500)
    for r in result:
        assert r.allocated_yen % 100 == 0


# ─────────────────────────────────────────────────────────────────────
# ケース 5: EV <= 1.0 の保険枠
# ─────────────────────────────────────────────────────────────────────


def test_non_positive_ev_capped_at_three_items():
    """EV <= 1.0 の買い目は最大 3 件のみ含まれる。"""
    bets = [
        NoteBet(bet_type="複勝", horse_desc=f"{i + 1}番", ev=0.9 - i * 0.01)
        for i in range(6)  # 6件全てEV<=1.0
    ]
    result = allocate_budget(bets, total_budget=10_000)
    assert len(result) <= 3


def test_non_positive_ev_top3_by_ev_are_selected():
    """EV <= 1.0 の中から EV 上位 3 件が選ばれる。"""
    bets = [
        NoteBet(bet_type="複勝", horse_desc="A", ev=0.95),  # 1位
        NoteBet(bet_type="複勝", horse_desc="B", ev=0.85),  # 3位
        NoteBet(bet_type="複勝", horse_desc="C", ev=0.70),  # 除外
        NoteBet(bet_type="複勝", horse_desc="D", ev=0.90),  # 2位
    ]
    result = allocate_budget(bets, total_budget=10_000)
    descs = {r.horse_desc for r in result}
    assert "A" in descs  # EV 0.95 → 含まれる
    assert "D" in descs  # EV 0.90 → 含まれる
    assert "B" in descs  # EV 0.85 → 含まれる
    assert "C" not in descs  # EV 0.70 → 除外


def test_non_positive_ev_fixed_at_100_yen_when_positive_exists():
    """EV <= 1.0 の保険枠は 100 円固定（positive EV 買い目が共存する場合）。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="1番", ev=1.5),  # positive
        NoteBet(bet_type="複勝", horse_desc="3番", ev=0.9),  # non-positive
    ]
    result = allocate_budget(bets, total_budget=10_000)
    d = _by_desc(result)
    assert d["3番"].allocated_yen == 100
    assert _total(result) == 10_000


# ─────────────────────────────────────────────────────────────────────
# ケース 6: ラベル
# ─────────────────────────────────────────────────────────────────────


def test_labels_match_ev_thresholds():
    """EV しきい値に応じたラベルが付与される。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="a", ev=1.45),  # >= 1.40 → 激熱勝負！
        NoteBet(bet_type="複勝", horse_desc="b", ev=1.25),  # >= 1.20 → 中勝負
        NoteBet(bet_type="複勝", horse_desc="c", ev=1.10),  # <  1.20 → 安心投資
    ]
    result = allocate_budget(bets, total_budget=10_000)
    d = _by_desc(result)
    assert d["a"].label == "激熱勝負！"
    assert d["b"].label == "中勝負"
    assert d["c"].label == "安心投資"


def test_non_positive_ev_gets_anshin_label():
    """EV <= 1.0 の保険枠のラベルは '安心投資'。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="1番", ev=1.5),
        NoteBet(bet_type="複勝", horse_desc="3番", ev=0.8),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    d = _by_desc(result)
    assert d["3番"].label == "安心投資"


# ─────────────────────────────────────────────────────────────────────
# ケース 7: 入力順の保持
# ─────────────────────────────────────────────────────────────────────


def test_output_preserves_input_order():
    """結果は入力 bets の順序を保持して返す。"""
    bets = [
        NoteBet(bet_type="単勝", horse_desc="7番", ev=1.3),
        NoteBet(bet_type="複勝", horse_desc="3番", ev=1.6),
        NoteBet(bet_type="馬連", horse_desc="1-5", ev=1.1),
    ]
    result = allocate_budget(bets, total_budget=10_000)
    assert [r.horse_desc for r in result] == ["7番", "3番", "1-5"]
