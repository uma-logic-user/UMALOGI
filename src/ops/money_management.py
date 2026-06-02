"""
src/ops/money_management.py — Note/X 表示用の予算配分エンジン

サブスク読者向けに「¥10,000 で買うとしたらこう配分する」参考額を算出する純関数モジュール。
実弾投票・bet_policy・DB とは完全に切り離された表示専用ロジック。

Usage:
    from src.ops.money_management import allocate_budget, BetAllocation
    from src.ops.sns_publisher import NoteBet

    bets = [NoteBet("複勝", "3番", ev=1.5), NoteBet("単勝", "1番", ev=2.0)]
    plan = allocate_budget(bets, total_budget=10_000)
    for b in plan:
        print(b.bet_type, b.horse_desc, b.allocated_yen, b.label)
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from src.ops.sns_publisher import NoteBet

_UNIT_YEN: int = 100  # JRA 最小購入単位
_DEFAULT_BUDGET: int = 10_000  # デフォルト総予算
_MAX_NON_POSITIVE: int = 3  # EV <= 1.0 買い目の最大含有件数

# EV → 勝負ラベルのマッピング（降順評価）
_LABEL_TIERS: tuple[tuple[float, str], ...] = (
    (1.40, "激熱勝負！"),
    (1.20, "中勝負"),
    (0.00, "安心投資"),
)


def _bet_label(ev: float) -> str:
    """EV に対応する勝負ラベルを返す。"""
    for threshold, label in _LABEL_TIERS:
        if ev >= threshold:
            return label
    return "安心投資"


def _horse_number(horse_desc: str) -> int:
    """horse_desc の先頭整数を馬番として返す（不明時は 999 = ソート末尾）。"""
    m = re.search(r"\d+", horse_desc or "")
    return int(m.group()) if m else 999


@dataclass(frozen=True)
class BetAllocation:
    """Note/X 表示用の 1 買い目ごとの参考配分額。"""

    bet_type: str
    horse_desc: str
    ev: float
    allocated_yen: int  # 100 円単位
    label: str  # "激熱勝負！" / "中勝負" / "安心投資"


def allocate_budget(
    bets: list[NoteBet],
    total_budget: int = _DEFAULT_BUDGET,
) -> list[BetAllocation]:
    """EV 比例で総予算を按分した Note/X 表示用の配分リストを返す（純関数）。

    アルゴリズム:
      1. EV > 1.0 の買い目を「正 EV」、EV <= 1.0 を「保険枠」と分類する。
      2. 保険枠は EV 上位 _MAX_NON_POSITIVE 件のみ選択し、各 100 円固定とする。
      3. 正 EV 買い目へは残余プールを EV エッジ（EV-1.0）比例で配分し 100 円切り捨て。
      4. 切り捨て残余と未配分プール（保険枠のみの場合）は最高 EV 買い目に加算する。
         最高 EV が同値の場合は 馬番若い順 → 先着順 で 1 件に集約する。

    合計保証: bets 非空かつ total_budget > 0 の場合、sum(allocated_yen) == total_budget。
    100 円単位保証: total_budget が 100 の倍数であれば全 allocated_yen も 100 の倍数。

    Args:
        bets:         Note に載せる買い目リスト（NoteBet）。
        total_budget: 按分する総予算（円）。デフォルト 10,000 円。

    Returns:
        入力順を保持した BetAllocation リスト（除外された EV <= 1.0 の買い目は含まない）。
    """
    if not bets or total_budget <= 0:
        return []

    # ── 対象インデックスの選定 ──────────────────────────────────────────
    pos_idx: list[int] = [i for i, b in enumerate(bets) if b.ev > 1.0]
    np_idx: list[int] = sorted(
        [i for i, b in enumerate(bets) if b.ev <= 1.0],
        key=lambda i: bets[i].ev,
        reverse=True,
    )[:_MAX_NON_POSITIVE]
    included: set[int] = set(pos_idx) | set(np_idx)

    if not included:
        return []

    # ── 基本配分の計算 ──────────────────────────────────────────────────
    alloc: list[int] = [0] * len(bets)

    # 保険枠: 100 円固定
    for i in np_idx:
        alloc[i] = _UNIT_YEN

    # 正 EV: EV エッジ比例 / 100 円切り捨て
    if pos_idx:
        insurance = len(np_idx) * _UNIT_YEN
        pool = max(total_budget - insurance, 0)
        edges = [bets[i].ev - 1.0 for i in pos_idx]
        edge_sum = sum(edges)
        if edge_sum > 0 and pool > 0:
            for j, i in enumerate(pos_idx):
                alloc[i] = (
                    math.floor(pool * edges[j] / edge_sum / _UNIT_YEN) * _UNIT_YEN
                )
        # edge_sum == 0 は EV がすべて 1.0 の境界値のみ（実運用上ほぼ発生しない）

    # ── 残余を最高 EV 買い目に加算 ────────────────────────────────────────
    remainder = total_budget - sum(alloc[i] for i in included)
    if remainder > 0:
        best = min(
            included,
            key=lambda i: (-bets[i].ev, _horse_number(bets[i].horse_desc), i),
        )
        alloc[best] += remainder

    # ── 入力順を保持して BetAllocation リストを返す ───────────────────────
    return [
        BetAllocation(
            bet_type=bets[i].bet_type,
            horse_desc=bets[i].horse_desc,
            ev=bets[i].ev,
            allocated_yen=alloc[i],
            label=_bet_label(bets[i].ev),
        )
        for i in range(len(bets))
        if i in included
    ]
