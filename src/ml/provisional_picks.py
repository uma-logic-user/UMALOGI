"""
src/ml/provisional_picks.py — 暫定予想の「能力評価ベース」具体買い目生成（オッズ非依存）

背景（2026-06-14 / v1.16.0-dev・Task2）:
  金曜夜〜当日朝の暫定予想はオッズが未確定なため、EV（=勝率×オッズ）を機械的に
  計算する実弾パイプラインでは買い目が一切生成されず（卍/Oracle は明示スキップ、
  本命も EV ゲートで全除外）、UI に「具体的な印も買い目も出ない」状態だった。

  本モジュールは **オッズに依存しない能力値（本命モデルの勝率スコア）だけで**
  具体的な印（◎〇▲△）と推奨買い目（単勝・複勝・ワイド・馬連）を生成する。
  暫定段階での「叩き台」を提供し、直前にオッズが揃った時点で EV ベースの実弾
  買い目へ置き換わる前提（条項1: 既存の確定予想は上書きしない）。

注意:
  - ここで出すのは「能力評価ベースの暫定」であり、期待値（EV）は未知（=0.0 で保存）。
  - 実弾の単複ロック・EV ゲートとは独立。保存する券種は本命の単勝/複勝に限定し、
    ワイド/馬連は UI 表示用 display にのみ含める（実弾計上には乗せない）。
"""

from __future__ import annotations

import pandas as pd

# 順位 → 印（0始まり）。note_generator と同一の体系。
_MARKS: list[str] = ["◎", "○", "▲", "△", "×", "注"]


def mark_for_rank(rank: int) -> str:
    """0始まりの能力順位を印に変換する（範囲外は空文字＝無印）。"""
    if rank < 0:
        return ""
    return _MARKS[rank] if rank < len(_MARKS) else ""


def _ranked_numbers(df: pd.DataFrame, scores: pd.Series) -> list[tuple[int, float]]:
    """(馬番, 能力スコア) を能力降順で返す。"""
    df_reset = df.reset_index(drop=True)
    pairs: list[tuple[int, float]] = []
    for i, row in df_reset.iterrows():
        try:
            num = int(row["horse_number"])
        except (TypeError, ValueError):
            continue
        score = float(scores.iloc[i]) if i < len(scores) else 0.0
        if pd.isna(score):
            score = 0.0
        pairs.append((num, score))
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs


def assign_ability_marks(
    df: pd.DataFrame, honmei_scores: pd.Series, *, max_marks: int = 4
) -> dict[int, str]:
    """能力スコア上位に ◎〇▲△ を割り当てた {馬番: 印} を返す。

    Args:
        df: 出走馬 DataFrame（horse_number 必須）。
        honmei_scores: 本命モデルの勝率スコア（df 行と同順）。
        max_marks: 印を付ける頭数（既定4=◎〇▲△）。

    Returns:
        {馬番: 印}。上位 max_marks 頭のみ。
    """
    ranked = _ranked_numbers(df, honmei_scores)
    marks: dict[int, str] = {}
    for rank, (num, _score) in enumerate(ranked[:max_marks]):
        marks[num] = mark_for_rank(rank)
    return marks


def _name_map(df: pd.DataFrame) -> dict[int, str]:
    out: dict[int, str] = {}
    for _, row in df.reset_index(drop=True).iterrows():
        try:
            out[int(row["horse_number"])] = str(row.get("horse_name", "") or "")
        except (TypeError, ValueError):
            continue
    return out


def build_provisional_display(df: pd.DataFrame, honmei_scores: pd.Series) -> dict:
    """UI 表示用の暫定予想（印＋推奨買い目）を組み立てる（オッズ非依存）。

    Returns:
        {
          "basis": "ability",
          "marks": {馬番: 印, ...},
          "ranked": [{"rank","horse_number","horse_name","mark","honmei_score"}, ...],
          "bets": [{"bet_type","combination","horse_names","note"}, ...],
        }
    """
    ranked = _ranked_numbers(df, honmei_scores)
    names = _name_map(df)
    marks = assign_ability_marks(df, honmei_scores)

    ranked_list: list[dict] = []
    for rank, (num, score) in enumerate(ranked):
        ranked_list.append(
            {
                "rank": rank + 1,
                "horse_number": num,
                "horse_name": names.get(num, ""),
                "mark": marks.get(num, ""),
                "honmei_score": round(score, 4),
            }
        )

    bets: list[dict] = []
    nums = [n for n, _ in ranked]
    if nums:
        axis = nums[0]  # ◎
        bets.append(
            {
                "bet_type": "単勝",
                "combination": [axis],
                "horse_names": [names.get(axis, "")],
                "note": "◎ 能力評価1位（オッズ未確定・暫定）",
            }
        )
        place_nums = nums[:3]
        bets.append(
            {
                "bet_type": "複勝",
                "combination": place_nums,
                "horse_names": [names.get(n, "") for n in place_nums],
                "note": "◎〇▲ の複勝（能力上位3頭・暫定）",
            }
        )
        # ワイド: ◎-〇, ◎-▲（表示用のみ）
        for partner in nums[1:3]:
            bets.append(
                {
                    "bet_type": "ワイド",
                    "combination": sorted([axis, partner]),
                    "horse_names": [names.get(axis, ""), names.get(partner, "")],
                    "note": "◎軸ワイド（能力上位・暫定）",
                }
            )
        if len(nums) >= 2:
            second = nums[1]
            bets.append(
                {
                    "bet_type": "馬連",
                    "combination": sorted([axis, second]),
                    "horse_names": [names.get(axis, ""), names.get(second, "")],
                    "note": "◎-〇 馬連（能力上位2頭・暫定）",
                }
            )

    return {
        "basis": "ability",
        "marks": {str(k): v for k, v in marks.items()},
        "ranked": ranked_list,
        "bets": bets,
    }


def build_provisional_racebets(
    race_id: str, df: pd.DataFrame, honmei_scores: pd.Series
):
    """保存用の暫定買い目（本命の単勝◎＋複勝◎〇▲）を RaceBets として返す。

    実弾の EV ゲート/オッズ帯フィルタは通さない（オッズ未確定のため）。
    expected_value は未知として 0.0、recommended_bet は参照用フラット100円。

    Returns:
        RaceBets（model_type="本命"）。出走頭数0なら空の RaceBets。
    """
    # 遅延 import（循環依存回避）。
    from src.ml.bet_generator import BetRecommendation, RaceBets

    ranked = _ranked_numbers(df, honmei_scores)
    names = _name_map(df)
    rb = RaceBets(race_id=race_id, model_type="本命")
    if not ranked:
        return rb

    nums = [n for n, _ in ranked]
    top_score = ranked[0][1]
    axis = nums[0]

    # 単勝 ◎
    rb.bets.append(
        BetRecommendation(
            bet_type="単勝",
            combinations=[(axis,)],
            horse_names=[names.get(axis, "")],
            expected_value=0.0,  # オッズ未確定のため EV 未知
            model_score=float(top_score),
            recommended_bet=100.0,  # 参照用フラット（実弾会計は別管理）
            confidence=float(min(max(top_score, 0.0), 1.0)),
            notes="暫定◎ 能力評価1位（オッズ未確定）",
        )
    )
    # 複勝 ◎〇▲（各頭1点）
    for rank, num in enumerate(nums[:3]):
        rb.bets.append(
            BetRecommendation(
                bet_type="複勝",
                combinations=[(num,)],
                horse_names=[names.get(num, "")],
                expected_value=0.0,
                model_score=float(ranked[rank][1]),
                recommended_bet=100.0,
                confidence=float(min(max(ranked[rank][1], 0.0), 1.0)),
                notes=f"暫定{mark_for_rank(rank)} 能力評価{rank + 1}位（複勝・オッズ未確定）",
            )
        )
    return rb
