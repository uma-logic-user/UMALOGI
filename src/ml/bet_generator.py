"""
全券種買い目ジェネレーター

モデルの予測スコアを元に、以下の券種の推奨買い目を生成する。
  単勝 / 複勝 / 馬連 / ワイド / 三連複 / 三連単 / WIN5

卍モデル  : EV_score > 1.0 の馬（期待値プラス）を優先
本命モデル: 勝率スコア上位を優先、フォーメーション軸に使用
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

logger = logging.getLogger(__name__)

BetType = Literal["単勝", "複勝", "馬連", "ワイド", "三連複", "三連単", "WIN5"]

# Kelly Criterion 上限（過剰賭けリスク抑制）
_KELLY_CAP = 0.25
# デフォルト賭け単位（円）
_BASE_BET = 100


@dataclass
class BetRecommendation:
    """1つの買い目推奨。"""
    bet_type: BetType
    combinations: list[tuple[int, ...]]   # 馬番の組み合わせ（昇順）
    horse_names: list[str]                # 馬名（表示用）
    expected_value: float                 # 期待値（1.0 超 = プラス収支見込み）
    model_score: float                    # モデルスコア（0〜1）
    recommended_bet: float                # 推奨購入金額（円）
    confidence: float                     # 信頼度（0〜1）
    notes: str = ""                       # 根拠メモ


@dataclass
class RaceBets:
    """1レースの全推奨買い目。"""
    race_id: str
    model_type: Literal["卍", "本命"]
    bets: list[BetRecommendation] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "race_id": self.race_id,
            "model_type": self.model_type,
            "bets": [
                {
                    "bet_type": b.bet_type,
                    "combinations": [list(c) for c in b.combinations],
                    "horse_names": b.horse_names,
                    "expected_value": round(b.expected_value, 3),
                    "model_score": round(b.model_score, 3),
                    "recommended_bet": b.recommended_bet,
                    "confidence": round(b.confidence, 3),
                    "notes": b.notes,
                }
                for b in self.bets
            ],
        }


# ── Kelly Criterion ────────────────────────────────────────────────

def _kelly_bet(
    win_prob: float,
    odds: float,
    base_bet: float = _BASE_BET,
    cap: float = _KELLY_CAP,
) -> float:
    """
    Kelly Criterion で最適賭け比率を算出し、ベット額を返す。

    f* = (p*(b+1) - 1) / b   ただし b = odds - 1
    """
    if odds <= 1.0 or win_prob <= 0:
        return 0.0
    b = odds - 1.0
    f = (win_prob * (b + 1) - 1) / b
    f = max(0.0, min(f, cap))
    return round(base_bet * (1 + f * 10), -2)  # 100円単位に丸め


# ── 馬名マップ取得ユーティリティ ──────────────────────────────────

def _name_map(df: pd.DataFrame) -> dict[int, str]:
    """DataFrame から {馬番: 馬名} マップを返す。"""
    if "horse_number" in df.columns and "horse_name" in df.columns:
        return dict(zip(df["horse_number"], df["horse_name"]))
    return {}


# ── 卍モデル用買い目生成 ──────────────────────────────────────────

class ManjiStrategy:
    """
    卍モデル（回収率特化）の買い目戦略。

    - EV_score > 1.0 の馬のみを対象に単勝・複勝を推奨
    - EV 上位 2 頭で馬連・ワイドを推奨
    - EV 上位 3 頭で三連複を推奨（EV >= 1.2 の場合のみ）
    """

    EV_THRESHOLD     = 1.0   # 単勝・複勝 推奨の最低 EV
    EV_COMBO_MIN     = 1.1   # 組み合わせ馬券の最低 EV（平均）
    EV_SANREN_MIN    = 1.2   # 三連複推奨の最低 EV

    def generate(
        self,
        race_id: str,
        df: pd.DataFrame,
        manji_scores: pd.Series,
    ) -> RaceBets:
        """
        卍モデルのスコアと出馬表 DataFrame から買い目を生成する。

        Args:
            race_id:      レース ID
            df:           FeatureBuilder.build_race_features() の出力
            manji_scores: ManjiModel.ev_score() の出力（EV 比率）

        Returns:
            RaceBets
        """
        result = RaceBets(race_id=race_id, model_type="卍")
        names = _name_map(df)

        # EV スコアを DataFrame に付加
        ev = manji_scores.rename("ev_score")
        scored = df.copy()
        scored["ev_score"] = ev.values if len(ev) == len(scored) else ev.reindex(scored.index).values
        scored = scored.sort_values("ev_score", ascending=False)

        # EV > 1.0 の馬を抽出
        pos_ev = scored[scored["ev_score"] >= self.EV_THRESHOLD]

        if pos_ev.empty:
            logger.info("race_id=%s: EV>1.0 の馬なし — 卍買い目なし", race_id)
            return result

        # ── 単勝 ──────────────────────────────────────────────────
        for _, row in pos_ev.iterrows():
            num  = int(row["horse_number"])
            ev_s = float(row["ev_score"])
            odds = float(row.get("win_odds") or 1.0)
            prob = min(ev_s / max(odds, 1.0), 1.0)
            bet  = _kelly_bet(prob, odds)
            if bet <= 0:
                bet = _BASE_BET

            result.bets.append(BetRecommendation(
                bet_type="単勝",
                combinations=[(num,)],
                horse_names=[names.get(num, str(num))],
                expected_value=ev_s,
                model_score=float(row["ev_score"]),
                recommended_bet=bet,
                confidence=min(ev_s / 2.0, 1.0),
                notes=f"EV={ev_s:.2f} odds={odds:.1f}",
            ))

        # ── 複勝（単勝候補全馬）────────────────────────────────────
        top_nums = [int(r["horse_number"]) for _, r in pos_ev.iterrows()]
        if top_nums:
            result.bets.append(BetRecommendation(
                bet_type="複勝",
                combinations=[(n,) for n in top_nums],
                horse_names=[names.get(n, str(n)) for n in top_nums],
                expected_value=float(pos_ev["ev_score"].mean()),
                model_score=float(pos_ev["ev_score"].mean()),
                recommended_bet=_BASE_BET * len(top_nums),
                confidence=0.6,
                notes=f"EV>{self.EV_THRESHOLD} の{len(top_nums)}頭を複勝",
            ))

        # ── 馬連・ワイド（EV上位2頭）─────────────────────────────
        if len(pos_ev) >= 2:
            top2 = pos_ev.head(2)
            nums2 = [int(r["horse_number"]) for _, r in top2.iterrows()]
            ev_mean2 = float(top2["ev_score"].mean())

            if ev_mean2 >= self.EV_COMBO_MIN:
                combo = tuple(sorted(nums2))
                for btype in ("馬連", "ワイド"):
                    result.bets.append(BetRecommendation(
                        bet_type=btype,  # type: ignore[arg-type]
                        combinations=[combo],
                        horse_names=[names.get(n, str(n)) for n in combo],
                        expected_value=ev_mean2,
                        model_score=ev_mean2,
                        recommended_bet=_BASE_BET * 2,
                        confidence=0.5,
                        notes=f"EV上位2頭 平均EV={ev_mean2:.2f}",
                    ))

        # ── 三連複（EV上位3頭）────────────────────────────────────
        if len(pos_ev) >= 3:
            top3 = pos_ev.head(3)
            ev_mean3 = float(top3["ev_score"].mean())
            if ev_mean3 >= self.EV_SANREN_MIN:
                nums3 = [int(r["horse_number"]) for _, r in top3.iterrows()]
                combo3 = tuple(sorted(nums3))
                result.bets.append(BetRecommendation(
                    bet_type="三連複",
                    combinations=[combo3],
                    horse_names=[names.get(n, str(n)) for n in combo3],
                    expected_value=ev_mean3,
                    model_score=ev_mean3,
                    recommended_bet=_BASE_BET * 3,
                    confidence=0.4,
                    notes=f"EV上位3頭 平均EV={ev_mean3:.2f}",
                ))

        logger.info(
            "卍買い目生成: race_id=%s %d 件 (EV>1.0 馬=%d 頭)",
            race_id, len(result.bets), len(pos_ev),
        )
        return result


# ── 本命モデル用買い目生成 ────────────────────────────────────────

class HonmeiStrategy:
    """
    本命モデル（的中率特化）の買い目戦略。

    - 勝率上位 1 頭を本命として単勝・複勝を推奨
    - 上位 2 頭で馬連・ワイドをフォーメーション
    - 上位 3 頭で三連複
    - 上位 1→2→3 で三連単（1着軸流し）
    """

    TOP_N_COMBO = 3   # 組み合わせに使う上位頭数

    def generate(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        """
        本命モデルのスコアから買い目を生成する。

        Args:
            race_id:        レース ID
            df:             特徴量 DataFrame
            honmei_scores:  HonmeiModel.predict() の出力

        Returns:
            RaceBets
        """
        result = RaceBets(race_id=race_id, model_type="本命")
        names = _name_map(df)

        scored = df.copy()
        scored["honmei_score"] = honmei_scores.values if len(honmei_scores) == len(scored) \
            else honmei_scores.reindex(scored.index).values
        scored = scored.sort_values("honmei_score", ascending=False)

        n = min(self.TOP_N_COMBO, len(scored))
        top = scored.head(n)
        top_nums = [int(r["horse_number"]) for _, r in top.iterrows()]

        if not top_nums:
            return result

        # ── 単勝（1位本命）────────────────────────────────────────
        num1 = top_nums[0]
        sc1  = float(scored.iloc[0]["honmei_score"])
        odds1 = float(scored.iloc[0].get("win_odds") or 1.0)
        bet1  = _kelly_bet(sc1, odds1)
        result.bets.append(BetRecommendation(
            bet_type="単勝",
            combinations=[(num1,)],
            horse_names=[names.get(num1, str(num1))],
            expected_value=sc1 * odds1,
            model_score=sc1,
            recommended_bet=max(bet1, _BASE_BET),
            confidence=sc1,
            notes=f"本命 P(win)={sc1:.2f} odds={odds1:.1f}",
        ))

        # ── 複勝（上位3頭）───────────────────────────────────────
        result.bets.append(BetRecommendation(
            bet_type="複勝",
            combinations=[(n,) for n in top_nums],
            horse_names=[names.get(n, str(n)) for n in top_nums],
            expected_value=float(top["honmei_score"].sum()),
            model_score=float(top["honmei_score"].mean()),
            recommended_bet=_BASE_BET * n,
            confidence=float(top["honmei_score"].sum()),
            notes=f"上位{n}頭を複勝",
        ))

        # ── 馬連・ワイド（上位2頭）────────────────────────────────
        if len(top_nums) >= 2:
            combo2 = tuple(sorted(top_nums[:2]))
            sc2 = float(scored.head(2)["honmei_score"].mean())
            for btype in ("馬連", "ワイド"):
                result.bets.append(BetRecommendation(
                    bet_type=btype,  # type: ignore[arg-type]
                    combinations=[combo2],
                    horse_names=[names.get(n, str(n)) for n in combo2],
                    expected_value=sc2 * 2,
                    model_score=sc2,
                    recommended_bet=_BASE_BET * 2,
                    confidence=sc2,
                    notes="本命・対抗フォーメーション",
                ))

        # ── 三連複（上位3頭 ボックス）────────────────────────────
        if len(top_nums) >= 3:
            combo3 = tuple(sorted(top_nums[:3]))
            sc3 = float(scored.head(3)["honmei_score"].mean())
            result.bets.append(BetRecommendation(
                bet_type="三連複",
                combinations=[combo3],
                horse_names=[names.get(n, str(n)) for n in combo3],
                expected_value=sc3 * 3,
                model_score=sc3,
                recommended_bet=_BASE_BET * 3,
                confidence=sc3,
                notes="本命・対抗・単穴 三連複",
            ))

            # ── 三連単（1着軸→2・3着流し）────────────────────────
            permutations = list(itertools.permutations(top_nums[:3]))
            result.bets.append(BetRecommendation(
                bet_type="三連単",
                combinations=[tuple(p) for p in permutations],
                horse_names=[names.get(n, str(n)) for n in top_nums[:3]],
                expected_value=sc3 * 4,
                model_score=sc3,
                recommended_bet=_BASE_BET * len(permutations),
                confidence=sc3 * 0.8,
                notes=f"上位3頭 {len(permutations)} 点ボックス",
            ))

        logger.info(
            "本命買い目生成: race_id=%s %d 件 (上位%d頭)",
            race_id, len(result.bets), n,
        )
        return result


# ── WIN5 ────────────────────────────────────────────────────────

@dataclass
class Win5Recommendation:
    """WIN5 レース横断推奨。"""
    race_ids: list[str]
    selections: dict[str, list[int]]   # {race_id: [馬番, ...]}
    horse_names: dict[str, list[str]]  # {race_id: [馬名, ...]}
    total_combinations: int
    recommended_bet: float
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "race_ids": self.race_ids,
            "selections": self.selections,
            "horse_names": self.horse_names,
            "total_combinations": self.total_combinations,
            "recommended_bet": self.recommended_bet,
            "notes": self.notes,
        }


def generate_win5(
    races: dict[str, pd.DataFrame],
    scores: dict[str, pd.Series],
    top_n: int = 2,
    max_combinations: int = 20,
) -> Win5Recommendation | None:
    """
    WIN5 対象レース群から推奨買い目を生成する。

    Args:
        races:  {race_id: 特徴量 DataFrame}
        scores: {race_id: モデルスコア Series}
        top_n:  各レースで選ぶ頭数（デフォルト2頭）
        max_combinations: 組み合わせ上限（超える場合は点数を絞る）

    Returns:
        Win5Recommendation（組み合わせ 0 の場合 None）
    """
    if len(races) != 5:
        logger.warning("WIN5 には 5 レース必要です (現在 %d レース)", len(races))
        return None

    selections: dict[str, list[int]] = {}
    horse_names_map: dict[str, list[str]] = {}

    for race_id, df in races.items():
        sc = scores.get(race_id)
        if sc is None or df.empty:
            continue
        names = _name_map(df)
        scored = df.copy()
        scored["score"] = sc.values if len(sc) == len(scored) else sc.reindex(scored.index).values
        top = scored.nlargest(top_n, "score")
        nums = [int(r["horse_number"]) for _, r in top.iterrows()]
        selections[race_id] = nums
        horse_names_map[race_id] = [names.get(n, str(n)) for n in nums]

    if not selections:
        return None

    total = 1
    for nums in selections.values():
        total *= len(nums)

    # 組み合わせが多すぎる場合は各レース1頭に絞る
    if total > max_combinations:
        for race_id in selections:
            selections[race_id] = selections[race_id][:1]
            horse_names_map[race_id] = horse_names_map[race_id][:1]
        total = 1

    return Win5Recommendation(
        race_ids=list(races.keys()),
        selections=selections,
        horse_names=horse_names_map,
        total_combinations=total,
        recommended_bet=float(_BASE_BET * total),
        notes=f"{total} 点購入（各レース上位 {top_n} 頭）",
    )


# ── ファサード ────────────────────────────────────────────────────

class BetGenerator:
    """
    HonmeiStrategy / ManjiStrategy のファサード。

    Usage:
        gen = BetGenerator()
        honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
        manji_bets  = gen.generate_manji(race_id, df, ev_scores)
    """

    def __init__(self) -> None:
        self._honmei = HonmeiStrategy()
        self._manji  = ManjiStrategy()

    def generate_honmei(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        return self._honmei.generate(race_id, df, honmei_scores)

    def generate_manji(
        self,
        race_id: str,
        df: pd.DataFrame,
        ev_scores: pd.Series,
    ) -> RaceBets:
        return self._manji.generate(race_id, df, ev_scores)

    def generate_win5(
        self,
        races: dict[str, pd.DataFrame],
        scores: dict[str, pd.Series],
        top_n: int = 2,
    ) -> Win5Recommendation | None:
        return generate_win5(races, scores, top_n=top_n)
