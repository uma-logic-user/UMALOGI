"""
全券種買い目ジェネレーター

モデルの予測スコアを元に、以下の券種の推奨買い目を生成する。
  単勝 / 複勝 / 馬連 / ワイド / 馬単 / 三連複 / 三連単 / WIN5

確率計算:
  Harville公式を用いて組み合わせ馬券の確率を推定する。
  P(A 1着) = p_A
  P(A 1着, B 2着) = p_A * p_B / (1 - p_A)      ... Harville exacta
  P(A-B 馬連) = P(A→B) + P(B→A)                 ... Harville quinella
  P(A 1着, B 2着, C 3着) = p_A * p_B/(1-p_A) * p_C/(1-p_A-p_B)

卍モデル  : EV_score > 1.0 の馬（期待値プラス）を優先
本命モデル: 勝率スコア上位を優先、フォーメーション軸に使用
"""

from __future__ import annotations

import itertools
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

logger = logging.getLogger(__name__)

BetType = Literal["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単", "WIN5"]

# Kelly Criterion 上限（過剰賭けリスク抑制）
_KELLY_CAP = 0.25
# デフォルト賭け単位（円）
_BASE_BET = 100
# JRA 控除率（券種別）
_TRACK_TAKE: dict[str, float] = {
    "単勝": 0.200, "複勝": 0.200,
    "馬連": 0.225, "ワイド": 0.225, "馬単": 0.250,
    "三連複": 0.250, "三連単": 0.275,
}


@dataclass
class BetConfig:
    """
    ケリー基準のハードキャップ設定。

    Attributes:
        bankroll:          総資金（円）
        max_bet_fraction:  1レースあたりの最大投資比率（0.0〜1.0）
        max_bet_per_combo: 1点あたりの最大購入額（円）
    """
    bankroll: float = 100_000.0
    max_bet_fraction: float = 0.05
    max_bet_per_combo: float = 1_000.0

    @property
    def max_race_bet(self) -> float:
        """1レースあたりの最大投資額（円）。"""
        return self.bankroll * self.max_bet_fraction


class OddsEstimator:
    """
    各券種の推定払戻オッズを過去実績から統計的に学習する。

    学習式:
        scale = median( race_payouts.payout / 100 / axis_win_odds )
        axis_win_odds = 当該レースの1着馬の単勝オッズ
    データ不足（< MIN_SAMPLES 件）の場合は固定スケールにフォールバックする。

    EV 算出式:
        EV = harville_prob × axis_win_odds × scale
        EV > 1.0 が期待値プラスの基準
    """

    _MIN_SAMPLES: int = 50

    # フォールバック: 単勝オッズに掛ける経験則スケール
    _DEFAULT_SCALE: dict[str, float] = {
        "単勝": 1.0, "複勝": 0.33,
        "馬連": 6.0, "ワイド": 2.5,
        "馬単": 12.0, "三連複": 30.0, "三連単": 150.0,
    }

    def __init__(self, conn: sqlite3.Connection | None = None) -> None:
        self._scales: dict[str, float] = dict(self._DEFAULT_SCALE)
        if conn is not None:
            self._fit(conn)

    def _fit(self, conn: sqlite3.Connection) -> None:
        """過去の race_payouts から券種別スケールを推定する。"""
        for bet_type in self._DEFAULT_SCALE:
            try:
                rows = conn.execute(
                    """
                    SELECT CAST(rp.payout AS REAL) / 100.0 / rr.win_odds
                    FROM race_payouts rp
                    JOIN race_results rr
                      ON rr.race_id = rp.race_id
                     AND rr.rank    = 1
                     AND rr.win_odds >= 1.2
                    WHERE rp.bet_type = ?
                      AND rp.payout   > 0
                    """,
                    (bet_type,),
                ).fetchall()

                ratios = sorted(r[0] for r in rows if r[0] is not None and r[0] > 0)
                n = len(ratios)
                if n >= self._MIN_SAMPLES:
                    mid = n // 2
                    median = (
                        ratios[mid] if n % 2
                        else (ratios[mid - 1] + ratios[mid]) / 2
                    )
                    self._scales[bet_type] = round(median, 3)
                    logger.debug(
                        "OddsEstimator %s: n=%d median_scale=%.3f (default=%.3f)",
                        bet_type, n, median, self._DEFAULT_SCALE[bet_type],
                    )
                else:
                    logger.debug(
                        "OddsEstimator %s: データ不足(%d件) デフォルトスケール使用",
                        bet_type, n,
                    )
            except Exception as exc:
                logger.warning("OddsEstimator._fit %s 失敗: %s", bet_type, exc)

    def ev(self, harville_prob: float, bet_type: str, axis_odds: float) -> float:
        """
        Harville確率と軸馬単勝オッズから期待値を推定する。

        Returns:
            EV = harville_prob × axis_win_odds × learned_scale
            (1.0 超 = 期待値プラス)
        """
        scale = self._scales.get(bet_type, 1.0)
        return harville_prob * axis_odds * scale

    def scale(self, bet_type: str) -> float:
        """券種のスケール係数を返す（デバッグ・テスト用）。"""
        return self._scales.get(bet_type, 1.0)


@dataclass
class BetRecommendation:
    """1つの買い目推奨。"""
    bet_type: BetType
    combinations: list[tuple[int, ...]]   # 馬番の組み合わせ（馬連は昇順、馬単は着順）
    horse_names: list[str]                # 馬名（表示用、組み合わせ順）
    expected_value: float                 # 期待値（1.0 超 = プラス収支見込み）
    model_score: float                    # モデルスコア（0〜1、Harville確率）
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


# ── Harville 確率計算 ────────────────────────────────────────────

def _normalize(probs: list[float]) -> list[float]:
    """win_probs を合計=1 に正規化する。0以下は 1e-9 に丸め。"""
    probs = [max(p, 1e-9) for p in probs]
    total = sum(probs)
    return [p / total for p in probs]


def _harville_exacta(probs: list[float], i: int, j: int) -> float:
    """
    Harville公式: 馬番インデックス i が1着、j が2着になる確率。

    P(i 1st, j 2nd) = p_i * p_j / (1 - p_i)
    """
    p = _normalize(probs)
    if i == j or i >= len(p) or j >= len(p):
        return 0.0
    denom = 1.0 - p[i]
    if denom <= 0:
        return 0.0
    return p[i] * p[j] / denom


def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    """
    Harville公式: i・j が1着2着（順不問）になる確率。
    馬連・ワイドの確率推定に使用。
    """
    return _harville_exacta(probs, i, j) + _harville_exacta(probs, j, i)


def _harville_trifecta(probs: list[float], i: int, j: int, k: int) -> float:
    """
    Harville公式: i が1着、j が2着、k が3着になる確率。

    P = p_i * (p_j/(1-p_i)) * (p_k/(1-p_i-p_j))
    """
    p = _normalize(probs)
    if len({i, j, k}) < 3 or max(i, j, k) >= len(p):
        return 0.0
    d1 = 1.0 - p[i]
    d2 = 1.0 - p[i] - p[j]
    if d1 <= 0 or d2 <= 0:
        return 0.0
    return p[i] * (p[j] / d1) * (p[k] / d2)


def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    """
    Harville公式: i・j・k が1〜3着（順不問）に入る確率。
    三連複の確率推定に使用。
    """
    return sum(
        _harville_trifecta(probs, a, b, c)
        for a, b, c in itertools.permutations([i, j, k])
    )


def _ev_estimate(harville_prob: float, win_odds: float, bet_type: str) -> float:
    """
    単勝オッズを使って多馬券の期待値を推定する（後方互換ラッパー）。

    EV = harville_prob × axis_win_odds × scale
    scale: OddsEstimator._DEFAULT_SCALE に基づく固定値
    """
    scale = OddsEstimator._DEFAULT_SCALE.get(bet_type, 1.0)
    return harville_prob * win_odds * scale


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
    - EV 上位 2 頭で馬連・ワイド・馬単を推奨
    - EV 上位 3 頭で三連複を推奨（EV >= 1.2 の場合のみ）
    """

    EV_THRESHOLD  = 1.0   # 単勝・複勝 推奨の最低 EV
    EV_COMBO_MIN  = 1.1   # 組み合わせ馬券の最低 EV（平均）
    EV_SANREN_MIN = 1.2   # 三連複推奨の最低 EV

    def __init__(self, estimator: OddsEstimator | None = None) -> None:
        self._estimator = estimator or OddsEstimator()

    def generate(
        self,
        race_id: str,
        df: pd.DataFrame,
        manji_scores: pd.Series,
    ) -> RaceBets:
        """
        卍モデルのスコアと出馬表 DataFrame から全券種の買い目を生成する。

        Args:
            race_id:      レース ID
            df:           FeatureBuilder.build_race_features() の出力
            manji_scores: ManjiModel.ev_score() の出力（EV 比率）

        Returns:
            RaceBets
        """
        result = RaceBets(race_id=race_id, model_type="卍")
        names = _name_map(df)

        ev = manji_scores.rename("ev_score")
        scored = df.copy()
        scored["ev_score"] = (
            ev.values if len(ev) == len(scored)
            else ev.reindex(scored.index).values
        )
        scored = scored.sort_values("ev_score", ascending=False)

        # 全馬スコアリスト（Harville 計算用）
        all_nums   = [int(r["horse_number"]) for _, r in scored.iterrows()]
        all_scores = [float(r["ev_score"]) for _, r in scored.iterrows()]

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

        # ── 馬連・ワイド・馬単（EV上位2頭）───────────────────────
        if len(pos_ev) >= 2:
            top2    = pos_ev.head(2)
            n0      = int(top2.iloc[0]["horse_number"])
            n1      = int(top2.iloc[1]["horse_number"])
            ev_mean2 = float(top2["ev_score"].mean())

            if ev_mean2 >= self.EV_COMBO_MIN:
                i0 = all_nums.index(n0) if n0 in all_nums else 0
                i1 = all_nums.index(n1) if n1 in all_nums else 1
                q_prob  = _harville_quinella(all_scores, i0, i1)
                ex01    = _harville_exacta(all_scores, i0, i1)
                ex10    = _harville_exacta(all_scores, i1, i0)
                ex_prob = max(ex01, ex10)
                combo   = tuple(sorted([n0, n1]))
                axis_odds = float(top2.iloc[0].get("win_odds") or 10.0)

                result.bets.append(BetRecommendation(
                    bet_type="馬連",
                    combinations=[combo],
                    horse_names=[names.get(n, str(n)) for n in combo],
                    expected_value=self._estimator.ev(q_prob, "馬連", axis_odds),
                    model_score=q_prob,
                    recommended_bet=_BASE_BET * 2,
                    confidence=min(q_prob * 5, 1.0),
                    notes=f"EV上位2頭 Harville={q_prob:.3f} 平均EV={ev_mean2:.2f}",
                ))

                result.bets.append(BetRecommendation(
                    bet_type="ワイド",
                    combinations=[combo],
                    horse_names=[names.get(n, str(n)) for n in combo],
                    expected_value=self._estimator.ev(q_prob, "ワイド", axis_odds),
                    model_score=q_prob,
                    recommended_bet=_BASE_BET * 2,
                    confidence=min(q_prob * 6, 1.0),
                    notes=f"EV上位2頭 ワイド Harville={q_prob:.3f}",
                ))

                # 馬単（EV大きい方を軸1着）
                exacta_combo = (n0, n1)
                result.bets.append(BetRecommendation(
                    bet_type="馬単",
                    combinations=[exacta_combo],
                    horse_names=[names.get(n, str(n)) for n in exacta_combo],
                    expected_value=self._estimator.ev(ex_prob, "馬単", axis_odds),
                    model_score=ex_prob,
                    recommended_bet=_BASE_BET * 2,
                    confidence=min(ex_prob * 8, 1.0),
                    notes=f"EV軸{n0}→{n1} Harville={ex_prob:.3f}",
                ))

        # ── 三連複（EV上位3頭）────────────────────────────────────
        if len(pos_ev) >= 3:
            top3    = pos_ev.head(3)
            ev_mean3 = float(top3["ev_score"].mean())
            if ev_mean3 >= self.EV_SANREN_MIN:
                nums3 = [int(r["horse_number"]) for _, r in top3.iterrows()]
                idxs3 = [all_nums.index(n) if n in all_nums else j for j, n in enumerate(nums3)]
                trio_prob  = _harville_trio(all_scores, *idxs3)
                combo3     = tuple(sorted(nums3))
                axis_odds3 = float(top3.iloc[0].get("win_odds") or 10.0)

                result.bets.append(BetRecommendation(
                    bet_type="三連複",
                    combinations=[combo3],
                    horse_names=[names.get(n, str(n)) for n in combo3],
                    expected_value=self._estimator.ev(trio_prob, "三連複", axis_odds3),
                    model_score=trio_prob,
                    recommended_bet=_BASE_BET * 3,
                    confidence=min(trio_prob * 15, 1.0),
                    notes=f"EV上位3頭 Harville={trio_prob:.3f} 平均EV={ev_mean3:.2f}",
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
    - 上位 2 頭で馬連・ワイド・馬単をフォーメーション
    - 上位 3 頭で三連複・三連単
    """

    TOP_N_COMBO = 3   # 組み合わせに使う上位頭数

    def __init__(self, estimator: OddsEstimator | None = None) -> None:
        self._estimator = estimator or OddsEstimator()

    def generate(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        """
        本命モデルのスコアから全券種の買い目を生成する。

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
        scored["honmei_score"] = (
            honmei_scores.values if len(honmei_scores) == len(scored)
            else honmei_scores.reindex(scored.index).values
        )
        scored = scored.sort_values("honmei_score", ascending=False)

        n = min(self.TOP_N_COMBO, len(scored))
        top = scored.head(n)
        top_nums   = [int(r["horse_number"]) for _, r in top.iterrows()]
        top_scores = [float(r["honmei_score"]) for _, r in top.iterrows()]

        # 全馬スコアリスト（Harville 計算用）
        all_nums   = [int(r["horse_number"]) for _, r in scored.iterrows()]
        all_scores = [float(r["honmei_score"]) for _, r in scored.iterrows()]

        if not top_nums:
            return result

        # ── 単勝（1位本命）────────────────────────────────────────
        num1  = top_nums[0]
        sc1   = top_scores[0]
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
            combinations=[(num,) for num in top_nums],
            horse_names=[names.get(num, str(num)) for num in top_nums],
            expected_value=float(top["honmei_score"].sum()),
            model_score=float(top["honmei_score"].mean()),
            recommended_bet=_BASE_BET * n,
            confidence=float(top["honmei_score"].sum()),
            notes=f"上位{n}頭を複勝",
        ))

        # ── 馬連・ワイド・馬単（上位2頭）──────────────────────────
        if len(top_nums) >= 2:
            n0, n1  = top_nums[0], top_nums[1]
            i0 = all_nums.index(n0) if n0 in all_nums else 0
            i1 = all_nums.index(n1) if n1 in all_nums else 1
            q_prob     = _harville_quinella(all_scores, i0, i1)
            ex01       = _harville_exacta(all_scores, i0, i1)
            ex10       = _harville_exacta(all_scores, i1, i0)
            ex_prob    = max(ex01, ex10)
            combo2     = tuple(sorted([n0, n1]))
            axis_odds2 = float(scored.iloc[0].get("win_odds") or 10.0)

            result.bets.append(BetRecommendation(
                bet_type="馬連",
                combinations=[combo2],
                horse_names=[names.get(c, str(c)) for c in combo2],
                expected_value=self._estimator.ev(q_prob, "馬連", axis_odds2),
                model_score=q_prob,
                recommended_bet=_BASE_BET * 2,
                confidence=q_prob,
                notes=f"本命・対抗 Harville={q_prob:.3f}",
            ))

            result.bets.append(BetRecommendation(
                bet_type="ワイド",
                combinations=[combo2],
                horse_names=[names.get(c, str(c)) for c in combo2],
                expected_value=self._estimator.ev(q_prob, "ワイド", axis_odds2),
                model_score=q_prob,
                recommended_bet=_BASE_BET * 2,
                confidence=min(q_prob * 1.3, 1.0),
                notes=f"本命・対抗 ワイド Harville={q_prob:.3f}",
            ))

            # 馬単（本命→対抗 + 対抗→本命 の2点）
            exacta_combos = [(n0, n1), (n1, n0)]
            result.bets.append(BetRecommendation(
                bet_type="馬単",
                combinations=exacta_combos,
                horse_names=[names.get(n0, str(n0)), names.get(n1, str(n1))],
                expected_value=self._estimator.ev(ex_prob, "馬単", axis_odds2),
                model_score=ex_prob,
                recommended_bet=_BASE_BET * 2,
                confidence=ex_prob,
                notes=f"本命↔対抗 馬単 Harville={ex_prob:.3f}",
            ))

        # ── 三連複（上位3頭 ボックス）────────────────────────────
        if len(top_nums) >= 3:
            n0, n1, n2 = top_nums[0], top_nums[1], top_nums[2]
            i0 = all_nums.index(n0) if n0 in all_nums else 0
            i1 = all_nums.index(n1) if n1 in all_nums else 1
            i2 = all_nums.index(n2) if n2 in all_nums else 2
            trio_prob     = _harville_trio(all_scores, i0, i1, i2)
            trifecta_prob = _harville_trifecta(all_scores, i0, i1, i2)
            combo3        = tuple(sorted([n0, n1, n2]))
            axis_odds3    = float(scored.iloc[0].get("win_odds") or 10.0)

            result.bets.append(BetRecommendation(
                bet_type="三連複",
                combinations=[combo3],
                horse_names=[names.get(c, str(c)) for c in combo3],
                expected_value=self._estimator.ev(trio_prob, "三連複", axis_odds3),
                model_score=trio_prob,
                recommended_bet=_BASE_BET * 3,
                confidence=trio_prob,
                notes=f"本命・対抗・単穴 Harville={trio_prob:.3f}",
            ))

            # 三連単（6点ボックス）
            trifecta_combos = list(itertools.permutations([n0, n1, n2]))
            result.bets.append(BetRecommendation(
                bet_type="三連単",
                combinations=[tuple(p) for p in trifecta_combos],
                horse_names=[names.get(n, str(n)) for n in [n0, n1, n2]],
                expected_value=self._estimator.ev(trifecta_prob, "三連単", axis_odds3),
                model_score=trifecta_prob,
                recommended_bet=_BASE_BET * len(trifecta_combos),
                confidence=trifecta_prob * 0.8,
                notes=f"上位3頭 {len(trifecta_combos)}点ボックス Harville={trifecta_prob:.3f}",
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
        scored["score"] = (
            sc.values if len(sc) == len(scored)
            else sc.reindex(scored.index).values
        )
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
        gen = BetGenerator(conn=conn, config=BetConfig(bankroll=200_000))
        honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
        manji_bets  = gen.generate_manji(race_id, df, ev_scores)
    """

    def __init__(
        self,
        conn: sqlite3.Connection | None = None,
        config: BetConfig | None = None,
    ) -> None:
        estimator = OddsEstimator(conn)
        self._config = config or BetConfig()
        self._honmei = HonmeiStrategy(estimator=estimator)
        self._manji  = ManjiStrategy(estimator=estimator)

    def _apply_caps(self, bets: RaceBets) -> None:
        """
        in-place で recommended_bet にハードキャップを適用する。

        Step 1: 1点あたりキャップ（max_bet_per_combo）
        Step 2: レース合計キャップ（bankroll × max_bet_fraction）
                合計超過時は比例縮小し 100円単位に丸める。
        """
        if not bets.bets:
            return

        max_per_combo = self._config.max_bet_per_combo
        max_total = self._config.max_race_bet

        for b in bets.bets:
            b.recommended_bet = max(
                min(b.recommended_bet, max_per_combo),
                float(_BASE_BET),
            )

        total = sum(b.recommended_bet for b in bets.bets)
        if total > max_total and total > 0:
            ratio = max_total / total
            for b in bets.bets:
                raw = b.recommended_bet * ratio
                b.recommended_bet = float(max(round(raw / 100) * 100, _BASE_BET))

    def generate_honmei(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        bets = self._honmei.generate(race_id, df, honmei_scores)
        self._apply_caps(bets)
        return bets

    def generate_manji(
        self,
        race_id: str,
        df: pd.DataFrame,
        ev_scores: pd.Series,
    ) -> RaceBets:
        bets = self._manji.generate(race_id, df, ev_scores)
        self._apply_caps(bets)
        return bets

    def generate_win5(
        self,
        races: dict[str, pd.DataFrame],
        scores: dict[str, pd.Series],
        top_n: int = 2,
    ) -> Win5Recommendation | None:
        return generate_win5(races, scores, top_n=top_n)
