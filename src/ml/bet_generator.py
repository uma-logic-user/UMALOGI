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

卍モデル  : EV_score >= 1.1 の馬（バックテスト最適閾値）を優先
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
# 卍モデル 単勝・複勝推奨の最低EV閾値（backtest_ev_threshold.py で最適化: 4/12-4/19 ROI=118%）
_MANJI_EV_THRESHOLD: float = 1.1
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

    # Platt確率過大評価 × 高オッズ × 大スケールによる「EV幻覚」防止用キャップ
    # 理論的に信頼できるモデルでも券種別に超えないはずの上限値
    _EV_MAX: dict[str, float] = {
        "単勝": 5.0, "複勝": 3.0,
        "馬連": 4.0, "ワイド": 3.0,
        "馬単": 5.0, "三連複": 5.0, "三連単": 6.0,
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
            EV = harville_prob × axis_win_odds × learned_scale  (上限 _EV_MAX)
            (1.0 超 = 期待値プラス)

        Note:
            Platt Scaling が大穴馬の確率を過大評価すると EV が爆発する。
            _EV_MAX で上限を設けて「期待値の幻覚」を防止する。
        """
        scale = self._scales.get(bet_type, 1.0)
        raw   = harville_prob * axis_odds * scale
        cap   = self._EV_MAX.get(bet_type, 5.0)
        if raw > cap:
            logger.debug(
                "EV capped: bet_type=%s raw=%.2f → %.2f (harville=%.4f axis_odds=%.1f scale=%.1f)",
                bet_type, raw, cap, harville_prob, axis_odds, scale,
            )
        return min(raw, cap)

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

    EV = harville_prob × axis_win_odds × scale  (上限 OddsEstimator._EV_MAX)
    scale: OddsEstimator._DEFAULT_SCALE に基づく固定値
    """
    scale = OddsEstimator._DEFAULT_SCALE.get(bet_type, 1.0)
    cap   = OddsEstimator._EV_MAX.get(bet_type, 5.0)
    return min(harville_prob * win_odds * scale, cap)


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

    EV_THRESHOLD  = _MANJI_EV_THRESHOLD  # 単勝・複勝 推奨の最低 EV（最適化済み定数）
    EV_COMBO_MIN  = 1.2   # 組み合わせ馬券の最低 EV（平均）
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

        # 馬番が不正な行を除外（枠順未確定で horse_number=0 が入る場合）
        invalid = scored[scored["horse_number"] < 1]
        if not invalid.empty:
            logger.warning(
                "race_id=%s: 馬番 < 1 の行を除外します (%d 頭: %s)",
                race_id, len(invalid),
                invalid["horse_name"].tolist(),
            )
            scored = scored[scored["horse_number"] >= 1]

        if scored.empty:
            return result

        # 全馬スコアリスト（Harville 計算用）
        all_nums   = [int(r["horse_number"]) for _, r in scored.iterrows()]
        all_scores = [float(r["ev_score"]) for _, r in scored.iterrows()]

        # 確率至上主義: EVゲート撤廃 → モデルスコア上位3頭を無条件選択
        _TOP_N = min(3, len(scored))
        pos_ev = scored.head(_TOP_N)

        # ── 単勝（スコア1位を無条件推奨）────────────────────────
        top_row  = pos_ev.iloc[0]
        num_top  = int(top_row["horse_number"])
        ev_top   = float(top_row["ev_score"])
        odds_top = float(top_row.get("win_odds") or 1.0)
        prob_top = min(ev_top / max(odds_top, 1.0), 1.0)
        bet_top  = _kelly_bet(prob_top, odds_top) or _BASE_BET
        result.bets.append(BetRecommendation(
            bet_type="単勝",
            combinations=[(num_top,)],
            horse_names=[names.get(num_top, str(num_top))],
            expected_value=ev_top,
            model_score=ev_top,
            recommended_bet=bet_top,
            confidence=min(ev_top / 2.0, 1.0),
            notes=f"確率1位(卍) EV={ev_top:.2f} odds={odds_top:.1f}",
        ))

        # ── 複勝（上位3頭）────────────────────────────────────────
        top_nums = [int(r["horse_number"]) for _, r in pos_ev.iterrows()]
        result.bets.append(BetRecommendation(
            bet_type="複勝",
            combinations=[(n,) for n in top_nums],
            horse_names=[names.get(n, str(n)) for n in top_nums],
            expected_value=float(pos_ev["ev_score"].mean()),
            model_score=float(pos_ev["ev_score"].mean()),
            recommended_bet=_BASE_BET * len(top_nums),
            confidence=0.6,
            notes=f"確率上位{len(top_nums)}頭を複勝",
        ))

        # ── 馬連・ワイド（確率上位2頭）───────────────────────────
        if len(pos_ev) >= 2:
            top2    = pos_ev.head(2)
            n0      = int(top2.iloc[0]["horse_number"])
            n1      = int(top2.iloc[1]["horse_number"])
            ev_mean2 = float(top2["ev_score"].mean())

            if True:  # EVゲート撤廃: 常に推奨
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
                # 馬単・三連単は ROI=0% のため生成しない（2024バックテスト実証済み）

        # ── 三連複（合成EV上位3点まで）──────────────────────────
        # 候補: EV > 0.8 の全馬を対象に全3頭組み合わせを列挙し
        # 確率至上主義: EVゲート撤廃 → 上位5頭から三連複組み合わせを探索
        _MIN_TRIO_PROB  = 0.003 # Harville確率の最低フィルター
        _MAX_SANREN     = 3     # 最大推奨点数

        cand_ev = scored.head(min(5, len(scored)))
        if len(cand_ev) >= 3:
            axis_odds_s = float(cand_ev.iloc[0].get("win_odds") or 10.0)
            cand_list = [(int(r["horse_number"]), float(r["ev_score"]))
                         for _, r in cand_ev.iterrows()]
            trio_candidates: list[tuple[float, float, tuple, list[str]]] = []
            for (na, ea), (nb, eb), (nc, ec) in itertools.combinations(cand_list, 3):
                try:
                    ia = all_nums.index(na)
                    ib = all_nums.index(nb)
                    ic = all_nums.index(nc)
                except ValueError:
                    continue
                tp = _harville_trio(all_scores, ia, ib, ic)
                if tp < _MIN_TRIO_PROB:
                    continue
                ev_composite = self._estimator.ev(tp, "三連複", axis_odds_s)
                trio_candidates.append((
                    ev_composite, tp,
                    tuple(sorted([na, nb, nc])),
                    [names.get(n, str(n)) for n in sorted([na, nb, nc])],
                ))
            trio_candidates.sort(key=lambda x: x[0], reverse=True)

            seen_combos: set[tuple] = set()
            for ev_c, tp, combo3, hnames3 in trio_candidates[:_MAX_SANREN]:
                if combo3 in seen_combos:
                    continue
                seen_combos.add(combo3)
                result.bets.append(BetRecommendation(
                    bet_type="三連複",
                    combinations=[combo3],
                    horse_names=hnames3,
                    expected_value=ev_c,
                    model_score=tp,
                    recommended_bet=_BASE_BET * 3,
                    confidence=min(tp * 15, 1.0),
                    notes=f"合成EV={ev_c:.2f} Harville={tp:.4f} 馬番={combo3}",
                ))

        logger.info(
            "卍買い目生成: race_id=%s %d 件 (確率上位%d頭選択)",
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
        # Platt確率の過大評価防止: 市場オッズが示す公平確率の4倍を上限にクリップする
        # 例: 20倍馬の公平確率 = 1/20 = 5% → 最大20%まで許容（4× cap）
        all_nums   = [int(r["horse_number"])   for _, r in scored.iterrows()]
        all_scores = [
            float(min(
                r["honmei_score"],
                4.0 / max(float(r.get("win_odds") or 999.0), 1.0),
            ))
            for _, r in scored.iterrows()
        ]

        if not top_nums:
            return result

        # ── 単勝（確率1位を無条件推奨）────────────────────────────
        num1  = top_nums[0]
        sc1   = top_scores[0]
        odds1 = float(scored.iloc[0].get("win_odds") or 1.0)
        ev1   = min(sc1 * odds1, OddsEstimator._EV_MAX["単勝"])
        bet1  = _kelly_bet(sc1, odds1)
        result.bets.append(BetRecommendation(
            bet_type="単勝",
            combinations=[(num1,)],
            horse_names=[names.get(num1, str(num1))],
            expected_value=ev1,
            model_score=sc1,
            recommended_bet=max(bet1, _BASE_BET),
            confidence=sc1,
            notes=f"確率1位 P(win)={sc1:.2f} odds={odds1:.1f} EV={ev1:.2f}",
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
            # 馬単・三連単は ROI=0% のため生成しない（2024バックテスト実証済み）

        # ── 三連複（上位5頭から合成EV上位2点）─────────────────
        # 固定3頭ではなく上位5頭の全組み合わせを探索し
        # 合成EV（Harville確率×スケール）が高い組み合わせを推奨
        _HC_CANDS    = min(5, len(scored))
        _HC_MIN_PROB = 0.003
        _HC_MAX_BETS = 2

        top5_cands = scored.head(_HC_CANDS)
        cand5_list = [(int(r["horse_number"]), float(r["honmei_score"]))
                      for _, r in top5_cands.iterrows()]
        axis_odds5 = float(scored.iloc[0].get("win_odds") or 10.0)

        trio_cands5: list[tuple[float, float, tuple, list[str]]] = []
        for (na, _), (nb, __), (nc, ___) in itertools.combinations(cand5_list, 3):
            try:
                ia5 = all_nums.index(na); ib5 = all_nums.index(nb); ic5 = all_nums.index(nc)
            except ValueError:
                continue
            tp5 = _harville_trio(all_scores, ia5, ib5, ic5)
            if tp5 < _HC_MIN_PROB:
                continue
            ev5 = self._estimator.ev(tp5, "三連複", axis_odds5)
            combo5 = tuple(sorted([na, nb, nc]))
            trio_cands5.append((ev5, tp5, combo5,
                                 [names.get(n, str(n)) for n in combo5]))
        trio_cands5.sort(key=lambda x: x[0], reverse=True)

        # 三連複を1レコードに集約（UNIQUE制約対応）
        seen_h: set[tuple] = set()
        trio_combos5:  list[tuple] = []
        trio_names5:   list[str]   = []
        trio_ev5_best  = 0.0
        trio_tp5_best  = 0.0
        for ev5c, tp5, combo5, hnames5 in trio_cands5[:_HC_MAX_BETS]:
            if combo5 in seen_h:
                continue
            seen_h.add(combo5)
            trio_combos5.append(combo5)
            for nm in hnames5:
                if nm not in trio_names5:
                    trio_names5.append(nm)
            if ev5c > trio_ev5_best:
                trio_ev5_best = ev5c
                trio_tp5_best = tp5

        if trio_combos5:
            result.bets.append(BetRecommendation(
                bet_type="三連複",
                combinations=trio_combos5,
                horse_names=trio_names5,
                expected_value=trio_ev5_best,
                model_score=trio_tp5_best,
                recommended_bet=_BASE_BET * 3 * len(trio_combos5),
                confidence=min(trio_tp5_best * 12, 1.0),
                notes=f"合成EV最大={trio_ev5_best:.2f} {len(trio_combos5)}点 馬番={trio_combos5}",
            ))

        # ── 三連単フォーメーション（1着固定マルチ）─────────────────
        # 軸: スコア最上位1頭（1着固定）
        # 相手: 上位4頭（軸除く）から2・3着候補（全順列）
        # Harville確率上位6点を1レコードに集約（UNIQUE制約対応）
        _ST_AITE_N   = min(4, len(scored) - 1)   # 相手候補数（軸除く）
        _ST_MAX_BETS = 6                          # 最大点数

        if len(top_nums) >= 3 and _ST_AITE_N >= 2:
            axis_num  = top_nums[0]
            axis_idx  = all_nums.index(axis_num) if axis_num in all_nums else 0
            aite_nums = [num for num in all_nums[1:_ST_AITE_N + 1]
                         if num != axis_num][:_ST_AITE_N]

            trifecta_cands: list[tuple[float, tuple]] = []
            for ni, nj in itertools.permutations(aite_nums, 2):
                try:
                    ii = all_nums.index(ni)
                    ij = all_nums.index(nj)
                except ValueError:
                    continue
                p_tf = _harville_trio(all_scores, axis_idx, ii, ij)
                trifecta_cands.append((p_tf, (axis_num, ni, nj)))

            trifecta_cands.sort(key=lambda x: x[0], reverse=True)
            top_tf = trifecta_cands[:_ST_MAX_BETS]

            if top_tf:
                axis_odds_tf = float(scored.iloc[0].get("win_odds") or 10.0)
                best_p_tf    = top_tf[0][0]
                ev_tf        = self._estimator.ev(best_p_tf, "三連単", axis_odds_tf)
                all_tf_combos = [c for _, c in top_tf]
                # 重複なし馬名
                seen_nms: set[str] = set()
                all_tf_names: list[str] = []
                for _, (ca, cb, cc) in top_tf:
                    for num in (ca, cb, cc):
                        nm = names.get(num, str(num))
                        if nm not in seen_nms:
                            all_tf_names.append(nm)
                            seen_nms.add(nm)
                result.bets.append(BetRecommendation(
                    bet_type="三連単",
                    combinations=all_tf_combos,
                    horse_names=all_tf_names,
                    expected_value=ev_tf,
                    model_score=best_p_tf,
                    recommended_bet=_BASE_BET * len(all_tf_combos),
                    confidence=min(best_p_tf * 30, 1.0),
                    notes=(
                        f"1着固定フォーメーション 軸={axis_num}番 "
                        f"{len(all_tf_combos)}点 相手={aite_nums} "
                        f"Harville最大={best_p_tf:.4f}"
                    ),
                ))

        logger.info(
            "本命買い目生成: race_id=%s %d 件 (上位%d頭)",
            race_id, len(result.bets), n,
        )
        return result


# ── WIN5 ────────────────────────────────────────────────────────

# WIN5 SABC ランク閾値（Harville win_prob 基準）
_WIN5_RANK_S = 0.30   # S: 勝率30%超 → 本命
_WIN5_RANK_A = 0.18   # A: 18-30%   → 対抗
_WIN5_RANK_B = 0.09   # B: 9-18%    → 注意
                       # C: 9%未満  → ヒモ


def _win5_rank(prob: float) -> str:
    """Harville 勝率から WIN5 SABC ランクを返す。"""
    if prob >= _WIN5_RANK_S:
        return "S"
    if prob >= _WIN5_RANK_A:
        return "A"
    if prob >= _WIN5_RANK_B:
        return "B"
    return "C"


@dataclass
class Win5HorseRank:
    """WIN5 における1頭の SABC ランク情報。"""
    horse_number: int
    horse_name: str
    win_prob: float
    rank: str   # "S" / "A" / "B" / "C"


@dataclass
class Win5Recommendation:
    """WIN5 レース横断推奨。"""
    race_ids: list[str]
    selections: dict[str, list[int]]         # {race_id: [馬番, ...]}
    horse_names: dict[str, list[str]]        # {race_id: [馬名, ...]}
    horse_ranks: dict[str, list[Win5HorseRank]]  # {race_id: [ランク情報, ...]}
    total_combinations: int
    recommended_bet: float
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "race_ids": self.race_ids,
            "selections": self.selections,
            "horse_names": self.horse_names,
            "horse_ranks": {
                rid: [{"horse_number": h.horse_number, "horse_name": h.horse_name,
                        "win_prob": round(h.win_prob, 4), "rank": h.rank}
                       for h in ranks]
                for rid, ranks in self.horse_ranks.items()
            },
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
    horse_ranks_map: dict[str, list[Win5HorseRank]] = {}

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
        scored = scored[scored["horse_number"] >= 1].copy()
        # 正規化してHarville win_prob を計算
        total_sc = max(scored["score"].sum(), 1e-9)
        scored["win_prob"] = scored["score"] / total_sc

        top = scored.nlargest(top_n, "score")
        nums = [int(r["horse_number"]) for _, r in top.iterrows()]
        selections[race_id] = nums
        horse_names_map[race_id] = [names.get(n, str(n)) for n in nums]

        # 全馬にSABCランクを付与（上位5頭まで記録）
        horse_ranks_map[race_id] = [
            Win5HorseRank(
                horse_number=int(r["horse_number"]),
                horse_name=names.get(int(r["horse_number"]), str(int(r["horse_number"]))),
                win_prob=float(r["win_prob"]),
                rank=_win5_rank(float(r["win_prob"])),
            )
            for _, r in scored.nlargest(min(8, len(scored)), "score").iterrows()
        ]

    if not selections:
        return None

    total = 1
    for nums in selections.values():
        total *= len(nums)

    # 組み合わせが多すぎる場合は各レースSランク頭に絞る（なければA→B→先頭）
    if total > max_combinations:
        for race_id in selections:
            ranks = horse_ranks_map.get(race_id, [])
            for target_rank in ("S", "A", "B"):
                rank_nums = [h.horse_number for h in ranks if h.rank == target_rank]
                if rank_nums:
                    selections[race_id] = rank_nums[:1]
                    horse_names_map[race_id] = [names.get(n, str(n)) for n in rank_nums[:1]]
                    break
            else:
                selections[race_id] = selections[race_id][:1]
                horse_names_map[race_id] = horse_names_map[race_id][:1]
        total = 1

    return Win5Recommendation(
        race_ids=list(races.keys()),
        selections=selections,
        horse_names=horse_names_map,
        horse_ranks=horse_ranks_map,
        total_combinations=total,
        recommended_bet=float(_BASE_BET * total),
        notes=f"{total}点購入（各レース上位{top_n}頭・SランクS→A優先）",
    )


# ── Virtual Oracle Strategy ───────────────────────────────────────

class VirtualOracleStrategy:
    """
    的中確率最大化ストラテジー（実戦買い目には含めない）。

    EV（期待値）ではなく Harville 確率そのものを最大化する組み合わせを生成し、
    「もしモデルが正しければ最も当たりやすい買い目」を記録用に保存する。

    用途:
      - 三連複・三連単の的中実績データとして predictions/prediction_horses に保存
      - 実際の Kelly 推奨（BetGenerator 出力）からは完全に除外される
      - model_type="Oracle" として保存することで通常予想と区別する

    アルゴリズム:
      三連複: C(n, 3) 全組み合わせを Harville trio 確率（6順列の総和）で降順ソート
              → 上位 TOP_N_SANRENPUKU 点を推奨
      三連単: P(n, 3) 全順列を Harville trifecta 確率で降順ソート
              → 上位 TOP_N_SANRENTAN 点を推奨
    """

    TOP_N_SANRENPUKU = 3   # 三連複 最大推奨点数
    TOP_N_SANRENTAN  = 3   # 三連単 最大推奨点数

    def generate(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        """
        勝率スコアから Harville 確率最大の三連複・三連単を生成する。

        Args:
            race_id:       対象レース ID
            df:            特徴量 DataFrame
            honmei_scores: HonmeiModel.predict() の出力（勝率確率）

        Returns:
            RaceBets (model_type は呼び出し側が "Oracle" に設定すること)
        """
        result = RaceBets(race_id=race_id, model_type="本命")
        names = _name_map(df)

        scored = df.copy()
        scored["honmei_score"] = (
            honmei_scores.values if len(honmei_scores) == len(scored)
            else honmei_scores.reindex(scored.index).values
        )
        scored = scored.sort_values("honmei_score", ascending=False)

        # 馬番 < 1 を除外
        scored = scored[scored["horse_number"] >= 1]
        n = len(scored)
        if n < 3:
            return result

        all_nums   = [int(r["horse_number"])    for _, r in scored.iterrows()]
        all_scores = [float(r["honmei_score"])  for _, r in scored.iterrows()]

        # ── 三連複（的中確率最大化） ─────────────────────────────
        trio_probs: list[tuple[float, tuple[int, ...], list[str]]] = []
        for idx_combo in itertools.combinations(range(n), 3):
            ia, ib, ic = idx_combo
            # 全6順列の総和 = 三連複的中確率
            prob_sum = sum(
                _harville_trio(all_scores, *perm)
                for perm in itertools.permutations([ia, ib, ic])
            )
            na, nb, nc = all_nums[ia], all_nums[ib], all_nums[ic]
            combo3 = tuple(sorted([na, nb, nc]))
            trio_probs.append((
                prob_sum,
                combo3,
                [names.get(x, str(x)) for x in combo3],
            ))

        trio_probs.sort(key=lambda x: x[0], reverse=True)

        # 三連複を1レコードに集約（UNIQUE制約対応）
        seen_t: set[tuple] = set()
        oracle_puku_combos: list[tuple] = []
        oracle_puku_names:  list[str]   = []
        oracle_puku_best_p  = 0.0
        for prob3, combo3, hnames3 in trio_probs[:self.TOP_N_SANRENPUKU]:
            if combo3 in seen_t:
                continue
            seen_t.add(combo3)
            oracle_puku_combos.append(combo3)
            for nm in hnames3:
                if nm not in oracle_puku_names:
                    oracle_puku_names.append(nm)
            if prob3 > oracle_puku_best_p:
                oracle_puku_best_p = prob3

        if oracle_puku_combos:
            result.bets.append(BetRecommendation(
                bet_type="三連複",
                combinations=oracle_puku_combos,
                horse_names=oracle_puku_names,
                expected_value=oracle_puku_best_p * 30.0,
                model_score=oracle_puku_best_p,
                recommended_bet=_BASE_BET * len(oracle_puku_combos),
                confidence=oracle_puku_best_p,
                notes=(
                    f"[Oracle] 的中確率最大 {len(oracle_puku_combos)}点 "
                    f"P最大={oracle_puku_best_p:.4f} 馬番={oracle_puku_combos}"
                ),
            ))

        # ── 三連単（的中確率最大化・1レコード集約）─────────────────
        trifecta_probs: list[tuple[float, tuple[int, ...], list[str]]] = []
        for idx_perm in itertools.permutations(range(n), 3):
            ia, ib, ic = idx_perm
            p = _harville_trio(all_scores, ia, ib, ic)
            na, nb, nc = all_nums[ia], all_nums[ib], all_nums[ic]
            trifecta_probs.append((p, (na, nb, nc), []))

        trifecta_probs.sort(key=lambda x: x[0], reverse=True)

        top_tf_list = trifecta_probs[:self.TOP_N_SANRENTAN]
        if top_tf_list:
            oracle_tan_combos = [c for _, c, _ in top_tf_list]
            best_p_tan = top_tf_list[0][0]
            seen_nms_t: set[str] = set()
            oracle_tan_names: list[str] = []
            for _, (na, nb, nc), _ in top_tf_list:
                for num in (na, nb, nc):
                    nm = names.get(num, str(num))
                    if nm not in seen_nms_t:
                        oracle_tan_names.append(nm)
                        seen_nms_t.add(nm)
            result.bets.append(BetRecommendation(
                bet_type="三連単",
                combinations=oracle_tan_combos,
                horse_names=oracle_tan_names,
                expected_value=best_p_tan * 150.0,
                model_score=best_p_tan,
                recommended_bet=_BASE_BET * len(oracle_tan_combos),
                confidence=best_p_tan,
                notes=(
                    f"[Oracle] 的中確率最大 {len(oracle_tan_combos)}点 "
                    f"P最大={best_p_tan:.4f} 馬番={oracle_tan_combos}"
                ),
            ))

        logger.info(
            "Oracle買い目生成: race_id=%s 三連複%d点 三連単%d点",
            race_id,
            len(oracle_puku_combos),
            len(top_tf_list) if top_tf_list else 0,
        )
        return result


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
        self._config  = config or BetConfig()
        self._honmei  = HonmeiStrategy(estimator=estimator)
        self._manji   = ManjiStrategy(estimator=estimator)
        self._oracle  = VirtualOracleStrategy()

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

    def generate_oracle(
        self,
        race_id: str,
        df: pd.DataFrame,
        honmei_scores: pd.Series,
    ) -> RaceBets:
        """
        VirtualOracleStrategy で三連複・三連単の的中確率最大買い目を生成する。

        model_type は "Oracle" として返す（呼び出し側で insert_prediction に渡すこと）。
        recommended_bet は参照用の最小単位のみ（Kelly 対象外）。
        """
        bets = self._oracle.generate(race_id, df, honmei_scores)
        bets.model_type = "本命"   # 型互換のため本命を維持（保存時に "Oracle" を付加）
        return bets

    def generate_win5(
        self,
        races: dict[str, pd.DataFrame],
        scores: dict[str, pd.Series],
        top_n: int = 2,
    ) -> Win5Recommendation | None:
        return generate_win5(races, scores, top_n=top_n)
