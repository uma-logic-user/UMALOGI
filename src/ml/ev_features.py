"""
src/ml/ev_features.py — EV 特化特徴量エンジン

Shin (1993) 真確率推定・Harville 法複勝確率変換・オッズ異常検知・
EV 最大化拡張特徴量を提供する。

設計思想:
  - JRA 控除率はクラス定数で管理（ハードコード禁止・コンストラクタ上書き可）
  - 全計算は NumPy/Pandas のベクトル演算（Python ループは馬数分の内側計算のみ）
  - 型ヒント・docstring 必須（PEP-484 準拠）
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# JRA 控除率定数
# ─────────────────────────────────────────────────────────────────────────────


class JRATakeoutRates:
    """JRA 法定控除率（馬券種別ごとの払戻率の補数）。

    控除率 = 1 - 払戻率。払戻率は JRA レースのルールに基づく公式値（2024年時点）。
    コンストラクタ引数で各率を上書き可能（シミュレーション・競艇等への転用を想定）。
    """

    # デフォルト値（2024年時点 JRA 公式控除率）
    WIN: float = 0.200  # 単勝  80% 払戻
    PLACE: float = 0.200  # 複勝  80% 払戻
    QUINELLA: float = 0.225  # 馬連  77.5% 払戻
    WIDE: float = 0.225  # ワイド 77.5% 払戻
    EXACTA: float = 0.250  # 馬単  75% 払戻
    TRIO: float = 0.225  # 三連複 77.5% 払戻
    TRIFECTA: float = 0.275  # 三連単 72.5% 払戻

    def __init__(
        self,
        win: float | None = None,
        place: float | None = None,
        quinella: float | None = None,
        wide: float | None = None,
        exacta: float | None = None,
        trio: float | None = None,
        trifecta: float | None = None,
    ) -> None:
        if win is not None:
            self.WIN = win
        if place is not None:
            self.PLACE = place
        if quinella is not None:
            self.QUINELLA = quinella
        if wide is not None:
            self.WIDE = wide
        if exacta is not None:
            self.EXACTA = exacta
        if trio is not None:
            self.TRIO = trio
        if trifecta is not None:
            self.TRIFECTA = trifecta


# ─────────────────────────────────────────────────────────────────────────────
# Shin (1993) 真確率推定 + Harville 法
# ─────────────────────────────────────────────────────────────────────────────


class MarketProbabilityCalc:
    """Shin (1993) 真確率推定 + Harville 法 複勝確率変換。

    Shin (1993): "Prices of State Contingent Claims with Insider Traders,
    and the Favourite-Longshot Bias"

    実装方針:
      - Shin z（内部情報比率）は線形モデルの解析解 z = (K-1)/(N-1) を使用する。
        K = Σ(1/o_i)（オーバーラウンド係数）、N = 馬数。
      - 非線形二次 Shin 補正で FLB（大穴馬過小評価バイアス）を修正する。
      - 最後に正規化（Σp_i = 1 を保証）を行う。

    Args:
        takeout_rates: JRATakeoutRates インスタンス（デフォルト: JRA 公式控除率）
    """

    def __init__(
        self,
        takeout_rates: JRATakeoutRates | None = None,
    ) -> None:
        self._rates = takeout_rates or JRATakeoutRates()

    def compute_shin_probabilities(
        self,
        win_odds: np.ndarray,
    ) -> np.ndarray:
        """Shin (1993) 真確率ベクトルを返す。

        市場オッズに含まれる「最有力馬への過剰投票」バイアス (favorite-longshot bias)
        と控除率（オーバーラウンド）を同時補正し、真の勝利確率の推定値を返す。

        アルゴリズム:
          1. オーバーラウンド K = Σ(1/o_i) を計算する。
          2. 線形 Shin z = (K-1)/(N-1) を解析解として求める。
          3. 非線形二次補正: p_i_raw = (√(z² + 4(1-z)w_i) - z) / (2(1-z))
             w_i = (1/o_i)/K（正規化市場確率）
          4. p_raw を正規化して Σp_i = 1 を保証する。

        Args:
            win_odds: 単勝オッズ配列 (shape: [N,], dtype: float)。
                      0 または負の値は自動的に最大オッズで置換される。

        Returns:
            shape [N,] の Shin 真確率配列。Σ = 1.0 に正規化済み。
        """
        odds = np.asarray(win_odds, dtype=float)
        n = len(odds)

        # 無効オッズ（0, 負, NaN）を最大有効オッズで置換
        mask_invalid = (odds <= 0) | np.isnan(odds)
        if mask_invalid.any():
            valid_max = (
                float(odds[~mask_invalid].max()) if (~mask_invalid).any() else 100.0
            )
            odds = np.where(mask_invalid, valid_max, odds)

        raw_probs = 1.0 / odds
        k_sum = float(raw_probs.sum())  # オーバーラウンド係数 K
        w = raw_probs / k_sum  # 正規化市場確率 Σw = 1

        # 線形 Shin z: (K-1)/(N-1)
        # K=1（オーバーラウンドなし）→ z=0 → 補正不要
        z = max(0.0, min((k_sum - 1.0) / max(float(n) - 1.0, 1e-8), 0.5))

        if z < 1e-10 or n < 2:
            return w.copy()

        # 非線形 Shin 補正
        discriminant = np.maximum(z * z + 4.0 * (1.0 - z) * w, 0.0)
        p_raw = (np.sqrt(discriminant) - z) / (2.0 * (1.0 - z))
        p_raw = np.maximum(p_raw, 0.0)

        total = float(p_raw.sum())
        if total < 1e-10:
            return w.copy()

        return p_raw / total  # 正規化: Σ = 1

    def harville_place_prob(
        self,
        win_probs: np.ndarray,
        target_idx: int,
    ) -> float:
        """Harville 法で特定馬の複勝確率（3着以内）を計算する。

        Harville (1973): "Assigning Probabilities to the Outcomes of
        Multi-entry Competitions"

        公式:
          P(i is 1st) = p_i
          P(i is 2nd) = Σ_{j≠i} p_j × p_i / (1 - p_j)
          P(i is 3rd) = Σ_{j≠i} Σ_{k≠i,k≠j} p_j × [p_k/(1-p_j)] × [p_i/(1-p_j-p_k)]

        Args:
            win_probs: 全馬の勝利確率配列 (shape: [N,])
            target_idx: 複勝確率を求める馬のインデックス

        Returns:
            複勝確率（= 1着確率 + 2着確率 + 3着確率の和）
        """
        p = np.asarray(win_probs, dtype=float)
        total = float(p.sum())
        if total > 1e-10:
            p = p / total  # 正規化
        n = len(p)
        pi = float(p[target_idx])

        if n < 2:
            return pi

        # 1着確率
        prob_first = pi

        # 2着確率（ベクトル化）
        all_idx = np.arange(n)
        others_idx = all_idx[all_idx != target_idx]
        pj = p[others_idx]
        denom_2nd = np.where(1.0 - pj < 1e-10, 1e-10, 1.0 - pj)
        prob_second = float((pj * (pi / denom_2nd)).sum())

        if n < 3:
            return prob_first + prob_second

        # 3着確率（外ループ: j の数 ≤ N-1, 内ループ: numpy ベクトル）
        prob_third = 0.0
        for j in others_idx:
            pj_val = float(p[j])
            if pj_val < 1e-10:
                continue
            denom_k = 1.0 - pj_val
            if denom_k < 1e-10:
                continue
            non_ij = others_idx[others_idx != j]
            pk_arr = p[non_ij]
            denom_i = np.where(
                1.0 - pj_val - pk_arr < 1e-10, 1e-10, 1.0 - pj_val - pk_arr
            )
            prob_third += float((pj_val * (pk_arr / denom_k) * (pi / denom_i)).sum())

        return prob_first + prob_second + prob_third

    def harville_trifecta_prob(
        self,
        win_probs: np.ndarray,
        horse_a: int,
        horse_b: int,
        horse_c: int,
    ) -> float:
        """Harville 法で三連単 (A→B→C) の確率を計算する。

        公式:
          P(A 1st, B 2nd, C 3rd) = P(A) × P(B)/(1-P(A)) × P(C)/(1-P(A)-P(B))

        Args:
            win_probs: 全馬の勝利確率配列
            horse_a: 1着馬インデックス
            horse_b: 2着馬インデックス
            horse_c: 3着馬インデックス

        Returns:
            三連単確率
        """
        p = np.asarray(win_probs, dtype=float)
        total = float(p.sum())
        if total > 1e-10:
            p = p / total
        pa, pb, pc = float(p[horse_a]), float(p[horse_b]), float(p[horse_c])
        d_b = max(1.0 - pa, 1e-10)
        d_c = max(1.0 - pa - pb, 1e-10)
        return pa * (pb / d_b) * (pc / d_c)


# ─────────────────────────────────────────────────────────────────────────────
# オッズ異常検知
# ─────────────────────────────────────────────────────────────────────────────


class OddsAnomalyDetector:
    """スチームムーブ（大口投票シグナル）とオッズ逆行を検知する。

    Args:
        steam_threshold: スチーム移動と判定するオッズ下落率（デフォルト 15%）
        reversal_threshold: オッズ逆行スコアの正規化基準（現在未使用・拡張用）
    """

    def __init__(
        self,
        steam_threshold: float = 0.15,
        reversal_threshold: float = 0.30,
    ) -> None:
        self._steam_threshold = steam_threshold
        self._reversal_threshold = reversal_threshold

    def detect_steam_move(
        self,
        morning_odds: pd.Series,
        final_odds: pd.Series,
    ) -> pd.Series:
        """スチームムーブ（オッズ急落）フラグを返す。

        朝一オッズ → 最終オッズの下落率が steam_threshold を超える馬を 1 としてマーク。
        「スマートマネー（情報優位な大口）が入った」シグナルとして解釈する。

        Args:
            morning_odds: 朝一単勝オッズ Series（index: 馬番）
            final_odds:   最終単勝オッズ Series（index: 馬番）

        Returns:
            int 型 Series (1=スチームムーブあり, 0=なし)
        """
        with np.errstate(divide="ignore", invalid="ignore"):
            change_ratio = (morning_odds - final_odds) / morning_odds.replace(0, np.nan)
        steam = (change_ratio.fillna(0.0) >= self._steam_threshold).astype(int)
        return steam

    def detect_odds_reversal(
        self,
        win_odds: pd.Series,
        popularity: pd.Series,
    ) -> pd.Series:
        """オッズと人気順位の逆行スコアを返す（0-1 float）。

        「オッズが高いのに人気順位が低い（大口が特定馬を嫌う）」または
        「オッズが低いのに人気順位が高い（大口が特定馬に集中）」を検出する。

        実装: log(オッズ) 偏差と 人気順位偏差の差分絶対値を最大値で正規化。

        Args:
            win_odds:   単勝オッズ Series
            popularity: 人気順位 Series（1=1番人気）

        Returns:
            逆行スコア Series（高いほど異常。0-1 float）
        """
        log_odds = np.log(win_odds.clip(lower=1.01))
        std_odds = log_odds.std()
        z_odds = (log_odds - log_odds.mean()) / (std_odds + 1e-8)

        std_pop = popularity.std()
        z_pop = (popularity - popularity.mean()) / (std_pop + 1e-8)

        reversal = (z_odds - z_pop).abs()
        max_rev = float(reversal.max())
        if max_rev < 1e-8:
            return pd.Series(np.zeros(len(win_odds)), index=win_odds.index, dtype=float)
        return (reversal / max_rev).clip(0.0, 1.0)

    def compute_late_money_ratio(
        self,
        early_volume: pd.Series,
        late_volume: pd.Series,
    ) -> pd.Series:
        """直前（締め切り30分前〜）の投票量比率を返す。

        「直前に集中した大口」を示す比率。計算: late / (early + late + ε)。

        Args:
            early_volume: 早期投票量 Series（例: 朝〜30分前）
            late_volume:  直前投票量 Series

        Returns:
            直前投票比率 Series（0-1）
        """
        total = early_volume + late_volume + 1e-8
        return (late_volume / total).clip(0.0, 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# EV 強化特徴量エンジン
# ─────────────────────────────────────────────────────────────────────────────


class EVEnhancedFeatures:
    """EV 最大化のための拡張特徴量を DataFrame に追加するエンジン。

    以下の特徴量を race_df（1レース = 複数行の馬 DataFrame）に追加する:

    - ``shin_prob``:           Shin (1993) 真確率
    - ``implied_prob_excess``: Shin 確率 - 市場暗示確率（正 = モデルが市場より高評価）
    - ``harville_place_prob``: Harville 法複勝確率
    - ``odds_reversal_score``: オッズ逆行スコア（0-1）
    - ``odds_steam_flag``:     スチームムーブフラグ (0/1)
    - ``field_strength_ev_adj``: 競走馬層強度 EV 調整係数（高エントロピー = 保守化）
    - ``ev_rank_in_race``:     レース内 EV ランク（1 = 最高 EV）

    Args:
        prob_calc:        MarketProbabilityCalc インスタンス
        anomaly_detector: OddsAnomalyDetector インスタンス
    """

    def __init__(
        self,
        prob_calc: MarketProbabilityCalc | None = None,
        anomaly_detector: OddsAnomalyDetector | None = None,
    ) -> None:
        self._prob_calc = prob_calc or MarketProbabilityCalc()
        self._anomaly_detector = anomaly_detector or OddsAnomalyDetector()

    def add_ev_features(
        self,
        race_df: pd.DataFrame,
        odds_col: str = "win_odds",
        popularity_col: str = "popularity",
        morning_odds_col: str | None = "odds_vs_morning",
    ) -> pd.DataFrame:
        """EV 特化特徴量を race_df に追加して返す。

        全計算は pandas/numpy のベクトル演算。元の DataFrame は変更しない。

        Args:
            race_df:          1レースの出走馬 DataFrame（各行が1頭）
            odds_col:         単勝オッズ列名
            popularity_col:   人気順位列名
            morning_odds_col: 朝一オッズ列名（None の場合スチーム検出をスキップ）

        Returns:
            EV 特化特徴量列が追加された DataFrame（コピー）
        """
        df = race_df.copy()
        n = len(df)
        if n == 0:
            return df

        # ── 市場暗示確率 ──────────────────────────────────────────
        raw_odds = pd.to_numeric(df[odds_col], errors="coerce").fillna(99.9)
        raw_odds = raw_odds.clip(lower=1.01)
        market_implied = 1.0 / raw_odds
        market_prob = market_implied / float(market_implied.sum())

        # ── Shin (1993) 真確率 ────────────────────────────────────
        shin_probs = self._prob_calc.compute_shin_probabilities(raw_odds.values)
        df["shin_prob"] = shin_probs

        # ── 市場暗示確率超過分（Shin 補正後の優位性）──────────────
        df["implied_prob_excess"] = shin_probs - market_prob.values

        # ── Harville 法複勝確率 ───────────────────────────────────
        harville_place = np.array(
            [self._prob_calc.harville_place_prob(shin_probs, i) for i in range(n)]
        )
        df["harville_place_prob"] = harville_place

        # ── オッズ逆行スコア ──────────────────────────────────────
        if popularity_col in df.columns:
            pop = pd.to_numeric(df[popularity_col], errors="coerce").fillna(float(n))
            df["odds_reversal_score"] = self._anomaly_detector.detect_odds_reversal(
                raw_odds, pop
            ).values
        else:
            df["odds_reversal_score"] = 0.0

        # ── スチームムーブフラグ ──────────────────────────────────
        if morning_odds_col is not None and morning_odds_col in df.columns:
            morning = pd.to_numeric(df[morning_odds_col], errors="coerce")
            # odds_vs_morning が「直前/朝一」比率として保存されている場合に対応
            # 中央値 < 5.0 → 比率として扱い、1 - ratio = 下落率
            if float(morning.median()) < 5.0:
                steam_ratio = (1.0 - morning.clip(lower=0.01)).fillna(0.0)
                df["odds_steam_flag"] = (
                    steam_ratio >= self._anomaly_detector._steam_threshold
                ).astype(int)
            else:
                df["odds_steam_flag"] = self._anomaly_detector.detect_steam_move(
                    morning, raw_odds
                ).values
        else:
            df["odds_steam_flag"] = 0

        # ── フィールド強度 EV 調整係数 ────────────────────────────
        # Shin 確率エントロピー: 高いほど実力伯仲 → EV 計算を保守化
        entropy = float(-np.sum(shin_probs * np.log(shin_probs + 1e-10)))
        max_entropy = math.log(n) if n > 1 else 1.0
        field_strength = entropy / max(max_entropy, 1e-8)
        df["field_strength_ev_adj"] = 1.0 - 0.5 * field_strength

        # ── レース内 EV ランク（Shin 確率 × オッズ の近似 EV）────
        ev_approx = shin_probs * raw_odds.values
        ev_rank = pd.Series(ev_approx).rank(ascending=False, method="min")
        df["ev_rank_in_race"] = ev_rank.values

        return df
