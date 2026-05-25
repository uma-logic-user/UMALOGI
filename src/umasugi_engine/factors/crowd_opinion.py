"""
世論分析フィルター — AIウマスギ拡張因子（Phase 2）

「SNS影響者による人気操作」を検知し、世論圧力が高い馬の EV を減算する。
既存の crowd_bias_ratio（u_score 5%）を強化し、独立した負の相関ファクターとして実装する。

世論圧力スコア計算式:
  opinion_pressure = x_consensus_score × odds_compression_ratio
  crowd_penalty = sigmoid(opinion_pressure - OPINION_THRESHOLD) × MAX_PENALTY
  ev_final = ev_base × (1.0 - crowd_penalty)

出力列:
  opinion_pressure_score : 世論圧力スコア (0.0〜1.0)
  crowd_ev_penalty       : EV 減算ペナルティ係数 (0.0〜MAX_PENALTY)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

OPINION_THRESHOLD = 0.4  # この閾値を超えると EV ペナルティ発動
MAX_PENALTY = 0.35  # 最大 EV 減算率 35%


def calc_crowd_opinion_score(df: pd.DataFrame) -> pd.DataFrame:
    """世論圧力スコアと EV ペナルティを DataFrame に追加する。

    Args:
        df: 以下の列を含む DataFrame。
            x_consensus_score: X (Twitter) 世論コンセンサス (0〜1)。
            odds_steam_flag: スチームムーブ検出フラグ (0/1)。
            crowd_bias_ratio: 既存の大衆心理乖離比率 (legacy u_score 由来)。
            いずれの列も欠損している場合はデフォルト値で補完される。

    Returns:
        元 df に opinion_pressure_score 列（世論圧力スコア 0.0〜1.0）と
        crowd_ev_penalty 列（EV 減算ペナルティ係数 0.0〜MAX_PENALTY）を
        追加した DataFrame。
    """
    df = df.copy()

    x_consensus = df.get("x_consensus_score", pd.Series(0.0, index=df.index)).fillna(
        0.0
    )
    steam_flag = df.get("odds_steam_flag", pd.Series(0.0, index=df.index)).fillna(0.0)
    bias_ratio = df.get("crowd_bias_ratio", pd.Series(1.0, index=df.index)).fillna(1.0)

    # オッズ圧縮度 = steam_flag（opening オッズ未実装のため代替）
    # steam_flag=1 なら 0.6、0 なら 0.1（確率変換）
    odds_compression = steam_flag * 0.6 + (1 - steam_flag) * 0.1

    # 既存の crowd_bias が低い（市場過大評価）ほど opinion_pressure を増幅
    # crowd_bias_ratio < 1.0: 市場が過大評価 → x_consensus_score の重みを増やす
    bias_factor = np.clip(2.0 - bias_ratio, 0.5, 2.0)

    # 世論圧力スコア (0〜1)
    raw_pressure = x_consensus * odds_compression * bias_factor
    opinion_pressure = np.clip(raw_pressure, 0.0, 1.0)

    # EV ペナルティ: sigmoid で滑らかに (THRESHOLD 付近で急激に立ち上がる)
    sigmoid_val = 1.0 / (1.0 + np.exp(-10.0 * (opinion_pressure - OPINION_THRESHOLD)))
    crowd_ev_penalty = sigmoid_val * MAX_PENALTY

    df["opinion_pressure_score"] = opinion_pressure.astype(float)
    df["crowd_ev_penalty"] = crowd_ev_penalty.astype(float)
    return df
