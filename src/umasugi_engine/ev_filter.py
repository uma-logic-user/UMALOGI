"""
世論分析 EV 減算フィルター — AIウマスギ コア

既存の legacy_logic が算出した EV に対し、世論圧力ペナルティを「負の相関」として適用する。

設計思想:
  legacy_logic: crowd_bias_ratio > 1.3 → EV × 最大 1.5 倍（正の相関）
  umasugi_engine: opinion_pressure 高 → EV × (1 - penalty)（負の相関）

  これにより「影響者が推奨した穴馬はオッズが既に下がっており期待値がない」
  という大衆心理のジレンマを自動検知・補正する。
"""

from __future__ import annotations

import logging

import pandas as pd

from .factors.crowd_opinion import calc_crowd_opinion_score

logger = logging.getLogger(__name__)


def apply_ev_filter(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame 内の EV 列に世論分析ペナルティを適用する。

    Args:
        df: 必須列として ev (legacy EV スコア)、x_consensus_score、
            odds_steam_flag、crowd_bias_ratio を含む DataFrame。

    Returns:
        以下の列を追加した DataFrame。
        opinion_pressure_score: 世論圧力スコア。
        crowd_ev_penalty: EV 減算ペナルティ係数。
        umasugi_ev: ペナルティ適用後の EV（0 以上にクリップ済み）。
        legacy_ev: 適用前の EV（保存用）。
        ev_delta: umasugi_ev - legacy_ev の差分。
    """
    df = df.copy()

    # ── 世論ペナルティ計算 ───────────────────────────────────────────────
    df = calc_crowd_opinion_score(df)

    # ── EV 列を特定（alpha_model / bet_generator の命名に対応） ──────────
    ev_col = _detect_ev_col(df)
    if ev_col is None:
        logger.warning("EV 列が見つかりません。umasugi_ev = ev = 0.0 で初期化します。")
        df["legacy_ev"] = 0.0
        df["umasugi_ev"] = 0.0
        return df

    df["legacy_ev"] = df[ev_col].astype(float)

    # EV × (1 - crowd_ev_penalty)
    df["umasugi_ev"] = df["legacy_ev"] * (1.0 - df["crowd_ev_penalty"])

    # EV は 0 を下限とする（マイナス EV の増幅は行わない）
    df["umasugi_ev"] = df["umasugi_ev"].clip(lower=0.0)
    df["ev_delta"] = df["umasugi_ev"] - df["legacy_ev"]

    logger.debug(
        "EV フィルター適用: %d 頭 / 平均ペナルティ %.3f / 平均ΔEV %.3f",
        len(df),
        df["crowd_ev_penalty"].mean(),
        (df["legacy_ev"] - df["umasugi_ev"]).mean(),
    )
    return df


def _detect_ev_col(df: pd.DataFrame) -> str | None:
    """DataFrame から EV スコア列名を優先順で検索する。

    Args:
        df: 検索対象の DataFrame。

    Returns:
        最初に見つかった EV 列名。見つからない場合は None。
    """
    candidates = ["ev_score", "ev", "alpha_ev", "umasugi_ev_base"]
    for col in candidates:
        if col in df.columns:
            return col
    return None
