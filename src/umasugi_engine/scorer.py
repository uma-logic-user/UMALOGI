"""
30因子スコアリング — AIウマスギ統合スコアラー

legacy_logic の UScoreEngine (19因子) をラッパーとして利用し、
拡張因子（小回り・野芝/洋芝・調教グレード・オッズモメンタム・世論分析）を
加算して umasugi_score を算出する。

ウェイト配分（2026-05-24 Phase2 最適化後）:
  legacy u_score (19因子)  : 57%
  track_style_score        : 10%（小回り適性）
  turf_type_score          : 15%（野芝/洋芝適性）
  training_grade_score     :  8%（調教グレード S〜E）
  odds_momentum_score      :  5%（オッズ買い圧力）
  crowd_opinion 調整       :  5%（世論分析 — ev_filter.py で EV に直接適用）

出力列: umasugi_score (0.0〜1.0)
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

# ── ウェイト ────────────────────────────────────────────────────────────────
# 2026-05-24 Phase1 バックテスト:
#   turf_type_score < 0.3 → 的中率 0.0%（116件）の強力な除外シグナル。
#   turf ウェイトを 0.10 → 0.15 に引き上げ (Phase1)。
# 2026-05-24 Phase2 データ拡張:
#   調教グレード (training_grade_score) を 8% 追加。
#   オッズモメンタム (odds_momentum_score) を 5% 追加。
#   legacy を 0.65 → 0.57 に削減してウェイト合計を 1.00 に維持。
_W_LEGACY          = 0.57
_W_TRACK           = 0.10  # 小回り適性
_W_TURF            = 0.15  # 野芝/洋芝適性（強力な除外シグナル）
_W_TRAINING_GRADE  = 0.08  # 調教グレード (S〜E)
_W_ODDS_MOMENTUM   = 0.05  # オッズ買い圧力
_W_CROWD           = 0.05  # 世論分析（EV 直接適用のため scorer では中立固定）


def calc_umasugi_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    umasugi_score を DataFrame に追加して返す。

    Parameters
    ----------
    df : DataFrame
        UScoreEngine.calc() 適用済みの DataFrame。
        必須列: u_score, race_id, horse_id
    conn : sqlite3.Connection
        umalogi.db 接続。

    Returns
    -------
    df : DataFrame  (元 df + 各拡張スコア列 + umasugi_score 列)
    """
    from .factors.odds_momentum import calc_odds_momentum_score
    from .factors.track_style import calc_track_style_score
    from .factors.training_grade import calc_training_grade_score
    from .factors.turf_type import calc_turf_type_score

    if df.empty:
        for col in (
            "track_style_score",
            "turf_type_score",
            "training_grade_score",
            "odds_momentum_score",
            "umasugi_score",
        ):
            df[col] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # ── 拡張因子を追加 ──────────────────────────────────────────────────────
    df = calc_track_style_score(df, conn)
    df = calc_turf_type_score(df, conn)
    df = calc_training_grade_score(df, conn)
    df = calc_odds_momentum_score(df, conn)

    # ── 統合スコア ──────────────────────────────────────────────────────────
    u_score        = df.get("u_score",               pd.Series(0.5, index=df.index)).fillna(0.5)
    track_score    = df.get("track_style_score",      pd.Series(0.5, index=df.index)).fillna(0.5)
    turf_score     = df.get("turf_type_score",        pd.Series(0.5, index=df.index)).fillna(0.5)
    grade_score    = df.get("training_grade_score",   pd.Series(0.5, index=df.index)).fillna(0.5)
    momentum_score = df.get("odds_momentum_score",    pd.Series(0.5, index=df.index)).fillna(0.5)

    df["umasugi_score"] = (
        _W_LEGACY         * u_score
        + _W_TRACK        * track_score
        + _W_TURF         * turf_score
        + _W_TRAINING_GRADE * grade_score
        + _W_ODDS_MOMENTUM  * momentum_score
        + _W_CROWD          * 0.5  # crowd は ev_filter で直接 EV 調整するため中立値
    ).clip(0.0, 1.0)

    return df
