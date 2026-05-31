"""
30因子スコアリング — AIウマスギ統合スコアラー (Phase3)

legacy_logic の UScoreEngine (19因子) をラッパーとして利用し、
拡張因子を加算して umasugi_score を算出する。

ウェイト配分（2026-05-24 Phase3 フル統合後）:
  legacy u_score (19因子)       : 50%
  track_style_score             : 10%（小回り適性）
  turf_type_score               : 15%（野芝/洋芝適性）
  training_grade_score          :  7%（調教グレード S〜E）
  odds_momentum_score           :  5%（オッズ買い圧力 + スパイク検知）
  crowd_opinion 調整            :  5%（世論分析 — ev_filter.py で EV に直接適用）
  paddock_score                 :  3%（Discord パドック気配メモ）
  jockey_course_score           :  3%（騎手コース別成績）
  trainer_course_score          :  2%（調教師コース別成績）
  合計                          : 100%

出力列: umasugi_score (0.0〜1.0)
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

# ── ウェイト ────────────────────────────────────────────────────────────────
# Phase3 フル統合 (2026-05-24):
#   paddock (3%) / jockey_course (3%) / trainer_course (2%) を追加。
#   legacy を 0.57 → 0.50 に削減、training_grade を 0.08 → 0.07 に微調整。
_W_LEGACY = 0.50
_W_TRACK = 0.10  # 小回り適性
_W_TURF = 0.15  # 野芝/洋芝適性（強力な除外シグナル）
_W_TRAINING_GRADE = 0.07  # 調教グレード (S〜E)
_W_ODDS_MOMENTUM = 0.05  # オッズ買い圧力 + スパイク検知
_W_CROWD = 0.05  # 世論分析（EV 直接適用のため scorer では中立固定）
_W_PADDOCK = 0.03  # パドック気配メモ
_W_JOCKEY_COURSE = 0.03  # 騎手コース別成績
_W_TRAINER_COURSE = 0.02  # 調教師コース別成績

assert (
    abs(
        _W_LEGACY
        + _W_TRACK
        + _W_TURF
        + _W_TRAINING_GRADE
        + _W_ODDS_MOMENTUM
        + _W_CROWD
        + _W_PADDOCK
        + _W_JOCKEY_COURSE
        + _W_TRAINER_COURSE
        - 1.0
    )
    < 1e-9
), "ウェイト合計が 1.0 ではありません"


def calc_umasugi_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """umasugi_score を DataFrame に追加して返す。

    各拡張因子関数を順番に呼び出し、ウェイト付き加重和で umasugi_score を算出する。
    空の DataFrame が渡された場合はスコア列を float 型で初期化して即返す。

    Args:
        df: UScoreEngine.calc() 適用済みの DataFrame。
            必須列: u_score, race_id, horse_id。
        conn: umalogi.db 接続。

    Returns:
        元 df に各拡張スコア列（track_style_score, turf_type_score,
        training_grade_score, odds_momentum_score, paddock_score,
        jockey_course_score, trainer_course_score）と umasugi_score 列を
        追加した DataFrame。
    """
    from .factors.jockey_trainer import (
        calc_jockey_course_score,
        calc_trainer_course_score,
    )
    from .factors.odds_momentum import calc_odds_momentum_score
    from .factors.paddock import calc_paddock_score
    from .factors.track_style import calc_track_style_score
    from .factors.training_grade import calc_training_grade_score
    from .factors.turf_type import calc_turf_type_score

    _all_score_cols = (
        "track_style_score",
        "turf_type_score",
        "training_grade_score",
        "odds_momentum_score",
        "paddock_score",
        "jockey_course_score",
        "trainer_course_score",
        "umasugi_score",
    )
    if df.empty:
        for col in _all_score_cols:
            df[col] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # ── 拡張因子を追加 ──────────────────────────────────────────────────────
    df = calc_track_style_score(df, conn)
    df = calc_turf_type_score(df, conn)
    df = calc_training_grade_score(df, conn)
    df = calc_odds_momentum_score(df, conn)
    df = calc_paddock_score(df, conn)
    df = calc_jockey_course_score(df, conn)
    df = calc_trainer_course_score(df, conn)

    # ── 統合スコア ──────────────────────────────────────────────────────────
    u_score = df.get("u_score", pd.Series(0.5, index=df.index)).fillna(0.5)
    track_score = df.get("track_style_score", pd.Series(0.5, index=df.index)).fillna(
        0.5
    )
    turf_score = df.get("turf_type_score", pd.Series(0.5, index=df.index)).fillna(0.5)
    grade_score = df.get("training_grade_score", pd.Series(0.5, index=df.index)).fillna(
        0.5
    )
    momentum_score = df.get(
        "odds_momentum_score", pd.Series(0.5, index=df.index)
    ).fillna(0.5)
    paddock_score = df.get("paddock_score", pd.Series(0.5, index=df.index)).fillna(0.5)
    jockey_score = df.get("jockey_course_score", pd.Series(0.5, index=df.index)).fillna(
        0.5
    )
    trainer_score = df.get(
        "trainer_course_score", pd.Series(0.5, index=df.index)
    ).fillna(0.5)

    df["umasugi_score"] = (
        _W_LEGACY * u_score
        + _W_TRACK * track_score
        + _W_TURF * turf_score
        + _W_TRAINING_GRADE * grade_score
        + _W_ODDS_MOMENTUM * momentum_score
        + _W_CROWD * 0.5  # crowd は ev_filter で直接 EV 調整
        + _W_PADDOCK * paddock_score
        + _W_JOCKEY_COURSE * jockey_score
        + _W_TRAINER_COURSE * trainer_score
    ).clip(0.0, 1.0)

    return df
