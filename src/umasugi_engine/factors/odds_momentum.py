"""
オッズ変動スコア — AIウマスギ拡張因子

odds_timeseries の直近 N スナップショットからオッズのモメンタム（傾き）を算出する。
オッズ下落 = 買い圧力 = ポジティブシグナル → 高スコア
オッズ上昇 = 売り圧力 = ネガティブシグナル → 低スコア

出力列: odds_momentum_score (0.0〜1.0, 0.5 = 中立)
"""

from __future__ import annotations

import logging
import sqlite3

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_WINDOW = 5  # 直近スナップショット数
_DEFAULT = 0.5


def calc_odds_momentum_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """
    オッズ変動スコアを DataFrame に追加して返す。

    Parameters
    ----------
    df : DataFrame  (必須列: race_id, horse_number)
    conn : sqlite3.Connection

    Returns
    -------
    df + odds_momentum_score 列 (0.0〜1.0)
    """
    if df.empty:
        df["odds_momentum_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    race_ids = df["race_id"].unique().tolist()
    ph = ",".join("?" * len(race_ids))
    rows = conn.execute(
        f"""
        SELECT race_id, horse_number, win_odds, recorded_at
        FROM odds_timeseries
        WHERE race_id IN ({ph})
          AND win_odds IS NOT NULL AND win_odds > 0
        ORDER BY race_id, horse_number, recorded_at DESC
        """,
        race_ids,
    ).fetchall()

    if not rows:
        df["odds_momentum_score"] = _DEFAULT
        return df

    ts_df = pd.DataFrame(
        rows, columns=["race_id", "horse_number", "win_odds", "recorded_at"]
    )

    # (race_id, horse_number) → 直近 _WINDOW 件のモメンタムスコア
    score_map: dict[tuple[str, int], float] = {}
    for (rid, hn), grp in ts_df.groupby(["race_id", "horse_number"]):
        # head() は DESC 順なので逆転して時系列昇順に直す
        recent = grp.head(_WINDOW)["win_odds"].tolist()[::-1]
        if len(recent) < 2:
            score_map[(str(rid), int(hn))] = _DEFAULT
            continue
        # 線形回帰の傾き（正=上昇, 負=下落）
        x = np.arange(len(recent), dtype=float)
        slope = float(np.polyfit(x, recent, 1)[0])
        # 傾きを [-5, 5] にクリップして [0, 1] に反転変換
        # 下落（slope 負） → 高スコア
        normalized = np.clip(-slope / 5.0, -1.0, 1.0)
        score = (normalized + 1.0) / 2.0  # [-1, 1] → [0, 1]
        score_map[(str(rid), int(hn))] = round(float(score), 4)

    df["odds_momentum_score"] = df.apply(
        lambda r: score_map.get((str(r["race_id"]), int(r["horse_number"])), _DEFAULT),
        axis=1,
    )
    return df
