"""
オッズ変動スコア — AIウマスギ拡張因子 (Phase3 スパイク検知追加)

odds_timeseries の直近 N スナップショットからオッズのモメンタム（傾き）と
「直前10分以内の急落スパイク」を合算して算出する。

モメンタム: 線形回帰傾き → 下落ほど高スコア
スパイク:   直近10スナップショット中 15%以上急落を検知 → ボーナス加算

出力列: odds_momentum_score (0.0〜1.0, 0.5 = 中立)
"""
from __future__ import annotations

import logging
import sqlite3

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_WINDOW          = 10   # 直近スナップショット数 (スパイク含め10点に拡張)
_MOMENTUM_WINDOW = 5    # 線形回帰に使う点数
_DEFAULT         = 0.5

# スパイク閾値
_SPIKE_MILD_THRESH  = 0.15   # 15%以上急落 → mild spike bonus
_SPIKE_STRONG_THRESH = 0.25  # 25%以上急落 → strong spike bonus
_SPIKE_MILD_BONUS   = 0.10
_SPIKE_STRONG_BONUS = 0.20


def _detect_spike(odds_asc: list[float]) -> float:
    """
    時系列昇順のオッズリストからスパイクボーナスを返す。

    最高値から現在値への下落率を計算:
      drop_rate = (peak - current) / peak
      15% 以上  → +0.10
      25% 以上  → +0.20
    """
    if len(odds_asc) < 2:
        return 0.0
    peak    = max(odds_asc)
    current = odds_asc[-1]
    if peak <= 0:
        return 0.0
    drop_rate = (peak - current) / peak
    if drop_rate >= _SPIKE_STRONG_THRESH:
        return _SPIKE_STRONG_BONUS
    if drop_rate >= _SPIKE_MILD_THRESH:
        return _SPIKE_MILD_BONUS
    return 0.0


def calc_odds_momentum_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """
    オッズ変動スコア（モメンタム + スパイク）を DataFrame に追加して返す。

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

    score_map: dict[tuple[str, int], float] = {}
    for (rid, hn), grp in ts_df.groupby(["race_id", "horse_number"]):
        # DESC → ascending 変換（window 上限 _WINDOW 点）
        odds_asc = grp.head(_WINDOW)["win_odds"].tolist()[::-1]

        # ── モメンタムスコア（線形回帰） ────────────────────────────────
        recent_mom = odds_asc[-_MOMENTUM_WINDOW:]  # 直近 5 点
        if len(recent_mom) >= 2:
            x = np.arange(len(recent_mom), dtype=float)
            slope = float(np.polyfit(x, recent_mom, 1)[0])
            # 傾き [-5, 5] → [-1, 1] → [0, 1]
            norm = np.clip(-slope / 5.0, -1.0, 1.0)
            momentum_score = (norm + 1.0) / 2.0
        else:
            momentum_score = _DEFAULT

        # ── スパイクボーナス ────────────────────────────────────────────
        spike_bonus = _detect_spike(odds_asc)

        final_score = round(min(1.0, momentum_score + spike_bonus), 4)
        score_map[(str(rid), int(hn))] = final_score

    df["odds_momentum_score"] = df.apply(
        lambda r: score_map.get((str(r["race_id"]), int(r["horse_number"])), _DEFAULT),
        axis=1,
    )
    return df
