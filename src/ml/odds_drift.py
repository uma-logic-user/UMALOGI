"""
オッズ歪み（大口投票）検知 — 朝オッズ vs 直前オッズの変動率。

``realtime_odds`` は呼び出しごとにスナップショットを追記保持する（履歴テーブル）。
本モジュールは馬番単位で最古スナップショット（baseline=朝/暫定）と
最新スナップショット（直前）を比較し、変動率から以下を検知する。

  - 急落（大口流入 / plunge）  : baseline から大きくオッズが下がった = 資金集中
  - 急騰（市場見限り / abandoned）: baseline から大きくオッズが上がった = 危険馬

用途（src/pipeline/prediction.py Step 4c で統合）:
  1. 危険馬フィルタ : 急騰した馬を「軸」に含む買い目の期待値を減衰し、
                     閾値（EV>=1.0）を割った買い目を除外する。
  2. 大口検知ログ   : 急落馬を WARNING ログ・根拠メモへ供給する。
"""

from __future__ import annotations

import logging
import sqlite3
import statistics
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# フィールド相対変動率しきい値（各馬 drift − レース中央値 drift）
#   2 スナップショットが朝暫定 vs 直前で規模が異なると全馬が系統的にシフトするため、
#   生の baseline 比ではなくレース中央値からの「相対乖離」で突出した馬だけを検知する。
PLUNGE_THRESHOLD: float = 0.25  # 中央値より 25% 以上 余計に急落 → 大口流入（plunge）
ABANDON_THRESHOLD: float = 0.40  # 中央値より 40% 以上 余計に急騰 → 市場見限り（危険馬）

# 危険馬を軸に含む買い目に乗じる EV 減衰係数（< 1.0）
DANGER_EV_FACTOR: float = 0.5

# baseline オッズの下限（1.0 以下は不正値として除外）
_MIN_BASELINE_ODDS: float = 1.0

# drift を計算するのに必要な最小スナップショット数
_MIN_SNAPSHOTS: int = 2

# 中央値を安定して取るために必要な最小頭数（これ未満はノイズとみなし空を返す）
_MIN_FIELD: int = 4


@dataclass(frozen=True)
class OddsDrift:
    """1 頭分のオッズ変動情報。"""

    horse_number: int
    baseline_odds: float  # 最古スナップショット（朝/暫定）
    latest_odds: float  # 最新スナップショット（直前）
    drift_ratio: (
        float  # (latest - baseline) / baseline。負=急落 / 正=急騰（生の変動率）
    )
    rel_drift: float  # drift_ratio − レース中央値drift（フィールド相対乖離）
    is_plunge: bool  # 大口流入（フィールド比で突出して急落）
    is_abandoned: bool  # 危険馬（フィールド比で突出して急騰）


def compute_drift_map(conn: sqlite3.Connection, race_id: str) -> dict[int, OddsDrift]:
    """``realtime_odds`` 履歴から馬番単位のオッズ変動マップを構築する。

    各馬の最古スナップショットを baseline、最新スナップショットを latest として
    生の変動率を算出後、**レース中央値からの相対乖離**で plunge/abandon を判定する。
    全馬が同方向に系統シフトしても中央値が吸収するため誤検知が起きにくい。

    スナップショットが 1 点以下の馬・baseline 不正馬は除外し、
    drift を計算できる頭数が ``_MIN_FIELD`` 未満のレースは空 dict を返す。

    Args:
        conn: umalogi.db 接続。
        race_id: 対象レース ID。

    Returns:
        ``{馬番: OddsDrift}``。drift を計算できる馬が無ければ空 dict。
    """
    rows = conn.execute(
        """
        SELECT horse_number, win_odds, recorded_at
        FROM realtime_odds
        WHERE race_id = ?
          AND win_odds IS NOT NULL
          AND win_odds > 0
        ORDER BY horse_number, recorded_at
        """,
        (race_id,),
    ).fetchall()

    by_horse: dict[int, list[float]] = {}
    for hn, odds, _ts in rows:
        try:
            num = int(hn)
            val = float(odds)
        except (TypeError, ValueError):
            continue
        by_horse.setdefault(num, []).append(val)

    # 第1パス: 各馬の生 drift を算出
    raw: dict[int, tuple[float, float, float]] = {}  # 馬番 -> (baseline, latest, drift)
    for num, seq in by_horse.items():
        if len(seq) < _MIN_SNAPSHOTS:
            continue
        baseline = seq[0]
        latest = seq[-1]
        if baseline < _MIN_BASELINE_ODDS:
            continue
        raw[num] = (baseline, latest, (latest - baseline) / baseline)

    if len(raw) < _MIN_FIELD:
        return {}

    median_drift = statistics.median(d for _b, _l, d in raw.values())

    # 第2パス: 中央値からの相対乖離で判定
    out: dict[int, OddsDrift] = {}
    for num, (baseline, latest, drift) in raw.items():
        rel = drift - median_drift
        # is_abandoned: 自身も急騰(drift>0)かつフィールド比でも突出
        # is_plunge:   自身も急落(drift<0)かつフィールド比でも突出
        out[num] = OddsDrift(
            horse_number=num,
            baseline_odds=round(baseline, 1),
            latest_odds=round(latest, 1),
            drift_ratio=round(drift, 4),
            rel_drift=round(rel, 4),
            is_plunge=(drift < 0.0 and rel <= -PLUNGE_THRESHOLD),
            is_abandoned=(drift > 0.0 and rel >= ABANDON_THRESHOLD),
        )
    return out


def danger_horses(drift_map: dict[int, OddsDrift]) -> set[int]:
    """急騰（市場見限り）した危険馬の馬番集合を返す。"""
    return {hn for hn, d in drift_map.items() if d.is_abandoned}


def plunge_horses(drift_map: dict[int, OddsDrift]) -> set[int]:
    """急落（大口流入）した馬の馬番集合を返す。"""
    return {hn for hn, d in drift_map.items() if d.is_plunge}


def _axis_horses(combinations: list[tuple[int, ...]]) -> set[int]:
    """全組み合わせに共通して出現する馬番（＝流し軸）の集合を返す。

    単勝・複勝（組み合わせ 1 つ）は当該馬がそのまま軸となる。
    """
    if not combinations:
        return set()
    common: set[int] = set(combinations[0])
    for combo in combinations[1:]:
        common &= set(combo)
    return common


def apply_drift_filter(
    bets: list,  # list[BetRecommendation]
    drift_map: dict[int, OddsDrift],
    *,
    ev_min: float = 1.0,
) -> tuple[list, int, int]:
    """危険馬を軸に含む買い目の EV を減衰し、閾値割れを除外する。

    BetRecommendation を破壊的に変更する（expected_value / notes）。

    Args:
        bets: BetRecommendation のリスト。
        drift_map: :func:`compute_drift_map` の結果。
        ev_min: EV 下限。減衰後にこれを下回る買い目は除外する。

    Returns:
        ``(残った買い目, 除外件数, 減衰件数)``。
    """
    dangers = danger_horses(drift_map)
    if not dangers:
        return bets, 0, 0

    kept: list = []
    dropped = 0
    penalized = 0
    for bet in bets:
        axis = _axis_horses(getattr(bet, "combinations", []))
        if axis & dangers:
            new_ev = float(bet.expected_value) * DANGER_EV_FACTOR
            penalized += 1
            if new_ev < ev_min:
                dropped += 1
                logger.info(
                    "危険馬フィルタ除外: 軸=%s EV %.2f→%.2f (<%.2f) bet_type=%s",
                    sorted(axis & dangers),
                    bet.expected_value,
                    new_ev,
                    ev_min,
                    getattr(bet, "bet_type", "?"),
                )
                continue
            bet.expected_value = round(new_ev, 3)
            note = getattr(bet, "notes", "") or ""
            bet.notes = (note + " ⚠️大口逆行(危険馬)EV減衰").strip()
        kept.append(bet)
    return kept, dropped, penalized
