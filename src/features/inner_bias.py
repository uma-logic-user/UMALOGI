"""
src/features/inner_bias.py — 内枠(1-3枠)複勝バイアスの z スコア（W-098 / 研究）

定義（社長指定 2026-06-15）:
  today_inner_bias     = 当日の **既走レース**（発走前に確定済み）における内枠(1-3枠)の
                          複勝率が、過去平均比で何σ高いか（z スコア）。
  yesterday_inner_bias = 直近の前開催日（完了済み）の全レースで同様に算出した z スコア。

リークフリーの担保（最重要）:
  - today: 対象レースより **race_number が小さい同日レースの確定結果のみ** を参照する
    （対象レース自身・後続レースは絶対に見ない）。
  - yesterday: 対象日より前の直近開催日（完全に終了）のみ参照する。
  - z スコアの基準 (μ, σ) は **参照期間（既定 reference_hi 未満）** の全開催日フル日次率から算出
    （対象レースの結果を一切含まない）。

内枠の定義: gate_number ∈ {1, 2, 3}。複勝 = rank ∈ {1, 2, 3}。
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field

# 当日の早期レースが少なすぎる場合、today 率は不安定なので neutral(z=0) にするしきい値。
_MIN_TODAY_INNER_STARTERS = 8
_Z_CLIP = 3.0


@dataclass
class DailyInnerIndex:
    """日付 → 内枠スタータ数/複勝数 の索引（前計算してルックアップ高速化）。"""

    # date -> list[(race_number, inner_started, inner_placed)]（race_number 昇順）
    by_date: dict[str, list[tuple[int, int, int]]] = field(default_factory=dict)
    # date -> (full_day_started, full_day_placed)
    full_day: dict[str, tuple[int, int]] = field(default_factory=dict)
    sorted_dates: list[str] = field(default_factory=list)
    baseline_mu: float = 0.0
    baseline_sigma: float = 1.0

    def _prev_racing_date(self, date: str) -> str | None:
        import bisect

        i = bisect.bisect_left(self.sorted_dates, date)
        return self.sorted_dates[i - 1] if i > 0 else None

    def _z(self, rate: float | None) -> float:
        if rate is None or self.baseline_sigma <= 0:
            return 0.0
        z = (rate - self.baseline_mu) / self.baseline_sigma
        return max(-_Z_CLIP, min(_Z_CLIP, z))

    def today_bias_z(self, date: str, race_number: int) -> float:
        """当日 race_number 未満の既走レースから内枠複勝率の z を返す（無/少数は 0）。"""
        races = self.by_date.get(date)
        if not races:
            return 0.0
        started = placed = 0
        for rn, s, p in races:
            if rn < race_number:
                started += s
                placed += p
        if started < _MIN_TODAY_INNER_STARTERS:
            return 0.0
        return self._z(placed / started)

    def yesterday_bias_z(self, date: str) -> float:
        """直近の前開催日（完了）の内枠複勝率の z を返す。"""
        prev = self._prev_racing_date(date)
        if prev is None:
            return 0.0
        s, p = self.full_day.get(prev, (0, 0))
        if s < _MIN_TODAY_INNER_STARTERS:
            return 0.0
        return self._z(p / s)


def build_daily_inner_index(
    conn: sqlite3.Connection,
    lo: str,
    hi: str,
    *,
    reference_hi: str | None = None,
) -> DailyInnerIndex:
    """[lo, hi) の開催を集計して索引を作る。基準 (μ,σ) は [lo, reference_hi) の日次率から。

    Args:
        conn: DB 接続。
        lo, hi: 索引対象の日付範囲（hi は排他）。
        reference_hi: μ,σ 算出に使う上限日（排他）。既定は lo+1年相当でなく lo 起点〜
            最初の cutoff より前を呼び出し側で渡す想定（リーク防止）。None なら lo..hi 全体。
    """
    rows = conn.execute(
        """
        SELECT r.date AS d, r.race_number AS rn,
               SUM(CASE WHEN rr.gate_number BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS inner_started,
               SUM(CASE WHEN rr.gate_number BETWEEN 1 AND 3
                        AND rr.rank BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS inner_placed
        FROM races r JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.date >= ? AND r.date < ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
          AND rr.gate_number IS NOT NULL
        GROUP BY r.date, r.race_number
        ORDER BY r.date, r.race_number
        """,
        (lo, hi),
    ).fetchall()

    idx = DailyInnerIndex()
    for d, rn, s, p in rows:
        if d is None or rn is None:
            continue
        idx.by_date.setdefault(str(d), []).append((int(rn), int(s or 0), int(p or 0)))

    # フル日次集計
    daily_rates: list[float] = []
    ref_hi = reference_hi
    for d, races in idx.by_date.items():
        ts = sum(s for _, s, _ in races)
        tp = sum(p for _, _, p in races)
        idx.full_day[d] = (ts, tp)
        if ts >= _MIN_TODAY_INNER_STARTERS and (ref_hi is None or d < ref_hi):
            daily_rates.append(tp / ts)

    idx.sorted_dates = sorted(idx.by_date.keys())
    if daily_rates:
        mu = sum(daily_rates) / len(daily_rates)
        var = sum((x - mu) ** 2 for x in daily_rates) / max(1, len(daily_rates) - 1)
        idx.baseline_mu = mu
        idx.baseline_sigma = math.sqrt(var) if var > 0 else 1.0
    else:
        # フォールバック: 内枠3枠/平均出走頭数 ~ 複勝基準率の理論値近辺
        idx.baseline_mu = 0.375
        idx.baseline_sigma = 0.1
    return idx
