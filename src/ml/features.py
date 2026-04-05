"""
特徴量生成モジュール

FeatureBuilder は SQLite DB から出馬表・過去成績・オッズを読み込み、
機械学習モデルに投入する特徴量 DataFrame を生成する。

特徴量一覧:
  数値: weight_carried, horse_weight, win_odds, popularity
  馬成績: win_rate_all, win_rate_surface, win_rate_distance_band, recent_rank_mean
  カテゴリ: surface_code, sex_code, venue_encoded, sire_encoded
"""

from __future__ import annotations

import logging
import re
import sqlite3
from functools import lru_cache
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# 距離バンドの境界（m）
_DISTANCE_BANDS = [
    (0,    1400, "sprint"),
    (1400, 1800, "mile"),
    (1800, 2200, "intermediate"),
    (2200, 9999, "long"),
]

# 固定カテゴリ辞書（未知値は -1 にフォールバック）
_SURFACE_CODE: dict[str, int] = {"芝": 0, "ダート": 1, "障害": 2}
_SEX_CODE: dict[str, int] = {"牡": 0, "牝": 1, "セ": 2}

# 会場エンコーディング（JRA 10 場 + 地方/海外）
_VENUE_CODE: dict[str, int] = {
    "札幌": 0, "函館": 1, "福島": 2, "新潟": 3,
    "東京": 4, "中山": 5, "中京": 6, "京都": 7,
    "阪神": 8, "小倉": 9,
}


def _distance_band(distance: int) -> str:
    for lo, hi, label in _DISTANCE_BANDS:
        if lo <= distance < hi:
            return label
    return "long"


def _parse_sex(sex_age: str) -> str:
    """'牡3' → '牡'"""
    m = re.match(r"([牡牝セ])", sex_age)
    return m.group(1) if m else ""


class FeatureBuilder:
    """
    SQLite DB を参照して機械学習用の特徴量 DataFrame を生成するクラス。

    Usage:
        conn = init_db(db_path)
        fb = FeatureBuilder(conn)
        df = fb.build_race_features("202506050811")
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._sire_map: dict[str, int] = {}

    # ── パブリック API ──────────────────────────────────────────

    def build_race_features(self, race_id: str) -> pd.DataFrame:
        """
        指定レースの出馬表を基に特徴量 DataFrame を生成して返す。

        Args:
            race_id: netkeiba の race_id

        Returns:
            各行が1頭、各列が特徴量の DataFrame。
            horse_number / horse_name / horse_id 列は識別用として保持。
        """
        race_row = self._conn.execute(
            "SELECT distance, surface, venue FROM races WHERE race_id = ?",
            (race_id,),
        ).fetchone()

        if race_row is None:
            raise ValueError(f"race_id が DB に存在しません: {race_id!r}")

        distance, surface, venue = race_row
        dist_band = _distance_band(distance)

        entries = self._conn.execute(
            """
            SELECT horse_number, horse_id, horse_name, sex_age,
                   weight_carried, horse_weight
            FROM entries
            WHERE race_id = ?
            ORDER BY horse_number
            """,
            (race_id,),
        ).fetchall()

        # 最新オッズを馬番で引く
        odds_map = self._latest_odds_map(race_id)

        records = []
        for horse_number, horse_id, horse_name, sex_age, weight_carried, horse_weight in entries:
            stats = self._get_horse_stats(horse_id, surface, distance)
            odds = odds_map.get(horse_number, {})

            records.append({
                # 識別子（モデル学習には使わない）
                "horse_number": horse_number,
                "horse_id":     horse_id,
                "horse_name":   horse_name,
                # 数値特徴量
                "weight_carried":       weight_carried,
                "horse_weight":         horse_weight,
                "win_odds":             odds.get("win_odds"),
                "popularity":           odds.get("popularity"),
                # 馬成績特徴量
                "win_rate_all":             stats["win_rate_all"],
                "win_rate_surface":         stats["win_rate_surface"],
                "win_rate_distance_band":   stats["win_rate_distance_band"],
                "recent_rank_mean":         stats["recent_rank_mean"],
                # カテゴリ特徴量（整数エンコード）
                "surface_code": _SURFACE_CODE.get(surface, -1),
                "sex_code":     _SEX_CODE.get(_parse_sex(sex_age), -1),
                "venue_encoded": _VENUE_CODE.get(venue, len(_VENUE_CODE)),
                "sire_encoded": self._encode_sire(
                    self._get_sire(horse_id)
                ),
                # レース情報（セグメント識別用）
                "distance":    distance,
                "dist_band":   dist_band,
            })

        df = pd.DataFrame(records)
        logger.info(
            "特徴量生成 race_id=%s: %d 頭 × %d 特徴量",
            race_id, len(df), df.shape[1],
        )
        return df

    # ── 内部メソッド ───────────────────────────────────────────

    def _get_horse_stats(
        self,
        horse_id: str | None,
        surface: str,
        distance: int,
    ) -> dict[str, float | None]:
        """
        horses / race_results テーブルから馬の過去成績指標を算出する。

        Returns:
            {
              "win_rate_all":           全成績における1着率 (0〜1)
              "win_rate_surface":       同馬場面での1着率
              "win_rate_distance_band": 同距離帯での1着率
              "recent_rank_mean":       直近5走の平均着順
            }
        """
        _null: dict[str, float | None] = {
            "win_rate_all": None,
            "win_rate_surface": None,
            "win_rate_distance_band": None,
            "recent_rank_mean": None,
        }
        if not horse_id:
            return _null

        dist_band = _distance_band(distance)

        # 全成績
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            WHERE rr.horse_id = ? AND rr.rank IS NOT NULL
            """,
            (horse_id,),
        ).fetchone()
        total, wins = row if row else (0, 0)
        win_rate_all = (wins / total) if total else None

        # 同馬場
        row_sf = self._conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND r.surface = ? AND rr.rank IS NOT NULL
            """,
            (horse_id, surface),
        ).fetchone()
        total_sf, wins_sf = row_sf if row_sf else (0, 0)
        win_rate_surface = (wins_sf / total_sf) if total_sf else None

        # 同距離帯（距離の下限〜上限で range に変換）
        lo, hi = next(
            (lo, hi) for lo, hi, label in _DISTANCE_BANDS if label == dist_band
        )
        row_db = self._conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ?
              AND r.distance >= ? AND r.distance < ?
              AND rr.rank IS NOT NULL
            """,
            (horse_id, lo, hi),
        ).fetchone()
        total_db, wins_db = row_db if row_db else (0, 0)
        win_rate_distance_band = (wins_db / total_db) if total_db else None

        # 直近5走の平均着順
        rows_recent = self._conn.execute(
            """
            SELECT rr.rank
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND rr.rank IS NOT NULL
            ORDER BY r.date DESC
            LIMIT 5
            """,
            (horse_id,),
        ).fetchall()
        recent_rank_mean: float | None = (
            sum(r[0] for r in rows_recent) / len(rows_recent)
            if rows_recent else None
        )

        return {
            "win_rate_all":           win_rate_all,
            "win_rate_surface":       win_rate_surface,
            "win_rate_distance_band": win_rate_distance_band,
            "recent_rank_mean":       recent_rank_mean,
        }

    def _get_sire(self, horse_id: str | None) -> str | None:
        """horses テーブルから父名を取得する。"""
        if not horse_id:
            return None
        row = self._conn.execute(
            "SELECT sire FROM horses WHERE horse_id = ?", (horse_id,)
        ).fetchone()
        return row[0] if row else None

    def _encode_sire(self, sire: str | None) -> int:
        """
        父名をラベルエンコードする。
        同一セッション内で一貫した整数を返し、未知は新規割り当て。
        """
        if not sire:
            return -1
        if sire not in self._sire_map:
            self._sire_map[sire] = len(self._sire_map)
        return self._sire_map[sire]

    def _latest_odds_map(self, race_id: str) -> dict[int, dict]:
        """
        realtime_odds から各馬の最新オッズを {馬番: {win_odds, popularity}} で返す。
        レコードがなければ空辞書を返す。
        """
        rows = self._conn.execute(
            """
            SELECT horse_number, win_odds, popularity
            FROM realtime_odds
            WHERE race_id = ?
              AND recorded_at = (
                  SELECT MAX(recorded_at) FROM realtime_odds WHERE race_id = ?
              )
            """,
            (race_id, race_id),
        ).fetchall()
        return {
            r[0]: {"win_odds": r[1], "popularity": r[2]}
            for r in rows
        }
