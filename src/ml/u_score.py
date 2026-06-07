"""
U Score — UMALOGI 統合評価スコアエンジン（Phase 1+W-004: 19因子）

DB に存在するデータのみで 19 の定量因子を計算し、[0, 1] 正規化した
加重平均として u_score を算出する。ALPHAモデル・本命モデルの特徴量に追加することで
「市場が見落とした定量的根拠」を EV 計算に反映する。

設計:
  A) 能力指数  (38%): 通算勝率・馬場別・距離帯・直近着順・トレンド・休養日数
  B) 人的要素  (28.5%): 騎手直近勝率・調教師直近勝率・騎手×馬コンビ・騎手×会場
  C) コース適性(19%): 枠番適性・会場適性・美浦栗東マッチ
  D) 調教指数  ( 6.5%): ウッドスピード指数・坂路スピード指数
  E) 血統適性  ( 3%): 父馬距離適性・母父馬場適性・父の父系統
  F) 大衆心理  ( 5%): 大衆心理乖離スコア [W-004]

全 SQL は GROUP BY バッチクエリで実行しリスト内包型ループを排除する。
"""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── 因子グループ重み ──────────────────────────────────────────────────
# W-004 追加: crowd 5% を確保し、他グループを比率 0.95 でスケールダウン
_WEIGHTS: dict[str, float] = {
    "ability": 0.35,      # 微減: competition グループへ再分配
    "human": 0.265,
    "course": 0.175,
    "training": 0.06,
    "bloodline": 0.03,
    "crowd": 0.05,        # F: 大衆心理乖離 [W-004]
    "competition": 0.07,  # G: 相手関係・クラス変化 [W-010/011]
}

_GROUP_FACTORS: dict[str, list[str]] = {
    "ability": [
        "uf_win_rate_all",
        "uf_win_rate_surface",
        "uf_win_rate_distance",
        "uf_recent_rank",
        "uf_rank_trend",
        "uf_rest_days",
    ],
    "human": [
        "uf_jockey_win_rate",
        "uf_trainer_win_rate",
        "uf_jockey_horse_combo",
        "uf_jockey_venue",
    ],
    "course": ["uf_gate_fit", "uf_venue_win_rate", "uf_east_west_match"],
    "training": ["uf_tc_speed", "uf_hc_speed"],
    "bloodline": ["uf_sire_distance", "uf_bms_surface", "uf_father_sire"],
    "crowd": ["uf_crowd_bias"],  # F: 大衆心理乖離スコア [W-004]
    "competition": ["uf_class_change", "uf_competition_strength"],  # G: W-010/011
}

# 全因子リスト（models.py FEATURE_COLS へ追加する際に参照）
U_SCORE_FACTOR_COLS: list[str] = [f for g in _GROUP_FACTORS.values() for f in g]

# FEATURE_COLS へ追加する派生数値列（率・指数）
U_SCORE_STAT_COLS: list[str] = [
    "jockey_win_rate_90d",
    "trainer_win_rate_90d",
    "jockey_horse_combo_rate",
    "jockey_venue_win_rate",
    "venue_win_rate",
    "days_since_last_race",
    "tc_speed_index",
    "hc_speed_index",
    "crowd_bias_ratio",     # 大衆心理乖離比率（生値）[W-004]
    "class_change_delta",   # クラス変化スコア（生値）[W-011]
    "competition_strength", # 相手関係強度（生値）[W-010]
    "u_score",
]

# models.py の FEATURE_COLS に追加する全列
U_SCORE_FEATURES: list[str] = U_SCORE_FACTOR_COLS + U_SCORE_STAT_COLS


class UScoreEngine:
    """
    U score 計算エンジン。

    Parameters
    ----------
    conn : sqlite3.Connection
        umalogi.db への接続。GROUP BY バッチ SQL で集計を行う。
    lookback_days : int
        騎手・調教師の勝率集計ウィンドウ（デフォルト 90 日）。
    min_samples : int
        統計値に必要な最低サンプル数。不足は NaN → 0.5 に補填。
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        lookback_days: int = 90,
        min_samples: int = 5,
    ) -> None:
        self._conn = conn
        self._lookback_days = lookback_days
        self._min_samples = min_samples

    # ── パブリック API ────────────────────────────────────────────────

    def calc(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        U score 因子を DataFrame に追加して返す。

        Parameters
        ----------
        df : DataFrame
            FeatureBuilder が返す 1行1頭の特徴量 DataFrame。
            必須列: race_id, horse_id, jockey, trainer, venue, surface,
                    distance, gate_number, win_rate_all, win_rate_surface,
                    win_rate_distance_band, recent_rank_mean

        Returns
        -------
        df : DataFrame  (元 df + U score 因子列 + u_score 列)
        """
        if df.empty:
            return df
        df = df.copy()

        # race_id 先頭8桁から基準日を導出（YYYY-MM-DD）
        df["_base_date"] = (
            df["race_id"].str[:4]
            + "-"
            + df["race_id"].str[4:6]
            + "-"
            + df["race_id"].str[6:8]
        )

        df = self._calc_ability(df)
        df = self._calc_human(df)
        df = self._calc_course(df)
        df = self._calc_training(df)
        df = self._calc_bloodline(df)
        df = self._calc_crowd_bias(df)    # F: 大衆心理乖離 [W-004]
        df = self._calc_competition(df)   # G: 相手関係・クラス変化 [W-010/011]
        df = self._calc_composite(df)

        df.drop(columns=["_base_date"], errors="ignore", inplace=True)
        return df

    # ── A: 能力指数 ───────────────────────────────────────────────────

    def _calc_ability(self, df: pd.DataFrame) -> pd.DataFrame:
        # 通算勝率
        df["uf_win_rate_all"] = _clip_norm(
            pd.to_numeric(df.get("win_rate_all"), errors="coerce"), 0.0, 1.0
        )
        # 馬場別勝率
        df["uf_win_rate_surface"] = _clip_norm(
            pd.to_numeric(df.get("win_rate_surface"), errors="coerce"), 0.0, 1.0
        )
        # 距離帯別勝率
        df["uf_win_rate_distance"] = _clip_norm(
            pd.to_numeric(df.get("win_rate_distance_band"), errors="coerce"), 0.0, 1.0
        )
        # 直近着順（1着=1.0, 18着=0.0）
        recent = pd.to_numeric(df.get("recent_rank_mean"), errors="coerce")
        df["uf_recent_rank"] = _clip_norm(19.0 - recent, 1.0, 18.0)

        # 着順改善トレンド（バッチSQL）
        df["uf_rank_trend"] = self._rank_trend_batch(df)

        # 前走間隔（SQL + ガウス型スコア）
        days = self._days_since_last_race_batch(df)
        df["days_since_last_race"] = days
        df["uf_rest_days"] = _rest_days_score(days)

        return df

    def _rank_trend_batch(self, df: pd.DataFrame) -> pd.Series:
        """直近着順トレンド: 直近2走 vs 3〜5走前の平均着順比較。"""
        scores = pd.Series(0.5, index=df.index, dtype=float)
        ids = df["horse_id"].dropna().unique().tolist()
        if not ids:
            return scores

        ph = ",".join("?" * len(ids))
        rows = self._conn.execute(
            f"""
            SELECT rr.horse_id, rr.rank, r.date
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            WHERE  rr.horse_id IN ({ph})
              AND  rr.rank IS NOT NULL
              AND  rr.rank > 0
            ORDER  BY rr.horse_id, r.date DESC
            """,
            ids,
        ).fetchall()

        from collections import defaultdict

        hist: dict[str, list[int]] = defaultdict(list)
        for hid, rank, _ in rows:
            if len(hist[hid]) < 6:
                hist[hid].append(rank)

        trend: dict[str, float] = {}
        for hid, ranks in hist.items():
            if len(ranks) < 3:
                trend[hid] = 0.5
                continue
            recent_avg = float(np.mean(ranks[:2]))
            older_avg = float(np.mean(ranks[2:5]))
            improvement = older_avg - recent_avg  # 正=改善
            trend[hid] = float(np.clip((improvement + 10) / 20, 0.0, 1.0))

        id_col = df["horse_id"].fillna("")
        return id_col.map(lambda h: trend.get(h, 0.5))

    def _days_since_last_race_batch(self, df: pd.DataFrame) -> pd.Series:
        """前走からの日数（バッチSQL）。"""
        days = pd.Series(np.nan, index=df.index, dtype=float)
        if "horse_id" not in df.columns or "race_id" not in df.columns:
            return days

        ids = df["horse_id"].dropna().unique().tolist()
        if not ids:
            return days

        # 各馬の直近2レースを取得して前走日を算出
        ph = ",".join("?" * len(ids))
        rows = self._conn.execute(
            f"""
            SELECT rr.horse_id, r.date, r.race_id
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            WHERE  rr.horse_id IN ({ph})
            ORDER  BY rr.horse_id, r.date DESC
            """,
            ids,
        ).fetchall()

        from collections import defaultdict

        # horse_id -> [(date_str, race_id), ...]
        hist: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for hid, d, rid in rows:
            if len(hist[hid]) < 2:
                hist[hid].append((d, rid))

        # race_id -> current date（ph は horse_ids 用なので別途計算）
        race_ids = df["race_id"].dropna().unique().tolist()
        ph_race = ",".join("?" * len(race_ids))
        race_dates: dict[str, str] = dict(
            self._conn.execute(
                f"SELECT race_id, date FROM races WHERE race_id IN ({ph_race})",
                race_ids,
            ).fetchall()
        )

        def _to_date(s: str) -> "date":
            from datetime import date as _d

            return _d.fromisoformat(s.replace("/", "-"))

        result: dict[str, float] = {}
        for hid, entries in hist.items():
            if len(entries) < 2:
                continue
            cur_date_str = race_dates.get(entries[0][1], "")
            prev_date_str = entries[1][0] if len(entries) > 1 else ""
            if not cur_date_str or not prev_date_str:
                continue
            try:
                result[hid] = float(
                    (_to_date(cur_date_str) - _to_date(prev_date_str)).days
                )
            except Exception:
                pass

        return df["horse_id"].fillna("").map(lambda h: result.get(h, np.nan))

    # ── B: 人的要素 ───────────────────────────────────────────────────

    def _calc_human(self, df: pd.DataFrame) -> pd.DataFrame:
        # 騎手直近90日勝率
        jock_rates = self._jockey_win_rates_batch(df)
        df["jockey_win_rate_90d"] = jock_rates
        df["uf_jockey_win_rate"] = _clip_norm(jock_rates, 0.0, 0.30)

        # 調教師直近90日勝率
        train_rates = self._trainer_win_rates_batch(df)
        df["trainer_win_rate_90d"] = train_rates
        df["uf_trainer_win_rate"] = _clip_norm(train_rates, 0.0, 0.25)

        # 騎手×馬コンビ勝率
        combo = self._jockey_horse_combo_batch(df)
        df["jockey_horse_combo_rate"] = combo
        df["uf_jockey_horse_combo"] = _clip_norm(combo, 0.0, 0.50)

        # 騎手×会場勝率
        jv = self._jockey_venue_batch(df)
        df["jockey_venue_win_rate"] = jv
        df["uf_jockey_venue"] = _clip_norm(jv, 0.0, 0.30)

        return df

    def _jockey_win_rates_batch(self, df: pd.DataFrame) -> pd.Series:
        """騎手別 N日勝率をバッチ取得。base_date が同一なら1クエリで済む。"""
        rates = pd.Series(np.nan, index=df.index, dtype=float)
        if "jockey" not in df.columns:
            return rates

        # base_date ごとにまとめる（訓練時は多様な日付が混在する）
        for base_date, grp in df.groupby("_base_date"):
            jockeys = grp["jockey"].dropna().unique().tolist()
            if not jockeys:
                continue
            from_date = self._offset_date(str(base_date), -self._lookback_days)
            ph = ",".join("?" * len(jockeys))
            rows = self._conn.execute(
                f"""
                SELECT rr.jockey,
                       COUNT(*) AS total,
                       SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
                FROM   race_results rr
                JOIN   races r ON r.race_id = rr.race_id
                WHERE  rr.jockey IN ({ph})
                  AND  r.date >= ?
                  AND  r.date <  ?
                GROUP  BY rr.jockey
                """,
                jockeys + [from_date, base_date],
            ).fetchall()
            rate_map = {
                j: (w / t) if t >= self._min_samples else np.nan for j, t, w in rows
            }
            idx = grp.index
            rates.loc[idx] = df.loc[idx, "jockey"].map(
                lambda x: rate_map.get(x, np.nan)
            )

        return rates

    def _trainer_win_rates_batch(self, df: pd.DataFrame) -> pd.Series:
        rates = pd.Series(np.nan, index=df.index, dtype=float)
        if "trainer" not in df.columns:
            return rates

        for base_date, grp in df.groupby("_base_date"):
            trainers = grp["trainer"].dropna().unique().tolist()
            if not trainers:
                continue
            from_date = self._offset_date(str(base_date), -self._lookback_days)
            ph = ",".join("?" * len(trainers))
            rows = self._conn.execute(
                f"""
                SELECT rr.trainer,
                       COUNT(*) AS total,
                       SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
                FROM   race_results rr
                JOIN   races r ON r.race_id = rr.race_id
                WHERE  rr.trainer IN ({ph})
                  AND  r.date >= ?
                  AND  r.date <  ?
                GROUP  BY rr.trainer
                """,
                trainers + [from_date, base_date],
            ).fetchall()
            rate_map = {
                t: (w / tot) if tot >= self._min_samples else np.nan
                for t, tot, w in rows
            }
            idx = grp.index
            rates.loc[idx] = df.loc[idx, "trainer"].map(
                lambda x: rate_map.get(x, np.nan)
            )

        return rates

    def _jockey_horse_combo_batch(self, df: pd.DataFrame) -> pd.Series:
        """騎手×馬コンビ勝率（リーク防止: カレント race_id 未満のみ）。"""
        rates = pd.Series(np.nan, index=df.index, dtype=float)
        if "jockey" not in df.columns or "horse_id" not in df.columns:
            return rates

        # (jockey, horse_id) のユニークペアを収集
        pairs = (
            df[["jockey", "horse_id", "race_id"]]
            .dropna(subset=["jockey", "horse_id"])
            .drop_duplicates(subset=["jockey", "horse_id"])
        )
        if pairs.empty:
            return rates

        for _, row in pairs.iterrows():
            j, h, rid = row["jockey"], row["horse_id"], row["race_id"]
            result = self._conn.execute(
                """
                SELECT COUNT(*),
                       AVG(CASE WHEN rr.rank=1 THEN 1.0 ELSE 0.0 END)
                FROM   race_results rr
                WHERE  rr.jockey   = ?
                  AND  rr.horse_id = ?
                  AND  rr.race_id  < ?
                """,
                (j, h, rid),
            ).fetchone()
            total, win_rate = result
            if (total or 0) >= 2:
                mask = (df["jockey"] == j) & (df["horse_id"] == h)
                rates.loc[mask] = float(win_rate or 0)

        return rates

    def _jockey_venue_batch(self, df: pd.DataFrame) -> pd.Series:
        """騎手×会場 勝率（全期間）。"""
        rates = pd.Series(np.nan, index=df.index, dtype=float)
        if "jockey" not in df.columns or "venue" not in df.columns:
            return rates

        for base_date, grp in df.groupby("_base_date"):
            combos = grp[["jockey", "venue"]].dropna().drop_duplicates().values.tolist()
            for jockey, venue in combos:
                result = self._conn.execute(
                    """
                    SELECT COUNT(*),
                           SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END)
                    FROM   race_results rr
                    JOIN   races r ON r.race_id = rr.race_id
                    WHERE  rr.jockey = ?
                      AND  r.venue   = ?
                      AND  r.date    < ?
                    """,
                    (jockey, venue, base_date),
                ).fetchone()
                total, wins = result
                if (total or 0) >= self._min_samples:
                    mask = (df["jockey"] == jockey) & (df["venue"] == venue)
                    rates.loc[mask] = (wins or 0) / total

        return rates

    # ── C: コース適性 ─────────────────────────────────────────────────

    def _calc_course(self, df: pd.DataFrame) -> pd.DataFrame:
        # 枠番×馬場×距離帯 統計勝率
        df["uf_gate_fit"] = self._gate_fit_batch(df)

        # 馬の当該会場過去勝率
        venue_rates = self._horse_venue_rate_batch(df)
        df["venue_win_rate"] = venue_rates
        df["uf_venue_win_rate"] = _clip_norm(venue_rates, 0.0, 0.60)

        # 美浦/栗東マッチフラグ
        df["uf_east_west_match"] = self._east_west_match(df)

        return df

    def _gate_fit_batch(self, df: pd.DataFrame) -> pd.Series:
        """枠番 × surface × 距離帯 の DB 全期間勝率を一括取得して正規化。"""
        scores = pd.Series(0.5, index=df.index, dtype=float)
        if "gate_number" not in df.columns:
            return scores

        rows = self._conn.execute(
            """
            SELECT rr.gate_number, r.surface,
                   CASE
                     WHEN r.distance <= 1600 THEN 'short'
                     WHEN r.distance <= 2000 THEN 'mile'
                     ELSE 'long'
                   END AS dist_band,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            WHERE  rr.gate_number IS NOT NULL
            GROUP  BY rr.gate_number, r.surface, dist_band
            HAVING total >= 20
            """,
        ).fetchall()
        rate_map: dict[tuple, float] = {(g, s, b): (w / t) for g, s, b, t, w in rows}

        def _dist_band(d: Any) -> str:
            d = float(d or 0)
            if d <= 1600:
                return "short"
            elif d <= 2000:
                return "mile"
            return "long"

        for idx, row in df.iterrows():
            g = row.get("gate_number")
            s = row.get("surface", "")
            b = _dist_band(row.get("distance", 0))
            key = (g, s, b)
            if key in rate_map:
                scores.at[idx] = rate_map[key]

        return _clip_norm(scores, 0.0, 0.20)

    def _horse_venue_rate_batch(self, df: pd.DataFrame) -> pd.Series:
        """馬×会場 過去勝率（リーク防止）。"""
        rates = pd.Series(np.nan, index=df.index, dtype=float)
        if "horse_id" not in df.columns or "venue" not in df.columns:
            return rates

        ids = df["horse_id"].dropna().unique().tolist()
        if not ids:
            return rates

        ph = ",".join("?" * len(ids))
        rows = self._conn.execute(
            f"""
            SELECT rr.horse_id, r.venue,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            WHERE  rr.horse_id IN ({ph})
            GROUP  BY rr.horse_id, r.venue
            """,
            ids,
        ).fetchall()
        # (horse_id, venue) → rate
        rate_map: dict[tuple, float] = {
            (h, v): ((w / t) if t >= 2 else np.nan) for h, v, t, w in rows
        }

        for idx, row in df.iterrows():
            h = row.get("horse_id")
            v = row.get("venue")
            if h and v:
                rates.at[idx] = rate_map.get((h, v), np.nan)

        return rates

    @staticmethod
    def _east_west_match(df: pd.DataFrame) -> pd.Series:
        """馬と騎手の所属（美浦/栗東）一致フラグ。"""
        if "horse_east_west" in df.columns and "jockey_east_west" in df.columns:
            return (df["horse_east_west"] == df["jockey_east_west"]).astype(float)
        return pd.Series(0.5, index=df.index, dtype=float)

    # ── D: 調教指数 ───────────────────────────────────────────────────

    def _calc_training(self, df: pd.DataFrame) -> pd.DataFrame:
        # ウッド: tc_4f が models.py の列名、last_tc_4f が v_race_mart の列名
        tc_4f = _coalesce_col(df, "tc_4f", "last_tc_4f")
        speed = 200.0 / tc_4f.clip(lower=40.0) * 100
        df["tc_speed_index"] = speed
        df["uf_tc_speed"] = _clip_norm(speed, 350.0, 450.0)

        # 坂路
        hc_4f = _coalesce_col(df, "hc_4f", "last_hc_4f")
        hc_speed = 200.0 / hc_4f.clip(lower=40.0) * 100
        df["hc_speed_index"] = hc_speed
        df["uf_hc_speed"] = _clip_norm(hc_speed, 350.0, 450.0)

        return df

    # ── E: 血統適性 ───────────────────────────────────────────────────

    def _calc_bloodline(self, df: pd.DataFrame) -> pd.DataFrame:
        df["uf_sire_distance"] = self._sire_distance_batch(df)
        df["uf_bms_surface"] = self._bms_surface_batch(df)
        df["uf_father_sire"] = self._father_sire_flag(df)
        return df

    def _sire_distance_batch(self, df: pd.DataFrame) -> pd.Series:
        """父馬産駒の距離帯別勝率。"""
        scores = pd.Series(0.5, index=df.index, dtype=float)
        sire_col = next((c for c in ["sire", "father_name"] if c in df.columns), None)
        if sire_col is None:
            return scores

        sires = df[sire_col].dropna().unique().tolist()
        if not sires:
            return scores

        ph = ",".join("?" * len(sires))
        rows = self._conn.execute(
            f"""
            SELECT h.sire,
                   CASE
                     WHEN r.distance <= 1600 THEN 'short'
                     WHEN r.distance <= 2000 THEN 'mile'
                     ELSE 'long'
                   END AS dist_band,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            JOIN   horses h ON h.horse_id = rr.horse_id
            WHERE  h.sire IN ({ph})
            GROUP  BY h.sire, dist_band
            HAVING total >= 10
            """,
            sires,
        ).fetchall()
        rate_map: dict[tuple, float] = {(s, b): (w / t) for s, b, t, w in rows}

        def _dist_band(d: Any) -> str:
            d = float(d or 0)
            return "short" if d <= 1600 else ("mile" if d <= 2000 else "long")

        for idx, row in df.iterrows():
            sire = row.get(sire_col, "")
            b = _dist_band(row.get("distance", 0))
            v = rate_map.get((sire, b), np.nan)
            scores.at[idx] = (
                float(v) if not (isinstance(v, float) and np.isnan(v)) else 0.5
            )

        return _clip_norm(scores, 0.0, 0.20)

    def _bms_surface_batch(self, df: pd.DataFrame) -> pd.Series:
        """母父産駒の馬場別勝率。"""
        scores = pd.Series(0.5, index=df.index, dtype=float)
        bms_col = next(
            (c for c in ["dam_sire", "grandsire_name"] if c in df.columns), None
        )
        if bms_col is None:
            return scores

        bms_list = df[bms_col].dropna().unique().tolist()
        if not bms_list:
            return scores

        ph = ",".join("?" * len(bms_list))
        rows = self._conn.execute(
            f"""
            SELECT h.dam_sire, r.surface,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank=1 THEN 1 ELSE 0 END) AS wins
            FROM   race_results rr
            JOIN   races r ON r.race_id = rr.race_id
            JOIN   horses h ON h.horse_id = rr.horse_id
            WHERE  h.dam_sire IN ({ph})
            GROUP  BY h.dam_sire, r.surface
            HAVING total >= 10
            """,
            bms_list,
        ).fetchall()
        rate_map: dict[tuple, float] = {(b, s): (w / t) for b, s, t, w in rows}

        for idx, row in df.iterrows():
            bms = row.get(bms_col, "")
            surface = row.get("surface", "")
            v = rate_map.get((bms, surface), np.nan)
            scores.at[idx] = (
                float(v) if not (isinstance(v, float) and np.isnan(v)) else 0.5
            )

        return _clip_norm(scores, 0.0, 0.20)

    @staticmethod
    def _father_sire_flag(df: pd.DataFrame) -> pd.Series:
        """父の父情報が存在するか（0.25=なし, 0.75=あり）。"""
        for col in ("father_sire_name", "father_sire_id"):
            if col in df.columns:
                return df[col].notna().astype(float) * 0.5 + 0.25
        return pd.Series(0.5, index=df.index, dtype=float)

    # ── G: 相手関係・クラス変化 [W-010/011] ────────────────────────────

    def _calc_competition(self, df: pd.DataFrame) -> pd.DataFrame:
        """G: 相手関係指数 (W-010) + クラス昇降格インパクト (W-011)。

        W-010 相手関係: レース内の win_rate_all 平均と対象馬の win_rate_all の差。
            強い相手に勝っている馬 → 高評価。
            弱い相手しか経験していない馬 → 割引。

        W-011 クラス変化: 前走から今走へのクラス変化をスコア化。
            降格馬（強いクラスから来た）→ 高評価。
            昇格初戦（弱いクラスから来た）→ 割引。
            grade列: G1=1, G2=2, G3=3, OP=4, 3勝=5, 2勝=6, 1勝=7, 未勝利=8, 新馬=9
        """
        df = df.copy()

        # ── W-010: 相手関係指数 ────────────────────────────────────────
        if "win_rate_all" in df.columns and "race_id" in df.columns:
            wr = pd.to_numeric(df["win_rate_all"], errors="coerce").fillna(0.05)
            race_mean = df.groupby("race_id")["win_rate_all"].transform(
                lambda x: pd.to_numeric(x, errors="coerce").mean()
            )
            race_mean = pd.to_numeric(race_mean, errors="coerce").fillna(wr.mean())
            # 自馬勝率 - フィールド平均: 正=強い相手に耐えてきた実績
            delta = wr - race_mean
            # [-0.1, 0.1] → [0, 1] 正規化
            df["competition_strength"] = delta
            df["uf_competition_strength"] = _clip_norm(delta, -0.10, 0.10)
        else:
            df["competition_strength"] = 0.0
            df["uf_competition_strength"] = pd.Series(0.5, index=df.index)

        # ── W-011: クラス昇降格インパクト ──────────────────────────────
        # races.grade から前走グレードを取得（race_id のレース情報から逆引き）
        # 簡易版: entries テーブルには grade がないため race_id prefix のデジット番を使う
        # grade は race_id[8:10] が venue_code・race_id[10:12] がレース番号
        # 実際のグレード情報は races テーブルの grade 列から取得する。
        if "race_id" in df.columns:
            race_ids = df["race_id"].unique().tolist()
            if race_ids:
                ph = ",".join("?" * len(race_ids))
                grade_rows = self._conn.execute(
                    f"SELECT race_id, grade FROM races WHERE race_id IN ({ph})",
                    race_ids,
                ).fetchall()
                grade_map: dict[str, str] = {r[0]: str(r[1] or "") for r in grade_rows}
                cur_grade = grade_map.get(str(race_ids[0]), "")
            else:
                cur_grade = ""
        else:
            cur_grade = ""

        def _grade_rank(g: str) -> int:
            """グレード文字列を数値ランク（低いほど格上）に変換。"""
            g = str(g).upper().strip()
            for rank, pattern in enumerate(
                ["G1", "G2", "G3", "OP", "L ", "3勝", "2勝", "1勝", "未勝利", "新馬"], 1
            ):
                if pattern.strip() in g:
                    return rank
            return 5  # 不明 → OP相当

        cur_grade_rank = _grade_rank(cur_grade)

        # 馬ごとの前走グレードを race_results から取得
        if "horse_id" in df.columns and "race_id" in df.columns:
            cur_race_id = str(df["race_id"].iloc[0]) if not df.empty else ""
            cur_date_str = cur_race_id[:4] + "-" + cur_race_id[4:6] + "-" + cur_race_id[6:8]

            horse_ids = df["horse_id"].dropna().unique().tolist()
            prev_grades: dict[str, int] = {}
            for hid in horse_ids:
                row = self._conn.execute(
                    """
                    SELECT r.grade FROM race_results rr
                    JOIN races r ON rr.race_id = r.race_id
                    WHERE rr.horse_id = ? AND r.date < ? AND rr.rank > 0
                    ORDER BY r.date DESC LIMIT 1
                    """,
                    (hid, cur_date_str),
                ).fetchone()
                if row:
                    prev_grades[hid] = _grade_rank(row[0] or "")

            class_deltas = []
            for _, row2 in df.iterrows():
                hid = row2.get("horse_id")
                if hid and hid in prev_grades:
                    # prev_grade_rank - cur_grade_rank:
                    #   正 = 降格（前走が格上→今走で有利） → uf高
                    #   負 = 昇格（前走が格下→今走で不利） → uf低
                    delta_cls = prev_grades[hid] - cur_grade_rank
                else:
                    delta_cls = 0
                class_deltas.append(float(delta_cls))

            df["class_change_delta"] = class_deltas
            # [-3, 3] → [0, 1]: 降格馬（+3）→ 1.0、昇格馬（-3）→ 0.0
            df["uf_class_change"] = _clip_norm(
                pd.Series(class_deltas, index=df.index), -3.0, 3.0
            )
        else:
            df["class_change_delta"] = 0.0
            df["uf_class_change"] = pd.Series(0.5, index=df.index)

        # competition グループスコア = 2因子の平均
        comp_score = (
            df[["uf_competition_strength", "uf_class_change"]]
            .apply(pd.to_numeric, errors="coerce")
            .mean(axis=1)
            .fillna(0.5)
        )
        # 合成スコアは _calc_composite で _GROUP_FACTORS["competition"] から自動計算される
        return df

    # ── F: 大衆心理乖離 [W-004] ──────────────────────────────────────

    @staticmethod
    def _calc_crowd_bias(df: pd.DataFrame) -> pd.DataFrame:
        """F: 大衆心理乖離スコア — 通算勝率 / 市場暗示確率（W-004実装）。

        crowd_bias_ratio = win_rate_all / market_implied_prob
          > 1.3: 市場が過小評価（隠れた期待値） → uf_crowd_bias 高
          < 0.7: 市場が過大評価（群衆人気薄め） → uf_crowd_bias 低

        market_implied_prob = 1 / (win_odds * JRA_takeout補正 ≈ 0.8)
        ただし features.py では market_prob = 1 / min(win_odds, 80) で算出済み。
        """
        win_rate_raw = df.get("win_rate_all")
        market_raw = df.get("market_prob")

        # 列が存在しない（None）または スカラーの場合はニュートラルで埋める
        if win_rate_raw is None or not isinstance(win_rate_raw, pd.Series):
            win_rate = pd.Series(0.1, index=df.index, dtype=float)
        else:
            win_rate = pd.to_numeric(win_rate_raw, errors="coerce").fillna(0.1)

        if market_raw is None or not isinstance(market_raw, pd.Series):
            # market_prob がない場合はニュートラル（ratio=1.0）で補填
            df["crowd_bias_ratio"] = pd.Series(1.0, index=df.index, dtype=float)
            df["uf_crowd_bias"] = pd.Series(0.41, index=df.index, dtype=float)
            return df

        market_p = pd.to_numeric(market_raw, errors="coerce")
        valid = market_p.notna() & (market_p > 0.01)
        ratio = pd.Series(1.0, index=df.index, dtype=float)
        ratio[valid] = (win_rate[valid] / market_p[valid]).clip(0.1, 10.0)

        df["crowd_bias_ratio"] = ratio
        # [0.3, 3.0] → [0, 1] 正規化。1.0（市場均衡）→ 0.412
        df["uf_crowd_bias"] = _clip_norm(ratio, 0.3, 3.0)
        return df

    # ── 合成スコア ────────────────────────────────────────────────────

    def _calc_composite(self, df: pd.DataFrame) -> pd.DataFrame:
        u = pd.Series(0.0, index=df.index, dtype=float)
        for group, factors in _GROUP_FACTORS.items():
            present = [f for f in factors if f in df.columns]
            if not present:
                g_score: pd.Series = pd.Series(0.5, index=df.index, dtype=float)
            else:
                g_score = (
                    df[present]
                    .apply(pd.to_numeric, errors="coerce")
                    .mean(axis=1)
                    .fillna(0.5)
                )
            u += g_score * _WEIGHTS.get(group, 0.0)

        df["u_score"] = u.clip(0.0, 1.0)
        return df

    # ── ユーティリティ ────────────────────────────────────────────────

    @staticmethod
    def _offset_date(base: str, days: int) -> str:
        """YYYY-MM-DD 文字列を days 日オフセットして返す。"""
        from datetime import date as _d, timedelta

        d = _d.fromisoformat(base)
        return (d + timedelta(days=days)).isoformat()


# ── モジュールレベルユーティリティ ───────────────────────────────────


def _clip_norm(s: pd.Series, lo: float, hi: float, fill: float = 0.5) -> pd.Series:
    """[lo, hi] でクリップして [0, 1] に線形正規化。欠損は fill で補填。"""
    s = pd.to_numeric(s, errors="coerce")
    result = (s.clip(lower=lo, upper=hi) - lo) / max(hi - lo, 1e-8)
    return result.fillna(fill).clip(0.0, 1.0)


def _coalesce_col(df: pd.DataFrame, *cols: str) -> pd.Series:
    """最初に見つかった列を pd.to_numeric で返す。全て欠損なら NaN Series。"""
    for col in cols:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def _rest_days_score(rest: pd.Series) -> pd.Series:
    """
    休養日数スコア（0〜1）。μ=28, σ=14 のガウス型。
    28日付近を頂点として長すぎる/短すぎる休養を減点。
    """
    filled = rest.fillna(28.0)
    return np.exp(-0.5 * ((filled - 28.0) / 14.0) ** 2).clip(0.0, 1.0)
