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
from collections import defaultdict
from datetime import datetime, timedelta
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

# 馬場状態エンコーディング
_CONDITION_CODE: dict[str, int] = {"良": 0, "稍重": 1, "重": 2, "不良": 3}


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
        self._jockey_map: dict[str, int] = {}
        self._trainer_map: dict[str, int] = {}

    # ── パブリック API ──────────────────────────────────────────

    def build_race_features_for_simulate(self, race_id: str) -> pd.DataFrame:
        """
        過去レース（race_results）から擬似出馬表を構築して特徴量 DataFrame を返す。

        **リーク防止の原則**
        race_results から以下の「レース終了後にしか判明しない情報」を除外する:
          - rank        (着順)
          - finish_time (タイム)
          - margin      (着差)

        horse_stats の計算では exclude_race_id=race_id を渡し、
        シミュレーション対象レース自身の rank が過去成績に混入しないようにする。

        horse_number は popularity 昇順（1番人気→馬番1）で付与する。
        """
        race_row = self._conn.execute(
            "SELECT distance, surface, venue, condition, race_number, date FROM races WHERE race_id = ?",
            (race_id,),
        ).fetchone()

        if race_row is None:
            raise ValueError(f"race_id が DB に存在しません: {race_id!r}")

        distance, surface, venue, condition, race_number, race_date = race_row
        dist_band = _distance_band(distance)

        # 当日バイアス（current_race_number より前の確定済みレースから算出）
        bias = self._get_today_bias(race_date, venue, race_number)

        # オッズ時系列特徴量（シミュレーション時は realtime_odds が未記録のため全 None）
        # スキーマを prerace と統一するために呼び出す（LightGBM は欠損として扱う）
        odds_trend = self._get_odds_trend(race_id)

        # race_results から安全なフィールドのみ取得（rank/finish_time/margin は取らない）
        # 騎手・調教師は名前をそのまま取得してセッション内ラベルエンコードする
        # （jockeys/trainers マスタが未投入の場合も名前ベースで機能する）
        rows = self._conn.execute(
            """
            SELECT
                rr.horse_id,
                rr.horse_name,
                rr.sex_age,
                rr.weight_carried,
                rr.jockey,
                rr.horse_weight,
                rr.win_odds,
                rr.popularity,
                rr.gate_number,
                rr.horse_weight_diff,
                COALESCE(rr.jockey,  '') AS jockey_key,
                COALESCE(rr.trainer, '') AS trainer_key
            FROM race_results rr
            WHERE rr.race_id = ?
            ORDER BY
                CASE WHEN rr.popularity IS NULL THEN 1 ELSE 0 END,
                rr.popularity
            """,
            (race_id,),
        ).fetchall()

        if not rows:
            logger.warning("race_results が 0 件: race_id=%s", race_id)
            return pd.DataFrame()

        records = []
        for sim_num, (horse_id, horse_name, sex_age,
                       weight_carried, jockey, horse_weight,
                       win_odds, popularity,
                       gate_number, horse_weight_diff,
                       jockey_key, trainer_key) in enumerate(rows, start=1):
            # リーク防止: 統計からシミュレーション対象レース自身を除外
            stats = self._get_horse_stats(
                horse_id, surface, distance, exclude_race_id=race_id
            )
            # 調教特徴量: レース当日より前の最新調教データを参照（リーク排除済み）
            training = self._get_training_stats(horse_id, race_date)

            # オッズ→市場確率変換（アンチパターン: 生オッズ直接使用禁止）
            raw_odds = float(win_odds) if win_odds else None
            market_prob = (1.0 / min(raw_odds, 80.0)) if raw_odds else None

            records.append({
                "horse_number":           sim_num,
                "horse_id":               horse_id,
                "horse_name":             horse_name,
                "weight_carried":         weight_carried,
                "horse_weight":           horse_weight,
                "win_odds":               win_odds,
                "popularity":             popularity,
                "win_rate_all":           stats["win_rate_all"],
                "win_rate_surface":       stats["win_rate_surface"],
                "win_rate_distance_band": stats["win_rate_distance_band"],
                "recent_rank_mean":       stats["recent_rank_mean"],
                "surface_code":           _SURFACE_CODE.get(surface, -1),
                "sex_code":               _SEX_CODE.get(_parse_sex(sex_age or ""), -1),
                "venue_encoded":          _VENUE_CODE.get(venue, len(_VENUE_CODE)),
                "sire_encoded":           self._encode_sire(self._get_sire(horse_id)),
                "distance":               distance,
                "dist_band":              dist_band,
                # ── 追加特徴量 ────────────────────────────────────
                "horse_weight_diff":      horse_weight_diff,   # 前走比体重増減
                "gate_number":            gate_number,          # 枠番
                "condition_code":         _CONDITION_CODE.get(condition or "", -1),  # 馬場状態
                "market_prob":            market_prob,          # 市場確率 1/odds.clip(max=80)
                "race_number":            race_number,          # レース番号
                # ── 人的要素特徴量 ───────────────────────────────
                "jockey_code_encoded":    self._encode_jockey(jockey_key),    # 騎手名エンコード
                "trainer_code_encoded":   self._encode_trainer(trainer_key),  # 調教師名エンコード
                # ── 調教特徴量（WOOD:TC / WOOD:HC） ─────────────
                "tc_4f":            training["tc_4f"],          # ウッド直近4Fタイム（秒）
                "tc_lap":           training["tc_lap"],         # ウッド直近ラスト1Fタイム
                "tc_accel_flag":    training["tc_accel_flag"],  # ウッド加速ラップ (1=好調)
                "tc_4f_diff":       training["tc_4f_diff"],     # ウッド前回比タイム差（負=好転）
                "hc_4f":            training["hc_4f"],          # 坂路直近4Fタイム（秒）
                "hc_lap":           training["hc_lap"],         # 坂路直近ラスト1Fタイム
                "hc_accel_flag":    training["hc_accel_flag"],  # 坂路加速ラップ (1=好調)
                "hc_4f_diff":       training["hc_4f_diff"],     # 坂路前回比タイム差（負=好転）
                # ── 当日バイアス特徴量 ────────────────────────────
                # current_race_number より前の確定済みレースから算出（リーク排除済み）
                "today_inner_bias": bias["today_inner_bias"],   # 内枠勝率 - 外枠勝率
                "today_front_bias": bias["today_front_bias"],   # 当日・人気馬勝率（先行バイアス代理）
                "today_race_count": bias["today_race_count"],   # 集計レース数（信頼度）
                "today_gate_match": (                           # バイアス×枠番の相性スコア
                    bias["today_inner_bias"] * (1.0 if (gate_number or 0) <= 4 else -1.0)
                    if bias["today_inner_bias"] is not None else None
                ),
                # ── オッズ時系列特徴量（大口投票シグナル） ───────────
                # シミュレーション時は realtime_odds が未記録のため全 None。
                # prerace 時に複数スナップショットが記録されていれば有効。
                "odds_vs_morning": odds_trend.get(sim_num, {}).get("odds_vs_morning"),
                "odds_velocity":   odds_trend.get(sim_num, {}).get("odds_velocity"),
                # 識別子（モデル学習には使わない）
                "sex_age":                sex_age,
                "jockey":                 jockey,
            })

        df = pd.DataFrame(records)
        df = self._add_intra_race_features(df)
        logger.info(
            "[SIMULATE] 特徴量生成 race_id=%s: %d 頭 × %d 特徴量 (リーク除外済み)",
            race_id, len(df), df.shape[1],
        )
        return df

    def build_race_features(self, race_id: str) -> pd.DataFrame:
        """
        指定レースの出馬表を基に特徴量 DataFrame を生成して返す。

        entries テーブル（netkeiba スクレイプ済み）から全特徴量を生成する。
        simulate 版と同じ特徴量セットを出力するため、欠損値は -1 で埋められる。

        Args:
            race_id: netkeiba の race_id

        Returns:
            各行が1頭、各列が特徴量の DataFrame。
            horse_number / horse_name / horse_id 列は識別用として保持。
        """
        race_row = self._conn.execute(
            "SELECT distance, surface, venue, condition, race_number, date FROM races WHERE race_id = ?",
            (race_id,),
        ).fetchone()

        if race_row is None:
            raise ValueError(f"race_id が DB に存在しません: {race_id!r}")

        distance, surface, venue, condition, race_number, race_date = race_row
        dist_band = _distance_band(distance or 0)

        # 当日バイアス（current_race_number より前の確定済みレースから算出）
        bias = self._get_today_bias(race_date or "", venue or "", race_number or 0)

        # オッズ時系列特徴量（prerace 時に realtime_odds が複数スナップショットある場合に有効）
        odds_trend = self._get_odds_trend(race_id)

        entries = self._conn.execute(
            """
            SELECT horse_number, horse_id, horse_name, sex_age,
                   weight_carried, horse_weight, gate_number,
                   horse_weight_diff, jockey, trainer
            FROM entries
            WHERE race_id = ?
            ORDER BY horse_number
            """,
            (race_id,),
        ).fetchall()

        # 最新オッズを馬番で引く
        odds_map = self._latest_odds_map(race_id)

        records = []
        for (horse_number, horse_id, horse_name, sex_age,
             weight_carried, horse_weight, gate_number,
             horse_weight_diff, jockey, trainer) in entries:

            stats = self._get_horse_stats(horse_id, surface or "", distance or 0)
            training = self._get_training_stats(horse_id, race_date or "")
            odds = odds_map.get(horse_number, {})

            jockey_key  = jockey  or ""
            trainer_key = trainer or ""

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
                "surface_code":   _SURFACE_CODE.get(surface or "", -1),
                "sex_code":       _SEX_CODE.get(_parse_sex(sex_age or ""), -1),
                "venue_encoded":  _VENUE_CODE.get(venue or "", len(_VENUE_CODE)),
                "sire_encoded":   self._encode_sire(self._get_sire(horse_id)),
                # レース情報
                "distance":         distance or 0,
                "dist_band":        dist_band,
                "horse_weight_diff": horse_weight_diff,
                "gate_number":       gate_number,
                "condition_code":    _CONDITION_CODE.get(condition or "", -1),
                "race_number":       race_number or 0,
                # 人的要素特徴量
                "jockey_code_encoded":  self._encode_jockey(jockey_key),
                "trainer_code_encoded": self._encode_trainer(trainer_key),
                # 調教特徴量（WOOD:TC / WOOD:HC）
                "tc_4f":         training["tc_4f"],
                "tc_lap":        training["tc_lap"],
                "tc_accel_flag": training["tc_accel_flag"],
                "tc_4f_diff":    training["tc_4f_diff"],
                "hc_4f":         training["hc_4f"],
                "hc_lap":        training["hc_lap"],
                "hc_accel_flag": training["hc_accel_flag"],
                "hc_4f_diff":    training["hc_4f_diff"],
                # ── 当日バイアス特徴量 ────────────────────────────────
                # race_number より前の確定済みレースから算出（リーク排除済み）
                "today_inner_bias": bias["today_inner_bias"],
                "today_front_bias": bias["today_front_bias"],
                "today_race_count": bias["today_race_count"],
                "today_gate_match": (
                    bias["today_inner_bias"] * (1.0 if (gate_number or 0) <= 4 else -1.0)
                    if bias["today_inner_bias"] is not None else None
                ),
                # ── オッズ時系列特徴量（大口投票シグナル） ───────────
                # realtime_odds に複数スナップショットある場合のみ有効。
                # スナップショットが1点以下の場合は None（LightGBM が欠損として扱う）。
                "odds_vs_morning": odds_trend.get(horse_number, {}).get("odds_vs_morning"),
                "odds_velocity":   odds_trend.get(horse_number, {}).get("odds_velocity"),
                # 識別子
                "sex_age": sex_age,
                "jockey":  jockey,
            })

        df = pd.DataFrame(records)
        df = self._add_intra_race_features(df)
        logger.info(
            "特徴量生成 race_id=%s: %d 頭 × %d 特徴量",
            race_id, len(df), df.shape[1],
        )
        return df

    # ── 内部メソッド ───────────────────────────────────────────

    def _get_today_bias(
        self,
        race_date: str,
        venue: str,
        current_race_number: int,
    ) -> dict[str, float | None]:
        """
        当日の確定済みレース結果から内外・先行バイアスを算出する。

        **リーク防止**: current_race_number より前のレースのみ集計。
        当日の完了レースが 0 件の場合は全て None を返す（1Rなど）。

        内外バイアスの算出ロジック:
          - 内枠(gate 1-4)に出走した馬のうち1着になった割合
          - 外枠(gate 5-8)に出走した馬のうち1着になった割合
          - today_inner_bias = 内枠勝率 - 外枠勝率
            → 正値: 内枠有利（芝・小回りコースで多い）
            → 負値: 外枠有利（外差しコース・長距離で稀に発生）

        先行バイアスの代理変数:
          - today_front_bias = 当日の人気1-3位馬の勝率
          - 高い → 本命決着が続く → 展開が読みやすい・先行有利傾向
          - 低い → 波乱続出 → ペース乱調・差し・追い込み台頭傾向

        Args:
            race_date:            "YYYY/MM/DD" 形式の日付
            venue:                開催場（例: "東京"）
            current_race_number:  この特徴量を生成するレースの番号

        Returns:
            {
              "today_inner_bias":  float | None  # 正=内枠有利、負=外枠有利
              "today_front_bias":  float | None  # 人気馬勝率（0.0〜1.0）
              "today_race_count":  float | None  # 集計対象レース数（信頼度）
            }
        """
        _null: dict[str, float | None] = {
            "today_inner_bias": None,
            "today_front_bias": None,
            "today_race_count": None,
        }

        row = self._conn.execute(
            """
            SELECT
              SUM(CASE WHEN rr.gate_number BETWEEN 1 AND 4 AND rr.rank = 1 THEN 1 ELSE 0 END)
                  AS inner_wins,
              SUM(CASE WHEN rr.gate_number BETWEEN 1 AND 4 THEN 1 ELSE 0 END)
                  AS inner_horses,
              SUM(CASE WHEN rr.gate_number BETWEEN 5 AND 8 AND rr.rank = 1 THEN 1 ELSE 0 END)
                  AS outer_wins,
              SUM(CASE WHEN rr.gate_number BETWEEN 5 AND 8 THEN 1 ELSE 0 END)
                  AS outer_horses,
              SUM(CASE WHEN rr.popularity BETWEEN 1 AND 3 AND rr.rank = 1 THEN 1 ELSE 0 END)
                  AS fav_wins,
              COUNT(DISTINCT rr.race_id)
                  AS completed_races
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.date         = ?
              AND r.venue        = ?
              AND r.race_number  < ?
              AND rr.rank        IS NOT NULL
            """,
            (race_date, venue, current_race_number),
        ).fetchone()

        if row is None or (row[5] or 0) == 0:
            return _null

        inner_wins, inner_horses, outer_wins, outer_horses, fav_wins, completed_races = row

        inner_rate: float | None = (inner_wins / inner_horses) if (inner_horses or 0) > 0 else None
        outer_rate: float | None = (outer_wins / outer_horses) if (outer_horses or 0) > 0 else None

        if inner_rate is not None and outer_rate is not None:
            today_inner_bias: float | None = inner_rate - outer_rate
        elif inner_rate is not None:
            today_inner_bias = inner_rate - 0.5
        elif outer_rate is not None:
            today_inner_bias = 0.5 - outer_rate
        else:
            today_inner_bias = None

        today_front_bias: float | None = (
            (fav_wins or 0) / completed_races if completed_races > 0 else None
        )

        return {
            "today_inner_bias": today_inner_bias,
            "today_front_bias": today_front_bias,
            "today_race_count": float(completed_races),
        }

    def _add_intra_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        レース内相対特徴量（順位・偏差）を追加する。

        全馬のデータが揃った DataFrame に対して groupby なしで適用する
        （呼び出し元が 1レース分の DataFrame を渡す前提）。

        - rank:   1 = レース内最良（勝率高・着順低・調教タイム速）
        - zscore: (値 - mean) / std。速さ系は符号反転して高いほど良い方向に揃える。
        値がすべて NaN の場合は NaN のまま残す（LightGBM が欠損として扱う）。
        """
        if df.empty:
            return df

        df = df.copy()

        def _rank_desc(col: str) -> None:
            """高いほど良い特徴量：rank=1 が最高値。全 NaN の場合は NaN 列を追加。"""
            if col not in df.columns or df[col].isna().all():
                # 列が存在しない or 全欠損 → NaN 列を追加してモデルが欠損扱いにできるよう統一
                df[f"{col}_rank"]   = float("nan")
                df[f"{col}_zscore"] = float("nan")
                return
            df[f"{col}_rank"]   = df[col].rank(ascending=False, na_option="bottom")
            std = df[col].std()
            df[f"{col}_zscore"] = ((df[col] - df[col].mean()) / std) if std and std > 0 else 0.0

        def _rank_asc_inv(col: str) -> None:
            """低いほど良い特徴量：rank=1 が最小値。zscore は符号反転で高=良に統一。全 NaN の場合は NaN 列を追加。"""
            if col not in df.columns or df[col].isna().all():
                df[f"{col}_rank"]   = float("nan")
                df[f"{col}_zscore"] = float("nan")
                return
            df[f"{col}_rank"]   = df[col].rank(ascending=True, na_option="bottom")
            std = df[col].std()
            df[f"{col}_zscore"] = (-(df[col] - df[col].mean()) / std) if std and std > 0 else 0.0

        # 高いほど良い特徴量
        _rank_desc("win_rate_all")
        _rank_desc("win_rate_surface")
        _rank_desc("win_rate_distance_band")

        # 低いほど良い特徴量（直近着順平均、調教タイム）
        _rank_asc_inv("recent_rank_mean")
        _rank_asc_inv("tc_4f")
        _rank_asc_inv("tc_3f") if "tc_3f" in df.columns else None
        _rank_asc_inv("hc_4f") if "hc_4f" in df.columns else None

        return df

    def _get_horse_stats(
        self,
        horse_id: str | None,
        surface: str,
        distance: int,
        *,
        exclude_race_id: str | None = None,
    ) -> dict[str, float | None]:
        """
        horses / race_results テーブルから馬の過去成績指標を算出する。

        Args:
            exclude_race_id: このレース ID を統計から除外する（シミュレーション時に
                             対象レース自身の着順がリークしないよう指定する）。

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

        # exclude_race_id が指定された場合、そのレースを除外する句を追加
        excl_clause = "AND rr.race_id != ?" if exclude_race_id else ""
        excl_param  = (exclude_race_id,) if exclude_race_id else ()

        # 全成績
        row = self._conn.execute(
            f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            WHERE rr.horse_id = ? AND rr.rank IS NOT NULL
            {excl_clause}
            """,
            (horse_id, *excl_param),
        ).fetchone()
        total, wins = row if row else (0, 0)
        win_rate_all = (wins / total) if total else None

        # 同馬場
        row_sf = self._conn.execute(
            f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND r.surface = ? AND rr.rank IS NOT NULL
            {excl_clause}
            """,
            (horse_id, surface, *excl_param),
        ).fetchone()
        total_sf, wins_sf = row_sf if row_sf else (0, 0)
        win_rate_surface = (wins_sf / total_sf) if total_sf else None

        # 同距離帯（距離の下限〜上限で range に変換）
        lo, hi = next(
            (lo, hi) for lo, hi, label in _DISTANCE_BANDS if label == dist_band
        )
        row_db = self._conn.execute(
            f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ?
              AND r.distance >= ? AND r.distance < ?
              AND rr.rank IS NOT NULL
            {excl_clause}
            """,
            (horse_id, lo, hi, *excl_param),
        ).fetchone()
        total_db, wins_db = row_db if row_db else (0, 0)
        win_rate_distance_band = (wins_db / total_db) if total_db else None

        # 直近5走の平均着順
        rows_recent = self._conn.execute(
            f"""
            SELECT rr.rank
            FROM race_results rr
            JOIN  races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND rr.rank IS NOT NULL
            {excl_clause}
            ORDER BY r.date DESC
            LIMIT 5
            """,
            (horse_id, *excl_param),
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

    def _get_training_stats(
        self,
        horse_id: str | None,
        race_date: str,
    ) -> dict[str, float | None]:
        """
        レース当日より前の直近調教データを取得して特徴量を返す。

        **リーク排除**: `training_date < race_date` のみ参照。
        データが存在しない場合は None を返す（モデル側で fillna(-1) される）。

        **加速ラップの解釈**
        - `4F合計タイム / 4` = 平均1Fペース
        - `ラスト1Fタイム < 平均1Fペース` → 最後で加速 → 好調サイン (flag=1)

        **前回比タイム差**
        - 直近2回の調教を取得し `最新4F - 前回4F` を計算
        - 負値 = タイムが縮まった (好転)、正値 = 遅くなった (悪化)

        Returns:
            {
              "tc_4f":         ウッド直近4Fタイム (秒)
              "tc_lap":        ウッド直近ラスト1Fタイム (秒)
              "tc_accel_flag": 加速ラップフラグ (1=ラスト加速, 0=失速, None=データなし)
              "tc_4f_diff":    ウッド前回比タイム差 (秒, 負=好転)
              "hc_4f":         坂路直近4Fタイム (秒)
              "hc_lap":        坂路直近ラスト1Fタイム (秒)
              "hc_accel_flag": 坂路加速ラップフラグ
              "hc_4f_diff":    坂路前回比タイム差 (秒, 負=好転)
            }
        """
        _null: dict[str, float | None] = {
            "tc_4f":         None,
            "tc_lap":        None,
            "tc_accel_flag": None,
            "tc_4f_diff":    None,
            "hc_4f":         None,
            "hc_lap":        None,
            "hc_accel_flag": None,
            "hc_4f_diff":    None,
        }
        if not horse_id or not race_date:
            return _null

        # race_results.horse_id は YYYY+SSSSSS（4桁年+6桁連番）の10桁数値文字列。
        # training_times.horse_id は D+YYYY+SSSSS（1桁+4桁年+5桁連番）の10桁。
        # 共通キー(9桁) = substr(horse_id,1,4)||substr(horse_id,5,5)
        #               = substr(tc.horse_id,2,9)
        if len(horse_id) != 10 or not horse_id.isdigit():
            return _null
        tc_key = horse_id[:4] + horse_id[4:9]   # "YYYY" + 最初5桁の連番

        result = dict(_null)

        # ── ウッド調教 (training_times) ──────────────────────────
        tc_rows = self._conn.execute(
            """
            SELECT time_4f, lap_time
            FROM   training_times
            WHERE  substr(horse_id,2,9) = ?
            AND    training_date < ?
            AND    time_4f       IS NOT NULL
            ORDER  BY training_date DESC
            LIMIT  2
            """,
            (tc_key, race_date),
        ).fetchall()

        if tc_rows:
            tc_4f, tc_lap = tc_rows[0]
            result["tc_4f"]  = tc_4f
            result["tc_lap"] = tc_lap
            # 加速ラップ: ラスト1F < 4F合計÷4 なら加速
            if tc_4f and tc_lap:
                result["tc_accel_flag"] = float(tc_lap < tc_4f / 4.0)
            # 前回比タイム差
            if len(tc_rows) >= 2 and tc_rows[1][0] is not None:
                result["tc_4f_diff"] = tc_4f - tc_rows[1][0]

        # ── 坂路調教 (training_hillwork) ─────────────────────────
        hc_rows = self._conn.execute(
            """
            SELECT time_4f, lap_time
            FROM   training_hillwork
            WHERE  substr(horse_id,2,9) = ?
            AND    training_date < ?
            AND    time_4f       IS NOT NULL
            ORDER  BY training_date DESC
            LIMIT  2
            """,
            (tc_key, race_date),
        ).fetchall()

        if hc_rows:
            hc_4f, hc_lap = hc_rows[0]
            result["hc_4f"]  = hc_4f
            result["hc_lap"] = hc_lap
            # 加速ラップ
            if hc_4f and hc_lap:
                result["hc_accel_flag"] = float(hc_lap < hc_4f / 4.0)
            # 前回比タイム差
            if len(hc_rows) >= 2 and hc_rows[1][0] is not None:
                result["hc_4f_diff"] = hc_4f - hc_rows[1][0]

        return result

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

    def _encode_jockey(self, jockey_key: str | None) -> int:
        """
        騎手名（または騎手コード）をラベルエンコードする。
        jockeys マスタが未投入の場合は名前を直接使用する。
        空文字・None は -1 を返す。
        """
        if not jockey_key:
            return -1
        if jockey_key not in self._jockey_map:
            self._jockey_map[jockey_key] = len(self._jockey_map)
        return self._jockey_map[jockey_key]

    def _encode_trainer(self, trainer_key: str | None) -> int:
        """
        調教師名（または調教師コード）をラベルエンコードする。
        trainers マスタが未投入の場合は名前を直接使用する。
        空文字・None は -1 を返す。
        """
        if not trainer_key:
            return -1
        if trainer_key not in self._trainer_map:
            self._trainer_map[trainer_key] = len(self._trainer_map)
        return self._trainer_map[trainer_key]

    def _get_odds_trend(
        self,
        race_id: str,
    ) -> dict[int, dict[str, float | None]]:
        """
        realtime_odds テーブルの時系列から馬別のオッズ変動特徴量を算出する。

        **大口投票シグナルの検知ロジック**

        odds_vs_morning（朝一比率）:
          - latest_odds / morning_odds で算出
          - 1.0 未満: 直前が朝一より低い = 人気上昇（大口が入った可能性）
          - 1.0 超:   直前が朝一より高い = 人気低下（嫌われている）
          - 例: 朝一 10.0 → 直前 5.0 → odds_vs_morning = 0.50

        odds_velocity（下落速度、オッズ/分）:
          - (past_odds - latest_odds) / elapsed_minutes で算出
          - 正値: 直近1時間でオッズが下落中 = 資金流入が加速している
          - 負値: 直近1時間でオッズが上昇中 = 資金が抜けている
          - 約0: 変動なし

        データが不足している場合（スナップショットが1点以下等）は None を返す。
        主に prerace_pipeline で realtime_odds が複数時点で記録されている場合に有効。

        Returns:
            {horse_number: {
                "odds_vs_morning": float | None,
                "odds_velocity":   float | None,
            }}
        """
        rows = self._conn.execute(
            """
            SELECT horse_number, win_odds, recorded_at
            FROM   realtime_odds
            WHERE  race_id  = ?
              AND  win_odds IS NOT NULL
            ORDER  BY horse_number, recorded_at ASC
            """,
            (race_id,),
        ).fetchall()

        if not rows:
            return {}

        # 馬番別に時系列を構築: {horse_num: [(recorded_at_str, win_odds), ...]}
        horse_ts: dict[int, list[tuple[str, float]]] = defaultdict(list)
        for horse_num, odds, rec_at in rows:
            horse_ts[horse_num].append((rec_at, float(odds)))

        result: dict[int, dict[str, float | None]] = {}

        for horse_num, ts in horse_ts.items():
            morning_odds = ts[0][1]   # 最初に記録されたオッズ（朝一）
            latest_odds  = ts[-1][1]  # 最新オッズ
            latest_str   = ts[-1][0]  # 最新 recorded_at 文字列

            # ── 朝一比率 ──────────────────────────────────────────
            odds_vs_morning: float | None = None
            if morning_odds > 0:
                odds_vs_morning = latest_odds / morning_odds

            # ── 直近1時間の下落速度 ───────────────────────────────
            odds_velocity: float | None = None
            if len(ts) >= 2:
                try:
                    t_latest = datetime.fromisoformat(latest_str)
                    cutoff   = t_latest - timedelta(minutes=60)

                    # cutoff 以前の最新スナップショットを探す
                    past: tuple[str, float] | None = None
                    for t_str, o in ts[:-1]:   # 最新1点は除外
                        try:
                            if datetime.fromisoformat(t_str) <= cutoff:
                                past = (t_str, o)
                        except (ValueError, TypeError):
                            continue

                    if past is not None:
                        t_past = datetime.fromisoformat(past[0])
                        elapsed_min = (t_latest - t_past).total_seconds() / 60.0
                        if elapsed_min > 0:
                            # 正値 = 過去より現在が低い = 下落中 = 資金流入
                            odds_velocity = (past[1] - latest_odds) / elapsed_min
                except (ValueError, TypeError):
                    pass

            result[horse_num] = {
                "odds_vs_morning": odds_vs_morning,
                "odds_velocity":   odds_velocity,
            }

        return result

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
