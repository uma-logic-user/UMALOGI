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
import pickle
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# 学習時に保存した sire_map を推論時に自動ロードするためのパス
_SIRE_MAP_PKL = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "models"
    / "cascade"
    / "label_encoders.pkl"
)

# 距離バンドの境界（m）
_DISTANCE_BANDS = [
    (0, 1400, "sprint"),
    (1400, 1800, "mile"),
    (1800, 2200, "intermediate"),
    (2200, 9999, "long"),
]

# 固定カテゴリ辞書（未知値は -1 にフォールバック）
_SURFACE_CODE: dict[str, int] = {"芝": 0, "ダート": 1, "障害": 2}
_SEX_CODE: dict[str, int] = {"牡": 0, "牝": 1, "セ": 2}

# 会場エンコーディング（JRA 10 場 + 地方/海外）
_VENUE_CODE: dict[str, int] = {
    "札幌": 0,
    "函館": 1,
    "福島": 2,
    "新潟": 3,
    "東京": 4,
    "中山": 5,
    "中京": 6,
    "京都": 7,
    "阪神": 8,
    "小倉": 9,
}

# 馬場状態エンコーディング
_CONDITION_CODE: dict[str, int] = {"良": 0, "稍重": 1, "重": 2, "不良": 3}


def _distance_band(distance: int) -> str:
    """距離（m）を距離帯ラベルに変換する。

    Args:
        distance: レース距離（メートル）。

    Returns:
        距離帯ラベル文字列（"sprint" / "mile" / "intermediate" / "long"）。
    """
    for lo, hi, label in _DISTANCE_BANDS:
        if lo <= distance < hi:
            return label
    return "long"


def _parse_sex(sex_age: str) -> str:
    """性齢文字列から性別コードを抽出する。

    '牡3' → '牡' のように先頭の性別文字のみを返す。

    Args:
        sex_age: 性齢文字列（例: "牡3"、"牝4"、"セ5"）。

    Returns:
        性別文字（"牡"/"牝"/"セ"）。マッチしない場合は空文字列。
    """
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
        # 学習済み sire_map を復元して推論時のエンコーディング一貫性を保証する。
        # pkl が存在しない場合（初回学習前）は空 dict で始め、動的に割り当てる。
        self._sire_map: dict[str, int] = self._load_sire_map()
        # 騎手・調教師は jockeys/trainers マスタから固定コードマップを生成する。
        # マスタが空（未投入）の場合は空 dict → fallback=0 で全馬同一値になる。
        # セッション間でコードが変わらないため、学習・推論の一貫性が保たれる。
        self._jockey_code_map: dict[str, int] = self._load_jockey_codes()
        self._trainer_code_map: dict[str, int] = self._load_trainer_codes()

    @staticmethod
    def _load_sire_map() -> dict[str, int]:
        """label_encoders.pkl から sire_map を読み込む。ファイルがなければ空 dict。

        Returns:
            父名→整数コードのマッピング辞書。ファイル不在または読み込み失敗時は空辞書。
        """
        try:
            if _SIRE_MAP_PKL.exists():
                with open(_SIRE_MAP_PKL, "rb") as f:
                    data = pickle.load(f)
                return data.get("sire_map", {}) if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning("label_encoders.pkl の読み込みに失敗しました: %s", e)
        return {}

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
                COALESCE(rr.trainer, '') AS trainer_key,
                rr.jockey_code,
                rr.trainer_code
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

        # バルク取得: 1レースあたり N×6クエリ → 7クエリに圧縮
        # race_date を渡して時系列リーク（未来レースの着順混入）を完全排除する
        horse_ids = [r[0] for r in rows]
        stats_bulk = self._get_horse_stats_bulk(
            horse_ids,
            surface,
            distance,
            exclude_race_id=race_id,
            race_date=race_date,
        )
        training_bulk = self._get_training_stats_bulk(horse_ids, race_date)
        sire_bulk = self._get_sire_bulk(horse_ids)

        records = []
        for sim_num, (
            horse_id,
            horse_name,
            sex_age,
            weight_carried,
            jockey,
            horse_weight,
            win_odds,
            popularity,
            gate_number,
            horse_weight_diff,
            jockey_key,
            trainer_key,
            jockey_code,
            trainer_code,
        ) in enumerate(rows, start=1):
            stats = stats_bulk.get(
                horse_id,
                {
                    "win_rate_all": None,
                    "win_rate_surface": None,
                    "win_rate_distance_band": None,
                    "recent_rank_mean": None,
                },
            )
            training = training_bulk.get(
                horse_id,
                {
                    "tc_4f": None,
                    "tc_lap": None,
                    "tc_accel_flag": None,
                    "tc_4f_diff": None,
                    "hc_4f": None,
                    "hc_lap": None,
                    "hc_accel_flag": None,
                    "hc_4f_diff": None,
                },
            )

            # オッズ→市場確率変換（アンチパターン: 生オッズ直接使用禁止）
            raw_odds = float(win_odds) if win_odds else None
            market_prob = (1.0 / min(raw_odds, 80.0)) if raw_odds else None

            records.append(
                {
                    "horse_number": sim_num,
                    "horse_id": horse_id,
                    "horse_name": horse_name,
                    "weight_carried": weight_carried,
                    "horse_weight": horse_weight,
                    "win_odds": win_odds,
                    "popularity": popularity,
                    "win_rate_all": stats["win_rate_all"],
                    "win_rate_surface": stats["win_rate_surface"],
                    "win_rate_distance_band": stats["win_rate_distance_band"],
                    "recent_rank_mean": stats["recent_rank_mean"],
                    "surface_code": _SURFACE_CODE.get(surface, -1),
                    "sex_code": _SEX_CODE.get(_parse_sex(sex_age or ""), -1),
                    "venue_encoded": _VENUE_CODE.get(venue, len(_VENUE_CODE)),
                    "sire_encoded": self._encode_sire(sire_bulk.get(horse_id)),
                    "distance": distance,
                    "dist_band": dist_band,
                    # ── 追加特徴量 ────────────────────────────────────
                    "horse_weight_diff": horse_weight_diff,  # 前走比体重増減
                    "gate_number": gate_number,  # 枠番
                    "condition_code": _CONDITION_CODE.get(
                        condition or "", -1
                    ),  # 馬場状態
                    "market_prob": market_prob,  # 市場確率 1/odds.clip(max=80)
                    "race_number": race_number,  # レース番号
                    # ── 人的要素特徴量 ───────────────────────────────
                    "jockey_code_encoded": self._encode_jockey(
                        jockey_key, jockey_code
                    ),  # 騎手コード（W-076: コード優先）
                    "trainer_code_encoded": self._encode_trainer(
                        trainer_key, trainer_code
                    ),  # 調教師コード（W-076: コード優先）
                    # ── 調教特徴量（WOOD:TC / WOOD:HC） ─────────────
                    "tc_4f": training["tc_4f"],  # ウッド直近4Fタイム（秒）
                    "tc_lap": training["tc_lap"],  # ウッド直近ラスト1Fタイム
                    "tc_accel_flag": training[
                        "tc_accel_flag"
                    ],  # ウッド加速ラップ (1=好調)
                    "tc_4f_diff": training[
                        "tc_4f_diff"
                    ],  # ウッド前回比タイム差（負=好転）
                    "hc_4f": training["hc_4f"],  # 坂路直近4Fタイム（秒）
                    "hc_lap": training["hc_lap"],  # 坂路直近ラスト1Fタイム
                    "hc_accel_flag": training[
                        "hc_accel_flag"
                    ],  # 坂路加速ラップ (1=好調)
                    "hc_4f_diff": training[
                        "hc_4f_diff"
                    ],  # 坂路前回比タイム差（負=好転）
                    # ── 当日バイアス特徴量 ────────────────────────────
                    # current_race_number より前の確定済みレースから算出（リーク排除済み）
                    "today_inner_bias": bias["today_inner_bias"],  # 内枠勝率 - 外枠勝率
                    "today_front_bias": bias[
                        "today_front_bias"
                    ],  # 当日・人気馬勝率（先行バイアス代理）
                    "today_race_count": bias[
                        "today_race_count"
                    ],  # 集計レース数（信頼度）
                    "today_gate_match": (  # バイアス×枠番の相性スコア
                        bias["today_inner_bias"]
                        * (1.0 if (gate_number or 0) <= 4 else -1.0)
                        if bias["today_inner_bias"] is not None
                        else None
                    ),
                    # ── オッズ時系列特徴量（大口投票シグナル） ───────────
                    # シミュレーション時は realtime_odds が未記録のため全 None。
                    # prerace 時に複数スナップショットが記録されていれば有効。
                    "odds_vs_morning": odds_trend.get(sim_num, {}).get(
                        "odds_vs_morning"
                    ),
                    "odds_velocity": odds_trend.get(sim_num, {}).get("odds_velocity"),
                    # 識別子（モデル学習には使わない）
                    "sex_age": sex_age,
                    "jockey": jockey,
                }
            )

        df = pd.DataFrame(records)
        df["race_id"] = race_id  # UScoreEngine が基準日導出に使用
        df = self._add_intra_race_features(df)
        df = self._add_u_score(df)
        # シミュレーション時は過去 x_signals がある場合のみ反映（なければ 0 埋め）
        df = self._add_x_consensus(df, race_id)
        logger.info(
            "[SIMULATE] 特徴量生成 race_id=%s: %d 頭 × %d 特徴量 (リーク除外済み)",
            race_id,
            len(df),
            df.shape[1],
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

        # horse_weight は entries が NULL の場合、直近 race_results から補完する
        entries = self._conn.execute(
            """
            SELECT
                e.horse_number, e.horse_id, e.horse_name, e.sex_age,
                e.weight_carried,
                COALESCE(
                    e.horse_weight,
                    (SELECT rr.horse_weight FROM race_results rr
                     WHERE rr.horse_id = e.horse_id
                       AND rr.horse_weight IS NOT NULL AND rr.horse_weight > 0
                     ORDER BY rr.race_id DESC LIMIT 1)
                ) AS horse_weight,
                e.gate_number,
                COALESCE(
                    e.horse_weight_diff,
                    (SELECT rr.horse_weight_diff FROM race_results rr
                     WHERE rr.horse_id = e.horse_id
                       AND rr.horse_weight IS NOT NULL AND rr.horse_weight > 0
                     ORDER BY rr.race_id DESC LIMIT 1)
                ) AS horse_weight_diff,
                e.jockey, e.trainer,
                e.jockey_code, e.trainer_code
            FROM entries e
            WHERE e.race_id = ?
              AND e.horse_number > 0
            ORDER BY e.horse_number
            """,
            (race_id,),
        ).fetchall()

        # 最新オッズを馬番で引く
        odds_map = self._latest_odds_map(race_id)

        # バルク取得: 1レースあたり N×6クエリ → 7クエリに圧縮
        horse_ids = [r[1] for r in entries]  # index 1 = horse_id
        stats_bulk = self._get_horse_stats_bulk(horse_ids, surface or "", distance or 0)
        training_bulk = self._get_training_stats_bulk(horse_ids, race_date or "")
        sire_bulk = self._get_sire_bulk(horse_ids)

        records = []
        for (
            horse_number,
            horse_id,
            horse_name,
            sex_age,
            weight_carried,
            horse_weight,
            gate_number,
            horse_weight_diff,
            jockey,
            trainer,
            jockey_code,
            trainer_code,
        ) in entries:
            stats = stats_bulk.get(
                horse_id,
                {
                    "win_rate_all": None,
                    "win_rate_surface": None,
                    "win_rate_distance_band": None,
                    "recent_rank_mean": None,
                },
            )
            training = training_bulk.get(
                horse_id,
                {
                    "tc_4f": None,
                    "tc_lap": None,
                    "tc_accel_flag": None,
                    "tc_4f_diff": None,
                    "hc_4f": None,
                    "hc_lap": None,
                    "hc_accel_flag": None,
                    "hc_4f_diff": None,
                },
            )
            odds = odds_map.get(horse_number, {})

            jockey_key = jockey or ""
            trainer_key = trainer or ""

            raw_odds_val = odds.get("win_odds")
            market_prob = (
                (1.0 / min(float(raw_odds_val), 80.0)) if raw_odds_val else None
            )

            records.append(
                {
                    # 識別子（モデル学習には使わない）
                    "horse_number": horse_number,
                    "horse_id": horse_id,
                    "horse_name": horse_name,
                    # 数値特徴量
                    "weight_carried": weight_carried,
                    "horse_weight": horse_weight,
                    "win_odds": raw_odds_val,
                    "popularity": odds.get("popularity"),
                    "market_prob": market_prob,  # 市場確率 1/odds.clip(max=80) [W-004]
                    # 馬成績特徴量
                    "win_rate_all": stats["win_rate_all"],
                    "win_rate_surface": stats["win_rate_surface"],
                    "win_rate_distance_band": stats["win_rate_distance_band"],
                    "recent_rank_mean": stats["recent_rank_mean"],
                    # カテゴリ特徴量（整数エンコード）
                    "surface_code": _SURFACE_CODE.get(surface or "", -1),
                    "sex_code": _SEX_CODE.get(_parse_sex(sex_age or ""), -1),
                    "venue_encoded": _VENUE_CODE.get(venue or "", len(_VENUE_CODE)),
                    "sire_encoded": self._encode_sire(sire_bulk.get(horse_id)),
                    # レース情報
                    "distance": distance or 0,
                    "dist_band": dist_band,
                    "horse_weight_diff": horse_weight_diff,
                    "gate_number": gate_number,
                    "condition_code": _CONDITION_CODE.get(condition or "", -1),
                    "race_number": race_number or 0,
                    # 人的要素特徴量
                    "jockey_code_encoded": self._encode_jockey(
                        jockey_key, jockey_code
                    ),
                    "trainer_code_encoded": self._encode_trainer(
                        trainer_key, trainer_code
                    ),
                    # 調教特徴量（WOOD:TC / WOOD:HC）
                    "tc_4f": training["tc_4f"],
                    "tc_lap": training["tc_lap"],
                    "tc_accel_flag": training["tc_accel_flag"],
                    "tc_4f_diff": training["tc_4f_diff"],
                    "hc_4f": training["hc_4f"],
                    "hc_lap": training["hc_lap"],
                    "hc_accel_flag": training["hc_accel_flag"],
                    "hc_4f_diff": training["hc_4f_diff"],
                    # ── 当日バイアス特徴量 ────────────────────────────────
                    # race_number より前の確定済みレースから算出（リーク排除済み）
                    "today_inner_bias": bias["today_inner_bias"],
                    "today_front_bias": bias["today_front_bias"],
                    "today_race_count": bias["today_race_count"],
                    "today_gate_match": (
                        bias["today_inner_bias"]
                        * (1.0 if (gate_number or 0) <= 4 else -1.0)
                        if bias["today_inner_bias"] is not None
                        else None
                    ),
                    # ── オッズ時系列特徴量（大口投票シグナル） ───────────
                    # realtime_odds に複数スナップショットある場合のみ有効。
                    # スナップショットが1点以下の場合は None（LightGBM が欠損として扱う）。
                    "odds_vs_morning": odds_trend.get(horse_number, {}).get(
                        "odds_vs_morning"
                    ),
                    "odds_velocity": odds_trend.get(horse_number, {}).get(
                        "odds_velocity"
                    ),
                    # 識別子
                    "sex_age": sex_age,
                    "jockey": jockey,
                }
            )

        df = pd.DataFrame(records)
        df["race_id"] = race_id  # UScoreEngine が基準日導出に使用
        df = self._add_intra_race_features(df)
        df = self._add_u_score(df)
        df = self._add_x_consensus(df, race_id)
        df = self._add_ev_features(df)
        logger.info(
            "特徴量生成 race_id=%s: %d 頭 × %d 特徴量",
            race_id,
            len(df),
            df.shape[1],
        )
        return df

    # ── U Score 統合 ───────────────────────────────────────────

    def _add_u_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """UScoreEngine を呼び出して 18 因子 + u_score を DataFrame に追加する。

        インポートエラーや計算エラーが起きても元の df をそのまま返すため、
        U score 未対応の環境でも既存パイプラインは正常稼働する。

        Args:
            df: FeatureBuilder が生成した出走馬特徴量 DataFrame。

        Returns:
            U score 因子列が追加された DataFrame。エラー時は入力 df をそのまま返す。
        """
        try:
            from src.ml.u_score import UScoreEngine

            engine = UScoreEngine(self._conn)
            return engine.calc(df)
        except Exception as exc:
            logger.warning("U score 計算をスキップ（エラー: %s）", exc)
            return df

    def _add_x_consensus(
        self,
        df: pd.DataFrame,
        race_id: str,
        *,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """X 世論コンセンサススコアと複合特徴量を DataFrame に追加する。

        追加列:
          x_consensus_score  : 馬番別コンセンサス加重平均 (Phase B 出力)
          x_crowd_divergence : x_consensus × (crowd_bias_ratio - 1.0)
                               専門家が推す かつ 市場が過小評価 → 高プラス値
                               専門家が推す かつ 市場が過大評価 → マイナス（矛盾）
          x_signal_count     : その馬へのシグナル件数（信頼度代理変数）

        dry_run=True または x_signals が空の場合はダミー 0.0 で埋める。
        """
        import numpy as np

        n = len(df)
        # デフォルト: シグナルなし（0 = ニュートラル）
        x_score = pd.Series(0.0, index=df.index, dtype=float)
        x_count = pd.Series(0, index=df.index, dtype=int)

        if not dry_run:
            try:
                from src.ml.x_signal_parser import get_x_consensus_score

                scores = get_x_consensus_score(self._conn, race_id)
                if "horse_number" in df.columns:
                    for idx, row in df.iterrows():
                        hn = row.get("horse_number")
                        if hn is not None and int(hn) in scores:
                            x_score.at[idx] = float(scores[int(hn)])
                # シグナル件数も取得
                cnt_rows = self._conn.execute(
                    """
                    SELECT horse_number, COUNT(*) AS cnt
                    FROM   x_signals
                    WHERE  race_id = ? AND parsed = 1 AND horse_number IS NOT NULL
                    GROUP  BY horse_number
                    """,
                    (race_id,),
                ).fetchall()
                cnt_map = {r[0]: r[1] for r in cnt_rows}
                if "horse_number" in df.columns:
                    for idx, row in df.iterrows():
                        hn = row.get("horse_number")
                        if hn is not None:
                            x_count.at[idx] = cnt_map.get(int(hn), 0)
            except Exception as exc:
                logger.warning("x_consensus_score 取得スキップ: %s", exc)

        df["x_consensus_score"] = x_score.clip(-1.0, 1.0)
        df["x_signal_count"] = x_count.clip(0, 50)

        # 複合特徴量: 専門家世論 × 市場乖離の積
        _crowd_raw = df.get("crowd_bias_ratio")
        if isinstance(_crowd_raw, pd.Series):
            crowd_ratio = pd.to_numeric(_crowd_raw, errors="coerce").fillna(1.0)
        else:
            crowd_ratio = pd.Series(1.0, index=df.index)
        # tanh で [-1,1] に圧縮し、過激な crowd_bias を抑制
        divergence = df["x_consensus_score"] * np.tanh(crowd_ratio - 1.0)
        df["x_crowd_divergence"] = divergence.clip(-1.0, 1.0).fillna(0.0)

        logger.debug(
            "[X-feature] race_id=%s: %d頭中%d頭にシグナルあり (dry_run=%s)",
            race_id,
            n,
            int((x_score != 0.0).sum()),
            dry_run,
        )
        return df

    def _add_ev_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """EVEnhancedFeatures を呼び出して EV 特化特徴量 7 列を DataFrame に追加する。

        追加列:
          shin_prob             : Shin (1993) 真確率推定
          implied_prob_excess   : Shin確率 − 市場確率（FLB 補正量）
          harville_place_prob   : Harville 法複勝確率
          odds_reversal_score   : オッズ逆転スコア
          odds_steam_flag       : スチームムーブ検出フラグ
          field_strength_ev_adj : フィールド強度 EV 調整値
          ev_rank_in_race       : レース内 EV ランク

        インポートエラーや計算エラーが起きても元の df をそのまま返す。
        win_odds 列が存在しない場合はスキップする。
        """
        if "win_odds" not in df.columns:
            return df
        try:
            from src.ml.ev_features import EVEnhancedFeatures

            engine = EVEnhancedFeatures()
            return engine.add_ev_features(df)
        except Exception as exc:
            logger.warning("EV 特徴量計算をスキップ（エラー: %s）", exc)
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
            race_date:            "YYYY-MM-DD" 形式の日付 (ISO 8601)
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

        (
            inner_wins,
            inner_horses,
            outer_wins,
            outer_horses,
            fav_wins,
            completed_races,
        ) = row

        inner_rate: float | None = (
            (inner_wins / inner_horses) if (inner_horses or 0) > 0 else None
        )
        outer_rate: float | None = (
            (outer_wins / outer_horses) if (outer_horses or 0) > 0 else None
        )

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
        """レース内相対特徴量（順位・偏差）を追加する。

        全馬のデータが揃った DataFrame に対して groupby なしで適用する
        （呼び出し元が 1レース分の DataFrame を渡す前提）。

        - rank:   1 = レース内最良（勝率高・着順低・調教タイム速）
        - zscore: (値 - mean) / std。速さ系は符号反転して高いほど良い方向に揃える。
        値がすべて NaN の場合は NaN のまま残す（LightGBM が欠損として扱う）。

        Args:
            df: 1レース分の出走馬特徴量 DataFrame。

        Returns:
            レース内ランク・偏差列が追加された DataFrame。空の場合はそのまま返す。
        """
        if df.empty:
            return df

        df = df.copy()

        def _rank_desc(col: str) -> None:
            """高いほど良い特徴量：rank=1 が最高値。全 NaN の場合は NaN 列を追加。"""
            if col not in df.columns or df[col].isna().all():
                # 列が存在しない or 全欠損 → NaN 列を追加してモデルが欠損扱いにできるよう統一
                df[f"{col}_rank"] = float("nan")
                df[f"{col}_zscore"] = float("nan")
                return
            df[f"{col}_rank"] = df[col].rank(ascending=False, na_option="bottom")
            std = df[col].std()
            df[f"{col}_zscore"] = (
                ((df[col] - df[col].mean()) / std) if std and std > 0 else 0.0
            )

        def _rank_asc_inv(col: str) -> None:
            """低いほど良い特徴量：rank=1 が最小値。zscore は符号反転で高=良に統一。全 NaN の場合は NaN 列を追加。"""
            if col not in df.columns or df[col].isna().all():
                df[f"{col}_rank"] = float("nan")
                df[f"{col}_zscore"] = float("nan")
                return
            df[f"{col}_rank"] = df[col].rank(ascending=True, na_option="bottom")
            std = df[col].std()
            df[f"{col}_zscore"] = (
                (-(df[col] - df[col].mean()) / std) if std and std > 0 else 0.0
            )

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
        race_date: str | None = None,
    ) -> dict[str, float | None]:
        """
        horses / race_results テーブルから馬の過去成績指標を算出する。

        Args:
            exclude_race_id: このレース ID を統計から除外する（シミュレーション時に
                             対象レース自身の着順がリークしないよう指定する）。
            race_date:       この日付より前のレースのみ参照する（時系列リーク防止）。

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
        excl_param = (exclude_race_id,) if exclude_race_id else ()
        # 時系列リーク防止: race_date より後のレースを除外する
        date_clause = "AND r.date < ?" if race_date else ""
        date_param = (race_date,) if race_date else ()

        # 全成績（races テーブルを JOIN して日付フィルタを適用）
        row = self._conn.execute(
            f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND rr.rank IS NOT NULL
            {excl_clause} {date_clause}
            """,
            (horse_id, *excl_param, *date_param),
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
            {excl_clause} {date_clause}
            """,
            (horse_id, surface, *excl_param, *date_param),
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
            {excl_clause} {date_clause}
            """,
            (horse_id, lo, hi, *excl_param, *date_param),
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
            {excl_clause} {date_clause}
            ORDER BY r.date DESC
            LIMIT 5
            """,
            (horse_id, *excl_param, *date_param),
        ).fetchall()
        recent_rank_mean: float | None = (
            sum(r[0] for r in rows_recent) / len(rows_recent) if rows_recent else None
        )

        return {
            "win_rate_all": win_rate_all,
            "win_rate_surface": win_rate_surface,
            "win_rate_distance_band": win_rate_distance_band,
            "recent_rank_mean": recent_rank_mean,
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
            "tc_4f": None,
            "tc_lap": None,
            "tc_accel_flag": None,
            "tc_4f_diff": None,
            "hc_4f": None,
            "hc_lap": None,
            "hc_accel_flag": None,
            "hc_4f_diff": None,
        }
        if not horse_id or not race_date:
            return _null

        # race_results.horse_id は YYYY+SSSSSS（4桁年+6桁連番）の10桁数値文字列。
        # training_times.horse_id は D+YYYY+SSSSS（1桁+4桁年+5桁連番）の10桁。
        # 共通キー(9桁) = substr(horse_id,1,4)||substr(horse_id,5,5)
        #               = substr(tc.horse_id,2,9)
        if len(horse_id) != 10 or not horse_id.isdigit():
            return _null
        tc_key = horse_id[:4] + horse_id[4:9]  # "YYYY" + 最初5桁の連番

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
            result["tc_4f"] = tc_4f
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
            result["hc_4f"] = hc_4f
            result["hc_lap"] = hc_lap
            # 加速ラップ
            if hc_4f and hc_lap:
                result["hc_accel_flag"] = float(hc_lap < hc_4f / 4.0)
            # 前回比タイム差
            if len(hc_rows) >= 2 and hc_rows[1][0] is not None:
                result["hc_4f_diff"] = hc_4f - hc_rows[1][0]

        return result

    def _get_sire(self, horse_id: str | None) -> str | None:
        """horses テーブルから父名を取得する。

        Args:
            horse_id: 馬 ID 文字列。None の場合は None を返す。

        Returns:
            父馬名文字列。テーブルに存在しない場合は None。
        """
        if not horse_id:
            return None
        row = self._conn.execute(
            "SELECT sire FROM horses WHERE horse_id = ?", (horse_id,)
        ).fetchone()
        return row[0] if row else None

    def _get_sire_bulk(
        self, horse_ids: list[str | None]
    ) -> dict[str | None, str | None]:
        """複数馬の父名を1クエリで一括取得する。

        Args:
            horse_ids: 馬 ID 文字列のリスト（None を含んでよい）。

        Returns:
            {horse_id: sire_name} マッピング辞書。horses テーブルに存在しない場合は None。
        """
        valid = [h for h in horse_ids if h]
        if not valid:
            return {h: None for h in horse_ids}
        ph = ", ".join("?" * len(valid))
        rows = self._conn.execute(
            f"SELECT horse_id, sire FROM horses WHERE horse_id IN ({ph})", valid
        ).fetchall()
        sire_db = {r[0]: r[1] for r in rows}
        return {h: (sire_db.get(h) if h else None) for h in horse_ids}

    def _get_horse_stats_bulk(
        self,
        horse_ids: list[str | None],
        surface: str,
        distance: int,
        *,
        exclude_race_id: str | None = None,
        race_date: str | None = None,
    ) -> dict[str | None, dict[str, float | None]]:
        """
        複数馬の過去成績指標を4クエリで一括取得する（_get_horse_stats の N+1 解消版）。

        バックテスト・シミュレーション時は以下を指定してリークを完全排除する:
          - exclude_race_id: 対象レース自身の着順が混入しないよう除外
          - race_date:       これより後の日付（未来レース）の着順が混入しないよう除外
                             実予想（build_race_features）では None のまま使用してよい。
        """
        _null: dict[str, float | None] = {
            "win_rate_all": None,
            "win_rate_surface": None,
            "win_rate_distance_band": None,
            "recent_rank_mean": None,
        }
        valid = [h for h in horse_ids if h]
        if not valid:
            return {h: dict(_null) for h in horse_ids}

        ph = ", ".join("?" * len(valid))
        excl = "AND rr.race_id != ?" if exclude_race_id else ""
        ep = [exclude_race_id] if exclude_race_id else []
        # 時系列リーク防止: race_date 以降のレースを除外（同日レースも含め除外）
        dfilt = "AND r.date < ?" if race_date else ""
        dep = [race_date] if race_date else []

        dist_band = _distance_band(distance)
        lo, hi = next((l, hh) for l, hh, lab in _DISTANCE_BANDS if lab == dist_band)

        # ── 全成績 ──────────────────────────────────────────────
        # 時系列リーク防止のため races テーブルを JOIN して日付フィルタを適用する
        all_rows = self._conn.execute(
            f"""
            SELECT rr.horse_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.horse_id IN ({ph}) AND rr.rank IS NOT NULL {excl} {dfilt}
            GROUP BY rr.horse_id
            """,
            [*valid, *ep, *dep],
        ).fetchall()
        all_map = {r[0]: (r[1], r[2]) for r in all_rows}

        # ── 馬場別成績 ──────────────────────────────────────────
        sf_rows = self._conn.execute(
            f"""
            SELECT rr.horse_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.horse_id IN ({ph}) AND r.surface = ? AND rr.rank IS NOT NULL {excl} {dfilt}
            GROUP BY rr.horse_id
            """,
            [*valid, surface, *ep, *dep],
        ).fetchall()
        sf_map = {r[0]: (r[1], r[2]) for r in sf_rows}

        # ── 距離帯別成績 ────────────────────────────────────────
        db_rows = self._conn.execute(
            f"""
            SELECT rr.horse_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) AS wins
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.horse_id IN ({ph})
              AND r.distance >= ? AND r.distance < ?
              AND rr.rank IS NOT NULL {excl} {dfilt}
            GROUP BY rr.horse_id
            """,
            [*valid, lo, hi, *ep, *dep],
        ).fetchall()
        db_map = {r[0]: (r[1], r[2]) for r in db_rows}

        # ── 直近5走平均着順（ウィンドウ関数で最新5行を馬単位で取得） ──
        recent_rows = self._conn.execute(
            f"""
            SELECT horse_id, rank FROM (
                SELECT rr.horse_id, rr.rank,
                       ROW_NUMBER() OVER (
                           PARTITION BY rr.horse_id ORDER BY r.date DESC, rr.race_id DESC
                       ) AS rn
                FROM race_results rr
                JOIN races r ON rr.race_id = r.race_id
                WHERE rr.horse_id IN ({ph}) AND rr.rank IS NOT NULL {excl} {dfilt}
            ) WHERE rn <= 5
            """,
            [*valid, *ep, *dep],
        ).fetchall()
        recent_map: dict[str, list[int]] = {}
        for horse_id, rank in recent_rows:
            recent_map.setdefault(horse_id, []).append(rank)

        result: dict[str | None, dict[str, float | None]] = {}
        for h in horse_ids:
            if not h:
                result[h] = dict(_null)
                continue
            at, aw = all_map.get(h, (0, 0))
            st, sw = sf_map.get(h, (0, 0))
            dt, dw = db_map.get(h, (0, 0))
            rec = recent_map.get(h, [])
            result[h] = {
                "win_rate_all": (aw / at) if at else None,
                "win_rate_surface": (sw / st) if st else None,
                "win_rate_distance_band": (dw / dt) if dt else None,
                "recent_rank_mean": (sum(rec) / len(rec)) if rec else None,
            }
        return result

    def _get_training_stats_bulk(
        self,
        horse_ids: list[str | None],
        race_date: str,
    ) -> dict[str | None, dict[str, float | None]]:
        """
        複数馬の調教特徴量を2クエリで一括取得する（_get_training_stats の N+1 解消版）。

        ウィンドウ関数で馬別に直近2回分の調教記録を取得し、加速ラップ・タイム差を計算する。
        """
        _null: dict[str, float | None] = {
            "tc_4f": None,
            "tc_lap": None,
            "tc_accel_flag": None,
            "tc_4f_diff": None,
            "hc_4f": None,
            "hc_lap": None,
            "hc_accel_flag": None,
            "hc_4f_diff": None,
        }
        result: dict[str | None, dict[str, float | None]] = {
            h: dict(_null) for h in horse_ids
        }

        # 10桁数値 horse_id のみ対象（training_times の JOIN キー変換が可能なもの）
        valid: dict[str, str] = {
            h: h[:4] + h[4:9] for h in horse_ids if h and len(h) == 10 and h.isdigit()
        }
        if not valid:
            return result

        tc_keys = list(valid.values())
        ph = ", ".join("?" * len(tc_keys))

        def _fetch_recs(table: str) -> dict[str, list[tuple[float, float | None]]]:
            rows = self._conn.execute(
                f"""
                SELECT tc_key, time_4f, lap_time FROM (
                    SELECT substr(horse_id,2,9) AS tc_key,
                           time_4f, lap_time,
                           ROW_NUMBER() OVER (
                               PARTITION BY substr(horse_id,2,9)
                               ORDER BY training_date DESC
                           ) AS rn
                    FROM {table}
                    WHERE substr(horse_id,2,9) IN ({ph})
                      AND training_date < ?
                      AND training_date != ''
                      AND time_4f IS NOT NULL
                ) WHERE rn <= 2
                """,
                [*tc_keys, race_date],
            ).fetchall()
            rec_map: dict[str, list[tuple[float, float | None]]] = {}
            for tc_key, t4f, lap in rows:
                rec_map.setdefault(tc_key, []).append((t4f, lap))
            return rec_map

        tc_map = _fetch_recs("training_times")
        hc_map = _fetch_recs("training_hillwork")

        # horse_id → tc_key の逆引き
        key_to_horses: dict[str, list[str]] = {}
        for horse_id, tc_key in valid.items():
            key_to_horses.setdefault(tc_key, []).append(horse_id)

        for tc_key, horse_list in key_to_horses.items():
            tc_recs = tc_map.get(tc_key, [])
            hc_recs = hc_map.get(tc_key, [])
            stats: dict[str, float | None] = dict(_null)

            if tc_recs:
                tc_4f, tc_lap = tc_recs[0]
                stats["tc_4f"] = tc_4f
                stats["tc_lap"] = tc_lap
                if tc_4f and tc_lap:
                    stats["tc_accel_flag"] = float(tc_lap < tc_4f / 4.0)
                if len(tc_recs) >= 2 and tc_recs[1][0] is not None:
                    stats["tc_4f_diff"] = tc_4f - tc_recs[1][0]

            if hc_recs:
                hc_4f, hc_lap = hc_recs[0]
                stats["hc_4f"] = hc_4f
                stats["hc_lap"] = hc_lap
                if hc_4f and hc_lap:
                    stats["hc_accel_flag"] = float(hc_lap < hc_4f / 4.0)
                if len(hc_recs) >= 2 and hc_recs[1][0] is not None:
                    stats["hc_4f_diff"] = hc_4f - hc_recs[1][0]

            for horse_id in horse_list:
                result[horse_id] = stats

        return result

    def _encode_sire(self, sire: str | None) -> int:
        """父名をラベルエンコードする。

        同一セッション内で一貫した整数を返し、未知は新規割り当て。

        Args:
            sire: 父馬名文字列。None または空文字列の場合は -1 を返す。

        Returns:
            整数コード。None/空文字列の場合は -1。
        """
        if not sire:
            return -1
        if sire not in self._sire_map:
            self._sire_map[sire] = len(self._sire_map)
        return self._sire_map[sire]

    def _load_jockey_codes(self) -> dict[str, int]:
        """jockeys マスタから騎手名→コードのマップを生成する。

        Returns:
            騎手名から整数コードへのマッピング辞書。テーブルが空または
            存在しない場合は空辞書を返す。
        """
        try:
            rows = self._conn.execute(
                "SELECT jockey_name, CAST(jockey_code AS INTEGER) "
                "FROM jockeys WHERE jockey_name IS NOT NULL"
            ).fetchall()
            return {name: code for name, code in rows if name}
        except Exception:
            return {}

    def _load_trainer_codes(self) -> dict[str, int]:
        """trainers マスタから調教師名→コードのマップを生成する。

        Returns:
            調教師名から整数コードへのマッピング辞書。テーブルが空または
            存在しない場合は空辞書を返す。
        """
        try:
            rows = self._conn.execute(
                "SELECT trainer_name, CAST(trainer_code AS INTEGER) "
                "FROM trainers WHERE trainer_name IS NOT NULL"
            ).fetchall()
            return {name: code for name, code in rows if name}
        except Exception:
            return {}

    def _encode_jockey(
        self, jockey_key: str | None, jockey_code: str | None = None
    ) -> int:
        """騎手をコードでエンコードする（W-076: コード優先・名前フォールバック）。

        race_results/entries の ``jockey_code`` を最優先で使う。氏名は SE 8バイト
        切り詰め・文字化けで分散するため、コードがあれば名前マッチを介さず安定した
        騎手識別子になる。コード欠損行は従来の名前→コードマップにフォールバックする。

        Args:
            jockey_key:  騎手名文字列（フォールバック用）。
            jockey_code: JRA-VAN 騎手コード（最優先）。

        Returns:
            整数コード。コード/名前とも不明なら 0、key も無ければ -1。
        """
        if jockey_code:
            try:
                return int(jockey_code)
            except (ValueError, TypeError):
                pass
        if not jockey_key:
            return -1
        return self._jockey_code_map.get(jockey_key, 0)

    def _encode_trainer(
        self, trainer_key: str | None, trainer_code: str | None = None
    ) -> int:
        """調教師をコードでエンコードする（W-076: コード優先・名前フォールバック）。

        Args:
            trainer_key:  調教師名文字列（フォールバック用）。
            trainer_code: JRA-VAN 調教師コード（最優先）。

        Returns:
            整数コード。コード/名前とも不明なら 0、key も無ければ -1。
        """
        if trainer_code:
            try:
                return int(trainer_code)
            except (ValueError, TypeError):
                pass
        if not trainer_key:
            return -1
        return self._trainer_code_map.get(trainer_key, 0)

    def _get_odds_trend(
        self,
        race_id: str,
    ) -> dict[int, dict[str, float | None]]:
        """realtime_odds テーブルの時系列から馬別のオッズ変動特徴量を算出する。

        大口投票シグナルの検知ロジック:

        - odds_vs_morning（朝一比率）: latest_odds / morning_odds で算出。
          1.0 未満は人気上昇（大口流入の可能性）、1.0 超は人気低下を示す。
        - odds_velocity（下落速度、オッズ/分）: (past_odds - latest_odds) /
          elapsed_minutes で算出。正値は資金流入加速、負値は資金流出を示す。

        データ不足（スナップショット1点以下等）の場合は None を返す。
        主に prerace_pipeline で realtime_odds が複数時点記録済みの場合に有効。

        Args:
            race_id: レース ID 文字列。

        Returns:
            馬番をキー、オッズ変動特徴量辞書を値とするマッピング。
            各値辞書は "odds_vs_morning" と "odds_velocity" を持ち、
            データ不足の場合は None となる。
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
            morning_odds = ts[0][1]  # 最初に記録されたオッズ（朝一）
            latest_odds = ts[-1][1]  # 最新オッズ
            latest_str = ts[-1][0]  # 最新 recorded_at 文字列

            # ── 朝一比率 ──────────────────────────────────────────
            odds_vs_morning: float | None = None
            if morning_odds > 0:
                odds_vs_morning = latest_odds / morning_odds

            # ── 直近1時間の下落速度 ───────────────────────────────
            odds_velocity: float | None = None
            if len(ts) >= 2:
                try:
                    t_latest = datetime.fromisoformat(latest_str)
                    cutoff = t_latest - timedelta(minutes=60)

                    # cutoff 以前の最新スナップショットを探す
                    past: tuple[str, float] | None = None
                    for t_str, o in ts[:-1]:  # 最新1点は除外
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
                "odds_velocity": odds_velocity,
            }

        return result

    def _latest_odds_map(self, race_id: str) -> dict[int, dict]:
        """realtime_odds から各馬の最新オッズを取得する。

        Args:
            race_id: レース ID 文字列。

        Returns:
            馬番をキー、{"win_odds": ..., "popularity": ...} 辞書を値とするマッピング。
            レコードが存在しない場合は空辞書を返す。
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
        return {r[0]: {"win_odds": r[1], "popularity": r[2]} for r in rows}
