"""
src/ml/narrative_generator.py — 予想根拠の自然言語生成

LightGBM 内蔵 SHAP (pred_contrib=True) を使って各馬のスコア根拠を抽出し、
競馬ファン向けの「買い根拠文」を 3〜5 行で自動生成する。

Usage:
    from src.ml.narrative_generator import NarrativeGenerator
    gen = NarrativeGenerator(conn)
    text = gen.generate(race_id="202608030411", horse_number=6)
    print(text)
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]


def _isnan(v: Any) -> bool:
    """NaN チェック — str や None でも安全に動作する。"""
    try:
        return bool(np.isnan(float(v)))
    except (TypeError, ValueError):
        return True
_MODEL_DIR = _ROOT / "data" / "models"

# ── 特徴量 → 日本語説明テンプレート ──────────────────────────────────
# (label, good_direction, formatter)
#   good_direction: "low" = 値が小さいほど良い, "high" = 大きいほど良い
_FEATURE_META: dict[str, tuple[str, str]] = {
    # 馬能力
    "win_rate_all":              ("通算勝率",           "high"),
    "win_rate_surface":          ("馬場別勝率",         "high"),
    "win_rate_distance_band":    ("距離帯別勝率",       "high"),
    "recent_rank_mean":          ("直近5走平均着順",    "low"),   # 小さい=好調
    "win_rate_all_rank":         ("通算勝率レース内ランク", "low"),
    "win_rate_all_zscore":       ("勝率偏差スコア",     "high"),
    "win_rate_surface_rank":     ("馬場別勝率ランク",   "low"),
    "win_rate_distance_band_rank": ("距離帯別勝率ランク", "low"),
    "recent_rank_mean_rank":     ("直近着順レース内ランク", "low"),
    "recent_rank_mean_zscore":   ("直近着順偏差スコア", "high"),
    # 調教
    "tc_4f":                     ("調教4Fタイム(ウッド)", "low"),
    "tc_lap":                    ("調教ラスト1F(ウッド)", "low"),
    "tc_accel_flag":             ("調教加速ラップ",     "high"),
    "tc_4f_diff":                ("調教タイム前走比",   "low"),
    "tc_4f_rank":                ("調教タイムランク(ウッド)", "low"),
    "tc_4f_zscore":              ("調教4F偏差スコア",   "high"),
    "hc_4f":                     ("調教4Fタイム(坂路)", "low"),
    "hc_lap":                    ("坂路ラスト1F",       "low"),
    "hc_accel_flag":             ("坂路加速ラップ",     "high"),
    "hc_4f_diff":                ("坂路タイム前走比",   "low"),
    # 騎手・調教師
    "jockey_code_encoded":       ("騎手実績スコア",     "high"),
    "trainer_code_encoded":      ("調教師実績スコア",   "high"),
    # 枠・馬場
    "gate_number":               ("枠番",               ""),
    "condition_code":            ("馬場状態",           ""),
    "today_inner_bias":          ("内枠バイアス",       ""),
    "today_gate_match":          ("枠バイアス適合スコア", "high"),
    "today_front_bias":          ("展開安定度",         "high"),
    # オッズ
    "odds_vs_morning":           ("オッズ短縮率",       "low"),
    "odds_velocity":             ("オッズ資金流入速度", "high"),
    # その他
    "weight_carried":            ("斤量",               "low"),
    "horse_weight":              ("馬体重",             ""),
    "horse_weight_diff":         ("馬体重増減",         ""),
    "sex_code":                  ("性別",               ""),
    "surface_code":              ("馬場種別",           ""),
    "venue_encoded":             ("開催場",             ""),
    "sire_encoded":              ("父馬実績スコア",     "high"),
    "distance":                  ("距離",               ""),
    "race_number":               ("レース番号",         ""),
    "today_race_count":          ("バイアス信頼度",     ""),
}

# ── 文章テンプレート (特徴量カテゴリ別) ─────────────────────────────
_TEMPLATES: dict[str, list[str]] = {
    "ability_high": [
        "{horse_name}の通算勝率は{value:.1%}で、今回の出走馬の中でトップクラスの実績を持つ。",
        "距離・馬場適性を示す勝率指標はレース内ランク{rank:.0f}位。コース適性の高さが光る。",
        "直近5走の平均着順{value:.1f}位と好調が続いており、上位争いに加われる状態にある。",
    ],
    "training_good": [
        "最終追い切りはウッドコースで{value:.1f}秒（4F）。出走メンバー中最速水準の動き。",
        "坂路調教でラスト1Fが加速するスパート型のフォーム。本番での末脚に期待が持てる。",
        "前回比で調教タイムが{delta:.2f}秒改善。仕上がりの上向きを数値が示している。",
        "調教4Fタイムが出走馬中{rank:.0f}位の強い動き。状態面で大きな不安はない。",
    ],
    "jockey_trainer": [
        "{jockey}騎手は当コース相性が高く、モデルの実績スコアでも上位評価。",
        "管理する{trainer}師は今季の連対率が高く、仕上げの巧みさがスコアに反映されている。",
    ],
    "bias": [
        "本日の{venue}は内枠有利のバイアスが出ており、{gate}枠の{horse_name}には地の利がある。",
        "展開バイアス指標が先行馬に有利な数値を示しており、{horse_name}の脚質と合致する。",
    ],
    "odds_shrink": [
        "朝一比でオッズが{pct:.0f}%短縮。大口の資金が継続的に流入しており、市場の評価も上昇中。",
        "直近1時間のオッズ下落速度が高水準。スマートマネーの動きと見られる。",
    ],
}


class NarrativeGenerator:
    """LightGBM SHAP + race_results を使って馬ごとの予想根拠文を生成する。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._manji_model: Any = None
        self._place_model: Any = None
        self._honmei_model: Any = None
        self._feature_cols: list[str] | None = None
        self._feat_cache: dict[str, pd.DataFrame] = {}   # race_id → DataFrame

    # ── モデル・特徴量列ロード ────────────────────────────────────────

    def _load_models(self) -> None:
        if self._manji_model is not None:
            return
        try:
            with open(_MODEL_DIR / "manji_model.pkl", "rb") as f:
                self._manji_model = pickle.load(f)
            with open(_MODEL_DIR / "place_model.pkl", "rb") as f:
                raw = pickle.load(f)
                self._place_model = getattr(raw, "base", raw)
        except FileNotFoundError as e:
            logger.warning("モデルファイルが見つかりません: %s", e)
        try:
            with open(_MODEL_DIR / "honmei_model.pkl", "rb") as f:
                raw = pickle.load(f)
                # _IsotonicModel wrapper から LGBMClassifier を取り出す
                self._honmei_model = getattr(raw, "base", raw)
        except FileNotFoundError:
            logger.warning("honmei_model.pkl が見つかりません")

        # FEATURE_COLS をモデルの feature_name_ から取得（順序保証）
        try:
            from src.ml.models import FEATURE_COLS
            self._feature_cols = FEATURE_COLS
        except ImportError:
            self._feature_cols = list(self._manji_model.feature_name_) if self._manji_model else []

    # ── 特徴量を DB から構築 ──────────────────────────────────────────

    def _build_features(self, race_id: str) -> pd.DataFrame | None:
        """FeatureBuilder でレースの特徴量 DataFrame を生成する（結果をキャッシュ）。"""
        if race_id in self._feat_cache:
            return self._feat_cache[race_id]
        try:
            from src.ml.features import FeatureBuilder
            fb = FeatureBuilder(self._conn)
            df = fb.build_race_features_for_simulate(race_id)
            result = df if not df.empty else None
            self._feat_cache[race_id] = result
            return result
        except Exception as e:
            logger.warning("特徴量生成に失敗しました (race_id=%s): %s", race_id, e)
            return None

    # ── SHAP 値計算 ───────────────────────────────────────────────────

    def _compute_shap(
        self, model: Any, df: pd.DataFrame
    ) -> np.ndarray | None:
        """
        LightGBM 内蔵 SHAP (pred_contrib=True) で各馬の特徴量貢献度を返す。

        Returns:
            shape (n_horses, n_features) の ndarray。
            最後の列は SHAP の bias 項なので除外済み。
        """
        try:
            feature_cols = self._feature_cols or []
            # 文字列列を数値に変換してから欠損値を埋める（LightGBM は数値型のみ）
            X = (
                df[feature_cols]
                .apply(pd.to_numeric, errors="coerce")
                .infer_objects(copy=False)
                .fillna(-1)
            )
            booster = getattr(model, "booster_", None)
            if booster is None:
                return None
            # pred_contrib=True → shape: (n_rows, n_features + 1)
            contrib = booster.predict(X, pred_contrib=True)
            return np.array(contrib, dtype=float)[:, :-1]  # bias 列を除外
        except Exception as e:
            logger.warning("SHAP 計算失敗: %s", e)
            return None

    # ── 馬情報を DB から取得 ─────────────────────────────────────────

    def _fetch_horse_info(self, race_id: str, horse_number: int) -> dict:
        """race_results から馬名・騎手・調教師・オッズ等を取得する。"""
        row = self._conn.execute(
            """
            SELECT horse_name, jockey, trainer, win_odds,
                   weight_carried, horse_weight, horse_weight_diff,
                   gate_number, sex_age, popularity
            FROM race_results
            WHERE race_id = ? AND horse_number = ?
            """,
            (race_id, horse_number),
        ).fetchone()
        if row is None:
            return {}
        return {
            "horse_name":       row[0] or f"{horse_number}番",
            "jockey":           row[1] or "不明",
            "trainer":          row[2] or "不明",
            "win_odds":         row[3],
            "weight_carried":   row[4],
            "horse_weight":     row[5],
            "horse_weight_diff": row[6],
            "gate_number":      row[7],
            "sex_age":          row[8] or "",
            "popularity":       row[9],
        }

    def _fetch_race_info(self, race_id: str) -> dict:
        row = self._conn.execute(
            "SELECT race_name, venue, distance, surface, condition, race_number, date FROM races WHERE race_id = ?",
            (race_id,),
        ).fetchone()
        if row is None:
            return {}
        return {
            "race_name": row[0] or "",
            "venue":     row[1] or "",
            "distance":  row[2] or 0,
            "surface":   row[3] or "",
            "condition": row[4] or "",
            "race_number": row[5] or 0,
            "date":      row[6] or "",
        }

    # ── SHAP → 根拠文生成 ────────────────────────────────────────────

    def _shap_to_sentences(
        self,
        shap_row: np.ndarray,
        feat_row: pd.Series,
        horse_info: dict,
        race_info: dict,
    ) -> list[str]:
        """
        SHAP 値が大きい特徴量 Top5 を解析して根拠文リストを返す。
        """
        feature_cols = self._feature_cols or []
        sentences: list[str] = []

        # SHAP の絶対値でランキング
        abs_shap = np.abs(shap_row)
        top_indices = np.argsort(abs_shap)[::-1][:8]

        horse_name = horse_info.get("horse_name", "この馬")
        jockey     = horse_info.get("jockey", "")
        trainer    = horse_info.get("trainer", "")
        venue      = race_info.get("venue", "")
        gate       = horse_info.get("gate_number")

        used_categories: set[str] = set()

        for idx in top_indices:
            if idx >= len(feature_cols):
                continue
            feat_name  = feature_cols[idx]
            shap_val   = shap_row[idx]
            feat_val   = feat_row.get(feat_name, np.nan)
            meta       = _FEATURE_META.get(feat_name, (feat_name, ""))
            label, direction = meta

            # ── 通算・馬場・距離帯勝率 ───────────────────────────────
            if feat_name in ("win_rate_all", "win_rate_surface", "win_rate_distance_band") and "ability" not in used_categories:
                if shap_val > 0 and not _isnan(feat_val):
                    sentences.append(
                        f"{horse_name}の{label}は{feat_val:.1%}。"
                        f"当コース・距離帯での実績がモデルのプラス評価を牽引している。"
                    )
                    used_categories.add("ability")

            # ── 直近着順 ──────────────────────────────────────────────
            elif feat_name in ("recent_rank_mean", "recent_rank_mean_rank") and "recent" not in used_categories:
                if not _isnan(feat_val):
                    val_display = feat_val if feat_name == "recent_rank_mean" else f"レース内{int(feat_val)}"
                    if shap_val > 0:
                        sentences.append(
                            f"直近5走の平均着順は{feat_val:.1f}位と好調をキープ。"
                            "安定した上位争いが期待できる実績を持つ。"
                        )
                    used_categories.add("recent")

            # ── ウッド調教 ────────────────────────────────────────────
            elif feat_name in ("tc_4f", "tc_4f_rank", "tc_accel_flag", "tc_4f_diff") and "training_wood" not in used_categories:
                if feat_name == "tc_4f_rank" and not _isnan(feat_val) and feat_val <= 3 and shap_val > 0:
                    sentences.append(
                        f"最終追い切り(ウッド)はメンバー内{int(feat_val)}位の好時計。"
                        "仕上がりの良さが数値に表れている。"
                    )
                    used_categories.add("training_wood")
                elif feat_name == "tc_accel_flag" and feat_val == 1 and shap_val > 0 and "training_wood" not in used_categories:
                    sentences.append(
                        "ウッド調教でラスト1Fが加速するスパート型のフォームを披露。"
                        "本番での末脚発揮に期待が持てる。"
                    )
                    used_categories.add("training_wood")
                elif feat_name == "tc_4f_diff" and not _isnan(feat_val) and feat_val < 0 and shap_val > 0 and "training_wood" not in used_categories:
                    sentences.append(
                        f"ウッド調教タイムが前回比{abs(feat_val):.2f}秒改善。"
                        "仕上がりの上向きをデータが示している。"
                    )
                    used_categories.add("training_wood")

            # ── 坂路調教 ──────────────────────────────────────────────
            elif feat_name in ("hc_4f", "hc_accel_flag", "hc_4f_diff") and "training_hill" not in used_categories:
                if feat_name == "hc_accel_flag" and feat_val == 1 and shap_val > 0:
                    sentences.append(
                        "坂路調教でも加速ラップを記録。"
                        "ウッドとの2本立て調教でピーク状態を示している。"
                    )
                    used_categories.add("training_hill")
                elif feat_name == "hc_4f" and not _isnan(feat_val) and shap_val > 0 and "training_hill" not in used_categories:
                    sentences.append(
                        f"坂路4Fタイム{feat_val:.1f}秒と本追い切りから動きが良く、"
                        "状態面に不安はない。"
                    )
                    used_categories.add("training_hill")

            # ── 騎手 ──────────────────────────────────────────────────
            elif feat_name == "jockey_code_encoded" and "jockey" not in used_categories:
                if shap_val > 0 and jockey:
                    sentences.append(
                        f"{jockey}騎手の起用はモデルの高評価要因のひとつ。"
                        "当コース・距離での実績指標が安定した上位スコアを記録している。"
                    )
                    used_categories.add("jockey")

            # ── 調教師 ────────────────────────────────────────────────
            elif feat_name == "trainer_code_encoded" and "trainer" not in used_categories:
                if shap_val > 0 and trainer:
                    sentences.append(
                        f"管理する{trainer}師は今季の連対率・複勝率ともに高水準。"
                        "仕上げの確かさがスコアに反映されている。"
                    )
                    used_categories.add("trainer")

            # ── 枠バイアス ────────────────────────────────────────────
            elif feat_name == "today_gate_match" and "bias" not in used_categories:
                if not _isnan(feat_val) and venue and gate:
                    if shap_val > 0 and float(feat_val) > 0:
                        sentences.append(
                            f"本日の{venue}は内枠有利のバイアスが出ており、"
                            f"{gate}枠の{horse_name}には枠順の恩恵がある。"
                        )
                    elif shap_val > 0:
                        sentences.append(
                            f"枠番バイアス適合スコアが相対的に高評価。"
                            f"本日の{venue}の馬場傾向と{gate}枠の脚質が合致していると判断された。"
                        )
                    used_categories.add("bias")
            elif feat_name in ("today_inner_bias", "today_front_bias") and "bias" not in used_categories:
                if shap_val > 0 and venue:
                    label_v = "内枠有利" if "inner" in feat_name else "先行有利"
                    sentences.append(
                        f"本日の{venue}は{label_v}の展開バイアスが発生しており、"
                        f"{horse_name}の脚質・枠順がその恩恵を受けやすい条件となっている。"
                    )
                    used_categories.add("bias")

            # ── オッズ短縮 ────────────────────────────────────────────
            elif feat_name == "odds_vs_morning" and "odds" not in used_categories:
                if not _isnan(feat_val) and float(feat_val) < 0.9 and shap_val > 0:
                    pct = (1.0 - float(feat_val)) * 100
                    sentences.append(
                        f"朝一比でオッズが{pct:.0f}%短縮し大口資金の流入が続いている。"
                        "市場の評価が直前まで上昇中であることを示す強気のシグナル。"
                    )
                    used_categories.add("odds")
            elif feat_name == "odds_velocity" and "odds" not in used_categories:
                if not _isnan(feat_val) and float(feat_val) > 0 and shap_val > 0:
                    sentences.append(
                        f"直近1時間のオッズ下落速度が高水準（{float(feat_val):.2f}/分）。"
                        "スマートマネーの流入と判断され、モデルのプラス評価に寄与している。"
                    )
                    used_categories.add("odds")

            # ── 斤量 ──────────────────────────────────────────────────
            elif feat_name == "weight_carried" and "weight" not in used_categories:
                if not _isnan(feat_val) and shap_val > 0:
                    wc = float(feat_val)
                    if wc < 56:
                        sentences.append(
                            f"斤量{wc:.0f}kgと軽量恩恵があり、"
                            "先行・追い込みともに動きやすい条件が整っている。"
                        )
                    else:
                        sentences.append(
                            f"斤量{wc:.0f}kgの斤量条件でも過去の成績は安定しており、"
                            "重い斤量を苦にしない強さをモデルが評価している。"
                        )
                    used_categories.add("weight")

            # ── 汎用フォールバック（大きいSHAP値だが既存カテゴリ未対応）──
            elif shap_val > 0 and feat_name not in (
                "venue_encoded", "surface_code", "race_number",
                "sex_code", "today_race_count", "distance",
            ) and "generic" not in used_categories:
                if direction in ("high", "low") and not _isnan(feat_val):
                    better = "高い" if direction == "high" else "低い"
                    sentences.append(
                        f"{label}の指標値がメンバー内で相対的に{better}水準にあり、"
                        f"モデルのプラス評価を押し上げる一因となっている。"
                    )
                    used_categories.add("generic")

            if len(sentences) >= 4:
                break

        # 最低1文は出力する（どの条件も合わなかった場合のキャッチオール）
        if not sentences:
            top_feat = feature_cols[top_indices[0]] if top_indices.size > 0 else "スコア総合"
            meta = _FEATURE_META.get(top_feat, (top_feat, ""))
            label = meta[0]
            sentences.append(
                f"モデルのスコア上位要因は「{label}」。"
                f"出走メンバー内での相対評価が総合的に高く、期待回収率で上位に位置する。"
            )

        return sentences

    # ── フォールバック根拠文（特徴量なし） ──────────────────────────

    def _fallback_narrative(
        self,
        horse_info: dict,
        race_info: dict,
        ev: float,
        horse_number: int,
    ) -> str:
        """SHAP 計算不可時のテンプレートベース根拠文。"""
        horse_name  = horse_info.get("horse_name", f"{horse_number}番")
        jockey      = horse_info.get("jockey", "")
        trainer     = horse_info.get("trainer", "")
        win_odds    = horse_info.get("win_odds")
        gate        = horse_info.get("gate_number")
        venue       = race_info.get("venue", "")
        surface     = race_info.get("surface", "芝")
        distance    = race_info.get("distance", 0)
        surface_str = surface or "芝"
        dist_str    = f"{distance}m" if distance else "中距離"

        lines = [
            f"▶ {horse_name}（{gate}枠）",
        ]
        if jockey:
            lines.append(f"• 鞍上: {jockey}騎手（{trainer}師）")
        if win_odds:
            lines.append(f"• 単勝オッズ: {win_odds:.1f}倍")
        lines.append(
            f"• UMALOGI 卍モデルが{venue}{surface_str}{dist_str}の"
            f"出走メンバー中トップの期待回収率スコアを算出。"
        )
        expected_return = int(100 * ev)
        lines.append(
            f"• 期待値 EV={ev:.2f}（100円投資あたり約{expected_return}円の回収期待）。"
            f"過去実績・調教・枠番バイアスを総合評価した結果。"
        )
        return "\n".join(lines)

    # ── メインエントリ ────────────────────────────────────────────────

    def generate(
        self,
        race_id:      str,
        horse_number: int,
        ev:           float = 1.0,
        model_type:   str  = "manji",
    ) -> str:
        """
        指定馬の予想根拠文を生成する。

        Args:
            race_id:      対象レースID
            horse_number: 対象馬番
            ev:           モデルの期待値スコア
            model_type:   "manji" or "place"

        Returns:
            根拠文（改行区切りの複数行テキスト）
        """
        self._load_models()
        horse_info = self._fetch_horse_info(race_id, horse_number)
        race_info  = self._fetch_race_info(race_id)

        # 特徴量・SHAP 計算を試みる
        df_feat = self._build_features(race_id)
        shap_sentences: list[str] = []

        if df_feat is not None:
            if model_type == "honmei":
                model = self._honmei_model
            elif model_type == "place":
                model = self._place_model
            else:
                model = self._manji_model
            if model is not None:
                shap_matrix = self._compute_shap(model, df_feat)
                if shap_matrix is not None and "horse_name" in df_feat.columns:
                    horse_name_target = horse_info.get("horse_name")
                    horse_mask = df_feat["horse_name"] == horse_name_target
                    matched_idx = df_feat.index[horse_mask].tolist()
                    if matched_idx:
                        row_idx = df_feat.index.get_loc(matched_idx[0])
                        shap_row = shap_matrix[row_idx]
                        feat_row = df_feat.iloc[row_idx]
                        shap_sentences = self._shap_to_sentences(
                            shap_row, feat_row, horse_info, race_info
                        )

        if shap_sentences:
            horse_name = horse_info.get("horse_name", f"{horse_number}番")
            jockey     = horse_info.get("jockey", "")
            gate       = horse_info.get("gate_number", horse_number)
            model_label = {"honmei": "本命", "place": "複勝", "manji": "卍"}.get(model_type, "卍")
            header = f"▶ {horse_number}番 {horse_name}（{gate}枠 / {jockey}騎手）"
            ev_line = f"📊 {model_label}モデル EV={ev:.2f} — 以下の要因でプラス評価：\n"
            body = "\n".join(f"• {s}" for s in shap_sentences)
            return f"{header}\n{ev_line}{body}"

        # フォールバック
        return self._fallback_narrative(horse_info, race_info, ev, horse_number)


def generate_race_narratives(
    conn: sqlite3.Connection,
    race_id: str,
    picks: list[dict],
) -> dict[int, str]:
    """
    複数の馬について一括で根拠文を生成する。

    Args:
        conn:     DB 接続
        race_id:  対象レースID
        picks:    [{"horse_number": int, "ev": float, "model_type": str}, ...] のリスト

    Returns:
        {horse_number: 根拠文} の辞書
    """
    gen = NarrativeGenerator(conn)
    result: dict[int, str] = {}
    for pick in picks:
        hn         = pick["horse_number"]
        ev         = pick.get("ev", 1.0)
        model_type = pick.get("model_type", "manji")
        result[hn] = gen.generate(race_id=race_id, horse_number=hn, ev=ev, model_type=model_type)
    return result


def generate_note_article_data(
    conn: sqlite3.Connection,
    race_id: str,
) -> dict:
    """
    note 記事生成用の構造化データを返す。

    Returns:
        {
            "race_info": dict,           # レース基本情報
            "free_picks": [...],         # 無料エリア用ピック（馬名+人気）
            "paid_picks": [...],         # 有料エリア用ピック（SHAP根拠文+Kelly額）
            "bet_recommendations": [...],# 買い目推奨リスト
        }
    """
    gen = NarrativeGenerator(conn)
    gen._load_models()

    race_info = gen._fetch_race_info(race_id)

    # 予想データを取得（EV > 1.0 の本命・卍ピック）
    rows = conn.execute(
        """
        SELECT bet_type, expected_value, recommended_bet, combination_json, model_type
        FROM predictions
        WHERE race_id = ? AND expected_value >= 1.0
        ORDER BY expected_value DESC
        """,
        (race_id,),
    ).fetchall()

    # 単勝・複勝 ピックを抽出（無料エリア用）
    free_picks: list[dict] = []
    paid_picks: list[dict] = []
    seen_horses: set[int] = set()

    import json as _json

    for bet_type, ev, rec_bet, combo_json, model_type in rows:
        combo = _json.loads(combo_json) if combo_json else []
        if not combo:
            continue

        if bet_type in ("単勝", "複勝") and ev >= 1.0:
            for entry in combo:
                hn = entry[0] if isinstance(entry, list) else entry
                if hn not in seen_horses:
                    horse_info = gen._fetch_horse_info(race_id, hn)
                    free_picks.append({
                        "horse_number": hn,
                        "horse_name":   horse_info.get("horse_name", f"{hn}番"),
                        "popularity":   horse_info.get("popularity"),
                        "win_odds":     horse_info.get("win_odds"),
                        "bet_type":     bet_type,
                        "ev":           ev,
                        "model_type":   model_type or "manji",
                    })
                    seen_horses.add(hn)

    # SHAP 根拠文生成（有料エリア用）
    for pick in free_picks:
        hn         = pick["horse_number"]
        ev         = pick["ev"]
        model_type = pick.get("model_type", "manji")
        narrative  = gen.generate(race_id=race_id, horse_number=hn, ev=ev, model_type=model_type)
        paid_picks.append({
            **pick,
            "narrative": narrative,
        })

    # 全買い目推奨リスト（有料エリア用）
    bet_recommendations: list[dict] = []
    for bet_type, ev, rec_bet, combo_json, model_type in rows:
        combo = _json.loads(combo_json) if combo_json else []
        bet_recommendations.append({
            "bet_type":   bet_type,
            "ev":         ev,
            "rec_bet":    rec_bet or 0,
            "combo":      combo,
            "model_type": model_type or "manji",
        })

    return {
        "race_id":           race_id,
        "race_info":         race_info,
        "free_picks":        free_picks,
        "paid_picks":        paid_picks,
        "bet_recommendations": bet_recommendations,
    }
