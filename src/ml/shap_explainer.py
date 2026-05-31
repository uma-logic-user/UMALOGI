"""src/ml/shap_explainer.py

LightGBM モデルに対して SHAP TreeExplainer を使い、
各馬の特徴量寄与度（上位 N 件）を計算するユーティリティ。

DB 保存フォーマット:
  prediction_horses.shap_json = '{"jockey_win_rate_90d": 0.45, "tc_4f": -0.12, ...}'
  （絶対値降順・上位 top_n 件・符号保持）

shap パッケージが未インストールの場合は空の結果を返し、
予測パイプラインを止めない設計にしている。
"""
from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── 特徴量名 → 日本語ラベル ────────────────────────────────────
FEATURE_LABEL_JP: dict[str, str] = {
    "weight_carried":              "斤量",
    "horse_weight":                "馬体重",
    "win_rate_all":                "通算勝率",
    "win_rate_surface":            "馬場別勝率",
    "win_rate_distance_band":      "距離帯別勝率",
    "recent_rank_mean":            "直近5走平均着順",
    "surface_code":                "馬場コード",
    "sex_code":                    "性別コード",
    "venue_encoded":               "開催場コード",
    "sire_encoded":                "父馬コード",
    "distance":                    "距離",
    "horse_weight_diff":           "前走比体重差",
    "gate_number":                 "枠番",
    "condition_code":              "馬場状態",
    "race_number":                 "レース番号",
    "jockey_code_encoded":         "騎手コード",
    "trainer_code_encoded":        "調教師コード",
    "tc_4f":                       "ウッド4Fタイム",
    "tc_lap":                      "ウッドラスト1Fタイム",
    "tc_accel_flag":               "ウッド加速ラップ",
    "tc_4f_diff":                  "ウッド前回比4F差",
    "hc_4f":                       "坂路4Fタイム",
    "hc_lap":                      "坂路ラスト1Fタイム",
    "hc_accel_flag":               "坂路加速ラップ",
    "hc_4f_diff":                  "坂路前回比4F差",
    "win_rate_all_rank":           "通算勝率ランク",
    "win_rate_all_zscore":         "通算勝率偏差値",
    "win_rate_surface_rank":       "馬場別勝率ランク",
    "win_rate_distance_band_rank": "距離帯別勝率ランク",
    "recent_rank_mean_rank":       "直近着順ランク",
    "recent_rank_mean_zscore":     "直近着順偏差値",
    "tc_4f_rank":                  "調教タイムランク",
    "tc_4f_zscore":                "調教タイム偏差値",
    "today_inner_bias":            "当日内枠バイアス",
    "today_front_bias":            "当日人気馬勝率",
    "today_race_count":            "当日集計レース数",
    "today_gate_match":            "当日枠バイアス相性",
    "odds_vs_morning":             "朝一比オッズ変動",
    "odds_velocity":               "オッズ下落速度",
    "uf_win_rate_all":             "U:通算勝率スコア",
    "uf_win_rate_surface":         "U:馬場別勝率スコア",
    "uf_win_rate_distance":        "U:距離帯別勝率スコア",
    "uf_recent_rank":              "U:直近着順スコア",
    "uf_rank_trend":               "U:着順改善トレンド",
    "uf_rest_days":                "U:休養日数スコア",
    "uf_jockey_win_rate":          "U:騎手勝率スコア",
    "uf_trainer_win_rate":         "U:調教師勝率スコア",
    "uf_jockey_horse_combo":       "U:騎手×馬コンビスコア",
    "uf_jockey_venue":             "U:騎手×会場スコア",
    "uf_gate_fit":                 "U:枠番×馬場適性スコア",
    "uf_venue_win_rate":           "U:会場勝率スコア",
    "uf_east_west_match":          "U:厩舎東西一致スコア",
    "uf_tc_speed":                 "U:ウッドスピード指数スコア",
    "uf_hc_speed":                 "U:坂路スピード指数スコア",
    "uf_sire_distance":            "U:父馬距離適性スコア",
    "uf_bms_surface":              "U:母父馬場適性スコア",
    "uf_father_sire":              "U:父の父スコア",
    "uf_crowd_bias":               "U:大衆心理乖離スコア",
    "u_score":                     "U合成スコア",
    "days_since_last_race":        "前走からの日数",
    "jockey_win_rate_90d":         "騎手直近90日勝率",
    "trainer_win_rate_90d":        "調教師直近90日勝率",
    "jockey_horse_combo_rate":     "騎手×馬コンビ勝率",
    "jockey_venue_win_rate":       "騎手×会場勝率",
    "venue_win_rate":              "当該会場勝率",
    "tc_speed_index":              "ウッドスピード指数",
    "hc_speed_index":              "坂路スピード指数",
    "crowd_bias_ratio":            "大衆心理乖離率",
    "x_consensus_score":           "X予想家コンセンサス",
}


def feature_label(name: str) -> str:
    """内部特徴量名を日本語ラベルに変換する（未登録は内部名をそのまま返す）。"""
    return FEATURE_LABEL_JP.get(name, name)


def compute_shap_top(
    model: Any,
    X: pd.DataFrame,
    top_n: int = 10,
) -> list[dict[str, float]]:
    """各馬の SHAP 上位特徴量を計算して返す。

    Args:
        model:  HonmeiModel / ManjiModel 等のラッパー。
                内部に ``_model`` 属性を持ち、さらに ``booster_`` プロパティを
                持つことを期待する（LightGBM booster）。
        X:      ``_safe_feature_matrix()`` 適用済みの特徴量 DataFrame。
                行数 = 出走馬数、列 = FEATURE_COLS。
        top_n:  取得する上位特徴量数（デフォルト 10）。

    Returns:
        馬ごとの辞書リスト（X の行順、長さ = len(X)）。
        各辞書は ``{"feature_name": shap_value, ...}`` 形式で
        絶対値降順上位 top_n 件。計算失敗時は空辞書のリストを返す。
    """
    empty: list[dict[str, float]] = [{} for _ in range(len(X))]

    try:
        import shap as _shap  # optional dependency
    except ImportError:
        logger.debug("shap 未インストール — SHAP 計算をスキップします")
        return empty

    try:
        inner = getattr(model, "_model", None)
        if inner is None:
            logger.debug("SHAP: _model 属性なし → スキップ")
            return empty

        booster = getattr(inner, "booster_", None)
        if booster is None:
            logger.debug("SHAP: booster_ 属性なし → スキップ")
            return empty

        explainer = _shap.TreeExplainer(booster)
        sv: Any = explainer.shap_values(X)

        # LightGBM 2値分類は list[(n,f),(n,f)] を返す場合がある
        if isinstance(sv, list):
            sv = sv[1]  # class=1 (win/high-payout) の寄与値

        sv_arr = np.asarray(sv, dtype=float)
        feature_names: list[str] = list(X.columns)
        result: list[dict[str, float]] = []

        for row_sv in sv_arr:
            top_idx = np.argsort(np.abs(row_sv))[::-1][:top_n]
            top: dict[str, float] = {}
            for idx in top_idx:
                top[feature_names[idx]] = float(row_sv[idx])
            result.append(top)

        return result

    except Exception as exc:
        logger.warning("SHAP 計算失敗（予測は続行）: %s", exc)
        return empty


def shap_to_json(shap_map: dict[str, float]) -> str | None:
    """SHAP 辞書を JSON 文字列に変換する。空辞書は None を返す。"""
    if not shap_map:
        return None
    return json.dumps(shap_map, ensure_ascii=False)


def build_shap_map(
    model: Any,
    X: pd.DataFrame,
    df: pd.DataFrame,
    top_n: int = 10,
) -> dict[int, str | None]:
    """horse_number → shap_json 文字列のマップを構築する。

    Args:
        model:  HonmeiModel / ManjiModel 等のラッパー。
        X:      ``_safe_feature_matrix(df)`` の結果。
        df:     特徴量生成済みの出走馬 DataFrame（horse_number 列を含む）。
        top_n:  取得する上位特徴量数。

    Returns:
        ``{horse_number: shap_json_str_or_None}`` の辞書。
        horse_number が df にない場合は row position をキーとする。
    """
    shap_list = compute_shap_top(model, X, top_n=top_n)

    result: dict[int, str | None] = {}
    df_reset = df.reset_index(drop=True)

    for i, shap_map in enumerate(shap_list):
        if i >= len(df_reset):
            break
        row = df_reset.iloc[i]
        hn_raw = row.get("horse_number") if hasattr(row, "get") else getattr(row, "horse_number", None)
        key = int(hn_raw) if hn_raw is not None else i
        result[key] = shap_to_json(shap_map)

    return result
