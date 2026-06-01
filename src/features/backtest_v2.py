"""再シミュレーション(v2)用の特徴量アセンブリ（FEATURE_COLS 非破壊）。

稼働中モデル(v1.2.0)の入力 ``src.ml.models.FEATURE_COLS``(69列) を**変更せず**、
次期特徴量（PCI・加速力スコア・上がり3F秒）を **別リスト** として結合し、
再学習/再シミュレーション用の DataFrame を組み立てる前処理を提供する。

ここで作る ``FEATURE_COLS_V2`` は本番に結線されない（再学習で明示採用するまで
評価専用）。これにより「並行計算・安全な準備」を担保する。
"""

from __future__ import annotations

import sqlite3

import pandas as pd

from src.features.acceleration import build_acceleration_features

# 次期特徴量名（race_results.last_3f → acceleration.py で計算）
# W-001: pci(馬別) / acceleration_score / last_3f_sec、W-002: race_pci(レース別ペース指数)
ACCEL_FEATURE_COLS: list[str] = [
    "pci",
    "acceleration_score",
    "last_3f_sec",
    "race_pci",
]


def build_feature_cols_v2(base_cols: list[str]) -> list[str]:
    """base_cols（=FEATURE_COLS）に加速力系特徴量を**非破壊で**連結した新リストを返す。

    入力 ``base_cols`` は変更しない（コピーを返す）。重複は除外する。
    """
    out = list(base_cols)  # コピー（入力非破壊）
    for c in ACCEL_FEATURE_COLS:
        if c not in out:
            out.append(c)
    return out


def attach_acceleration_features(
    base_df: pd.DataFrame, conn: sqlite3.Connection, race_id: str
) -> pd.DataFrame:
    """base_df（1レースの馬別特徴量・horse_number を持つ）に加速力特徴量を左結合する。

    base_df は変更せず新しい DataFrame を返す。加速力特徴量が無い馬は
    pci=NaN / acceleration_score=0.0 / last_3f_sec=NaN で埋まる（非破壊・安全）。

    Args:
        base_df: 少なくとも "horse_number" 列を持つ DataFrame。
        conn: DB 接続。
        race_id: 対象レース ID。

    Returns:
        base_df のコピー＋[pci, acceleration_score, last_3f_sec] 列。
    """
    accel = build_acceleration_features(conn, race_id)
    merged = base_df.copy()
    if "horse_number" not in merged.columns or accel.empty:
        for c in ACCEL_FEATURE_COLS:
            if c not in merged.columns:
                merged[c] = 0.0 if c == "acceleration_score" else pd.NA
        return merged
    merged = merged.merge(
        accel[["horse_number", "last_3f_sec", "pci", "acceleration_score", "race_pci"]],
        on="horse_number",
        how="left",
    )
    # 結合できなかった馬の acceleration_score は中立 0.0
    merged["acceleration_score"] = merged["acceleration_score"].fillna(0.0)
    return merged
