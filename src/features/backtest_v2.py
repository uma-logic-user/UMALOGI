"""再シミュレーション(v2)用の特徴量アセンブリ（FEATURE_COLS 非破壊・リークフリー）。

稼働中モデル(v1.x)の入力 ``src.ml.models.FEATURE_COLS``(69列) を**変更せず**、
再学習/再シミュレーション用の特徴量を別リストとして結合する前処理を提供する。

⚠️ データリーク対策（W-070 / W-001 監査・2026-06-07）:
  加速力系（pci / acceleration_score / last_3f_sec / race_pci）は
  ``build_acceleration_features`` が **予測対象レース自身の上がり3F（=そのレースの
  結果）** から計算する **ポストレース特徴量** である。これらを予測モデルの入力に
  使うと「未来（当該レース結果）の混入＝ターゲットリーク」になり、的中率/ROIが
  非現実的に膨張する（実測: ROI 230% の偽陽性を検出・除外）。

  したがって本モジュールでは:
    - 予測用の既定特徴量 ``build_feature_cols_v2`` は **リークフリー列のみ**
      （前走詳細 ``prerun`` ＋ 血統TE ``pedigree_te``）を追加する。
    - ポストレース加速力列は ``POSTRACE_LEAK_COLS`` として明示分離し、
      ``include_postrace=True`` を指定したレース後分析・ラベル生成時のみ使う。
"""

from __future__ import annotations

import sqlite3

import pandas as pd

from src.features.acceleration import build_acceleration_features
from src.features.pedigree_te import PEDIGREE_FEATURE_COLS
from src.features.prerun import PRERUN_FEATURE_COLS

# ⚠️ ポストレース（=予測対象レース自身の結果）由来。予測入力に使うとリーク。
#    レース後の分析・ペースラベル・「次走の前走特徴量」生成にのみ使用可。
POSTRACE_LEAK_COLS: list[str] = [
    "pci",
    "acceleration_score",
    "last_3f_sec",
    "race_pci",
]

# 後方互換エイリアス（既存 import 名を壊さない）。中身はポストレース列＝リーク注意。
ACCEL_FEATURE_COLS: list[str] = POSTRACE_LEAK_COLS

# リークフリーな次期予測特徴量（過去出走のみ参照の前走系 ＋ cutoff前fitの血統TE）。
LEAKFREE_NEW_COLS: list[str] = PRERUN_FEATURE_COLS + PEDIGREE_FEATURE_COLS


def build_feature_cols_v2(
    base_cols: list[str], *, include_postrace: bool = False
) -> list[str]:
    """base_cols（=FEATURE_COLS）に次期特徴量を**非破壊で**連結した新リストを返す。

    既定（``include_postrace=False``）では **リークフリー列のみ**（前走詳細＋血統TE）を
    追加する。これが予測モデル再学習で使うべき安全なリスト。

    ``include_postrace=True`` のときに限り、ポストレース加速力列（``POSTRACE_LEAK_COLS``）も
    追加する。これは **レース後分析・ラベル生成専用** であり、予測モデルの入力に使っては
    ならない（当該レース結果のリークになる）。

    入力 ``base_cols`` は変更しない（コピーを返す）。重複は除外する。
    """
    out = list(base_cols)  # コピー（入力非破壊）
    additions = list(LEAKFREE_NEW_COLS)
    if include_postrace:
        additions += POSTRACE_LEAK_COLS
    for c in additions:
        if c not in out:
            out.append(c)
    return out


def attach_acceleration_features(
    base_df: pd.DataFrame, conn: sqlite3.Connection, race_id: str
) -> pd.DataFrame:
    """base_df に**ポストレース**加速力特徴量を左結合する（レース後分析専用）。

    ⚠️ リーク注意: ここで付与する pci / acceleration_score / last_3f_sec / race_pci は
    **対象レース自身の結果**（上がり3F）から算出される。**予測モデルの入力に使っては
    ならない**（当該レース結果のリーク）。次走以降の「前走特徴量」を作る素材、または
    レースのペース性質を事後分析する目的にのみ用いること。予測用のリークフリーな
    前走加速力は ``src.features.prerun``（prev_last_3f_sec 等）を使う。

    base_df は変更せず新しい DataFrame を返す。加速力特徴量が無い馬は
    pci=NaN / acceleration_score=0.0 / last_3f_sec=NaN で埋まる（非破壊・安全）。

    Args:
        base_df: 少なくとも "horse_number" 列を持つ DataFrame。
        conn: DB 接続。
        race_id: 対象レース ID。

    Returns:
        base_df のコピー＋POSTRACE_LEAK_COLS 列。
    """
    accel = build_acceleration_features(conn, race_id)
    merged = base_df.copy()
    if "horse_number" not in merged.columns or accel.empty:
        for c in POSTRACE_LEAK_COLS:
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
