"""
legacy_logic vs umasugi_engine 並列比較ラッパー

同一レースに対して両エンジンの予想を並列出力し、
EV の差分・世論ペナルティ・新規因子スコアを横断比較できる DataFrame を生成する。

将来的に /api/compare/<race_id> エンドポイントで提供予定。
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)


def compare_predictions(
    race_id: str,
    conn: sqlite3.Connection,
    *,
    legacy_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """legacy_logic と umasugi_engine の予想を並列比較する DataFrame を返す。

    Args:
        race_id: 対象レース ID。
        conn: umalogi.db 接続。
        legacy_df: legacy_logic が既に算出した特徴量 DataFrame。
            未指定の場合は内部で FeatureBuilder を呼び出す。

    Returns:
        列 horse_number, horse_name, legacy_ev, umasugi_ev, ev_delta,
        track_style_score, turf_type_score, opinion_pressure_score,
        crowd_ev_penalty, umasugi_score, recommendation を持つ DataFrame。
        FeatureBuilder が失敗した場合は空の DataFrame を返す。
    """
    from .engine import UmasugiEngine

    engine = UmasugiEngine(conn)

    # legacy_df が未指定の場合は FeatureBuilder で生成
    if legacy_df is None:
        try:
            from src.ml.features import FeatureBuilder

            builder = FeatureBuilder(conn)
            legacy_df = builder.build_race_features(race_id)
        except Exception as exc:
            logger.error("FeatureBuilder 失敗: %s", exc)
            return pd.DataFrame()

    # umasugi_engine で拡張スコアを計算
    umasugi_df = engine.score(legacy_df)

    # ── 比較用 DataFrame を構築 ──────────────────────────────────────────
    result_cols = [
        "horse_number",
        "horse_name",
        "legacy_ev",
        "umasugi_ev",
        "track_style_score",
        "turf_type_score",
        "opinion_pressure_score",
        "crowd_ev_penalty",
        "umasugi_score",
    ]

    out = pd.DataFrame()
    for col in result_cols:
        if col in umasugi_df.columns:
            out[col] = umasugi_df[col].values
        elif col in legacy_df.columns:
            out[col] = legacy_df[col].values
        else:
            out[col] = None

    # ── ev_delta と推奨判定 ───────────────────────────────────────────────
    legacy_ev = out.get("legacy_ev", pd.Series(0.0, index=out.index)).fillna(0.0)
    umasugi_ev = out.get("umasugi_ev", pd.Series(0.0, index=out.index)).fillna(0.0)

    out["ev_delta"] = umasugi_ev - legacy_ev

    def _recommend(row: pd.Series) -> str:
        """EV デルタから推奨エンジンを返す。

        Args:
            row: ev_delta 列を含む DataFrame 行。

        Returns:
            "agree" / "umasugi_better" / "legacy_better" のいずれか。
        """
        if abs(row["ev_delta"]) < 0.05:
            return "agree"
        if row["ev_delta"] > 0:
            return "umasugi_better"
        return "legacy_better"

    out["recommendation"] = out.apply(_recommend, axis=1)

    logger.info(
        "比較完了 race_id=%s: %d 頭 / umasugi_better=%d / legacy_better=%d",
        race_id,
        len(out),
        (out["recommendation"] == "umasugi_better").sum(),
        (out["recommendation"] == "legacy_better").sum(),
    )
    return out
