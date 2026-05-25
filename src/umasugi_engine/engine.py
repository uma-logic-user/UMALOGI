"""
UmasugiEngine — AIウマスギ次世代予測エンジン（メインエントリポイント）

legacy_logic (src/ml/) の FeatureBuilder + UScoreEngine をインポートし、
拡張因子（小回り・野芝/洋芝・世論分析）と EV フィルターを適用する。

使用例:
    import sqlite3
    from src.umasugi_engine import UmasugiEngine

    conn = sqlite3.connect("data/umalogi.db")
    engine = UmasugiEngine(conn)
    df = engine.predict("202605040811")
    print(df[["horse_name", "legacy_ev", "umasugi_ev", "ev_delta"]])
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

from .ev_filter import apply_ev_filter
from .scorer import calc_umasugi_score

logger = logging.getLogger(__name__)


class UmasugiEngine:
    """AIウマスギ拡張予測エンジン。

    legacy_logic の FeatureBuilder + UScoreEngine をラップし、
    拡張因子スコアリングと EV フィルターを統合する。

    Attributes:
        _conn: umalogi.db への SQLite 接続。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ── パブリック API ────────────────────────────────────────────────────

    def predict(self, race_id: str) -> pd.DataFrame:
        """指定レースに対して umasugi_engine の予想を生成する。

        処理フロー:
            1. legacy FeatureBuilder で基本特徴量を生成。
            2. 拡張因子（track_style, turf_type 等）を追加。
            3. 世論分析 EV フィルターを適用。
            4. 最終 DataFrame を返す。

        Args:
            race_id: 対象レース ID（YYYYMMDDJJRR 形式）。

        Returns:
            列 horse_number, horse_name, legacy_ev, umasugi_ev, ev_delta,
            track_style_score, turf_type_score, umasugi_score,
            opinion_pressure_score, crowd_ev_penalty 等を持つ DataFrame。
            FeatureBuilder が空を返した場合は空の DataFrame を返す。
        """
        # ── Step 1: legacy FeatureBuilder ────────────────────────────────
        try:
            from src.ml.features import FeatureBuilder
        except ImportError:
            from ml.features import FeatureBuilder  # type: ignore[no-redef]

        builder = FeatureBuilder(self._conn)
        df = builder.build_race_features(race_id)

        if df.empty:
            logger.warning(
                "FeatureBuilder が空 DataFrame を返しました: race_id=%s", race_id
            )
            return df

        return self.score(df)

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """既存の特徴量 DataFrame に umasugi 拡張スコアを適用して返す。

        predict() とは異なり、外部から生成済みの DataFrame を受け取る。
        comparator.py からの呼び出しに使用する。

        Args:
            df: FeatureBuilder が生成した 1行1頭の特徴量 DataFrame。
                u_score 列が含まれている必要がある（UScoreEngine 適用済み）。

        Returns:
            元 df に各拡張スコア列・umasugi_score・ev_delta を追加した DataFrame。
        """
        df = df.copy()

        # ── Step 2: 拡張因子 + umasugi_score 計算 ────────────────────────
        df = calc_umasugi_score(df, self._conn)

        # ── Step 3: EV フィルター（世論ペナルティ）を適用 ────────────────
        df = apply_ev_filter(df)

        # ── Step 4: ev_delta を計算 ───────────────────────────────────────
        if "legacy_ev" in df.columns and "umasugi_ev" in df.columns:
            df["ev_delta"] = df["umasugi_ev"] - df["legacy_ev"]

        logger.info(
            "UmasugiEngine.score: %d 頭 / umasugi_score 平均 %.3f / ev_delta 平均 %.3f",
            len(df),
            df.get("umasugi_score", pd.Series([0])).mean(),
            df.get("ev_delta", pd.Series([0])).mean(),
        )
        return df
