"""
騎手・調教師 コース別成績スコア — AIウマスギ拡張因子 (Phase3)

jockey_stats / trainer_stats テーブルから当該騎手・調教師の
コース(venue×surface)別 win_rate と last_30d_win_rate を取得し、
総合スコアを [0, 1] に変換して返す。

スコア = win_rate × 0.6 + last_30d_win_rate × 0.4 を
全出走馬間でランク正規化してスコア化する。

出力列:
  jockey_course_score  (0.0〜1.0, 0.5 = 中立/データなし)
  trainer_course_score (0.0〜1.0, 0.5 = 中立/データなし)
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd
from scipy.stats import rankdata  # type: ignore[import]

logger = logging.getLogger(__name__)

_DEFAULT = 0.5
# win_rate 上限（外れ値クリップ: 0.5 = 50%以上はクリップ）
_WINRATE_CAP = 0.50


def _fetch_stats(
    conn: sqlite3.Connection,
    table: str,
    name_col: str,
    names: list[str],
    venues: list[str],
    surfaces: list[str],
) -> dict[tuple[str, str, str], float]:
    """(name, venue, surface) → スコア値の辞書を返す。

    スコア = win_rate × 0.6 + last_30d_win_rate × 0.4。
    win_rate は _WINRATE_CAP (0.50) でクリップされる。

    Args:
        conn: umalogi.db 接続。
        table: 検索対象テーブル名（jockey_stats / trainer_stats）。
        name_col: 名前カラム名（jockey_name / trainer_name）。
        names: 対象の騎手名または調教師名リスト。
        venues: 対象の開催場所リスト。
        surfaces: 対象の馬場種別リスト。

    Returns:
        (名前, 開催場所, 馬場種別) をキーとし、スコア値を値とする辞書。
        names が空の場合は空の辞書を返す。
    """
    if not names:
        return {}

    ph_n = ",".join("?" * len(names))
    ph_v = ",".join("?" * len(venues))
    ph_s = ",".join("?" * len(surfaces))

    rows = conn.execute(
        f"""
        SELECT {name_col}, venue, surface,
               win_rate, last_30d_win_rate
        FROM {table}
        WHERE {name_col} IN ({ph_n})
          AND venue IN ({ph_v})
          AND surface IN ({ph_s})
        """,
        (*names, *venues, *surfaces),
    ).fetchall()

    result: dict[tuple[str, str, str], float] = {}
    for name, venue, surface, wr, last_wr in rows:
        wr = min(float(wr or 0.0), _WINRATE_CAP)
        last_wr = min(float(last_wr or 0.0), _WINRATE_CAP)
        raw_score = wr * 0.6 + last_wr * 0.4
        result[(str(name), str(venue), str(surface))] = raw_score

    return result


def _normalize_scores(series: pd.Series) -> pd.Series:
    """ランク正規化: 同一レース内のスコアを 0〜1 に変換する。

    _DEFAULT (0.5) の行はデータなしとみなし変換対象から除外する。
    有効データが 1 件以下の場合は元の Series をそのまま返す。

    Args:
        series: スコア値を持つ Series（_DEFAULT はデータなしを意味する）。

    Returns:
        ランク正規化後の Series（0.0〜1.0 にクリップ済み）。
    """
    valid_mask = series != _DEFAULT
    if valid_mask.sum() < 2:
        return series
    series.values.copy()
    valid_idx = series[valid_mask].index
    ranks = rankdata(series[valid_mask].values, method="average")
    n = len(ranks)
    normalized = (ranks - 1) / (n - 1) if n > 1 else ranks * 0.0 + 0.5
    result = series.copy()
    result[valid_idx] = normalized
    return result.clip(0.0, 1.0)


def calc_jockey_course_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """騎手コース別成績スコアを DataFrame に追加して返す。

    jockey_stats テーブルからコース（venue × surface）別の勝率を取得し、
    レース内ランク正規化を経て jockey_course_score 列を追加する。

    Args:
        df: 必須列として race_id, jockey, venue, surface を含む DataFrame。
            venue / surface が欠損している場合は races テーブルから補完する。
        conn: umalogi.db 接続。

    Returns:
        元 df に jockey_course_score 列（0.0〜1.0、データなし = 0.5）を
        追加した DataFrame。
    """
    if df.empty:
        df["jockey_course_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # venue / surface が df にない場合は races テーブルから補完
    if "venue" not in df.columns or "surface" not in df.columns:
        df = _join_race_info(df, conn)

    jockeys = df["jockey"].dropna().unique().tolist() if "jockey" in df.columns else []
    venues = df["venue"].dropna().unique().tolist() if "venue" in df.columns else []
    surfaces = (
        df["surface"].dropna().unique().tolist() if "surface" in df.columns else []
    )

    score_map = _fetch_stats(
        conn, "jockey_stats", "jockey_name", jockeys, venues, surfaces
    )

    def _score(row: pd.Series) -> float:
        j = row.get("jockey")
        v = row.get("venue")
        s = row.get("surface")
        if not j or not v or not s:
            return _DEFAULT
        return score_map.get((str(j), str(v), str(s)), _DEFAULT)

    df["jockey_course_score"] = df.apply(_score, axis=1)
    # レース内でランク正規化（同じレース内の相対評価へ変換）
    if "race_id" in df.columns:
        df["jockey_course_score"] = df.groupby("race_id", group_keys=False)[
            "jockey_course_score"
        ].transform(_normalize_scores)
    return df


def calc_trainer_course_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """調教師コース別成績スコアを DataFrame に追加して返す。

    trainer_stats テーブルからコース（venue × surface）別の勝率を取得し、
    レース内ランク正規化を経て trainer_course_score 列を追加する。

    Args:
        df: 必須列として race_id, trainer, venue, surface を含む DataFrame。
            venue / surface が欠損している場合は races テーブルから補完する。
        conn: umalogi.db 接続。

    Returns:
        元 df に trainer_course_score 列（0.0〜1.0、データなし = 0.5）を
        追加した DataFrame。
    """
    if df.empty:
        df["trainer_course_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    if "venue" not in df.columns or "surface" not in df.columns:
        df = _join_race_info(df, conn)

    trainers = (
        df["trainer"].dropna().unique().tolist() if "trainer" in df.columns else []
    )
    venues = df["venue"].dropna().unique().tolist() if "venue" in df.columns else []
    surfaces = (
        df["surface"].dropna().unique().tolist() if "surface" in df.columns else []
    )

    score_map = _fetch_stats(
        conn, "trainer_stats", "trainer_name", trainers, venues, surfaces
    )

    def _score(row: pd.Series) -> float:
        t = row.get("trainer")
        v = row.get("venue")
        s = row.get("surface")
        if not t or not v or not s:
            return _DEFAULT
        return score_map.get((str(t), str(v), str(s)), _DEFAULT)

    df["trainer_course_score"] = df.apply(_score, axis=1)
    if "race_id" in df.columns:
        df["trainer_course_score"] = df.groupby("race_id", group_keys=False)[
            "trainer_course_score"
        ].transform(_normalize_scores)
    return df


def _join_race_info(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """races テーブルから venue / surface 列を df に LEFT JOIN して返す。

    Args:
        df: race_id 列を含む DataFrame。
        conn: umalogi.db 接続。

    Returns:
        venue / surface 列を追加した DataFrame。
    """
    race_ids = df["race_id"].unique().tolist()
    ph = ",".join("?" * len(race_ids))
    race_info = conn.execute(
        f"SELECT race_id, venue, surface FROM races WHERE race_id IN ({ph})",
        race_ids,
    ).fetchall()
    rdf = pd.DataFrame(race_info, columns=["race_id", "venue", "surface"])
    return df.merge(rdf, on="race_id", how="left")
