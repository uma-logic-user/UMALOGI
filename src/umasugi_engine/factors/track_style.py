"""
小回り適性ファクター — AIウマスギ拡張因子

track_direction カラムが全行空値のため、会場名 + 距離ヒューリスティックで
「小回りコース」を定義し、馬ごとの小回り実績から適性スコアを算出する。

小回り定義:
  確定的小回り : 小倉・函館・札幌・福島
  条件的小回り : 中山（芝/ダート 1800m 以下）・阪神（芝 1400m 以下）
  大回り       : 東京・京都・中京・新潟

出力列: track_style_score (0.0〜1.0)
  ・対象レースが小回りコースの場合 → 小回り勝率 / 全体勝率
  ・対象レースが大回りコースの場合 → 大回り勝率 / 全体勝率
  ・判定不能（実績なし・ダートのみ等）→ 0.5（中立）
"""

from __future__ import annotations

import logging
import sqlite3

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── コース分類定数 ────────────────────────────────────────────────────────
_KOMAWARI_VENUES: frozenset[str] = frozenset({"小倉", "函館", "札幌", "福島"})
_OOMAWARI_VENUES: frozenset[str] = frozenset({"東京", "京都", "中京", "新潟"})
# 中山・阪神は距離で条件判定（それ以外は 0.5 扱い）

_MIN_SAMPLES = 3  # 最低サンプル数未満 → 0.5 補填


def _is_komawari(venue: str, surface: str, distance: int) -> bool | None:
    """レースが小回りコースかを判定する。

    会場名と距離のヒューリスティックで判定する。
    中山・阪神は条件付きで内回りと判定する。

    Args:
        venue: 開催場所名（例: "小倉", "東京"）。
        surface: 馬場種別（"芝" / "ダート"）。
        distance: レース距離（メートル）。

    Returns:
        True: 小回りコース。
        False: 大回りコース。
        None: 判定不能（条件的小回りの境界外など）。
    """
    if venue in _KOMAWARI_VENUES:
        return True
    if venue in _OOMAWARI_VENUES:
        return False
    # 中山: 芝/ダート 1800m 以下は内回り扱い
    if venue == "中山" and distance <= 1800:
        return True
    # 阪神: 芝 1400m 以下は内回り扱い
    if venue == "阪神" and surface == "芝" and distance <= 1400:
        return True
    return None  # 判定不能


def calc_track_style_score(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    """小回り適性スコアを DataFrame に追加して返す。

    対象レースの会場・馬場・距離を races テーブルから取得し、
    _is_komawari() で小回り/大回りを判定する。次に各馬の過去成績を
    同種別に集計し、適性比率を [0, 1] に正規化してスコア化する。

    Args:
        df: FeatureBuilder（または UmasugiEngine）が返す 1行1頭の特徴量 DataFrame。
            必須列: race_id, horse_id。
        conn: umalogi.db 接続。

    Returns:
        元 df に track_style_score 列（0.0〜1.0、判定不能 = 0.5）を
        追加した DataFrame。一時列（_venue, _surface, _distance, _is_komawari）
        は削除済み。
    """
    if df.empty:
        df["track_style_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # ── 対象レースの会場・馬場・距離を取得 ──────────────────────────────
    race_ids = df["race_id"].unique().tolist()
    placeholders = ",".join("?" * len(race_ids))
    race_info = conn.execute(
        f"SELECT race_id, venue, surface, distance FROM races WHERE race_id IN ({placeholders})",
        race_ids,
    ).fetchall()
    race_map: dict[str, tuple[str, str, int]] = {
        r[0]: (r[1] or "", r[2] or "", int(r[3] or 0)) for r in race_info
    }

    # ── 対象レースごとの小回り判定 ───────────────────────────────────────
    df["_venue"] = df["race_id"].map(lambda x: race_map.get(x, ("", "", 0))[0])
    df["_surface"] = df["race_id"].map(lambda x: race_map.get(x, ("", "", 0))[1])
    df["_distance"] = df["race_id"].map(lambda x: race_map.get(x, ("", "", 0))[2])
    df["_is_komawari"] = df.apply(
        lambda row: _is_komawari(row["_venue"], row["_surface"], row["_distance"]),
        axis=1,
    )

    # ── 各馬の過去成績を小回り/大回り別に集計 ───────────────────────────
    horse_ids = df["horse_id"].dropna().unique().tolist()
    if not horse_ids:
        df["track_style_score"] = 0.5
        return _drop_tmp(df)

    # 基準日（最古の race_id から推定）
    base_date = _earliest_race_date(df)

    ph = ",".join("?" * len(horse_ids))
    rows = conn.execute(
        f"""
        SELECT
            rr.horse_id,
            r.venue,
            r.surface,
            r.distance,
            CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END AS is_win
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.horse_id IN ({ph})
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
          AND r.date < ?
          AND r.venue IS NOT NULL AND r.venue != ''
        """,
        horse_ids + [base_date],
    ).fetchall()

    # horse_id → {komawari_races, komawari_wins, oomawari_races, oomawari_wins}
    stats: dict[str, dict[str, int]] = {}
    for horse_id, venue, surface, distance, is_win in rows:
        s = stats.setdefault(horse_id, {"k_n": 0, "k_w": 0, "o_n": 0, "o_w": 0})
        ko = _is_komawari(venue or "", surface or "", int(distance or 0))
        if ko is True:
            s["k_n"] += 1
            s["k_w"] += is_win
        elif ko is False:
            s["o_n"] += 1
            s["o_w"] += is_win

    # ── スコア算出 ───────────────────────────────────────────────────────
    def _score(row: pd.Series) -> float:
        hid = row["horse_id"]
        is_ko = row["_is_komawari"]

        if is_ko is None or hid not in stats:
            return 0.5

        s = stats[hid]
        total_n = s["k_n"] + s["o_n"]
        if total_n < _MIN_SAMPLES:
            return 0.5

        total_win_rate = (s["k_w"] + s["o_w"]) / total_n

        if is_ko is True:
            if s["k_n"] < _MIN_SAMPLES:
                return 0.5
            ko_win_rate = s["k_w"] / s["k_n"]
        else:
            if s["o_n"] < _MIN_SAMPLES:
                return 0.5
            ko_win_rate = s["o_w"] / s["o_n"]

        if total_win_rate == 0:
            return 0.5

        # 適性比率を [0, 1] へ正規化（比率 0.3〜3.0 の範囲でクリップ）
        ratio = np.clip(ko_win_rate / total_win_rate, 0.3, 3.0)
        return float((ratio - 0.3) / (3.0 - 0.3))

    df["track_style_score"] = df.apply(_score, axis=1)
    return _drop_tmp(df)


# ── ヘルパー ──────────────────────────────────────────────────────────────


def _earliest_race_date(df: pd.DataFrame) -> str:
    """df 内の最古の race_id から 'YYYY-MM-DD' 基準日を返す（時系列リーク防止）。

    Args:
        df: race_id 列を含む DataFrame。

    Returns:
        最古の race_id 先頭 8 文字から生成した 'YYYY-MM-DD' 形式の日付文字列。
        変換に失敗した場合は '9999-12-31' を返す。
    """
    try:
        rid = df["race_id"].min()
        return f"{rid[:4]}-{rid[4:6]}-{rid[6:8]}"
    except Exception:
        return "9999-12-31"


def _drop_tmp(df: pd.DataFrame) -> pd.DataFrame:
    """一時作業列を DataFrame から削除して返す。

    Args:
        df: _venue, _surface, _distance, _is_komawari 列を含む可能性がある DataFrame。

    Returns:
        上記一時列を削除した DataFrame。列が存在しない場合はそのまま返す。
    """
    return df.drop(
        columns=["_venue", "_surface", "_distance", "_is_komawari"],
        errors="ignore",
    )
