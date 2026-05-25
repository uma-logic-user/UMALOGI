"""
野芝/洋芝 適性ファクター — AIウマスギ拡張因子

races テーブルに turf_type カラムが存在しないため、
「会場 + 芝レース」で洋芝/野芝を推定し、馬ごとの芝種別実績から適性スコアを算出する。

洋芝コース（札幌・函館）と野芝コース（その他）の実績差を定量化する。
ダートレースには適用せず 0.5（中立）を返す。

洋芝定義:
  洋芝 : 札幌, 函館
  野芝 : 中京, 中山, 京都, 小倉, 新潟, 東京, 福島, 阪神

出力列: turf_type_score (0.0〜1.0)
  ・対象レースが洋芝コース → 洋芝勝率 / 全芝勝率
  ・対象レースが野芝コース → 野芝勝率 / 全芝勝率
  ・ダートレース / 芝実績なし → 0.5（中立）
"""

from __future__ import annotations

import logging
import sqlite3

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── 芝種定数 ───────────────────────────────────────────────────────────────
_YOSHIBA_VENUES: frozenset[str] = frozenset({"札幌", "函館"})
_TURF_SURFACE = "芝"

_MIN_SAMPLES = 3  # 最低サンプル数未満 → 0.5 補填


def _is_yoshiba(venue: str) -> bool | None:
    """会場から洋芝/野芝を判定する。

    ダート/障害レースへの適用判断は呼び出し側（surface チェック）に委ねる。
    本関数は venue のみで洋芝/野芝を識別する。

    Args:
        venue: 開催場所名（例: "札幌", "東京"）。

    Returns:
        True: 洋芝コース（札幌・函館）。
        False: 野芝コース（それ以外の芝会場）。
        None は返さないが、呼び出し側が surface != '芝' の場合に None を割り当てる。
    """
    if venue in _YOSHIBA_VENUES:
        return True
    return False


def calc_turf_type_score(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    """野芝/洋芝 適性スコアを DataFrame に追加して返す。

    対象レースの会場・馬場を races テーブルから取得し、洋芝/野芝を判定する。
    各馬の過去芝成績を洋芝/野芝別に集計し、適性比率を [0, 1] に正規化する。
    ダートレースおよび芝実績が _MIN_SAMPLES 未満の馬は 0.5（中立）を返す。

    Args:
        df: FeatureBuilder（または UmasugiEngine）が返す 1行1頭の特徴量 DataFrame。
            必須列: race_id, horse_id。
        conn: umalogi.db 接続。

    Returns:
        元 df に turf_type_score 列（0.0〜1.0、ダート/実績不足 = 0.5）を
        追加した DataFrame。一時列（_venue, _surface, _is_yoshiba）は削除済み。
    """
    if df.empty:
        df["turf_type_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # ── 対象レースの会場・馬場を取得 ────────────────────────────────────
    race_ids = df["race_id"].unique().tolist()
    placeholders = ",".join("?" * len(race_ids))
    race_info = conn.execute(
        f"SELECT race_id, venue, surface FROM races WHERE race_id IN ({placeholders})",
        race_ids,
    ).fetchall()
    race_map: dict[str, tuple[str, str]] = {
        r[0]: (r[1] or "", r[2] or "") for r in race_info
    }

    df["_venue"]   = df["race_id"].map(lambda x: race_map.get(x, ("", ""))[0])
    df["_surface"] = df["race_id"].map(lambda x: race_map.get(x, ("", ""))[1])

    # ダートレースには適用しない
    df["_is_yoshiba"] = df.apply(
        lambda row: _is_yoshiba(row["_venue"]) if row["_surface"] == _TURF_SURFACE else None,
        axis=1,
    )

    # ── 各馬の過去芝成績を洋芝/野芝別に集計 ─────────────────────────────
    horse_ids = df["horse_id"].dropna().unique().tolist()
    if not horse_ids:
        df["turf_type_score"] = 0.5
        return _drop_tmp(df)

    base_date = _earliest_race_date(df)
    ph = ",".join("?" * len(horse_ids))

    rows = conn.execute(
        f"""
        SELECT
            rr.horse_id,
            r.venue,
            r.race_name,
            CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END AS is_win
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.horse_id IN ({ph})
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
          AND r.surface != 'ダート'
          AND r.venue IS NOT NULL AND r.venue != ''
          AND r.race_name NOT LIKE '%ダート%'
          AND r.race_name NOT LIKE '%障%'
          AND r.date < ?
        """,
        horse_ids + [base_date],
    ).fetchall()

    # horse_id → {yoshiba_races, yoshiba_wins, noshiba_races, noshiba_wins}
    stats: dict[str, dict[str, int]] = {}
    for horse_id, venue, _race_name, is_win in rows:
        s = stats.setdefault(horse_id, {"y_n": 0, "y_w": 0, "n_n": 0, "n_w": 0})
        if venue in _YOSHIBA_VENUES:
            s["y_n"] += 1
            s["y_w"] += is_win
        else:
            s["n_n"] += 1
            s["n_w"] += is_win

    # ── スコア算出 ────────────────────────────────────────────────────────
    def _score(row: pd.Series) -> float:
        hid = row["horse_id"]
        is_yo = row["_is_yoshiba"]

        if is_yo is None:
            return 0.5  # ダートレース

        if hid not in stats:
            return 0.5

        s = stats[hid]
        total_n = s["y_n"] + s["n_n"]
        total_w = s["y_w"] + s["n_w"]

        if total_n < _MIN_SAMPLES:
            return 0.5

        total_win_rate = total_w / total_n
        if total_win_rate == 0:
            return 0.5

        if is_yo:
            if s["y_n"] < _MIN_SAMPLES:
                return 0.5
            target_win_rate = s["y_w"] / s["y_n"]
        else:
            if s["n_n"] < _MIN_SAMPLES:
                return 0.5
            target_win_rate = s["n_w"] / s["n_n"]

        # 適性比率を [0, 1] へ正規化（比率 0.3〜3.0 の範囲でクリップ）
        ratio = np.clip(target_win_rate / total_win_rate, 0.3, 3.0)
        return float((ratio - 0.3) / (3.0 - 0.3))

    df["turf_type_score"] = df.apply(_score, axis=1)
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
        df: _venue, _surface, _is_yoshiba 列を含む可能性がある DataFrame。

    Returns:
        上記一時列を削除した DataFrame。列が存在しない場合はそのまま返す。
    """
    return df.drop(
        columns=["_venue", "_surface", "_is_yoshiba"],
        errors="ignore",
    )
