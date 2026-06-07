"""前走詳細・同コース実績 特徴量（W-070 続き / タスク1）。

各馬の「現レース日より前」の出走履歴のみを参照してリークフリーに前走系特徴量を
計算する。稼働中モデル(v1.x)の ``FEATURE_COLS`` は変更せず、再学習用に並行計算する
（acceleration.py と同じ非破壊方針）。

データ源:
    race_results（horse_id / horse_name / rank / margin / last_3f）と
    races（date / venue / surface / distance）。horse_id を主キーに馬を同定し、
    欠損時は horse_name でフォールバックする。

⚠️ リークフリーの担保: 対象レースの ``races.date`` より **厳密に過去** の出走のみを
   集計する。同日・未来のレースは一切参照しない（test_no_future_leak で検証）。
"""

from __future__ import annotations

import math
import sqlite3

import pandas as pd

from src.features.acceleration import parse_time_to_seconds

PRERUN_FEATURE_COLS: list[str] = [
    "prev_last_3f_sec",  # 直近過去走の上がり3F秒
    "prev_rank",  # 直近過去走の着順
    "prev_margin_sec",  # 直近過去走の着差（秒・正=負け差）
    "days_since_prev",  # 直近過去走からの間隔日数
    "avg_last_3f_3",  # 直近3走の上がり3F平均
    "same_course_runs",  # 同会場・同馬場での過去出走数
    "same_course_place_rate",  # 同会場・同馬場での複勝率(rank<=3)
]


def _parse_margin(value: object) -> float | None:
    """着差文字列を秒に変換する。"クビ"/"ハナ"等は近似秒へ、数値はそのまま。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if math.isfinite(v) else None
    text = str(value).strip()
    if not text:
        return None
    approx = {
        "ハナ": 0.0,
        "アタマ": 0.0,
        "クビ": 0.1,
        "同着": 0.0,
        "大差": 3.0,
    }
    if text in approx:
        return approx[text]
    try:
        return float(text)
    except ValueError:
        return None


def _surface_band(surface: str | None) -> str:
    return (surface or "").strip()


def build_prerun_features(conn: sqlite3.Connection, race_id: str) -> pd.DataFrame:
    """1レース分の前走詳細・同コース実績特徴量をリークフリーに計算する。

    Returns:
        columns = ["horse_number", *PRERUN_FEATURE_COLS] の DataFrame。
        出走データが無い場合は空 DataFrame。過去走の無い馬は NaN/0 で安全に埋める。
    """
    cols = ["horse_number", *PRERUN_FEATURE_COLS]
    meta = conn.execute(
        "SELECT date, venue, surface, distance FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if not meta:
        return pd.DataFrame(columns=cols)
    cur_date, cur_venue, cur_surface, _cur_dist = meta
    cur_surface = _surface_band(cur_surface)

    runners = conn.execute(
        "SELECT horse_number, horse_id, horse_name FROM race_results "
        "WHERE race_id = ? AND horse_number IS NOT NULL ORDER BY horse_number",
        (race_id,),
    ).fetchall()
    if not runners:
        return pd.DataFrame(columns=cols)

    records: list[dict[str, object]] = []
    for horse_number, horse_id, horse_name in runners:
        # 馬の同定: horse_id 優先、無ければ horse_name。
        if horse_id:
            where = "rr.horse_id = ?"
            key: object = horse_id
        else:
            where = "rr.horse_name = ?"
            key = horse_name

        # リークフリー: 現レース日より厳密に過去の出走のみ（自レコード除外）。
        past = conn.execute(
            f"""
            SELECT r.date, r.venue, r.surface, rr.rank, rr.margin, rr.last_3f
            FROM race_results rr JOIN races r ON rr.race_id = r.race_id
            WHERE {where} AND r.date < ? AND rr.race_id != ?
              AND rr.rank IS NOT NULL AND rr.rank > 0
            ORDER BY r.date DESC
            """,
            (key, cur_date, race_id),
        ).fetchall()

        feat: dict[str, object] = {
            "horse_number": horse_number,
            "prev_last_3f_sec": math.nan,
            "prev_rank": math.nan,
            "prev_margin_sec": math.nan,
            "days_since_prev": math.nan,
            "avg_last_3f_3": math.nan,
            "same_course_runs": 0,
            "same_course_place_rate": math.nan,
        }

        if past:
            p0 = past[0]
            feat["prev_rank"] = float(p0[3]) if p0[3] is not None else math.nan
            l3 = parse_time_to_seconds(p0[5])
            feat["prev_last_3f_sec"] = l3 if l3 is not None else math.nan
            mg = _parse_margin(p0[4])
            feat["prev_margin_sec"] = mg if mg is not None else math.nan
            try:
                from datetime import datetime

                d_prev = datetime.strptime(str(p0[0]), "%Y-%m-%d")
                d_cur = datetime.strptime(str(cur_date), "%Y-%m-%d")
                feat["days_since_prev"] = float((d_cur - d_prev).days)
            except (ValueError, TypeError):
                pass

            l3_list = [
                s
                for s in (parse_time_to_seconds(p[5]) for p in past[:3])
                if s is not None
            ]
            if l3_list:
                feat["avg_last_3f_3"] = sum(l3_list) / len(l3_list)

            # 同会場・同馬場の実績
            same = [
                p
                for p in past
                if p[1] == cur_venue and _surface_band(p[2]) == cur_surface
            ]
            feat["same_course_runs"] = len(same)
            if same:
                placed = sum(1 for p in same if p[3] is not None and p[3] <= 3)
                feat["same_course_place_rate"] = placed / len(same)

        records.append(feat)

    return pd.DataFrame(records, columns=cols)
