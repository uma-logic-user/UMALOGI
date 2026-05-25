"""
調教評価グレード適性スコア — AIウマスギ拡張因子

training_times.training_grade (S〜E) から、直近調教の品質を [0, 1] スコアに変換する。
S=1.0, A=0.83, B=0.67, C=0.50, D=0.33, E=0.17 の線形マッピング。

horse_id フォーマット注意:
  race_results: YYYY+SSSSSS (10桁)  例: 2022105081
  training_times: D+YYYY+SSSSS (10桁)  例: 0202210508
  正規化キー(9桁): race_results → horse_id[:4]+horse_id[4:9]
                  training_times → substr(horse_id,2,9)
"""
from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

_GRADE_SCORE: dict[str, float] = {
    "S": 1.00,
    "A": 0.83,
    "B": 0.67,
    "C": 0.50,
    "D": 0.33,
    "E": 0.17,
}
_DEFAULT = 0.5  # データなし時の中立値


def _to_tc_key(horse_id: str) -> str:
    """race_results.horse_id (10桁) を training_times の正規化キー (9桁) に変換する。

    race_results: YYYY+SSSSSS (10桁) → 先頭4桁 + 5〜9桁目 = 9桁キー。

    Args:
        horse_id: race_results テーブルの horse_id 文字列（10桁）。

    Returns:
        training_times の substr(horse_id,2,9) と照合できる 9桁のキー文字列。
        入力が 9桁未満の場合はそのまま返す。
    """
    if len(horse_id) >= 9:
        return horse_id[:4] + horse_id[4:9]
    return horse_id


def calc_training_grade_score(
    df: pd.DataFrame, conn: sqlite3.Connection
) -> pd.DataFrame:
    """各馬の直近調教グレードスコアを算出して DataFrame に追加する。

    training_times テーブルから基準日以前の最新調教グレードを取得し、
    _GRADE_SCORE マッピング（S=1.0〜E=0.17）でスコア化する。

    Args:
        df: 必須列として race_id, horse_id を含む DataFrame。
        conn: umalogi.db 接続。

    Returns:
        元 df に training_grade_score 列（0.0〜1.0、データなし = 0.5）を
        追加した DataFrame。
    """
    if df.empty:
        df["training_grade_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    # 基準日（時系列リーク防止）
    base_date = _earliest_race_date(df)

    horse_ids = df["horse_id"].dropna().unique().tolist()
    if not horse_ids:
        df["training_grade_score"] = _DEFAULT
        return df

    # race_results の horse_id を正規化キーに変換
    tc_keys = [_to_tc_key(h) for h in horse_ids]
    horse_to_tc = {h: _to_tc_key(h) for h in horse_ids}
    tc_to_horse: dict[str, list[str]] = {}
    for h, k in horse_to_tc.items():
        tc_to_horse.setdefault(k, []).append(h)

    ph = ",".join("?" * len(tc_keys))
    rows = conn.execute(
        f"""
        SELECT substr(horse_id,2,9) AS tc_key, training_grade
        FROM training_times
        WHERE substr(horse_id,2,9) IN ({ph})
          AND training_grade IS NOT NULL AND training_grade != ''
          AND training_date < ?
        ORDER BY training_date DESC
        """,
        tc_keys + [base_date],
    ).fetchall()

    # tc_key → 直近グレードスコア（最初に見つかった行 = 最新）
    grade_map: dict[str, float] = {}
    seen_keys: set[str] = set()
    for tc_key, grade in rows:
        if tc_key not in seen_keys:
            score = _GRADE_SCORE.get(grade, _DEFAULT)
            for orig_horse in tc_to_horse.get(tc_key, []):
                grade_map[orig_horse] = score
            seen_keys.add(tc_key)

    df["training_grade_score"] = df["horse_id"].map(grade_map).fillna(_DEFAULT)
    return df


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
