"""W-001 加速力スコア（上がり3F）＋ PCI（ペースチェンジ指数）計算モジュール。

レース後半3ハロン（上がり3F）のタイムを基に、各馬の「加速力」とレースの
ペース性質（前傾/後傾）を定量化する **次期学習用の特徴量** を計算する。

⚠️ 本モジュールは稼働中モデル(v1.2.0)の入力次元 ``src.ml.models.FEATURE_COLS`` を
   一切変更しない。あくまで並行計算・蓄積用であり、再学習時に明示的に取り込むまで
   推論パイプラインには影響しない。

データ源:
    race_results.last_3f（netkeiba 結果ページ列[11]「上がり」由来・additive 列）と
    race_results.finish_time（"M:SS.s"）、races.distance（m）。
    last_3f が未取得(NULL)のレースでは安全に NaN/空を返す（破壊しない）。

PCI（西田式準拠）:
    overall_per_furlong = finish_time / (distance / 200)   # 全体の平均 1F(200m) 秒
    last_per_furlong    = last_3f / 3                       # 後半3Fの平均 1F 秒
    PCI = 50 × overall_per_furlong / last_per_furlong
    → PCI > 50: 後傾（上がり勝負・スローペース）/ PCI < 50: 前傾（ハイペース）。
"""

from __future__ import annotations

import math
import sqlite3

import pandas as pd

# 1 ハロン = 200m（日本競馬）。上がりは後半 3 ハロン = 600m。
FURLONG_M: float = 200.0
LAST_FURLONGS: int = 3
PCI_BASELINE: float = 50.0


def parse_time_to_seconds(value: object) -> float | None:
    """"M:SS.s" / "SS.s" / 数値 形式のタイムを秒(float)へ変換する。

    Args:
        value: "1:11.6"（分:秒）/ "34.5"（秒）/ 71.6（数値）/ None など。

    Returns:
        秒数(float)。解釈不能・非正値は None。
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        s = float(value)
        return s if s > 0 and math.isfinite(s) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        if ":" in text:
            mm, ss = text.split(":", 1)
            total = float(mm) * 60.0 + float(ss)
        else:
            total = float(text)
    except (ValueError, TypeError):
        return None
    return total if total > 0 and math.isfinite(total) else None


def compute_pci(
    last_3f_sec: float | None,
    finish_time_sec: float | None,
    distance_m: float | None,
    *,
    furlong_m: float = FURLONG_M,
    last_furlongs: int = LAST_FURLONGS,
) -> float | None:
    """西田式準拠の PCI（ペースチェンジ指数）を計算する。

    PCI = 50 × (全体平均1Fタイム) / (後半3F平均1Fタイム)。
    後半が速い（上がりが小さい）ほど PCI は大きくなる（後傾＝上がり勝負）。

    Returns:
        PCI（float）。入力不正（None / 非正 / 距離 < 後半3F）の場合は None。
    """
    if not last_3f_sec or not finish_time_sec or not distance_m:
        return None
    if last_3f_sec <= 0 or finish_time_sec <= 0 or distance_m <= 0:
        return None
    last_segment_m = furlong_m * last_furlongs
    if distance_m < last_segment_m:
        return None
    overall_per_furlong = finish_time_sec / (distance_m / furlong_m)
    last_per_furlong = last_3f_sec / last_furlongs
    if last_per_furlong <= 0:
        return None
    return PCI_BASELINE * overall_per_furlong / last_per_furlong


def compute_race_pci(
    last_3f_secs: list[float | None],
    finish_time_secs: list[float | None],
    distance_m: float | None,
) -> float | None:
    """W-002 レースレベル PCI（RPCI＝ペースチェンジ指数のレース代表値）を返す。

    出走各馬の PCI（`compute_pci`）の **中央値** を採り、そのレースが前傾（ハイペース・
    RPCI<50）か後傾（スローペース・上がり勝負・RPCI>50）かを 1 値で表す。

    ⚠️ 西田式の本来形 ``50+(前3F−後3F)`` は **前半3F** を要するが、netkeiba 由来
    データには上がり3F（後半）しか無い。そこで本実装は上がり3F・走破タイム・距離から
    導出する比率版 PCI の中央値で **同等のペース性質** を表現する（前3F は JVLink SE の
    HaronTimeF3 が取得可能になった段階で `50+(前3F−後3F)` 版へ拡張余地あり）。

    Returns:
        RPCI（float・中央値）。有効 PCI が無ければ None。
    """
    pcis = [
        p
        for l3, ft in zip(last_3f_secs, finish_time_secs)
        if (p := compute_pci(l3, ft, distance_m)) is not None
    ]
    if not pcis:
        return None
    pcis.sort()
    n = len(pcis)
    mid = n // 2
    return pcis[mid] if n % 2 else (pcis[mid - 1] + pcis[mid]) / 2.0


def acceleration_score(last_3f_secs: list[float | None]) -> list[float]:
    """レース内の相対加速力スコア（上がり3Fの z-score・速いほど正）を返す。

    各馬の上がり3Fをレース内で標準化し、平均より速い（小さい）ほど正の値。
    有効値が 2 未満、または標準偏差 0 のときは全馬 0.0（中立）。

    Args:
        last_3f_secs: 各馬の上がり3F秒（None 可・順序は入力のまま）。

    Returns:
        入力と同順・同長のスコアリスト（None は 0.0）。
    """
    valid = [s for s in last_3f_secs if s is not None and s > 0]
    if len(valid) < 2:
        return [0.0] * len(last_3f_secs)
    mean = sum(valid) / len(valid)
    var = sum((s - mean) ** 2 for s in valid) / len(valid)
    std = math.sqrt(var)
    if std <= 0:
        return [0.0] * len(last_3f_secs)
    # 速い(小さい)ほど正にするため (mean - value)/std
    return [
        (mean - s) / std if (s is not None and s > 0) else 0.0 for s in last_3f_secs
    ]


def build_acceleration_features(
    conn: sqlite3.Connection, race_id: str
) -> pd.DataFrame:
    """1レース分の加速力特徴量（last_3f / pci / acceleration_score）を計算する。

    race_results.last_3f が未取得（NULL）の場合も例外を出さず、pci=NaN /
    acceleration_score=0.0 を埋めて返す（本番非破壊・並行計算の安全設計）。

    Returns:
        columns = [horse_number, last_3f_sec, pci, acceleration_score] の DataFrame。
        出走データが無い場合は空 DataFrame。
    """
    cols = ["horse_number", "last_3f_sec", "pci", "acceleration_score", "race_pci"]
    drow = conn.execute(
        "SELECT distance FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    distance = float(drow[0]) if drow and drow[0] else None

    # last_3f 列が存在しない古いスキーマでも落ちないようにする
    has_last3f = any(
        r[1] == "last_3f"
        for r in conn.execute("PRAGMA table_info(race_results)").fetchall()
    )
    select_last3f = "last_3f" if has_last3f else "NULL AS last_3f"
    rows = conn.execute(
        f"SELECT horse_number, finish_time, {select_last3f} "
        "FROM race_results WHERE race_id = ? ORDER BY horse_number",
        (race_id,),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)

    horse_numbers = [r[0] for r in rows]
    last3f_secs = [parse_time_to_seconds(r[2]) for r in rows]
    finish_secs = [parse_time_to_seconds(r[1]) for r in rows]
    pcis = [compute_pci(l3, ft, distance) for l3, ft in zip(last3f_secs, finish_secs)]
    accel = acceleration_score(last3f_secs)
    # W-002: レースレベル RPCI（全馬同値・ペース文脈）
    race_pci = compute_race_pci(last3f_secs, finish_secs, distance)

    return pd.DataFrame(
        {
            "horse_number": horse_numbers,
            "last_3f_sec": last3f_secs,
            "pci": pcis,
            "acceleration_score": accel,
            "race_pci": [race_pci] * len(horse_numbers),
        }
    )
