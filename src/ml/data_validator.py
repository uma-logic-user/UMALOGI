"""
データバリデーター — パイプライン先頭でセンチネル値・異常データを除外する。

責務:
  - win_odds=999.9 等のセンチネル値（オッズ未確定）馬を軸候補から除外
  - 距離0m・エントリー空等の異常レースデータを検知
  - 検証結果をログに記録し、呼び出し側が安全に処理できる状態を保証する
"""

from __future__ import annotations

import logging
from typing import TypedDict

import pandas as pd

logger = logging.getLogger(__name__)

# オッズ未確定センチネルの閾値。この値以上は「未確定」として軸候補から外す。
ODDS_SENTINEL_THRESHOLD: float = 500.0

# 最低有効頭数（これ未満はベット生成不可）
MIN_VALID_HORSES: int = 3


class ValidationResult(TypedDict):
    is_valid: bool
    n_total: int
    n_valid_odds: int
    n_sentinel: int
    warnings: list[str]


def validate_race_df(
    df: pd.DataFrame,
    race_id: str = "",
) -> ValidationResult:
    """出走馬DataFrameをバリデーションし、異常を検知・ログ出力する。

    Args:
        df:      出走馬DataFrame（horse_number, win_odds 等を含む）
        race_id: ログ用レースID（省略可）

    Returns:
        ValidationResult — is_valid=False の場合はベット生成をスキップ推奨
    """
    warnings: list[str] = []
    n_total = len(df)

    if n_total == 0:
        return ValidationResult(
            is_valid=False,
            n_total=0,
            n_valid_odds=0,
            n_sentinel=0,
            warnings=["エントリーが空（0頭）"],
        )

    if "win_odds" not in df.columns:
        return ValidationResult(
            is_valid=False,
            n_total=n_total,
            n_valid_odds=0,
            n_sentinel=0,
            warnings=["win_oddsカラムが存在しない"],
        )

    # センチネル値カウント
    sentinel_mask = df["win_odds"].apply(
        lambda v: (
            v is not None and float(v) >= ODDS_SENTINEL_THRESHOLD
            if v is not None
            else False
        )
    )
    n_sentinel = int(sentinel_mask.sum())
    n_valid_odds = n_total - n_sentinel

    if n_sentinel > 0:
        sentinel_nums = (
            df[sentinel_mask]["horse_number"].tolist()
            if "horse_number" in df.columns
            else []
        )
        warnings.append(
            f"センチネルオッズ（≥{ODDS_SENTINEL_THRESHOLD}）: {n_sentinel}頭 馬番={sentinel_nums}"
        )
        logger.warning(
            "データ検証: %s — センチネルオッズ %d頭 (計%d頭) 馬番=%s",
            race_id or "?",
            n_sentinel,
            n_total,
            sentinel_nums,
        )

    if n_valid_odds < MIN_VALID_HORSES:
        warnings.append(
            f"有効オッズ確定馬が{MIN_VALID_HORSES}頭未満 (valid={n_valid_odds})"
        )
        logger.warning(
            "データ検証: %s — 有効馬 %d頭 < 最低必要 %d頭 → ベット生成スキップ推奨",
            race_id or "?",
            n_valid_odds,
            MIN_VALID_HORSES,
        )

    is_valid = n_valid_odds >= MIN_VALID_HORSES
    return ValidationResult(
        is_valid=is_valid,
        n_total=n_total,
        n_valid_odds=n_valid_odds,
        n_sentinel=n_sentinel,
        warnings=warnings,
    )


def filter_sentinel_horses(
    df: pd.DataFrame,
    race_id: str = "",
) -> pd.DataFrame:
    """センチネルオッズ馬をDataFrameから除外して返す。

    軸候補データとして使う前に呼び出すことで、win_odds=999.9 馬が
    Harville計算・EV計算・軸選択に混入するのを防ぐ。

    Args:
        df:      出走馬DataFrame
        race_id: ログ用レースID（省略可）

    Returns:
        センチネル馬を除いたDataFrame（元DataFrameは変更しない）
    """
    if "win_odds" not in df.columns:
        return df

    valid_mask = df["win_odds"].apply(
        lambda v: (
            v is None or float(v) < ODDS_SENTINEL_THRESHOLD if v is not None else True
        )
    )
    filtered = df[valid_mask].copy()
    n_removed = len(df) - len(filtered)
    if n_removed > 0:
        logger.info(
            "filter_sentinel_horses: %s — %d頭除外 → %d頭残存",
            race_id or "?",
            n_removed,
            len(filtered),
        )
    return filtered


def validate_horse_for_axis(row: "pd.Series | dict") -> bool:
    """1頭分のデータが軸・紐候補として有効か判定する。

    Args:
        row: 馬のデータ行（horse_number, win_odds 等を持つ）

    Returns:
        True = 軸候補として有効
        False = センチネルオッズ等により除外すべき
    """
    try:
        odds = row.get("win_odds") if hasattr(row, "get") else row["win_odds"]
        if odds is None:
            return True  # NaN/None はオッズ未取得扱いで通過（暫定モード）
        return float(odds) < ODDS_SENTINEL_THRESHOLD
    except (TypeError, ValueError, KeyError):
        return True  # パース失敗は通過させる（過剰除外を防ぐ）


def validate_race_meta(
    race_id: str,
    distance: int | float | None,
    n_entries: int,
) -> tuple[bool, list[str]]:
    """レースメタデータ（距離・頭数）の妥当性を検証する。

    Args:
        race_id:   レースID（ログ用）
        distance:  距離（m）
        n_entries: エントリー頭数

    Returns:
        (is_valid, warnings)
    """
    warnings: list[str] = []

    if not distance or int(distance) <= 0:
        warnings.append(f"距離0m（未取得）: race_id={race_id}")
        logger.warning(
            "レースメタ検証: 距離0m race_id=%s → エントリー取得が必要", race_id
        )

    if n_entries < MIN_VALID_HORSES:
        warnings.append(f"エントリー頭数不足: {n_entries}頭 < {MIN_VALID_HORSES}頭")
        logger.warning(
            "レースメタ検証: race_id=%s エントリー %d頭 < 必要%d頭",
            race_id,
            n_entries,
            MIN_VALID_HORSES,
        )

    return len(warnings) == 0, warnings
