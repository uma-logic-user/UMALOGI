"""
パイプライン共通ユーティリティ

複数のパイプラインモジュールから参照される定数・ヘルパー関数をまとめる。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# UI 用 JSON 出力先
_ROOT = Path(__file__).resolve().parents[2]
JSON_OUT_DIR = _ROOT / "data" / "predictions"

# 競馬場コード → 名称
JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


def format_race_label(race_id: str) -> str:
    """race_id から "東京 11R" のような表示文字列を生成する。"""
    venue_code = race_id[4:6] if len(race_id) >= 6 else "??"
    venue = JYO.get(venue_code, venue_code)
    race_num = str(int(race_id[10:12])) + "R" if len(race_id) >= 12 else race_id
    return f"{venue} {race_num}"


def kelly_fraction(p_win: float, odds: float, multiplier: float = 0.1) -> float:
    """1/10 ケリー基準による最適賭け比率を返す。

    Args:
        p_win:      勝利確率 (0〜1)
        odds:       単勝オッズ（例: 3.5倍）
        multiplier: ケリー乗数（デフォルト 0.1 = 1/10 Kelly）

    Returns:
        総資金に対する推奨賭け比率 (0〜1)。期待値 < 1.0 の場合は 0.0。
    """
    if odds <= 1.0 or p_win <= 0.0:
        return 0.0
    b = odds - 1.0
    q = 1.0 - p_win
    f_star = (p_win * b - q) / b
    return max(0.0, f_star * multiplier)


def build_output_json(
    race_id: str,
    df: pd.DataFrame,
    honmei_scores: pd.Series,
    honmei_ev_scores: pd.Series,
    ev_scores: pd.Series,
    honmei_bets: object,
    manji_bets: object,
) -> dict:
    """UI 用の JSON ペイロードを組み立てる。

    各馬に honmei_score / ev_score / kelly_fraction / manji_ev を付与する。
    """
    def _int_or_none(v: object) -> int | None:
        return int(v) if (v is not None and pd.notna(v) and v != 0) else None  # type: ignore[arg-type]

    def _float_or_none(v: object) -> float | None:
        return float(v) if (v is not None and pd.notna(v)) else None  # type: ignore[arg-type]

    df_reset = df.reset_index(drop=True)
    horses: list[dict] = []
    ev_recommend: list[dict] = []

    for i, row in df_reset.iterrows():
        num    = int(row["horse_number"])
        p_win  = float(honmei_scores.iloc[i]) if i < len(honmei_scores) else 0.0
        ev_val = float(honmei_ev_scores.iloc[i]) if i < len(honmei_ev_scores) else 0.0
        odds   = float(row.get("win_odds") or 0.0)
        kelly  = kelly_fraction(p_win, odds) if odds > 0 else 0.0

        entry: dict = {
            "horse_number":   num,
            "horse_name":     str(row.get("horse_name", "")),
            "horse_id":       str(row.get("horse_id", "") or ""),
            "sex_age":        str(row.get("sex_age", "") or ""),
            "weight_carried": float(row.get("weight_carried") or 0),
            "horse_weight":   _int_or_none(row.get("horse_weight")),
            "win_odds":       _float_or_none(odds),
            "popularity":     _int_or_none(row.get("popularity")),
            "honmei_score":   round(p_win, 4),
            "ev_score":       round(ev_val, 4),
            "kelly_fraction": round(kelly, 4),
            "manji_ev":       round(float(ev_scores.iloc[i]) if i < len(ev_scores) else 0, 4),
            "odds_vs_morning": _float_or_none(row.get("odds_vs_morning")),
            "odds_velocity":   _float_or_none(row.get("odds_velocity")),
        }
        horses.append(entry)

        if ev_val >= 1.0:
            ev_recommend.append({
                "horse_number":   num,
                "horse_name":     entry["horse_name"],
                "win_odds":       entry["win_odds"],
                "ev_score":       entry["ev_score"],
                "kelly_fraction": entry["kelly_fraction"],
            })

    ev_recommend.sort(key=lambda x: x["ev_score"], reverse=True)

    first = df_reset.iloc[0] if not df_reset.empty else {}
    bias = {
        "today_inner_bias":  _float_or_none(first.get("today_inner_bias")),
        "today_front_bias":  _float_or_none(first.get("today_front_bias")),
        "today_race_count":  _int_or_none(first.get("today_race_count")),
    }

    return {
        "race_id":      race_id,
        "generated_at": datetime.now().isoformat(),
        "bias":         bias,
        "horses":       horses,
        "ev_recommend": ev_recommend,
        "honmei_bets":  honmei_bets.to_dict(),  # type: ignore[attr-defined]
        "manji_bets":   manji_bets.to_dict(),    # type: ignore[attr-defined]
    }


def save_json(race_id: str, payload: dict) -> Path:
    """UI 用 JSON を data/predictions/<race_id>.json に書き出す。"""
    JSON_OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = JSON_OUT_DIR / f"{race_id}.json"
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON 出力: %s", out)
    return out
