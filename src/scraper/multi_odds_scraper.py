"""
マルチ券種オッズ スクレイパー

netkeiba オッズ API から馬連・ワイド・馬単・三連複・三連単・枠連のオッズを取得し、
multi_odds テーブルに保存する。

環境変数 UMALOGI_MOCK_MULTI_ODDS=1 を設定するとモックデータを返す（テスト用）。

API エンドポイント:
  https://race.netkeiba.com/api/api_get_jra_odds.html?race_id={}&type={}&action=update
  type=3 → 枠連
  type=4 → 馬連
  type=5 → ワイド
  type=6 → 馬単
  type=7 → 三連複
  type=8 → 三連単
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import requests

from src.database.init_db import MultiOddsEntry, init_db, insert_multi_odds

logger = logging.getLogger(__name__)

ODDS_API_URL = "https://race.netkeiba.com/api/api_get_jra_odds.html"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://race.netkeiba.com/",
}

# netkeiba の type 番号 → 券種名 / 組み合わせフォーマット
_BET_TYPES: dict[int, tuple[str, str]] = {
    3: ("枠連",   "unordered"),   # "1-2"
    4: ("馬連",   "unordered"),   # "1-2"
    5: ("ワイド", "unordered"),   # "1-2"  (odds=min, odds_max=max)
    6: ("馬単",   "ordered"),     # "1→2"
    7: ("三連複", "unordered"),   # "1-2-3"
    8: ("三連単", "ordered"),     # "1→2→3"
}

# モックデータ（UMALOGI_MOCK_MULTI_ODDS=1 のとき使用）
_MOCK_DATA: dict[int, dict[str, list[str]]] = {
    4: {"01-02": ["15.4", "3"], "01-03": ["8.2", "1"], "02-03": ["22.1", "5"]},
    5: {"01-02": ["3.5", "5.2", "2"], "01-03": ["2.1", "3.8", "1"], "02-03": ["6.0", "9.5", "4"]},
    6: {"01-02": ["30.1", "4"], "02-01": ["18.5", "2"]},
    7: {"01-02-03": ["55.2", "1"], "01-02-04": ["120.0", "3"]},
    8: {"01-02-03": ["110.0", "2"], "03-01-02": ["250.0", "6"]},
    3: {"1-2": ["10.5", "2"], "1-3": ["5.2", "1"]},
}


def _safe_float(val: str | None) -> float | None:
    """文字列を float に変換する。カンマ区切り ("1,481.9") にも対応。"""
    if val is None or val in ("", "---"):
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val: str | None) -> int | None:
    """文字列を int に変換する。変換不能なら None を返す。"""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _format_combination(raw_key: str, fmt: str) -> str:
    """netkeiba のキー文字列をフォーマット済み買い目文字列に変換する。

    実 API は区切りなしの連結形式 ("0102", "010203") を返す。
    モックデータは "-" 区切り ("01-02") を使用する。どちらにも対応する。

    Args:
        raw_key: "0102" / "010203" / "01-02" 等のゼロ埋め文字列。
        fmt:     "unordered" または "ordered"。

    Returns:
        "1-2" / "1-2-3" / "1→2" / "1→2→3" 等（先頭ゼロを除去）。
    """
    sep_char = "→" if fmt == "ordered" else "-"
    if "-" in raw_key or "→" in raw_key:
        # モックや旧形式: "-" / "→" で分割
        parts = [str(int(p)) for p in raw_key.replace("→", "-").split("-") if p]
    else:
        # 実 API 形式: ゼロ埋め 2 桁の連結 ("0102" → [1, 2])
        parts = [str(int(raw_key[i : i + 2])) for i in range(0, len(raw_key), 2)]
    return sep_char.join(parts)


def _fetch_odds_json(race_id: str, odds_type: int, delay: float = 1.0) -> dict:
    """1 券種分のオッズ JSON を取得する。

    Args:
        race_id:   netkeiba の race_id。
        odds_type: API type 番号 (3-8)。
        delay:     リクエスト前の待機秒数。

    Returns:
        買い目 key → [odds_str, ...] の辞書。取得失敗時は空辞書。
    """
    time.sleep(delay)
    try:
        resp = requests.get(
            ODDS_API_URL,
            params={"race_id": race_id, "type": str(odds_type), "action": "update"},
            headers=_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        logger.warning("multi_odds fetch 失敗 type=%d race_id=%s: %s", odds_type, race_id, exc)
        return {}
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("multi_odds JSON パース失敗 type=%d: %s", odds_type, exc)
        return {}

    # 新形式: {"status":..., "data": {"odds": {"4": {...}}}}
    nested = data.get("data", {}).get("odds", {})
    if nested:
        return nested.get(str(odds_type), {})
    # 旧形式互換
    return data.get(str(odds_type), {}) or data.get("1", {}) or {}


def _parse_odds_dict(
    raw: dict[str, list[str]],
    bet_type_name: str,
    fmt: str,
    race_id: str,
    recorded_at: str,
) -> list[MultiOddsEntry]:
    """生オッズ辞書を MultiOddsEntry リストに変換する。

    Args:
        raw:           買い目 key → [odds_str, (odds_max_str,) pop_str] の辞書。
        bet_type_name: "馬連" 等の券種名。
        fmt:           "ordered" または "unordered"。
        race_id:       対象レース ID。
        recorded_at:   取得時刻文字列。

    Returns:
        変換済み MultiOddsEntry リスト。
    """
    results: list[MultiOddsEntry] = []
    is_wide = bet_type_name == "ワイド"

    for raw_key, vals in raw.items():
        if not raw_key or not vals:
            continue
        combo = _format_combination(raw_key, fmt)
        if is_wide:
            # ワイド: [odds_min, odds_max, popularity]
            odds_val  = _safe_float(vals[0]) if len(vals) > 0 else None
            odds_max  = _safe_float(vals[1]) if len(vals) > 1 else None
            pop       = _safe_int(vals[2])   if len(vals) > 2 else None
        else:
            # 実 API: [odds, 0.0, popularity]  モック: [odds, popularity]
            odds_val  = _safe_float(vals[0]) if len(vals) > 0 else None
            odds_max  = None
            if len(vals) > 2:
                pop = _safe_int(vals[2])   # 実 API 形式
            elif len(vals) > 1:
                pop = _safe_int(vals[1])   # 2 要素のモック形式
            else:
                pop = None

        results.append(MultiOddsEntry(
            race_id=race_id,
            bet_type=bet_type_name,
            combination=combo,
            odds=odds_val,
            odds_max=odds_max,
            popularity=pop,
            recorded_at=recorded_at,
        ))
    return results


def fetch_multi_odds(
    race_id: str,
    *,
    bet_types: list[int] | None = None,
    delay: float = 1.0,
) -> list[MultiOddsEntry]:
    """指定レースの複数券種オッズを取得する。

    モックモード（UMALOGI_MOCK_MULTI_ODDS=1）では HTTP 通信を行わず
    ハードコードされたサンプルデータを返す。

    Args:
        race_id:   netkeiba の race_id (12 桁)。
        bet_types: 取得する API type 番号リスト。省略時は [3,4,5,6,7,8]。
        delay:     各リクエスト間の待機秒数。

    Returns:
        MultiOddsEntry リスト（全券種の買い目を結合）。
    """
    is_mock = os.environ.get("UMALOGI_MOCK_MULTI_ODDS", "0") == "1"
    types_to_fetch = bet_types or list(_BET_TYPES.keys())
    recorded_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_entries: list[MultiOddsEntry] = []

    for t in types_to_fetch:
        if t not in _BET_TYPES:
            logger.warning("未対応の bet_type: %d", t)
            continue
        bet_name, fmt = _BET_TYPES[t]
        if is_mock:
            raw = _MOCK_DATA.get(t, {})
        else:
            raw = _fetch_odds_json(race_id, t, delay=delay)

        entries = _parse_odds_dict(raw, bet_name, fmt, race_id, recorded_at)
        logger.debug("multi_odds type=%d (%s): %d 件取得", t, bet_name, len(entries))
        all_entries.extend(entries)

    return all_entries


def scrape_and_save_multi_odds(
    race_id: str,
    *,
    db_path: Path | None = None,
    bet_types: list[int] | None = None,
    delay: float = 1.5,
) -> int:
    """マルチ券種オッズを取得して DB に保存する。

    Args:
        race_id:   netkeiba の race_id (12 桁)。
        db_path:   SQLite DB パス。省略時はデフォルト。
        bet_types: 取得する API type 番号リスト。省略時は全券種。
        delay:     各リクエスト間の待機秒数。

    Returns:
        DB に挿入した行数。
    """
    conn = init_db(db_path)
    entries = fetch_multi_odds(race_id, bet_types=bet_types, delay=delay)
    if not entries:
        logger.info("multi_odds: 取得データなし (race_id=%s)", race_id)
        return 0
    inserted = insert_multi_odds(conn, entries)
    conn.close()
    return inserted
