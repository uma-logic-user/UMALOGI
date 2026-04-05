"""
出馬表・リアルタイムオッズ スクレイパー

対象 URL:
  出馬表  : https://race.netkeiba.com/race/shutuba.html?race_id={race_id}
  オッズAPI: https://race.netkeiba.com/api/api_get_jra_odds.html?race_id={race_id}&type={1|2}&action=update
             type=1 → 単勝  type=2 → 複勝
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SHUTUBA_URL = "https://race.netkeiba.com/race/shutuba.html"
ODDS_API_URL = "https://race.netkeiba.com/api/api_get_jra_odds.html"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://race.netkeiba.com/",
}


# ── データクラス ───────────────────────────────────────────────────

@dataclass
class EntryHorse:
    horse_number: int           # 馬番
    gate_number: int            # 枠番
    horse_id: str | None        # horse_id（netkeiba）
    horse_name: str             # 馬名
    sex_age: str                # 性齢 例 "牡3"
    weight_carried: float       # 斤量
    jockey: str                 # 騎手名
    trainer: str                # 調教師名
    horse_weight: int | None    # 馬体重（kg）
    horse_weight_diff: int | None  # 前走比（+2 / -4 / 0）


@dataclass
class EntryTable:
    race_id: str
    entries: list[EntryHorse] = field(default_factory=list)


@dataclass
class HorseOdds:
    horse_number: int
    win_odds: float | None          # 単勝オッズ
    place_odds_min: float | None    # 複勝オッズ（下限）
    place_odds_max: float | None    # 複勝オッズ（上限）
    popularity: int | None          # 人気順


# ── 内部ユーティリティ ────────────────────────────────────────────

def _fetch(
    url: str,
    params: dict | None = None,
    *,
    max_retries: int = 3,
    delay: float = 1.5,
    timeout: int = 20,
) -> str:
    """HTTP GET を指数バックオフ付きで取得。"""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logger.warning("リトライ %d/%d (%s) — %.1f 秒後", attempt + 1, max_retries, exc, wait)
            time.sleep(wait)
    raise RuntimeError("到達不能コード")  # pragma: no cover


def _parse_weight(text: str) -> tuple[int | None, int | None]:
    """
    "482 (+2)" → (482, 2)
    "計不" や空文字 → (None, None)
    """
    m = re.search(r"(\d+)\s*\(([+\-]?\d+)\)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d{3,})", text)
    if m2:
        return int(m2.group(1)), None
    return None, None


def _safe_float(text: str) -> float | None:
    try:
        v = float(text.strip())
        return v if v > 0 else None
    except (ValueError, AttributeError):
        return None


def _safe_int(text: str) -> int | None:
    try:
        return int(text.strip())
    except (ValueError, AttributeError):
        return None


# ── 出馬表スクレイパー ───────────────────────────────────────────

def fetch_entry_table(
    race_id: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
) -> EntryTable:
    """
    race.netkeiba.com から出馬表を取得して EntryTable を返す。

    列マッピング（Shutuba_Table の td インデックス）:
      [0] 枠番  [1] 馬番  [3] 馬名 / horse_id  [4] 性齢
      [5] 斤量  [6] 騎手  [7] 調教師  [8] 馬体重

    Args:
        race_id:     netkeiba の race_id（例: "202506050811"）
        delay:       リクエスト間隔（秒）
        max_retries: 最大リトライ回数

    Returns:
        EntryTable
    """
    html = _fetch(
        SHUTUBA_URL,
        params={"race_id": race_id},
        delay=delay,
        max_retries=max_retries,
    )
    soup = BeautifulSoup(html, "lxml")
    table = EntryTable(race_id=race_id)

    rows = soup.select("table.Shutuba_Table tr.HorseList")
    logger.info("出馬表 race_id=%s: %d 頭取得", race_id, len(rows))

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 9:
            continue

        gate_number = _safe_int(cells[0].get_text(strip=True)) or 0
        horse_number = _safe_int(cells[1].get_text(strip=True)) or 0

        # 馬名・horse_id: <td class="HorseInfo"> の <a> リンク
        horse_info_td = cells[3]
        horse_link = horse_info_td.find("a", href=re.compile(r"/horse/"))
        if horse_link:
            horse_name = horse_link.get_text(strip=True)
            m = re.search(r"/horse/(\w+)/?", horse_link.get("href", ""))
            horse_id = m.group(1) if m else None
        else:
            horse_name = horse_info_td.get_text(strip=True)
            horse_id = None

        sex_age = cells[4].get_text(strip=True)
        weight_carried = _safe_float(cells[5].get_text(strip=True)) or 0.0
        jockey = cells[6].get_text(strip=True)
        trainer = cells[7].get_text(strip=True)

        weight_text = cells[8].get_text(" ", strip=True)
        horse_weight, horse_weight_diff = _parse_weight(weight_text)

        table.entries.append(
            EntryHorse(
                horse_number=horse_number,
                gate_number=gate_number,
                horse_id=horse_id,
                horse_name=horse_name,
                sex_age=sex_age,
                weight_carried=weight_carried,
                jockey=jockey,
                trainer=trainer,
                horse_weight=horse_weight,
                horse_weight_diff=horse_weight_diff,
            )
        )

    return table


# ── オッズ API クライアント ──────────────────────────────────────

def fetch_realtime_odds(
    race_id: str,
    *,
    delay: float = 1.0,
    max_retries: int = 3,
) -> list[HorseOdds]:
    """
    netkeiba オッズ JSON API から単勝・複勝オッズを取得する。

    API レスポンス例（type=1 単勝）:
      {"1": {"01": ["3.8", "", "3"], "02": ["5.1", "", "1"], ...}}

    API レスポンス例（type=2 複勝）:
      {"1": {"01": ["2.0", "3.5", "3"], "02": ["1.5", "2.8", "1"], ...}}

    Args:
        race_id:     netkeiba の race_id
        delay:       リクエスト間隔（秒）
        max_retries: 最大リトライ回数

    Returns:
        list[HorseOdds]（馬番昇順）
    """
    import json

    def _get(odds_type: int) -> dict:
        text = _fetch(
            ODDS_API_URL,
            params={"race_id": race_id, "type": odds_type, "action": "update"},
            delay=delay,
            max_retries=max_retries,
        )
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("オッズ JSON パース失敗 type=%d", odds_type)
            return {}
        # レスポンスは {"1": {"01": [...], ...}} 形式
        return data.get("1", {}) or data.get("odds", {}) or {}

    win_data = _get(1)
    time.sleep(delay)
    place_data = _get(2)

    results: dict[int, HorseOdds] = {}

    for num_str, vals in win_data.items():
        num = _safe_int(num_str)
        if num is None:
            continue
        win_odds = _safe_float(vals[0]) if vals else None
        popularity = _safe_int(vals[2]) if len(vals) > 2 else None
        results[num] = HorseOdds(
            horse_number=num,
            win_odds=win_odds,
            place_odds_min=None,
            place_odds_max=None,
            popularity=popularity,
        )

    for num_str, vals in place_data.items():
        num = _safe_int(num_str)
        if num is None:
            continue
        place_min = _safe_float(vals[0]) if vals else None
        place_max = _safe_float(vals[1]) if len(vals) > 1 else None
        if num in results:
            results[num].place_odds_min = place_min
            results[num].place_odds_max = place_max
        else:
            results[num] = HorseOdds(
                horse_number=num,
                win_odds=None,
                place_odds_min=place_min,
                place_odds_max=place_max,
                popularity=None,
            )

    logger.info("オッズ取得 race_id=%s: %d 頭", race_id, len(results))
    return sorted(results.values(), key=lambda h: h.horse_number)
