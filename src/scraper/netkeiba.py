"""
netkeiba.com スクレイパー

レースIDを指定してレース結果（馬名・着順・血統・タイム・オッズ）を取得する。
レースID形式: YYYYVVDDNN（例: 202506050811 = 2025年中山5回8日目11R）
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
RACE_URL_TEMPLATE  = "https://db.netkeiba.com/race/{race_id}/"
PED_URL_TEMPLATE   = "https://db.netkeiba.com/horse/ped/{horse_id}/"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9",
}

# netkeiba 結果テーブルの列インデックス（実測値）
_COL_RANK          = 0
_COL_HORSE_NAME    = 3
_COL_SEX_AGE       = 4
_COL_WEIGHT        = 5
_COL_JOCKEY        = 6
_COL_TIME          = 7
_COL_MARGIN        = 8
_COL_WIN_ODDS      = 16
_COL_POPULARITY    = 17
_COL_HORSE_WEIGHT  = 18


# ---------------------------------------------------------------------------
# データモデル
# ---------------------------------------------------------------------------
@dataclass
class PedigreeInfo:
    """血統情報（父・母・母父）"""
    sire:     Optional[str] = None   # 父
    dam:      Optional[str] = None   # 母
    dam_sire: Optional[str] = None   # 母父


@dataclass
class HorseResult:
    """1頭分のレース結果"""
    rank:           Optional[int]    # 着順（失格・除外は None）
    horse_name:     str              # 馬名
    horse_id:       Optional[str]    # netkeiba 馬ID
    sex_age:        str              # 性齢（例: "牡3"）
    weight_carried: float            # 斤量 (kg)
    jockey:         str              # 騎手名
    finish_time:    Optional[str]    # タイム（例: "2:31.5"）
    margin:         Optional[str]    # 着差（例: "クビ"）
    popularity:     Optional[int]    # 人気順位
    win_odds:       Optional[float]  # 単勝オッズ
    horse_weight:   Optional[int]    # 馬体重 (kg)
    pedigree: PedigreeInfo = field(default_factory=PedigreeInfo)


@dataclass
class RaceInfo:
    """レース基本情報 + 出走結果"""
    race_id:     str
    race_name:   str
    date:        str          # "YYYY/MM/DD"
    venue:       str          # 開催場所（例: "中山"）
    race_number: int          # 第N競走
    distance:    int          # 距離 (m)
    surface:     str          # "芝" / "ダート"
    weather:     str          # 天候
    condition:   str          # 馬場状態（例: "良"）
    results: list[HorseResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP ユーティリティ
# ---------------------------------------------------------------------------
def _fetch_html(
    url: str,
    *,
    max_retries: int = 3,
    delay: float = 1.5,
    timeout: int = 10,
) -> str:
    """
    URL を取得して HTML 文字列を返す。

    失敗時はエクスポネンシャルバックオフでリトライする。

    Raises:
        requests.RequestException: max_retries 回失敗した場合
    """
    time.sleep(delay)

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            return resp.text
        except (requests.HTTPError, requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "リクエスト失敗 (試行 %d/%d): %s — %.1f秒後にリトライ",
                attempt, max_retries, url, wait,
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise requests.RequestException(
        f"{url} の取得に {max_retries} 回失敗しました"
    ) from last_exc


# ---------------------------------------------------------------------------
# パーサー共通ユーティリティ
# ---------------------------------------------------------------------------
def _parse_rank(raw: str) -> Optional[int]:
    """着順文字列を int に変換。失格・除外等は None を返す。"""
    raw = raw.strip()
    return int(raw) if raw.isdigit() else None


def _parse_float(raw: str) -> Optional[float]:
    """数値文字列を float に変換。変換不可は None を返す。"""
    try:
        return float(raw.strip().replace(",", ""))
    except ValueError:
        return None


def _parse_int(raw: str) -> Optional[int]:
    """数値文字列（馬体重等）を int に変換。"480(+2)" → 480"""
    try:
        return int(raw.strip().split("(")[0])
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# レース基本情報パーサー
# ---------------------------------------------------------------------------
def _parse_race_info(soup: BeautifulSoup, race_id: str) -> RaceInfo:
    """
    レースページから基本情報（名称・距離・天候・馬場・日付・開催場所）を解析する。

    実際のHTML構造（2025年時点）:
      - dl.racedata / div.mainrace_data: "11 R第70回有馬記念(GI)芝右2500m / 天候:晴 / 芝:良"
      - p.smalltxt: "2025年12月28日 5回中山8日目 3歳以上オープン"
    """
    race_name = ""
    distance  = 0
    surface   = ""
    weather   = ""
    condition = ""
    date      = ""
    venue     = ""
    race_number = 0

    # --- レース名・距離・天候・馬場 ---
    data_tag = soup.select_one("dl.racedata, div.mainrace_data")
    if data_tag:
        text = data_tag.get_text(" ", strip=True)

        # レース名: "第70回有馬記念(GI)" のような形式
        m = re.search(r"R\s*(.+?)\s*(?:芝|ダート)", text)
        if m:
            race_name = m.group(1).strip()

        # 距離・馬場種別: "芝右2500m" or "ダート2000m"
        m = re.search(r"(芝|ダート)[左右]?\s*(\d+)m", text)
        if m:
            surface  = m.group(1)
            distance = int(m.group(2))

        # 天候
        m = re.search(r"天候\s*[：:]\s*(\S+?)\s*[/\xa0]", text + "/")
        if m:
            weather = m.group(1)

        # 馬場状態: "芝 : 良" or "ダート : 稍重"
        m = re.search(r"(?:芝|ダート)\s*[：:]\s*(\S+?)\s*[/\xa0]", text + "/")
        if m:
            condition = m.group(1)

    # --- 日付・開催場所・回次 ---
    small_tag = soup.select_one("p.smalltxt")
    if small_tag:
        text = small_tag.get_text(" ", strip=True)

        # 日付: "2025年12月28日"
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
        if m:
            date = f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

        # 開催場所: "5回中山8日目" → "中山"
        m = re.search(r"\d+回(\S+?)\d+日目", text)
        if m:
            venue = m.group(1)

        # 回次: "5回" → 5
        m = re.search(r"(\d+)回", text)
        if m:
            race_number = int(m.group(1))

    return RaceInfo(
        race_id=race_id,
        race_name=race_name,
        date=date,
        venue=venue,
        race_number=race_number,
        distance=distance,
        surface=surface,
        weather=weather,
        condition=condition,
    )


# ---------------------------------------------------------------------------
# 結果テーブルパーサー
# ---------------------------------------------------------------------------
def _parse_results_table(
    soup: BeautifulSoup,
) -> list[tuple[str, str, list[str]]]:
    """
    結果テーブルから (horse_name, horse_id, cells) を抽出する。

    netkeiba の結果テーブルは 25 列構成（2025年時点）。
    重要列: [0]着順 [4]性齢 [5]斤量 [6]騎手 [7]タイム [8]着差
            [16]単勝 [17]人気 [18]馬体重
    """
    table = soup.select_one("table.race_table_01")
    if table is None:
        return []

    result: list[tuple[str, str, list[str]]] = []
    for tr in table.select("tr")[1:]:   # ヘッダー行をスキップ
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cells) < 10:
            continue

        horse_link = tr.select_one("td a[href*='/horse/']")
        horse_name = horse_link.get_text(strip=True) if horse_link else cells[_COL_HORSE_NAME]
        horse_id   = ""
        if horse_link and horse_link.get("href"):
            parts    = str(horse_link["href"]).rstrip("/").split("/")
            horse_id = parts[-1]

        result.append((horse_name, horse_id, cells))

    return result


# ---------------------------------------------------------------------------
# 血統情報パーサー
# ---------------------------------------------------------------------------
def _fetch_pedigree(horse_id: str, delay: float = 1.5) -> PedigreeInfo:
    """
    血統専用ページ（/horse/ped/{id}/）から父・母・母父を取得する。

    blood_table の構造:
      row[ 0].td[0] rowspan=16 → 父 (sire)
      row[16].td[0] rowspan=16 → 母 (dam)
      row[16].td[1] rowspan=8  → 母父 (dam's sire)
    各セルは <a> タグで馬名を保持している。
    """
    if not horse_id:
        return PedigreeInfo()

    url = PED_URL_TEMPLATE.format(horse_id=horse_id)
    try:
        html = _fetch_html(url, delay=delay)
    except requests.RequestException as exc:
        logger.warning("血統取得失敗 horse_id=%s: %s", horse_id, exc)
        return PedigreeInfo()

    soup  = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.blood_table")
    if table is None:
        return PedigreeInfo()

    rows = table.select("tr")
    if len(rows) < 17:
        return PedigreeInfo()

    def _link_text(row_idx: int, td_idx: int) -> Optional[str]:
        tds  = rows[row_idx].select("td")
        if td_idx >= len(tds):
            return None
        link = tds[td_idx].select_one("a")
        return link.get_text(strip=True) if link else None

    return PedigreeInfo(
        sire     = _link_text(0,  0),   # row[0].td[0]  → 父
        dam      = _link_text(16, 0),   # row[16].td[0] → 母
        dam_sire = _link_text(16, 1),   # row[16].td[1] → 母父
    )


# ---------------------------------------------------------------------------
# パブリック API
# ---------------------------------------------------------------------------
def fetch_race_results(
    race_id: str,
    *,
    fetch_pedigree: bool = True,
    delay: float = 1.5,
    max_retries: int = 3,
) -> RaceInfo:
    """
    レース ID を指定してレース結果を取得する。

    Args:
        race_id: netkeiba レース ID（例: "202506050811"）
        fetch_pedigree: True の場合、各馬の血統情報も取得する
        delay: 各リクエスト前の待機秒数（サーバー負荷軽減）
        max_retries: HTTP リトライ上限

    Returns:
        RaceInfo（レース基本情報 + 各馬結果リスト）

    Raises:
        ValueError: レース ID が不正な場合
        requests.RequestException: レースページの取得に失敗した場合
    """
    if not race_id or not race_id.isdigit():
        raise ValueError(f"不正なレース ID: {race_id!r}")

    logger.info("レース結果取得開始: race_id=%s", race_id)
    url  = RACE_URL_TEMPLATE.format(race_id=race_id)
    html = _fetch_html(url, max_retries=max_retries, delay=delay)
    soup = BeautifulSoup(html, "lxml")

    race_info = _parse_race_info(soup, race_id)
    raw_rows  = _parse_results_table(soup)

    results: list[HorseResult] = []
    for horse_name, horse_id, cells in raw_rows:
        ped = _fetch_pedigree(horse_id, delay=delay) if fetch_pedigree and horse_id else PedigreeInfo()

        results.append(HorseResult(
            rank           = _parse_rank(cells[_COL_RANK])             if len(cells) > _COL_RANK          else None,
            horse_name     = horse_name,
            horse_id       = horse_id or None,
            sex_age        = cells[_COL_SEX_AGE]                       if len(cells) > _COL_SEX_AGE        else "",
            weight_carried = _parse_float(cells[_COL_WEIGHT]) or 0.0   if len(cells) > _COL_WEIGHT         else 0.0,
            jockey         = cells[_COL_JOCKEY]                        if len(cells) > _COL_JOCKEY         else "",
            finish_time    = cells[_COL_TIME]   or None                if len(cells) > _COL_TIME           else None,
            margin         = cells[_COL_MARGIN] or None                if len(cells) > _COL_MARGIN         else None,
            win_odds       = _parse_float(cells[_COL_WIN_ODDS])        if len(cells) > _COL_WIN_ODDS       else None,
            popularity     = _parse_int(cells[_COL_POPULARITY])        if len(cells) > _COL_POPULARITY     else None,
            horse_weight   = _parse_int(cells[_COL_HORSE_WEIGHT])      if len(cells) > _COL_HORSE_WEIGHT   else None,
            pedigree       = ped,
        ))

    race_info.results = results
    logger.info("取得完了: race_id=%s, 出走頭数=%d", race_id, len(results))
    return race_info
