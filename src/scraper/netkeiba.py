"""
netkeiba.com スクレイパー

レースIDを指定してレース結果（馬名・着順・血統・タイム・オッズ）を取得する。
レースID形式: YYYYVVDDNN（例: 202306050811 = 2023年阪神6日目5R 8レース目 … 実際は10桁）
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
BASE_URL = "https://db.netkeiba.com"
RACE_URL_TEMPLATE = "https://db.netkeiba.com/race/{race_id}/"
HORSE_URL_TEMPLATE = "https://db.netkeiba.com/horse/{horse_id}/"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9",
}


# ---------------------------------------------------------------------------
# データモデル
# ---------------------------------------------------------------------------
@dataclass
class PedigreeInfo:
    """血統情報（父・母・母父）"""

    sire: Optional[str] = None       # 父
    dam: Optional[str] = None        # 母
    dam_sire: Optional[str] = None   # 母父


@dataclass
class HorseResult:
    """1頭分のレース結果"""

    rank: Optional[int]          # 着順（失格・除外は None）
    horse_name: str              # 馬名
    horse_id: Optional[str]      # netkeiba 馬ID
    sex_age: str                 # 性齢（例: "牡3"）
    weight_carried: float        # 斤量 (kg)
    jockey: str                  # 騎手名
    finish_time: Optional[str]   # タイム（例: "1:33.5"）
    margin: Optional[str]        # 着差（例: "クビ"）
    popularity: Optional[int]    # 人気順位
    win_odds: Optional[float]    # 単勝オッズ
    horse_weight: Optional[int]  # 馬体重 (kg)
    pedigree: PedigreeInfo = field(default_factory=PedigreeInfo)


@dataclass
class RaceInfo:
    """レース基本情報"""

    race_id: str
    race_name: str
    date: str          # "YYYY/MM/DD"
    venue: str         # 開催場所（例: "東京"）
    race_number: int   # 第N競走
    distance: int      # 距離 (m)
    surface: str       # "芝" / "ダート"
    weather: str       # 天候
    condition: str     # 馬場状態（例: "良"）
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

    Args:
        url: 取得対象 URL
        max_retries: 最大リトライ回数
        delay: 初回リクエスト前の待機秒数（サーバー負荷軽減）
        timeout: 接続タイムアウト秒数

    Returns:
        レスポンス HTML 文字列

    Raises:
        requests.HTTPError: HTTP エラーが max_retries 回連続した場合
        requests.Timeout: タイムアウトが max_retries 回連続した場合
    """
    time.sleep(delay)

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=timeout,
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            return resp.text
        except (requests.HTTPError, requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "リクエスト失敗 (試行 %d/%d): %s — %ds 後にリトライ",
                attempt,
                max_retries,
                url,
                wait,
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise requests.RequestException(
        f"{url} の取得に {max_retries} 回失敗しました"
    ) from last_exc


# ---------------------------------------------------------------------------
# パーサー
# ---------------------------------------------------------------------------
def _parse_rank(raw: str) -> Optional[int]:
    """着順文字列を int に変換。失格・除外等は None を返す。"""
    raw = raw.strip()
    if raw.isdigit():
        return int(raw)
    return None


def _parse_float(raw: str) -> Optional[float]:
    """数値文字列を float に変換。変換不可は None を返す。"""
    try:
        return float(raw.strip().replace(",", ""))
    except ValueError:
        return None


def _parse_int(raw: str) -> Optional[int]:
    """数値文字列（馬体重等）を int に変換。"""
    try:
        # "480(+2)" → 480
        return int(raw.strip().split("(")[0])
    except (ValueError, IndexError):
        return None


def _parse_race_info(soup: BeautifulSoup, race_id: str) -> RaceInfo:
    """レース基本情報を解析する。"""
    race_name = ""
    name_tag = soup.select_one("h1.RaceName, div.race_name h1")
    if name_tag:
        race_name = name_tag.get_text(strip=True)

    # レース詳細テキスト（距離・馬場・天候・馬場状態）
    date, venue, race_number = "", "", 0
    distance, surface, weather, condition = 0, "", "", ""

    data_tag = soup.select_one("div.RaceData01, p.smalltxt")
    if data_tag:
        text = data_tag.get_text(" ", strip=True)
        # 距離・芝ダート
        import re
        m = re.search(r"(芝|ダート)(\d+)m", text)
        if m:
            surface = m.group(1)
            distance = int(m.group(2))
        m = re.search(r"天候\s*[:：]\s*(\S+)", text)
        if m:
            weather = m.group(1)
        m = re.search(r"馬場\s*[:：]\s*(\S+)", text)
        if m:
            condition = m.group(1)

    date_tag = soup.select_one("div.RaceData02 span, dd.smalltxt")
    if date_tag:
        import re
        m = re.search(r"\d{4}年\d{1,2}月\d{1,2}日", date_tag.get_text())
        if m:
            date = m.group(0).replace("年", "/").replace("月", "/").replace("日", "")

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


def _parse_results_table(soup: BeautifulSoup) -> list[tuple[str, str, str]]:
    """
    結果テーブルから (horse_name, horse_id, row_cells_raw) を抽出する。

    Returns:
        (horse_name, horse_id, cells) のリスト。cells は td テキストのリスト。
    """
    table = soup.select_one("table.race_table_01, table.nk_tb_common")
    if table is None:
        return []

    rows = table.select("tr")[1:]  # ヘッダー行をスキップ
    parsed: list[tuple[str, str, list[str]]] = []
    for tr in rows:
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cells) < 10:
            continue
        horse_link = tr.select_one("td a[href*='/horse/']")
        horse_name = horse_link.get_text(strip=True) if horse_link else cells[3]
        horse_id = ""
        if horse_link and horse_link.get("href"):
            parts = str(horse_link["href"]).rstrip("/").split("/")
            horse_id = parts[-1]
        parsed.append((horse_name, horse_id, cells))

    return parsed  # type: ignore[return-value]


def _fetch_pedigree(horse_id: str, delay: float = 1.5) -> PedigreeInfo:
    """
    馬 ID から血統情報（父・母・母父）を取得する。

    Args:
        horse_id: netkeiba 馬 ID
        delay: リクエスト前の待機秒数

    Returns:
        PedigreeInfo。取得失敗時は空フィールドで返す。
    """
    if not horse_id:
        return PedigreeInfo()

    url = HORSE_URL_TEMPLATE.format(horse_id=horse_id)
    try:
        html = _fetch_html(url, delay=delay)
    except requests.RequestException as exc:
        logger.warning("血統取得失敗 horse_id=%s: %s", horse_id, exc)
        return PedigreeInfo()

    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.blood_table, table.db_prof_table")
    if table is None:
        return PedigreeInfo()

    cells = [td.get_text(strip=True) for td in table.select("td")]
    # blood_table の構造: [父, 父父, 父母, 母, 母父, 母母, ...]
    sire = cells[0] if len(cells) > 0 else None
    dam = cells[3] if len(cells) > 3 else None
    dam_sire = cells[4] if len(cells) > 4 else None

    return PedigreeInfo(sire=sire, dam=dam, dam_sire=dam_sire)


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
        race_id: netkeiba レース ID（例: "202306050811"）
        fetch_pedigree: True の場合、各馬の血統情報も取得する（追加リクエスト発生）
        delay: 各リクエスト前の待機秒数
        max_retries: HTTP リトライ上限

    Returns:
        RaceInfo（レース基本情報 + 各馬結果リスト）

    Raises:
        requests.RequestException: レースページの取得に失敗した場合
        ValueError: レース ID が不正な場合
    """
    if not race_id or not race_id.isdigit():
        raise ValueError(f"不正なレース ID: {race_id!r}")

    url = RACE_URL_TEMPLATE.format(race_id=race_id)
    logger.info("レース結果取得開始: race_id=%s", race_id)

    html = _fetch_html(url, max_retries=max_retries, delay=delay)
    soup = BeautifulSoup(html, "lxml")

    race_info = _parse_race_info(soup, race_id)
    raw_rows = _parse_results_table(soup)

    results: list[HorseResult] = []
    for horse_name, horse_id, cells in raw_rows:
        # 列インデックスは netkeiba の標準レイアウトに準拠
        # [0]着順 [1]枠 [2]馬番 [3]馬名 [4]性齢 [5]斤量 [6]騎手
        # [7]タイム [8]着差 [9]人気 [10]単勝 [11]馬体重 ...
        pedigree = PedigreeInfo()
        if fetch_pedigree and horse_id:
            pedigree = _fetch_pedigree(horse_id, delay=delay)

        result = HorseResult(
            rank=_parse_rank(cells[0]) if len(cells) > 0 else None,
            horse_name=horse_name,
            horse_id=horse_id or None,
            sex_age=cells[4] if len(cells) > 4 else "",
            weight_carried=_parse_float(cells[5]) or 0.0 if len(cells) > 5 else 0.0,
            jockey=cells[6] if len(cells) > 6 else "",
            finish_time=cells[7] if len(cells) > 7 else None,
            margin=cells[8] if len(cells) > 8 else None,
            popularity=_parse_int(cells[9]) if len(cells) > 9 else None,
            win_odds=_parse_float(cells[10]) if len(cells) > 10 else None,
            horse_weight=_parse_int(cells[11]) if len(cells) > 11 else None,
            pedigree=pedigree,
        )
        results.append(result)

    race_info.results = results
    logger.info(
        "取得完了: race_id=%s, 出走頭数=%d", race_id, len(results)
    )
    return race_info
