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

from src.scraper.http_client import (
    NETKEIBA_LIMITER,
    backoff_seconds,
    build_headers,
    is_retryable_status,
    retry_after_seconds,
)

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
    horse_number: int  # 馬番
    gate_number: int  # 枠番
    horse_id: str | None  # horse_id（netkeiba）
    horse_name: str  # 馬名
    sex_age: str  # 性齢 例 "牡3"
    weight_carried: float  # 斤量
    jockey: str  # 騎手名
    trainer: str  # 調教師名
    horse_weight: int | None  # 馬体重（kg）
    horse_weight_diff: int | None  # 前走比（+2 / -4 / 0）


@dataclass
class EntryTable:
    race_id: str
    entries: list[EntryHorse] = field(default_factory=list)
    race_name: str = ""
    distance: int = 0
    surface: str = ""
    track_direction: str = ""
    weather: str = ""
    condition: str = ""
    post_time: str = ""  # 実発走時刻 "HH:MM"


@dataclass
class HorseOdds:
    horse_number: int
    win_odds: float | None  # 単勝オッズ
    place_odds_min: float | None  # 複勝オッズ（下限）
    place_odds_max: float | None  # 複勝オッズ（上限）
    popularity: int | None  # 人気順


# ── 内部ユーティリティ ────────────────────────────────────────────


def _http_get(
    url: str,
    params: dict | None,
    timeout: int,
    *,
    max_retries: int = 3,
    base_delay: float = 1.5,
) -> requests.Response:
    """堅牢化 HTTP GET（netkeiba 503/429/403 対策・http_client 共通ロジック）。

    - グローバルレート制限（並列スレッドの自己 DoS 防止）
    - User-Agent ローテーション + ブラウザ完全ヘッダ（bot 判定回避）
    - Retry-After ヘッダの尊重（429/503）と、ステータス別の長めバックオフ
    - 恒久エラー（404 等の 429 以外 4xx）はリトライせず即中断

    Args:
        url:         取得先 URL。
        params:      クエリパラメータ。
        timeout:     HTTP タイムアウト秒数。
        max_retries: 最大試行回数。
        base_delay:  バックオフ基準秒数。

    Returns:
        取得成功した requests.Response。

    Raises:
        requests.RequestException: max_retries 回失敗した場合（恒久エラーは即時）。
    """
    last_exc: Exception | None = None
    last_status: int | None = None
    for attempt in range(1, max_retries + 1):
        NETKEIBA_LIMITER.wait()
        try:
            resp = requests.get(
                url, params=params, headers=build_headers(), timeout=timeout
            )
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            last_exc = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            last_status = status
            if not is_retryable_status(status):
                logger.warning(
                    "netkeiba 取得中断 (HTTP %s, リトライ不可): %s", status, url
                )
                raise
            ra = retry_after_seconds(getattr(exc, "response", None))
            wait = (
                ra if ra is not None else backoff_seconds(attempt, base_delay, status)
            )
            logger.warning(
                "リクエスト失敗 (試行 %d/%d, HTTP %s): %s — %.1f秒後にリトライ",
                attempt,
                max_retries,
                status,
                url,
                wait,
            )
            if attempt < max_retries:
                time.sleep(wait)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            wait = backoff_seconds(attempt, base_delay, None)
            logger.warning(
                "リクエスト失敗 (試行 %d/%d, %s): %s — %.1f秒後にリトライ",
                attempt,
                max_retries,
                type(exc).__name__,
                url,
                wait,
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise requests.RequestException(
        f"{url} の取得に {max_retries} 回失敗しました (最終HTTP={last_status})"
    ) from last_exc


def _fetch(
    url: str,
    params: dict | None = None,
    *,
    delay: float = 1.5,
    timeout: int = 20,
    max_retries: int = 3,
) -> str:
    """
    レート制限・UA ローテーション・Retry-After 付き HTTP GET。

    実際の待機・リトライ・ヘッダ生成は _http_get（http_client 共通ロジック）が担う。
    max_retries 回失敗した場合は requests.RequestException を送出する。

    Args:
        url:         取得先 URL
        params:      クエリパラメータ
        delay:       バックオフ基準秒数（_http_get の base_delay に渡す）
        timeout:     HTTP タイムアウト秒数
        max_retries: 最大試行回数

    Returns:
        レスポンス本文（文字列）

    Raises:
        requests.RequestException: max_retries 回失敗した場合
    """
    # レート制限・リトライは _http_get 内の NETKEIBA_LIMITER / バックオフが担う
    resp = _http_get(url, params, timeout, max_retries=max_retries, base_delay=delay)
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def _parse_weight(text: str) -> tuple[int | None, int | None]:
    """馬体重テキストを (体重, 増減) にパースする。

    Args:
        text: 馬体重テキスト（例: "482 (+2)"）。

    Returns:
        (体重kg, 増減kg) のタプル。"計不" や空文字は (None, None)。
    """
    m = re.search(r"(\d+)\s*\(([+\-]?\d+)\)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d{3,})", text)
    if m2:
        return int(m2.group(1)), None
    return None, None


def _safe_float(text: str) -> float | None:
    """文字列を float に変換する。0 以下は None を返す。

    Args:
        text: 変換対象の文字列。

    Returns:
        変換後の正の浮動小数点数。変換不可または 0 以下は None。
    """
    try:
        v = float(text.strip())
        return v if v > 0 else None
    except (ValueError, AttributeError):
        return None


def _safe_int(text: str) -> int | None:
    """文字列を int に変換する。変換不可は None を返す。

    Args:
        text: 変換対象の文字列。

    Returns:
        変換後の整数。変換不可は None。
    """
    try:
        return int(text.strip())
    except (ValueError, AttributeError):
        return None


# ── 出馬表スクレイパー ───────────────────────────────────────────


def _parse_race_condition(soup: BeautifulSoup) -> str | None:
    """
    出馬表ページ（shutuba.html）から馬場状態を抽出する。

    netkeiba shutuba ページの実 HTML 構造（2025年時点）:
      div.RaceData01 内テキストに "馬場 : 良" や "芝 : 稍重" が含まれる。
      または div.RaceData02 の span.turf_state 等でも確認できる。

    Returns:
        "良" / "稍重" / "重" / "不良" / None（未発表・取得不可）
    """
    # 優先順に複数セレクタを試みる
    for selector in (
        "div.RaceData01",
        "div.RaceData02",
        "dl.racedata",
        "div.mainrace_data",
    ):
        tag = soup.select_one(selector)
        if not tag:
            continue
        text = tag.get_text(" ", strip=True)
        # "馬場 : 良", "芝 : 稍重", "ダート : 重", "不良"
        m = re.search(r"(?:馬場|芝|ダート)\s*[：:]\s*([良稍重不]+)", text)
        if m:
            return m.group(1)
    return None


def _parse_race_header(soup: BeautifulSoup) -> tuple[str, int, str, str, str, str, str]:
    """
    出馬表ページ（shutuba.html）からレース基本情報を抽出する。

    Returns:
        (race_name, distance, surface, track_direction, weather, condition, post_time)
        - race_name: レース名（例: "3歳未勝利"）
        - distance: 距離 m（例: 1800）、未取得時は 0
        - surface: "芝" / "ダート" / "障害" / ""
        - track_direction: "右" / "左" / "直線" / ""
        - weather: "晴" / "曇" / "雨" / ""
        - condition: "良" / "稍重" / "重" / "不良" / ""
        - post_time: 実発走時刻 "HH:MM"（例: "09:50"）、未取得時は ""
    """
    race_name = ""
    distance = 0
    surface = ""
    track_direction = ""
    weather = ""
    condition = ""
    post_time = ""

    # --- レース名 ---
    for sel in ("div.RaceName", "h1.RaceName", "span.RaceName"):
        tag = soup.select_one(sel)
        if tag:
            race_name = tag.get_text(strip=True)
            break
    if not race_name:
        # RaceList_Item02 / レースタイトル行（テキスト先頭部分）
        tag = soup.select_one("div.RaceList_Item02")
        if tag:
            raw = tag.get_text(" ", strip=True)
            # "3歳未勝利 09:50発走 / ダ1800m..." の先頭部分
            race_name = re.split(r"\s+\d{2}:\d{2}発走", raw)[0].strip()

    # --- div.RaceData01 から距離・馬場・天候・馬場状態を取得 ---
    data01 = soup.select_one("div.RaceData01")
    if data01:
        text = data01.get_text(" ", strip=True)

        # 距離・馬場種別: "ダ1800m" / "芝2500m" / "障2970m (右)"
        m = re.search(
            r"(芝|ダート|ダ|障害|障)\s*(右\s*外|左\s*外|右|左|直線?)?\s*(\d+)m",
            text,
        )
        if m:
            raw_surf = m.group(1)
            if raw_surf == "芝":
                surface = "芝"
            elif raw_surf in ("障", "障害"):
                surface = "障害"
            else:
                surface = "ダート"
            track_direction = (m.group(2) or "").replace(" ", "")
            distance = int(m.group(3))

        # 天候
        mw = re.search(r"天候\s*[：:]\s*(\S+?)(?:\s|/|$)", text)
        if mw:
            weather = mw.group(1)

        # 馬場状態
        mc = re.search(r"馬場\s*[：:]\s*([良稍重不]+)", text)
        if not mc:
            mc = re.search(r"(?:芝|ダート)\s*[：:]\s*([良稍重不]+)", text)
        if mc:
            condition = mc.group(1)

    # --- 発走時刻 "HH:MM発走" を RaceData01 / RaceList_Item02 から抽出 ---
    for sel in ("div.RaceData01", "div.RaceList_Item02"):
        tag = soup.select_one(sel)
        if not tag:
            continue
        mt = re.search(r"(\d{1,2}:\d{2})\s*発走", tag.get_text(" ", strip=True))
        if mt:
            hh, mm = mt.group(1).split(":")
            post_time = f"{int(hh):02d}:{int(mm):02d}"
            break

    return race_name, distance, surface, track_direction, weather, condition, post_time


def _find_weight_cell(cells: list, horse_name: str) -> tuple[int | None, int | None]:
    """
    cells リストから馬体重セルを探してパースする。

    Strategy:
      1. class="Weight" の td を優先探索（最も信頼性が高い）
      2. フォールバック: cells[8] のインデックスアクセス（旧仕様）
      3. 最後の手段: 3桁数値パターンを含むセルをスキャン
    """
    # Strategy 1: class属性でクラスが "Weight" の td
    for cell in cells:
        classes = cell.get("class", [])
        if any("Weight" in c for c in classes):
            return _parse_weight(cell.get_text(" ", strip=True))

    # Strategy 2: cells[8] インデックスアクセス
    if len(cells) >= 9:
        text = cells[8].get_text(" ", strip=True)
        hw, hd = _parse_weight(text)
        if hw is not None:
            return hw, hd

    # Strategy 3: 後ろから走査して "NNN (+/-N)" パターンを探す
    for cell in reversed(cells):
        text = cell.get_text(" ", strip=True)
        if re.search(r"\d{3}\s*\([+\-]?\d+\)", text):
            return _parse_weight(text)

    return None, None


def _parse_entry_rows(soup: BeautifulSoup) -> list[EntryHorse]:
    """
    出馬表 HTML（BeautifulSoup 解析済み）から EntryHorse のリストを返す。

    列マッピング（Shutuba_Table の td インデックス）:
      [0] 枠番  [1] 馬番  [3] 馬名 / horse_id  [4] 性齢
      [5] 斤量  [6] 騎手  [7] 調教師  [8] 馬体重（追加列により変動の可能性）
    """
    entries: list[EntryHorse] = []
    rows = soup.select("table.Shutuba_Table tr.HorseList")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 9:
            continue

        gate_number = _safe_int(cells[0].get_text(strip=True)) or 0
        horse_number = _safe_int(cells[1].get_text(strip=True)) or 0

        # 馬名・horse_id: <td class="HorseInfo"> の <a> リンク
        # セル[2]は CheckMark (フィルタ用チェックボックス) のため [3] を参照する
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

        # 馬体重: 複数セレクタ戦略でHTML構造変更に対応
        horse_weight, horse_weight_diff = _find_weight_cell(cells, horse_name or "")

        if horse_number < 1:
            logger.debug(
                "horse_number < 1 の行をスキップ (gate=%d, name=%r)",
                gate_number,
                horse_name,
            )
            continue

        entries.append(
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

    if not entries:
        # 0頭はHTML構造変更の可能性が高いため診断情報を出力する
        tables_found = soup.find_all("table", class_="Shutuba_Table")
        all_horse_rows = soup.find_all("tr", class_="HorseList")
        logger.warning(
            "⚠️ 出馬表の取得結果が 0 頭です。HTML 構造変更の可能性があります。"
            " Shutuba_Table=%d件 tr.HorseList(全テーブル合計)=%d件 "
            "tr.HorseList(対象rows)=%d件",
            len(tables_found),
            len(all_horse_rows),
            len(rows),
        )

    return entries


def fetch_entry_table(
    race_id: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
) -> EntryTable:
    """
    race.netkeiba.com から出馬表を取得して EntryTable を返す。

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
    table.entries = _parse_entry_rows(soup)

    # レース基本情報（距離・馬場・馬場状態・発走時刻）を同一ページから取得
    rname, dist, surf, tdir, weather, cond, ptime = _parse_race_header(soup)
    table.race_name = rname
    table.distance = dist
    table.surface = surf
    table.track_direction = tdir
    table.weather = weather
    table.condition = cond
    table.post_time = ptime

    if len(table.entries) == 0:
        logger.error(
            "🚨 出馬表が 0 頭 (race_id=%s) — netkeiba HTML 構造変更またはページ未公開の可能性",
            race_id,
        )
    else:
        logger.info(
            "出馬表 race_id=%s: %d 頭取得 (dist=%dm surface=%s)",
            race_id,
            len(table.entries),
            dist,
            surf,
        )
    return table


def fetch_live_race_info(
    race_id: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
) -> tuple[str | None, list[EntryHorse]]:
    """
    出馬表ページから「馬場状態」と「最新の馬体重」を1リクエストで取得する。

    prerace_pipeline での使用を想定。金曜バッチで entries を保存済みでも、
    当日発表された馬体重・馬場状態でDBを更新するために再取得する。

    馬体重が取得できない場合（発表前）は horse_weight=None の EntryHorse を返す。
    馬場状態が取得できない場合は None を返す（レース後の確定前など）。

    Args:
        race_id:     netkeiba の race_id
        delay:       リクエスト間隔（秒）
        max_retries: 最大リトライ回数

    Returns:
        (condition, entries)
        - condition: "良" / "稍重" / "重" / "不良" / None
        - entries:   最新馬体重を含む EntryHorse リスト（空リストの場合あり）
    """
    html = _fetch(
        SHUTUBA_URL,
        params={"race_id": race_id},
        delay=delay,
        max_retries=max_retries,
    )
    soup = BeautifulSoup(html, "lxml")
    condition = _parse_race_condition(soup)
    entries = _parse_entry_rows(soup)
    logger.info(
        "ライブ情報取得 race_id=%s: 馬場=%s 馬体重=%d頭",
        race_id,
        condition or "未発表",
        len(entries),
    )
    return condition, entries


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
        # 新形式: {"status":..., "data": {"odds": {"1": {...}, "2": {...}}}}
        # 旧形式: {"1": {"01": [...]}} — 単勝/複勝ともに外部キーは常に "1"
        nested = data.get("data", {}).get("odds", {})
        if nested:
            return nested.get(str(odds_type), {})
        return data.get("1", {}) or {}

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
