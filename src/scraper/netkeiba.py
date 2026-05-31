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

from src.scraper.http_client import (
    NETKEIBA_LIMITER,
    backoff_seconds,
    build_headers,
    is_retryable_status,
    retry_after_seconds,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
RACE_URL_TEMPLATE  = "https://race.netkeiba.com/race/result.html?race_id={race_id}"
PED_URL_TEMPLATE   = "https://db.netkeiba.com/horse/ped/{horse_id}/"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9",
}

# netkeiba 結果テーブルの列インデックス（race.netkeiba.com/race/result.html 実測・2025年時点）
# [0]着順 [1]枠番 [2]馬番 [3]馬名 [4]性齢 [5]斤量 [6]騎手 [7]タイム [8]着差
# [9]人気 [10]単勝 [11]上がり [12]通過 [13]調教師 [14]馬体重
_COL_RANK          = 0
_COL_GATE_NUMBER   = 1   # 枠番
_COL_HORSE_NUMBER  = 2   # 馬番
_COL_HORSE_NAME    = 3
_COL_SEX_AGE       = 4
_COL_WEIGHT        = 5
_COL_JOCKEY        = 6
_COL_TIME          = 7
_COL_MARGIN        = 8
_COL_POPULARITY    = 9
_COL_WIN_ODDS      = 10
_COL_TRAINER       = 13
_COL_HORSE_WEIGHT  = 14


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
    rank:              Optional[int]    # 着順（失格・除外は None）
    horse_name:        str              # 馬名
    horse_id:          Optional[str]    # netkeiba 馬ID
    gate_number:       Optional[int]    # 枠番（1〜8）
    horse_number:      Optional[int]    # 馬番
    sex_age:           str              # 性齢（例: "牡3"）
    weight_carried:    float            # 斤量 (kg)
    jockey:            str              # 騎手名
    trainer:           str              # 調教師名
    finish_time:       Optional[str]    # タイム（例: "2:31.5"）
    margin:            Optional[str]    # 着差（例: "クビ"）
    popularity:        Optional[int]    # 人気順位
    win_odds:          Optional[float]  # 単勝オッズ
    horse_weight:      Optional[int]    # 馬体重 (kg)
    horse_weight_diff: Optional[int]    # 馬体重増減（例: +2, -4）
    pedigree: PedigreeInfo = field(default_factory=PedigreeInfo)


@dataclass
class RaceInfo:
    """レース基本情報 + 出走結果"""
    race_id:         str
    race_name:       str
    date:            str    # "YYYY-MM-DD" (ISO 8601)
    venue:           str    # 開催場所（例: "中山"）
    race_number:     int    # 第N競走
    distance:        int    # 距離 (m)
    surface:         str    # "芝" / "ダート"
    track_direction: str    # コース方向（"右" / "左" / "直線" / ""）
    weather:         str    # 天候
    condition:       str    # 馬場状態（例: "良"）
    results: list[HorseResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP ユーティリティ
# ---------------------------------------------------------------------------

def _detect_encoding(resp: requests.Response) -> str:
    """
    レスポンスのエンコーディングを確定する。

    優先順位:
      1. Content-Type ヘッダーに charset が明示されている場合はそれを使用
      2. apparent_encoding (chardet/charset-normalizer による自動検知)
         - "euc" が含まれる場合 → "euc-jp"
         - "utf" が含まれる場合 → "utf-8"
         - "mac" が含まれる場合 → フォールバック (mac-greek 等は誤検知しやすい)
      3. フォールバック: "euc-jp" (netkeiba 旧来のデフォルト)

    背景: db.netkeiba.com の血統ページが EUC-JP を返すが、
    chardet が MacGreek 等に誤検知して文字化けが発生したため、
    Content-Type 優先 + "mac" 検知時のフォールバックを実装した。
    """
    ct = resp.headers.get("Content-Type", "").lower()
    if "utf-8" in ct or "utf8" in ct:
        return "utf-8"
    if "euc" in ct:
        return "euc-jp"
    if "shift_jis" in ct or "sjis" in ct or "shift-jis" in ct:
        return "cp932"

    apparent = (resp.apparent_encoding or "").lower()
    if "utf" in apparent:
        return "utf-8"
    if "euc" in apparent:
        return "euc-jp"
    if "shift" in apparent or "sjis" in apparent or "932" in apparent:
        return "cp932"
    # chardet が MacGreek / MacRoman 等を誤検知した場合は euc-jp にフォールバック
    if "mac" in apparent or "greek" in apparent or "iso-8859-7" in apparent:
        logger.debug(
            "apparent_encoding=%r は誤検知の可能性があるため euc-jp にフォールバック",
            resp.apparent_encoding,
        )
        return "euc-jp"

    return "euc-jp"


def _fetch_html(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    max_retries: int = 3,
    delay: float = 1.5,
    timeout: tuple[int, int] = (8, 20),
) -> str:
    """URL を取得して HTML 文字列を返す。

    失敗時はエクスポネンシャルバックオフでリトライする。

    Args:
        url:         取得先 URL。
        session:     再利用する requests.Session（None の場合は都度 requests.get）。
        max_retries: 最大リトライ回数。
        delay:       初回待機秒数（リトライごとに指数的に増加）。
        timeout:     (接続タイムアウト秒, 読み取りタイムアウト秒)。

    Returns:
        レスポンス本文の HTML 文字列。

    Raises:
        requests.RequestException: max_retries 回失敗した場合。

    Notes:
        503 / 429 / 403 多発（2026-05-31 本番障害）への対策として、
        http_client の共通ロジックを利用する:
          - グローバルレート制限（並列スレッドの自己 DoS 防止）
          - User-Agent ローテーション + ブラウザ完全ヘッダ（bot 判定回避）
          - Retry-After ヘッダの尊重（429/503）
          - 恒久エラー（404 等の 429 以外 4xx）は即中断
    """
    requester = session.get if session is not None else requests.get

    last_exc: Optional[Exception] = None
    last_status: Optional[int] = None
    for attempt in range(1, max_retries + 1):
        # グローバルレート制限: 並列スレッドが netkeiba を一斉に叩くのを抑制
        NETKEIBA_LIMITER.wait()
        try:
            resp = requester(url, headers=build_headers(), timeout=timeout)
            resp.raise_for_status()
            resp.encoding = _detect_encoding(resp)
            return resp.text
        except requests.HTTPError as exc:
            last_exc = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            last_status = status
            # 恒久エラー（404 等、429 以外の 4xx）はリトライ無意味 → 即中断
            if not is_retryable_status(status):
                logger.warning(
                    "netkeiba 取得中断 (HTTP %s, リトライ不可): %s", status, url
                )
                raise
            ra = retry_after_seconds(getattr(exc, "response", None))
            wait = ra if ra is not None else backoff_seconds(attempt, delay, status)
            logger.warning(
                "リクエスト失敗 (試行 %d/%d, HTTP %s): %s — %.1f秒後にリトライ",
                attempt, max_retries, status, url, wait,
            )
            if attempt < max_retries:
                time.sleep(wait)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            wait = backoff_seconds(attempt, delay, None)
            logger.warning(
                "リクエスト失敗 (試行 %d/%d, %s): %s — %.1f秒後にリトライ",
                attempt, max_retries, type(exc).__name__, url, wait,
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise requests.RequestException(
        f"{url} の取得に {max_retries} 回失敗しました (最終HTTP={last_status})"
    ) from last_exc


# ---------------------------------------------------------------------------
# パーサー共通ユーティリティ
# ---------------------------------------------------------------------------
def _parse_rank(raw: str) -> Optional[int]:
    """着順文字列を int に変換する。失格・除外等は None を返す。

    Args:
        raw: 着順テキスト（例: "1"、"除"）。

    Returns:
        整数の着順。数字以外は None。
    """
    raw = raw.strip()
    return int(raw) if raw.isdigit() else None


def _parse_float(raw: str) -> Optional[float]:
    """数値文字列を float に変換する。変換不可は None を返す。

    Args:
        raw: 数値テキスト（例: "3.8", "1,380"）。

    Returns:
        変換後の浮動小数点数。変換不可は None。
    """
    try:
        return float(raw.strip().replace(",", ""))
    except ValueError:
        return None


def _parse_int(raw: str) -> Optional[int]:
    """数値文字列（馬体重等）を int に変換する。"480(+2)" → 480。

    Args:
        raw: 数値テキスト（例: "480(+2)", "14"）。

    Returns:
        括弧前の整数部分。変換不可は None。
    """
    try:
        return int(raw.strip().split("(")[0])
    except (ValueError, IndexError):
        return None


def _parse_weight_diff(raw: str) -> Optional[int]:
    """馬体重増減を抽出する。"480(+2)" → 2、"480(-4)" → -4、"480" → None。

    Args:
        raw: 馬体重テキスト（例: "480(+2)"）。

    Returns:
        符号付き増減値。括弧がない場合は None。
    """
    m = re.search(r"\(([+-]?\d+)\)", raw)
    return int(m.group(1)) if m else None


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
    race_name       = ""
    distance        = 0
    surface         = ""
    track_direction = ""
    weather         = ""
    condition       = ""
    date            = ""
    venue           = ""
    race_number     = 0

    # --- レース名 ---
    # div.RaceList_Item02 に "3歳未勝利 09:45発走 / ダ1700m ..." のような形式
    name_tag = soup.select_one("div.RaceList_Item02, div.RaceName, h1.RaceName")
    if name_tag:
        race_name = name_tag.get_text(" ", strip=True).split("発走")[0].strip().split()
        race_name = race_name[0] if race_name else race_name

    # --- 距離・天候・馬場（RaceData01）---
    # 例: "09:45発走 / ダ1700m (右) / 天候:晴 / 馬場:良"
    data01 = soup.select_one("div.RaceData01")
    if data01:
        text = data01.get_text(" ", strip=True)

        # 距離・馬場種別: "ダ1700m" / "芝2500m" / "芝1600m (右)" / "障2970m (右 ダート)"
        # ※ 障害レースは "障" プレフィックス、外回りは "右 外" のようにスペース入り
        m = re.search(
            r"(芝|ダート|ダ|障害|障)\s*"
            r"(右\s*外|左\s*外|右|左|直線?)?\s*"
            r"(\d+)m",
            text,
        )
        if m:
            raw_surf = m.group(1)
            if raw_surf in ("芝",):
                surface = "芝"
            elif raw_surf in ("障", "障害"):
                surface = "障害"
            else:
                surface = "ダート"
            track_direction = (m.group(2) or "").replace(" ", "")
            distance        = int(m.group(3))

        # 天候
        m = re.search(r"天候\s*[：:]\s*(\S+?)(?:\s|/|$)", text)
        if m:
            weather = m.group(1)

        # 馬場状態
        m = re.search(r"馬場\s*[：:]\s*(\S+?)(?:\s|/|$)", text)
        if m:
            condition = m.group(1)

    # --- 日付・開催場所・回次（RaceData02）---
    # 例: "1回 福島 2日目 サラ系３歳 未勝利 牝[指] 馬齢 15頭 ..."
    data02 = soup.select_one("div.RaceData02")
    if data02:
        text = data02.get_text(" ", strip=True)

        # 開催場所: "1回 福島 2日目" → venue="福島"
        m = re.search(r"\d+回\s*(\S+?)\s*\d+日目", text)
        if m:
            venue = m.group(1)

        # 回次: "1回" → 1
        m = re.search(r"(\d+)回", text)
        if m:
            race_number = int(m.group(1))

    # race_id から日付を確定（YYYYMMDD → YYYY-MM-DD）
    if not date and len(race_id) >= 8:
        ymd = race_id[:8]
        # races テーブルに既存データがある場合は優先するため、ここでは race_id ベースで補完のみ
        # ただし race_id 先頭8文字が年月日ではなくプレフィックスの場合があるため DB 照会を優先
        # ここでは空のまま返し、呼び出し元で races テーブルの既存日付を利用する

    return RaceInfo(
        race_id=race_id,
        race_name=race_name,
        date=date,
        venue=venue,
        race_number=race_number,
        distance=distance,
        surface=surface,
        track_direction=track_direction,
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
    table = soup.select_one("table.RaceTable01")
    if table is None:
        return []

    result: list[tuple[str, str, list[str]]] = []
    for tr in table.select("tr.HorseList"):
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
def _fetch_pedigree(
    horse_id: str,
    delay: float = 1.5,
    session: Optional[requests.Session] = None,
) -> PedigreeInfo:
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
        html = _fetch_html(url, session=session, delay=delay)
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
    race_date: Optional[str] = None,
    fetch_pedigree: bool = True,
    delay: float = 1.5,
    max_retries: int = 3,
    session: Optional[requests.Session] = None,
) -> RaceInfo:
    """
    レース ID を指定してレース結果を取得する。

    Args:
        race_id:      netkeiba レース ID（例: "202506050811"）
        race_date:    日付文字列（"YYYYMMDD" または "YYYY-MM-DD"）。HTML から
                      日付が取得できない場合のフォールバックに使用する。
        fetch_pedigree: True の場合、各馬の血統情報も取得する
        delay:        各リクエスト前の待機秒数（サーバー負荷軽減）
        max_retries:  HTTP リトライ上限
        session:      再利用する requests.Session（スレッド並列時に渡す）

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
    html = _fetch_html(url, session=session, max_retries=max_retries, delay=delay)
    soup = BeautifulSoup(html, "lxml")

    race_info = _parse_race_info(soup, race_id)

    # HTML から日付が取得できなかった場合、引数 race_date でフォールバック
    if not race_info.date and race_date:
        d = race_date.replace("-", "")   # "YYYYMMDD" に正規化
        race_info.date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    raw_rows  = _parse_results_table(soup)

    results: list[HorseResult] = []
    for horse_name, horse_id, cells in raw_rows:
        ped = _fetch_pedigree(horse_id, delay=delay, session=session) if fetch_pedigree and horse_id else PedigreeInfo()

        hw_raw = cells[_COL_HORSE_WEIGHT] if len(cells) > _COL_HORSE_WEIGHT else ""
        results.append(HorseResult(
            rank              = _parse_rank(cells[_COL_RANK])                    if len(cells) > _COL_RANK          else None,
            horse_name        = horse_name,
            horse_id          = horse_id or None,
            gate_number       = _parse_int(cells[_COL_GATE_NUMBER])              if len(cells) > _COL_GATE_NUMBER   else None,
            horse_number      = _parse_int(cells[_COL_HORSE_NUMBER])             if len(cells) > _COL_HORSE_NUMBER  else None,
            sex_age           = cells[_COL_SEX_AGE]                              if len(cells) > _COL_SEX_AGE        else "",
            weight_carried    = _parse_float(cells[_COL_WEIGHT]) or 0.0          if len(cells) > _COL_WEIGHT         else 0.0,
            jockey            = cells[_COL_JOCKEY]                               if len(cells) > _COL_JOCKEY         else "",
            trainer           = cells[_COL_TRAINER]                              if len(cells) > _COL_TRAINER        else "",
            finish_time       = cells[_COL_TIME]   or None                       if len(cells) > _COL_TIME           else None,
            margin            = cells[_COL_MARGIN] or None                       if len(cells) > _COL_MARGIN         else None,
            win_odds          = _parse_float(cells[_COL_WIN_ODDS])               if len(cells) > _COL_WIN_ODDS       else None,
            popularity        = _parse_int(cells[_COL_POPULARITY])               if len(cells) > _COL_POPULARITY     else None,
            horse_weight      = _parse_int(hw_raw),
            horse_weight_diff = _parse_weight_diff(hw_raw),
            pedigree          = ped,
        ))

    race_info.results = results
    logger.info("取得完了: race_id=%s, 出走頭数=%d", race_id, len(results))
    return race_info


# ---------------------------------------------------------------------------
# 払戻テーブルパーサー
# ---------------------------------------------------------------------------

# race.netkeiba.com 払戻テーブルの th テキスト → 内部 bet_type マッピング
_TH_TEXT_TO_BET_TYPE: dict[str, str] = {
    "単勝":   "単勝",
    "複勝":   "複勝",
    "枠連":   "枠連",
    "馬連":   "馬連",
    "ワイド":  "ワイド",
    "馬単":   "馬単",
    "3連複":  "三連複",
    "3連単":  "三連単",
    "三連複":  "三連複",
    "三連単":  "三連単",
    "WIN5":   "WIN5",
    "ＷＩＮ５": "WIN5",
}

# th class → bet_type（旧 db.netkeiba.com 形式・後方互換）
_TH_CLASS_TO_BET_TYPE: dict[str, str] = {
    "tan":     "単勝",
    "fuku":    "複勝",
    "waku":    "枠連",
    "uren":    "馬連",
    "wide":    "ワイド",
    "utan":    "馬単",
    "sanfuku": "三連複",
    "santan":  "三連単",
}

# 着順依存型（組み合わせに → を使う）
_ORDERED_BET_TYPES = {"馬単", "三連単"}

# 馬券種ごとの組み合わせ馬番数（min, max）
_BET_COMBO_SIZES: dict[str, tuple[int, int]] = {
    "単勝":   (1, 1),
    "複勝":   (1, 1),
    "枠連":   (2, 2),
    "馬連":   (2, 2),
    "ワイド": (2, 2),
    "馬単":   (2, 2),
    "三連複": (3, 3),
    "三連単": (3, 3),
    "WIN5":   (5, 5),
}

# 馬券種ごとの最大有効番号（枠連のみ 1-8、他は 1-18）
_BET_MAX_NUM: dict[str, int] = {
    "単勝":   18,
    "複勝":   18,
    "枠連":   8,
    "馬連":   18,
    "ワイド": 18,
    "馬単":   18,
    "三連複": 18,
    "三連単": 18,
    "WIN5":   99,
}


def _normalize_combination(raw: str) -> str:
    """払戻組み合わせ文字列を "7-14" / "14→7" 形式に正規化する。

    全角数字・全角スペースを半角に変換し、区切り文字周囲の空白を除去する。

    Args:
        raw: 生の組み合わせ文字列。

    Returns:
        正規化後の組み合わせ文字列。
    """
    raw = raw.translate(str.maketrans("０１２３４５６７８９　", "0123456789 "))
    raw = re.sub(r"\s*→\s*", "→", raw)
    raw = re.sub(r"\s*-\s*", "-", raw)
    return raw.strip()


def _parse_payout_int(raw: str) -> Optional[int]:
    """"1,380" / "250円" / "1,450円" → 1380 / 250 / 1450 に変換する。

    Args:
        raw: 払戻金テキスト。

    Returns:
        払戻金の整数値。変換不可は None。
    """
    numeric = re.sub(r"[^\d,]", "", raw.strip())
    if not numeric:
        return None
    try:
        return int(numeric.replace(",", ""))
    except ValueError:
        return None


def _td_row_texts(td) -> list[str]:
    """
    <td> から行ごとのテキストを抽出する。

    優先順位: <li> 要素 → <br> 分割 → テキスト全体。
    """
    lis = td.find_all("li", recursive=True)
    if lis:
        return [li.get_text(" ", strip=True) for li in lis]

    html = td.decode_contents()
    parts = [re.sub(r"<[^>]+>", "", p).strip() for p in re.split(r"<br\s*/?>", html)]
    parts = [p for p in parts if p]
    if parts:
        return parts

    text = td.get_text(" ", strip=True)
    return [text] if text else []


def _combo_li_nums(td, bet_type: str) -> list[list[int]]:
    """
    組み合わせ列 <td> から「組み合わせごとの馬番リスト」を抽出する。

    netkeiba の HTML パターン:
      A) <ul> 1個に <li> 1個ずつ馬番 → 1 <ul> = 1 組み合わせ
         (馬連/枠連/馬単/三連複/三連単: <ul><li>9</li><li>10</li><li></li></ul>)
      B) <ul> 複数 = 複数の組み合わせ
         (ワイド: <ul><li>9</li><li>10</li></ul><ul><li>5</li><li>9</li></ul>...)
      C) <div> ベース・<li> なし
         (単勝/複勝: <div><span>9</span></div><div><span>10</span></div>...)
    """
    max_num = _BET_MAX_NUM.get(bet_type, 18)
    combo_size = _BET_COMBO_SIZES.get(bet_type, (1, 99))
    result: list[list[int]] = []

    # パターン A/B: <ul> 要素があれば、各 <ul> が 1 組み合わせ
    uls = td.find_all("ul", recursive=False)
    if not uls:
        uls = td.find_all("ul", recursive=True)

    if uls:
        for ul in uls:
            # この <ul> 内の全 <li> から数値を収集
            nums: list[int] = []
            for li in ul.find_all("li", recursive=False):
                li_nums = [
                    int(n) for n in re.findall(r'\d+', li.get_text())
                    if 0 < int(n) <= max_num
                ]
                nums.extend(li_nums)
            if combo_size[0] <= len(nums) <= combo_size[1]:
                result.append(nums)
            elif len(nums) > combo_size[1]:
                # 超過した場合は先頭 N 個のみ使用
                result.append(nums[:combo_size[1]])
        if result:
            return result

    # パターン C: <div> または <span> のフラットリスト → 全数値を step 個ずつ分割
    all_nums = [
        int(n) for n in re.findall(r'\d+', td.get_text())
        if 0 < int(n) <= max_num
    ]
    step = combo_size[0]
    if step > 0:
        for i in range(0, len(all_nums), step):
            chunk = all_nums[i:i + step]
            if len(chunk) == step:
                result.append(chunk)
    return result


def _validate_payout_record(bet_type: str, combo: str, payout: int, pop: Optional[int]) -> bool:
    """
    払戻レコードの妥当性チェック。問題があれば警告ログを出して False を返す。
    """
    # 最小払戻（JRA 最小 ¥100）
    if payout < 100:
        logger.warning("払戻不正: bet_type=%s combo=%s payout=%d (< 100)", bet_type, combo, payout)
        return False
    # 最大払戻（三連単 1000 万超はありえない）
    if payout > 10_000_000:
        logger.warning("払戻不正: bet_type=%s combo=%s payout=%d (> 10M)", bet_type, combo, payout)
        return False

    # 人気バリデーション
    if pop is not None and not (1 <= pop <= 9999):
        logger.warning("人気不正: bet_type=%s combo=%s pop=%d", bet_type, combo, pop)
        return False

    # 組み合わせ馬番バリデーション
    max_num = _BET_MAX_NUM.get(bet_type, 18)
    sep = "→" if bet_type in _ORDERED_BET_TYPES else "-"
    parts = combo.replace("→", "-").split("-")
    sizes = _BET_COMBO_SIZES.get(bet_type, (1, 99))
    if not (sizes[0] <= len(parts) <= sizes[1]):
        logger.warning("組み合わせ馬番数不正: bet_type=%s combo=%s (expected %s nums)", bet_type, combo, sizes)
        return False
    for p in parts:
        if not p.isdigit() or not (1 <= int(p) <= max_num):
            logger.warning("馬番不正: bet_type=%s combo=%s (part=%r, max=%d)", bet_type, combo, p, max_num)
            return False

    return True


def _parse_payout_table_new(soup: BeautifulSoup) -> list[dict]:
    """
    race.netkeiba.com の Payout_Detail_Table を解析する。

    <li> ベース（新形式）と <br> ベース（旧形式）の両方に対応。
    全券種の全行（複勝3行、ワイド最大7行等）を漏れなく取得する。
    不正なレコードはバリデーション後に除外する。
    """
    results: list[dict] = []

    for table in soup.select("table.Payout_Detail_Table"):
        for tr in table.find_all("tr"):
            th = tr.find("th")
            tds = tr.find_all("td")
            if not th or len(tds) < 2:
                continue

            bet_type = _TH_TEXT_TO_BET_TYPE.get(th.get_text(strip=True))
            if bet_type is None:
                continue

            sep = "→" if bet_type in _ORDERED_BET_TYPES else "-"

            # 払戻列: <br> 区切り or <li> でマルチ行を取得
            pay_rows = _td_row_texts(tds[1])
            pays = [_parse_payout_int(t) for t in pay_rows]

            # 人気列: 各 <span> が 1 エントリ（"3人気" "25人気" のように並ぶ）
            if len(tds) > 2:
                pop_spans = tds[2].find_all("span")
                if pop_spans:
                    pops: list[Optional[int]] = [
                        _parse_payout_int(re.sub(r"[^\d,]", "", sp.get_text()))
                        for sp in pop_spans
                    ]
                else:
                    pop_rows = _td_row_texts(tds[2])
                    pops = [_parse_payout_int(re.sub(r"[^\d,]", "", t)) for t in pop_rows]
            else:
                pops = []

            # 組み合わせ列: bet_type に応じた馬番リストを行ごとに取得
            combo_rows = _combo_li_nums(tds[0], bet_type)

            # 組み合わせ数とペイアウト数を合わせる（どちらか少ない方を基準）
            n = min(len(combo_rows), len(pays)) if combo_rows else len(pays)

            if not combo_rows and bet_type in ("単勝", "複勝"):
                # 単勝/複勝は馬番が1列テキストで来ることがある
                raw_rows = _td_row_texts(tds[0])
                for i, row_text in enumerate(raw_rows):
                    nums = [int(x) for x in re.findall(r'\d+', row_text)
                            if 0 < int(x) <= 18]
                    if not nums:
                        continue
                    pay = pays[i] if i < len(pays) else None
                    pop = pops[i] if i < len(pops) else None
                    if pay is None:
                        continue
                    combo = str(nums[0])
                    if _validate_payout_record(bet_type, combo, pay, pop):
                        results.append({"bet_type": bet_type, "combination": combo,
                                        "payout": pay, "popularity": pop})
                continue

            for i in range(n):
                nums = combo_rows[i] if i < len(combo_rows) else []
                pay  = pays[i] if i < len(pays) else None
                pop  = pops[i] if i < len(pops) else None
                if not nums or pay is None:
                    continue
                combo = sep.join(str(x) for x in nums)
                if _validate_payout_record(bet_type, combo, pay, pop):
                    results.append({"bet_type": bet_type, "combination": combo,
                                    "payout": pay, "popularity": pop})

    return results


def _parse_old_payout_tables(soup: BeautifulSoup) -> list[dict]:
    """旧 db.netkeiba.com 形式（pay_table_01/02）の払戻テーブルを解析する。"""
    results: list[dict] = []

    for table in soup.select("table.pay_table_01, table.pay_table_02"):
        for tr in table.select("tr"):
            th = tr.select_one("th")
            if th is None:
                continue
            bet_type = None
            for cls in (th.get("class") or []):
                bet_type = _TH_CLASS_TO_BET_TYPE.get(cls)
                if bet_type:
                    break
            if bet_type is None:
                bet_type = _TH_TEXT_TO_BET_TYPE.get(th.get_text(strip=True))
            if bet_type is None:
                continue

            tds = tr.select("td")
            if len(tds) < 2:
                continue

            sep = "→" if bet_type in _ORDERED_BET_TYPES else "-"
            combo_rows = _td_row_texts(tds[0])
            pay_rows   = _td_row_texts(tds[1])
            pop_rows   = _td_row_texts(tds[2]) if len(tds) > 2 else []

            pays = [_parse_payout_int(t) for t in pay_rows]
            pops = [_parse_payout_int(re.sub(r"人気", "", t)) for t in pop_rows]

            max_num = _BET_MAX_NUM.get(bet_type, 18)
            combo_size = _BET_COMBO_SIZES.get(bet_type, (1, 99))

            for i, raw_combo in enumerate(combo_rows):
                combo = _normalize_combination(BeautifulSoup(raw_combo, "lxml").get_text())
                if not combo:
                    continue
                # 番号バリデーション
                parts = combo.replace("→", "-").split("-")
                parts = [p for p in parts if p.isdigit() and 0 < int(p) <= max_num]
                if not (combo_size[0] <= len(parts) <= combo_size[1]):
                    logger.warning("旧形式 組み合わせ不正: bet_type=%s raw=%r", bet_type, raw_combo)
                    continue
                combo_clean = sep.join(parts)

                pay = pays[i] if i < len(pays) else None
                pop = pops[i] if i < len(pops) else None
                if pay is None:
                    continue
                if _validate_payout_record(bet_type, combo_clean, pay, pop):
                    results.append({"bet_type": bet_type, "combination": combo_clean,
                                    "payout": pay, "popularity": pop})

    return results


def fetch_race_payouts(
    race_id: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
    session: Optional[requests.Session] = None,
) -> list[dict]:
    """
    レースページの払戻テーブルを取得・解析する。

    netkeiba の HTML 構造:
      - table.Payout_Detail_Table: 単勝/複勝（上部目立つセクション）
      - table.pay_table_01 / pay_table_02: 連複/三連単など残りの券種

    両テーブルを取得・統合し、(bet_type, combination) で重複排除する。

    Returns:
        [{"bet_type": "単勝", "combination": "14", "payout": 380, "popularity": 1}, ...]
        複勝/ワイドは複数行あり。払戻テーブルが存在しない場合は空リスト。
    """
    if not race_id or not race_id.isdigit():
        raise ValueError(f"不正なレース ID: {race_id!r}")

    url  = RACE_URL_TEMPLATE.format(race_id=race_id)
    html = _fetch_html(url, session=session, max_retries=max_retries, delay=delay)
    soup = BeautifulSoup(html, "lxml")

    results: list[dict] = []

    # Payout_Detail_Table（新形式・単勝/複勝 等）
    if soup.select("table.Payout_Detail_Table"):
        new_results = _parse_payout_table_new(soup)
        results.extend(new_results)
        logger.debug("払戻取得 (Payout_Detail_Table): race_id=%s, %d 件", race_id, len(new_results))

    # pay_table_01 / pay_table_02（残りの券種 or 旧形式）
    old_results = _parse_old_payout_tables(soup)
    if old_results:
        # (bet_type, combination) 単位で重複排除して追加
        existing = {(r["bet_type"], r["combination"]) for r in results}
        for r in old_results:
            key = (r["bet_type"], r["combination"])
            if key not in existing:
                results.append(r)
                existing.add(key)
        logger.debug("払戻取得 (pay_table): race_id=%s, +%d 件追加", race_id, len(old_results))

    logger.info("払戻取得完了: race_id=%s, 合計 %d 件", race_id, len(results))
    return results
