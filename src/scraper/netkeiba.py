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
def _fetch_html(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    max_retries: int = 3,
    delay: float = 1.5,
    timeout: int = 10,
) -> str:
    """
    URL を取得して HTML 文字列を返す。

    失敗時はエクスポネンシャルバックオフでリトライする。

    Args:
        session: 再利用する requests.Session（None の場合は都度 requests.get）

    Raises:
        requests.RequestException: max_retries 回失敗した場合
    """
    time.sleep(delay)

    requester = session.get if session is not None else requests.get

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requester(url, headers=DEFAULT_HEADERS, timeout=timeout)
            resp.raise_for_status()
            # netkeiba は EUC-JP 固定（chardet の誤検知を防ぐ）
            resp.encoding = "euc-jp"
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


def _parse_weight_diff(raw: str) -> Optional[int]:
    """馬体重増減を抽出。"480(+2)" → 2、"480(-4)" → -4、"480" → None"""
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

# netkeiba の th クラス → 馬券種の対応
# race.netkeiba.com 払戻テーブルの th テキスト → 内部 bet_type マッピング
_TH_TEXT_TO_BET_TYPE: dict[str, str] = {
    "単勝":  "単勝",
    "複勝":  "複勝",
    "枠連":  "枠連",
    "馬連":  "馬連",
    "ワイド": "ワイド",
    "馬単":  "馬単",
    "3連複": "三連複",
    "3連単": "三連単",
    "三連複": "三連複",
    "三連単": "三連単",
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

# 着順依存型（払戻 combination に → を使う）
_ORDERED_BET_TYPES = {"馬単", "三連単"}


def _normalize_combination(raw: str) -> str:
    """
    netkeiba 払戻テーブルのコンビネーション文字列を正規化する。

    例:
      "  7  -  14  "  → "7-14"
      " 14  →  7  "   → "14→7"
      "14 → 7 → 16"   → "14→7→16"
    """
    # 全角スペース・全角数字を半角化
    raw = raw.translate(str.maketrans(
        "０１２３４５６７８９　", "0123456789 "
    ))
    # 矢印（→ U+2192 と → の全角）と ハイフン まわりのスペースを除去
    raw = re.sub(r"\s*→\s*", "→", raw)
    raw = re.sub(r"\s*-\s*", "-", raw)
    return raw.strip()


def _parse_payout_int(raw: str) -> Optional[int]:
    """"1,380" / "250円" / "1,450円" → 1380 / 250 / 1450"""
    # 数字とカンマのみ抽出（"円" や HTML 残渣を無視）
    numeric = re.sub(r"[^\d,]", "", raw.strip())
    if not numeric:
        return None
    try:
        return int(numeric.replace(",", ""))
    except ValueError:
        return None


def _parse_payout_table_new(soup: BeautifulSoup) -> list[dict]:
    """
    race.netkeiba.com/race/result.html の Payout_Detail_Table を解析する。

    構造:
        table.Payout_Detail_Table > tr
            th: 馬券種テキスト（"単勝" / "3連複" 等）
            td[0]: 馬番 <span> / <li> 要素の数字
            td[1]: 払戻 <span> に <br> 区切りで複数ある場合あり
            td[2]: 人気 <span> が複数ある場合あり

    組み合わせ区切り:
        馬単・三連単 → "→"  / その他 → "-"  / 単勝・複勝 → 単一数字
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

            # --- 馬番を取得 ---
            nums: list[str] = [
                sp.get_text(strip=True)
                for sp in tds[0].find_all("span")
                if sp.get_text(strip=True).isdigit()
            ]
            if not nums:
                nums = [
                    li.get_text(strip=True)
                    for li in tds[0].find_all("li")
                    if li.get_text(strip=True).isdigit()
                ]

            # --- 払戻を取得 (<br> 区切り) ---
            pay_html = tds[1].decode_contents()
            pays: list[Optional[int]] = [
                _parse_payout_int(re.sub(r"<[^>]+>", "", p))
                for p in re.split(r"<br\s*/?>", pay_html)
                if re.search(r"\d", p)
            ]

            # --- 人気を取得 ---
            pops: list[Optional[int]] = [
                _parse_payout_int(re.sub(r"人気|<[^>]+>", "", sp.decode_contents()))
                for sp in (tds[2].find_all("span") if len(tds) > 2 else [])
            ]

            # --- 組み合わせ文字列を構築 ---
            sep = "→" if bet_type in _ORDERED_BET_TYPES else "-"
            n_pays = len(pays)
            n_nums = len(nums)

            if bet_type in ("単勝", "複勝"):
                # 1馬番ずつ独立したレコード
                for i, num in enumerate(nums):
                    pay = pays[i] if i < n_pays else None
                    pop = pops[i] if i < len(pops) else None
                    if pay is None:
                        continue
                    results.append({"bet_type": bet_type, "combination": num,
                                    "payout": pay, "popularity": pop})

            elif bet_type == "ワイド":
                # ワイドは n_pays 組のペア。馬番は 2×n_pays 個
                for i in range(n_pays):
                    a, b = nums[i * 2] if i * 2 < n_nums else "?", \
                           nums[i * 2 + 1] if i * 2 + 1 < n_nums else "?"
                    pay = pays[i]
                    pop = pops[i] if i < len(pops) else None
                    if pay is None:
                        continue
                    combo = f"{a}{sep}{b}"
                    results.append({"bet_type": bet_type, "combination": combo,
                                    "payout": pay, "popularity": pop})

            else:
                # 馬連・枠連・馬単・三連複・三連単
                combo = sep.join(nums)
                pay = pays[0] if pays else None
                pop = pops[0] if pops else None
                if pay is None:
                    continue
                results.append({"bet_type": bet_type, "combination": combo,
                                "payout": pay, "popularity": pop})

    return results


def fetch_race_payouts(
    race_id: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
) -> list[dict]:
    """
    レースページの払戻テーブルを取得・解析する。

    Args:
        race_id: netkeiba レース ID

    Returns:
        [{"bet_type": "単勝", "combination": "14",
          "payout": 380, "popularity": 1}, ...]
        複勝/ワイドは複数行あり。
        払戻テーブルが存在しない（レース前・廃止等）は空リスト。
    """
    if not race_id or not race_id.isdigit():
        raise ValueError(f"不正なレース ID: {race_id!r}")

    url  = RACE_URL_TEMPLATE.format(race_id=race_id)
    html = _fetch_html(url, max_retries=max_retries, delay=delay)
    soup = BeautifulSoup(html, "lxml")

    # race.netkeiba.com: Payout_Detail_Table
    if soup.select("table.Payout_Detail_Table"):
        results = _parse_payout_table_new(soup)
        logger.info("払戻取得 (新形式): race_id=%s, %d 件", race_id, len(results))
        return results

    # フォールバック: 旧 db.netkeiba.com 形式 pay_table_01
    tables = soup.select("table.pay_table_01")
    if not tables:
        logger.debug("払戻テーブルなし: race_id=%s", race_id)
        return []

    results_old: list[dict] = []
    for table in tables:
        for tr in table.select("tr"):
            th = tr.select_one("th")
            if th is None:
                continue
            th_classes = th.get("class") or []
            bet_type = None
            for cls in th_classes:
                bet_type = _TH_CLASS_TO_BET_TYPE.get(cls)
                if bet_type:
                    break
            if bet_type is None:
                continue
            tds = tr.select("td")
            if len(tds) < 2:
                continue
            combo_html = tds[0].decode_contents()
            pay_html   = tds[1].decode_contents()
            pop_html   = tds[2].decode_contents() if len(tds) > 2 else ""
            combos  = [_normalize_combination(s) for s in re.split(r"<br\s*/?>", combo_html) if s.strip()]
            payouts = [_parse_payout_int(s)       for s in re.split(r"<br\s*/?>", pay_html)  if s.strip()]
            pops    = [_parse_payout_int(s)        for s in re.split(r"<br\s*/?>", pop_html)  if s.strip()]
            for i, combo in enumerate(combos):
                combo_clean = _normalize_combination(BeautifulSoup(combo, "lxml").get_text())
                if not combo_clean:
                    continue
                payout_val = payouts[i] if i < len(payouts) else None
                pop_val    = pops[i]    if i < len(pops)    else None
                if payout_val is None:
                    continue
                results_old.append({"bet_type": bet_type, "combination": combo_clean,
                                    "payout": payout_val, "popularity": pop_val})

    logger.info("払戻取得 (旧形式): race_id=%s, %d 件", race_id, len(results_old))
    return results_old
