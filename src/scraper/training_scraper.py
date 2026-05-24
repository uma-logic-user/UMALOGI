"""
netkeiba 調教タイムスクレイパー — AIウマスギ坂路データ補完

JVLink WOOD dataspec に WH (坂路) レコードが含まれない場合、
race.netkeiba.com の調教ページから坂路調教データを取得して
training_hillwork テーブルに保存する。

URL: https://race.netkeiba.com/race/training.html?race_id=<race_id>
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_NETKEIBA_TRAINING_URL = "https://race.netkeiba.com/race/training.html"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# 坂路コース識別キーワード
_HILLWORK_KEYWORDS = {"坂路", "坂"}

# コース種別コード → course_type 文字列
_COURSE_TYPE_MAP = {
    "坂路": "坂路",
    "南W": "南W",
    "北W": "北W",
    "Wコース": "W",
    "Cコース": "C",
    "Dコース": "D",
}


@dataclass
class TrainingRecord:
    horse_id: str           # netkeiba horse_id (血統ID形式)
    horse_name: str
    training_date: str      # YYYY-MM-DD
    course_type: str        # 坂路 / 南W / 北W / W / C / D
    time_4f: float | None
    time_3f: float | None
    time_2f: float | None
    time_1f: float | None
    lap_time: float | None
    gear: str               # 馬なり / 一杯 / 強め 等
    jockey_name: str
    is_hillwork: bool       # True = 坂路


def _safe_float_time(text: str) -> float | None:
    """
    "44.1" や "51.3" の形式のタイムを秒に変換する。
    "分:秒" 形式の場合は合算する。
    """
    text = text.strip()
    if not text or text in ("-", "－", ""):
        return None
    try:
        if ":" in text:
            parts = text.split(":")
            return float(parts[0]) * 60 + float(parts[1])
        return float(text)
    except (ValueError, IndexError):
        return None


def _detect_course_type(text: str) -> tuple[str, bool]:
    """
    コース種別テキストから course_type と is_hillwork を返す。
    """
    text = text.strip()
    for kw in _HILLWORK_KEYWORDS:
        if kw in text:
            return "坂路", True
    for key, val in _COURSE_TYPE_MAP.items():
        if key in text:
            return val, False
    return text[:4] if text else "", False


def fetch_training_page(race_id: str, delay: float = 1.5) -> list[TrainingRecord]:
    """
    netkeiba 調教ページから全馬の調教データを取得して返す。

    Parameters
    ----------
    race_id : str  例: "202604010601"
    delay   : float  リクエスト前の待機秒数（レート制限）

    Returns
    -------
    list[TrainingRecord]
    """
    time.sleep(delay)

    url = _NETKEIBA_TRAINING_URL
    params = {"race_id": race_id}
    headers = {"User-Agent": _USER_AGENT}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("調教ページ取得失敗 race_id=%s: %s", race_id, exc)
        return []

    # エンコーディング検出
    if resp.encoding and resp.encoding.lower() not in ("utf-8", "utf8"):
        try:
            text = resp.content.decode("utf-8")
        except UnicodeDecodeError:
            text = resp.content.decode("euc-jp", errors="replace")
    else:
        text = resp.text

    soup = BeautifulSoup(text, "html.parser")
    return _parse_training_page(soup, race_id)


def _parse_training_page(soup: BeautifulSoup, race_id: str) -> list[TrainingRecord]:
    """
    調教ページ HTML をパースして TrainingRecord リストを返す。

    netkeiba 調教ページの構造（2025年時点）:
      - <div class="TrainingList"> に全馬の調教テーブルが含まれる
      - 各馬は <div class="HorseTraining"> でグループ化
      - <a class="HorseNameLink" href="/horse/HORSEID/"> で horse_id を取得
      - <table class="TrainingTime"> に調教日・コース・タイムが並ぶ
    """
    records: list[TrainingRecord] = []

    # 馬単位でブロックを探す
    horse_blocks = soup.select(".HorseTraining, .training_horse_block, li.HorseList")
    if not horse_blocks:
        # フォールバック: horse リンクを直接探す
        horse_blocks = soup.select("div[class*='horse'], section[class*='horse']")

    if not horse_blocks:
        logger.debug("調教ページ: 馬ブロック未検出 race_id=%s", race_id)
        # テーブル形式のレイアウトも試みる
        records.extend(_parse_training_table_layout(soup, race_id))
        return records

    for block in horse_blocks:
        # horse_id 取得
        horse_link = block.find("a", href=re.compile(r"/horse/\w+"))
        if not horse_link:
            continue
        m = re.search(r"/horse/(\w+)", horse_link.get("href", ""))
        if not m:
            continue
        horse_id = m.group(1)
        horse_name = horse_link.get_text(strip=True)

        # 調教データ行を取得
        training_rows = block.select("tr")
        for tr in training_rows:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue

            # 列解析: 日付 / コース / タイム群 / 脚色 / 乗り役
            date_text   = cells[0].get_text(strip=True)
            course_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            times_text  = [cells[i].get_text(strip=True) for i in range(2, min(7, len(cells)))]
            gear_text   = cells[-2].get_text(strip=True) if len(cells) >= 2 else ""
            jockey_text = cells[-1].get_text(strip=True) if len(cells) >= 1 else ""

            # 日付パース (MM/DD or YYYY/MM/DD)
            training_date = _parse_date(date_text, race_id)
            if not training_date:
                continue

            course_type, is_hillwork = _detect_course_type(course_text)
            t4f = _safe_float_time(times_text[0]) if len(times_text) > 0 else None
            t3f = _safe_float_time(times_text[1]) if len(times_text) > 1 else None
            t2f = _safe_float_time(times_text[2]) if len(times_text) > 2 else None
            t1f = _safe_float_time(times_text[3]) if len(times_text) > 3 else None
            lap = _safe_float_time(times_text[4]) if len(times_text) > 4 else None

            records.append(TrainingRecord(
                horse_id=horse_id,
                horse_name=horse_name,
                training_date=training_date,
                course_type=course_type,
                time_4f=t4f,
                time_3f=t3f,
                time_2f=t2f,
                time_1f=t1f,
                lap_time=lap,
                gear=gear_text,
                jockey_name=jockey_text,
                is_hillwork=is_hillwork,
            ))

    return records


def _parse_training_table_layout(soup: BeautifulSoup, race_id: str) -> list[TrainingRecord]:
    """
    テーブル一体型レイアウト（全馬を1テーブルに表示）のフォールバックパーサー。
    """
    records: list[TrainingRecord] = []
    tables = soup.select("table.TrainingTime, table[class*='training'], table[class*='Training']")

    current_horse_id = ""
    current_horse_name = ""

    for table in tables:
        for tr in table.select("tr"):
            # 馬名行の検出
            horse_link = tr.find("a", href=re.compile(r"/horse/\w+"))
            if horse_link:
                m = re.search(r"/horse/(\w+)", horse_link.get("href", ""))
                if m:
                    current_horse_id = m.group(1)
                    current_horse_name = horse_link.get_text(strip=True)
                continue

            if not current_horse_id:
                continue

            cells = tr.find_all("td")
            if len(cells) < 3:
                continue

            date_text   = cells[0].get_text(strip=True)
            course_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            times_text  = [cells[i].get_text(strip=True) for i in range(2, min(7, len(cells)))]
            gear_text   = cells[-2].get_text(strip=True) if len(cells) >= 2 else ""
            jockey_text = cells[-1].get_text(strip=True) if len(cells) >= 1 else ""

            training_date = _parse_date(date_text, race_id)
            if not training_date:
                continue

            course_type, is_hillwork = _detect_course_type(course_text)
            t4f = _safe_float_time(times_text[0]) if len(times_text) > 0 else None
            t3f = _safe_float_time(times_text[1]) if len(times_text) > 1 else None
            t2f = _safe_float_time(times_text[2]) if len(times_text) > 2 else None
            t1f = _safe_float_time(times_text[3]) if len(times_text) > 3 else None
            lap = _safe_float_time(times_text[4]) if len(times_text) > 4 else None

            records.append(TrainingRecord(
                horse_id=current_horse_id,
                horse_name=current_horse_name,
                training_date=training_date,
                course_type=course_type,
                time_4f=t4f,
                time_3f=t3f,
                time_2f=t2f,
                time_1f=t1f,
                lap_time=lap,
                gear=gear_text,
                jockey_name=jockey_text,
                is_hillwork=is_hillwork,
            ))

    return records


def _parse_date(text: str, race_id: str) -> str:
    """日付テキストを YYYY-MM-DD 形式に変換する。"""
    text = text.strip()
    year = race_id[:4]  # レース年を基準年とする

    # MM/DD 形式
    m = re.match(r"(\d{1,2})[/月](\d{1,2})", text)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        return f"{year}-{month:02d}-{day:02d}"

    # YYYY/MM/DD 形式
    m2 = re.match(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", text)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"

    return ""


def save_training_records(
    conn: sqlite3.Connection, records: list[TrainingRecord]
) -> tuple[int, int]:
    """
    TrainingRecord リストを training_hillwork と training_times に保存する。

    Returns
    -------
    (hillwork_saved, times_saved) : 保存件数のタプル
    """
    hillwork_saved = 0
    times_saved = 0

    for rec in records:
        if rec.is_hillwork:
            try:
                conn.execute(
                    """
                    INSERT INTO training_hillwork
                        (horse_id, horse_name, training_date,
                         time_4f, time_3f, time_2f, time_1f, lap_time,
                         gear, jockey_name, data_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
                    ON CONFLICT(horse_id, training_date) DO UPDATE SET
                        time_4f    = COALESCE(excluded.time_4f,    training_hillwork.time_4f),
                        time_3f    = COALESCE(excluded.time_3f,    training_hillwork.time_3f),
                        time_2f    = COALESCE(excluded.time_2f,    training_hillwork.time_2f),
                        time_1f    = COALESCE(excluded.time_1f,    training_hillwork.time_1f),
                        lap_time   = COALESCE(excluded.lap_time,   training_hillwork.lap_time),
                        gear       = COALESCE(NULLIF(excluded.gear,''), training_hillwork.gear),
                        jockey_name = COALESCE(NULLIF(excluded.jockey_name,''), training_hillwork.jockey_name)
                    """,
                    (rec.horse_id, rec.horse_name, rec.training_date,
                     rec.time_4f, rec.time_3f, rec.time_2f, rec.time_1f, rec.lap_time,
                     rec.gear, rec.jockey_name),
                )
                hillwork_saved += 1
            except Exception as exc:
                logger.debug("training_hillwork 保存失敗: %s", exc)
        else:
            # コース調教 → training_times に保存（重複はスキップ）
            if rec.time_4f is not None:
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO training_times
                            (horse_id, training_date, course_type,
                             time_4f, time_3f, time_2f, time_1f, lap_time, gear)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (rec.horse_id, rec.training_date, rec.course_type,
                         rec.time_4f, rec.time_3f, rec.time_2f, rec.time_1f,
                         rec.lap_time, rec.gear),
                    )
                    times_saved += 1
                except Exception as exc:
                    logger.debug("training_times 保存失敗: %s", exc)

    conn.commit()
    return hillwork_saved, times_saved
