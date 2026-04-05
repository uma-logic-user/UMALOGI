"""
過去レースデータ一括取得スクリプト

netkeiba のレース一覧ページから指定日・指定年のレース ID を収集し、
fetch_race_results() を呼んで DB に保存する。

使用例:
  # 特定日のレースを全取得
  python -m src.scraper.fetch_historical --date 20251228

  # 年単位で全重賞を取得（時間がかかる）
  python -m src.scraper.fetch_historical --year 2024 --grade gi

  # 過去5年分を取得
  python -m src.scraper.fetch_historical --years 5
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# プロジェクトルートを sys.path に追加（直接実行用）
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ── URL テンプレート ──────────────────────────────────────────────

# 開催日のレース一覧（kaisai_date=YYYYMMDD）
# NOTE: race_list.html は JS で動的ロードするため race_id を取得できない。
#       実データは race_list_sub.html から取得する。
_RACE_LIST_URL = "https://race.netkeiba.com/top/race_list_sub.html"
# DB サイトのレース一覧（kaisai_date=YYYYMMDD）
_DB_RACE_LIST_URL = "https://db.netkeiba.com/?pid=race_list&word=&track%5B%5D=1&track%5B%5D=2&start_year={year}&start_mon={month}&end_year={year}&end_mon={month}&grade%5B%5D={grade}&kyori_min=&kyori_max=&sort=date&list=100"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://race.netkeiba.com/",
}

# 会場コード（netkeiba race_id の YYYY[VV]DD NN 形式）
_VENUE_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

# グレードフィルタ
_GRADE_FILTER = {
    "gi":   ["GⅠ"],
    "gii":  ["GⅡ"],
    "giii": ["GⅢ"],
    "g":    ["GⅠ", "GⅡ", "GⅢ"],
    "all":  [],  # 全クラス
}


# ── 日別レース ID 取得 ────────────────────────────────────────────

def fetch_race_ids_for_date(
    date_str: str,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
) -> list[str]:
    """
    指定日（YYYYMMDD）の全レース ID を取得する。

    netkeiba のレース一覧ページをスクレイプして
    race_id（12桁）のリストを返す。

    Args:
        date_str:    対象日 "YYYYMMDD"
        delay:       リクエスト間隔（秒）
        max_retries: 最大リトライ回数

    Returns:
        list[str] — race_id のリスト（重複なし）
    """
    url = _RACE_LIST_URL
    params = {"kaisai_date": date_str}

    html = _fetch(url, params=params, delay=delay, max_retries=max_retries)
    soup = BeautifulSoup(html, "lxml")

    race_ids: list[str] = []
    seen: set[str] = set()

    # race_id=XXXXXXXXXXXX を含む全リンクから抽出
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        rid = _extract_race_id(href)
        if rid and rid not in seen:
            seen.add(rid)
            race_ids.append(rid)

    logger.info("日別レース一覧 %s: %d 件", date_str, len(race_ids))
    return race_ids


def _extract_race_id(href: str) -> str | None:
    """URL から race_id（12桁の数字列）を抽出する。"""
    import re
    m = re.search(r"race_id=(\d{12})", href)
    if m:
        return m.group(1)
    m = re.search(r"/race/(\d{12})/?", href)
    if m:
        return m.group(1)
    return None


# ── 年月別 GI/重賞 レース ID 取得 ────────────────────────────────

def fetch_race_ids_by_month(
    year: int,
    month: int,
    grades: list[str] | None = None,
    *,
    delay: float = 2.0,
    max_retries: int = 3,
) -> list[str]:
    """
    DB サイトの検索ページから指定年月・グレードのレース ID を取得する。

    Args:
        year:   対象年（例: 2024）
        month:  対象月 1-12
        grades: ["GⅠ", "GⅡ", ...] — None の場合全グレード

    Returns:
        list[str] — race_id のリスト
    """
    grade_param = "1" if (grades and "GⅠ" in grades) else "5"  # 1=GI, 5=重賞

    url = (
        f"https://db.netkeiba.com/?pid=race_list"
        f"&start_year={year}&start_mon={month}"
        f"&end_year={year}&end_mon={month}"
        f"&grade%5B%5D={grade_param}"
        f"&sort=date&list=100"
    )

    html = _fetch(url, delay=delay, max_retries=max_retries)
    soup = BeautifulSoup(html, "lxml")

    race_ids: list[str] = []
    seen: set[str] = set()

    for a in soup.select("a[href*='/race/']"):
        href = a.get("href", "")
        rid = _extract_race_id(href)
        if rid and rid not in seen:
            seen.add(rid)
            race_ids.append(rid)

    logger.info("月別レース一覧 %04d/%02d: %d 件", year, month, len(race_ids))
    return race_ids


# ── バルク取得 ────────────────────────────────────────────────────

def fetch_and_save_range(
    start_date: date,
    end_date: date,
    grade: str = "gi",
    *,
    delay: float = 2.5,
    skip_existing: bool = True,
    db_path: Path | None = None,
) -> dict[str, int]:
    """
    指定期間のレースを一括取得して DB に保存する。

    Args:
        start_date:    取得開始日
        end_date:      取得終了日
        grade:         "gi" | "g" | "all"
        delay:         リクエスト間隔（秒）
        skip_existing: DB に既存のレースをスキップするか
        db_path:       DB ファイルパス

    Returns:
        {"saved": 保存件数, "skipped": スキップ件数, "errors": エラー件数}
    """
    from src.database.init_db import init_db, insert_race
    from src.scraper.netkeiba import fetch_race_results

    conn = init_db(db_path)
    stats = {"saved": 0, "skipped": 0, "errors": 0}

    # 年月ループで重賞一覧を収集
    grade_filter = _GRADE_FILTER.get(grade, [])
    months_seen: set[str] = set()
    all_race_ids: list[str] = []

    cursor = date(start_date.year, start_date.month, 1)
    end_month = date(end_date.year, end_date.month, 1)

    while cursor <= end_month:
        ym = f"{cursor.year:04d}{cursor.month:02d}"
        if ym not in months_seen:
            months_seen.add(ym)
            if grade == "all":
                # 全クラスは日別一覧を使う（月別は件数が多すぎる）
                logger.info("全クラス取得は日別モードを使用してください")
                break
            ids = fetch_race_ids_by_month(
                cursor.year, cursor.month, grade_filter, delay=delay
            )
            # 期間フィルタリング
            for rid in ids:
                try:
                    rid_date = _race_id_to_date(rid)
                    if start_date <= rid_date <= end_date:
                        all_race_ids.append(rid)
                except ValueError:
                    all_race_ids.append(rid)
            time.sleep(delay)
        # 翌月へ
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    logger.info("対象レース総数: %d 件 (%s 〜 %s)",
                len(all_race_ids), start_date, end_date)

    for race_id in all_race_ids:
        if skip_existing:
            exists = conn.execute(
                "SELECT 1 FROM races WHERE race_id=?", (race_id,)
            ).fetchone()
            if exists:
                logger.debug("スキップ (既存): race_id=%s", race_id)
                stats["skipped"] += 1
                continue

        try:
            race = fetch_race_results(race_id, fetch_pedigree=True, delay=delay)
            insert_race(conn, race)
            stats["saved"] += 1
            logger.info(
                "保存完了 [%d/%d]: %s %s",
                stats["saved"], len(all_race_ids), race_id, race.race_name,
            )
            time.sleep(delay)
        except Exception as exc:
            logger.error("取得失敗 race_id=%s: %s", race_id, exc)
            stats["errors"] += 1
            time.sleep(delay * 2)

    conn.close()
    logger.info("バルク取得完了: %s", stats)
    return stats


def fetch_and_save_date(
    date_str: str,
    *,
    delay: float = 2.5,
    skip_existing: bool = True,
    db_path: Path | None = None,
) -> dict[str, int]:
    """
    指定日の全レースを取得して DB に保存する。

    Args:
        date_str:      対象日 "YYYYMMDD"
        delay:         リクエスト間隔（秒）
        skip_existing: DB に既存のレースをスキップするか
        db_path:       DB ファイルパス

    Returns:
        {"saved": 保存件数, "skipped": スキップ件数, "errors": エラー件数}
    """
    from src.database.init_db import init_db, insert_race
    from src.scraper.netkeiba import fetch_race_results

    race_ids = fetch_race_ids_for_date(date_str, delay=delay)
    conn = init_db(db_path)
    stats = {"saved": 0, "skipped": 0, "errors": 0}

    for race_id in race_ids:
        if skip_existing:
            exists = conn.execute(
                "SELECT 1 FROM races WHERE race_id=?", (race_id,)
            ).fetchone()
            if exists:
                logger.debug("スキップ (既存): race_id=%s", race_id)
                stats["skipped"] += 1
                continue

        try:
            race = fetch_race_results(race_id, fetch_pedigree=True, delay=delay)
            insert_race(conn, race)
            stats["saved"] += 1
            logger.info("保存完了: %s %s", race_id, race.race_name)
            time.sleep(delay)
        except Exception as exc:
            logger.error("取得失敗 race_id=%s: %s", race_id, exc)
            stats["errors"] += 1
            time.sleep(delay * 2)

    conn.close()
    return stats


# ── ユーティリティ ────────────────────────────────────────────────

def _race_id_to_date(race_id: str) -> date:
    """race_id（YYYYVVDDNN）から概算日付を返す（VV=会場、DD=開催日数のため近似）。"""
    year = int(race_id[:4])
    # 厳密な日付はレース情報から取得するしかないため、年だけ使う
    return date(year, 1, 1)


def _fetch(
    url: str,
    params: dict | None = None,
    *,
    delay: float = 1.5,
    max_retries: int = 3,
    timeout: int = 20,
) -> str:
    """HTTP GET をリトライ付きで取得する。"""
    time.sleep(delay)
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                url, params=params, headers=_HEADERS, timeout=timeout
            )
            resp.raise_for_status()
            # netkeiba は EUC-JP を返す。apparent_encoding が誤検知する場合があるため
            # Content-Type ヘッダまたは HTML の charset 宣言を優先する。
            ct = resp.headers.get("Content-Type", "")
            if "euc" in ct.lower() or "euc" in (resp.apparent_encoding or "").lower():
                resp.encoding = "euc-jp"
            else:
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logger.warning("リトライ %d/%d (%s) %.1f秒後", attempt + 1, max_retries, exc, wait)
            time.sleep(wait)
    raise RuntimeError("到達不能コード")  # pragma: no cover


# ── CLI ──────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="過去レースデータ一括取得",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 特定日（有馬記念当日）
  python -m src.scraper.fetch_historical --date 20251228

  # 2024年の全GIを取得
  python -m src.scraper.fetch_historical --year 2024 --grade gi

  # 過去5年の全重賞
  python -m src.scraper.fetch_historical --years 5 --grade g

  # 全レース（指定日）
  python -m src.scraper.fetch_historical --date 20251228 --grade all
""",
    )
    parser.add_argument("--date",  metavar="YYYYMMDD", help="特定日のレースを取得")
    parser.add_argument("--year",  type=int, help="特定年のレースを取得")
    parser.add_argument("--years", type=int, default=5, help="過去N年分を取得（デフォルト5）")
    parser.add_argument(
        "--grade",
        choices=["gi", "gii", "giii", "g", "all"],
        default="gi",
        help="取得グレード (デフォルト: gi)",
    )
    parser.add_argument("--delay",  type=float, default=2.5, help="リクエスト間隔秒数")
    parser.add_argument("--no-skip", action="store_true", help="既存レースも再取得")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()

    skip = not args.no_skip

    if args.date:
        # 特定日モード（全クラス対応）
        stats = fetch_and_save_date(args.date, delay=args.delay, skip_existing=skip)

    elif args.year:
        # 特定年モード
        start = date(args.year, 1, 1)
        end   = date(args.year, 12, 31)
        stats = fetch_and_save_range(start, end, grade=args.grade,
                                     delay=args.delay, skip_existing=skip)

    else:
        # 過去N年モード（デフォルト5年）
        today = date.today()
        start = date(today.year - args.years, 1, 1)
        end   = today
        stats = fetch_and_save_range(start, end, grade=args.grade,
                                     delay=args.delay, skip_existing=skip)

    print(f"\n取得完了: 保存={stats['saved']} スキップ={stats['skipped']} エラー={stats['errors']}")


if __name__ == "__main__":
    main()
