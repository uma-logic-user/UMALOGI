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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False

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

# ── スレッドローカル Session ──────────────────────────────────────

_thread_local = threading.local()


def _get_session() -> requests.Session:
    """スレッドごとに requests.Session を1つ作成・再利用する。"""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update(_HEADERS)
        _thread_local.session = session
    return _thread_local.session


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
    # grades が空リスト / None → グレード絞り込みなし（全クラス）
    if grades:
        grade_param = "1" if "GⅠ" in grades else "5"
        grade_qs = f"&grade%5B%5D={grade_param}"
    else:
        grade_qs = ""

    url = (
        f"https://db.netkeiba.com/?pid=race_list"
        f"&start_year={year}&start_mon={month}"
        f"&end_year={year}&end_mon={month}"
        f"{grade_qs}"
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


# ── ワーカー関数 ──────────────────────────────────────────────────

def _fetch_and_save_one(
    race_id: str,
    *,
    delay: float,
    skip_existing: bool,
    db_path: "Path | None",
    db_lock: threading.Lock,
) -> str:
    """
    1レースを取得して DB に保存するワーカー関数。

    Returns:
        "saved" | "skipped" | "error"
    """
    from src.database.init_db import init_db, insert_race
    from src.scraper.netkeiba import fetch_race_results

    # スキップチェック（ロックあり）
    if skip_existing:
        with db_lock:
            conn = init_db(db_path)
            exists = conn.execute(
                "SELECT 1 FROM races WHERE race_id=?", (race_id,)
            ).fetchone()
            conn.close()
        if exists:
            logger.debug("スキップ (既存): race_id=%s", race_id)
            return "skipped"

    try:
        session = _get_session()
        race = fetch_race_results(
            race_id,
            fetch_pedigree=True,
            delay=delay,
            session=session,
        )
        with db_lock:
            conn = init_db(db_path)
            insert_race(conn, race)
            conn.close()
        logger.info("保存完了: %s %s", race_id, race.race_name)
        return "saved"
    except Exception as exc:
        logger.error("取得失敗 race_id=%s: %s", race_id, exc)
        time.sleep(delay)  # エラー時は追加待機
        return "error"


# ── バルク取得 ────────────────────────────────────────────────────

def fetch_and_save_range(
    start_date: date,
    end_date: date,
    grade: str = "gi",
    *,
    delay: float = 2.5,
    skip_existing: bool = True,
    db_path: Path | None = None,
    max_workers: int = 4,
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
        max_workers:   並列スレッド数（3〜5 推奨）

    Returns:
        {"saved": 保存件数, "skipped": スキップ件数, "errors": エラー件数}
    """
    stats = {"saved": 0, "skipped": 0, "errors": 0}

    all_race_ids: list[str] = []

    if grade == "all":
        # 全クラス: db.netkeiba.com/race/list/YYYYMMDD/ で土日ごとに取得
        all_race_ids = _collect_race_ids_by_weekends(start_date, end_date, delay=delay)
    else:
        # 重賞系: 月別一覧ページから収集
        grade_filter = _GRADE_FILTER.get(grade, [])
        months_seen: set[str] = set()
        seen_ids: set[str] = set()

        cursor = date(start_date.year, start_date.month, 1)
        end_month = date(end_date.year, end_date.month, 1)

        while cursor <= end_month:
            ym = f"{cursor.year:04d}{cursor.month:02d}"
            if ym not in months_seen:
                months_seen.add(ym)
                ids = fetch_race_ids_by_month(
                    cursor.year, cursor.month, grade_filter, delay=delay
                )
                for rid in ids:
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
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

    db_lock = threading.Lock()
    _run_parallel(
        all_race_ids,
        delay=delay,
        skip_existing=skip_existing,
        db_path=db_path,
        db_lock=db_lock,
        max_workers=max_workers,
        stats=stats,
    )

    logger.info("バルク取得完了: %s", stats)
    return stats


def fetch_and_save_date(
    date_str: str,
    *,
    delay: float = 2.5,
    skip_existing: bool = True,
    db_path: Path | None = None,
    max_workers: int = 4,
) -> dict[str, int]:
    """
    指定日の全レースを取得して DB に保存する。

    Args:
        date_str:      対象日 "YYYYMMDD"
        delay:         リクエスト間隔（秒）
        skip_existing: DB に既存のレースをスキップするか
        db_path:       DB ファイルパス
        max_workers:   並列スレッド数（3〜5 推奨）

    Returns:
        {"saved": 保存件数, "skipped": スキップ件数, "errors": エラー件数}
    """
    race_ids = fetch_race_ids_for_date(date_str, delay=delay)
    stats = {"saved": 0, "skipped": 0, "errors": 0}
    db_lock = threading.Lock()

    _run_parallel(
        race_ids,
        delay=delay,
        skip_existing=skip_existing,
        db_path=db_path,
        db_lock=db_lock,
        max_workers=max_workers,
        stats=stats,
    )

    # 1日分の取得完了後にバックアップを作成
    if stats["saved"] > 0:
        try:
            from utils.backup import make_backup
            make_backup()
        except Exception as exc:
            logger.warning("バックアップ失敗（処理は継続）: %s", exc)

    return stats


# ── 並列実行コア ──────────────────────────────────────────────────

def _run_parallel(
    race_ids: list[str],
    *,
    delay: float,
    skip_existing: bool,
    db_path: Path | None,
    db_lock: threading.Lock,
    max_workers: int,
    stats: dict[str, int],
) -> None:
    """ThreadPoolExecutor で race_ids を並列取得し stats を更新する。"""
    if not race_ids:
        return

    total = len(race_ids)

    if _HAS_TQDM:
        progress = _tqdm(total=total, desc="レース取得", unit="race", dynamic_ncols=True)
    else:
        progress = None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _fetch_and_save_one,
                race_id,
                delay=delay,
                skip_existing=skip_existing,
                db_path=db_path,
                db_lock=db_lock,
            ): race_id
            for race_id in race_ids
        }

        for future in as_completed(futures):
            race_id = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("ワーカー例外 race_id=%s: %s", race_id, exc)
                result = "error"

            stats[result] += 1

            if progress is not None:
                progress.set_postfix(
                    saved=stats["saved"],
                    skip=stats["skipped"],
                    err=stats["errors"],
                    refresh=False,
                )
                progress.update(1)
            else:
                done = stats["saved"] + stats["skipped"] + stats["errors"]
                logger.info(
                    "[%d/%d] %s → %s (保存:%d スキップ:%d エラー:%d)",
                    done, total, race_id, result,
                    stats["saved"], stats["skipped"], stats["errors"],
                )

    if progress is not None:
        progress.close()


# ── ユーティリティ ────────────────────────────────────────────────


def _collect_race_ids_by_weekends(
    start_date: date,
    end_date: date,
    *,
    delay: float = 2.0,
) -> list[str]:
    """
    期間内の土曜・日曜を列挙し、db.netkeiba.com/race/list/YYYYMMDD/ から
    全レース ID を収集して返す。

    race_list_sub.html（当日専用）ではなく DB サイトの日付別一覧を使うため
    過去レースにも対応している。
    """
    seen: set[str] = set()
    all_ids: list[str] = []

    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() in (5, 6):  # 土=5, 日=6
            date_str = cursor.strftime("%Y%m%d")
            url = f"https://db.netkeiba.com/race/list/{date_str}/"
            try:
                html = _fetch(url, delay=delay)
                soup = BeautifulSoup(html, "lxml")
                for a in soup.select("a[href*='/race/']"):
                    rid = _extract_race_id(a.get("href", ""))
                    if rid and rid not in seen:
                        seen.add(rid)
                        all_ids.append(rid)
                logger.info("DB日別一覧 %s: 累計 %d 件", date_str, len(all_ids))
            except Exception as exc:
                logger.warning("日別ID取得失敗 %s: %s", date_str, exc)
        cursor += timedelta(days=1)

    logger.info("土日スキャン完了 (%s 〜 %s): %d 件", start_date, end_date, len(all_ids))
    return all_ids


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
    """HTTP GET をリトライ付きで取得する（Session 再利用）。"""
    time.sleep(delay)
    session = _get_session()
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            # netkeiba は EUC-JP を返す。apparent_encoding が誤検知する場合があるため
            # Content-Type ヘッダまたは HTML の charset 宣言を優先する。
            ct = resp.headers.get("Content-Type", "")
            if "euc" in ct.lower() or "euc" in (resp.apparent_encoding or "").lower():
                resp.encoding = "euc-jp"
            else:
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.HTTPError as exc:
            # 4xx はリトライしても無意味（認証エラー・ブロック等）
            if exc.response is not None and exc.response.status_code < 500:
                raise
            if attempt == max_retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logger.warning("リトライ %d/%d (%s) %.1f秒後", attempt + 1, max_retries, exc, wait)
            time.sleep(wait)
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
    parser.add_argument("--delay",   type=float, default=2.5, help="リクエスト間隔秒数")
    parser.add_argument("--workers", type=int,   default=4,   help="並列スレッド数（デフォルト: 4）")
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
    workers = max(1, min(args.workers, 8))  # 安全のため 1〜8 に制限

    if args.date:
        # 特定日モード（全クラス対応）
        stats = fetch_and_save_date(
            args.date, delay=args.delay, skip_existing=skip, max_workers=workers
        )

    elif args.year:
        # 特定年モード
        start = date(args.year, 1, 1)
        end   = date(args.year, 12, 31)
        stats = fetch_and_save_range(
            start, end, grade=args.grade,
            delay=args.delay, skip_existing=skip, max_workers=workers,
        )

    else:
        # 過去N年モード（デフォルト5年）
        today = date.today()
        start = date(today.year - args.years, 1, 1)
        end   = today
        stats = fetch_and_save_range(
            start, end, grade=args.grade,
            delay=args.delay, skip_existing=skip, max_workers=workers,
        )

    print(f"\n取得完了: 保存={stats['saved']} スキップ={stats['skipped']} エラー={stats['errors']}")


if __name__ == "__main__":
    main()
