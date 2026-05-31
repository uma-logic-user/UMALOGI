"""
R&D スクレイパー — netkeiba から過去オッズと WIN5 情報を収集

【隔離ルール】
  - umalogi.db には一切書き込まない
  - 取得データは data/netkeiba_research.db のみに保存
  - リクエスト間に 1〜2 秒 sleep（礼儀正しいクローラー）
  - 取得済みレースはスキップ（レジューム対応）

実行例:
  # 2024-02〜2025-12 のオッズ取得
  python scripts/scrape_research_data.py --mode odds --from 2024-02-01 --to 2025-12-31
  # ドライラン（実際のリクエストなし）
  python scripts/scrape_research_data.py --mode odds --dry-run
  # umalogi.db 内 WIN5 払戻確認
  python scripts/scrape_research_data.py --mode check-win5
  # WIN5 払戻スクレイプ（2026年）
  python scripts/scrape_research_data.py --mode win5 --from 2026-01-01 --to 2026-05-03
  # 進捗確認
  python scripts/scrape_research_data.py --mode status
"""

from __future__ import annotations

import argparse
import logging
import random
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

import requests

from src.database.init_db import init_db
from src.scraper.netkeiba import (
    fetch_race_results,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── 定数 ──────────────────────────────────────────────────────────────────

_RESEARCH_DB_PATH = _ROOT / "data" / "netkeiba_research.db"

# WIN5 結果ページ URL
_WIN5_URL_TEMPLATE = "https://race.netkeiba.com/special/win5.html?date={date}"

# ── DDL ───────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS horse_odds (
    race_id        TEXT    NOT NULL,
    horse_number   INTEGER NOT NULL,
    win_odds       REAL,
    rank           INTEGER,
    scraped_at     TEXT    NOT NULL,
    PRIMARY KEY (race_id, horse_number)
);

CREATE TABLE IF NOT EXISTS scraped_races (
    race_id        TEXT    PRIMARY KEY,
    status         TEXT    NOT NULL,
    scraped_at     TEXT    NOT NULL,
    horse_count    INTEGER,
    error_msg      TEXT
);

CREATE TABLE IF NOT EXISTS win5_races (
    week_id        TEXT    NOT NULL,
    race_order     INTEGER NOT NULL,
    race_id        TEXT    NOT NULL,
    PRIMARY KEY (week_id, race_order)
);

CREATE TABLE IF NOT EXISTS win5_results (
    week_id        TEXT    PRIMARY KEY,
    combination    TEXT,
    payout         INTEGER,
    scraped_at     TEXT    NOT NULL
);
"""


# ── DB 初期化 ──────────────────────────────────────────────────────────────


def init_research_db(path: Path = _RESEARCH_DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.executescript(_DDL)
    conn.commit()
    logger.info("Research DB 初期化: %s", path)
    return conn


# ── ヘルパー ───────────────────────────────────────────────────────────────


def get_race_ids_from_main_db(
    main_conn: sqlite3.Connection,
    min_date: str,
    max_date: str,
) -> list[str]:
    rows = main_conn.execute(
        "SELECT race_id FROM races WHERE date >= ? AND date <= ? ORDER BY date, race_id",
        (min_date, max_date),
    ).fetchall()
    return [r[0] for r in rows]


def get_scraped_ok(research_conn: sqlite3.Connection) -> set[str]:
    rows = research_conn.execute(
        "SELECT race_id FROM scraped_races WHERE status = 'ok'"
    ).fetchall()
    return {r[0] for r in rows}


def save_horse_odds(
    research_conn: sqlite3.Connection,
    race_id: str,
    results: list,
    scraped_at: str,
) -> int:
    count = 0
    for hr in results:
        if hr.horse_number is None:
            continue
        research_conn.execute(
            """
            INSERT OR REPLACE INTO horse_odds
                (race_id, horse_number, win_odds, rank, scraped_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (race_id, hr.horse_number, hr.win_odds, hr.rank, scraped_at),
        )
        count += 1
    research_conn.execute(
        """
        INSERT OR REPLACE INTO scraped_races
            (race_id, status, scraped_at, horse_count)
        VALUES (?, 'ok', ?, ?)
        """,
        (race_id, scraped_at, count),
    )
    research_conn.commit()
    return count


def mark_race_error(
    research_conn: sqlite3.Connection,
    race_id: str,
    error_msg: str,
    scraped_at: str,
) -> None:
    research_conn.execute(
        """
        INSERT OR REPLACE INTO scraped_races
            (race_id, status, scraped_at, error_msg)
        VALUES (?, 'error', ?, ?)
        """,
        (race_id, scraped_at, error_msg),
    )
    research_conn.commit()


# ── WIN5 スクレイプ（R12払戻ページ方式） ────────────────────────────────────


def _get_win5_race_ids_from_db(
    main_conn: sqlite3.Connection,
    date_iso: str,
) -> list[tuple[int, str]]:
    """
    umalogi.db から WIN5 対象会場の R8〜R12 レースIDを取得する。

    会場選択heuristic: 命名レース数が最も多い会場（同数の場合は主要会場優先）。
    Returns: [(race_order, race_id), ...]  （1始まり）
    """
    _VENUE_PRIORITY = {
        "東京": 1,
        "阪神": 2,
        "京都": 3,
        "中山": 4,
        "中京": 5,
        "新潟": 6,
        "小倉": 7,
        "福島": 8,
        "函館": 9,
        "札幌": 10,
    }
    rows = main_conn.execute(
        """
        SELECT venue,
               SUM(CASE WHEN race_name IS NOT NULL AND race_name != '' THEN 1 ELSE 0 END) AS named_cnt
        FROM races
        WHERE date = ? AND race_number BETWEEN 8 AND 12
        GROUP BY venue ORDER BY named_cnt DESC
        """,
        (date_iso,),
    ).fetchall()
    if not rows:
        return []

    max_named = rows[0][1]
    candidates = [r[0] for r in rows if r[1] == max_named]
    candidates.sort(key=lambda v: _VENUE_PRIORITY.get(v, 99))
    venue = candidates[0]

    race_rows = main_conn.execute(
        """
        SELECT race_id FROM races
        WHERE date = ? AND venue = ? AND race_number BETWEEN 8 AND 12
        ORDER BY race_number
        """,
        (date_iso, venue),
    ).fetchall()
    return [(i + 1, r[0]) for i, r in enumerate(race_rows)]


# ── オッズスクレイプ ────────────────────────────────────────────────────────


def scrape_odds(
    min_date: str,
    max_date: str,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
    research_db_path: Path = _RESEARCH_DB_PATH,
    dry_run: bool = False,
    max_errors: int = 30,
) -> None:
    """指定期間のレースオッズをスクレイプして research DB に保存。"""
    main_conn = init_db()
    research_conn = init_research_db(research_db_path)

    race_ids = get_race_ids_from_main_db(main_conn, min_date, max_date)
    already_scraped = get_scraped_ok(research_conn)
    pending = [rid for rid in race_ids if rid not in already_scraped]

    print(
        f"オッズスクレイプ: 期間={min_date}〜{max_date}\n"
        f"  対象: {len(pending)}件 (全{len(race_ids)}件, 取得済み{len(already_scraped)}件)"
    )

    if dry_run:
        print("[DRY RUN] 実際のリクエストは行いません。終了。")
        main_conn.close()
        research_conn.close()
        return

    session = requests.Session()
    error_count = 0
    success_count = 0
    start_time = datetime.now()

    for i, race_id in enumerate(pending, 1):
        now_str = datetime.now().isoformat()

        try:
            delay = random.uniform(delay_min, delay_max)
            race_info = fetch_race_results(
                race_id,
                fetch_pedigree=False,
                delay=delay,
                session=session,
            )
            count = save_horse_odds(research_conn, race_id, race_info.results, now_str)
            success_count += 1

            if i % 100 == 0 or i <= 3:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = success_count / elapsed if elapsed > 0 else 0
                remaining = (len(pending) - i) / rate / 60 if rate > 0 else 0
                print(
                    f"[{i:>5}/{len(pending)}] {race_id}: {count}頭  "
                    f"errors={error_count}  "
                    f"残り≈{remaining:.0f}分"
                )

        except requests.RequestException as exc:
            error_count += 1
            mark_race_error(research_conn, race_id, str(exc)[:200], now_str)
            logger.warning("[%d/%d] %s 失敗: %s", i, len(pending), race_id, exc)
            if error_count >= max_errors:
                print(f"エラー上限({max_errors}回)到達。中断します。")
                break

        except Exception as exc:
            error_count += 1
            mark_race_error(research_conn, race_id, str(exc)[:200], now_str)
            logger.error("[%d/%d] %s 予期せぬエラー: %s", i, len(pending), race_id, exc)

    elapsed_total = (datetime.now() - start_time).total_seconds()
    print(
        f"\n完了: 成功={success_count}, エラー={error_count}, 経過={elapsed_total / 60:.1f}分"
    )
    main_conn.close()
    research_conn.close()


# ── WIN5 スクレイプ ─────────────────────────────────────────────────────────


def scrape_win5(
    min_date: str,
    max_date: str,
    delay_min: float = 1.5,
    delay_max: float = 2.5,
    research_db_path: Path = _RESEARCH_DB_PATH,
    dry_run: bool = False,
) -> None:
    """
    指定期間の日曜日の WIN5 データをスクレイプして research DB に保存。

    R12（最終WIN5レース）の払戻ページから WIN5 払戻を取得する。
    """
    main_conn = init_db()
    research_conn = init_research_db(research_db_path)

    # 対象の日曜日リスト
    sundays: list[str] = [
        r[0]
        for r in main_conn.execute(
            """
            SELECT DISTINCT date FROM races
            WHERE date BETWEEN ? AND ?
              AND strftime('%w', date) = '0'
            ORDER BY date
            """,
            (min_date, max_date),
        ).fetchall()
    ]

    # 既取得済み週
    already_scraped: set[str] = {
        r[0]
        for r in research_conn.execute("SELECT week_id FROM win5_results").fetchall()
    }
    pending = [d for d in sundays if d.replace("-", "") not in already_scraped]

    print(
        f"WIN5 スクレイプ: 期間={min_date}〜{max_date}\n"
        f"  日曜日: {len(pending)}週 (全{len(sundays)}週, 取得済み{len(already_scraped)}週)"
    )

    if dry_run:
        print("[DRY RUN] 終了。")
        main_conn.close()
        research_conn.close()
        return

    from src.scraper.netkeiba import fetch_race_payouts

    for i, date_iso in enumerate(pending, 1):
        week_id = date_iso.replace("-", "")
        now_str = datetime.now().isoformat()

        # DB から WIN5 対象レースリストを取得
        race_list = _get_win5_race_ids_from_db(main_conn, date_iso)
        if len(race_list) < 5:
            logger.warning("%s: WIN5 レース数不足 (%d件)", date_iso, len(race_list))
            continue

        # win5_races テーブルに保存
        for order, race_id in race_list:
            research_conn.execute(
                "INSERT OR REPLACE INTO win5_races (week_id, race_order, race_id) VALUES (?, ?, ?)",
                (week_id, order, race_id),
            )
        research_conn.commit()

        # R12（race_list[-1]）の払戻ページからWIN5払戻を取得
        _, r12_race_id = race_list[-1]
        delay = random.uniform(delay_min, delay_max)
        try:
            payouts = fetch_race_payouts(r12_race_id, delay=delay)
            win5_rows = [p for p in payouts if p.get("bet_type") == "WIN5"]

            if win5_rows:
                payout_val = win5_rows[0]["payout"]
                combo = win5_rows[0]["combination"]
                research_conn.execute(
                    "INSERT OR REPLACE INTO win5_results (week_id, combination, payout, scraped_at) VALUES (?, ?, ?, ?)",
                    (week_id, combo, payout_val, now_str),
                )
                print(
                    f"[{i:>3}/{len(pending)}] {date_iso}: R12={r12_race_id}, WIN5払戻={payout_val:,}円"
                )
            else:
                research_conn.execute(
                    "INSERT OR REPLACE INTO win5_results (week_id, combination, payout, scraped_at) VALUES (?, NULL, NULL, ?)",
                    (week_id, now_str),
                )
                print(
                    f"[{i:>3}/{len(pending)}] {date_iso}: R12={r12_race_id}, WIN5払戻=未取得"
                )

            research_conn.commit()

        except requests.RequestException as exc:
            logger.warning(
                "[%d/%d] WIN5 %s 払戻取得失敗: %s", i, len(pending), date_iso, exc
            )

    print("WIN5 スクレイプ完了。")
    main_conn.close()
    research_conn.close()


# ── WIN5 チェック（main DB） ────────────────────────────────────────────────


def check_win5_in_main_db() -> None:
    """umalogi.db に WIN5 払戻データが存在するか確認する。"""
    conn = init_db()

    bet_types = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT bet_type FROM race_payouts ORDER BY 1"
        ).fetchall()
    ]
    print("race_payouts の bet_type 一覧:", bet_types)

    win5_like = [
        bt
        for bt in bet_types
        if "WIN" in bt.upper() or "WIN5" in bt.upper() or "Ｗ" in bt
    ]
    if win5_like:
        print("WIN5 系データ発見:", win5_like)
        rows = conn.execute(
            "SELECT race_id, combination, payout FROM race_payouts WHERE bet_type IN ({})".format(
                ",".join("?" * len(win5_like))
            ),
            win5_like,
        ).fetchall()
        print(f"  件数: {len(rows)}")
        for r in rows[:5]:
            print(" ", r)
    else:
        print("WIN5 払戻は umalogi.db に存在しません → netkeiba スクレイプが必要。")

    conn.close()


# ── 進捗表示 ───────────────────────────────────────────────────────────────


def show_status(research_db_path: Path = _RESEARCH_DB_PATH) -> None:
    """Research DB の取得進捗を表示する。"""
    if not research_db_path.exists():
        print("Research DB がまだ存在しません。")
        return

    conn = sqlite3.connect(str(research_db_path))

    total, ok, err = conn.execute(
        "SELECT COUNT(*), SUM(status='ok'), SUM(status='error') FROM scraped_races"
    ).fetchone() or (0, 0, 0)
    horse_count = conn.execute("SELECT COUNT(*) FROM horse_odds").fetchone()[0]
    win5_count = conn.execute("SELECT COUNT(*) FROM win5_results").fetchone()[0]

    print("=== Research DB 進捗 ===")
    print(f"  スクレイプ済みレース: {total}件 (OK={ok}, ERROR={err})")
    print(f"  horse_odds レコード: {horse_count:,}行")
    print(f"  WIN5 週: {win5_count}週")

    if total and total > 0:
        # umalogi.db を attach して実際のレース日で集計
        main_db = _ROOT / "data" / "umalogi.db"
        if main_db.exists():
            conn.execute(
                f"ATTACH DATABASE '{str(main_db).replace(chr(92), '/')}' AS mdb"
            )
            rows = conn.execute(
                """
                SELECT strftime('%Y-%m', r.date) as ym, COUNT(*) as cnt
                FROM scraped_races s
                JOIN mdb.races r ON s.race_id = r.race_id
                WHERE s.status = 'ok'
                GROUP BY ym ORDER BY ym
                """
            ).fetchall()
            conn.execute("DETACH DATABASE mdb")
        else:
            rows = []
        print("\nレース日 年月別カバレッジ:")
        for ym, cnt in rows:
            print(f"  {ym}: {cnt}件")

    conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="R&D スクレイパー — netkeiba からオッズ・WIN5情報を収集",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python scripts/scrape_research_data.py --mode status
  python scripts/scrape_research_data.py --mode check-win5
  python scripts/scrape_research_data.py --mode odds --from 2024-02-01 --to 2025-12-31
  python scripts/scrape_research_data.py --mode odds --from 2024-02-01 --to 2024-06-30 --dry-run
  python scripts/scrape_research_data.py --mode win5 --from 2026-01-01 --to 2026-05-03
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["odds", "win5", "check-win5", "status"],
        default="status",
        help="動作モード (デフォルト: status)",
    )
    parser.add_argument(
        "--from", dest="from_date", default="2024-02-01", help="開始日 YYYY-MM-DD"
    )
    parser.add_argument(
        "--to", dest="to_date", default="2025-12-31", help="終了日 YYYY-MM-DD"
    )
    parser.add_argument(
        "--delay-min", type=float, default=1.0, help="最小待機秒数 (デフォルト: 1.0)"
    )
    parser.add_argument(
        "--delay-max", type=float, default=2.0, help="最大待機秒数 (デフォルト: 2.0)"
    )
    parser.add_argument(
        "--max-errors", type=int, default=30, help="エラー上限 (デフォルト: 30)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="ドライラン（実際のリクエストなし）"
    )
    parser.add_argument("--db", default=str(_RESEARCH_DB_PATH), help="Research DB パス")
    args = parser.parse_args()

    research_db_path = Path(args.db)

    match args.mode:
        case "status":
            show_status(research_db_path)

        case "check-win5":
            check_win5_in_main_db()

        case "odds":
            scrape_odds(
                min_date=args.from_date,
                max_date=args.to_date,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
                research_db_path=research_db_path,
                dry_run=args.dry_run,
                max_errors=args.max_errors,
            )

        case "win5":
            scrape_win5(
                min_date=args.from_date,
                max_date=args.to_date,
                delay_min=max(args.delay_min, 2.0),
                delay_max=max(args.delay_max, 3.0),
                research_db_path=research_db_path,
                dry_run=args.dry_run,
            )


if __name__ == "__main__":
    main()
