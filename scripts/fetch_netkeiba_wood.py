"""
netkeiba 調教評価スクレイパー
==============================

【重要な制約事項】
- netkeiba の調教「タイム」データはプレミアム会員専用（月640円〜）
- 過去レースの調教データはレース後に削除されるため、取得不可
- 本スクリプトで取得できるのは「調教評価（A/B/C/D + テキスト）」のみ

【取得対象】
- 未開催レース（今日から7日以内）の調教評価
- source: race.netkeiba.com/race/oikiri.html

【保存先】
- training_evaluations テーブル（初回実行時に自動作成）
  UNIQUE(race_id, horse_id) でスキップ制御

【使い方】
  python scripts/fetch_netkeiba_wood.py              # 今週の未開催レース
  python scripts/fetch_netkeiba_wood.py --days 14    # 2週間以内
  python scripts/fetch_netkeiba_wood.py --dry-run    # DB保存なし
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "umalogi.db"
SLEEP   = 1.5   # リクエスト間隔（秒）

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT,
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
# DDL
# ────────────────────────────────────────────────────────────────────────────

_DDL_EVAL = """
CREATE TABLE IF NOT EXISTS training_evaluations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id      TEXT    NOT NULL,
    horse_id     TEXT    NOT NULL,
    horse_name   TEXT    NOT NULL DEFAULT '',
    horse_number INTEGER NOT NULL DEFAULT 0,
    eval_text    TEXT    NOT NULL DEFAULT '',   -- 例: 動き上々
    eval_grade   TEXT    NOT NULL DEFAULT '',   -- A / B / C / D
    source_date  TEXT    NOT NULL DEFAULT '',   -- 取得日 YYYY-MM-DD
    created_at   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(race_id, horse_id)
)
"""


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_EVAL)
    conn.commit()


# ────────────────────────────────────────────────────────────────────────────
# 対象レース取得
# ────────────────────────────────────────────────────────────────────────────

def _get_target_races(conn: sqlite3.Connection, days: int) -> list[str]:
    """今日から days 日以内の未開催レースの race_id 一覧を返す。"""
    today    = date.today()
    deadline = today + timedelta(days=days)
    today_str    = today.strftime("%Y/%m/%d")
    deadline_str = deadline.strftime("%Y/%m/%d")

    rows = conn.execute(
        """
        SELECT DISTINCT r.race_id
        FROM races r
        WHERE r.date BETWEEN ? AND ?
          AND SUBSTR(r.race_id, 5, 2) BETWEEN '01' AND '10'  -- JRA会場のみ
        ORDER BY r.date, r.race_number
        """,
        (today_str, deadline_str),
    ).fetchall()
    return [row[0] for row in rows]


def _already_fetched(conn: sqlite3.Connection, race_id: str) -> bool:
    """race_id の評価データが1件でも存在すればスキップ。"""
    count = conn.execute(
        "SELECT COUNT(*) FROM training_evaluations WHERE race_id = ?", (race_id,)
    ).fetchone()[0]
    return count > 0


# ────────────────────────────────────────────────────────────────────────────
# Playwright スクレイピング
# ────────────────────────────────────────────────────────────────────────────

def _scrape_oikiri(page, race_id: str) -> list[dict]:
    """
    oikiri ページから各馬の評価を取得する。

    戻り値: [
        {"horse_id": str, "horse_name": str, "horse_number": int,
         "eval_text": str, "eval_grade": str},
        ...
    ]
    """
    url = f"https://race.netkeiba.com/race/oikiri.html?race_id={race_id}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PlaywrightTimeout:
        logger.warning("タイムアウト: %s", race_id)
        return []

    # JS実行待ち（評価データのレンダリング）
    page.wait_for_timeout(4000)

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    # 公開前チェック
    info_box = soup.find("div", class_="Race_Infomation_Box")
    if info_box:
        logger.info("  %s: %s（スキップ）", race_id, info_box.get_text(strip=True))
        return []

    table = soup.find("table", class_="OikiriTable")
    if not table:
        logger.warning("  %s: OikiriTable なし", race_id)
        return []

    results: list[dict] = []
    for row in table.find_all("tr", class_="HorseList"):
        # 馬番
        umaban_td = row.find("td", class_="Umaban")
        horse_number = int(umaban_td.get_text(strip=True)) if umaban_td else 0

        # 馬名 + horse_id（db.netkeiba.com/horse/{id} から抽出）
        horse_info = row.find("td", class_="Horse_Info")
        if not horse_info:
            continue
        horse_link = horse_info.find("a", href=lambda h: h and "/horse/" in h)
        if not horse_link:
            continue
        horse_name = horse_link.get_text(strip=True)
        horse_id   = horse_link["href"].split("/horse/")[-1].rstrip("/")

        # 評価テキスト（例: 動き上々）
        critic_td = row.find("td", class_="Training_Critic")
        eval_text = critic_td.get_text(strip=True) if critic_td else ""

        # 評価グレード（A/B/C/D）
        eval_grade = ""
        for td in row.find_all("td"):
            cls_list = td.get("class", [])
            for cls in cls_list:
                if cls.startswith("Rank_"):
                    eval_grade = td.get_text(strip=True)

        results.append({
            "horse_id":     horse_id,
            "horse_name":   horse_name,
            "horse_number": horse_number,
            "eval_text":    eval_text,
            "eval_grade":   eval_grade,
        })

    return results


# ────────────────────────────────────────────────────────────────────────────
# DB 保存
# ────────────────────────────────────────────────────────────────────────────

def _save(conn: sqlite3.Connection, race_id: str,
          records: list[dict], dry_run: bool) -> int:
    if not records:
        return 0
    today = date.today().isoformat()
    saved = 0
    for rec in records:
        if dry_run:
            logger.info("  [DRY] %s %s %s %s %s",
                        race_id, rec["horse_number"], rec["horse_name"],
                        rec["eval_text"], rec["eval_grade"])
            saved += 1
            continue
        try:
            conn.execute(
                """
                INSERT INTO training_evaluations
                  (race_id, horse_id, horse_name, horse_number,
                   eval_text, eval_grade, source_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_id, horse_id) DO UPDATE SET
                  eval_text  = excluded.eval_text,
                  eval_grade = excluded.eval_grade,
                  source_date = excluded.source_date
                """,
                (race_id, rec["horse_id"], rec["horse_name"],
                 rec["horse_number"], rec["eval_text"],
                 rec["eval_grade"], today),
            )
            saved += 1
        except Exception as exc:
            logger.warning("  INSERT失敗 %s %s: %s", race_id, rec["horse_id"], exc)
    if not dry_run:
        conn.commit()
    return saved


# ────────────────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="netkeiba 調教評価スクレイパー")
    parser.add_argument("--days",    type=int, default=7,
                        help="今日から何日以内の未開催レースを対象にするか（デフォルト: 7）")
    parser.add_argument("--dry-run", action="store_true",
                        help="DB に保存せず結果を表示するだけ")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    _ensure_table(conn)

    race_ids = _get_target_races(conn, args.days)
    if not race_ids:
        logger.info("対象レースなし（今日から %d 日以内）", args.days)
        conn.close()
        return

    logger.info("対象レース数: %d（今日から%d日以内）", len(race_ids), args.days)

    total_saved = 0
    skipped     = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
        )
        page = ctx.new_page()

        for i, race_id in enumerate(race_ids, 1):
            # 既取得はスキップ（--dry-run は常に実行）
            if not args.dry_run and _already_fetched(conn, race_id):
                skipped += 1
                logger.debug("[%d/%d] %s: 取得済みスキップ", i, len(race_ids), race_id)
                continue

            logger.info("[%d/%d] %s を処理中...", i, len(race_ids), race_id)
            records = _scrape_oikiri(page, race_id)
            saved   = _save(conn, race_id, records, args.dry_run)
            total_saved += saved
            logger.info("  → %d件 %s", saved, "（DRY）" if args.dry_run else "保存")

            time.sleep(SLEEP)

        browser.close()

    conn.close()
    logger.info("完了: 保存=%d件 スキップ=%dレース", total_saved, skipped)


if __name__ == "__main__":
    main()
