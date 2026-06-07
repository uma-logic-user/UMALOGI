"""
src/monitoring/mojibake_watcher.py — 文字化け自動検知・修復システム

【機能】
  1. DB スキャン: entries.sex_age / entries.horse_name / races.race_name の
     文字化けを自動検知する。
  2. 自動修復: 対象 race_id を netkeiba から再フェッチして UPDATE する。
  3. Discord アラート: 検知・修復の結果を DISCORD_SYSTEM_WEBHOOK_URL へ送信する。
  4. UI チェック: Port 3000 の /api/races エンドポイントをポーリングし、
     API レスポンス内の文字化けを検知する。

【呼び出し方】
  # スタンドアロン実行（今日の全レースをスキャン）
  py -m src.monitoring.mojibake_watcher --date 20260607

  # today_auto_runner から定期呼び出し
  from src.monitoring.mojibake_watcher import run_scan_and_fix
  run_scan_and_fix()  # 本日のレースを対象に自動スキャン・修復

  # UI（API）のレスポンスをスキャン
  from src.monitoring.mojibake_watcher import scan_api
  issues = scan_api(base_url="http://localhost:3000", date="2026-06-07")
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

# ── 文字化けパターン（DB / API 共通） ───────────────────────────────────────
_GARBLED_NAME = re.compile(
    r"\?[A-Za-z\[\]＝]"   # ?X: JVLink CP932 リードバイト脱落
    r"|[｡-ﾟ]"             # 半角カタカナ (U+FF61-U+FF9F)
    r"|['‘’“”†‡]"  # カーリークォート・ダガー
    r"|[�]"           # Unicode 置換文字
    r"|[\x80-\x9f]"       # C1 制御文字
)

_GARBLED_SEX_AGE = re.compile(
    r"^\?|\?\?|\?[0-9]"   # ?? または ?数字: sex フィールドが化けたパターン
    r"|[�]"
    r"|[｡-ﾟ]"
)

# race_name の「有効とみなせない」パターン
_EMPTY_RACE_NAME = re.compile(r"^$|^第\d+レース$|^レース$")


def is_garbled_name(s: str | None) -> bool:
    return bool(s and _GARBLED_NAME.search(s))


def is_garbled_sex_age(s: str | None) -> bool:
    return bool(s and _GARBLED_SEX_AGE.search(s))


def is_empty_race_name(s: str | None) -> bool:
    return not s or bool(_EMPTY_RACE_NAME.match(s.strip()))


# ── スキャン結果 ─────────────────────────────────────────────────────────────

@dataclass
class ScanResult:
    date: str
    scanned_races: int = 0
    garbled_race_names: list[str] = field(default_factory=list)    # race_id
    garbled_horse_names: dict[str, list[int]] = field(default_factory=dict)  # race_id -> [horse_number]
    garbled_sex_ages: dict[str, list[int]] = field(default_factory=dict)
    fixed_races: list[str] = field(default_factory=list)
    fix_errors: list[str] = field(default_factory=list)
    api_issues: list[str] = field(default_factory=list)

    @property
    def total_issues(self) -> int:
        return (len(self.garbled_race_names)
                + sum(len(v) for v in self.garbled_horse_names.values())
                + sum(len(v) for v in self.garbled_sex_ages.values()))

    def to_discord_text(self) -> str:
        if self.total_issues == 0 and not self.api_issues:
            return f"✅ 文字化けスキャン完了 ({self.date}) — 問題なし ({self.scanned_races}レース確認)"

        lines = [f"⚠️ **文字化け検知レポート** `{self.date}`"]
        lines.append(f"スキャン: {self.scanned_races}レース  問題: {self.total_issues}件")

        if self.garbled_race_names:
            lines.append(f"  race_name 文字化け: {', '.join(self.garbled_race_names)}")
        if self.garbled_horse_names:
            for rid, nums in list(self.garbled_horse_names.items())[:3]:
                lines.append(f"  horse_name 文字化け: {rid} 馬番{nums}")
        if self.garbled_sex_ages:
            for rid, nums in list(self.garbled_sex_ages.items())[:3]:
                lines.append(f"  sex_age 文字化け: {rid} 馬番{nums}")
        if self.api_issues:
            lines.append("  **UI API 文字化け検知:**")
            for iss in self.api_issues[:5]:
                lines.append(f"    {iss}")

        if self.fixed_races:
            lines.append(f"✅ 自動修復済み: {len(self.fixed_races)}レース")
        if self.fix_errors:
            lines.append(f"❌ 修復失敗: {len(self.fix_errors)}件")
            for err in self.fix_errors[:3]:
                lines.append(f"  {err}")

        return "\n".join(lines)


# ── DB スキャン ──────────────────────────────────────────────────────────────

def scan_db(conn: sqlite3.Connection, date_str: str) -> ScanResult:
    """指定日のDBデータを文字化けスキャンする。"""
    result = ScanResult(date=date_str)

    races = conn.execute(
        "SELECT race_id, race_name FROM races WHERE date = ? ORDER BY race_id",
        (date_str,),
    ).fetchall()
    result.scanned_races = len(races)

    for race_id, race_name in races:
        if is_empty_race_name(race_name):
            result.garbled_race_names.append(race_id)

    for race_id, _ in races:
        entries = conn.execute(
            "SELECT horse_number, horse_name, sex_age FROM entries WHERE race_id = ?",
            (race_id,),
        ).fetchall()
        bad_names = [e[0] for e in entries if is_garbled_name(e[1])]
        bad_sex   = [e[0] for e in entries if is_garbled_sex_age(e[2])]
        if bad_names:
            result.garbled_horse_names[race_id] = bad_names
        if bad_sex:
            result.garbled_sex_ages[race_id] = bad_sex

    return result


# ── 自動修復 ─────────────────────────────────────────────────────────────────

def fix_races(conn: sqlite3.Connection, race_ids: list[str], result: ScanResult) -> None:
    """対象レースを netkeiba から再フェッチして修復する。"""
    if not race_ids:
        return

    try:
        from src.scraper.entry_table import fetch_entry_table
        from src.pipeline.scraping import save_entries_to_db
    except ImportError as e:
        result.fix_errors.append(f"import error: {e}")
        return

    for race_id in race_ids:
        try:
            tbl = fetch_entry_table(race_id)
            if tbl and tbl.entries:
                save_entries_to_db(conn, tbl)
                result.fixed_races.append(race_id)
                logger.info("[MojibakeWatcher] 修復完了: %s", race_id)
            else:
                result.fix_errors.append(f"{race_id}: データなし")
            time.sleep(0.8)
        except Exception as e:
            result.fix_errors.append(f"{race_id}: {e}")
            logger.warning("[MojibakeWatcher] 修復失敗: %s %s", race_id, e)


# ── UI API スキャン ───────────────────────────────────────────────────────────

def scan_api(
    base_url: str = "http://localhost:3000",
    date: str | None = None,
    timeout: int = 8,
) -> list[str]:
    """Port 3000 の /api/races エンドポイントをポーリングし文字化けを検知する。

    Returns:
        問題が見つかった場合の説明文字列リスト。問題なければ空リスト。
    """
    import urllib.request
    import json

    issues: list[str] = []
    target_date = date or str(_today())
    url = f"{base_url}/api/races?date={target_date}&limit=50"

    try:
        req = urllib.request.urlopen(url, timeout=timeout)
        races: list[dict[str, Any]] = json.loads(req.read().decode("utf-8"))
    except Exception as e:
        issues.append(f"API接続失敗: {e}")
        return issues

    for race in races:
        race_id = race.get("race_id", "?")
        race_name = race.get("race_name", "")
        if is_empty_race_name(race_name):
            issues.append(f"{race_id}: race_name='{race_name}' (要修復)")

        for r in race.get("results", []):
            name = r.get("horse_name", "") or ""
            sex  = r.get("sex_age", "") or ""
            if is_garbled_name(name):
                issues.append(f"{race_id} 馬番{r.get('horse_number')}: horse_name='{name[:15]}' 文字化け")
            if is_garbled_sex_age(sex):
                issues.append(f"{race_id} 馬番{r.get('horse_number')}: sex_age='{sex}' 文字化け")

    return issues


# ── Discord 通知 ──────────────────────────────────────────────────────────────

def _notify_discord(text: str) -> None:
    """DISCORD_SYSTEM_WEBHOOK_URL（または DISCORD_WEBHOOK_SYSTEM）へ送信する。"""
    import requests

    url = (
        os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "").strip()
        or os.environ.get("DISCORD_WEBHOOK_SYSTEM", "").strip()
        or os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    )
    if not url:
        logger.debug("[MojibakeWatcher] Discord Webhook 未設定 — 通知スキップ")
        return
    try:
        requests.post(url, json={"content": text}, timeout=5)
    except Exception as e:
        logger.warning("[MojibakeWatcher] Discord 通知失敗: %s", e)


# ── メインエントリーポイント ──────────────────────────────────────────────────

def _today() -> str:
    return str(date.today())


def run_scan_and_fix(
    db_path: str | None = None,
    target_date: str | None = None,
    *,
    check_api: bool = True,
    api_base: str = "http://localhost:3000",
    auto_fix: bool = True,
    notify: bool = True,
) -> ScanResult:
    """文字化けスキャンと自動修復を実行する。

    Args:
        db_path: DB パス（None = デフォルト）
        target_date: 対象日 YYYY-MM-DD（None = 今日）
        check_api: UI API もスキャンする
        api_base: Next.js のベース URL
        auto_fix: 検知した場合に自動修復する
        notify: 結果を Discord へ通知する

    Returns:
        ScanResult
    """
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)

    db = db_path or str(_ROOT / "data" / "umalogi.db")
    date_str = target_date or _today()
    # YYYY-MM-DD 形式に正規化
    if len(date_str) == 8:
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")

    # DB スキャン
    logger.info("[MojibakeWatcher] DB スキャン開始: date=%s", date_str)
    result = scan_db(conn, date_str)

    # 要修復レースを集計
    fix_targets = sorted(set(
        result.garbled_race_names
        + list(result.garbled_horse_names.keys())
        + list(result.garbled_sex_ages.keys())
    ))

    if fix_targets and auto_fix:
        logger.info("[MojibakeWatcher] 自動修復: %d レース", len(fix_targets))
        fix_races(conn, fix_targets, result)
    elif fix_targets:
        logger.info("[MojibakeWatcher] 要修復 %d レース（auto_fix=False のためスキップ）", len(fix_targets))

    conn.close()

    # API スキャン
    if check_api:
        logger.info("[MojibakeWatcher] API スキャン: %s", api_base)
        result.api_issues = scan_api(base_url=api_base, date=date_str)

    logger.info(
        "[MojibakeWatcher] 完了: 問題=%d 修復=%d API問題=%d",
        result.total_issues, len(result.fixed_races), len(result.api_issues),
    )

    # Discord 通知（問題があるか修復した場合のみ）
    if notify and (result.total_issues > 0 or result.api_issues or result.fixed_races):
        _notify_discord(result.to_discord_text())

    return result


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

    ap = argparse.ArgumentParser(description="文字化けスキャン・自動修復")
    ap.add_argument("--date", default=None, help="YYYYMMDD or YYYY-MM-DD (default: today)")
    ap.add_argument("--db", default=None)
    ap.add_argument("--no-fix", action="store_true", help="修復をスキップ（スキャンのみ）")
    ap.add_argument("--no-api", action="store_true", help="API スキャンをスキップ")
    ap.add_argument("--no-notify", action="store_true", help="Discord 通知をスキップ")
    ap.add_argument("--api-base", default="http://localhost:3000")
    args = ap.parse_args()

    result = run_scan_and_fix(
        db_path=args.db,
        target_date=args.date,
        check_api=not args.no_api,
        api_base=args.api_base,
        auto_fix=not args.no_fix,
        notify=not args.no_notify,
    )

    print()
    print(result.to_discord_text())
    sys.exit(0 if result.total_issues == 0 and not result.api_issues else 1)


if __name__ == "__main__":
    main()
