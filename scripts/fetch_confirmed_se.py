"""
fetch_confirmed_se.py — 確報 SE/HR レコード一括取得スクリプト（レジューム機能付き）
================================================================================

【目的】
  race_results.win_odds が NULL の期間について、JVLink から cat='1' 確報
  SE（出馬成績）・HR（払戻）レコードを取得してDBをUPSERTする。

【レジューム設計】
  JVLink OPT_NORMAL=1 は「fromtime 以降の未配信データを全件」返す仕様。
  そのため 1回の JVOpen で fromtime 〜 現在の全データが流れてくる。

  レジューム戦略:
    - JVDataLoader 内部で 500件ごとに batch commit → 中断しても保存済み分はDB残存
    - 再実行時は DB内の「最終確報日 (win_odds有り)」+ 1日を fromtime として自動設定
    - 進捗ファイル (logs/fetch_confirmed_progress.json) で最終 fromtime を記録

  クラッシュ後:
    py -3.14-32 scripts/fetch_confirmed_se.py  (引数なし = 自動レジューム)

【実行（32bit Python 必須）】
  py -3.14-32 scripts/fetch_confirmed_se.py
  py -3.14-32 scripts/fetch_confirmed_se.py --from 20240201
  py -3.14-32 scripts/fetch_confirmed_se.py --verify-only   # カバレッジ確認のみ
  py -3.14-32 scripts/fetch_confirmed_se.py --dry-run       # DB保存なし（テスト）
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

import datetime
from src.database.init_db import init_db
from src.scraper.jravan_client import (
    JVDataLoader,
    DATASPEC_RACE,
    OPT_NORMAL,
    OPT_SETUP,
    OPT_STORED,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_PROGRESS_FILE = _ROOT / "logs" / "fetch_confirmed_progress.json"
_DEFAULT_FROM = "20240201"  # 2024-01 は既にほぼ完全取得済み


# ── 進捗ファイル ─────────────────────────────────────────────────────────────


def _load_progress() -> dict:
    if _PROGRESS_FILE.exists():
        try:
            with open(_PROGRESS_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("進捗ファイル読み込み失敗: %s -> 初期化", e)
    return {}


def _save_progress(data: dict) -> None:
    _PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _PROGRESS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(_PROGRESS_FILE)


# ── DB ベースの確報状況確認 ──────────────────────────────────────────────────


def _get_confirmed_coverage(conn: sqlite3.Connection) -> dict[str, dict]:
    """月次の確報データ（win_odds有り）の件数をDBから集計する。"""
    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', r.date) AS ym,
            COUNT(*) AS total,
            SUM(CASE WHEN rr.win_odds IS NOT NULL AND rr.win_odds > 0
                     THEN 1 ELSE 0 END) AS with_odds
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE strftime('%Y', r.date) >= '2024'
        GROUP BY ym
        ORDER BY ym
    """).fetchall()
    coverage: dict[str, dict] = {}
    for ym, total, with_odds in rows:
        pct = with_odds / total * 100 if total > 0 else 0.0
        coverage[ym] = {"total": total, "with_odds": with_odds, "pct": round(pct, 1)}
    return coverage


def _get_last_confirmed_date(conn: sqlite3.Connection) -> str | None:
    """DB から win_odds が存在する最後のレース日を返す ('YYYY-MM-DD')。"""
    row = conn.execute("""
        SELECT MAX(r.date)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE rr.win_odds IS NOT NULL AND rr.win_odds > 0
    """).fetchone()
    return row[0] if row and row[0] else None


def _get_total_coverage(coverage: dict[str, dict]) -> tuple[int, int]:
    """(total_rows, total_odds_rows) を返す。"""
    tr = sum(d["total"] for d in coverage.values())
    to = sum(d["with_odds"] for d in coverage.values())
    return tr, to


def _print_coverage(
    coverage: dict[str, dict], title: str = "確報データ カバレッジ（月次）"
) -> None:
    print(f"\n=== {title} ===")
    print(f"  {'年月':^8} | {'総行数':>6} | {'odds有':>6} | {'カバー率':>7}")
    print("  " + "-" * 40)
    total_rows = total_odds = 0
    for ym, d in sorted(coverage.items()):
        bar = "#" * int(d["pct"] / 10) + "." * (10 - int(d["pct"] / 10))
        mark = "*" if d["pct"] < 5 and d["total"] > 100 else " "
        print(
            f"  {ym} | {d['total']:>6,} | {d['with_odds']:>6,} | {d['pct']:>6.1f}% [{bar}]{mark}"
        )
        total_rows += d["total"]
        total_odds += d["with_odds"]
    overall = total_odds / total_rows * 100 if total_rows > 0 else 0.0
    print("  " + "-" * 40)
    print(f"  {'合計':^8} | {total_rows:>6,} | {total_odds:>6,} | {overall:>6.1f}%")
    print("  (* = 100行以上あるのにodds < 5% — 未取得月)")


def _date_plus_one_day(date_str: str) -> str:
    """'YYYY-MM-DD' + 1日 -> 'YYYYMMDD'。"""
    d = datetime.date.fromisoformat(date_str) + datetime.timedelta(days=1)
    return d.strftime("%Y%m%d")


# ── メイン取得ロジック ────────────────────────────────────────────────────────


def run_fetch(
    sid: str,
    fromtime: str,
    dry_run: bool = False,
    use_stored: bool = False,
    use_setup: bool = False,
) -> dict:
    """
    JVLink から fromtime 以降の確報 RACE データ (SE/HR) を一括取得する。

    OPT_NORMAL=1: fromtime 以降の全未配信データを一括ストリーミング配信する。
    OPT_STORED=4: ローカルキャッシュ済みデータを再配信する（破損データ復元）。
    OPT_SETUP=2:  fromtime 以降を JVLink サーバから全件再ダウンロードする（最終手段）。
    JVDataLoader が 500件ごとに batch commit するため、中断しても保存済み分は残る。

    Returns:
        stats dict (se, payout, total_read, open_code, improved)
    """
    if use_setup:
        option, opt_name = OPT_SETUP, "OPT_SETUP=2"
    elif use_stored:
        option, opt_name = OPT_STORED, "OPT_STORED=4"
    else:
        option, opt_name = OPT_NORMAL, "OPT_NORMAL=1"
    logger.info(
        "JVLink RACE 取得開始: fromtime=%s option=%s dry_run=%s",
        fromtime,
        opt_name,
        dry_run,
    )

    conn = init_db()
    coverage_before = _get_confirmed_coverage(conn)
    tr_before, to_before = _get_total_coverage(coverage_before)
    conn.close()

    _print_coverage(coverage_before, "取得前 カバレッジ")
    print(f"\n  取得開始 fromtime={fromtime} ({opt_name})")

    if dry_run:
        logger.info("[DRY-RUN] 実際の取得はスキップします")
        return {"se": 0, "payout": 0, "total_read": 0, "open_code": 0, "improved": 0}

    loader = JVDataLoader(sid=sid)
    stats = loader.load(
        dataspec=DATASPEC_RACE,
        fromtime=fromtime,
        option=option,
    )

    se_saved = stats.get("se", 0)
    pay_saved = stats.get("payout", 0)
    open_code = stats.get("open_code", -1)
    total_read = stats.get("total_read", 0)

    logger.info(
        "取得完了: SE=%d payout=%d read=%d open_code=%d",
        se_saved,
        pay_saved,
        total_read,
        open_code,
    )

    # ── カバレッジ比較 ────────────────────────────────────────────────────────
    conn = init_db()
    coverage_after = _get_confirmed_coverage(conn)
    last_date = _get_last_confirmed_date(conn)
    conn.close()

    tr_after, to_after = _get_total_coverage(coverage_after)
    improved = to_after - to_before

    _print_coverage(coverage_after, "取得後 カバレッジ")

    # 月別改善
    all_ym = sorted(set(list(coverage_before.keys()) + list(coverage_after.keys())))
    has_improvement = False
    for ym in all_ym:
        bef = coverage_before.get(ym, {}).get("with_odds", 0)
        aft = coverage_after.get(ym, {}).get("with_odds", 0)
        diff = aft - bef
        if diff > 0:
            if not has_improvement:
                print("\n=== 月別 改善サマリー ===")
            print(f"  {ym}: {bef:,} -> {aft:,} (+{diff:,}件 odds更新)")
            has_improvement = True

    if not has_improvement:
        print(
            "\n[INFO] カバレッジ変化なし。"
            "\n  考えられる原因:"
            "\n    A) JVLink キューに該当データがない（既に消費済み）"
            "\n    B) 該当期間のSEレコードが cat='1' で配信されていない"
            "\n    C) open_code < 0 → JVOpen 失敗 (今回 open_code=%d)" % open_code
        )

    print(
        f"\n[完了] fromtime={fromtime} SE={se_saved:,} payout={pay_saved:,} "
        f"odds更新={improved:,} 最終確報日={last_date}"
    )

    return {
        "se": se_saved,
        "payout": pay_saved,
        "total_read": total_read,
        "open_code": open_code,
        "improved": improved,
        "last_confirmed_date": last_date,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="JVLink 確報 SE/HR レコード一括取得（レジューム機能付き）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
実行例（32bit Python 必須）:
  # DB 最終確報日の翌日から自動取得（レジューム含む）
  py -3.14-32 scripts/fetch_confirmed_se.py

  # 期間を明示して取得
  py -3.14-32 scripts/fetch_confirmed_se.py --from 20240201

  # カバレッジ確認のみ（DBを変更しない）
  py -3.14-32 scripts/fetch_confirmed_se.py --verify-only

  # 動作確認（DB保存なし）
  py -3.14-32 scripts/fetch_confirmed_se.py --dry-run --from 20240201

  # OPT_STORED: 破損データ復元（cat='7'で上書きされたwin_oddsを再取得）
  py -3.14-32 scripts/fetch_confirmed_se.py --stored --from 20260401

  # OPT_SETUP: 歴史データ全件再ダウンロード（未取得月のcat='1'確報データ取得）
  py -3.14-32 scripts/fetch_confirmed_se.py --setup --from 20240201
""",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default=None,
        metavar="YYYYMMDD",
        help="取得開始日（デフォルト: DB最終確報日の翌日 or 20240201）",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="カバレッジ確認のみ（取得しない）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB保存なし（動作確認のみ）",
    )
    parser.add_argument(
        "--stored",
        action="store_true",
        help="OPT_STORED=4 を使用（ローカルキャッシュから再配信 — 破損データ復元に使用）",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="OPT_SETUP=2 を使用（JVLink サーバから全件再ダウンロード — 最終手段・時間がかかる）",
    )
    args = parser.parse_args()

    # ── SID 取得 ─────────────────────────────────────────────────────────────
    sid = os.getenv("JRAVAN_SID", "")
    if not sid:
        logger.error("JRAVAN_SID が設定されていません。.env を確認してください。")
        sys.exit(1)

    # ── verify-only ──────────────────────────────────────────────────────────
    if args.verify_only:
        conn = init_db()
        coverage = _get_confirmed_coverage(conn)
        last_date = _get_last_confirmed_date(conn)
        conn.close()
        _print_coverage(coverage)
        print(f"\n最終確報日: {last_date or 'なし'}")
        progress = _load_progress()
        if progress:
            print(f"進捗ファイル: {_PROGRESS_FILE}")
            print(f"  last_fromtime   : {progress.get('last_fromtime', 'なし')}")
            print(f"  last_success_ts : {progress.get('last_success_ts', 'なし')}")
        return

    # ── from_date 決定 ───────────────────────────────────────────────────────
    from_date: str = args.from_date or ""

    if not from_date:
        # DBから最終確報日を取得して翌日を起点に
        conn = init_db()
        last_date = _get_last_confirmed_date(conn)
        conn.close()
        if last_date:
            from_date = _date_plus_one_day(last_date)
            logger.info(
                "DB最終確報日=%s -> fromtime=%s（翌日から取得）", last_date, from_date
            )
        else:
            from_date = _DEFAULT_FROM
            logger.info("DB確報なし -> fromtime=%s から全量取得", from_date)

    # fromtime を "YYYYMMDDhhmmss" 形式に正規化
    from_date = from_date.replace("-", "")
    if len(from_date) == 8:
        fromtime = from_date + "000000"
    else:
        fromtime = from_date

    # 進捗ファイルに現在の fromtime を記録
    progress = _load_progress()
    progress["last_fromtime"] = fromtime
    progress["last_started_ts"] = datetime.datetime.now().isoformat()
    _save_progress(progress)

    try:
        result = run_fetch(
            sid=sid,
            fromtime=fromtime,
            dry_run=args.dry_run,
            use_stored=args.stored,
            use_setup=args.setup,
        )

        # 成功時に進捗ファイルを更新
        progress["last_success_ts"] = datetime.datetime.now().isoformat()
        progress["last_success_fromtime"] = fromtime
        progress["last_result"] = {
            "se": result["se"],
            "payout": result["payout"],
            "improved": result["improved"],
            "last_confirmed_date": result.get("last_confirmed_date"),
        }
        _save_progress(progress)

    except Exception as e:
        logger.error("取得失敗: %s", e)
        progress["last_error"] = {
            "ts": datetime.datetime.now().isoformat(),
            "fromtime": fromtime,
            "error": str(e),
        }
        _save_progress(progress)
        logger.info(
            "再実行コマンド: py -3.14-32 scripts/fetch_confirmed_se.py\n"
            "  (fromtime を DB 最終確報日から自動計算して再開します)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
