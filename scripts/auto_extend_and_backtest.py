"""
2022-2023年オッズデータ取得 → ALPHA再学習 → バックテスト 自動パイプライン

使い方:
  # スクレイプ + 自動バックテストを一気に実行（バックグラウンド推奨）
  py scripts/auto_extend_and_backtest.py

  # スクレイプをスキップしてバックテストのみ（データ取得済みの場合）
  py scripts/auto_extend_and_backtest.py --skip-scrape

  # 進捗確認のみ
  py scripts/auto_extend_and_backtest.py --status
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
_RESEARCH_DB = ROOT / "data" / "netkeiba_research.db"
_MAIN_DB     = ROOT / "data" / "umalogi.db"

SCRAPE_FROM  = "2021-01-01"   # 2021年に拡張（full_pipeline.py と同期）
SCRAPE_TO    = "2023-12-31"

POLL_INTERVAL_SEC  = 60   # 進捗チェック間隔
PROGRESS_EVERY_MIN = 5    # 進捗ログ出力間隔（分）


# ── ヘルパー ──────────────────────────────────────────────────────────────

def _research_progress() -> tuple[int, int, int]:
    """(total, ok, error) を返す。DBがなければ (0,0,0)。"""
    if not _RESEARCH_DB.exists():
        return 0, 0, 0
    conn = sqlite3.connect(str(_RESEARCH_DB))
    row = conn.execute(
        "SELECT COUNT(*), SUM(status='ok'), SUM(status='error') FROM scraped_races"
    ).fetchone()
    conn.close()
    return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))


def _target_count(from_date: str, to_date: str) -> int:
    """umalogi.db 内の対象期間レース数。"""
    conn = sqlite3.connect(str(_MAIN_DB))
    cnt = conn.execute(
        "SELECT COUNT(*) FROM races WHERE date BETWEEN ? AND ?",
        (from_date, to_date),
    ).fetchone()[0]
    conn.close()
    return cnt


def _already_scraped_in_range(from_date: str, to_date: str) -> int:
    """research DB に取得済みの対象期間レース数。"""
    if not _RESEARCH_DB.exists():
        return 0
    main_conn = sqlite3.connect(str(_MAIN_DB))
    res_conn  = sqlite3.connect(str(_RESEARCH_DB))
    res_conn.execute(
        f"ATTACH DATABASE '{str(_MAIN_DB).replace(chr(92), '/')}' AS mdb"
    )
    cnt = res_conn.execute(
        """
        SELECT COUNT(*)
        FROM scraped_races s
        JOIN mdb.races r ON s.race_id = r.race_id
        WHERE s.status = 'ok'
          AND r.date BETWEEN ? AND ?
        """,
        (from_date, to_date),
    ).fetchone()[0]
    res_conn.close()
    main_conn.close()
    return int(cnt or 0)


# ── フェーズ1: スクレイプ ────────────────────────────────────────────────

def run_scrape() -> bool:
    """2022-2023年オッズをスクレイプする。完了したら True。"""
    target = _target_count(SCRAPE_FROM, SCRAPE_TO)
    already = _already_scraped_in_range(SCRAPE_FROM, SCRAPE_TO)
    pending = target - already

    if pending <= 0:
        print(f"[SKIP] 2022-2023年は既に全件取得済み ({already}/{target})")
        return True

    print(f"\n{'='*60}")
    print(f"  フェーズ1: オッズスクレイプ")
    print(f"  期間: {SCRAPE_FROM} 〜 {SCRAPE_TO}")
    print(f"  対象: {pending}件 (取得済み{already}/{target})")
    print(f"{'='*60}\n")

    # サブプロセスとして実行（メモリを共有しないため）
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "scrape_research_data.py"),
        "--mode", "odds",
        "--from", SCRAPE_FROM,
        "--to",   SCRAPE_TO,
        "--delay-min", "1.0",
        "--delay-max", "2.0",
        "--max-errors", "50",
    ]
    print(f"  実行: {' '.join(cmd[2:])}\n")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        cwd=str(ROOT),
    )

    start = datetime.now()
    last_log = start
    log_lines: list[str] = []

    while proc.poll() is None:
        line = proc.stdout.readline()  # type: ignore[union-attr]
        if line:
            print(line, end="", flush=True)
            log_lines.append(line)
        else:
            time.sleep(1)

        # 定期ログ
        now = datetime.now()
        if (now - last_log).total_seconds() >= PROGRESS_EVERY_MIN * 60:
            total, ok, err = _research_progress()
            elapsed = (now - start).total_seconds() / 60
            print(
                f"\n[進捗] {now.strftime('%H:%M:%S')}  "
                f"scraped={ok}件 / err={err}件  経過={elapsed:.0f}分\n",
                flush=True,
            )
            last_log = now

    # 残りの出力を flush
    for line in proc.stdout:  # type: ignore[union-attr]
        print(line, end="")

    if proc.returncode != 0:
        print(f"\n[ERROR] スクレイプ終了 (exit={proc.returncode})")
        return False

    total, ok, err = _research_progress()
    elapsed_total = (datetime.now() - start).total_seconds() / 60
    print(
        f"\n[完了] スクレイプ終了  ok={ok}件 / err={err}件  "
        f"経過={elapsed_total:.1f}分"
    )
    return True


# ── フェーズ2: ALPHA 再学習 + バックテスト ───────────────────────────────

def run_alpha_backtest() -> bool:
    """
    ALPHA モデルを 2022-2024 年データで再学習し、2025年テストを実行して保存する。
    alpha_backtest.py の --save オプションでモデルを保存。
    """
    print(f"\n{'='*60}")
    print("  フェーズ2: ALPHA 再学習 + バックテスト (train=2021-2024, test=2025)")
    print(f"{'='*60}\n")

    research_db = ROOT / "data" / "netkeiba_research.db"

    for bet_type in ("単勝", "複勝"):
        print(f"\n  ── {bet_type} ──")
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "alpha_backtest.py"),
            "--train", "2021", "2022", "2023", "2024",
            "--test",  "2025",
            "--bet-type", bet_type,
            "--save",
        ]
        if research_db.exists():
            cmd += ["--research-db", str(research_db)]

        print(f"  実行: {' '.join(cmd[1:])}\n")
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            encoding="utf-8",
            errors="replace",
            capture_output=False,
        )
        if result.returncode != 0:
            print(f"\n[ERROR] ALPHA {bet_type} 失敗 (exit={result.returncode})")
            return False

    print("\n[完了] ALPHA 再学習 + バックテスト完了")
    return True


# ── ステータス表示 ───────────────────────────────────────────────────────

def show_status() -> None:
    target = _target_count(SCRAPE_FROM, SCRAPE_TO)
    already = _already_scraped_in_range(SCRAPE_FROM, SCRAPE_TO)
    total, ok, err = _research_progress()

    print("=== 2021-2023 スクレイプ進捗 ===")
    print(f"  対象期間  : {SCRAPE_FROM} 〜 {SCRAPE_TO}")
    print(f"  umalogi.db: {target}レース")
    print(f"  取得済み  : {already}レース ({already/target*100:.1f}%)")
    print(f"  research DB 全体 ok={ok} / err={err} / total={total}")

    # 月別カバレッジ
    if _RESEARCH_DB.exists():
        res_conn = sqlite3.connect(str(_RESEARCH_DB))
        res_conn.execute(
            f"ATTACH DATABASE '{str(_MAIN_DB).replace(chr(92), '/')}' AS mdb"
        )
        rows = res_conn.execute(
            """
            SELECT strftime('%Y-%m', r.date) as ym, COUNT(*) as cnt
            FROM scraped_races s
            JOIN mdb.races r ON s.race_id = r.race_id
            WHERE s.status = 'ok'
              AND r.date BETWEEN ? AND ?
            GROUP BY ym ORDER BY ym
            """,
            (SCRAPE_FROM, SCRAPE_TO),
        ).fetchall()
        res_conn.close()
        if rows:
            print("\n月別取得数:")
            for ym, cnt in rows:
                print(f"  {ym}: {cnt}件")


# ── main ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="2022-2023スクレイプ→ALPHA再学習→バックテスト 自動パイプライン"
    )
    parser.add_argument("--skip-scrape", action="store_true", help="スクレイプをスキップ")
    parser.add_argument("--skip-train",  action="store_true", help="ALPHA再学習・バックテストをスキップ")
    parser.add_argument("--status",      action="store_true", help="進捗確認のみ")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    start_all = datetime.now()
    print(f"\n{'='*60}")
    print(f"  UMALOGI — 拡張バックテストパイプライン")
    print(f"  開始: {start_all.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # フェーズ1: スクレイプ
    if not args.skip_scrape:
        ok = run_scrape()
        if not ok:
            print("\n[ABORT] スクレイプ失敗のため中断。")
            sys.exit(1)

    # フェーズ2: ALPHA 再学習 + バックテスト
    if not args.skip_train:
        ok = run_alpha_backtest()
        if not ok:
            print("\n[ABORT] ALPHA 学習・バックテスト失敗のため中断。")
            sys.exit(1)

    elapsed = (datetime.now() - start_all).total_seconds() / 60
    print(f"\n{'='*60}")
    print(f"  全パイプライン完了  経過={elapsed:.1f}分")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
