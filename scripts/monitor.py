"""
WOOD取得プロセス監視 → 完了後に自動学習スクリプト
====================================================

使い方:
  python scripts/monitor.py --pid 1562

PID が終了するまで 60 秒ごとに training_times の件数を表示し、
終了後に py scripts/run_train.py を自動実行します。
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT   = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "umalogi.db"
INTERVAL = 60  # 秒


def is_pid_alive(pid: int) -> bool:
    """tasklist で PID が存在するか確認する（Windows）。"""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
            capture_output=True, text=True, timeout=10,
        )
        # tasklist は該当なしでも exit 0 を返す。出力に PID 文字列が含まれるか確認。
        return str(pid) in result.stdout
    except Exception:
        # tasklist が使えない環境ではプロセスが終了したとみなす
        return False


def get_count() -> int:
    """training_times の現在件数を返す。DB 未接続時は -1。"""
    try:
        with sqlite3.connect(str(DB_PATH), timeout=5) as conn:
            row = conn.execute("SELECT COUNT(*) FROM training_times").fetchone()
            return row[0] if row else 0
    except Exception:
        return -1


def run_training() -> int:
    """py scripts/run_train.py を実行し、returncode を返す。"""
    cmd = [sys.executable, str(ROOT / "scripts" / "run_train.py")]
    print(f"\n{'='*60}")
    print("データ取得完了。学習を開始します")
    print(f"コマンド: {' '.join(cmd)}")
    print(f"{'='*60}\n")
    result = subprocess.run(cmd, cwd=str(ROOT))
    return result.returncode


def monitor(pid: int) -> None:
    prev_count = get_count()
    start_time = datetime.now()

    print(f"{'='*60}")
    print(f"監視開始: PID={pid}  開始時刻={start_time.strftime('%H:%M:%S')}")
    print(f"DB: {DB_PATH}")
    print(f"チェック間隔: {INTERVAL} 秒")
    print(f"{'='*60}")
    print(f"  初期件数: training_times = {prev_count:,} 件\n")

    while True:
        time.sleep(INTERVAL)

        now   = datetime.now()
        count = get_count()
        diff  = count - prev_count if prev_count >= 0 else 0

        elapsed = int((now - start_time).total_seconds())
        h, rem  = divmod(elapsed, 3600)
        m, s    = divmod(rem, 60)

        print(
            f"[{now.strftime('%H:%M:%S')}] "
            f"経過 {h:02d}:{m:02d}:{s:02d}  "
            f"training_times = {count:,} 件  "
            f"(前回比 +{diff:,} 件)"
        )

        prev_count = count

        if not is_pid_alive(pid):
            print(f"\n  → PID {pid} の終了を検知しました。")
            break

    rc = run_training()
    if rc == 0:
        print("\n学習が正常に完了しました。")
    else:
        print(f"\n学習が異常終了しました（returncode={rc}）。")
    sys.exit(rc)


def main() -> None:
    parser = argparse.ArgumentParser(description="WOOD取得監視 → 自動学習")
    parser.add_argument("--pid", type=int, required=True, help="監視する PID")
    args = parser.parse_args()
    monitor(args.pid)


if __name__ == "__main__":
    main()
