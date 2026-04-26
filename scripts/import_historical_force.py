"""
過去データ強制インポートスクリプト  UMALOGI Phase 2.5 (OPT_STORED 版)

【目的】
  TARGET frontier JV でローカルキャッシュのセットアップを完了した後に実行。
  OPT_STORED (option=4) を使ってキャッシュから直接読み込むため、
  サーバー通信なしで高速にインポートできる。

【前提条件】
  TARGET frontier JV 側でフルセットアップ（過去データ蓄積）が完了していること。

【OOM 対策】
  scripts/_jvlink_force_worker.py (32bit) がファイル境界 (JVREAD_FILECHANGE=-1)
  ごとにコミットするため、数十万レコードでもメモリ枯渇しない。
  64bit のこのスクリプトは年ごとにワーカーを subprocess 呼び出しするだけ。

【実行方法】
  # フル実行（2021〜2025年、RACE + WOOD）
  py scripts/import_historical_force.py

  # 年範囲を指定
  py scripts/import_historical_force.py --from-year 2023 --to-year 2025

  # RACE のみ
  py scripts/import_historical_force.py --dataspec RACE

  # ドライラン（コマンドを表示するだけ）
  py scripts/import_historical_force.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────────────────────

PY32       = "py"
PY32_FLAGS = ["-3.14-32"]

DEFAULT_FROM_YEAR = 2021
DEFAULT_TO_YEAR   = 2025

OPT_NORMAL = 1   # JRA-VANサーバーから差分取得（TARGETキャッシュ不要）
OPT_STORED = 4   # TARGET ローカルキャッシュから読む

# タイムアウト (秒)
TIMEOUT_RACE = 10800  # RACE/年: 最大3時間
TIMEOUT_WOOD = 3600   # WOOD/年: 最大1時間

MAX_RETRIES  = 2
BACKOFF_BASE = 30   # 秒

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


# ────────────────────────────────────────────────────────────────────────────
# ロガー
# ────────────────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    log_path = LOG_DIR / "import_historical_force.log"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            log_path, maxBytes=100 * 1024 * 1024, backupCount=3, encoding="utf-8"
        ),
    ]
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=handlers)
    return logging.getLogger("import_force")


logger = _setup_logger()


# ────────────────────────────────────────────────────────────────────────────
# DB 状態確認
# ────────────────────────────────────────────────────────────────────────────

def _db_counts() -> dict[str, int]:
    try:
        sys.path.insert(0, str(ROOT))
        from src.database.init_db import init_db
        conn = init_db()
        counts: dict[str, int] = {}
        for tbl in [
            "races", "race_results", "race_payouts",
            "jockeys", "trainers", "racehorses",
            "training_times", "training_hillwork",
        ]:
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                counts[tbl] = n
            except Exception:
                counts[tbl] = -1
        conn.close()
        return counts
    except Exception as e:
        logger.warning("DB カウント取得失敗: %s", e)
        return {}


def _print_db_status(phase: str) -> None:
    counts = _db_counts()
    logger.info("─── DB 状態 [%s] ─────────────────────────────────────────", phase)
    for tbl, n in counts.items():
        logger.info("  %-24s: %8d 件", tbl, n)
    logger.info("─" * 64)


# ────────────────────────────────────────────────────────────────────────────
# ワーカー呼び出し
# ────────────────────────────────────────────────────────────────────────────

def _run_worker(
    dataspec: str,
    fromtime: str,
    option: int,
    timeout: int,
    label: str,
    batch_size: int = 5000,
    dry_run: bool = False,
) -> bool:
    """32bit ワーカーを subprocess で呼び出す。リトライ付き。"""
    cmd = [
        PY32, *PY32_FLAGS,
        "scripts/_jvlink_force_worker.py",
        "--dataspec", dataspec,
        "--fromtime", fromtime,
        "--option",   str(option),
        "--batch-size", str(batch_size),
    ]

    logger.info("=" * 64)
    logger.info("[%s] 取得開始", label)
    logger.info("  コマンド: %s", " ".join(cmd))
    logger.info("  fromtime=%s  dataspec=%s  option=%d", fromtime, dataspec, option)
    logger.info("=" * 64)

    if dry_run:
        logger.info("  [DRY-RUN] 実行スキップ")
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info("[%s] 試行 %d/%d ...", label, attempt, MAX_RETRIES)
        start = time.time()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(ROOT),
                timeout=timeout,
            )
            elapsed = time.time() - start
            if result.returncode == 0:
                logger.info(
                    "[%s] 完了 (試行 %d, 経過 %.0f 秒)",
                    label, attempt, elapsed,
                )
                return True
            logger.warning(
                "[%s] returncode=%d (試行 %d, 経過 %.0f 秒)",
                label, result.returncode, attempt, elapsed,
            )
        except subprocess.TimeoutExpired:
            logger.error("[%s] タイムアウト (%d 秒)", label, timeout)
        except Exception as exc:
            logger.error("[%s] 例外: %s", label, exc)

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            logger.info("[%s]  → %d 秒後にリトライ...", label, wait)
            time.sleep(wait)

    logger.error("[%s] 最大リトライ (%d 回) 超過 → スキップして次へ", label, MAX_RETRIES)
    return False


# ────────────────────────────────────────────────────────────────────────────
# フェーズ別取得
# ────────────────────────────────────────────────────────────────────────────

def step_race(
    from_year: int,
    to_year: int,
    option: int,
    batch_size: int,
    dry_run: bool,
) -> None:
    """年ごとに RACE データを OPT_STORED で取得する。"""
    logger.info("━━━━ STEP: RACE 取得 (option=%d) %d〜%d年 ━━━━", option, from_year, to_year)
    for year in range(from_year, to_year + 1):
        fromtime = f"{year}0101"
        ok = _run_worker(
            dataspec   = "RACE",
            fromtime   = fromtime,
            option     = option,
            timeout    = TIMEOUT_RACE,
            label      = f"RACE-{year}",
            batch_size = batch_size,
            dry_run    = dry_run,
        )
        if ok:
            _print_db_status(f"RACE-{year} 完了後")
        # 年跨ぎで少し間隔を空ける
        if year < to_year and not dry_run:
            time.sleep(3)


def step_wood(
    from_year: int,
    to_year: int,
    option: int,
    batch_size: int,
    dry_run: bool,
) -> None:
    """年ごとに WOOD データを取得する。"""
    logger.info("━━━━ STEP: WOOD 取得 (option=%d) %d〜%d年 ━━━━", option, from_year, to_year)
    for year in range(from_year, to_year + 1):
        fromtime = f"{year}0101"
        ok = _run_worker(
            dataspec   = "WOOD",
            fromtime   = fromtime,
            option     = option,
            timeout    = TIMEOUT_WOOD,
            label      = f"WOOD-{year}",
            batch_size = batch_size,
            dry_run    = dry_run,
        )
        if ok:
            _print_db_status(f"WOOD-{year} 完了後")
        if year < to_year and not dry_run:
            time.sleep(3)


# ────────────────────────────────────────────────────────────────────────────
# エントリポイント
# ────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    logger.info("=" * 64)
    logger.info("  UMALOGI 過去データ強制インポート (OPT_STORED 版)")
    logger.info("  対象年: %d〜%d", args.from_year, args.to_year)
    logger.info("  dataspec: %s", args.dataspec or "RACE + WOOD")
    logger.info("  option:   %d", args.option)
    logger.info("  dry-run:  %s", args.dry_run)
    logger.info("=" * 64)

    _print_db_status("インポート前")
    start_all = time.time()

    run_race = args.dataspec in (None, "RACE")
    run_wood = args.dataspec in (None, "WOOD")

    if run_race:
        step_race(
            from_year  = args.from_year,
            to_year    = args.to_year,
            option     = args.option,
            batch_size = args.batch_size,
            dry_run    = args.dry_run,
        )

    if run_wood:
        step_wood(
            from_year  = args.from_year,
            to_year    = args.to_year,
            option     = args.option,
            batch_size = args.batch_size,
            dry_run    = args.dry_run,
        )

    elapsed = time.time() - start_all
    logger.info("=" * 64)
    logger.info("  インポート完了 (%.0f 分)", elapsed / 60)
    logger.info("=" * 64)
    _print_db_status("インポート後")

    if not args.dry_run:
        logger.info("")
        logger.info("次のステップ:")
        logger.info("  py -m src.ops.retrain_trigger")
        logger.info("  py scripts/backtest_ev_threshold.py --bet-type 単勝")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="UMALOGI 過去データ強制インポート (OPT_STORED 版)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # フル実行 (2021〜2025年, RACE + WOOD)
  py scripts/import_historical_force.py

  # 年範囲指定
  py scripts/import_historical_force.py --from-year 2023 --to-year 2025

  # RACE のみ
  py scripts/import_historical_force.py --dataspec RACE

  # ドライラン
  py scripts/import_historical_force.py --dry-run
""",
    )
    p.add_argument(
        "--from-year", type=int, default=DEFAULT_FROM_YEAR,
        help=f"取得開始年 (デフォルト: {DEFAULT_FROM_YEAR})",
    )
    p.add_argument(
        "--to-year", type=int, default=DEFAULT_TO_YEAR,
        help=f"取得終了年 (デフォルト: {DEFAULT_TO_YEAR})",
    )
    p.add_argument(
        "--dataspec", choices=["RACE", "WOOD"], default=None,
        help="取得データ種別 (デフォルト: RACE + WOOD 両方)",
    )
    p.add_argument(
        "--option", type=int, choices=[1, 2, 4], default=OPT_NORMAL,
        help="JVOpen オプション: 1=NORMAL(サーバー直取得) 2=SETUP 4=STORED(キャッシュ) (デフォルト: 1=NORMAL)",
    )
    p.add_argument(
        "--batch-size", type=int, default=5000,
        help="ファイル境界間の最大コミット件数 (デフォルト: 5000)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="コマンドを表示するだけで実行しない",
    )
    return p.parse_args()


if __name__ == "__main__":
    main()
