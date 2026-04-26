"""
過去データ一括インポートスクリプト  UMALOGI Phase 2.5

【目的】
  2021〜2025年（5年分）の JRA レースデータを umalogi.db に一括インポートし、
  モデル訓練に使える歴史的データ量を一気に増強する。

【安全方針】
  ※ netkeiba 等への大量スクレイピングは絶対禁止。IP BAN の確実な原因になる。
  ※ すべてのデータ取得は JRA-VAN Data Lab. (JV-Link) の公式 COM インターフェースを使用。
  ※ netkeiba は「1着馬なし」「距離0m」等の JVLink 失敗時のみ 1 件ずつフォールバック。

【取得戦略】
  Step 1: SETUP  マスタ一括初期取得 (DIFN + BLOD)
          → 騎手・調教師・競走馬・繁殖馬マスタを全件取得（DB の土台を構築）
  Step 2: RACE   過去レース結果一括取得 (OPT_SETUP: 全量 or OPT_STORED: ローカルキャッシュ)
          → RA(レース基本) + SE(馬毎結果) + HR(払戻) を対象年月ごとに分割取得
          → 分割する理由: SETUP モードは全量を一度に返すため OOM を回避
  Step 3: WOOD   調教タイム一括取得 (OPT_NORMAL: fromtime ロール方式)
          → TC(ウッド) + HC(坂路) を年ごとに取得

  取得順序のポイント:
    - SETUP/DIFN を先に実行して jockeys/trainers/racehorses テーブルを埋める
    - その後 RACE を取得することで SE レコードの FK 制約違反を最小化

【実行方法】
  # フル実行（5年分・所要時間: 数十分〜数時間）
  py scripts/import_historical.py

  # ストアードモード（TARGET frontier JV キャッシュを優先利用・高速）
  py scripts/import_historical.py --mode stored

  # 取得開始年を指定（中断再開）
  py scripts/import_historical.py --from-year 2023

  # マスタ取得のみ（STEP 1 のみ）
  py scripts/import_historical.py --only-masters

  # レース取得のみ（STEP 2 のみ）
  py scripts/import_historical.py --only-races

  # ドライラン（コマンドを表示するが実行しない）
  py scripts/import_historical.py --dry-run

【JVLink 制約】
  - JV-Link は 32bit COM サーバーのため 32bit Python (py -3.14-32) が必須
  - OPT_SETUP (option=2) は JRA-VAN サーバーからの全量ダウンロードのため時間がかかる
  - OPT_STORED (option=4) は TARGET frontier JV のローカルキャッシュを読み込む（高速）
  - WOOD dataspec は OPT_SETUP 非対応 → OPT_NORMAL (option=1) で年ごとに取得
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────────────────────

PY32 = "py"           # 32bit Python 呼び出しコマンド
PY32_FLAGS = ["-3.14-32"]  # 32bit フラグ

# 取得対象年 (fromtime は各年1月1日)
DEFAULT_FROM_YEAR = 2021
DEFAULT_TO_YEAR   = 2025

# 32bit サブプロセスのタイムアウト
TIMEOUT_MASTERS  = 7200   # マスタ: 2時間
TIMEOUT_RACE     = 10800  # RACE/年: 3時間（OPT_SETUP はサーバー全量取得のため長め）
TIMEOUT_WOOD     = 3600   # WOOD/年: 1時間

MAX_RETRIES  = 3
BACKOFF_BASE = 60  # 秒

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

# ────────────────────────────────────────────────────────────────────────────
# ロガー設定
# ────────────────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    log_path = LOG_DIR / "import_historical.log"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            log_path,
            maxBytes=100 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        ),
    ]
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=handlers)
    return logging.getLogger("import_historical")


logger = _setup_logger()


# ────────────────────────────────────────────────────────────────────────────
# JVLink 呼び出しヘルパー
# ────────────────────────────────────────────────────────────────────────────

def _run_jvlink(
    dataspec: str,
    fromtime: str,
    option: int,
    *,
    timeout: int,
    label: str,
    dry_run: bool = False,
) -> bool:
    """
    py -3.14-32 -m src.scraper.jravan_client を subprocess で呼び出す。

    Args:
        dataspec: "RACE" / "WOOD" / "SETUP" / "DIFN" / "BLOD"
        fromtime: "YYYYMMDD"
        option:   1=NORMAL, 2=SETUP, 3=TODAY, 4=STORED
        timeout:  秒単位タイムアウト
        label:    ログ表示用ラベル
        dry_run:  True の場合コマンドを表示するだけで実行しない

    Returns:
        成功した場合 True
    """
    cmd = [
        PY32, *PY32_FLAGS,
        "-m", "src.scraper.jravan_client",
        "--fromtime", fromtime,
        "--dataspec", dataspec,
        "--option",   str(option),
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

    logger.error("[%s] 最大リトライ (%d 回) 超過", label, MAX_RETRIES)
    return False


# ────────────────────────────────────────────────────────────────────────────
# DB 状態確認
# ────────────────────────────────────────────────────────────────────────────

def _db_counts() -> dict[str, int]:
    """現在の DB のレコード数を返す。"""
    try:
        sys.path.insert(0, str(ROOT))
        from src.database.init_db import init_db
        conn = init_db()
        counts = {}
        for tbl in ["races", "race_results", "race_payouts",
                    "jockeys", "trainers", "racehorses",
                    "training_times", "training_hillwork"]:
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
    logger.info("─── DB 状態 [%s] ───────────────────────────────────────", phase)
    for tbl, n in counts.items():
        logger.info("  %-22s: %8d 件", tbl, n)
    logger.info("─" * 64)


# ────────────────────────────────────────────────────────────────────────────
# フェーズ別取得ロジック
# ────────────────────────────────────────────────────────────────────────────

def step_masters(mode: str, dry_run: bool) -> bool:
    """
    Step 1: マスタ一括初期取得

    SETUP dataspec (option=2) で騎手・調教師・競走馬・繁殖馬マスタを全件取得。
    DIFN/BLOD を個別に取得するより SETUP が確実。
    """
    logger.info("━━━ STEP 1: マスタ一括取得 (SETUP + DIFN + BLOD) ━━━━━━━━━━━━━━━━━━")

    results = []

    # SETUP (全マスタ一括)
    ok = _run_jvlink(
        dataspec="SETUP",
        fromtime="20200101",   # SETUP はマスタ全量のため fromtime は参考値
        option=2,              # OPT_SETUP 固定
        timeout=TIMEOUT_MASTERS,
        label="SETUP 全マスタ一括",
        dry_run=dry_run,
    )
    results.append(ok)

    # DIFN 差分（競走馬/騎手/調教師: 2021年以降の更新分）
    ok = _run_jvlink(
        dataspec="DIFN",
        fromtime="20210101",
        option=1,              # OPT_NORMAL: 差分
        timeout=TIMEOUT_MASTERS,
        label="DIFN マスタ差分 2021-",
        dry_run=dry_run,
    )
    results.append(ok)

    # BLOD 差分（繁殖馬/産駒: 2021年以降）
    ok = _run_jvlink(
        dataspec="BLOD",
        fromtime="20210101",
        option=1,
        timeout=TIMEOUT_MASTERS,
        label="BLOD 血統差分 2021-",
        dry_run=dry_run,
    )
    results.append(ok)

    success = sum(results)
    logger.info("━━━ STEP 1 完了: %d/%d 成功 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", success, len(results))
    return all(results)


def step_races(from_year: int, to_year: int, mode: str, dry_run: bool) -> bool:
    """
    Step 2: 過去レース結果一括取得

    年単位で分割して取得することで OOM・タイムアウトリスクを軽減する。

    mode:
      "setup"  → OPT_SETUP  (JRA-VAN サーバーから強制全量ダウンロード)
      "stored" → OPT_STORED (TARGET frontier JV のローカルキャッシュ優先)
      "normal" → OPT_NORMAL (差分取得: 最速だが古いデータが欠落する場合あり)
    """
    option_map = {"setup": 2, "stored": 4, "normal": 1}
    option = option_map.get(mode, 2)
    option_label = {1: "OPT_NORMAL", 2: "OPT_SETUP", 4: "OPT_STORED"}.get(option, str(option))

    logger.info(
        "━━━ STEP 2: RACE 一括取得 %d〜%d年 [%s] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        from_year, to_year, option_label,
    )
    logger.info(
        "  取得方針: netkeiba スクレイピング 0件 / JVLink (%s) のみ使用",
        option_label,
    )

    all_ok = True
    for year in range(from_year, to_year + 1):
        fromtime = f"{year}0101"
        label = f"RACE {year}年 [{option_label}]"

        ok = _run_jvlink(
            dataspec="RACE",
            fromtime=fromtime,
            option=option,
            timeout=TIMEOUT_RACE,
            label=label,
            dry_run=dry_run,
        )

        if not ok:
            logger.warning(
                "[%s] 取得失敗。フォールバックとして OPT_STORED を試みます...", label
            )
            # フォールバック: STORED で再試行（サーバー障害時のローカルキャッシュ利用）
            if option != 4:
                ok_fb = _run_jvlink(
                    dataspec="RACE",
                    fromtime=fromtime,
                    option=4,   # OPT_STORED
                    timeout=TIMEOUT_RACE,
                    label=f"RACE {year}年 [OPT_STORED fallback]",
                    dry_run=dry_run,
                )
                if ok_fb:
                    logger.info("[%s] OPT_STORED フォールバック成功", label)
                    ok = True

        all_ok = all_ok and ok
        if not dry_run:
            _print_db_status(f"RACE {year}年 取得後")

        # JRA-VAN サーバー負荷軽減のため年をまたぐ際に待機
        if year < to_year and not dry_run:
            logger.info("次の年の取得前に 10 秒待機...")
            time.sleep(10)

    logger.info("━━━ STEP 2 完了: %s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "全成功" if all_ok else "一部失敗あり")
    return all_ok


def step_wood(from_year: int, to_year: int, dry_run: bool) -> bool:
    """
    Step 3: 調教タイム一括取得

    WOOD dataspec は OPT_SETUP 非対応のため OPT_NORMAL (option=1) で
    年ごとにロール取得する。
    """
    logger.info(
        "━━━ STEP 3: WOOD 調教データ取得 %d〜%d年 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        from_year, to_year,
    )

    all_ok = True
    for year in range(from_year, to_year + 1):
        fromtime = f"{year}0101"
        ok = _run_jvlink(
            dataspec="WOOD",
            fromtime=fromtime,
            option=1,   # WOOD は OPT_NORMAL のみ
            timeout=TIMEOUT_WOOD,
            label=f"WOOD {year}年",
            dry_run=dry_run,
        )
        all_ok = all_ok and ok

        if year < to_year and not dry_run:
            time.sleep(5)

    logger.info("━━━ STEP 3 完了: %s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "全成功" if all_ok else "一部失敗あり")
    return all_ok


# ────────────────────────────────────────────────────────────────────────────
# generate_data.py 呼び出し
# ────────────────────────────────────────────────────────────────────────────

def _refresh_dashboard(dry_run: bool) -> None:
    """web/generate_data.py を実行してダッシュボード JSON を更新する。"""
    web_gen = ROOT / "web" / "generate_data.py"
    if not web_gen.exists():
        logger.info("generate_data.py が見つかりません: %s", web_gen)
        return

    cmd = [PY32, str(web_gen)]
    logger.info("ダッシュボード JSON 更新: %s", " ".join(cmd))
    if dry_run:
        logger.info("  [DRY-RUN] スキップ")
        return
    try:
        subprocess.run(cmd, cwd=str(ROOT), timeout=120)
        logger.info("ダッシュボード JSON 更新完了")
    except Exception as e:
        logger.warning("ダッシュボード JSON 更新失敗: %s", e)


# ────────────────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "過去データ一括インポート (2021〜2025年)\n"
            "JVLink (JRA-VAN) の公式APIのみを使用。netkeiba スクレイピング禁止。"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  py scripts/import_historical.py                    # フル実行
  py scripts/import_historical.py --mode stored      # TARGET キャッシュ優先
  py scripts/import_historical.py --from-year 2023   # 2023年以降のみ
  py scripts/import_historical.py --only-masters     # マスタのみ
  py scripts/import_historical.py --only-races       # レースデータのみ
  py scripts/import_historical.py --dry-run          # コマンド確認のみ
""",
    )
    parser.add_argument(
        "--from-year", type=int, default=DEFAULT_FROM_YEAR,
        help=f"取得開始年 (デフォルト: {DEFAULT_FROM_YEAR})",
    )
    parser.add_argument(
        "--to-year", type=int, default=DEFAULT_TO_YEAR,
        help=f"取得終了年 (デフォルト: {DEFAULT_TO_YEAR})",
    )
    parser.add_argument(
        "--mode",
        choices=["setup", "stored", "normal"],
        default="setup",
        help=(
            "RACE 取得モード: "
            "setup=JRA-VAN全量(推奨), "
            "stored=TARGETキャッシュ優先(高速), "
            "normal=差分(最速・欠落あり). "
            "デフォルト: setup"
        ),
    )
    parser.add_argument(
        "--only-masters", action="store_true",
        help="STEP 1 (マスタ取得) のみ実行",
    )
    parser.add_argument(
        "--only-races", action="store_true",
        help="STEP 2 (レース結果取得) のみ実行",
    )
    parser.add_argument(
        "--only-wood", action="store_true",
        help="STEP 3 (調教タイム取得) のみ実行",
    )
    parser.add_argument(
        "--skip-wood", action="store_true",
        help="STEP 3 (調教タイム) をスキップ（速度優先）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="コマンドを表示するだけで実行しない",
    )
    args = parser.parse_args()

    # ── 実行前チェック ───────────────────────────────────────────
    if args.from_year > args.to_year:
        logger.error("--from-year (%d) > --to-year (%d) は不正です", args.from_year, args.to_year)
        sys.exit(1)

    logger.info("=" * 64)
    logger.info("  UMALOGI 過去データ一括インポート")
    logger.info("  対象: %d〜%d年  モード: %s  dry_run: %s",
                args.from_year, args.to_year, args.mode, args.dry_run)
    logger.info("=" * 64)
    logger.info("  ⚠️  netkeiba スクレイピング: 絶対禁止（IP BAN 防止）")
    logger.info("  ✅  JVLink (JRA-VAN 公式 COM API) のみ使用")
    logger.info("=" * 64)

    if not args.dry_run:
        _print_db_status("実行前")

    total_start = time.time()
    results: dict[str, bool] = {}

    only_specified = args.only_masters or args.only_races or args.only_wood

    # ── STEP 1: マスタ ──────────────────────────────────────────
    if not only_specified or args.only_masters:
        ok = step_masters(mode=args.mode, dry_run=args.dry_run)
        results["STEP1_masters"] = ok

    # ── STEP 2: RACE ────────────────────────────────────────────
    if not only_specified or args.only_races:
        ok = step_races(
            from_year=args.from_year,
            to_year=args.to_year,
            mode=args.mode,
            dry_run=args.dry_run,
        )
        results["STEP2_races"] = ok

    # ── STEP 3: WOOD ────────────────────────────────────────────
    if (not only_specified or args.only_wood) and not args.skip_wood:
        ok = step_wood(
            from_year=args.from_year,
            to_year=args.to_year,
            dry_run=args.dry_run,
        )
        results["STEP3_wood"] = ok

    # ── 完了サマリー ─────────────────────────────────────────────
    elapsed_total = time.time() - total_start
    logger.info("=" * 64)
    logger.info("  一括インポート完了 (経過時間: %.0f 秒 / %.1f 分)",
                elapsed_total, elapsed_total / 60)
    logger.info("  ステップ別結果:")
    all_ok = True
    for step, ok in results.items():
        status = "✅ 成功" if ok else "❌ 失敗"
        logger.info("    %-22s: %s", step, status)
        all_ok = all_ok and ok
    logger.info("=" * 64)

    if not args.dry_run:
        _print_db_status("実行後")
        _refresh_dashboard(dry_run=False)

    if all_ok:
        logger.info("次のステップ:")
        logger.info("  フルリトレイン  : py -m src.ops.retrain_trigger")
        logger.info("  EV閾値バックテスト: py scripts/backtest_ev_threshold.py --bet-type 単勝")
    else:
        logger.warning("一部のステップが失敗しました。ログを確認してください。")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
