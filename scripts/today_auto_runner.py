"""
UMALOGI 直前予想 + 結果速報 完全自律監視ループ（週次オートパイロット版）

本日（YYYYMMDD）のレースを DB から取得し、以下の2段階ジョブを自動実行する常駐スクリプト。

  [prerace]  各レース発走の N 分前（デフォルト 20 分前）に prerace パイプラインを実行
  [postrace] 各レース発走の M 分後（デフォルト 15 分後）に結果速報を取得しダッシュボード更新

--continuous モード（推奨・オートパイロット）:
  金曜夜 → 土日監視 → 日曜週次レポート → 次の金曜夜まで自動スリープ
  の完全週次サイクルを人間介入ゼロで実行する最強スケジューラデーモン。

  [金曜 20:00] JVLink RACE/WOOD 同期 → 土日両日分の暫定予想生成
  [土曜 08:30] 土曜監視ループ（prerace/postrace）開始
  [土曜 20:00] JVLink RACE/WOOD 同期 → 日曜暫定予想再生成（最新データ）
  [日曜 08:30] 日曜監視ループ（prerace/postrace）開始
  [日曜 完了後] 週次収支レポートを Discord 送信 → 次週金曜 20:00 まで自動スリープ
  [月〜木]    完全スリープ（レースなし）→ 次週金曜 20:00 に自動復帰

使用方法:
    python scripts/today_auto_runner.py                          # 本日1日のみ
    python scripts/today_auto_runner.py --continuous             # 週次オートパイロット
    python scripts/today_auto_runner.py --date 20260412          # 指定日
    python scripts/today_auto_runner.py --fire-ahead-min 20
    python scripts/today_auto_runner.py --result-after-min 15
    python scripts/today_auto_runner.py --dry-run

発走時刻の推定式:
    R1 = 10:00 JST、以降 30 分間隔
    R1 → 10:00, R2 → 10:30, ..., R11 → 15:00, R12 → 15:30
"""

from __future__ import annotations

import argparse
import datetime
import logging
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)


def _send_discord(text: str) -> None:
    """Discord Webhook にメッセージを送信する。"""
    import os
    try:
        import requests as _req
        url = os.getenv("DISCORD_WEBHOOK_URL", "")
        if url:
            safe_text = text.replace('\x00', '').strip()
            _req.post(url, json={"content": safe_text}, timeout=10)
    except Exception:
        pass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)
        ),
        RotatingFileHandler(
            _ROOT / "data" / "scheduler.log",
            maxBytes=50 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("auto_runner")

# 発走推定: R1 = 10:00 JST、以降 30 分間隔
_R1_HOUR      = 10
_R1_MINUTE    = 0
_INTERVAL_MIN = 30

# 夜間バッチの時刻
_EVENING_FETCH_HOUR   = 20
_EVENING_FETCH_MINUTE = 0

# 翌朝ループ開始時刻
_MORNING_START_HOUR   = 8
_MORNING_START_MINUTE = 30

# 再起動待機時間（秒）
_RESTART_WAIT_SEC = 60

# 曜日定数
_MON, _TUE, _WED, _THU, _FRI, _SAT, _SUN = range(7)


# ─────────────────────────────────────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────────────────────────────────────

def _weekday(date_str: str) -> int:
    """date_str (YYYYMMDD) の曜日を返す（0=月, 4=金, 5=土, 6=日）。"""
    return datetime.datetime.strptime(date_str, "%Y%m%d").weekday()


def _is_racing_day(date_str: str) -> bool:
    """JRA 競馬開催日（基本は土日）か判定する。"""
    return _weekday(date_str) in (_SAT, _SUN)


def _next_friday_evening(from_dt: datetime.datetime) -> datetime.datetime:
    """from_dt 以降で最初の金曜 20:00 を返す。"""
    wd = from_dt.weekday()
    days_ahead = (_FRI - wd) % 7
    # 今日が金曜かつ既に 20:00 を過ぎている → 来週金曜
    if days_ahead == 0 and from_dt.hour >= _EVENING_FETCH_HOUR:
        days_ahead = 7
    target = (from_dt + datetime.timedelta(days=days_ahead)).replace(
        hour=_EVENING_FETCH_HOUR, minute=_EVENING_FETCH_MINUTE, second=0, microsecond=0
    )
    return target


def _estimate_start(race_date_str: str, race_number: int) -> datetime.datetime:
    """レース発走時刻を推定して返す（JST, tzinfo なし）。"""
    base = datetime.datetime.strptime(race_date_str, "%Y-%m-%d").replace(
        hour=_R1_HOUR, minute=_R1_MINUTE
    )
    return base + datetime.timedelta(minutes=(race_number - 1) * _INTERVAL_MIN)


def _wait_until(target: datetime.datetime, dry_run: bool = False) -> None:
    """target 時刻まで 30 秒ポーリングで待機する。dry_run=True の場合は即時返る。"""
    if dry_run:
        logger.info("[DRY-RUN] 待機スキップ (目標 %s)", target.strftime("%H:%M:%S"))
        return
    while True:
        remaining = (target - datetime.datetime.now()).total_seconds()
        if remaining <= 0:
            return
        sleep_secs = min(30.0, remaining)
        logger.info("待機中: あと %.0f 秒 (目標 %s)", remaining, target.strftime("%H:%M:%S"))
        time.sleep(sleep_secs)


# ─────────────────────────────────────────────────────────────────────────────
# DB アクセス
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_today_races(target_date: str) -> list[tuple[str, str, int]]:
    """DB から当日の (race_id, date, race_number) を返す。"""
    from src.database.init_db import init_db

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = init_db()
    rows = conn.execute(
        """
        SELECT race_id,
               COALESCE(date, ?) AS date,
               CAST(SUBSTR(race_id, 11, 2) AS INTEGER) AS race_number
        FROM races
        WHERE date = ?
        ORDER BY race_id
        """,
        (formatted, formatted),
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# サブプロセス実行
# ─────────────────────────────────────────────────────────────────────────────

def _run_prerace(race_id: str, dry_run: bool) -> int:
    """prerace パイプラインを実行して returncode を返す。"""
    cmd = [sys.executable, "-m", "src.main_pipeline", "prerace", race_id]
    if dry_run:
        logger.info("[DRY-RUN] 実行コマンド: %s", " ".join(cmd))
        return 0
    result = subprocess.run(cmd, cwd=str(_ROOT))
    return result.returncode


def _run_fetch_result(race_id: str, dry_run: bool) -> int:
    """レース結果速報取得スクリプトを実行して returncode を返す。"""
    cmd = [sys.executable, str(_ROOT / "scripts" / "fetch_race_result.py"),
           "--race-id", race_id]
    if dry_run:
        logger.info("[DRY-RUN] 実行コマンド: %s", " ".join(cmd))
        return 0
    result = subprocess.run(cmd, cwd=str(_ROOT))
    return result.returncode


def _run_jvlink_sync(dry_run: bool) -> None:
    """JVLink RACE + WOOD の STORED 同期を実行する（32bit 専用プロセス）。"""
    if dry_run:
        logger.info("[DRY-RUN] JVLink 同期をスキップします")
        return
    for dataspec in ("RACE", "WOOD"):
        logger.info("JVLink %s 同期開始...", dataspec)
        subprocess.run(
            ["py", "-3.14-32",
             str(_ROOT / "scripts" / "_jvlink_force_worker.py"),
             "--dataspec", dataspec, "--option", "3"],
            cwd=str(_ROOT),
        )
        logger.info("JVLink %s 同期完了", dataspec)


def _run_provisional(date_str: str, dry_run: bool) -> None:
    """指定日の暫定予想を生成する。"""
    if dry_run:
        logger.info("[DRY-RUN] 暫定予想生成をスキップします (date=%s)", date_str)
        return
    logger.info("暫定予想生成: %s", date_str)
    subprocess.run(
        [sys.executable, "-m", "src.main_pipeline", "provisional",
         "--date", date_str],
        cwd=str(_ROOT),
    )


def _run_generate_web_data(dry_run: bool) -> None:
    """Web ダッシュボード用 JSON を再生成する。"""
    if dry_run:
        logger.info("[DRY-RUN] Web データ生成をスキップします")
        return
    logger.info("Web データ生成中...")
    subprocess.run(
        [sys.executable, str(_ROOT / "web" / "generate_data.py")],
        cwd=str(_ROOT),
    )


# ─────────────────────────────────────────────────────────────────────────────
# バッチ処理
# ─────────────────────────────────────────────────────────────────────────────

def _run_friday_batch(saturday_date: str, dry_run: bool) -> None:
    """
    金曜夜間バッチ:
      1. JVLink RACE/WOOD 同期（土日両日分まとめて）
      2. 土曜の暫定予想生成
      3. 日曜の暫定予想生成
      4. Discord 通知
    """
    sunday_dt   = datetime.datetime.strptime(saturday_date, "%Y%m%d") + datetime.timedelta(days=1)
    sunday_date = sunday_dt.strftime("%Y%m%d")

    logger.info("=" * 60)
    logger.info("金曜夜間バッチ開始: 土曜=%s  日曜=%s", saturday_date, sunday_date)
    logger.info("=" * 60)

    _run_jvlink_sync(dry_run)
    _run_provisional(saturday_date, dry_run)
    _run_provisional(sunday_date, dry_run)

    logger.info("金曜夜間バッチ完了")
    _send_discord(
        f"🗓️ **[UMALOGI] 金曜夜間バッチ完了**\n"
        f"土曜 `{saturday_date}` / 日曜 `{sunday_date}` の暫定予想を生成しました。\n"
        f"明朝 {_MORNING_START_HOUR:02d}:{_MORNING_START_MINUTE:02d} から土曜監視ループ開始予定"
    )


def _run_evening_fetch(next_date: str, dry_run: bool) -> None:
    """
    土曜→日曜 夜間バッチ:
      1. JVLink RACE/WOOD 同期
      2. 翌日（日曜）の暫定予想生成
    """
    logger.info("=" * 60)
    logger.info("夜間バッチ開始: 対象翌日=%s", next_date)
    logger.info("=" * 60)

    _run_jvlink_sync(dry_run)
    _run_provisional(next_date, dry_run)

    logger.info("夜間バッチ完了: 翌日=%s", next_date)
    _send_discord(
        f"🌙 **[UMALOGI] 夜間バッチ完了**\n"
        f"翌日 `{next_date}` の暫定予想を生成しました。\n"
        f"明朝 {_MORNING_START_HOUR:02d}:{_MORNING_START_MINUTE:02d} から直前予想ループ開始予定"
    )


def _send_weekly_report(sunday_date: str, dry_run: bool) -> None:
    """日曜の全レース終了後に週次収支サマリーを Discord に送信する。"""
    if dry_run:
        logger.info("[DRY-RUN] 週次レポート送信をスキップします")
        return

    from src.database.init_db import init_db

    sunday_dt    = datetime.datetime.strptime(sunday_date, "%Y%m%d")
    saturday_dt  = sunday_dt - datetime.timedelta(days=1)
    saturday_str = saturday_dt.strftime("%Y-%m-%d")
    sunday_str   = sunday_dt.strftime("%Y-%m-%d")

    try:
        conn = init_db()
        row = conn.execute(
            """
            SELECT
                COUNT(*)                                          AS total_bets,
                SUM(CASE WHEN pr.is_hit = 1 THEN 1 ELSE 0 END)  AS hits,
                COALESCE(SUM(pr.payout), 0)                      AS total_payout,
                COALESCE(SUM(pr.profit), 0)                      AS total_profit
            FROM predictions p
            LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
            JOIN races r ON r.race_id = p.race_id
            WHERE r.date IN (?, ?)
            """,
            (saturday_str, sunday_str),
        ).fetchone()
        conn.close()

        total, hits, payout, profit = row if row else (0, 0, 0, 0)
        hits   = hits   or 0
        payout = payout or 0
        profit = profit or 0

        hit_rate = f"{hits / total * 100:.1f}%" if total > 0 else "N/A"
        p_emoji  = "🟢" if profit >= 0 else "🔴"
        sign     = "+" if profit >= 0 else ""

        _send_discord(
            f"📊 **[UMALOGI] 週次サマリー ({saturday_str} 〜 {sunday_str})**\n"
            f"予想件数: {total} 件 / 的中: {hits} 件 (的中率 {hit_rate})\n"
            f"払戻合計: ¥{int(payout):,}\n"
            f"損益: {p_emoji} {sign}¥{int(abs(profit)):,}\n"
            f"次週は来週金曜 {_EVENING_FETCH_HOUR:02d}:{_EVENING_FETCH_MINUTE:02d} に自動再起動します 🤖"
        )
        logger.info(
            "週次レポート送信完了: 予想%d件 / 的中%d件 / 損益%+d円",
            total, hits, int(profit),
        )
    except Exception as e:
        logger.error("週次レポート生成に失敗しました: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# 1日分の監視ループ
# ─────────────────────────────────────────────────────────────────────────────

def _run_one_day(
    target_date: str,
    fire_ahead: datetime.timedelta,
    result_after: datetime.timedelta,
    dry_run: bool,
) -> tuple[int, int, int, int]:
    """
    指定日の全レース監視ループを実行する。
    戻り値: (prerace_success, prerace_fail, postrace_success, postrace_fail)
    """
    races = _fetch_today_races(target_date)
    if not races:
        logger.warning(
            "対象日 %s のレースが DB にありません。夜間バッチを先に実行してください。",
            target_date,
        )
        return 0, 0, 0, 0

    logger.info("=" * 60)
    logger.info("UMALOGI 直前予想 + 結果速報 自律監視ループ 起動")
    logger.info(
        "対象日: %s  対象レース: %d 件  発走%d分前/発走%d分後",
        target_date, len(races),
        int(fire_ahead.total_seconds() // 60),
        int(result_after.total_seconds() // 60),
    )
    logger.info("=" * 60)

    date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    schedule: list[tuple[datetime.datetime, str, int, str]] = []
    for race_id, race_date, race_number in races:
        start = _estimate_start(race_date or date_str, race_number)
        schedule.append((start - fire_ahead,  race_id, race_number, "prerace"))
        schedule.append((start + result_after, race_id, race_number, "postrace"))
        logger.info(
            "  R%02d  %s  発走推定 %s  prerace→%s  postrace→%s",
            race_number, race_id,
            start.strftime("%H:%M"),
            (start - fire_ahead).strftime("%H:%M"),
            (start + result_after).strftime("%H:%M"),
        )

    logger.info("-" * 60)

    if dry_run:
        logger.info("[DRY-RUN] スケジュール表示のみで終了します。")
        return 0, 0, 0, 0

    schedule.sort(key=lambda x: (x[0], x[1], x[3]))

    done:    set[tuple[str, str]] = set()
    skipped: set[tuple[str, str]] = set()
    now = datetime.datetime.now()

    # 発走時刻を過ぎた prerace はスキップ
    for fire_at, race_id, race_number, job_type in schedule:
        if job_type == "prerace":
            start = fire_at + fire_ahead
            if now >= start:
                logger.warning(
                    "R%02d %s [prerace] 発走推定時刻 %s を過ぎています -> スキップ",
                    race_number, race_id, start.strftime("%H:%M"),
                )
                skipped.add((race_id, "prerace"))

    pending_jobs = [s for s in schedule if (s[1], s[3]) not in skipped]
    total = len(pending_jobs)

    logger.info("スケジュール済みジョブ: %d 件 (スキップ: %d 件)", total, len(skipped))
    logger.info("監視ループ開始 - Ctrl+C で中断")
    logger.info("-" * 60)

    prerace_success = prerace_fail = 0
    postrace_success = postrace_fail = 0

    try:
        while len(done) < total:
            now = datetime.datetime.now()

            for fire_at, race_id, race_number, job_type in pending_jobs:
                key = (race_id, job_type)
                if key in done:
                    continue
                if now < fire_at:
                    continue

                if job_type == "prerace":
                    start = fire_at + fire_ahead
                    # 5 分超過は見送り
                    if now >= start + datetime.timedelta(minutes=5):
                        logger.warning(
                            "R%02d %s [prerace] 発走 5 分超過のため見送り",
                            race_number, race_id,
                        )
                        done.add(key)
                        skipped.add(key)
                        continue

                    logger.info(
                        "[START] R%02d %s  [prerace] 直前予想開始 (推定発走 %s)",
                        race_number, race_id, start.strftime("%H:%M"),
                    )
                    rc = _run_prerace(race_id, dry_run)
                    done.add(key)
                    if rc == 0:
                        prerace_success += 1
                        logger.info("[OK] R%02d %s  [prerace] 完了", race_number, race_id)
                    else:
                        prerace_fail += 1
                        logger.error(
                            "[NG] R%02d %s  [prerace] 失敗 (rc=%d)", race_number, race_id, rc
                        )

                else:  # postrace
                    logger.info(
                        "[START] R%02d %s  [postrace] 結果取得開始", race_number, race_id
                    )
                    rc = _run_fetch_result(race_id, dry_run)
                    done.add(key)
                    if rc == 0:
                        postrace_success += 1
                        logger.info("[OK] R%02d %s  [postrace] 完了", race_number, race_id)
                    else:
                        postrace_fail += 1
                        logger.warning(
                            "[NG] R%02d %s  [postrace] 失敗 (rc=%d) → 未確定の可能性あり",
                            race_number, race_id, rc,
                        )

            if len(done) >= total:
                break

            next_fires = [s[0] for s in pending_jobs if (s[1], s[3]) not in done]
            if next_fires:
                next_fire  = min(next_fires)
                sleep_secs = min(30.0, max(1.0, (next_fire - datetime.datetime.now()).total_seconds()))
                time.sleep(sleep_secs)
            else:
                break

    except KeyboardInterrupt:
        logger.info("\n[中断] Ctrl+C を受け取りました。監視ループを終了します。")
        raise

    # 全レース完了後に Web データを更新
    _run_generate_web_data(dry_run)

    return prerace_success, prerace_fail, postrace_success, postrace_fail


# ─────────────────────────────────────────────────────────────────────────────
# エントリポイント
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="本日の全レース直前予想 + 結果速報 完全自律監視ループ（週次オートパイロット版）"
    )
    parser.add_argument("--date", default=None,
                        help="対象日 YYYYMMDD（省略時=当日）")
    parser.add_argument("--fire-ahead-min", type=int, default=20,
                        help="発走何分前に prerace を実行するか（デフォルト 20）")
    parser.add_argument("--result-after-min", type=int, default=15,
                        help="発走何分後に結果速報を取得するか（デフォルト 15）")
    parser.add_argument("--dry-run", action="store_true",
                        help="コマンドを実行せずにスケジュールのみ表示")
    parser.add_argument("--continuous", action="store_true",
                        help=(
                            "週次オートパイロット: 金→土→日→次週金のサイクルを自動継続。"
                            "人間介入ゼロで永続動作する。"
                        ))
    args = parser.parse_args()

    fire_ahead   = datetime.timedelta(minutes=args.fire_ahead_min)
    result_after = datetime.timedelta(minutes=args.result_after_min)
    dry_run      = args.dry_run
    continuous   = args.continuous

    target_date = args.date or datetime.date.today().strftime("%Y%m%d")

    _send_discord(
        f"🚀 **[UMALOGI] 週次オートパイロット 起動**\n"
        f"対象日: `{target_date}`  継続モード: {'ON (週次サイクル)' if continuous else 'OFF (本日のみ)'}"
    )

    while True:
        try:
            wd         = _weekday(target_date)
            is_friday  = (wd == _FRI)
            is_saturday = (wd == _SAT)
            is_sunday  = (wd == _SUN)
            is_racing  = wd in (_SAT, _SUN)

            # ── 当日の監視ループ実行 ──────────────────────────────────────
            if is_friday:
                # 金曜はJRAレースなし → 夜間バッチを待機するだけ
                logger.info("金曜日 (%s) - レースなし。夜間バッチ時刻を待機します", target_date)
                ps = pf = rs = rf = 0

            elif is_racing:
                # 土曜・日曜: 通常監視ループ
                ps, pf, rs, rf = _run_one_day(target_date, fire_ahead, result_after, dry_run)

                status = "✅" if (pf + rf) == 0 else "⚠️"
                _send_discord(
                    f"{status} **[UMALOGI] 本日監視ループ 完了**\n"
                    f"対象日: `{target_date}`\n"
                    f"直前予想: 成功 {ps} 件 / 失敗 {pf} 件\n"
                    f"結果速報: 成功 {rs} 件 / 失敗 {rf} 件\n"
                    f"{'→ 全ジョブ正常完了' if (pf + rf) == 0 else '→ 一部失敗あり。ログを確認してください。'}"
                )

            else:
                # 月〜木: レースなし
                logger.info("平日 (%s, weekday=%d) - レースなし", target_date, wd)
                ps = pf = rs = rf = 0

            if not continuous:
                break

            # ── 継続モード: 曜日別ルーティング ───────────────────────────
            now      = datetime.datetime.now()
            today_dt = datetime.datetime.strptime(target_date, "%Y%m%d")

            if is_sunday:
                # ── 日曜完了後: 週次レポート → 次週金曜まで長期スリープ ──
                logger.info("日曜監視完了。週次収支レポートを生成します...")
                _send_weekly_report(target_date, dry_run)

                next_friday_ev = _next_friday_evening(now)
                logger.info(
                    "次週金曜夜間バッチまでスリープ: %s",
                    next_friday_ev.strftime("%Y-%m-%d %H:%M"),
                )
                _send_discord(
                    f"💤 **[UMALOGI] 週次スリープ開始**\n"
                    f"次の起動: {next_friday_ev.strftime('%Y-%m-%d %H:%M')} (金曜夜間バッチ)"
                )
                _wait_until(next_friday_ev, dry_run)

                # 金曜夜間バッチ: 翌週土日の暫定予想生成
                saturday_dt   = next_friday_ev + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                # 土曜朝まで待機
                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR, minute=_MORNING_START_MINUTE, second=0, microsecond=0
                )
                _wait_until(morning_start, dry_run)
                target_date = saturday_date

            elif is_saturday:
                # ── 土曜完了後: 20:00 夜間バッチ → 日曜朝まで待機 ─────────
                evening_trigger = today_dt.replace(
                    hour=_EVENING_FETCH_HOUR, minute=_EVENING_FETCH_MINUTE, second=0, microsecond=0
                )
                if now < evening_trigger:
                    logger.info("夜間バッチ待機: %s まで", evening_trigger.strftime("%H:%M"))
                    _wait_until(evening_trigger, dry_run)

                next_date_dt  = today_dt + datetime.timedelta(days=1)
                next_date_str = next_date_dt.strftime("%Y%m%d")
                _run_evening_fetch(next_date_str, dry_run)

                morning_start = next_date_dt.replace(
                    hour=_MORNING_START_HOUR, minute=_MORNING_START_MINUTE, second=0, microsecond=0
                )
                _wait_until(morning_start, dry_run)
                target_date = next_date_str

            elif is_friday:
                # ── 金曜: 20:00 に金曜夜間バッチ → 土曜朝まで待機 ───────
                evening_trigger = today_dt.replace(
                    hour=_EVENING_FETCH_HOUR, minute=_EVENING_FETCH_MINUTE, second=0, microsecond=0
                )
                if now < evening_trigger:
                    logger.info("金曜夜間バッチ待機: %s まで", evening_trigger.strftime("%H:%M"))
                    _wait_until(evening_trigger, dry_run)

                saturday_dt   = today_dt + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR, minute=_MORNING_START_MINUTE, second=0, microsecond=0
                )
                _wait_until(morning_start, dry_run)
                target_date = saturday_date

            else:
                # ── 月〜木: 次の金曜 20:00 まで長期スリープ ─────────────
                next_friday_ev = _next_friday_evening(now)
                logger.info(
                    "平日のため次の金曜夜間バッチまでスリープ: %s",
                    next_friday_ev.strftime("%Y-%m-%d %H:%M"),
                )
                _send_discord(
                    f"💤 **[UMALOGI] 平日スリープ**\n"
                    f"次の起動: {next_friday_ev.strftime('%Y-%m-%d %H:%M')} (金曜夜間バッチ)"
                )
                _wait_until(next_friday_ev, dry_run)

                saturday_dt   = next_friday_ev + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR, minute=_MORNING_START_MINUTE, second=0, microsecond=0
                )
                _wait_until(morning_start, dry_run)
                target_date = saturday_date

        except KeyboardInterrupt:
            logger.info("[終了] Ctrl+C で停止しました")
            _send_discord("🛑 **[UMALOGI] 手動停止** Ctrl+C で監視ループを終了しました")
            break

        except Exception as exc:
            logger.error(
                "[ERROR] 予期しない例外が発生しました: %s\n%d 秒後に自動再起動します...",
                exc, _RESTART_WAIT_SEC,
            )
            _send_discord(
                f"⚠️ **[UMALOGI] 例外発生 → 自動再起動**\n"
                f"エラー: {exc}\n"
                f"{_RESTART_WAIT_SEC} 秒後に再起動します"
            )
            time.sleep(_RESTART_WAIT_SEC)
            if not continuous:
                logger.info("[RETRY] 1回だけ再試行します")
                try:
                    _run_one_day(target_date, fire_ahead, result_after, dry_run)
                except Exception as exc2:
                    logger.error("[FATAL] 再試行も失敗しました: %s", exc2)
                    _send_discord(f"🚨 **[UMALOGI] 再起動失敗** {exc2}")
                break


if __name__ == "__main__":
    main()
