"""
UMA-LOGI AI — 自律スケジューラー

競馬週次サイクルに合わせた自動実行スクリプト。
`schedule` ライブラリを使って各タスクを登録し、常駐プロセスとして動作する。

【32bit/64bit 分離設計】
  JVLink COM サーバーは 32bit プロセスからしか呼び出せない。
  このデーモン自体は 64bit Python で動作し、JVLink 操作は
  subprocess 経由で 32bit Python (`py -3-32`) を呼び出す。

スケジュール一覧:
  金曜 20:00   : JVLink RACE 同期(32bit) → WOOD(32bit) → マスタ(32bit)
                  → 暫定予想生成(64bit) → Discord 暫定予想サマリー通知
  土曜 07:30   : JVLink WOOD 同期(32bit)（調教タイム）
  日曜 07:30   : 同上
  土曜 08:30   : 当日全レース直前予想ループ起動（today_auto_runner）
  日曜 08:30   : 同上
  土曜 09:00   : WIN5 バッチ予測（独立ジョブ・WIN5締切前）
  日曜 09:00   : 同上
  土曜 10:30   : note AI厳選記事生成 → Discord転送 → (NOTE_DRAFT_AUTO_POST=1 時) note.com下書き保存
  日曜 10:30   : 同上
  土曜 13:00   : ウマニティ自動投稿（EV>=1.0 の直前予想をまとめて投稿）
  日曜 13:00   : 同上
  土曜 13:00   : レース中間 結果同期（OPT_STORED）
  土曜 15:30   : 同上
  日曜 13:00   : 同上
  日曜 15:30   : 同上
  土曜 17:30   : レース確定後 払戻同期(32bit) + 評価 + 通知 + 増分学習 + バックアップ
  日曜 17:30   : 同上
  月曜 06:00   : マスタ差分更新 (DIFN/BLOD)(32bit)
  月曜 07:00   : 週次全件再学習(64bit)
  月曜 08:00   : GitHub 自動コミット・プッシュ

Usage:
    python scripts/scheduler.py                        # デーモン起動
    python scripts/scheduler.py --run-now friday       # 即時実行（テスト用）
    python scripts/scheduler.py --run-now auto_runner  # 直前予想ループ即時起動
    python scripts/scheduler.py --run-now post_race --date 2024/01/06
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            open(
                sys.stdout.fileno(),
                mode="w",
                encoding="utf-8",
                errors="replace",
                closefd=False,
            )
        ),
        RotatingFileHandler(
            _ROOT / "data" / "scheduler.log",
            maxBytes=50 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("scheduler")

# ================================================================
# ジョブ状態管理（取りこぼしリカバリー用）
# ================================================================

_STATE_FILE: Path = _ROOT / "data" / "scheduler_state.json"

# ジョブ名 → 取りこぼし許容時間（時間）
# この時間を超えたら "時機を逸した" として再実行しない
_CATCHUP_HOURS: dict[str, int] = {
    "job_friday_sync": 16,  # Fri 20:00 → Sat 12:00 まで
    "job_morning_wood": 4,  # 07:30 → 11:30 まで
    "job_weekend_batch_pre": 4,  # 07:00 → 11:00 まで
    "job_today_auto_runner": 3,  # 08:30 → 11:30 まで
    "job_win5_prediction": 2,  # 09:00 → 11:00 まで
    "job_note_daily_article": 3,  # 10:30 → 13:30 まで
    "job_win5_result_fetch": 4,  # 17:15 → 21:15 まで
    "job_post_race": 4,  # 17:30 → 21:30 まで
    "job_health_report": 4,  # 17:50 → 21:50 まで（W-058 日次ヘルスレポート）
    "job_ab_report": 4,  # 18:00 → 22:00 まで
    "job_weekend_batch_post": 4,  # 18:30 → 22:30 まで
    "job_fit_manji_calibrator": 6,  # 月03:00 → 09:00 まで（W-057関連・卍較正週次再学習）
    "job_monday_masters": 12,  # 06:00 → 18:00 まで
    "job_weekly_retrain": 12,  # 07:00 → 19:00 まで
    "job_segment_analysis": 4,  # 07:30 → 11:30 まで
    "job_git_push": 12,  # 08:00 → 20:00 まで
}

# 各ジョブのスケジュール定義 (weekday_int: 0=月…6=日, hour, minute)
_JOB_SCHEDULES: dict[str, list[tuple[int, int, int]]] = {
    "job_friday_sync": [
        (4, 20, 0),
        (5, 20, 0),
    ],  # 金曜20:00(→土曜予想) + 土曜20:00(→日曜予想)
    "job_morning_wood": [(5, 7, 30), (6, 7, 30)],
    "job_weekend_batch_pre": [(5, 7, 0), (6, 7, 0)],
    "job_today_auto_runner": [(5, 8, 30), (6, 8, 30)],
    "job_win5_prediction": [(5, 9, 0), (6, 9, 0)],
    "job_note_daily_article": [(5, 10, 30), (6, 10, 30)],  # 土日 10:30
    "job_win5_result_fetch": [(5, 17, 15), (6, 17, 15)],
    "job_post_race": [(5, 17, 30), (6, 17, 30)],
    "job_health_report": [(5, 17, 50), (6, 17, 50)],  # 土日 17:50（W-058）
    "job_ab_report": [(6, 18, 0)],  # 日曜18:00
    "job_weekend_batch_post": [(5, 18, 30), (6, 18, 30)],
    "job_fit_manji_calibrator": [(0, 3, 0)],  # 月曜03:00（開催なし・データ確定済み）
    "job_monday_masters": [(0, 6, 0)],
    "job_weekly_retrain": [(0, 7, 0)],
    "job_segment_analysis": [(0, 7, 30)],
    "job_git_push": [(0, 8, 0)],
}


def _load_job_state() -> dict[str, str]:
    """scheduler_state.json を読み込む。ファイルがなければ空辞書を返す。"""
    if not _STATE_FILE.exists():
        return {}
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_job_state(state: dict[str, str]) -> None:
    """scheduler_state.json に書き込む。"""
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("scheduler_state.json 書き込み失敗: %s", exc)


def _mark_job_done(job_name: str) -> None:
    """ジョブ成功時に state ファイルを更新する。"""
    state = _load_job_state()
    state[job_name] = datetime.now().isoformat(timespec="seconds")
    _save_job_state(state)


def _should_recover(
    last_run: datetime | None,
    scheduled_today: datetime,
    now: datetime,
    catchup_hours: int,
) -> bool:
    """
    ジョブを今すぐ実行すべきかを判定する。

    条件:
    1. スケジュール時刻がすでに過ぎている
    2. 取りこぼし許容窓内（elapsed_hours < catchup_hours）
    3. 当日まだ実行されていない（last_run が今日より前か None）
    """
    if now < scheduled_today:
        return False
    elapsed_hours = (now - scheduled_today).total_seconds() / 3600
    if elapsed_hours >= catchup_hours:
        return False
    if last_run is None:
        return True
    return last_run.date() < now.date()


def _recover_missed_jobs(
    job_map: dict[str, object],
) -> None:
    """
    起動時に取りこぼしたジョブを検出して即実行する。

    PC 再起動やスリープ復帰後にスケジューラーが起動した場合に対応する。
    キャッチアップ窓が日をまたぐジョブ（例: job_friday_sync の 16h 窓）にも対応するため、
    当日だけでなく前日のスケジュールも確認する。
    """
    state = _load_job_state()
    now = datetime.now()

    for job_name, schedules in _JOB_SCHEDULES.items():
        fn = job_map.get(job_name)
        if fn is None:
            continue

        for wd, h, m in schedules:
            # 当日 (day_delta=0) と前日 (day_delta=-1) を両方チェック
            # これにより金曜夜ジョブが土曜朝起動時にも正しく回収される
            for day_delta in (0, -1):
                target_day = now + timedelta(days=day_delta)
                if wd != target_day.weekday():
                    continue

                scheduled_time = target_day.replace(
                    hour=h, minute=m, second=0, microsecond=0
                )
                last_run_str = state.get(job_name)
                last_run: datetime | None = None
                if last_run_str:
                    try:
                        last_run = datetime.fromisoformat(last_run_str)
                    except ValueError:
                        pass

                catchup = _CATCHUP_HOURS.get(job_name, 4)
                if _should_recover(last_run, scheduled_time, now, catchup):
                    logger.warning(
                        "[リカバリー] %s は %s に実行すべきでしたが取りこぼしを検出 → 今すぐ実行",
                        job_name,
                        scheduled_time.strftime("%m/%d %H:%M"),
                    )
                    _send_discord(
                        f"⚠️ [UMALOGI] ジョブ取りこぼしをリカバリー: `{job_name}` "
                        f"（予定 {scheduled_time.strftime('%m/%d %H:%M')} → 今実行）"
                    )
                    try:
                        fn()  # type: ignore[operator]
                        _mark_job_done(job_name)
                    except Exception as exc:
                        logger.error("[リカバリー] %s 実行失敗: %s", job_name, exc)
                    break  # このジョブはリカバリー済み、次のジョブへ


try:
    import schedule  # type: ignore[import-untyped]

    _SCHEDULE_AVAILABLE = True
except ImportError:
    logger.warning("schedule がインストールされていません: pip install schedule")
    _SCHEDULE_AVAILABLE = False


# ================================================================
# サブプロセス / Discord ユーティリティ
# ================================================================

# JVLink は 32bit COM のため専用インタープリタを使用する
# PY32_CMD 環境変数で上書き可能（例: "py -3.11-32"）
_PY32 = os.environ.get("PY32_CMD", "py -3.14-32").split()
_PY64 = ["py"]


def _run(cmd: list[str], label: str, timeout: int = 3600) -> int:
    """
    サブプロセスを実行して returncode を返す。

    stdout/stderr はリアルタイムでロガーに流す。
    timeout 秒（デフォルト1時間）を超えた場合は強制終了して -1 を返す。
    """
    logger.info("[%s] 開始: %s", label, " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logger.info("[%s] %s", label, line)
        proc.wait(timeout=timeout)
        rc = proc.returncode
        if rc == 0:
            logger.info("[%s] 完了: rc=0", label)
        else:
            logger.warning("[%s] 終了: rc=%d", label, rc)
        return rc
    except subprocess.TimeoutExpired:
        proc.kill()
        logger.error("[%s] タイムアウト（%d秒）", label, timeout)
        return -1
    except Exception as exc:
        logger.error("[%s] 実行エラー: %s", label, exc)
        return -1


# JVLink 起動タイムアウト設計値 (2026-05-22 更新):
#   JVInit 3リトライ: 1秒 × 2スリープ = 2秒 (スリープ間隔を 3s→1s に短縮)
#   合計想定: 2〜5秒 → マージン込みで 10秒
#   ダイアログ発生時は JVSetDialog/JVSetAutoDownload + STARTUPINFO SW_HIDE で抑制済み。
#   残存するダイアログは JVLINK_FAILED シグナルで即通知 → 10秒後タイムアウトで Kill → Netkeiba fallback。
_JVLINK_STARTUP_TIMEOUT = 10  # JVLink初期化タイムアウト秒数（GUIダイアログ検出用）


def _run_jvlink(
    cmd: list[str],
    label: str,
    startup_timeout: int = _JVLINK_STARTUP_TIMEOUT,
    fetch_timeout: int = 3600,
) -> int:
    """
    JVLink専用subprocess実行。

    GUIダイアログブロック検出: 子プロセスが startup_timeout 秒以内に
    "JVLINK_READY" を stdout に出力しない場合、ダイアログで停止していると判断して
    Kill し、-2 (GUI_BLOCKED) を返す。呼び出し元はこの値を受け取ったら
    Netkeiba フォールバックへ切り替えること。

    CREATE_NO_WINDOW フラグでダイアログウィンドウの描画自体も抑制する。

    Returns:
        0   : 正常完了
        -1  : タイムアウトまたは起動失敗
        -2  : GUI_BLOCKED — JVLink設定ダイアログで停止を検出。Netkeibaへフォールバックすること
    """
    import threading as _threading

    CREATE_NO_WINDOW = 0x08000000

    # STARTUPINFO(SW_HIDE=0): COM が子ウィンドウを生成しても非表示にする
    _si = subprocess.STARTUPINFO()
    _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    _si.wShowWindow = 0  # SW_HIDE

    logger.info("[%s] JVLink起動: %s", label, " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            creationflags=CREATE_NO_WINDOW,
            startupinfo=_si,
        )
    except Exception as exc:
        logger.error("[%s] 起動失敗: %s", label, exc)
        return -1

    ready_event = _threading.Event()
    failed_event = _threading.Event()  # JVLINK_FAILED 受信フラグ

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logger.info("[%s] %s", label, line)
            if "JVLINK_READY" in line:
                ready_event.set()
            elif "JVLINK_FAILED" in line:
                # JVInit が全リトライ消化して失敗した — タイムアウトを待たずに即Fallback
                failed_event.set()
                ready_event.set()  # wait() を解除するため set() する

    reader_thread = _threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    if not ready_event.wait(timeout=startup_timeout):
        proc.kill()
        proc.wait()
        logger.error(
            "[%s] JVLink起動タイムアウト(%d秒) — GUIダイアログブロック疑い。"
            "Kill完了。setup_jvlink.py で初期設定を実施してください。",
            label,
            startup_timeout,
        )
        return -2  # GUI_BLOCKED

    if failed_event.is_set():
        proc.kill()
        proc.wait()
        logger.error(
            "[%s] JVLINK_FAILED 受信 — JVInit が全リトライ消化して失敗。"
            "SID/TARGET状態を確認してください。Netkeibaフォールバックへ切り替えます。",
            label,
        )
        return -2  # JVLINK_FAILED → same as GUI_BLOCKED for fallback routing

    reader_thread.join(timeout=fetch_timeout)
    if reader_thread.is_alive():
        proc.kill()
        logger.error("[%s] データ取得タイムアウト(%d秒)", label, fetch_timeout)
        return -1

    proc.wait()
    rc = proc.returncode
    if rc == 0:
        logger.info("[%s] 完了: rc=0", label)
    else:
        logger.warning("[%s] 終了: rc=%d", label, rc)
    return rc


def _netkeiba_fallback_entries(target_date: str, label: str) -> None:
    """JVLink失敗時のエントリーNetkeiba補完（GUI_BLOCKEDフォールバック専用）。"""
    logger.warning(
        "[%s] JVLink GUI_BLOCKED → Netkeiba フォールバックでエントリー取得を試みます (date=%s)",
        label,
        target_date,
    )
    _send_discord(
        f"⚠️【{label}】JVLink設定ダイアログ停止を検出。"
        f"Netkeibaフォールバックでエントリー取得 ({target_date})。"
        f"夜間に `scripts/setup_jvlink.py` を実行してJVLink初期設定を完了してください。"
    )
    rc_fb = _run(
        _PY64 + ["scripts/refetch_entries_from_netkeiba.py", "--date", target_date],
        f"{label}-Netkeiba補完",
        timeout=1800,
    )
    if rc_fb == 0:
        logger.info("[%s] Netkeiba エントリー補完成功", label)
    else:
        logger.error("[%s] Netkeiba エントリー補完も失敗: rc=%d", label, rc_fb)


def _netkeiba_fallback_results(target_date: str, label: str) -> None:
    """JVLink失敗時の払戻・結果Netkeiba補完（GUI_BLOCKEDフォールバック専用）。"""
    logger.warning(
        "[%s] JVLink GUI_BLOCKED → Netkeiba フォールバックでレース結果取得を試みます (date=%s)",
        label,
        target_date,
    )
    _send_discord(
        f"⚠️【{label}】JVLink設定ダイアログ停止を検出。"
        f"Netkeibaフォールバックでレース結果取得 ({target_date})。"
        f"夜間に `scripts/setup_jvlink.py` を実行してJVLink初期設定を完了してください。"
    )
    rc_fb = _run(
        _PY64 + ["-m", "src.ops.data_sync", "netkeiba_results", "--date", target_date],
        f"{label}-Netkeiba補完",
        timeout=1800,
    )
    if rc_fb == 0:
        logger.info("[%s] Netkeiba 結果補完成功", label)
    else:
        logger.error("[%s] Netkeiba 結果補完も失敗: rc=%d", label, rc_fb)


def _run_with_retry(
    cmd: list[str],
    label: str,
    timeout: int = 3600,
    max_retries: int = 3,
    base_delay: float = 60.0,
) -> int:
    """
    _run() を Exponential Backoff で最大 max_retries 回リトライするラッパー。

    成功（rc==0）した時点で即座にリターン。全試行失敗時は最後の rc を返す。
    base_delay=0 はテスト用（実運用は 60 以上を推奨）。

    Backoff schedule (base_delay=60):
      試行1: 即時
      試行2: 60s 後
      試行3: 180s 後
      試行4: 600s 後（上限 cap=600）
    """
    rc = _run(cmd, label, timeout=timeout)
    if rc == 0:
        return 0

    for attempt in range(1, max_retries + 1):
        delay = min(base_delay * (3 ** (attempt - 1)), 600.0)
        logger.warning(
            "[%s] 失敗(rc=%d) — %d秒後に再試行 (%d/%d)",
            label,
            rc,
            int(delay),
            attempt,
            max_retries,
        )
        if delay > 0:
            time.sleep(delay)
        rc = _run(cmd, label, timeout=timeout)
        if rc == 0:
            logger.info("[%s] リトライ %d 回目で成功", label, attempt)
            return 0

    logger.error("[%s] 全リトライ失敗（%d 回試行）rc=%d", label, max_retries + 1, rc)
    return rc


def _send_discord(text: str) -> None:
    """システムチャンネルにテキストを送信する。NotificationRouter 経由。"""
    try:
        from src.notification.router import NotificationRouter

        NotificationRouter().send_system_text(text)
    except Exception as exc:
        logger.warning("Discord 送信失敗: %s", exc)


def _send_discord_embed(embeds: list[dict]) -> None:
    """システムチャンネルに Embed を送信する。NotificationRouter 経由。"""
    try:
        from src.notification.router import NotificationRouter

        router = NotificationRouter()
        for embed in embeds:
            title = embed.get("title", "通知")
            desc = embed.get("description", "")
            router.send_system_embed(title, desc, color=embed.get("color"))
    except Exception as exc:
        logger.warning("Discord Embed 送信失敗: %s", exc)


def _notify_provisional_summary(target_date: str) -> None:
    """
    暫定予想バッチ完了後に Discord Embed でリッチ通知を送信する。

    送信内容:
      - 対象日・会場別レース数
      - EV ≥ 1.0 の推奨買い目（上位 10 件）を会場別に整理
      - 集計サマリー（総投資額・期待払戻）
      - ステップ完了タイムスタンプ

    Args:
        target_date: "YYYY-MM-DD" 形式の対象日
    """
    try:
        import sqlite3
        from datetime import datetime as _dt

        db_path = _ROOT / "data" / "umalogi.db"
        conn = sqlite3.connect(str(db_path))

        # 会場別レース数
        venue_rows = conn.execute(
            """
            SELECT venue, COUNT(*) AS cnt
            FROM races
            WHERE date = ?
            GROUP BY venue
            ORDER BY cnt DESC
            """,
            (target_date,),
        ).fetchall()
        race_count = sum(c for _, c in venue_rows)

        # EV >= 1.0 の暫定予想（馬分析・WIN5 除く）
        ev_rows = conn.execute(
            """
            SELECT r.venue, r.race_number, p.bet_type,
                   p.expected_value, p.recommended_bet, p.model_type
            FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            WHERE r.date = ?
              AND p.model_type LIKE '%暫定%'
              AND p.bet_type NOT IN ('馬分析', 'WIN5')
              AND p.expected_value >= 1.0
            ORDER BY p.expected_value DESC
            """,
            (target_date,),
        ).fetchall()

        # 土日両日を想定: 翌日も取得
        tomorrow2 = conn.execute(
            "SELECT COUNT(*) FROM races WHERE date > ? ORDER BY date LIMIT 1",
            (target_date,),
        ).fetchone()[0]

        conn.close()

        now_str = _dt.now().strftime("%Y-%m-%d %H:%M")
        color_ok = 0x00C8FF  # シアン（正常）
        color_warn = 0xFFD700  # ゴールド（推奨なし）

        # ── 会場別レース数フィールド ─────────────────────────────
        venue_text = "\n".join(f"**{v}** {c}R" for v, c in venue_rows) or "—"

        # ── 推奨買い目フィールド（上位 10 件） ───────────────────
        if ev_rows:
            pick_lines = []
            for venue, race_no, bet_type, ev, rec_bet, model in ev_rows[:10]:
                bet_str = f"¥{int(rec_bet):,}" if rec_bet else "—"
                icon = "⚡" if "卍" in model else "🎯"
                pick_lines.append(
                    f"{icon} **{venue}{race_no}R** {bet_type}  EV `{ev:.2f}`  推奨 {bet_str}"
                )
            if len(ev_rows) > 10:
                pick_lines.append(f"… 他 {len(ev_rows) - 10} 件")
            picks_text = "\n".join(pick_lines)
            total_rec = sum(r[4] or 0 for r in ev_rows)
            summary_text = (
                f"推奨 **{len(ev_rows)}** 件 ／ "
                f"総推奨投資額 **¥{int(total_rec):,}** ／ "
                f"対象 {race_count} レース"
            )
            embed_color = color_ok
        else:
            picks_text = "EV ≥ 1.0 の買い目なし — 全レース見送り推奨"
            summary_text = f"対象 {race_count} レース"
            embed_color = color_warn

        embed: dict = {
            "title": f"📋 暫定予想バッチ完了 — {target_date}",
            "color": embed_color,
            "description": summary_text,
            "fields": [
                {
                    "name": "🏟️ 会場別レース数",
                    "value": venue_text,
                    "inline": True,
                },
                {
                    "name": "🔥 EV ≥ 1.0 推奨買い目",
                    "value": picks_text,
                    "inline": False,
                },
            ],
            "footer": {
                "text": f"UMALOGI AI  |  {now_str} 生成  |  詳細は Streamlit ダッシュボードで確認",
            },
        }
        _send_discord_embed([embed])

    except Exception as exc:
        logger.warning("暫定予想サマリー通知失敗（続行）: %s", exc)
        _send_discord(f"📋 【暫定予想完了】{target_date} — サマリー取得エラー: {exc}")


# ================================================================
# 各ジョブ定義
# ================================================================


def job_friday_sync() -> None:
    """
    金曜夜バッチ（完全自動版）

    【設計】32bit と 64bit を subprocess で分離し、64bit デーモンから
    すべての処理を一気通貫で実行する。

    Step 1: JVLink RACE 同期       (32bit) — 出馬表・成績レコード取得
    Step 2: JVLink WOOD 同期       (32bit) — 調教タイム取得
    Step 3: JVLink マスタ差分更新  (32bit) — 騎手・調教師マスタ
    Step 4: AI 暫定予想生成        (64bit) — LightGBM で全レース暫定予想
    Step 5: Discord 暫定予想通知   (64bit) — EV≥1.0 の買い目サマリーを送信
    Step 6: DB バックアップ        (64bit) — ローカル + クラウド
    """
    logger.info("=" * 60)
    logger.info("=== [金曜バッチ] 開始 ===")
    logger.info("=" * 60)

    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    target_yyyymmdd = tomorrow.replace("-", "")

    errors: list[str] = []

    # ── Step 1: JVLink RACE 同期（32bit 必須）───────────────────
    # _run_jvlink: 10秒でJVLINK_READY未到着 → GUIダイアログブロック検出 → -2返却
    # rc=-2: GUIブロック, rc=1: JVRead -503 (SID制約) 等の JVLink内部エラー
    # どちらの場合も netkeiba フォールバックでエントリーを補完する
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "friday"], "JVLink-RACE")
    if rc != 0:
        _netkeiba_fallback_entries(target_yyyymmdd, "JVLink-RACE")
        errors.append(f"JVLink RACE 同期失敗(rc={rc}) → Netkeibaフォールバック実施")

    # ── Step 2: JVLink WOOD 同期（32bit 必須）───────────────────
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD")
    if rc == -2:
        logger.warning(
            "[金曜バッチ] JVLink WOOD GUI_BLOCKED — 調教タイムは取得不可（続行）"
        )
        errors.append("JVLink WOOD 同期 GUI_BLOCKED（調教タイムは暫定予想でも使用可）")
    elif rc != 0:
        errors.append(f"JVLink WOOD 同期失敗(rc={rc})")

    # ── Step 3: JVLink マスタ差分更新（32bit 必須）──────────────
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters")
    if rc == -2:
        logger.warning(
            "[金曜バッチ] JVLink Masters GUI_BLOCKED — マスタ更新スキップ（続行）"
        )
        errors.append("JVLink Masters GUI_BLOCKED（既存マスタで続行）")
    elif rc != 0:
        errors.append(f"JVLink マスタ更新失敗(rc={rc})")

    # ── Step 4: AI 暫定予想生成（64bit）─────────────────────────
    rc = _run_with_retry(
        _PY64 + ["-m", "src.main_pipeline", "provisional", "--date", target_yyyymmdd],
        "暫定予想",
        timeout=3600,
        max_retries=2,
        base_delay=120.0,
    )
    if rc != 0:
        msg = f"🚨【緊急】金曜バッチ: 暫定予想生成が失敗しました (rc={rc})。サーバーを確認してください。"
        logger.error("[金曜バッチ] 暫定予想失敗 — Discord SOS 通知")
        _send_discord(msg)
        errors.append(f"暫定予想失敗(rc={rc})")
        # 予想失敗でもバックアップは実行する
    else:
        # ── Step 5: Discord 暫定予想通知 ───────────────────────
        _notify_provisional_summary(tomorrow)

    # ── Step 6: DB バックアップ（64bit）─────────────────────────
    try:
        from src.ops.backup import backup_db

        backup_db()
        logger.info("[金曜バッチ] バックアップ完了")
    except Exception as bk_exc:
        logger.warning("[金曜バッチ] バックアップ失敗: %s", bk_exc)

    if errors:
        logger.warning("=== [金曜バッチ] 完了（一部エラー: %s）===", " / ".join(errors))
    else:
        _mark_job_done("job_friday_sync")
        logger.info("=== [金曜バッチ] 完了（全ステップ正常）===")


def job_morning_wood() -> None:
    """土日朝: 調教タイム同期（32bit subprocess）"""
    logger.info("=== [朝調教同期] 開始 ===")
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD朝")
    if rc == -2:
        logger.warning(
            "[朝調教同期] JVLink GUI_BLOCKED — 調教タイムは取得不可。setup_jvlink.py を実行してください。"
        )
    elif rc != 0:
        logger.error("[朝調教同期] 失敗: rc=%d", rc)
    else:
        _mark_job_done("job_morning_wood")
        logger.info("=== [朝調教同期] 完了 ===")


_auto_runner_lock = threading.Lock()
_post_race_lock = threading.Lock()
_weekly_retrain_lock = threading.Lock()  # W-052: 全件再学習(SIMULATE)の二重起動防止


def job_today_auto_runner() -> None:
    """
    土日 08:30: 当日全レース直前予想ループ起動

    【設計】today_auto_runner.py は一日中常駐する長時間プロセスのため、
    バックグラウンドスレッドで起動し、スケジューラーをブロックしない。

    - 各レースの推定発走 20 分前に `prerace_pipeline` を自動実行
    - prerace_pipeline は完了後に Discord へ全券種まとめ通知を送信
    - 全レース終了後にスレッドは自然終了する

    重複起動ガード: 既に auto_runner スレッドが動作中の場合はスキップ。
    """
    if not _auto_runner_lock.acquire(blocking=False):
        logger.warning("[直前予想ループ] 既に起動中のため二重起動をスキップします")
        return

    def _run_loop() -> None:
        import datetime as _dt

        _RACING_CUTOFF_HOUR = 19  # 19:00 以降は再起動しない
        attempt = 0
        try:
            while True:
                attempt += 1
                logger.info(
                    "=== [直前予想ループ] バックグラウンドスレッド開始 (attempt=%d) ===",
                    attempt,
                )
                rc = _run(
                    _PY64 + ["scripts/today_auto_runner.py"],
                    "直前予想ループ",
                    timeout=14 * 3600,  # 最大14時間（8:30〜22:30）
                )
                if rc == 0:
                    logger.info("=== [直前予想ループ] 正常終了 ===")
                    break

                now_h = _dt.datetime.now().hour
                if now_h >= _RACING_CUTOFF_HOUR:
                    logger.warning(
                        "[直前予想ループ] 異常終了 rc=%d だが %d時以降のため再起動しません",
                        rc,
                        now_h,
                    )
                    _send_discord(
                        f"⚠️【直前予想ループ】異常終了 rc={rc}。レース終了後のため再起動不要。"
                    )
                    break

                logger.error(
                    "[直前予想ループ] 異常終了: rc=%d — 30秒後に再起動します (attempt=%d)",
                    rc,
                    attempt,
                )
                _send_discord(
                    f"🔄【直前予想ループ】異常終了 rc={rc}。30秒後に自動再起動します (attempt={attempt})。"
                )
                time.sleep(30)
        finally:
            _auto_runner_lock.release()

    thread = threading.Thread(
        target=_run_loop,
        name="today_auto_runner",
        daemon=True,
    )
    thread.start()
    logger.info(
        "[直前予想ループ] バックグラウンドスレッドを起動しました (thread=%s)",
        thread.name,
    )


def job_win5_prediction() -> None:
    """
    土日朝 9:00: WIN5 バッチ予測を実行する。

    金曜バッチで races が揃った後、当日レースの先頭5件を対象に
    Win5Engine で予測して predictions テーブルに保存し Discord 通知する。
    prerace_pipeline 内の _try_win5() とは独立して実行するため、
    WIN5 の締切（最初の対象レース発走前）に間に合うように朝9:00に設定。
    """
    logger.info("=== [WIN5予測] 開始 ===")
    try:
        from src.main_pipeline import win5_batch

        result = win5_batch()
        if result.get("skipped"):
            logger.info("[WIN5予測] スキップ: %s", result.get("reason", ""))
            # スキップも「正常完了」として記録（取りこぼし再試行を防ぐ）
            _mark_job_done("job_win5_prediction")
        elif result.get("error"):
            logger.error("[WIN5予測] エラー: %s", result["error"])
            # エラー時は state を更新しない → 起動時リカバリーが再実行できる
        else:
            ev = result.get("ev", 0) or 0
            bet = result.get("bet", 0) or 0
            logger.info("[WIN5予測] 完了: EV=%.3f 推定払戻=¥%.0f", ev, bet)
            _mark_job_done("job_win5_prediction")
    except Exception as e:
        logger.error("[WIN5予測] 例外: %s", e, exc_info=True)
        _send_discord(f"🚨 [ERROR] WIN5タスクが失敗しました: {e}")
    logger.info("=== [WIN5予測] 終了 ===")


def job_win5_result_fetch() -> None:
    """
    土日 17:15: WIN5 確定結果（的中馬番5つ＋払戻）を netkeiba から取得する。

    全レース終了後（最終レース約15:30 + バッファ）に実行。
    取得結果は win5_results テーブルに保存し、UI の予実比較に反映される。
    """
    logger.info("=== [WIN5結果取得] 開始 ===")
    try:
        from scripts.fetch_win5_result import fetch_win5_result

        result = fetch_win5_result()
        if result.get("skipped"):
            logger.info("[WIN5結果取得] スキップ: %s", result.get("reason", ""))
            _mark_job_done("job_win5_result_fetch")
        elif result.get("winning_numbers"):
            logger.info(
                "[WIN5結果取得] 完了: 的中馬番=%s 払戻=¥%d",
                result["winning_numbers"],
                result.get("payout", 0),
            )
            _mark_job_done("job_win5_result_fetch")
        else:
            logger.warning(
                "[WIN5結果取得] 結果なし: %s", result.get("reason", "unknown")
            )
    except Exception as e:
        logger.error("[WIN5結果取得] 例外: %s", e, exc_info=True)
        _send_discord(f"🚨 [ERROR] WIN5結果取得タスクが失敗しました: {e}")
    logger.info("=== [WIN5結果取得] 終了 ===")


def job_umanity_upload() -> None:
    """
    土日 各レース直前: ウマニティへの予想自動投稿。

    EV >= 1.0 の当日予想を Playwright でウマニティに投稿する。
    today_auto_runner が全レースを直前予想した後（概ね13:00以降）に
    まとめて投稿するバッチ。BAN 回避のためランダムスリープを内包する。

    UMANITY_EMAIL / UMANITY_PASSWORD が未設定の場合はスキップ。
    """
    import os

    if not os.environ.get("UMANITY_EMAIL") or not os.environ.get("UMANITY_PASSWORD"):
        logger.info("[Umanity投稿] UMANITY_EMAIL/PASSWORD 未設定のためスキップ")
        return

    logger.info("=== [Umanity投稿] 開始 ===")
    try:
        from src.ops.umanity_uploader import run_upload

        target_date = date.today().strftime("%Y%m%d")
        stats = run_upload(target_date=target_date, dry_run=False, headless=True)
        logger.info(
            "[Umanity投稿] 完了: 成功=%d スキップ=%d エラー=%d",
            stats["success"],
            stats["skip"],
            stats["error"],
        )
        _send_discord(
            f"🐴 **[Umanity] 本日の予想投稿完了**\n"
            f"成功: {stats['success']} 件 / スキップ: {stats['skip']} 件 / エラー: {stats['error']} 件"
        )
    except ImportError:
        logger.warning(
            "[Umanity投稿] playwright 未インストール — pip install playwright && playwright install chromium"
        )
    except Exception as e:
        logger.error("[Umanity投稿] 例外: %s", e, exc_info=True)
        _send_discord(f"🚨 [Umanity] 投稿失敗: {e}")
    logger.info("=== [Umanity投稿] 終了 ===")


def job_intraday_sync(target_date: str | None = None) -> None:
    """
    土日レース中間: 確定済みレースの結果を随時 DB に同期する。

    OPT_STORED を使用するため、TARGET frontier JV が先に取得済みでも確実に取得できる。
    評価・通知は行わず、race_results / race_payouts の充填のみを目的とする。
    """
    if target_date is None:
        target_date = date.today().strftime("%Y/%m/%d")
    date_yyyymmdd = target_date.replace("/", "")
    logger.info("=== [中間結果同期] %s 開始 ===", target_date)
    rc = _run_with_retry(
        _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
        "JVLink-中間結果同期",
    )
    if rc == 0:
        logger.info("=== [中間結果同期] %s 完了 ===", target_date)
    else:
        logger.warning("[中間結果同期] 失敗: rc=%d", rc)

    # 中間同期後: 払戻データから着順自動補完（SEレコード未着対策）
    try:
        from src.database.init_db import init_db as _init_db
        from scripts.infer_ranks_from_payouts import infer_ranks as _infer_ranks

        _conn_infer = _init_db()
        _stats = _infer_ranks(_conn_infer, year_filter=None, dry_run=False)
        _conn_infer.close()
        logger.info(
            "[中間結果同期] 払戻補完: rank1=%d rank2=%d rank3=%d",
            _stats["rank1_set"],
            _stats["rank2_set"],
            _stats["rank3_set"],
        )
    except Exception as _infer_exc:
        logger.warning("[中間結果同期] 払戻補完失敗（続行）: %s", _infer_exc)


def _job_post_race_body(target_date: str) -> None:
    """
    job_post_race の本体処理（バックグラウンドスレッドから呼び出される）。

    Step 1: JVLink 払戻同期（32bit）
    Step 1.5: 払戻データから着順自動補完
    Step 2: 評価 + 通知 + 増分学習（64bit）
    Step 3: DB バックアップ
    Step 4: ダッシュボード JSON 再生成
    """
    logger.info("=== [レース後処理] %s 開始 ===", target_date)

    # Step 1: JVLink RACE 払戻同期（32bit）
    # OPT_NORMAL → OPT_STORED → OPT_SETUP の3段階フォールバックを data_sync が自動実施
    date_yyyymmdd = target_date.replace("/", "")
    rc = _run_jvlink(
        _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
        "JVLink-払戻同期",
    )
    if rc == -2:
        _netkeiba_fallback_results(date_yyyymmdd, "JVLink-払戻同期")
        logger.info("[レース後処理] Netkeibaフォールバック完了 — 後続処理を続行")
    elif rc != 0:
        logger.warning(
            "[レース後処理] JVLink 払戻同期失敗: rc=%d — 払戻推論フォールバックへ",
            rc,
        )

    # Step 1.5: 払戻データから着順自動補完（SEレコード未達対策）
    try:
        from src.database.init_db import init_db as _init_db
        from scripts.infer_ranks_from_payouts import infer_ranks as _infer_ranks

        _conn_infer = _init_db()
        _stats = _infer_ranks(_conn_infer, year_filter=None, dry_run=False)
        _conn_infer.close()
        if _stats["rank1_set"] > 0:
            logger.info(
                "[レース後処理] 払戻補完: rank1=%d rank2=%d rank3=%d (スキップ=%d)",
                _stats["rank1_set"],
                _stats["rank2_set"],
                _stats["rank3_set"],
                _stats["skipped"],
            )
    except Exception as _infer_exc:
        logger.warning("[レース後処理] 払戻補完失敗（続行）: %s", _infer_exc)

    # Step 2: 評価 + 通知 + 増分学習（64bit）
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import batch_evaluate_date

        conn = init_db()
        try:
            # batch_evaluate_date は 'YYYY-MM-DD' 形式を期待するが
            # target_date は 'YYYY/MM/DD' 形式のため変換する
            iso_date = target_date.replace("/", "-")
            # W-052: retrain=False で当日評価+通知のみ。再訓練は月曜 weekly_retrain に集約し、
            # 毎レースの全年度再シミュレーション（スケジューラ長時間ブロック）を根絶する。
            results = batch_evaluate_date(conn, iso_date, notify=True, retrain=False)
            hit_count = sum(
                r["evaluation"].hit_count for r in results if "evaluation" in r
            )
            logger.info(
                "[レース後処理] 完了: %d レース 合計的中=%d", len(results), hit_count
            )
        finally:
            conn.close()
    except Exception as e:
        logger.error("[レース後処理] 評価失敗: %s", e, exc_info=True)

    # Step 3: DB バックアップ（エラーでも実行）
    try:
        from src.ops.backup import backup_db

        backup_db()
        logger.info("[バックアップ] 完了")
    except Exception as bk_exc:
        logger.warning("[バックアップ] 失敗: %s", bk_exc)

    # Step 4: ダッシュボード JSON 再生成（エラーでも実行）
    try:
        import subprocess as _sp
        import sys as _sys

        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen], check=True, timeout=120)
        logger.info("[ダッシュボード] JSON 再生成完了")
    except Exception as gen_exc:
        logger.warning("[ダッシュボード] JSON 再生成失敗: %s", gen_exc)

    _mark_job_done("job_post_race")
    logger.info("=== [レース後処理] %s 終了 ===", target_date)


def job_post_race(target_date: str | None = None) -> None:
    """
    土日夕方: レース確定後の払戻同期(32bit) + 評価 + 通知 + 増分学習 + バックアップ

    【設計】JVLink払戻同期 + batch_evaluate_date（増分学習含む）は数時間かかる場合があり、
    スケジューラーのメインループ（schedule.run_pending）をブロックすると
    同日 20:00 の job_friday_sync（翌日分の暫定予想）が実行されない問題が発生する。
    このため本関数はバックグラウンドスレッドで実体処理を起動してすぐにリターンする。

    重複起動ガード: 既に post_race スレッドが動作中の場合はスキップ。
    """
    if not _post_race_lock.acquire(blocking=False):
        logger.warning("[レース後処理] 既に実行中のため二重起動をスキップします")
        return

    resolved_date = (
        target_date if target_date is not None else date.today().strftime("%Y/%m/%d")
    )

    def _body() -> None:
        try:
            _job_post_race_body(resolved_date)
        except Exception as exc:
            logger.error("[レース後処理] スレッド内例外: %s", exc, exc_info=True)
        finally:
            _post_race_lock.release()

    thread = threading.Thread(
        target=_body,
        name=f"post_race_{resolved_date.replace('/', '')}",
        daemon=True,
    )
    thread.start()
    logger.info(
        "[レース後処理] バックグラウンドスレッドを起動しました (date=%s, thread=%s)",
        resolved_date,
        thread.name,
    )


def job_monday_masters() -> None:
    """月曜: マスタデータ差分更新（32bit subprocess）"""
    logger.info("=== [マスタ更新] 開始 ===")
    rc = _run_jvlink(
        _PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters月曜"
    )
    if rc == -2:
        logger.warning(
            "[マスタ更新] JVLink GUI_BLOCKED — setup_jvlink.py を実行してください。"
        )
    elif rc != 0:
        logger.error("[マスタ更新] 失敗: rc=%d", rc)
    else:
        _mark_job_done("job_monday_masters")
        logger.info("=== [マスタ更新] 完了 ===")


def job_fit_manji_calibrator() -> None:
    """月曜AM3:00: 卍較正器(manji_win_calibrator)を直近確定実績で週次再学習する（W-057関連）。

    競馬開催がなくデータが確定している時間帯に Isotonic を再 fit し pkl を上書き更新する。
    較正の鮮度を保ち、EV=較正P×odds の信頼性を維持する。失敗してもスケジューラは継続。
    """
    logger.info("=== [卍較正 週次再学習] 開始 ===")
    try:
        from src.database.init_db import init_db
        from src.ml.manji_calibration import fit_manji_win_calibrator

        conn = init_db()
        try:
            diag = fit_manji_win_calibrator(conn)
            logger.info("[卍較正 週次再学習] 完了: %s", diag)
        finally:
            conn.close()
        _mark_job_done("job_fit_manji_calibrator")
    except Exception as e:
        logger.error("[卍較正 週次再学習] 失敗: %s", e, exc_info=True)


def _job_weekly_retrain_body() -> None:
    """週次全件再学習の実体（全件特徴量再生成=SIMULATE を含む長時間処理）。"""
    logger.info("=== [週次再学習] 開始 ===")
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import weekly_retrain

        conn = init_db()
        try:
            # weekly_retrain 内に土日ガード（条項2 / W-052）あり。月曜起動が前提。
            result = weekly_retrain(conn)
            logger.info("[週次再学習] 完了: %s", result)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[週次再学習] 失敗: %s", e, exc_info=True)

    # 再学習後に summary / financial.json を更新
    try:
        import subprocess as _sp
        import sys as _sys

        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen, "--no-detail"], check=True, timeout=120)
        logger.info("[週次再学習] ダッシュボード JSON 更新完了")
    except Exception as gen_exc:
        logger.warning("[週次再学習] JSON 更新失敗: %s", gen_exc)
    _mark_job_done("job_weekly_retrain")


def job_weekly_retrain() -> None:
    """月曜: 全件再学習 + summary.json 再生成（64bit）。

    W-052: 全件再学習は全履歴の特徴量再生成（SIMULATE）を伴い数時間かかりうる。
    schedule.run_pending と同一スレッドで実行するとスケジューラ全体をブロックするため、
    バックグラウンドスレッドで起動して即リターンする（二重起動はロックでガード）。
    """
    if not _weekly_retrain_lock.acquire(blocking=False):
        logger.warning("[週次再学習] 既に実行中のため二重起動をスキップします")
        return

    def _body() -> None:
        try:
            _job_weekly_retrain_body()
        except Exception as exc:
            logger.error("[週次再学習] スレッド内例外: %s", exc, exc_info=True)
        finally:
            _weekly_retrain_lock.release()

    thread = threading.Thread(target=_body, name="weekly_retrain", daemon=True)
    thread.start()
    logger.info(
        "[週次再学習] バックグラウンドスレッドを起動しました (thread=%s)", thread.name
    )


def job_git_push() -> None:
    """月曜: GitHub 自動プッシュ"""
    logger.info("=== [Git プッシュ] 開始 ===")
    try:
        from src.ops.git_ops import weekly_auto_commit

        success = weekly_auto_commit()
        if success:
            _mark_job_done("job_git_push")
        logger.info("[Git プッシュ] %s", "成功" if success else "失敗")
    except Exception as e:
        logger.error("[Git プッシュ] 失敗: %s", e, exc_info=True)


def job_heartbeat() -> None:
    """毎時0分: Discord にハートビートを送信する（死活監視）"""
    from datetime import datetime

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    _send_discord(f"✅ UMALOGI alive ({now})")


def job_heartbeat_sns() -> None:
    """3時間おき: DISCORD_WEBHOOK_SNS へ定期生存報告を送信する（死活監視・つなぎ）。

    AntiCrow 等の本格リモート監視統合までの暫定。送信は src.ops.sns_publisher.send_heartbeat
    に委譲し、内部で例外を握りつぶすため、メインの予測・SNS 集客処理を一切ブロックしない。
    """
    try:
        from src.ops.sns_publisher import send_heartbeat

        ok = send_heartbeat()
        logger.info("[ハートビート(SNS)] 送信 %s", "成功" if ok else "スキップ/失敗")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[ハートビート(SNS)] 例外（無視して続行）: %s", exc)


# ================================================================
# TARGET frontier JV ウォッチドッグ
# ================================================================

_TARGET_JV_EXE: Path | None = None
_TARGET_JV_RESTART_COUNT: int = 0
_TARGET_JV_MAX_RESTARTS_PER_DAY: int = 5
_TARGET_JV_LAST_RESTART_DATE: str = ""


def _find_target_jv_exe() -> Path | None:
    """
    TARGET frontier JV 実行ファイルパスを決定する。
    優先順: 環境変数 TARGET_JV_PATH → 既知パス。
    """
    from dotenv import load_dotenv as _ldotenv

    _ldotenv(_ROOT / ".env", override=False)

    env_path = os.environ.get("TARGET_JV_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p

    known = [
        Path(r"C:\Program Files\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
        Path(r"C:\Program Files (x86)\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
        Path(r"C:\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
    ]
    for p in known:
        if p.exists():
            return p
    return None


def _is_target_jv_running() -> bool:
    """psutil で TargetFrontierJV.exe または JVLinkAgent.exe プロセスの生存を確認する。"""
    try:
        import psutil

        for proc in psutil.process_iter(["name"]):
            name = (proc.info.get("name") or "").lower()
            if "targetfrontierjv" in name or "jvlinkagent" in name:
                return True
    except Exception:
        pass
    return False


def _restart_target_jv(exe: Path) -> bool:
    """TARGET frontier JV を subprocess.Popen で再起動する。"""
    global _TARGET_JV_RESTART_COUNT, _TARGET_JV_LAST_RESTART_DATE

    today = date.today().isoformat()
    if _TARGET_JV_LAST_RESTART_DATE != today:
        _TARGET_JV_RESTART_COUNT = 0
        _TARGET_JV_LAST_RESTART_DATE = today

    if _TARGET_JV_RESTART_COUNT >= _TARGET_JV_MAX_RESTARTS_PER_DAY:
        logger.error(
            "[TARGETウォッチドッグ] 本日の再起動上限(%d回)に達しました。手動確認が必要です。",
            _TARGET_JV_MAX_RESTARTS_PER_DAY,
        )
        return False

    try:
        logger.warning("[TARGETウォッチドッグ] TARGET JV が停止 → 再起動: %s", exe)
        # SW_SHOWMINIMIZED: 最小化状態で起動 → フォーカス奪取・ブラウザ開きを抑制
        # 認証が必要な場合はタスクバーアイコンが点滅するため手動確認は可能
        _si = subprocess.STARTUPINFO()
        _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        _si.wShowWindow = 2  # SW_SHOWMINIMIZED
        subprocess.Popen(
            [str(exe)],
            cwd=str(exe.parent),
            creationflags=subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NEW_PROCESS_GROUP,
            startupinfo=_si,
        )
        _TARGET_JV_RESTART_COUNT += 1
        logger.info(
            "[TARGETウォッチドッグ] 再起動完了 (本日 %d/%d 回目)",
            _TARGET_JV_RESTART_COUNT,
            _TARGET_JV_MAX_RESTARTS_PER_DAY,
        )
        _send_discord(
            f"🔄 **[UMALOGI] TARGET frontier JV 自動再起動**\n"
            f"停止を検知したため自動再起動しました。\n"
            f"本日 {_TARGET_JV_RESTART_COUNT}/{_TARGET_JV_MAX_RESTARTS_PER_DAY} 回目\n"
            f"実行ファイル: `{exe}`"
        )
        return True
    except Exception as exc:
        logger.error("[TARGETウォッチドッグ] 再起動失敗: %s", exc)
        _send_discord(
            f"🚨 **[UMALOGI] TARGET frontier JV 再起動失敗**\n"
            f"エラー: `{exc}`\n"
            f"手動で起動してください: `{exe}`"
        )
        return False


def _target_jv_watchdog_loop() -> None:
    """
    TARGET frontier JV ウォッチドッグ（バックグラウンドスレッド）。

    60 秒ごとに TargetFrontierJV.exe の生存を確認し、
    停止を検知した場合は自動再起動する。

    平日 (月〜木) は競馬がないため監視のみ（再起動はしない）。
    金〜日は積極的に再起動する。
    """
    global _TARGET_JV_EXE

    # 起動から15秒後に開始（メインスレッドの初期化が完了するまで待機）
    time.sleep(15)

    _TARGET_JV_EXE = _find_target_jv_exe()
    if _TARGET_JV_EXE is None:
        logger.warning(
            "[TARGETウォッチドッグ] TARGET frontier JV が見つかりません。"
            "setup_target_autostart.py を実行してください。ウォッチドッグを無効化します。"
        )
        return

    logger.info("[TARGETウォッチドッグ] 起動: %s (60秒間隔)", _TARGET_JV_EXE)

    while True:
        try:
            time.sleep(60)

            if not _is_target_jv_running():
                today_wd = date.today().weekday()  # 0=月, 4=金, 5=土, 6=日
                is_racing_day = today_wd in (4, 5, 6)  # 金土日
                hour = datetime.now().hour

                if is_racing_day and 6 <= hour <= 22:
                    _restart_target_jv(_TARGET_JV_EXE)
                else:
                    logger.info(
                        "[TARGETウォッチドッグ] TARGET JV 停止を検知 (非レース日 or 深夜) → 再起動スキップ"
                    )
        except Exception as exc:
            logger.warning("[TARGETウォッチドッグ] ループ例外（続行）: %s", exc)


def job_weekend_batch_pre() -> None:
    """
    土日 07:00: 週末バッチ Pre フェーズ

    金曜夜の暫定予想をもとに以下を自動実行:
      1. note.com に下書き記事を保存（Playwright）
      2. ウマニティ コロシアムに暫定予想を投稿
      3. X（Twitter）に本日予想のお知らせツイート

    NOTE_EMAIL / NOTE_PASSWORD が未設定でも他のステップは続行する。
    """
    logger.info("=== [週末バッチ Pre] 開始 ===")
    rc = _run(
        _PY64 + ["scripts/weekend_batch.py", "--phase", "pre"],
        "週末バッチ-Pre",
        timeout=1800,
    )
    if rc == 0:
        _mark_job_done("job_weekend_batch_pre")
        logger.info("=== [週末バッチ Pre] 完了 ===")
    else:
        logger.error("[週末バッチ Pre] 失敗: rc=%d", rc)
        _send_discord(f"🚨 [UMALOGI] 週末バッチ Pre 失敗 (rc={rc})")


def job_weekend_batch_post() -> None:
    """
    土日 18:30: 週末バッチ Post フェーズ

    レース確定後の成績を集計して以下を自動実行:
      1. 的中証拠画像（Pillow）を生成
      2. X（Twitter）に本日 P&L 結果ツイート（画像付き）
      3. batch_runs テーブルにログ記録
      4. 日曜のみ: note.com 週次まとめ記事を docs/note_drafts/ に生成

    prediction_results が空の場合は自動スキップ。
    """
    logger.info("=== [週末バッチ Post] 開始 ===")
    rc = _run(
        _PY64 + ["scripts/weekend_batch.py", "--phase", "post"],
        "週末バッチ-Post",
        timeout=1800,
    )
    if rc == 0:
        _mark_job_done("job_weekend_batch_post")
        logger.info("=== [週末バッチ Post] 完了 ===")
    else:
        logger.error("[週末バッチ Post] 失敗: rc=%d", rc)
        _send_discord(f"🚨 [UMALOGI] 週末バッチ Post 失敗 (rc={rc})")

    # 日曜のみ: note.com 週次まとめ記事を生成（土日両日分の結果が揃った後）
    if date.today().weekday() == 6:  # 0=月, 6=日
        logger.info("[週末バッチ Post] note週次記事生成を開始")
        rc_note = _run(
            _PY64 + ["scripts/generate_weekly_note.py"],
            "note週次記事生成",
            timeout=120,
        )
        if rc_note == 0:
            logger.info("[週末バッチ Post] note週次記事生成完了")
        else:
            logger.warning(
                "[週末バッチ Post] note週次記事生成失敗(rc=%d) — 続行", rc_note
            )


def job_ab_report() -> None:
    """日曜 18:00: V1 vs V2 週次 A/B 成績比較レポートを Discord ab_test チャンネルへ送信。"""
    logger.info("=== [A/B レポート] 開始 ===")
    rc = _run(
        _PY64 + ["scripts/generate_ab_report.py", "--days", "7"],
        "A/B-レポート",
        timeout=120,
    )
    if rc == 0:
        logger.info("=== [A/B レポート] 完了 ===")
    else:
        logger.warning("[A/B レポート] 失敗(rc=%d) — 続行", rc)


def _notify_note_article_summary(
    recommended: list,
    target_date: str,  # YYYYMMDD
    discord_sent: bool,
) -> None:
    """
    厳選レース一覧を Discord system チャンネルへ Embed で送信する。

    Args:
        recommended: note_generator.select_recommended_races() の返り値
        target_date: YYYYMMDD 形式
        discord_sent: note_draft チャンネルへの記事転送成否
    """
    import sqlite3 as _sqlite3
    from datetime import datetime as _dt

    display_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
    now_str = _dt.now().strftime("%H:%M")
    n_races = len(recommended)

    db_path = _ROOT / "data" / "umalogi.db"
    try:
        conn = _sqlite3.connect(str(db_path))
        conn.row_factory = _sqlite3.Row
    except Exception:
        conn = None

    race_lines: list[str] = []
    for i, sc in enumerate(recommended, 1):
        race_id = sc["race_id"]
        venue, race_no, race_name = "—", 0, "—"
        if conn:
            row = conn.execute(
                "SELECT venue, race_number, race_name FROM races WHERE race_id=?",
                (race_id,),
            ).fetchone()
            if row:
                venue = row["venue"]
                race_no = row["race_number"]
                race_name = row["race_name"] or f"R{race_no}"

        stars = "⭐" * min(sc["consensus"], 4)
        score = sc["score"]
        best_ev = max(sc.get("alpha_ev", 0.0), sc.get("manji_ev", 0.0), 0.0)
        ev_str = f"  EV={best_ev:.2f}" if best_ev > 0 else ""
        race_lines.append(
            f"{i}. {stars} **{venue}{race_no}R**「{race_name}」 {score:.0f}pt{ev_str}"
        )

    if conn:
        conn.close()

    race_text = "\n".join(race_lines) or "（推奨レースなし）"
    discord_status = (
        "✅ note_draft チャンネルへ転送済み" if discord_sent else "⚠️ Discord 転送失敗"
    )
    out_file = f"`outputs/note/{target_date}_recommendations.md`"

    embed = {
        "title": f"🏇 本日AI厳選記事 完成 — {display_date}",
        "color": 0x00C8FF,
        "description": f"4モデル合意スコアで **{n_races}** レースを厳選しました",
        "fields": [
            {"name": "🎯 厳選レース一覧", "value": race_text, "inline": False},
            {"name": "📨 Discord 転送", "value": discord_status, "inline": True},
            {"name": "📁 ファイル", "value": out_file, "inline": True},
        ],
        "footer": {
            "text": f"UMALOGI AI  |  {now_str} 生成  |  noteは下書き一覧から手動公開",
        },
    }
    _send_discord_embed([embed])


def job_note_daily_article() -> None:
    """
    土日 10:30: 直前予想データが揃った時点でAI厳選記事を生成し
    Discord + (オプション) note.com 下書きへ自動配信する。

    Step 1: note_generator.generate() で Markdown 記事を生成・ファイル保存
    Step 2: Discord note_draft チャンネルへ記事全文をチャンク転送
    Step 3: Discord system チャンネルへ厳選レース Embed サマリーを送信
    Step 4: NOTE_DRAFT_AUTO_POST=1 かつ .note_session.json が存在する場合のみ
            note.com 下書きを Playwright で自動保存（未設定時は安全スキップ）

    環境変数:
        NOTE_DRAFT_AUTO_POST : '1' または 'true' で自動投稿を有効化（デフォルト無効）
        NOTE_EMAIL           : note.com メールアドレス（Playwright ログイン用）
        NOTE_PASSWORD        : note.com パスワード（Playwright ログイン用）
    """
    logger.info("=== [note記事生成] 開始 ===")
    target_date = date.today().strftime("%Y%m%d")
    db_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
    title_line = f"🏇【{db_date}】AI厳選レース予想｜4モデル全弾発射"

    # ── Step 1: 記事生成 ────────────────────────────────────────────
    try:
        from src.ops import note_generator as _ng

        md = _ng.generate(date_str=target_date, top_n=5)
        logger.info("[note記事生成] 記事生成完了: %d 文字", len(md))
    except Exception as exc:
        logger.error("[note記事生成] 記事生成失敗: %s", exc, exc_info=True)
        _send_discord(f"🚨 [note記事生成] 失敗: {exc}")
        return

    # 厳選レース情報を notification 用に取得
    recommended: list = []
    try:
        from src.ops import note_generator as _ng2

        _conn = _ng2._db()
        try:
            recommended = _ng2.select_recommended_races(_conn, db_date)
        finally:
            _conn.close()
    except Exception as exc:
        logger.warning("[note記事生成] 推奨レース取得失敗（続行）: %s", exc)

    # ── Step 2: Discord note_draft チャンネルへ記事全文を転送 ──────
    discord_sent = False
    try:
        from src.notification.router import NotificationRouter

        router = NotificationRouter()
        router.send_note_draft(title_line, md)
        discord_sent = True
        logger.info("[note記事生成] Discord note_draft チャンネルへ転送完了")
    except Exception as exc:
        logger.warning("[note記事生成] Discord 転送失敗（続行）: %s", exc)

    # ── Step 3: Discord system チャンネルへ Embed サマリー送信 ─────
    try:
        _notify_note_article_summary(recommended, target_date, discord_sent)
    except Exception as exc:
        logger.warning("[note記事生成] Embed 通知失敗（続行）: %s", exc)

    # ── Step 4: note.com 下書き自動保存（オプション）───────────────
    auto_post = os.environ.get("NOTE_DRAFT_AUTO_POST", "").lower() in ("1", "true")
    session_path = _ROOT / ".note_session.json"
    note_com_label = "スキップ（NOTE_DRAFT_AUTO_POST 未設定）"

    if not auto_post:
        logger.info(
            "[note記事生成] NOTE_DRAFT_AUTO_POST 未設定 → note.com 投稿スキップ。"
            "自動投稿を有効にするには .env に NOTE_DRAFT_AUTO_POST=1 を追加してください。"
        )
    elif not session_path.exists():
        note_com_label = "スキップ（セッション未作成）"
        logger.warning(
            "[note記事生成] .note_session.json が見つかりません。\n"
            "  → py scripts/post_weekly_note_draft.py --login-only を実行して"
            "セッションを作成してください。"
        )
        _send_discord(
            "⚠️ [note記事生成] note.com セッションがありません。\n"
            "`py scripts/post_weekly_note_draft.py --login-only` を実行して\n"
            "セッションを作成すると次回から自動投稿できます。"
        )
    else:
        try:
            from src.ops.note_draft_publisher import save_draft

            ok = save_draft(
                title=title_line,
                body=md,
                tags=["競馬", "AI予想", "UMALOGI", "JRA", "期待値", "競馬AI"],
                headless=True,
            )
            if ok:
                note_com_label = "✅ 下書き保存完了"
                logger.info("[note記事生成] note.com 下書き保存完了")
                _send_discord(
                    f"📝 [note記事生成] note.com 下書き保存完了: {title_line}"
                )
            else:
                note_com_label = "❌ 保存失敗"
                logger.error("[note記事生成] note.com 下書き保存失敗")
                _send_discord(
                    "❌ [note記事生成] note.com 下書き保存失敗。\n"
                    "outputs/debug/ のスクリーンショットを確認してください。\n"
                    "セッション期限切れの場合: "
                    "`py scripts/post_weekly_note_draft.py --login-only`"
                )
        except ImportError:
            note_com_label = "スキップ（playwright 未インストール）"
            logger.warning(
                "[note記事生成] playwright 未インストール → note.com 投稿スキップ。"
                "pip install playwright && playwright install chromium で有効化できます。"
            )
        except Exception as exc:
            note_com_label = f"エラー: {type(exc).__name__}"
            logger.error("[note記事生成] note.com 投稿例外: %s", exc, exc_info=True)
            _send_discord(f"🚨 [note記事生成] note.com 投稿例外: {exc}")

    _mark_job_done("job_note_daily_article")
    logger.info("=== [note記事生成] 完了 (note.com: %s) ===", note_com_label)


def job_daily_backup() -> None:
    """毎日23:00: DB を data/backups/ に日付付きでバックアップ（5世代ローテーション）"""
    try:
        from src.ops.backup import backup_db

        path = backup_db()
        logger.info("DB バックアップ完了: %s", path)
    except Exception as exc:
        logger.error("DB バックアップ失敗: %s", exc)


def job_weekly_backup() -> None:
    """毎週月曜 05:00: DB+ログ+モデルを ZIP 圧縮して data/backups/ に退避（12世代保持）"""
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "weekly_backup", Path(__file__).parent / "weekly_backup.py"
        )
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        path = mod.run_backup()
        logger.info("週次バックアップ完了: %s", path)
    except Exception as exc:
        logger.error("週次バックアップ失敗: %s", exc)


def job_nightly_maintenance() -> None:
    """毎日 04:00: 事前バックアップ → VACUUM/ANALYZE で DB を最適化する深夜保守ジョブ。

    VACUUM は DB ファイル全体を再構築する大規模操作のため、CLAUDE.md 条項4 に従い
    必ず先に backup_db() でホットバックアップを取得してから最適化を実行する。
    レース取得・予想・Hit Flash と干渉しない深夜帯（04:00）に実行する。
    """
    try:
        from src.ops.backup import backup_db
        from src.ops.db_optimize import optimize_db

        # ① 事前バックアップ（条項4: DB 大規模操作前の必須バックアップ）
        backup_path = backup_db()
        logger.info("[深夜保守] 事前バックアップ完了: %s", backup_path)

        # ② VACUUM + ANALYZE による最適化
        result = optimize_db()
        saved_mb = int(result["saved_bytes"]) / 1_048_576
        logger.info(
            "[深夜保守] DB 最適化完了: VACUUM=%s ANALYZE=%s 削減=%.1fMB %.1f秒",
            result["vacuumed"],
            result["analyzed"],
            saved_mb,
            result["elapsed_sec"],
        )
        if not result["ok"]:
            _send_discord(
                "⚠️ [深夜保守] DB 最適化中に VACUUM/ANALYZE エラーが発生しました"
                "（バックアップは取得済み）。ログを確認してください。"
            )
    except Exception as exc:
        logger.error("[深夜保守] 失敗: %s", exc)
        _send_discord(f"🚨 [深夜保守] バックアップ/DB最適化ジョブが例外終了: {exc}")
    finally:
        _mark_job_done("job_nightly_maintenance")


def job_training_hillwork_scrape() -> None:
    """木・金曜 20:00: 今週末レースの坂路調教データを netkeiba から取得する。"""
    try:
        import sqlite3
        from datetime import date, timedelta

        conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))
        # 今週末（土・日）の race_id を取得
        today = date.today()
        days_until_sat = (5 - today.weekday()) % 7
        days_until_sun = (6 - today.weekday()) % 7
        # 木曜実行なら +2(土)/+3(日)、金曜実行なら +1(土)/+2(日)
        target_dates = [
            (today + timedelta(days=days_until_sat)).isoformat(),
            (today + timedelta(days=days_until_sun)).isoformat(),
        ]
        race_ids: list[str] = []
        for d in target_dates:
            rows = conn.execute(
                "SELECT DISTINCT race_id FROM races WHERE date = ?", (d,)
            ).fetchall()
            race_ids.extend(r[0] for r in rows)
        conn.close()

        if not race_ids:
            logger.info("training_hillwork_scrape: 対象レースなし")
            return

        logger.info(
            "training_hillwork_scrape: %d レースの坂路データ取得開始", len(race_ids)
        )
        _run(
            _PY64
            + [
                "scripts/backfill_training_hillwork.py",
                "--days",
                "3",
            ],
            "坂路調教スクレイプ",
            timeout=600,
        )
    except Exception as exc:
        logger.error("training_hillwork_scrape 失敗: %s", exc)


def job_record_odds_timeseries() -> None:
    """10分毎: 発走前レースのオッズを realtime_odds へ実取得（時系列蓄積・8:00〜17:59）。

    P0-1: 単一ソース統一。旧「odds_timeseries へコピー」を廃し、
    record_odds_timeseries.capture_today_odds() が発走前ウィンドウのレースに対し
    fetch_and_save_odds を実行して realtime_odds に新スナップショットを追記する。
    これで odds_drift / odds_momentum が「最低2点」を確実に得る。
    """
    now = datetime.now()
    if not (8 <= now.hour < 18):
        return
    try:
        import subprocess

        subprocess.run(
            [_PY64[0], "scripts/record_odds_timeseries.py"],
            timeout=180,
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("オッズ時系列取得失敗: %s", exc)


def job_prerace_15min_alert() -> None:
    """毎分: 発走15分前の EV 激熱レースを Discord ev_alert へ通知する（8:30〜16:30 のみ）。

    土日にレースがある場合、各レースの発走14〜16分前の1回だけ通知する。
    in-memory の既通知セットで重複送信を防ぐ。
    EV 閾値は環境変数 PRERACE_ALERT_EV_THRESHOLD（デフォルト 1.2）で調整可能。
    """
    now = datetime.now()
    # 朝8:30〜夕方16:30 の間のみ実行（最終R12 = 15:30 発走 + バッファ）
    if not (8 * 60 + 30 <= now.hour * 60 + now.minute <= 16 * 60 + 30):
        return
    try:
        from src.notification.prerace_alert import check_and_send_prerace_alerts

        count = check_and_send_prerace_alerts()
        if count > 0:
            logger.info("[発走前アラート] %d件の通知を送信しました", count)
    except Exception as exc:
        logger.warning("[発走前アラート] 実行エラー（続行）: %s", exc)


def job_segment_analysis() -> None:
    """月曜07:30: セグメント別ROI分析を実行しDiscordに送信する。"""
    try:
        from scripts.segment_analysis import analyze_by_segment, _print_segment_table
        import io
        import sys as _sys

        buf = io.StringIO()
        _orig = _sys.stdout
        _sys.stdout = buf

        try:
            for group in ["bet_type", "venue", "condition", "model"]:
                rows = analyze_by_segment(
                    db_path="data/umalogi.db", group_by=group, min_bets=30
                )
                _print_segment_table(rows, group)
        finally:
            _sys.stdout = _orig

        report = buf.getvalue()

        # Discord に送信（1800字ずつ分割）
        chunks = [report[i : i + 1800] for i in range(0, len(report), 1800)]
        for chunk in chunks:
            _send_discord(f"📊 週次セグメントROI分析\n```\n{chunk}\n```")

        logger.info("job_segment_analysis: 完了 %d文字", len(report))
    except Exception as exc:  # noqa: BLE001
        logger.exception("job_segment_analysis 失敗: %s", exc)
        _send_discord(f"⚠️ segment_analysis 失敗: {exc}")


# ================================================================
# スケジューラー本体
# ================================================================


def job_health_report() -> None:
    """土日夕方: 日次ヘルスレポートを Discord #system へ送信する（W-058・可観測性）。

    予想カバー率・オッズ時系列健全性（最低2点取得率）・結果欠損・通知エラーを集計し、
    劣化を人間が即座に検知できるようにする。
    """
    logger.info("=== [ヘルスレポート] 開始 ===")
    try:
        from src.ops.health_reporter import send_health_report

        report = send_health_report()
        logger.info(
            "[ヘルスレポート] 送信完了: 予想 %d/%d 直前 %d オッズ2点+ %d 結果欠損 %d 通知err %d",
            report.n_predicted,
            report.n_races,
            report.n_chokuzen,
            report.n_odds_ge2,
            report.n_results_missing,
            report.n_discord_errors,
        )
    except Exception as exc:
        logger.error("[ヘルスレポート] 失敗: %s", exc, exc_info=True)
    _mark_job_done("job_health_report")


def register_schedules() -> None:
    """全ジョブをスケジュールに登録する。"""
    if not _SCHEDULE_AVAILABLE:
        raise RuntimeError("schedule ライブラリが必要です: pip install schedule")

    # 金曜夜: JVLink同期(32bit) → 暫定予想(64bit) → Discord通知（土曜分）
    # 木・金曜夜: 今週末レースの坂路調教データをnetkeiba先行取得
    schedule.every().thursday.at("20:00").do(job_training_hillwork_scrape)
    schedule.every().friday.at("18:00").do(job_training_hillwork_scrape)

    schedule.every().friday.at("20:00").do(job_friday_sync)
    # 土曜夜: JVLink同期(32bit) → 暫定予想(64bit) → Discord通知（日曜分）
    schedule.every().saturday.at("20:00").do(job_friday_sync)

    # 土日朝: 週末バッチ Pre（note下書き + Umanity暫定投稿 + X告知）
    schedule.every().saturday.at("07:00").do(job_weekend_batch_pre)
    schedule.every().sunday.at("07:00").do(job_weekend_batch_pre)

    # 土日朝: 調教タイム同期（JVLink 32bit）
    schedule.every().saturday.at("07:30").do(job_morning_wood)
    schedule.every().sunday.at("07:30").do(job_morning_wood)

    # 土日朝: 当日全レース直前予想ループ起動（Discord通知まで自動）
    schedule.every().saturday.at("08:30").do(job_today_auto_runner)
    schedule.every().sunday.at("08:30").do(job_today_auto_runner)

    # 土日朝: WIN5 バッチ予測（9:00 — 金曜バッチ完了後・WIN5締切前）
    schedule.every().saturday.at("09:00").do(job_win5_prediction)
    schedule.every().sunday.at("09:00").do(job_win5_prediction)

    # 土日 10:30: note AI厳選記事生成 → Discord転送 → (オプション)note.com下書き保存
    schedule.every().saturday.at("10:30").do(job_note_daily_article)
    schedule.every().sunday.at("10:30").do(job_note_daily_article)

    # 土日昼: ウマニティ予想投稿（直前予想が揃う13:00以降）
    schedule.every().saturday.at("13:00").do(job_umanity_upload)
    schedule.every().sunday.at("13:00").do(job_umanity_upload)

    # 土日レース中: 確定済みレース結果を随時同期（OPT_STORED で確実取得）
    schedule.every().saturday.at("13:00").do(job_intraday_sync)
    schedule.every().saturday.at("15:30").do(job_intraday_sync)
    schedule.every().sunday.at("13:00").do(job_intraday_sync)
    schedule.every().sunday.at("15:30").do(job_intraday_sync)

    # 土日夕方: WIN5 確定結果取得（全レース確定後・17:15 に先行実行）
    schedule.every().saturday.at("17:15").do(job_win5_result_fetch)
    schedule.every().sunday.at("17:15").do(job_win5_result_fetch)

    # 土日夕方: 払戻確定後のレース後処理（全レース終了後・OPT_STORED で確実取得）
    schedule.every().saturday.at("17:30").do(job_post_race)
    schedule.every().sunday.at("17:30").do(job_post_race)

    # 土日 17:50 — 日次ヘルスレポート（post_race後・可観測性 W-058）
    schedule.every().saturday.at("17:50").do(job_health_report)
    schedule.every().sunday.at("17:50").do(job_health_report)

    # 日曜 18:00 — V1/V2 A/B 週次成績比較レポート（日曜のみ）
    schedule.every().sunday.at("18:00").do(job_ab_report)

    # 土日夜: 週末バッチ Post（P&L集計 + 的中カード + X結果報告）
    schedule.every().saturday.at("18:30").do(job_weekend_batch_post)
    schedule.every().sunday.at("18:30").do(job_weekend_batch_post)

    # 月曜: 卍較正器 週次再学習(03:00) → DB+ログ週次ZIPバックアップ → マスタ更新 → 全件再学習 → Git プッシュ
    schedule.every().monday.at("03:00").do(job_fit_manji_calibrator)
    schedule.every().monday.at("05:00").do(job_weekly_backup)
    schedule.every().monday.at("06:00").do(job_monday_masters)
    schedule.every().monday.at("07:00").do(job_weekly_retrain)

    # 月曜 07:30: セグメント別ROI分析
    schedule.every().monday.at("07:30").do(job_segment_analysis)

    schedule.every().monday.at("08:00").do(job_git_push)

    # 月曜 08:30 — 直近28日実績サマリーを Discord へ自動送信
    schedule.every().monday.at("08:30").do(
        lambda: _run(
            _PY64 + ["scripts/generate_performance_report.py", "--days", "28"],
            "実績レポート",
            timeout=120,
        )
    )

    # 毎時0分: 死活監視ハートビート → Discord（システムチャンネル）
    schedule.every().hour.at(":00").do(job_heartbeat)

    # 3時間おき: 定期生存報告 → Discord 集客チャンネル(DISCORD_WEBHOOK_SNS)
    # 完全無人運用のつなぎ（AntiCrow 統合までの暫定）。非ブロッキング送信。
    schedule.every(3).hours.do(job_heartbeat_sns)

    # 毎日23:00: DB バックアップ（5世代ローテーション）
    schedule.every().day.at("23:00").do(job_daily_backup)

    # 毎日04:00: 深夜保守（事前バックアップ → VACUUM/ANALYZE で DB 最適化）
    schedule.every().day.at("04:00").do(job_nightly_maintenance)

    # 10分毎: 発走前レースのオッズを realtime_odds へ実取得（時系列蓄積・8:00〜17:59 のみ）
    schedule.every(10).minutes.do(job_record_odds_timeseries)

    # 毎分: 発走15分前アラート（8:30〜16:30 のみ実際に送信）
    schedule.every(1).minutes.do(job_prerace_15min_alert)

    logger.info("スケジュール登録完了: %d ジョブ", len(schedule.jobs))
    for job in schedule.jobs:
        logger.info("  %s", job)


def run_daemon() -> None:
    """スケジューラーをデーモンとして常駐させる。Ctrl+C で終了。"""
    register_schedules()

    # JVLink ダイアログ自動突破ハンドラーをバックグラウンドスレッドで起動
    # JVLink の設定/セットアップ画面が出現したら 0.3 秒以内に自動クリックして消去する。
    # 既存の 10 秒タイムアウト → netkeiba フォールバックと共存する二重安全網。
    try:
        from src.ops.jvlink_dialog_handler import start_dialog_handler

        start_dialog_handler(interval=0.3)
        logger.info(
            "[JVLinkダイアログハンドラー] バックグラウンドスレッド起動済み (0.3秒間隔)"
        )
    except Exception as _dh_exc:
        logger.warning("[JVLinkダイアログハンドラー] 起動失敗（続行）: %s", _dh_exc)

    # TARGET JV ウォッチドッグをバックグラウンドスレッドで起動
    watchdog_thread = threading.Thread(
        target=_target_jv_watchdog_loop,
        name="target_jv_watchdog",
        daemon=True,
    )
    watchdog_thread.start()
    logger.info("[TARGETウォッチドッグ] バックグラウンドスレッド起動済み")

    # 起動時リカバリー: 取りこぼしジョブを検出して即実行（PC 再起動・スリープ復帰対応）
    _recover_missed_jobs(_JOB_MAP_FULL)

    _send_discord(
        f"🤖 **[UMALOGI] スケジューラー起動**\n"
        f"起動時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"登録ジョブ: {len(schedule.jobs)} 件\n"
        f"次回実行: {schedule.next_run()}"
    )

    logger.info("UMA-LOGI AI スケジューラー起動 — Ctrl+C で終了")
    try:
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                logger.critical("スケジューラー未処理例外: %s", e, exc_info=True)
                _send_discord(
                    f"🚨 [UMALOGI] スケジューラー例外 — デーモンは継続中\n`{type(e).__name__}: {e}`"
                )
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("スケジューラー停止")
        _send_discord("🛑 [UMALOGI] スケジューラーが手動停止されました")


# ================================================================
# CLI
# ================================================================

# リカバリー用フルマップ（関数名 → 関数オブジェクト）
_JOB_MAP_FULL: dict[str, object] = {
    "job_friday_sync": job_friday_sync,
    "job_training_hillwork_scrape": job_training_hillwork_scrape,
    "job_morning_wood": job_morning_wood,
    "job_weekend_batch_pre": job_weekend_batch_pre,
    "job_today_auto_runner": job_today_auto_runner,
    "job_win5_prediction": job_win5_prediction,
    "job_note_daily_article": job_note_daily_article,
    "job_win5_result_fetch": job_win5_result_fetch,
    "job_post_race": job_post_race,
    "job_ab_report": job_ab_report,
    "job_weekend_batch_post": job_weekend_batch_post,
    "job_fit_manji_calibrator": job_fit_manji_calibrator,
    "job_monday_masters": job_monday_masters,
    "job_weekly_retrain": job_weekly_retrain,
    "job_segment_analysis": job_segment_analysis,
    "job_git_push": job_git_push,
}

# CLI --run-now 用マップ（短縮名 → 関数）
_JOB_MAP: dict[str, object] = {
    "friday": job_friday_sync,
    "training_hillwork": job_training_hillwork_scrape,
    "wood": job_morning_wood,
    "batch_pre": job_weekend_batch_pre,
    "batch_post": job_weekend_batch_post,
    "win5": job_win5_prediction,
    "win5_result": job_win5_result_fetch,
    "note_article": job_note_daily_article,
    "umanity": job_umanity_upload,
    "auto_runner": job_today_auto_runner,
    "intraday_sync": job_intraday_sync,
    "post_race": job_post_race,
    "ab_report": job_ab_report,
    "masters": job_monday_masters,
    "retrain": job_weekly_retrain,
    "fit_calibrator": job_fit_manji_calibrator,
    "git": job_git_push,
    "prerace_alert": job_prerace_15min_alert,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="UMA-LOGI AI スケジューラー")
    parser.add_argument(
        "--run-now",
        metavar="JOB",
        choices=list(_JOB_MAP.keys()),
        help=f"即時実行するジョブ: {list(_JOB_MAP.keys())}",
    )
    parser.add_argument(
        "--date", help="post_race / intraday_sync ジョブの対象日 YYYY/MM/DD"
    )
    args = parser.parse_args()

    if args.run_now:
        logger.info("即時実行: %s", args.run_now)
        fn = _JOB_MAP[args.run_now]
        if args.run_now in ("post_race", "intraday_sync") and args.date:
            fn(args.date)  # type: ignore[call-arg]
        elif args.run_now == "auto_runner":
            fn()  # type: ignore[call-arg]
            # バックグラウンドスレッドが終わるまで待機
            logger.info("直前予想ループ実行中… Ctrl+C で中断")
            try:
                while threading.active_count() > 1:
                    time.sleep(5)
            except KeyboardInterrupt:
                logger.info("中断")
        else:
            fn()  # type: ignore[call-arg]
    else:
        run_daemon()


if __name__ == "__main__":
    main()
