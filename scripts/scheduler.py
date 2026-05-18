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
    "job_friday_sync":        16,  # Fri 20:00 → Sat 12:00 まで
    "job_morning_wood":        4,  # 07:30 → 11:30 まで
    "job_weekend_batch_pre":   4,  # 07:00 → 11:00 まで
    "job_today_auto_runner":   3,  # 08:30 → 11:30 まで
    "job_win5_prediction":     2,  # 09:00 → 11:00 まで
    "job_win5_result_fetch":   4,  # 17:15 → 21:15 まで
    "job_post_race":           4,  # 17:30 → 21:30 まで
    "job_weekend_batch_post":  4,  # 18:30 → 22:30 まで
    "job_monday_masters":     12,  # 06:00 → 18:00 まで
    "job_weekly_retrain":     12,  # 07:00 → 19:00 まで
    "job_git_push":           12,  # 08:00 → 20:00 まで
}

# 各ジョブのスケジュール定義 (weekday_int: 0=月…6=日, hour, minute)
_JOB_SCHEDULES: dict[str, list[tuple[int, int, int]]] = {
    "job_friday_sync":        [(4, 20,  0)],
    "job_morning_wood":       [(5,  7, 30), (6,  7, 30)],
    "job_weekend_batch_pre":  [(5,  7,  0), (6,  7,  0)],
    "job_today_auto_runner":  [(5,  8, 30), (6,  8, 30)],
    "job_win5_prediction":    [(5,  9,  0), (6,  9,  0)],
    "job_win5_result_fetch":  [(5, 17, 15), (6, 17, 15)],
    "job_post_race":          [(5, 17, 30), (6, 17, 30)],
    "job_weekend_batch_post": [(5, 18, 30), (6, 18, 30)],
    "job_monday_masters":     [(0,  6,  0)],
    "job_weekly_retrain":     [(0,  7,  0)],
    "job_git_push":           [(0,  8,  0)],
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
    """
    state = _load_job_state()
    now = datetime.now()
    weekday = now.weekday()

    for job_name, schedules in _JOB_SCHEDULES.items():
        fn = job_map.get(job_name)
        if fn is None:
            continue

        for wd, h, m in schedules:
            if wd != weekday:
                continue

            scheduled_today = now.replace(hour=h, minute=m, second=0, microsecond=0)
            last_run_str = state.get(job_name)
            last_run: datetime | None = None
            if last_run_str:
                try:
                    last_run = datetime.fromisoformat(last_run_str)
                except ValueError:
                    pass

            catchup = _CATCHUP_HOURS.get(job_name, 4)
            if _should_recover(last_run, scheduled_today, now, catchup):
                logger.warning(
                    "[リカバリー] %s は %s に実行すべきでしたが取りこぼしを検出 → 今すぐ実行",
                    job_name,
                    scheduled_today.strftime("%H:%M"),
                )
                _send_discord(
                    f"⚠️ [UMALOGI] ジョブ取りこぼしをリカバリー: `{job_name}` "
                    f"（予定 {scheduled_today.strftime('%H:%M')} → 今実行）"
                )
                try:
                    fn()  # type: ignore[operator]
                    _mark_job_done(job_name)
                except Exception as exc:
                    logger.error("[リカバリー] %s 実行失敗: %s", job_name, exc)


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


# JVInit は失敗時に3回リトライ (3秒 × 3 = 9秒) するため、
# タイムアウトは 30秒以上を確保しないとリトライ完了前にKillされる。
# 旧値 10秒 は JVInit 失敗リトライ中にタイムアウトし GUI_BLOCKED 誤判定を招いていた。
_JVLINK_STARTUP_TIMEOUT = 30  # JVLink初期化タイムアウト秒数（GUIダイアログ検出用）


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
        )
    except Exception as exc:
        logger.error("[%s] 起動失敗: %s", label, exc)
        return -1

    ready_event  = _threading.Event()
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
    """Discord Webhook にテキストメッセージを送信する。"""
    try:
        import requests as _req
    except ImportError:
        logger.warning("requests 未インストール — Discord 通知スキップ")
        return

    url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL 未設定 — Discord 通知スキップ")
        return
    try:
        resp = _req.post(url, json={"content": text}, timeout=10)
        resp.raise_for_status()
        logger.info("Discord 送信完了: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord 送信失敗: %s", exc)


def _send_discord_embed(embeds: list[dict]) -> None:
    """
    Discord Webhook に Embed メッセージを送信する。

    Discord Embed 仕様: https://discord.com/developers/docs/resources/message#embed-object
    color は 0xRRGGBB の整数値（例: シアン = 0x00C8FF = 52479）。
    """
    try:
        import requests as _req
    except ImportError:
        logger.warning("requests 未インストール — Discord 通知スキップ")
        return

    url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL 未設定 — Discord 通知スキップ")
        return
    try:
        resp = _req.post(url, json={"embeds": embeds}, timeout=10)
        resp.raise_for_status()
        logger.info("Discord Embed 送信完了: HTTP %d", resp.status_code)
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
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "friday"], "JVLink-RACE")
    if rc == -2:
        _netkeiba_fallback_entries(target_yyyymmdd, "JVLink-RACE")
        errors.append("JVLink RACE 同期 GUI_BLOCKED → Netkeibaフォールバック実施")
    elif rc != 0:
        errors.append(f"JVLink RACE 同期失敗(rc={rc})")

    # ── Step 2: JVLink WOOD 同期（32bit 必須）───────────────────
    rc = _run_jvlink(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD")
    if rc == -2:
        logger.warning("[金曜バッチ] JVLink WOOD GUI_BLOCKED — 調教タイムは取得不可（続行）")
        errors.append("JVLink WOOD 同期 GUI_BLOCKED（調教タイムは暫定予想でも使用可）")
    elif rc != 0:
        errors.append(f"JVLink WOOD 同期失敗(rc={rc})")

    # ── Step 3: JVLink マスタ差分更新（32bit 必須）──────────────
    rc = _run_jvlink(
        _PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters"
    )
    if rc == -2:
        logger.warning("[金曜バッチ] JVLink Masters GUI_BLOCKED — マスタ更新スキップ（続行）")
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
        logger.warning("[朝調教同期] JVLink GUI_BLOCKED — 調教タイムは取得不可。setup_jvlink.py を実行してください。")
    elif rc != 0:
        logger.error("[朝調教同期] 失敗: rc=%d", rc)
    else:
        _mark_job_done("job_morning_wood")
        logger.info("=== [朝調教同期] 完了 ===")


_auto_runner_lock = threading.Lock()


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
                logger.info("=== [直前予想ループ] バックグラウンドスレッド開始 (attempt=%d) ===", attempt)
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
                    logger.warning("[直前予想ループ] 異常終了 rc=%d だが %d時以降のため再起動しません", rc, now_h)
                    _send_discord(
                        f"⚠️【直前予想ループ】異常終了 rc={rc}。レース終了後のため再起動不要。"
                    )
                    break

                logger.error("[直前予想ループ] 異常終了: rc=%d — 30秒後に再起動します (attempt=%d)", rc, attempt)
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
                result["winning_numbers"], result.get("payout", 0),
            )
            _mark_job_done("job_win5_result_fetch")
        else:
            logger.warning("[WIN5結果取得] 結果なし: %s", result.get("reason", "unknown"))
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


def job_post_race(target_date: str | None = None) -> None:
    """
    土日夕方: レース確定後の払戻同期(32bit) + 評価 + 通知 + 増分学習 + バックアップ
    """
    if target_date is None:
        target_date = date.today().strftime("%Y/%m/%d")
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
            results = batch_evaluate_date(conn, iso_date, notify=True)
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
        import subprocess as _sp, sys as _sys

        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen], check=True, timeout=120)
        logger.info("[ダッシュボード] JSON 再生成完了")
    except Exception as gen_exc:
        logger.warning("[ダッシュボード] JSON 再生成失敗: %s", gen_exc)

    _mark_job_done("job_post_race")
    logger.info("=== [レース後処理] %s 終了 ===", target_date)


def job_monday_masters() -> None:
    """月曜: マスタデータ差分更新（32bit subprocess）"""
    logger.info("=== [マスタ更新] 開始 ===")
    rc = _run_jvlink(
        _PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters月曜"
    )
    if rc == -2:
        logger.warning("[マスタ更新] JVLink GUI_BLOCKED — setup_jvlink.py を実行してください。")
    elif rc != 0:
        logger.error("[マスタ更新] 失敗: rc=%d", rc)
    else:
        _mark_job_done("job_monday_masters")
        logger.info("=== [マスタ更新] 完了 ===")


def job_weekly_retrain() -> None:
    """月曜: 全件再学習 + summary.json 再生成（64bit）"""
    logger.info("=== [週次再学習] 開始 ===")
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import weekly_retrain

        conn = init_db()
        try:
            result = weekly_retrain(conn)
            logger.info("[週次再学習] 完了: %s", result)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[週次再学習] 失敗: %s", e, exc_info=True)

    # 再学習後に summary / financial.json を更新
    try:
        import subprocess as _sp, sys as _sys

        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen, "--no-detail"], check=True, timeout=120)
        logger.info("[週次再学習] ダッシュボード JSON 更新完了")
    except Exception as gen_exc:
        logger.warning("[週次再学習] JSON 更新失敗: %s", gen_exc)
    _mark_job_done("job_weekly_retrain")


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
        subprocess.Popen(
            [str(exe)],
            cwd=str(exe.parent),
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        _TARGET_JV_RESTART_COUNT += 1
        logger.info(
            "[TARGETウォッチドッグ] 再起動完了 (本日 %d/%d 回目)",
            _TARGET_JV_RESTART_COUNT, _TARGET_JV_MAX_RESTARTS_PER_DAY,
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


# ================================================================
# スケジューラー本体
# ================================================================


def register_schedules() -> None:
    """全ジョブをスケジュールに登録する。"""
    if not _SCHEDULE_AVAILABLE:
        raise RuntimeError("schedule ライブラリが必要です: pip install schedule")

    # 金曜夜: JVLink同期(32bit) → 暫定予想(64bit) → Discord通知
    schedule.every().friday.at("20:00").do(job_friday_sync)

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

    # 土日夜: 週末バッチ Post（P&L集計 + 的中カード + X結果報告）
    schedule.every().saturday.at("18:30").do(job_weekend_batch_post)
    schedule.every().sunday.at("18:30").do(job_weekend_batch_post)

    # 月曜: DB+ログ週次ZIPバックアップ → マスタ更新 → 全件再学習 → Git プッシュ
    schedule.every().monday.at("05:00").do(job_weekly_backup)
    schedule.every().monday.at("06:00").do(job_monday_masters)
    schedule.every().monday.at("07:00").do(job_weekly_retrain)
    schedule.every().monday.at("08:00").do(job_git_push)

    # 毎時0分: 死活監視ハートビート → Discord
    schedule.every().hour.at(":00").do(job_heartbeat)

    # 毎日23:00: DB バックアップ（5世代ローテーション）
    schedule.every().day.at("23:00").do(job_daily_backup)

    logger.info("スケジュール登録完了: %d ジョブ", len(schedule.jobs))
    for job in schedule.jobs:
        logger.info("  %s", job)


def run_daemon() -> None:
    """スケジューラーをデーモンとして常駐させる。Ctrl+C で終了。"""
    register_schedules()

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
                logger.critical(
                    "スケジューラー未処理例外: %s", e, exc_info=True
                )
                _send_discord(
                    f"🚨 [UMALOGI] スケジューラー例外 — デーモンは継続中\n"
                    f"`{type(e).__name__}: {e}`"
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
    "job_friday_sync":        job_friday_sync,
    "job_morning_wood":       job_morning_wood,
    "job_weekend_batch_pre":  job_weekend_batch_pre,
    "job_today_auto_runner":  job_today_auto_runner,
    "job_win5_prediction":    job_win5_prediction,
    "job_win5_result_fetch":  job_win5_result_fetch,
    "job_post_race":          job_post_race,
    "job_weekend_batch_post": job_weekend_batch_post,
    "job_monday_masters":     job_monday_masters,
    "job_weekly_retrain":     job_weekly_retrain,
    "job_git_push":           job_git_push,
}

# CLI --run-now 用マップ（短縮名 → 関数）
_JOB_MAP: dict[str, object] = {
    "friday":        job_friday_sync,
    "wood":          job_morning_wood,
    "batch_pre":     job_weekend_batch_pre,
    "batch_post":    job_weekend_batch_post,
    "win5":          job_win5_prediction,
    "win5_result":   job_win5_result_fetch,
    "umanity":       job_umanity_upload,
    "auto_runner":   job_today_auto_runner,
    "intraday_sync": job_intraday_sync,
    "post_race":     job_post_race,
    "masters":       job_monday_masters,
    "retrain":       job_weekly_retrain,
    "git":           job_git_push,
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
