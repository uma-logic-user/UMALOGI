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
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)


def _send_discord(text: str, *, color: int | None = None) -> None:
    """システムチャンネルにメッセージを送信する。NotificationRouter 経由。"""
    try:
        from src.notification.router import NotificationRouter

        safe_text = text.replace("\x00", "").strip()
        NotificationRouter().send_system_text(safe_text)
    except Exception:
        pass


def _send_discord_race(text: str) -> None:
    """予想チャンネルにメッセージを送信する（買い目・結果・週次レポート用）。"""
    try:
        from src.notification.router import NotificationRouter

        safe_text = text.replace("\x00", "").strip()
        NotificationRouter().send_text(safe_text)
    except Exception:
        pass


def _is_umalogi_process(pid: int) -> bool:
    """psutil でプロセスの生存と同一性を厳密に検証する。

    以下のすべてを満たす場合のみ True を返す:
      1. 指定 PID のプロセスが OS 上で生存している
      2. そのプロセスが Python インタープリタを使用している
      3. コマンドライン引数に 'today_auto_runner' または 'scheduler' が含まれる

    これにより「PID が偶然再利用された別プロセス」を誤検知しない。
    """
    import psutil

    try:
        proc = psutil.Process(pid)
        if not proc.is_running():
            return False
        # コマンドライン引数を取得（アクセス権限不足の場合は NoSuchProcess 扱い）
        try:
            cmdline = proc.cmdline()
        except (psutil.AccessDenied, psutil.ZombieProcess):
            # 権限なし = 別ユーザーの別プロセス → ゾンビ扱い
            return False
        cmdline_str = " ".join(cmdline).lower()
        # Python プロセスで UMALOGI 関連スクリプトを実行しているか確認
        is_python = any(p in cmdline_str for p in ("python", "py.exe", "python3"))
        is_umalogi = any(
            s in cmdline_str for s in ("today_auto_runner", "scheduler", "umalogi")
        )
        return is_python and is_umalogi
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return False


def _check_single_instance() -> bool:
    """重複起動防止: 既存の同プロセスが動作中なら False を返す。

    psutil で PID の生存と UMALOGI スクリプトとしての同一性を厳密に検証する。
    ゾンビ PID（死亡プロセス or 別プロセスが PID 再利用）の場合は
    ロックファイルを自動削除して起動を続行する自己修復ロジック。
    """
    if _LOCK_FILE.exists():
        pid_str = _LOCK_FILE.read_text(encoding="utf-8").strip()
        try:
            old_pid = int(pid_str)
        except ValueError:
            # PID ファイルが壊れている → ゾンビ扱い
            logger.warning("PID ファイルが不正です (%r) → 自動削除して続行", pid_str)
            _LOCK_FILE.unlink(missing_ok=True)
        else:
            if _is_umalogi_process(old_pid):
                # 本物の UMALOGI プロセスが生存中 → 重複起動を拒否
                return False
            else:
                # ゾンビ PID（死亡 or 別プロセスが PID 再利用）→ 自動削除
                logger.warning(
                    "ゾンビ PID 検出 (pid=%d): UMALOGI プロセスが見つかりません。"
                    "ロックファイルを自動削除して起動を続行します。",
                    old_pid,
                )
                _LOCK_FILE.unlink(missing_ok=True)

    _LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    return True


# ログは日次ローテーション + 7日保持（src/ops/logger.py）。肥大化によるディスク枯渇を防ぐ。
from src.ops.logger import setup_logging

# W-093: 生存ハートビート（watchdog がハング検知して強制再起動するための鼓動）。
from src.ops.heartbeat import write_heartbeat, clear_heartbeat

logger = setup_logging(
    "auto_runner",
    "auto_runner.log",
    fmt="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    use_rich=True,  # コンソールのみ rich 装飾（ファイル出力フォーマットは不変）
)

# 発走推定: R1 = 10:00 JST、以降 30 分間隔
_R1_HOUR = 10
_R1_MINUTE = 0
_INTERVAL_MIN = 30

# 夜間バッチの時刻
_EVENING_FETCH_HOUR = 20
_EVENING_FETCH_MINUTE = 0

# 翌朝ループ開始時刻
_MORNING_START_HOUR = 8
_MORNING_START_MINUTE = 30

# 再起動待機時間（秒）
_RESTART_WAIT_SEC = 30

# W-043: 日次損失サーキットブレーカー設定
# 当日の確定P&L損失がこの金額を超えたら、その日の prerace 実行を停止する。
# 環境変数 DAILY_LOSS_LIMIT_JPY（整数）で上書き可能。デフォルト ¥30,000
_DAILY_LOSS_LIMIT_JPY: int = int(os.getenv("DAILY_LOSS_LIMIT_JPY", "30000"))

# W-087: サーキットブレーカーの Soft Stop 化（2026-06-13 社長指令）
# True（既定）= CB 発動時も「予想の生成と DB 保存は継続」しアラート通知のみ行う。
#   データ蓄積・監視を止めないための Soft Stop。実弾発注は人間判断に委ねる。
# False = 旧来の Hard Stop（CB 発動時に prerace をスキップ＝予想を生成しない）。
# 環境変数 CIRCUIT_BREAKER_SOFT_STOP="0" で Hard Stop に戻せる。
_CB_SOFT_STOP: bool = os.getenv("CIRCUIT_BREAKER_SOFT_STOP", "1").strip() != "0"

# CB アラートの日次重複排除（発動後レース毎に Discord 連投するのを防ぐ）
_cb_alerted_dates: set[str] = set()

# W-044: セッション全体クラッシュ上限（連続/非連続問わず）
# _consecutive_errors は連続エラーのみカウント（成功でリセット）するが、
# _session_total_crashes はリセットしない。長期フラッピング障害で無限ループになるのを防ぐ。
# 環境変数 MAX_SESSION_CRASHES で上書き可能。デフォルト 50 回
_MAX_SESSION_CRASHES: int = int(os.getenv("MAX_SESSION_CRASHES", "50"))

# postrace 再試行設定（審議・写真判定: 最大40分対応）
# 300秒→120秒に短縮: スレッドを早く解放してポストレースキューを消化しやすくする
_POSTRACE_MAX_RETRY = 20  # 20回 × 120秒 = 最大40分（変わらず）
_POSTRACE_RETRY_WAIT_SEC = 120  # 120秒（旧300→短縮）

# 重複起動防止 PID ファイル
_LOCK_FILE = _ROOT / "data" / "auto_runner.pid"

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


def _estimate_start(
    race_date_str: str, race_number: int, post_time: str = ""
) -> datetime.datetime:
    """レース発走時刻を返す（JST, tzinfo なし）。

    races.post_time（実発走時刻 "HH:MM"）があればそれを使い、
    無ければ R1=10:00＋30分間隔のハードコード推定にフォールバックする（P1-5）。
    """
    base_date = datetime.datetime.strptime(race_date_str, "%Y-%m-%d")
    pt = (post_time or "").strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", pt)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return base_date.replace(hour=hh, minute=mm)
    return base_date.replace(hour=_R1_HOUR, minute=_R1_MINUTE) + datetime.timedelta(
        minutes=(race_number - 1) * _INTERVAL_MIN
    )


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
        logger.info(
            "待機中: あと %.0f 秒 (目標 %s)", remaining, target.strftime("%H:%M:%S")
        )
        # W-093: スリープ中も鼓動を刻む（生きているが進捗なし、ではなく正常待機だと示す）。
        write_heartbeat("auto_runner", note=f"wait->{target.strftime('%H:%M')}")
        time.sleep(sleep_secs)


# ─────────────────────────────────────────────────────────────────────────────
# DB アクセス
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_today_races(target_date: str) -> list[tuple[str, str, int, str]]:
    """DB から当日の (race_id, date, race_number, post_time) を返す。"""
    from src.database.init_db import init_db

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    conn = init_db()
    rows = conn.execute(
        """
        SELECT race_id,
               COALESCE(date, ?) AS date,
               CAST(SUBSTR(race_id, 11, 2) AS INTEGER) AS race_number,
               COALESCE(post_time, '') AS post_time
        FROM races
        WHERE date = ?
        ORDER BY race_id
        """,
        (formatted, formatted),
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# サブプロセス実行
# ─────────────────────────────────────────────────────────────────────────────


def _check_daily_loss_circuit_breaker(date_str: str) -> bool:
    """W-043: 当日P&L損失がCB閾値を超えているか確認する。

    W-087 Soft Stop: トリップ判定（戻り値）と通知を分離。本関数は判定のみ責務を持ち、
    発動時のアラートは日次1回に重複排除して送る。実際に予想生成を止めるか
    （Hard Stop）/ 継続するか（Soft Stop）は呼び出し側が `_CB_SOFT_STOP` で決める。

    Args:
        date_str: 対象日 "YYYYMMDD" 形式。

    Returns:
        True = CBトリップ（当日損失が閾値超過）。
        False = 正常。
    """
    if _DAILY_LOSS_LIMIT_JPY <= 0:
        return False  # CB 無効化（0以下で無制限）
    try:
        import sqlite3

        db_path = _ROOT / "data" / "umalogi.db"
        date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        conn = sqlite3.connect(str(db_path), timeout=10)
        try:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(pr.profit), 0.0) AS net_profit
                FROM prediction_results pr
                JOIN predictions p ON p.id = pr.prediction_id
                JOIN races r       ON r.race_id = p.race_id
                WHERE r.date = ?
                  AND COALESCE(p.is_superseded, 0) = 0
                  AND pr.is_hit IS NOT NULL
                """,
                (date_iso,),
            ).fetchone()
            net_profit = float(row[0]) if row else 0.0
        finally:
            conn.close()

        if net_profit <= -_DAILY_LOSS_LIMIT_JPY:
            mode = (
                "Soft Stop（予想生成は継続）"
                if _CB_SOFT_STOP
                else "Hard Stop（予想停止）"
            )
            logger.warning(
                "🛑 [W-043 CB] 当日損失 ¥%.0f が閾値 ¥%d を超過 → %s",
                abs(net_profit),
                _DAILY_LOSS_LIMIT_JPY,
                mode,
            )
            # W-087: 発動アラートは日次1回のみ（レース毎の Discord 連投を防止）
            if date_str not in _cb_alerted_dates:
                _cb_alerted_dates.add(date_str)
                if _CB_SOFT_STOP:
                    _send_discord(
                        f"🟡 **[UMALOGI W-043 CB 発動 / Soft Stop]**\n"
                        f"当日確定損失: **▲¥{abs(net_profit):,.0f}** (閾値: ▲¥{_DAILY_LOSS_LIMIT_JPY:,})\n"
                        f"本日 `{date_str}` は**予想の生成・DB保存・監視を継続**します（データ蓄積優先）。\n"
                        f"⚠️ 実弾発注は損失拡大リスクを考慮し、ご自身の判断でお願いします。\n"
                        f"完全停止に戻す場合は `CIRCUIT_BREAKER_SOFT_STOP=0` を設定してください。"
                    )
                else:
                    _send_discord(
                        f"🛑 **[UMALOGI W-043 サーキットブレーカー発動 / Hard Stop]**\n"
                        f"当日確定損失: **▲¥{abs(net_profit):,.0f}** (閾値: ▲¥{_DAILY_LOSS_LIMIT_JPY:,})\n"
                        f"本日 `{date_str}` の残レース予想生成を自動停止しました。\n"
                        f"再開する場合は `DAILY_LOSS_LIMIT_JPY=0` または `CIRCUIT_BREAKER_SOFT_STOP=1` を設定してください。"
                    )
            return True
        return False
    except Exception as exc:
        logger.warning("日次損失CBチェック失敗（続行）: %s", exc)
        return False  # チェック失敗時は安全側に倒して続行


def _run_prerace(race_id: str, dry_run: bool, model_version: str = "v1") -> int:
    """prerace パイプラインを実行して returncode を返す。

    Args:
        race_id:       対象レース ID
        dry_run:       True = 実行せずログのみ
        model_version: "v1" (既存) or "v2" (W-004+動的EV+Kelly)
    """
    cmd = [
        sys.executable,
        "-m",
        "src.main_pipeline",
        "prerace",
        race_id,
        "--model-version",
        model_version,
    ]
    if dry_run:
        logger.info("[DRY-RUN] 実行コマンド: %s", " ".join(cmd))
        return 0
    try:
        result = subprocess.run(
            cmd,
            cwd=str(_ROOT),
            timeout=300,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0 and result.stderr:
            logger.error(
                "prerace[%s] stderr [%s]: %s",
                model_version,
                race_id,
                result.stderr[-2000:],
            )
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error("prerace[%s] タイムアウト (300s): %s", model_version, race_id)
        return -1


def _run_prerace_v2(race_id: str, dry_run: bool) -> int:
    """V2 モデルで prerace パイプラインを実行する（A/B テスト用）。

    V1 実行後に呼ぶこと。V2 の prediction は {race_id}_v2.json / "本命V2(直前)" で保存される。
    """
    return _run_prerace(race_id, dry_run, model_version="v2")


def _run_fetch_result(race_id: str, dry_run: bool) -> int:
    """
    レース結果速報取得スクリプトを実行して returncode を返す。

    失敗時は _POSTRACE_MAX_RETRY 回まで _POSTRACE_RETRY_WAIT_SEC 秒待機して再試行する。
    全試行失敗時は Discord にアラートを送信する。
    """
    cmd = [
        sys.executable,
        str(_ROOT / "scripts" / "fetch_race_result.py"),
        "--race-id",
        race_id,
        "--no-dashboard",
    ]
    if dry_run:
        logger.info("[DRY-RUN] 実行コマンド: %s", " ".join(cmd))
        return 0

    for attempt in range(1, _POSTRACE_MAX_RETRY + 1):
        try:
            result = subprocess.run(
                cmd,
                cwd=str(_ROOT),
                timeout=300,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )
            if result.returncode == 0:
                return 0
            # 途中失敗はログのみ（Discordに通知しない）
            logger.warning(
                "[NG] 結果取得 rc=%d (試行 %d/%d): %s — %d 秒後に再試行",
                result.returncode,
                attempt,
                _POSTRACE_MAX_RETRY,
                race_id,
                _POSTRACE_RETRY_WAIT_SEC,
            )
            if result.stderr:
                logger.warning("[NG] 結果取得 stderr: %s", result.stderr[-1000:])
        except subprocess.TimeoutExpired:
            logger.warning(
                "結果速報取得 タイムアウト (300s) 試行 %d/%d: %s — %d 秒後に再試行",
                attempt,
                _POSTRACE_MAX_RETRY,
                race_id,
                _POSTRACE_RETRY_WAIT_SEC,
            )

        if attempt < _POSTRACE_MAX_RETRY:
            time.sleep(_POSTRACE_RETRY_WAIT_SEC)

    # 全試行失敗 → Discord 警告（最終のみ）
    wait_min = _POSTRACE_MAX_RETRY * _POSTRACE_RETRY_WAIT_SEC // 60
    _send_discord(
        f"⚠️ **[UMALOGI] 結果取得 失敗** `{race_id}`\n"
        f"{_POSTRACE_MAX_RETRY} 回 ({wait_min}分) 試行後も未確定。審議継続中か確認してください。\n"
        f"手動: `py scripts/fetch_race_result.py --race-id {race_id}`"
    )
    return 1


def _is_notable_race(race_id: str) -> bool:
    """
    note 記事生成対象かどうかを判定する。

    条件（いずれかに該当）:
      - レース名に重賞グレード記号または重賞ワードが含まれる
      - モデルの最大 EV が 5.0 以上
    """
    import re
    from src.database.init_db import init_db

    try:
        conn = init_db()
        row = conn.execute(
            "SELECT race_name FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        race_name = (row[0] or "") if row else ""

        # G1〜G3・国内重賞の主要パターン + EV閾値で重要度を判定
        is_graded = bool(
            re.search(
                r"[ＧGⅠⅡⅢ]|重賞|ステークス|カップ|記念|天皇賞|有馬|菊花賞|桜花賞|"
                r"ダービー|オークス|皐月賞|宝塚|スプリンターズ|マイルＣＳ|ジャパンＣ|"
                r"秋華賞|エリザベス|ヴィクトリア|高松宮|フェブラリー|チャンピオンズ",
                race_name,
            )
        )

        max_ev_row = conn.execute(
            "SELECT MAX(expected_value) FROM predictions WHERE race_id = ?", (race_id,)
        ).fetchone()
        max_ev = float(max_ev_row[0] or 0) if max_ev_row else 0.0

        conn.close()
        return is_graded or max_ev >= 5.0
    except Exception as e:
        logger.warning("notable_race 判定失敗 (race_id=%s): %s", race_id, e)
        return False


def _run_note_article(race_id: str, dry_run: bool) -> None:
    """prerace 完了後に note 用記事を非同期で生成・保存する。"""
    if dry_run:
        logger.info("[DRY-RUN] note 記事生成スキップ: %s", race_id)
        return
    try:
        cmd = [
            sys.executable,
            str(_ROOT / "scripts" / "generate_note_article.py"),
            "--race-id",
            race_id,
        ]
        logger.info("[NOTE] 記事生成開始: %s", race_id)
        result = subprocess.run(cmd, cwd=str(_ROOT), timeout=120)
        if result.returncode == 0:
            logger.info("[NOTE] 記事生成完了: %s", race_id)
        else:
            logger.warning(
                "[NOTE] 記事生成失敗 (rc=%d): %s", result.returncode, race_id
            )
    except subprocess.TimeoutExpired:
        logger.warning("[NOTE] 記事生成タイムアウト (120s): %s", race_id)
    except Exception as e:
        logger.warning("[NOTE] 記事生成エラー: %s — %s", race_id, e)


def _has_hit(race_id: str) -> bool:
    """postrace 完了後にそのレースで的中があったかを確認する。"""
    from src.database.init_db import init_db

    try:
        conn = init_db()
        row = conn.execute(
            """
            SELECT COUNT(*) FROM prediction_results pr
            JOIN predictions p ON p.id = pr.prediction_id
            WHERE p.race_id = ? AND pr.is_hit = 1
            """,
            (race_id,),
        ).fetchone()
        conn.close()
        return bool(row and row[0] > 0)
    except Exception as e:
        logger.warning("的中判定失敗 (race_id=%s): %s", race_id, e)
        return False


def _run_result_card(race_id: str, dry_run: bool) -> None:
    """postrace 完了・的中確認後に的中実績カード画像を生成・保存する。"""
    if dry_run:
        logger.info("[DRY-RUN] 的中カード生成スキップ: %s", race_id)
        return
    try:
        cmd = [
            sys.executable,
            str(_ROOT / "scripts" / "generate_result_card.py"),
            "--race-id",
            race_id,
            "--min-payout",
            "0",
        ]
        logger.info("[CARD] 的中カード生成開始: %s", race_id)
        result = subprocess.run(cmd, cwd=str(_ROOT), timeout=60)
        if result.returncode == 0:
            logger.info("[CARD] 的中カード生成完了: %s", race_id)
        else:
            logger.warning(
                "[CARD] 的中カード生成失敗 (rc=%d): %s", result.returncode, race_id
            )
    except subprocess.TimeoutExpired:
        logger.warning("[CARD] 的中カード生成タイムアウト (60s): %s", race_id)
    except Exception as e:
        logger.warning("[CARD] 的中カード生成エラー: %s — %s", race_id, e)


def _run_sns_post(race_id: str, dry_run: bool, pattern: str = "ab") -> None:
    """SNS 投稿テキスト（パターンA/B）を生成・保存する。"""
    if dry_run:
        logger.info(
            "[DRY-RUN] SNS ポスト生成スキップ: %s (pattern=%s)", race_id, pattern
        )
        return
    try:
        cmd = [
            sys.executable,
            str(_ROOT / "scripts" / "generate_sns_post.py"),
            "--race-id",
            race_id,
            "--pattern",
            pattern,
        ]
        logger.info("[SNS] ポスト生成開始: %s (pattern=%s)", race_id, pattern)
        result = subprocess.run(cmd, cwd=str(_ROOT), timeout=30)
        if result.returncode == 0:
            logger.info("[SNS] ポスト生成完了: %s", race_id)
        else:
            logger.warning(
                "[SNS] ポスト生成失敗 (rc=%d): %s", result.returncode, race_id
            )
    except subprocess.TimeoutExpired:
        logger.warning("[SNS] ポスト生成タイムアウト: %s", race_id)
    except Exception as e:
        logger.warning("[SNS] ポスト生成エラー: %s — %s", race_id, e)


def _run_jvlink_sync(dry_run: bool) -> None:
    """JVLink RACE + WOOD の STORED 同期を実行する（32bit 専用プロセス）。

    JVLINK_DISABLED=1 の場合はダイアログ回避のためスキップする。
    """
    if dry_run:
        logger.info("[DRY-RUN] JVLink 同期をスキップします")
        return
    if os.getenv("JVLINK_DISABLED", "").strip() == "1":
        logger.info(
            "JVLINK_DISABLED=1: JVLink RACE/WOOD 同期をスキップします（ダイアログ回避）"
        )
        _send_discord(
            "ℹ️ **[UMALOGI]** `JVLINK_DISABLED=1` のため JVLink 夜間同期をスキップ。"
            "netkeiba フォールバックで続行します。"
        )
        return
    # W-086: _jvlink_force_worker.py の --fromtime は argparse required のため、
    # 渡さないと argparse が rc=2 で即終了し同期が一度も走らない（2026-06-13 障害）。
    # OPT_TODAY(3) でも形式上必須なので当日日付を渡す。
    fromtime = datetime.date.today().strftime("%Y%m%d")
    for dataspec in ("RACE", "WOOD"):
        logger.info("JVLink %s 同期開始...", dataspec)
        try:
            result = subprocess.run(
                [
                    "py",
                    "-3.14-32",
                    str(_ROOT / "scripts" / "_jvlink_force_worker.py"),
                    "--dataspec",
                    dataspec,
                    "--fromtime",
                    fromtime,
                    "--option",
                    "3",
                ],
                cwd=str(_ROOT),
                timeout=1800,  # 30分上限（JVLinkハング対策）
            )
            if result.returncode != 0:
                logger.warning("JVLink %s 同期: rc=%d", dataspec, result.returncode)
                _send_discord(
                    f"⚠️ [UMALOGI] JVLink {dataspec} 同期失敗 (rc={result.returncode})。"
                    "netkeiba フォールバックで続行します。"
                )
        except subprocess.TimeoutExpired:
            logger.error(
                "JVLink %s 同期タイムアウト (1800s) — スキップして続行", dataspec
            )
            _send_discord(
                f"⚠️ [UMALOGI] JVLink {dataspec} 同期が30分でタイムアウト。次ステップに続行します。"
            )
        except Exception as exc:
            logger.error("JVLink %s 同期エラー: %s — 続行", dataspec, exc)
        logger.info("JVLink %s 同期完了", dataspec)


def _run_provisional(date_str: str, dry_run: bool) -> None:
    """指定日の暫定予想を生成する。"""
    if dry_run:
        logger.info("[DRY-RUN] 暫定予想生成をスキップします (date=%s)", date_str)
        return
    logger.info("暫定予想生成: %s", date_str)
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "src.main_pipeline",
                "provisional",
                "--date",
                date_str,
            ],
            cwd=str(_ROOT),
            timeout=3600,  # 1時間上限（全レース暫定予想ハング対策）
        )
        if result.returncode != 0:
            logger.warning("暫定予想生成 rc=%d (date=%s)", result.returncode, date_str)
            _send_discord(
                f"⚠️ [UMALOGI] 暫定予想生成失敗 (date={date_str} rc={result.returncode})"
            )
    except subprocess.TimeoutExpired:
        logger.error("暫定予想生成タイムアウト (3600s) date=%s — 続行", date_str)
        _send_discord(
            f"🚨 [UMALOGI] 暫定予想生成が1時間でタイムアウト (date={date_str})。確認してください。"
        )
    except Exception as exc:
        logger.error("暫定予想生成エラー date=%s: %s", date_str, exc)


def _run_odds_timeseries_recorder(dry_run: bool) -> None:
    """発走前レースのオッズ時系列スナップショットを追記する（W-080）。

    scripts/record_odds_timeseries.py を subprocess で起動する。従来は不使用の
    scheduler.py（10分間隔ジョブ）にのみ配線されており、本番オートパイロットでは
    一度も走っていなかった＝realtime_odds が単一時点のままで「朝→直前のオッズ
    変動率（スマートマネー検知）」特徴量の歴史データが永遠に蓄積されない構造だった。
    監視ループから約10分間隔で呼ばれる（発走前ウィンドウのレースのみ実取得）。
    """
    if os.environ.get("ODDS_TIMESERIES_DISABLED") == "1":
        return
    if dry_run:
        logger.info("[DRY-RUN] オッズ時系列レコーダーをスキップします")
        return
    try:
        result = subprocess.run(
            [sys.executable, str(_ROOT / "scripts" / "record_odds_timeseries.py")],
            cwd=str(_ROOT),
            timeout=300,  # 5分上限（ハングしても次周期で再試行）
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            # W-093: errors未指定だと子(JVLink 32bit由来)のCP932バイト(0x83)で
            # _readerthread が UnicodeDecodeError 死亡→パイプ詰まり→毎周期300sタイムアウト
            # の空転（2026-06-14 サイレント停止）になる。replace で構造的に封じる。
            errors="replace",
        )
        if result.returncode != 0:
            logger.warning(
                "[W-080] オッズ時系列レコーダー rc=%d stderr=%s",
                result.returncode,
                (result.stderr or "")[-300:],
            )
    except subprocess.TimeoutExpired:
        logger.error("[W-080] オッズ時系列レコーダー タイムアウト (300s) — 続行")
    except Exception as exc:  # noqa: BLE001 — 収集失敗で監視ループは止めない
        logger.error("[W-080] オッズ時系列レコーダー エラー: %s — 続行", exc)


def _run_x_scraper(date_str: str, dry_run: bool) -> None:
    """指定日の X（凄腕予想家）シグナルを収集し x_signals へ格納する（W-065）。

    フェイルセーフ方針: 収集に失敗・0件の場合は x_consensus_score を**無言で 0 埋め
    したまま放置せず**、Discord #system へ明示アラートを送る。これにより「第4ファクター
    が常時0のデッドフィーチャー化」というサイレント障害を運用者が即座に検知できる。

    Args:
        date_str: 対象日（YYYYMMDD・オートパイロット慣習）。
        dry_run: True なら実行しない。
    """
    if os.environ.get("X_SCRAPER_DISABLED") == "1":
        logger.info(
            "X_SCRAPER_DISABLED=1: x_scraper をスキップします (date=%s)", date_str
        )
        return
    if dry_run:
        logger.info("[DRY-RUN] x_scraper をスキップします (date=%s)", date_str)
        return

    iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    logger.info("x_signals 収集: %s", iso)
    saved = -1
    try:
        result = subprocess.run(
            [sys.executable, "-m", "src.scraper.x_scraper", "--date", iso],
            cwd=str(_ROOT),
            timeout=1800,  # 30分上限（Playwright ハング対策）
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",  # W-093: CP932混入でのリーダースレッド死亡を防ぐ
        )
        out = (result.stdout or "") + (result.stderr or "")
        m = re.search(r"saved=(\d+)", out)
        if m:
            saved = int(m.group(1))
        if result.returncode != 0:
            logger.warning(
                "x_scraper rc=%d (date=%s) stderr=%s",
                result.returncode,
                iso,
                (result.stderr or "")[-500:],
            )
    except subprocess.TimeoutExpired:
        logger.error("x_scraper タイムアウト (1800s) date=%s — 続行", iso)
    except Exception as exc:  # noqa: BLE001 — 収集失敗で本処理は止めない
        logger.error("x_scraper エラー date=%s: %s — 続行", iso, exc)

    # フェイルセーフ: 0件 / 失敗（saved<=0）なら明示通知（無言の0埋めを許さない）
    if saved <= 0:
        logger.warning(
            "[W-065] x_signals 収集 0件/失敗 (date=%s)。"
            "x_consensus_score は本日 0 埋め（デッドフィーチャー）で稼働します。",
            iso,
        )
        _send_discord(
            f"⚠️ **[UMALOGI] X シグナル収集 0件/失敗** (date=`{iso}`)\n"
            f"`x_consensus_score` が本日 0 埋め（事実上無効）になります。"
            f"X スクレイパー（Playwright / アカウント設定 / レート制限）を確認してください。\n"
            f"※ 一時無効化する場合は環境変数 `X_SCRAPER_DISABLED=1`。",
            color=0xF1C40F,
        )
    else:
        logger.info("x_signals 収集完了: %s 件 (date=%s)", saved, iso)


def _run_generate_web_data(dry_run: bool) -> None:
    """Web ダッシュボード用 JSON を再生成する。"""
    if dry_run:
        logger.info("[DRY-RUN] Web データ生成をスキップします")
        return
    logger.info("Web データ生成中...")
    try:
        subprocess.run(
            [sys.executable, str(_ROOT / "web" / "generate_data.py")],
            cwd=str(_ROOT),
            timeout=180,  # 3分上限
        )
    except subprocess.TimeoutExpired:
        logger.warning("Web データ生成タイムアウト (180s) — 続行")
    except Exception as exc:
        logger.warning("Web データ生成エラー: %s — 続行", exc)


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
    sunday_dt = datetime.datetime.strptime(
        saturday_date, "%Y%m%d"
    ) + datetime.timedelta(days=1)
    sunday_date = sunday_dt.strftime("%Y%m%d")

    logger.info("=" * 60)
    logger.info("金曜夜間バッチ開始: 土曜=%s  日曜=%s", saturday_date, sunday_date)
    logger.info("=" * 60)

    _run_jvlink_sync(dry_run)
    # W-065: 暫定予想の特徴量生成前に X シグナルを収集し x_consensus_score を有効化。
    #   失敗時は _run_x_scraper 内で明示アラート（無言の 0 埋めを許さない）。
    _run_x_scraper(saturday_date, dry_run)
    _run_x_scraper(sunday_date, dry_run)
    _run_provisional(saturday_date, dry_run)
    _run_provisional(sunday_date, dry_run)

    # 注目レース（重賞・高EV）の note 記事を暫定予想ベースで先行生成
    for date_str in (saturday_date, sunday_date):
        from src.database.init_db import init_db as _idb

        try:
            formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            _conn = _idb()
            rows = _conn.execute(
                "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
                (formatted,),
            ).fetchall()
            _conn.close()
            for (rid,) in rows:
                if _is_notable_race(rid):
                    logger.info(
                        "[NOTE] 暫定予想完了後 → 注目レース note 記事生成: %s", rid
                    )
                    _run_note_article(rid, dry_run)
        except Exception as e:
            logger.warning("[NOTE] 金曜バッチ note 記事生成エラー: %s", e)

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
    _run_x_scraper(next_date, dry_run)  # W-065: 前夜投稿の X シグナルを収集
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

    sunday_dt = datetime.datetime.strptime(sunday_date, "%Y%m%d")
    saturday_dt = sunday_dt - datetime.timedelta(days=1)
    saturday_str = saturday_dt.strftime("%Y-%m-%d")
    sunday_str = sunday_dt.strftime("%Y-%m-%d")

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
        hits = hits or 0
        payout = payout or 0
        profit = profit or 0

        hit_rate = f"{hits / total * 100:.1f}%" if total > 0 else "N/A"
        p_emoji = "🟢" if profit >= 0 else "🔴"
        sign = "+" if profit >= 0 else ""

        _send_discord_race(
            f"📊 **[UMALOGI] 週次サマリー ({saturday_str} 〜 {sunday_str})**\n"
            f"予想件数: {total} 件 / 的中: {hits} 件 (的中率 {hit_rate})\n"
            f"払戻合計: ¥{int(payout):,}\n"
            f"損益: {p_emoji} {sign}¥{int(abs(profit)):,}\n"
            f"次週は来週金曜 {_EVENING_FETCH_HOUR:02d}:{_EVENING_FETCH_MINUTE:02d} に自動再起動します 🤖"
        )
        logger.info(
            "週次レポート送信完了: 予想%d件 / 的中%d件 / 損益%+d円",
            total,
            hits,
            int(profit),
        )
    except Exception as e:
        logger.error("週次レポート生成に失敗しました: %s", e)


def _kick_post_race_analysis(sunday_date: str, dry_run: bool) -> None:
    """週次レポート直後に敗因分析(Phase-A)を非同期・best-effort で起動する。

    既存の週次サイクルを絶対に巻き添えにしないための徹底:
      - **daemon スレッド**で非同期実行（メインループは即スリープへ移行できる）。
      - 内部を try/except で完全内包し、例外は握り潰してログのみ（ループを止めない）。
      - DB は post_race_analyzer 側の **読み取り専用(mode=ro)** 接続のみを使用。
      - 当該週末（土〜日）の EV>=1.0 不的中レースに絞って分析する。

    Args:
        sunday_date: 日曜日付 "YYYYMMDD"。
        dry_run:     True なら起動しない。
    """
    if dry_run:
        logger.info("[DRY-RUN] 敗因分析(Phase-A)起動をスキップします")
        return

    def _worker() -> None:
        try:
            from src.analysis.post_race_analyzer import run_post_race_analysis

            sunday_dt = datetime.datetime.strptime(sunday_date, "%Y%m%d")
            saturday_str = (sunday_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            run_post_race_analysis(since=saturday_str)
            logger.info("敗因分析(Phase-A) 送信完了 (since=%s)", saturday_str)
        except Exception as e:  # 週次サイクルを止めない（best-effort）
            logger.error("敗因分析(Phase-A)に失敗しました（無視して継続）: %s", e)

    threading.Thread(target=_worker, name="post_race_analysis", daemon=True).start()
    logger.info("敗因分析(Phase-A)を非同期起動しました")


# ─────────────────────────────────────────────────────────────────────────────
# 1日分の監視ループ
# ─────────────────────────────────────────────────────────────────────────────


def _run_one_day(
    target_date: str,
    fire_ahead: datetime.timedelta,
    result_after: datetime.timedelta,
    dry_run: bool,
    recheck_ahead: datetime.timedelta | None = None,
) -> tuple[int, int, int, int]:
    """
    指定日の全レース監視ループを実行する。
    戻り値: (prerace_success, prerace_fail, postrace_success, postrace_fail)

    recheck_ahead を指定すると、各レース発走の recheck_ahead 前に
    異常検知（出走取消・騎手変更）→ 必要時のみ自動再推論を実行する（ステップ2-3）。
    """
    races = _fetch_today_races(target_date)
    if not races:
        logger.warning(
            "対象日 %s のレースが DB にありません。夜間バッチを先に実行してください。",
            target_date,
        )
        return 0, 0, 0, 0

    try:
        from src.ui.console import get_console

        get_console().banner(
            "UMA-LOGIC AUTOPILOT",
            f"対象日 {target_date} ｜ {len(races)} レース監視 ｜ "
            f"発走{int(fire_ahead.total_seconds() // 60)}分前予想 / "
            f"{int(result_after.total_seconds() // 60)}分後速報",
        )
    except Exception:  # noqa: BLE001 - 装飾失敗で本番を殺さない
        pass
    logger.info("=" * 60)
    logger.info("UMALOGI 直前予想 + 結果速報 自律監視ループ 起動")
    logger.info(
        "対象日: %s  対象レース: %d 件  発走%d分前/発走%d分後",
        target_date,
        len(races),
        int(fire_ahead.total_seconds() // 60),
        int(result_after.total_seconds() // 60),
    )
    logger.info("=" * 60)

    date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    schedule: list[tuple[datetime.datetime, str, int, str]] = []
    for race_id, race_date, race_number, post_time in races:
        start = _estimate_start(race_date or date_str, race_number, post_time)
        schedule.append((start - fire_ahead, race_id, race_number, "prerace"))
        # ステップ2-3: 発走 recheck_ahead 前に異常検知→再推論（prerace より後に発火）
        if recheck_ahead is not None and recheck_ahead < fire_ahead:
            schedule.append((start - recheck_ahead, race_id, race_number, "recheck"))
        schedule.append((start + result_after, race_id, race_number, "postrace"))
        logger.info(
            "  R%02d  %s  発走推定 %s  prerace→%s  postrace→%s",
            race_number,
            race_id,
            start.strftime("%H:%M"),
            (start - fire_ahead).strftime("%H:%M"),
            (start + result_after).strftime("%H:%M"),
        )

    logger.info("-" * 60)

    if dry_run:
        logger.info("[DRY-RUN] スケジュール表示のみで終了します。")
        return 0, 0, 0, 0

    schedule.sort(key=lambda x: (x[0], x[1], x[3]))

    # ── 起動直後の文字化けスキャン & 自動修復 ──────────────────────────────
    try:
        from src.monitoring.mojibake_watcher import run_scan_and_fix

        logger.info("[MojibakeWatcher] 起動時スキャン開始: date=%s", target_date)
        _scan_result = run_scan_and_fix(
            target_date=target_date,
            check_api=True,
            auto_fix=True,
            notify=True,
        )
        logger.info(
            "[MojibakeWatcher] スキャン完了: 問題=%d 修復=%d API問題=%d",
            _scan_result.total_issues,
            len(_scan_result.fixed_races),
            len(_scan_result.api_issues),
        )
    except Exception as _we:
        logger.warning("[MojibakeWatcher] スキャンエラー（処理継続）: %s", _we)

    # ── マーケティング資産の自動生成（SNS集客＋サブスク向けプレミアム）────────
    # 失敗しても監視ループ本体（予想・結果取得）を絶対に妨げない best-effort。
    try:
        from src.marketing.premium_pack import generate_marketing_assets

        _mk_files = generate_marketing_assets(target_date)
        logger.info("[Marketing] 自動生成完了: %d ファイル", len(_mk_files))
    except Exception as _me:
        logger.warning("[Marketing] 生成エラー（処理継続）: %s", _me)

    skipped: set[tuple[str, str]] = set()
    now = datetime.datetime.now()

    # 発走時刻を過ぎた prerace はスキップ（発走+30分以内ならまだ実行可）
    # 30分: 写真判定・審議を考慮しても確定前に予想を DB に確保できるウィンドウ
    for fire_at, race_id, race_number, job_type in schedule:
        if job_type == "prerace":
            start = fire_at + fire_ahead
            if now >= start + datetime.timedelta(minutes=30):
                logger.warning(
                    "R%02d %s [prerace] 発走推定時刻 %s を 30 分超過 -> スキップ",
                    race_number,
                    race_id,
                    start.strftime("%H:%M"),
                )
                skipped.add((race_id, "prerace"))
        elif job_type == "recheck" and recheck_ahead is not None:
            # 異常検知の再推論は発走後に走らせない（発走 +5 分超過でスキップ）
            start = fire_at + recheck_ahead
            if now >= start + datetime.timedelta(minutes=5):
                skipped.add((race_id, "recheck"))

    pending_jobs = [s for s in schedule if (s[1], s[3]) not in skipped]
    total = len(pending_jobs)

    logger.info("スケジュール済みジョブ: %d 件 (スキップ: %d 件)", total, len(skipped))
    logger.info("監視ループ開始 [非同期スレッドプール] - Ctrl+C で中断")
    logger.info("-" * 60)

    prerace_success = prerace_fail = 0
    postrace_success = postrace_fail = 0

    # ── スレッドプール非同期実行 ──────────────────────────────────────────────
    # postrace は最大 40 分のリトライループを持つため、同期実行すると後続の
    # prerace ジョブが遅延する。各ジョブを独立スレッドで並行実行することで
    # 手動介入・審議による遅延に関係なくスケジュールどおり発火する。
    # ── メモリ管理: Future を3段階で分離して完了後即解放 ──────────────────
    # submitted_keys : 重複起動防止（全ジョブの起動済みキーセット）
    # active_futures : 実行中 Future のみ保持。完了次第 result_cache に移して解放
    # result_cache   : 完了後の戻り値 (int) を保持（結果集計用）
    submitted_keys: set[tuple[str, str]] = set()
    active_futures: dict[tuple[str, str], Future] = {}
    result_cache: dict[tuple[str, str], int] = {}

    def _prerace_worker(
        race_id: str, race_number: int, fire_at: datetime.datetime
    ) -> int:
        start = fire_at + fire_ahead
        logger.info(
            "[START] R%02d %s  [prerace] 直前予想開始 (推定発走 %s)",
            race_number,
            race_id,
            start.strftime("%H:%M"),
        )
        # W-043 / W-087: サーキットブレーカーチェック。
        #   Soft Stop（既定）= 予想の生成と DB 保存は継続し、アラートのみ（データ蓄積・監視を止めない）。
        #   Hard Stop = 旧来どおり prerace をスキップ（予想を生成しない）。
        if _check_daily_loss_circuit_breaker(target_date):
            if _CB_SOFT_STOP:
                logger.warning(
                    "[CB-SOFT] R%02d %s [prerace] CB発動中だが予想生成を継続（Soft Stop）",
                    race_number,
                    race_id,
                )
                # フォールスルーして _run_prerace を実行する
            else:
                logger.warning(
                    "[CB] R%02d %s [prerace] CB発動 → スキップ（Hard Stop）",
                    race_number,
                    race_id,
                )
                return -2  # CB トリップコード（Hard Stop 時のみ）
        rc = _run_prerace(race_id, dry_run)
        if rc == 0:
            logger.info("[OK] R%02d %s  [prerace V1] 完了", race_number, race_id)
            # V2 A/Bテスト: V1 成功後に V2 も並列実行（独立した predictions として保存）
            rc_v2 = _run_prerace_v2(race_id, dry_run)
            if rc_v2 == 0:
                logger.info(
                    "[OK] R%02d %s  [prerace V2] 完了 (A/Bテスト)", race_number, race_id
                )
            else:
                logger.warning(
                    "[WARN] R%02d %s  [prerace V2] 失敗 (rc=%d) — V1 予想は正常",
                    race_number,
                    race_id,
                    rc_v2,
                )
            if _is_notable_race(race_id):
                logger.info("[NOTE] 注目レース検知 → note 記事生成: %s", race_id)
                _run_note_article(race_id, dry_run)
                _run_sns_post(race_id, dry_run, pattern="a")
        else:
            logger.error(
                "[NG] R%02d %s  [prerace V1] 失敗 (rc=%d)", race_number, race_id, rc
            )
            _send_discord(
                f"🚨 **[UMALOGI]** R{race_number:02d} `{race_id}` 直前予想 V1 失敗 (rc={rc})\n"
                f"手動実行: `py -m src.main_pipeline prerace {race_id}`"
            )
        return rc

    def _recheck_worker(race_id: str, race_number: int) -> int:
        """発走直前の異常検知（取消・騎手変更）→ 変化があれば自動再推論（ステップ2-3）。"""
        from src.database.init_db import init_db
        from src.pipeline.anomaly import check_race_anomalies
        from src.pipeline.scraping import fetch_and_save_odds

        check_jockey = os.getenv("ANOMALY_JOCKEY_CHECK", "1").strip() != "0"
        logger.info(
            "[START] R%02d %s  [recheck] 直前異常検知開始", race_number, race_id
        )
        if dry_run:
            logger.info("[DRY-RUN] recheck スキップ: %s", race_id)
            return 0
        try:
            conn = init_db()
            try:
                # 最新の JRA-VAN 速報を取得してから検知（取消は最新feed欠落で判定）
                try:
                    fetch_and_save_odds(conn, race_id)
                except Exception as e:
                    logger.warning("[recheck] 速報オッズ更新失敗（続行）: %s", e)
                anomalies = check_race_anomalies(
                    conn, race_id, check_jockey=check_jockey
                )
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("[recheck] 異常検知失敗（続行）: %s — %s", race_id, exc)
            return 0

        if not anomalies.has_changes:
            logger.info("[OK] R%02d %s  [recheck] 異常なし", race_number, race_id)
            return 0

        parts: list[str] = []
        if anomalies.scratched:
            parts.append(f"取消/除外: {sorted(anomalies.scratched)}")
        if anomalies.jockey_changes:
            jc = "・".join(
                f"{hn}番 {old}→{new}"
                for hn, (old, new) in sorted(anomalies.jockey_changes.items())
            )
            parts.append(f"騎手変更: {jc}")
        summary = " / ".join(parts)
        logger.warning(
            "[recheck] R%02d %s 異常検知 → 再推論: %s", race_number, race_id, summary
        )
        _send_discord_race(
            f"🔁 **[UMALOGI] R{race_number:02d} 直前異常検知 → 自動再推論**\n`{race_id}`  {summary}"
        )
        rc = _run_prerace(race_id, dry_run)
        if rc == 0:
            logger.info("[OK] R%02d %s  [recheck] 再推論完了", race_number, race_id)
            _run_prerace_v2(race_id, dry_run)
        else:
            logger.error(
                "[NG] R%02d %s  [recheck] 再推論失敗 (rc=%d)", race_number, race_id, rc
            )
        return rc

    def _postrace_worker(race_id: str, race_number: int) -> int:
        logger.info("[START] R%02d %s  [postrace] 結果取得開始", race_number, race_id)
        rc = _run_fetch_result(race_id, dry_run)
        if rc == 0:
            logger.info("[OK] R%02d %s  [postrace] 完了", race_number, race_id)
            if _has_hit(race_id):
                logger.info("[CARD] 的中検知 → カード＋SNS生成: %s", race_id)
                _run_result_card(race_id, dry_run)
                _run_sns_post(race_id, dry_run, pattern="b")
        else:
            logger.warning(
                "[NG] R%02d %s  [postrace] 失敗 (rc=%d) → 未確定の可能性あり",
                race_number,
                race_id,
                rc,
            )
        return rc

    try:
        # prerace と postrace を完全分離したスレッドプール
        # postrace の長期リトライ（最大40分）が prerace 発火を絶対にブロックしない。
        with (
            ThreadPoolExecutor(
                max_workers=12, thread_name_prefix="umalogi-pre"
            ) as pre_ex,
            ThreadPoolExecutor(
                max_workers=40, thread_name_prefix="umalogi-post"
            ) as post_ex,
        ):
            # W-080: オッズ時系列スナップショットを約10分間隔で蓄積
            # （スマートマネー＝朝→直前オッズ変動率 特徴量の歴史データを育てる）
            next_odds_capture = datetime.datetime.now()
            while True:
                now = datetime.datetime.now()
                # W-093: 監視ループ各周回で鼓動。空転（オッズレコーダー等の詰まり）に
                # 陥っても、ここが回り続ける限り鼓動が更新される。鼓動が止まったら
                # watchdog がハング扱いで強制再起動する。
                write_heartbeat(
                    "auto_runner",
                    note=f"monitor submitted={len(submitted_keys)}/{total}",
                )

                if now >= next_odds_capture:
                    post_ex.submit(_run_odds_timeseries_recorder, dry_run)
                    next_odds_capture = now + datetime.timedelta(minutes=10)

                for fire_at, race_id, race_number, job_type in pending_jobs:
                    key = (race_id, job_type)
                    if key in submitted_keys:
                        continue  # 既に起動済み
                    if now < fire_at:
                        continue  # まだ時刻未到達

                    if job_type == "prerace":
                        active_futures[key] = pre_ex.submit(
                            _prerace_worker, race_id, race_number, fire_at
                        )
                    elif job_type == "recheck":
                        active_futures[key] = pre_ex.submit(
                            _recheck_worker, race_id, race_number
                        )
                    else:  # postrace
                        active_futures[key] = post_ex.submit(
                            _postrace_worker, race_id, race_number
                        )
                    submitted_keys.add(key)

                # 完了済み Future をメモリから即時解放
                for k in list(active_futures.keys()):
                    f = active_futures[k]
                    if f.done():
                        try:
                            result_cache[k] = f.result()
                        except Exception as exc:
                            logger.error("[例外] %s [%s]: %s", k[0], k[1], exc)
                            result_cache[k] = -1
                        del active_futures[k]

                # 全ジョブ起動 & 完了確認
                all_submitted = len(submitted_keys) == total
                all_done = all_submitted and len(active_futures) == 0
                if all_done:
                    break

                # 次発火まで待機（最大 10 秒）
                unfired = [
                    s[0] for s in pending_jobs if (s[1], s[3]) not in submitted_keys
                ]
                if unfired:
                    next_fire = min(unfired)
                    sleep_secs = min(
                        10.0,
                        max(1.0, (next_fire - datetime.datetime.now()).total_seconds()),
                    )
                else:
                    sleep_secs = 10.0
                time.sleep(sleep_secs)

        # 結果集計（result_cache から取得）
        for race_id, job_type in submitted_keys:
            rc = result_cache.get((race_id, job_type), -1)
            if job_type == "prerace":
                if rc == 0:
                    prerace_success += 1
                else:
                    prerace_fail += 1
            elif job_type == "postrace":
                if rc == 0:
                    postrace_success += 1
                else:
                    postrace_fail += 1
            # recheck は統計に含めない（再推論はログ・Discord通知で可視化）

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
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（省略時=当日）")
    parser.add_argument(
        "--fire-ahead-min",
        type=int,
        default=20,
        help="発走何分前に prerace を実行するか（デフォルト 20）",
    )
    parser.add_argument(
        "--result-after-min",
        type=int,
        default=15,
        help="発走何分後に結果速報を取得するか（デフォルト 15）",
    )
    parser.add_argument(
        "--recheck-ahead-min",
        type=int,
        default=8,
        help=(
            "発走何分前に直前異常検知（取消・騎手変更）→再推論を実行するか"
            "（デフォルト 8。0 で無効化。ステップ2-3）"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="コマンドを実行せずにスケジュールのみ表示",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help=(
            "週次オートパイロット: 金→土→日→次週金のサイクルを自動継続。"
            "人間介入ゼロで永続動作する。"
        ),
    )
    args = parser.parse_args()

    try:
        from src.ops.jvlink_dialog_handler import start_dialog_handler

        start_dialog_handler(interval=0.3)
    except Exception:
        pass

    fire_ahead = datetime.timedelta(minutes=args.fire_ahead_min)
    result_after = datetime.timedelta(minutes=args.result_after_min)
    recheck_ahead = (
        datetime.timedelta(minutes=args.recheck_ahead_min)
        if args.recheck_ahead_min and args.recheck_ahead_min > 0
        else None
    )
    dry_run = args.dry_run
    continuous = args.continuous

    target_date = args.date or datetime.date.today().strftime("%Y%m%d")

    # ── 重複起動防止 ──────────────────────────────────────────────────
    if not _check_single_instance():
        print(
            f"[ABORT] 別インスタンスが既に動作中です (PID={_LOCK_FILE.read_text(encoding='utf-8').strip()})。終了します。",
            flush=True,
        )
        sys.exit(0)

    # PID ファイルを書き込んだ直後に必ずクリーンアップを登録する。
    # atexit: 正常終了・例外による終了の両方で発動。
    # signal: SIGTERM（外部 kill コマンド）でも発動。
    import atexit
    import signal

    def _cleanup_pid() -> None:
        """PID ファイル・ハートビートを確実に削除する（べき等）。"""
        _LOCK_FILE.unlink(missing_ok=True)
        # W-093: 鼓動も消す。プロセス不在＋鼓動なし＝watchdog は再起動を試みない
        # （意図的停止 / supervisor 側の再起動に委ねる）。
        clear_heartbeat("auto_runner")

    atexit.register(_cleanup_pid)

    def _handle_sigterm(signum: int, frame: object) -> None:
        """SIGTERM を受信したら PID ファイルを削除して終了する。"""
        logger.info("SIGTERM 受信 → PID ファイルを削除して終了します")
        _cleanup_pid()
        _send_discord(
            "🛑 **[UMALOGI] SIGTERM 受信** PID ファイルをクリーンアップして終了しました"
        )
        sys.exit(0)

    # Windows は SIGTERM をサポートしているが SIGKILL は存在しないため SIGTERM のみ登録
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
    except (OSError, ValueError):
        pass  # 一部環境では登録不可（子スレッドからの呼び出し等）→ 無視

    _send_discord(
        f"🚀 **[UMALOGI] 週次オートパイロット 起動**\n"
        f"対象日: `{target_date}`  継続モード: {'ON (週次サイクル)' if continuous else 'OFF (本日のみ)'}\n"
        f"PID: `{os.getpid()}`"
    )

    _consecutive_errors = 0
    _MAX_CONSECUTIVE_ERRORS = 10
    # W-044: セッション全体クラッシュカウンタ（成功時にリセットしない）
    _session_total_crashes = 0

    while True:
        try:
            # W-093: サイクル開始でも鼓動（曜日判定・各フェーズ間でも生存を更新）。
            write_heartbeat("auto_runner", note=f"cycle-start {target_date}")
            wd = _weekday(target_date)
            is_friday = wd == _FRI
            is_saturday = wd == _SAT
            is_sunday = wd == _SUN
            is_racing = wd in (_SAT, _SUN)

            # ── 当日の監視ループ実行 ──────────────────────────────────────
            if is_friday:
                # 金曜はJRAレースなし → 夜間バッチを待機するだけ
                logger.info(
                    "金曜日 (%s) - レースなし。夜間バッチ時刻を待機します", target_date
                )
                ps = pf = rs = rf = 0

            elif is_racing:
                # 土曜・日曜: 通常監視ループ
                ps, pf, rs, rf = _run_one_day(
                    target_date, fire_ahead, result_after, dry_run, recheck_ahead
                )

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

            # 正常完了: 連続エラーカウンタをリセット
            _consecutive_errors = 0

            if not continuous:
                break

            # ── 継続モード: 曜日別ルーティング ───────────────────────────
            now = datetime.datetime.now()
            today_dt = datetime.datetime.strptime(target_date, "%Y%m%d")

            if is_sunday:
                # ── 日曜完了後: 週次レポート → 次週金曜まで長期スリープ ──
                logger.info("日曜監視完了。週次収支レポートを生成します...")
                _send_weekly_report(target_date, dry_run)
                # 週次レポート直後に敗因分析(Phase-A)を非同期・best-effort で起動
                # （失敗してもスリープ移行を妨げない）。
                _kick_post_race_analysis(target_date, dry_run)

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
                saturday_dt = next_friday_ev + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                # 土曜朝まで待機
                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR,
                    minute=_MORNING_START_MINUTE,
                    second=0,
                    microsecond=0,
                )
                _wait_until(morning_start, dry_run)
                target_date = saturday_date

            elif is_saturday:
                # ── 土曜完了後: 20:00 夜間バッチ → 日曜朝まで待機 ─────────
                evening_trigger = today_dt.replace(
                    hour=_EVENING_FETCH_HOUR,
                    minute=_EVENING_FETCH_MINUTE,
                    second=0,
                    microsecond=0,
                )
                if now < evening_trigger:
                    logger.info(
                        "夜間バッチ待機: %s まで", evening_trigger.strftime("%H:%M")
                    )
                    _wait_until(evening_trigger, dry_run)

                next_date_dt = today_dt + datetime.timedelta(days=1)
                next_date_str = next_date_dt.strftime("%Y%m%d")
                _run_evening_fetch(next_date_str, dry_run)

                morning_start = next_date_dt.replace(
                    hour=_MORNING_START_HOUR,
                    minute=_MORNING_START_MINUTE,
                    second=0,
                    microsecond=0,
                )
                _wait_until(morning_start, dry_run)
                target_date = next_date_str

            elif is_friday:
                # ── 金曜: 20:00 に金曜夜間バッチ → 土曜朝まで待機 ───────
                evening_trigger = today_dt.replace(
                    hour=_EVENING_FETCH_HOUR,
                    minute=_EVENING_FETCH_MINUTE,
                    second=0,
                    microsecond=0,
                )
                if now < evening_trigger:
                    logger.info(
                        "金曜夜間バッチ待機: %s まで", evening_trigger.strftime("%H:%M")
                    )
                    _wait_until(evening_trigger, dry_run)

                saturday_dt = today_dt + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR,
                    minute=_MORNING_START_MINUTE,
                    second=0,
                    microsecond=0,
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

                saturday_dt = next_friday_ev + datetime.timedelta(days=1)
                saturday_date = saturday_dt.strftime("%Y%m%d")
                _run_friday_batch(saturday_date, dry_run)

                morning_start = saturday_dt.replace(
                    hour=_MORNING_START_HOUR,
                    minute=_MORNING_START_MINUTE,
                    second=0,
                    microsecond=0,
                )
                _wait_until(morning_start, dry_run)
                target_date = saturday_date

        except KeyboardInterrupt:
            logger.info("[終了] Ctrl+C で停止しました")
            _send_discord("🛑 **[UMALOGI] 手動停止** Ctrl+C で監視ループを終了しました")
            break

        except Exception as exc:
            import traceback

            _consecutive_errors += 1
            _session_total_crashes += 1  # W-044: 成功時にリセットしないカウンタ
            tb_str = traceback.format_exc()
            logger.error(
                "[ERROR] 予期しない例外 (連続%d/%d回 / セッション計%d/%d回): %s\n%s",
                _consecutive_errors,
                _MAX_CONSECUTIVE_ERRORS,
                _session_total_crashes,
                _MAX_SESSION_CRASHES,
                exc,
                tb_str,
            )

            if _consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                # 連続エラー上限到達 → Discord 通知して安全に終了
                logger.critical(
                    "連続 %d 回エラー上限到達。プロセスを安全に終了します。",
                    _MAX_CONSECUTIVE_ERRORS,
                )
                _send_discord(
                    f"🚨 **[UMALOGI] 連続{_MAX_CONSECUTIVE_ERRORS}回エラー → 自動停止**\n"
                    f"最終エラー: `{exc}`\n"
                    f"手動で原因を確認してからプロセスを再起動してください。\n"
                    f"再起動コマンド: `py scripts/today_auto_runner.py --continuous`"
                )
                break  # sys.exit より break を使用（atexit/_cleanup_pid が確実に実行される）

            # W-044: セッション全体クラッシュ上限チェック（フラッピング障害対策）
            if _session_total_crashes >= _MAX_SESSION_CRASHES:
                logger.critical(
                    "セッション総クラッシュ数 %d 回上限到達 (W-044)。プロセスを停止します。",
                    _MAX_SESSION_CRASHES,
                )
                _send_discord(
                    f"🚨 **[UMALOGI W-044] セッション総クラッシュ{_session_total_crashes}回上限**\n"
                    f"長期フラッピング障害の可能性。プロセスを停止しました。\n"
                    f"手動で原因を特定し、`py scripts/today_auto_runner.py --continuous` で再起動してください。"
                )
                break

            _send_discord(
                f"⚠️ **[UMALOGI] 例外発生 → 自動再起動 ({_consecutive_errors}/{_MAX_CONSECUTIVE_ERRORS})**\n"
                f"エラー: `{exc}`\n"
                f"{_RESTART_WAIT_SEC} 秒後に同じ対象日 `{target_date}` で再起動します"
            )
            time.sleep(_RESTART_WAIT_SEC)
            continue


if __name__ == "__main__":
    main()
