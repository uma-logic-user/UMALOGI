"""
UMALOGI 自己修復・番犬システム (watchdog.py)
=============================================
10 分おきに本日レースのオッズ欠損を監視し、欠損検知時に段階的修復を実行する。

修復ステップ:
  Step 1 (ソフト): py -3-32 -m src.ops.data_sync friday --date YYYYMMDD
  Step 2 (ハード): JVLinkAgent.exe を taskkill → 再起動 → data_sync 再実行

Discord へ修復の進捗をリアルタイム通知する。

Usage:
    py scripts/watchdog.py              # 通常起動（10 分ループ）
    py scripts/watchdog.py --once       # 1 回だけチェックして終了（デバッグ用）
    py scripts/watchdog.py --interval 5 # チェック間隔を変更（分）
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("watchdog")

# ── 定数 ──────────────────────────────────────────────────────────────────────

_DB_PATH        = _ROOT / "data" / "umalogi.db"
_SYNC_CMD_32    = [sys.executable.replace("python.exe", "python.exe"), "-3-32"]
# 32bit Python のパス候補（環境によって異なる）
_PY32_CANDIDATES = [
    r"C:\Python311-32\python.exe",
    r"C:\Python312-32\python.exe",
    r"C:\Python310-32\python.exe",
    r"C:\Python39-32\python.exe",
    r"C:\Windows\py.exe",  # py launcher fallback
]

# 修復試行間の最小インターバル（秒）
_SOFT_RETRY_WAIT   = 120   # ソフト修復後 2 分待つ
_HARD_RETRY_WAIT   = 180   # ハード修復後 3 分待つ
_JVLINK_KILL_WAIT  =  10   # taskkill 後の待機（秒）
_SYNC_TIMEOUT      = 300   # data_sync タイムアウト（秒）

# 1 レースあたりの NaN 検知しきい値（割合）
_NAN_RATIO_THRESHOLD = 0.5   # 50% 以上が NaN なら修復トリガー

_shutdown_flag = False


def _signal_handler(sig: int, frame: object) -> None:
    global _shutdown_flag
    logger.info("シグナル %d 受信 — 次のチェック後に終了します", sig)
    _shutdown_flag = True


signal.signal(signal.SIGINT,  _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ── Discord 通知 ────────────────────────────────────────────────────────────

def _discord(msg: str) -> None:
    """Discord システムチャンネルにメッセージを送信。失敗しても例外を握り潰す。
    DISCORD_SYSTEM_WEBHOOK_URL を優先し、未設定時は DISCORD_WEBHOOK_URL へ fallback する。
    """
    webhook_url = (
        os.getenv("DISCORD_SYSTEM_WEBHOOK_URL", "")
        or os.getenv("DISCORD_WEBHOOK_URL", "")
    )
    if not webhook_url:
        logger.debug("Discord URL 未設定 — 通知スキップ")
        return
    try:
        import requests
        resp = requests.post(webhook_url, json={"content": msg}, timeout=10)
        if resp.status_code not in (200, 204):
            logger.warning("Discord 送信失敗: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord 送信エラー: %s", exc)


# ── DB チェック ─────────────────────────────────────────────────────────────

def _get_today_races(target_date: str) -> list[str]:
    """本日の race_id 一覧を返す（発走前のレースのみ）。"""
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        rows = conn.execute(
            "SELECT race_id FROM races WHERE date=? ORDER BY race_id",
            (target_date,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _count_odds_nan(race_ids: list[str]) -> tuple[int, int, int]:
    """
    対象レースの中でオッズ欠損状況を返す。
    (nan_count, total, rtd_count) を返す。
      nan_count : race_results で win_odds が NULL/0 の頭数
      total     : race_results の全頭数
      rtd_count : realtime_odds の件数
    """
    if not race_ids:
        return 0, 1, 0

    conn = sqlite3.connect(str(_DB_PATH))
    try:
        placeholders = ",".join("?" * len(race_ids))

        # realtime_odds テーブルの件数
        rtd_count: int = conn.execute(
            f"SELECT COUNT(*) FROM realtime_odds WHERE race_id IN ({placeholders})",
            race_ids,
        ).fetchone()[0]

        # race_results の win_odds 状況
        total: int = conn.execute(
            f"SELECT COUNT(*) FROM race_results WHERE race_id IN ({placeholders})",
            race_ids,
        ).fetchone()[0]

        nan_count: int = conn.execute(
            f"""
            SELECT COUNT(*) FROM race_results
            WHERE race_id IN ({placeholders})
              AND (win_odds IS NULL OR win_odds = 0)
            """,
            race_ids,
        ).fetchone()[0]

        return nan_count, max(total, 1), rtd_count
    finally:
        conn.close()


def _has_sufficient_odds(race_ids: list[str]) -> bool:
    """オッズが十分に取得できているか判定する。"""
    nan_count, total, rtd_count = _count_odds_nan(race_ids)
    nan_ratio = nan_count / total if total > 0 else 1.0
    logger.info(
        "オッズ状況: race_results=%d NaN=%d (%.0f%%) realtime_odds=%d件",
        total, nan_count, nan_ratio * 100, rtd_count,
    )
    # realtime_odds があれば OK、または race_results の半数以上にオッズあれば OK
    return rtd_count > 0 or nan_ratio < _NAN_RATIO_THRESHOLD


# ── 修復ロジック ─────────────────────────────────────────────────────────────

def _find_py32() -> str | None:
    """32bit Python 実行ファイルを探す。"""
    # py launcher で -3-32 フラグを試す
    try:
        result = subprocess.run(
            ["py", "-3-32", "-c", "import sys; print(sys.version)"],
            capture_output=True, timeout=10, encoding="utf-8",
        )
        if result.returncode == 0:
            return "py"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    for path in _PY32_CANDIDATES:
        if Path(path).exists():
            return path
    return None


def _run_data_sync(target_date: str, py32_cmd: str) -> bool:
    """
    data_sync friday を 32bit Python で実行する。
    target_date は "YYYY-MM-DD" または "YYYYMMDD" どちらでも可。
    Returns True if succeeded.
    """
    # data_sync は YYYYMMDD 形式を要求
    date_compact = target_date.replace("-", "")
    cmd = [py32_cmd]
    if py32_cmd == "py":
        cmd += ["-3-32"]
    cmd += ["-m", "src.ops.data_sync", "friday", "--date", date_compact]

    logger.info("data_sync 実行: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            cwd=str(_ROOT),
            timeout=_SYNC_TIMEOUT,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            logger.info("data_sync 完了 (returncode=0)")
            return True
        else:
            logger.warning("data_sync 異常終了 (returncode=%d)", result.returncode)
            return False
    except subprocess.TimeoutExpired:
        logger.error("data_sync タイムアウト (%d 秒)", _SYNC_TIMEOUT)
        return False
    except Exception as exc:
        logger.error("data_sync 実行エラー: %s", exc)
        return False


def _kill_jvlink() -> bool:
    """JVLinkAgent.exe を強制終了する。Returns True if any process was killed."""
    targets = ["JVLinkAgent.exe", "JVLink.exe", "JVDTLA.exe"]
    killed = False
    for proc_name in targets:
        try:
            result = subprocess.run(
                ["taskkill", "/F", "/IM", proc_name],
                capture_output=True,
                encoding="cp932",
                errors="replace",
                timeout=15,
            )
            if result.returncode == 0:
                logger.info("taskkill 成功: %s", proc_name)
                killed = True
            else:
                logger.debug("taskkill 対象なし: %s", proc_name)
        except Exception as exc:
            logger.warning("taskkill エラー (%s): %s", proc_name, exc)
    return killed


def _restart_jvlink_agent() -> bool:
    """JVLinkAgent.exe を再起動する（サービスとして登録されている場合）。"""
    # Windows サービスとして登録されている場合
    for svc_name in ["JVLinkAgent", "JVDTLA"]:
        try:
            result = subprocess.run(
                ["net", "start", svc_name],
                capture_output=True,
                encoding="cp932",
                errors="replace",
                timeout=30,
            )
            if result.returncode == 0:
                logger.info("サービス再起動成功: %s", svc_name)
                return True
        except Exception:
            pass

    # サービスでない場合はパスから起動を試みる
    agent_paths = [
        r"C:\Program Files\JRA-VAN\JVLinkAgent\JVLinkAgent.exe",
        r"C:\Program Files (x86)\JRA-VAN\JVLinkAgent\JVLinkAgent.exe",
    ]
    for path in agent_paths:
        if Path(path).exists():
            try:
                subprocess.Popen([path])
                logger.info("JVLinkAgent 起動: %s", path)
                return True
            except Exception as exc:
                logger.warning("JVLinkAgent 起動失敗 (%s): %s", path, exc)
    return False


def _soft_repair(target_date: str, py32_cmd: str, race_ids_to_fix: list[str]) -> int:
    """
    ソフト修復: netkeiba オッズ API から直接 Backfill する。
    RTD が死んでいても netkeiba から取得できれば復旧可能。
    Returns: 保存できたレース数
    """
    logger.info("▶ ソフト修復開始: netkeiba Backfill (%s, %d レース)", target_date, len(race_ids_to_fix))
    _discord(
        f"⚠️ **UMALOGI watchdog**: オッズ欠損を検知しました。\n"
        f"修復プロセスを開始します... (netkeiba backfill / {target_date})"
    )
    return _backfill_odds_from_netkeiba(race_ids_to_fix)


def _backfill_odds_from_netkeiba(race_ids: list[str]) -> int:
    """
    netkeiba オッズ API から各レースのオッズを取得して realtime_odds に保存する。
    Returns: 保存に成功したレース数
    """
    from src.scraper.entry_table import fetch_realtime_odds
    from src.database.init_db import insert_realtime_odds, init_db

    conn = init_db()
    saved_races = 0

    for race_id in race_ids:
        try:
            nb_odds = fetch_realtime_odds(race_id, delay=0.5, max_retries=2)
            if not nb_odds or not any(o.win_odds for o in nb_odds):
                logger.warning("netkeiba: オッズ取得なし (race_id=%s)", race_id)
                continue

            # entries から name_map を構築
            name_map: dict[int, str] = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT horse_number, horse_name FROM entries WHERE race_id=?",
                    (race_id,),
                ).fetchall()
            }

            # HorseOdds は place_odds_min/max を持つため insert_realtime_odds と完全互換
            n = insert_realtime_odds(conn, race_id, nb_odds, name_map)
            if n > 0:
                logger.info("Backfill 成功 [netkeiba]: %d 頭保存 (race_id=%s)", n, race_id)
                saved_races += 1
            time.sleep(0.3)  # レート制限対策
        except Exception as exc:
            logger.warning("Backfill 失敗 (race_id=%s): %s", race_id, exc)

    conn.close()
    return saved_races


def _hard_repair(target_date: str, py32_cmd: str, race_ids_to_fix: list[str]) -> bool:
    """
    ハード修復: JVLinkAgent を強制終了 → 再起動 → netkeiba Backfill 実行。
    Returns True if repair succeeded.
    """
    logger.warning("▶ ハード修復開始: JVLinkAgent をリセットします")
    _discord(
        f"🔴 **UMALOGI watchdog**: ソフト修復失敗。\n"
        f"JVLinkAgent をハードリセット後、netkeiba から補完します... ({target_date})"
    )

    killed = _kill_jvlink()
    if killed:
        logger.info("JVLink kill 完了。%d 秒待機中...", _JVLINK_KILL_WAIT)
        time.sleep(_JVLINK_KILL_WAIT)

    restarted = _restart_jvlink_agent()
    if restarted:
        logger.info("JVLinkAgent 再起動完了。30 秒待機中（初期化待ち）...")
        time.sleep(30)
    else:
        logger.warning("JVLinkAgent 再起動スキップ。5 秒待機...")
        time.sleep(5)

    # JVLink 再起動後まず data_sync で SE オッズを試みる
    _run_data_sync(target_date, py32_cmd)
    time.sleep(5)

    # それでも不足なら netkeiba で補完
    saved = _backfill_odds_from_netkeiba(race_ids_to_fix)
    return saved > 0


def _rerun_prerace_for_races(race_ids: list[str]) -> int:
    """オッズ修復後に該当レースの直前予想を再実行する。"""
    from src.main_pipeline import prerace_pipeline

    # まだ発走前のレースのみ対象（発走済みは除外）
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        now_str = datetime.now().strftime("%H:%M")
        # race_ids のうち締め切りが過ぎていないものを特定
        pending = []
        for rid in race_ids:
            row = conn.execute(
                "SELECT post_time FROM races WHERE race_id=?", (rid,)
            ).fetchone()
            if row and row[0]:
                if row[0] > now_str:
                    pending.append(rid)
            else:
                pending.append(rid)  # post_time 不明はとりあえず実行
    finally:
        conn.close()

    if not pending:
        logger.info("全レース発走済み — 予想再実行スキップ")
        return 0

    logger.info("予想再実行: %d レース", len(pending))
    ok_count = 0
    for rid in pending[:6]:  # 上限 6 レース（負荷対策）
        try:
            result = prerace_pipeline(rid, provisional=False)
            if not result.get("error"):
                ev = len(result.get("ev_recommend", []))
                logger.info("再予想完了 %s: EV推奨=%d件", rid, ev)
                ok_count += 1
        except Exception as exc:
            logger.warning("再予想失敗 %s: %s", rid, exc)
    return ok_count


# ── メインループ ─────────────────────────────────────────────────────────────

def _check_and_repair(target_date: str, py32_cmd: str, repair_counts: dict[str, int]) -> None:
    """1 回のチェックサイクルを実行する。"""
    race_ids = _get_today_races(target_date)
    if not race_ids:
        logger.info("本日のレースなし (date=%s) — スキップ", target_date)
        return

    # 現在時刻確認（レース開催時間外は軽量チェックのみ）
    now_h = datetime.now().hour
    if now_h < 8 or now_h >= 17:
        logger.debug("開催時間外 (%d 時) — オッズチェックスキップ", now_h)
        return

    if _has_sufficient_odds(race_ids):
        logger.info("オッズ正常 — 修復不要")
        repair_counts["consecutive_ok"] = repair_counts.get("consecutive_ok", 0) + 1
        return

    repair_counts["consecutive_ok"] = 0
    total_repairs = repair_counts.get("total", 0)

    # 連続修復試行回数チェック（1日あたり最大20回でAlertのみ）
    if total_repairs >= 20 and repair_counts.get("over_limit_notified") != target_date:
        repair_counts["over_limit_notified"] = target_date
        logger.warning("修復試行 %d 回超過 — 継続しますが Discord に警告を送ります", total_repairs)
        _discord(
            f"⚠️ **UMALOGI watchdog**: 自動修復を {total_repairs} 回試みています。\n"
            "継続監視中ですが、JVLink や netkeiba への接続を手動確認することを推奨します。"
        )

    repair_counts["total"] = total_repairs + 1

    # オッズ未取得レースを特定
    races_nan: list[str] = []
    conn_tmp = sqlite3.connect(str(_DB_PATH))
    try:
        for rid in race_ids:
            rtd = conn_tmp.execute(
                "SELECT COUNT(*) FROM realtime_odds WHERE race_id=?", (rid,)
            ).fetchone()[0]
            if rtd == 0:
                races_nan.append(rid)
    finally:
        conn_tmp.close()

    logger.info("オッズ未取得レース: %d / %d 件", len(races_nan), len(race_ids))

    # ソフト修復: netkeiba 直接 Backfill
    saved = _soft_repair(target_date, py32_cmd, races_nan)
    if saved > 0:
        logger.info("ソフト修復: %d レース Backfill 完了", saved)
        if _has_sufficient_odds(race_ids):
            ok_races = _rerun_prerace_for_races(race_ids)
            _discord(
                f"🛠️ **UMALOGI watchdog**: JRA-VAN 欠損を検知。\n"
                f"netkeiba より全 **{saved} レース**のオッズを補完し、予測を「確定」に更新しました。\n"
                f"({target_date} / 再予想: {ok_races} R)"
            )
            return

    # ハード修復: JVLinkAgent リセット + netkeiba Backfill
    logger.warning("ソフト修復後もオッズ欠損継続 — ハード修復に移行")
    hard_ok = _hard_repair(target_date, py32_cmd, races_nan)
    time.sleep(_HARD_RETRY_WAIT)

    if _has_sufficient_odds(race_ids):
        ok_races = _rerun_prerace_for_races(race_ids)
        _discord(
            f"🛠️ **UMALOGI watchdog**: JVLink ハードリセット後、\n"
            f"netkeiba より全 **{len(races_nan)} レース**のオッズを補完しました。\n"
            f"({target_date} / 再予想: {ok_races} R)"
        )
        return

    logger.error("ハード修復後もオッズ欠損継続 — 次のチェックサイクルで再試行します")
    _discord(
        f"⚠️ **UMALOGI watchdog**: ハードリセット後もオッズ未回復。\n"
        f"再試行中 (試行 {total_repairs + 1}/6)..."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI オッズ欠損監視・自動修復デーモン")
    parser.add_argument("--interval", type=int, default=5, help="チェック間隔（分, デフォルト=5）")
    parser.add_argument("--once", action="store_true", help="1 回チェックして終了")
    parser.add_argument("--date", default=None, help="監視対象日 YYYYMMDD (デフォルト: 今日)")
    args = parser.parse_args()

    target_date_raw = args.date or date.today().strftime("%Y%m%d")
    target_date = f"{target_date_raw[:4]}-{target_date_raw[4:6]}-{target_date_raw[6:8]}"

    py32_cmd = _find_py32()
    if py32_cmd is None:
        logger.error("32bit Python が見つかりません — ソフト/ハード修復は無効")
        _discord(
            "🆘 **UMALOGI watchdog**: 32bit Python が見つかりません。\n"
            "JVLink 修復機能が無効です。手動確認をお願いします。"
        )
    else:
        logger.info("32bit Python: %s", py32_cmd)

    interval_sec = args.interval * 60
    repair_counts: dict[str, int] = {"total": 0, "consecutive_ok": 0}

    logger.info(
        "watchdog 起動: 対象日=%s チェック間隔=%d 分 32bitPy=%s",
        target_date, args.interval, py32_cmd or "N/A",
    )
    if not args.once:
        _discord(
            f"🐕 **UMALOGI watchdog** 起動しました。\n"
            f"監視対象: {target_date} / チェック間隔: {args.interval} 分"
        )

    try:
        while not _shutdown_flag:
            logger.info("── チェック開始 ──")
            try:
                _check_and_repair(target_date, py32_cmd or "py", repair_counts)
            except Exception as exc:
                logger.error("チェックサイクル例外: %s", exc, exc_info=True)

            if args.once:
                break

            # 次のチェックまでスリープ（シャットダウン検知しながら）
            remaining = interval_sec
            while remaining > 0 and not _shutdown_flag:
                time.sleep(min(5, remaining))
                remaining -= 5

    finally:
        logger.info("watchdog 終了")
        _discord("🐕 **UMALOGI watchdog** が停止しました。")


if __name__ == "__main__":
    main()
