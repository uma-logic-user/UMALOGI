"""
scripts/self_healing_monitor.py — UMALOGI 自律型データ品質監視・自己修復エンジン
=================================================================================

5 分ごとに以下を監視し、異常を検知した場合は自動修復する。

【監視項目】
  1. 文字化け検出: races.race_name に "?" パターン、または空白
  2. メタデータ欠損: races.distance = 0 または surface = ''
  3. 予想欠損: 当日レースのうち predictions が 0 件のレース

【自己修復アクション】
  - 文字化け / メタデータ欠損 → repair_race_data.py を自動実行
  - 予想欠損 → src.main_pipeline prerace を自動実行

Usage:
    py scripts/self_healing_monitor.py             # 5 分ループで常駐
    py scripts/self_healing_monitor.py --once      # 1 回だけチェック（デバッグ用）
    py scripts/self_healing_monitor.py --interval 3 # チェック間隔を変更（分）
    py scripts/self_healing_monitor.py --date 20260510  # 対象日を指定
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
from datetime import date, datetime
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
logger = logging.getLogger("self_heal")

# ── 定数 ───────────────────────────────────────────────────────────────────

_DB_PATH            = _ROOT / "data" / "umalogi.db"
_REPAIR_SCRIPT      = _ROOT / "scripts" / "repair_race_data.py"
_SYNC_SCRIPT        = _ROOT / "scripts" / "data_sync.py"
_PIPELINE_CMD       = [sys.executable, "-m", "src.main_pipeline", "prerace"]
_REPAIR_CMD_BASE    = [sys.executable, str(_REPAIR_SCRIPT)]
_SYNC_CMD_BASE      = [sys.executable, str(_SYNC_SCRIPT)]

_REPAIR_TIMEOUT     = 300   # repair_race_data タイムアウト（秒）
_PIPELINE_TIMEOUT   = 120   # prerace pipeline タイムアウト（秒）
_COOLDOWN_SECS      = 180   # 同一 race_id への連続修復間隔（秒）

# 文字化けパターン（"?" が1文字以上続く、または制御文字）
import re
_GARBLED_PATTERN = re.compile(r'\?[^\s\?]{0,4}\?|^\s*$')

_shutdown_flag = False


def _signal_handler(sig: int, frame: object) -> None:
    global _shutdown_flag
    logger.info("シグナル %d 受信 — 次のループ後に終了します", sig)
    _shutdown_flag = True


signal.signal(signal.SIGINT,  _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ── Discord 通知 ───────────────────────────────────────────────────────────

def _discord(msg: str, *, channel: str = "system") -> None:
    """Discord にメッセージ送信。channel='system' または 'pred'。"""
    if channel == "system":
        webhook_url = (
            os.getenv("DISCORD_SYSTEM_WEBHOOK_URL", "")
            or os.getenv("DISCORD_WEBHOOK_URL", "")
        )
    else:
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")

    if not webhook_url:
        return
    try:
        import requests
        resp = requests.post(webhook_url, json={"content": msg}, timeout=10)
        if resp.status_code not in (200, 204):
            logger.warning("Discord 送信失敗: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord 送信エラー: %s", exc)


# ── DB ヘルパー ────────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _is_garbled(name: str | None) -> bool:
    if not name or name.strip() == "":
        return True
    return bool(_GARBLED_PATTERN.search(name))


# ── 監視ロジック ───────────────────────────────────────────────────────────

class AnomalyReport:
    def __init__(self) -> None:
        self.garbled_races:   list[str] = []   # race_id
        self.metadata_races:  list[str] = []   # distance=0 or surface=''
        self.no_pred_races:   list[str] = []   # predictions が 0 件

    @property
    def has_anomaly(self) -> bool:
        return bool(self.garbled_races or self.metadata_races or self.no_pred_races)


def check_today(target_date: str) -> AnomalyReport:
    """当日データの異常を検出して AnomalyReport を返す。"""
    report = AnomalyReport()
    conn = _get_conn()
    try:
        # 今日のレース一覧
        races = conn.execute(
            """
            SELECT r.race_id, r.race_name, r.distance, r.surface
            FROM races r WHERE r.date = ?
            ORDER BY r.race_id
            """,
            (target_date,),
        ).fetchall()

        if not races:
            logger.debug("対象日にレースなし: %s", target_date)
            return report

        race_ids = [r[0] for r in races]

        # ① 文字化け / レース名空白
        for race_id, race_name, dist, surf in races:
            if _is_garbled(race_name):
                report.garbled_races.append(race_id)
            elif (dist is None or dist == 0) or (surf is None or surf == ""):
                report.metadata_races.append(race_id)

        # ② 予想欠損（predictions が 0 件）
        pred_counts = {
            r[0]: r[1]
            for r in conn.execute(
                f"""
                SELECT race_id, COUNT(*) FROM predictions
                WHERE race_id IN ({','.join('?'*len(race_ids))})
                GROUP BY race_id
                """,
                race_ids,
            ).fetchall()
        }
        for race_id in race_ids:
            if pred_counts.get(race_id, 0) == 0:
                report.no_pred_races.append(race_id)

    finally:
        conn.close()
    return report


# ── 自己修復アクション ─────────────────────────────────────────────────────

# 直近の修復タイムスタンプ（race_id → timestamp）— 連続修復の throttle
_last_repair: dict[str, float] = {}


def _throttled(race_id: str) -> bool:
    now = time.time()
    last = _last_repair.get(race_id, 0.0)
    if now - last < _COOLDOWN_SECS:
        return True
    _last_repair[race_id] = now
    return False


def heal_metadata(target_date: str, race_ids: list[str]) -> bool:
    """
    repair_race_data.py を呼んで文字化け / メタデータ欠損を修復する。
    Returns: 成功なら True
    """
    # throttle: 日付単位で管理
    key = f"metadata:{target_date}"
    if _throttled(key):
        logger.debug("メタデータ修復スキップ（クールダウン中）: %s", target_date)
        return True

    iso_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    cmd = _REPAIR_CMD_BASE + ["--date", iso_date, "--skip-results"]
    logger.info("🔧 メタデータ修復実行: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            encoding="utf-8", timeout=_REPAIR_TIMEOUT,
            cwd=str(_ROOT),
        )
        if result.returncode == 0:
            logger.info("✅ メタデータ修復完了 (%d件)", len(race_ids))
            return True
        else:
            logger.error("❌ メタデータ修復失敗:\n%s", result.stderr[:500])
            return False
    except subprocess.TimeoutExpired:
        logger.error("❌ メタデータ修復タイムアウト")
        return False


def heal_predictions(race_ids: list[str]) -> tuple[int, int]:
    """
    欠損 race_id に対して prerace pipeline を実行して予想を補完する。
    Returns: (成功数, 失敗数)
    """
    ok = 0
    ng = 0
    for race_id in race_ids:
        if _throttled(race_id):
            logger.debug("予想修復スキップ（クールダウン中）: %s", race_id)
            continue
        cmd = _PIPELINE_CMD + [race_id]
        logger.info("🔧 予想補完実行: %s", race_id)
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                encoding="utf-8", timeout=_PIPELINE_TIMEOUT,
                cwd=str(_ROOT),
            )
            if result.returncode == 0:
                logger.info("✅ 予想補完完了: %s", race_id)
                ok += 1
            else:
                logger.error("❌ 予想補完失敗: %s\n%s", race_id, result.stderr[:300])
                ng += 1
        except subprocess.TimeoutExpired:
            logger.error("❌ 予想補完タイムアウト: %s", race_id)
            ng += 1
    return ok, ng


def heal_data_sync(target_date: str) -> bool:
    """data_sync.py を呼んで JVLink 経由でデータを再同期する（重度の欠損向け）。"""
    key = f"sync:{target_date}"
    if _throttled(key):
        logger.debug("data_sync スキップ（クールダウン中）")
        return True

    cmd = _SYNC_CMD_BASE + ["--date", target_date]
    logger.info("🔧 data_sync 実行: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            encoding="utf-8", timeout=_REPAIR_TIMEOUT,
            cwd=str(_ROOT),
        )
        if result.returncode == 0:
            logger.info("✅ data_sync 完了")
            return True
        else:
            logger.warning("⚠️ data_sync 失敗 (rc=%d)", result.returncode)
            return False
    except subprocess.TimeoutExpired:
        logger.error("❌ data_sync タイムアウト")
        return False


# ── メインループ ───────────────────────────────────────────────────────────

def run_once(target_date: str) -> AnomalyReport:
    """1 回チェックして、異常があれば修復する。結果の report を返す。"""
    logger.info("━━ チェック開始: %s ━━", target_date)
    report = check_today(target_date)

    if not report.has_anomaly:
        logger.info("✅ 異常なし — 全データ正常")
        return report

    # ① 文字化け / メタデータ欠損の修復
    dirty = report.garbled_races + report.metadata_races
    if dirty:
        logger.warning(
            "⚠️ メタデータ異常検知: garbled=%d  metadata=%d",
            len(report.garbled_races), len(report.metadata_races),
        )
        _discord(
            f"⚠️ **[自己修復]** メタデータ異常を検知しました\n"
            f"文字化け: {len(report.garbled_races)}レース\n"
            f"距離/馬場欠損: {len(report.metadata_races)}レース\n"
            f"→ repair_race_data.py を自動実行します"
        )
        success = heal_metadata(target_date, dirty)
        if success:
            _discord(f"✅ **[自己修復完了]** メタデータ修復 {len(dirty)} レース")
        else:
            # フォールバック: data_sync で全体再取得
            logger.warning("repair 失敗 → data_sync にフォールバック")
            _discord("🔄 repair 失敗 → data_sync でフォールバック修復を試みます")
            heal_data_sync(target_date)

    # ② 予想欠損の修復
    if report.no_pred_races:
        logger.warning("⚠️ 予想欠損検知: %d レース", len(report.no_pred_races))
        _discord(
            f"⚠️ **[自己修復]** 予想欠損を検知: {len(report.no_pred_races)}レース\n"
            f"→ prerace pipeline を自動実行します"
        )
        ok, ng = heal_predictions(report.no_pred_races)
        _discord(
            f"✅ **[予想補完]** 完了 成功={ok} 失敗={ng}\n"
            f"対象: {', '.join(report.no_pred_races[:5])}"
            + ("..." if len(report.no_pred_races) > 5 else "")
        )

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 自律型データ品質監視・自己修復エンジン")
    parser.add_argument("--once",     action="store_true", help="1 回だけ実行して終了")
    parser.add_argument("--interval", type=int, default=5, help="チェック間隔（分、デフォルト 5）")
    parser.add_argument("--date",     default=None, help="対象日 YYYYMMDD（省略時=本日）")
    args = parser.parse_args()

    target_date = args.date or date.today().strftime("%Y%m%d")
    interval_secs = args.interval * 60

    logger.info("=" * 60)
    logger.info("UMALOGI Self-Healing Monitor 起動")
    logger.info("対象日: %s  チェック間隔: %d 分", target_date, args.interval)
    logger.info("=" * 60)

    _discord(
        f"🤖 **[UMALOGI Self-Healing Monitor]** 起動しました\n"
        f"対象日: {target_date}  チェック間隔: {args.interval}分\n"
        f"監視項目: 文字化け / メタデータ欠損 / 予想欠損\n"
        f"自動修復: repair_race_data / prerace pipeline"
    )

    if args.once:
        run_once(target_date)
        return

    while not _shutdown_flag:
        try:
            run_once(target_date)
        except Exception as exc:
            logger.exception("チェックループで予期しないエラー: %s", exc)
            _discord(f"❌ **[Self-Heal エラー]** `{exc}`")

        # 翌日になったら自動で日付を更新
        today_str = date.today().strftime("%Y%m%d")
        if today_str != target_date and args.date is None:
            logger.info("日付更新: %s → %s", target_date, today_str)
            target_date = today_str

        logger.info("次のチェックまで %d 秒待機...", interval_secs)
        for _ in range(interval_secs):
            if _shutdown_flag:
                break
            time.sleep(1)

    logger.info("Self-Healing Monitor 終了")
    _discord("🛑 **[Self-Healing Monitor]** 終了しました")


if __name__ == "__main__":
    main()
