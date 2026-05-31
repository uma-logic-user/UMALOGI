"""
src/notification/prerace_alert.py — 発走15分前アラート

土日レース日に、各レースの発走15分前に EV >= 閾値 の予想を
Discord ev_alert チャンネルへ @everyone 付きで通知する。

重複通知防止: in-memory set で当日既通知の race_id を管理する。
日付変わりでリセット。
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── パス ────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_DB_PATH = _ROOT / "data" / "umalogi.db"

# ── 発走推定式（today_auto_runner.py と同一定数） ────────────────────────────
_R1_HOUR = 10
_R1_MINUTE = 0
_INTERVAL_MIN = 30

# ── アラート設定 ──────────────────────────────────────────────────────────────
# 環境変数 PRERACE_ALERT_EV_THRESHOLD で上書き可能（デフォルト 1.2）
PRERACE_ALERT_EV_THRESHOLD: float = float(
    os.environ.get("PRERACE_ALERT_EV_THRESHOLD", "1.2")
)
# 14〜16分前の範囲を1分毎チェックで捕捉（±1分のウィンドウ）
_ALERT_TARGET_MIN: int = 15
_ALERT_WINDOW_MIN: int = 1

# ── 重複通知防止 ──────────────────────────────────────────────────────────────
_notified_races: set[str] = set()
_notified_date: str = ""


def _reset_if_new_day(today_iso: str | None = None) -> None:
    """日付が変わった場合に通知済みセットをリセットする。

    Args:
        today_iso: 'YYYY-MM-DD' 形式の日付（テスト用上書き）。省略時は date.today()。
    """
    global _notified_date
    today = today_iso or date.today().isoformat()
    if today != _notified_date:
        _notified_races.clear()  # オブジェクト同一性を保ちつつクリア
        _notified_date = today


def _estimate_start(race_date: str, race_number: int) -> datetime:
    """race_number から発走推定時刻を計算する（R1=10:00, +30分/レース）。

    Args:
        race_date: 'YYYY/MM/DD' または 'YYYY-MM-DD' 形式の日付。
        race_number: レース番号（1〜12）。

    Returns:
        推定発走時刻。
    """
    d = race_date.replace("/", "-")
    base_date = date.fromisoformat(d)
    base = datetime(base_date.year, base_date.month, base_date.day, _R1_HOUR, _R1_MINUTE)
    return base + timedelta(minutes=(race_number - 1) * _INTERVAL_MIN)


def _format_stake(recommended_bet: float | None) -> str:
    """推奨投資額を '¥N,NNN' 形式（100円単位）で返す。値が0以下なら '—'。"""
    if not recommended_bet or recommended_bet <= 0:
        return "—"
    rounded = int(recommended_bet // 100) * 100
    return f"¥{rounded:,}"


def _build_alert_message(
    race_id: str,
    race_info: dict,
    preds: list[dict],
    start_time: datetime,
) -> str:
    """Discord に送信するアラートメッセージ本文を組み立てる。

    Args:
        race_id: レース ID。
        race_info: races テーブルの行辞書。
        preds: predictions テーブルの行辞書リスト（EV降順・上位5件以内）。
        start_time: 推定発走時刻。

    Returns:
        Discord メッセージ本文。
    """
    venue = race_info.get("venue", "")
    race_number = race_info.get("race_number", 0)
    race_name = race_info.get("race_name", "")

    label = f"{venue} {race_number}R"
    if race_name:
        label += f"（{race_name}）"

    max_ev = max(p["expected_value"] for p in preds)

    lines = []
    for p in preds:
        bet_type = p.get("bet_type", "")
        model = p.get("model_type", "")
        ev = p.get("expected_value", 0.0)
        stake = _format_stake(p.get("recommended_bet"))
        lines.append(f"・{model} **{bet_type}**  EV={ev:.2f}  推奨投資: {stake}")

    bet_lines = "\n".join(lines)
    return (
        f"@everyone\n\n"
        f"🔥 **[{label}] 発走15分前アラート**\n"
        f"📅 発走予定: {start_time.strftime('%H:%M')}\n\n"
        f"**EV ≥ {PRERACE_ALERT_EV_THRESHOLD:.1f}** の買い目 **{len(preds)}件**:\n"
        f"{bet_lines}\n\n"
        f"`{race_id}` ← ダッシュボードで確認してください。"
    )


def check_and_send_prerace_alerts(
    db_path: str | Path | None = None,
    now: datetime | None = None,
) -> int:
    """発走15分前のレースに対して EV >= 閾値 の予想を Discord へ通知する。

    scheduler の毎分ジョブから呼び出されることを想定している。
    土日以外でも動作するが、DB にレースがなければ何もしない。

    Args:
        db_path: SQLite パス（テスト用上書き）。省略時は _DB_PATH。
        now: 現在時刻（テスト用上書き）。省略時は datetime.now()。

    Returns:
        送信した通知件数。
    """
    current = now if now is not None else datetime.now()
    today_str = current.date().isoformat()  # races.date は YYYY-MM-DD 形式で保存
    _reset_if_new_day(today_iso=current.date().isoformat())

    db = Path(db_path) if db_path else _DB_PATH

    sent_count = 0

    try:
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            "SELECT race_id, race_name, venue, race_number, date FROM races WHERE date = ?",
            (today_str,),
        ).fetchall()

        for row in rows:
            race_id: str = row["race_id"]
            if race_id in _notified_races:
                continue

            race_number: int = row["race_number"]
            race_date: str = row["date"]
            start = _estimate_start(race_date, race_number)
            minutes_until = (start - current).total_seconds() / 60

            lo = _ALERT_TARGET_MIN - _ALERT_WINDOW_MIN
            hi = _ALERT_TARGET_MIN + _ALERT_WINDOW_MIN
            if not (lo <= minutes_until < hi):
                continue

            # EV >= 閾値 の予想を取得（EV降順・上位5件）
            preds = conn.execute(
                """
                SELECT model_type, bet_type, expected_value, recommended_bet
                FROM predictions
                WHERE race_id = ? AND expected_value >= ?
                ORDER BY expected_value DESC
                LIMIT 5
                """,
                (race_id, PRERACE_ALERT_EV_THRESHOLD),
            ).fetchall()

            if not preds:
                logger.debug(
                    "[発走前アラート] %s %sR: EV>=%.1f の予想なし → スキップ",
                    row["venue"], race_number, PRERACE_ALERT_EV_THRESHOLD,
                )
                continue

            pred_list = [dict(p) for p in preds]
            msg = _build_alert_message(race_id, dict(row), pred_list, start)
            max_ev = max(p["expected_value"] for p in pred_list)

            _dispatch_alert(race_id, max_ev, msg)
            _notified_races.add(race_id)
            sent_count += 1
            logger.info(
                "[発走前アラート] %s %sR → %d件通知 (max_ev=%.2f)",
                row["venue"], race_number, len(pred_list), max_ev,
            )

        conn.close()
    except Exception as exc:
        logger.error("[発走前アラート] エラー: %s", exc, exc_info=True)

    return sent_count


def _dispatch_alert(race_id: str, max_ev: float, message: str) -> None:
    """NotificationRouter 経由で ev_alert チャンネルへ送信する。

    ev_alert チャンネルが未設定の場合は prediction チャンネルへフォールバック。
    """
    try:
        from src.notification.router import NotificationRouter
        router = NotificationRouter()
        router.notify_prerace_15min(race_id=race_id, max_ev=max_ev, message=message)
    except Exception as exc:
        logger.error("[発走前アラート] Discord 送信失敗: %s", exc)
