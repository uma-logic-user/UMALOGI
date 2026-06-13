"""
当日の発走前レースのオッズを定期取得し realtime_odds に時系列スナップショットを蓄積する。

【P0-1 単一ソース統一・2026-05-31】
  旧実装は realtime_odds を odds_timeseries へ「コピー」するだけで独立取得が無く、
  コピー元が薄いと時系列が育たない／空になる問題があった。
  本実装は **realtime_odds を唯一の真実のソース** とし、発走前のレースに対して
  fetch_and_save_odds() を実際に呼んで新しいスナップショットを追記する。
  これによりオッズ歪み検知(odds_drift)・モメンタム(odds_momentum)が
  「最低2点」を確実に得られる（朝暫定 + 定期取得 + 直前）。

scheduler から数分間隔で呼ばれる（発走前ウィンドウのみ実取得）。

使用方法:
    py scripts/record_odds_timeseries.py            # 当日の発走前レースを取得
    py scripts/record_odds_timeseries.py 2026...    # 特定 race_id のみ
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# W-086: スクリプト直接実行時は sys.path[0] = scripts/ になり `import src` が
# ModuleNotFoundError で常時失敗していた（W-080 レコーダーが一度も走れない）。
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

# 発走何分前から取得を開始するか（このウィンドウ内のレースのみ実取得）
_CAPTURE_AHEAD_MIN = 80
# 発走後この分までは「まだ取りこぼし得る」として取得を許容
_CAPTURE_AFTER_MIN = 2
# R1=10:00 + 30分間隔（post_time 未取得時のフォールバック推定）
_R1_HOUR = 10
_INTERVAL_MIN = 30


def _resolve_start(date_str: str, race_number: int, post_time: str) -> datetime:
    """races.post_time（HH:MM）優先で発走時刻を返す。無ければ推定。"""
    base = datetime.strptime(date_str, "%Y-%m-%d")
    pt = (post_time or "").strip()
    if len(pt) >= 4 and ":" in pt:
        try:
            hh, mm = pt.split(":")
            return base.replace(hour=int(hh), minute=int(mm))
        except (ValueError, TypeError):
            pass
    return base.replace(hour=_R1_HOUR) + timedelta(
        minutes=(race_number - 1) * _INTERVAL_MIN
    )


def capture_today_odds(race_id: str | None = None) -> int:
    """当日の発走前レースに対し fetch_and_save_odds で realtime_odds を更新する。

    Args:
        race_id: 指定時はそのレースのみ。None なら当日の発走前ウィンドウ内全レース。

    Returns:
        スナップショットを取得（試行成功）したレース数。
    """
    from src.database.init_db import init_db
    from src.pipeline.scraping import fetch_and_save_odds

    conn = init_db()
    try:
        today = date.today().isoformat()
        if race_id:
            rows = conn.execute(
                "SELECT race_id, date, race_number, COALESCE(post_time,'') "
                "FROM races WHERE race_id = ?",
                (race_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT race_id, date, race_number, COALESCE(post_time,'') "
                "FROM races WHERE date = ? ORDER BY race_id",
                (today,),
            ).fetchall()

        now = datetime.now()
        captured = 0
        for rid, d, rno, ptime in rows:
            start = _resolve_start(d or today, int(rno), ptime)
            # 発走 _CAPTURE_AHEAD_MIN 分前〜発走 +_CAPTURE_AFTER_MIN 分のレースのみ実取得
            if race_id is None:
                if now < start - timedelta(minutes=_CAPTURE_AHEAD_MIN):
                    continue
                if now > start + timedelta(minutes=_CAPTURE_AFTER_MIN):
                    continue
            try:
                n = fetch_and_save_odds(conn, rid)
                captured += 1
                logger.info(
                    "オッズ時系列取得: race_id=%s %d頭 (発走 %s)",
                    rid,
                    n,
                    start.strftime("%H:%M"),
                )
            except Exception as exc:  # noqa: BLE001 — 1レースの失敗で全体を止めない
                logger.warning("オッズ取得失敗 race_id=%s: %s", rid, exc)

        if captured == 0:
            logger.debug("発走前ウィンドウ内のレースなし (%s)", today)
        return captured
    finally:
        conn.close()


def main() -> int:
    rid = sys.argv[1] if len(sys.argv) > 1 else None
    return capture_today_odds(rid)


if __name__ == "__main__":
    main()
