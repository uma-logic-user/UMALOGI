"""
UMALOGI X（Twitter）用投稿文生成 → Discord 一括転送スクリプト

使用方法:
    py scripts/post_x_drafts_to_discord.py --webhook URL [--date 20260524] [--dry-run]

処理フロー:
    1. DB から本日のレース + EV 推奨買い目を取得
    2. 各レースの X 用投稿文を 280 字以内で生成（ハッシュタグ付き）
    3. 指定 Discord Webhook へ転送
    4. 完了通知
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import requests

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 定数
# ──────────────────────────────────────────────
_MAX_X_CHARS = 280
_IMPORTANT_EV_THRESHOLD = 5.0   # 【重要推奨】を付与する最高EV閾値
_MIN_POST_EV = 1.0               # Discord 転送対象の最低EV
_DISCORD_RATE_LIMIT_SEC = 1.0   # Discord レート制限対策（秒）


def _fmt_combos(combinations: list[list[int]], max_show: int = 3) -> str:
    """買い目リストを短縮表記する（例: 4-1-2/4-2-1/...）。"""
    shown = ["-".join(map(str, c)) for c in combinations[:max_show]]
    suffix = f"他{len(combinations) - max_show}点" if len(combinations) > max_show else ""
    return "/".join(shown) + (f" {suffix}" if suffix else "")


def _build_x_post(
    race_name: str,
    venue: str,
    race_number: int,
    bets: list[dict],
    max_ev: float,
) -> str:
    """X 用投稿文を 280 字以内で生成する。"""
    is_important = max_ev >= _IMPORTANT_EV_THRESHOLD
    prefix = "【重要推奨】\n" if is_important else ""

    header = f"{prefix}📊 {venue}{race_number}R / {race_name}\n\n"
    body_lines: list[str] = ["統計アルゴリズム選別の期待値推奨買い目："]

    # EV>=_MIN_POST_EV の上位3買い目を記載
    ev_bets = sorted(
        [b for b in bets if (b.get("expected_value") or 0) >= _MIN_POST_EV],
        key=lambda b: b.get("expected_value") or 0,
        reverse=True,
    )[:3]

    for bet in ev_bets:
        bet_type = bet["bet_type"]
        ev = bet.get("expected_value") or 0.0
        combos = bet.get("combinations") or []
        recommended = int(bet.get("recommended_bet") or 100)
        combo_str = _fmt_combos(combos)
        body_lines.append(f"▶ {bet_type}: {combo_str}（EV={ev:.1f}）¥{recommended:,}")

    # ハッシュタグ
    hashtags = (
        f"#競馬予想 #期待値アルゴリズム "
        f"#{race_name.replace(' ', '')} #JRA "
        f"#{venue}R{race_number}"
    )
    footer = f"\n{hashtags}"

    body = "\n".join(body_lines)
    full = header + body + footer

    # 280字オーバー時は本文を短縮
    if len(full) > _MAX_X_CHARS:
        # body_lines を削って調整
        while len(header + "\n".join(body_lines) + footer) > _MAX_X_CHARS and len(body_lines) > 1:
            body_lines.pop()
        body = "\n".join(body_lines)
        full = header + body + footer

    return full


def _build_discord_message(
    race_name: str,
    venue: str,
    race_number: int,
    x_post: str,
    max_ev: float,
    race_id: str,
) -> dict:
    """Discord 送信用の embed を構築する。"""
    is_important = max_ev >= _IMPORTANT_EV_THRESHOLD
    color = 0xFFD700 if is_important else 0x00BFFF  # 重要=ゴールド / 通常=シアン

    embed: dict = {
        "title": f"{'🔥' if is_important else '🏇'} {venue}{race_number}R / {race_name}",
        "description": (
            "```\n"
            f"{x_post}\n"
            "```\n"
            "⬆️ X（Twitter）用投稿文（コピーしてご利用ください）\n\n"
            "📝 Note下書き: 本日の予想記事に含まれます"
        ),
        "color": color,
        "footer": {
            "text": f"race_id: {race_id} | maxEV: {max_ev:.2f} | UMALOGI AI"
        },
    }
    return {"embeds": [embed]}


def _send_to_discord(
    webhook_url: str,
    payload: dict,
    dry_run: bool = False,
) -> bool:
    """Discord Webhook に POST する。"""
    if dry_run:
        logger.info("[DRY-RUN] Discord POST: %s", str(payload)[:120])
        return True
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            return True
        logger.warning("Discord HTTP %d: %s", resp.status_code, resp.text[:200])
        return False
    except Exception as exc:
        logger.error("Discord 送信エラー: %s", exc)
        return False


def _fetch_today_races(conn, date_str: str) -> list[dict]:
    """本日のレース一覧 + 最高EV を取得する。"""
    formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    rows = conn.execute(
        """
        SELECT r.race_id, r.race_name, r.venue, r.race_number,
               MAX(p.expected_value) as max_ev
        FROM races r
        LEFT JOIN predictions p ON r.race_id = p.race_id
            AND p.bet_type != '馬分析'
        WHERE r.date = ?
        GROUP BY r.race_id
        HAVING max_ev >= ?
        ORDER BY r.race_id
        """,
        (formatted, _MIN_POST_EV),
    ).fetchall()
    return [
        {
            "race_id": row[0],
            "race_name": row[1],
            "venue": row[2],
            "race_number": row[3],
            "max_ev": row[4] or 0.0,
        }
        for row in rows
    ]


def _fetch_bets_for_race(conn, race_id: str) -> list[dict]:
    """DB からレースの推奨買い目を取得する。"""
    rows = conn.execute(
        """
        SELECT bet_type, combination_json, expected_value, recommended_bet, notes, model_type
        FROM predictions
        WHERE race_id = ?
          AND bet_type != '馬分析'
          AND expected_value >= ?
        ORDER BY expected_value DESC
        LIMIT 5
        """,
        (race_id, _MIN_POST_EV),
    ).fetchall()
    result: list[dict] = []
    for row in rows:
        try:
            combos = json.loads(row[1] or "[]")
        except Exception:
            combos = []
        result.append(
            {
                "bet_type": row[0],
                "combinations": combos,
                "expected_value": row[2] or 0.0,
                "recommended_bet": row[3] or 100,
                "notes": row[4] or "",
                "model_type": row[5],
            }
        )
    return result


def run(date_str: str, webhook_url: str, dry_run: bool = False) -> None:
    """メイン処理: 全レース X 投稿文生成 → Discord 転送。"""
    conn = init_db()
    races = _fetch_today_races(conn, date_str)

    if not races:
        logger.warning("対象日 %s に EV>=%.1f のレースが見つかりません", date_str, _MIN_POST_EV)
        conn.close()
        return

    logger.info("処理対象: %d レース (date=%s)", len(races), date_str)

    success_count = 0
    important_races: list[str] = []

    for race in races:
        race_id = race["race_id"]
        race_name = race["race_name"]
        venue = race["venue"]
        race_number = race["race_number"]
        max_ev = race["max_ev"]

        bets = _fetch_bets_for_race(conn, race_id)
        if not bets:
            logger.debug("買い目なし: %s", race_id)
            continue

        # X 用投稿文生成
        x_post = _build_x_post(race_name, venue, race_number, bets, max_ev)

        # Discord embed 構築
        payload = _build_discord_message(race_name, venue, race_number, x_post, max_ev, race_id)

        # 送信
        ok = _send_to_discord(webhook_url, payload, dry_run=dry_run)
        if ok:
            success_count += 1
            if max_ev >= _IMPORTANT_EV_THRESHOLD:
                important_races.append(f"{venue}R{race_number}({race_name})")
            logger.info(
                "転送完了: %s %sR%d EV=%.2f %s",
                race_id, venue, race_number, max_ev,
                "⭐重要" if max_ev >= _IMPORTANT_EV_THRESHOLD else "",
            )
        else:
            logger.warning("転送失敗: %s", race_id)

        # レート制限対策
        if not dry_run:
            time.sleep(_DISCORD_RATE_LIMIT_SEC)

    conn.close()

    # 完了通知
    completion_msg: dict = {
        "content": (
            f"✅ **本日の全レース分、Note下書き投稿およびX用投稿文の連携が完了しました**\n"
            f"📅 対象日: {date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}\n"
            f"📤 転送済み: **{success_count}件** / {len(races)}件\n"
            f"🔥 重要推奨レース(EV≥{_IMPORTANT_EV_THRESHOLD:.0f}): **{len(important_races)}件**\n"
            + (
                "  " + " / ".join(important_races[:8]) + ("\n  ..." if len(important_races) > 8 else "")
                if important_races else ""
            )
        )
    }
    _send_to_discord(webhook_url, completion_msg, dry_run=dry_run)
    logger.info("=== 完了: %d / %d レース転送済み ===", success_count, len(races))


def main() -> None:
    parser = argparse.ArgumentParser(description="X用投稿文生成 → Discord 一括転送")
    parser.add_argument("--webhook", required=True, help="Discord Webhook URL")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（省略時=本日）")
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信を行わない")
    args = parser.parse_args()

    date_str = args.date or date.today().strftime("%Y%m%d")
    logger.info("=== X投稿文生成 → Discord転送 date=%s dry_run=%s ===", date_str, args.dry_run)
    run(date_str=date_str, webhook_url=args.webhook, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
