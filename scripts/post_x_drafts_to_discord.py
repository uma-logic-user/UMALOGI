"""
UMALOGI X（Twitter）用投稿文生成 → Discord 一括転送スクリプト

使用方法:
    py scripts/post_x_drafts_to_discord.py --webhook URL [--date 20260524] [--dry-run]
    py scripts/post_x_drafts_to_discord.py --webhook URL --note-url https://note.com/yourpage

テンプレート（280 字特化・note 誘導型）:
    【推奨：会場NR】 または 【重要：会場NR】（EV高）
    AIによる期待値選別結果：EV XX.XX
    市場の歪みを捉えた勝算のある買い目のみを抽出しました。
    詳細はnoteで公開中。
    買い目・推奨理由はコチラ👇
    https://note.com/...
    #競馬予想 #期待値アルゴリズム #レース名 #開催地

環境変数:
    NOTE_PROFILE_URL  — note.com プロフィールページ URL（.env で設定可）
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
_IMPORTANT_EV_THRESHOLD = 10.0  # 【重要】タグを付与する最高EV閾値
_MIN_POST_EV = 1.0               # Discord 転送対象の最低EV
_DISCORD_RATE_LIMIT_SEC = 1.0   # Discord レート制限対策（秒）

_NOTE_PROFILE_URL: str = os.environ.get("NOTE_PROFILE_URL", "https://note.com/umalogi")


def _build_x_post(
    race_name: str,
    venue: str,
    race_number: int,
    max_ev: float,
    note_url: str = "",
) -> str:
    """X 用投稿文を 280 字以内で生成する（note 誘導型テンプレート）。

    テンプレート:
        【重要：会場NR】  ← EV>=_IMPORTANT_EV_THRESHOLD の場合
        AIによる期待値選別結果：EV XX.XX
        市場の歪みを捉えた勝算のある買い目のみを抽出しました。
        詳細はnoteで公開中。
        買い目・推奨理由はコチラ👇
        https://note.com/...
        #競馬予想 #期待値アルゴリズム #レース名 #開催地
    """
    is_important = max_ev >= _IMPORTANT_EV_THRESHOLD
    tag = "【重要】" if is_important else "【推奨】"
    label = f"{tag}{venue}{race_number}R"

    url = note_url or _NOTE_PROFILE_URL
    # レース名の長さによってタグを省略しスペースを節約
    short_name = race_name.replace(" ", "").replace("　", "")[:10]

    hashtags = f"#競馬予想 #期待値アルゴリズム #{short_name} #{venue}"

    lines = [
        label,
        f"AIによる期待値選別結果：EV {max_ev:.2f}",
        "",
        "市場の歪みを捉えた勝算のある買い目のみを抽出しました。",
        "詳細はnoteで公開中。",
        "",
        "買い目・推奨理由はコチラ👇",
        url,
        hashtags,
    ]
    post = "\n".join(lines)

    # 280 字を超える場合はハッシュタグを削減して調整
    if len(post) > _MAX_X_CHARS:
        hashtags = f"#競馬予想 #期待値アルゴリズム #{venue}"
        lines[-1] = hashtags
        post = "\n".join(lines)

    return post[:_MAX_X_CHARS]


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


def run(date_str: str, webhook_url: str, note_url: str = "", dry_run: bool = False) -> None:
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

        # X 用投稿文生成（note 誘導テンプレート）
        x_post = _build_x_post(race_name, venue, race_number, max_ev, note_url=note_url)

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
    parser.add_argument(
        "--note-url",
        default="",
        help="note.com の URL（省略時は NOTE_PROFILE_URL 環境変数、またはデフォルト）",
    )
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信を行わない")
    args = parser.parse_args()

    date_str = args.date or date.today().strftime("%Y%m%d")
    note_url = args.note_url or _NOTE_PROFILE_URL
    logger.info("=== X投稿文生成 → Discord転送 date=%s note_url=%s dry_run=%s ===",
                date_str, note_url, args.dry_run)
    run(date_str=date_str, webhook_url=args.webhook, note_url=note_url, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
