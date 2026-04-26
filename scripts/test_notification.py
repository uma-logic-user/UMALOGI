"""
Discord / LINE / X 通知テストスクリプト

UMALOGI システムが正常に起動し、通知チャンネルが疎通していることを確認する。

Usage:
    python scripts/test_notification.py              # Discord へ「準備完了」通知
    python scripts/test_notification.py --channel all # 全チャンネルへ送信
    python scripts/test_notification.py --dry-run    # 実際には送信しない（設定確認のみ）
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
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
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def _build_message() -> "NotifyMessage":  # type: ignore[name-defined]
    from src.notification.base import NotifyMessage
    from src.database.init_db import init_db

    # DB 情報を取得してメッセージに含める
    try:
        conn = init_db()
        races  = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
        models = conn.execute("SELECT COUNT(*) FROM races WHERE race_id IS NOT NULL").fetchone()[0]
        preds  = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        conn.close()
        db_info = f"レース数: {races:,} / 予想数: {preds:,}"
    except Exception:
        db_info = "DB 情報取得不可"

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    return NotifyMessage(
        title="[UMALOGI] 準備完了",
        body=(
            f"**システム起動確認** — {now}\n\n"
            f"📊 {db_info}\n"
            "🤖 JRA-VAN データ同期・AI予想・通知・バックアップ — 全機能稼働中"
        ),
    )


def send_discord(dry_run: bool = False) -> bool:
    from src.notification.discord_notifier import DiscordNotifier
    notifier = DiscordNotifier()
    msg = _build_message()
    if dry_run:
        logger.info("[DRY-RUN] Discord 送信予定: %s", msg.title)
        return True
    ok = notifier.send(msg)
    logger.info("Discord 送信: %s", "成功" if ok else "失敗 (DISCORD_WEBHOOK_URL を確認)")
    return ok


def send_line(dry_run: bool = False) -> bool:
    from src.notification.line_notifier import LineNotifier
    notifier = LineNotifier()
    msg = _build_message()
    if dry_run:
        logger.info("[DRY-RUN] LINE 送信予定: %s", msg.title)
        return True
    ok = notifier.send(msg)
    logger.info("LINE 送信: %s", "成功" if ok else "失敗 (LINE_NOTIFY_TOKEN を確認)")
    return ok


def send_sos(message: str, dry_run: bool = False) -> bool:
    """緊急 SOS メッセージを Discord に送信する。"""
    from src.notification.discord_notifier import DiscordNotifier
    from src.notification.base import NotifyMessage
    notifier = DiscordNotifier()
    msg = NotifyMessage(
        title="[UMALOGI][KINKYU] BATCH ERROR",
        body=message,
    )
    if dry_run:
        logger.info("[DRY-RUN] SOS送信予定: %s", msg.body)
        return True
    ok = notifier.send(msg)
    logger.info("SOS通知送信: %s", "成功" if ok else "失敗 (DISCORD_WEBHOOK_URL を確認)")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI 通知テスト")
    parser.add_argument(
        "--channel",
        choices=["discord", "line", "all"],
        default="discord",
        help="送信先チャンネル（デフォルト: discord）",
    )
    parser.add_argument("--dry-run", action="store_true", help="実際には送信しない")
    parser.add_argument(
        "--sos",
        metavar="MSG",
        help="緊急SOS通知メッセージを Discord に送信（--channel 指定より優先）",
    )
    args = parser.parse_args()

    results: dict[str, bool] = {}

    if args.sos:
        results["Discord(SOS)"] = send_sos(args.sos, dry_run=args.dry_run)
    else:
        if args.channel in ("discord", "all"):
            results["Discord"] = send_discord(dry_run=args.dry_run)

        if args.channel in ("line", "all"):
            results["LINE"] = send_line(dry_run=args.dry_run)

    print("\n=== 通知テスト結果 ===")
    for ch, ok in results.items():
        status = "[OK] 成功" if ok else "[NG] 失敗"
        print(f"  {ch}: {status}")

    all_ok = all(results.values())
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
