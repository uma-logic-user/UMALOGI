# -*- coding: utf-8 -*-
"""
Discord デュアルチャンネル テスト送信スクリプト

使い方:
    py scripts/test_discord_channels.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

from src.notification.discord_notifier import DiscordNotifier


def _make_dummy_bets(model_type: str, bets_data: list[dict]) -> object:
    """ダミーの RaceBets オブジェクトを生成する。"""
    from types import SimpleNamespace

    bets = []
    for d in bets_data:
        bets.append(SimpleNamespace(
            bet_type=d["bet_type"],
            expected_value=d["ev"],
            recommended_bet=d["bet"],
            n_tickets=d.get("n_tickets", 1),
            combinations=d.get("combos", [[1, 3]]),
            horse_names=d.get("names", ["テストホース1", "テストホース2"]),
            model_score=d.get("score", 0.65),
        ))
    return SimpleNamespace(model_type=model_type, bets=bets)


def test_system_channel(notifier: DiscordNotifier) -> None:
    """システムチャンネルへのテスト通知（3種）。"""
    print("\n[1] システムチャンネル: 起動通知")
    notifier.send_system_embed(
        title="🚀 UMALOGI システム起動テスト",
        description=(
            "これはシステムチャンネルへのテスト通知です。\n"
            "稼働ログ・エラー・再起動通知がここに集約されます。"
        ),
        color=0x5865F2,
        fields=[
            {"name": "バージョン", "value": "UMALOGI Ver1.0", "inline": True},
            {"name": "チャンネル種別", "value": "SYSTEM", "inline": True},
            {"name": "対象", "value": "監視ループ / JVLink / 暫定予想バッチ", "inline": False},
        ],
        footer="DISCORD_SYSTEM_WEBHOOK_URL へ送信されています",
    )

    print("[2] システムチャンネル: エラー通知")
    notifier.notify_scraping_alert(
        race_id="202605090511",
        detail="テスト用スクレイピング異常通知（実際のエラーではありません）",
    )

    print("[3] システムチャンネル: 介入要請通知")
    notifier.notify_intervention_required(
        step="テスト — note 記事投稿",
        error="ConnectionRefusedError: [WinError 10061] テスト用ダミーエラー",
        action="ブラウザで手動ログインし、再実行してください: py scripts/post_note.py",
    )


def test_prediction_channel(notifier: DiscordNotifier) -> None:
    """予想チャンネルへのテスト通知（2種）。"""
    print("\n[4] 予想チャンネル: 直前予想 Embed カード")

    # ダミー買い目データ
    honmei = _make_dummy_bets("本命", [
        {
            "bet_type": "三連複",
            "ev": 2.13,
            "bet": 800,
            "n_tickets": 6,
            "combos": [[5, 12, 3], [5, 12, 7], [5, 12, 9], [5, 3, 7], [5, 3, 9], [5, 7, 9]],
            "names": ["アーバンシック", "タスティエーラ", "ドウデュース", "リバティアイランド"],
        },
        {
            "bet_type": "複勝",
            "ev": 1.42,
            "bet": 400,
            "n_tickets": 1,
            "combos": [[5]],
            "names": ["アーバンシック"],
        },
    ])
    manji = _make_dummy_bets("卍", [
        {
            "bet_type": "馬連",
            "ev": 1.85,
            "bet": 600,
            "n_tickets": 4,
            "combos": [[5, 12], [5, 3], [5, 7], [12, 3]],
            "names": ["アーバンシック", "タスティエーラ", "ドウデュース", "リバティアイランド"],
        },
    ])
    oracle = _make_dummy_bets("Oracle", [
        {
            "bet_type": "三連単",
            "ev": 0.72,
            "bet": 200,
            "n_tickets": 1,
            "combos": [[5, 12, 3]],
            "names": ["アーバンシック", "タスティエーラ", "ドウデュース"],
        },
    ])

    notifier.notify_prerace_result(
        race_id="202605090511",
        honmei_bets=honmei,
        manji_bets=manji,
        oracle_bets=oracle,
        dashboard_url="https://umalogi.example.com",
    )

    print("[5] 予想チャンネル: 週次サマリー（テスト）")
    notifier.send_text(
        "📊 **[UMALOGI テスト] 週次サマリー**\n"
        "予想件数: 148 件 / 的中: 23 件 (的中率 15.5%)\n"
        "払戻合計: ¥312,400\n"
        "損益: 🟢 +¥46,800\n"
        "これは DISCORD_WEBHOOK_URL（予想チャンネル）へ届いています"
    )


def main() -> None:
    import os
    notifier = DiscordNotifier()

    pred_url   = os.getenv("DISCORD_WEBHOOK_URL",        "")
    system_url = os.getenv("DISCORD_SYSTEM_WEBHOOK_URL", "")

    print("=" * 55)
    print("  UMALOGI Discord デュアルチャンネル テスト")
    print("=" * 55)
    print(f"  予想チャンネル URL : {'設定済み ✓' if pred_url   else '未設定 ✗ (DISCORD_WEBHOOK_URL)'}")
    print(f"  システムチャンネル URL: {'設定済み ✓' if system_url else '未設定 ✗ → 予想チャンネルへ fallback'}")
    print()

    if not pred_url and not system_url:
        print("❌ Discord URL が1つも設定されていません。.env を確認してください。")
        sys.exit(1)

    test_system_channel(notifier)
    test_prediction_channel(notifier)

    print("\n" + "=" * 55)
    print("  ✅ テスト送信完了")
    print("  Discord で2つのチャンネルを確認してください")
    print("=" * 55)


if __name__ == "__main__":
    main()
