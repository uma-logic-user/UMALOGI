"""
scripts/generate_result_note_draft.py — 万馬券特化的中報告 Markdown を生成して Discord 転送する

対象: payout >= 10,000 円 OR (is_hit=1 かつ ROI >= 300%)
Markdown を生成し DISCORD_WEBHOOK_NOTE_DRAFT チャンネルへ送信する。

使い方:
    py scripts/generate_result_note_draft.py
    py scripts/generate_result_note_draft.py --days 14   # 直近14日間
    py scripts/generate_result_note_draft.py --dry-run   # Discord 送信なし（標準出力のみ）
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_DB_PATH = _ROOT / "data" / "umalogi.db"
_PAYOUT_THRESHOLD = 10_000
_ROI_THRESHOLD = 3.0  # 300%


def _fetch_hits(conn: sqlite3.Connection, since: date) -> list[dict]:
    sql = """
    SELECT
        r.race_date,
        r.race_name,
        r.venue,
        p.model_type,
        p.bet_type,
        p.horse_number,
        p.horse_name,
        p.expected_value,
        COALESCE(pr.payout, 0) AS payout,
        COALESCE(pr.is_hit, 0) AS is_hit,
        pr.settled_at
    FROM predictions p
    JOIN prediction_results pr ON pr.prediction_id = p.prediction_id
    LEFT JOIN races r ON r.race_id = p.race_id
    WHERE pr.is_hit = 1
      AND r.race_date >= ?
      AND (
          pr.payout >= ?
          OR (pr.payout >= 100 AND CAST(pr.payout AS REAL) / 100.0 >= ?)
      )
    ORDER BY pr.payout DESC
    """
    rows = conn.execute(
        sql, (since.isoformat(), _PAYOUT_THRESHOLD, _ROI_THRESHOLD * 100)
    ).fetchall()
    return [dict(row) for row in rows]


_BET_TYPE_LABELS: dict[str, str] = {
    "win": "単勝",
    "place": "複勝",
    "quinella": "馬連",
    "wide": "ワイド",
    "exacta": "馬単",
    "trio": "三連複",
    "trifecta": "三連単",
}


def _build_markdown(hits: list[dict], since: date, until: date) -> str:
    if not hits:
        return (
            f"# 🏇 万馬券炸裂レポート（{since}〜{until}）\n\n"
            "この期間の対象的中はありませんでした。\n"
        )

    lines: list[str] = [
        f"# 🏇 万馬券炸裂レポート（{since}〜{until}）",
        "",
        f"> UMALOGI AI が狙い打った **{len(hits)}件** の高配当的中を公開します。",
        "",
        "---",
        "",
    ]

    total_payout = sum(h["payout"] for h in hits)
    total_cost = len(hits) * 100
    roi = total_payout / total_cost if total_cost > 0 else 0.0

    lines += [
        "## 📊 集計サマリー",
        "",
        "| 項目 | 値 |",
        "|---|---|",
        f"| 対象期間 | {since} 〜 {until} |",
        f"| 対象的中件数 | {len(hits)} 件 |",
        f"| 合計払戻 | ¥{total_payout:,} |",
        f"| 合計投資 | ¥{total_cost:,} |",
        f"| ROI | {roi:.1%} |",
        "",
        "---",
        "",
        "## 🎯 的中詳細",
        "",
    ]

    for i, h in enumerate(hits, 1):
        bet_label = _BET_TYPE_LABELS.get(h["bet_type"] or "", h["bet_type"] or "")
        roi_single = h["payout"] / 100.0
        ev_line = (
            f"- **EV スコア**: {h['expected_value']:.2f}"
            if h.get("expected_value")
            else ""
        )
        lines += [
            f"### {i}. {h['race_date']} {h['race_name'] or ''}（{h['venue'] or ''}）",
            "",
            f"- **モデル**: {h['model_type']}",
            f"- **券種**: {bet_label}",
            f"- **馬番**: {h['horse_number']}番 {h['horse_name'] or ''}",
        ]
        if ev_line:
            lines.append(ev_line)
        lines += [
            f"- **払戻**: ¥{h['payout']:,}（{roi_single:.1f}倍）",
            "",
        ]

    lines += [
        "---",
        "",
        "## 💡 AIの予測根拠（サンプル）",
        "",
        "> U score（大衆心理乖離スコア）・オッズ逆行シグナル・前走比較などを総合的に評価して選定しました。",
        "> 詳細な特徴量重要度はダッシュボードでご確認いただけます。",
        "",
        "---",
        "",
        "*このレポートは UMALOGI AI によって自動生成されました。*",
        "*馬券の購入は自己責任でお願いします。未来の結果を保証するものではありません。*",
    ]

    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(
        description="万馬券特化的中報告 Markdown を生成して Discord 転送"
    )
    p.add_argument(
        "--days", type=int, default=7, help="直近N日間を対象（デフォルト7日）"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Discord 送信なし（標準出力のみ）"
    )
    args = p.parse_args()

    until = date.today()
    since = until - timedelta(days=args.days)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        hits = _fetch_hits(conn, since)
    finally:
        conn.close()

    print(f"🔍 対象期間: {since} 〜 {until}（{args.days}日間）")
    print(f"🎯 対象的中件数: {len(hits)} 件")

    body = _build_markdown(hits, since, until)
    title = f"🏇【万馬券炸裂レポート】{since}〜{until}"

    print()
    print("=" * 60)
    print(body[:500] + ("..." if len(body) > 500 else ""))
    print("=" * 60)

    if args.dry_run:
        print("\n✅ --dry-run: Discord 送信をスキップしました。")
        return

    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=False)

    from src.notification.router import NotificationRouter

    router = NotificationRouter()
    ok = router.send_note_draft(title, body)
    if ok:
        print("\n✅ Discord note-draft チャンネルへ転送完了。")
    else:
        print("\n⚠️  Discord 転送スキップ（DISCORD_WEBHOOK_NOTE_DRAFT 未設定）。")


if __name__ == "__main__":
    main()
