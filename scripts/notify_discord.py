"""
Discord 予想通知スクリプト
===========================

data/predictions/ 配下の当日 JSON を読み取り、
期待値 (ev_score or manji_ev) >= EV_THRESHOLD の馬を Discord Webhook で通知する。

使い方:
  python scripts/notify_discord.py
  python scripts/notify_discord.py --date 20260411
  python scripts/notify_discord.py --dry-run        # Webhook 未送信でコンソール表示のみ

環境変数:
  DISCORD_WEBHOOK_URL  必須（.env に記載）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

ROOT         = Path(__file__).resolve().parent.parent
PREDICTIONS  = ROOT / "data" / "predictions"
EV_THRESHOLD = 1.0

# 会場コード → 競馬場名
_JYO = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


def _venue(race_id: str) -> str:
    """race_id (12桁) から競馬場名を返す。"""
    # race_id 例: 202606030501 → 年(4) 場(2) 回(2) 日(2) R(2)
    return _JYO.get(race_id[4:6], race_id[4:6])


def _race_num(race_id: str) -> str:
    return str(int(race_id[10:12])) + "R"


def _load_predictions(target_date: str) -> list[dict[str, Any]]:
    """
    prediction JSON を読み込む。
    - target_date (YYYYMMDD) は generated_at フィールドの日付で絞る。
    - "--all" が渡された場合は全ファイルを対象にする。
    """
    all_files = sorted(PREDICTIONS.glob("*.json"))
    results = []
    for f in all_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if target_date == "all":
            results.append(data)
        else:
            generated_at: str = data.get("generated_at", "")
            # "2026-04-10T15:46:25.279288" → "20260410"
            gen_date = generated_at[:10].replace("-", "")
            if gen_date == target_date:
                results.append(data)
    return results


def build_messages(preds: list[dict[str, Any]]) -> list[str]:
    """EV >= EV_THRESHOLD の馬を整形してメッセージリストを返す。"""
    messages: list[str] = []

    for pred in preds:
        race_id = pred.get("race_id", "")
        venue   = _venue(race_id)
        r_num   = _race_num(race_id)

        # ev_recommend リストがある場合はそれを優先
        ev_recommend: list[dict] = pred.get("ev_recommend", [])

        # ev_recommend が空なら horses から manji_ev >= threshold を拾う
        if not ev_recommend:
            for h in pred.get("horses", []):
                ev = h.get("manji_ev", 0.0) or 0.0
                if ev >= EV_THRESHOLD:
                    ev_recommend.append({
                        "horse_number": h["horse_number"],
                        "horse_name":   h["horse_name"],
                        "win_odds":     h.get("win_odds", 0.0),
                        "ev_score":     ev,
                        "kelly_fraction": h.get("kelly_fraction", 0.0),
                    })
            ev_recommend.sort(key=lambda x: x["ev_score"], reverse=True)

        if not ev_recommend:
            continue

        lines = [f"**【{venue} {r_num}】** `{race_id}`"]
        for h in ev_recommend:
            num    = h["horse_number"]
            name   = h["horse_name"]
            ev     = h["ev_score"]
            odds   = h.get("win_odds", 0.0)
            kelly  = h.get("kelly_fraction", 0.0)
            odds_str  = f"{odds:.1f}倍" if odds > 0 else "未定"
            kelly_str = f"Kelly={kelly*100:.1f}%" if kelly > 0 else ""
            lines.append(f"  {num}番 **{name}**  EV={ev:.2f}  {odds_str}  {kelly_str}")

        messages.append("\n".join(lines))

    return messages


def send_to_discord(webhook_url: str, messages: list[str], dry_run: bool) -> None:
    if not messages:
        print("EV >= 1.0 の買い推奨馬はありません。")
        return

    header = f"**[UMALOGI] EV推奨買い目** ({len(messages)} レース)"
    body   = "\n\n".join(messages)
    payload = {"content": f"{header}\n\n{body}"}

    # Discord の 2000 文字制限に対応して分割送信
    full_text = payload["content"]
    chunks: list[str] = []
    while len(full_text) > 1900:
        split = full_text.rfind("\n\n", 0, 1900)
        if split == -1:
            split = 1900
        chunks.append(full_text[:split])
        full_text = full_text[split:].lstrip()
    chunks.append(full_text)

    for i, chunk in enumerate(chunks):
        prefix = header + "\n\n" if i == 0 else f"(続き {i+1}/{len(chunks)})\n\n"
        text = chunk if i == 0 else prefix + chunk

        if dry_run:
            sys.stdout.buffer.write(("=" * 60 + "\n").encode("utf-8"))
            sys.stdout.buffer.write("[DRY-RUN] 以下を Discord に送信します:\n".encode("utf-8"))
            sys.stdout.buffer.write((text + "\n").encode("utf-8"))
            sys.stdout.buffer.flush()
        else:
            resp = requests.post(webhook_url, json={"content": text}, timeout=10)
            resp.raise_for_status()
            print(f"送信完了 ({i+1}/{len(chunks)}): HTTP {resp.status_code}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Discord 予想通知")
    parser.add_argument(
        "--date", default=date.today().strftime("%Y%m%d"),
        help="対象日付 YYYYMMDD（デフォルト: 今日）。'all' を指定すると全ファイル対象",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Webhook を呼ばずコンソールに出力するだけ",
    )
    args = parser.parse_args()

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not webhook_url and not args.dry_run:
        print("エラー: 環境変数 DISCORD_WEBHOOK_URL が未設定です。", file=sys.stderr)
        print("  .env に DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... を追加してください。")
        sys.exit(1)

    preds = _load_predictions(args.date)
    if not preds:
        print(f"対象ファイルが見つかりません: {PREDICTIONS}/{args.date}*.json")
        sys.exit(0)

    print(f"対象日: {args.date}  読み込みレース数: {len(preds)}")
    messages = build_messages(preds)
    print(f"EV推奨レース数: {len(messages)}")

    send_to_discord(webhook_url, messages, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
