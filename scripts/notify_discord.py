"""
Discord 予想通知スクリプト
===========================

data/predictions/ 配下の当日 JSON を読み取り、
期待値 (ev_score or manji_ev) >= ev_threshold の馬を Discord Webhook で通知する。

使い方:
  python scripts/notify_discord.py
  python scripts/notify_discord.py --date 20260411
  python scripts/notify_discord.py --dry-run        # Webhook 未送信でコンソール表示のみ

環境変数:
  DISCORD_WEBHOOK_URL  必須（.env に記載）
  INITIAL_BANKROLL     初期資金（デフォルト: 100000）
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

# Windows CP932 環境での文字化け防止
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=False)

PREDICTIONS = ROOT / "data" / "predictions"
_DB_PATH = ROOT / "data" / "umalogi.db"
EV_THRESHOLD_DEFAULT = 1.0  # DB接続不可時のフォールバック

# 会場コード → 競馬場名
_JYO = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}


def _venue(race_id: str) -> str:
    """race_id (12桁) から競馬場名を返す。"""
    return _JYO.get(race_id[4:6], race_id[4:6])


def _race_num(race_id: str) -> str:
    return str(int(race_id[10:12])) + "R"


def _load_predictions(
    target_date: str,
    model_version: str = "v1",
) -> list[dict[str, Any]]:
    """prediction JSON を読み込む。

    Args:
        target_date:   YYYYMMDD or "all"
        model_version: "v1" = {race_id}.json / "v2" = {race_id}_v2.json
    """
    if model_version == "v2":
        all_files = sorted(PREDICTIONS.glob("*_v2.json"))
    else:
        all_files = [
            f
            for f in sorted(PREDICTIONS.glob("*.json"))
            if not f.name.endswith("_v2.json")
        ]
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
            gen_date = generated_at[:10].replace("-", "")
            if gen_date == target_date:
                results.append(data)
    return results


def _get_threshold_and_bankroll() -> tuple[float, float, str]:
    """DBから動的EV閾値と現在バンクロールを取得する。失敗時はデフォルト値を返す。"""
    if not _DB_PATH.exists():
        return EV_THRESHOLD_DEFAULT, 100_000.0, "DB未接続"
    try:
        from src.ml.bet_generator import (
            get_current_bankroll,
            get_dynamic_ev_threshold,
        )

        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        threshold, roi_pct, mode = get_dynamic_ev_threshold(conn)
        bankroll = get_current_bankroll(conn)
        conn.close()
        return threshold, bankroll, f"{mode}(直近28日ROI={roi_pct:.1f}%)"
    except Exception as exc:
        print(f"[WARN] 動的閾値取得失敗 → デフォルト{EV_THRESHOLD_DEFAULT}: {exc}")
        return EV_THRESHOLD_DEFAULT, 100_000.0, "フォールバック"


def _kelly_from_ev(
    ev_score: float, win_odds: float, bankroll: float
) -> tuple[int, float]:
    """EV スコアと単勝オッズから 1/4 Kelly ベット額を算出する（簡易版）。"""
    try:
        from src.ml.bet_generator import calc_qf_kelly_bet

        return calc_qf_kelly_bet(ev_score, win_odds, bankroll)
    except Exception:
        # フォールバック: 総資金の0.5%（固定）
        bet = max(int(bankroll * 0.005 // 100) * 100, 100)
        return bet, 0.0


def build_messages(
    preds: list[dict[str, Any]],
    ev_threshold: float = EV_THRESHOLD_DEFAULT,
    bankroll: float = 100_000.0,
) -> list[str]:
    """EV >= ev_threshold の馬を整形してメッセージリストを返す。"""
    messages: list[str] = []

    for pred in preds:
        race_id = pred.get("race_id", "")
        venue = _venue(race_id)
        r_num = _race_num(race_id)

        # ev_recommend リストがある場合はそれを優先
        ev_recommend: list[dict] = pred.get("ev_recommend", [])

        # ev_recommend が空なら horses から manji_ev >= threshold を拾う
        if not ev_recommend:
            for h in pred.get("horses", []):
                ev = h.get("manji_ev", 0.0) or 0.0
                if ev >= ev_threshold:
                    ev_recommend.append(
                        {
                            "horse_number": h["horse_number"],
                            "horse_name": h["horse_name"],
                            "win_odds": h.get("win_odds", 0.0),
                            "ev_score": ev,
                            "kelly_fraction": h.get("kelly_fraction", 0.0),
                        }
                    )
            ev_recommend.sort(key=lambda x: x["ev_score"], reverse=True)

        if not ev_recommend:
            continue

        lines = [f"**【{venue} {r_num}】** `{race_id}`"]

        # ★QF推奨: EV上位2頭からワイド+馬連を強調表示（WFバックテスト実証済み最優先戦略）
        if len(ev_recommend) >= 2:
            h1 = ev_recommend[0]
            h2 = ev_recommend[1]
            n1, n2 = sorted([h1["horse_number"], h2["horse_number"]])
            nm1 = h1["horse_name"]
            nm2 = h2["horse_name"]

            # Kelly ベット額算出（軸馬 h1 の EV・オッズを基準に計算）
            odds1 = float(h1.get("win_odds", 0.0) or 0.0)
            ev1 = float(h1.get("ev_score", 0.0) or 0.0)
            bet_yen, kelly_pct = _kelly_from_ev(ev1, odds1, bankroll)
            total_wide = bet_yen  # ワイド 1点
            total_umaren = bet_yen  # 馬連 1点
            total_outlay = total_wide + total_umaren
            bankroll_pct = (total_outlay / bankroll * 100.0) if bankroll > 0 else 0.0

            kelly_info = (
                f"1/4 Kelly={kelly_pct:.1f}%" if kelly_pct > 0 else "固定ベット"
            )

            lines.append(
                f"  **★QF推奨** ワイド`{n1}-{n2}` / 馬連`{n1}-{n2}`  ({nm1} × {nm2})"
            )
            lines.append(
                f"  　推奨投資: ¥{bet_yen:,}/点  合計¥{total_outlay:,}"
                f"  ({kelly_info} / 総資金の{bankroll_pct:.2f}%)"
            )
            lines.append("  ─────────────────")

        for h in ev_recommend:
            num = h["horse_number"]
            name = h["horse_name"]
            ev = h["ev_score"]
            odds = h.get("win_odds", 0.0)
            kelly = h.get("kelly_fraction", 0.0)
            odds_str = f"{odds:.1f}倍" if odds > 0 else "未定"
            kelly_str = f"Kelly={kelly * 100:.1f}%" if kelly > 0 else ""
            lines.append(f"  {num}番 **{name}**  EV={ev:.2f}  {odds_str}  {kelly_str}")

        messages.append("\n".join(lines))

    return messages


def send_to_discord(
    webhook_url: str,
    messages: list[str],
    dry_run: bool,
    header: str = "",
) -> None:
    if not messages:
        print("EV推奨買い目はありません（閾値を超えるレースなし）。")
        return

    if not header:
        header = f"**[UMALOGI] EV推奨買い目** ({len(messages)} レース)"
    body = "\n\n".join(messages)
    full_text = f"{header}\n\n{body}"

    # Discord の 2000 文字制限に対応して分割送信
    chunks: list[str] = []
    while len(full_text) > 1900:
        split = full_text.rfind("\n\n", 0, 1900)
        if split == -1:
            split = 1900
        chunks.append(full_text[:split])
        full_text = full_text[split:].lstrip()
    chunks.append(full_text)

    for i, chunk in enumerate(chunks):
        text = chunk if i == 0 else f"(続き {i + 1}/{len(chunks)})\n\n{chunk}"

        if dry_run:
            sys.stdout.buffer.write(("=" * 60 + "\n").encode("utf-8"))
            sys.stdout.buffer.write(
                "[DRY-RUN] 以下を Discord に送信します:\n".encode("utf-8")
            )
            sys.stdout.buffer.write((text + "\n").encode("utf-8"))
            sys.stdout.buffer.flush()
        else:
            resp = requests.post(webhook_url, json={"content": text}, timeout=10)
            resp.raise_for_status()
            print(f"送信完了 ({i + 1}/{len(chunks)}): HTTP {resp.status_code}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Discord 予想通知")
    parser.add_argument(
        "--date",
        default=date.today().strftime("%Y%m%d"),
        help="対象日付 YYYYMMDD（デフォルト: 今日）。'all' を指定すると全ファイル対象",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Webhook を呼ばずコンソールに出力するだけ",
    )
    parser.add_argument(
        "--no-comparison",
        action="store_true",
        help="V1比較セクションを省略する（送信量削減）",
    )
    args = parser.parse_args()

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not webhook_url and not args.dry_run:
        print("エラー: 環境変数 DISCORD_WEBHOOK_URL が未設定です。", file=sys.stderr)
        print(
            "  .env に DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... を追加してください。"
        )
        sys.exit(1)

    # 動的EV閾値・バンクロール取得
    ev_threshold, bankroll, mode_desc = _get_threshold_and_bankroll()
    print(f"動的EV閾値: {ev_threshold:.1f} ({mode_desc})")
    print(f"現在バンクロール: {bankroll:,.0f}円")

    # ── 【メイン】V2 予想（W-004+動的EV+Kelly）────────────────────
    preds_v2 = _load_predictions(args.date, model_version="v2")
    if preds_v2:
        print(f"[V2] 対象日: {args.date}  読み込みレース数: {len(preds_v2)}")
        msgs_v2 = build_messages(preds_v2, ev_threshold=ev_threshold, bankroll=bankroll)
        print(f"[V2] EV推奨レース数: {len(msgs_v2)}")
        header_v2 = (
            f"**[UMALOGI V2] EV≥{ev_threshold:.1f} 推奨買い目 ★**  "
            f"({len(msgs_v2)} レース | {mode_desc} | 総資金{bankroll:,.0f}円)"
        )
        send_to_discord(webhook_url, msgs_v2, dry_run=args.dry_run, header=header_v2)
    else:
        print(f"[V2] 予想ファイルなし: {PREDICTIONS}/{args.date}*_v2.json")

    # ── 【比較】V1 予想（固定EV閾値・W-004なし）─────────────────────
    if not args.no_comparison:
        preds_v1 = _load_predictions(args.date, model_version="v1")
        if preds_v1:
            print(f"[V1] 読み込みレース数: {len(preds_v1)}")
            msgs_v1 = build_messages(
                preds_v1, ev_threshold=EV_THRESHOLD_DEFAULT, bankroll=bankroll
            )
            print(f"[V1] EV推奨レース数: {len(msgs_v1)}")
            if msgs_v1:
                header_v1 = (
                    f"**[UMALOGI V1 比較用] EV≥{EV_THRESHOLD_DEFAULT:.1f} 予想**  "
                    f"({len(msgs_v1)} レース | 固定閾値・W-004なし)"
                )
                send_to_discord(
                    webhook_url, msgs_v1, dry_run=args.dry_run, header=header_v1
                )
        else:
            # V2 しか存在しない場合、V2 を V1 ロジックとして表示
            if not preds_v2:
                print(f"V1/V2 どちらの予想ファイルも見つかりません: {PREDICTIONS}")
                sys.exit(0)
            preds_fallback = _load_predictions(args.date, model_version="v2")
            msgs_fallback = build_messages(
                preds_fallback, ev_threshold=EV_THRESHOLD_DEFAULT, bankroll=bankroll
            )
            if msgs_fallback:
                header_fb = (
                    f"**[UMALOGI 参考] EV≥{EV_THRESHOLD_DEFAULT:.1f}**  "
                    f"({len(msgs_fallback)} レース)"
                )
                send_to_discord(
                    webhook_url, msgs_fallback, dry_run=args.dry_run, header=header_fb
                )


if __name__ == "__main__":
    main()
