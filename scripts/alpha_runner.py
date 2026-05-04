"""
ALPHA モデル デイリーランナー（別枠予想配信）
==============================================

既存の today_auto_runner.py / prerace_pipeline.py とは独立して動作。
ALPHA モデルの予想のみを別枠で Discord / UI に配信するプロトタイプ。

使用例:
  python scripts/alpha_runner.py --date 20260503
  python scripts/alpha_runner.py --dry-run
  python scripts/alpha_runner.py --date 20260503 --venue 東京
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

import os
import sqlite3
import pandas as pd
from src.database.init_db import init_db
from src.ml.alpha_model import AlphaModel, ALPHA_EV_THRESHOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _send_discord(msg: str) -> None:
    """Discord Webhook に ALPHA 予想を送信する。"""
    try:
        import requests
        url = os.getenv("DISCORD_WEBHOOK_URL", "")
        if url:
            requests.post(url, json={"content": msg[:2000]}, timeout=10)
    except Exception as e:
        logger.warning("Discord 送信失敗: %s", e)


def _load_race_df(
    conn: sqlite3.Connection,
    target_date: str,
    venue: str | None = None,
) -> pd.DataFrame:
    """
    指定日の出馬表を v_race_mart から読み込む。

    target_date: 'YYYY-MM-DD'
    """
    venue_clause = "AND r.venue = ?" if venue else ""
    params: list = [target_date]
    if venue:
        params.append(venue)

    sql = f"""
    SELECT vm.*, r.date, r.venue, r.race_number
    FROM v_race_mart vm
    JOIN races r ON vm.race_id = r.race_id
    WHERE r.date = ?
      {venue_clause}
    ORDER BY r.race_number, vm.horse_number
    """
    df = pd.read_sql_query(sql, conn, params=params)
    logger.info("出馬表ロード: %d 行 (date=%s)", len(df), target_date)
    return df


def _format_alpha_prediction(
    race_id: str,
    race_name: str,
    venue: str,
    race_number: int,
    signals: pd.DataFrame,
    date_str: str,
) -> str:
    """ALPHA 予想を人間が読めるテキストに整形する。"""
    lines = [
        f"🔥【ALPHA 収益特化予想】{venue} {race_number}R {race_name}",
        f"   {date_str}",
        "",
    ]

    for _, row in signals.iterrows():
        ev = float(row["ev_pred"])
        odds = float(row["win_odds"]) if pd.notna(row["win_odds"]) else 0
        horse_num = int(row["horse_number"]) if pd.notna(row["horse_number"]) else 0
        kelly = int(row.get("kelly_bet", 0))
        lines.append(
            f"  ★ {horse_num:02d}番  オッズ {odds:.1f}倍  "
            f"ALPHA-EV {ev:.2f}  推奨賭額 ¥{kelly:,}"
        )

    lines += ["", f"  ※ EV>{ALPHA_EV_THRESHOLD:.1f} の馬のみ表示"]
    return "\n".join(lines)


def _save_to_db(
    conn: sqlite3.Connection,
    signals_df: pd.DataFrame,
    date_str: str,
    dry_run: bool = False,
) -> None:
    """ALPHA 予想を predictions テーブルに保存する（既存と重複チェックあり）。"""
    if dry_run:
        logger.info("[DRY-RUN] DB保存スキップ")
        return

    saved = 0
    for _, row in signals_df.iterrows():
        race_id = row["race_id"]
        horse_num = int(row["horse_number"]) if pd.notna(row["horse_number"]) else 0
        ev_pred = float(row["ev_pred"])
        kelly = int(row.get("kelly_bet", 0))
        combination = json.dumps([horse_num])
        notes = f"ALPHA-EV={ev_pred:.3f}"

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO predictions
                    (race_id, model_type, bet_type, confidence,
                     expected_value, recommended_bet, notes, combination_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (race_id, "ALPHA", "単勝", ev_pred,
                 ev_pred, kelly, notes, combination),
            )
            saved += 1
        except Exception as e:
            logger.warning("予想保存失敗 race=%s: %s", race_id, e)

    conn.commit()
    logger.info("ALPHA 予想 %d 件保存", saved)


def run(
    target_date: str,
    venue: str | None = None,
    dry_run: bool = False,
    ev_threshold: float = ALPHA_EV_THRESHOLD,
    bankroll: int = 100_000,
    notify_discord: bool = True,
) -> list[dict]:
    """
    ALPHA モデルで指定日の予想を生成する。

    Returns:
        予想リスト [{"race_id", "signals": DataFrame}, ...]
    """
    conn = init_db()

    # ── モデルロード ─────────────────────────────────────────────────
    model_path = _ROOT / "data" / "models" / "alpha" / "alpha_model.pkl"
    if not model_path.exists():
        logger.error("ALPHA モデルが見つかりません: %s", model_path)
        logger.error("先に `python scripts/alpha_backtest.py --save` を実行してください")
        conn.close()
        return []

    model = AlphaModel.load(model_path)

    # ── 出馬表ロード ─────────────────────────────────────────────────
    race_df = _load_race_df(conn, target_date, venue)
    if len(race_df) == 0:
        logger.warning("出馬表なし: date=%s venue=%s", target_date, venue)
        conn.close()
        return []

    # ── ALPHA 予想生成 ───────────────────────────────────────────────
    all_signals: list[dict] = []

    for race_id, grp in race_df.groupby("race_id"):
        try:
            signals = model.generate_buy_signals(grp.copy(), ev_threshold, bankroll)
        except Exception as e:
            logger.warning("予想失敗 %s: %s", race_id, e)
            continue

        if len(signals) == 0:
            continue

        signals["race_id"] = race_id

        # レース名・会場を取得
        meta = grp.iloc[0]
        race_name = str(meta.get("race_name", "")) if "race_name" in meta else ""
        race_venue = str(meta.get("venue", "")) if "venue" in meta else ""
        race_num = int(meta.get("race_number", 0)) if "race_number" in meta else 0

        if not dry_run:
            _save_to_db(conn, signals, target_date, dry_run=False)

        # Discord 通知
        if notify_discord and not dry_run:
            msg = _format_alpha_prediction(
                race_id, race_name, race_venue, race_num, signals, target_date
            )
            _send_discord(msg)

        all_signals.append({
            "race_id": race_id,
            "race_name": race_name,
            "venue": race_venue,
            "race_number": race_num,
            "signals": signals,
        })

        logger.info(
            "ALPHA 予想: %s %dR → %d 頭 (EV>%.1f)",
            race_venue, race_num, len(signals), ev_threshold,
        )

    conn.close()

    # ── サマリー表示 ─────────────────────────────────────────────────
    total_horses = sum(len(s["signals"]) for s in all_signals)
    total_invest = sum(s["signals"]["kelly_bet"].sum() for s in all_signals)
    print(f"\n[ALPHA 予想完了] {target_date}")
    print(f"  対象レース: {len(all_signals)} R")
    print(f"  買いシグナル: {total_horses} 頭")
    print(f"  推定投資額: ¥{total_invest:,.0f}")

    return all_signals


def main() -> None:
    parser = argparse.ArgumentParser(description="ALPHA モデル デイリーランナー")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"),
                        metavar="YYYYMMDD or YYYY-MM-DD",
                        help="対象日（デフォルト: 今日）")
    parser.add_argument("--venue", default=None,
                        help="会場絞り込み (例: 東京)")
    parser.add_argument("--dry-run", action="store_true",
                        help="DB保存・Discord通知なし")
    parser.add_argument("--threshold", type=float, default=ALPHA_EV_THRESHOLD,
                        help=f"EV閾値（デフォルト: {ALPHA_EV_THRESHOLD}）")
    parser.add_argument("--bankroll", type=int, default=100_000,
                        help="仮想資金（デフォルト: 100,000円）")
    parser.add_argument("--no-notify", action="store_true",
                        help="Discord 通知なし")
    args = parser.parse_args()

    # YYYYMMDD → YYYY-MM-DD 正規化
    d = args.date.replace("-", "")
    if len(d) == 8:
        target_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    else:
        target_date = args.date

    logger.info("ALPHA ランナー開始: date=%s venue=%s dry_run=%s",
                target_date, args.venue, args.dry_run)

    run(
        target_date=target_date,
        venue=args.venue,
        dry_run=args.dry_run,
        ev_threshold=args.threshold,
        bankroll=args.bankroll,
        notify_discord=not args.no_notify,
    )


if __name__ == "__main__":
    main()
