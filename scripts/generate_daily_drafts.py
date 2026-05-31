"""
UMALOGI 本日予想 全件一括生成 + SNS/NOTE 下書き保存スクリプト

使用方法:
    py scripts/generate_daily_drafts.py [--date 20260524] [--dry-run]

処理フロー:
    1. provisional_batch() で全レース暫定予想を Kelly ロジックで再生成
    2. predictions テーブルから買い目を取得して下書き作成
    3. data/drafts/YYYYMMDD/ に各レースの .txt を保存
    4. Discord へ完了通知
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import init_db
from src.pipeline.prediction import provisional_batch
from src.notification.discord_notifier import DiscordNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 定数
# ──────────────────────────────────────────────
_MIN_EV = 1.0  # 下書き掲載の最低 EV 閾値
_DRAFT_DIR = _ROOT / "data" / "drafts"

_BET_TYPE_LABEL: dict[str, str] = {
    "単勝": "単勝",
    "複勝": "複勝",
    "馬連": "馬連",
    "ワイド": "ワイド",
    "馬単": "馬単",
    "三連複": "三連複",
    "三連単": "三連単",
}


def _fmt_combos(combinations: list[list[int]]) -> str:
    """買い目リストを「N-M / N-M-K」形式に変換する。"""
    return " / ".join("-".join(map(str, c)) for c in combinations[:6])


def _build_draft(
    race_name: str,
    venue: str,
    race_number: int,
    bets: list[dict],
) -> str:
    """1レース分の下書きテキストを生成する（SNS/NOTE兼用）。"""
    header = (
        f"【本日の期待値推奨レース：{venue}{race_number}R / {race_name}】\n"
        f"統計的アルゴリズムが市場のノイズを排除し、高い期待値を検出。\n"
        f"選別された勝算のある買い目のみを公開します。\n\n"
    )

    lines: list[str] = []
    for bet in bets:
        ev = bet.get("expected_value") or 0.0
        if ev < _MIN_EV:
            continue
        bet_type = bet.get("bet_type", "")
        combos = bet.get("combinations", [])
        recommended = int(bet.get("recommended_bet") or 100)
        notes = bet.get("notes") or ""

        label = _BET_TYPE_LABEL.get(bet_type, bet_type)
        combo_str = _fmt_combos(combos)
        n = len(combos)

        lines.append(
            f"◆ {label}  EV={ev:.2f}  推奨金額: {recommended:,}円\n"
            f"   買い目({n}点): {combo_str}\n"
            f"   {notes[:80] if notes else ''}"
        )

    if not lines:
        return ""

    body = "\n".join(lines)
    footer = (
        "\n\n---\n"
        "※当予想は期待値ベースの投資であり、的中を保証するものではありません。\n"
        "※資金管理を徹底し、自己責任でご参加ください。"
    )
    return header + body + footer


def _fetch_bets_for_race(conn, race_id: str) -> list[dict]:
    """DB から該当 race_id の Kelly 推奨買い目を取得する。"""
    import json

    rows = conn.execute(
        """
        SELECT bet_type, combination_json, expected_value, recommended_bet, notes, model_type
        FROM predictions
        WHERE race_id = ?
          AND bet_type != '馬分析'
          AND model_type NOT LIKE '%暫定%'  -- 暫定は直前で上書きされるため直前のみ
        ORDER BY expected_value DESC
        """,
        (race_id,),
    ).fetchall()

    # 暫定予想のみの場合（直前がない）はフォールバック
    if not rows:
        rows = conn.execute(
            """
            SELECT bet_type, combination_json, expected_value, recommended_bet, notes, model_type
            FROM predictions
            WHERE race_id = ?
              AND bet_type != '馬分析'
            ORDER BY expected_value DESC
            """,
            (race_id,),
        ).fetchall()

    bets: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        key = f"{row[0]}_{row[5]}"
        if key in seen:
            continue
        seen.add(key)
        try:
            combos = json.loads(row[1] or "[]")
        except Exception:
            combos = []
        bets.append(
            {
                "bet_type": row[0],
                "combinations": combos,
                "expected_value": row[2] or 0.0,
                "recommended_bet": row[3] or 100,
                "notes": row[4] or "",
                "model_type": row[5],
            }
        )
    return bets


def generate_drafts(date_str: str, dry_run: bool = False) -> list[Path]:
    """指定日の全レース下書きを生成して保存し、保存パスのリストを返す。"""
    formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    out_dir = _DRAFT_DIR / date_str
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    conn = init_db()
    races = conn.execute(
        "SELECT race_id, race_name, venue, race_number FROM races WHERE date = ? ORDER BY race_id",
        (formatted,),
    ).fetchall()

    if not races:
        logger.warning("対象日 %s のレースが見つかりません", date_str)
        conn.close()
        return []

    logger.info("下書き生成対象: %d レース", len(races))

    saved: list[Path] = []
    skipped_no_ev = 0

    for race_id, race_name, venue, race_number in races:
        bets = _fetch_bets_for_race(conn, race_id)
        # EV>=1.0 の買い目が1件以上あるか確認
        ev_bets = [b for b in bets if b["expected_value"] >= _MIN_EV]
        if not ev_bets:
            skipped_no_ev += 1
            logger.debug("EV<%s のためスキップ: %s %s", _MIN_EV, race_id, race_name)
            continue

        draft = _build_draft(race_name, venue, race_number, bets)
        if not draft:
            skipped_no_ev += 1
            continue

        fname = out_dir / f"{race_id}_{venue}R{race_number:02d}.txt"
        if dry_run:
            logger.info("[DRY-RUN] 保存予定: %s (%d 行)", fname.name, draft.count("\n"))
        else:
            fname.write_text(draft, encoding="utf-8")
            logger.info("保存: %s", fname.name)
        saved.append(fname)

    conn.close()
    logger.info(
        "下書き完了: 保存=%d件 / EV不足スキップ=%d件",
        len(saved),
        skipped_no_ev,
    )
    return saved


def notify_complete(date_str: str, saved: list[Path], dry_run: bool) -> None:
    """Discord へ「本日の全レース予想準備完了」を通知する。"""
    venue_groups: dict[str, list[str]] = {}
    for p in saved:
        # ファイル名例: 202604010801_新潟R01.txt
        parts = p.stem.split("_", 1)
        venue_part = parts[1] if len(parts) > 1 else p.stem
        # venue_part = "新潟R01" など
        venue_key = venue_part.split("R")[0]
        race_no = venue_part.split("R")[-1].lstrip("0") or "?"
        venue_groups.setdefault(venue_key, []).append(f"R{race_no}")

    venue_lines = []
    for venue, races in sorted(venue_groups.items()):
        venue_lines.append(f"  **{venue}** {', '.join(races)}")

    msg_lines = (
        [
            "🏇 **本日の全レース予想準備完了**",
            f"📅 対象日: {date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}",
            f"📝 EV推奨レース: **{len(saved)}件**（Kelly 1/4法・銀行¥100,000）",
            "",
        ]
        + venue_lines
        + [
            "",
            "✅ `data/drafts/` に SNS/NOTE 下書きを保存しました。",
            "※ EV<1.0 のレースは掲載対象外です。",
        ]
    )
    msg = "\n".join(msg_lines)

    if dry_run:
        logger.info("[DRY-RUN] Discord 通知:\n%s", msg)
        return

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook:
        logger.warning("DISCORD_WEBHOOK_URL 未設定 — Discord 通知スキップ")
        return

    notifier = DiscordNotifier(webhook_url=webhook)
    try:
        notifier.send_text(msg)
        logger.info("Discord 通知送信完了")
    except Exception as exc:
        logger.error("Discord 通知失敗: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="本日予想全件生成 + 下書き保存")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（省略時=本日）")
    parser.add_argument(
        "--dry-run", action="store_true", help="DB 書き込み・通知を行わない"
    )
    parser.add_argument(
        "--skip-batch",
        action="store_true",
        help="provisional_batch を省略（予想再生成をスキップ）",
    )
    args = parser.parse_args()

    date_str = args.date or date.today().strftime("%Y%m%d")
    logger.info(
        "=== UMALOGI 日次予想一括生成 date=%s dry_run=%s ===", date_str, args.dry_run
    )

    # Step 1: 全レース暫定予想を Kelly ロジックで再生成
    if not args.skip_batch:
        logger.info("Step1: provisional_batch 開始 ...")
        succeeded = provisional_batch(target_date=date_str)
        logger.info("Step1: 完了 %d レース", len(succeeded))
    else:
        logger.info("Step1: skip-batch フラグあり — 再生成をスキップ")

    # Step 2: 下書き生成
    logger.info("Step2: 下書き生成 ...")
    saved = generate_drafts(date_str=date_str, dry_run=args.dry_run)

    # Step 3: Discord 通知
    logger.info("Step3: Discord 通知 ...")
    notify_complete(date_str=date_str, saved=saved, dry_run=args.dry_run)

    logger.info("=== 完了 ===")


if __name__ == "__main__":
    main()
