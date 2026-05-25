"""
scripts/batch_post_note_drafts_today.py

本日の高EV厳選レース予想を Note 下書きとして一括投稿する。

処理フロー:
  1. data/drafts/YYYYMMDD/*.txt から対象レースを読み込む
  2. EV スコアに応じて「期待値選別・推奨」タグを付与
  3. note_draft_publisher.save_draft() で下書き投稿
  4. 完了後 Discord へ結果通知

Usage:
    py scripts/batch_post_note_drafts_today.py
    py scripts/batch_post_note_drafts_today.py --date 20260524 --top 5
    py scripts/batch_post_note_drafts_today.py --date 20260524 --all
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import date as dt_date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("batch_post_note")

_DB_PATH = _ROOT / "data" / "umalogi.db"
_DRAFTS_DIR = _ROOT / "data" / "drafts"
_SESSION_FILE = _ROOT / ".note_session.json"

# Note 投稿タグ
_NOTE_TAGS = ["競馬", "UMALOGI", "AI予想", "期待値投資"]

# 高EV 判定しきい値（これ以上なら「重要・推奨」バナー付与）
_HIGH_EV_THRESHOLD = 5.0

# バッチ間の待機秒数（Bot 検知回避）
_INTER_POST_SLEEP = 15


# ── DB ヘルパー ──────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_top_races(
    conn: sqlite3.Connection,
    date_str: str,   # 'YYYY-MM-DD'
    top_n: int,
) -> list[dict[str, Any]]:
    """EV 上位レースを返す。date_str は 'YYYY-MM-DD' 形式。"""
    rows = conn.execute(
        """
        SELECT p.race_id,
               r.venue,
               r.race_number,
               r.race_name,
               MAX(p.expected_value) AS max_ev,
               COUNT(*)              AS n_picks
        FROM   predictions p
        JOIN   races        r ON p.race_id = r.race_id
        WHERE  r.date = ?
          AND  p.expected_value > 1.0
        GROUP  BY p.race_id
        ORDER  BY max_ev DESC
        LIMIT  ?
        """,
        (date_str, top_n),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_all_races(conn: sqlite3.Connection, date_str: str) -> list[dict[str, Any]]:
    """当日全レースを venue / race_number 順で返す。"""
    rows = conn.execute(
        """
        SELECT p.race_id,
               r.venue,
               r.race_number,
               r.race_name,
               MAX(p.expected_value) AS max_ev,
               COUNT(*)              AS n_picks
        FROM   predictions p
        JOIN   races        r ON p.race_id = r.race_id
        WHERE  r.date = ?
        GROUP  BY p.race_id
        ORDER  BY r.venue, r.race_number
        """,
        (date_str,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── ドラフトファイル読み込み ─────────────────────────────────────────────

def _find_draft_file(date_yyyymmdd: str, race_id: str) -> Path | None:
    """data/drafts/YYYYMMDD/ 以下から race_id に対応する txt を探す。"""
    draft_dir = _DRAFTS_DIR / date_yyyymmdd
    if not draft_dir.exists():
        return None
    # 完全一致ファイル名
    for f in draft_dir.glob(f"{race_id}_*.txt"):
        return f
    # race_id プレフィックスで探す
    for f in draft_dir.glob("*.txt"):
        if f.stem.startswith(race_id):
            return f
    return None


def _parse_draft(path: Path) -> tuple[str, str]:
    """
    draft txt から (title_hint, body) を返す。
    先頭の【...】行をタイトルヒントとして取り出す。
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    title_hint = ""
    body_start = 0
    if lines and lines[0].startswith("【"):
        title_hint = lines[0].strip("【】").strip()
        body_start = 1
    body = "\n".join(lines[body_start:]).strip()
    return title_hint, body


# ── Note 記事フォーマット ─────────────────────────────────────────────────

def _build_note_title(venue: str, race_number: int, race_name: str, date_str: str) -> str:
    """
    Note 投稿タイトルを生成する。
    例: 【AI予想】2026/05/24 東京11R 優駿牝馬｜UMALOGI期待値選別予想
    """
    d = date_str  # 'YYYY-MM-DD'
    ymd = d.replace("-", "/")
    return f"【AI予想】{ymd} {venue}{race_number}R {race_name}｜UMALOGI期待値選別予想"


def _build_note_body(
    draft_body: str,
    race_info: dict[str, Any],
    date_str: str,
    is_high_ev: bool,
) -> str:
    """Note 本文を組み立てる。"""
    venue       = race_info["venue"]
    race_number = race_info["race_number"]
    race_name   = race_info["race_name"]
    max_ev      = race_info["max_ev"]
    d           = date_str.replace("-", "/")

    parts: list[str] = []

    # 高EV バナー
    if is_high_ev:
        parts.append(
            "【重要：期待値選別・推奨】\n"
            f"このレースは AI が最高期待値 **EV={max_ev:.2f}** を検出した「激熱推奨」レースです。\n"
            "過去実績でこのクラスの EV は高回収率と相関しています。\n"
        )

    parts.append(
        f"# {d} {venue}{race_number}R「{race_name}」AI 期待値予想\n\n"
        f"> UMALOGI — 4 モデル合意 × 期待値ベース買い目選別システム\n\n"
        "---\n"
    )

    parts.append(draft_body)

    parts.append(
        "\n\n---\n"
        "## 免責事項\n"
        "本予想は統計的期待値に基づくものであり、的中を保証するものではありません。\n"
        "投資は自己責任でお願いします。資金管理を徹底し、余裕資金の範囲内でご参加ください。\n\n"
        "*UMALOGI — AI 競馬予測プラットフォーム*"
    )

    return "\n".join(parts)


# ── Discord 通知 ─────────────────────────────────────────────────────────

def _notify_discord(
    results: list[dict[str, Any]],
    date_str: str,
    total: int,
    success: int,
) -> None:
    try:
        from src.notification.discord_notifier import DiscordNotifier
        dn = DiscordNotifier()
        lines = [
            f"📝 **Note 下書き一括投稿完了** ({date_str})",
            f"投稿試行: {total} 件 / 成功: {success} 件",
            "",
            "**投稿レース一覧**",
        ]
        for r in results:
            status = "✅" if r["ok"] else "❌"
            lines.append(
                f"{status} {r['venue']} R{r['race_number']:02d} {r['race_name']}"
                f"　EV={r['max_ev']:.1f}"
            )
        lines.append("")
        lines.append("下書き確認: https://note.com/notes")
        msg = "\n".join(lines)
        dn.send_system_text(msg)
        logger.info("[discord] 投稿完了通知を送信しました")
    except Exception as e:
        logger.warning("[discord] 通知失敗: %s", e)


# ── メイン ───────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="本日の高EV予想を Note 下書きとして一括投稿")
    p.add_argument("--date", default=None,
                   help="対象日付 YYYYMMDD (省略時=本日)")
    p.add_argument("--top", type=int, default=5,
                   help="EV 上位 N レースを投稿 (デフォルト=5)")
    p.add_argument("--all", action="store_true",
                   help="当日全レースを投稿 (--top を無視)")
    p.add_argument("--dry-run", action="store_true",
                   help="実際には投稿しない (タイトル・本文をコンソールに出力)")
    args = p.parse_args()

    # 日付決定
    if args.date:
        yyyymmdd = args.date
        iso_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
    else:
        today = dt_date.today()
        yyyymmdd = today.strftime("%Y%m%d")
        iso_date = today.strftime("%Y-%m-%d")

    logger.info("対象日付: %s", iso_date)

    # セッション確認
    if not args.dry_run and not _SESSION_FILE.exists():
        logger.error(
            "セッションファイルが見つかりません: %s\n"
            "  先に: py scripts/publish_note_draft.py --login",
            _SESSION_FILE,
        )
        sys.exit(1)

    # 対象レース取得
    conn = _db()
    try:
        if args.all:
            races = _fetch_all_races(conn, iso_date)
        else:
            races = _fetch_top_races(conn, iso_date, args.top)
    finally:
        conn.close()

    if not races:
        logger.warning("対象レースが見つかりません (date=%s)", iso_date)
        sys.exit(0)

    logger.info("投稿対象: %d レース", len(races))

    # Note 投稿ライブラリ
    if not args.dry_run:
        from src.ops.note_draft_publisher import save_draft
    else:
        save_draft = None  # type: ignore[assignment]

    results: list[dict[str, Any]] = []
    success = 0

    for i, race in enumerate(races, 1):
        race_id     = race["race_id"]
        venue       = race["venue"]
        race_number = race["race_number"]
        race_name   = race["race_name"]
        max_ev      = race["max_ev"] or 0.0

        logger.info(
            "[%d/%d] %s %sR%02d %s (max_ev=%.2f)",
            i, len(races), race_id, venue, race_number, race_name, max_ev,
        )

        # ドラフトファイル読み込み
        draft_path = _find_draft_file(yyyymmdd, race_id)
        if not draft_path:
            logger.warning("  ドラフトファイルが見つかりません: %s", race_id)
            results.append({**race, "ok": False, "reason": "draft not found"})
            continue

        _, draft_body = _parse_draft(draft_path)

        # Note 記事組み立て
        is_high_ev  = max_ev >= _HIGH_EV_THRESHOLD
        note_title  = _build_note_title(venue, race_number, race_name, iso_date)
        note_body   = _build_note_body(draft_body, race, iso_date, is_high_ev)

        logger.info("  タイトル: %s", note_title)
        logger.info("  高EV推奨: %s (max_ev=%.2f)", is_high_ev, max_ev)

        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"TITLE: {note_title}")
            print(f"HIGH_EV: {is_high_ev}")
            print(f"BODY (先頭300文字):\n{note_body[:300]}")
            results.append({**race, "ok": True, "reason": "dry-run"})
            success += 1
            continue

        # Note 下書き投稿
        try:
            ok = save_draft(
                title=note_title,
                body=note_body,
                tags=_NOTE_TAGS,
            )
        except Exception as e:
            logger.error("  save_draft 例外: %s", e)
            ok = False

        result_entry = {**race, "ok": ok, "reason": "ok" if ok else "save_draft failed"}
        results.append(result_entry)

        if ok:
            success += 1
            logger.info("  ✅ 下書き保存成功")
        else:
            logger.error("  ❌ 下書き保存失敗")

        # Bot 検知回避のためにインターバルを挟む
        if i < len(races):
            logger.info("  %d 秒待機中...", _INTER_POST_SLEEP)
            time.sleep(_INTER_POST_SLEEP)

    # サマリー
    print(f"\n{'='*60}")
    print(f"投稿完了: {success}/{len(races)} 件")
    for r in results:
        mark = "✅" if r["ok"] else "❌"
        print(f"  {mark} {r['venue']} R{r['race_number']:02d} {r['race_name']} EV={r['max_ev']:.1f}")

    # Discord 通知
    if not args.dry_run:
        _notify_discord(results, iso_date, len(races), success)

    sys.exit(0 if success == len(races) else 1)


if __name__ == "__main__":
    main()
