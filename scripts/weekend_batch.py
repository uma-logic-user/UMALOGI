"""
UMALOGI 週末バッチ — Pre/Post 2フェーズ オーケストレーター

Pre フェーズ (07:00):
  1. DB から本日の EV>=1.0 予想を取得
  2. レースごとに note 記事データを生成 → note.com に下書き保存
  3. ウマニティ 予想コロシアムに投稿
  4. X (Twitter) に本日予想のお知らせツイート

Post フェーズ (18:30):
  5. DB から本日の的中・払戻実績を取得
  6. 的中証拠画像 (Pillow) を生成
  7. X に本日 P&L 結果ツイート（画像付き）
  8. batch_runs テーブルにログを書き込み

Usage:
  py scripts/weekend_batch.py --phase pre
  py scripts/weekend_batch.py --phase post
  py scripts/weekend_batch.py --phase pre  --date 20260510 --dry-run
  py scripts/weekend_batch.py --phase post --date 20260510 --dry-run
  py scripts/weekend_batch.py --phase pre  --no-headless
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

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
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weekend_batch")

_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR = _ROOT / "outputs"

# 競馬場コード → 名称
_VENUE_NAMES: dict[str, str] = {
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


# ================================================================
# ── DB ヘルパー ────────────────────────────────────────────────
# ================================================================


def _connect() -> sqlite3.Connection:
    if not _DB_PATH.exists():
        raise FileNotFoundError(f"DB が見つかりません: {_DB_PATH}")
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _fetch_predictions(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
    """指定日の EV>=1.0 予想を 1レース1件（卍複勝優先）で返す。"""
    date_iso = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:8]}"
    return conn.execute(
        """
        WITH ranked AS (
            SELECT
                p.id          AS prediction_id,
                p.race_id,
                r.race_name,
                r.venue,
                r.race_number,
                r.date        AS race_date,
                r.distance,
                r.surface,
                p.model_type,
                p.bet_type,
                p.combination_json,
                p.recommended_bet,
                p.expected_value,
                p.notes,
                ROW_NUMBER() OVER (
                    PARTITION BY p.race_id
                    ORDER BY
                        CASE WHEN p.model_type LIKE '卍%' AND p.bet_type='複勝' THEN 0 ELSE 1 END,
                        COALESCE(p.expected_value, 0) DESC
                ) AS rn
            FROM predictions p
            JOIN races r ON p.race_id = r.race_id
            WHERE r.date = ?
              AND p.expected_value >= 1.0
        )
        SELECT * FROM ranked WHERE rn = 1
        ORDER BY race_id
        """,
        (date_iso,),
    ).fetchall()


def _fetch_results(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
    """指定日の prediction_results（的中実績）を返す。"""
    date_iso = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:8]}"
    return conn.execute(
        """
        SELECT
            pr.prediction_id,
            pr.is_hit,
            pr.payout,
            pr.roi,
            COALESCE(p.recommended_bet, 100)  AS bet_amount,
            p.race_id,
            p.bet_type,
            p.model_type,
            p.combination_json,
            r.race_name,
            r.venue,
            r.race_number,
            r.date AS race_date
        FROM prediction_results pr
        JOIN predictions p ON pr.prediction_id = p.id
        JOIN races r       ON p.race_id = r.race_id
        WHERE r.date = ?
        ORDER BY p.race_id
        """,
        (date_iso,),
    ).fetchall()


def _upsert_batch_run(
    conn: sqlite3.Connection,
    run_date: str,
    phase: str,
    status: str,
    note_ok: int,
    umanity_ok: int,
    x_ok: int,
    error_msg: str | None,
) -> None:
    date_iso = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:8]}"
    existing = conn.execute(
        "SELECT id FROM batch_runs WHERE run_date=? AND phase=?",
        (date_iso, phase),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE batch_runs
            SET status=?, note_ok=?, umanity_ok=?, x_ok=?, error_msg=?, finished_at=datetime('now','localtime')
            WHERE run_date=? AND phase=?
            """,
            (status, note_ok, umanity_ok, x_ok, error_msg, date_iso, phase),
        )
    else:
        conn.execute(
            """
            INSERT INTO batch_runs (run_date, phase, status, note_ok, umanity_ok, x_ok, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (date_iso, phase, status, note_ok, umanity_ok, x_ok, error_msg),
        )
    conn.commit()


# ================================================================
# ── Pre フェーズ ───────────────────────────────────────────────
# ================================================================


def run_pre(run_date: str, *, dry_run: bool, headless: bool) -> dict[str, int]:
    """07:00 フェーズ: note 下書き + ウマニティ投稿 + X 告知。"""
    logger.info("=" * 55)
    logger.info("Pre フェーズ開始 (date=%s dry_run=%s)", run_date, dry_run)
    logger.info("=" * 55)

    stats = {"note_ok": 0, "umanity_ok": 0, "x_ok": 0, "error": 0}
    errors: list[str] = []

    conn = _connect()
    preds = _fetch_predictions(conn, run_date)
    logger.info("本日の EV>=1.0 予想: %d 件", len(preds))

    if not preds:
        logger.info("予想なし。Pre フェーズをスキップします。")
        conn.close()
        _upsert_batch_run(
            conn if not conn else _connect(),
            run_date,
            "pre",
            "success",
            0,
            0,
            0,
            "予想なし",
        )
        return stats

    # ── 1. note 下書き保存 ────────────────────────────────────
    note_ok = _run_note_draft(conn, preds, run_date, dry_run)
    if note_ok:
        stats["note_ok"] = 1
    else:
        errors.append("note 下書き保存失敗")

    # ── 2. ウマニティ投稿 ─────────────────────────────────────
    umanity_ok = _run_umanity(run_date, dry_run=dry_run, headless=headless)
    stats["umanity_ok"] = umanity_ok

    # ── 3. X 告知ツイート ─────────────────────────────────────
    x_ok = _post_x_pre_announcement(preds, run_date, dry_run)
    stats["x_ok"] = 1 if x_ok else 0

    # ── ログ記録 ──────────────────────────────────────────────
    conn2 = _connect()
    status = "success" if not errors else "partial"
    _upsert_batch_run(
        conn2,
        run_date,
        "pre",
        status,
        stats["note_ok"],
        stats["umanity_ok"],
        stats["x_ok"],
        "; ".join(errors) if errors else None,
    )
    conn2.close()
    conn.close()

    logger.info("Pre フェーズ完了: %s", stats)
    return stats


def _run_note_draft(
    conn: sqlite3.Connection,
    preds: list[sqlite3.Row],
    run_date: str,
    dry_run: bool,
) -> bool:
    """note.com に下書き記事を保存する。"""
    try:
        from src.ops.note_draft_publisher import save_draft
    except ImportError as e:
        logger.error("[Pre] note_draft_publisher インポート失敗: %s", e)
        return False

    date_label = f"{run_date[:4]}年{run_date[4:6]}月{run_date[6:8]}日"

    # 無料エリア（馬番・bet_type・EV 一覧）
    free_lines: list[str] = []
    for p in preds[:10]:  # 最大10レース
        combos: list = []
        if p["combination_json"]:
            try:
                combos = json.loads(p["combination_json"])
            except Exception:
                pass
        combo_str = ""
        if combos:
            first = combos[0]
            combo_str = (
                "-".join(str(n) for n in first)
                if isinstance(first, list)
                else str(first)
            )
        venue = _VENUE_NAMES.get(str(p["race_id"])[4:6], "")
        ev_val = p["expected_value"] or 0.0
        free_lines.append(
            f"◆ {venue} {p['race_number']}R  {p['bet_type']} {combo_str}  EV={ev_val:.2f}"
        )

    body = (
        f"# {date_label} UMALOGI AI 週末予想まとめ\n\n"
        f"## 無料エリア — 注目買い目 top{len(free_lines)}\n\n"
        + "\n".join(free_lines)
        + "\n\n---\n\n"
        "## 有料エリア — 全SHAP根拠文 & Kelly投資額\n\n"
        "（ここから有料コンテンツです）\n\n"
        "※ 本予想は JRA-VAN データ × LightGBM モデルによる期待値ベース絞り込みです。\n"
        "※ 投資は自己責任でお願いします。\n"
    )

    if dry_run:
        logger.info(
            "[Pre][DRY-RUN] note 下書き保存をスキップ (title=%s)",
            f"UMALOGI {date_label}",
        )
        return True

    return save_draft(
        title=f"UMALOGI AI予想｜{date_label}",
        body=body,
        tags=["競馬", "AI予想", "UMALOGI", "JRA", "期待値"],
    )


def _run_umanity(run_date: str, dry_run: bool, headless: bool) -> int:
    """ウマニティ コロシアムに投稿し、成功件数を返す。"""
    try:
        from src.ops.umanity_uploader import run_upload

        stats = run_upload(
            target_date=run_date,
            dry_run=dry_run,
            headless=headless,
        )
        total_ok = stats.get("coliseum_ok", 0) + stats.get("board_ok", 0)
        logger.info("[Pre] ウマニティ投稿結果: %s", stats)
        return total_ok
    except Exception as e:
        logger.error("[Pre] ウマニティ投稿失敗: %s", e)
        return 0


def _post_x_pre_announcement(
    preds: list[sqlite3.Row],
    run_date: str,
    dry_run: bool,
) -> bool:
    """X に本日予想のお知らせを投稿する。"""
    try:
        from src.notification.twitter_notifier import TwitterNotifier

        notifier = TwitterNotifier()
    except Exception as e:
        logger.error("[Pre] TwitterNotifier 初期化失敗: %s", e)
        return False

    date_label = f"{run_date[4:6]}/{run_date[6:8]}"
    ev_max = max((p["expected_value"] or 0.0) for p in preds)
    race_count = len(preds)

    text = (
        f"🏇【本日の AI 予想】{date_label}\n\n"
        f"EV>=1.0 買い目: {race_count} レース\n"
        f"最大 EV: {ev_max:.2f}\n\n"
        f"JRA-VAN × LightGBM 期待値特化モデルによる絞り込み予想\n"
        f"👇 詳細は note で公開中（下書き準備完了）\n"
        f"#競馬AI #UMALOGI #JRA"
    )

    if dry_run:
        logger.info("[Pre][DRY-RUN] X ツイートスキップ:\n%s", text)
        return True

    try:
        ok = notifier.post_text(text)
        logger.info("[Pre] X ツイート: %s", "成功" if ok else "失敗")
        return ok
    except Exception as e:
        logger.error("[Pre] X ツイート例外: %s", e)
        return False


# ================================================================
# ── Post フェーズ ──────────────────────────────────────────────
# ================================================================


def run_post(run_date: str, *, dry_run: bool) -> dict[str, int | float]:
    """18:30 フェーズ: 結果取得 + 的中画像生成 + X 報告。"""
    logger.info("=" * 55)
    logger.info("Post フェーズ開始 (date=%s dry_run=%s)", run_date, dry_run)
    logger.info("=" * 55)

    stats: dict[str, int | float] = {
        "hits": 0,
        "total": 0,
        "payout": 0.0,
        "bet": 0.0,
        "x_ok": 0,
    }
    errors: list[str] = []

    conn = _connect()
    results = _fetch_results(conn, run_date)
    logger.info("本日の prediction_results: %d 件", len(results))

    if not results:
        logger.info("結果なし。Post フェーズをスキップします。")
        conn.close()
        _upsert_batch_run(_connect(), run_date, "post", "success", 0, 0, 0, "結果なし")
        return stats

    # ── 集計 ──────────────────────────────────────────────────
    hits = [r for r in results if r["is_hit"]]
    total_bet = sum(r["bet_amount"] or 0 for r in results)
    total_payout = sum(r["payout"] or 0 for r in results)
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0.0
    profit = total_payout - total_bet

    stats["hits"] = len(hits)
    stats["total"] = len(results)
    stats["payout"] = total_payout
    stats["bet"] = total_bet

    logger.info(
        "集計: 的中 %d/%d  払戻 ¥%s  投資 ¥%s  ROI=%.1f%%  損益=¥%s",
        len(hits),
        len(results),
        f"{total_payout:,.0f}",
        f"{total_bet:,.0f}",
        roi,
        f"{profit:+,.0f}",
    )

    # ── 的中証拠画像生成 ───────────────────────────────────────
    hit_images: list[Path] = _generate_hit_cards(hits, run_date)

    # ── X 結果ツイート ─────────────────────────────────────────
    x_ok = _post_x_post_report(
        run_date=run_date,
        hits=len(hits),
        total=len(results),
        total_bet=total_bet,
        total_payout=total_payout,
        roi=roi,
        profit=profit,
        hit_images=hit_images,
        dry_run=dry_run,
    )
    stats["x_ok"] = 1 if x_ok else 0

    # ── ログ記録 ──────────────────────────────────────────────
    conn2 = _connect()
    status = "success" if not errors else "partial"
    _upsert_batch_run(
        conn2,
        run_date,
        "post",
        status,
        0,
        0,
        stats["x_ok"],  # type: ignore[arg-type]
        "; ".join(errors) if errors else None,
    )
    conn2.close()
    conn.close()

    logger.info("Post フェーズ完了: %s", stats)
    return stats


def _generate_hit_cards(
    hits: list[sqlite3.Row],
    run_date: str,
) -> list[Path]:
    """的中した予想に対して証拠画像を生成し、パスリストを返す。"""
    from src.notification.image_builder import build_hit_image

    out_dir = _OUT_DIR / "hit_cards" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)

    paths: list[Path] = []
    for r in hits[:5]:  # 最大5枚
        venue = _VENUE_NAMES.get(str(r["race_id"])[4:6], "")
        out_path = out_dir / f"{r['race_id']}_{r['bet_type']}.png"

        payout = float(r["payout"] or 0)
        invested = float(r["bet_amount"] or 100)
        roi_val = payout / invested * 100 if invested > 0 else 0.0

        # combination_json から文字列リストを生成
        combo_strs: list[str] = []
        if r["combination_json"]:
            try:
                raw = json.loads(r["combination_json"])
                for entry in raw if raw else []:
                    if isinstance(entry, list):
                        combo_strs.append("-".join(str(n) for n in entry))
                    else:
                        combo_strs.append(str(entry))
            except Exception:
                pass
        if not combo_strs:
            combo_strs = ["?"]

        result = build_hit_image(
            race_name=f"{venue}{r['race_number']}R {r['race_name'] or ''}",
            date=r["race_date"],
            bet_type=r["bet_type"],
            combination=combo_strs[:3],
            payout=payout,
            roi=roi_val,
            invested=invested,
            out_path=out_path,
        )
        if result:
            paths.append(Path(str(result)))
            logger.info("[Post] 的中カード生成: %s", out_path.name)

    return paths


def _post_x_post_report(
    run_date: str,
    hits: int,
    total: int,
    total_bet: float,
    total_payout: float,
    roi: float,
    profit: float,
    hit_images: list[Path],
    dry_run: bool,
) -> bool:
    """X に本日 P&L 結果を投稿する。"""
    try:
        from src.notification.twitter_notifier import TwitterNotifier

        notifier = TwitterNotifier()
    except Exception as e:
        logger.error("[Post] TwitterNotifier 初期化失敗: %s", e)
        return False

    date_label = f"{run_date[4:6]}/{run_date[6:8]}"
    profit_sign = "+" if profit >= 0 else ""

    if roi >= 150:
        header = "🎉【大的中】"
    elif hits > 0:
        header = "✅【的中】"
    else:
        header = "📊【本日結果】"

    text = (
        f"{header} {date_label} UMALOGI AI\n\n"
        f"的中: {hits}/{total} レース\n"
        f"回収率: {roi:.0f}%\n"
        f"損益: {profit_sign}¥{int(abs(profit)):,}\n"
        f"（投資 ¥{int(total_bet):,} → 払戻 ¥{int(total_payout):,}）\n\n"
        f"#競馬AI #UMALOGI #JRA #競馬予想"
    )

    # 画像は最初の1枚のみ添付
    image_path = str(hit_images[0]) if hit_images else None

    if dry_run:
        logger.info("[Post][DRY-RUN] X 結果ツイートスキップ:\n%s", text)
        return True

    try:
        ok = notifier.post_text(text, image_path=image_path)
        logger.info("[Post] X ツイート: %s", "成功" if ok else "失敗")
        return ok
    except Exception as e:
        logger.error("[Post] X ツイート例外: %s", e)
        return False


# ================================================================
# ── CLI ────────────────────────────────────────────────────────
# ================================================================


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="UMALOGI 週末バッチ (Pre/Post)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  py scripts/weekend_batch.py --phase pre
  py scripts/weekend_batch.py --phase post
  py scripts/weekend_batch.py --phase pre  --date 20260510 --dry-run
  py scripts/weekend_batch.py --phase post --date 20260510 --dry-run
  py scripts/weekend_batch.py --phase pre  --no-headless
""",
    )
    p.add_argument(
        "--phase", required=True, choices=["pre", "post"], help="実行フェーズ"
    )
    p.add_argument("--date", help="対象日 YYYYMMDD（省略=今日）")
    p.add_argument("--dry-run", action="store_true", help="実際の投稿/保存なし")
    p.add_argument(
        "--no-headless",
        action="store_true",
        help="ブラウザ画面を表示する（デバッグ用）",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    run_date = args.date or date.today().strftime("%Y%m%d")
    headless = not args.no_headless

    logger.info(
        "UMALOGI Weekend Batch  phase=%s  date=%s  dry=%s  headless=%s",
        args.phase,
        run_date,
        args.dry_run,
        headless,
    )

    if args.phase == "pre":
        stats = run_pre(run_date, dry_run=args.dry_run, headless=headless)
    else:
        stats = run_post(run_date, dry_run=args.dry_run)

    print()
    print("=" * 55)
    print(f"  UMALOGI Weekend Batch 完了  [{args.phase.upper()}]")
    print(f"  date={run_date}  dry_run={args.dry_run}")
    print("-" * 55)
    for k, v in stats.items():
        print(f"  {k:<15s}: {v}")
    print("=" * 55)


if __name__ == "__main__":
    main()
