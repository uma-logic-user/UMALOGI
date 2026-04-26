"""
UMA-LOGI AI — 自律スケジューラー

競馬週次サイクルに合わせた自動実行スクリプト。
`schedule` ライブラリを使って各タスクを登録し、常駐プロセスとして動作する。

【32bit/64bit 分離設計】
  JVLink COM サーバーは 32bit プロセスからしか呼び出せない。
  このデーモン自体は 64bit Python で動作し、JVLink 操作は
  subprocess 経由で 32bit Python (`py -3-32`) を呼び出す。

スケジュール一覧:
  金曜 20:00   : JVLink RACE 同期(32bit) → WOOD(32bit) → マスタ(32bit)
                  → 暫定予想生成(64bit) → Discord 暫定予想サマリー通知
  土曜 07:30   : JVLink WOOD 同期(32bit)（調教タイム）
  日曜 07:30   : 同上
  土曜 08:30   : 当日全レース直前予想ループ起動（today_auto_runner）
  日曜 08:30   : 同上
  土曜 09:00   : WIN5 バッチ予測（独立ジョブ・WIN5締切前）
  日曜 09:00   : 同上
  土曜 13:00   : ウマニティ自動投稿（EV>=1.0 の直前予想をまとめて投稿）
  日曜 13:00   : 同上
  土曜 13:00   : レース中間 結果同期（OPT_STORED）
  土曜 15:30   : 同上
  日曜 13:00   : 同上
  日曜 15:30   : 同上
  土曜 17:30   : レース確定後 払戻同期(32bit) + 評価 + 通知 + 増分学習 + バックアップ
  日曜 17:30   : 同上
  月曜 06:00   : マスタ差分更新 (DIFN/BLOD)(32bit)
  月曜 07:00   : 週次全件再学習(64bit)
  月曜 08:00   : GitHub 自動コミット・プッシュ

Usage:
    python scripts/scheduler.py                        # デーモン起動
    python scripts/scheduler.py --run-now friday       # 即時実行（テスト用）
    python scripts/scheduler.py --run-now auto_runner  # 直前予想ループ即時起動
    python scripts/scheduler.py --run-now post_race --date 2024/01/06
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
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
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8",
                 errors="replace", closefd=False)
        ),
        RotatingFileHandler(
            _ROOT / "data" / "scheduler.log",
            maxBytes=50 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("scheduler")

try:
    import schedule  # type: ignore[import-untyped]
    _SCHEDULE_AVAILABLE = True
except ImportError:
    logger.warning("schedule がインストールされていません: pip install schedule")
    _SCHEDULE_AVAILABLE = False


# ================================================================
# サブプロセス / Discord ユーティリティ
# ================================================================

# JVLink は 32bit COM のため専用インタープリタを使用する
_PY32 = ["py", "-3.14-32"]
_PY64 = ["py"]


def _run(cmd: list[str], label: str, timeout: int = 3600) -> int:
    """
    サブプロセスを実行して returncode を返す。

    stdout/stderr はリアルタイムでロガーに流す。
    timeout 秒（デフォルト1時間）を超えた場合は強制終了して -1 を返す。
    """
    logger.info("[%s] 開始: %s", label, " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logger.info("[%s] %s", label, line)
        proc.wait(timeout=timeout)
        rc = proc.returncode
        if rc == 0:
            logger.info("[%s] 完了: rc=0", label)
        else:
            logger.warning("[%s] 終了: rc=%d", label, rc)
        return rc
    except subprocess.TimeoutExpired:
        proc.kill()
        logger.error("[%s] タイムアウト（%d秒）", label, timeout)
        return -1
    except Exception as exc:
        logger.error("[%s] 実行エラー: %s", label, exc)
        return -1


def _send_discord(text: str) -> None:
    """Discord Webhook にテキストメッセージを送信する。"""
    try:
        import requests as _req
    except ImportError:
        logger.warning("requests 未インストール — Discord 通知スキップ")
        return

    url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL 未設定 — Discord 通知スキップ")
        return
    try:
        resp = _req.post(url, json={"content": text}, timeout=10)
        resp.raise_for_status()
        logger.info("Discord 送信完了: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord 送信失敗: %s", exc)


def _send_discord_embed(embeds: list[dict]) -> None:
    """
    Discord Webhook に Embed メッセージを送信する。

    Discord Embed 仕様: https://discord.com/developers/docs/resources/message#embed-object
    color は 0xRRGGBB の整数値（例: シアン = 0x00C8FF = 52479）。
    """
    try:
        import requests as _req
    except ImportError:
        logger.warning("requests 未インストール — Discord 通知スキップ")
        return

    url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL 未設定 — Discord 通知スキップ")
        return
    try:
        resp = _req.post(url, json={"embeds": embeds}, timeout=10)
        resp.raise_for_status()
        logger.info("Discord Embed 送信完了: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord Embed 送信失敗: %s", exc)


def _notify_provisional_summary(target_date: str) -> None:
    """
    暫定予想バッチ完了後に Discord Embed でリッチ通知を送信する。

    送信内容:
      - 対象日・会場別レース数
      - EV ≥ 1.0 の推奨買い目（上位 10 件）を会場別に整理
      - 集計サマリー（総投資額・期待払戻）
      - ステップ完了タイムスタンプ

    Args:
        target_date: "YYYY-MM-DD" 形式の対象日
    """
    try:
        import sqlite3
        from datetime import datetime as _dt

        db_path = _ROOT / "data" / "umalogi.db"
        conn = sqlite3.connect(str(db_path))

        # 会場別レース数
        venue_rows = conn.execute(
            """
            SELECT venue, COUNT(*) AS cnt
            FROM races
            WHERE date = ?
            GROUP BY venue
            ORDER BY cnt DESC
            """,
            (target_date,),
        ).fetchall()
        race_count = sum(c for _, c in venue_rows)

        # EV >= 1.0 の暫定予想（馬分析・WIN5 除く）
        ev_rows = conn.execute(
            """
            SELECT r.venue, r.race_number, p.bet_type,
                   p.expected_value, p.recommended_bet, p.model_type
            FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            WHERE r.date = ?
              AND p.model_type LIKE '%暫定%'
              AND p.bet_type NOT IN ('馬分析', 'WIN5')
              AND p.expected_value >= 1.0
            ORDER BY p.expected_value DESC
            """,
            (target_date,),
        ).fetchall()

        # 土日両日を想定: 翌日も取得
        tomorrow2 = conn.execute(
            "SELECT COUNT(*) FROM races WHERE date > ? ORDER BY date LIMIT 1",
            (target_date,),
        ).fetchone()[0]

        conn.close()

        now_str = _dt.now().strftime("%Y-%m-%d %H:%M")
        color_ok   = 0x00C8FF   # シアン（正常）
        color_warn = 0xFFD700   # ゴールド（推奨なし）

        # ── 会場別レース数フィールド ─────────────────────────────
        venue_text = "\n".join(
            f"**{v}** {c}R" for v, c in venue_rows
        ) or "—"

        # ── 推奨買い目フィールド（上位 10 件） ───────────────────
        if ev_rows:
            pick_lines = []
            for venue, race_no, bet_type, ev, rec_bet, model in ev_rows[:10]:
                bet_str = f"¥{int(rec_bet):,}" if rec_bet else "—"
                icon = "⚡" if "卍" in model else "🎯"
                pick_lines.append(
                    f"{icon} **{venue}{race_no}R** {bet_type}  EV `{ev:.2f}`  推奨 {bet_str}"
                )
            if len(ev_rows) > 10:
                pick_lines.append(f"… 他 {len(ev_rows) - 10} 件")
            picks_text = "\n".join(pick_lines)
            total_rec = sum(r[4] or 0 for r in ev_rows)
            summary_text = (
                f"推奨 **{len(ev_rows)}** 件 ／ "
                f"総推奨投資額 **¥{int(total_rec):,}** ／ "
                f"対象 {race_count} レース"
            )
            embed_color = color_ok
        else:
            picks_text   = "EV ≥ 1.0 の買い目なし — 全レース見送り推奨"
            summary_text = f"対象 {race_count} レース"
            embed_color  = color_warn

        embed: dict = {
            "title": f"📋 暫定予想バッチ完了 — {target_date}",
            "color": embed_color,
            "description": summary_text,
            "fields": [
                {
                    "name": "🏟️ 会場別レース数",
                    "value": venue_text,
                    "inline": True,
                },
                {
                    "name": "🔥 EV ≥ 1.0 推奨買い目",
                    "value": picks_text,
                    "inline": False,
                },
            ],
            "footer": {
                "text": f"UMALOGI AI  |  {now_str} 生成  |  詳細は Streamlit ダッシュボードで確認",
            },
        }
        _send_discord_embed([embed])

    except Exception as exc:
        logger.warning("暫定予想サマリー通知失敗（続行）: %s", exc)
        _send_discord(f"📋 【暫定予想完了】{target_date} — サマリー取得エラー: {exc}")


# ================================================================
# 各ジョブ定義
# ================================================================

def job_friday_sync() -> None:
    """
    金曜夜バッチ（完全自動版）

    【設計】32bit と 64bit を subprocess で分離し、64bit デーモンから
    すべての処理を一気通貫で実行する。

    Step 1: JVLink RACE 同期       (32bit) — 出馬表・成績レコード取得
    Step 2: JVLink WOOD 同期       (32bit) — 調教タイム取得
    Step 3: JVLink マスタ差分更新  (32bit) — 騎手・調教師マスタ
    Step 4: AI 暫定予想生成        (64bit) — LightGBM で全レース暫定予想
    Step 5: Discord 暫定予想通知   (64bit) — EV≥1.0 の買い目サマリーを送信
    Step 6: DB バックアップ        (64bit) — ローカル + クラウド
    """
    logger.info("=" * 60)
    logger.info("=== [金曜バッチ] 開始 ===")
    logger.info("=" * 60)

    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    target_yyyymmdd = tomorrow.replace("-", "")

    errors: list[str] = []

    # ── Step 1: JVLink RACE 同期（32bit 必須）───────────────────
    rc = _run(_PY32 + ["-m", "src.ops.data_sync", "friday"], "JVLink-RACE")
    if rc != 0:
        errors.append(f"JVLink RACE 同期失敗(rc={rc})")

    # ── Step 2: JVLink WOOD 同期（32bit 必須）───────────────────
    rc = _run(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD")
    if rc != 0:
        errors.append(f"JVLink WOOD 同期失敗(rc={rc})")

    # ── Step 3: JVLink マスタ差分更新（32bit 必須）──────────────
    rc = _run(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters")
    if rc != 0:
        errors.append(f"JVLink マスタ更新失敗(rc={rc})")

    # ── Step 4: AI 暫定予想生成（64bit）─────────────────────────
    rc = _run(
        _PY64 + ["-m", "src.main_pipeline", "provisional", "--date", target_yyyymmdd],
        "暫定予想",
        timeout=3600,
    )
    if rc != 0:
        msg = f"🚨【緊急】金曜バッチ: 暫定予想生成が失敗しました (rc={rc})。サーバーを確認してください。"
        logger.error("[金曜バッチ] 暫定予想失敗 — Discord SOS 通知")
        _send_discord(msg)
        errors.append(f"暫定予想失敗(rc={rc})")
        # 予想失敗でもバックアップは実行する
    else:
        # ── Step 5: Discord 暫定予想通知 ───────────────────────
        _notify_provisional_summary(tomorrow)

    # ── Step 6: DB バックアップ（64bit）─────────────────────────
    try:
        from src.ops.backup import backup_db
        backup_db()
        logger.info("[金曜バッチ] バックアップ完了")
    except Exception as bk_exc:
        logger.warning("[金曜バッチ] バックアップ失敗: %s", bk_exc)

    if errors:
        logger.warning("=== [金曜バッチ] 完了（一部エラー: %s）===", " / ".join(errors))
    else:
        logger.info("=== [金曜バッチ] 完了（全ステップ正常）===")


def job_morning_wood() -> None:
    """土日朝: 調教タイム同期（32bit subprocess）"""
    logger.info("=== [朝調教同期] 開始 ===")
    rc = _run(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD朝")
    if rc != 0:
        logger.error("[朝調教同期] 失敗: rc=%d", rc)
    else:
        logger.info("=== [朝調教同期] 完了 ===")


_auto_runner_lock = threading.Lock()


def job_today_auto_runner() -> None:
    """
    土日 08:30: 当日全レース直前予想ループ起動

    【設計】today_auto_runner.py は一日中常駐する長時間プロセスのため、
    バックグラウンドスレッドで起動し、スケジューラーをブロックしない。

    - 各レースの推定発走 20 分前に `prerace_pipeline` を自動実行
    - prerace_pipeline は完了後に Discord へ全券種まとめ通知を送信
    - 全レース終了後にスレッドは自然終了する

    重複起動ガード: 既に auto_runner スレッドが動作中の場合はスキップ。
    """
    if not _auto_runner_lock.acquire(blocking=False):
        logger.warning("[直前予想ループ] 既に起動中のため二重起動をスキップします")
        return

    def _run_loop() -> None:
        try:
            logger.info("=== [直前予想ループ] バックグラウンドスレッド開始 ===")
            rc = _run(
                _PY64 + ["scripts/today_auto_runner.py"],
                "直前予想ループ",
                timeout=14 * 3600,  # 最大14時間（8:30〜22:30）
            )
            if rc != 0:
                logger.error("[直前予想ループ] 異常終了: rc=%d", rc)
                _send_discord(
                    f"🚨【緊急】直前予想ループが異常終了しました (rc={rc})。"
                    f"手動で `py scripts/today_auto_runner.py` を起動してください。"
                )
            else:
                logger.info("=== [直前予想ループ] 正常終了 ===")
        finally:
            _auto_runner_lock.release()

    thread = threading.Thread(
        target=_run_loop,
        name="today_auto_runner",
        daemon=True,
    )
    thread.start()
    logger.info("[直前予想ループ] バックグラウンドスレッドを起動しました (thread=%s)", thread.name)


def job_win5_prediction() -> None:
    """
    土日朝 9:00: WIN5 バッチ予測を実行する。

    金曜バッチで races が揃った後、当日レースの先頭5件を対象に
    Win5Engine で予測して predictions テーブルに保存し Discord 通知する。
    prerace_pipeline 内の _try_win5() とは独立して実行するため、
    WIN5 の締切（最初の対象レース発走前）に間に合うように朝9:00に設定。
    """
    logger.info("=== [WIN5予測] 開始 ===")
    try:
        from src.main_pipeline import win5_batch
        result = win5_batch()
        if result.get("skipped"):
            logger.info("[WIN5予測] スキップ: %s", result.get("reason", ""))
        elif result.get("error"):
            logger.error("[WIN5予測] エラー: %s", result["error"])
        else:
            logger.info(
                "[WIN5予測] 完了: EV=%.3f 推定払戻=¥%,.0f",
                result.get("ev", 0),
                result.get("bet", 0) or 0,
            )
    except Exception as e:
        logger.error("[WIN5予測] 例外: %s", e, exc_info=True)
    logger.info("=== [WIN5予測] 終了 ===")


def job_umanity_upload() -> None:
    """
    土日 各レース直前: ウマニティへの予想自動投稿。

    EV >= 1.0 の当日予想を Playwright でウマニティに投稿する。
    today_auto_runner が全レースを直前予想した後（概ね13:00以降）に
    まとめて投稿するバッチ。BAN 回避のためランダムスリープを内包する。

    UMANITY_EMAIL / UMANITY_PASSWORD が未設定の場合はスキップ。
    """
    import os
    if not os.environ.get("UMANITY_EMAIL") or not os.environ.get("UMANITY_PASSWORD"):
        logger.info("[Umanity投稿] UMANITY_EMAIL/PASSWORD 未設定のためスキップ")
        return

    logger.info("=== [Umanity投稿] 開始 ===")
    try:
        from src.ops.umanity_uploader import run_upload
        target_date = date.today().strftime("%Y%m%d")
        stats = run_upload(target_date=target_date, dry_run=False, headless=True)
        logger.info(
            "[Umanity投稿] 完了: 成功=%d スキップ=%d エラー=%d",
            stats["success"], stats["skip"], stats["error"],
        )
        _send_discord(
            f"🐴 **[Umanity] 本日の予想投稿完了**\n"
            f"成功: {stats['success']} 件 / スキップ: {stats['skip']} 件 / エラー: {stats['error']} 件"
        )
    except ImportError:
        logger.warning("[Umanity投稿] playwright 未インストール — pip install playwright && playwright install chromium")
    except Exception as e:
        logger.error("[Umanity投稿] 例外: %s", e, exc_info=True)
        _send_discord(f"🚨 [Umanity] 投稿失敗: {e}")
    logger.info("=== [Umanity投稿] 終了 ===")


def job_intraday_sync(target_date: str | None = None) -> None:
    """
    土日レース中間: 確定済みレースの結果を随時 DB に同期する。

    OPT_STORED を使用するため、TARGET frontier JV が先に取得済みでも確実に取得できる。
    評価・通知は行わず、race_results / race_payouts の充填のみを目的とする。
    """
    if target_date is None:
        target_date = date.today().strftime("%Y/%m/%d")
    date_yyyymmdd = target_date.replace("/", "")
    logger.info("=== [中間結果同期] %s 開始 ===", target_date)
    rc = _run(
        _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
        "JVLink-中間結果同期",
    )
    if rc == 0:
        logger.info("=== [中間結果同期] %s 完了 ===", target_date)
    else:
        logger.warning("[中間結果同期] 失敗: rc=%d", rc)


def job_post_race(target_date: str | None = None) -> None:
    """
    土日夕方: レース確定後の払戻同期(32bit) + 評価 + 通知 + 増分学習 + バックアップ
    """
    if target_date is None:
        target_date = date.today().strftime("%Y/%m/%d")
    logger.info("=== [レース後処理] %s 開始 ===", target_date)

    # Step 1: JVLink RACE 払戻同期（32bit）
    # OPT_NORMAL → OPT_STORED → OPT_SETUP の3段階フォールバックを data_sync が自動実施
    date_yyyymmdd = target_date.replace("/", "")
    rc = _run(
        _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
        "JVLink-払戻同期",
    )
    if rc != 0:
        logger.warning("[レース後処理] JVLink 払戻同期失敗（netkeiba フォールバックへ）: rc=%d", rc)

    # Step 2: 評価 + 通知 + 増分学習（64bit）
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import batch_evaluate_date

        conn = init_db()
        try:
            results = batch_evaluate_date(conn, target_date, notify=True)
            hit_count = sum(
                r["evaluation"].hit_count
                for r in results
                if "evaluation" in r
            )
            logger.info("[レース後処理] 完了: %d レース 合計的中=%d", len(results), hit_count)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[レース後処理] 評価失敗: %s", e, exc_info=True)

    # Step 3: DB バックアップ（エラーでも実行）
    try:
        from src.ops.backup import backup_db
        backup_db()
        logger.info("[バックアップ] 完了")
    except Exception as bk_exc:
        logger.warning("[バックアップ] 失敗: %s", bk_exc)

    # Step 4: ダッシュボード JSON 再生成（エラーでも実行）
    try:
        import subprocess as _sp, sys as _sys
        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen], check=True, timeout=120)
        logger.info("[ダッシュボード] JSON 再生成完了")
    except Exception as gen_exc:
        logger.warning("[ダッシュボード] JSON 再生成失敗: %s", gen_exc)

    logger.info("=== [レース後処理] %s 終了 ===", target_date)


def job_monday_masters() -> None:
    """月曜: マスタデータ差分更新（32bit subprocess）"""
    logger.info("=== [マスタ更新] 開始 ===")
    rc = _run(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters月曜")
    if rc != 0:
        logger.error("[マスタ更新] 失敗: rc=%d", rc)
    else:
        logger.info("=== [マスタ更新] 完了 ===")


def job_weekly_retrain() -> None:
    """月曜: 全件再学習 + summary.json 再生成（64bit）"""
    logger.info("=== [週次再学習] 開始 ===")
    try:
        from src.database.init_db import init_db
        from src.ops.retrain_trigger import weekly_retrain
        conn = init_db()
        try:
            result = weekly_retrain(conn)
            logger.info("[週次再学習] 完了: %s", result)
        finally:
            conn.close()
    except Exception as e:
        logger.error("[週次再学習] 失敗: %s", e, exc_info=True)

    # 再学習後に summary / financial.json を更新
    try:
        import subprocess as _sp, sys as _sys
        _web_gen = str(Path(__file__).resolve().parents[1] / "web" / "generate_data.py")
        _sp.run([_sys.executable, _web_gen, "--no-detail"], check=True, timeout=120)
        logger.info("[週次再学習] ダッシュボード JSON 更新完了")
    except Exception as gen_exc:
        logger.warning("[週次再学習] JSON 更新失敗: %s", gen_exc)


def job_git_push() -> None:
    """月曜: GitHub 自動プッシュ"""
    logger.info("=== [Git プッシュ] 開始 ===")
    try:
        from src.ops.git_ops import weekly_auto_commit
        success = weekly_auto_commit()
        logger.info("[Git プッシュ] %s", "成功" if success else "失敗")
    except Exception as e:
        logger.error("[Git プッシュ] 失敗: %s", e, exc_info=True)


def job_heartbeat() -> None:
    """毎時0分: Discord にハートビートを送信する（死活監視）"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    _send_discord(f"✅ UMALOGI alive ({now})")


def job_daily_backup() -> None:
    """毎日23:00: DB を data/backups/ に日付付きでバックアップ（5世代ローテーション）"""
    try:
        from src.ops.backup import backup_db
        path = backup_db()
        logger.info("DB バックアップ完了: %s", path)
    except Exception as exc:
        logger.error("DB バックアップ失敗: %s", exc)


# ================================================================
# スケジューラー本体
# ================================================================

def register_schedules() -> None:
    """全ジョブをスケジュールに登録する。"""
    if not _SCHEDULE_AVAILABLE:
        raise RuntimeError("schedule ライブラリが必要です: pip install schedule")

    # 金曜夜: JVLink同期(32bit) → 暫定予想(64bit) → Discord通知
    schedule.every().friday.at("20:00").do(job_friday_sync)

    # 土日朝: 調教タイム同期（JVLink 32bit）
    schedule.every().saturday.at("07:30").do(job_morning_wood)
    schedule.every().sunday.at("07:30").do(job_morning_wood)

    # 土日朝: WIN5 バッチ予測（9:00 — 金曜バッチ完了後・WIN5締切前）
    schedule.every().saturday.at("09:00").do(job_win5_prediction)
    schedule.every().sunday.at("09:00").do(job_win5_prediction)

    # 土日朝: 当日全レース直前予想ループ起動（Discord通知まで自動）
    schedule.every().saturday.at("08:30").do(job_today_auto_runner)
    schedule.every().sunday.at("08:30").do(job_today_auto_runner)

    # 土日昼: ウマニティ予想投稿（直前予想が揃う13:00以降）
    schedule.every().saturday.at("13:00").do(job_umanity_upload)
    schedule.every().sunday.at("13:00").do(job_umanity_upload)

    # 土日レース中: 確定済みレース結果を随時同期（OPT_STORED で確実取得）
    schedule.every().saturday.at("13:00").do(job_intraday_sync)
    schedule.every().saturday.at("15:30").do(job_intraday_sync)
    schedule.every().sunday.at("13:00").do(job_intraday_sync)
    schedule.every().sunday.at("15:30").do(job_intraday_sync)

    # 土日夕方: 払戻確定後のレース後処理（全レース終了後・OPT_STORED で確実取得）
    schedule.every().saturday.at("17:30").do(job_post_race)
    schedule.every().sunday.at("17:30").do(job_post_race)

    # 月曜: マスタ更新 → 全件再学習 → Git プッシュ
    schedule.every().monday.at("06:00").do(job_monday_masters)
    schedule.every().monday.at("07:00").do(job_weekly_retrain)
    schedule.every().monday.at("08:00").do(job_git_push)

    # 毎時0分: 死活監視ハートビート → Discord
    schedule.every().hour.at(":00").do(job_heartbeat)

    # 毎日23:00: DB バックアップ（5世代ローテーション）
    schedule.every().day.at("23:00").do(job_daily_backup)

    logger.info("スケジュール登録完了: %d ジョブ", len(schedule.jobs))
    for job in schedule.jobs:
        logger.info("  %s", job)


def run_daemon() -> None:
    """スケジューラーをデーモンとして常駐させる。Ctrl+C で終了。"""
    register_schedules()

    _send_discord(
        f"🤖 **[UMALOGI] スケジューラー起動**\n"
        f"起動時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"登録ジョブ: {len(schedule.jobs)} 件\n"
        f"次回実行: {schedule.next_run()}"
    )

    logger.info("UMA-LOGI AI スケジューラー起動 — Ctrl+C で終了")
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("スケジューラー停止")
        _send_discord("🛑 [UMALOGI] スケジューラーが手動停止されました")


# ================================================================
# CLI
# ================================================================

_JOB_MAP: dict[str, object] = {
    "friday":        job_friday_sync,
    "wood":          job_morning_wood,
    "win5":          job_win5_prediction,
    "umanity":       job_umanity_upload,
    "auto_runner":   job_today_auto_runner,
    "intraday_sync": job_intraday_sync,
    "post_race":     job_post_race,
    "masters":       job_monday_masters,
    "retrain":       job_weekly_retrain,
    "git":           job_git_push,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="UMA-LOGI AI スケジューラー")
    parser.add_argument(
        "--run-now",
        metavar="JOB",
        choices=list(_JOB_MAP.keys()),
        help=f"即時実行するジョブ: {list(_JOB_MAP.keys())}",
    )
    parser.add_argument("--date", help="post_race / intraday_sync ジョブの対象日 YYYY/MM/DD")
    args = parser.parse_args()

    if args.run_now:
        logger.info("即時実行: %s", args.run_now)
        fn = _JOB_MAP[args.run_now]
        if args.run_now in ("post_race", "intraday_sync") and args.date:
            fn(args.date)  # type: ignore[call-arg]
        elif args.run_now == "auto_runner":
            fn()  # type: ignore[call-arg]
            # バックグラウンドスレッドが終わるまで待機
            logger.info("直前予想ループ実行中… Ctrl+C で中断")
            try:
                while threading.active_count() > 1:
                    time.sleep(5)
            except KeyboardInterrupt:
                logger.info("中断")
        else:
            fn()  # type: ignore[call-arg]
    else:
        run_daemon()


if __name__ == "__main__":
    main()
