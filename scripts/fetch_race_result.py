# -*- coding: utf-8 -*-
"""
レース結果速報取得スクリプト

発走後15分で today_auto_runner から呼び出す。
JRA-VAN (JVLink) 経由でレース結果・払戻を取得して DB に保存し、
予想評価（prediction_results）を更新してダッシュボード JSON を再生成する。

【注意】netkeiba.com へのアクセスは CLAUDE.md により永久禁止。
全データは JVLink 経由で取得する。

Usage:
    python scripts/fetch_race_result.py --race-id 202603010501
    python scripts/fetch_race_result.py --date 20260425   # 指定日の全未取得レース
    python scripts/fetch_race_result.py --date 20260425 --all   # 既取得も上書き
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)
        ),
    ],
)
logger = logging.getLogger("fetch_result")


def _run_jvlink_race_sync(race_date: str) -> bool:
    """
    JVLink RACE TODAY 同期を実行して当日の SE/HR レコードを DB に取り込む。

    JVLINK_DISABLED=1 の場合は即座に False を返し netkeiba フォールバックへ委譲する。
    （JVLink ダイアログによるブロッキングを完全回避）

    Args:
        race_date: YYYYMMDD 形式の日付

    Returns:
        True = 成功 / False = 失敗（JVLink無効時は常に False）
    """
    if os.getenv("JVLINK_DISABLED", "").strip() == "1":
        logger.info(
            "JVLINK_DISABLED=1: JVLink 同期をスキップ → netkeiba フォールバックへ (date=%s)",
            race_date,
        )
        return False

    logger.info("JVLink RACE TODAY 同期開始: date=%s", race_date)
    try:
        proc = subprocess.run(
            ["py", "-3.14-32",
             str(_ROOT / "scripts" / "_jvlink_force_worker.py"),
             "--dataspec", "RACE",
             "--fromtime", race_date,
             "--option", "3"],  # OPT_TODAY: 当日データ
            cwd=str(_ROOT),
            timeout=180,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        last_line = proc.stdout.splitlines()[-1] if proc.stdout else ""
        if proc.returncode != 0:
            logger.warning("JVLink ワーカー rc=%d stderr=%s", proc.returncode, proc.stderr[:300])
            return False
        logger.info("JVLink 同期完了: %s", last_line)
        return True
    except subprocess.TimeoutExpired:
        logger.error("JVLink ワーカー タイムアウト (180s): date=%s", race_date)
        return False
    except Exception as exc:
        logger.error("JVLink ワーカー 実行失敗: %s", exc)
        return False


def _get_target_race_ids(conn, date_iso: str, force_all: bool) -> list[str]:
    """取得対象の race_id リストを返す。"""
    if force_all:
        rows = conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (date_iso,),
        ).fetchall()
    else:
        # rank が存在しないレースのみ
        rows = conn.execute(
            """
            SELECT r.race_id FROM races r
            WHERE r.date = ?
              AND NOT EXISTS (
                  SELECT 1 FROM race_results rr
                  WHERE rr.race_id = r.race_id AND rr.rank IS NOT NULL AND rr.rank > 0
              )
            ORDER BY r.race_id
            """,
            (date_iso,),
        ).fetchall()
    return [r[0] for r in rows]


def _upsert_race_results(conn, race_id: str, race_info) -> int:
    """
    netkeiba から取得した RaceInfo を race_results に UPSERT する。

    既存レコード（JVLink が事前に rank=NULL で作成した行）を確定値で上書きする。
    INSERT OR IGNORE では上書きされないため ON CONFLICT DO UPDATE を使う。
    """
    saved = 0
    with conn:
        for r in race_info.results:
            conn.execute(
                """
                INSERT INTO race_results
                    (race_id, horse_id, horse_name, rank,
                     gate_number, horse_number,
                     sex_age, weight_carried, jockey, trainer,
                     finish_time, margin, popularity, win_odds,
                     horse_weight, horse_weight_diff)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_id, horse_name) DO UPDATE SET
                    rank              = excluded.rank,
                    gate_number       = excluded.gate_number,
                    horse_number      = excluded.horse_number,
                    finish_time       = excluded.finish_time,
                    margin            = excluded.margin,
                    popularity        = excluded.popularity,
                    win_odds          = excluded.win_odds,
                    horse_weight      = excluded.horse_weight,
                    horse_weight_diff = excluded.horse_weight_diff,
                    jockey            = COALESCE(excluded.jockey, jockey),
                    trainer           = COALESCE(excluded.trainer, trainer)
                """,
                (
                    race_id,
                    getattr(r, "horse_id", None),
                    r.horse_name,
                    r.rank,
                    getattr(r, "gate_number", None),
                    getattr(r, "horse_number", None),
                    getattr(r, "sex_age", ""),
                    getattr(r, "weight_carried", 0.0),
                    getattr(r, "jockey", ""),
                    getattr(r, "trainer", ""),
                    getattr(r, "finish_time", None),
                    getattr(r, "margin", None),
                    getattr(r, "popularity", None),
                    getattr(r, "win_odds", None),
                    getattr(r, "horse_weight", None),
                    getattr(r, "horse_weight_diff", None),
                ),
            )
            saved += 1
    return saved


def _fetch_result_from_netkeiba(race_id: str, conn) -> bool:
    """
    netkeiba から着順・払戻を取得して DB に保存する（JVLink フォールバック）。

    Returns:
        True = 保存成功（1着馬確認済み） / False = 未確定 or 取得失敗
    """
    from src.scraper.netkeiba import fetch_race_results, fetch_race_payouts
    from src.database.init_db import insert_race, insert_race_payouts

    race_date = f"{race_id[0:4]}-{race_id[4:6]}-{race_id[6:8]}"

    try:
        logger.info("netkeiba から着順取得: race_id=%s", race_id)
        race_info = fetch_race_results(
            race_id, race_date=race_date, fetch_pedigree=False, delay=1.0, max_retries=3,
        )
    except Exception as exc:
        logger.warning("netkeiba 着順取得失敗 (race_id=%s): %s", race_id, exc)
        return False

    # 結果が空 or 1着未確定 → レース未確定
    if not race_info.results:
        logger.info("netkeiba: 結果テーブル空 (未確定?) race_id=%s", race_id)
        return False
    ranks = [r.rank for r in race_info.results if r.rank is not None and r.rank > 0]
    if 1 not in ranks:
        logger.info("netkeiba: 1着未確定 race_id=%s (ranks=%s)", race_id, sorted(ranks)[:5])
        return False

    # 着順を DB に保存（race_id は JVLink 形式のまま使用）
    race_info.race_id = race_id
    if not race_info.date:
        race_info.date = race_date
    try:
        _upsert_race_results(conn, race_id, race_info)
        logger.info("netkeiba 着順保存: race_id=%s %d頭", race_id, len(race_info.results))
    except Exception as exc:
        logger.warning("netkeiba 着順DB保存失敗 (race_id=%s): %s", race_id, exc)
        return False

    # 払戻を取得・保存
    try:
        payouts = fetch_race_payouts(race_id, delay=1.0, max_retries=3)
        if payouts:
            insert_race_payouts(conn, race_id, payouts)
            logger.info("netkeiba 払戻保存: race_id=%s %d件", race_id, len(payouts))
        else:
            logger.warning("netkeiba 払戻なし (race_id=%s) — 確定前の可能性", race_id)
    except Exception as exc:
        logger.warning("netkeiba 払戻取得失敗 (race_id=%s): %s", race_id, exc)

    return True


def _send_hit_flash(result: object, race_name: str) -> None:
    """評価完了直後に Discord へ速報を送信する。

    的中あり → 予想チャンネル (DISCORD_WEBHOOK_URL) に 🎉 的中速報
    的中なし → システムチャンネル (DISCORD_SYSTEM_WEBHOOK_URL) に 🏁 完走ログ
               ※ 予想チャンネルをノイズで汚さないよう振り分け
    """
    import requests as _req

    pred_url   = os.environ.get("DISCORD_WEBHOOK_URL", "")
    system_url = os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "") or pred_url

    try:
        hit_items = [h for h in result.hits if h.is_hit]
        race_label = race_name or result.race_id

        if hit_items:
            lines: list[str] = []
            for h in hit_items:
                combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"
                profit_sign = "+" if h.profit >= 0 else ""
                lines.append(
                    f"**{h.bet_type}**  {combo_str}  "
                    f"¥{int(h.payout):,}  "
                    f"(投資¥{int(h.invested):,} / 利益{profit_sign}¥{int(h.profit):,})"
                )
            description = "\n".join(lines)
            payout_total = int(result.total_payout)
            if payout_total >= 100_000:
                color = 0xFF4500   # 赤橙: 万馬券
            elif payout_total >= 10_000:
                color = 0xFFD700   # 金: 高配当
            else:
                color = 0x43B581   # 緑: 通常的中
            title  = f"🎉 的中速報！  {race_label}"
            footer = (
                f"投資合計 ¥{int(result.total_invested):,}  "
                f"払戻合計 ¥{payout_total:,}  "
                f"ROI {result.roi:.1f}%"
            )
            target_url = pred_url
        else:
            # 外れ → システムチャンネルへ静かに流す
            color       = 0x555555
            title       = f"🏁 完走  {race_label}"
            description = "的中なし"
            footer      = f"投資合計 ¥{int(result.total_invested):,}"
            target_url  = system_url

        if not target_url:
            return

        payload = {
            "embeds": [{
                "title":       title,
                "description": description,
                "color":       color,
                "footer":      {"text": footer},
            }]
        }
        resp = _req.post(target_url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            logger.warning("[HitFlash] Discord POST 失敗 status=%d", resp.status_code)
    except Exception as exc:
        logger.warning("[HitFlash] 送信例外: %s", exc)


def fetch_single_race(race_id: str, delay: float = 1.5) -> bool:
    """
    指定レースの結果を JVLink → netkeiba のハイブリッド方式で取得して DB に保存する。

    フォールバック戦略:
      Stage 1: JVLink RACE TODAY 同期 → race_results から着順確認
      Stage 2: JVLink 失敗 or 未確定 → netkeiba からスクレイピング

    Args:
        race_id: 12桁のレースID
        delay:   使用しない（後方互換のため残す）

    Returns:
        True = 結果あり保存成功 / False = まだ結果なし or エラー
    """
    from src.database.init_db import init_db
    from src.evaluation.evaluator import Evaluator

    race_date = f"{race_id[0:4]}{race_id[4:6]}{race_id[6:8]}"  # YYYYMMDD

    conn = init_db()

    # Stage 1: JVLink RACE TODAY 同期
    jvlink_ok = _run_jvlink_race_sync(race_date)
    if not jvlink_ok:
        logger.warning("JVLink 同期失敗 — netkeiba にフォールバック: race_id=%s", race_id)

    # 着順確認（JVLink が成功した場合）
    with_rank = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
        (race_id,),
    ).fetchone()[0]
    rank1 = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank = 1",
        (race_id,),
    ).fetchone()[0]

    if with_rank == 0 or rank1 == 0:
        # Stage 2: netkeiba フォールバック
        logger.info("JVLink 結果なし → netkeiba フォールバック: race_id=%s", race_id)
        nb_ok = _fetch_result_from_netkeiba(race_id, conn)
        if not nb_ok:
            logger.info("netkeiba: 未確定 (race_id=%s)", race_id)
            conn.close()
            return False
        # netkeiba 保存後に再確認
        with_rank = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
            (race_id,),
        ).fetchone()[0]
        if with_rank == 0:
            conn.close()
            return False

    logger.info("race_results 確認: race_id=%s (%d頭 rank有)", race_id, with_rank)

    # 予想評価
    try:
        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, race_id)
        logger.info(
            "評価完了: race_id=%s  的中=%d件  投資¥%.0f  払戻¥%.0f  ROI=%.1f%%",
            race_id, result.hit_count,
            result.total_invested, result.total_payout, result.roi,
        )
        # 的中速報を Discord へ送信
        _send_hit_flash(result, result.race_name)
    except Exception as ee:
        logger.warning("評価失敗 race_id=%s: %s", race_id, ee)

    conn.close()
    return True


def fetch_for_date(date_str: str, force_all: bool = False, delay: float = 1.5) -> int:
    """
    指定日の全(未取得)レースの結果を取得して評価する。

    JVLink 同期を会場グループ別に1回ずつ実行し、結果未取得レースは
    netkeiba フォールバックで補完する。評価完了後に Hit Flash を送信。

    Args:
        date_str: YYYYMMDD 形式
        force_all: True なら既存データも上書き
        delay:     使用しない（後方互換のため残す）

    Returns:
        取得成功レース数
    """
    from src.database.init_db import init_db
    from src.evaluation.evaluator import Evaluator

    date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    conn = init_db()
    race_ids = _get_target_race_ids(conn, date_iso, force_all)

    if not race_ids:
        logger.info("結果取得済みレースなし (date=%s)", date_iso)
        conn.close()
        return 0

    # 会場グループ (race_id[:8]) 別に JVLink 同期を1回だけ実行
    synced_groups: set[str] = set()
    for race_id in race_ids:
        group_key = race_id[:8]  # YYYY+venue+kai+day
        if group_key not in synced_groups:
            jvlink_date = race_id[:8]   # 会場内部日付コード (YYYYMMDD ではなく YYYYVVKK)
            ok = _run_jvlink_race_sync(jvlink_date)
            if not ok:
                logger.warning("JVLink 同期失敗 (group=%s)", group_key)
            synced_groups.add(group_key)

    logger.info("評価対象: %d レース (date=%s)", len(race_ids), date_iso)
    evaluator = Evaluator()
    saved = 0

    for race_id in race_ids:
        with_rank = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
            (race_id,),
        ).fetchone()[0]

        # JVLink で結果なし → netkeiba フォールバック
        if with_rank == 0:
            logger.info("JVLink 結果なし → netkeiba フォールバック: race_id=%s", race_id)
            nb_ok = _fetch_result_from_netkeiba(race_id, conn)
            if not nb_ok:
                logger.info("netkeiba: 未確定 (race_id=%s)", race_id)
                continue
            with_rank = conn.execute(
                "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
                (race_id,),
            ).fetchone()[0]
            if with_rank == 0:
                continue

        try:
            result = evaluator.evaluate_race(conn, race_id)
            saved += 1
            logger.info(
                "評価完了: %s  的中=%d  ROI=%.1f%%",
                race_id, result.hit_count, result.roi,
            )
            _send_hit_flash(result, result.race_name)
        except Exception as e:
            logger.warning("評価失敗 %s: %s", race_id, e)

    conn.close()
    return saved


def _run_generate_data() -> None:
    """web/generate_data.py を実行してダッシュボード JSON を再生成する。"""
    cmd = [sys.executable, str(_ROOT / "web" / "generate_data.py")]
    try:
        result = subprocess.run(cmd, cwd=str(_ROOT), timeout=120)
        if result.returncode == 0:
            logger.info("ダッシュボード更新完了")
        else:
            logger.warning("generate_data.py 失敗 (rc=%d)", result.returncode)
    except Exception as e:
        logger.warning("generate_data.py 実行エラー: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="レース結果速報取得・評価・ダッシュボード更新 (JVLink経由)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--race-id",  help="対象レース ID (例: 202603010501)")
    group.add_argument("--date",     help="対象日 YYYYMMDD (全未取得レース)")
    parser.add_argument("--all",     action="store_true",
                        help="--date 指定時: 既存データも上書き取得")
    parser.add_argument("--delay",   type=float, default=1.5,
                        help="（後方互換: 無効）")
    parser.add_argument("--no-dashboard", action="store_true",
                        help="generate_data.py を実行しない")
    args = parser.parse_args()

    try:
        from src.ops.jvlink_dialog_handler import start_dialog_handler
        start_dialog_handler(interval=0.3)
    except Exception:
        pass

    if args.race_id:
        ok = fetch_single_race(args.race_id, delay=args.delay)
        if ok and not args.no_dashboard:
            _run_generate_data()
        sys.exit(0 if ok else 1)

    else:  # --date
        saved = fetch_for_date(args.date, force_all=args.all, delay=args.delay)
        logger.info("完了: %d レース評価", saved)
        if saved > 0 and not args.no_dashboard:
            _run_generate_data()


if __name__ == "__main__":
    main()
