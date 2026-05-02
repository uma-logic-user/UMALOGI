"""
WIN5 予測パイプライン

責務:
  - win5_batch()  : 指定日の WIN5 予測バッチ（スケジューラーから独立）
  - try_win5()    : 直前予想パイプライン内から呼ばれるヘルパー
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date

from src.database.init_db import init_db, insert_prediction
from src.ml.models import load_models
from src.notification.discord_notifier import DiscordNotifier

logger = logging.getLogger(__name__)
_discord = DiscordNotifier()


def try_win5(conn: sqlite3.Connection, race_id: str) -> None:
    """同日に5レース以上存在する場合に WIN5 予測を実行して DB に保存する。

    直前予想パイプライン（prerace_pipeline）の内部から呼ばれる。
    既に WIN5 予想が保存済みの場合はスキップ。
    """
    date_str  = race_id[:8]
    formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY race_id", (formatted,)
    ).fetchall()
    race_ids_today = [r[0] for r in rows]

    if len(race_ids_today) < 5:
        logger.debug("WIN5 スキップ: 同日レース %d 件（5件必要）", len(race_ids_today))
        return

    win5_race_ids = race_ids_today[:5]
    existing = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE race_id = ? AND model_type = 'WIN5'",
        (win5_race_ids[0],),
    ).fetchone()[0]
    if existing > 0:
        logger.debug("WIN5 予測済みのためスキップ: %s", win5_race_ids[0])
        return

    try:
        from src.ml.win5 import Win5Engine
        honmei_model, _ = load_models()
        engine       = Win5Engine(model=honmei_model)
        combinations = engine.predict_top_n(conn, win5_race_ids)

        if not combinations:
            logger.info("WIN5: EV >= 1.0 の組み合わせなし（当日レース: %s）", win5_race_ids)
            return

        best = combinations[0]
        horses_payload = [
            {
                "horse_name":     p.horse_name,
                "horse_number":   p.horse_number,
                "predicted_rank": idx + 1,
                "model_score":    p.blend_prob,
                "ev_score":       best.expected_value,
            }
            for idx, p in enumerate(best.picks)
        ]
        combo_json      = json.dumps([[p.horse_number] for p in best.picks])
        horse_names_str = " / ".join(p.horse_name for p in best.picks)

        insert_prediction(
            conn,
            race_id=win5_race_ids[0],
            model_type="WIN5",
            bet_type="WIN5",
            horses=horses_payload,
            confidence=best.combined_prob,
            expected_value=best.expected_value,
            recommended_bet=best.recommended_bet,
            notes=f"WIN5: {horse_names_str}",
            combination_json=combo_json,
        )
        logger.info(
            "WIN5 予測保存: EV=%.3f 推定払戻=¥%,.0f [%s]",
            best.expected_value, best.estimated_payout, horse_names_str,
        )
        _discord.send_text(
            f"[WIN5] 推奨買い目 EV={best.expected_value:.3f} "
            f"推定払戻 ¥{best.estimated_payout:,.0f}\n{horse_names_str}"
        )
    except Exception as exc:
        logger.warning("WIN5 予測失敗（続行）: %s", exc)


def win5_batch(target_date: str | None = None) -> dict:
    """指定日（省略時=当日）の WIN5 予測を実行して DB に保存する。

    金曜バッチで races が揃った直後に確実に実行できるよう独立バッチ化。

    Args:
        target_date: "YYYYMMDD" 形式。None なら当日。

    Returns:
        {"date": ..., "win5_races": [...], "ev": ..., "bet": ..., "skipped": bool}
    """
    if target_date is None:
        target_date = date.today().strftime("%Y%m%d")

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    logger.info("WIN5 バッチ開始: 対象日=%s", formatted)

    conn = init_db()
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY race_id", (formatted,)
    ).fetchall()
    race_ids_today = [r[0] for r in rows]

    if len(race_ids_today) < 5:
        msg = f"WIN5 スキップ: 当日レース {len(race_ids_today)} 件（5件必要）"
        logger.warning(msg)
        conn.close()
        return {"date": target_date, "skipped": True, "reason": msg}

    existing = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE race_id = ? AND model_type = 'WIN5'",
        (race_ids_today[0],),
    ).fetchone()[0]
    if existing > 0:
        logger.info("WIN5 予測済みのためスキップ: %s", race_ids_today[0])
        conn.close()
        return {"date": target_date, "skipped": True, "reason": "already predicted"}

    win5_race_ids = race_ids_today[:5]
    logger.info("WIN5 対象レース: %s", win5_race_ids)

    try:
        from src.ml.win5 import Win5Engine
        honmei_model, _ = load_models()
        engine       = Win5Engine(model=honmei_model)
        combinations = engine.predict_top_n(conn, win5_race_ids)

        if not combinations:
            logger.info("WIN5: EV >= 1.0 の組み合わせなし")
            _discord.send_text("[WIN5] 本日は推奨買い目なし（全組み合わせ EV < 1.0）")
            conn.close()
            return {"date": target_date, "win5_races": win5_race_ids, "skipped": False, "combinations": 0}

        best = combinations[0]
        horses_payload = [
            {
                "horse_name":     p.horse_name,
                "horse_number":   p.horse_number,
                "predicted_rank": idx + 1,
                "model_score":    p.blend_prob,
                "ev_score":       best.expected_value,
            }
            for idx, p in enumerate(best.picks)
        ]
        combo_json      = json.dumps([[p.horse_number] for p in best.picks])
        horse_names_str = " / ".join(p.horse_name for p in best.picks)

        insert_prediction(
            conn,
            race_id=win5_race_ids[0],
            model_type="WIN5",
            bet_type="WIN5",
            horses=horses_payload,
            confidence=best.combined_prob,
            expected_value=best.expected_value,
            recommended_bet=best.recommended_bet,
            notes=f"WIN5: {horse_names_str}",
            combination_json=combo_json,
        )
        logger.info(
            "WIN5 予測保存: EV=%.3f 推定払戻=¥%,.0f [%s]",
            best.expected_value, best.estimated_payout, horse_names_str,
        )
        _discord.send_text(
            f"🎯 **[WIN5] 本日推奨買い目**\n"
            f"EV={best.expected_value:.3f}  推定払戻 ¥{best.estimated_payout:,.0f}\n"
            f"```\n{horse_names_str}\n```\n"
            f"対象レース: {' → '.join(win5_race_ids)}"
        )
        conn.close()
        return {
            "date": target_date,
            "win5_races": win5_race_ids,
            "ev": best.expected_value,
            "bet": best.recommended_bet,
            "horses": horse_names_str,
            "skipped": False,
        }

    except Exception as exc:
        logger.error("WIN5 バッチ失敗: %s", exc, exc_info=True)
        _discord.send_text(f"🚨 [WIN5] 予測失敗: {exc}")
        conn.close()
        return {"date": target_date, "error": str(exc), "skipped": False}
