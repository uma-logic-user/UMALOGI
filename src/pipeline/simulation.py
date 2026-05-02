"""
過去レースシミュレーションパイプライン

責務:
  - simulate_pipeline(): リーク防止済みの過去レース再現（backtest 用）
"""

from __future__ import annotations

import json as _json
import logging

from src.database.init_db import init_db, insert_prediction
from src.ml.features import FeatureBuilder
from src.ml.models import load_models
from src.ml.bet_generator import BetGenerator
from src.ml.reconcile import reconcile as _reconcile
from ._common import build_output_json, save_json

logger = logging.getLogger(__name__)


def simulate_pipeline(race_id: str) -> dict:
    """過去レースの AI 予想を再現するシミュレーションパイプライン。

    prerace_pipeline との差異:
      - ネットワークアクセスなし（全データを DB から取得）
      - リーク防止: rank / finish_time / margin を特徴量から除外
      - 統計計算で対象レース自身を除外 (exclude_race_id)
      - predictions.notes に "[SIMULATE]" を付与して実予想と区別

    Args:
        race_id: シミュレーション対象の過去レース ID

    Returns:
        UI 用 JSON データ（dict）
    """
    logger.info("[SIMULATE] パイプライン開始: race_id=%s", race_id)

    conn = init_db()

    race_row = conn.execute(
        "SELECT race_name, date, venue FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if race_row is None:
        conn.close()
        return {"error": f"race_id が DB に存在しません: {race_id}", "race_id": race_id}

    race_name, race_date, venue = race_row
    logger.info("[SIMULATE] 対象レース: %s %s %s", race_date, venue, race_name)

    try:
        fb = FeatureBuilder(conn)
        df = fb.build_race_features_for_simulate(race_id)
    except ValueError as exc:
        logger.error("[SIMULATE] 特徴量生成失敗: %s", exc)
        conn.close()
        return {"error": str(exc), "race_id": race_id}

    if df.empty:
        conn.close()
        return {"error": "race_results が 0 件です", "race_id": race_id}

    honmei_model, manji_model = load_models()
    honmei_scores    = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)
    ev_scores        = manji_model.ev_score(df)

    gen         = BetGenerator()
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets  = gen.generate_manji(race_id, df, ev_scores)

    sim_note = f"[SIMULATE] {race_date} {venue} {race_name}"
    prediction_ids: dict[str, list[int]] = {"本命": [], "卍": []}

    for race_bets in (honmei_bets, manji_bets):
        for bet in race_bets.bets:
            horses_payload = [
                {
                    "horse_number":   c[0] if len(c) == 1 else None,
                    "horse_name":     bet.horse_names[i] if i < len(bet.horse_names)
                                      else race_bets.model_type,
                    "predicted_rank": i + 1,
                    "model_score":    bet.model_score,
                    "ev_score":       bet.expected_value,
                }
                for i, c in enumerate(bet.combinations[:5])
            ]
            combo_json = _json.dumps([list(c) for c in bet.combinations])
            try:
                pid = insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type=race_bets.model_type,
                    bet_type=bet.bet_type,
                    horses=horses_payload,
                    confidence=bet.confidence,
                    expected_value=bet.expected_value,
                    recommended_bet=bet.recommended_bet,
                    notes=sim_note + (f" / {bet.notes}" if bet.notes else ""),
                    combination_json=combo_json,
                )
                prediction_ids[race_bets.model_type].append(pid)
            except Exception as exc:
                logger.error("[SIMULATE] 予想保存失敗 %s %s: %s",
                             race_bets.model_type, bet.bet_type, exc)

    payout_count = conn.execute(
        "SELECT COUNT(*) FROM race_payouts WHERE race_id = ?", (race_id,)
    ).fetchone()[0]
    reconcile_stats: dict | None = None

    if payout_count == 0:
        logger.warning(
            "[SIMULATE] 払戻データ未取得: %s — data_sync race_results を先に実行してください",
            race_id,
        )
    else:
        try:
            reconcile_stats = _reconcile(conn, race_id=race_id)
            reconciled = reconcile_stats["hit"] + reconcile_stats["miss"]
            hit_rate = (reconcile_stats["hit"] / reconciled * 100) if reconciled > 0 else 0.0
            logger.info(
                "[SIMULATE] 照合完了: 的中=%d 外れ=%d skip=%d no_payout=%d 的中率=%.1f%%",
                reconcile_stats["hit"], reconcile_stats["miss"],
                reconcile_stats["skip"], reconcile_stats["no_payout"],
                hit_rate,
            )
        except Exception as exc:
            logger.error("[SIMULATE] 照合失敗: %s", exc)

    conn.close()

    payload = build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores, honmei_bets, manji_bets
    )
    payload["simulate"]   = True
    payload["race_name"]  = race_name
    payload["race_date"]  = race_date
    if reconcile_stats is not None:
        payload["reconcile"] = reconcile_stats
    save_json(race_id, payload)

    logger.info(
        "[SIMULATE] 完了: race_id=%s 本命%d件 卍%d件",
        race_id, len(prediction_ids["本命"]), len(prediction_ids["卍"]),
    )
    return payload
