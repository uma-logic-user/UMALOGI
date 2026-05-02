"""
直前予想・暫定予想パイプライン

責務:
  - prerace_pipeline()    : レース直前の本番予想
  - provisional_batch()   : 翌日全レースの暫定予想バッチ
  - _check_data_quality() : 出馬表品質チェック
  - _check_race_deadline(): 締め切り時刻チェック
"""

from __future__ import annotations

import json as _json
import logging
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd

from src.database.init_db import init_db, insert_prediction
from src.ml.features import FeatureBuilder
from src.ml.models import load_models
from src.ml.bet_generator import BetGenerator
from src.notification.discord_notifier import DiscordNotifier
from src.pipeline.scraping import fetch_and_save_odds
from ._common import build_output_json, save_json

logger = logging.getLogger(__name__)

# モジュールレベルの通知インスタンス（DISCORD_WEBHOOK_URL から自動初期化）
_discord = DiscordNotifier()


def _check_data_quality(df: pd.DataFrame) -> tuple[bool, str]:
    """出馬表 DataFrame のデータ品質チェック。

    Returns:
        (True, "OK") or (False, "見送り理由")
    """
    n = len(df)
    if n == 0:
        return False, "出馬表が 0 頭"

    missing_odds = int(df["win_odds"].isna().sum()) if "win_odds" in df.columns else n
    odds_rate = missing_odds / n

    missing_weight = (
        int(df["horse_weight"].isna().sum()) if "horse_weight" in df.columns else 0
    )
    if missing_weight > 0:
        logger.warning(
            "⚠️ 馬体重欠損 %d/%d頭 (%.0f%%) — NaN のまま推論します",
            missing_weight,
            n,
            missing_weight / n * 100,
        )

    if odds_rate > 0.8:
        return False, (
            f"単勝オッズの欠損率が高すぎます ({missing_odds}/{n}頭={odds_rate:.0%})"
            " オッズ未発売または取得完全失敗の可能性があります"
        )
    return True, "OK"


def _estimate_race_start_jst(race_number: int, race_date: str) -> datetime:
    """R1=10:00 JST、以降 30 分間隔で発走時刻を推定する。"""
    base = datetime.strptime(race_date, "%Y%m%d").replace(hour=10, minute=0)
    return base + timedelta(minutes=(race_number - 1) * 30)


def _check_race_deadline(conn: sqlite3.Connection, race_id: str) -> None:
    """締め切り 15 分前を過ぎていれば Discord に遅延警告を送る。"""
    try:
        row = conn.execute(
            "SELECT date, race_number FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()

        if row is None:
            race_date = race_id[:8]
            race_number = int(race_id[10:12]) if len(race_id) >= 12 else 1
        else:
            race_date = row[0].replace("-", "")[:8]
            race_number = int(row[1])

        estimated_start = _estimate_race_start_jst(race_number, race_date)
        deadline = estimated_start - timedelta(minutes=15)
        now = datetime.now()

        if now >= deadline:
            from ._common import format_race_label

            label = format_race_label(race_id)
            elapsed = int((now - deadline).total_seconds() / 60)
            text = (
                f"[遅延警告] {label} (`{race_id}`) 予測処理が遅れています\n"
                f"推定発走: {estimated_start.strftime('%H:%M')} JST / "
                f"締切15分前: {deadline.strftime('%H:%M')} JST / "
                f"現在: {now.strftime('%H:%M')} JST (締切から +{elapsed}分)"
            )
            logger.warning("[遅延警告] %s: 締切から +%d 分", race_id, elapsed)
            _discord.send_text(text)
        else:
            remaining = int((deadline - now).total_seconds() / 60)
            logger.info("締め切りまで残り %d 分 (race_id=%s)", remaining, race_id)
    except Exception as exc:
        logger.warning("締め切りチェック失敗（続行）: %s", exc)


def _save_predictions(
    conn: sqlite3.Connection,
    race_id: str,
    df: pd.DataFrame,
    honmei_scores: pd.Series,
    honmei_ev_scores: pd.Series,
    ev_scores: pd.Series,
    honmei_bets: object,
    manji_bets: object,
    oracle_bets: object,
    suffix: str,
) -> dict[str, list[int]]:
    """本命・卍・Oracle 買い目と全馬スコアを DB に保存する。"""
    prediction_ids: dict[str, list[int]] = {"本命": [], "卍": []}

    for race_bets in (honmei_bets, manji_bets):
        mt_tagged = f"{race_bets.model_type}{suffix}"  # type: ignore[attr-defined]
        for bet in race_bets.bets:  # type: ignore[attr-defined]
            horses_payload: list[dict] = []
            for i, c in enumerate(bet.combinations[:5]):
                if len(c) == 1:
                    horses_payload.append(
                        {
                            "horse_number": c[0],
                            "horse_name": bet.horse_names[i]
                            if i < len(bet.horse_names)
                            else race_bets.model_type,  # type: ignore[attr-defined]
                            "predicted_rank": i + 1,
                            "model_score": bet.model_score,
                            "ev_score": bet.expected_value,
                        }
                    )
                else:
                    for j, horse_num in enumerate(c):
                        horses_payload.append(
                            {
                                "horse_number": horse_num,
                                "horse_name": bet.horse_names[j]
                                if j < len(bet.horse_names)
                                else str(horse_num),
                                "predicted_rank": j + 1,
                                "model_score": bet.model_score,
                                "ev_score": bet.expected_value,
                            }
                        )
            combo_json = _json.dumps([list(c) for c in bet.combinations])
            try:
                pid = insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type=mt_tagged,
                    bet_type=bet.bet_type,
                    horses=horses_payload,
                    confidence=bet.confidence,
                    expected_value=bet.expected_value,
                    recommended_bet=bet.recommended_bet,
                    notes=bet.notes,
                    combination_json=combo_json,
                )
                prediction_ids[race_bets.model_type].append(pid)  # type: ignore[attr-defined]
            except Exception as exc:
                logger.error("予想保存失敗 %s %s: %s", mt_tagged, bet.bet_type, exc)

    # Oracle 買い目
    oracle_suffix = f"Oracle{suffix}"
    for bet in oracle_bets.bets:  # type: ignore[attr-defined]
        horses_payload_o: list[dict] = []
        for j, horse_num in enumerate(bet.combinations[0] if bet.combinations else []):
            horses_payload_o.append(
                {
                    "horse_number": horse_num,
                    "horse_name": bet.horse_names[j]
                    if j < len(bet.horse_names)
                    else str(horse_num),
                    "predicted_rank": j + 1,
                    "model_score": bet.model_score,
                    "ev_score": bet.expected_value,
                }
            )
        combo_json_o = _json.dumps([list(c) for c in bet.combinations])
        try:
            insert_prediction(
                conn,
                race_id=race_id,
                model_type=oracle_suffix,
                bet_type=bet.bet_type,
                horses=horses_payload_o,
                confidence=bet.confidence,
                expected_value=bet.expected_value,
                recommended_bet=bet.recommended_bet,
                notes=bet.notes,
                combination_json=combo_json_o,
            )
        except Exception as exc:
            logger.warning("Oracle予想保存失敗 %s: %s", bet.bet_type, exc)

    # 全馬スコア（馬分析タブ用）
    df_sorted = df.reset_index(drop=True)
    rank_order = honmei_scores.argsort()[::-1].reset_index(drop=True)
    all_horse_payload: list[dict] = []
    for rank_pos, orig_idx in enumerate(rank_order):
        row = df_sorted.iloc[int(orig_idx)]
        all_horse_payload.append(
            {
                "horse_id": row.get("horse_id") or None,
                "horse_name": str(row.get("horse_name", "")),
                "predicted_rank": rank_pos + 1,
                "model_score": float(honmei_scores.iloc[int(orig_idx)]),
                "ev_score": float(honmei_ev_scores.iloc[int(orig_idx)]),
            }
        )
    try:
        insert_prediction(
            conn,
            race_id=race_id,
            model_type=f"本命{suffix}",
            bet_type="馬分析",
            horses=all_horse_payload,
            confidence=None,
            expected_value=None,
            recommended_bet=None,
            notes="全馬モデルスコア（馬分析タブ用）",
            combination_json="[]",
        )
    except Exception as exc:
        logger.warning("全馬スコア保存失敗（続行）: %s", exc)

    return prediction_ids


def prerace_pipeline(race_id: str, provisional: bool = False) -> dict:
    """レース直前（または前日暫定）の自動予想パイプライン。

    Args:
        race_id:     対象レース ID
        provisional: True = 暫定モード（オッズ・馬体重欠損を許容）

    Returns:
        UI 用 JSON データ（dict）
    """
    from src.pipeline.scraping import save_entries_to_db
    from src.pipeline.win5 import try_win5

    mode_label = "暫定" if provisional else "直前"
    logger.info("%sパイプライン開始: race_id=%s", mode_label, race_id)

    conn = init_db()

    # Step 0: 締め切りチェック（直前のみ）
    if not provisional:
        _check_race_deadline(conn, race_id)

    # Step 1: キャッシュ確認
    cached_entries = conn.execute(
        "SELECT COUNT(*) FROM entries WHERE race_id = ?", (race_id,)
    ).fetchone()[0]
    cached_odds = conn.execute(
        "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
    ).fetchone()[0]
    logger.info(
        "DB キャッシュ: オッズ=%d件 エントリ=%d頭 (race_id=%s)",
        cached_odds,
        cached_entries,
        race_id,
    )

    # Step 1b: entries が空なら netkeiba フォールバック
    if cached_entries == 0:
        logger.warning("⚠️ entries が空 → netkeiba から出馬表を自動取得: %s", race_id)
        try:
            from src.scraper.entry_table import fetch_entry_table

            tbl = fetch_entry_table(race_id, delay=1.5)
            if tbl.entries:
                cached_entries = save_entries_to_db(conn, tbl)
                logger.info("netkeiba フォールバック成功: %d 頭保存", cached_entries)
            else:
                logger.error("🚨 netkeiba からも出馬表が 0 頭: %s", race_id)
                _discord.notify_scraping_alert(
                    race_id,
                    "JRA-VAN entries も netkeiba 出馬表も 0 頭 — HTML 構造変更を確認してください",
                )
        except Exception as exc:
            logger.error("netkeiba 出馬表フォールバック失敗 (%s): %s", race_id, exc)

    # Step 1c: オッズ取得（直前のみ）
    if cached_odds == 0 and not provisional:
        fetch_and_save_odds(conn, race_id)

    # Step 2: 特徴量生成
    try:
        fb = FeatureBuilder(conn)
        df = fb.build_race_features(race_id)
    except ValueError as exc:
        logger.error("特徴量生成失敗: %s", exc)
        conn.close()
        return {"error": str(exc), "race_id": race_id}

    if df.empty:
        _discord.notify_scraping_alert(
            race_id, "出馬表が 0 頭（features DataFrame が空）"
        )
        conn.close()
        return {"error": "出馬表が空です", "race_id": race_id}

    # Step 2b: データ品質チェック（直前のみ）
    if not provisional:
        if "win_odds" in df.columns and df["win_odds"].isna().all():
            new_odds = conn.execute(
                "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
            ).fetchone()[0]
            if new_odds > 0:
                logger.info("オッズ保存済み(%d件) → DataFrame を再ビルド", new_odds)
                try:
                    df2 = FeatureBuilder(conn).build_race_features(race_id)
                    if not df2.empty and not df2["win_odds"].isna().all():
                        df = df2
                except Exception as e:
                    logger.warning("DataFrame 再ビルド失敗（続行）: %s", e)

            if "win_odds" in df.columns and df["win_odds"].isna().all():
                logger.warning(
                    "⚠️ 全馬の単勝オッズが NaN — オッズ未取得のまま推論: %s", race_id
                )

        ok, reason = _check_data_quality(df)
        if not ok:
            # 出馬表が 0 頭（真の失敗）のみ中断する。オッズ欠損は暫定モードで続行。
            if "0 頭" in reason or df.empty:
                conn.close()
                _discord.notify_skip(race_id, reason)
                return {"skipped": True, "reason": reason, "race_id": race_id}
            logger.warning(
                "⚠️ データ品質チェック警告: %s → 暫定モードで強制続行 (race_id=%s)",
                reason,
                race_id,
            )
            _discord.send_text(
                f"⚠️ [オッズ欠損フォールバック] `{race_id}` — {reason}\n"
                f"暫定モードで予測を強制続行します。"
            )
            provisional = True
    else:
        n = len(df)
        logger.info(
            "暫定モード: 馬体重欠損=%d/%d 単勝オッズ欠損=%d/%d — NaN のまま推論",
            int(df["horse_weight"].isna().sum()) if "horse_weight" in df.columns else n,
            n,
            int(df["win_odds"].isna().sum()) if "win_odds" in df.columns else n,
            n,
        )

    # Step 3: モデル予測
    honmei_model, _place_model, manji_model = load_models()
    honmei_scores = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)
    ev_scores = manji_model.ev_score(df)

    # Step 4: 買い目生成
    gen = BetGenerator()
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets = gen.generate_manji(race_id, df, ev_scores)
    oracle_bets = gen.generate_oracle(race_id, df, honmei_scores)

    # Step 5: DB 保存
    suffix = "(暫定)" if provisional else "(直前)"
    prediction_ids = _save_predictions(
        conn,
        race_id,
        df,
        honmei_scores,
        honmei_ev_scores,
        ev_scores,
        honmei_bets,
        manji_bets,
        oracle_bets,
        suffix,
    )

    # Step 5c: WIN5（直前のみ）
    if not provisional:
        try_win5(conn, race_id)

    conn.close()

    # Step 6: JSON 出力
    payload = build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores, honmei_bets, manji_bets
    )
    payload["provisional"] = provisional
    save_json(race_id, payload)

    # Step 7: Discord 通知（直前のみ）
    if not provisional:
        _discord.notify_prerace_result(race_id, honmei_bets, manji_bets)

    logger.info(
        "%sパイプライン完了: race_id=%s 本命%d件 卍%d件",
        mode_label,
        race_id,
        len(prediction_ids["本命"]),
        len(prediction_ids["卍"]),
    )
    return payload


def provisional_batch(target_date: str | None = None) -> list[str]:
    """指定日（省略時=翌日）の全レースを暫定予想する。

    Args:
        target_date: "YYYYMMDD" 形式。None なら翌日。

    Returns:
        暫定予想を完了したレース ID のリスト
    """
    if target_date is None:
        target_date = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    logger.info("暫定予想バッチ開始: 対象日=%s", formatted)

    conn = init_db()
    race_ids: list[str] = [
        r[0]
        for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (formatted,),
        ).fetchall()
    ]

    if race_ids:
        placeholders = ",".join("?" * len(race_ids))
        deleted = conn.execute(
            f"DELETE FROM predictions WHERE model_type LIKE '%暫定%'"
            f" AND race_id IN ({placeholders})",
            race_ids,
        )
        conn.commit()
        if deleted.rowcount:
            logger.info("既存の暫定予想を削除: %d 件", deleted.rowcount)
    conn.close()

    if not race_ids:
        logger.warning(
            "対象日 %s のレースが races テーブルに見つかりません", target_date
        )
        return []

    succeeded: list[str] = []
    for rid in race_ids:
        try:
            result = prerace_pipeline(rid, provisional=True)
            if result.get("skipped") or result.get("error"):
                logger.warning(
                    "暫定予想スキップ %s: %s",
                    rid,
                    result.get("reason") or result.get("error"),
                )
            else:
                succeeded.append(rid)
        except Exception as exc:
            logger.error("暫定予想失敗 %s: %s", rid, exc)

    logger.info("暫定予想バッチ完了: %d / %d レース", len(succeeded), len(race_ids))
    return succeeded
