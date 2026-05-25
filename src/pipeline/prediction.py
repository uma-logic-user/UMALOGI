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
from src.ml.models_v2 import load_models_v2
from src.ml.bet_generator import (
    BetGenerator, BetGeneratorV2, BetConfig, get_current_bankroll,
    RaceBets, BetRecommendation,
)
from src.notification.discord_notifier import DiscordNotifier  # noqa: F401 (後方互換のため保持)
from src.notification.router import NotificationRouter
from src.pipeline.scraping import fetch_and_save_odds
from ._common import build_output_json, save_json

logger = logging.getLogger(__name__)

# モジュールレベルの通知インスタンス（NotificationRouter 経由でチャンネル分離）
_discord = NotificationRouter()


def _check_data_quality(df: pd.DataFrame) -> tuple[bool, str]:
    """出馬表 DataFrame のデータ品質チェック。

    Args:
        df: 出馬表の特徴量 DataFrame（win_odds / horse_weight 列を含む）。

    Returns:
        品質が問題なければ (True, "OK")、見送り基準を超えた場合は (False, 理由文字列)。
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
    """R1=10:00 JST、以降 30 分間隔で発走時刻を推定する。

    Args:
        race_number: レース番号（1〜12）。
        race_date: レース開催日を "YYYYMMDD" 形式で表した文字列。

    Returns:
        推定発走時刻（JST・タイムゾーン情報なし）の datetime オブジェクト。
    """
    base = datetime.strptime(race_date, "%Y%m%d").replace(hour=10, minute=0)
    return base + timedelta(minutes=(race_number - 1) * 30)


def _check_race_deadline(conn: sqlite3.Connection, race_id: str) -> None:
    """締め切り 15 分前を過ぎていれば Discord に遅延警告を送る。

    races テーブルから発走時刻を推定し、締め切り（発走 -15 分）を超過している場合は
    NotificationRouter 経由でシステムチャンネルに警告テキストを送信する。

    Args:
        conn: SQLite 接続オブジェクト。races テーブルへの参照に使用する。
        race_id: 対象レース ID。
    """
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
            _discord.send_system_text(text)
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
    suffix: str,
) -> dict[str, list[int]]:
    """本命・卍 買い目と全馬スコアを DB に保存する。

    insert_prediction を呼び出して predictions テーブルへ INSERT する。
    全馬スコアは "馬分析" bet_type として別途 1 レコード保存される。

    Args:
        conn: SQLite 接続オブジェクト。
        race_id: 対象レース ID。
        df: 出走馬の特徴量 DataFrame。
        honmei_scores: 本命モデルの勝利確率スコア系列。
        honmei_ev_scores: 本命モデルの EV スコア系列。
        ev_scores: 卍モデルの EV スコア系列。
        honmei_bets: 本命モデルの買い目（RaceBets 互換オブジェクト）。
        manji_bets: 卍モデルの買い目（RaceBets 互換オブジェクト）。
        suffix: model_type に付与するサフィックス（例: "(直前)" / "V2(暫定)"）。

    Returns:
        保存した prediction_id の辞書 {"本命": [...], "卍": [...]}。
    """
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


def _run_alpha_payout(
    conn: sqlite3.Connection,
    race_id: str,
    df: pd.DataFrame,
    bankroll: float,
) -> "RaceBets | None":
    """Alpha-Payout 複勝シグナル + 三連複・三連単を生成して DB に保存し RaceBets を返す。

    モデルファイル (data/models/alpha_payout/alpha_payout_model.pkl) が
    存在しない場合は None を返す。買いシグナルがゼロ件の場合も None を返す。

    Args:
        conn: SQLite 接続オブジェクト。predictions テーブルへの INSERT に使用する。
        race_id: 対象レース ID。
        df: 出走馬の特徴量 DataFrame。
        bankroll: 現在の総資金（円）。Kelly 賭け額の計算基準。

    Returns:
        Discord 通知用の RaceBets オブジェクト。モデル未存在またはシグナルなしの場合は None。
    """
    try:
        from src.ml.alpha_payout_model import AlphaPayoutModel, _MODEL_PATH
        if not _MODEL_PATH.exists():
            logger.debug("Alpha-Payout モデルなし → スキップ (race_id=%s)", race_id)
            return

        ap = AlphaPayoutModel.load()

        # FeatureBuilder は encoded integers のみ提供するため
        # AlphaPayoutModel が必要とする raw 文字列カラムを DB から補完する
        df_ap = df.copy()
        df_ap["race_id"] = race_id  # groupby 用

        race_row = conn.execute(
            "SELECT venue, condition, surface FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        if race_row:
            df_ap["venue"]     = race_row[0] or ""
            df_ap["condition"] = race_row[1] or ""
            df_ap["surface"]   = race_row[2] or ""

        # 馬別の生文字列: entries 優先、なければ race_results から取得
        entry_rows = conn.execute(
            "SELECT horse_number, sex_age, jockey, trainer FROM entries WHERE race_id = ?",
            (race_id,),
        ).fetchall()
        if not entry_rows:
            entry_rows = conn.execute(
                "SELECT horse_number, sex_age, jockey, trainer FROM race_results WHERE race_id = ?",
                (race_id,),
            ).fetchall()
        if entry_rows:
            meta = pd.DataFrame(
                entry_rows, columns=["horse_number", "sex_age", "jockey", "trainer"]
            )
            df_ap = df_ap.merge(meta, on="horse_number", how="left")

        pred_ev   = ap.predict_payout_ev(df_ap)
        threshold = ap._ev_threshold

        # ── オッズデータ品質チェック ──────────────────────────────────────
        # win_odds が 50%超 NaN → オッズ依存特徴量が崩壊 → EV値が信頼できない
        odds_col   = "win_odds" if "win_odds" in df_ap.columns else None
        nan_rate   = float(df_ap[odds_col].isna().mean()) if odds_col else 1.0
        has_odds   = nan_rate < 0.5   # オッズデータが十分にある

        # ── ハイブリッド抽出 ──────────────────────────────────────────────
        # 絶対閾値（通常モード）
        abs_pairs: list[tuple[int, float]] = sorted(
            [(i, float(pred_ev.iloc[i])) for i in range(len(pred_ev))
             if float(pred_ev.iloc[i]) >= threshold],
            key=lambda x: x[1], reverse=True,
        )

        # 相対シグナル（オッズあり & 絶対閾値ゼロ件 の場合のみ補完）
        # 条件: EV が race 内上位3頭 かつ median比 1.2倍以上 かつ 絶対値 >= 0.6
        rel_pairs: list[tuple[int, float]] = []
        signal_type = "absolute"
        if has_odds and not abs_pairs:
            race_median = float(pred_ev.median())
            rel_floor   = max(race_median * 1.2, 0.6)
            sorted_idx  = sorted(range(len(pred_ev)),
                                  key=lambda i: float(pred_ev.iloc[i]), reverse=True)
            rel_pairs = [
                (i, float(pred_ev.iloc[i]))
                for i in sorted_idx[:3]
                if float(pred_ev.iloc[i]) >= rel_floor
            ]
            if rel_pairs:
                signal_type = "relative"
                logger.info(
                    "Alpha-Payout: 相対シグナル発動 (nan_rate=%.0f%% median=%.3f floor=%.3f) "
                    "%d頭 max=%.3f (race_id=%s)",
                    nan_rate * 100, race_median, rel_floor,
                    len(rel_pairs), rel_pairs[0][1], race_id,
                )

        buy_pairs = abs_pairs or rel_pairs

        if not buy_pairs:
            logger.info(
                "Alpha-Payout: 買いシグナルなし "
                "(nan_rate=%.0f%% max_ev=%.3f threshold=%.2f race_id=%s)",
                nan_rate * 100, float(pred_ev.max()), threshold, race_id,
            )
            return

        combos = [[int(df_ap.iloc[i]["horse_number"])] for i, _ in buy_pairs]
        horses_payload: list[dict] = []
        for rank, (i, ev_val) in enumerate(buy_pairs):
            row = df_ap.iloc[i]
            horses_payload.append({
                "horse_id":      row.get("horse_id") or None,
                "horse_name":    str(row.get("horse_name", "")),
                "predicted_rank": rank + 1,
                "model_score":   ev_val,
                "ev_score":      ev_val,
            })

        max_ev      = buy_pairs[0][1]
        total_kelly = sum(ap._kelly_bet(ev_val, bankroll) for _, ev_val in buy_pairs)

        # 相対シグナルは kelly を 50% 割引（オッズ品質が低下しているため）
        if signal_type == "relative":
            total_kelly *= 0.5

        notes_str = (
            f"{len(buy_pairs)}頭 pred_ev>{threshold:.2f} max={max_ev:.3f}"
            if signal_type == "absolute"
            else f"{len(buy_pairs)}頭 relative nan={nan_rate:.0%} max={max_ev:.3f}"
        )

        insert_prediction(
            conn,
            race_id=race_id,
            model_type="Alpha-Payout(直前)",
            bet_type="複勝",
            horses=horses_payload,
            confidence=min(max_ev / 3.0, 1.0),
            expected_value=max_ev,
            recommended_bet=float(total_kelly),
            notes=notes_str,
            combination_json=_json.dumps(combos),
        )
        logger.info(
            "Alpha-Payout[%s]: %d頭複勝 ev_max=%.3f kelly=¥%d (race_id=%s)",
            signal_type, len(buy_pairs), max_ev, int(total_kelly), race_id,
        )

        # ── Alpha 三連系（複勝シグナルと独立して三連複・三連単を生成）──────
        alpha_gen   = BetGenerator(conn=conn, config=BetConfig(bankroll=bankroll))
        alpha_bets  = alpha_gen.generate_alpha_trifecta(race_id, df_ap, pred_ev)

        # 三連系を predictions に保存
        for bet in alpha_bets.bets:
            import json as _json_inner
            try:
                insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type="Alpha-Payout(直前)",
                    bet_type=bet.bet_type,
                    horses=horses_payload,
                    confidence=bet.confidence,
                    expected_value=bet.expected_value,
                    recommended_bet=bet.recommended_bet,
                    notes=bet.notes or "",
                    combination_json=_json_inner.dumps([list(c) for c in bet.combinations]),
                )
            except Exception as e_bet:
                logger.warning("Alpha三連系保存失敗 %s: %s", bet.bet_type, e_bet)

        # 複勝シグナルを RaceBets に変換して返す（Discord 通知用）
        ret = RaceBets(race_id=race_id, model_type="Alpha-Payout")
        fukusho_combos = [tuple(c) for c in combos]
        fukusho_names  = [str(df_ap.iloc[i].get("horse_name", "")) for i, _ in buy_pairs]
        ret.bets.append(BetRecommendation(
            bet_type="複勝",
            combinations=fukusho_combos,
            horse_names=fukusho_names,
            expected_value=max_ev,
            model_score=max_ev,
            recommended_bet=float(total_kelly),
            confidence=min(max_ev / 3.0, 1.0),
            notes=notes_str,
        ))
        ret.bets.extend(alpha_bets.bets)
        return ret

    except Exception as exc:
        logger.warning("Alpha-Payout スキップ（例外）: %s", exc)
        return None


def prerace_pipeline(
    race_id: str,
    provisional: bool = False,
    model_version: str = "v1",
) -> dict:
    """レース直前（または前日暫定）の自動予想パイプライン。

    Args:
        race_id:       対象レース ID
        provisional:   True = 暫定モード（オッズ・馬体重欠損を許容）
        model_version: "v1" = 既存モデル（固定EV閾値）
                       "v2" = V2モデル（W-004+動的EV閾値+Kelly）

    Returns:
        UI 用 JSON データ（dict）
    """
    from src.pipeline.scraping import save_entries_to_db
    from src.pipeline.win5 import try_win5

    mode_label = "暫定" if provisional else "直前"
    logger.info("%sパイプライン開始: race_id=%s", mode_label, race_id)

    conn = init_db()
    try:
        return _prerace_pipeline_inner(
            conn, race_id, provisional, model_version, mode_label
        )
    finally:
        conn.close()


def _prerace_pipeline_inner(
    conn: "sqlite3.Connection",
    race_id: str,
    provisional: bool,
    model_version: str,
    mode_label: str,
) -> dict:
    """prerace_pipeline の内部実装。conn は呼び出し元で finally close される。

    締め切りチェック → エントリ取得 → オッズ取得 → 特徴量生成 →
    モデル予測 → 買い目生成 → DB 保存 → JSON 出力 → Discord 通知
    の順で処理を実行する。

    Args:
        conn: SQLite 接続オブジェクト（呼び出し元が finally で close する）。
        race_id: 対象レース ID。
        provisional: True の場合は暫定モード（オッズ欠損を許容）。
        model_version: "v1" または "v2"。
        mode_label: ログ表示用のモードラベル（"直前" または "暫定"）。

    Returns:
        UI 用 JSON ペイロード（dict）。スキップ時は {"skipped": True, ...}、
        エラー時は {"error": ..., ...} を返す。
    """
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

    # Step 1c: オッズ取得（RTD→netkeiba 3段階フォールバック）
    # 暫定モードでも試みる（netkeiba に既に公開されていれば Alpha-Payout EV が改善する）
    # fetch_and_save_odds は取得失敗時も 0 を返して安全に続行するため、
    # 暫定バッチの早朝実行でオッズ未公開でも問題なし。
    if cached_odds == 0:
        fetch_and_save_odds(conn, race_id)

    # Step 2: 特徴量生成
    try:
        fb = FeatureBuilder(conn)
        df = fb.build_race_features(race_id)
    except ValueError as exc:
        logger.error("特徴量生成失敗: %s", exc)
        return {"error": str(exc), "race_id": race_id}

    if df.empty:
        _discord.notify_scraping_alert(
            race_id, "出馬表が 0 頭（features DataFrame が空）"
        )
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
                _discord.notify_skip(race_id, reason)
                return {"skipped": True, "reason": reason, "race_id": race_id}
            logger.warning(
                "⚠️ データ品質チェック警告: %s → 暫定モードで強制続行 (race_id=%s)",
                reason,
                race_id,
            )
            _discord.send_system_text(
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

    # Step 2c: センチネルオッズ馬の自動除外（data_validator）
    # win_odds >= 500 の馬は JRA-VAN 未確定値であり、軸計算・EV計算から除外する
    try:
        from src.ml.data_validator import validate_race_df, filter_sentinel_horses
        _vr = validate_race_df(df, race_id=race_id)
        if _vr["n_sentinel"] > 0:
            if _vr["is_valid"]:
                df = filter_sentinel_horses(df, race_id=race_id)
                logger.info(
                    "センチネル除外 (race_id=%s): %d頭除外 → %d頭で推論",
                    race_id, _vr["n_sentinel"], len(df),
                )
            else:
                logger.warning(
                    "センチネル除外スキップ (race_id=%s): 除外後の有効頭数 %d < 最低 3頭 → 全馬で続行",
                    race_id, _vr["n_valid_odds"],
                )
    except Exception as _ve:
        logger.warning("data_validator 呼び出し失敗（続行）: %s", _ve)

    # Step 3: モデル予測（V1/V2 分岐）
    is_v2 = (model_version == "v2")
    if is_v2:
        honmei_model, _place_model, manji_model = load_models_v2()
        logger.info("V2 モデル使用: honmei_model_v2 / manji_model_v2")
    else:
        honmei_model, _place_model, manji_model = load_models()
    honmei_scores = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)
    ev_scores = manji_model.ev_score(df)

    # Step 4: 買い目生成（V2 は BetGeneratorV2 を使用）
    current_bankroll = get_current_bankroll(conn)
    if is_v2:
        gen: BetGenerator = BetGeneratorV2(
            conn=conn, config=BetConfig(bankroll=current_bankroll, provisional=provisional)
        )
    else:
        gen = BetGenerator(
            conn=conn, config=BetConfig(bankroll=current_bankroll, provisional=provisional)
        )
    # ── AIウマスギフィルター統合済み ─────────────────────────────────────────
    # generate_honmei() / generate_alpha_trifecta() は内部で _apply_roi_filter() を呼び出す。
    # 本命: 単勝/複勝 + 三連単(EV≥1.5のみ) / 卍: 三連単除外 / Alpha: 三連単除外
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets = gen.generate_manji(race_id, df, ev_scores)
    logger.info(
        "[AIウマスギ] ROIフィルター適用完了: %s  本命=%d件 卍=%d件",
        race_id, len(honmei_bets.bets), len(manji_bets.bets),
    )

    # Step 4b: Alpha-Payout 複勝+三連系シグナル（直前のみ）
    alpha_bets = None
    if not provisional:
        alpha_bets = _run_alpha_payout(conn, race_id, df, current_bankroll)

    # Step 5: DB 保存（V2 は suffix に "V2" を付与して V1 と識別）
    if is_v2:
        suffix = "V2(暫定)" if provisional else "V2(直前)"
    else:
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
        suffix,
    )

    # Step 5c: WIN5（直前のみ）
    if not provisional:
        from src.pipeline.win5 import try_win5
        try_win5(conn, race_id)

    # Step 6: JSON 出力（V2 は {race_id}_v2.json に分離保存）
    payload = build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores,
        honmei_bets, manji_bets, provisional=provisional,
    )
    payload["provisional"] = provisional
    payload["model_version"] = model_version
    if is_v2:
        from src.pipeline._common import JSON_OUT_DIR
        import json as _json_mod
        v2_out = JSON_OUT_DIR / f"{race_id}_v2.json"
        JSON_OUT_DIR.mkdir(parents=True, exist_ok=True)
        v2_out.write_text(_json_mod.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("V2 JSON 出力: %s", v2_out)
    else:
        save_json(race_id, payload)

    # Step 7: Discord 通知（直前のみ）— ALPHA / 卍 / 本命 独立3セクション送信
    if not provisional:
        _discord.notify_prerace_result(
            race_id, honmei_bets, manji_bets,
            alpha_bets=alpha_bets,
        )

    alpha_bet_count = len(alpha_bets.bets) if alpha_bets is not None else 0
    logger.info(
        "%sパイプライン完了: race_id=%s 本命%d件 卍%d件 Alpha%d件",
        mode_label,
        race_id,
        len(prediction_ids["本命"]),
        len(prediction_ids["卍"]),
        alpha_bet_count,
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
