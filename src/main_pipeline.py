"""
UMALOGI 完全自動化パイプライン

エントリポイント:
  python -m src.main_pipeline friday              # 金曜夜バッチ
  python -m src.main_pipeline prerace <race_id>   # レース直前予想
  python -m src.main_pipeline train               # モデル再学習
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── プロジェクトルートを sys.path に追加（直接実行用）──────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json as _json
import os

import pandas as pd
import requests

from src.database.init_db import (
    init_db,
    insert_entries,
    insert_realtime_odds,
    insert_prediction,
    insert_race_payouts,
    get_db_path,
)
from src.ml.features import FeatureBuilder
from src.ml.models import load_models, train_all
from src.ml.bet_generator import BetGenerator
from src.ml.reconcile import reconcile as _reconcile
from src.scraper.entry_table import fetch_entry_table, fetch_realtime_odds, fetch_live_race_info
from src.scraper.netkeiba import fetch_race_payouts

# UI 用 JSON 出力先
_JSON_OUT_DIR = _ROOT / "data" / "predictions"


def _check_data_quality(df: pd.DataFrame) -> tuple[bool, str]:
    """
    出馬表の特徴量 DataFrame に対してデータ品質チェックを行う。

    以下の条件を満たさない場合は False と理由を返し、予想を見送る:
      - 馬体重 (horse_weight) の欠損率が 50% 超
      - 単勝オッズ (win_odds) の欠損率が 30% 超

    Args:
        df: FeatureBuilder.build_race_features() の出力 DataFrame

    Returns:
        (True, "OK") または (False, "見送り理由の文字列")
    """
    n = len(df)
    if n == 0:
        return False, "出馬表が 0 頭"

    missing_weight = int(df["horse_weight"].isna().sum()) if "horse_weight" in df.columns else n
    missing_odds   = int(df["win_odds"].isna().sum())     if "win_odds"   in df.columns else n

    weight_rate = missing_weight / n
    odds_rate   = missing_odds   / n

    if weight_rate > 0.5:
        return False, (
            f"馬体重の欠損率が高すぎます ({missing_weight}/{n}頭={weight_rate:.0%})"
            " 当日馬体重の公開前または取得失敗の可能性があります"
        )
    if odds_rate > 0.3:
        return False, (
            f"単勝オッズの欠損率が高すぎます ({missing_odds}/{n}頭={odds_rate:.0%})"
            " オッズ未発売または取得失敗の可能性があります"
        )
    return True, "OK"


_JYO = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


def _send_discord(text: str) -> None:
    """
    Discord Webhook にメッセージを送信する共通ヘルパー。

    DISCORD_WEBHOOK_URL 環境変数が未設定の場合はログ警告のみ。
    送信失敗は WARNING として記録し、例外は握りつぶす（フェイルセーフ優先）。
    """
    url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL が未設定のため Discord 通知をスキップします")
        return
    try:
        resp = requests.post(url, json={"content": text}, timeout=10)
        resp.raise_for_status()
        logger.info("Discord 送信完了: HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Discord 送信失敗: %s", exc)


def _format_race_label(race_id: str) -> str:
    """race_id から "東京 11R" のような表示文字列を生成する。"""
    venue_code = race_id[4:6] if len(race_id) >= 6 else "??"
    venue = _JYO.get(venue_code, venue_code)
    race_num = str(int(race_id[10:12])) + "R" if len(race_id) >= 12 else race_id
    return f"{venue} {race_num}"


def _notify_skip(race_id: str, reason: str) -> None:
    """予想見送りを Discord Webhook で通知する。"""
    label = _format_race_label(race_id)
    text = f"[見送り] {label} (`{race_id}`) データ不足: {reason}"
    logger.warning("[見送り] %s: %s", race_id, reason)
    _send_discord(text)


def _estimate_race_start_jst(race_number: int, race_date: str) -> datetime:
    """
    レース発走時刻を推定して返す（JST）。

    推定式: R1 = 10:00 JST、以降 30 分間隔
      - R1  → 10:00
      - R2  → 10:30
      - R11 → 15:00

    Args:
        race_number: レース番号（1〜12）
        race_date:   "YYYYMMDD" 形式の日付

    Returns:
        推定発走時刻 (datetime, tzinfo なし JST 相当)
    """
    base = datetime.strptime(race_date, "%Y%m%d").replace(hour=10, minute=0)
    return base + timedelta(minutes=(race_number - 1) * 30)


def _check_race_deadline(conn, race_id: str) -> None:
    """
    現在時刻と推定発走時刻を比較し、締め切り 15 分前を過ぎている場合に
    Discord へ遅延警告を送信する。

    締め切り = 推定発走時刻（10:00 + (N-1)×30 分）
    15 分前を過ぎてから prerace_pipeline が呼ばれた場合は遅延とみなす。

    Args:
        conn:    DB 接続（races テーブルから日付・レース番号を取得）
        race_id: 対象レース ID
    """
    try:
        row = conn.execute(
            "SELECT date, race_number FROM races WHERE race_id = ?",
            (race_id,),
        ).fetchone()

        if row is None:
            # races テーブルに未登録の場合は race_id から直接推定
            race_date   = race_id[:8]
            race_number = int(race_id[10:12]) if len(race_id) >= 12 else 1
        else:
            # date は "YYYY/MM/DD" 形式
            race_date_raw = row[0].replace("/", "")
            race_date   = race_date_raw[:8]
            race_number = int(row[1])

        # 発走推定: R1=10:00 + (N-1)×30 分
        estimated_start = _estimate_race_start_jst(race_number, race_date)
        deadline = estimated_start - timedelta(minutes=15)
        now = datetime.now()

        if now >= deadline:
            label = _format_race_label(race_id)
            elapsed = int((now - deadline).total_seconds() / 60)
            text = (
                f"[遅延警告] {label} (`{race_id}`) 予測処理が遅れています\n"
                f"推定発走: {estimated_start.strftime('%H:%M')} JST / "
                f"締切15分前: {deadline.strftime('%H:%M')} JST / "
                f"現在: {now.strftime('%H:%M')} JST (締切から +{elapsed}分)"
            )
            logger.warning("[遅延警告] %s: 締切から +%d 分", race_id, elapsed)
            _send_discord(text)
        else:
            remaining = int((deadline - now).total_seconds() / 60)
            logger.info("締め切りまで残り %d 分 (race_id=%s)", remaining, race_id)

    except Exception as exc:
        logger.warning("締め切りチェック失敗（続行）: %s", exc)


def _kelly_fraction(p_win: float, odds: float, multiplier: float = 0.1) -> float:
    """1/10 ケリー基準による最適賭け比率を返す。

    Args:
        p_win:      勝利確率 (0〜1)
        odds:       単勝オッズ（例: 3.5倍）
        multiplier: ケリー乗数（デフォルト 0.1 = 1/10 Kelly）

    Returns:
        総資金に対する推奨賭け比率 (0〜1)。期待値 < 1.0 の場合は 0.0。
    """
    if odds <= 1.0 or p_win <= 0.0:
        return 0.0
    b = odds - 1.0
    q = 1.0 - p_win
    f_star = (p_win * b - q) / b
    return max(0.0, f_star * multiplier)


# ================================================================
# 金曜夜バッチ: 翌日のレース情報を取得・保存
# ================================================================

def friday_batch(target_date: str | None = None) -> list[str]:
    """
    翌日（または指定日）の全レース出馬表を取得して DB に保存する。

    Args:
        target_date: 対象日 "YYYYMMDD"。None なら翌日。

    Returns:
        保存したレース ID のリスト
    """
    if target_date is None:
        tomorrow = date.today() + timedelta(days=1)
        target_date = tomorrow.strftime("%Y%m%d")

    logger.info("金曜バッチ開始: 対象日=%s", target_date)

    # netkeiba からレース ID リストを取得
    from src.scraper.fetch_historical import fetch_race_ids_for_date
    race_ids = fetch_race_ids_for_date(target_date)

    if not race_ids:
        logger.warning("対象日 %s のレースが見つかりませんでした", target_date)
        return []

    conn = init_db()
    saved: list[str] = []

    for race_id in race_ids:
        try:
            table = fetch_entry_table(race_id, delay=2.0)
            if not table.entries:
                logger.warning("出馬表が空 race_id=%s", race_id)
                continue

            # races テーブルへのダミー挿入（出馬表のみの段階ではレース情報が不完全）
            _ensure_race_record(conn, race_id, target_date)
            insert_entries(conn, race_id, table.entries)
            saved.append(race_id)
            logger.info("出馬表保存: race_id=%s (%d頭)", race_id, len(table.entries))
            time.sleep(2.0)
        except Exception as exc:
            logger.error("出馬表取得失敗 race_id=%s: %s", race_id, exc)
            continue

    conn.close()
    logger.info("金曜バッチ完了: %d / %d レース保存", len(saved), len(race_ids))
    return saved


def _ensure_race_record(conn, race_id: str, date_str: str) -> None:
    """
    races テーブルにレコードがなければ仮登録する。
    出馬表取得後・結果確定後に上書きされる。
    """
    exists = conn.execute(
        "SELECT 1 FROM races WHERE race_id=?", (race_id,)
    ).fetchone()
    if not exists:
        # race_id の形式: YYYYVVDDNN
        # VV=会場コード, DD=開催日数, NN=レース番号
        try:
            race_num = int(race_id[-2:])
        except ValueError:
            race_num = 0
        formatted = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
        with conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO races
                    (race_id, race_name, date, venue, race_number,
                     distance, surface, weather, condition)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (race_id, f"レース{race_num}", formatted, "未定",
                 race_num, 0, "未定", "", ""),
            )


# ================================================================
# レース直前パイプライン
# ================================================================

def prerace_pipeline(race_id: str) -> dict:
    """
    レース直前の自動予想パイプライン。

    処理フロー:
      1. リアルタイムオッズ取得 → DB 保存
      2. 特徴量生成
      3. 本命・卍モデルで予測
      4. 買い目生成
      5. predictions / prediction_horses へ保存
      6. UI 用 JSON 出力

    Args:
        race_id: 対象レース ID

    Returns:
        UI 用 JSON データ（dict）
    """
    logger.info("直前パイプライン開始: race_id=%s", race_id)

    conn = init_db()

    # ── Step 0: 締め切り時刻チェック ──────────────────────────
    # 推定発走 15 分前を過ぎて実行された場合 Discord に遅延警告を送信する。
    _check_race_deadline(conn, race_id)

    # ── Step 1: リアルタイムオッズ取得 ────────────────────────
    # fetch_realtime_odds は tenacity により最大3回リトライ（指数バックオフ）。
    # 3回失敗した場合は realtime_odds テーブルの最終スナップショットが自動使用される
    # (build_race_features → _latest_odds_map がDBから直接読み込むため)。
    try:
        odds_list = fetch_realtime_odds(race_id, delay=1.0)
        if odds_list:
            name_map = _get_entry_name_map(conn, race_id)
            insert_realtime_odds(conn, race_id, odds_list, name_map)
            logger.info("オッズ保存完了: %d 頭", len(odds_list))
    except Exception as exc:
        # tenacity が3回リトライ後も失敗 → DB の最終スナップショットで代替
        cached = conn.execute(
            "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
        ).fetchone()[0]
        if cached > 0:
            logger.warning(
                "オッズ取得失敗（tenacity 3回リトライ上限）: %s — "
                "DB の最終スナップショット %d 件を使用します",
                exc, cached,
            )
        else:
            logger.warning(
                "オッズ取得失敗（tenacity 3回リトライ上限）: %s — "
                "フォールバックなし（オッズ未取得のまま続行）",
                exc,
            )

    # ── Step 1b: ライブデータ取得（馬体重・馬場状態をリアルタイム上書き） ──
    # 金曜バッチで保存済みの entries を最新値で上書きする。
    # - 馬体重は当日発表（発走2時間前頃）に確定するため再取得が必要。
    # - 馬場状態は天候変化で変わるため金曜固定値は使わない。
    # 取得失敗時は既存 DB の値をそのまま使用（フェイルセーフ）。
    try:
        condition, live_entries = fetch_live_race_info(race_id, delay=1.5)

        if live_entries:
            insert_entries(conn, race_id, live_entries)
            weight_known = sum(1 for e in live_entries if e.horse_weight is not None)
            logger.info(
                "馬体重更新: %d 頭中 %d 頭の体重を取得",
                len(live_entries), weight_known,
            )

        if condition:
            conn.execute(
                "UPDATE races SET condition = ? WHERE race_id = ?",
                (condition, race_id),
            )
            conn.commit()
            logger.info("馬場状態を更新: %s", condition)
        else:
            logger.info("馬場状態: 未発表のため既存値を維持")

    except Exception as exc:
        logger.warning("ライブデータ取得失敗（既存DB値で続行）: %s", exc)

    # ── Step 2: 特徴量生成 ─────────────────────────────────────
    try:
        fb = FeatureBuilder(conn)
        df = fb.build_race_features(race_id)
    except ValueError as exc:
        logger.error("特徴量生成失敗: %s", exc)
        conn.close()
        return {"error": str(exc), "race_id": race_id}

    if df.empty:
        logger.error("出馬表が空です race_id=%s", race_id)
        conn.close()
        return {"error": "出馬表が空です", "race_id": race_id}

    # ── Step 2b: データ品質チェック ────────────────────────────
    ok, reason = _check_data_quality(df)
    if not ok:
        conn.close()
        _notify_skip(race_id, reason)
        return {"skipped": True, "reason": reason, "race_id": race_id}

    # ── Step 3: モデル予測 ─────────────────────────────────────
    honmei_model, manji_model = load_models()
    honmei_scores    = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)   # EV = P(win) × 単勝オッズ
    ev_scores        = manji_model.ev_score(df)

    # ── Step 4: 買い目生成 ─────────────────────────────────────
    gen = BetGenerator()
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets  = gen.generate_manji(race_id, df, ev_scores)

    # ── Step 5: DB 保存 ────────────────────────────────────────
    prediction_ids: dict[str, list[int]] = {"本命": [], "卍": []}

    for race_bets in (honmei_bets, manji_bets):
        for bet in race_bets.bets:
            horses_payload = [
                {
                    "horse_number": c[0] if len(c) == 1 else None,
                    "horse_name":   bet.horse_names[i] if i < len(bet.horse_names)
                                    else race_bets.model_type,
                    "predicted_rank": i + 1,
                    "model_score":  bet.model_score,
                    "ev_score":     bet.expected_value,
                }
                for i, c in enumerate(bet.combinations[:5])  # 最大5頭
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
                    notes=bet.notes,
                    combination_json=combo_json,
                )
                prediction_ids[race_bets.model_type].append(pid)
            except Exception as exc:
                logger.error("予想保存失敗 %s %s: %s",
                             race_bets.model_type, bet.bet_type, exc)

    conn.close()

    # ── Step 6: JSON 出力 ──────────────────────────────────────
    payload = _build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores, honmei_bets, manji_bets
    )
    _save_json(race_id, payload)

    logger.info(
        "直前パイプライン完了: race_id=%s 本命%d件 卍%d件",
        race_id,
        len(prediction_ids["本命"]),
        len(prediction_ids["卍"]),
    )
    return payload


def _get_entry_name_map(conn, race_id: str) -> dict[int, str]:
    rows = conn.execute(
        "SELECT horse_number, horse_name FROM entries WHERE race_id=?", (race_id,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _build_output_json(
    race_id: str,
    df,
    honmei_scores,
    honmei_ev_scores,
    ev_scores,
    honmei_bets,
    manji_bets,
) -> dict:
    """UI 用の JSON ペイロードを組み立てる。

    各馬に以下を付与する:
      honmei_score  : P(win) 本命モデル確率
      ev_score      : EV = P(win) × 単勝オッズ （1.0 超で期待値プラス）
      kelly_fraction: 1/10 Kelly による推奨賭け比率
      manji_ev      : 卍モデルの回収率スコア
    """
    import pandas as pd

    def _int_or_none(v) -> int | None:
        return int(v) if (v is not None and pd.notna(v) and v != 0) else None

    def _float_or_none(v) -> float | None:
        return float(v) if (v is not None and pd.notna(v)) else None

    df_reset = df.reset_index(drop=True)
    horses = []
    ev_recommend: list[dict] = []   # EV >= 1.0 の推奨リスト

    for i, row in df_reset.iterrows():
        num       = int(row["horse_number"])
        p_win     = float(honmei_scores.iloc[i]) if i < len(honmei_scores) else 0.0
        ev_val    = float(honmei_ev_scores.iloc[i]) if i < len(honmei_ev_scores) else 0.0
        odds      = float(row.get("win_odds") or 0.0)
        kelly     = _kelly_fraction(p_win, odds) if odds > 0 else 0.0

        entry: dict = {
            "horse_number":   num,
            "horse_name":     str(row.get("horse_name", "")),
            "horse_id":       str(row.get("horse_id", "") or ""),
            "sex_age":        str(row.get("sex_age", "") or ""),
            "weight_carried": float(row.get("weight_carried") or 0),
            "horse_weight":   _int_or_none(row.get("horse_weight")),
            "win_odds":       _float_or_none(odds),
            "popularity":     _int_or_none(row.get("popularity")),
            "honmei_score":   round(p_win, 4),
            "ev_score":       round(ev_val, 4),
            "kelly_fraction": round(kelly, 4),
            "manji_ev":       round(float(ev_scores.iloc[i]) if i < len(ev_scores) else 0, 4),
            # オッズ時系列特徴量
            "odds_vs_morning": _float_or_none(row.get("odds_vs_morning")),
            "odds_velocity":   _float_or_none(row.get("odds_velocity")),
        }
        horses.append(entry)

        if ev_val >= 1.0:
            ev_recommend.append({
                "horse_number":   num,
                "horse_name":     entry["horse_name"],
                "win_odds":       entry["win_odds"],
                "ev_score":       entry["ev_score"],
                "kelly_fraction": entry["kelly_fraction"],
            })

    # EV 降順でソート
    ev_recommend.sort(key=lambda x: x["ev_score"], reverse=True)

    # race-level バイアス（全馬共通値 → 先頭行から取得）
    first = df_reset.iloc[0] if not df_reset.empty else {}
    bias = {
        "today_inner_bias":  _float_or_none(first.get("today_inner_bias")),
        "today_front_bias":  _float_or_none(first.get("today_front_bias")),
        "today_race_count":  _int_or_none(first.get("today_race_count")),
    }

    return {
        "race_id":      race_id,
        "generated_at": datetime.now().isoformat(),
        "bias":         bias,
        "horses":       horses,
        "ev_recommend": ev_recommend,   # EV >= 1.0 の推奨馬
        "honmei_bets":  honmei_bets.to_dict(),
        "manji_bets":   manji_bets.to_dict(),
    }


def _save_json(race_id: str, payload: dict) -> Path:
    _JSON_OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = _JSON_OUT_DIR / f"{race_id}.json"
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON 出力: %s", out)
    return out


# ================================================================
# 過去レースシミュレーション
# ================================================================

def simulate_pipeline(race_id: str) -> dict:
    """
    過去レースの AI 予想を再現するシミュレーションパイプライン。

    prerace_pipeline との違い:
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

    # ── Step 1: レースが DB に存在するか確認 ──────────────────────
    race_row = conn.execute(
        "SELECT race_name, date, venue FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if race_row is None:
        conn.close()
        return {"error": f"race_id が DB に存在しません: {race_id}", "race_id": race_id}

    race_name, race_date, venue = race_row
    logger.info("[SIMULATE] 対象レース: %s %s %s", race_date, venue, race_name)

    # ── Step 2: 特徴量生成（リーク防止済み） ─────────────────────
    # race_results から rank/finish_time/margin を除いた安全な特徴量を構築。
    # _get_horse_stats は exclude_race_id=race_id により対象レース自身を統計から除外。
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

    # ── Step 3: モデル予測 ─────────────────────────────────────────
    honmei_model, manji_model = load_models()
    honmei_scores    = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)
    ev_scores        = manji_model.ev_score(df)

    # ── Step 4: 買い目生成 ─────────────────────────────────────────
    gen = BetGenerator()
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets  = gen.generate_manji(race_id, df, ev_scores)

    # ── Step 5: DB 保存（notes に [SIMULATE] を付与） ──────────────
    sim_note = f"[SIMULATE] {race_date} {venue} {race_name}"
    prediction_ids: dict[str, list[int]] = {"本命": [], "卍": []}

    for race_bets in (honmei_bets, manji_bets):
        for bet in race_bets.bets:
            horses_payload = [
                {
                    "horse_number": c[0] if len(c) == 1 else None,
                    # horse_names[i] に実際の馬名が入っている
                    "horse_name":   bet.horse_names[i] if i < len(bet.horse_names)
                                    else race_bets.model_type,
                    "predicted_rank": i + 1,
                    "model_score":  bet.model_score,
                    "ev_score":     bet.expected_value,
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

    # ── Step 6: 払戻データ取得（未取得の場合のみ） ─────────────────
    payout_count = conn.execute(
        "SELECT COUNT(*) FROM race_payouts WHERE race_id = ?", (race_id,)
    ).fetchone()[0]

    reconcile_stats: dict | None = None

    if payout_count == 0:
        logger.info("[SIMULATE] 払戻データなし → netkeiba から取得: race_id=%s", race_id)
        try:
            payouts = fetch_race_payouts(race_id, delay=1.5)
            if payouts:
                saved_count = insert_race_payouts(conn, race_id, payouts)
                logger.info("[SIMULATE] 払戻保存完了: %d 件", saved_count)
            else:
                logger.warning("[SIMULATE] 払戻データが取得できませんでした: race_id=%s", race_id)
        except Exception as exc:
            logger.error("[SIMULATE] 払戻取得失敗: %s", exc)
    else:
        logger.info("[SIMULATE] 払戻データは取得済み (%d 件): race_id=%s", payout_count, race_id)

    # ── Step 7: 照合（このレースの予想のみ） ──────────────────────
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

    # ── Step 8: JSON 出力 ──────────────────────────────────────────
    payload = _build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores, honmei_bets, manji_bets
    )
    payload["simulate"] = True
    payload["race_name"] = race_name
    payload["race_date"] = race_date
    if reconcile_stats is not None:
        payload["reconcile"] = reconcile_stats
    _save_json(race_id, payload)

    logger.info(
        "[SIMULATE] 完了: race_id=%s 本命%d件 卍%d件",
        race_id,
        len(prediction_ids["本命"]),
        len(prediction_ids["卍"]),
    )
    return payload


# ================================================================
# モデル学習エントリポイント
# ================================================================

def train_pipeline() -> None:
    """DB の全データでモデルを学習・保存する。"""
    # 学習前にDBをバックアップ
    try:
        from utils.backup import make_backup
        make_backup()
        logger.info("学習前バックアップ完了")
    except Exception as exc:
        logger.warning("バックアップ失敗（学習は継続）: %s", exc)

    conn = init_db()
    result = train_all(conn)
    conn.close()
    logger.info("学習結果: %s", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


# ================================================================
# CLI
# ================================================================

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="UMALOGI 自動予想パイプライン",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.main_pipeline friday                  # 翌日の出馬表取得
  python -m src.main_pipeline friday --date 20250628  # 指定日の出馬表取得
  python -m src.main_pipeline prerace 202506050811    # 指定レースの直前予想
  python -m src.main_pipeline simulate 202506050811   # 過去レースのシミュレーション
  python -m src.main_pipeline train                   # モデル再学習
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # friday サブコマンド
    p_fri = sub.add_parser("friday", help="金曜バッチ: 翌日の出馬表取得")
    p_fri.add_argument("--date", metavar="YYYYMMDD", help="対象日（省略時=翌日）")

    # prerace サブコマンド
    p_pre = sub.add_parser("prerace", help="レース直前予想パイプライン")
    p_pre.add_argument("race_id", help="対象レース ID")

    # simulate サブコマンド
    p_sim = sub.add_parser(
        "simulate",
        help="過去レースのシミュレーション（リーク防止済み）",
    )
    p_sim.add_argument("race_id", help="対象過去レース ID")

    # train サブコマンド
    sub.add_parser("train", help="モデル再学習")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args(argv)

    if args.command == "friday":
        saved = friday_batch(target_date=getattr(args, "date", None))
        print(f"保存レース数: {len(saved)}")
        for r in saved:
            print(f"  {r}")

    elif args.command == "prerace":
        result = prerace_pipeline(args.race_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "simulate":
        result = simulate_pipeline(args.race_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "train":
        train_pipeline()


if __name__ == "__main__":
    main()
