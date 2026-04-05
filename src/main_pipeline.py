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

from src.database.init_db import (
    init_db,
    insert_entries,
    insert_realtime_odds,
    insert_prediction,
    get_db_path,
)
from src.ml.features import FeatureBuilder
from src.ml.models import load_models, train_all
from src.ml.bet_generator import BetGenerator
from src.scraper.entry_table import fetch_entry_table, fetch_realtime_odds

# UI 用 JSON 出力先
_JSON_OUT_DIR = _ROOT / "data" / "predictions"


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

    # ── Step 1: リアルタイムオッズ取得 ────────────────────────
    try:
        odds_list = fetch_realtime_odds(race_id, delay=1.0)
        if odds_list:
            name_map = _get_entry_name_map(conn, race_id)
            insert_realtime_odds(conn, race_id, odds_list, name_map)
            logger.info("オッズ保存完了: %d 頭", len(odds_list))
    except Exception as exc:
        logger.warning("オッズ取得失敗: %s", exc)

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

    # ── Step 3: モデル予測 ─────────────────────────────────────
    honmei_model, manji_model = load_models()
    honmei_scores = honmei_model.predict(df)
    ev_scores     = manji_model.ev_score(df)

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
                    "horse_name":   race_bets.model_type,
                    "predicted_rank": i + 1,
                    "model_score":  bet.model_score,
                    "ev_score":     bet.expected_value,
                }
                for i, c in enumerate(bet.combinations[:5])  # 最大5頭
            ]
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
                )
                prediction_ids[race_bets.model_type].append(pid)
            except Exception as exc:
                logger.error("予想保存失敗 %s %s: %s",
                             race_bets.model_type, bet.bet_type, exc)

    conn.close()

    # ── Step 6: JSON 出力 ──────────────────────────────────────
    payload = _build_output_json(
        race_id, df, honmei_scores, ev_scores, honmei_bets, manji_bets
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
    ev_scores,
    honmei_bets,
    manji_bets,
) -> dict:
    """UI 用の JSON ペイロードを組み立てる。"""
    import pandas as pd

    def _int_or_none(v) -> int | None:
        return int(v) if (v is not None and pd.notna(v) and v != 0) else None

    def _float_or_none(v) -> float | None:
        return float(v) if (v is not None and pd.notna(v)) else None

    horses = []
    for i, row in df.iterrows():
        num = int(row["horse_number"])
        horses.append({
            "horse_number":   num,
            "horse_name":     str(row.get("horse_name", "")),
            "horse_id":       str(row.get("horse_id", "") or ""),
            "sex_age":        str(row.get("sex_age", "") or ""),
            "weight_carried": float(row.get("weight_carried") or 0),
            "horse_weight":   _int_or_none(row.get("horse_weight")),
            "win_odds":       _float_or_none(row.get("win_odds")),
            "popularity":     _int_or_none(row.get("popularity")),
            "honmei_score":   round(float(honmei_scores.iloc[i]) if i < len(honmei_scores) else 0, 4),
            "ev_score":       round(float(ev_scores.iloc[i]) if i < len(ev_scores) else 0, 4),
        })

    return {
        "race_id":     race_id,
        "generated_at": datetime.now().isoformat(),
        "horses":      horses,
        "honmei_bets": honmei_bets.to_dict(),
        "manji_bets":  manji_bets.to_dict(),
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
    honmei_scores = honmei_model.predict(df)
    ev_scores     = manji_model.ev_score(df)

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
                    "horse_name":   race_bets.model_type,
                    "predicted_rank": i + 1,
                    "model_score":  bet.model_score,
                    "ev_score":     bet.expected_value,
                }
                for i, c in enumerate(bet.combinations[:5])
            ]
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
                )
                prediction_ids[race_bets.model_type].append(pid)
            except Exception as exc:
                logger.error("[SIMULATE] 予想保存失敗 %s %s: %s",
                             race_bets.model_type, bet.bet_type, exc)

    conn.close()

    # ── Step 6: JSON 出力 ──────────────────────────────────────────
    payload = _build_output_json(
        race_id, df, honmei_scores, ev_scores, honmei_bets, manji_bets
    )
    payload["simulate"] = True
    payload["race_name"] = race_name
    payload["race_date"] = race_date
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
