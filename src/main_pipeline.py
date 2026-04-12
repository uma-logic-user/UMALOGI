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
from dotenv import load_dotenv

# プロジェクトルートの .env を読み込む（既に環境変数がセットされている場合は上書きしない）
load_dotenv(_ROOT / ".env", override=False)

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


def _send_scraping_health_alert(race_id: str, detail: str) -> None:
    """スクレイピング異常を Discord Webhook で緊急通知する。

    0頭取得・全オッズ NaN などスクレイパーの構造的破綻が疑われる場合に使用。
    """
    label = _format_race_label(race_id)
    text = (
        f"🚨【緊急】スクレイピング仕様変更の可能性\n"
        f"対象: {label} (`{race_id}`)\n"
        f"詳細: {detail}\n"
        f"→ netkeiba / JRA-VAN の HTML 構造変更を確認してください"
    )
    logger.error("[スクレイピング異常] %s: %s", race_id, detail)
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
            # date は "YYYY-MM-DD" 形式（ISO 8601）
            race_date_raw = row[0].replace("-", "")
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
# Discord 全券種まとめ通知
# ================================================================

def _notify_prerace_result(
    race_id: str,
    honmei_bets: object,
    manji_bets: object,
) -> None:
    """
    直前予想の全券種買い目を 1 つの Discord Embed にまとめて送信する。

    同一レースの券種ごとにメッセージが分散するのを防ぐため、
    本命モデル・卍モデルの全買い目を EV 降順に並べて 1 embed に集約する。

    EV >= 1.0 の買い目には 🔥、それ以外は空白で区別する。
    推奨買い目がない場合（全 EV <= 0）は見送り扱いとして通知しない。
    """
    label = _format_race_label(race_id)

    def _combo_str(bet: object) -> str:
        """組み合わせを読みやすい文字列にする。"""
        bt = bet.bet_type  # type: ignore[attr-defined]
        combos = bet.combinations  # type: ignore[attr-defined]
        names  = bet.horse_names   # type: ignore[attr-defined]
        if not combos:
            return "—"
        first = combos[0]
        if bt in ("馬単", "三連単"):
            # 着順あり: → で繋ぐ
            nums = " → ".join(str(n) for n in first)
        else:
            nums = " - ".join(str(n) for n in sorted(first))
        # 馬名（先頭3頭まで）
        name_str = " / ".join(names[:3]) if names else ""
        suffix = f"（+{len(combos)-1}組）" if len(combos) > 1 else ""
        return f"{nums}{suffix}  ({name_str})"

    def _build_section(race_bets: object) -> str:
        """1モデル分の買い目行リストを文字列で返す。"""
        bets = sorted(
            race_bets.bets,  # type: ignore[attr-defined]
            key=lambda b: b.expected_value,
            reverse=True,
        )
        if not bets:
            return "  (推奨なし)\n"
        lines = []
        for b in bets:
            ev    = b.expected_value
            flag  = "🔥" if ev >= 1.0 else "  "
            bet_y = f"¥{int(b.recommended_bet):,}" if b.recommended_bet else "—"
            combo = _combo_str(b)
            lines.append(f"{flag} **{b.bet_type}** EV={ev:.2f}  {bet_y}\n    └ {combo}")
        return "\n".join(lines)

    honmei_section = _build_section(honmei_bets)
    manji_section  = _build_section(manji_bets)

    # 全 EV <= 0 なら通知スキップ
    all_bets = list(honmei_bets.bets) + list(manji_bets.bets)  # type: ignore[attr-defined]
    if not any(b.expected_value > 0 for b in all_bets):
        logger.info("全 EV <= 0 のため Discord 通知をスキップ: %s", race_id)
        return

    # 推奨合計購入金額
    total_bet = sum(
        b.recommended_bet for b in all_bets
        if b.expected_value >= 1.0 and b.recommended_bet
    )
    total_str = f"¥{int(total_bet):,}" if total_bet > 0 else "なし"

    # EV の最大値でカラーを決定
    max_ev = max((b.expected_value for b in all_bets), default=0.0)
    color  = 0xFF4500 if max_ev >= 3.0 else (0xFFD700 if max_ev >= 1.5 else 0x00FF88)

    description = (
        f"**本命モデル**\n{honmei_section}\n\n"
        f"**卍モデル**\n{manji_section}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"EV >= 1.0 推奨合計: {total_str}"
    )

    # Discord Embed (2048 文字超は切り捨て)
    if len(description) > 4000:
        description = description[:3997] + "..."

    url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL 未設定のため通知スキップ: %s", race_id)
        return

    payload = {
        "embeds": [{
            "title": f"🏇 {label}  直前予想  (`{race_id}`)",
            "description": description,
            "color": color,
        }]
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Discord 直前予想通知 送信完了: %s HTTP %d", race_id, resp.status_code)
    except Exception as exc:
        logger.warning("Discord 直前予想通知 送信失敗: %s", exc)


# ================================================================
# 暫定予想バッチ: 指定日（省略時=翌日）の全レースを暫定予想
# ================================================================

def provisional_batch(target_date: str | None = None) -> list[str]:
    """
    指定日（省略時=翌日）の全レースに対して暫定予想を生成する。

    金曜バッチ（friday_batch）の直後に実行し、週末分の暫定予想を
    ダッシュボードに先行表示するために使う。

    暫定予想の特徴:
      - オッズ・馬体重が未発表でも欠損 NaN のまま LightGBM で推論
      - model_type は "本命(暫定)" / "卍(暫定)" で保存
      - 直前予想（本命(直前)）が生成されると UI 上で並べて比較可能

    Args:
        target_date: "YYYYMMDD" 形式の日付。None なら翌日。

    Returns:
        暫定予想を完了したレース ID のリスト
    """
    if target_date is None:
        tomorrow    = date.today() + timedelta(days=1)
        target_date = tomorrow.strftime("%Y%m%d")

    formatted = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    logger.info("暫定予想バッチ開始: 対象日=%s (%s)", target_date, formatted)

    conn = init_db()
    race_ids: list[str] = [
        r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (formatted,),
        ).fetchall()
    ]

    # 既存の暫定予想を削除（再実行時の重複防止）
    if race_ids:
        placeholders = ",".join("?" * len(race_ids))
        deleted = conn.execute(
            f"DELETE FROM predictions WHERE model_type LIKE '%暫定%'"
            f" AND race_id IN ({placeholders})",
            race_ids,
        )
        conn.commit()
        if deleted.rowcount:
            logger.info("既存の暫定予想を削除: %d 件（再生成前クリーン）", deleted.rowcount)
    conn.close()

    if not race_ids:
        logger.warning("対象日 %s のレースが races テーブルに見つかりません", target_date)
        return []

    succeeded: list[str] = []
    for race_id in race_ids:
        try:
            result = prerace_pipeline(race_id, provisional=True)
            if result.get("skipped") or result.get("error"):
                logger.warning("暫定予想スキップ %s: %s",
                               race_id, result.get("reason") or result.get("error"))
            else:
                succeeded.append(race_id)
        except Exception as exc:
            logger.error("暫定予想失敗 %s: %s", race_id, exc)

    logger.info("暫定予想バッチ完了: %d / %d レース", len(succeeded), len(race_ids))
    return succeeded


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
                _send_scraping_health_alert(
                    race_id,
                    "出馬表が 0 頭（fetch_entry_table が空リストを返しました）",
                )
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
        formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
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
# WIN5 予測ヘルパー
# ================================================================

def _try_win5(conn, race_id: str) -> None:
    """同日に5レース以上存在する場合に WIN5 予測を実行して DB に保存する。

    Win5Engine を honmei モデルで初期化し、当日レース先頭5件で予測。
    既に WIN5 予想が保存済みの場合はスキップ（重複防止）。
    """
    date_str = race_id[:8]
    formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
        (formatted,),
    ).fetchall()
    race_ids_today = [r[0] for r in rows]

    if len(race_ids_today) < 5:
        logger.debug("WIN5 スキップ: 同日レース %d 件（5件必要）", len(race_ids_today))
        return

    win5_race_ids = race_ids_today[:5]

    # 重複防止: 既に同じ先頭レースで WIN5 予想が存在するか確認
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
        engine = Win5Engine(model=honmei_model)
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
        combo_json = json.dumps([[p.horse_number] for p in best.picks])
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

        _send_discord(
            f"[WIN5] 推奨買い目 EV={best.expected_value:.3f} "
            f"推定払戻 ¥{best.estimated_payout:,.0f}\n"
            f"{horse_names_str}"
        )
    except Exception as exc:
        logger.warning("WIN5 予測失敗（続行）: %s", exc)


# ================================================================
# レース直前パイプライン
# ================================================================

def prerace_pipeline(race_id: str, provisional: bool = False) -> dict:
    """
    レース直前（または前日暫定）の自動予想パイプライン。

    処理フロー:
      0. 締め切り時刻チェック（直前モードのみ）
      1. リアルタイムオッズ取得 → DB 保存（直前モードのみ）
      1b. ライブデータ更新（馬体重・馬場状態）（直前モードのみ）
      2. 特徴量生成
      2b. データ品質チェック（直前モードのみ）
      3. 本命・卍モデルで予測
      4. 買い目生成
      5. predictions / prediction_horses へ保存
         - model_type は "本命(暫定)" / "本命(直前)" の形式で保存
      6. UI 用 JSON 出力

    Args:
        race_id:     対象レース ID
        provisional: True = 暫定予想モード
                       - オッズ・馬体重の欠損を許容（LightGBM の NaN 処理に委ねる）
                       - リアルタイムデータ取得をスキップ
                       - model_type に "(暫定)" サフィックスを付与
                     False = 直前予想モード（従来の動作）
                       - model_type に "(直前)" サフィックスを付与

    Returns:
        UI 用 JSON データ（dict）
    """
    mode_label = "暫定" if provisional else "直前"
    logger.info("%sパイプライン開始: race_id=%s", mode_label, race_id)

    conn = init_db()

    # ── Step 0: 締め切り時刻チェック（直前モードのみ） ────────
    if not provisional:
        _check_race_deadline(conn, race_id)

    # ── Step 1: リアルタイムオッズ取得（直前モードのみ） ──────
    # 暫定モードではオッズ未発売のためスキップ。NaN のまま LightGBM に推論させる。
    if not provisional:
        try:
            odds_list = fetch_realtime_odds(race_id, delay=1.0)
            if odds_list:
                name_map = _get_entry_name_map(conn, race_id)
                insert_realtime_odds(conn, race_id, odds_list, name_map)
                logger.info("オッズ保存完了: %d 頭", len(odds_list))
        except Exception as exc:
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

    # ── Step 1b: ライブデータ取得（直前モードのみ） ──────────
    # 暫定モードでは馬体重・馬場状態は当日未発表のためスキップ。
    if not provisional:
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
        _send_scraping_health_alert(race_id, "出馬表が 0 頭（features DataFrame が空）")
        conn.close()
        return {"error": "出馬表が空です", "race_id": race_id}

    # ── Step 2b: データ品質チェック（直前モードのみ） ─────────
    # 暫定モードでは馬体重/オッズ欠損を許容し、NaN のまま LightGBM に推論させる。
    if not provisional:
        # 全馬のオッズが NaN → スクレイピング構造的破綻の可能性
        if "win_odds" in df.columns and df["win_odds"].isna().all():
            _send_scraping_health_alert(
                race_id,
                f"全馬の単勝オッズが NaN ({len(df)} 頭) — オッズ取得先の HTML 構造変更を確認してください",
            )
        ok, reason = _check_data_quality(df)
        if not ok:
            conn.close()
            _notify_skip(race_id, reason)
            return {"skipped": True, "reason": reason, "race_id": race_id}
    else:
        n = len(df)
        missing_w = int(df["horse_weight"].isna().sum()) if "horse_weight" in df.columns else n
        missing_o = int(df["win_odds"].isna().sum()) if "win_odds" in df.columns else n
        logger.info(
            "暫定モード: 馬体重欠損=%d/%d 単勝オッズ欠損=%d/%d — NaN のまま推論",
            missing_w, n, missing_o, n,
        )

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
    # model_type に "(暫定)" / "(直前)" サフィックスを付与して区別する
    suffix     = "(暫定)" if provisional else "(直前)"
    prediction_ids: dict[str, list[int]] = {"本命": [], "卍": []}

    for race_bets in (honmei_bets, manji_bets):
        mt_tagged = f"{race_bets.model_type}{suffix}"   # 例: "本命(暫定)"
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
                    model_type=mt_tagged,       # "(暫定)" / "(直前)" 付きで保存
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
                             mt_tagged, bet.bet_type, exc)

    # ── Step 5b: 全馬スコア保存（馬分析表示用）────────────────────
    # Streamlit の「馬分析」で全出走馬のモデルスコアを表示するため、
    # 全頭分のスコアを bet_type='馬分析' として保存する。
    # prediction_horses は買い目馬しか格納しないため、このレコードが
    # 全馬表示の唯一のソースとなる。
    df_sorted = df.reset_index(drop=True)
    # honmei_scores の降順でランク付け
    rank_order = honmei_scores.argsort()[::-1].reset_index(drop=True)
    all_horse_payload: list[dict] = []
    for rank_pos, orig_idx in enumerate(rank_order):
        row = df_sorted.iloc[int(orig_idx)]
        all_horse_payload.append({
            "horse_id":      str(row.get("horse_id") or ""),
            "horse_name":    str(row.get("horse_name", "")),
            "predicted_rank": rank_pos + 1,
            "model_score":   float(honmei_scores.iloc[int(orig_idx)]),
            "ev_score":      float(honmei_ev_scores.iloc[int(orig_idx)]),
        })
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

    # ── Step 5c: WIN5 予測（直前モードのみ）──────────────────────
    # 同日5レース以上存在する場合に先頭5レースで WIN5 予想を生成・保存する。
    if not provisional:
        _try_win5(conn, race_id)

    conn.close()

    # ── Step 6: JSON 出力 ──────────────────────────────────────
    payload = _build_output_json(
        race_id, df, honmei_scores, honmei_ev_scores, ev_scores, honmei_bets, manji_bets
    )
    payload["provisional"] = provisional
    _save_json(race_id, payload)

    # ── Step 7: Discord 全券種まとめ通知（直前モードのみ）────────
    # 暫定モードは見送り。直前モードの本気予想のみ通知する。
    if not provisional:
        _notify_prerace_result(race_id, honmei_bets, manji_bets)

    logger.info(
        "%sパイプライン完了: race_id=%s 本命%d件 卍%d件",
        mode_label, race_id,
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
  python -m src.main_pipeline friday                           # 翌日の出馬表取得
  python -m src.main_pipeline friday --date 20250628           # 指定日の出馬表取得
  python -m src.main_pipeline provisional                      # 翌日の全レース暫定予想
  python -m src.main_pipeline provisional --date 20260411      # 指定日の全レース暫定予想
  python -m src.main_pipeline prerace 202506050811             # 指定レースの直前予想（本番）
  python -m src.main_pipeline prerace 202506050811 --provisional  # 指定レースの暫定予想
  python -m src.main_pipeline simulate 202506050811            # 過去レースのシミュレーション
  python -m src.main_pipeline train                            # モデル再学習
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # friday サブコマンド
    p_fri = sub.add_parser("friday", help="金曜バッチ: 翌日の出馬表取得")
    p_fri.add_argument("--date", metavar="YYYYMMDD", help="対象日（省略時=翌日）")

    # provisional サブコマンド（日次暫定予想バッチ）
    p_prov = sub.add_parser("provisional", help="暫定予想バッチ: 指定日の全レースを暫定予想")
    p_prov.add_argument("--date", metavar="YYYYMMDD", help="対象日（省略時=翌日）")

    # prerace サブコマンド
    p_pre = sub.add_parser("prerace", help="レース直前予想パイプライン")
    p_pre.add_argument("race_id", help="対象レース ID")
    p_pre.add_argument(
        "--provisional", action="store_true",
        help="暫定予想モード: 馬体重・オッズ欠損を許容し model_type に '(暫定)' を付与",
    )

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

    elif args.command == "provisional":
        race_ids = provisional_batch(target_date=getattr(args, "date", None))
        print(f"暫定予想完了: {len(race_ids)} レース")
        for r in race_ids:
            print(f"  {r}")

    elif args.command == "prerace":
        prov = getattr(args, "provisional", False)
        result = prerace_pipeline(args.race_id, provisional=prov)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "simulate":
        result = simulate_pipeline(args.race_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "train":
        train_pipeline()


if __name__ == "__main__":
    main()
