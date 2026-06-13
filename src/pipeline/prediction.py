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
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    # 型注釈専用（from __future__ import annotations で実行時評価されない）。
    from src.ml.bet_generator import RaceBets
    from src.ml.models import HonmeiModel, ManjiModel, PlaceModel
    from src.ml.pure_ev_edge import PureEVRaceBets

from src.database.init_db import init_db, insert_prediction
from src.ml.features import FeatureBuilder
from src.ml.models import load_models
from src.ml.models_v2 import load_models_v2
from src.ml.bet_generator import (
    BetGenerator,
    BetGeneratorV2,
    BetConfig,
    get_current_bankroll,
    RaceBets,
    BetRecommendation,
    should_skip_race_for_betting,
)
from src.notification.discord_notifier import DiscordNotifier  # noqa: F401 (後方互換のため保持)
from src.notification.router import NotificationRouter
from src.pipeline.scraping import fetch_and_save_odds, save_entries_to_db

# SNS マーケティング専用シャドー（5月末ポリシー全券種）配線。monkeypatch 可能なよう
# モジュールレベルで束縛する（best-effort ヘルパー _maybe_save_shadow から参照）。
from src.analysis.shadow_recompute import generate_shadow_bets, save_shadow_bets
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


# 注: predictions.recommended_bet は「実発注額（Kelly 等）」を保存する。
# P&L 会計・A/B 評価のコストは evaluator / pnl_accounting が bet_policy.flat_cost
# (¥100×点数) で独立計算するため、recommended_bet を会計に用いない（二重性の厳密分離）。


def _supersede_prior_predictions(
    conn: sqlite3.Connection, race_id: str, suffix: str
) -> int:
    """同レース・同バリアント（直前 / V2直前）の既存予想を論理無効化する（P1-4）。

    直前の再推論（recheck 等）で買い目を再生成する際、INSERT OR REPLACE は
    同一 (race_id, model_type, bet_type) のみ置換するため、新買い目に含まれない
    旧買い目レコードが残留し評価・ROI が二重計上される。これを防ぐため
    保存前に当該バリアントの旧予想へ is_superseded=1 を立てる。

    条項1（予測データ不変性）の例外: 内容の改変・削除ではなく「論理的な無効化」
    フラグのみを更新する（オーナー承認・P1-4）。評価は is_superseded=0 のみ採用する。

    Args:
        conn: DB コネクション。
        race_id: 対象レース ID。
        suffix: 保存時の model_type サフィックス（"(直前)" または "V2(直前)"）。

    Returns:
        無効化した行数。
    """
    pattern = f"%{suffix}"
    with conn:
        if "V2" in suffix:
            cur = conn.execute(
                "UPDATE predictions SET is_superseded = 1 "
                "WHERE race_id = ? AND COALESCE(is_superseded, 0) = 0 "
                "AND model_type LIKE ?",
                (race_id, pattern),
            )
        else:
            # 非V2の直前のみ（"...V2(直前)" を巻き込まない）
            cur = conn.execute(
                "UPDATE predictions SET is_superseded = 1 "
                "WHERE race_id = ? AND COALESCE(is_superseded, 0) = 0 "
                "AND model_type LIKE ? AND model_type NOT LIKE ?",
                (race_id, pattern, f"%V2{suffix}"),
            )
    n = cur.rowcount or 0
    if n:
        logger.info(
            "旧予想を論理無効化(P1-4): race_id=%s suffix=%s %d件", race_id, suffix, n
        )
    return n


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
    oracle_bets: object | None = None,
    hit_focus_bets: object | None = None,
    honmei_shap: dict[int, str | None] | None = None,
    manji_shap: dict[int, str | None] | None = None,
) -> dict[str, list[int]]:
    """本命・卍・Oracle・HitFocus 買い目と全馬スコアを DB に保存する。

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
    _honmei_shap = honmei_shap or {}
    _manji_shap = manji_shap or {}

    for race_bets, shap_map in (
        (honmei_bets, _honmei_shap),
        (manji_bets, _manji_shap),
    ):
        # W-050: suffix が "V2..." のとき model_type の末尾 "V2" を除去して二重付与を防ぐ
        # 例: model_type="卍V2" + suffix="V2(直前)" → "卍V2(直前)"
        mt_base: str = race_bets.model_type  # type: ignore[attr-defined]
        if suffix.startswith("V2") and mt_base.endswith("V2"):
            mt_base = mt_base[:-2]
        mt_tagged = f"{mt_base}{suffix}"
        for bet in race_bets.bets:  # type: ignore[attr-defined]
            horses_payload: list[dict] = []
            for i, c in enumerate(bet.combinations[:5]):
                if len(c) == 1:
                    hn = int(c[0])
                    horses_payload.append(
                        {
                            "horse_number": hn,
                            "horse_name": bet.horse_names[i]
                            if i < len(bet.horse_names)
                            else race_bets.model_type,  # type: ignore[attr-defined]
                            "predicted_rank": i + 1,
                            "model_score": bet.model_score,
                            "ev_score": bet.expected_value,
                            "shap_json": shap_map.get(hn),
                        }
                    )
                else:
                    for j, horse_num in enumerate(c):
                        hn = int(horse_num)
                        horses_payload.append(
                            {
                                "horse_number": hn,
                                "horse_name": bet.horse_names[j]
                                if j < len(bet.horse_names)
                                else str(horse_num),
                                "predicted_rank": j + 1,
                                "model_score": bet.model_score,
                                "ev_score": bet.expected_value,
                                "shap_json": shap_map.get(hn),
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
                    recommended_bet=bet.recommended_bet,  # 実発注額（会計は evaluator flat_cost で独立）
                    notes=bet.notes,
                    combination_json=combo_json,
                )
                prediction_ids[race_bets.model_type].append(pid)  # type: ignore[attr-defined]
            except Exception as exc:
                logger.error("予想保存失敗 %s %s: %s", mt_tagged, bet.bet_type, exc)

    # Oracle 買い目（VirtualOracleStrategy — 的中確率最大の三連複・三連単。Kelly 対象外の記録用）
    if oracle_bets is not None:
        oracle_suffix = f"Oracle{suffix}"
        for bet in oracle_bets.bets:  # type: ignore[attr-defined]
            horses_payload_o: list[dict] = []
            for j, horse_num in enumerate(
                bet.combinations[0] if bet.combinations else []
            ):
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
                    recommended_bet=bet.recommended_bet,  # 実発注額（会計は evaluator flat_cost で独立）
                    notes=bet.notes,
                    combination_json=combo_json_o,
                )
            except Exception as exc:
                logger.warning("Oracle予想保存失敗 %s: %s", bet.bet_type, exc)

    # 全馬スコア（馬分析タブ用）— honmei SHAP を付与
    df_sorted = df.reset_index(drop=True)
    rank_order = honmei_scores.argsort()[::-1].reset_index(drop=True)
    all_horse_payload: list[dict] = []
    for rank_pos, orig_idx in enumerate(rank_order):
        row = df_sorted.iloc[int(orig_idx)]
        hn = int(row.get("horse_number", 0)) if hasattr(row, "get") else 0
        all_horse_payload.append(
            {
                "horse_id": row.get("horse_id") or None,
                "horse_name": str(row.get("horse_name", "")),
                "predicted_rank": rank_pos + 1,
                "model_score": float(honmei_scores.iloc[int(orig_idx)]),
                "ev_score": float(honmei_ev_scores.iloc[int(orig_idx)]),
                "shap_json": _honmei_shap.get(hn),
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

    # HitFocus 買い目（HitFocusStrategy — 2軸マルチフォーメーション。均等100円・Kelly 不使用）
    if hit_focus_bets is not None:
        hit_focus_suffix = f"HitFocus{suffix}"
        for bet in hit_focus_bets.bets:  # type: ignore[attr-defined]
            hf_payload: list[dict] = []
            for j, horse_num in enumerate(
                bet.combinations[0] if bet.combinations else []
            ):
                hf_payload.append(
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
            combo_json_hf = _json.dumps([list(c) for c in bet.combinations])
            try:
                insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type=hit_focus_suffix,
                    bet_type=bet.bet_type,
                    horses=hf_payload,
                    confidence=bet.confidence,
                    expected_value=bet.expected_value,
                    recommended_bet=bet.recommended_bet,  # 実発注額（会計は evaluator flat_cost で独立）
                    notes=bet.notes,
                    combination_json=combo_json_hf,
                )
            except Exception as exc:
                logger.warning("HitFocus予想保存失敗 %s: %s", bet.bet_type, exc)

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
            return None

        ap = AlphaPayoutModel.load()

        # FeatureBuilder は encoded integers のみ提供するため
        # AlphaPayoutModel が必要とする raw 文字列カラムを DB から補完する
        df_ap = df.copy()
        df_ap["race_id"] = race_id  # groupby 用

        race_row = conn.execute(
            "SELECT venue, condition, surface FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        if race_row:
            df_ap["venue"] = race_row[0] or ""
            df_ap["condition"] = race_row[1] or ""
            df_ap["surface"] = race_row[2] or ""

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

        pred_ev = ap.predict_payout_ev(df_ap)
        threshold = ap._ev_threshold

        # ── オッズデータ品質チェック ──────────────────────────────────────
        # win_odds が 50%超 NaN → オッズ依存特徴量が崩壊 → EV値が信頼できない
        odds_col = "win_odds" if "win_odds" in df_ap.columns else None
        nan_rate = float(df_ap[odds_col].isna().mean()) if odds_col else 1.0
        has_odds = nan_rate < 0.5  # オッズデータが十分にある

        # ── ハイブリッド抽出 ──────────────────────────────────────────────
        # 絶対閾値（通常モード）
        abs_pairs: list[tuple[int, float]] = sorted(
            [
                (i, float(pred_ev.iloc[i]))
                for i in range(len(pred_ev))
                if float(pred_ev.iloc[i]) >= threshold
            ],
            key=lambda x: x[1],
            reverse=True,
        )

        # 相対シグナル（オッズあり & 絶対閾値ゼロ件 の場合のみ補完）
        # 条件: EV が race 内上位3頭 かつ median比 1.2倍以上 かつ 絶対値 >= 0.6
        rel_pairs: list[tuple[int, float]] = []
        signal_type = "absolute"
        if has_odds and not abs_pairs:
            race_median = float(pred_ev.median())
            rel_floor = max(race_median * 1.2, 0.6)
            sorted_idx = sorted(
                range(len(pred_ev)), key=lambda i: float(pred_ev.iloc[i]), reverse=True
            )
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
                    nan_rate * 100,
                    race_median,
                    rel_floor,
                    len(rel_pairs),
                    rel_pairs[0][1],
                    race_id,
                )

        buy_pairs = abs_pairs or rel_pairs

        if not buy_pairs:
            logger.info(
                "Alpha-Payout: 買いシグナルなし "
                "(nan_rate=%.0f%% max_ev=%.3f threshold=%.2f race_id=%s)",
                nan_rate * 100,
                float(pred_ev.max()),
                threshold,
                race_id,
            )
            return None

        combos = [[int(df_ap.iloc[i]["horse_number"])] for i, _ in buy_pairs]
        horses_payload: list[dict] = []
        for rank, (i, ev_val) in enumerate(buy_pairs):
            row = df_ap.iloc[i]
            horses_payload.append(
                {
                    "horse_id": row.get("horse_id") or None,
                    "horse_name": str(row.get("horse_name", "")),
                    "predicted_rank": rank + 1,
                    "model_score": ev_val,
                    "ev_score": ev_val,
                }
            )

        max_ev = buy_pairs[0][1]
        total_kelly: float = sum(
            ap._kelly_bet(ev_val, bankroll) for _, ev_val in buy_pairs
        )

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
            signal_type,
            len(buy_pairs),
            max_ev,
            int(total_kelly),
            race_id,
        )

        # ── Alpha 三連系（複勝シグナルと独立して三連複・三連単を生成）──────
        alpha_gen = BetGenerator(conn=conn, config=BetConfig(bankroll=bankroll))
        alpha_bets = alpha_gen.generate_alpha_trifecta(race_id, df_ap, pred_ev)

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
                    recommended_bet=bet.recommended_bet,  # 実発注額（会計は evaluator flat_cost で独立）
                    notes=bet.notes or "",
                    combination_json=_json_inner.dumps(
                        [list(c) for c in bet.combinations]
                    ),
                )
            except Exception as e_bet:
                logger.warning("Alpha三連系保存失敗 %s: %s", bet.bet_type, e_bet)

        # 複勝シグナルを RaceBets に変換して返す（Discord 通知用）
        ret = RaceBets(race_id=race_id, model_type="Alpha-Payout")
        fukusho_combos = [tuple(c) for c in combos]
        fukusho_names = [str(df_ap.iloc[i].get("horse_name", "")) for i, _ in buy_pairs]
        ret.bets.append(
            BetRecommendation(
                bet_type="複勝",
                combinations=fukusho_combos,
                horse_names=fukusho_names,
                expected_value=max_ev,
                model_score=max_ev,
                recommended_bet=float(total_kelly),
                confidence=min(max_ev / 3.0, 1.0),
                notes=notes_str,
            )
        )
        ret.bets.extend(alpha_bets.bets)
        return ret

    except Exception as exc:
        logger.warning("Alpha-Payout スキップ（例外）: %s", exc)
        return None


def _run_pure_ev_edge(
    conn: sqlite3.Connection,
    race_id: str,
    df: pd.DataFrame,
    manji_ev_scores: pd.Series,
    place_scores: pd.Series,
    suffix: str,
    rdate: str | None = None,
) -> "PureEVRaceBets | None":
    """黒字化専用バリアント Pure_EV_Edge（単複のみ）の買い目を生成・保存する。

    卍 Isotonic 較正確率ベースの EV>=1.15・1/10 Kelly・サーキットブレーカー付き。
    既存ロジック（本命/卍/Oracle/HitFocus）とは完全に分離した独立枠。

    Args:
        conn: DB コネクション。
        race_id: 対象レース ID。
        df: 出走馬の特徴量 DataFrame。
        manji_ev_scores: ManjiModel.ev_score(df)。
        place_scores: PlaceModel.predict(df)（複勝較正確率）。
        suffix: model_type サフィックス（"(直前)" / "(暫定)"）。
        rdate: レース開催日（YYYY-MM-DD）。サーキットブレーカー判定に使用。

    Returns:
        PureEVRaceBets（買い目あり）または None（見送り/CB発動）。
    """
    try:
        from src.ml.pure_ev_edge import (
            PURE_EV_MODEL_NAME,
            PureEVConfig,
            circuit_breaker_status,
            select_pure_ev_bets,
        )

        cfg = PureEVConfig(initial_bankroll=get_current_bankroll(conn))

        # サーキットブレーカー: 当日/当週の確定損失が上限超のとき。
        # W-087 Soft Stop（既定）= シグナル生成と DB 保存は継続し、警告ログのみ
        #   （データ蓄積・監視を止めない／実弾発注は人間判断）。
        # Hard Stop（CIRCUIT_BREAKER_SOFT_STOP=0）= 旧来どおり新規生成を停止（return None）。
        soft_stop = os.getenv("CIRCUIT_BREAKER_SOFT_STOP", "1").strip() != "0"
        if rdate:
            try:
                cb = circuit_breaker_status(conn, rdate, cfg)
                if cb.tripped:
                    if soft_stop:
                        logger.warning(
                            "[Pure_EV_Edge] CB発動中だがシグナル生成を継続 race_id=%s: %s（Soft Stop）",
                            race_id,
                            cb.reason,
                        )
                    else:
                        logger.warning(
                            "[Pure_EV_Edge] サーキットブレーカー発動 race_id=%s: %s（Hard Stop）",
                            race_id,
                            cb.reason,
                        )
                        return None
            except Exception as _cbe:  # noqa: BLE001
                logger.debug("[Pure_EV_Edge] CB判定スキップ: %s", _cbe)

        # 馬ごとの入力 dict を構築（win_odds は df、place_odds は realtime から任意）
        horses: list[dict] = []
        for i, (_, hrow) in enumerate(df.iterrows()):
            if i >= len(manji_ev_scores):
                break
            horses.append(
                {
                    "horse_number": hrow.get("horse_number"),
                    "horse_name": hrow.get("horse_name", ""),
                    "win_odds": hrow.get("win_odds"),
                    "manji_ev_score": float(manji_ev_scores.iloc[i]),
                    "place_prob": float(place_scores.iloc[i])
                    if i < len(place_scores)
                    else None,
                }
            )

        bets = select_pure_ev_bets(race_id, horses, cfg)
        if not bets.bets:
            logger.info(
                "[Pure_EV_Edge] 買い目なし race_id=%s（EV<%.2f）",
                race_id,
                cfg.ev_threshold,
            )
            return None

        # DB 保存（単勝・複勝を別レコードで・combination_json は単頭リスト）
        for bt in ("単勝", "複勝"):
            sub = [b for b in bets.bets if b.bet_type == bt]
            if not sub:
                continue
            horses_payload = [
                {
                    "horse_number": b.horse_number,
                    "horse_name": b.horse_name,
                    "predicted_rank": rank + 1,
                    "model_score": b.prob,
                    "ev_score": b.expected_value,
                }
                for rank, b in enumerate(sub)
            ]
            combo_json = _json.dumps([[b.horse_number] for b in sub])
            # recommended_bet=実発注額（1/10 Kelly 実額の合計）。会計コストは別途
            # bet_policy.flat_cost(¥100×点数) で評価され混同されない（評価用点数=len(sub)）。
            kelly_sum = int(sum(b.stake for b in sub))
            try:
                insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type=f"{PURE_EV_MODEL_NAME}{suffix}",
                    bet_type=bt,
                    horses=horses_payload,
                    confidence=max(b.prob for b in sub),
                    expected_value=max(b.expected_value for b in sub),
                    recommended_bet=float(kelly_sum),
                    notes=(
                        f"黒字化専用枠 {len(sub)}点 EV>={cfg.ev_threshold} "
                        f"1/10Kelly実額¥{kelly_sum:,}（会計はflat ¥{100 * len(sub):,}）"
                    ),
                    combination_json=combo_json,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Pure_EV_Edge] 保存失敗 %s: %s", bt, exc)

        logger.info(
            "[Pure_EV_Edge] race_id=%s 単%d複%d点 保存",
            race_id,
            sum(1 for b in bets.bets if b.bet_type == "単勝"),
            sum(1 for b in bets.bets if b.bet_type == "複勝"),
        )
        return bets
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Pure_EV_Edge] スキップ（例外）: %s", exc)
        return None


def _run_fukusho_elite(
    conn: sqlite3.Connection,
    race_id: str,
    df: pd.DataFrame,
    manji_ev_scores: pd.Series,
    place_scores: pd.Series,
    suffix: str,
) -> "RaceBets | None":
    """FukushoElite 複勝（segment+edge × 複勝EV最優先ゲート）を生成・保存する（W-020）。

    収益セグメント(venue/頭数/edge)を通過し、かつ統計的複勝 EV が
    ``FUKUSHO_ELITE_EV_MIN`` 以上の馬のみを実弾複勝として保存する。
    勝率・複勝率単独でのベットは行わない（EV 最優先）。失敗しても本処理は止めない。

    Returns:
        RaceBets（買い目あり）または None（segment/EV ゲート見送り・例外）。
    """
    try:
        from src.ml.bet_generator import (
            FUKUSHO_ELITE_EV_MIN,
            generate_elite_fukusho_bets,
        )

        if "horse_number" not in df.columns or "win_odds" not in df.columns:
            return None

        race_row = conn.execute(
            "SELECT venue FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        venue = (race_row[0] if race_row else "") or ""

        n = len(df)
        horse_numbers: list[int] = []
        win_odds: list[float] = []
        ev_list: list[float] = []
        place_list: list[float] = []
        names: list[str] = []
        for i in range(n):
            hrow = df.iloc[i]
            try:
                hn = int(hrow.get("horse_number"))
            except (TypeError, ValueError):
                continue
            o = hrow.get("win_odds")
            o_f = float(o) if o is not None and not pd.isna(o) else 0.0
            horse_numbers.append(hn)
            win_odds.append(o_f)
            ev_list.append(
                float(manji_ev_scores.iloc[i]) if i < len(manji_ev_scores) else 0.0
            )
            place_list.append(
                float(place_scores.iloc[i]) if i < len(place_scores) else 0.0
            )
            names.append(str(hrow.get("horse_name", hn)))

        if not horse_numbers:
            return None

        # 市場 implied prob（1/odds を per-race 正規化）
        raw_imp = [1.0 / o if o > 1.0 else 0.0 for o in win_odds]
        s = sum(raw_imp)
        implied = [x / s if s > 0 else 1.0 / len(raw_imp) for x in raw_imp]

        rec = generate_elite_fukusho_bets(
            race_id=race_id,
            venue=venue,
            n_horses=n,
            horse_numbers=horse_numbers,
            horse_names=names,
            ev_scores=ev_list,
            implied_probs=implied,
            win_odds=win_odds,
            place_probs=place_list,
            ev_min=FUKUSHO_ELITE_EV_MIN,
        )
        if rec is None or not rec.bets:
            return None

        bet = rec.bets[0]  # 複勝1レコード（combinations=単頭リスト）
        sel = [c[0] for c in bet.combinations]
        place_map = dict(zip(horse_numbers, place_list))
        horses_payload = [
            {
                "horse_number": hn,
                "horse_name": dict(zip(horse_numbers, names)).get(hn, str(hn)),
                "predicted_rank": rank + 1,
                "model_score": float(place_map.get(hn, 0.0)),
                "ev_score": float(bet.expected_value),
            }
            for rank, hn in enumerate(sel)
        ]
        combo_json = _json.dumps([[hn] for hn in sel])
        try:
            insert_prediction(
                conn,
                race_id=race_id,
                model_type=f"FukushoElite{suffix}",
                bet_type="複勝",
                horses=horses_payload,
                confidence=bet.confidence,
                expected_value=bet.expected_value,
                recommended_bet=float(bet.recommended_bet),
                notes=bet.notes,
                combination_json=combo_json,
            )
            logger.info(
                "[FukushoElite] race_id=%s 複勝%d点 保存 (EV平均=%.2f)",
                race_id,
                len(sel),
                bet.expected_value,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[FukushoElite] 保存失敗: %s", exc)
            return None
        return rec
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FukushoElite] スキップ（例外）: %s", exc)
        return None


def _maybe_save_shadow(
    conn: "sqlite3.Connection",
    race_id: str,
    df: pd.DataFrame,
    honmei_scores: pd.Series,
    ev_scores: pd.Series,
) -> int:
    """SNS マーケティング専用シャドー（5月末ポリシー全券種）を best-effort で追記保存する。

    既に計算済みの ``df`` / ``honmei_scores`` / ``ev_scores`` を再利用するため、特徴量の
    二重生成も外部スクレイピングも行わない（タスク3: 直前バッチの負荷・タイムアウト対策）。
    別 model_type ``{base}_v0525(再計算)`` で INSERT するため、既存 live 予想を一切
    変更しない（CLAUDE.md 条項1）。env ``SHADOW_SNS_ENABLE=0`` で完全無効化できる。

    例外は warning ログのみで握り潰し、絶対に再送出しない（実弾予想・通知を止めない）。

    Returns:
        保存したシャドー prediction の件数（無効化・失敗時は 0）。
    """
    if os.getenv("SHADOW_SNS_ENABLE", "1").strip() == "0":
        logger.info("[ShadowSNS] SHADOW_SNS_ENABLE=0 のためスキップ: %s", race_id)
        return 0
    try:
        model_bets = generate_shadow_bets(race_id, df, honmei_scores, ev_scores)
        saved = save_shadow_bets(conn, race_id, model_bets)
        n = sum(len(pids) for pids in saved.values())
        logger.info(
            "[ShadowSNS] 5月末ポリシー多券種を追記: race_id=%s %d 件 (📱SNS専用)",
            race_id,
            n,
        )
        return n
    except Exception as exc:  # noqa: BLE001 — best-effort: 実弾予想を絶対に止めない
        logger.warning("[ShadowSNS] スキップ（例外・処理継続）: %s — %s", race_id, exc)
        return 0


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

    # Step 0b: レース選定フィルタ（#3）— 新馬戦・障害戦は買い目対象外として見送る
    # フラットモデルの予測信頼度が低くオッズ歪み検知精度が落ちるため、出馬表/オッズ取得前に弾く。
    _race_meta = conn.execute(
        "SELECT race_name, surface FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if _race_meta is not None and len(_race_meta) >= 2:
        _skip, _skip_reason = should_skip_race_for_betting(_race_meta[0], _race_meta[1])
        if _skip:
            logger.info("レース選定フィルタ: %s を見送り — %s", race_id, _skip_reason)
            _discord.notify_skip(race_id, _skip_reason)
            return {"skipped": True, "reason": _skip_reason, "race_id": race_id}

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

    # Step 1c: オッズ取得（JRA-VAN速報→RTD→netkeiba フォールバック）
    # ステップ2-1（特徴量の直前オーバーライド）:
    #   直前モードは毎回 fetch_and_save_odds を実行し、JRA-VAN 速報(JVRTOpen)の
    #   最新オッズ・馬体重・天候馬場を推論直前に強制反映する。
    #   _apply_jvrt_weight_weather が entries.horse_weight / races.weather を
    #   COALESCE 上書きし、最新スナップショット追記で _latest_odds_map も更新される。
    #   暫定モードは早朝でオッズ未公開のことが多いため従来どおり未取得時のみ試みる。
    if (not provisional) or cached_odds == 0:
        fetch_and_save_odds(conn, race_id)

    # Step 1d: 直前モードの馬体重強制更新（W-069）
    # JRAは発走約50分前に馬体重を公開する。fetch_and_save_odds 経由の JVRTOpen(0B11)で
    # 取得できなかった場合のフォールバックとして netkeiba から再取得し entries を UPSERT する。
    # これにより「馬体重欠損100%」警告を解消しモデルに正しい体重情報を渡す。
    if not provisional:
        try:
            from src.scraper.entry_table import fetch_entry_table

            weight_tbl = fetch_entry_table(race_id, delay=0.5)
            if weight_tbl.entries:
                weight_updated = 0
                for e in weight_tbl.entries:
                    if e.horse_weight is not None and e.horse_weight > 0:
                        conn.execute(
                            "UPDATE entries SET horse_weight = ?, horse_weight_diff = ? "
                            "WHERE race_id = ? AND horse_number = ? AND "
                            "(horse_weight IS NULL OR horse_weight = 0)",
                            (
                                e.horse_weight,
                                e.horse_weight_diff,
                                race_id,
                                e.horse_number,
                            ),
                        )
                        weight_updated += 1
                conn.commit()
                if weight_updated > 0:
                    logger.info(
                        "直前馬体重更新(W-069): %d頭 (race_id=%s)",
                        weight_updated,
                        race_id,
                    )
                else:
                    logger.debug(
                        "直前馬体重: 更新対象なし（JVRTOpenで取得済み）(race_id=%s)",
                        race_id,
                    )
        except Exception as exc:
            logger.warning("直前馬体重更新失敗（続行）: %s", exc)

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
                    race_id,
                    _vr["n_sentinel"],
                    len(df),
                )
            else:
                logger.warning(
                    "センチネル除外スキップ (race_id=%s): 除外後の有効頭数 %d < 最低 3頭 → 全馬で続行",
                    race_id,
                    _vr["n_valid_odds"],
                )
    except Exception as _ve:
        logger.warning("data_validator 呼び出し失敗（続行）: %s", _ve)

    # Step 3: モデル予測（V1/V2 分岐）
    # V2 系は V1 のサブクラス（HonmeiModelV2(HonmeiModel) 等）なので基底型で受ける。
    honmei_model: HonmeiModel
    _place_model: PlaceModel
    manji_model: ManjiModel
    is_v2 = model_version == "v2"
    if is_v2:
        honmei_model, _place_model, manji_model = load_models_v2()
        logger.info("V2 モデル使用: honmei_model_v2 / manji_model_v2")
    else:
        honmei_model, _place_model, manji_model = load_models()
    honmei_scores = honmei_model.predict(df)
    honmei_ev_scores = honmei_model.ev_predict(df)
    ev_scores = manji_model.ev_score(df)
    # Pure_EV_Edge（黒字化専用枠）の複勝較正確率に使用
    place_scores = _place_model.predict(df)

    # W-071: 学習済みモデル EV に手動係数を掛けていないことを宣言する
    # 手動オーバーレイが必要な場合は src/ml/ev_overlay_guard.apply_validated_overlay を使用し
    # 特徴量化→再学習→OOS ROI実証の手順を踏むこと（CLAUDE.md 条項 W-071）
    from src.ml.ev_overlay_guard import assert_no_manual_overlay

    assert_no_manual_overlay(ev_scores, context="prerace_pipeline/ev_scores")
    assert_no_manual_overlay(honmei_scores, context="prerace_pipeline/honmei_scores")

    # Step 3b: SHAP 寄与度計算（失敗しても予測は継続）
    honmei_shap_by_num: dict[int, str | None] = {}
    manji_shap_by_num: dict[int, str | None] = {}
    try:
        from src.ml.shap_explainer import build_shap_map
        from src.ml.models import _safe_feature_matrix

        X_shap = _safe_feature_matrix(df)
        honmei_shap_by_num = build_shap_map(honmei_model, X_shap, df)
        manji_shap_by_num = build_shap_map(manji_model, X_shap, df)
        logger.info("[SHAP] %d 頭分の寄与度を計算しました", len(honmei_shap_by_num))
    except Exception as _se:
        logger.warning("[SHAP] 計算失敗（予測は続行）: %s", _se)

    # Step 4: 買い目生成（V2 は BetGeneratorV2 を使用）
    current_bankroll = get_current_bankroll(conn)
    if is_v2:
        gen: BetGenerator = BetGeneratorV2(
            conn=conn,
            config=BetConfig(bankroll=current_bankroll, provisional=provisional),
        )
    else:
        gen = BetGenerator(
            conn=conn,
            config=BetConfig(bankroll=current_bankroll, provisional=provisional),
        )
    # ── AIウマスギフィルター統合済み ─────────────────────────────────────────
    # generate_honmei() / generate_alpha_trifecta() は内部で _apply_roi_filter() を呼び出す。
    # 本命: 単勝/複勝 + 三連単(EV≥1.5のみ) / 卍: 三連単除外 / Alpha: 三連単除外
    honmei_bets = gen.generate_honmei(race_id, df, honmei_scores)
    manji_bets = gen.generate_manji(race_id, df, ev_scores)
    # Oracle / HitFocus: 本命スコアから派生する記録・表示用ストラテジー（実 Kelly 投票は対象外）。
    # 暫定モードでは generate_oracle() が内部で空を返す（オッズ未取得のため）。
    oracle_bets = gen.generate_oracle(race_id, df, honmei_scores)
    hit_focus_bets = gen.generate_hit_focus(race_id, df, honmei_scores)
    logger.info(
        "[AIウマスギ] ROIフィルター適用完了: %s  本命=%d件 卍=%d件 Oracle=%d件 HitFocus=%d件",
        race_id,
        len(honmei_bets.bets),
        len(manji_bets.bets),
        len(oracle_bets.bets),
        len(hit_focus_bets.bets),
    )

    # P1-4: 直前の再推論では、このランの保存（Alpha含む）の前に
    #   同バリアントの旧「直前」予想を論理無効化し評価・ROI の二重計上を防ぐ。
    if not provisional:
        _supersede_prior_predictions(
            conn, race_id, "V2(直前)" if model_version == "v2" else "(直前)"
        )

    # Step 4b: Alpha-Payout 複勝+三連系シグナル（直前のみ）
    alpha_bets = None
    if not provisional:
        alpha_bets = _run_alpha_payout(conn, race_id, df, current_bankroll)

    # Step 4b2: Pure_EV_Edge 黒字化専用枠（単複のみ・直前のみ）
    # 既存ロジックと完全分離。卍Isotonic較正確率ベースのEV>=1.15・1/10 Kelly・
    # サーキットブレーカー付き。失敗しても本処理は止めない。
    pure_ev_bets = None
    if not provisional:
        _rdate = (
            f"{race_id[:4]}-{race_id[4:6]}-{race_id[6:8]}"
            if len(race_id) >= 8
            else None
        )
        pure_ev_bets = _run_pure_ev_edge(
            conn,
            race_id,
            df,
            ev_scores,
            place_scores,
            "(直前)",
            rdate=_rdate,
        )

    # Step 4b3: FukushoElite 複勝特化（segment+edge × 複勝EV最優先ゲート・直前のみ）
    #   W-020 本番統合。複勝 EV>=FUKUSHO_ELITE_EV_MIN のレースのみ生成。失敗しても止めない。
    fukusho_elite_bets = None
    if not provisional:
        fukusho_elite_bets = _run_fukusho_elite(
            conn, race_id, df, ev_scores, place_scores, "(直前)"
        )

    # Step 4c: オッズ歪み補正・危険馬フィルタ（直前のみ・ステップ2-2）
    #   realtime_odds の 朝→直前 変動率で「急騰=市場見限り(危険馬)」「急落=大口流入」を
    #   検知し、危険馬を軸に含む買い目の EV を減衰、EV<1.0 を除外する（買い目保存前）。
    if not provisional:
        try:
            from src.ml.odds_drift import (
                apply_drift_filter,
                compute_drift_map,
                danger_horses,
                plunge_horses,
            )

            drift_map = compute_drift_map(conn, race_id)
            if drift_map:
                plunges = plunge_horses(drift_map)
                dangers = danger_horses(drift_map)
                if plunges:
                    logger.warning(
                        "大口流入(オッズ急落)検知 race_id=%s 馬番=%s",
                        race_id,
                        sorted(plunges),
                    )
                if dangers:
                    logger.warning(
                        "危険馬(オッズ急騰)検知 race_id=%s 馬番=%s",
                        race_id,
                        sorted(dangers),
                    )
                    for _rb in (
                        honmei_bets,
                        manji_bets,
                        oracle_bets,
                        hit_focus_bets,
                        alpha_bets,
                    ):
                        if _rb is None:
                            continue
                        _rb.bets, _drp, _pen = apply_drift_filter(_rb.bets, drift_map)
                        if _drp or _pen:
                            logger.info(
                                "[危険馬フィルタ] %s: 減衰%d件 除外%d件",
                                getattr(_rb, "model_type", "?"),
                                _pen,
                                _drp,
                            )
        except Exception as _de:  # noqa: BLE001 — フィルタ失敗は予測本体を止めない
            logger.warning("オッズ歪みフィルタ失敗（続行）: %s", _de)

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
        oracle_bets=oracle_bets,
        hit_focus_bets=hit_focus_bets,
        honmei_shap=honmei_shap_by_num,
        manji_shap=manji_shap_by_num,
    )

    # Step 5b: SNS マーケティング専用シャドー（5月末ポリシー全券種）を best-effort で追記。
    #   実弾 EV パス（上記 _save_predictions）とは別 model_type ``_v0525(再計算)`` に隔離され、
    #   既存 live 予想を一切変更しない（条項1）。計算済みスコアを再利用するため負荷は最小。
    #   失敗してもライブ予想・通知を絶対に止めない（_maybe_save_shadow が例外を握り潰す）。
    _maybe_save_shadow(conn, race_id, df, honmei_scores, ev_scores)

    # Step 5c: WIN5（直前のみ）
    if not provisional:
        from src.pipeline.win5 import try_win5

        try_win5(conn, race_id)

    # Step 6: JSON 出力（V2 は {race_id}_v2.json に分離保存）
    payload = build_output_json(
        race_id,
        df,
        honmei_scores,
        honmei_ev_scores,
        ev_scores,
        honmei_bets,
        manji_bets,
        provisional=provisional,
    )
    payload["provisional"] = provisional
    payload["model_version"] = model_version
    # Pure_EV_Edge（黒字化専用枠）を独立セクションとして JSON に格納（UI トグル用）
    if pure_ev_bets is not None and getattr(pure_ev_bets, "bets", None):
        payload["pure_ev_edge"] = pure_ev_bets.to_dict()
    else:
        payload["pure_ev_edge"] = {
            "race_id": race_id,
            "model_type": "Pure_EV_Edge",
            "bets": [],
        }
    # FukushoElite（複勝特化・EV最優先ゲート）を独立セクションとして格納（W-020）
    if fukusho_elite_bets is not None and getattr(fukusho_elite_bets, "bets", None):
        payload["fukusho_elite"] = fukusho_elite_bets.to_dict()
    else:
        payload["fukusho_elite"] = {
            "race_id": race_id,
            "model_type": "FukushoElite",
            "bets": [],
        }
    if is_v2:
        from src.pipeline._common import JSON_OUT_DIR
        import json as _json_mod

        v2_out = JSON_OUT_DIR / f"{race_id}_v2.json"
        JSON_OUT_DIR.mkdir(parents=True, exist_ok=True)
        v2_out.write_text(
            _json_mod.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("V2 JSON 出力: %s", v2_out)
    else:
        save_json(race_id, payload)

    # Step 7: Discord 通知（直前のみ）— ALPHA / 卍 / 本命 独立3セクション送信
    if not provisional:
        _discord.notify_prerace_result(
            race_id,
            honmei_bets,
            manji_bets,
            oracle_bets=oracle_bets,
            hit_focus_bets=hit_focus_bets,
            alpha_bets=alpha_bets,
            race_name=str(_race_meta[0] or "") if _race_meta else "",
        )

        # Step 7a2: Pure_EV_Edge（黒字化専用枠）を独立 Discord 通知（EV アラートch）
        if pure_ev_bets is not None and getattr(pure_ev_bets, "bets", None):
            try:
                _discord.notify_pure_ev_edge(race_id, pure_ev_bets)
            except Exception as _pe_exc:  # noqa: BLE001
                logger.warning("[Pure_EV_Edge通知] スキップ（続行）: %s", _pe_exc)

        # Step 7b: 厳選レース判定 → X/note 下書き自動生成 + Discord 集客通知
        # （EV≥1.25 or 勝負レース。失敗しても本処理は止めない）
        try:
            from src.ops.note_generator import notify_gachi_for_race

            notify_gachi_for_race(race_id)
        except Exception as _gachi_exc:  # noqa: BLE001
            logger.warning("[厳選レース通知] スキップ（続行）: %s", _gachi_exc)

    alpha_bet_count = len(alpha_bets.bets) if alpha_bets is not None else 0
    hf_bets_count = len(hit_focus_bets.bets) if hit_focus_bets is not None else 0
    logger.info(
        "%sパイプライン完了: race_id=%s 本命%d件 卍%d件 Oracle%d件 HitFocus%d件 Alpha%d件",
        mode_label,
        race_id,
        len(prediction_ids["本命"]),
        len(prediction_ids["卍"]),
        len(oracle_bets.bets),
        hf_bets_count,
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
