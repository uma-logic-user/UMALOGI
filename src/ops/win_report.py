"""
src/ops/win_report.py

的中報告レポート生成パイプライン。

使用例（fetch_race_result.py から）:
    from src.ops.win_report import publish_win_report
    publish_win_report(result, race_id, conn)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_RESULTS_DIR = _ROOT / "data" / "results"


@dataclass
class WinReportData:
    race_id:        str
    race_name:      str
    venue:          str
    race_number:    int
    date_str:       str           # YYYY-MM-DD
    hit_items:      list[Any]     # BetHitDetail リスト（is_hit=True のもの）
    total_invested: float
    total_payout:   float
    roi:            float
    top_ev:         float         # 的中した買い目の中で最高の expected_value
    ev_vs_odds:     list[dict[str, Any]]    # [{horse_number, odds, ev, gap}, ...]


def build_x_post(data: WinReportData) -> str:
    """280字以内の X 投稿テキストを返す。"""
    if not data.hit_items:
        raise ValueError("hit_items must not be empty")
    h = data.hit_items[0]
    race_name = data.race_name
    hashtags_full  = f"#競馬予想 #期待値アルゴリズム #的中実績 #{race_name}"
    hashtags_short = "#競馬予想 #期待値アルゴリズム #的中実績"

    base = (
        f"🎉【的中】{data.venue}{data.race_number}R {race_name}\n"
        f"EV={data.top_ev:.2f}の歪みを捉えて{h.bet_type}的中\n\n"
        f"投資¥{int(data.total_invested):,} → 払戻¥{int(data.total_payout):,}"
        f"（ROI {data.roi:.0f}%）\n\n"
        "期待値アルゴリズムが市場の非効率を見抜きました📊\n\n"
    )
    for tags in (hashtags_full, hashtags_short):
        candidate = base + tags
        if len(candidate) <= 280:
            return candidate

    short_base = (
        f"🎉【的中】{data.venue}{data.race_number}R {race_name}\n"
        f"投資¥{int(data.total_invested):,} → 払戻¥{int(data.total_payout):,}"
        f"（ROI {data.roi:.0f}%）\n\n"
        "期待値アルゴリズムが的中を導きました📊\n\n"
    )
    return (short_base + hashtags_short)[:280]


def generate_win_report_file(data: WinReportData) -> Path:
    """data/results/YYYYMMDD/{race_id}_win_report.txt を生成して Path を返す。"""
    date_nodash = data.date_str.replace("-", "")
    out_dir = _RESULTS_DIR / date_nodash
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{data.race_id}_win_report.txt"

    hit_lines: list[str] = []
    for h in data.hit_items:
        combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"
        sign = "+" if h.profit >= 0 else ""
        hit_lines.append(
            f"  {h.bet_type}  {combo_str}  "
            f"¥{int(h.payout):,} 払戻"
            f"（投資¥{int(h.invested):,} / 利益{sign}¥{int(h.profit):,}）"
        )
    hit_text = "\n".join(hit_lines)

    if data.ev_vs_odds:
        odds_lines = [
            f"  馬番{e['horse_number']}  "
            f"市場オッズ {e['odds']:.1f}倍 / AI推奨EV {e['ev']:.2f} / 乖離スコア {e['gap']:+.2f}"
            for e in data.ev_vs_odds
        ]
        odds_text = "\n".join(odds_lines)
    else:
        odds_text = "  (データなし)"

    title = "【的中実績】期待値最適化アルゴリズムによる選別成功"
    body = (
        f"本日、{data.venue}{data.race_number}R「{data.race_name}」において、"
        f"アルゴリズムが市場の歪みを捉え的中を達成しました。\n\n"
        f"推奨根拠：対象馬のEV値は{data.top_ev:.2f}であり、"
        f"ROIフィルター通過後の確実な選別を行いました。\n\n"
        f"■ 的中買い目\n{hit_text}\n\n"
        f"本日の合計ROI：{data.roi:.1f}％\n\n"
        f"■ 市場オッズ vs AIスコア（検証データ）\n{odds_text}"
    )
    x_post = build_x_post(data)

    content = f"=== TITLE ===\n{title}\n\n=== BODY ===\n{body}\n\n=== X_POST ===\n{x_post}\n"
    path.write_text(content, encoding="utf-8")
    return path


def build_note_draft(
    data: WinReportData,
    predictions: list[dict[str, Any]],
) -> tuple[str, str]:
    """(note_title, note_body) の Markdown を返す。"""
    if not data.hit_items:
        raise ValueError("hit_items must not be empty")
    ymd = data.date_str.replace("-", "/")
    title = (
        f"【的中実績】{ymd} {data.venue}{data.race_number}R"
        f"「{data.race_name}」— EV期待値アルゴリズム選別成功"
    )

    h = data.hit_items[0]  # 主力買い目のみ表示（複数的中時は先頭のみ）
    combo_str = "-".join(str(c) for c in h.combination) if h.combination else "?"

    top_model = "AIモデル"
    if predictions:
        best = max(predictions, key=lambda p: p.get("expected_value") or 0.0)
        top_model = best.get("model_type") or "AIモデル"

    if data.ev_vs_odds:
        odds_rows = "\n".join(
            f"| {e['horse_number']} | {e['odds']:.1f}倍 | {e['ev']:.2f} | {e['gap']:+.2f} |"
            for e in data.ev_vs_odds
        )
    else:
        odds_rows = "| — | — | — | — |"

    body = (
        f"# 【的中実績】{ymd} {data.venue}{data.race_number}R"
        f"「{data.race_name}」— EV期待値アルゴリズム選別成功\n\n"
        f"## 的中サマリー\n"
        f"| 項目 | 内容 |\n"
        f"|------|------|\n"
        f"| 買い目 | {h.bet_type} {combo_str} |\n"
        f"| 投資 | ¥{int(data.total_invested):,} |\n"
        f"| 払戻 | ¥{int(data.total_payout):,} |\n"
        f"| ROI | {data.roi:.1f}% |\n\n"
        f"## なぜこのレースを選んだか\n"
        f"本日、{data.venue}{data.race_number}R「{data.race_name}」において、"
        f"アルゴリズムが市場の歪みを捉え的中を達成しました。\n"
        f"推奨根拠：対象馬のEV値は{data.top_ev:.2f}であり、"
        f"ROIフィルター通過後の確実な選別を行いました。\n\n"
        f"## 市場オッズ vs AIスコア（比較データ）\n"
        f"| 馬番 | 市場オッズ | AI推奨EV | 乖離スコア |\n"
        f"|------|----------|---------|----------|\n"
        f"{odds_rows}\n\n"
        f"## フィルター貢献度\n"
        f"- {top_model}: EV={data.top_ev:.2f} で最高スコア\n"
        f"- ROIフィルター: 通過（EV > 1.0）\n"
        f"- ウマスギフィルター: 適用済み\n\n"
        f"## 免責事項\n"
        f"本記事は統計的期待値に基づく投資記録であり、的中を保証するものではありません。\n"
        f"投資は自己責任でお願いします。\n\n"
        f"*UMALOGI — AI 競馬予測プラットフォーム*"
    )
    return title, body


def _fetch_ev_vs_odds(
    conn: sqlite3.Connection,
    race_id: str,
    hit_items: list[Any],
) -> list[dict[str, Any]]:
    """的中買い目の馬番ごとに EV vs 市場オッズを構築する。

    predictions.combination_json の形式: [[horse_num, ...], ...]
    race_results.win_odds: 単勝オッズ
    gap = ev - (1.0 / win_odds)  正値 = AI が市場より高く評価
    """
    if not hit_items:
        return []

    pred_ids = [h.prediction_id for h in hit_items]
    placeholders = ",".join("?" * len(pred_ids))
    preds = conn.execute(
        f"SELECT id, expected_value, combination_json "
        f"FROM predictions WHERE id IN ({placeholders})",
        pred_ids,
    ).fetchall()

    pred_map: dict[int, tuple[float, list[int]]] = {}
    for p in preds:
        try:
            combos = json.loads(p["combination_json"]) if p["combination_json"] else []
            horse_nums = list(combos[0]) if combos else []
        except (json.JSONDecodeError, IndexError, TypeError, ValueError) as exc:
            logger.warning("[WinReport] combination_json パース失敗 id=%s: %s", p["id"], exc)
            horse_nums = []
        pred_map[p["id"]] = (p["expected_value"] or 0.0, horse_nums)

    odds_rows = conn.execute(
        "SELECT horse_number, win_odds FROM race_results WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    odds_map: dict[int, float] = {
        r["horse_number"]: r["win_odds"] or 0.0 for r in odds_rows
    }

    seen: set[int] = set()
    result: list[dict[str, Any]] = []
    for h in hit_items:
        ev, horse_nums = pred_map.get(h.prediction_id, (0.0, []))
        if not horse_nums:
            continue
        main_horse = int(horse_nums[0])
        if main_horse in seen:
            continue
        seen.add(main_horse)
        odds = odds_map.get(main_horse, 0.0)
        gap = ev - (1.0 / odds) if odds > 0 else 0.0
        result.append({"horse_number": main_horse, "odds": odds, "ev": ev, "gap": gap})

    return result


def _post_note_draft(data: WinReportData, predictions: list[dict[str, Any]]) -> None:
    raise NotImplementedError


def publish_win_report(result: Any, race_id: str, conn: sqlite3.Connection) -> None:
    raise NotImplementedError
