"""
note 予想記事生成エンジン

本日の全レースを3モデル（本命・卍・ALPHA）の合意スコアで採点し、
「本日のおすすめ厳選レース」3〜5本を自動抽出。
各レースの買い目・根拠・馬プロファイルを網羅した記事 Markdown を生成する。

Usage:
    py -m src.ops.note_generator --date 20260523
    py -m src.ops.note_generator --date 20260523 --top 5
    py -m src.ops.note_generator --date 20260523 --stdout
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import date as _dt_date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logger = logging.getLogger(__name__)

# ── 定数 ─────────────────────────────────────────────────────────

_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR  = _ROOT / "outputs" / "note"

_SURFACE_JP:    dict[str, str] = {"芝": "芝", "ダート": "ダ", "障害": "障", "dirt": "ダ", "turf": "芝"}
_CONDITION_JP:  dict[str, str] = {"良": "良", "稍": "稍重", "重": "重", "不": "不良",
                                   "firm": "良", "good": "良", "yielding": "稍重", "soft": "重"}
_MARKS = ["◎", "○", "▲", "△", "×", "注"]

# 標準発走時刻（JRA）  race_number → "HH:MM"
_START_TIMES: dict[int, str] = {
    1: "10:00", 2: "10:30",  3: "11:00",  4: "11:30",
    5: "12:00", 6: "12:30",  7: "13:00",  8: "13:30",
    9: "14:00", 10: "14:30", 11: "15:00", 12: "15:30",
}

# 採点の重み
_W_ALPHA     = 3.0
_W_MANJI     = 2.0
_W_HONMEI    = 0.5
_W_CONSENSUS = 2.5   # 複数モデル同意ボーナスの単価

# EV しきい値（卍推奨条件）
_MANJI_EV_THRESHOLD = 1.15

# 最低スコア（選定候補に入るための足切り）
_MIN_SCORE = 2.0


# ── DB ヘルパー ──────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_race_ids(conn: sqlite3.Connection, date_str: str) -> list[str]:
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY venue, race_number",
        (date_str,),
    ).fetchall()
    return [r["race_id"] for r in rows]


def _fetch_race(conn: sqlite3.Connection, race_id: str) -> dict[str, Any]:
    row = conn.execute(
        """SELECT race_id, race_name, date, venue, race_number,
                  distance, surface, weather, condition, track_direction
           FROM races WHERE race_id = ?""",
        (race_id,),
    ).fetchone()
    return dict(row) if row else {}


def _fetch_entries(conn: sqlite3.Connection, race_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT e.horse_number, e.gate_number, e.horse_name, e.sex_age,
                  e.weight_carried, e.jockey, e.trainer,
                  e.horse_weight, e.horse_weight_diff,
                  o.win_odds, o.place_odds_min, o.place_odds_max, o.popularity
           FROM entries e
           LEFT JOIN realtime_odds o
             ON o.race_id = e.race_id AND o.horse_number = e.horse_number
           WHERE e.race_id = ?
           ORDER BY e.horse_number""",
        (race_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_preds_for_race(
    conn: sqlite3.Connection,
    race_id: str,
    model_pattern: str,
) -> list[dict[str, Any]]:
    """model_type LIKE model_pattern の直近予想を取得（EV 降順）。"""
    rows = conn.execute(
        """SELECT model_type, bet_type, confidence, expected_value,
                  recommended_bet, notes, combination_json
           FROM predictions
           WHERE race_id = ?
             AND model_type LIKE ?
           ORDER BY expected_value DESC NULLS LAST, id DESC
           LIMIT 30""",
        (race_id, model_pattern),
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["combos"] = json.loads(r["combination_json"]) if r["combination_json"] else []
        except (json.JSONDecodeError, TypeError):
            d["combos"] = []
        result.append(d)
    return result


def _fetch_honmei_scores(
    conn: sqlite3.Connection,
    race_id: str,
) -> list[dict[str, Any]]:
    """本命(直前) の per-horse スコアを confidence 降順で返す（馬番重複除去済み）。"""
    rows = conn.execute(
        """SELECT bet_type, confidence, expected_value, combination_json, notes
           FROM predictions
           WHERE race_id = ?
             AND model_type LIKE '本命%'
           ORDER BY confidence DESC NULLS LAST, id DESC
           LIMIT 40""",
        (race_id,),
    ).fetchall()
    seen_horses: set[int] = set()
    result = []
    for r in rows:
        d = dict(r)
        try:
            combo = json.loads(r["combination_json"]) if r["combination_json"] else []
            hn = combo[0][0] if combo and combo[0] else None
        except Exception:
            hn = None
        d["horse_number"] = hn
        # 同じ馬番は最高 confidence のものだけ残す
        if hn is not None and hn not in seen_horses:
            seen_horses.add(hn)
            result.append(d)
        elif hn is None:
            result.append(d)
    return result


# ── レース採点 ────────────────────────────────────────────────────

def _score_race(conn: sqlite3.Connection, race_id: str) -> dict[str, Any]:
    """
    3モデルの合意スコアでレースを採点する。

    Returns:
        {
            "race_id":     str,
            "score":       float,   # 総合スコア
            "alpha_ev":    float,   # Alpha-Payout 最大 EV (0 = シグナルなし)
            "manji_ev":    float,   # 卍 複勝 最大 EV
            "honmei_conf": float,   # 本命 最高 confidence
            "consensus":   int,     # EV≥1.0 のモデル数
            "alpha_preds": list,
            "manji_preds": list,
            "honmei_preds": list,
        }
    """
    alpha_preds  = _fetch_preds_for_race(conn, race_id, "Alpha-Payout%")
    manji_preds  = _fetch_preds_for_race(conn, race_id, "卍%")
    honmei_preds = _fetch_honmei_scores(conn, race_id)

    alpha_ev   = max((p["expected_value"] or 0.0 for p in alpha_preds),  default=0.0)
    manji_ev   = max(
        (p["expected_value"] or 0.0 for p in manji_preds if p["bet_type"] == "複勝"),
        default=0.0,
    )
    honmei_conf = max((p["confidence"] or 0.0 for p in honmei_preds), default=0.0)

    # 合意ボーナス: EV≥1.0 のモデル数
    consensus = sum([
        alpha_ev   >= 1.0,
        manji_ev   >= 1.0,
        honmei_conf >= 0.5,
    ])

    score = (
        alpha_ev   * _W_ALPHA
        + manji_ev   * _W_MANJI
        + honmei_conf * _W_HONMEI
        + consensus  * _W_CONSENSUS
    )

    return {
        "race_id":      race_id,
        "score":        round(score, 3),
        "alpha_ev":     round(alpha_ev,    3),
        "manji_ev":     round(manji_ev,    3),
        "honmei_conf":  round(honmei_conf, 3),
        "consensus":    consensus,
        "alpha_preds":  alpha_preds,
        "manji_preds":  manji_preds,
        "honmei_preds": honmei_preds,
    }


def select_recommended_races(
    conn: sqlite3.Connection,
    date_str: str,
    top_n: int = 5,
    min_n: int = 3,
) -> list[dict[str, Any]]:
    """
    本日の全レースを採点し上位 top_n 件（最低 min_n 件）を返す。

    Args:
        date_str: "YYYY-MM-DD" 形式（または "YYYYMMDD" も受け付ける）
        top_n:    最大選定数
        min_n:    最低選定数（足切り後でも満たす）
    """
    if len(date_str) == 8 and date_str.isdigit():
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    race_ids = _fetch_race_ids(conn, date_str)
    if not race_ids:
        logger.warning("レースが見つかりません: %s", date_str)
        return []

    scored = [_score_race(conn, rid) for rid in race_ids]
    scored.sort(key=lambda x: x["score"], reverse=True)

    # 足切り: _MIN_SCORE 未満は除外（ただし最低 min_n 件は確保）
    qualified = [s for s in scored if s["score"] >= _MIN_SCORE]
    if len(qualified) < min_n:
        qualified = scored[:min_n]

    return qualified[:top_n]


# ── Markdown 生成ユーティリティ ──────────────────────────────────

def _fmt_combo(combo: list[list[int]], bet_type: str, max_show: int = 8) -> str:
    """買い目リストを人が読みやすい文字列に変換する。"""
    if not combo:
        return "―"
    flat: list[str] = []
    for c in combo[:max_show]:
        if len(c) == 1:
            flat.append(f"{c[0]}番")
        else:
            flat.append("→".join(str(x) for x in c))
    suffix = f" 他{len(combo) - max_show}点" if len(combo) > max_show else ""
    sep = " / " if bet_type in ("馬連", "複勝") else "  "
    return sep.join(flat) + suffix


def _star_rating(consensus: int) -> str:
    return "⭐" * min(consensus, 4) + ("☆" * (4 - min(consensus, 4)))


def _surface_str(surface: str, distance: int, direction: str) -> str:
    s = _SURFACE_JP.get(surface, surface or "芝")
    d = f"{distance}m" if distance else ""
    dr = f"（{direction}回り）" if direction else ""
    return f"{s}{d}{dr}"


def _condition_str(cond: str) -> str:
    return _CONDITION_JP.get(cond, cond or "良")


def _start_time(race_number: int) -> str:
    return _START_TIMES.get(race_number, f"R{race_number}")


def _mark_horse(rank: int) -> str:
    return _MARKS[rank] if rank < len(_MARKS) else "△"


def _ev_bar(ev: float) -> str:
    """EV を絵文字バーで視覚化する。"""
    if ev >= 3.0:  return "🔥🔥🔥 超高EV"
    if ev >= 2.0:  return "🔥🔥 高EV"
    if ev >= 1.5:  return "🔥 EV良好"
    if ev >= 1.0:  return "✅ EV適正"
    return "⚠️ EV参考値"


# ── レース記事生成 ────────────────────────────────────────────────

def _build_race_section(
    conn: sqlite3.Connection,
    sc: dict[str, Any],
    rank: int,
) -> list[str]:
    """
    1レース分の Markdown ブロックを生成する。
    """
    race_id = sc["race_id"]
    race    = _fetch_race(conn, race_id)
    entries = _fetch_entries(conn, race_id)

    venue     = race.get("venue", "")
    race_no   = race.get("race_number", 0)
    race_name = race.get("race_name") or f"R{race_no}"
    distance  = race.get("distance", 0)
    surface   = race.get("surface", "")
    condition = race.get("condition", "")
    weather   = race.get("weather", "")
    direction = race.get("track_direction", "")

    surf_str = _surface_str(surface, distance, direction)
    cond_str = _condition_str(condition)
    start    = _start_time(race_no)
    stars    = _star_rating(sc["consensus"])

    lines: list[str] = []

    # ── ヘッダー ──────────────────────────────────────────────────
    lines += [
        f"## 🎯 推奨{rank}：{venue} {race_no}R「{race_name}」",
        "",
        f"> **4モデル合意スコア: {stars}**　（総合 {sc['score']:.1f}pt）",
        "",
    ]

    # ── レース情報テーブル ─────────────────────────────────────────
    lines += [
        "### 📋 レース情報",
        "",
        "| 項目 | 内容 |",
        "|------|------|",
        f"| 開催 | {venue}競馬場 |",
        f"| レース番号 | {race_no}R |",
        f"| レース名 | {race_name} |",
        f"| 発走時刻 | {start} |",
        f"| 条件 | {surf_str} |",
        f"| 馬場状態 | {cond_str}（天候: {weather or '―'}）|",
        f"| 出走頭数 | {len(entries)}頭 |",
        "",
    ]

    # ── 4モデルシグナル一覧 ────────────────────────────────────────
    lines += ["### 🤖 モデルシグナル", ""]

    # 本命モデル
    honmei = sc["honmei_preds"]
    if honmei:
        top_horses = []
        for i, p in enumerate(honmei[:5]):
            hn = p.get("horse_number")
            if hn is None:
                continue
            ent = next((e for e in entries if e["horse_number"] == hn), {})
            name = ent.get("horse_name", f"{hn}番")
            mark = _mark_horse(i)
            conf = p.get("confidence") or 0.0
            ev   = p.get("expected_value") or 0.0
            top_horses.append(f"{mark} {name}（{hn}番） `conf={conf:.3f}` `EV={ev:.2f}`")
        lines.append("**🎯 本命モデル** — 勝率スコア上位馬")
        lines += [f"- {h}" for h in top_horses]
        lines.append("")

    # 卍モデル
    manji = sc["manji_preds"]
    if manji:
        lines.append(f"**⚡ 卍（まんじ）モデル** — 期待値特化 {_ev_bar(sc['manji_ev'])}")
        shown_types: set[str] = set()
        for p in manji[:5]:
            bt  = p["bet_type"]
            ev  = p.get("expected_value") or 0.0
            cmb = _fmt_combo(p["combos"], bt)
            ev_str = f"EV={ev:.2f}" if ev else ""
            # 同じ bet_type の最高 EV だけ表示
            if bt not in shown_types:
                shown_types.add(bt)
                lines.append(f"- **{bt}**: {cmb}  `{ev_str}`")
        lines.append("")

    # ALPHA モデル
    alpha = sc["alpha_preds"]
    if alpha:
        lines.append(f"**📈 ALPHA（アルファ）モデル** — 特徴量EV特化 {_ev_bar(sc['alpha_ev'])}")
        for p in alpha[:3]:
            bt  = p["bet_type"]
            ev  = p.get("expected_value") or 0.0
            cmb = _fmt_combo(p["combos"], bt, max_show=6)
            lines.append(f"- **{bt}**: {cmb}  `EV={ev:.2f}`")
        lines.append("")

    # ── 推奨買い目まとめ ─────────────────────────────────────────
    lines += ["### 💰 推奨買い目まとめ", ""]
    bet_lines = _build_bet_summary(sc, entries)
    lines += bet_lines
    lines.append("")

    # ── 出走馬プロファイル ─────────────────────────────────────────
    lines += ["### 🐎 出走馬プロファイル", ""]
    lines += _build_horse_table(entries, honmei)
    lines.append("")

    # ── 投資メモ ─────────────────────────────────────────────────
    best_ev = max(sc["alpha_ev"], sc["manji_ev"], 0.0)
    if best_ev >= 1.0:
        rec_amount = _calc_rec_bet(best_ev)
        lines += [
            "### 📝 投資メモ",
            "",
            f"- 最高EV: **{best_ev:.2f}**  →  期待収益率 **+{(best_ev - 1) * 100:.0f}%**",
            f"- 推奨ベット規模: **¥{rec_amount:,}**（軍資金1万円あたり）",
            "> ⚠️ 本予想は AI 分析に基づく参考情報です。馬券投票は余裕資金の範囲内でお願いします。",
            "",
        ]

    lines += ["---", ""]
    return lines


def _build_bet_summary(
    sc: dict[str, Any],
    entries: list[dict[str, Any]],
) -> list[str]:
    """4モデルの結果を統合して「推奨買い目」箇条書きを生成する。"""
    lines: list[str] = []
    order = 1

    # 本命の軸馬を特定（confidence 最高馬）
    axis: int | None = None
    if sc["honmei_preds"]:
        top = sc["honmei_preds"][0]
        axis = top.get("horse_number")

    def _name(hn: int | None) -> str:
        if hn is None:
            return "―"
        e = next((x for x in entries if x["horse_number"] == hn), {})
        return e.get("horse_name", f"{hn}番") or f"{hn}番"

    # ① 単勝（卍か ALPHA の単勝シグナルがあれば）
    manji_tansho = next(
        (p for p in sc["manji_preds"] if p["bet_type"] == "単勝"), None
    )
    if manji_tansho and manji_tansho.get("expected_value", 0) >= 1.0:
        hn  = manji_tansho["combos"][0][0] if manji_tansho["combos"] else axis
        ev  = manji_tansho.get("expected_value", 0)
        lines.append(f"{order}. **単勝**: ◎ {_name(hn)}（{hn}番）  `EV={ev:.2f}` ← 卍モデル推奨")
        order += 1

    # ② 複勝（卍の複勝シグナル）
    manji_fuku = [p for p in sc["manji_preds"] if p["bet_type"] == "複勝"]
    if manji_fuku:
        ev  = manji_fuku[0].get("expected_value", 0)
        cmb = _fmt_combo(manji_fuku[0]["combos"], "複勝")
        lines.append(f"{order}. **複勝**: {cmb}  `EV={ev:.2f}` ← 卍モデル推奨（期待値{ev*100:.0f}%）")
        order += 1

    # ③ ALPHA 複勝
    alpha_fuku = [p for p in sc["alpha_preds"] if p["bet_type"] == "複勝"]
    if alpha_fuku:
        ev  = alpha_fuku[0].get("expected_value", 0)
        cmb = _fmt_combo(alpha_fuku[0]["combos"], "複勝")
        lines.append(f"{order}. **複勝（ALPHA）**: {cmb}  `EV={ev:.2f}` ← ALPHAシグナル")
        order += 1

    # ④ 馬連（馬連 EV 自体が ≥ 1.0 のときのみ）
    manji_umaren = next(
        (p for p in sc["manji_preds"] if p["bet_type"] == "馬連"), None
    )
    if manji_umaren:
        umaren_ev = manji_umaren.get("expected_value", 0) or 0.0
        if umaren_ev >= 1.0:
            cmb = _fmt_combo(manji_umaren["combos"], "馬連", max_show=5)
            lines.append(f"{order}. **馬連**: {cmb}  `EV={umaren_ev:.2f}` ← 卍モデル推奨")
            order += 1

    # ⑤ 三連複（ALPHA）
    alpha_tri = next(
        (p for p in sc["alpha_preds"] if p["bet_type"] == "三連複"), None
    )
    if alpha_tri:
        ev  = alpha_tri.get("expected_value", 0)
        cmb = _fmt_combo(alpha_tri["combos"], "三連複", max_show=6)
        lines.append(f"{order}. **三連複**: {cmb}  `EV={ev:.2f}` ← ALPHAモデル推奨")
        order += 1

    if not lines:
        lines.append("※ EV ≥ 1.0 の強いシグナルはありません。参考程度でご検討ください。")

    return lines


def _build_horse_table(
    entries: list[dict[str, Any]],
    honmei_preds: list[dict[str, Any]],
) -> list[str]:
    """出走馬テーブルを生成する。"""
    # honmei スコアを馬番→rank にマッピング
    rank_map: dict[int, int] = {}
    for i, p in enumerate(honmei_preds):
        hn = p.get("horse_number")
        if hn is not None and hn not in rank_map:
            rank_map[hn] = i

    lines = [
        "| 馬番 | 印 | 馬名 | 性齢 | 斤量 | 騎手 | 単勝オッズ | 人気 |",
        "|------|-----|------|------|------|------|----------|------|",
    ]
    for e in entries:
        hn    = e["horse_number"]
        gate  = e["gate_number"]
        mark  = _mark_horse(rank_map[hn]) if hn in rank_map else "　"
        name  = e.get("horse_name", "―")
        age   = e.get("sex_age", "―")
        wc    = e.get("weight_carried", "―")
        jock  = e.get("jockey", "―")
        odds  = e.get("win_odds")
        pop   = e.get("popularity")
        odds_str = f"{odds:.1f}倍" if odds else "―"
        pop_str  = f"{pop}番人気" if pop else "―"
        lines.append(
            f"| {hn}（{gate}枠） | **{mark}** | {name} | {age} | {wc}kg | {jock} "
            f"| {odds_str} | {pop_str} |"
        )
    return lines


def _calc_rec_bet(ev: float) -> int:
    """EV に基づく推奨ベット額（軍資金1万円あたり）を返す。"""
    kelly = max(0, (ev - 1) / (ev if ev > 0 else 1))
    kelly = min(kelly, 0.25)   # 最大 Kelly 25% キャップ
    amount = int(10000 * kelly / 100) * 100  # 100円単位
    return max(amount, 100)


# ── 記事ドキュメント生成 ──────────────────────────────────────────

def _build_paywall_separator() -> list[str]:
    """1レース目（無料公開）と2レース目以降（有料）の仕切りブロックを生成する。

    note の有料ライン機能は、記事本文中の「---」区切りで設定するのではなく
    note エディタ上で「有料設定」を手動 or Playwright 経由で行う。
    ここでは読者向けの視覚的な案内テキストとして仕切りを挿入する。
    """
    return [
        "",
        "---",
        "",
        "> ## 🔒 ここから先は有料エリアです",
        ">",
        "> **2レース目以降の詳細予想・買い目・根拠は有料コンテンツです。**  ",
        "> 購入いただくとすべての厳選レースの AI 予想が閲覧できます。",
        ">",
        "> - 1レース目（↑上記）は無料でお読みいただけます",
        "> - 2レース目以降：AI 合意スコア上位の厳選レース予想を公開",
        "> - 買い目 / 根拠 / 馬プロファイルを完全収録",
        "",
        "---",
        "",
    ]


def _build_header(date_str: str, n_races: int) -> list[str]:
    """記事の冒頭部分（note 記事トップ）を生成する。"""
    y, m, d = date_str[:4], date_str[4:6], date_str[6:]
    disp = f"{y}年{m}月{d}日"
    return [
        f"# 🏇【{disp}】AI厳選レース予想｜4モデル全弾発射",
        "",
        f"> **{disp}開催分**　厳選 {n_races} レース　by UMALOGI AI予測システム",
        "",
        (
            "本命・卍（まんじ）・ALPHAの**3つのAIモデルが合意したレース**のみを厳選しました。  \n"
            "期待値（EV）ベースの買い目と根拠を丁寧に解説しています。"
        ),
        "",
        "📖 **1レース目の詳細予想は無料公開。2レース目以降は有料エリアです。**",
        "",
        "---",
        "",
    ]


def _build_footer(date_str: str) -> list[str]:
    return [
        "## 📌 免責事項",
        "",
        "- 本記事は AI モデルの分析結果を基にした参考情報であり、的中を保証するものではありません。",
        "- 馬券投票は **余裕資金の範囲内** で自己責任でお願いします。",
        "- レース直前に発走取消・除外が発生した場合、買い目が変わる場合があります。",
        "",
        f"> UMALOGI AI予測システム | {date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
        "",
    ]


# ── メインエントリ ────────────────────────────────────────────────

def generate(
    date_str: str,
    top_n: int = 5,
    stdout: bool = False,
) -> str:
    """
    note 予想記事を生成して文字列で返す（同時にファイルへ保存）。

    Args:
        date_str: "YYYYMMDD" または "YYYY-MM-DD"
        top_n:    最大推奨レース数（3〜5 推奨）
        stdout:   True の場合は標準出力にも印刷する

    Returns:
        生成した Markdown 文字列
    """
    # date_str を YYYYMMDD に正規化
    ds = date_str.replace("-", "")
    if len(ds) != 8 or not ds.isdigit():
        raise ValueError(f"不正な日付形式: {date_str!r}  (YYYYMMDD または YYYY-MM-DD を指定)")

    db_date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"

    conn = _db()
    try:
        recommended = select_recommended_races(conn, db_date, top_n=top_n, min_n=3)
        if not recommended:
            msg = f"<!-- {date_str}: 予想データなし（予想バッチ未実行の可能性）-->\n"
            logger.warning("推奨レースが見つかりません: %s", date_str)
            return msg

        logger.info("推奨レース %d 件抽出 (%s)", len(recommended), date_str)
        for i, r in enumerate(recommended, 1):
            race = _fetch_race(conn, r["race_id"])
            logger.info(
                "  %d位: %s %dR (スコア=%.2f, 卍EV=%.2f, AlphaEV=%.2f, 合意=%d)",
                i, race.get("venue"), race.get("race_number", 0),
                r["score"], r["manji_ev"], r["alpha_ev"], r["consensus"],
            )

        lines: list[str] = []
        lines += _build_header(ds, len(recommended))

        for rank, sc in enumerate(recommended, 1):
            lines += _build_race_section(conn, sc, rank)
            # 1レース目（無料）と2レース目以降（有料）の仕切りを挿入
            if rank == 1 and len(recommended) > 1:
                lines += _build_paywall_separator()

        lines += _build_footer(ds)
        md = "\n".join(lines)

    finally:
        conn.close()

    # ファイル保存
    out_path = _OUT_DIR / f"{ds}_recommendations.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    logger.info("保存完了: %s", out_path)

    if stdout:
        print(md)

    return md


# ── CLI ─────────────────────────────────────────────────────────

def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )
    ap = argparse.ArgumentParser(description="note 予想記事自動生成エンジン")
    ap.add_argument(
        "--date", default=None,
        help="対象日 YYYYMMDD（省略時=本日）",
    )
    ap.add_argument(
        "--top", type=int, default=5,
        help="最大推奨レース数（デフォルト=5）",
    )
    ap.add_argument(
        "--stdout", action="store_true",
        help="標準出力にも記事を表示する",
    )
    args = ap.parse_args()

    date_str = args.date or _dt_date.today().strftime("%Y%m%d")
    md = generate(date_str=date_str, top_n=args.top, stdout=args.stdout)
    out_path = _OUT_DIR / f"{date_str.replace('-', '')}_recommendations.md"
    print(f"\n✅ 生成完了 → {out_path}")


if __name__ == "__main__":
    _cli()
