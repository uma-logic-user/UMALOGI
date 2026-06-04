"""
note 予想記事生成エンジン

本日の全レースを4モデル（本命・卍・Oracle・ALPHA）の合意スコアで採点し、
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
import os
import re
import sqlite3
import sys
from datetime import date as _dt_date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.ops.money_management import BetAllocation, allocate_budget  # noqa: E402
from src.ops.sns_publisher import NoteBet  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

logger = logging.getLogger(__name__)

# ── 定数 ─────────────────────────────────────────────────────────

_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR = _ROOT / "outputs" / "note"
# 厳選レース単体 note 用コピペ Markdown の出力先（X/SNS 集客導線）
_GACHI_DIR = _ROOT / "dist" / "notes"
_DRAFTS_DIR = _ROOT / "outputs" / "sns" / "drafts"

# ── 厳選レース（ガチ）判定パラメータ ──────────────────────────────
# 仕様: Alpha-Payout の実払戻 EV が _GACHI_EV_THRESHOLD 以上、または
#       卍を除いたクリーン合意が _GACHI_CLEAN_CONSENSUS 以上の「勝負レース」を厳選とする。
# - 卍モデルは W-048（confidence=1.0 固定キャリブレーション破綻・投資停止中）のため
#   EV・合意の判定対象から完全に除外する（EV が異常膨張し全件誤検知の温床になるため）。
# - 本命モデルの EV は勝率特化で 6.0 上限に張り付くため EV ゲートには使わない
#   （誤検知防止。本命は consensus 側の honmei_conf で評価する）。
# - 日次の最終出力は _GACHI_TOP_N 本までスコア降順で厳選する。
_GACHI_EV_THRESHOLD = 1.25
_GACHI_CLEAN_CONSENSUS = 3
_GACHI_TOP_N = 5

# 集客導線 URL（.env で上書き可能）
_NOTE_MYPAGE_URL = os.environ.get("NOTE_MYPAGE_URL", "https://note.com/umalogi")
_X_ACCOUNT_URL = os.environ.get("X_ACCOUNT_URL", "https://x.com/umalogi_ai")

_SURFACE_JP: dict[str, str] = {
    "芝": "芝",
    "ダート": "ダ",
    "障害": "障",
    "dirt": "ダ",
    "turf": "芝",
}
_CONDITION_JP: dict[str, str] = {
    "良": "良",
    "稍": "稍重",
    "重": "重",
    "不": "不良",
    "firm": "良",
    "good": "良",
    "yielding": "稍重",
    "soft": "重",
}
_MARKS = ["◎", "○", "▲", "△", "×", "注"]

# 標準発走時刻（JRA）  race_number → "HH:MM"
_START_TIMES: dict[int, str] = {
    1: "10:00",
    2: "10:30",
    3: "11:00",
    4: "11:30",
    5: "12:00",
    6: "12:30",
    7: "13:00",
    8: "13:30",
    9: "14:00",
    10: "14:30",
    11: "15:00",
    12: "15:30",
}

# 採点の重み
_W_ALPHA = 3.0
_W_MANJI = 2.0
_W_ORACLE = 1.0
_W_HONMEI = 0.5
_W_CONSENSUS = 2.5  # 複数モデル同意ボーナスの単価

# EV しきい値（卍推奨条件）
_MANJI_EV_THRESHOLD = 1.15

# 最低スコア（選定候補に入るための足切り）
_MIN_SCORE = 2.0


# ── DB ヘルパー ──────────────────────────────────────────────────


def _db() -> sqlite3.Connection:
    """umalogi.db への接続を返す（Row ファクトリ設定済み）。

    Returns:
        sqlite3.Row をロウファクトリとした Connection オブジェクト。
    """
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_race_ids(conn: sqlite3.Connection, date_str: str) -> list[str]:
    """指定日の race_id リストを会場・レース番号順で返す。

    Args:
        conn:     DB コネクション。
        date_str: 対象日（YYYY-MM-DD 形式）。

    Returns:
        race_id 文字列のリスト。
    """
    rows = conn.execute(
        "SELECT race_id FROM races WHERE date = ? ORDER BY venue, race_number",
        (date_str,),
    ).fetchall()
    return [r["race_id"] for r in rows]


def _fetch_race(conn: sqlite3.Connection, race_id: str) -> dict[str, Any]:
    """races テーブルから1レースの基本情報を辞書で返す。

    Args:
        conn:    DB コネクション。
        race_id: 取得するレース ID。

    Returns:
        レース情報の辞書。レコードが存在しない場合は空辞書。
    """
    row = conn.execute(
        """SELECT race_id, race_name, date, venue, race_number,
                  distance, surface, weather, condition, track_direction
           FROM races WHERE race_id = ?""",
        (race_id,),
    ).fetchone()
    return dict(row) if row else {}


def _fetch_entries(conn: sqlite3.Connection, race_id: str) -> list[dict[str, Any]]:
    """entries テーブルから出走馬一覧をリアルタイムオッズ付きで返す。

    Args:
        conn:    DB コネクション。
        race_id: 取得するレース ID。

    Returns:
        馬番昇順の出走馬情報リスト。realtime_odds が未登録の馬はオッズ NULL。
    """
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
            d["combos"] = (
                json.loads(r["combination_json"]) if r["combination_json"] else []
            )
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
    4モデルの合意スコアでレースを採点する。

    Returns:
        {
            "race_id":        str,
            "score":          float,   # 総合スコア
            "alpha_ev":       float,   # Alpha-Payout 最大 EV (0 = シグナルなし)
            "manji_ev":       float,   # 卍 複勝 最大 EV
            "oracle_count":   int,     # Oracle 買い目数
            "honmei_conf":    float,   # 本命 最高 confidence
            "honmei_ev":      float,   # 本命 最高 EV
            "consensus":      int,     # EV≥1.0 のモデル数
            "alpha_preds":    list,
            "manji_preds":    list,
            "oracle_preds":   list,
            "honmei_preds":   list,
        }
    """
    alpha_preds = _fetch_preds_for_race(conn, race_id, "Alpha-Payout%")
    manji_preds = _fetch_preds_for_race(conn, race_id, "卍%")
    oracle_preds = _fetch_preds_for_race(conn, race_id, "Oracle%")
    honmei_preds = _fetch_honmei_scores(conn, race_id)

    alpha_ev = max((p["expected_value"] or 0.0 for p in alpha_preds), default=0.0)
    manji_ev = max(
        (p["expected_value"] or 0.0 for p in manji_preds if p["bet_type"] == "複勝"),
        default=0.0,
    )
    oracle_count = len(oracle_preds)
    honmei_conf = max((p["confidence"] or 0.0 for p in honmei_preds), default=0.0)
    honmei_ev = max((p["expected_value"] or 0.0 for p in honmei_preds), default=0.0)

    # 合意ボーナス: EV≥1.0 のモデル数
    consensus = sum(
        [
            alpha_ev >= 1.0,
            manji_ev >= 1.0,
            oracle_count >= 1,
            honmei_conf >= 0.5,
        ]
    )

    score = (
        alpha_ev * _W_ALPHA
        + manji_ev * _W_MANJI
        + oracle_count * _W_ORACLE
        + honmei_conf * _W_HONMEI
        + consensus * _W_CONSENSUS
    )

    return {
        "race_id": race_id,
        "score": round(score, 3),
        "alpha_ev": round(alpha_ev, 3),
        "manji_ev": round(manji_ev, 3),
        "oracle_count": oracle_count,
        "honmei_conf": round(honmei_conf, 3),
        "honmei_ev": round(honmei_ev, 3),
        "consensus": consensus,
        "alpha_preds": alpha_preds,
        "manji_preds": manji_preds,
        "oracle_preds": oracle_preds,
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
    """買い目リストをマルチ/軸流し/ボックス表記に自動変換する。

    ベタ展開（▶ A→B→C の箇条書き）は廃止し、即PAT入力しやすい
    「【三連単マルチ 軸X - 相手Y,Z】計N点」形式を優先して返す。
    """
    if not combo:
        return "―"

    n = len(combo)
    all_nums = sorted({int(x) for c in combo for x in c})

    # 単勝・複勝
    if bet_type in ("単勝", "複勝"):
        nums = [c[0] for c in combo]
        return " / ".join(f"{x}番" for x in nums)

    # 三連単・馬単: マルチ または 1着固定
    if bet_type in ("三連単", "馬単"):
        always_in = [x for x in all_nums if all(x in list(c) for c in combo)]
        if always_in:
            others = [x for x in all_nums if x not in always_in]
            return (
                f"【{bet_type}マルチ 軸{','.join(str(a) for a in always_in)} "
                f"- 相手{','.join(str(o) for o in others)}】 計{n}点"
            )
        firsts = sorted({int(c[0]) for c in combo if c})
        if len(firsts) <= 2:
            others = [x for x in all_nums if x not in firsts]
            return (
                f"【{bet_type} 軸{','.join(str(f) for f in firsts)}(1着) "
                f"- 相手{','.join(str(o) for o in others)}】 計{n}点"
            )
        # フォーメーション（上記非該当）
        flat = ["→".join(str(x) for x in c) for c in combo[:max_show]]
        suffix = f" 他{n - max_show}点" if n > max_show else ""
        return "  ".join(flat) + suffix

    # 三連複・馬連・ワイド: 軸流し or ボックス
    axes = [x for x in all_nums if all(x in list(c) for c in combo)]
    if axes:
        others = [x for x in all_nums if x not in axes]
        if others:
            return (
                f"【{bet_type}流し 軸{','.join(str(a) for a in axes)} "
                f"- 相手{','.join(str(o) for o in others)}】 計{n}点"
            )
        # 相手がない（全馬が軸 = ボックス1通り）
        return f"{bet_type} {','.join(str(x) for x in all_nums)}番 計{n}点"
    return f"{bet_type}ボックス {','.join(str(x) for x in all_nums)}番 計{n}点"


def _star_rating(consensus: int) -> str:
    """合意モデル数を⭐/☆の4段階評価文字列に変換する。

    Args:
        consensus: EV≥1.0 のモデル数（0〜3）。

    Returns:
        最大4つの⭐と残り☆の組み合わせ文字列（例: "⭐⭐☆☆"）。
    """
    return "⭐" * min(consensus, 4) + ("☆" * (4 - min(consensus, 4)))


def _surface_str(surface: str, distance: int, direction: str) -> str:
    """馬場・距離・方向を日本語の表示文字列に変換する。

    Args:
        surface:   馬場種別（"芝" / "ダート" / "turf" 等）。
        distance:  距離（メートル）。0 の場合は省略。
        direction: 回り方向（"右" / "左" 等）。空文字の場合は省略。

    Returns:
        "芝1600m（右回り）" 形式の文字列。
    """
    s = _SURFACE_JP.get(surface, surface or "芝")
    d = f"{distance}m" if distance else ""
    dr = f"（{direction}回り）" if direction else ""
    return f"{s}{d}{dr}"


def _condition_str(cond: str) -> str:
    """馬場状態コードを日本語表示に変換する。

    Args:
        cond: 馬場状態（"良" / "稍" / "firm" 等）。

    Returns:
        日本語の馬場状態文字列。未知の値はそのまま返す。
    """
    return _CONDITION_JP.get(cond, cond or "良")


def _start_time(race_number: int) -> str:
    """レース番号から標準発走時刻文字列を返す。

    Args:
        race_number: レース番号（1〜12）。

    Returns:
        "HH:MM" 形式の発走時刻。テーブルにない場合は "RN" 形式を返す。
    """
    return _START_TIMES.get(race_number, f"R{race_number}")


def _mark_horse(rank: int) -> str:
    """本命スコア順位を予想印に変換する。

    Args:
        rank: 0始まりの順位（0=◎, 1=○, 2=▲, ...）。

    Returns:
        対応する予想印文字列。インデックス範囲外の場合は "△"。
    """
    return _MARKS[rank] if rank < len(_MARKS) else "△"


def _ev_bar(ev: float) -> str:
    """EV を絵文字バーで視覚化した文字列を返す。

    Args:
        ev: 期待値（Expected Value）。1.0 未満は参考値扱い。

    Returns:
        EV の高さに応じた絵文字ラベル文字列。
    """
    if ev >= 3.0:
        return "🔥🔥🔥 超高EV"
    if ev >= 2.0:
        return "🔥🔥 高EV"
    if ev >= 1.5:
        return "🔥 EV良好"
    if ev >= 1.0:
        return "✅ EV適正"
    return "⚠️ EV参考値"


# ── レース記事生成 ────────────────────────────────────────────────


def _build_race_section(
    conn: sqlite3.Connection,
    sc: dict[str, Any],
    rank: int,
) -> list[str]:
    """1レース分の Markdown ブロックを生成する。

    Args:
        conn: DB コネクション。
        sc:   _score_race() が返すスコア辞書。
        rank: 推奨順位（1始まり）。

    Returns:
        Markdown 行のリスト。
    """
    race_id = sc["race_id"]
    race = _fetch_race(conn, race_id)
    entries = _fetch_entries(conn, race_id)

    venue = race.get("venue", "")
    race_no = race.get("race_number", 0)
    race_name = race.get("race_name") or f"R{race_no}"
    distance = race.get("distance", 0)
    surface = race.get("surface", "")
    condition = race.get("condition", "")
    weather = race.get("weather", "")
    direction = race.get("track_direction", "")

    surf_str = _surface_str(surface, distance, direction)
    cond_str = _condition_str(condition)
    start = _start_time(race_no)
    stars = _star_rating(sc["consensus"])

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
            ev = p.get("expected_value") or 0.0
            top_horses.append(
                f"{mark} {name}（{hn}番） `conf={conf:.3f}` `EV={ev:.2f}`"
            )
        lines.append("**🎯 本命モデル** — 勝率スコア上位馬")
        lines += [f"- {h}" for h in top_horses]
        lines.append("")

    # 卍モデル
    manji = sc["manji_preds"]
    if manji:
        lines.append(
            f"**⚡ 卍（まんじ）モデル** — 期待値特化 {_ev_bar(sc['manji_ev'])}"
        )
        shown_types: set[str] = set()
        for p in manji[:5]:
            bt = p["bet_type"]
            ev = p.get("expected_value") or 0.0
            cmb = _fmt_combo(p["combos"], bt)
            ev_str = f"EV={ev:.2f}" if ev else ""
            # 同じ bet_type の最高 EV だけ表示
            if bt not in shown_types:
                shown_types.add(bt)
                lines.append(f"- **{bt}**: {cmb}  `{ev_str}`")
        lines.append("")

    # Oracle モデル
    oracle = sc["oracle_preds"]
    if oracle:
        lines.append("**🔮 Oracle（オラクル）モデル** — 高配当組み合わせ")
        shown_oracle: set[str] = set()
        for p in oracle:
            bt = p["bet_type"]
            if bt in shown_oracle:
                continue
            shown_oracle.add(bt)
            ev = p.get("expected_value") or 0.0
            cmb = _fmt_combo(p["combos"], bt, max_show=6)
            lines.append(f"- **{bt}**: {cmb}  `EV={ev:.2f}`")
            if len(shown_oracle) >= 3:
                break
        lines.append("")

    # ALPHA モデル
    alpha = sc["alpha_preds"]
    if alpha:
        lines.append(
            f"**📈 ALPHA（アルファ）モデル** — 特徴量EV特化 {_ev_bar(sc['alpha_ev'])}"
        )
        for p in alpha[:3]:
            bt = p["bet_type"]
            ev = p.get("expected_value") or 0.0
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
    """4モデルの結果を統合して「推奨買い目」箇条書きを生成する。

    EV ≥ 1.0 のシグナルを卍・ALPHA の順に列挙する。
    シグナルがない場合は参考コメントのみ返す。

    Args:
        sc:      _score_race() が返すスコア辞書。
        entries: 出走馬エントリーのリスト。

    Returns:
        Markdown 行のリスト（各行が1買い目の説明）。
    """
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
    manji_tansho = next((p for p in sc["manji_preds"] if p["bet_type"] == "単勝"), None)
    if manji_tansho and manji_tansho.get("expected_value", 0) >= 1.0:
        hn = manji_tansho["combos"][0][0] if manji_tansho["combos"] else axis
        ev = manji_tansho.get("expected_value", 0)
        lines.append(
            f"{order}. **単勝**: ◎ {_name(hn)}（{hn}番）  `EV={ev:.2f}` ← 卍モデル推奨"
        )
        order += 1

    # ② 複勝（卍の複勝シグナル）
    manji_fuku = [p for p in sc["manji_preds"] if p["bet_type"] == "複勝"]
    if manji_fuku:
        ev = manji_fuku[0].get("expected_value", 0)
        cmb = _fmt_combo(manji_fuku[0]["combos"], "複勝")
        lines.append(
            f"{order}. **複勝**: {cmb}  `EV={ev:.2f}` ← 卍モデル推奨（期待値{ev * 100:.0f}%）"
        )
        order += 1

    # ③ ALPHA 複勝
    alpha_fuku = [p for p in sc["alpha_preds"] if p["bet_type"] == "複勝"]
    if alpha_fuku:
        ev = alpha_fuku[0].get("expected_value", 0)
        cmb = _fmt_combo(alpha_fuku[0]["combos"], "複勝")
        lines.append(
            f"{order}. **複勝（ALPHA）**: {cmb}  `EV={ev:.2f}` ← ALPHAシグナル"
        )
        order += 1

    # ④ 馬連（馬連 EV 自体が ≥ 1.0 のときのみ）
    manji_umaren = next((p for p in sc["manji_preds"] if p["bet_type"] == "馬連"), None)
    if manji_umaren:
        umaren_ev = manji_umaren.get("expected_value", 0) or 0.0
        if umaren_ev >= 1.0:
            cmb = _fmt_combo(manji_umaren["combos"], "馬連", max_show=5)
            lines.append(
                f"{order}. **馬連**: {cmb}  `EV={umaren_ev:.2f}` ← 卍モデル推奨"
            )
            order += 1

    # ⑤ 三連複（Oracle or ALPHA）
    oracle_tri = next(
        (p for p in sc["oracle_preds"] if p["bet_type"] == "三連複"), None
    )
    alpha_tri = next((p for p in sc["alpha_preds"] if p["bet_type"] == "三連複"), None)
    tri = oracle_tri or alpha_tri
    if tri:
        src = "Oracle" if oracle_tri else "ALPHA"
        ev = tri.get("expected_value", 0)
        cmb = _fmt_combo(tri["combos"], "三連複", max_show=6)
        lines.append(f"{order}. **三連複**: {cmb}  `EV={ev:.2f}` ← {src}モデル推奨")
        order += 1

    if not lines:
        lines.append(
            "※ EV ≥ 1.0 の強いシグナルはありません。参考程度でご検討ください。"
        )

    return lines


def _build_horse_table(
    entries: list[dict[str, Any]],
    honmei_preds: list[dict[str, Any]],
) -> list[str]:
    """出走馬テーブルを Markdown 形式で生成する。

    Args:
        entries:      出走馬エントリーのリスト（馬番昇順）。
        honmei_preds: 本命モデルの予想リスト（confidence 降順）。予想印の付与に使用。

    Returns:
        Markdown テーブル行のリスト（ヘッダー行含む）。
    """
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
        hn = e["horse_number"]
        gate = e["gate_number"]
        mark = _mark_horse(rank_map[hn]) if hn in rank_map else "　"
        name = e.get("horse_name", "―")
        age = e.get("sex_age", "―")
        wc = e.get("weight_carried", "―")
        jock = e.get("jockey", "―")
        odds = e.get("win_odds")
        pop = e.get("popularity")
        odds_str = f"{odds:.1f}倍" if odds else "―"
        pop_str = f"{pop}番人気" if pop else "―"
        lines.append(
            f"| {hn}（{gate}枠） | **{mark}** | {name} | {age} | {wc}kg | {jock} "
            f"| {odds_str} | {pop_str} |"
        )
    return lines


def _calc_rec_bet(ev: float) -> int:
    """EV に基づく推奨ベット額（軍資金1万円あたり）を Kelly 基準で算出する。

    Kelly 基準を最大 25% でキャップし、100円単位に丸める。

    Args:
        ev: 期待値（Expected Value）。

    Returns:
        推奨ベット額（100円単位）。最低 100 円。
    """
    kelly = max(0, (ev - 1) / (ev if ev > 0 else 1))
    kelly = min(kelly, 0.25)  # 最大 Kelly 25% キャップ
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
    """記事の冒頭部分（note 記事トップ）を生成する。

    Args:
        date_str: 対象日 YYYYMMDD 形式。
        n_races:  厳選レース数。

    Returns:
        Markdown 行のリスト。
    """
    y, m, d = date_str[:4], date_str[4:6], date_str[6:]
    disp = f"{y}年{m}月{d}日"
    return [
        f"# 🏇【{disp}】AI厳選レース予想｜4モデル全弾発射",
        "",
        f"> **{disp}開催分**　厳選 {n_races} レース　by UMALOGI AI予測システム",
        "",
        (
            "本命・卍（まんじ）・Oracle・ALPHAの**4つのAIモデルが合意したレース**のみを厳選しました。  \n"
            "期待値（EV）ベースの買い目と根拠を丁寧に解説しています。"
        ),
        "",
        "📖 **1レース目の詳細予想は無料公開。2レース目以降は有料エリアです。**",
        "",
        "---",
        "",
    ]


def _build_footer(date_str: str) -> list[str]:
    """記事のフッター部分（免責事項 + クレジット）を生成する。

    Args:
        date_str: 対象日 YYYYMMDD 形式。

    Returns:
        Markdown 行のリスト。
    """
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
        raise ValueError(
            f"不正な日付形式: {date_str!r}  (YYYYMMDD または YYYY-MM-DD を指定)"
        )

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
                i,
                race.get("venue"),
                race.get("race_number", 0),
                r["score"],
                r["manji_ev"],
                r["alpha_ev"],
                r["consensus"],
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


# ── 厳選レース（ガチ）判定・X/Note 下書き自動生成 ────────────────────


def _clean_consensus(sc: dict[str, Any]) -> int:
    """卍（W-048 破綻）を除いた合意モデル数を返す（最大3）。

    Args:
        sc: _score_race() が返すスコア辞書。

    Returns:
        ALPHA(EV≥1.0) / 本命(conf≥0.5) / Oracle(買い目≥1) のうち成立した数。
    """
    return int(
        ((sc.get("alpha_ev", 0.0) or 0.0) >= 1.0)
        + ((sc.get("honmei_conf", 0.0) or 0.0) >= 0.5)
        + ((sc.get("oracle_count", 0) or 0) >= 1)
    )


def is_gachi_race(sc: dict[str, Any]) -> tuple[bool, list[str]]:
    """スコア辞書から「厳選レース（ガチ）」かどうかを判定する。

    判定条件（いずれか1つを満たせば厳選）:
      1. Alpha-Payout モデルの実払戻 EV が _GACHI_EV_THRESHOLD 以上
      2. 卍を除いたクリーン合意が _GACHI_CLEAN_CONSENSUS 以上（勝負レース判定）

    卍モデルの EV は W-048（キャリブレーション破綻・投資停止中）のため、
    また本命 EV は勝率特化で 6.0 上限に張り付くため、いずれも EV ゲートには使わない。

    Args:
        sc: _score_race() が返すスコア辞書。

    Returns:
        (厳選フラグ, 根拠ラベルのリスト)。厳選でない場合は (False, [])。
    """
    alpha_ev = sc.get("alpha_ev", 0.0) or 0.0
    clean_c = _clean_consensus(sc)

    reasons: list[str] = []
    if alpha_ev >= _GACHI_EV_THRESHOLD:
        reasons.append(
            f"ALPHA実払戻EV={alpha_ev:.2f}（想定回収率{alpha_ev * 100:.0f}%）"
        )
    if clean_c >= _GACHI_CLEAN_CONSENSUS:
        reasons.append(f"{clean_c}モデル合意の勝負レース")

    return (bool(reasons), reasons)


def _gachi_reason_text(sc: dict[str, Any]) -> str:
    """厳選根拠を X 投稿向けの短文（1行）に整形する。

    Args:
        sc: _score_race() が返すスコア辞書。

    Returns:
        期待値の歪み・合意状況を要約した短文。
    """
    alpha_ev = sc.get("alpha_ev", 0.0) or 0.0
    clean_c = _clean_consensus(sc)
    parts: list[str] = []
    if alpha_ev >= _GACHI_EV_THRESHOLD:
        parts.append(f"想定回収率{alpha_ev * 100:.0f}%超の期待値の歪みをAIが検知")
    if clean_c >= _GACHI_CLEAN_CONSENSUS:
        parts.append(f"{clean_c}モデル合意の勝負レース")
    return "／".join(parts) or "AI4モデルが総合的に注目"


def _hashtag(text: str) -> str:
    """ハッシュタグ用に空白・記号を除去した文字列を返す。

    Args:
        text: 元のラベル文字列。

    Returns:
        全角/半角スペース・# を除去した文字列。
    """
    return re.sub(r"[\s#　]+", "", text or "")


def build_hashtags(
    date_str: str, venue: str, race_no: int, race_name: str
) -> list[str]:
    """検索流入最大化のためのハッシュタグ群を動的合成する。

    Args:
        date_str:  YYYYMMDD 形式の対象日。
        venue:     競馬場名（例: "東京"）。
        race_no:   レース番号。
        race_name: レース名。

    Returns:
        ハッシュタグ文字列のリスト（"#..." 形式）。
    """
    y, m, d = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
    tags = [f"#{y}年{m}月{d}日"]
    if venue and race_no:
        tags.append(f"#{_hashtag(venue)}{race_no}R")
    name = _hashtag(race_name)
    # レース名がレース番号だけ（"R11" 等）の場合はタグ化しない
    if name and not re.fullmatch(r"R?\d+", name):
        tags.append(f"#{name}")
    tags += ["#競馬予想", "#UMALogic", "#期待値競馬", "#競馬AI", "#JRA"]
    # 重複除去（順序保持）
    seen: set[str] = set()
    uniq: list[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def _real_race_name(race_name: str | None, race_no: int) -> str:
    """実レース名を返す。空文字や "R6" 等のプレースホルダは "" に正規化する。

    Args:
        race_name: races.race_name の値（空/None あり得る）。
        race_no:   レース番号。

    Returns:
        実名がある場合はその文字列、なければ空文字。
    """
    name = (race_name or "").strip()
    if not name or re.fullmatch(r"R?\d+", name):
        return ""
    return name


def _race_label(venue: str, race_no: int, race_name: str | None) -> str:
    """'東京 11R「日本ダービー」' / 名前なしなら '東京 11R' を返す。

    Args:
        venue:     競馬場名。
        race_no:   レース番号。
        race_name: レース名（空/プレースホルダ可）。

    Returns:
        表示用レースラベル。
    """
    name = _real_race_name(race_name, race_no)
    base = f"{venue} {race_no}R"
    return f"{base}「{name}」" if name else base


def build_x_post(
    conn: sqlite3.Connection,
    sc: dict[str, Any],
    date_str: str,
    note_url: str | None = None,
) -> str:
    """厳選レース1本分の X（Twitter）コピペ投稿テキストを生成する。

    Args:
        conn:     DB コネクション。
        sc:       _score_race() が返すスコア辞書。
        date_str: YYYYMMDD 形式の対象日。
        note_url: 誘導先 note URL（未指定時は _NOTE_MYPAGE_URL）。

    Returns:
        そのまま X に貼り付け可能なテキスト。
    """
    race = _fetch_race(conn, sc["race_id"])
    venue = race.get("venue", "")
    race_no = race.get("race_number", 0)
    race_name = race.get("race_name")
    m, d = int(date_str[4:6]), int(date_str[6:])
    url = note_url or _NOTE_MYPAGE_URL
    reason = _gachi_reason_text(sc)
    tags = " ".join(build_hashtags(date_str, venue, race_no, race_name or ""))
    label = _race_label(venue, race_no, race_name)

    return (
        f"【UMA-Logic 厳選予想】{m}月{d}日 {label}\n"
        f"{reason}。\n"
        f"ガチで狙える厳選の買い目は、Noteにて限定公開中！\n"
        f"👇詳細はこちら\n"
        f"{url}\n"
        f"\n"
        f"{tags}"
    )


def build_single_race_note_md(
    conn: sqlite3.Connection,
    sc: dict[str, Any],
    date_str: str,
    note_url: str | None = None,
) -> str:
    """厳選レース1本分の note 用コピペ Markdown を生成する。

    note エディタに丸ごと貼り付ければ完成する構成:
      タイトル（ハッシュタグ付き）→ 導入文 → 具体的買い目・期待値解説
      （_build_race_section を流用）→ 末尾に X アカウントリンク + 共通ハッシュタグ。

    Args:
        conn:     DB コネクション。
        sc:       _score_race() が返すスコア辞書。
        date_str: YYYYMMDD 形式の対象日。
        note_url: 自記事中で言及する note URL（未指定時は _NOTE_MYPAGE_URL）。

    Returns:
        note 貼り付け用 Markdown 文字列。
    """
    race = _fetch_race(conn, sc["race_id"])
    venue = race.get("venue", "")
    race_no = race.get("race_number", 0)
    race_name = race.get("race_name")
    y, m, d = date_str[:4], int(date_str[4:6]), int(date_str[6:])
    disp = f"{y}年{m}月{d}日"
    reason = _gachi_reason_text(sc)
    tags = " ".join(build_hashtags(date_str, venue, race_no, race_name or ""))
    label = _race_label(venue, race_no, race_name)
    x_url = _X_ACCOUNT_URL

    lines: list[str] = [
        f"# 🏇【UMA-Logic厳選】{disp} {label}｜AI期待値予想",
        "",
        tags,
        "",
        (
            f"本日の **{label}** は、UMA-Logic の AI 4モデルが"
            f"特に注目する『厳選レース』です。  \n"
            f"**{reason}**。期待値（EV）ベースで“ちゃんと買える”買い目を解説します。"
        ),
        "",
        "---",
        "",
    ]

    # 詳細ブロック（モデルシグナル・推奨買い目・出走馬・投資メモ）を流用
    lines += _build_race_section(conn, sc, 1)

    # ── 末尾: X 導線 + 共通ハッシュタグ ──────────────────────────────
    lines += [
        "## 🔔 最新の厳選予想をフォローで受け取る",
        "",
        f"X（旧Twitter）で厳選レースを速報配信中 👉 {x_url}",
        "",
        "> 本記事は AI 分析に基づく参考情報です。馬券は余裕資金の範囲内で自己責任でお楽しみください。",
        "",
        tags,
        "",
    ]
    return "\n".join(lines)


def export_single_race_note(
    conn: sqlite3.Connection,
    sc: dict[str, Any],
    date_str: str,
    note_url: str | None = None,
) -> Path:
    """厳選レースの note Markdown を dist/notes/ に保存しパスを返す。

    ファイル名: dist/notes/[yyyymmdd]_[競馬場]_[R]R_note.md

    Args:
        conn:     DB コネクション。
        sc:       _score_race() が返すスコア辞書。
        date_str: YYYYMMDD 形式の対象日。
        note_url: note URL（未指定時は _NOTE_MYPAGE_URL）。

    Returns:
        保存先ファイルの Path。
    """
    race = _fetch_race(conn, sc["race_id"])
    venue = _hashtag(race.get("venue", "") or "")
    race_no = race.get("race_number", 0)
    md = build_single_race_note_md(conn, sc, date_str, note_url)

    _GACHI_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _GACHI_DIR / f"{date_str}_{venue}_{race_no}R_note.md"
    out_path.write_text(md, encoding="utf-8")
    logger.info("厳選note保存: %s", out_path)
    return out_path


def select_gachi_races(
    conn: sqlite3.Connection,
    date_str: str,
    top_n: int = _GACHI_TOP_N,
) -> list[dict[str, Any]]:
    """指定日の全レースを採点し「厳選レース」上位 top_n 件をスコア降順で返す。

    Args:
        conn:     DB コネクション。
        date_str: "YYYY-MM-DD" または "YYYYMMDD" 形式。
        top_n:    返す最大件数（0以下で無制限）。日次のスパム防止のための厳選上限。

    Returns:
        is_gachi_race() を満たすスコア辞書のリスト（"gachi_reasons" キー付与済み）。
    """
    if len(date_str) == 8 and date_str.isdigit():
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    race_ids = _fetch_race_ids(conn, date_str)
    gachi: list[dict[str, Any]] = []
    for rid in race_ids:
        sc = _score_race(conn, rid)
        ok, reasons = is_gachi_race(sc)
        if ok:
            sc["gachi_reasons"] = reasons
            gachi.append(sc)
    gachi.sort(key=lambda x: x["score"], reverse=True)
    return gachi[:top_n] if top_n and top_n > 0 else gachi


def _send_gachi_discord(
    sc: dict[str, Any],
    x_text: str,
    note_path: Path | None,
) -> bool:
    """厳選レースの X コピペテキストを Discord に通知する。

    DiscordNotifier 未設定・送信失敗時も例外を投げず False を返す。

    Args:
        sc:        スコア辞書（race_id をラベル化に使用）。
        x_text:    X 投稿用テキスト。
        note_path: 生成済み note Markdown のパス（任意）。

    Returns:
        送信成功なら True。
    """
    try:
        from src.notification.discord_notifier import DiscordNotifier

        notifier = DiscordNotifier()
        return notifier.notify_gachi_x_post(
            sc["race_id"],
            x_text,
            note_path=str(note_path) if note_path else None,
        )
    except Exception as exc:  # noqa: BLE001 — 通知失敗は本処理を止めない
        logger.warning("[厳選レース] Discord 通知失敗（続行）: %s", exc)
        return False


def run_gachi_pipeline(
    date_str: str,
    *,
    dry_run: bool = False,
    note_url: str | None = None,
    top_n: int = _GACHI_TOP_N,
) -> list[dict[str, Any]]:
    """指定日の厳選レースを抽出し、X テキスト生成・note md 出力・Discord 通知を行う。

    Args:
        date_str: "YYYYMMDD" または "YYYY-MM-DD" 形式。
        dry_run:  True の場合は Discord 送信を行わず生成のみ実施する。
        note_url: 誘導先 note URL（未指定時は _NOTE_MYPAGE_URL）。
        top_n:    厳選する最大件数（既定 _GACHI_TOP_N）。

    Returns:
        各厳選レースの結果サマリー辞書のリスト。
    """
    ds = date_str.replace("-", "")
    if len(ds) != 8 or not ds.isdigit():
        raise ValueError(f"不正な日付形式: {date_str!r}")
    db_date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"

    conn = _db()
    results: list[dict[str, Any]] = []
    daily_note_bets: list[NoteBet] = []
    try:
        gachi = select_gachi_races(conn, db_date, top_n=top_n)
        logger.info("厳選レース %d 件抽出 (%s, dry_run=%s)", len(gachi), ds, dry_run)
        for sc in gachi:
            x_text = build_x_post(conn, sc, ds, note_url)
            note_path = export_single_race_note(conn, sc, ds, note_url)
            sent = False if dry_run else _send_gachi_discord(sc, x_text, note_path)
            results.append(
                {
                    "race_id": sc["race_id"],
                    "reasons": sc.get("gachi_reasons", []),
                    "x_text": x_text,
                    "note_path": str(note_path),
                    "discord_sent": sent,
                }
            )
            daily_note_bets.extend(_extract_note_bets(sc))
    finally:
        conn.close()

    # 日次下書き生成（実弾処理に影響を与えないよう例外セーフ）
    try:
        allocs = allocate_budget(daily_note_bets)
        write_daily_drafts(daily_note_bets, allocs, date=ds, note_url=note_url)
        logger.info("[daily_drafts] 生成完了 (%s, bets=%d)", ds, len(daily_note_bets))
    except Exception as exc:  # noqa: BLE001
        logger.warning("[daily_drafts] 生成失敗（メイン処理に影響なし）: %s", exc)

    return results


def notify_gachi_for_race(
    race_id: str, *, dry_run: bool = False
) -> dict[str, Any] | None:
    """単一レースを採点し、厳選判定された場合のみ X/Note 下書きと Discord 通知を行う。

    予測パイプライン完走時のフックから呼び出される。判定対象外・例外時は None を返す。

    Args:
        race_id: 対象レース ID（12桁）。先頭8桁が日付として使われる。
        dry_run: True の場合は Discord 送信を行わず生成のみ実施する。

    Returns:
        厳選だった場合は結果サマリー辞書、それ以外は None。
    """
    conn = _db()
    try:
        sc = _score_race(conn, race_id)
        ok, reasons = is_gachi_race(sc)
        if not ok:
            return None
        sc["gachi_reasons"] = reasons

        race = _fetch_race(conn, race_id)
        ds = (race.get("date") or "").replace("-", "")
        if len(ds) != 8:
            logger.warning("[厳選レース] 日付不明のためスキップ: %s", race_id)
            return None

        x_text = build_x_post(conn, sc, ds)
        note_path = export_single_race_note(conn, sc, ds)
        sent = False if dry_run else _send_gachi_discord(sc, x_text, note_path)
        logger.info("[厳選レース] 検出 %s 根拠=%s discord=%s", race_id, reasons, sent)
        return {
            "race_id": race_id,
            "reasons": reasons,
            "x_text": x_text,
            "note_path": str(note_path),
            "discord_sent": sent,
        }
    finally:
        conn.close()


# ── 日次下書き生成 ────────────────────────────────────────────────────


_PAYWALL_FALLBACK_HEADER: str = (
    "> ## 🔒 ここから先は有料エリアです（安全ガード）\n"
    ">\n"
    "> **本コンテンツは有料です。予算配分の詳細は購入後にご覧いただけます。**\n"
    ">\n"
    "---\n"
)


def _ensure_paywall(text: str, allocations_present: bool) -> str:
    """allocations がある場合に 🔒 マーカーが含まれることを保証する安全ガード。

    通常の `generate_note_draft()` コードパスでは必ず 🔒 が挿入されるが、
    将来の変更でペイウォールがスキップされた場合にも有料コンテンツが
    無料公開されないよう先頭にマーカーを挿入して保護する。

    Args:
        text:               生成済みの Markdown テキスト。
        allocations_present: 買い目配分が存在するフラグ（False なら何もしない）。

    Returns:
        ガード適用後のテキスト（通常はそのまま返る）。
    """
    if not allocations_present:
        return text
    if "🔒" in text:
        return text
    logger.warning("[note_draft] ペイウォールマーカー未検出・安全ガード発動")
    return _PAYWALL_FALLBACK_HEADER + text


def generate_note_draft(
    bets: list[NoteBet],
    allocations: list[BetAllocation],
    *,
    date: str | None = None,
    total_budget: int = 10_000,
) -> str:
    """Note/X 向け事前予想下書き（Markdown）を生成して文字列で返す（純関数）。

    Args:
        bets:         買い目リスト（NoteBet）。空の場合はプレースホルダを含む記事を返す。
        allocations:  allocate_budget() の結果。bets と同順で対応する。
        date:         対象日 YYYYMMDD / YYYY-MM-DD（省略時: 本日）。
        total_budget: 記事中に表示する総予算基準（デフォルト ¥10,000）。

    Returns:
        そのまま note.com に貼り付け可能な Markdown 文字列。
    """
    ds = (date or _dt_date.today().strftime("%Y%m%d")).replace("-", "")
    y, m, d = ds[:4], ds[4:6], ds[6:]

    lines: list[str] = [
        f"# 【UMALOGI予想】{y}年{m}月{d}日 厳選勝負レース（推奨予算1万円の資金配分付き）",
        "",
        "> UMALOGIのAIが**期待値（EV）**を基準に厳選した本日の勝負買い目と、",
        f"> ¥{total_budget:,}を効率的に配分するための参考プランです。",
        "> EV &gt; 1.0 は長期的にプラス収支が期待できる「割安なオッズの歪み」です。",
        "",
        "---",
        "",
    ]

    if not allocations:
        lines += [
            "## ⚠️ 本日の予想データ",
            "",
            "本日は予想データが準備中です。開催当日の朝、予想が生成され次第このページに反映されます。",
            "",
        ]
    else:
        # ── 有料ライン ────────────────────────────────────────────────────
        lines += [
            "",
            "---",
            "",
            "> ## 🔒 ここから先は有料エリアです",
            ">",
            "> **詳細な資金配分と推奨買い目は有料コンテンツです。**",
            "> 購入いただくと本日のAI厳選レースの完全予想が閲覧できます。",
            "",
            "---",
            "",
        ]
        lines += [
            f"## 💰 AI推奨 資金配分プラン（基準：¥{total_budget:,}）",
            "",
            "| 券種 | 対象 | 期待値(EV) | 推奨配分額 | 勝負レベル |",
            "|------|------|:--------:|----------:|:---------|",
        ]
        for a in allocations:
            lines.append(
                f"| {a.bet_type} | {a.horse_desc} | {a.ev:.2f}"
                f" | **¥{a.allocated_yen:,}** | {a.label} |"
            )
        lines += [
            "",
            "> 💡 上記は100円単位でのAI推奨比率です。ご自身のバンクロールに合わせて調整ください。",
            "",
            "---",
            "",
            "## 📊 期待値（EV）について",
            "",
            "UMALOGIのAIは統計的に「オッズが割安」な馬券を自動検出します。",
            "EV（期待値）= 予測的中率 × オッズ で計算され、EV &gt; 1.0 なら長期的にプラスが期待できます。",
            "",
        ]

    lines += [
        "---",
        "",
        "*本記事はAI分析に基づく参考情報です。"
        "馬券投票は余裕資金の範囲内で自己責任でお楽しみください。*",
        "",
    ]
    result = "\n".join(lines)
    return _ensure_paywall(result, allocations_present=bool(allocations))


def generate_x_promo_tweet(
    bets: list[NoteBet],
    *,
    note_url: str | None = None,
) -> str:
    """X（Twitter）向け集客プロモツイートを生成する（≤140 文字保証）。

    EVが高い買い目があれば煽り系フレーズを先頭に置き、末尾に note URL を含める。

    Args:
        bets:     買い目リスト（NoteBet）。空でも有効なツイートを返す。
        note_url: 誘導先 note URL（未指定時は _NOTE_MYPAGE_URL）。

    Returns:
        140 文字以内のツイートテキスト。
    """
    url = note_url or _NOTE_MYPAGE_URL
    tags = "#競馬予想 #UMALOGI #期待値競馬 #JRA"

    pos_bets = [b for b in bets if b.ev > 1.0]
    if not pos_bets:
        body = "本日の予想準備中！AI厳選の買い目と¥1万資金配分はNoteにて👇"
    else:
        max_ev = max(b.ev for b in pos_bets)
        if max_ev >= 1.4:
            body = (
                f"🔥本日の勝負レース！期待値{max_ev:.2f}の激熱穴馬を検知。"
                "推奨買い目と¥1万配分はNoteで限定公開👇"
            )
        elif max_ev >= 1.2:
            body = (
                f"📈本日の勝負レース！期待値{max_ev:.2f}の好材料を検知。"
                "推奨買い目と¥1万配分はNoteで公開👇"
            )
        else:
            body = "本日の勝負レース！期待値特大の穴馬を発見。推奨買い目と資金配分はNoteにて👇"

    core = f"\n{url}\n{tags}"
    tweet = body + core
    if len(tweet) > 140:
        available = 140 - len(core)
        tweet = body[: max(available, 0)] + core
    return tweet[:140]


def write_daily_drafts(
    bets: list[NoteBet],
    allocations: list[BetAllocation],
    *,
    date: str | None = None,
    note_url: str | None = None,
    out_dir: Path | None = None,
    total_budget: int = 10_000,
) -> tuple[Path, Path]:
    """Note/X 日次下書きをファイルに書き出し (note_path, x_path) を返す。

    Args:
        bets:         買い目リスト。
        allocations:  allocate_budget() の結果。
        date:         対象日 YYYYMMDD / YYYY-MM-DD（省略時: 本日）。
        note_url:     X ツイートに含める note URL（省略時: 環境変数 NOTE_MYPAGE_URL）。
        out_dir:      出力先ディレクトリ（省略時: _DRAFTS_DIR）。テスト時は tmp_path を渡す。
        total_budget: Note 記事中に表示する総予算基準。

    Returns:
        (note_pre_YYYYMMDD.md の Path, x_pre_YYYYMMDD.txt の Path)
    """
    ds = (date or _dt_date.today().strftime("%Y%m%d")).replace("-", "")
    d = out_dir if out_dir is not None else _DRAFTS_DIR
    d.mkdir(parents=True, exist_ok=True)

    note_md = generate_note_draft(bets, allocations, date=ds, total_budget=total_budget)
    x_text = generate_x_promo_tweet(bets, note_url=note_url)

    note_path = d / f"note_pre_{ds}.md"
    x_path = d / f"x_pre_{ds}.txt"

    note_path.write_text(note_md, encoding="utf-8")
    x_path.write_text(x_text, encoding="utf-8")

    logger.info("[daily_drafts] note=%s x=%s", note_path, x_path)
    return note_path, x_path


def _extract_note_bets(sc: dict[str, Any]) -> list[NoteBet]:
    """スコア辞書（_score_race）から日次下書き用 NoteBet リストを抽出する。

    Alpha-Payout の最高 EV 買い目 1 件と、卍複勝の最高 EV 買い目 1 件（いずれも EV≥1.0）を返す。
    """
    result: list[NoteBet] = []
    for p in sc.get("alpha_preds", [])[:3]:
        ev = float(p.get("expected_value") or 0.0)
        if ev >= 1.0:
            desc = _fmt_combo(p.get("combos", []), p["bet_type"])
            result.append(NoteBet(bet_type=p["bet_type"], horse_desc=desc, ev=ev))
            break
    for p in sc.get("manji_preds", []):
        if p.get("bet_type") == "複勝":
            ev = float(p.get("expected_value") or 0.0)
            if ev >= 1.0:
                desc = _fmt_combo(p.get("combos", []), "複勝")
                result.append(NoteBet(bet_type="複勝", horse_desc=desc, ev=ev))
            break
    return result


# ── CLI ─────────────────────────────────────────────────────────


def _cli() -> None:
    """note 記事生成ツールの CLI エントリーポイント。

    --date で対象日を指定し、--top で最大推奨レース数を指定できる。
    --stdout を指定すると Markdown をターミナルにも出力する。
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )
    ap = argparse.ArgumentParser(description="note 予想記事自動生成エンジン")
    ap.add_argument(
        "--date",
        default=None,
        help="対象日 YYYYMMDD（省略時=本日）",
    )
    ap.add_argument(
        "--top",
        type=int,
        default=5,
        help="最大推奨レース数（デフォルト=5）",
    )
    ap.add_argument(
        "--stdout",
        action="store_true",
        help="標準出力にも記事を表示する",
    )
    ap.add_argument(
        "--gachi",
        action="store_true",
        help="厳選レースの X コピペ + note md を生成（Discord 通知あり）",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="--gachi 時に Discord 送信せず生成内容のみ表示する",
    )
    args = ap.parse_args()

    date_str = args.date or _dt_date.today().strftime("%Y%m%d")

    # ── 厳選レース（X / note 下書き）モード ──────────────────────────
    if args.gachi:
        results = run_gachi_pipeline(date_str, dry_run=args.dry_run)
        if not results:
            print(f"\n⚠️ {date_str}: 厳選レースなし（予測データ未生成 or 条件未達）")
            return
        print(f"\n✅ 厳選レース {len(results)} 件 （dry_run={args.dry_run}）\n")
        for i, r in enumerate(results, 1):
            print(f"━━━━━━ 厳選 {i}/{len(results)} ━━━━━━")
            print(f"race_id : {r['race_id']}")
            print(f"根拠     : {' / '.join(r['reasons'])}")
            print(f"note md  : {r['note_path']}")
            print(f"Discord  : {'送信' if r['discord_sent'] else '未送信(dry_run)'}")
            print("--- X コピペ ---")
            print(r["x_text"])
            print()
        return

    generate(
        date_str=date_str, top_n=args.top, stdout=args.stdout
    )  # 生成副作用のみ（md未使用）
    out_path = _OUT_DIR / f"{date_str.replace('-', '')}_recommendations.md"
    print(f"\n✅ 生成完了 → {out_path}")


if __name__ == "__main__":
    _cli()
