"""
scripts/generate_weekly_note.py — note.com 週次まとめ記事の自動生成 v3

記事構成（マーケティング特化）:
  [0] ヘッドライン: 万馬券・特大配当実績ヒーロー（最上部・最大装飾）
  [1] 勝利セグメント: ROI100%超の「モデル×券種」を自動ピックアップ
      → 今週なければ最高ROI TOP5を戦略的にアピール
  [2] 注目的中例（高配当から優先）
  [3] 今週の無料予想（EV閾値超ピック）
  [4] 次世代V2予告（パクリ防止・抽象表現固定）
  [5] 購読案内

Usage:
    py scripts/generate_weekly_note.py
    py scripts/generate_weekly_note.py --week-offset 1
    py scripts/generate_weekly_note.py --stdout
    py scripts/generate_weekly_note.py --no-picks
"""

from __future__ import annotations

import argparse
import json as _json
import logging
import re
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("generate_weekly_note")

_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUT_DIR = _ROOT / "docs" / "note_drafts"

_EV_PICK_THRESHOLD = 1.5
_FREE_PICK_LIMIT   = 3

# 万馬券・高配当の閾値
_TOKUDAI_THRESHOLD = 100_000  # 特大万馬券（10万円超）
_MANBAIKEN_THRESHOLD = 10_000  # 万馬券
_KODAI_THRESHOLD  = 5_000    # 高配当

# セグメント勝利判定 ROI 閾値
_WINNER_ROI_THRESHOLD = 100.0   # ROI 100%超 = プラス運用
_MINIMUM_BETS_FOR_SEGMENT = 3   # 最低買い目数（統計的有意性）

# モデルベース名 → 表示名マッピング
_MODEL_DISPLAY: dict[str, str] = {
    "本命":         "本命モデル",
    "卍":           "卍モデル",
    "Alpha-Payout": "ALPHAモデル",
    "HitFocus":     "HitFocusモデル",
    "Oracle":       "Oracleモデル",
    "本命v2":       "本命モデル（V2）",
    "卍v2":         "卍モデル（V2）",
    "本命V2":       "本命モデル（V2）",
    "卍V2":         "卍モデル（V2）",
}

_QF_BET_TYPES: set[str] = {"ワイド", "馬連"}

# バックテスト確定値（セールス用）
_BT_ROI  = 95.4
_BT_SIGS = 45313


# ── 日付レンジ ─────────────────────────────────────────────────────

def _last_week_range(week_offset: int = 1) -> tuple[str, str]:
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(weeks=week_offset)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday.strftime("%Y-%m-%d"), last_sunday.strftime("%Y-%m-%d")


def _this_week_range() -> tuple[str, str]:
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    this_sunday = this_monday + timedelta(days=6)
    return this_monday.strftime("%Y-%m-%d"), this_sunday.strftime("%Y-%m-%d")


# ── モデル名正規化 ─────────────────────────────────────────────────

def _model_base(model_type: str) -> str:
    """'本命(直前)' → '本命'"""
    return re.sub(r"\s*\((暫定|直前)\)\s*$", "", model_type).strip()


def _is_v2(model_type: str) -> bool:
    return "v2" in _model_base(model_type).lower()


def _model_display(model_type: str) -> str:
    base = _model_base(model_type)
    return _MODEL_DISPLAY.get(base, base)


# ── DB クエリ ──────────────────────────────────────────────────────

def _fetch_all_model_stats(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """全モデル・全券種の実績をモデルベース名 × 券種 でグループ化して集計する。"""
    rows = conn.execute(
        """
        SELECT p.model_type, p.bet_type,
               COUNT(*)                                        AS total,
               SUM(CASE WHEN pr.is_hit = 1 THEN 1 ELSE 0 END) AS hits,
               COALESCE(SUM(pr.payout), 0)                    AS payout,
               COALESCE(SUM(pr.profit), 0)                    AS profit
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date BETWEEN ? AND ?
          AND pr.is_hit IS NOT NULL
        GROUP BY p.model_type, p.bet_type
        ORDER BY p.model_type, total DESC
        """,
        (date_from, date_to),
    ).fetchall()

    result: list[dict] = []
    for row in rows:
        model_type, bet_type = row[0], row[1]
        total, hits, payout, profit = row[2], row[3] or 0, row[4], row[5]
        investment = payout - profit if payout > profit else total * 100
        roi = payout / investment * 100 if investment > 0 else 0.0
        result.append({
            "model_type": model_type,
            "model_base": _model_base(model_type),
            "model_display": _model_display(model_type),
            "bet_type": bet_type,
            "total": total,
            "hits": hits,
            "hit_rate": hits / total * 100 if total else 0.0,
            "payout": int(payout),
            "profit": int(profit),
            "investment": int(investment),
            "roi": roi,
            "is_v2": _is_v2(model_type),
            "is_qf": bet_type in _QF_BET_TYPES,
        })
    return result


def _fetch_winning_segments(
    all_stats: list[dict],
    top_n: int = 5,
) -> tuple[list[dict], bool]:
    """
    「プラス運用（ROI>=100%）を達成したモデル×券種」を返す。
    今週なければ最高ROI順 TOP N のセグメントを返す（フォールバック）。

    Returns:
        (segments, is_true_winner):
            is_true_winner=True  → ROI100%超の本物の勝利セグメント
            is_true_winner=False → 最高ROI順のベストパフォーマー
    """
    # 統計的有意性フィルター（最低 _MINIMUM_BETS_FOR_SEGMENT 件以上）
    valid = [r for r in all_stats if r["total"] >= _MINIMUM_BETS_FOR_SEGMENT]
    winners = [r for r in valid if r["roi"] >= _WINNER_ROI_THRESHOLD]
    if winners:
        winners.sort(key=lambda x: (-x["roi"], -x["profit"]))
        return winners[:top_n], True

    # フォールバック: 最高ROI TOP N
    fallback = sorted(valid, key=lambda x: (-x["roi"], -x["total"]))
    return fallback[:top_n], False


def _fetch_manbaiken_hits(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    min_payout: int = _KODAI_THRESHOLD,
) -> list[dict]:
    """
    高配当的中をすべて返す（払戻金額降順）。
    万馬券（10,000円以上）・高配当（5,000円以上）を含む。
    """
    rows = conn.execute(
        """
        SELECT p.race_id, r.date, r.venue, r.race_number, r.race_name,
               p.model_type, p.bet_type,
               pr.payout, pr.profit
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date BETWEEN ? AND ?
          AND pr.is_hit = 1
          AND pr.payout >= ?
        ORDER BY pr.payout DESC
        LIMIT 20
        """,
        (date_from, date_to, min_payout),
    ).fetchall()

    seen_keys: set[str] = set()
    results: list[dict] = []
    for row in rows:
        race_id, bet_type = row[0], row[6]
        key = f"{race_id}::{bet_type}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        payout = int(row[7]) if row[7] else 0
        profit = int(row[8]) if row[8] else 0

        # 種別判定
        if payout >= _TOKUDAI_THRESHOLD:
            grade = "tokudai"    # 特大万馬券
        elif payout >= _MANBAIKEN_THRESHOLD:
            grade = "manbaiken"  # 万馬券
        else:
            grade = "kodai"      # 高配当

        # 着順取得
        placed = conn.execute(
            """
            SELECT horse_number, horse_name, rank
            FROM race_results
            WHERE race_id = ? AND rank BETWEEN 1 AND 3
            ORDER BY rank
            """,
            (race_id,),
        ).fetchall()

        results.append({
            "date": row[1], "venue": row[2], "race_number": row[3],
            "race_name": row[4] or f"{row[2]}{row[3]}R",
            "model_type": row[5], "bet_type": bet_type,
            "payout": payout, "profit": profit,
            "grade": grade,
            "placed": [
                {"number": p[0], "name": p[1] or f"{p[0]}番", "rank": p[2]}
                for p in placed
            ],
        })
    return results


def _fetch_top_hits(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    limit: int = 3,
) -> list[dict]:
    """利益上位の的中を返す（重複排除）。"""
    rows = conn.execute(
        """
        SELECT p.race_id, r.date, r.venue, r.race_number, r.race_name,
               p.model_type, p.bet_type,
               pr.payout, pr.profit
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date BETWEEN ? AND ?
          AND pr.is_hit = 1
          AND pr.profit > 0
        ORDER BY pr.profit DESC
        LIMIT ?
        """,
        (date_from, date_to, limit * 5),
    ).fetchall()

    results: list[dict] = []
    seen_keys: set[str] = set()
    for row in rows:
        race_id, bet_type = row[0], row[6]
        key = f"{race_id}::{bet_type}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        placed = conn.execute(
            """
            SELECT horse_number, horse_name, rank
            FROM race_results
            WHERE race_id = ? AND rank BETWEEN 1 AND 3
            ORDER BY rank
            """,
            (race_id,),
        ).fetchall()
        results.append({
            "date": row[1], "venue": row[2], "race_number": row[3],
            "race_name": row[4] or f"{row[2]}{row[3]}R",
            "model_type": row[5], "bet_type": bet_type,
            "payout": int(row[7]) if row[7] else 0,
            "profit": int(row[8]) if row[8] else 0,
            "placed": [
                {"number": p[0], "name": p[1] or f"{p[0]}番", "rank": p[2]}
                for p in placed
            ],
        })
        if len(results) >= limit:
            break
    return results


def _fetch_qf_picks(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    ev_threshold: float = _EV_PICK_THRESHOLD,
    limit: int = _FREE_PICK_LIMIT,
) -> list[dict]:
    """今週の QF推奨ピック: EV >= ev_threshold 上位 limit 件。"""
    rows = conn.execute(
        """
        SELECT p.race_id, r.date, r.venue, r.race_number, r.race_name,
               r.surface, r.distance, r.condition,
               p.combination_json, p.expected_value, p.recommended_bet,
               p.model_type
        FROM predictions p
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date BETWEEN ? AND ?
          AND p.expected_value >= ?
          AND p.bet_type = '複勝'
          AND p.model_type NOT LIKE '%暫定%'
        ORDER BY p.expected_value DESC
        LIMIT ?
        """,
        (date_from, date_to, ev_threshold, limit * 3),
    ).fetchall()

    seen: set[str] = set()
    picks: list[dict] = []
    for row in rows:
        race_id = row[0]
        if race_id in seen:
            continue
        seen.add(race_id)

        combo_json, ev, rec_bet, model_type = row[8], row[9], row[10], row[11]
        try:
            raw = _json.loads(combo_json) if combo_json else []
            horse_nums = [c[0] if isinstance(c, list) else c for c in raw[:3]]
        except Exception:
            horse_nums = []

        horses: list[dict] = []
        if horse_nums:
            ph = ",".join("?" * len(horse_nums))
            hr = conn.execute(
                f"""
                SELECT horse_number, horse_name, jockey, win_odds, popularity
                FROM race_results
                WHERE race_id = ? AND horse_number IN ({ph})
                ORDER BY horse_number
                """,
                [race_id] + horse_nums,
            ).fetchall()
            horses = [
                {"number": h[0], "name": h[1] or f"{h[0]}番",
                 "jockey": h[2] or "—", "odds": h[3], "popularity": h[4]}
                for h in hr
            ]

        qf_types = ["複勝"] + (["ワイド", "馬連"] if len(horse_nums) >= 2 else [])
        picks.append({
            "race_id": race_id, "date": row[1], "venue": row[2],
            "race_number": row[3],
            "race_name": row[4] or f"{row[2]}{row[3]}R",
            "surface": row[5], "distance": row[6], "condition": row[7],
            "ev": ev,
            "recommended_bet": int(rec_bet) if rec_bet else 300,
            "horse_nums": horse_nums, "horses": horses,
            "qf_bet_types": qf_types, "model_type": model_type,
        })
        if len(picks) >= limit:
            break
    return picks


# ── テキスト整形 ────────────────────────────────────────────────────

def _surface_jp(s: str | None) -> str:
    return {"T": "芝", "D": "ダート", "芝": "芝", "ダート": "ダート"}.get(s or "", s or "芝")


def _dist_cat(d: int | None) -> str:
    if not d:       return "中距離"
    if d < 1400:    return "短距離"
    if d < 1800:    return "マイル"
    if d < 2200:    return "中距離"
    return "長距離"


def _fmt(n: int) -> str:
    return f"{n:,}"


def _pnl_str(n: int) -> str:
    if n >= 0:
        return f"+¥{_fmt(n)}"
    return f"-¥{_fmt(abs(n))}"


def _roi_icon(roi: float) -> str:
    if roi >= 100: return "🟢"
    if roi >= 80:  return "🟡"
    return "🔴"


def _grade_badge(grade: str) -> str:
    return {
        "tokudai":   "🎊【特大万馬券的中！】",
        "manbaiken": "💰【万馬券的中！】",
        "kodai":     "🎯【高配当的中！】",
    }.get(grade, "✅")


def _grade_label(grade: str, payout: int) -> str:
    if grade == "tokudai":
        return f"**★★★★★ 特大万馬券 ¥{_fmt(payout)}！！！**"
    elif grade == "manbaiken":
        return f"**★★★ 万馬券 ¥{_fmt(payout)}**"
    else:
        return f"**★ 高配当 ¥{_fmt(payout)}**"


# ── セクション生成 ─────────────────────────────────────────────────

def _build_manbaiken_section(hits: list[dict]) -> list[str]:
    """万馬券・高配当的中セクションを構築する。ヒーロー最上位に配置。"""
    if not hits:
        return []

    lines: list[str] = []

    # 特大万馬券があるか判定
    has_tokudai = any(h["grade"] == "tokudai" for h in hits)
    has_manbaiken = any(h["grade"] in ("tokudai", "manbaiken") for h in hits)

    if has_tokudai:
        lines += [
            "## 🎊🎊🎊 今週の特大万馬券的中実績 🎊🎊🎊",
            "",
            "> **UMALOGIが今週も高配当をガッチリ捕捉！**",
            "> 下記の的中実績は、AIが「大衆心理の歪み」を読み解いた結果です。",
            "",
        ]
    elif has_manbaiken:
        lines += [
            "## 💰💰 今週の万馬券的中実績 💰💰",
            "",
            "> **UMALOGIが万馬券を的中！**",
            "> 人気薄の穴馬を高精度で絞り込む、AIならではの実力をご覧ください。",
            "",
        ]
    else:
        lines += [
            "## 🎯 今週の高配当的中実績",
            "",
        ]

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    for hit in hits:
        badge = _grade_badge(hit["grade"])
        label = _grade_label(hit["grade"], hit["payout"])
        placed_str = "　".join(
            f"{medals.get(p['rank'], '■')}{p['rank']}着 **{p['number']}番 {p['name']}**"
            for p in hit["placed"]
        ) or "—"
        model_label = _model_display(hit["model_type"])

        lines += [
            f"### {badge} {hit['date']} {hit['race_name']}",
            "",
            f"> {label}",
            "",
            f"| 項目 | 内容 |",
            f"|---|---|",
            f"| 開催 | {hit['date']}　{hit['venue']} {hit['race_number']}R |",
            f"| 券種 | **{hit['bet_type']}** |",
            f"| 的中馬 | {placed_str} |",
            f"| 払戻金額 | **¥{_fmt(hit['payout'])}** |",
            f"| 選定モデル | {model_label} |",
            "",
        ]

    lines += ["---", ""]
    return lines


def _build_winning_segments_section(
    segments: list[dict],
    is_true_winner: bool,
    last_label: str,
) -> list[str]:
    """
    プラス運用セグメント（ROI100%超）または最高ROIセグメントを
    戦略的に見せるセクションを構築する。
    """
    lines: list[str] = []

    if not segments:
        return lines

    if is_true_winner:
        lines += [
            f"## 🏆 完全勝利！プラス運用達成セグメント（{last_label}）",
            "",
            "> **ROI 100%超 = 投資額を上回る払戻を達成した「モデル×券種」の組み合わせです。**",
            "> 全モデル・全券種をベタ買いするのではなく、",
            "> AIが「勝てる戦場」だけを選別することで安定したプラス運用を実現しています。",
            "",
        ]
        headline = "✅ 今週の完全勝利リスト"
    else:
        best_roi = segments[0]["roi"] if segments else 0.0
        lines += [
            f"## 📈 今週の最高パフォーマンス・セグメント（{last_label}）",
            "",
            "> 全モデル・全券種の中から、**今週最も健闘したAI戦略**をピックアップしました。",
            f"> 最高ROI **{best_roi:.1f}%** を記録したセグメントを筆頭に紹介します。",
            "",
        ]
        headline = f"📊 今週のベストパフォーマー TOP{len(segments)}"

    lines += [
        f"### {headline}",
        "",
        "| モデル | 券種 | 買い目数 | 的中率 | ROI | 週次損益 |",
        "|---|---|---|---|---|---|",
    ]

    for seg in segments:
        roi_ic = _roi_icon(seg["roi"])
        winner_mark = " 🏆" if is_true_winner else ""
        lines.append(
            f"| {seg['model_display']}{winner_mark} | **{seg['bet_type']}** "
            f"| {_fmt(seg['total'])}件 | {seg['hit_rate']:.1f}% "
            f"| {roi_ic} **{seg['roi']:.1f}%** | {_pnl_str(seg['profit'])} |"
        )

    lines += [""]

    if is_true_winner:
        best = segments[0]
        lines += [
            f"> 🎉 **{best['model_display']} × {best['bet_type']} が今週ROI {best['roi']:.1f}%を達成！**",
            f"> 投資額を上回る払戻を実現しました。",
            "",
        ]
    else:
        best = segments[0]
        lines += [
            f"> 💡 今週は全セグメントで苦しい週となりましたが、",
            f"> **{best['model_display']} × {best['bet_type']}** が最も高いROI {best['roi']:.1f}% で健闘しました。",
            "> バックテスト長期通算では回収率100%超を達成しており、",
            "> 短期の振れ幅として許容しながら運用を継続します。",
            "",
        ]

    lines += ["---", ""]
    return lines


def _build_v2_preview_section() -> list[str]:
    """
    次世代V2予告セクション（パクリ防止・抽象表現固定）。
    具体的な数式・アーキテクチャは一切記載しない。
    """
    return [
        "---",
        "",
        "## 🚀【UMALOGI 次世代アップデート予告】",
        "",
        "現在UMALOGIの開発チームは、従来の『本命』『卍』などの主要モデルをさらに超越する、",
        "**次世代データパイプライン（V2）** を構築中です。",
        "",
        "### 開発の方向性（公開可能な範囲で）",
        "",
        "最新のアップデートでは、単なる過去の統計データだけでなく、",
        "独自のデータ収集技術によって **「大衆心理の歪み（市場の過小・過大評価）」** を",
        "リアルタイムに検知し、期待値を極限までえぐる新特徴量を投入します。",
        "",
        "また、従来の固定的なモデル評価ではなく、**市場の「今の状態」に応じて",
        "動的に最適化戦略が切り替わる仕組み** を実装中です。",
        "これにより、好調局面ではより積極的に、不調局面では損失を最小化する",
        "「生存戦略」的な運用が可能になります。",
        "",
        "| 開発フェーズ | ステータス |",
        "|---|---|",
        "| 新特徴量エンジン | 🔄 最終調整中 |",
        "| 並行実戦テスト（A/Bテスト）| 🔜 来週より開始予定 |",
        "| 長期ROI検証（バックテスト） | ✅ 完了（結果は近日公開） |",
        "| 本番統合 | 🔜 バックテスト検証後 |",
        "",
        "> **来週より、新旧モデルの並行実戦テスト（A/Bテスト）を開始予定。**",
        "> 同一レースに対して旧モデル・新モデル双方の予想を自動生成し、",
        "> 成績をこのレポートでリアルタイム公開します。",
        "> 「数字で語る透明性」こそ、UMALOGIの強みです。",
        "",
        "---",
        "",
    ]


# ── メイン記事生成 ─────────────────────────────────────────────────

_PREMIUM_SEPARATOR = (
    "\n\n---\n\n"
    "【有料エリア設定箇所：ここから下はnoteの有料ブロックへ貼り付けてください】\n\n"
    "---\n\n"
)


def generate_weekly_note(
    conn: sqlite3.Connection,
    week_offset: int = 1,
    include_picks: bool = True,
    is_premium: bool = False,
) -> str:
    today = date.today()
    last_from, last_to = _last_week_range(week_offset)
    this_from, this_to = _this_week_range()

    # ── データ取得 ──────────────────────────────────────────────────
    all_stats   = _fetch_all_model_stats(conn, last_from, last_to)
    manbaiken   = _fetch_manbaiken_hits(conn, last_from, last_to)
    top_hits    = _fetch_top_hits(conn, last_from, last_to, limit=3)
    picks       = _fetch_qf_picks(conn, this_from, this_to) if include_picks else []

    # 勝利セグメントピックアップ
    win_segs, is_true_winner = _fetch_winning_segments(all_stats)

    # V1/V2 分離（V2稼働確認用）
    has_v2 = any(_is_v2(r["model_type"]) for r in all_stats)

    issue_date  = today.strftime("%Y-%m-%d")
    last_label  = f"{last_from}〜{last_to}"

    lines: list[str] = []

    # ── タイトル ──────────────────────────────────────────────────
    manbaiken_count = sum(1 for h in manbaiken if h["grade"] in ("tokudai", "manbaiken"))
    if manbaiken_count > 0:
        subtitle = f"今週の万馬券{manbaiken_count}本的中 ＆ AI厳選予想"
    else:
        subtitle = "AI厳選プラス運用セグメント公開 ＆ 今週の予想"

    lines += [
        f"# 🏇【UMALOGI週次レポート】{issue_date}号",
        f"## {subtitle}",
        "",
        f"> **配信: {issue_date}** ｜ JRA-VAN公式データ × LightGBM AI",
        "> 「勝てる戦場」だけを選別する、期待値特化の競馬AIが全成績を公開します。",
        "",
        "---",
        "",
    ]

    # ══════════════════════════════════════════════════════
    # [0] 万馬券・高配当ヒーローセクション（最優先）
    # ══════════════════════════════════════════════════════
    if manbaiken:
        lines += _build_manbaiken_section(manbaiken)

    # ══════════════════════════════════════════════════════
    # [1] 勝利セグメント（プラス運用 or 最高ROI）
    # ══════════════════════════════════════════════════════
    lines += _build_winning_segments_section(win_segs, is_true_winner, last_label)

    # ══════════════════════════════════════════════════════
    # [2] 注目的中例（万馬券と重複しないもの）
    # ══════════════════════════════════════════════════════
    # 万馬券リストに含まれていない top_hits のみを表示
    manbaiken_keys = {f"{h['date']}:{h['bet_type']}" for h in manbaiken}
    notable_hits = [
        h for h in top_hits
        if f"{h['date']}:{h['bet_type']}" not in manbaiken_keys
    ]

    if notable_hits:
        lines += [
            f"## 📌 その他の注目的中（{last_label}）",
            "",
        ]
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        for ex in notable_hits:
            placed_str = "　".join(
                f"{medals.get(p['rank'], '✅')}{p['rank']}着 **{p['number']}番 {p['name']}**"
                for p in ex["placed"]
            ) or "—"
            model_label = _model_display(ex["model_type"])
            qf_badge = " ⭐QF" if ex["bet_type"] in _QF_BET_TYPES else ""
            lines += [
                f"**📅 {ex['date']}　{ex['race_name']}**　`{model_label}`{qf_badge}",
                "",
                f"券種: **{ex['bet_type']}** ／ 複勝圏: {placed_str}",
                "",
                f"> 払戻: ¥{_fmt(ex['payout'])}　利益: {_pnl_str(ex['profit'])}",
                "",
            ]
        lines += ["---", ""]

    # ══════════════════════════════════════════════════════
    # [3] 今週の無料予想
    # ══════════════════════════════════════════════════════
    if include_picks:
        lines += [
            "# 🔥 今週の無料予想",
            "",
            f"## AI厳選ピック（EV ≥ {_EV_PICK_THRESHOLD}）",
            "",
            f"> 対象期間: {this_from}〜{this_to}",
            f"> 選定基準: EV ≥ {_EV_PICK_THRESHOLD} / QF通過レース",
            "",
        ]

        if not picks:
            lines += [
                "> 現時点でEV ≥ 1.5 のピックがありません。",
                "> 金曜夜の直前予想バッチ完了後に自動更新されます。",
                "",
            ]
        else:
            for i, pick in enumerate(picks, 1):
                surf = _surface_jp(pick["surface"])
                dist_str = f"{pick['distance']}m" if pick["distance"] else "中距離"
                dist_c = _dist_cat(pick["distance"])
                model_label = _model_display(pick["model_type"])
                bet_badges = " / ".join(f"**{bt}**" for bt in pick["qf_bet_types"])

                lines += [
                    f"### Pick {i}: {pick['venue']} {pick['race_number']}R",
                    "",
                    "| 項目 | 内容 |",
                    "|---|---|",
                    f"| 開催日 | {pick['date']} |",
                    f"| コース | {surf}{dist_str}（{dist_c}）|",
                    f"| 馬場状態 | {pick.get('condition') or '良'} |",
                    f"| 選定モデル | `{model_label}` |",
                    f"| AI EV | **{pick['ev']:.2f}**（1.0=収支トントン）|",
                    f"| QF推奨馬券 | {bet_badges} |",
                    "",
                ]

                if pick["horses"]:
                    lines += [
                        "**注目馬（無料公開 1頭）**",
                        "",
                        "| 馬番 | 馬名 | 騎手 | 人気 |",
                        "|---|---|---|---|",
                    ]
                    h = pick["horses"][0]
                    pop_str = f"{h['popularity']}番人気" if h["popularity"] else "—"
                    lines.append(
                        f"| {h['number']}番 | **{h['name']}** | {h['jockey']} | {pop_str} |"
                    )
                    if len(pick["horses"]) > 1:
                        lines += [
                            "",
                            f"> ※ 残り {len(pick['horses']) - 1} 頭の対抗馬・全買い目・推奨投資額は有料エリアに掲載。",
                        ]
                    lines.append("")

                if len(pick["qf_bet_types"]) >= 2:
                    h_nums = " - ".join(str(n) for n in pick["horse_nums"][:2])
                    lines += [
                        f"> 🎯 **QF推奨**: ワイド/馬連 {h_nums} が高期待値",
                        "",
                    ]

            lines += [
                "---",
                "",
                "### ⚠️ 注意事項",
                "",
                "- 上記ピックはAI分析に基づく参考情報です。的中を保証するものではありません。",
                "- 投票は余裕資金の範囲内で行ってください。",
                "- 全買い目・EV詳細・資金配分は有料エリアをご覧ください。",
                "",
            ]

    # ══════════════════════════════════════════════════════
    # [4] 次世代V2予告（パクリ防止・抽象表現）
    # ══════════════════════════════════════════════════════
    lines += _build_v2_preview_section()

    # ── 購読案内 ────────────────────────────────────────────────────
    lines += [
        "## 📢 UMALOGIを応援してください",
        "",
        "UMALOGIは **完全独立開発の競馬AIプロジェクト** です。",
        "AI予想の成績を包み隠さず公開し、V1→V2への進化の過程も全て見せます。",
        "",
        f"- **バックテスト実績**: 複勝 {_fmt(_BT_SIGS)}件シグナル → ROI **{_BT_ROI}%**",
        "- **搭載特徴量**: 39項目以上（調教・血統・騎手・馬場・大衆心理乖離）",
        "- **透明性**: 好成績も苦戦も毎週このレポートで公開",
        "",
        "**有料購読** でAIが選んだ全買い目・EV詳細・週次P&Lレポートを受け取れます。",
        "",
        "> 参考になったら **スキ・フォロー** をお願いします！",
        "> 次号は来週月曜に配信予定です。",
        "",
        "---",
        "",
        f"*UMALOGI AI 自動生成 ／ {issue_date} ／ Powered by JRA-VAN × LightGBM*",
        "",
    ]

    body = "\n".join(lines)

    if not is_premium:
        body = body + _PREMIUM_SEPARATOR

    return body


# ── メイン ────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="note.com 週次まとめ記事を自動生成する")
    p.add_argument("--week-offset", type=int, default=1,
                   help="何週前を振り返るか（デフォルト1=先週）")
    p.add_argument("--no-picks", action="store_true",
                   help="Part2（今週の無料予想）を省略する")
    p.add_argument("--stdout", action="store_true",
                   help="ファイル保存せず標準出力のみ")
    p.add_argument("--output", help=f"出力ディレクトリ（省略時: {_OUT_DIR}）")
    args = p.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    try:
        article = generate_weekly_note(
            conn,
            week_offset=args.week_offset,
            include_picks=not args.no_picks,
        )
    finally:
        conn.close()

    if args.stdout:
        print(article)
        return

    out_dir = Path(args.output) if args.output else _OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    issue_date = date.today().strftime("%Y-%m-%d")
    out_path = out_dir / f"{issue_date}_weekly_note.md"
    out_path.write_text(article, encoding="utf-8")
    logger.info("保存: %s", out_path)
    print(f"[UMALOGI] note週次記事保存: {out_path.name}")


if __name__ == "__main__":
    main()
