"""
UMALOGI — プロ投資家ダッシュボード (Streamlit)

起動:
  streamlit run web_streamlit/app.py

アーキテクチャ:
  JSON を一切使わず SQLite (data/umalogi.db) へ直接クエリを投げる設計。
  @st.cache_data でクエリ結果をキャッシュ、不要な再描画を防ぐ。

テストデータ除外フィルタ:
  - DATE(created_at) BETWEEN '2020-01-01' AND '2030-12-31'
  - expected_value > 0
  - SUBSTR(race_id,5,2) BETWEEN '01' AND '10'（JRA正規会場コードのみ）
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── プロジェクトルートを sys.path に追加 ────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import get_db_path

# ── 定数 ────────────────────────────────────────────────────────────
_JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}
_KELLY_BASE = 1_000_000   # Kelly 基準資金（100万円）
_VELOCITY_THRESHOLD = 0.05

# テストデータ除外フィルタ（SQL WHERE 句で共通使用）
_DATE_FILTER = "DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'"
_EV_FILTER   = "p.expected_value > 0"
_VENUE_FILTER = "SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'"

# ── DB 接続ヘルパー ─────────────────────────────────────────────────

@st.cache_resource
def _get_conn() -> sqlite3.Connection:
    """シングルトン接続（read-only）を返す。"""
    path = str(get_db_path())
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _q(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """クエリを実行して dict のリストを返す。"""
    conn = _get_conn()
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """クエリを実行して DataFrame を返す。"""
    rows = _q(sql, params)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _safe_race_name(name: Any) -> str:
    """DB から取得した race_name が文字化けしていない場合のみ返す。"""
    if not name:
        return ""
    s = str(name)
    # U+FFFD（UTF-8 デコード失敗の代替文字）が含まれる場合は文字化けと判断
    if "\ufffd" in s or not any(c.isalpha() for c in s):
        return ""
    return s.strip()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  データ取得レイヤー                                               ║
# ╚══════════════════════════════════════════════════════════════════╝

@st.cache_data(ttl=300)
def fetch_available_dates() -> list[str]:
    """テストデータを除外した予想生成日（YYYY-MM-DD）を降順で返す。"""
    rows = _q(f"""
        SELECT DISTINCT DATE(p.created_at) AS d
        FROM predictions p
        WHERE p.created_at IS NOT NULL
          AND {_DATE_FILTER}
          AND {_EV_FILTER}
          AND {_VENUE_FILTER}
        ORDER BY d DESC
    """)
    return [r["d"] for r in rows if r["d"]]


@st.cache_data(ttl=300)
def fetch_venues_for_date(date_str: str) -> list[tuple[str, str]]:
    """指定日の会場コードと名前リスト (code, name) を返す。"""
    rows = _q("""
        SELECT DISTINCT SUBSTR(p.race_id,5,2) AS vc
        FROM predictions p
        WHERE DATE(p.created_at) = ?
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          AND p.expected_value > 0
        ORDER BY vc
    """, (date_str,))
    return [(r["vc"], _JYO.get(r["vc"], r["vc"])) for r in rows]


@st.cache_data(ttl=300)
def fetch_races_for_venue(date_str: str, venue_code: str) -> pd.DataFrame:
    """指定日・会場のレース一覧（race_id, race_number, race_name）を返す。"""
    return _df("""
        SELECT DISTINCT
            p.race_id,
            CAST(SUBSTR(p.race_id,11,2) AS INTEGER) AS race_number,
            r.race_name
        FROM predictions p
        LEFT JOIN races r ON r.race_id = p.race_id
        WHERE DATE(p.created_at) = ?
          AND SUBSTR(p.race_id,5,2) = ?
          AND p.expected_value > 0
        ORDER BY race_number
    """, (date_str, venue_code))


@st.cache_data(ttl=60)
def fetch_race_predictions(race_id: str, kind: str = "any") -> pd.DataFrame:
    """
    指定レースの予想データ（1行=1頭）を返す。

    Args:
        race_id: レース ID
        kind: "暫定" → model_type LIKE '%暫定%'
              "直前" → model_type LIKE '%直前%'
              "any"  → 直前を優先し、なければ暫定を使用

    データソース:
      1. prediction_horses（AI予想スコア・馬名）
      2. entries（horse_weight, jockey 等）
      3. race_results（着順）
      4. training_evaluations
    """
    if kind == "暫定":
        mt_filter = "AND p.model_type LIKE '%暫定%'"
    elif kind == "直前":
        mt_filter = "AND p.model_type LIKE '%直前%'"
    else:
        # 直前・暫定どちらも OR で取得し、直前を優先（EV・model_score 最大）
        mt_filter = ""

    return _df(f"""
        WITH best_pred AS (
            SELECT
                ph.horse_name,
                ph.horse_id,
                MAX(ph.predicted_rank)    AS predicted_rank,
                MAX(ph.ev_score)          AS ev_score,
                MAX(ph.model_score)       AS model_score,
                MAX(p.recommended_bet)    AS recommended_bet,
                MAX(p.expected_value)     AS expected_value,
                MAX(p.confidence)         AS confidence,
                MAX(p.model_type)         AS model_type
            FROM prediction_horses ph
            JOIN predictions p ON p.id = ph.prediction_id
            WHERE p.race_id     = ?
              AND p.model_type  LIKE '%本命%'
              AND p.bet_type    = '単勝'
              AND p.expected_value > 0
              {mt_filter}
            GROUP BY ph.horse_name
        ),
        best_entry AS (
            SELECT horse_number, gate_number, horse_name, horse_id,
                   sex_age, weight_carried, jockey, trainer,
                   horse_weight, horse_weight_diff
            FROM entries
            WHERE race_id = ?
            GROUP BY horse_number
            HAVING id = MAX(id)
        ),
        best_result AS (
            SELECT horse_name, MIN(rank) AS rank,
                   win_odds, popularity, finish_time
            FROM race_results
            WHERE race_id = ?
            GROUP BY horse_name
        ),
        training AS (
            SELECT horse_name, eval_grade, eval_text
            FROM training_evaluations
            WHERE race_id = ?
        )
        SELECT
            bp.horse_name,
            bp.horse_id,
            bp.predicted_rank,
            bp.ev_score,
            bp.model_score,
            bp.recommended_bet,
            bp.expected_value,
            bp.confidence,
            bp.model_type,
            COALESCE(be.horse_number, bp.predicted_rank) AS horse_number,
            be.gate_number,
            be.sex_age,
            be.weight_carried,
            be.jockey,
            be.trainer,
            be.horse_weight,
            be.horse_weight_diff,
            r.rank,
            r.win_odds,
            r.popularity,
            r.finish_time,
            t.eval_grade,
            t.eval_text
        FROM best_pred bp
        LEFT JOIN best_entry  be ON be.horse_name = bp.horse_name
        LEFT JOIN best_result r  ON r.horse_name  = bp.horse_name
        LEFT JOIN training    t  ON t.horse_name  = bp.horse_name
        ORDER BY bp.predicted_rank, bp.ev_score DESC
    """, (race_id, race_id, race_id, race_id))


# 後方互換エイリアス（既存コードが fetch_race_prerace を呼ぶ場合）
def fetch_race_prerace(race_id: str) -> pd.DataFrame:
    return fetch_race_predictions(race_id, kind="any")


@st.cache_data(ttl=60)
def fetch_race_bets(race_id: str, kind: str = "any") -> pd.DataFrame:
    """
    全券種の買い目データを返す（1行=1予想レコード）。

    combination_json を人間が読める買い目文字列に変換して返す。

    Args:
        race_id: レース ID
        kind: "暫定" / "直前" / "any"
    """
    if kind == "暫定":
        mt_filter = "AND p.model_type LIKE '%暫定%'"
    elif kind == "直前":
        mt_filter = "AND p.model_type LIKE '%直前%'"
    else:
        mt_filter = ""

    return _df(f"""
        SELECT
            p.id            AS prediction_id,
            p.model_type,
            p.bet_type,
            p.expected_value,
            p.recommended_bet,
            p.confidence,
            p.combination_json,
            p.notes,
            p.created_at,
            (SELECT GROUP_CONCAT(ph.horse_name, ' / ')
             FROM prediction_horses ph
             WHERE ph.prediction_id = p.id
             ORDER BY ph.predicted_rank) AS horse_names_str
        FROM predictions p
        WHERE p.race_id = ?
          {mt_filter}
          AND p.expected_value > 0
        ORDER BY p.model_type, p.bet_type,
                 p.expected_value DESC
    """, (race_id,))


@st.cache_data(ttl=60)
def fetch_realtime_odds_trend(race_id: str) -> pd.DataFrame:
    """realtime_odds テーブルから時系列データを取得する。"""
    return _df("""
        SELECT horse_number, horse_name, win_odds, popularity, recorded_at
        FROM realtime_odds
        WHERE race_id = ?
        ORDER BY recorded_at, horse_number
    """, (race_id,))


@st.cache_data(ttl=60)
def fetch_today_bias(race_id: str) -> dict[str, Any]:
    """当日バイアス（内外・前残り）を計算する。"""
    venue   = race_id[4:6]
    race_no = int(race_id[10:12])
    prefix  = race_id[:4] + venue  # YYYYVV

    rows = _q("""
        SELECT rr.horse_number, rr.gate_number, rr.rank, rr.popularity
        FROM race_results rr
        WHERE rr.race_id LIKE ?
          AND CAST(SUBSTR(rr.race_id,11,2) AS INTEGER) < ?
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
    """, (f"{prefix}%", race_no))

    if not rows:
        return {"inner_bias": None, "front_bias": None, "race_count": 0}

    df = pd.DataFrame(rows)
    winners     = df[df["rank"] == 1]
    total_races = len(winners)
    if total_races == 0:
        return {"inner_bias": None, "front_bias": None, "race_count": 0}

    inner_wins = len(winners[winners["gate_number"].between(1, 4)])
    outer_wins = len(winners[winners["gate_number"].between(5, 8)])
    inner_bias = (inner_wins / total_races) - (outer_wins / total_races)

    fav_wins   = len(winners[winners["popularity"] == 1])
    front_bias = fav_wins / total_races

    return {"inner_bias": inner_bias, "front_bias": front_bias, "race_count": total_races}


@st.cache_data(ttl=300)
def fetch_payouts(race_id: str) -> pd.DataFrame:
    """払戻データを取得する。"""
    return _df("""
        SELECT bet_type, combination, payout, popularity
        FROM race_payouts
        WHERE race_id = ?
        ORDER BY popularity NULLS LAST
    """, (race_id,))


@st.cache_data(ttl=300)
def fetch_race_archive(race_id: str) -> pd.DataFrame:
    """指定レースの全予想バリアントを返す（予想アーカイブタブ用）。"""
    return _df("""
        SELECT
            p.model_type,
            p.bet_type,
            p.expected_value,
            p.recommended_bet,
            p.confidence,
            p.created_at,
            ph.horse_name,
            ph.predicted_rank,
            ph.ev_score,
            ph.model_score
        FROM prediction_horses ph
        JOIN predictions p ON p.id = ph.prediction_id
        WHERE p.race_id = ?
          AND p.expected_value > 0
        ORDER BY p.created_at DESC, p.model_type, ph.predicted_rank
    """, (race_id,))


# ── Analytics 用クエリ ────────────────────────────────────────────

def _kind_sql(kind: str) -> str:
    """暫定/直前フィルタ用 WHERE 句フラグメントを返す。"""
    if kind == "暫定":
        return "AND p.model_type LIKE '%暫定%'"
    if kind == "直前":
        return "AND p.model_type LIKE '%直前%'"
    return ""  # すべて


@st.cache_data(ttl=300)
def fetch_monthly_roi(kind: str = "all") -> pd.DataFrame:
    kf = _kind_sql(kind)
    return _df(f"""
        SELECT
            strftime('%Y-%m', p.created_at) AS month,
            p.model_type,
            p.bet_type,
            COUNT(*)                        AS bets,
            SUM(pr.is_hit)                  AS hits,
            SUM(p.recommended_bet)          AS invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          {kf}
        GROUP BY month, p.model_type, p.bet_type
        ORDER BY month, p.model_type, p.bet_type
    """)


@st.cache_data(ttl=300)
def fetch_venue_performance(kind: str = "all") -> pd.DataFrame:
    kf = _kind_sql(kind)
    return _df(f"""
        SELECT
            SUBSTR(p.race_id,5,2)          AS venue_code,
            COUNT(*)                       AS bets,
            SUM(pr.is_hit)                 AS hits,
            SUM(p.recommended_bet)         AS invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          AND DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'
          {kf}
        GROUP BY venue_code
    """)


@st.cache_data(ttl=300)
def fetch_kelly_simulation(kind: str = "all") -> pd.DataFrame:
    """EV >= 1.0 のベット履歴をケリーシミュレーション用に返す。"""
    kf = _kind_sql(kind)
    return _df(f"""
        SELECT
            p.created_at,
            strftime('%Y-%m', p.created_at) AS month,
            pr.is_hit,
            pr.payout,
            p.recommended_bet,
            p.expected_value
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND p.expected_value >= 1.0
          AND DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          {kf}
        ORDER BY p.created_at, pr.id
    """)


@st.cache_data(ttl=300)
def fetch_summary_stats(kind: str = "all") -> dict[str, Any]:
    kf = _kind_sql(kind)
    rows = _q(f"""
        SELECT
            COUNT(pr.id)           AS total_bets,
            SUM(pr.is_hit)         AS total_hits,
            SUM(p.recommended_bet) AS total_invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS total_payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          {kf}
    """)
    return rows[0] if rows else {}


@st.cache_data(ttl=300)
def fetch_hit_performance(kind: str = "all") -> pd.DataFrame:
    """EV >= 1.0 の予想結果を一覧で返す（的中実績タブ用）。"""
    kf = _kind_sql(kind)
    return _df(f"""
        SELECT
            DATE(p.created_at)             AS race_date,
            p.race_id,
            SUBSTR(p.race_id,5,2)          AS venue_code,
            CAST(SUBSTR(p.race_id,11,2) AS INTEGER) AS race_number,
            p.model_type,
            p.bet_type,
            p.expected_value,
            p.recommended_bet,
            pr.is_hit,
            pr.payout,
            (SELECT ph.horse_name
             FROM prediction_horses ph
             WHERE ph.prediction_id = p.id
             ORDER BY ph.predicted_rank
             LIMIT 1)                      AS top_horse
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.expected_value >= 1.0
          AND DATE(p.created_at) BETWEEN '2020-01-01' AND '2030-12-31'
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
          {kf}
        ORDER BY p.created_at DESC
        LIMIT 500
    """)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  UI コンポーネント                                                ║
# ╚══════════════════════════════════════════════════════════════════╝

def _eval_badge(grade: str | None) -> str:
    """調教評価をバッジ絵文字つき文字列に変換する。"""
    if grade is None:
        return "—"
    color = {"A": "🌸", "B": "🟠", "C": "🔵", "D": "⚫"}.get(grade, "⚪")
    return f"{color} {grade}"


def render_bias_panel(race_id: str) -> None:
    """当日馬場バイアスパネルを描画する。"""
    bias  = fetch_today_bias(race_id)
    inner = bias.get("inner_bias")
    front = bias.get("front_bias")
    count = bias.get("race_count", 0)

    st.markdown("#### 📊 当日馬場バイアス")
    c1, c2, c3 = st.columns(3)

    with c1:
        if inner is not None:
            pct   = min(max((inner + 0.20) / 0.40, 0.0), 1.0)
            label = (
                "内枠大有利 🔵" if inner >  0.15 else
                "内枠有利 🟢"   if inner >  0.07 else
                "外枠有利 🟠"   if inner < -0.07 else
                "外枠大有利 🔴" if inner < -0.15 else
                "フラット ⚪"
            )
            st.metric("内外バイアス", label, f"{inner*100:+.1f}pt")
            st.progress(pct, text="← 外枠    内枠 →")
        else:
            st.metric("内外バイアス", "データ不足", "")
            st.progress(0.5, text="先行データ待ち")

    with c2:
        if front is not None:
            label = (
                "前残り強 🏇"  if front > 0.60 else
                "前残り傾向"   if front > 0.45 else
                "差し有利 💨"  if front > 0.30 else
                "追込み場 ⚡"
            )
            st.metric("前残り率（1番人気勝率）", label, f"{front*100:.0f}%")
            st.progress(front, text="← 差し    前残り →")
        else:
            st.metric("前残り率", "データ不足", "")
            st.progress(0.5, text="先行データ待ち")

    with c3:
        st.metric("サンプルレース数", f"{count} R", "当日先行レース")
        if inner is not None and abs(inner) > 0.15:
            st.warning("⚠️ バイアスが強く出ています。枠順を重視してください。")


def render_prerace_table(df: pd.DataFrame) -> None:
    """直前分析テーブルを描画する。EV >= 1.0 の行を強調。"""
    if df.empty:
        st.info("出走データなし（prediction_horses に未登録）")
        return

    display = df.copy()
    display["ev_score"]        = pd.to_numeric(display.get("ev_score"),        errors="coerce")
    display["model_score"]     = pd.to_numeric(display.get("model_score"),     errors="coerce")
    display["recommended_bet"] = pd.to_numeric(display.get("recommended_bet"), errors="coerce")
    display["win_odds"]        = pd.to_numeric(display.get("win_odds"),        errors="coerce")
    display["expected_value"]  = pd.to_numeric(display.get("expected_value"),  errors="coerce")

    def _kelly_label(row: pd.Series) -> str:
        bet = row.get("recommended_bet")
        ev  = row.get("expected_value") or row.get("ev_score")
        if pd.isna(bet) if isinstance(bet, float) else bet is None:
            return "—"
        bet = float(bet)
        ev_val = float(ev) if ev is not None and not (isinstance(ev, float) and pd.isna(ev)) else 0.0
        if bet <= 0 or ev_val < 1.0:
            return "—"
        return f"¥{int(bet):,}"

    display["kelly_bet"] = display.apply(_kelly_label, axis=1)
    display["調教"]      = display.get("eval_grade", pd.Series(dtype=str)).apply(_eval_badge)

    def _ev_label(row: pd.Series) -> str:
        ev = row.get("expected_value")
        if ev is None or (isinstance(ev, float) and pd.isna(ev)):
            ev = row.get("ev_score")
        if ev is None or (isinstance(ev, float) and pd.isna(ev)):
            return "—"
        return f"🔥 {ev:.2f}" if float(ev) >= 1.0 else f"{ev:.2f}"

    display["EV"]      = display.apply(_ev_label, axis=1)
    display["_ev_num"] = (
        pd.to_numeric(display.get("expected_value"), errors="coerce")
        .fillna(pd.to_numeric(display.get("ev_score"), errors="coerce").fillna(0))
    )

    display["本命%"] = display["model_score"].apply(
        lambda x: f"{x*100:.1f}%" if pd.notna(x) else "—"
    )
    display["単勝"] = display["win_odds"].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )

    def _weight_str(row: pd.Series) -> str:
        w = row.get("horse_weight")
        d = row.get("horse_weight_diff")
        if w is None or (isinstance(w, float) and pd.isna(w)):
            return "—"
        s = str(int(w))
        if d is not None and not (isinstance(d, float) and pd.isna(d)):
            sign = "+" if int(d) > 0 else ""
            s += f"({sign}{int(d)})"
        return s

    display["体重"] = display.apply(_weight_str, axis=1)
    display["着順"] = display.get("rank", pd.Series(dtype=str)).apply(
        lambda x: f"{int(x)}着" if pd.notna(x) and x is not None else "—"
    )

    col_map = {
        "horse_number":  "馬番",
        "gate_number":   "枠",
        "horse_name":    "馬名",
        "sex_age":       "性齢",
        "weight_carried":"斤量",
        "jockey":        "騎手",
        "単勝":          "単勝",
        "popularity":    "人気",
        "体重":          "体重",
        "調教":          "調教",
        "本命%":         "本命%",
        "EV":            "EV",
        "kelly_bet":     "Kelly推奨",
        "着順":          "着順",
    }
    out = pd.DataFrame()
    for src, dst in col_map.items():
        if src in display.columns:
            out[dst] = display[src].values

    def _row_style(row: pd.Series):
        is_hot = str(row.get("EV", "")).startswith("🔥")
        bg = "background-color: rgba(255,50,80,0.15); color: #ff6080;" if is_hot else ""
        return [bg] * len(row)

    styled = out.style.apply(_row_style, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True,
                 height=min(60 + len(out) * 42, 700))

    # 激アツ推奨馬サマリー
    hot_rows = display[display["_ev_num"] >= 1.0]
    if not hot_rows.empty:
        st.markdown("---")
        st.markdown("### 🔥 激アツ推奨馬（EV ≥ 1.0）")
        for _, h in hot_rows.sort_values("_ev_num", ascending=False).iterrows():
            ev    = h["_ev_num"]
            odds  = h.get("win_odds")
            bet   = h.get("recommended_bet")
            name  = h.get("horse_name", "")
            num   = h.get("horse_number", "")
            grade = h.get("eval_grade", "")
            grade_str = f"  |  調教 {_eval_badge(grade)}" if grade else ""
            bet_str   = f"  |  推奨: ¥{int(bet):,}" if pd.notna(bet) and float(bet) > 0 else ""
            odds_str  = f"  |  単勝 {odds:.1f}倍" if pd.notna(odds) else ""
            st.error(f"🔥 **{num}番 {name}**  |  EV = **{ev:.3f}**{odds_str}{grade_str}{bet_str}")


def render_odds_alert(race_id: str) -> None:
    """オッズ時系列アラートを描画する。"""
    df = fetch_realtime_odds_trend(race_id)
    if df.empty:
        st.info("リアルタイムオッズデータなし（レース当日に prerace_pipeline を実行すると蓄積されます）")
        return

    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
    df["win_odds"]    = pd.to_numeric(df["win_odds"], errors="coerce")

    summary_rows = []
    for horse_num, grp in df.groupby("horse_number"):
        grp = grp.sort_values("recorded_at")
        if len(grp) < 2:
            continue
        first_odds  = grp.iloc[0]["win_odds"]
        latest_odds = grp.iloc[-1]["win_odds"]
        elapsed_min = (grp.iloc[-1]["recorded_at"] - grp.iloc[0]["recorded_at"]).total_seconds() / 60
        if elapsed_min > 0 and pd.notna(first_odds) and pd.notna(latest_odds) and first_odds > 0:
            velocity = (first_odds - latest_odds) / elapsed_min
            summary_rows.append({
                "馬番":          horse_num,
                "馬名":          grp.iloc[-1]["horse_name"],
                "朝一オッズ":    first_odds,
                "現在オッズ":    latest_odds,
                "朝比率":        latest_odds / first_odds,
                "下落速度(/分)": velocity,
                "シグナル":      "🔥 大口" if velocity >= _VELOCITY_THRESHOLD else "—",
            })

    if not summary_rows:
        st.info("オッズ変動データなし")
        return

    sumdf = pd.DataFrame(summary_rows).sort_values("下落速度(/分)", ascending=False)
    st.dataframe(sumdf, use_container_width=True, hide_index=True)

    fig = px.line(
        df, x="recorded_at", y="win_odds", color="horse_name",
        title="単勝オッズ推移",
        labels={"recorded_at": "時刻", "win_odds": "単勝オッズ", "horse_name": "馬名"},
        template="plotly_dark",
    )
    fig.update_layout(height=350, margin=dict(t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def render_result_with_payouts(race_id: str, prerace_df: pd.DataFrame) -> None:
    """レース結果と払戻を統合表示する。"""
    # 着順結果テーブル
    st.markdown("#### 🏁 着順結果")
    if prerace_df.empty:
        st.info("出走データなし")
    else:
        result_df = prerace_df[prerace_df["rank"].notna()].copy()
        if result_df.empty:
            st.info("レース結果未確定")
        else:
            result_df = result_df.sort_values("rank")
            col_rename = {
                "rank":              "着順",
                "gate_number":       "枠",
                "horse_number":      "馬番",
                "horse_name":        "馬名",
                "sex_age":           "性齢",
                "weight_carried":    "斤量",
                "jockey":            "騎手",
                "finish_time":       "タイム",
                "win_odds":          "単勝",
                "popularity":        "人気",
                "horse_weight":      "体重",
                "horse_weight_diff": "増減",
            }
            avail = {k: v for k, v in col_rename.items() if k in result_df.columns}
            disp  = result_df[list(avail.keys())].copy()
            disp.columns = list(avail.values())
            st.dataframe(disp, use_container_width=True, hide_index=True)

    # 払戻テーブル
    st.markdown("#### 💴 払戻金")
    payouts = fetch_payouts(race_id)
    if payouts.empty:
        st.info("払戻データなし")
    else:
        _BET_ORDER = {
            "単勝": 1, "複勝": 2, "枠連": 3, "馬連": 4,
            "ワイド": 5, "馬単": 6, "三連複": 7, "三連単": 8,
        }
        payouts["_order"] = payouts["bet_type"].map(lambda x: _BET_ORDER.get(x, 99))
        payouts = payouts.sort_values(["_order", "popularity"]).drop(columns=["_order"])
        payouts["payout"] = payouts["payout"].apply(
            lambda x: f"¥{int(x):,}" if pd.notna(x) else "—"
        )
        st.dataframe(payouts, use_container_width=True, hide_index=True)


def _combo_display(combination_json: str | None) -> str:
    """combination_json を人間が読める買い目文字列に変換する。

    Examples:
        [[1]]           → "1"
        [[1,5]]         → "1-5"
        [[1,5,3]]       → "1-5-3"
        [[1,5],[1,8]]   → "1-5 / 1-8"
        (6点以上)        → "1-5-3 (他5点)"
    """
    if not combination_json:
        return "—"
    try:
        combos = json.loads(combination_json)
        if not combos:
            return "—"

        def fmt(c: list) -> str:
            return "-".join(str(x) for x in c)

        if len(combos) == 1:
            return fmt(combos[0])
        elif len(combos) <= 4:
            return " / ".join(fmt(c) for c in combos)
        else:
            return f"{fmt(combos[0])} 他{len(combos)-1}点"
    except Exception:
        return str(combination_json)[:30]


# 券種表示順
_BET_ORDER: dict[str, int] = {
    "単勝": 1, "複勝": 2, "馬連": 3, "ワイド": 4,
    "馬単": 5, "三連複": 6, "三連単": 7, "WIN5": 8,
}


def render_bets_table(bets_df: pd.DataFrame, tab_key: str = "") -> None:
    """
    全券種の買い目テーブルを描画する。

    券種 radio ボタンでフィルタリング可能。
    EV >= 1.0 の行を強調表示する。
    """
    if bets_df.empty:
        st.info("買い目データなし")
        return

    bets_df = bets_df.copy()
    bets_df["expected_value"] = pd.to_numeric(bets_df["expected_value"], errors="coerce").fillna(0)
    bets_df["recommended_bet"] = pd.to_numeric(bets_df["recommended_bet"], errors="coerce").fillna(0)
    bets_df["confidence"] = pd.to_numeric(bets_df["confidence"], errors="coerce").fillna(0)

    # 券種フィルター（radio ボタン）
    available = sorted(bets_df["bet_type"].dropna().unique().tolist(),
                       key=lambda x: _BET_ORDER.get(x, 99))
    options = ["全券種"] + available
    selected = st.radio(
        "券種を選択",
        options,
        horizontal=True,
        key=f"bet_type_radio_{tab_key}",
    )
    if selected != "全券種":
        bets_df = bets_df[bets_df["bet_type"] == selected]

    if bets_df.empty:
        st.info(f"{selected} の買い目なし")
        return

    # 買い目文字列を生成
    bets_df["買い目"] = bets_df["combination_json"].apply(_combo_display)

    # 表示用列を整形
    display = bets_df.copy()

    display["EV"] = display["expected_value"].apply(
        lambda x: f"🔥 {x:.3f}" if x >= 1.0 else f"{x:.3f}"
    )
    display["推奨金額"] = display["recommended_bet"].apply(
        lambda x: f"¥{int(x):,}" if x > 0 else "—"
    )
    display["信頼度"] = display["confidence"].apply(
        lambda x: f"{x*100:.1f}%" if x > 0 else "—"
    )

    # モデルタイプの短縮表示
    display["モデル"] = display["model_type"].apply(
        lambda x: x if pd.notna(x) else "—"
    )

    # 表示列を選択
    col_order = ["モデル", "bet_type", "買い目", "horse_names_str", "EV", "推奨金額", "信頼度"]
    col_rename = {
        "bet_type":        "券種",
        "horse_names_str": "対象馬",
    }
    out = display[col_order].rename(columns=col_rename)

    # EV>=1.0 行にスタイルを適用
    def _row_style(row: pd.Series):
        ev_str = str(row.get("EV", ""))
        is_hot = ev_str.startswith("🔥")
        bg = "background-color: rgba(255,50,80,0.18); color: #ff8080;" if is_hot else ""
        return [bg] * len(row)

    styled = out.style.apply(_row_style, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True,
                 height=min(80 + len(out) * 42, 700))

    # 激アツ買い目ハイライト
    hot = display[display["expected_value"] >= 1.0].sort_values("expected_value", ascending=False)
    if not hot.empty:
        st.markdown("---")
        st.markdown("### 🔥 推奨買い目（EV ≥ 1.0）")
        for _, row in hot.iterrows():
            ev   = row["expected_value"]
            combo = row["買い目"]
            btype = row.get("bet_type", "")
            model = row.get("モデル", "")
            bet  = row["recommended_bet"]
            bet_str = f"  |  推奨: ¥{int(bet):,}" if bet > 0 else ""
            notes = row.get("notes", "")
            st.error(
                f"🔥 **{model} / {btype}**  {combo}  |  EV = **{ev:.3f}**{bet_str}"
                + (f"  |  {notes}" if notes else "")
            )


def render_race_archive(race_id: str) -> None:
    """予想アーカイブタブ: 指定レースの全予想バリアントを表示する。"""
    st.markdown("#### 🗂️ 予想アーカイブ")
    df = fetch_race_archive(race_id)
    if df.empty:
        st.info("このレースの予想データなし")
        return

    # モデル×券種ごとにグループ表示
    for (model_type, bet_type), grp in df.groupby(["model_type", "bet_type"]):
        ev    = grp.iloc[0]["expected_value"]
        bet   = grp.iloc[0]["recommended_bet"]
        ts    = grp.iloc[0]["created_at"]
        label = f"**{model_type} / {bet_type}**  |  EV={ev:.3f}  |  推奨¥{int(bet):,}  |  {ts}"
        with st.expander(label, expanded=(float(ev) >= 1.0)):
            sub = grp[["horse_name", "predicted_rank", "model_score", "ev_score"]].copy()
            sub["model_score"] = sub["model_score"].apply(
                lambda x: f"{x*100:.1f}%" if pd.notna(x) else "—"
            )
            sub["ev_score"] = sub["ev_score"].apply(
                lambda x: f"🔥 {x:.3f}" if pd.notna(x) and x >= 1.0 else (f"{x:.3f}" if pd.notna(x) else "—")
            )
            sub.columns = ["馬名", "予想順位", "本命スコア", "EV"]
            st.dataframe(sub, use_container_width=True, hide_index=True)


# ── Analytics ─────────────────────────────────────────────────────

def render_analytics() -> None:
    """月次 ROI・ドローダウン・会場別成績を描画する。"""
    st.markdown("## 📈 Analytics — 収益分析")

    # ── 暫定/直前フィルタ ─────────────────────────────────────
    kind = st.selectbox(
        "予想タイプ絞り込み",
        ["すべて", "暫定予想のみ", "直前予想のみ"],
        index=0,
        key="analytics_kind",
    )
    kind_key = {"すべて": "all", "暫定予想のみ": "暫定", "直前予想のみ": "直前"}[kind]

    stats = fetch_summary_stats(kind=kind_key)
    if stats:
        invested = stats.get("total_invested") or 0
        payout   = stats.get("total_payout")   or 0
        bets     = stats.get("total_bets")      or 0
        hits     = stats.get("total_hits")      or 0
        roi      = payout / invested if invested > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("総ベット数", f"{int(bets):,} 件")
        m2.metric("総的中数",   f"{int(hits):,} 件", f"{hits/bets*100:.1f}%" if bets else "—")
        m3.metric("総投資額",   f"¥{int(invested):,}")
        m4.metric("ROI",        f"{roi:.3f}", f"{(roi-1)*100:+.1f}%" if invested else "—")

    st.divider()

    monthly_df = fetch_monthly_roi(kind=kind_key)
    if monthly_df.empty:
        st.warning("月次データなし")
        return

    monthly_df["roi"]      = (monthly_df["payout"] / monthly_df["invested"].replace(0, pd.NA)).fillna(0)
    monthly_df["hit_rate"] = monthly_df["hits"] / monthly_df["bets"].replace(0, pd.NA)

    monthly_total = (
        monthly_df.groupby("month")
        .agg(invested=("invested", "sum"), payout=("payout", "sum"),
             bets=("bets", "sum"), hits=("hits", "sum"))
        .reset_index()
    )
    monthly_total["roi"]      = monthly_total["payout"] / monthly_total["invested"].replace(0, pd.NA)
    monthly_total["hit_rate"] = monthly_total["hits"] / monthly_total["bets"].replace(0, pd.NA)

    tab_roi, tab_kelly, tab_venue = st.tabs(["📊 月次ROI", "💰 ケリーシミュレーション", "🏇 会場別成績"])

    with tab_roi:
        c1, c2 = st.columns(2)
        with c1:
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Bar(
                x=monthly_total["month"],
                y=monthly_total["roi"],
                marker_color=["#00ff88" if r >= 1.0 else "#ff3366"
                              for r in monthly_total["roi"].fillna(0)],
                name="月次ROI",
                text=[f"{r:.3f}" for r in monthly_total["roi"].fillna(0)],
                textposition="outside",
            ))
            fig_roi.add_hline(y=1.0, line_dash="dash", line_color="#ffd700",
                              annotation_text="損益分岐 (1.0)", annotation_position="top right")
            fig_roi.update_layout(title="月次 ROI", template="plotly_dark",
                                  height=360, margin=dict(t=40, b=30),
                                  yaxis_title="ROI", xaxis_title="月")
            st.plotly_chart(fig_roi, use_container_width=True)

        with c2:
            fig_hit = px.line(
                monthly_total, x="month", y="hit_rate",
                title="月次的中率",
                labels={"month": "月", "hit_rate": "的中率"},
                template="plotly_dark", markers=True,
                color_discrete_sequence=["#00c8ff"],
            )
            fig_hit.update_layout(height=360, margin=dict(t=40, b=30))
            fig_hit.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig_hit, use_container_width=True)

        with st.expander("モデル別・券種別詳細"):
            detail = monthly_df.copy()
            detail["roi_pct"]  = (detail["roi"] * 100).round(1).astype(str) + "%"
            detail["hit_rate"] = (detail["hit_rate"] * 100).round(1).astype(str) + "%"
            st.dataframe(
                detail[["month", "model_type", "bet_type", "bets", "hits",
                         "hit_rate", "invested", "payout", "roi_pct"]],
                use_container_width=True, hide_index=True,
            )

    with tab_kelly:
        sim_df = fetch_kelly_simulation(kind=kind_key)
        if sim_df.empty:
            st.info("EV >= 1.0 のベット履歴がありません")
        else:
            bankroll = 1_000_000.0
            peak     = bankroll
            max_dd   = 0.0
            series   = [{"date": sim_df.iloc[0]["created_at"], "bankroll": bankroll}]
            wins, total = 0, 0

            for _, row in sim_df.iterrows():
                actual_bet = min(float(row["recommended_bet"]), bankroll * 0.10)
                if actual_bet <= 0 or bankroll <= 0:
                    continue
                profit   = float(row["payout"]) - actual_bet if row["is_hit"] else -actual_bet
                bankroll += profit
                if bankroll > peak:
                    peak = bankroll
                dd     = (peak - bankroll) / peak if peak > 0 else 0
                max_dd = max(max_dd, dd)
                if row["is_hit"]:
                    wins += 1
                total  += 1
                series.append({"date": row["created_at"], "bankroll": bankroll})

            series_df = pd.DataFrame(series)
            series_df["date"] = pd.to_datetime(series_df["date"])

            fig_bk = go.Figure()
            fig_bk.add_trace(go.Scatter(
                x=series_df["date"], y=series_df["bankroll"],
                mode="lines", name="資金",
                line=dict(color="#00c8ff", width=2),
                fill="tozeroy", fillcolor="rgba(0,200,255,0.07)",
            ))
            fig_bk.add_hline(y=1_000_000, line_dash="dash", line_color="#ffd700",
                              annotation_text="初期資金 ¥1,000,000")
            fig_bk.update_layout(
                title="ケリー基準 資金推移（EV ≥ 1.0 ベット）",
                template="plotly_dark", height=380, margin=dict(t=40, b=30),
                yaxis_title="資金 (円)", xaxis_title="日付",
            )
            st.plotly_chart(fig_bk, use_container_width=True)

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("最終資金",        f"¥{bankroll:,.0f}")
            k2.metric("最大ドローダウン", f"{max_dd*100:.1f}%")
            k3.metric("的中率",          f"{wins/total*100:.1f}%" if total else "—")
            k4.metric("総ベット",         f"{total:,} 件")

    with tab_venue:
        venue_df = fetch_venue_performance(kind=kind_key)
        if venue_df.empty:
            st.info("会場別データなし")
        else:
            venue_df["venue"]    = venue_df["venue_code"].map(lambda c: _JYO.get(c, c))
            venue_df["roi"]      = venue_df["payout"] / venue_df["invested"].replace(0, pd.NA)
            venue_df["hit_rate"] = venue_df["hits"] / venue_df["bets"].replace(0, pd.NA)
            venue_df = venue_df.sort_values("roi", ascending=False)

            fig_v = px.bar(
                venue_df, x="venue", y="roi",
                color="roi",
                color_continuous_scale=["#ff3366", "#ffd700", "#00ff88"],
                color_continuous_midpoint=1.0,
                title="会場別 ROI（降順）",
                labels={"venue": "会場", "roi": "ROI"},
                template="plotly_dark",
                text=venue_df["roi"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else ""),
            )
            fig_v.add_hline(y=1.0, line_dash="dash", line_color="#ffd700")
            fig_v.update_layout(height=380, margin=dict(t=40, b=30), coloraxis_showscale=False)
            fig_v.update_traces(textposition="outside")
            st.plotly_chart(fig_v, use_container_width=True)

            tbl = venue_df[["venue", "bets", "hits", "hit_rate", "invested", "payout", "roi"]].copy()
            tbl["hit_rate"] = (tbl["hit_rate"] * 100).round(1).astype(str) + "%"
            tbl["roi"]      = tbl["roi"].round(3)
            tbl["invested"] = tbl["invested"].apply(lambda x: f"¥{int(x):,}")
            tbl["payout"]   = tbl["payout"].apply(lambda x: f"¥{int(x):,}")
            tbl.columns     = ["会場", "ベット数", "的中", "的中率", "投資額", "回収額", "ROI"]
            st.dataframe(tbl, use_container_width=True, hide_index=True)


def render_hit_performance() -> None:
    """EV >= 1.0 の的中実績を一覧表示する。"""
    st.markdown("## 🎯 的中実績 — EV ≥ 1.0 ベット追跡")

    # ── 暫定/直前フィルタ ─────────────────────────────────────
    kind = st.selectbox(
        "予想タイプ絞り込み",
        ["すべて", "暫定予想のみ", "直前予想のみ"],
        index=0,
        key="hits_kind",
    )
    kind_key = {"すべて": "all", "暫定予想のみ": "暫定", "直前予想のみ": "直前"}[kind]

    df = fetch_hit_performance(kind=kind_key)
    if df.empty:
        st.info("EV >= 1.0 の予想履歴がありません")
        return

    # サマリーメトリクス
    total  = len(df)
    hits   = int(df["is_hit"].sum())
    inv    = df["recommended_bet"].sum()
    payout = df.loc[df["is_hit"] == 1, "payout"].sum()
    roi    = payout / inv if inv > 0 else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("EV≥1.0 件数",  f"{total:,} 件")
    m2.metric("的中",          f"{hits:,} 件",      f"{hits/total*100:.1f}%" if total else "—")
    m3.metric("総投資",        f"¥{int(inv):,}")
    m4.metric("総回収",        f"¥{int(payout):,}")
    m5.metric("ROI",           f"{roi:.3f}",         f"{(roi-1)*100:+.1f}%")

    st.divider()

    # テーブル表示
    disp = df.copy()
    disp["venue"] = disp["venue_code"].map(lambda c: _JYO.get(c, c))
    disp["結果"]  = disp["is_hit"].apply(lambda x: "✅ 的中" if x == 1 else "❌ 外れ")
    disp["payout_str"] = disp.apply(
        lambda r: f"¥{int(r['payout']):,}" if r["is_hit"] == 1 and pd.notna(r["payout"]) else "—",
        axis=1,
    )
    disp["bet_str"] = disp["recommended_bet"].apply(
        lambda x: f"¥{int(x):,}" if pd.notna(x) else "—"
    )
    disp["ev_str"] = disp["expected_value"].apply(
        lambda x: f"🔥 {x:.3f}" if pd.notna(x) and x >= 1.5 else (f"{x:.3f}" if pd.notna(x) else "—")
    )

    show = disp[["race_date", "venue", "race_number", "top_horse", "model_type",
                 "bet_type", "ev_str", "bet_str", "結果", "payout_str"]].copy()
    show.columns = ["日付", "会場", "R", "本命馬", "モデル", "券種", "EV", "投資", "結果", "払戻"]

    def _hit_style(row: pd.Series):
        is_hit = str(row.get("結果", "")).startswith("✅")
        bg = "background-color: rgba(0,255,136,0.12);" if is_hit else ""
        return [bg] * len(row)

    styled = show.style.apply(_hit_style, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True,
                 height=min(80 + len(show) * 38, 800))


# ╔══════════════════════════════════════════════════════════════════╗
# ║  メインアプリ                                                     ║
# ╚══════════════════════════════════════════════════════════════════╝

def main() -> None:
    st.set_page_config(
        page_title="UMALOGI — Pro Dashboard",
        page_icon="🏇",
        layout="wide",
        initial_sidebar_state="collapsed",  # サイドバーは使用しない
    )

    # ── グローバル CSS ─────────────────────────────────────────
    st.markdown("""
    <style>
      /* サイドバー完全非表示 */
      [data-testid="collapsedControl"] { display: none; }
      section[data-testid="stSidebar"] { display: none; }

      /* フォントサイズ全体拡大 */
      html, body, [class*="css"] { font-size: 15px !important; }
      .stMetric label            { font-size: 0.82rem !important; letter-spacing: 0.08em; }
      .stMetric .metric-container {
        background: rgba(0,200,255,0.06);
        border: 1px solid rgba(0,200,255,0.20);
        border-radius: 8px; padding: 12px;
      }
      div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
      h1, h2, h3 { letter-spacing: 0.05em; }

      /* トップナビ強調 */
      .top-nav-container {
        background: rgba(0,200,255,0.06);
        border: 1px solid rgba(0,200,255,0.18);
        border-radius: 10px;
        padding: 12px 16px;
        margin-bottom: 12px;
      }
      /* selectbox ラベル */
      .stSelectbox label { font-size: 0.85rem !important; color: #7ec8e3; }
    </style>
    """, unsafe_allow_html=True)

    # ── ページタイトル ─────────────────────────────────────────
    st.markdown("# 🏇 UMALOGI  —  Pro Investor Dashboard")
    st.divider()

    # ── トップレベルタブ（メインナビゲーション） ──────────────
    main_tabs = st.tabs(["🏇 レース分析", "📈 Analytics", "🎯 的中実績"])

    # ══════════════════════════════════════════════════════════
    #  Tab 1: レース分析
    # ══════════════════════════════════════════════════════════
    with main_tabs[0]:
        # ── 上部ナビゲーション（日付・会場・レース） ──────────
        st.markdown('<div class="top-nav-container">', unsafe_allow_html=True)
        nav_c1, nav_c2, nav_c3 = st.columns([2, 2, 4])

        dates = fetch_available_dates()
        if not dates:
            st.warning("予想データがありません（valid venue codes 01〜10 かつ EV > 0 の予想が必要）")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            with nav_c1:
                selected_date = st.selectbox("📅 開催日", dates, index=0)

            venues = fetch_venues_for_date(selected_date)
            if not venues:
                with nav_c2:
                    st.warning("この日の会場データなし")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                venue_options = {f"{name}（{code}）": code for code, name in venues}
                with nav_c2:
                    selected_venue_label = st.selectbox("🏟️ 会場", list(venue_options.keys()))
                selected_venue = venue_options[selected_venue_label]

                races_df = fetch_races_for_venue(selected_date, selected_venue)
                if races_df.empty:
                    with nav_c3:
                        st.warning("レースデータなし")
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    def _race_label(row) -> str:
                        rn   = int(row["race_number"])
                        name = _safe_race_name(row.get("race_name"))
                        return f"{rn:02d}R  {name}" if name else f"{rn:02d}R"

                    race_options = {
                        _race_label(row): row["race_id"]
                        for _, row in races_df.iterrows()
                    }
                    with nav_c3:
                        selected_race_label = st.selectbox("🏁 レース", list(race_options.keys()))
                    selected_race_id = race_options[selected_race_label]

                    st.markdown('</div>', unsafe_allow_html=True)

                    # ── レース見出し ──────────────────────────────
                    venue_name = _JYO.get(selected_venue, selected_venue)
                    race_no    = int(selected_race_id[10:12])
                    st.markdown(
                        f"### {selected_date}  {venue_name}  第 {race_no} R"
                        f"  <span style='font-size:0.7em;color:#7ec8e3;font-weight:normal;'>"
                        f"`{selected_race_id}`</span>",
                        unsafe_allow_html=True,
                    )

                    # ── サブタブ（5タブ構成） ─────────────────────
                    stab_prov, stab_final, stab_odds, stab_result, stab_arch = st.tabs([
                        "🔮 暫定予想",
                        "🔍 直前予想",
                        "📡 オッズ動向",
                        "📋 レース結果",
                        "🗂️ 予想アーカイブ",
                    ])

                    # 🔮 暫定予想タブ
                    with stab_prov:
                        prov_bets_df = fetch_race_bets(selected_race_id, kind="暫定")
                        prov_horse_df = fetch_race_predictions(selected_race_id, kind="暫定")
                        if prov_bets_df.empty and prov_horse_df.empty:
                            st.info(
                                "暫定予想データなし。\n\n"
                                "金曜バッチ後に `python scripts/force_provisional_today.py` "
                                "または `python -m src.main_pipeline provisional` を実行してください。"
                            )
                        else:
                            n_bets = len(prov_bets_df)
                            st.caption(
                                f"買い目: {n_bets} 件  |  "
                                f"馬分析: {len(prov_horse_df)} 頭"
                            )
                            render_bias_panel(selected_race_id)
                            st.divider()
                            ptab_horse, ptab_bets = st.tabs(["🐴 馬ランキング", "🎯 買い目一覧"])
                            with ptab_horse:
                                render_prerace_table(prov_horse_df)
                            with ptab_bets:
                                render_bets_table(prov_bets_df, tab_key="prov")

                    # 🔍 直前予想タブ
                    with stab_final:
                        final_bets_df = fetch_race_bets(selected_race_id, kind="直前")
                        final_horse_df = fetch_race_predictions(selected_race_id, kind="直前")
                        if final_bets_df.empty and final_horse_df.empty:
                            st.info(
                                "直前予想データなし。\n\n"
                                "レース当日に `python -m src.main_pipeline prerace <race_id>` "
                                "を実行すると表示されます。"
                            )
                        else:
                            n_bets = len(final_bets_df)
                            st.caption(
                                f"買い目: {n_bets} 件  |  "
                                f"馬分析: {len(final_horse_df)} 頭"
                            )
                            render_bias_panel(selected_race_id)
                            st.divider()
                            ftab_horse, ftab_bets = st.tabs(["🐴 馬ランキング", "🎯 買い目一覧"])
                            with ftab_horse:
                                render_prerace_table(final_horse_df)
                            with ftab_bets:
                                render_bets_table(final_bets_df, tab_key="final")

                    with stab_odds:
                        render_odds_alert(selected_race_id)

                    with stab_result:
                        # レース結果タブでは直前予想→暫定予想の順で fallback
                        result_base = fetch_race_predictions(selected_race_id, kind="直前")
                        if result_base.empty:
                            result_base = fetch_race_predictions(selected_race_id, kind="暫定")
                        render_result_with_payouts(selected_race_id, result_base)

                    with stab_arch:
                        render_race_archive(selected_race_id)

    # ══════════════════════════════════════════════════════════
    #  Tab 2: Analytics
    # ══════════════════════════════════════════════════════════
    with main_tabs[1]:
        render_analytics()

    # ══════════════════════════════════════════════════════════
    #  Tab 3: 的中実績
    # ══════════════════════════════════════════════════════════
    with main_tabs[2]:
        render_hit_performance()


if __name__ == "__main__":
    main()
