"""
UMALOGI — Streamlit プロ投資家ダッシュボード

起動:
  streamlit run web_streamlit/app.py

アーキテクチャ:
  JSON を一切使わず SQLite (data/umalogi.db) へ直接クエリを投げる設計。
  @st.cache_data でクエリ結果をキャッシュし、不要な再描画を防ぐ。
"""

from __future__ import annotations

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
_KELLY_BASE = 1_000_000   # Kelly 推奨額の基準資金（100万円）
_VELOCITY_THRESHOLD = 0.05

# ── DB 接続ヘルパー ─────────────────────────────────────────────────

@st.cache_resource
def _get_conn() -> sqlite3.Connection:
    """シングルトン接続（read-only）を返す。"""
    path = str(get_db_path())
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                           check_same_thread=False)
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


# ╔══════════════════════════════════════════════════════════════════╗
# ║  データ取得レイヤー                                               ║
# ╚══════════════════════════════════════════════════════════════════╝

@st.cache_data(ttl=300)
def fetch_available_dates() -> list[str]:
    """予想データが存在する日付（YYYY-MM-DD 形式）を降順で返す。

    predictions.created_at（実際の予想生成日）を基準とする。
    """
    rows = _q("""
        SELECT DISTINCT DATE(created_at) AS d
        FROM predictions
        WHERE created_at IS NOT NULL
        ORDER BY d DESC
    """)
    return [r["d"] for r in rows if r["d"]]


@st.cache_data(ttl=300)
def fetch_venues_for_date(date_str: str) -> list[tuple[str, str]]:
    """指定日に予想が生成された会場コードと名前リストを返す。 (code, name) のリスト。"""
    rows = _q("""
        SELECT DISTINCT SUBSTR(race_id,5,2) AS vc
        FROM predictions
        WHERE DATE(created_at) = ?
          AND SUBSTR(race_id,5,2) BETWEEN '01' AND '10'
        ORDER BY vc
    """, (date_str,))
    return [(r["vc"], _JYO.get(r["vc"], r["vc"])) for r in rows]


@st.cache_data(ttl=300)
def fetch_races_for_venue(date_str: str, venue_code: str) -> pd.DataFrame:
    """指定日・会場のレース一覧 (race_id, race_number) を返す。"""
    return _df("""
        SELECT DISTINCT
            p.race_id,
            CAST(SUBSTR(p.race_id,11,2) AS INTEGER) AS race_number
        FROM predictions p
        WHERE DATE(p.created_at) = ?
          AND SUBSTR(p.race_id,5,2) = ?
        ORDER BY race_number
    """, (date_str, venue_code))


@st.cache_data(ttl=60)
def fetch_race_prerace(race_id: str) -> pd.DataFrame:
    """
    指定レースの直前分析データ（1行=1頭）を返す。

    データソース優先順:
      1. prediction_horses（AI予想スコア・馬名）
      2. entries（horse_weight, jockey 等）— horse_name で LEFT JOIN
      3. race_results（着順）— horse_name で LEFT JOIN
      4. training_evaluations — horse_name で LEFT JOIN
    """
    return _df("""
        WITH best_pred AS (
            -- 本命モデル単勝: 各馬の代表行（EV 最大）
            SELECT
                ph.horse_name,
                ph.horse_id,
                MAX(ph.predicted_rank)               AS predicted_rank,
                MAX(ph.ev_score)                     AS ev_score,
                MAX(ph.model_score)                  AS model_score,
                MAX(p.recommended_bet)               AS recommended_bet,
                MAX(p.expected_value)                AS expected_value,
                MAX(p.confidence)                    AS confidence
            FROM prediction_horses ph
            JOIN predictions p ON p.id = ph.prediction_id
            WHERE p.race_id    = ?
              AND p.model_type = '本命'
              AND p.bet_type   = '単勝'
            GROUP BY ph.horse_name
        ),
        best_entry AS (
            -- entries: 最新登録分のみ
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
    """
    当日バイアスを計算する。

    同一日・同一会場で現在のレース番号より小さいレースの
    race_results から内外・前残り傾向を計算する。
    """
    venue   = race_id[4:6]
    ymd8    = race_id[:8]          # YYYYMMDD
    race_no = int(race_id[10:12])  # 現レース番号

    # 同日・同場の前レース一覧
    prefix = ymd8[:4] + venue  # YYYYVV
    rows = _q("""
        SELECT
            rr.horse_number,
            rr.gate_number,
            rr.rank,
            rr.popularity
        FROM race_results rr
        JOIN races ra ON ra.race_id = rr.race_id
        WHERE rr.race_id LIKE ?
          AND CAST(SUBSTR(rr.race_id,11,2) AS INTEGER) < ?
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
    """, (f"{prefix}%", race_no))

    if not rows:
        return {"inner_bias": None, "front_bias": None, "race_count": 0}

    df = pd.DataFrame(rows)
    # 1着の馬のみ抽出
    winners = df[df["rank"] == 1]
    total_races = len(winners)
    if total_races == 0:
        return {"inner_bias": None, "front_bias": None, "race_count": 0}

    # 内枠（1〜4）vs 外枠（5〜8）の1着率
    inner_wins = len(winners[winners["gate_number"].between(1, 4)])
    outer_wins = len(winners[winners["gate_number"].between(5, 8)])
    inner_rate = inner_wins / total_races
    outer_rate = outer_wins / total_races
    inner_bias = inner_rate - outer_rate   # プラス = 内枠有利

    # 1番人気の勝率（前残り指標）
    fav_wins  = len(winners[winners["popularity"] == 1])
    front_bias = fav_wins / total_races

    return {
        "inner_bias": inner_bias,
        "front_bias": front_bias,
        "race_count": total_races,
    }


@st.cache_data(ttl=300)
def fetch_payouts(race_id: str) -> pd.DataFrame:
    """払戻データを取得する。"""
    return _df("""
        SELECT bet_type, combination, payout, popularity
        FROM race_payouts
        WHERE race_id = ?
        ORDER BY popularity NULLS LAST
    """, (race_id,))


# ── Analytics 用クエリ ────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_monthly_roi() -> pd.DataFrame:
    return _df("""
        SELECT
            strftime('%Y-%m', p.created_at)       AS month,
            p.model_type,
            p.bet_type,
            COUNT(*)                              AS bets,
            SUM(pr.is_hit)                        AS hits,
            SUM(p.recommended_bet)                AS invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
        GROUP BY month, p.model_type, p.bet_type
        ORDER BY month, p.model_type, p.bet_type
    """)


@st.cache_data(ttl=300)
def fetch_venue_performance() -> pd.DataFrame:
    return _df("""
        SELECT
            SUBSTR(p.race_id, 5, 2)               AS venue_code,
            COUNT(*)                              AS bets,
            SUM(pr.is_hit)                        AS hits,
            SUM(p.recommended_bet)                AS invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND SUBSTR(p.race_id,5,2) BETWEEN '01' AND '10'
        GROUP BY venue_code
    """)


@st.cache_data(ttl=300)
def fetch_kelly_simulation() -> pd.DataFrame:
    """EV >= 1.0 のベットを時系列順に取得してケリーシミュレーション用データを返す。"""
    return _df("""
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
        ORDER BY p.created_at, pr.id
    """)


@st.cache_data(ttl=300)
def fetch_summary_stats() -> dict[str, Any]:
    rows = _q("""
        SELECT
            COUNT(pr.id)           AS total_bets,
            SUM(pr.is_hit)         AS total_hits,
            SUM(p.recommended_bet) AS total_invested,
            SUM(CASE WHEN pr.is_hit=1 THEN pr.payout ELSE 0 END) AS total_payout
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
    """)
    return rows[0] if rows else {}


# ╔══════════════════════════════════════════════════════════════════╗
# ║  UI コンポーネント                                                ║
# ╚══════════════════════════════════════════════════════════════════╝

def render_bias_panel(race_id: str) -> None:
    """当日バイアスパネルを描画する。"""
    bias = fetch_today_bias(race_id)
    inner = bias.get("inner_bias")
    front = bias.get("front_bias")
    count = bias.get("race_count", 0)

    st.markdown("#### 📊 当日馬場バイアス")
    c1, c2, c3 = st.columns(3)

    with c1:
        if inner is not None:
            pct = min(max((inner + 0.20) / 0.40, 0.0), 1.0)
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


def _eval_badge(grade: str | None) -> str:
    """調教評価を絵文字つき文字列に変換する。"""
    if grade is None:
        return "—"
    color = {"A": "🌸", "B": "🟠", "C": "🔵", "D": "⚫"}.get(grade, "⚪")
    return f"{color} {grade}"


def render_prerace_table(df: pd.DataFrame) -> None:
    """
    直前分析テーブルを描画する。

    EV >= 1.0 の行を赤背景でハイライト。
    🔥 は odds_velocity（ここでは推定）で付与。
    """
    if df.empty:
        st.info("出馬表データがありません（entries テーブルに未登録）")
        return

    # 表示用カラムを整理
    display = df.copy()

    # ev_score が NaN の場合のデフォルト
    display["ev_score"]       = pd.to_numeric(display.get("ev_score"), errors="coerce")
    display["model_score"]    = pd.to_numeric(display.get("model_score"), errors="coerce")
    display["recommended_bet"] = pd.to_numeric(display.get("recommended_bet"), errors="coerce")
    display["win_odds"]       = pd.to_numeric(display.get("win_odds"), errors="coerce")

    # Kelly 推奨額: recommended_bet は実際の賭け金（100〜400円）なので
    # kelly_fraction = recommended_bet / _KELLY_BASE として逆算して表示
    def _kelly_label(row: pd.Series) -> str:
        bet = row.get("recommended_bet")
        ev  = row.get("ev_score")
        if pd.isna(bet) if isinstance(bet, float) else bet is None:
            return "—"
        bet = float(bet)
        if bet <= 0 or (isinstance(ev, float) and ev < 1.0) or (ev is not None and ev < 1.0):
            return "—"
        return f"¥{int(bet):,}"

    display["kelly_bet"] = display.apply(_kelly_label, axis=1)

    # 調教評価バッジ
    display["調教"] = display.get("eval_grade", pd.Series(dtype=str)).apply(_eval_badge)

    # EV 表示（expected_value を優先、なければ ev_score）
    def _ev_label(row: pd.Series) -> str:
        ev = row.get("expected_value") if pd.notna(row.get("expected_value", float("nan"))) else row.get("ev_score")
        if ev is None or (isinstance(ev, float) and pd.isna(ev)):
            return "—"
        return f"🔥 {ev:.2f}" if ev >= 1.0 else f"{ev:.2f}"

    display["EV"]         = display.apply(_ev_label, axis=1)
    display["_ev_num"]    = pd.to_numeric(display.get("expected_value"), errors="coerce").fillna(
                             pd.to_numeric(display.get("ev_score"), errors="coerce").fillna(0)
                           )

    # 本命スコア(%)
    display["本命%"] = display["model_score"].apply(
        lambda x: f"{x*100:.1f}%" if pd.notna(x) else "—"
    )

    # オッズ
    display["単勝"] = display["win_odds"].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )

    # 馬体重(増減)
    def weight_str(row: pd.Series) -> str:
        w = row.get("horse_weight")
        d = row.get("horse_weight_diff")
        if pd.isna(w) if isinstance(w, float) else w is None:
            return "—"
        s = str(int(w))
        if d is not None and not (isinstance(d, float) and pd.isna(d)):
            sign = "+" if int(d) > 0 else ""
            s += f"({sign}{int(d)})"
        return s

    display["体重"] = display.apply(weight_str, axis=1)

    # 着順（結果確定後）
    display["着順"] = display.get("rank", pd.Series(dtype=str)).apply(
        lambda x: f"{int(x)}着" if pd.notna(x) and x is not None else "—"
    )

    # 表示列の選択と順序
    cols = {
        "horse_number": "馬番",
        "gate_number":  "枠",
        "horse_name":   "馬名",
        "sex_age":      "性齢",
        "weight_carried": "斤量",
        "jockey":       "騎手",
        "単勝":         "単勝",
        "popularity":   "人気",
        "体重":         "体重",
        "調教":         "調教",
        "本命%":        "本命%",
        "EV":           "EV",
        "kelly_bet":    "Kelly推奨",
        "着順":         "着順",
    }

    # 存在する列だけ使う
    available = {k: v for k, v in cols.items() if k in display.columns or k in [
        "単勝", "体重", "調教", "本命%", "EV", "kelly_bet", "着順"
    ]}

    out = pd.DataFrame()
    for src, dst in available.items():
        if src in display.columns:
            out[dst] = display[src].values
        elif src in ["単勝", "体重", "調教", "本命%", "EV", "kelly_bet", "着順"]:
            out[dst] = display[src].values if src in display.columns else "—"

    # EV >= 1.0 行を強調するためにスタイルを適用
    ev_col = out.get("EV", pd.Series(dtype=str))

    def _row_style(row: pd.Series):
        is_hot = str(row.get("EV", "")).startswith("🔥")
        bg = "background-color: rgba(255,50,80,0.15); color: #ff6080;" if is_hot else ""
        return [bg] * len(row)

    styled = out.style.apply(_row_style, axis=1)

    # 調教バッジ列は左寄せ
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=min(60 + len(out) * 42, 700),
    )

    # 激アツ推奨馬サマリー
    hot_mask = display["_ev_num"] >= 1.0
    hot_rows = display[hot_mask]
    if not hot_rows.empty:
        st.markdown("---")
        st.markdown("### 🔥 激アツ推奨馬（EV ≥ 1.0）")
        for _, h in hot_rows.sort_values("_ev_num", ascending=False).iterrows():
            ev   = h["_ev_num"]
            odds = h.get("win_odds")
            bet  = h.get("recommended_bet")
            name = h.get("horse_name", "")
            num  = h.get("horse_number", "")
            grade = h.get("eval_grade", "")
            grade_str = f"  |  調教 {_eval_badge(grade)}" if grade else ""
            bet_str   = f"  |  推奨: ¥{int(bet):,}" if pd.notna(bet) and float(bet) > 0 else ""
            odds_str  = f"  |  単勝 {odds:.1f}倍" if pd.notna(odds) else ""
            st.error(
                f"🔥 **{num}番 {name}**  |  EV = **{ev:.3f}**{odds_str}{grade_str}{bet_str}"
            )


def render_odds_alert(race_id: str) -> None:
    """オッズ時系列アラートを描画する。"""
    df = fetch_realtime_odds_trend(race_id)

    if df.empty:
        st.info("リアルタイムオッズデータなし（レース当日に prerace_pipeline を実行すると蓄積されます）")
        return

    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
    df["win_odds"]     = pd.to_numeric(df["win_odds"], errors="coerce")

    # 馬ごとに最古・最新オッズを比較してベロシティを計算
    summary_rows = []
    for horse_num, grp in df.groupby("horse_number"):
        grp = grp.sort_values("recorded_at")
        if len(grp) < 2:
            continue
        first_odds  = grp.iloc[0]["win_odds"]
        latest_odds = grp.iloc[-1]["win_odds"]
        elapsed_min = (grp.iloc[-1]["recorded_at"] - grp.iloc[0]["recorded_at"]).total_seconds() / 60
        if elapsed_min > 0 and pd.notna(first_odds) and pd.notna(latest_odds) and first_odds > 0:
            velocity     = (first_odds - latest_odds) / elapsed_min
            vs_morning   = latest_odds / first_odds
            summary_rows.append({
                "horse_number":  horse_num,
                "horse_name":    grp.iloc[-1]["horse_name"],
                "朝一オッズ":    first_odds,
                "現在オッズ":    latest_odds,
                "朝比率":        vs_morning,
                "下落速度(/分)": velocity,
                "シグナル":      "🔥 大口" if velocity >= _VELOCITY_THRESHOLD else "—",
            })

    if not summary_rows:
        st.info("オッズ変動データなし")
        return

    sumdf = pd.DataFrame(summary_rows).sort_values("下落速度(/分)", ascending=False)
    st.dataframe(sumdf, use_container_width=True, hide_index=True)

    # オッズ推移グラフ（全馬）
    fig = px.line(
        df, x="recorded_at", y="win_odds", color="horse_name",
        title="単勝オッズ推移",
        labels={"recorded_at": "時刻", "win_odds": "単勝オッズ", "horse_name": "馬名"},
        template="plotly_dark",
    )
    fig.update_layout(height=350, margin=dict(t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def render_payouts(race_id: str) -> None:
    """払戻テーブルを描画する。"""
    df = fetch_payouts(race_id)
    if df.empty:
        st.info("払戻データなし")
        return
    df["payout"] = df["payout"].apply(lambda x: f"¥{int(x):,}" if pd.notna(x) else "—")
    _BET_ORDER = {"単勝":1,"複勝":2,"枠連":3,"馬連":4,"ワイド":5,"馬単":6,"三連複":7,"三連単":8}
    df["_order"] = df["bet_type"].map(lambda x: _BET_ORDER.get(x, 99))
    df = df.sort_values(["_order", "popularity"]).drop(columns=["_order"])
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── Analytics ビュー ─────────────────────────────────────────────

def render_analytics() -> None:
    """月次 ROI・ドローダウン・会場別成績を描画する。"""
    st.markdown("## 📈 Analytics — 収益分析")

    # ── サマリーメトリクス ──────────────────────────────────────
    stats = fetch_summary_stats()
    if stats:
        invested = stats.get("total_invested") or 0
        payout   = stats.get("total_payout")   or 0
        bets     = stats.get("total_bets")      or 0
        hits     = stats.get("total_hits")      or 0
        roi      = payout / invested if invested > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("総ベット数",   f"{int(bets):,} 件")
        m2.metric("総的中数",     f"{int(hits):,} 件",   f"{hits/bets*100:.1f}%" if bets else "—")
        m3.metric("総投資額",     f"¥{int(invested):,}")
        m4.metric("ROI",          f"{roi:.3f}",           f"{(roi-1)*100:+.1f}%" if invested else "—")

    st.divider()

    # ── 月次 ROI グラフ ─────────────────────────────────────────
    monthly_df = fetch_monthly_roi()
    if monthly_df.empty:
        st.warning("月次データなし")
        return

    monthly_df["roi"]      = (monthly_df["payout"] / monthly_df["invested"].replace(0, pd.NA)).fillna(0)
    monthly_df["hit_rate"] = monthly_df["hits"] / monthly_df["bets"].replace(0, pd.NA)

    # 月×モデルの ROI 集計
    monthly_total = (
        monthly_df.groupby("month")
        .agg(invested=("invested", "sum"), payout=("payout", "sum"), bets=("bets", "sum"), hits=("hits", "sum"))
        .reset_index()
    )
    monthly_total["roi"]      = monthly_total["payout"] / monthly_total["invested"].replace(0, pd.NA)
    monthly_total["hit_rate"] = monthly_total["hits"] / monthly_total["bets"].replace(0, pd.NA)

    tab1, tab2, tab3 = st.tabs(["📊 月次ROI", "💰 ケリーシミュレーション", "🏇 会場別成績"])

    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Bar(
                x=monthly_total["month"],
                y=monthly_total["roi"],
                marker_color=[
                    "#00ff88" if r >= 1.0 else "#ff3366"
                    for r in monthly_total["roi"].fillna(0)
                ],
                name="月次ROI",
                text=[f"{r:.3f}" for r in monthly_total["roi"].fillna(0)],
                textposition="outside",
            ))
            fig_roi.add_hline(y=1.0, line_dash="dash", line_color="#ffd700",
                              annotation_text="損益分岐 (1.0)", annotation_position="top right")
            fig_roi.update_layout(
                title="月次 ROI",
                template="plotly_dark",
                height=360,
                margin=dict(t=40, b=30),
                yaxis_title="ROI",
                xaxis_title="月",
            )
            st.plotly_chart(fig_roi, use_container_width=True)

        with col2:
            fig_hit = px.line(
                monthly_total, x="month", y="hit_rate",
                title="月次的中率",
                labels={"month": "月", "hit_rate": "的中率"},
                template="plotly_dark",
                markers=True,
                color_discrete_sequence=["#00c8ff"],
            )
            fig_hit.update_layout(height=360, margin=dict(t=40, b=30))
            fig_hit.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig_hit, use_container_width=True)

        # モデル別詳細テーブル
        with st.expander("モデル別・券種別詳細"):
            detail = monthly_df.copy()
            detail["roi_pct"] = (detail["roi"] * 100).round(1).astype(str) + "%"
            detail["hit_rate"] = (detail["hit_rate"] * 100).round(1).astype(str) + "%"
            st.dataframe(
                detail[["month", "model_type", "bet_type", "bets", "hits",
                         "hit_rate", "invested", "payout", "roi_pct"]],
                use_container_width=True, hide_index=True,
            )

    with tab2:
        sim_df = fetch_kelly_simulation()
        if sim_df.empty:
            st.info("EV >= 1.0 のベット履歴がありません")
        else:
            bankroll    = 1_000_000.0
            peak        = bankroll
            max_dd      = 0.0
            series      = [{"date": sim_df.iloc[0]["created_at"], "bankroll": bankroll}]
            wins, total = 0, 0

            for _, row in sim_df.iterrows():
                actual_bet = min(float(row["recommended_bet"]), bankroll * 0.10)
                if actual_bet <= 0 or bankroll <= 0:
                    continue
                profit   = float(row["payout"]) - actual_bet if row["is_hit"] else -actual_bet
                bankroll += profit
                if bankroll > peak:
                    peak = bankroll
                dd       = (peak - bankroll) / peak if peak > 0 else 0
                max_dd   = max(max_dd, dd)
                if row["is_hit"]:
                    wins += 1
                total   += 1
                series.append({"date": row["created_at"], "bankroll": bankroll})

            series_df = pd.DataFrame(series)
            series_df["date"] = pd.to_datetime(series_df["date"])

            # 資金推移
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
                template="plotly_dark",
                height=380,
                margin=dict(t=40, b=30),
                yaxis_title="資金 (円)",
                xaxis_title="日付",
            )
            st.plotly_chart(fig_bk, use_container_width=True)

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("最終資金",       f"¥{bankroll:,.0f}")
            k2.metric("最大ドローダウン", f"{max_dd*100:.1f}%")
            k3.metric("的中率",         f"{wins/total*100:.1f}%" if total else "—")
            k4.metric("総ベット",        f"{total:,} 件")

    with tab3:
        venue_df = fetch_venue_performance()
        if venue_df.empty:
            st.info("会場別データなし")
        else:
            venue_df["venue"] = venue_df["venue_code"].map(
                lambda c: _JYO.get(c, c)
            )
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
            fig_v.update_layout(height=380, margin=dict(t=40, b=30),
                                 coloraxis_showscale=False)
            fig_v.update_traces(textposition="outside")
            st.plotly_chart(fig_v, use_container_width=True)

            display_cols = ["venue", "bets", "hits", "hit_rate", "invested", "payout", "roi"]
            tbl = venue_df[display_cols].copy()
            tbl["hit_rate"] = (tbl["hit_rate"] * 100).round(1).astype(str) + "%"
            tbl["roi"]      = tbl["roi"].round(3)
            tbl["invested"] = tbl["invested"].apply(lambda x: f"¥{int(x):,}")
            tbl["payout"]   = tbl["payout"].apply(lambda x: f"¥{int(x):,}")
            tbl.columns     = ["会場", "ベット数", "的中", "的中率", "投資額", "回収額", "ROI"]
            st.dataframe(tbl, use_container_width=True, hide_index=True)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  メインアプリ                                                     ║
# ╚══════════════════════════════════════════════════════════════════╝

def main() -> None:
    st.set_page_config(
        page_title="UMALOGI — Pro Dashboard",
        page_icon="🏇",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── グローバル CSS ─────────────────────────────────────────
    st.markdown("""
    <style>
      [data-testid="stSidebar"] { background: #060f1e; }
      .stMetric label            { font-size: 0.78rem !important; letter-spacing: 0.1em; }
      .stMetric .metric-container { background: rgba(0,200,255,0.05);
                                     border: 1px solid rgba(0,200,255,0.18);
                                     border-radius: 8px; padding: 12px; }
      div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
      h1, h2, h3 { letter-spacing: 0.05em; }
    </style>
    """, unsafe_allow_html=True)

    # ── サイドバー ─────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 🏇 UMALOGI")
        st.markdown("*Pro Investor Dashboard*")
        st.divider()

        view = st.radio(
            "ビュー選択",
            ["🏇 レース分析", "📈 Analytics"],
            label_visibility="collapsed",
        )

    # ── Analytics ビュー ───────────────────────────────────────
    if view == "📈 Analytics":
        render_analytics()
        return

    # ── レース分析ビュー ───────────────────────────────────────
    with st.sidebar:
        st.markdown("### 📅 レース選択")

        dates = fetch_available_dates()
        if not dates:
            st.warning("予想データがありません")
            st.stop()

        selected_date = st.selectbox("開催日", dates, index=0)

        venues = fetch_venues_for_date(selected_date)
        if not venues:
            st.warning("この日の会場データなし")
            st.stop()

        venue_options = {f"{name}（{code}）": code for code, name in venues}
        selected_venue_label = st.selectbox("会場", list(venue_options.keys()))
        selected_venue = venue_options[selected_venue_label]

        races_df = fetch_races_for_venue(selected_date, selected_venue)
        if races_df.empty:
            st.warning("レースデータなし")
            st.stop()

        race_options = {
            f"R{int(row['race_number']):02d}  ({row['race_id']})": row["race_id"]
            for _, row in races_df.iterrows()
        }
        selected_race_label = st.selectbox("レース", list(race_options.keys()))
        selected_race_id    = race_options[selected_race_label]

        st.divider()
        st.caption(f"race_id: `{selected_race_id}`")

    # ── メインエリア ────────────────────────────────────────────
    venue_name = _JYO.get(selected_venue, selected_venue)
    race_no    = int(selected_race_id[10:12])
    st.markdown(f"# 🏇 {selected_date}  {venue_name}  第 {race_no} R")
    st.markdown(f"`{selected_race_id}`")

    # タブ定義
    tab_prerace, tab_odds, tab_result, tab_payout = st.tabs(
        ["🔍 AI直前分析", "📡 オッズ動向", "📋 レース結果", "💴 払戻金"]
    )

    # ── AI直前分析タブ ─────────────────────────────────────────
    with tab_prerace:
        render_bias_panel(selected_race_id)
        st.divider()
        prerace_df = fetch_race_prerace(selected_race_id)
        render_prerace_table(prerace_df)

    # ── オッズ動向タブ ─────────────────────────────────────────
    with tab_odds:
        render_odds_alert(selected_race_id)

    # ── レース結果タブ ─────────────────────────────────────────
    with tab_result:
        if prerace_df.empty:
            st.info("出走データなし")
        else:
            result_df = prerace_df[prerace_df["rank"].notna()].copy()
            if result_df.empty:
                st.info("レース結果未確定")
            else:
                result_df = result_df.sort_values("rank")
                display   = result_df[[c for c in [
                    "rank", "gate_number", "horse_number", "horse_name",
                    "sex_age", "weight_carried", "jockey",
                    "finish_time", "win_odds", "popularity",
                    "horse_weight", "horse_weight_diff",
                ] if c in result_df.columns]].copy()
                display.columns = [
                    c.replace("rank", "着順").replace("gate_number", "枠")
                     .replace("horse_number", "馬番").replace("horse_name", "馬名")
                     .replace("sex_age", "性齢").replace("weight_carried", "斤量")
                     .replace("jockey", "騎手").replace("finish_time", "タイム")
                     .replace("win_odds", "単勝").replace("popularity", "人気")
                     .replace("horse_weight", "体重").replace("horse_weight_diff", "増減")
                    for c in display.columns
                ]
                st.dataframe(display, use_container_width=True, hide_index=True)

    # ── 払戻金タブ ─────────────────────────────────────────────
    with tab_payout:
        render_payouts(selected_race_id)


if __name__ == "__main__":
    main()
