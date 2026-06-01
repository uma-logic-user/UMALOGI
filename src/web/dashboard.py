"""src/web/dashboard.py — UMALOGI 成果可視化ダッシュボード（Streamlit）。

起動:
    streamlit run src/web/dashboard.py

表示内容:
    1. 直近レース結果（1 着馬・オッズ）
    2. 本日（最新予想日）の EV 上位馬一覧
    3. モデル別 ROI / 的中率の Plotly グラフ（Pure_EV_Edge 等）

DB は **読み取り専用接続** で開くため、常駐スケジューラ（書き込み側）と
競合せず安全に起動できる。データ取得ロジックは src/web/dashboard_data.py に分離し、
本ファイルは描画のみを担う。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import streamlit as st

from src.web import dashboard_data as dd

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "umalogi.db"


def resolve_db_path() -> Path:
    """環境変数 DB_PATH を優先し、なければ既定の data/umalogi.db を返す。"""
    env_path = os.environ.get("DB_PATH")
    return Path(env_path) if env_path else _DEFAULT_DB_PATH


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """ダッシュボード用の読み取り専用 DB 接続を返す。

    URI モード `mode=ro` で開くため、稼働中スケジューラの書き込みと衝突しない。
    DB ファイルが存在しない場合は分かりやすい例外を送出する。

    Args:
        db_path: 接続先。None なら resolve_db_path() を使用。

    Returns:
        row_factory=sqlite3.Row を設定した読み取り専用接続。
    """
    path = db_path or resolve_db_path()
    if not path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ── 描画ヘルパー ──────────────────────────────────────────────────────────────


def _roi_bar_figure(rows: list[dict[str, Any]]) -> go.Figure:
    """モデル別 ROI の横棒グラフを構築する（100% 損益分岐ラインを表示）。"""
    ordered = sorted(rows, key=lambda r: r["roi"])
    labels = [r["model_type"] for r in ordered]
    rois = [r["roi"] for r in ordered]
    colors = ["#2e9e5b" if v >= 100.0 else "#c0392b" for v in rois]

    fig = go.Figure(
        go.Bar(
            x=rois,
            y=labels,
            orientation="h",
            marker_color=colors,
            text=[f"{v:.1f}%" for v in rois],
            textposition="outside",
        )
    )
    fig.add_vline(
        x=100.0,
        line_dash="dash",
        line_color="#888",
        annotation_text="損益分岐 100%",
        annotation_position="top",
    )
    fig.update_layout(
        title="モデル別 回収率（ROI）",
        xaxis_title="ROI (%)",
        yaxis_title="モデル",
        template="plotly_dark",
        height=max(320, 48 * len(labels) + 120),
        margin=dict(l=10, r=40, t=60, b=40),
    )
    return fig


def _hit_rate_bar_figure(rows: list[dict[str, Any]]) -> go.Figure:
    """モデル別 的中率の横棒グラフを構築する。"""
    ordered = sorted(rows, key=lambda r: r["hit_rate"])
    labels = [r["model_type"] for r in ordered]
    hits = [r["hit_rate"] for r in ordered]

    fig = go.Figure(
        go.Bar(
            x=hits,
            y=labels,
            orientation="h",
            marker_color="#3498db",
            text=[f"{v:.1f}%" for v in hits],
            textposition="outside",
        )
    )
    fig.update_layout(
        title="モデル別 的中率",
        xaxis_title="的中率 (%)",
        yaxis_title="モデル",
        template="plotly_dark",
        height=max(320, 48 * len(labels) + 120),
        margin=dict(l=10, r=40, t=60, b=40),
    )
    return fig


# ── メイン描画 ────────────────────────────────────────────────────────────────


def render(conn: sqlite3.Connection) -> None:
    """与えられた接続を用いてダッシュボード全体を描画する。"""
    st.title("🏇 UMALOGI ダッシュボード")
    st.caption("AI 競馬予測プラットフォーム — 成果の可視化")

    # ── サイドバー: 集計条件 ──
    st.sidebar.header("集計条件")
    live_only = st.sidebar.toggle(
        "実弾のみ（単複・実弾モデル）",
        value=True,
        help="観賞用モデル（Oracle / HitFocus 等）を ROI 集計から除外します。",
    )
    since_text = st.sidebar.text_input(
        "集計開始日 (YYYY-MM-DD・空=全期間)",
        value="",
        help="predictions.created_at >= この日付 で絞り込みます。",
    )
    since = since_text.strip() or None

    # ── セクション 1: 直近結果 & 本日の EV 上位 ──
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📋 直近レース結果")
        results = dd.recent_results(conn, limit=20)
        if results:
            st.dataframe(
                [
                    {
                        "日付": r["date"],
                        "会場": r["venue"],
                        "R": r["race_number"],
                        "レース名": r["race_name"],
                        "1着": r["winner"],
                        "単勝": r["win_odds"],
                        "人気": r["popularity"],
                    }
                    for r in results
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("確定済みレース結果がまだありません。")

    with col_right:
        latest = dd.latest_prediction_date(conn)
        st.subheader(f"⭐ EV 上位馬{f'（{latest}）' if latest else ''}")
        ev_horses = dd.top_ev_horses(conn, limit=20)
        if ev_horses:
            st.dataframe(
                [
                    {
                        "会場": r["venue"],
                        "R": r["race_number"],
                        "モデル": r["model_type"],
                        "券種": r["bet_type"],
                        "馬": r["horse_name"],
                        "EV": round(r["expected_value"], 2)
                        if r["expected_value"] is not None
                        else None,
                        "信頼度": round(r["confidence"], 2)
                        if r["confidence"] is not None
                        else None,
                    }
                    for r in ev_horses
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("有効な予想がまだありません。")

    st.divider()

    # ── セクション 2: モデル別 ROI / 的中率 ──
    st.subheader("📈 モデル別パフォーマンス")
    roi_rows = dd.model_roi_table(conn, since=since, live_only=live_only)
    if not roi_rows:
        st.warning("集計対象の確定実績がありません（条件を緩めてください）。")
        return

    # サマリー指標（全体）。
    total_payout = sum(r["payout"] for r in roi_rows)
    total_cost = sum(r["cost"] for r in roi_rows)
    total_n = sum(r["n"] for r in roi_rows)
    overall_roi = (100.0 * total_payout / total_cost) if total_cost > 0 else 0.0
    m1, m2, m3 = st.columns(3)
    m1.metric("総ベット数", f"{total_n:,}")
    m2.metric("総回収率", f"{overall_roi:.1f}%")
    m3.metric("総損益", f"¥{total_payout - total_cost:,.0f}")

    g_left, g_right = st.columns(2)
    with g_left:
        st.plotly_chart(_roi_bar_figure(roi_rows), use_container_width=True)
    with g_right:
        st.plotly_chart(_hit_rate_bar_figure(roi_rows), use_container_width=True)

    st.dataframe(
        [
            {
                "モデル": r["model_type"],
                "ベット数": r["n"],
                "ROI(%)": r["roi"],
                "的中率(%)": r["hit_rate"],
                "払戻": r["payout"],
                "コスト": r["cost"],
                "損益": r["profit"],
            }
            for r in roi_rows
        ],
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    """Streamlit エントリポイント。read-only 接続を開いて描画する。"""
    st.set_page_config(
        page_title="UMALOGI ダッシュボード", page_icon="🏇", layout="wide"
    )
    try:
        conn = get_connection()
    except FileNotFoundError as exc:
        st.error(str(exc))
        return
    try:
        render(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
