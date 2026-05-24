"""
umasugi_engine バックテストスクリプト

直近30日の prediction_results を用い、
legacy_logic の全予想 vs umasugi フィルター適用後の予想で ROI を比較する。

使用方法:
    py scripts/backtest_umasugi.py [--days 30] [--threshold 0.45]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from typing import Any

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

from src.umasugi_engine.factors.track_style import calc_track_style_score
from src.umasugi_engine.factors.training_grade import calc_training_grade_score
from src.umasugi_engine.factors.turf_type import calc_turf_type_score


# ── 定数 ─────────────────────────────────────────────────────────────────────
UMASUGI_THRESHOLD = 0.45  # このスコア未満の予想はフィルタリング
_BATCH_SIZE = 200          # race_id をまとめてクエリする単位


def _parse_combo_horse_nums(combination_json: str | None) -> list[int]:
    """combination_json から馬番リストを返す。"""
    if not combination_json:
        return []
    try:
        combos = json.loads(combination_json)
        nums: set[int] = set()
        for c in combos:
            if isinstance(c, list):
                for n in c:
                    if isinstance(n, int):
                        nums.add(n)
            elif isinstance(c, int):
                nums.add(c)
        return list(nums)
    except Exception:
        return []


def _build_horse_map(conn: sqlite3.Connection, race_ids: list[str]) -> dict[tuple[str, int], str]:
    """(race_id, horse_number) → horse_id マップを構築する。"""
    if not race_ids:
        return {}
    ph = ",".join("?" * len(race_ids))
    rows = conn.execute(
        f"SELECT race_id, horse_number, horse_id FROM race_results WHERE race_id IN ({ph})",
        race_ids,
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def _compute_umasugi_scores(
    conn: sqlite3.Connection,
    race_horse_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    """
    (race_id, horse_id) ペアのリストに対して umasugi_score を返す。

    Phase2 ウェイト（2026-05-24 データ拡張後）:
      legacy 0.57固定 / track 0.10 / turf 0.15 / training_grade 0.08
      / odds_momentum 0.05固定(リアルタイムのためbacktest除外) / crowd 0.05固定
    """
    if not race_horse_pairs:
        return {}

    df = pd.DataFrame(race_horse_pairs, columns=["race_id", "horse_id"])
    df = df.drop_duplicates()

    df = calc_track_style_score(df, conn)
    df = calc_turf_type_score(df, conn)
    df = calc_training_grade_score(df, conn)
    # odds_momentum はリアルタイムのため backtest では 0.5 固定

    df["umasugi_score"] = (
        0.57 * 0.5                                           # legacy 固定
        + 0.10 * df["track_style_score"].fillna(0.5)
        + 0.15 * df["turf_type_score"].fillna(0.5)
        + 0.08 * df["training_grade_score"].fillna(0.5)
        + 0.05 * 0.5                                         # odds_momentum 固定
        + 0.05 * 0.5                                         # crowd 固定
    ).clip(0.0, 1.0)

    return {
        (row["race_id"], row["horse_id"]): row["umasugi_score"]
        for _, row in df.iterrows()
    }


def run_backtest(days: int = 30, threshold: float = UMASUGI_THRESHOLD) -> None:
    conn = sqlite3.connect("data/umalogi.db")

    # ── Step 1: 対象 predictions を取得 ─────────────────────────────────────
    print(f"\n{'='*60}")
    print(f" umasugi_engine バックテスト (直近 {days} 日 / 閾値 {threshold})")
    print(f"{'='*60}")

    rows = conn.execute(
        f"""
        SELECT
            p.id, p.race_id, p.bet_type, p.combination_json,
            p.expected_value, p.recommended_bet,
            pr.is_hit, pr.profit, pr.payout
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        WHERE p.created_at >= date('now', '-{days} days')
          AND pr.is_hit IS NOT NULL
          AND p.race_id IS NOT NULL
        ORDER BY p.race_id
        """,
    ).fetchall()

    df = pd.DataFrame(rows, columns=[
        "pred_id", "race_id", "bet_type", "combination_json",
        "expected_value", "recommended_bet",
        "is_hit", "profit", "payout",
    ])
    print(f"\n対象予想数: {len(df):,} 件 / レース数: {df['race_id'].nunique():,}")

    # ── Step 2: 馬番 → horse_id マップ ───────────────────────────────────────
    race_ids = df["race_id"].unique().tolist()
    horse_map = _build_horse_map(conn, race_ids)

    # ── Step 3: 各予想の代表馬番・horse_id を特定 ────────────────────────────
    def _get_primary_horse_id(row: pd.Series) -> str | None:
        nums = _parse_combo_horse_nums(row["combination_json"])
        if not nums:
            return None
        # 複数馬番の場合は最初の馬番を代表とする（単勝/複勝の軸）
        for n in sorted(nums):
            hid = horse_map.get((row["race_id"], n))
            if hid:
                return hid
        return None

    df["horse_id"] = df.apply(_get_primary_horse_id, axis=1)
    df = df.dropna(subset=["horse_id"])
    print(f"horse_id 紐付け成功: {len(df):,} 件")

    # ── Step 4: umasugi_score を計算（バッチ処理） ────────────────────────────
    print("\numasugi_score を計算中...")
    pairs = list(df[["race_id", "horse_id"]].drop_duplicates().itertuples(index=False, name=None))
    score_map = _compute_umasugi_scores(conn, pairs)
    df["umasugi_score"] = df.apply(
        lambda r: score_map.get((r["race_id"], r["horse_id"]), 0.5), axis=1
    )

    # ── Step 5: Legacy vs Umasugi ROI 比較 ────────────────────────────────
    def _roi(sub: pd.DataFrame) -> dict[str, Any]:
        if sub.empty:
            return {"count": 0, "staked": 0, "payout": 0, "roi": None}
        staked = sub.apply(
            lambda r: -r["profit"] if r["is_hit"] == 0 else (r["payout"] - r["profit"]),
            axis=1,
        ).sum()
        payout = sub["payout"].sum()
        profit = sub["profit"].sum()
        roi = (payout / staked * 100) if staked > 0 else None
        hits = sub["is_hit"].sum()
        return {
            "count": len(sub),
            "hits": int(hits),
            "hit_rate": hits / len(sub) * 100 if len(sub) > 0 else 0,
            "staked": int(staked),
            "payout": int(payout),
            "profit": int(profit),
            "roi": round(roi, 1) if roi else None,
        }

    legacy  = _roi(df)
    umasugi = _roi(df[df["umasugi_score"] >= threshold])
    filtered_out = _roi(df[df["umasugi_score"] < threshold])

    # ── Step 6: 結果表示 ──────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"{'指標':<20} {'Legacy':>12} {'Umasugi':>12} {'除外分':>12}")
    print(f"{'─'*60}")
    for key, label in [
        ("count", "予想件数"),
        ("hits", "的中件数"),
        ("hit_rate", "的中率 (%)"),
        ("staked", "投資額 (¥)"),
        ("payout", "払戻額 (¥)"),
        ("profit", "純損益 (¥)"),
        ("roi", "ROI (%)"),
    ]:
        lv = legacy.get(key)
        um = umasugi.get(key)
        fo = filtered_out.get(key)
        def fmt(v: Any) -> str:
            if v is None:
                return "  N/A"
            if isinstance(v, float):
                return f"{v:>12.1f}"
            return f"{v:>12,}"
        print(f"{label:<20} {fmt(lv)} {fmt(um)} {fmt(fo)}")
    print(f"{'─'*60}")

    # ── Step 7: umasugi_score 分布 ────────────────────────────────────────────
    print("\n【umasugi_score 分布】")
    bins = [0.0, 0.35, 0.40, 0.43, 0.45, 0.47, 0.50, 0.53, 0.55, 1.01]
    labels_b = ["<0.35", "0.35-0.40", "0.40-0.43", "0.43-0.45",
                "0.45-0.47", "0.47-0.50", "0.50-0.53", "0.53-0.55", "≥0.55"]
    df["score_bin"] = pd.cut(df["umasugi_score"], bins=bins, labels=labels_b, right=False)
    dist = df.groupby("score_bin", observed=True).agg(
        count=("pred_id", "count"),
        hit_rate=("is_hit", lambda x: x.mean() * 100),
        avg_profit=("profit", "mean"),
    ).reset_index()
    print(f"{'スコア帯':<12} {'件数':>8} {'的中率':>8} {'平均損益':>12}")
    for _, r in dist.iterrows():
        print(f"  {str(r['score_bin']):<10} {int(r['count']):>8,} {r['hit_rate']:>7.1f}% {r['avg_profit']:>12,.0f}")

    # ── Step 8: 改善が大きかった除外ケース（的中=0・除外正解） ──────────────
    print("\n【除外して正解だったケース TOP 5（損失回避）】")
    avoided_losses = df[
        (df["umasugi_score"] < threshold) & (df["is_hit"] == 0)
    ].nlargest(5, "profit", keep="all")  # profit が最も 0 に近い = 大きな損失を回避

    # 実際には profit が最も小さい（損失が大きい）ケースを避けた = profitが負で大きいもの
    avoided_losses = df[
        (df["umasugi_score"] < threshold) & (df["is_hit"] == 0)
    ].nsmallest(5, "profit")
    for _, r in avoided_losses.iterrows():
        print(f"  race={r['race_id']} type={r['bet_type']} score={r['umasugi_score']:.3f} profit={r['profit']:,.0f}")

    # ── Step 9: 閾値感度分析 ─────────────────────────────────────────────────
    print("\n【閾値感度分析】")
    print(f"{'閾値':<8} {'件数':>8} {'ROI %':>10} {'損益':>14}")
    for th in [0.35, 0.40, 0.43, 0.45, 0.47, 0.50]:
        sub = df[df["umasugi_score"] >= th]
        r = _roi(sub)
        roi_str = f"{r['roi']:.1f}" if r["roi"] else "N/A"
        print(f"  ≥{th:.2f}  {r['count']:>8,} {roi_str:>10} {r['profit']:>14,}")

    print("\n【最適閾値の推奨】")
    best_th, best_roi = threshold, legacy["roi"] or 0
    for th in [0.35, 0.40, 0.43, 0.45, 0.47, 0.50]:
        sub = df[df["umasugi_score"] >= th]
        r = _roi(sub)
        if r["roi"] and r["roi"] > best_roi and r["count"] > 50:
            best_roi = r["roi"]
            best_th = th
    print(f"  → 閾値 {best_th:.2f} (ROI {best_roi:.1f}%) を推奨")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--threshold", type=float, default=UMASUGI_THRESHOLD)
    args = parser.parse_args()
    run_backtest(days=args.days, threshold=args.threshold)
