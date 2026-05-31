"""
src/ml/pnl_accounting.py — 実弾(ライブ)ベットの真ROI会計

唯一の正しいコスト基準で P&L を集計する:
  実コスト = payout - profit  （prediction_results.profit は ¥100×点数 で算出済み）
  真ROI    = SUM(payout) / SUM(実コスト) × 100

`recommended_bet`(Kelly推奨額)は単価不統一バグがあるため**コスト基準に使わない**。
実弾の定義は src/ml/bet_policy.is_live_bet() に従い、観賞用(Oracle/HitFocus)・
三連系・馬連・馬単・ワイドを除外する。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from src.ml.bet_policy import is_live_bet


def compute_live_roi(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
    live_only: bool = True,
) -> dict[str, Any]:
    """実弾(単勝/複勝・実弾モデル)の確定 P&L を真コスト基準で集計する。

    Args:
        conn:      DB 接続。
        since:     "YYYY-MM-DD"。指定時は predictions.created_at >= since のみ。
        live_only: True なら is_live_bet() で実弾のみに絞る。False なら全予想。

    Returns:
        {n, cost, payout, profit, roi, hit_rate, by_model: {...}, by_bet_type: {...}}
        roi/hit_rate は %。
    """
    # is_superseded=1（直前再推論で論理無効化された旧予想）は二重計上を避けるため除外。
    where = "WHERE pr.payout IS NOT NULL AND COALESCE(p.is_superseded, 0) = 0"
    params: list[Any] = []
    if since:
        where += " AND p.created_at >= ?"
        params.append(since)

    rows = conn.execute(
        f"""
        SELECT p.model_type, p.bet_type,
               COALESCE(pr.payout, 0)  AS payout,
               COALESCE(pr.profit, 0)  AS profit,
               COALESCE(pr.is_hit, 0)  AS is_hit
          FROM predictions p
          JOIN prediction_results pr ON pr.prediction_id = p.id
          {where}
        """,
        params,
    ).fetchall()

    agg: dict[str, list[float]] = {}  # key -> [n, cost, payout, profit, hits]
    by_model: dict[str, list[float]] = {}
    by_bet: dict[str, list[float]] = {}

    def _add(
        d: dict[str, list[float]],
        key: str,
        cost: float,
        pay: float,
        prof: float,
        hit: float,
    ) -> None:
        e = d.setdefault(key, [0, 0.0, 0.0, 0.0, 0.0])
        e[0] += 1
        e[1] += cost
        e[2] += pay
        e[3] += prof
        e[4] += hit

    total = [0, 0.0, 0.0, 0.0, 0.0]
    for model_type, bet_type, payout, profit, is_hit in rows:
        if live_only and not is_live_bet(model_type, bet_type):
            continue
        cost = payout - profit  # 真コスト = ¥100 × 点数
        total[0] += 1
        total[1] += cost
        total[2] += payout
        total[3] += profit
        total[4] += is_hit
        _add(by_model, model_type, cost, payout, profit, is_hit)
        _add(by_bet, bet_type, cost, payout, profit, is_hit)

    def _summarize(e: list[float]) -> dict[str, float]:
        n, cost, pay, prof, hits = e
        return {
            "n": int(n),
            "cost": round(cost, 1),
            "payout": round(pay, 1),
            "profit": round(prof, 1),
            "roi": round(100.0 * pay / cost, 1) if cost > 0 else 0.0,
            "hit_rate": round(100.0 * hits / n, 1) if n > 0 else 0.0,
        }

    result = _summarize(total)
    result["by_model"] = {k: _summarize(v) for k, v in by_model.items()}
    result["by_bet_type"] = {k: _summarize(v) for k, v in by_bet.items()}
    return result


# 従来単複バリアント（Pure_EV_Edge フィルタ「非適用」側）の実弾モデル
_LEGACY_TANPUKU_MODELS = ("本命", "卍", "Alpha-Payout")
_PURE_EV_MODEL = "Pure_EV_Edge"


def compute_ab_variants(
    conn: sqlite3.Connection, *, since: str | None = None
) -> dict[str, Any]:
    """W-057 シャドーA/B: Pure_EV_Edge フィルタ「適用」vs「非適用(従来単複)」の確定P&L比較。

    両バリアントとも実弾券種(単勝/複勝)・コスト=payout−profit・is_superseded除外で集計し、
    どちらが利益を出しているか（純益差・ROI差・勝者）を返す。

    - 適用(pure_ev)   : model_type 基底 = "Pure_EV_Edge"
    - 非適用(legacy)  : model_type 基底 ∈ 本命/卍/Alpha-Payout（従来の単複ロジック）

    Returns:
        {pure_ev:{...}, legacy:{...}, diff_profit, diff_roi, winner, both_active}
    """
    from src.ml.bet_policy import base_model as _base

    where = "WHERE pr.payout IS NOT NULL AND COALESCE(p.is_superseded, 0) = 0"
    params: list[Any] = []
    if since:
        where += " AND p.created_at >= ?"
        params.append(since)
    rows = conn.execute(
        f"""
        SELECT p.model_type, p.bet_type,
               COALESCE(pr.payout, 0), COALESCE(pr.profit, 0), COALESCE(pr.is_hit, 0)
          FROM predictions p
          JOIN prediction_results pr ON pr.prediction_id = p.id
          {where}
        """,
        params,
    ).fetchall()

    pure = [0, 0.0, 0.0, 0.0, 0.0]  # n, cost, payout, profit, hits
    legacy = [0, 0.0, 0.0, 0.0, 0.0]
    for model_type, bet_type, payout, profit, is_hit in rows:
        if bet_type not in ("単勝", "複勝"):
            continue
        b = _base(model_type)
        if b == _PURE_EV_MODEL:
            tgt = pure
        elif b in _LEGACY_TANPUKU_MODELS:
            tgt = legacy
        else:
            continue
        tgt[0] += 1
        tgt[1] += payout - profit
        tgt[2] += payout
        tgt[3] += profit
        tgt[4] += is_hit

    def _s(e: list[float]) -> dict[str, float]:
        n, cost, pay, prof, hits = e
        return {
            "n": int(n),
            "cost": round(cost, 1),
            "payout": round(pay, 1),
            "profit": round(prof, 1),
            "roi": round(100.0 * pay / cost, 1) if cost > 0 else 0.0,
            "hit_rate": round(100.0 * hits / n, 1) if n > 0 else 0.0,
        }

    pe, lg = _s(pure), _s(legacy)
    diff_profit = round(pe["profit"] - lg["profit"], 1)
    diff_roi = round(pe["roi"] - lg["roi"], 1)
    both = pure[0] > 0 and legacy[0] > 0
    if not both:
        winner = "判定不能(片側データなし)"
    elif pe["roi"] > lg["roi"]:
        winner = "Pure_EV_Edge"
    elif lg["roi"] > pe["roi"]:
        winner = "従来単複"
    else:
        winner = "互角"
    return {
        "pure_ev": pe,
        "legacy": lg,
        "diff_profit": diff_profit,
        "diff_roi": diff_roi,
        "winner": winner,
        "both_active": both,
    }
