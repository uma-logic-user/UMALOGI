"""
UMALOGI モデル特性分析バッチ

予想実績データを多軸で分析し、「どの条件でモデルが稼げているか/いないか」を
自動的に明らかにする。分析軸:
  - 開催地（東京/阪神/中山...）
  - 馬場状態（良/稍重/重/不良）
  - クラス（G1/G2/G3/OP/3勝/2勝/1勝/未勝利）
  - 距離帯（短距離<1400/マイル1400-1800/中距離1800-2200/長距離>2200）
  - モデル種別（本命/卍/Pure_EV_Edge）
  - 券種（単勝/複勝）

出力: JSON + コンソール表示

使い方:
    py scripts/analyze_model_traits.py
    py scripts/analyze_model_traits.py --since 2025-01-01
    py scripts/analyze_model_traits.py --model 本命 --out results.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# セグメント別 ROI 集計
# ─────────────────────────────────────────────────────────────────────────────

_BASE_SQL = """
    SELECT
        {segment_col} AS segment,
        COUNT(*) AS n_bets,
        SUM(CASE WHEN pr.is_hit=1 THEN 1 ELSE 0 END) AS hits,
        SUM(COALESCE(pr.payout, 0)) AS total_payout,
        SUM(COALESCE(pr.profit, 0)) AS total_profit,
        -- bet_policy.flat_cost = ¥100 × 点数として近似（recommended_bet が実コスト）
        SUM(COALESCE(ABS(pr.profit - pr.payout), 100)) AS total_cost
    FROM predictions p
    JOIN races r ON r.race_id = p.race_id
    JOIN prediction_results pr ON pr.prediction_id = p.id
    WHERE COALESCE(p.is_superseded, 0) = 0
      AND pr.is_hit IS NOT NULL
      AND r.date >= :since
      AND r.date <= :until
      {model_filter}
      {bet_type_filter}
      AND {segment_col} IS NOT NULL
      AND {segment_col} != ''
    GROUP BY {segment_col}
    HAVING n_bets >= :min_bets
    ORDER BY total_profit DESC
"""


def _distance_band_expr() -> str:
    return """CASE
        WHEN r.distance < 1400 THEN '短距離(<1400)'
        WHEN r.distance < 1800 THEN 'マイル(1400-1800)'
        WHEN r.distance < 2200 THEN '中距離(1800-2200)'
        ELSE '長距離(>=2200)'
    END"""


def _class_expr() -> str:
    """race_name から簡易クラス推定。"""
    return """CASE
        WHEN r.race_name LIKE '%G1%' OR r.race_name LIKE '%グランプリ%' THEN 'G1'
        WHEN r.race_name LIKE '%G2%' THEN 'G2'
        WHEN r.race_name LIKE '%G3%' THEN 'G3'
        WHEN r.race_name LIKE '%オープン%' OR r.race_name LIKE '%OP%' THEN 'OP'
        WHEN r.race_name LIKE '%3勝%' OR r.race_name LIKE '%1600万%' THEN '3勝'
        WHEN r.race_name LIKE '%2勝%' OR r.race_name LIKE '%1000万%' THEN '2勝'
        WHEN r.race_name LIKE '%1勝%' OR r.race_name LIKE '%500万%' THEN '1勝'
        WHEN r.race_name LIKE '%未勝利%' THEN '未勝利'
        WHEN r.race_name LIKE '%新馬%' THEN '新馬'
        ELSE 'その他'
    END"""


def run_segment_analysis(
    conn: sqlite3.Connection,
    since: str,
    until: str,
    min_bets: int = 10,
    model_filter: str = "",
) -> dict[str, Any]:
    """全軸でセグメント分析を実行し辞書で返す。"""
    results: dict[str, Any] = {}

    segments = {
        "venue": "r.venue",
        "surface": "r.surface",
        "condition": "r.condition",
        "distance_band": _distance_band_expr(),
        "class": _class_expr(),
        "model_type": "p.model_type",
        "bet_type": "p.bet_type",
    }

    model_sql = f"AND p.model_type LIKE '%{model_filter}%'" if model_filter else ""

    for axis, col_expr in segments.items():
        sql = _BASE_SQL.format(
            segment_col=col_expr,
            model_filter=model_sql,
            bet_type_filter="",
        )
        rows = conn.execute(
            sql, {"since": since, "until": until, "min_bets": min_bets}
        ).fetchall()

        axis_result = []
        for row in rows:
            n = row["n_bets"]
            cost = row["total_cost"] or (n * 100)
            roi = round(row["total_payout"] / max(cost, 1) * 100, 1)
            hit_rate = round(row["hits"] / max(n, 1) * 100, 1)
            axis_result.append(
                {
                    "segment": row["segment"],
                    "n_bets": n,
                    "hits": row["hits"],
                    "hit_rate_pct": hit_rate,
                    "total_profit": round(row["total_profit"], 0),
                    "roi_pct": roi,
                }
            )
        results[axis] = axis_result

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Feature Importance 抽出
# ─────────────────────────────────────────────────────────────────────────────

def extract_feature_importance() -> dict[str, list[dict[str, Any]]]:
    """学習済みモデルから Feature Importance を取得。"""
    try:
        from src.ml.models import load_models, FEATURE_COLS
        import pandas as pd

        honmei, place, manji = load_models()
        result: dict[str, list[dict[str, Any]]] = {}

        for name, model_obj in [("honmei", honmei), ("manji", manji)]:
            m = model_obj._model
            if m and hasattr(m, "feature_importances_"):
                imp = pd.Series(m.feature_importances_, index=FEATURE_COLS)
                imp_pct = (imp / imp.sum() * 100).round(2)
                top = imp_pct.sort_values(ascending=False).head(30)
                result[name] = [{"feature": k, "importance_pct": float(v)} for k, v in top.items()]
            else:
                result[name] = []
        return result
    except Exception as exc:
        logger.warning("Feature Importance 取得失敗: %s", exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# 勝ちパターン判定
# ─────────────────────────────────────────────────────────────────────────────

def identify_win_patterns(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    """ROI 120%以上かつ n_bets >= 20 のセグメントを「勝ちパターン」として抽出。"""
    patterns = []
    for axis, rows in analysis.items():
        for row in rows:
            if row["roi_pct"] >= 120 and row["n_bets"] >= 20:
                patterns.append(
                    {
                        "axis": axis,
                        "segment": row["segment"],
                        "roi_pct": row["roi_pct"],
                        "n_bets": row["n_bets"],
                        "hit_rate_pct": row["hit_rate_pct"],
                        "total_profit": row["total_profit"],
                    }
                )
    patterns.sort(key=lambda x: x["roi_pct"], reverse=True)
    return patterns


def identify_loss_patterns(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    """ROI 80%以下かつ n_bets >= 20 のセグメントを「負けパターン」として抽出。"""
    patterns = []
    for axis, rows in analysis.items():
        for row in rows:
            if row["roi_pct"] <= 80 and row["n_bets"] >= 20:
                patterns.append(
                    {
                        "axis": axis,
                        "segment": row["segment"],
                        "roi_pct": row["roi_pct"],
                        "n_bets": row["n_bets"],
                        "total_profit": row["total_profit"],
                    }
                )
    patterns.sort(key=lambda x: x["roi_pct"])
    return patterns


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="モデル特性分析")
    parser.add_argument("--since", default="2024-01-01", help="集計開始日 YYYY-MM-DD")
    parser.add_argument("--until", default=date.today().isoformat())
    parser.add_argument("--min-bets", type=int, default=10)
    parser.add_argument("--model", default="", help="モデルフィルタ（例: 本命）")
    parser.add_argument("--out", default="data/model_traits_analysis.json")
    args = parser.parse_args()

    conn = _connect()

    logger.info("=== モデル特性分析 since=%s ===", args.since)

    # 1. セグメント分析
    logger.info("セグメント分析実行中...")
    analysis = run_segment_analysis(
        conn, args.since, args.until, args.min_bets, args.model
    )

    # 2. Feature Importance
    logger.info("Feature Importance 抽出中...")
    fi = extract_feature_importance()

    # 3. 勝ち・負けパターン
    win_patterns = identify_win_patterns(analysis)
    loss_patterns = identify_loss_patterns(analysis)

    # 4. 集計サマリ
    all_rows = conn.execute(
        """
        SELECT COUNT(*) as n, SUM(pr.payout) as payout, SUM(pr.profit) as profit
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date >= ? AND COALESCE(p.is_superseded,0)=0 AND pr.is_hit IS NOT NULL
        """,
        (args.since,),
    ).fetchone()
    total_n = all_rows["n"] or 0
    total_profit = all_rows["profit"] or 0
    total_payout = all_rows["payout"] or 0
    overall_roi = round(total_payout / max(total_n * 100, 1) * 100, 1)

    # 出力
    output = {
        "generated_at": date.today().isoformat(),
        "since": args.since,
        "until": args.until,
        "overall": {
            "n_bets": total_n,
            "total_profit": round(total_profit, 0),
            "overall_roi_pct": overall_roi,
        },
        "win_patterns": win_patterns[:20],
        "loss_patterns": loss_patterns[:20],
        "segment_analysis": analysis,
        "feature_importance": fi,
    }

    out_path = _ROOT / args.out
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("分析結果保存: %s", out_path)

    # コンソール表示
    logger.info("\n" + "=" * 60)
    logger.info("【全体サマリ (since=%s)】", args.since)
    logger.info("  総ベット数: %d件  総利益: ¥%.0f  全体ROI: %.1f%%",
                total_n, total_profit, overall_roi)

    logger.info("\n【🏆 勝ちパターン TOP10 (ROI>=120%% n>=20)】")
    for p in win_patterns[:10]:
        logger.info("  %-15s %-20s  ROI=%.0f%%  n=%d  利益=¥%.0f",
                    p["axis"], p["segment"], p["roi_pct"], p["n_bets"], p["total_profit"])

    logger.info("\n【❌ 負けパターン TOP10 (ROI<=80%% n>=20)】")
    for p in loss_patterns[:10]:
        logger.info("  %-15s %-20s  ROI=%.0f%%  n=%d",
                    p["axis"], p["segment"], p["roi_pct"], p["n_bets"])

    if fi.get("honmei"):
        logger.info("\n【本命モデル Feature Importance Top10】")
        for f in fi["honmei"][:10]:
            logger.info("  %-40s %.2f%%", f["feature"], f["importance_pct"])

    conn.close()


if __name__ == "__main__":
    main()
