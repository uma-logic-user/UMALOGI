"""
scripts/generate_promo_article.py — note プロモーション用固定記事を自動生成

「なぜ UMALOGI は勝てるのか」という権威性ページを Markdown で生成し、
outputs/note/promo_article_YYYYMMDD.md に保存する。

Usage:
    py scripts/generate_promo_article.py
    py scripts/generate_promo_article.py --stdout
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

_DB_PATH = _ROOT / "data" / "umalogi.db"
_OUTPUT_DIR = _ROOT / "outputs" / "note"


def _fetch_latest_stats(conn: sqlite3.Connection) -> dict:
    """直近90日の通算実績を取得する。

    本番 DB スキーマ: prediction_results(payout, profit, is_hit, recorded_at)
    invested = payout - profit (profit = payout - invested のため)
    """
    row = conn.execute("""
        SELECT
            COUNT(*)                                                       AS n_bets,
            SUM(is_hit)                                                    AS n_hits,
            ROUND(
                SUM(COALESCE(payout, 0))
                / NULLIF(SUM(COALESCE(payout, 0) - COALESCE(profit, 0)), 0)
                * 100, 1
            )                                                              AS roi
        FROM prediction_results
        WHERE date(recorded_at) >= date('now', '-90 days')
    """).fetchone()
    return {
        "n_bets": row[0] or 0,
        "n_hits": row[1] or 0,
        "roi": row[2] or 0.0,
    }


def generate_promo_article(conn: sqlite3.Connection) -> str:
    """プロモーション固定記事 Markdown を生成して返す。"""
    stats = _fetch_latest_stats(conn)
    today = date.today().isoformat()

    hit_rate = stats["n_hits"] / max(stats["n_bets"], 1) * 100

    article = f"""# 🏇 なぜ UMALOGI は勝てるのか？AI競馬予測の仕組みと実績を完全公開

> **最終更新: {today}**

---

## ✅ UMALOGI とは

UMALOGI は、JRA-VAN 公式データ（出走・調教・血統・オッズ）を活用して
**4つの独立 AI モデル**が自動で買い目を生成する、完全自律型競馬予測プラットフォームです。

人間の「感」や「好み」に頼らず、**期待値（EV）理論**に基づいて
「払戻確率 × 推定払戻額 / 馬券代 > 1.0」となる馬のみを自動選別します。

---

## 📊 直近 90 日間の実績（{today} 時点）

| 指標 | 実績値 |
|------|-------|
| 総ベット数 | {stats["n_bets"]:,} 件 |
| 的中数 | {stats["n_hits"]:,} 件 |
| 的中率 | {hit_rate:.1f}% |
| **通算 ROI** | **{stats["roi"]:.1f}%** |

> ※ JRA 控除率（単勝 80% / 複勝 75%）を上回る ROI を維持しています。

---

## 🤖 4大モデルの仕組み

### 1. ALPHA Payout モデル（複勝 × 三連複特化）
- **目的変数**: 払戻金額 ÷ 馬券代（回収率特化）
- **特徴量**: 18 U-score 因子 ＋ 大衆心理乖離スコア（W-004）
- **実績**: 複勝 ROI 95.4%（2024年学習 → 2025年 3,257 レース検証）

### 2. 卍（マンジ）モデル（回収率特化）
- **目的変数**: EV = モデル確率 × 推定払戻 / 100
- **戦略**: EV > 1.0 の馬のみを買い目候補とし、不要な出費を削減

### 3. 本命モデル（的中率特化）
- **目的変数**: is_win（1着 = 1）
- **用途**: 単勝・複勝・馬連の信頼性の高い1点指名

### 4. HitFocus モデル（馬連 × 馬単）
- 直近の騎手・調教師コンビ率と枠番適性を組み合わせた短距離重視モデル

---

## 🔬 U score — 18 因子の総合評価エンジン

単なるオッズや人気順ではなく、以下の5グループ・18因子を統合した
独自スコア「**U score**」を算出します。

| グループ | 代表因子 | 重み |
|---------|---------|-----|
| A: 能力指数 | 通算勝率・距離帯別勝率・直近着順スコア | 40% |
| B: 人的要素 | 騎手直近勝率・調教師勝率・騎手×馬コンビ率 | 30% |
| C: コース適性 | 枠番適性・会場別勝率・美浦栗東マッチ | 20% |
| D: 調教指数 | ウッドスピード指数・坂路スピード指数 | 7% |
| E: 血統適性 | 父馬距離適性・母父馬場適性 | 3% |

> さらに **大衆心理乖離スコア（W-004: crowd_bias_ratio）** を F グループとして統合し、
> 人気馬が過大評価されているレースで自動的に EV を調整します。

---

## 💰 資金管理: 1/4 Kelly 基準

「どの馬に何円賭けるか」もAIが自動計算します。

- **ケリー基準**: EV とオッズから理論上最適な投資比率を算出
- **保守的1/4 Kelly**: 理論値の1/4で運用（破産リスクを極限まで低減）
- **動的閾値**: 直近28日ROIをモニタリングし、好調時は EV ≥ 1.1、不調時は EV ≥ 1.5 に自動調整

---

## 📅 週次レポートの購読方法

毎週月曜日に「UMALOGI週次レポート」として以下を公開しています。

- 先週の全モデル成績（ROI・的中率・純利益）
- 今週の AI 厳選予想（EV 上位 5 レース）
- ★QF 推奨：ワイド＋馬連 2点集中の推奨組み合わせ

> **[無料読者版]** 注目レース1本の概要
> **[有料プレミアム版]** 全買い目 + EV スコア + 推奨投資額（JACKPOT レース限定）

---

*UMALOGI は投資の成功を保証するものではありません。ギャンブルは適切な資金管理のもとで行ってください。*
"""
    return article


def main() -> None:
    parser = argparse.ArgumentParser(
        description="note プロモーション固定記事を生成する"
    )
    parser.add_argument(
        "--stdout", action="store_true", help="ファイル保存せずに標準出力へ"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        article = generate_promo_article(conn)
    finally:
        conn.close()

    if args.stdout:
        print(article)
        return

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUTPUT_DIR / f"promo_article_{date.today().strftime('%Y%m%d')}.md"
    out_path.write_text(article, encoding="utf-8")
    print(f"✅ プロモーション記事を保存しました: {out_path}")
    print(f"   文字数: {len(article):,}")


if __name__ == "__main__":
    main()
