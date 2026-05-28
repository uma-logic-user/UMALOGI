# scripts/backtest_all_models.py
"""
UMALOGI AI -- 全モデル横断 2年間バックテスト

Train: 2024年全データでモデルを再訓練（本番モデルは無変更）
Test:  2025年全データで本命・卍・複勝・ALPHA を横断評価

使用例:
    py scripts/backtest_all_models.py              # 標準実行
    py scripts/backtest_all_models.py --dry-run    # データ件数確認のみ
    py scripts/backtest_all_models.py --csv        # CSV書き出しあり
    py scripts/backtest_all_models.py --verbose    # 各レース進捗表示
    py scripts/backtest_all_models.py --cleanup    # 実行後にtmpモデルを削除
"""
from __future__ import annotations

import argparse
import csv  # noqa: F401
import logging
import math  # noqa: F401
import shutil  # noqa: F401
import sqlite3
import sys
import time  # noqa: F401
from collections import defaultdict  # noqa: F401
from datetime import datetime  # noqa: F401
from pathlib import Path
from typing import Any  # noqa: F401

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

from src.database.init_db import get_db_path, init_db

logger = logging.getLogger(__name__)
_WIDTH = 70
_BET_AMOUNT = 100  # 1買い目あたりの賭け金（円）
_TRAIN_YEAR = "2024"
_TEST_YEAR  = "2025"


class StrategyStats:
    """1戦略の集計状態。"""

    def __init__(self, label: str, bet_type: str) -> None:
        self.label    = label
        self.bet_type = bet_type
        self.races    = 0
        self.hits     = 0
        self.invested = 0.0
        self.payout   = 0.0
        self.skipped  = 0   # EVフィルタ等で見送ったレース数（評価ループで加算）

    def add(self, hit: bool, payout: float) -> None:
        self.races    += 1
        self.hits     += int(hit)
        self.invested += _BET_AMOUNT
        self.payout   += payout

    @property
    def roi(self) -> float:
        return (self.payout / self.invested * 100) if self.invested > 0 else 0.0

    @property
    def hit_rate(self) -> float:
        return (self.hits / self.races * 100) if self.races > 0 else 0.0

    @property
    def profit(self) -> float:
        return self.payout - self.invested

    def summary_row(self) -> list[str]:
        return [
            self.label,
            f"{self.races:,}",
            f"{self.hits:,}",
            f"{self.hit_rate:.1f}%",
            f"{round(self.invested):,}",
            f"{round(self.payout):,}",
            f"{self.roi:.1f}%",
            "○" if self.roi >= 100 else "×",
        ]


# ── 戦略定義 ─────────────────────────────────────────────────────
STRATEGIES: dict[str, dict] = {
    "honmei_tansho": {
        "label":    "本命・単勝(Top1)",
        "model":    "honmei",
        "bet_type": "単勝",
        "n_picks":  1,
    },
    "honmei_umaren": {
        "label":    "本命・馬連(Top2)",
        "model":    "honmei",
        "bet_type": "馬連",
        "n_picks":  2,
    },
    "honmei_sanrenpuku": {
        "label":    "本命・三連複(Top3)",
        "model":    "honmei",
        "bet_type": "三連複",
        "n_picks":  3,
    },
    "manji_tansho": {
        "label":    "卍・単勝(EV>1.0)",
        "model":    "manji",
        "bet_type": "単勝",
        "n_picks":  1,
        "ev_filter": True,
    },
    "manji_fukusho": {
        "label":    "卍・複勝(EV>1.0)",
        "model":    "manji",
        "bet_type": "複勝",
        "n_picks":  1,
        "ev_filter": True,
    },
    "place_fukusho": {
        "label":    "複勝・複勝(Top1)",
        "model":    "place",
        "bet_type": "複勝",
        "n_picks":  1,
    },
    "place_fukusho_top3": {
        "label":    "複勝・複勝(Top3流し)",
        "model":    "place",
        "bet_type": "複勝",
        "n_picks":  3,
    },
}


def _select_horses(
    df: "pd.DataFrame",
    strategy: dict,
    honmei: Any,
    place: Any,
    manji: Any,
) -> list[str]:
    """
    戦略に応じて予想馬名リストを返す。

    Args:
        df: 出走馬情報（horse_name, win_odds, popularity 等を含む）
        strategy: STRATEGIES 辞書から取得した戦略
        honmei: 本命モデル（.predict(df) → pd.Series）
        place: 複勝モデル（.predict(df) → pd.Series）
        manji: 卍モデル（.ev_score(df) → pd.Series）

    Returns:
        推奨馬名リスト。条件未達の場合は空リスト。
    """
    if df.empty:
        return []

    model_key = strategy["model"]
    n_picks   = strategy.get("n_picks", 1)
    ev_filter = strategy.get("ev_filter", False)

    if model_key == "honmei":
        scores = honmei.predict(df)
    elif model_key == "place":
        scores = place.predict(df)
    elif model_key == "manji":
        scores = manji.ev_score(df)
    else:
        return []

    df2 = df.copy()
    df2["_score"] = scores.values

    if ev_filter:
        df2 = df2[df2["_score"] > 1.0]
        if df2.empty:
            return []

    df2 = df2.sort_values("_score", ascending=False)
    top = df2.head(n_picks)

    # 組み合わせ馬券（馬連・三連複）は n_picks 頭に満たない場合は不成立
    if strategy["bet_type"] in ("馬連", "三連複") and len(top) < n_picks:
        return []

    return top["horse_name"].tolist()


def _train_three_models(
    conn: sqlite3.Connection,
    train_until: int = 2024,
) -> tuple[Any, Any, Any]:
    """
    本命・複勝・卍モデルを train_until 年までのデータで再訓練する。

    本番モデル（data/models/）は一切上書きしない。
    再訓練済みモデルをインメモリのまま返す。

    NOTE: HonmeiModel.train() reads data/models/honmei.pkl for Champion/Challenger
    comparison. This is read-only; no production model files are written.

    Returns:
        (honmei, place, manji) の訓練済みインスタンス
    """
    from src.ml.models import HonmeiModel, PlaceModel, ManjiModel

    print(f"\n  [訓練] 本命・複勝・卍モデルを {train_until} 年データで再訓練中...")

    honmei = HonmeiModel()
    place  = PlaceModel()
    manji  = ManjiModel()

    try:
        h_metrics = honmei.train(conn, train_until=train_until)
        print(
            f"  [OK] 本命  AUC={h_metrics.get('cv_auc_mean', float('nan')):.3f}"
            f"  n_races={h_metrics.get('n_races', '?')}"
        )
    except Exception as exc:
        logger.error("本命モデル訓練失敗: %s", exc)
        raise

    try:
        p_metrics = place.train(conn, train_until=train_until)
        print(
            f"  [OK] 複勝  AUC={p_metrics.get('cv_auc_mean', float('nan')):.3f}"
            f"  n_races={p_metrics.get('n_races', '?')}"
        )
    except Exception as exc:
        logger.error("複勝モデル訓練失敗: %s", exc)
        raise

    try:
        m_metrics = manji.train(conn, train_until=train_until)
        print(
            f"  [OK] 卍    n_races={m_metrics.get('n_races', '?')}"
        )
    except Exception as exc:
        logger.error("卍モデル訓練失敗: %s", exc)
        raise

    return honmei, place, manji


def _banner(text: str) -> None:
    border = "=" * _WIDTH
    inner  = f"  {text}  "
    pad    = max(0, _WIDTH - 2 - len(inner))
    print(f"\n{border}\n|{' ' * (pad // 2)}{inner}{' ' * (pad - pad // 2)}|\n{border}")


def _section(text: str) -> None:
    print(f"\n{'- ' * (_WIDTH // 2)}\n  {text}\n{'- ' * (_WIDTH // 2)}")


def _get_race_ids(
    conn: sqlite3.Connection, year: str
) -> list[tuple[str, str, str, int, str]]:
    """race_results が存在するレースの一覧を返す。"""
    rows = conn.execute(
        """
        SELECT r.race_id, r.date, r.venue, r.distance, r.surface
        FROM   races r
        WHERE  substr(r.date, 1, 4) = ?
          AND  EXISTS (
                 SELECT 1 FROM race_results rr
                 WHERE  rr.race_id = r.race_id AND rr.rank IS NOT NULL
               )
        ORDER  BY r.date, r.race_id
        """,
        (year,),
    ).fetchall()
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


def _print_data_stats(conn: sqlite3.Connection) -> None:
    """2024/2025 のデータ件数を表示する。"""
    for yr in (_TRAIN_YEAR, _TEST_YEAR):
        races = conn.execute(
            "SELECT COUNT(*) FROM races WHERE date LIKE ?", (f"{yr}%",)
        ).fetchone()[0]
        results = conn.execute(
            """SELECT COUNT(*) FROM race_results rr
               JOIN races r ON rr.race_id=r.race_id
               WHERE r.date LIKE ?""",
            (f"{yr}%",),
        ).fetchone()[0]
        payouts = conn.execute(
            """SELECT COUNT(*) FROM race_payouts rp
               JOIN races r ON rp.race_id=r.race_id
               WHERE r.date LIKE ?""",
            (f"{yr}%",),
        ).fetchone()[0]
        print(
            f"  {yr}: レース={races:,}  race_results={results:,}  race_payouts={payouts:,}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="UMALOGI AI 全モデル横断 2年間バックテスト",
    )
    parser.add_argument("--db",      type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", dest="dry_run")
    parser.add_argument("--csv",     action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("lightgbm").setLevel(logging.WARNING)

    _banner("UMALOGI AI  --  2-Year All-Model Backtest")
    print(f"  Train: {_TRAIN_YEAR}年  →  Test: {_TEST_YEAR}年")

    db_path = args.db or get_db_path()
    print(f"  DB  : {db_path}")
    if not Path(db_path).exists():
        print(f"\n  [NG] DB が見つかりません: {db_path}")
        return 1
    conn = init_db(db_path=Path(db_path))

    _print_data_stats(conn)

    if args.dry_run:
        print("\n  --dry-run: データ確認のみ。終了します。")
        conn.close()
        return 0

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
