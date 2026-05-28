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
        self.skipped  = 0

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
            f"{int(self.invested):,}",
            f"{int(self.payout):,}",
            f"{self.roi:.1f}%",
            "○" if self.roi >= 100 else "×",
        ]


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
