# 2年間全モデル横断バックテスト Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `scripts/backtest_all_models.py` を新規作成し、本命・卍・複勝・ALPHAの全4モデルを2024年データで再訓練し2025年データで横断比較するバックテストを実行する。

**Architecture:** 本命/卍/複勝は `HonmeiModel/PlaceModel/ManjiModel.train(conn, train_until=2024)` でメモリ上のみ再訓練（本番モデル無変更）し、2025年レースを1件ずつ FeatureBuilder でスコアリングして評価する。ALPHA は `alpha_model.run_backtest(conn, [2024], [2025])` で自己完結。既存スクリプトへの変更はゼロ。

**Tech Stack:** Python 3.11+, SQLite (umalogi.db), LightGBM, pandas, pytest

---

## ファイル構成

| ファイル | 操作 | 責務 |
|---------|------|------|
| `scripts/backtest_all_models.py` | **新規作成** | バックテスト本体（全ロジック） |
| `tests/test_backtest_all_models.py` | **新規作成** | StrategyStats・_select_horses のユニットテスト |
| `docs/2_automation_schedule.md` | **更新** | backtest_all_models.py の使用方法を追記 |
| `docs/7_weakness_ledger.md` | **更新** | 複勝/ALPHAバックテスト未統合の弱点をクローズ |

既存ファイルへの変更: **なし**

---

## Task 1: スクリプト骨格 + argparse + dry-run

**Files:**
- Create: `scripts/backtest_all_models.py`

- [ ] **Step 1: ファイルを作成する**

```python
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
import csv
import logging
import math
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

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
    if not Path(db_path).exists():
        print(f"\n  [NG] DB が見つかりません: {db_path}")
        return 1
    conn = init_db(db_path=Path(db_path))
    print(f"  DB  : {db_path}")

    _print_data_stats(conn)

    if args.dry_run:
        print("\n  --dry-run: データ確認のみ。終了します。")
        conn.close()
        return 0

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: dry-run が正常終了することを確認する**

```
py scripts/backtest_all_models.py --dry-run
```

期待出力（抜粋）:
```
================================================================
  UMALOGI AI  --  2-Year All-Model Backtest
================================================================
  Train: 2024年  →  Test: 2025年
  DB  : data/umalogi.db
  2024: レース=N,NNN  race_results=N,NNN  race_payouts=N,NNN
  2025: レース=N,NNN  race_results=N,NNN  race_payouts=N,NNN
  --dry-run: データ確認のみ。終了します。
```

終了コード 0 であること。

- [ ] **Step 3: コミット**

```bash
git add scripts/backtest_all_models.py
git commit -m "feat: backtest_all_models.py の骨格・CLI・dry-run を追加"
```

---

## Task 2: StrategyStats クラスのユニットテスト → 実装

**Files:**
- Create: `tests/test_backtest_all_models.py`
- Modify: `scripts/backtest_all_models.py` (StrategyStats クラスを追加)

- [ ] **Step 1: 失敗するテストを書く**

```python
# tests/test_backtest_all_models.py
"""scripts/backtest_all_models.py の StrategyStats ユニットテスト"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from scripts.backtest_all_models import StrategyStats, _BET_AMOUNT


def test_strategy_stats_initial_state():
    s = StrategyStats(label="テスト", bet_type="単勝")
    assert s.races == 0
    assert s.hits == 0
    assert s.roi == 0.0
    assert s.hit_rate == 0.0
    assert s.profit == 0.0


def test_strategy_stats_add_hit():
    s = StrategyStats(label="テスト", bet_type="単勝")
    s.add(hit=True, payout=300.0)  # 100円賭けて300円回収
    assert s.races == 1
    assert s.hits == 1
    assert s.invested == _BET_AMOUNT
    assert s.payout == 300.0
    assert abs(s.roi - 300.0) < 0.01
    assert s.profit == 200.0


def test_strategy_stats_add_miss():
    s = StrategyStats(label="テスト", bet_type="単勝")
    s.add(hit=False, payout=0.0)
    assert s.races == 1
    assert s.hits == 0
    assert s.roi == 0.0
    assert s.profit == -_BET_AMOUNT


def test_strategy_stats_roi_multiple():
    s = StrategyStats(label="テスト", bet_type="複勝")
    s.add(True, 200.0)
    s.add(False, 0.0)
    s.add(True, 300.0)
    # invested = 300, payout = 500
    assert s.races == 3
    assert s.hits == 2
    assert abs(s.hit_rate - 200.0 / 3) < 0.01
    assert abs(s.roi - 500.0 / 300.0 * 100) < 0.01


def test_strategy_stats_summary_row_format():
    s = StrategyStats(label="本命・単勝(Top1)", bet_type="単勝")
    s.add(True, 500.0)
    row = s.summary_row()
    assert row[0] == "本命・単勝(Top1)"
    assert "1" in row[1]       # races
    assert "100.0%" in row[3]  # hit_rate
    assert "○" in row[7]       # 黒字フラグ
```

- [ ] **Step 2: テストが失敗することを確認する**

```
pytest tests/test_backtest_all_models.py -v
```

期待: `ImportError: cannot import name 'StrategyStats'`

- [ ] **Step 3: StrategyStats を実装する**

`scripts/backtest_all_models.py` の `_TRAIN_YEAR` 定数行の直後に追加:

```python
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
```

- [ ] **Step 4: テストが通ることを確認する**

```
pytest tests/test_backtest_all_models.py -v
```

期待: 5件すべて PASSED

- [ ] **Step 5: コミット**

```bash
git add scripts/backtest_all_models.py tests/test_backtest_all_models.py
git commit -m "feat: StrategyStats クラスを追加"
```

---

## Task 3: 戦略定義 + _select_horses() のユニットテスト → 実装

**Files:**
- Modify: `scripts/backtest_all_models.py`
- Modify: `tests/test_backtest_all_models.py`

- [ ] **Step 1: テストを追加する**

`tests/test_backtest_all_models.py` に以下を追記:

```python
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from scripts.backtest_all_models import _select_horses, STRATEGIES


def _make_df(n: int = 6) -> pd.DataFrame:
    """FeatureBuilder が返す形式を模したダミーDataFrame。"""
    return pd.DataFrame({
        "horse_name": [f"馬{i}" for i in range(n)],
        "win_odds":   [float(i * 2 + 1) for i in range(n)],
        "popularity": list(range(1, n + 1)),
    })


def _make_honmei(scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.predict.return_value = pd.Series(scores)
    return m


def _make_place(scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.predict.return_value = pd.Series(scores)
    return m


def _make_manji(ev_scores: list[float]) -> MagicMock:
    m = MagicMock()
    m.ev_score.return_value = pd.Series(ev_scores)
    return m


def test_select_horses_honmei_top1():
    df     = _make_df(6)
    scores = [0.1, 0.5, 0.3, 0.8, 0.2, 0.6]
    honmei = _make_honmei(scores)
    place  = _make_place([0.0] * 6)
    manji  = _make_manji([0.0] * 6)
    picks  = _select_horses(df, STRATEGIES["honmei_tansho"], honmei, place, manji)
    assert picks == ["馬3"]   # index 3 のスコア 0.8 が最高


def test_select_horses_honmei_top3_insufficient():
    """頭数不足（3頭未満）の場合は空リストを返す。"""
    df     = _make_df(2)
    honmei = _make_honmei([0.5, 0.8])
    place  = _make_place([0.0, 0.0])
    manji  = _make_manji([0.0, 0.0])
    picks  = _select_horses(df, STRATEGIES["honmei_sanrenpuku"], honmei, place, manji)
    assert picks == []


def test_select_horses_manji_ev_filter_pass():
    """EV > 1.0 の馬が1頭以上なら picks を返す。"""
    df    = _make_df(4)
    manji = _make_manji([0.8, 1.2, 0.9, 1.5])  # index 1 と 3 が EV>1
    picks = _select_horses(df, STRATEGIES["manji_tansho"],
                           _make_honmei([0.0]*4), _make_place([0.0]*4), manji)
    assert "馬3" in picks    # 最高EV=1.5


def test_select_horses_manji_ev_filter_no_pass():
    """EV>1.0 が0頭のとき空リストを返す。"""
    df    = _make_df(4)
    manji = _make_manji([0.5, 0.6, 0.7, 0.8])  # 全員 EV<1
    picks = _select_horses(df, STRATEGIES["manji_tansho"],
                           _make_honmei([0.0]*4), _make_place([0.0]*4), manji)
    assert picks == []


def test_select_horses_place_top3():
    df    = _make_df(6)
    place = _make_place([0.1, 0.6, 0.9, 0.4, 0.7, 0.3])
    picks = _select_horses(df, STRATEGIES["place_fukusho_top3"],
                           _make_honmei([0.0]*6), place, _make_manji([0.0]*6))
    assert len(picks) == 3
    assert "馬2" in picks  # score 0.9 が最高


def test_strategies_all_keys_present():
    expected = {
        "honmei_tansho", "honmei_umaren", "honmei_sanrenpuku",
        "manji_tansho", "manji_fukusho",
        "place_fukusho", "place_fukusho_top3",
    }
    assert expected.issubset(set(STRATEGIES.keys()))
```

- [ ] **Step 2: テストが失敗することを確認する**

```
pytest tests/test_backtest_all_models.py::test_select_horses_honmei_top1 -v
```

期待: `ImportError: cannot import name '_select_horses'`

- [ ] **Step 3: STRATEGIES と _select_horses() を実装する**

`scripts/backtest_all_models.py` の StrategyStats クラス定義直後に追加:

```python
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

    Returns:
        推奨馬名リスト。条件未達の場合は空リスト。
    """
    import pandas as pd

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
```

- [ ] **Step 4: テストが通ることを確認する**

```
pytest tests/test_backtest_all_models.py -v
```

期待: すべて PASSED

- [ ] **Step 5: コミット**

```bash
git add scripts/backtest_all_models.py tests/test_backtest_all_models.py
git commit -m "feat: STRATEGIES 定義と _select_horses() を追加"
```

---

## Task 4: 3モデル再訓練 (_train_three_models)

**Files:**
- Modify: `scripts/backtest_all_models.py`

- [ ] **Step 1: _train_three_models() を実装する**

`scripts/backtest_all_models.py` の `_select_horses` 関数の直後に追加:

```python
def _train_three_models(
    conn: sqlite3.Connection,
    train_until: int = 2024,
) -> tuple[Any, Any, Any]:
    """
    本命・複勝・卍モデルを train_until 年までのデータで再訓練する。

    本番モデル（data/models/）は一切上書きしない。
    再訓練済みモデルをインメモリのまま返す。

    Returns:
        (honmei, place, manji) の訓練済みインスタンス
    """
    from src.ml.models import HonmeiModel, PlaceModel, ManjiModel

    print(f"\n  [訓練] 本命・複勝・卍モデルを {train_until} 年データで再訓練中...")

    honmei = HonmeiModel()
    place  = PlaceModel()
    manji  = ManjiModel()

    h_metrics = honmei.train(conn, train_until=train_until)
    print(
        f"  [OK] 本命  AUC={h_metrics.get('cv_auc_mean', float('nan')):.3f}"
        f"  n_races={h_metrics.get('n_races', '?')}"
    )

    p_metrics = place.train(conn, train_until=train_until)
    print(
        f"  [OK] 複勝  AUC={p_metrics.get('cv_auc_mean', float('nan')):.3f}"
        f"  n_races={p_metrics.get('n_races', '?')}"
    )

    m_metrics = manji.train(conn, train_until=train_until)
    print(
        f"  [OK] 卍    n_races={m_metrics.get('n_races', '?')}"
    )

    return honmei, place, manji
```

- [ ] **Step 2: main() からの呼び出しを dry-run 分岐の下に仮追加して動作確認する**

`main()` の `conn.close()` の直前に一時的に:

```python
    # --- 動作確認用（Task 5 で置き換える） ---
    honmei, place, manji = _train_three_models(conn, train_until=int(_TRAIN_YEAR))
    print("  訓練完了。（この出力は Task 5 で本番ループに置き換えられます）")
    conn.close()
    return 0
```

- [ ] **Step 3: 訓練が完走することを確認する**

```
py scripts/backtest_all_models.py --verbose 2>&1 | head -50
```

期待: 3行の `[OK]` ログが出力され、AUC 値が表示される。
（実行時間目安: 数分〜10分程度）

- [ ] **Step 4: 動作確認コードを削除する**

Task 5 で main() を書き直すため、Step 2 で追加した仮コードを削除する:

```python
    # main() の末尾は再び:
    conn.close()
    return 0
```

- [ ] **Step 5: コミット**

```bash
git add scripts/backtest_all_models.py
git commit -m "feat: _train_three_models() を追加（本番モデル無変更・インメモリ再訓練）"
```

---

## Task 5: 2025年評価ループ (_run_three_model_backtest)

**Files:**
- Modify: `scripts/backtest_all_models.py`

- [ ] **Step 1: _run_three_model_backtest() を実装する**

`_train_three_models()` の直後に追加:

```python
def _run_three_model_backtest(
    conn: sqlite3.Connection,
    test_year: str,
    honmei: Any,
    place: Any,
    manji: Any,
    strategies: dict[str, dict],
    verbose: bool = False,
) -> tuple[
    dict[str, StrategyStats],
    dict[str, dict[str, StrategyStats]],   # monthly: "2025-01" → {strat_key → stats}
    dict[str, dict[str, StrategyStats]],   # by_venue
]:
    """
    test_year の全レースを対象にバックテストを実行する。

    Returns:
        (overall, monthly, by_venue)
    """
    from src.ml.features import FeatureBuilder
    from src.evaluation.evaluator import (
        _build_combination_key,
        _fetch_payouts,
        _fetch_horse_numbers,
        _is_hit,
        _lookup_payout,
    )

    race_rows = _get_race_ids(conn, test_year)
    n_races   = len(race_rows)
    print(f"\n  [評価] {test_year} 年 {n_races:,} レースを評価中...")

    fb = FeatureBuilder(conn)

    overall:  dict[str, StrategyStats] = {
        k: StrategyStats(label=v["label"], bet_type=v["bet_type"])
        for k, v in strategies.items()
    }
    monthly:  dict[str, dict[str, StrategyStats]] = defaultdict(dict)
    by_venue: dict[str, dict[str, StrategyStats]] = defaultdict(dict)

    def _make_stats(k: str) -> StrategyStats:
        return StrategyStats(label=strategies[k]["label"], bet_type=strategies[k]["bet_type"])

    for i, (race_id, date, venue, distance, surface) in enumerate(race_rows, 1):
        if verbose or i % 200 == 0:
            print(f"  [{i:>4}/{n_races}] {race_id} {date}", flush=True)

        month = date[:7]  # "2025-01"

        try:
            df = fb.build_race_features_for_simulate(race_id)
        except Exception as exc:
            logger.warning("特徴量生成失敗 race_id=%s: %s", race_id, exc)
            continue

        if df.empty:
            continue

        payouts       = _fetch_payouts(conn, race_id)
        horse_numbers = _fetch_horse_numbers(conn, race_id)
        result_map    = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT horse_name, rank FROM race_results WHERE race_id=?", (race_id,)
            ).fetchall()
        }

        for strat_key, strat in strategies.items():
            picks = _select_horses(df, strat, honmei, place, manji)
            if not picks:
                overall[strat_key].skipped += 1
                continue

            bet_type = strat["bet_type"]

            if bet_type in ("馬連", "三連複"):
                # 組み合わせ1点
                comb_key = _build_combination_key(bet_type, picks, horse_numbers)
                hit      = _is_hit(bet_type, picks, result_map)
                payout   = _lookup_payout(bet_type, comb_key, payouts) if comb_key else 0
                overall[strat_key].add(hit, float(payout))
                if strat_key not in monthly[month]:
                    monthly[month][strat_key] = _make_stats(strat_key)
                monthly[month][strat_key].add(hit, float(payout))
                if strat_key not in by_venue[venue]:
                    by_venue[venue][strat_key] = _make_stats(strat_key)
                by_venue[venue][strat_key].add(hit, float(payout))
            else:
                # 単勝・複勝: 各馬に1点ずつ
                for pick in picks:
                    comb_key = _build_combination_key(bet_type, [pick], horse_numbers)
                    hit      = _is_hit(bet_type, [pick], result_map)
                    payout   = _lookup_payout(bet_type, comb_key, payouts) if comb_key else 0
                    overall[strat_key].add(hit, float(payout))
                    if strat_key not in monthly[month]:
                        monthly[month][strat_key] = _make_stats(strat_key)
                    monthly[month][strat_key].add(hit, float(payout))
                    if strat_key not in by_venue[venue]:
                        by_venue[venue][strat_key] = _make_stats(strat_key)
                    by_venue[venue][strat_key].add(hit, float(payout))

    return overall, dict(monthly), dict(by_venue)
```

- [ ] **Step 2: コミット**

```bash
git add scripts/backtest_all_models.py
git commit -m "feat: _run_three_model_backtest() 2025年評価ループを追加"
```

---

## Task 6: ALPHA 統合 (_run_alpha_backtest)

**Files:**
- Modify: `scripts/backtest_all_models.py`

- [ ] **Step 1: _run_alpha_backtest() を実装する**

`_run_three_model_backtest()` の直後に追加:

```python
def _run_alpha_backtest(
    conn: sqlite3.Connection,
    train_years: list[int],
    test_years: list[int],
) -> list[Any]:
    """
    ALPHA モデルのバックテストを実行する（単勝・複勝の2種類）。

    alpha_model.run_backtest() は内部でデータロード・訓練・評価を完結させる。

    Returns:
        [AlphaBacktestResult(単勝), AlphaBacktestResult(複勝)]
    """
    from src.ml.alpha_model import run_backtest as alpha_run_backtest, ALPHA_EV_THRESHOLD

    print(f"\n  [ALPHA] {train_years} 学習 → {test_years} テスト中...")
    results = []
    for bet_type in ("単勝", "複勝"):
        try:
            result = alpha_run_backtest(
                conn,
                train_years=train_years,
                test_years=test_years,
                bet_type=bet_type,
                ev_threshold=ALPHA_EV_THRESHOLD,
                verbose=False,
            )
            results.append(result)
            label = "ALPHA・" + ("単勝" if bet_type == "単勝" else "複勝")
            print(
                f"  [OK] {label}  "
                f"シグナル={result.num_bets:,}  "
                f"的中={result.num_hits:,}  "
                f"ROI={result.roi:.1f}%"
            )
        except Exception as exc:
            logger.warning("ALPHA %s バックテスト失敗: %s", bet_type, exc)
    return results
```

- [ ] **Step 2: コミット**

```bash
git add scripts/backtest_all_models.py
git commit -m "feat: _run_alpha_backtest() ALPHA統合を追加"
```

---

## Task 7: 結果表示 + CSV + main() 完成

**Files:**
- Modify: `scripts/backtest_all_models.py`

- [ ] **Step 1: 表示関数を実装する**

`_run_alpha_backtest()` の直後に追加:

```python
_SUMMARY_HEADERS = ["戦略", "買い目数", "的中", "的中率", "投資(円)", "回収(円)", "ROI", "黒字"]


def _print_table(headers: list[str], rows: list[list[str]], title: str = "") -> None:
    if not rows:
        return
    if title:
        print(f"\n  {title}")
    widths = [max(len(h), max(len(r[i]) for r in rows)) + 2
              for i, h in enumerate(headers)]
    sep    = "  +" + "+".join("-" * w for w in widths) + "+"
    hdr    = "  |" + "|".join(f" {h:<{w-2}} " for h, w in zip(headers, widths)) + "|"
    print(sep); print(hdr); print(sep)
    for row in rows:
        print("  |" + "|".join(f" {c:<{w-2}} " for c, w in zip(row, widths)) + "|")
    print(sep)


def _print_three_model_summary(
    overall: dict[str, StrategyStats], title: str
) -> None:
    _section(title)
    rows = [s.summary_row() for s in overall.values()]
    _print_table(_SUMMARY_HEADERS, rows)
    winners = [s.label for s in overall.values() if s.roi >= 100]
    if winners:
        print(f"\n  ★ 黒字戦略: {', '.join(winners)}")
    else:
        print("\n  -- 黒字戦略なし")


def _print_alpha_summary(alpha_results: list[Any]) -> None:
    _section("ALPHA モデル結果")
    for r in alpha_results:
        label    = f"ALPHA・{'単勝' if r.bet_type == '単勝' else '複勝'}(EV>{r.ev_threshold:.1f})"
        invested = r.num_bets * _BET_AMOUNT
        row      = [
            label,
            f"{r.num_bets:,}",
            f"{r.num_hits:,}",
            f"{r.hit_rate:.1f}%",
            f"{invested:,}",
            f"{r.total_payout:,.0f}",
            f"{r.roi:.1f}%",
            "○" if r.roi >= 100 else "×",
        ]
        _print_table(_SUMMARY_HEADERS, [row])


def _print_monthly_breakdown(
    monthly: dict[str, dict[str, StrategyStats]],
    strat_keys: list[str],
) -> None:
    _section("月別 ROI 推移（本命・卍・複勝）")
    for month in sorted(monthly.keys()):
        parts = []
        for k in strat_keys:
            if k in monthly[month]:
                s = monthly[month][k]
                label_short = s.label.split("(")[0].replace("・", "/")
                parts.append(f"{label_short}={s.roi:.0f}%")
        print(f"  {month}: " + "  ".join(parts))


def _write_csv(
    overall: dict[str, StrategyStats],
    alpha_results: list[Any],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["strategy", "bets", "hits", "hit_rate_pct",
                    "invested", "payout", "roi_pct", "profit"])
        for s in overall.values():
            w.writerow([
                s.label, s.races, s.hits,
                round(s.hit_rate, 2),
                int(s.invested), int(s.payout),
                round(s.roi, 2), int(s.profit),
            ])
        for r in alpha_results:
            invested = r.num_bets * _BET_AMOUNT
            label    = f"ALPHA・{'単勝' if r.bet_type == '単勝' else '複勝'}(EV>{r.ev_threshold:.1f})"
            w.writerow([
                label, r.num_bets, r.num_hits,
                round(r.hit_rate, 2),
                invested, round(r.total_payout, 0),
                round(r.roi, 2), round(r.total_payout - invested, 0),
            ])
    print(f"\n  CSV 保存: {out_path}")
```

- [ ] **Step 2: main() を完成させる**

`main()` 関数の `if args.dry_run:` ブロック以降を以下に置き換える:

```python
    if args.dry_run:
        print("\n  --dry-run: データ確認のみ。終了します。")
        conn.close()
        return 0

    # ── Phase 2-4: 3モデル再訓練 + 2025年評価 ───────────────────
    wall_start = time.perf_counter()

    honmei, place, manji = _train_three_models(conn, train_until=int(_TRAIN_YEAR))

    overall, monthly, by_venue = _run_three_model_backtest(
        conn=conn,
        test_year=_TEST_YEAR,
        honmei=honmei,
        place=place,
        manji=manji,
        strategies=STRATEGIES,
        verbose=args.verbose,
    )

    # ── Phase 3': ALPHA バックテスト ────────────────────────────
    alpha_results = _run_alpha_backtest(
        conn,
        train_years=[int(_TRAIN_YEAR)],
        test_years=[int(_TEST_YEAR)],
    )

    conn.close()

    elapsed = time.perf_counter() - wall_start
    mins, secs = divmod(int(elapsed), 60)

    # ── 結果表示 ─────────────────────────────────────────────────
    _banner(f"Backtest Results  Train:{_TRAIN_YEAR} / Test:{_TEST_YEAR}  "
            f"({mins}m{secs:02d}s)")

    _print_three_model_summary(
        overall, f"2025年 アウト・オブ・サンプル（本命・卍・複勝）"
    )
    _print_alpha_summary(alpha_results)

    strat_keys = list(STRATEGIES.keys())
    _print_monthly_breakdown(monthly, strat_keys)

    # 会場別（verbose 時のみ表示）
    if args.verbose:
        _section("会場別（本命・卍・複勝）")
        for venue in sorted(by_venue.keys()):
            rows = [by_venue[venue][k].summary_row()
                    for k in strat_keys if k in by_venue[venue]]
            if rows:
                _print_table(_SUMMARY_HEADERS, rows, title=f"  [{venue}]")

    # CSV
    if args.csv:
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_dir = _ROOT / "results"
        _write_csv(overall, alpha_results, csv_dir / f"backtest_{ts}.csv")

    # クリーンアップ（不要な場合、tmp は残さない）
    if args.cleanup:
        tmp_dir = _ROOT / "data" / "models" / "backtest_tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
            print(f"  [cleanup] {tmp_dir} を削除しました。")

    # 総評
    _section("総評")
    all_rois = [(s.label, s.roi) for s in overall.values()]
    all_rois += [(f"ALPHA・{'単勝' if r.bet_type == '単勝' else '複勝'}", r.roi)
                 for r in alpha_results]
    best_label, best_roi = max(all_rois, key=lambda t: t[1])
    print(f"  最高ROI戦略: {best_label}  ROI={best_roi:.1f}%")
    black = [lbl for lbl, roi in all_rois if roi >= 100]
    if black:
        print(f"  ★ 黒字戦略: {', '.join(black)}")
    else:
        print("  -- 全戦略が赤字です。")
    print()

    return 0
```

- [ ] **Step 3: フルラン（本番テスト）を実行する**

```
py scripts/backtest_all_models.py --verbose 2>&1 | tee results/backtest_run_log.txt
```

期待:
- 3モデル訓練の `[OK]` ログ（AUC 表示）
- 2025年評価の進捗 `[NNN/NNNN]` ログ
- ALPHA `[OK]` ログ
- 最終サマリーテーブルが表示される
- 終了コード 0

- [ ] **Step 4: CSV 出力を確認する**

```
py scripts/backtest_all_models.py --csv --cleanup
```

`results/backtest_YYYYMMDD_HHMMSS.csv` が生成され、9行以上のデータがあること:

```
py -c "
import csv; rows = list(csv.DictReader(open('results/backtest_$(ls results/*.csv | tail -1 | xargs basename)', encoding='utf-8')))
print(f'{len(rows)} 行')
[print(r['strategy'], r['roi_pct']) for r in rows]
"
```

- [ ] **Step 5: コミット**

```bash
git add scripts/backtest_all_models.py
git commit -m "feat: backtest_all_models.py 結果表示・CSV出力・main()完成"
```

---

## Task 8: ドキュメント更新 + 最終確認

**Files:**
- Modify: `docs/2_automation_schedule.md`
- Modify: `docs/7_weakness_ledger.md`

- [ ] **Step 1: docs/2_automation_schedule.md を更新する**

ファイルを開き、更新履歴セクション（ファイル先頭付近）に以下を追記:

```markdown
| 2026-05-28 | 全モデル横断2年間バックテスト機能を追加。影響ファイル: scripts/backtest_all_models.py |
```

また、手動スクリプト一覧セクションに以下を追記:

```markdown
### backtest_all_models.py — 全モデル横断 2年間バックテスト

```bash
# 標準実行（2024学習 → 2025テスト、全4モデル横断比較）
py scripts/backtest_all_models.py

# オプション
py scripts/backtest_all_models.py --dry-run    # データ件数確認のみ
py scripts/backtest_all_models.py --csv        # results/backtest_YYYYMMDD.csv を出力
py scripts/backtest_all_models.py --verbose    # 各レースの進捗表示
py scripts/backtest_all_models.py --cleanup    # 実行後に一時モデルを削除
```

対象モデル: 本命（単勝/馬連/三連複）・卍（単勝/複勝）・複勝（Top1/Top3）・ALPHA（単勝/複勝）
```

- [ ] **Step 2: docs/7_weakness_ledger.md を確認して更新する**

ファイルを開き、「複勝モデルのバックテスト未統合」「ALPHAモデルのバックテスト未統合」に関連する弱点エントリを探し、ステータスを `🟢完了` に更新する。

更新時に対応日・コミット情報を追記すること:
```markdown
対応完了: 2026-05-28  scripts/backtest_all_models.py で統合
```

- [ ] **Step 3: 最終テストを実行する**

```
pytest tests/test_backtest_all_models.py -v
```

期待: すべて PASSED

```
py scripts/backtest_all_models.py --dry-run
```

期待: 終了コード 0、データ件数が表示される。

- [ ] **Step 4: 最終コミット**

```bash
git add docs/2_automation_schedule.md docs/7_weakness_ledger.md
git commit -m "docs: backtest_all_models.py の使用方法と弱点台帳の更新"
```

---

## 実行コマンドまとめ

```bash
# ユニットテスト
pytest tests/test_backtest_all_models.py -v

# dry-run（データ確認のみ、数秒）
py scripts/backtest_all_models.py --dry-run

# フルラン（2024訓練 + 2025評価、数十分）
py scripts/backtest_all_models.py --verbose --csv

# 実行後にtmpモデル削除
py scripts/backtest_all_models.py --cleanup
```
