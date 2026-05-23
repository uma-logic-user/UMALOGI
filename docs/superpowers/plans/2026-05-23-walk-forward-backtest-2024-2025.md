# Walk-Forward Backtest 2024-2025 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 2024年1〜5月をTrain、2025年全12ヶ月をTestとしたwalk-forwardバックテストを実装し、5券種 × EV閾値5段階 = 25パターンの成績マトリクスを生成して推奨購入ポートフォリオを導出する。

**Architecture:** 既存の `all_bets_backtest_2026.py` のデータ取得・モデル訓練・シミュレーションロジックをベースに流用。25パターン（bet_type × ev_threshold）を並行管理し、各パターン独立のbankrollでKelly賭け金を計算する。単一のLightGBM分類モデル（1着確率）からHarville確率式で各券種のP（的中確率）を導出し、EV = P × TYPICAL_ODDSで発注フィルタリングを行う。HitFocus・Oracle・暫定系は使用しない（本バックテストはモデル横断ではなく「買い方戦略横断」の比較を目的とする）。

**Tech Stack:** Python 3.11+, SQLite, LightGBM, scikit-learn (GroupKFold, IsotonicRegression), pandas, numpy

---

## ファイル構造

| ファイル | 操作 | 責務 |
|---------|------|------|
| `src/analysis/walk_forward_backtest_2024_2025.py` | 新規作成 | バックテスト本体（全25パターン網羅） |
| `.claudecode/rules/portfolio_strategy_2024_2025.md` | 新規作成 | 結果から導出した購入方針の永続ドキュメント |
| `data/walk_forward_backtest_2024_2025.json` | 新規生成 | 25パターンの数値結果JSON |
| `docs/1_prediction_logic.md` | 更新 | Changelogに今回のバックテスト結果を追記 |

---

## Task 1: スクリプト骨格・定数定義

**Files:**
- Create: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: スクリプトファイルを作成し、定数ブロックを書く**

```python
"""
src/analysis/walk_forward_backtest_2024_2025.py

Walk-Forward Backtest 2024-2025
Train : 2024-01-01 ～ 2024-05-31（race_results/race_payouts 充実期間）
Test  : 2025-01-01 ～ 2025-12-31（全12ヶ月）

全5券種 × EV閾値5段階 = 25パターン 成績マトリクス
除外: HitFocus / Oracle / 暫定系 / WIN5 / 馬単 / 三連単
"""
from __future__ import annotations

import itertools
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeAlias

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [wf_bt] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT     = Path(__file__).resolve().parents[2]
DB_PATH  = ROOT / "data" / "umalogi.db"
OUT_JSON = ROOT / "data" / "walk_forward_backtest_2024_2025.json"
RULES_DIR = ROOT / ".claudecode" / "rules"

# ── 期間 ──────────────────────────────────────────────────────────────────────
TRAIN_FROM = "2024-01-01"
TRAIN_TO   = "2024-05-31"
TEST_FROM  = "2025-01-01"
TEST_TO    = "2025-12-31"

# ── パラメータスイープ設定 ────────────────────────────────────────────────────
BET_TYPES:    list[str]   = ["単勝", "複勝", "ワイド", "馬連", "三連複"]
EV_THRESHOLDS: list[float] = [1.0, 1.1, 1.2, 1.3, 1.5]

# ── 資金管理 ──────────────────────────────────────────────────────────────────
INITIAL_BANKROLL    = 10_000.0
KELLY_FRACTION      = 0.05       # 1/20 Kelly（初期¥10,000でも破産しにくいよう控えめ）
MAX_BET_RATE        = 0.05       # 1ベット最大 bankroll × 5%
MAX_RACE_BUDGET_RATE= 0.15       # 1レース最大 bankroll × 15%
MIN_BET             = 100

# ── 典型オッズ（Kelly / EV 計算用） ───────────────────────────────────────────
TYPICAL_ODDS: dict[str, float] = {
    "単勝":   5.0,
    "複勝":   2.0,
    "ワイド":  2.5,
    "馬連":   5.0,
    "三連複": 15.0,
}

# ── LightGBM パラメータ ────────────────────────────────────────────────────────
LGBM_PARAMS: dict = {
    "objective":        "binary",
    "metric":           "auc",
    "learning_rate":    0.05,
    "num_leaves":       63,
    "min_data_in_leaf": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq":     5,
    "lambda_l1":        0.1,
    "lambda_l2":        0.1,
    "n_estimators":     500,
    "verbose":          -1,
}

FEATURE_COLS: list[str] = [
    "weight_carried", "horse_weight", "horse_weight_diff",
    "gate_number", "distance",
    "surface_code", "sex_code", "venue_code", "condition_code",
    "win_rate_all", "win_rate_surface", "win_rate_distance_band",
    "recent_rank_mean",
    "sire_code",
    "jockey_win_rate",
    "trainer_win_rate",
]

# ── コードマップ ──────────────────────────────────────────────────────────────
_SURFACE_CODE = {"芝": 0, "ダート": 1, "障害": 2}
_SEX_CODE     = {"牡": 0, "牝": 1, "セ": 2}
_VENUE_CODE   = {
    "札幌": 0, "函館": 1, "福島": 2, "新潟": 3, "東京": 4,
    "中山": 5, "中京": 6, "京都": 7, "阪神": 8, "小倉": 9,
}
_COND_CODE = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
_DIST_BANDS = {
    "s": (0, 1400), "m": (1400, 1800), "i": (1800, 2200), "l": (2200, 9999),
}
_SIRE_CACHE: dict[str, int] = {}

# パターンキー型エイリアス (bet_type, ev_threshold)
Pattern: TypeAlias = tuple[str, float]
```

- [ ] **Step 2: スクリプトが import エラーなく起動するか確認**

```bash
py -c "import src.analysis.walk_forward_backtest_2024_2025" 2>&1 | head -5
```
期待出力: エラーなし（または「module not found」ならpathを確認）

---

## Task 2: ヘルパー関数（_encode_sire / _dist_band / build_jockey_trainer_maps / _horse_stats）

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: _encode_sire / _dist_band を追加する**

```python
def _encode_sire(sire: str | None) -> int:
    s = sire or ""
    if s not in _SIRE_CACHE:
        _SIRE_CACHE[s] = len(_SIRE_CACHE)
    return _SIRE_CACHE[s]

def _dist_band(distance: int) -> str:
    for name, (lo, hi) in _DIST_BANDS.items():
        if lo <= distance < hi:
            return name
    return "l"
```

- [ ] **Step 2: 騎手/調教師 勝率マップ関数を追加する**

```python
_JKY_MAP: dict[str, float] = {}
_TRN_MAP: dict[str, float] = {}

def build_jockey_trainer_maps(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT rr.jockey,
               SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS wr
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
          AND rr.jockey IS NOT NULL
        GROUP BY rr.jockey
        HAVING COUNT(*) >= 5
        """,
        (TRAIN_FROM, TRAIN_TO),
    ).fetchall()
    _JKY_MAP.update({r[0]: float(r[1]) for r in rows})

    rows = conn.execute(
        """
        SELECT rr.trainer,
               SUM(CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS wr
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
          AND rr.trainer IS NOT NULL
        GROUP BY rr.trainer
        HAVING COUNT(*) >= 5
        """,
        (TRAIN_FROM, TRAIN_TO),
    ).fetchall()
    _TRN_MAP.update({r[0]: float(r[1]) for r in rows})
    logger.info("騎手マップ %d 人  調教師マップ %d 人", len(_JKY_MAP), len(_TRN_MAP))
```

- [ ] **Step 3: 馬の過去成績取得関数を追加する**

```python
def _horse_stats(
    conn: sqlite3.Connection,
    horse_id: str,
    race_date: str,
    surface: str,
    distance: int,
) -> dict[str, float]:
    band = _dist_band(distance)
    rows = conn.execute(
        """
        SELECT rr.rank, r.surface, r.distance
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.horse_id = ?
          AND r.date < ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY r.date DESC
        LIMIT 30
        """,
        (horse_id, race_date),
    ).fetchall()
    if not rows:
        return {
            "win_rate_all": 0.0,
            "win_rate_surface": 0.0,
            "win_rate_distance_band": 0.0,
            "recent_rank_mean": 10.0,
        }
    ranks_all = [r[0] for r in rows]
    win_all   = sum(1 for r in ranks_all if r == 1) / len(ranks_all)
    surf_rows = [r for r in rows if r[1] == surface]
    win_surf  = (sum(1 for r in surf_rows if r[0] == 1) / len(surf_rows)) if surf_rows else 0.0
    lo, hi    = _DIST_BANDS[band]
    dist_rows = [r for r in rows if lo <= r[2] < hi]
    win_dist  = (sum(1 for r in dist_rows if r[0] == 1) / len(dist_rows)) if dist_rows else 0.0
    return {
        "win_rate_all":           win_all,
        "win_rate_surface":       win_surf,
        "win_rate_distance_band": win_dist,
        "recent_rank_mean":       float(np.mean(ranks_all[:5])),
    }
```

---

## Task 3: レース特徴量DataFrame構築・払戻取得

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: build_race_df を追加する（test_mode対応）**

```python
def build_race_df(
    conn: sqlite3.Connection,
    race_id: str,
    race_date: str,
    include_rank: bool = False,
) -> pd.DataFrame | None:
    race_row = conn.execute(
        "SELECT distance, surface, venue, condition FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if not race_row:
        return None
    distance, surface, venue, condition = race_row

    rr_rows = conn.execute(
        """
        SELECT rr.horse_id, rr.horse_name, rr.rank,
               rr.weight_carried, rr.horse_weight, rr.horse_weight_diff,
               rr.gate_number, rr.horse_number,
               rr.sex_age, rr.jockey, rr.trainer,
               h.sire, rr.win_odds
        FROM race_results rr
        LEFT JOIN horses h ON h.horse_id = rr.horse_id
        WHERE rr.race_id = ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY rr.horse_number
        """,
        (race_id,),
    ).fetchall()
    if len(rr_rows) < 2:
        return None

    records = []
    for (horse_id, horse_name, raw_rank,
         weight_carried, horse_weight, hw_diff,
         gate_number, horse_number,
         sex_age, jockey, trainer, sire, win_odds) in rr_rows:
        stats   = _horse_stats(conn, horse_id or "", race_date, surface, distance)
        sex_str = (sex_age or "")[:1]
        rank    = int(raw_rank) if raw_rank and int(raw_rank) > 0 else 99
        records.append({
            "race_id":           race_id,
            "race_date":         race_date,
            "horse_id":          horse_id or "",
            "horse_name":        horse_name or "",
            "horse_number":      int(horse_number or 0),
            "rank":              rank,
            "win_odds":          float(win_odds) if win_odds else None,
            "weight_carried":    float(weight_carried or 55.0),
            "horse_weight":      float(horse_weight or 480.0),
            "horse_weight_diff": float(hw_diff or 0.0),
            "gate_number":       int(gate_number or 0),
            "distance":          int(distance or 1600),
            "surface_code":      _SURFACE_CODE.get(surface or "", -1),
            "sex_code":          _SEX_CODE.get(sex_str, -1),
            "venue_code":        _VENUE_CODE.get(venue or "", 10),
            "condition_code":    _COND_CODE.get(condition or "", 0),
            "sire_code":         _encode_sire(sire),
            "jockey_win_rate":   _JKY_MAP.get(jockey or "", 0.05),
            "trainer_win_rate":  _TRN_MAP.get(trainer or "", 0.05),
            **stats,
        })
    df = pd.DataFrame(records)
    if not include_rank:
        df = df.drop(columns=["rank"])
    return df
```

- [ ] **Step 2: 払戻取得関数を追加する**

```python
def get_payouts(conn: sqlite3.Connection, race_id: str) -> dict[str, dict]:
    """bet_type（日本語）→ {combination: payout} のマップを返す"""
    rows = conn.execute(
        "SELECT bet_type, combination, payout FROM race_payouts WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    result: dict[str, dict] = defaultdict(dict)
    for bt, combo, payout in rows:
        if bt in BET_TYPES:
            result[bt][combo] = int(payout or 0)
    return dict(result)
```

---

## Task 4: モデル訓練（2024年1〜5月データ）

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: train_model 関数を追加する**

```python
def train_model(conn: sqlite3.Connection) -> tuple:
    logger.info("Train セット構築中 (%s ～ %s)...", TRAIN_FROM, TRAIN_TO)
    race_list = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY r.date, r.race_id
        """,
        (TRAIN_FROM, TRAIN_TO),
    ).fetchall()
    logger.info("Train レース数: %d", len(race_list))

    all_dfs: list[pd.DataFrame] = []
    for race_id, race_date in race_list:
        df = build_race_df(conn, race_id, race_date, include_rank=True)
        if df is not None and len(df) >= 2:
            all_dfs.append(df)
    if not all_dfs:
        raise RuntimeError("Train データが0件")

    train_df = pd.concat(all_dfs, ignore_index=True)
    logger.info("Train サンプル数: %d  (レース=%d)", len(train_df), len(all_dfs))

    X      = train_df[FEATURE_COLS].astype(float).fillna(-1).values
    y      = (train_df["rank"] == 1).astype(int).values
    groups = train_df["race_id"].values

    cv        = GroupKFold(n_splits=5)
    aucs:     list[float] = []
    oof_preds = np.zeros(len(y))
    for fold, (tr_idx, va_idx) in enumerate(cv.split(X, y, groups)):
        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X[tr_idx], y[tr_idx])
        preds = clf.predict_proba(X[va_idx])[:, 1]
        oof_preds[va_idx] = preds
        if y[va_idx].sum() > 0:
            aucs.append(roc_auc_score(y[va_idx], preds))

    cv_auc = float(np.mean(aucs)) if aucs else float("nan")
    logger.info("CV AUC (train): %.4f", cv_auc)

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oof_preds, y)

    final_clf = LGBMClassifier(**LGBM_PARAMS)
    final_clf.fit(X, y)
    return final_clf, iso, cv_auc


def predict_win_prob(model, iso, df: pd.DataFrame) -> np.ndarray:
    X   = df[FEATURE_COLS].astype(float).fillna(-1)
    raw = model.predict_proba(X)[:, 1]
    return iso.predict(raw)
```

---

## Task 5: Harville確率計算関数

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: Harville確率の各関数を追加する**

```python
def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    """P(i と j が 1着・2着 = 馬連)"""
    pi, pj = probs[i], probs[j]
    q  = pi * pj / (1.0 - pi) if pi < 1.0 else 0.0
    q += pj * pi / (1.0 - pj) if pj < 1.0 else 0.0
    return min(q, 0.99)

def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    """P(i, j, k が 1-3着 = 三連複)"""
    total = 0.0
    for a, b, c in itertools.permutations([i, j, k]):
        pa, pb, pc = probs[a], probs[b], probs[c]
        d1 = 1.0 - pa
        d2 = 1.0 - pa - pb
        if d1 > 0 and d2 > 0:
            total += pa * pb / d1 * pc / d2
    return min(total, 0.99)

def _harville_wide(probs: list[float], i: int, j: int) -> float:
    """P(i と j が 1-3着に入る = ワイド)"""
    total = sum(
        _harville_trio(probs, i, j, k)
        for k in range(len(probs))
        if k != i and k != j
    )
    return min(total, 0.99)

def _combo_key(*nums: int) -> str:
    return "-".join(str(n) for n in sorted(nums))
```

---

## Task 6: Kelly賭け金算出・1レースシミュレーション

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: kelly_bet 関数を追加する（初期¥10,000対応の保守的設定）**

```python
def kelly_bet(
    bankroll: float,
    p: float,
    bet_type: str,
    ev_threshold: float,
    race_budget_remaining: float,
    actual_win_odds: float | None = None,
) -> float:
    """EV閾値フィルタ + フラクショナルKelly でベット額を算出"""
    # EV計算
    if bet_type == "単勝" and actual_win_odds is not None and actual_win_odds > 1.0:
        odds = actual_win_odds
    elif bet_type == "複勝" and actual_win_odds is not None and actual_win_odds > 1.0:
        odds = max(actual_win_odds / 3.0, 1.1)
    else:
        odds = TYPICAL_ODDS.get(bet_type, 5.0)

    ev = p * odds
    if ev < ev_threshold:
        return 0.0

    kelly_full = (p * odds - 1.0) / max(odds - 1.0, 1e-9)
    kelly_full = max(kelly_full, 0.0)
    effective  = kelly_full * KELLY_FRACTION

    rel_cap = bankroll * MAX_BET_RATE
    bet     = min(bankroll * effective, rel_cap, race_budget_remaining)
    if bet < MIN_BET:
        if kelly_full > 0 and race_budget_remaining >= MIN_BET:
            return float(MIN_BET)
        return 0.0
    return float(int(bet / 100) * 100)
```

- [ ] **Step 2: simulate_race_pattern 関数を追加する（1レース・1パターン分）**

```python
def simulate_race_pattern(
    race_id: str,
    race_date: str,
    race_df: pd.DataFrame,
    probs: np.ndarray,
    payouts: dict[str, dict],
    bet_type: str,
    ev_threshold: float,
    bankroll: float,
) -> list[dict]:
    """指定パターン（bet_type × ev_threshold）で1レースをシミュレートしてベット記録を返す"""
    n          = len(race_df)
    horse_nums = list(race_df["horse_number"].astype(int))
    prob_list  = list(probs)
    sorted_idx = sorted(range(n), key=lambda i: -prob_list[i])
    race_budget = min(bankroll * MAX_RACE_BUDGET_RATE, bankroll)  # 初期¥10,000対応

    records: list[dict] = []
    total_invested = 0.0

    def add_bet(combo: str, p_bet: float, w_odds: float | None = None) -> None:
        nonlocal total_invested
        remaining = race_budget - total_invested
        if remaining < MIN_BET:
            return
        bet = kelly_bet(bankroll, p_bet, bet_type, ev_threshold, remaining, w_odds)
        if bet <= 0:
            return
        pmap     = payouts.get(bet_type, {})
        payout_a = float(pmap.get(combo, 0))
        is_hit   = payout_a > 0
        profit   = (payout_a / 100.0 * bet) - bet if is_hit else -bet
        records.append({
            "race_id":      race_id,
            "date":         race_date,
            "bet_type":     bet_type,
            "ev_threshold": ev_threshold,
            "combo":        combo,
            "p_bet":        p_bet,
            "bet_amount":   bet,
            "payout_per100": payout_a,
            "is_hit":       int(is_hit),
            "profit":       profit,
        })
        total_invested += bet

    i1 = sorted_idx[0]
    h1 = horse_nums[i1]
    p1 = prob_list[i1]
    w1 = race_df.iloc[i1]["win_odds"] if "win_odds" in race_df.columns else None

    if bet_type == "単勝":
        add_bet(str(h1), p1, w1)

    elif bet_type == "複勝":
        p_place = min(p1 * 2.5, 0.95)
        add_bet(str(h1), p_place, w1)

    elif bet_type in ("ワイド", "馬連") and n >= 2:
        i2 = sorted_idx[1]
        h2 = horse_nums[i2]
        if bet_type == "ワイド":
            p_bet = _harville_wide(prob_list, i1, i2)
        else:
            p_bet = _harville_quinella(prob_list, i1, i2)
        combo = _combo_key(h1, h2)
        add_bet(combo, p_bet)

    elif bet_type == "三連複" and n >= 3:
        i2, i3 = sorted_idx[1], sorted_idx[2]
        h2, h3 = horse_nums[i2], horse_nums[i3]
        p_bet   = _harville_trio(prob_list, i1, i2, i3)
        combo   = _combo_key(h1, h2, h3)
        add_bet(combo, p_bet)

    return records
```

---

## Task 7: 25パターン並行シミュレーションループ

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: SimResult データクラスを追加する**

```python
@dataclass
class SimResult:
    bet_type:      str
    ev_threshold:  float
    bankroll:      float = INITIAL_BANKROLL
    peak_bankroll: float = INITIAL_BANKROLL
    records:       list[dict] = field(default_factory=list)
    bl_history:    list[float] = field(default_factory=list)

    @property
    def key(self) -> Pattern:
        return (self.bet_type, self.ev_threshold)
```

- [ ] **Step 2: run_simulation 関数を追加する（25パターン並行管理）**

```python
def run_simulation(conn: sqlite3.Connection, model, iso) -> dict[Pattern, SimResult]:
    race_list = conn.execute(
        """
        SELECT DISTINCT r.race_id, r.date
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
        JOIN race_payouts  rp ON rp.race_id = r.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        ORDER BY r.date, r.race_id
        """,
        (TEST_FROM, TEST_TO),
    ).fetchall()
    logger.info("テストレース数: %d", len(race_list))

    # 25パターン分の SimResult を初期化
    results: dict[Pattern, SimResult] = {
        (bt, ev): SimResult(bet_type=bt, ev_threshold=ev)
        for bt in BET_TYPES
        for ev in EV_THRESHOLDS
    }

    for idx, (race_id, race_date) in enumerate(race_list):
        race_df = build_race_df(conn, race_id, race_date, include_rank=True)
        if race_df is None or len(race_df) < 2:
            continue

        feat_df = race_df.drop(columns=["rank"])
        probs   = predict_win_prob(model, iso, feat_df)
        payouts = get_payouts(conn, race_id)

        # 25パターンそれぞれで独立シミュレーション
        for (bt, ev), sim in results.items():
            if sim.bankroll < MIN_BET:
                continue  # 破産したパターンはスキップ
            bets = simulate_race_pattern(
                race_id, race_date, feat_df, probs, payouts,
                bt, ev, sim.bankroll,
            )
            if bets:
                net = sum(b["profit"] for b in bets)
                sim.bankroll  = max(sim.bankroll + net, 0.0)
                sim.peak_bankroll = max(sim.peak_bankroll, sim.bankroll)
                sim.records.extend(bets)
            sim.bl_history.append(sim.bankroll)

        if (idx + 1) % 500 == 0:
            logger.info("進行 %d/%d レース", idx + 1, len(race_list))

    return results
```

---

## Task 8: 成績マトリクス集計・推奨ポートフォリオ出力

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: 指標集計ヘルパーを追加する**

```python
def _payout_sum(records: list[dict]) -> float:
    return sum(
        r["payout_per100"] / 100.0 * r["bet_amount"] if r["is_hit"] else 0.0
        for r in records
    )

def calc_max_drawdown(bl_history: list[float]) -> float:
    if not bl_history:
        return 0.0
    peak = bl_history[0]
    max_dd = 0.0
    for bl in bl_history:
        peak   = max(peak, bl)
        if peak > 0:
            dd     = (peak - bl) / peak * 100.0
            max_dd = max(max_dd, dd)
    return max_dd

def summarize_pattern(sim: SimResult) -> dict:
    recs     = sim.records
    n_bets   = len(recs)
    n_hits   = sum(r["is_hit"] for r in recs)
    total_in = sum(r["bet_amount"] for r in recs)
    total_out= _payout_sum(recs)
    roi      = total_out / total_in * 100 if total_in > 0 else 0.0
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0
    profit   = total_out - total_in
    mdd      = calc_max_drawdown(sim.bl_history)
    return {
        "bet_type":      sim.bet_type,
        "ev_threshold":  sim.ev_threshold,
        "n_bets":        n_bets,
        "n_hits":        n_hits,
        "hit_rate":      round(hit_rate, 1),
        "total_invest":  int(total_in),
        "total_return":  int(total_out),
        "net_profit":    int(profit),
        "roi":           round(roi, 1),
        "max_drawdown":  round(mdd, 1),
        "final_bankroll": int(sim.bankroll),
    }
```

- [ ] **Step 2: print_matrix 関数を追加する（25パターンのテーブル出力）**

```python
SEP = "=" * 100

def print_matrix(results: dict[Pattern, SimResult], cv_auc: float) -> pd.DataFrame:
    rows = [summarize_pattern(sim) for sim in results.values()]
    df   = pd.DataFrame(rows).sort_values(["bet_type", "ev_threshold"])

    print(SEP)
    print("【Walk-Forward Backtest 2024-2025 — 25パターン 成績マトリクス】")
    print(f"  Train  : {TRAIN_FROM} ～ {TRAIN_TO}")
    print(f"  Test   : {TEST_FROM}  ～ {TEST_TO}")
    print(f"  CV AUC : {cv_auc:.4f}")
    print(f"  初期資金: ¥{INITIAL_BANKROLL:,.0f}  Kelly分数: {KELLY_FRACTION}")
    print(SEP)
    print()

    # ROI マトリクス（行: bet_type, 列: ev_threshold）
    print("■ ROI(%) マトリクス")
    pivot_roi = df.pivot(index="bet_type", columns="ev_threshold", values="roi")
    pivot_roi = pivot_roi.reindex(BET_TYPES)
    print(pivot_roi.to_string(float_format=lambda x: f"{x:6.1f}%"))

    print()
    print("■ 的中率(%) マトリクス")
    pivot_hit = df.pivot(index="bet_type", columns="ev_threshold", values="hit_rate")
    pivot_hit = pivot_hit.reindex(BET_TYPES)
    print(pivot_hit.to_string(float_format=lambda x: f"{x:6.1f}%"))

    print()
    print("■ 最大ドローダウン(%) マトリクス")
    pivot_mdd = df.pivot(index="bet_type", columns="ev_threshold", values="max_drawdown")
    pivot_mdd = pivot_mdd.reindex(BET_TYPES)
    print(pivot_mdd.to_string(float_format=lambda x: f"{x:6.1f}%"))

    print()
    print("■ 純利益(円) マトリクス")
    pivot_pnl = df.pivot(index="bet_type", columns="ev_threshold", values="net_profit")
    pivot_pnl = pivot_pnl.reindex(BET_TYPES)
    print(pivot_pnl.to_string(float_format=lambda x: f"¥{x:+,.0f}"))

    # 詳細テーブル
    print()
    print("■ 全25パターン 詳細テーブル")
    hdr = (f"{'券種':<8}{'EV閾値':>7}{'件数':>6}{'的中率':>8}"
           f"{'投資額':>12}{'回収額':>12}{'ROI':>8}{'損益':>12}{'最大DD':>8}{'最終残高':>12}")
    print(hdr)
    print("-" * 95)
    for _, row in df.iterrows():
        star = " ★" if row["roi"] >= 100.0 else ("  △" if row["roi"] >= 80.0 else "")
        print(
            f"{row['bet_type']:<8}{row['ev_threshold']:>7.1f}"
            f"{row['n_bets']:>6}{row['hit_rate']:>7.1f}%"
            f"{row['total_invest']:>12,}{row['total_return']:>12,}"
            f"{row['roi']:>7.1f}%{row['net_profit']:>+12,}"
            f"{row['max_drawdown']:>7.1f}%{row['final_bankroll']:>12,}{star}"
        )
    print(SEP)

    return df
```

- [ ] **Step 3: recommend_portfolio 関数を追加する（推奨ポートフォリオ計算）**

```python
def recommend_portfolio(df: pd.DataFrame) -> str:
    """ROI・的中率・最大DDから推奨購入ポートフォリオを計算してテキストで返す"""
    profitable = df[(df["roi"] > 100.0) & (df["n_bets"] >= 10)].copy()

    lines = ["", "■ 推奨購入ポートフォリオ方針", "─" * 60]

    if profitable.empty:
        lines.append("  ⚠️  ROI>100%かつ10件以上のパターンが存在しません。")
        lines.append("  単勝・複勝のEV閾値を上げて（1.2〜1.3）絞り込み運用を推奨します。")
        best5 = df.sort_values("roi", ascending=False).head(5)
        lines.append("")
        lines.append("  ROI上位5パターン（参考）:")
        for _, r in best5.iterrows():
            lines.append(
                f"    {r['bet_type']} × EV≥{r['ev_threshold']:.1f}  "
                f"ROI={r['roi']:.1f}%  的中率={r['hit_rate']:.1f}%  DD={r['max_drawdown']:.1f}%"
            )
        return "\n".join(lines)

    # ROI × (1 - max_drawdown/100) でスコアリング（利益性と安定性の積）
    profitable["score"] = profitable["roi"] * (1 - profitable["max_drawdown"] / 100.0)
    total_score = profitable["score"].sum()
    profitable["weight"] = (profitable["score"] / total_score * 100).round(1)

    lines.append("")
    lines.append("  ✅ 推奨パターン（ROI>100% かつ件数>=10 の全組み合わせ）:")
    lines.append(f"  {'券種':<8}{'EV閾値':>7}{'ROI':>8}{'的中率':>8}{'最大DD':>8}{'推奨配分':>10}")
    lines.append("  " + "-" * 52)
    for _, r in profitable.sort_values("score", ascending=False).iterrows():
        lines.append(
            f"  {r['bet_type']:<8}{r['ev_threshold']:>7.1f}"
            f"{r['roi']:>7.1f}%{r['hit_rate']:>7.1f}%"
            f"{r['max_drawdown']:>7.1f}%{r['weight']:>9.1f}%"
        )

    lines.append("")
    lines.append("  【ポートフォリオ構築方針】")
    best = profitable.sort_values("score", ascending=False).iloc[0]
    lines.append(f"  1. 主軸: {best['bet_type']} × EV≥{best['ev_threshold']:.1f} に資金の{best['weight']:.0f}%を投下")
    lines.append(f"     ROI={best['roi']:.1f}%  的中率={best['hit_rate']:.1f}%  最大DD={best['max_drawdown']:.1f}%")
    lines.append("")
    lines.append("  2. リスク分散: 上記推奨パターンを「score比率」で資金を按分する。")
    lines.append("     score = ROI × (1 - 最大DD/100)  ← 利益性と安定性の積")
    lines.append("")
    lines.append("  3. 注意: EV閾値が高いほど件数が減り標準誤差が大きくなる。")
    lines.append("     10件未満のパターンは統計的信頼性が低いため本番投資には使わない。")
    lines.append("")
    lines.append("  4. 運用上の目安:")
    lines.append("     - 週末ごとに全レースのEVを計算し、閾値超えのみ発注")
    lines.append("     - 最大DD > 50% のパターンは破産リスクが高いため除外を検討")
    lines.append("     - 月次でROIを実績値と比較し、乖離が20%超なら配分を見直す")

    return "\n".join(lines)
```

---

## Task 9: JSON保存・メイン関数

**Files:**
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: save_results 関数を追加する**

```python
def save_results(df: pd.DataFrame, cv_auc: float) -> None:
    RULES_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "train": [TRAIN_FROM, TRAIN_TO],
            "test":  [TEST_FROM,  TEST_TO],
            "cv_auc": cv_auc,
            "kelly_fraction": KELLY_FRACTION,
            "initial_bankroll": INITIAL_BANKROLL,
            "generated_at": "2026-05-23",
        },
        "patterns": df.to_dict(orient="records"),
    }
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON 保存: %s", OUT_JSON)
```

- [ ] **Step 2: main 関数を追加する**

```python
def main() -> None:
    logger.info("=== Walk-Forward Backtest 2024-2025 開始 ===")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    logger.info("Step 1: 騎手/調教師 勝率マップ構築...")
    build_jockey_trainer_maps(conn)

    logger.info("Step 2: モデル訓練 (2024年1〜5月データ)...")
    model, iso, cv_auc = train_model(conn)

    logger.info("Step 3: 2025年 25パターン並行シミュレーション...")
    results = run_simulation(conn, model, iso)

    logger.info("Step 4: 成績マトリクス集計・出力...")
    summary_df = print_matrix(results, cv_auc)

    portfolio_text = recommend_portfolio(summary_df)
    print(portfolio_text)

    logger.info("Step 5: 結果保存...")
    save_results(summary_df, cv_auc)

    logger.info("=== バックテスト完了 ===")
    conn.close()


if __name__ == "__main__":
    main()
```

---

## Task 10: スクリプト実行・出力確認

**Files:**
- Run: `src/analysis/walk_forward_backtest_2024_2025.py`

- [ ] **Step 1: スクリプトを実行する**

```bash
py src/analysis/walk_forward_backtest_2024_2025.py 2>&1 | tee data/wf_backtest_2024_2025_run.log
```

期待される出力（抜粋）:
```
Walk-Forward Backtest 2024-2025 — 25パターン 成績マトリクス
Train  : 2024-01-01 ～ 2024-05-31
Test   : 2025-01-01 ～ 2025-12-31
CV AUC : 0.6xxx
...
■ ROI(%) マトリクス
...
```

- [ ] **Step 2: 実行ログで異常がないか確認する（ゼロ件・エラーなし）**

```bash
grep -E "ERROR|WARN|0件|破産" data/wf_backtest_2024_2025_run.log | head -20
```

期待: 致命的エラーがないこと。ベット件数が各パターン10件以上あること。

---

## Task 11: ポートフォリオ方針ドキュメント保存

**Files:**
- Create: `.claudecode/rules/portfolio_strategy_2024_2025.md`

- [ ] **Step 1: バックテスト実行結果を元に方針ドキュメントを作成する**

実行結果（ROIマトリクス・推奨パターン）を踏まえて、以下のテンプレートに数値を埋めて作成する:

```markdown
# 購入ポートフォリオ方針 — 2024-2025年バックテスト根拠版

**策定日**: 2026-05-23
**根拠**: Walk-Forward Backtest (Train: 2024-01〜05月 / Test: 2025年全12ヶ月)
**CV AUC**: [実測値]
**モデル**: LightGBM 1着確率分類（16特徴量）

---

## バックテスト結果サマリー

### ROI マトリクス（%）

[実行結果から貼り付け]

### 推奨購入パターン（ROI>100%かつ件数>=10）

[実行結果から貼り付け]

---

## 今後の購入方針（永続ルール）

### 主軸戦略
- 券種: [最高スコアのbet_type]
- EV閾値: [最高スコアのev_threshold]
- 資金比率: [推奨weight]%

### リスク管理
- 1レース最大投資: bankroll × 15%
- 1ベット最大投資: bankroll × 5%
- Kelly分数: 1/20（保守的）
- 最小ベット: ¥100

### 除外ルール（絶対）
- Oracle系 / HitFocus系: 過去的中ベースのフィルタ → 未来リーク
- 暫定予想: EV未計算のため統計的根拠なし
- WIN5: 単独システムにつきポートフォリオ外
- 馬単 / 三連単: バックテスト上ROI < 10%（赤字確定）

### 運用チェックリスト（週次）
- [ ] 当週の全レースEVを算出
- [ ] EV閾値以上のベットのみ実行
- [ ] 月次ROIを記録して乖離チェック（±20%超で方針見直し）
- [ ] 最大DD が50%に近づいたらベット停止
```

---

## Task 12: ドキュメント更新・git commit

**Files:**
- Modify: `docs/1_prediction_logic.md`

- [ ] **Step 1: `docs/1_prediction_logic.md` の更新履歴に追記する**

`docs/1_prediction_logic.md` の更新履歴セクションの先頭に以下を追加:

```markdown
| 2026-05-23 | Walk-Forward Backtest 2024-2025 実施: 5券種×EV閾値5段階=25パターン成績マトリクス生成。推奨ポートフォリオ方針を .claudecode/rules/portfolio_strategy_2024_2025.md に保存。影響ファイル: src/analysis/walk_forward_backtest_2024_2025.py |
```

- [ ] **Step 2: git commit する**

```bash
git add src/analysis/walk_forward_backtest_2024_2025.py \
        .claudecode/rules/portfolio_strategy_2024_2025.md \
        data/walk_forward_backtest_2024_2025.json \
        docs/1_prediction_logic.md \
        docs/superpowers/plans/2026-05-23-walk-forward-backtest-2024-2025.md
git commit -m "feat: walk-forward backtest 2024-2025 — 25パターン成績マトリクス & 推奨ポートフォリオ方針"
```

---

## Self-Review チェックリスト

- [x] **Spec coverage**: 5券種×EV閾値5段階=25パターン ✅ / Train 2024-01〜05 ✅ / Test 2025全12ヶ月 ✅ / 除外（HitFocus/Oracle/暫定/WIN5） ✅ / ポートフォリオ推奨 ✅ / git commit ✅
- [x] **Placeholder scan**: 全コードブロックは具体的な実装を含む。TBD/TODOなし ✅
- [x] **Type consistency**: `Pattern = tuple[str, float]` / `SimResult` / `summarize_pattern()` / `print_matrix()` / `recommend_portfolio()` で一貫 ✅
- [x] **bet_type比較**: `race_payouts.bet_type` は日本語直接格納確認済み → `payouts.get(bet_type, {})` で直接参照 ✅
- [x] **初期¥10,000でも破産しにくいKelly設定**: `KELLY_FRACTION=0.05` + `MAX_BET_RATE=0.05` (最大¥500/ベット初期段階) ✅
