# Strict Walk-Forward Backtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 2024-2025年の全モデルに対してデータリーク完全排除の Walk-Forward バックテストを実行し、年間ROI > 100% を達成したモデルを社長向けに報告する。

**Architecture:**
- 単一スクリプト `scripts/run_strict_backtest.py` に全ロジックを集約
- AlphaModel は既存の `load_training_data(min_date, max_date)` API を流用し半期3窓の expanding-window Walk-Forward を実行（高速）
- HonmeiModel / ManjiModel / PlaceModel は `_build_train_df(conn, train_until=2024)` で 2024学習→2025テストの年単位 Walk-Forward を実行（低速だが正確）
- データ品質チェックを全モデル共通の前処理として先行実施し、汚染 race_id を除外リストに登録

**Tech Stack:** Python 3.11, SQLite, LightGBM, pandas, scikit-learn, src.ml.alpha_model, src.ml.models

---

## File Map

| アクション | パス | 責務 |
|---|---|---|
| Create | `scripts/run_strict_backtest.py` | メインスクリプト（全フェーズ統合） |
| Read   | `src/ml/alpha_model.py` | AlphaModel.load_training_data / predict_ev / train |
| Read   | `src/ml/models.py` | _build_train_df / HonmeiModel / ManjiModel / PlaceModel |

---

## Task 1: データ品質チェック関数

**Files:**
- Create: `scripts/run_strict_backtest.py`（ここから書き始める）

- [ ] **Step 1: スクリプト骨格を作成**

```python
#!/usr/bin/env python
"""
厳密 Walk-Forward バックテスト (2024-2025)
=========================================
全モデル対象・データリーク完全排除・EV閾値スイープ・社長向け最終報告

実行:
  py scripts/run_strict_backtest.py
  py scripts/run_strict_backtest.py --alpha-only    # AlphaModelのみ（高速）
  py scripts/run_strict_backtest.py --no-sweep      # EV閾値スイープをスキップ
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_MAIN_DB     = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"

# ── Walk-Forward ウィンドウ定義（AlphaModel 用・半期 expanding）─────────────
# 形式: (train_start, train_end, test_start, test_end, label)
_ALPHA_WINDOWS: list[tuple[str, str, str, str, str]] = [
    ("2024-01-01", "2024-06-30", "2024-07-01", "2024-12-31", "2024H2"),
    ("2024-01-01", "2024-12-31", "2025-01-01", "2025-06-30", "2025H1"),
    ("2024-01-01", "2025-06-30", "2025-07-01", "2025-12-31", "2025H2"),
]

# EV閾値スイープ候補
_EV_SWEEP = [1.1, 1.2, 1.3, 1.5, 1.8, 2.0, 2.5]
# デフォルト評価閾値（スイープ前の基準）
_DEFAULT_ALPHA_THRESHOLD = 1.5
_DEFAULT_HONMEI_EV_THRESHOLD = 1.3  # Honmei EV = P(win) × win_odds
_DEFAULT_MANJI_EV_THRESHOLD  = 1.1  # Manji は直接 ev_target を予測

# ── 結果コンテナ ──────────────────────────────────────────────────────────

@dataclass
class WindowResult:
    """1バックテスト窓の結果"""
    model_name: str
    bet_type: str
    window_label: str
    ev_threshold: float
    n_bets: int
    n_hits: int
    hit_rate: float
    total_invest: int
    total_payout: float
    roi: float
    max_drawdown: float
    notes: list[str] = field(default_factory=list)

    @property
    def is_profitable(self) -> bool:
        return self.roi >= 100.0
```

- [ ] **Step 2: データ品質チェック関数を実装**

同じファイルに続けて書く:

```python
# ── データ品質チェック ────────────────────────────────────────────────────

@dataclass
class DataQualityReport:
    garbled_race_ids: list[str]
    corrupt_rank_ids: list[str]
    bad_odds_ids: list[str]
    excluded_ids: set[str]

    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print("  [データ品質チェック] 2024-2025年")
        print(f"{'='*60}")
        print(f"  文字化けレース  : {len(self.garbled_race_ids):>5,} 件")
        print(f"  rank 汚染レース  : {len(self.corrupt_rank_ids):>5,} 件")
        print(f"  オッズ欠損80%超  : {len(self.bad_odds_ids):>5,} 件")
        print(f"  ── 除外合計      : {len(self.excluded_ids):>5,} 件")
        if self.excluded_ids:
            sample = list(self.excluded_ids)[:5]
            print(f"  除外サンプル     : {sample}")


def check_data_quality(
    conn: sqlite3.Connection,
    start: str = "2024-01-01",
    end: str   = "2025-12-31",
) -> DataQualityReport:
    """
    対象期間の汚染 race_id を検出して返す。
    除外されたレースはバックテストから完全に除外する。
    """
    # 1. 文字化けレース名（race_name に '?' が含まれる）
    garbled = [
        r[0] for r in conn.execute(
            """SELECT r.race_id FROM races r
               WHERE r.date BETWEEN ? AND ?
                 AND (r.race_name LIKE '%?%' OR r.race_name = '')""",
            (start, end),
        ).fetchall()
    ]

    # 2. rank 汚染（障害払戻レコードの誤挿入: rank=20,30,...90）
    corrupt_rank = [
        r[0] for r in conn.execute(
            """SELECT DISTINCT rr.race_id FROM race_results rr
               JOIN races r ON rr.race_id = r.race_id
               WHERE r.date BETWEEN ? AND ?
                 AND rr.rank IN (20,30,40,50,60,70,80,90)""",
            (start, end),
        ).fetchall()
    ]

    # 3. オッズ欠損率 80% 超（レース中止等）
    bad_odds = [
        r[0] for r in conn.execute(
            """SELECT rr.race_id
               FROM race_results rr
               JOIN races r ON rr.race_id = r.race_id
               WHERE r.date BETWEEN ? AND ?
               GROUP BY rr.race_id
               HAVING SUM(CASE WHEN rr.win_odds IS NULL OR rr.win_odds <= 0 THEN 1 ELSE 0 END)
                      * 1.0 / COUNT(*) > 0.80""",
            (start, end),
        ).fetchall()
    ]

    excluded = set(garbled + corrupt_rank + bad_odds)
    return DataQualityReport(
        garbled_race_ids=garbled,
        corrupt_rank_ids=corrupt_rank,
        bad_odds_ids=bad_odds,
        excluded_ids=excluded,
    )
```

- [ ] **Step 3: スクリプトがここまで import エラーなく動くか確認**

```
py scripts/run_strict_backtest.py --help
```
→ `error: unrecognized arguments` で落ちなければ OK（main 未実装のため）

---

## Task 2: AlphaModel 半期 Walk-Forward

- [ ] **Step 1: 1窓ぶんの AlphaModel バックテスト関数を実装**

```python
from src.ml.alpha_model import AlphaModel, BET_TYPE_TANSHO, BET_TYPE_FUKUSHO


def _backtest_alpha_window(
    conn: sqlite3.Connection,
    train_start: str,
    train_end: str,
    test_start: str,
    test_end: str,
    window_label: str,
    bet_type: str,
    ev_threshold: float,
    excluded_ids: set[str],
    research_db_path: Optional[Path] = None,
) -> Optional[WindowResult]:
    """
    AlphaModel の1ウィンドウ Walk-Forward バックテスト。

    データリーク防止:
      - 学習データ: train_start ≤ date ≤ train_end
      - テストデータ: test_start ≤ date ≤ test_end
      - 両データセットは時系列的に重複しない
    """
    model = AlphaModel()

    # 学習データロード（除外 race_id をフィルタ）
    all_train = model.load_training_data(
        conn,
        min_date=train_start,
        max_date=train_end,
        bet_type=bet_type,
        research_db_path=research_db_path,
    )
    if excluded_ids:
        all_train = all_train[~all_train["race_id"].isin(excluded_ids)]
    if len(all_train) < 500:
        logger.warning("[%s] 学習データ不足: %d行 → スキップ", window_label, len(all_train))
        return None

    # テストデータロード
    test_df = model.load_training_data(
        conn,
        min_date=test_start,
        max_date=test_end,
        bet_type=bet_type,
        research_db_path=research_db_path,
    )
    if excluded_ids:
        test_df = test_df[~test_df["race_id"].isin(excluded_ids)]
    if len(test_df) < 50:
        logger.warning("[%s] テストデータ不足: %d行 → スキップ", window_label, len(test_df))
        return None

    print(f"  [{window_label}] 学習{len(all_train):,}行 → テスト{len(test_df):,}行", flush=True)

    metrics = model.train(all_train)
    print(f"  [{window_label}] AUC={metrics['auc']:.3f} LogLoss={metrics['logloss']:.4f}", flush=True)

    # EV 予測
    test_df = test_df.copy()
    test_df["ev_pred"] = model.predict_ev(test_df).values

    # 買いシグナル
    bets = test_df[test_df["ev_pred"] >= ev_threshold].copy()
    if bets.empty:
        return WindowResult(
            model_name=f"ALPHA({bet_type})",
            bet_type=bet_type,
            window_label=window_label,
            ev_threshold=ev_threshold,
            n_bets=0, n_hits=0, hit_rate=0,
            total_invest=0, total_payout=0, roi=0, max_drawdown=0,
            notes=["買いシグナルなし"],
        )

    # 固定 ¥100 賭け（Kelly は別途計算できるが、モデル比較には固定ベットが公平）
    invest = len(bets) * 100
    payout = float((bets["is_hit"] * bets["actual_payout"].fillna(0)).sum())
    roi = payout / invest * 100
    hit_rate = bets["is_hit"].mean() * 100

    # 最大ドローダウン
    pnl = (bets["is_hit"] * bets["actual_payout"].fillna(0) - 100).values
    cum = np.cumsum(pnl)
    max_dd = float(np.max(np.maximum.accumulate(cum) - cum)) if len(cum) > 0 else 0.0

    return WindowResult(
        model_name=f"ALPHA({bet_type})",
        bet_type=bet_type,
        window_label=window_label,
        ev_threshold=ev_threshold,
        n_bets=len(bets),
        n_hits=int(bets["is_hit"].sum()),
        hit_rate=hit_rate,
        total_invest=invest,
        total_payout=payout,
        roi=roi,
        max_drawdown=max_dd,
    )
```

- [ ] **Step 2: AlphaModel の全窓+全馬券種を実行する関数を実装**

```python
def run_alpha_walkforward(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
    ev_threshold: float = _DEFAULT_ALPHA_THRESHOLD,
    research_db_path: Optional[Path] = None,
) -> list[WindowResult]:
    """3窓×2馬券種 (単勝/複勝) の AlphaModel Walk-Forward を実行。"""
    results: list[WindowResult] = []

    for bet_type in (BET_TYPE_TANSHO, BET_TYPE_FUKUSHO):
        print(f"\n{'─'*60}")
        print(f"  AlphaModel [{bet_type}] EV閾値={ev_threshold}")
        print(f"{'─'*60}")
        for tr_start, tr_end, te_start, te_end, label in _ALPHA_WINDOWS:
            r = _backtest_alpha_window(
                conn=conn,
                train_start=tr_start,
                train_end=tr_end,
                test_start=te_start,
                test_end=te_end,
                window_label=label,
                bet_type=bet_type,
                ev_threshold=ev_threshold,
                excluded_ids=excluded_ids,
                research_db_path=research_db_path,
            )
            if r is not None:
                roi_mark = "✅" if r.is_profitable else "❌"
                print(
                    f"  {label}: {r.n_bets:,}点 的中{r.n_hits}({r.hit_rate:.1f}%) "
                    f"¥{r.total_invest:,}→¥{r.total_payout:,.0f} ROI={r.roi:.1f}%{roi_mark}",
                    flush=True,
                )
                results.append(r)
    return results
```

---

## Task 3: HonmeiModel / ManjiModel / PlaceModel 年単位 Walk-Forward

HonmeiModel は `_build_train_df(conn, train_until=2024)` で学習データを構築し、
テストデータも同様に生成する。EV = P(win) × win_odds で換算して投資シミュレート。

- [ ] **Step 1: v_race_mart ベースの高速データローダーを実装**

> ⚠️ `_build_train_df` は FeatureBuilder を race ごとに呼ぶため 30-90 分かかる。
> バックテスト用に v_race_mart から直接読む高速版を実装する。
> FEATURE_COLS のうち horse_stats 系は race_results の集計で近似する。

```python
def _load_mart_df(
    conn: sqlite3.Connection,
    min_date: str,
    max_date: str,
    excluded_ids: set[str],
) -> pd.DataFrame:
    """
    v_race_mart から基本特徴量を取得し、必要な変換を適用して返す。

    特徴量カバレッジ:
      ✅ 直接利用可能: weight_carried, horse_weight, horse_weight_diff,
                       distance, gate_number, race_number,
                       last_tc_4f, last_tc_lap, last_hc_4f, last_hc_lap,
                       win_odds, popularity
      ✅ 派生（単純変換）: surface_code, sex_code, venue_encoded, condition_code
      ✅ 派生（グループ内統計）: tc_4f_rank, win_odds_rank
      ✅ 目的変数: is_winner (rank==1), is_placed (rank<=3), payout_tansho
      ❌ 省略: win_rate_all, recent_rank_mean, today_bias, odds_velocity
               (これらは FeatureBuilder が計算。高速版では NaN として LightGBM に渡す)
    """
    df = pd.read_sql_query(
        """
        SELECT
            v.race_id, v.date, v.venue, v.race_number, v.distance,
            v.surface, v.condition, v.gate_number, v.horse_number,
            v.horse_name, v.sex_age, v.rank,
            v.win_odds, v.popularity,
            v.horse_weight, v.horse_weight_diff, v.weight_carried,
            v.jockey, v.trainer,
            v.jockey_code, v.trainer_code,
            v.last_tc_4f, v.last_tc_lap,
            v.last_hc_4f, v.last_hc_lap,
            v.payout_tansho, v.payout_fukusho,
            v.um_sex
        FROM v_race_mart v
        WHERE v.date BETWEEN ? AND ?
        ORDER BY v.date, v.race_id, v.horse_number
        """,
        conn,
        params=(min_date, max_date),
    )

    if excluded_ids:
        df = df[~df["race_id"].isin(excluded_ids)].copy()

    # ── エンコード ────────────────────────────────────────────────────
    _surface_map = {"芝": 0, "ダート": 1, "障害": 2}
    _condition_map = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
    _sex_map = {"牡": 0, "牝": 1, "セ": 2}

    df["surface_code"]   = df["surface"].map(_surface_map).fillna(0).astype(int)
    df["condition_code"] = df["condition"].map(_condition_map).fillna(0).astype(int)

    # sex_code: sex_age の先頭1文字から推定 (例: "牡3" → "牡")
    df["sex_code"] = df["sex_age"].str[0].map(_sex_map).fillna(0).astype(int)

    # venue_encoded: LabelEncoding
    _venues = df["venue"].unique().tolist()
    _venue_map = {v: i for i, v in enumerate(sorted(_venues))}
    df["venue_encoded"] = df["venue"].map(_venue_map).fillna(-1).astype(int)

    # jockey_code_encoded / trainer_code_encoded
    for col, src in [("jockey_code_encoded", "jockey_code"), ("trainer_code_encoded", "trainer_code")]:
        vals = df[src].dropna().unique().tolist()
        enc_map = {v: i for i, v in enumerate(sorted(vals))}
        df[col] = df[src].map(enc_map).fillna(-1).astype(int)

    # sire_encoded（v_race_mart には sire が入っていないため -1）
    df["sire_encoded"] = -1

    # ── レース内ランク特徴量（グループ内統計） ───────────────────────
    # win_rate_all_rank の代替: win_odds の逆数ランク（市場確率順位）
    df["win_odds_safe"] = pd.to_numeric(df["win_odds"], errors="coerce").fillna(99.9)
    df["tc_4f_rank"] = df.groupby("race_id")["last_tc_4f"].rank(method="min", ascending=True).fillna(-1)

    # ── 目的変数 ─────────────────────────────────────────────────────
    df["is_winner"] = (df["rank"] == 1).astype(int)
    df["is_placed"]  = (df["rank"] <= 3).astype(int)

    # ev_target: 単勝払戻 / 100 (ManjiModel 回帰用)
    df["ev_target"] = np.where(
        df["payout_tansho"].notna(),
        pd.to_numeric(df["payout_tansho"], errors="coerce").fillna(0) / 100.0,
        0.0,
    )

    return df
```

- [ ] **Step 2: HonmeiModel / ManjiModel / PlaceModel の1窓バックテスト関数を実装**

```python
_MART_FEATURE_COLS: list[str] = [
    "weight_carried", "horse_weight", "horse_weight_diff",
    "distance", "gate_number", "race_number",
    "surface_code", "sex_code", "venue_encoded", "condition_code",
    "jockey_code_encoded", "trainer_code_encoded", "sire_encoded",
    "last_tc_4f", "last_tc_lap", "last_hc_4f", "last_hc_lap",
    "tc_4f_rank",
]

_ALPHA_HONMEI_FEATURE_COLS: list[str] = _MART_FEATURE_COLS + [
    "win_odds_safe", "popularity",
]


def _fit_and_evaluate(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    model_name: str,
    window_label: str,
    bet_type: str,
    ev_threshold: float,
    use_odds_for_ev: bool = True,
) -> Optional[WindowResult]:
    """
    LightGBM モデルを学習・評価する汎用関数。

    use_odds_for_ev=True  → EV = P(win) × win_odds で換算（本命・複勝モデル）
    use_odds_for_ev=False → モデル出力を直接 EV として使用（卍モデル）
    """
    import lightgbm as lgb
    from lightgbm import LGBMClassifier, LGBMRegressor

    # 欠損列を NaN で補填
    for col in feature_cols:
        if col not in train_df.columns:
            train_df = train_df.copy()
            train_df[col] = np.nan
        if col not in test_df.columns:
            test_df = test_df.copy()
            test_df[col] = np.nan

    X_train = train_df[feature_cols].astype(float).fillna(-1)
    y_train = train_df[target_col].astype(float)
    X_test  = test_df[feature_cols].astype(float).fillna(-1)

    if len(X_train) < 200:
        logger.warning("[%s] %s 学習データ不足: %d行", window_label, model_name, len(X_train))
        return None

    # 分類 vs 回帰を target_col で判定
    is_classifier = target_col in ("is_winner", "is_placed")

    if is_classifier:
        clf = LGBMClassifier(
            n_estimators=800,
            learning_rate=0.03,
            num_leaves=63,
            min_child_samples=20,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )
        # 時系列分割（シャッフルなし）
        split_idx = int(len(X_train) * 0.85)
        clf.fit(
            X_train.iloc[:split_idx], y_train.iloc[:split_idx],
            eval_set=[(X_train.iloc[split_idx:], y_train.iloc[split_idx:])],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(period=-1)],
        )
        raw_pred = clf.predict_proba(X_test)[:, 1]
    else:
        reg = LGBMRegressor(
            n_estimators=800,
            learning_rate=0.03,
            num_leaves=63,
            min_child_samples=20,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )
        split_idx = int(len(X_train) * 0.85)
        reg.fit(
            X_train.iloc[:split_idx], y_train.iloc[:split_idx],
            eval_set=[(X_train.iloc[split_idx:], y_train.iloc[split_idx:])],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(period=-1)],
        )
        raw_pred = reg.predict(X_test)

    # EV 換算
    test_df = test_df.copy()
    test_df["raw_pred"] = raw_pred
    if use_odds_for_ev:
        odds = pd.to_numeric(test_df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        test_df["ev_pred"] = (test_df["raw_pred"] * odds).clip(lower=0.0)
    else:
        test_df["ev_pred"] = test_df["raw_pred"].clip(lower=0.0)

    # 払戻データ選択
    if bet_type == "複勝":
        test_df["is_hit"]       = test_df["is_placed"]
        test_df["actual_payout"] = pd.to_numeric(test_df["payout_fukusho"], errors="coerce").fillna(0)
    else:  # 単勝
        test_df["is_hit"]       = test_df["is_winner"]
        test_df["actual_payout"] = pd.to_numeric(test_df["payout_tansho"], errors="coerce").fillna(0)

    # 買いシグナル
    bets = test_df[test_df["ev_pred"] >= ev_threshold].copy()
    if bets.empty:
        return WindowResult(
            model_name=model_name, bet_type=bet_type,
            window_label=window_label, ev_threshold=ev_threshold,
            n_bets=0, n_hits=0, hit_rate=0,
            total_invest=0, total_payout=0, roi=0, max_drawdown=0,
            notes=["買いシグナルなし"],
        )

    invest = len(bets) * 100
    payout = float((bets["is_hit"] * bets["actual_payout"]).sum())
    roi = payout / invest * 100
    hit_rate = bets["is_hit"].mean() * 100
    pnl = (bets["is_hit"] * bets["actual_payout"] - 100).values
    cum = np.cumsum(pnl)
    max_dd = float(np.max(np.maximum.accumulate(cum) - cum)) if len(cum) > 0 else 0.0

    roi_mark = "✅" if roi >= 100 else "❌"
    print(
        f"  {model_name}[{window_label}]: {len(bets):,}点 的中{int(bets['is_hit'].sum())}({hit_rate:.1f}%) "
        f"ROI={roi:.1f}%{roi_mark}",
        flush=True,
    )

    return WindowResult(
        model_name=model_name, bet_type=bet_type,
        window_label=window_label, ev_threshold=ev_threshold,
        n_bets=len(bets),
        n_hits=int(bets["is_hit"].sum()),
        hit_rate=hit_rate,
        total_invest=invest,
        total_payout=payout,
        roi=roi,
        max_drawdown=max_dd,
    )
```

- [ ] **Step 3: Honmei/Manji/Place の Walk-Forward ドライバを実装**

```python
def run_legacy_walkforward(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
) -> list[WindowResult]:
    """
    HonmeiModel / ManjiModel / PlaceModel の年単位 Walk-Forward。
    Train: 全データ〜2024-12-31 / Test: 2025-01-01〜2025-12-31
    """
    print(f"\n{'─'*60}")
    print("  本命 / 卍 / 複勝モデル  (2024学習 → 2025テスト)")
    print(f"{'─'*60}")

    train_df = _load_mart_df(conn, "2020-01-01", "2024-12-31", excluded_ids)
    test_df  = _load_mart_df(conn, "2025-01-01", "2025-12-31", excluded_ids)
    print(f"  学習: {len(train_df):,}行 / テスト: {len(test_df):,}行", flush=True)

    if len(train_df) < 500 or len(test_df) < 50:
        print("  [WARN] データ不足のためスキップ")
        return []

    results: list[WindowResult] = []

    # 本命モデル（単勝 is_winner → EV = P(win)×win_odds）
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_MART_FEATURE_COLS,
        target_col="is_winner",
        model_name="本命",
        window_label="2025",
        bet_type="単勝",
        ev_threshold=_DEFAULT_HONMEI_EV_THRESHOLD,
        use_odds_for_ev=True,
    )
    if r:
        results.append(r)

    # 複勝モデル（is_placed → EV = P(place)×place_odds）
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_MART_FEATURE_COLS,
        target_col="is_placed",
        model_name="PlaceModel",
        window_label="2025",
        bet_type="複勝",
        ev_threshold=_DEFAULT_HONMEI_EV_THRESHOLD,
        use_odds_for_ev=True,
    )
    if r:
        results.append(r)

    # 卍モデル（ev_target 回帰 → 直接 EV として使用）
    # 注意: ev_target は単勝払戻/100 なので、threshold=1.0 = 100円回収見込み
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_ALPHA_HONMEI_FEATURE_COLS,
        target_col="ev_target",
        model_name="卍",
        window_label="2025",
        bet_type="単勝",
        ev_threshold=_DEFAULT_MANJI_EV_THRESHOLD,
        use_odds_for_ev=False,
    )
    if r:
        results.append(r)

    return results
```

---

## Task 4: EV 閾値スイープ

- [ ] **Step 1: 最適閾値探索関数を実装**

```python
def sweep_alpha_thresholds(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
    bet_type: str,
    research_db_path: Optional[Path] = None,
) -> tuple[float, float]:
    """
    AlphaModel に対して 2024→2025 の単一窓でスイープし、
    最適 EV 閾値と最大 ROI を返す。
    """
    model = AlphaModel()
    train_df = model.load_training_data(
        conn, min_date="2024-01-01", max_date="2024-12-31",
        bet_type=bet_type, research_db_path=research_db_path,
    )
    if excluded_ids:
        train_df = train_df[~train_df["race_id"].isin(excluded_ids)]
    if len(train_df) < 500:
        return _DEFAULT_ALPHA_THRESHOLD, 0.0

    test_df = model.load_training_data(
        conn, min_date="2025-01-01", max_date="2025-12-31",
        bet_type=bet_type, research_db_path=research_db_path,
    )
    if excluded_ids:
        test_df = test_df[~test_df["race_id"].isin(excluded_ids)]
    if len(test_df) < 50:
        return _DEFAULT_ALPHA_THRESHOLD, 0.0

    model.train(train_df)
    test_df["ev_pred"] = model.predict_ev(test_df).values

    print(f"\n  [{bet_type}] EV閾値スイープ (2024→2025)")
    print(f"  {'閾値':>5} | {'件数':>6} | {'的中率':>7} | {'ROI':>8}")
    print(f"  {'-'*38}")

    best_roi = 0.0
    best_threshold = _DEFAULT_ALPHA_THRESHOLD

    for thr in _EV_SWEEP:
        buy = test_df[test_df["ev_pred"] >= thr]
        if len(buy) == 0:
            continue
        invest = len(buy) * 100
        payout = float((buy["is_hit"] * buy["actual_payout"].fillna(0)).sum())
        roi = payout / invest * 100
        hit_r = buy["is_hit"].mean() * 100
        mark = "✅" if roi >= 100 else ""
        print(f"  {thr:>5.1f} | {len(buy):>6,} | {hit_r:>6.1f}% | {roi:>7.1f}%{mark}")
        if roi > best_roi:
            best_roi = roi
            best_threshold = thr

    print(f"  → 最適閾値: {best_threshold:.1f} (ROI {best_roi:.1f}%)")
    return best_threshold, best_roi
```

---

## Task 5: 最終報告 + main()

- [ ] **Step 1: 社長向け最終報告関数を実装**

```python
def print_final_report(all_results: list[WindowResult], sweep_summary: dict) -> None:
    """
    社長向け最終報告:
    - プラスROIモデル: モデル名・ROI・的中率・最適EV閾値
    - マイナスROIモデル: 敗因1行
    """
    if not all_results:
        print("\n[WARN] バックテスト結果なし")
        return

    df = pd.DataFrame([
        dict(
            model=r.model_name,
            bet_type=r.bet_type,
            window=r.window_label,
            ev_threshold=r.ev_threshold,
            n_bets=r.n_bets,
            hit_rate=r.hit_rate,
            invest=r.total_invest,
            payout=r.total_payout,
            roi=r.roi,
        )
        for r in all_results
    ])

    # モデル×馬券種で集計
    agg = (
        df.groupby(["model", "bet_type"])
        .agg(
            total_invest=("invest", "sum"),
            total_payout=("payout", "sum"),
            avg_hit_rate=("hit_rate", "mean"),
            windows=("window", "count"),
            profitable_windows=("roi", lambda x: (x >= 100).sum()),
        )
        .reset_index()
    )
    agg["overall_roi"] = agg["total_payout"] / agg["total_invest"] * 100

    print(f"\n{'='*70}")
    print("  UMALOGI 厳密 Walk-Forward バックテスト — 社長向け最終報告")
    print(f"  対象期間: 2024-01-01 〜 2025-12-31")
    print(f"{'='*70}")

    profitable = agg[agg["overall_roi"] >= 100].sort_values("overall_roi", ascending=False)
    unprofitable = agg[agg["overall_roi"] < 100]

    print(f"\n【✅ 年間ROI 100%超 — 採用候補モデル】")
    if profitable.empty:
        print("  なし（全モデル ROI < 100%）")
    else:
        for _, row in profitable.iterrows():
            opt_thr = sweep_summary.get(f"{row['model']}_{row['bet_type']}", {}).get("best_threshold", "—")
            opt_roi = sweep_summary.get(f"{row['model']}_{row['bet_type']}", {}).get("best_roi", "—")
            print(f"\n  ★ {row['model']} ({row['bet_type']})")
            print(f"     通算ROI       : {row['overall_roi']:.1f}%")
            print(f"     平均的中率    : {row['avg_hit_rate']:.1f}%")
            print(f"     最適EV閾値    : {opt_thr} (ROI {opt_roi}%)")
            print(f"     投資/払戻     : ¥{int(row['total_invest']):,} → ¥{int(row['total_payout']):,}")
            print(f"     ROI100%超の窓 : {int(row['profitable_windows'])}/{int(row['windows'])}")

    print(f"\n【❌ 年間ROI < 100% — 不採用モデル (敗因)】")
    if unprofitable.empty:
        print("  なし（全モデル ROI ≥ 100%）")
    else:
        for _, row in unprofitable.iterrows():
            roi = row["overall_roi"]
            if roi < 70:
                reason = "致命的な精度不足。特徴量・目的変数の抜本的見直しが必要。"
            elif roi < 90:
                reason = "EV閾値が最適化されていないか、オッズ歪み検知が不十分。"
            else:
                reason = "ROI95〜100%帯。閾値の微調整または手数料（JRA控除率）の壁。"
            print(f"  ✗ {row['model']} ({row['bet_type']}): ROI={roi:.1f}% — {reason}")

    print(f"\n{'='*70}")
    print("  全モデル一覧")
    print(f"{'='*70}")
    print(f"  {'モデル':<20} {'馬券':>4} {'ROI':>8} {'的中率':>7} {'総投資':>10} {'窓数':>4}")
    print(f"  {'-'*60}")
    for _, row in agg.sort_values("overall_roi", ascending=False).iterrows():
        mark = "✅" if row["overall_roi"] >= 100 else "❌"
        print(
            f"  {row['model']:<20} {row['bet_type']:>4} "
            f"{row['overall_roi']:>7.1f}%{mark} "
            f"{row['avg_hit_rate']:>6.1f}% "
            f"¥{int(row['total_invest']):>9,} "
            f"{int(row['windows']):>3}窓"
        )
```

- [ ] **Step 2: main() 関数を実装**

```python
def main() -> None:
    parser = argparse.ArgumentParser(description="厳密 Walk-Forward バックテスト (2024-2025)")
    parser.add_argument("--alpha-only", action="store_true",
                        help="AlphaModel のみ実行（高速）")
    parser.add_argument("--no-sweep",  action="store_true",
                        help="EV閾値スイープをスキップ")
    parser.add_argument("--research-db", default=None,
                        help="Research DB パス（netkeiba win_odds 補完用）")
    args = parser.parse_args()

    research_db: Optional[Path] = None
    if args.research_db:
        p = _ROOT / args.research_db if not Path(args.research_db).is_absolute() else Path(args.research_db)
        if p.exists():
            research_db = p

    print(f"\n{'='*60}")
    print("  UMALOGI 厳密 Walk-Forward バックテスト")
    print(f"  期間: 2024-01-01 〜 2025-12-31")
    print(f"  Research DB: {'あり' if research_db else 'なし'}")
    print(f"{'='*60}")

    conn = sqlite3.connect(str(_MAIN_DB))
    conn.execute("PRAGMA foreign_keys = ON")

    # ── Phase 1: データ品質チェック ─────────────────────────────────
    print("\n[Phase 1] データ品質チェック中...")
    dq = check_data_quality(conn)
    dq.print_summary()

    all_results: list[WindowResult] = []
    sweep_summary: dict = {}

    # ── Phase 2: AlphaModel Walk-Forward ─────────────────────────────
    print("\n[Phase 2] AlphaModel Walk-Forward バックテスト...")
    alpha_results = run_alpha_walkforward(
        conn=conn,
        excluded_ids=dq.excluded_ids,
        ev_threshold=_DEFAULT_ALPHA_THRESHOLD,
        research_db_path=research_db,
    )
    all_results.extend(alpha_results)

    # ── Phase 3: EV 閾値スイープ ────────────────────────────────────
    if not args.no_sweep:
        print("\n[Phase 3] EV 閾値スイープ (AlphaModel)...")
        for bet_type in ("単勝", "複勝"):
            best_thr, best_roi = sweep_alpha_thresholds(
                conn, dq.excluded_ids, bet_type, research_db_path=research_db
            )
            sweep_summary[f"ALPHA({bet_type})_{bet_type}"] = {
                "best_threshold": best_thr,
                "best_roi": round(best_roi, 1),
            }

    # ── Phase 4: Honmei / Manji / Place Walk-Forward ─────────────────
    if not args.alpha_only:
        print("\n[Phase 4] 本命 / 卍 / 複勝モデル Walk-Forward バックテスト...")
        print("  (FeatureBuilder を使わない高速版: 約 30-60 秒)")
        legacy_results = run_legacy_walkforward(conn, dq.excluded_ids)
        all_results.extend(legacy_results)

    conn.close()

    # ── Phase 5: 最終報告 ────────────────────────────────────────────
    print_final_report(all_results, sweep_summary)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: スクリプト全体を実行（Alpha のみで動作確認）**

```
py scripts/run_strict_backtest.py --alpha-only --no-sweep
```

Expected output:
```
============================================================
  UMALOGI 厳密 Walk-Forward バックテスト
  期間: 2024-01-01 〜 2025-12-31
  Research DB: なし
============================================================

[Phase 1] データ品質チェック中...
============================================================
  [データ品質チェック] 2024-2025年
============================================================
  文字化けレース  :    XX 件
  ...

[Phase 2] AlphaModel Walk-Forward バックテスト...
  ...
  [2024H2]: N,XXX点 的中XX(X.X%) ROI=XXX.X%✅/❌
```

- [ ] **Step 4: フルモード実行**

```
py scripts/run_strict_backtest.py
```

Expected: 全モデル結果 + EV スイープ + 社長向け最終報告が出力される（10-20分）

---

## Self-Review

### Spec Coverage

| 要件 | 実装タスク |
|---|---|
| Walk-Forward (カンニングなし) | Task 2: expanding window, train_end < test_start を厳守 |
| 文字化け・欠損データの事前排除 | Task 1: check_data_quality() が garbled/corrupt/bad_odds を除外 |
| 全モデル対象 | Task 2 (Alpha), Task 3 (Honmei/Manji/Place) |
| ROI > 100% モデルの厳選報告 | Task 5: print_final_report() で profitable/unprofitable 分類 |
| 最適 EV 閾値の報告 | Task 4: sweep_alpha_thresholds() |
| モデル名・ROI・的中率・EV閾値 | Task 5: 3点セット明示 |
| マイナスモデルの敗因 | Task 5: reason 文字列で1行説明 |

### Known Limitations

- HonmeiModel/ManjiModel は v_race_mart ベースの簡易特徴量（约20特徴量）を使用。
  `win_rate_all`, `recent_rank_mean` 等のレース履歴特徴量が欠落するため、
  本番モデルより精度が低い可能性がある（それでも比較的公平な比較になる）。
- AlphaModel の Walk-Forward は半期3窓のみ（2024年 H1 以前のオッズデータが存在しないため）。
- データ期間が 2024-2025 の 2年のみで、統計的信頼性は限定的。
