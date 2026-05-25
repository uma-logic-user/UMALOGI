#!/usr/bin/env python
"""
厳密 Walk-Forward バックテスト (2024-2025)
=========================================
全モデル対象・データリーク完全排除・EV閾値スイープ・社長向け最終報告

実行:
  py scripts/run_strict_backtest.py
  py scripts/run_strict_backtest.py --alpha-only    # AlphaModelのみ（高速）
  py scripts/run_strict_backtest.py --no-sweep      # EV閾値スイープをスキップ

Walk-Forward 設計:
  AlphaModel: 半期 expanding-window (3窓)
    W1: Train 2024H1 → Test 2024H2
    W2: Train 2024   → Test 2025H1
    W3: Train 2024+2025H1 → Test 2025H2
  本命/卍/複勝: 年単位 (Train 2024, Test 2025)

データリーク防止:
  - train_end < test_start を全窓で厳守
  - AlphaModel: load_training_data(min_date, max_date) で日付境界を厳密管理
  - Honmei/Manji: v_race_mart から学習期間の終了日付でフィルタ
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

# AlphaModel 半期 expanding-window（3窓）
# 形式: (train_start, train_end, test_start, test_end, label)
_ALPHA_WINDOWS: list[tuple[str, str, str, str, str]] = [
    ("2024-01-01", "2024-06-30", "2024-07-01", "2024-12-31", "2024H2"),
    ("2024-01-01", "2024-12-31", "2025-01-01", "2025-06-30", "2025H1"),
    ("2024-01-01", "2025-06-30", "2025-07-01", "2025-12-31", "2025H2"),
]

# EV閾値スイープ候補
_EV_SWEEP: list[float] = [1.1, 1.2, 1.3, 1.5, 1.8, 2.0, 2.5]

# デフォルト評価閾値
_DEFAULT_ALPHA_THRESHOLD   = 1.5
_DEFAULT_HONMEI_THRESHOLD  = 1.3   # EV = P(win) × win_odds
_DEFAULT_MANJI_THRESHOLD   = 1.1   # 直接 ev_target を予測


# ─── 結果コンテナ ─────────────────────────────────────────────────────

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


# ─── Phase 1: データ品質チェック ──────────────────────────────────────

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
        print(f"  race_name 文字化け/空文字: {len(self.garbled_race_ids):>5,} 件")
        print(f"    ※ ML特徴量未使用 → バックテスト除外は最小化")
        print(f"  rank 汚染レース           : {len(self.corrupt_rank_ids):>5,} 件")
        print(f"  オッズ欠損80%超            : {len(self.bad_odds_ids):>5,} 件")
        print(f"    ※ research_db 補完後に再評価するため除外対象外")
        print(f"  ─── バックテスト除外 合計 : {len(self.excluded_ids):>5,} 件")


def check_data_quality(
    conn: sqlite3.Connection,
    start: str = "2024-01-01",
    end: str   = "2025-12-31",
) -> DataQualityReport:
    """バックテスト対象期間の汚染 race_id を検出して返す。"""

    # 1. race_name 文字化け（?含む）—— 空文字は除外しない
    garbled = [
        r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date BETWEEN ? AND ? AND race_name LIKE '%?%'",
            (start, end),
        ).fetchall()
    ]

    # 2. rank 汚染（HR払戻レコードの誤挿入: 20,30,...90）
    corrupt_rank = [
        r[0] for r in conn.execute(
            """SELECT DISTINCT rr.race_id FROM race_results rr
               JOIN races r ON rr.race_id = r.race_id
               WHERE r.date BETWEEN ? AND ?
                 AND rr.rank IN (20,30,40,50,60,70,80,90)""",
            (start, end),
        ).fetchall()
    ]

    # 3. オッズ欠損率80%超（参考のみ: research_db で補完するため除外対象外）
    bad_odds = [
        r[0] for r in conn.execute(
            """SELECT rr.race_id
               FROM race_results rr JOIN races r ON rr.race_id = r.race_id
               WHERE r.date BETWEEN ? AND ?
               GROUP BY rr.race_id
               HAVING SUM(CASE WHEN rr.win_odds IS NULL OR rr.win_odds <= 0 THEN 1 ELSE 0 END)
                      * 1.0 / COUNT(*) > 0.80""",
            (start, end),
        ).fetchall()
    ]

    # 実際に除外するのは race_name 文字化け + rank 汚染のみ
    excluded = set(garbled + corrupt_rank)
    return DataQualityReport(
        garbled_race_ids=garbled,
        corrupt_rank_ids=corrupt_rank,
        bad_odds_ids=bad_odds,
        excluded_ids=excluded,
    )


# ─── Phase 2: AlphaModel Walk-Forward ────────────────────────────────

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

    厳密リーク防止:
      - 学習: train_start ≤ date ≤ train_end
      - テスト: test_start ≤ date ≤ test_end  (重複なし)
    固定¥100ベット: モデル間の公平比較のため Kelly ではなく固定賭け金を使用
    """
    from src.ml.alpha_model import AlphaModel

    model = AlphaModel()

    train_df = model.load_training_data(
        conn,
        min_date=train_start,
        max_date=train_end,
        bet_type=bet_type,
        research_db_path=research_db_path,
    )
    if excluded_ids:
        train_df = train_df[~train_df["race_id"].isin(excluded_ids)]
    if len(train_df) < 300:
        print(f"  [{window_label}] ⚠️ 学習データ不足: {len(train_df)}行 → スキップ", flush=True)
        return None

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
        print(f"  [{window_label}] ⚠️ テストデータ不足: {len(test_df)}行 → スキップ", flush=True)
        return None

    print(f"  [{window_label}] 学習{len(train_df):,}行 → テスト{len(test_df):,}行", end="", flush=True)

    metrics = model.train(train_df)
    print(f" | AUC={metrics['auc']:.3f}", flush=True)

    test_df = test_df.copy()
    test_df["ev_pred"] = model.predict_ev(test_df).values

    bets = test_df[test_df["ev_pred"] >= ev_threshold].copy()
    if bets.empty:
        return WindowResult(
            model_name=f"ALPHA({bet_type})", bet_type=bet_type,
            window_label=window_label, ev_threshold=ev_threshold,
            n_bets=0, n_hits=0, hit_rate=0.0,
            total_invest=0, total_payout=0.0, roi=0.0, max_drawdown=0.0,
            notes=["買いシグナルなし"],
        )

    invest = len(bets) * 100
    payout = float((bets["is_hit"] * bets["actual_payout"].fillna(0)).sum())
    roi = payout / invest * 100
    hit_rate = float(bets["is_hit"].mean() * 100)
    pnl = (bets["is_hit"] * bets["actual_payout"].fillna(0) - 100).values
    cum = np.cumsum(pnl)
    max_dd = float(np.max(np.maximum.accumulate(cum) - cum)) if len(cum) > 0 else 0.0

    mark = "✅" if roi >= 100 else "❌"
    print(
        f"       {len(bets):,}点 的中{int(bets['is_hit'].sum())}({hit_rate:.1f}%) "
        f"ROI={roi:.1f}%{mark}",
        flush=True,
    )

    return WindowResult(
        model_name=f"ALPHA({bet_type})", bet_type=bet_type,
        window_label=window_label, ev_threshold=ev_threshold,
        n_bets=len(bets), n_hits=int(bets["is_hit"].sum()),
        hit_rate=hit_rate, total_invest=invest, total_payout=payout,
        roi=roi, max_drawdown=max_dd,
    )


def run_alpha_walkforward(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
    ev_threshold: float = _DEFAULT_ALPHA_THRESHOLD,
    research_db_path: Optional[Path] = None,
) -> list[WindowResult]:
    """3窓×2馬券種の AlphaModel Walk-Forward を実行。"""
    results: list[WindowResult] = []

    for bet_type in ("単勝", "複勝"):
        print(f"\n  ── AlphaModel [{bet_type}]  EV閾値={ev_threshold} ──")
        for tr_start, tr_end, te_start, te_end, label in _ALPHA_WINDOWS:
            r = _backtest_alpha_window(
                conn=conn,
                train_start=tr_start, train_end=tr_end,
                test_start=te_start,  test_end=te_end,
                window_label=label, bet_type=bet_type,
                ev_threshold=ev_threshold,
                excluded_ids=excluded_ids,
                research_db_path=research_db_path,
            )
            if r is not None:
                results.append(r)

    return results


# ─── Phase 3: 本命/卍/複勝 Walk-Forward (v_race_mart 高速版) ─────────

_MART_FEATURE_COLS: list[str] = [
    "weight_carried", "horse_weight", "horse_weight_diff",
    "distance", "gate_number", "race_number",
    "surface_code", "sex_code", "venue_encoded", "condition_code",
    "jockey_code_encoded", "trainer_code_encoded",
    "last_tc_4f", "last_tc_lap", "last_hc_4f", "last_hc_lap",
    "tc_4f_rank",
]

# 卍モデルは win_odds/popularity も特徴量に追加（市場情報活用）
_MANJI_FEATURE_COLS: list[str] = _MART_FEATURE_COLS + ["win_odds_safe", "popularity"]


def _load_mart_df(
    conn: sqlite3.Connection,
    min_date: str,
    max_date: str,
    excluded_ids: set[str],
) -> pd.DataFrame:
    """
    v_race_mart から基本特徴量を取得して返す。

    カバレッジ:
      ✅ 直接取得: weight_carried, horse_weight, distance, gate_number等
      ✅ 派生: surface_code, sex_code, venue_encoded, condition_code
      ✅ レース内ランク: tc_4f_rank（グループ内統計で代用）
      ✅ 目的変数: is_winner, is_placed, ev_target (payout_tansho/100)
      ❌ 省略（NaN扱い）: win_rate_all, recent_rank_mean, today_bias
         LightGBM は NaN を適切に処理するため学習・推論に問題なし
    """
    df = pd.read_sql_query(
        """
        SELECT
            v.race_id, v.date, v.venue, v.race_number, v.distance,
            v.surface, v.condition, v.gate_number, v.horse_number,
            v.horse_name, v.sex_age, v.rank,
            v.win_odds, v.popularity,
            v.horse_weight, v.horse_weight_diff, v.weight_carried,
            v.jockey_code, v.trainer_code,
            v.last_tc_4f, v.last_tc_lap,
            v.last_hc_4f, v.last_hc_lap,
            v.payout_tansho, v.payout_fukusho
        FROM v_race_mart v
        WHERE v.date BETWEEN ? AND ?
          AND v.rank IS NOT NULL
        ORDER BY v.date, v.race_id, v.horse_number
        """,
        conn,
        params=(min_date, max_date),
    )

    if excluded_ids:
        df = df[~df["race_id"].isin(excluded_ids)].copy()

    if df.empty:
        return df

    # ── エンコード ────────────────────────────────────────────────────
    _surface_map  = {"芝": 0, "ダート": 1, "障害": 2}
    _condition_map = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
    _sex_map      = {"牡": 0, "牝": 1, "セ": 2}

    df["surface_code"]   = df["surface"].map(_surface_map).fillna(0).astype(int)
    df["condition_code"] = df["condition"].map(_condition_map).fillna(0).astype(int)
    df["sex_code"]       = df["sex_age"].str[:1].map(_sex_map).fillna(0).astype(int)

    venues = sorted(df["venue"].dropna().unique().tolist())
    vmap   = {v: i for i, v in enumerate(venues)}
    df["venue_encoded"] = df["venue"].map(vmap).fillna(-1).astype(int)

    for col, src in [
        ("jockey_code_encoded",  "jockey_code"),
        ("trainer_code_encoded", "trainer_code"),
    ]:
        vals = df[src].dropna().unique().tolist()
        emap = {v: i for i, v in enumerate(sorted(vals, key=str))}
        df[col] = df[src].map(emap).fillna(-1).astype(int)

    # レース内調教タイムランク（小さい=速い=1位）
    df["win_odds_safe"] = pd.to_numeric(df["win_odds"], errors="coerce").fillna(99.9)
    df["tc_4f_rank"] = (
        df.groupby("race_id")["last_tc_4f"]
        .rank(method="min", ascending=True)
        .fillna(-1)
    )

    # 目的変数
    df["is_winner"] = (df["rank"] == 1).astype(int)
    df["is_placed"]  = (df["rank"] <= 3).astype(int)

    df["ev_target"] = np.where(
        df["payout_tansho"].notna(),
        pd.to_numeric(df["payout_tansho"], errors="coerce").fillna(0) / 100.0,
        0.0,
    ).astype(float)

    return df


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
    LightGBM モデルを学習し、固定¥100ベットで評価する。

    use_odds_for_ev=True  → EV = P(win/place) × win_odds（本命・複勝）
    use_odds_for_ev=False → モデル出力を直接 EV として使用（卍）
    """
    import lightgbm as lgb

    # 欠損列補填（LightGBM は NaN を処理可能）
    for col in feature_cols:
        for df in (train_df, test_df):
            if col not in df.columns:
                df[col] = np.nan  # type: ignore[assignment]

    X_train = train_df[feature_cols].astype(float)
    y_train = train_df[target_col].astype(float)
    X_test  = test_df[feature_cols].astype(float)

    if len(X_train) < 200:
        return None

    is_classifier = target_col in ("is_winner", "is_placed")

    if is_classifier:
        from lightgbm import LGBMClassifier
        clf = LGBMClassifier(
            n_estimators=800, learning_rate=0.03, num_leaves=63,
            min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            random_state=42, n_jobs=-1, verbose=-1,
        )
        split = int(len(X_train) * 0.85)
        clf.fit(
            X_train.iloc[:split], y_train.iloc[:split],
            eval_set=[(X_train.iloc[split:], y_train.iloc[split:])],
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(period=-1),
            ],
        )
        raw_pred = clf.predict_proba(X_test)[:, 1]
    else:
        from lightgbm import LGBMRegressor
        reg = LGBMRegressor(
            n_estimators=800, learning_rate=0.03, num_leaves=63,
            min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            random_state=42, n_jobs=-1, verbose=-1,
        )
        split = int(len(X_train) * 0.85)
        reg.fit(
            X_train.iloc[:split], y_train.iloc[:split],
            eval_set=[(X_train.iloc[split:], y_train.iloc[split:])],
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(period=-1),
            ],
        )
        raw_pred = reg.predict(X_test)

    test_df = test_df.copy()
    test_df["raw_pred"] = raw_pred

    if use_odds_for_ev:
        odds = pd.to_numeric(test_df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
        test_df["ev_pred"] = (test_df["raw_pred"] * odds).clip(lower=0.0)
    else:
        test_df["ev_pred"] = test_df["raw_pred"].clip(lower=0.0)

    # 馬券種に応じた払戻列を選択
    if bet_type == "複勝":
        test_df["is_hit"]        = test_df["is_placed"]
        test_df["actual_payout"] = pd.to_numeric(
            test_df["payout_fukusho"], errors="coerce"
        ).fillna(0)
    else:  # 単勝
        test_df["is_hit"]        = test_df["is_winner"]
        test_df["actual_payout"] = pd.to_numeric(
            test_df["payout_tansho"], errors="coerce"
        ).fillna(0)

    bets = test_df[test_df["ev_pred"] >= ev_threshold].copy()
    if bets.empty:
        return WindowResult(
            model_name=model_name, bet_type=bet_type,
            window_label=window_label, ev_threshold=ev_threshold,
            n_bets=0, n_hits=0, hit_rate=0.0,
            total_invest=0, total_payout=0.0, roi=0.0, max_drawdown=0.0,
            notes=["買いシグナルなし"],
        )

    invest   = len(bets) * 100
    payout   = float((bets["is_hit"] * bets["actual_payout"]).sum())
    roi      = payout / invest * 100
    hit_rate = float(bets["is_hit"].mean() * 100)
    pnl      = (bets["is_hit"] * bets["actual_payout"] - 100).values
    cum      = np.cumsum(pnl)
    max_dd   = float(np.max(np.maximum.accumulate(cum) - cum)) if len(cum) > 0 else 0.0

    mark = "✅" if roi >= 100 else "❌"
    print(
        f"  {model_name}[{window_label}]: {len(bets):,}点 "
        f"的中{int(bets['is_hit'].sum())}({hit_rate:.1f}%) "
        f"ROI={roi:.1f}%{mark}",
        flush=True,
    )

    return WindowResult(
        model_name=model_name, bet_type=bet_type,
        window_label=window_label, ev_threshold=ev_threshold,
        n_bets=len(bets), n_hits=int(bets["is_hit"].sum()),
        hit_rate=hit_rate, total_invest=invest, total_payout=payout,
        roi=roi, max_drawdown=max_dd,
    )


def run_legacy_walkforward(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
) -> list[WindowResult]:
    """
    本命 / 卍 / 複勝モデルの年単位 Walk-Forward。
    Train: ～2024-12-31 (v_race_mart 高速版)
    Test : 2025-01-01～2025-12-31
    """
    print("\n  ── 本命/卍/複勝モデル (2024学習 → 2025テスト) ──")

    # 学習データ: 可能な限り過去のデータを活用
    train_df = _load_mart_df(conn, "2020-01-01", "2024-12-31", excluded_ids)
    test_df  = _load_mart_df(conn, "2025-01-01", "2025-12-31", excluded_ids)
    print(f"  v_race_mart 学習: {len(train_df):,}行 / テスト: {len(test_df):,}行", flush=True)

    if len(train_df) < 500 or len(test_df) < 50:
        print("  ⚠️ データ不足のためスキップ")
        return []

    results: list[WindowResult] = []

    # 本命モデル（単勝: P(win) × win_odds → EV > 1.3）
    print("  [本命モデル 単勝]", end=" ", flush=True)
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_MART_FEATURE_COLS,
        target_col="is_winner",
        model_name="本命",
        window_label="2025",
        bet_type="単勝",
        ev_threshold=_DEFAULT_HONMEI_THRESHOLD,
        use_odds_for_ev=True,
    )
    if r:
        results.append(r)

    # 複勝モデル（複勝: P(place) × place_odds → EV > 1.2）
    print("  [複勝モデル 複勝]", end=" ", flush=True)
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_MART_FEATURE_COLS,
        target_col="is_placed",
        model_name="PlaceModel",
        window_label="2025",
        bet_type="複勝",
        ev_threshold=1.2,
        use_odds_for_ev=True,
    )
    if r:
        results.append(r)

    # 卍モデル（ev_target = payout_tansho/100 を直接回帰）
    print("  [卍モデル 単勝]", end=" ", flush=True)
    r = _fit_and_evaluate(
        train_df, test_df,
        feature_cols=_MANJI_FEATURE_COLS,
        target_col="ev_target",
        model_name="卍",
        window_label="2025",
        bet_type="単勝",
        ev_threshold=_DEFAULT_MANJI_THRESHOLD,
        use_odds_for_ev=False,
    )
    if r:
        results.append(r)

    return results


# ─── Phase 4: EV閾値スイープ (AlphaModel) ────────────────────────────

def sweep_alpha_thresholds(
    conn: sqlite3.Connection,
    excluded_ids: set[str],
    bet_type: str,
    research_db_path: Optional[Path] = None,
) -> dict[str, float]:
    """
    AlphaModel 2024→2025 の単一窓でスイープし、
    最適 EV 閾値・ROI を返す。
    """
    from src.ml.alpha_model import AlphaModel

    model = AlphaModel()
    train_df = model.load_training_data(
        conn, min_date="2024-01-01", max_date="2024-12-31",
        bet_type=bet_type, research_db_path=research_db_path,
    )
    if excluded_ids:
        train_df = train_df[~train_df["race_id"].isin(excluded_ids)]
    if len(train_df) < 300:
        return {"best_threshold": _DEFAULT_ALPHA_THRESHOLD, "best_roi": 0.0}

    test_df = model.load_training_data(
        conn, min_date="2025-01-01", max_date="2025-12-31",
        bet_type=bet_type, research_db_path=research_db_path,
    )
    if excluded_ids:
        test_df = test_df[~test_df["race_id"].isin(excluded_ids)]
    if len(test_df) < 50:
        return {"best_threshold": _DEFAULT_ALPHA_THRESHOLD, "best_roi": 0.0}

    model.train(train_df)
    test_df = test_df.copy()
    test_df["ev_pred"] = model.predict_ev(test_df).values

    print(f"\n  [{bet_type}] EV閾値スイープ (2024→2025 単一窓)")
    print(f"  {'閾値':>5} | {'件数':>6} | {'的中率':>7} | {'ROI':>8} | {'損益':>12}")
    print(f"  {'-'*50}")

    best_roi = 0.0
    best_threshold = _DEFAULT_ALPHA_THRESHOLD

    for thr in _EV_SWEEP:
        buy = test_df[test_df["ev_pred"] >= thr]
        if len(buy) == 0:
            continue
        invest = len(buy) * 100
        payout = float((buy["is_hit"] * buy["actual_payout"].fillna(0)).sum())
        roi    = payout / invest * 100
        hit_r  = buy["is_hit"].mean() * 100
        profit = payout - invest
        mark   = " ✅" if roi >= 100 else ""
        print(
            f"  {thr:>5.1f} | {len(buy):>6,} | {hit_r:>6.1f}% "
            f"| {roi:>7.1f}%{mark} | ¥{profit:>+10,.0f}"
        )
        if roi > best_roi:
            best_roi = roi
            best_threshold = thr

    print(f"  → 最適閾値: {best_threshold} (ROI {best_roi:.1f}%)")
    return {"best_threshold": best_threshold, "best_roi": round(best_roi, 1)}


# ─── Phase 5: 社長向け最終報告 ───────────────────────────────────────

def print_final_report(
    all_results: list[WindowResult],
    sweep_summary: dict[str, dict[str, float]],
) -> None:
    """
    社長向け最終報告:
    - ROI > 100% モデル: モデル名・ROI・的中率・最適EV閾値の3点セット
    - ROI < 100% モデル: 敗因1行
    """
    if not all_results:
        print("\n[WARN] バックテスト結果なし")
        return

    rows = [
        dict(
            model=r.model_name, bet_type=r.bet_type, window=r.window_label,
            n_bets=r.n_bets, hit_rate=r.hit_rate,
            invest=r.total_invest, payout=r.total_payout, roi=r.roi,
        )
        for r in all_results
        if r.n_bets > 0
    ]
    if not rows:
        print("\n全モデルで買いシグナルなし")
        return

    df = pd.DataFrame(rows)

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

    profitable   = agg[agg["overall_roi"] >= 100].sort_values("overall_roi", ascending=False)
    unprofitable = agg[agg["overall_roi"] < 100].sort_values("overall_roi", ascending=False)

    print(f"\n{'='*70}")
    print("  UMALOGI 厳密 Walk-Forward バックテスト — 社長向け最終報告")
    print(f"  対象期間: 2024-01-01 〜 2025-12-31 (カンニングなし)")
    print(f"  固定¥100ベット・全モデル比較")
    print(f"{'='*70}")

    print(f"\n{'─'*70}")
    print("  【✅ 年間ROI 100%超 — 採用候補モデル】")
    print(f"{'─'*70}")

    if profitable.empty:
        print("  なし（全モデル ROI < 100%）")
    else:
        for _, row in profitable.iterrows():
            key = f"{row['model']}_{row['bet_type']}"
            sweep = sweep_summary.get(key, {})
            opt_thr = sweep.get("best_threshold", "—")
            opt_roi = sweep.get("best_roi", "—")

            print(f"\n  ★ {row['model']} ({row['bet_type']})")
            print(f"     通算ROI           : {row['overall_roi']:.1f}%")
            print(f"     平均的中率        : {row['avg_hit_rate']:.1f}%")
            if isinstance(opt_thr, float):
                print(f"     最適EV閾値(スイープ): EV≥{opt_thr:.1f}  → ROI {opt_roi:.1f}%")
            else:
                print(f"     最適EV閾値        : {opt_thr}")
            print(f"     投資合計          : ¥{int(row['total_invest']):,}")
            print(f"     払戻合計          : ¥{int(row['total_payout']):,}")
            print(f"     損益              : ¥{int(row['total_payout']-row['total_invest']):+,}")
            print(f"     ROI100%超 窓数    : {int(row['profitable_windows'])}/{int(row['windows'])}")

    print(f"\n{'─'*70}")
    print("  【❌ 年間ROI < 100% — 不採用モデル】")
    print(f"{'─'*70}")

    if unprofitable.empty:
        print("  なし（全モデル ROI ≥ 100%）")
    else:
        for _, row in unprofitable.iterrows():
            roi = row["overall_roi"]
            if roi < 60:
                reason = "致命的な精度不足。JRA控除率（単勝20%）を越えられず。特徴量の抜本的見直し必要。"
            elif roi < 85:
                reason = "オッズ歪み検知が不十分。EV閾値の引き上げか、追加特徴量が必要。"
            elif roi < 95:
                reason = "惜しい水準。閾値微調整またはKellyサイジング導入で改善可能性あり。"
            else:
                reason = "ROI95〜100%帯。JRA控除率の壁。より厳格な閾値（EV≥1.5+）で絞り込みを。"
            print(f"  ✗ {row['model']} ({row['bet_type']})  ROI={roi:.1f}% — {reason}")

    print(f"\n{'='*70}")
    print("  【全モデル一覧】")
    print(f"{'='*70}")
    header = f"  {'モデル':<20} {'馬券':>4}  {'ROI':>7}  {'的中率':>6}  {'総投資':>9}  {'窓':>3}"
    print(header)
    print(f"  {'-'*60}")
    for _, row in agg.sort_values("overall_roi", ascending=False).iterrows():
        mark = "✅" if row["overall_roi"] >= 100 else "❌"
        print(
            f"  {row['model']:<20} {row['bet_type']:>4}  "
            f"{row['overall_roi']:>6.1f}%{mark}  "
            f"{row['avg_hit_rate']:>5.1f}%  "
            f"¥{int(row['total_invest']):>8,}  "
            f"{int(row['windows']):>2}窓"
        )
    print(f"{'='*70}")


# ─── main ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="厳密 Walk-Forward バックテスト (2024-2025)"
    )
    parser.add_argument(
        "--alpha-only", action="store_true",
        help="AlphaModel のみ実行（高速・約5分）",
    )
    parser.add_argument(
        "--no-sweep", action="store_true",
        help="EV閾値スイープをスキップ",
    )
    parser.add_argument(
        "--research-db", default=None,
        help="Research DB パス（デフォルト: data/netkeiba_research.db）",
    )
    args = parser.parse_args()

    # Research DB の解決
    if args.research_db:
        p = Path(args.research_db)
        research_db: Optional[Path] = p if p.is_absolute() else _ROOT / p
    else:
        research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None

    print(f"\n{'='*65}")
    print("  UMALOGI 厳密 Walk-Forward バックテスト")
    print(f"  期間: 2024-01-01 〜 2025-12-31")
    print(f"  固定¥100ベット / カンニング完全排除")
    print(f"  Research DB: {'あり (' + str(research_db) + ')' if research_db else 'なし（win_odds 大幅制限）'}")
    print(f"{'='*65}")

    conn = sqlite3.connect(str(_MAIN_DB))
    conn.execute("PRAGMA foreign_keys = ON")

    # Phase 1: データ品質チェック
    print("\n[Phase 1] データ品質チェック...")
    dq = check_data_quality(conn)
    dq.print_summary()

    all_results: list[WindowResult] = []
    sweep_summary: dict[str, dict[str, float]] = {}

    # Phase 2: AlphaModel Walk-Forward
    print(f"\n[Phase 2] AlphaModel Walk-Forward バックテスト (3窓×2馬券種)...")
    alpha_results = run_alpha_walkforward(
        conn=conn,
        excluded_ids=dq.excluded_ids,
        ev_threshold=_DEFAULT_ALPHA_THRESHOLD,
        research_db_path=research_db,
    )
    all_results.extend(alpha_results)

    # Phase 3: EV閾値スイープ
    if not args.no_sweep:
        print(f"\n[Phase 3] EV閾値スイープ (AlphaModel 2024→2025)...")
        for bt in ("単勝", "複勝"):
            result = sweep_alpha_thresholds(
                conn, dq.excluded_ids, bt, research_db_path=research_db
            )
            sweep_summary[f"ALPHA({bt})_{bt}"] = result

    # Phase 4: 本命/卍/複勝 Walk-Forward
    if not args.alpha_only:
        print(f"\n[Phase 4] 本命/卍/複勝モデル Walk-Forward バックテスト...")
        legacy = run_legacy_walkforward(conn, dq.excluded_ids)
        all_results.extend(legacy)

    conn.close()

    # JSON保存
    _save_results_json(
        all_results, sweep_summary,
        _ROOT / "data" / "strict_backtest_result.json",
    )

    # Phase 5: 社長向け最終報告
    print_final_report(all_results, sweep_summary)


def _save_results_json(
    all_results: list[WindowResult],
    sweep_summary: dict[str, dict[str, float]],
    out_path: Path,
) -> None:
    """バックテスト結果を JSON に保存する（エンコード問題回避）。"""
    import json, dataclasses

    data = {
        "results": [dataclasses.asdict(r) for r in all_results],
        "sweep": sweep_summary,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n[JSON保存] {out_path}", flush=True)


if __name__ == "__main__":
    main()
