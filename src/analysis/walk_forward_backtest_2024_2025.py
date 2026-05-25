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

ROOT      = Path(__file__).resolve().parents[2]
DB_PATH   = ROOT / "data" / "umalogi.db"
OUT_JSON  = ROOT / "data" / "walk_forward_backtest_2024_2025.json"
RULES_DIR = ROOT / ".claudecode" / "rules"

# ── 期間 ──────────────────────────────────────────────────────────────────────
TRAIN_FROM = "2024-01-01"
TRAIN_TO   = "2024-05-31"
TEST_FROM  = "2025-01-01"
TEST_TO    = "2025-12-31"

# ── パラメータスイープ設定 ────────────────────────────────────────────────────
BET_TYPES:     list[str]   = ["単勝", "複勝", "ワイド", "馬連", "三連複"]
EV_THRESHOLDS: list[float] = [1.0, 1.1, 1.2, 1.3, 1.5]

# ── 流し設定 ───────────────────────────────────────────────────────────────────
# ワイド/馬連流し: 1軸(確率最上位) × NAGASHI_AITE頭 = NAGASHI_AITE通り
# 三連複流し    : 1軸            × C(NAGASHI_AITE,2)通り
NAGASHI_AITE = 4

# ── 資金管理 ──────────────────────────────────────────────────────────────────
INITIAL_BANKROLL     = 10_000.0
KELLY_FRACTION       = 0.05   # 1/20 Kelly（初期¥10,000でも破産しにくいよう控えめ）
MAX_BET_RATE         = 0.05   # 1ベット最大 bankroll × 5%（相対上限）
MAX_BET_ABS          = 15_000 # 1ベット物理上限 ¥15,000（JRAスリッページ対策）
MAX_RACE_BUDGET_RATE = 0.15   # 1レース最大 bankroll × 15%（相対上限）
MAX_RACE_BUDGET_ABS  = 50_000 # 1レース物理上限 ¥50,000（流動性上限）
MIN_BET              = 100

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


# ── ユーティリティ ─────────────────────────────────────────────────────────────
def _encode_sire(sire: str | None) -> int:
    """父馬名を整数コードに変換する（モジュールキャッシュ利用）。

    Args:
        sire: 父馬名。None または空文字の場合は "" として扱う。

    Returns:
        初登場の父馬には連番を割り当て、既出の場合はキャッシュ値を返す。
    """
    s = sire or ""
    if s not in _SIRE_CACHE:
        _SIRE_CACHE[s] = len(_SIRE_CACHE)
    return _SIRE_CACHE[s]


def _dist_band(distance: int) -> str:
    """距離バンド文字列を返す。

    Args:
        distance: レース距離（メートル）。

    Returns:
        "s" (短距離 <1400m) / "m" (マイル 1400-1800m) /
        "i" (中距離 1800-2200m) / "l" (長距離 2200m+)。
    """
    for name, (lo, hi) in _DIST_BANDS.items():
        if lo <= distance < hi:
            return name
    return "l"


# ── 騎手・調教師 勝率マップ ────────────────────────────────────────────────────
_JKY_MAP: dict[str, float] = {}
_TRN_MAP: dict[str, float] = {}


def build_jockey_trainer_maps(conn: sqlite3.Connection) -> None:
    """騎手・調教師の勝率マップをモジュールグローバルに構築する。

    TRAIN_FROM〜TRAIN_TO 期間の race_results から、5戦以上の騎手・調教師の
    勝率を集計して _JKY_MAP / _TRN_MAP を更新する。

    Args:
        conn: DB コネクション。
    """
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


# ── 馬の過去成績 ───────────────────────────────────────────────────────────────
def _horse_stats(
    conn: sqlite3.Connection,
    horse_id: str,
    race_date: str,
    surface: str,
    distance: int,
) -> dict[str, float]:
    """馬別の過去成績統計を返す（データリーク防止のため race_date 前のみ集計）。

    Args:
        conn: DB コネクション。
        horse_id: 馬 ID。
        race_date: 対象レース日付（"YYYY-MM-DD"）。この日付より前のレースのみ使用。
        surface: レース馬場（"芝" / "ダート" / "障害"）。
        distance: レース距離（メートル）。

    Returns:
        win_rate_all / win_rate_surface / win_rate_distance_band /
        recent_rank_mean の 4 キーを持つ辞書。過去成績なしの場合はゼロ値。
    """
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


# ── レース特徴量 DataFrame ─────────────────────────────────────────────────────
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


# ── 払戻取得 ───────────────────────────────────────────────────────────────────
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


# ── モデル訓練（2024年1〜5月データ）────────────────────────────────────────────
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


def predict_win_prob(model: LGBMClassifier, iso: IsotonicRegression, df: pd.DataFrame) -> np.ndarray:
    X   = df[FEATURE_COLS].astype(float).fillna(-1)
    raw = model.predict_proba(X.values)[:, 1]
    return iso.predict(raw)


# ── Harville 確率 ──────────────────────────────────────────────────────────────
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


# ── 流し/マルチ 購入点数計算 ────────────────────────────────────────────────────
def calculate_bet_combinations(
    ticket_type: str,
    jiku_count: int,
    aite_count: int,
    is_multi: bool = False,
) -> int:
    """
    流し・マルチの購入点数を返す。実投資額 = 100円 × 戻り値。

    ワイド/馬連 流し (jiku=1, aite=N):  N通り
    三連複 流し  (1軸, aite=N):        C(N,2) 通り
    ワイド/馬連 マルチ (N頭):           C(N,2) 通り
    三連複 マルチ (N頭):               C(N,3) 通り
    単勝/複勝:                         1通り（流し非対応）
    """
    from math import comb as _comb
    if ticket_type in ("単勝", "複勝"):
        return 1
    total = jiku_count + aite_count
    if ticket_type in ("ワイド", "馬連"):
        return _comb(total, 2) if is_multi else jiku_count * aite_count
    if ticket_type == "三連複":
        return _comb(total, 3) if is_multi else jiku_count * _comb(aite_count, 2)
    return 1


# ── Kelly 賭け金算出 ────────────────────────────────────────────────────────────
def kelly_bet(
    bankroll: float,
    p: float,
    bet_type: str,
    ev_threshold: float,
    race_budget_remaining: float,
    actual_win_odds: float | None = None,
) -> float:
    """EV閾値フィルタ + フラクショナルKelly + 物理上限キャップ でベット額を算出"""
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
    abs_cap = float(MAX_BET_ABS)
    bet     = min(bankroll * effective, rel_cap, abs_cap, race_budget_remaining)
    if bet < MIN_BET:
        if kelly_full > 0 and race_budget_remaining >= MIN_BET:
            return float(MIN_BET)
        return 0.0
    return float(int(bet / 100) * 100)


# ── SimResult データクラス ─────────────────────────────────────────────────────
@dataclass
class SimResult:
    bet_type:      str
    ev_threshold:  float
    bankroll:      float = field(default=INITIAL_BANKROLL)
    peak_bankroll: float = field(default=INITIAL_BANKROLL)
    records:       list[dict] = field(default_factory=list)
    bl_history:    list[float] = field(default_factory=list)

    @property
    def key(self) -> Pattern:
        return (self.bet_type, self.ev_threshold)


# ── 1レース・1パターン シミュレーション ────────────────────────────────────────
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
    race_budget = min(bankroll * MAX_RACE_BUDGET_RATE, bankroll, float(MAX_RACE_BUDGET_ABS))

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
            "race_id":       race_id,
            "date":          race_date,
            "bet_type":      bet_type,
            "ev_threshold":  ev_threshold,
            "combo":         combo,
            "p_bet":         p_bet,
            "bet_amount":    bet,
            "payout_per100": payout_a,
            "is_hit":        int(is_hit),
            "profit":        profit,
        })
        total_invested += bet

    i1 = sorted_idx[0]
    h1 = horse_nums[i1]
    p1 = prob_list[i1]
    # win_odds は race_df に含まれている（predict_win_prob には使わない）
    w1_raw = race_df.iloc[i1]["win_odds"] if "win_odds" in race_df.columns else None
    w1 = float(w1_raw) if w1_raw is not None and not pd.isna(w1_raw) else None

    if bet_type == "単勝":
        add_bet(str(h1), p1, w1)

    elif bet_type == "複勝":
        p_place = min(p1 * 2.5, 0.95)
        add_bet(str(h1), p_place, w1)

    elif bet_type in ("ワイド", "馬連") and n >= 2:
        # 流し: 1軸(確率最上位) × 相手 NAGASHI_AITE 頭
        n_aite    = min(NAGASHI_AITE, n - 1)
        aite_idxs = sorted_idx[1:n_aite + 1]
        n_combos  = calculate_bet_combinations(bet_type, 1, n_aite)
        remaining = race_budget - total_invested
        if remaining < MIN_BET * n_combos:
            return records  # 予算不足: 流し全体をスキップ

        # 各コンボの Harville 確率の平均で Kelly 算出
        p_fn = _harville_wide if bet_type == "ワイド" else _harville_quinella
        p_vals = [p_fn(prob_list, i1, ai) for ai in aite_idxs]
        avg_p  = float(np.mean(p_vals))

        total_kelly = kelly_bet(bankroll, avg_p, bet_type, ev_threshold, remaining)
        if total_kelly <= 0:
            return records

        # Kelly 総額をコンボ数で均等配分（各¥100単位・物理上限キャップ）
        per_combo = max(MIN_BET, min(MAX_BET_ABS, int(total_kelly / n_combos / 100) * 100))

        pmap = payouts.get(bet_type, {})
        for ai in aite_idxs:
            combo    = _combo_key(h1, horse_nums[ai])
            payout_a = float(pmap.get(combo, 0))
            is_hit   = payout_a > 0
            profit   = (payout_a / 100.0 * per_combo) - per_combo if is_hit else -per_combo
            records.append({
                "race_id":       race_id,
                "date":          race_date,
                "bet_type":      bet_type,
                "ev_threshold":  ev_threshold,
                "combo":         combo,
                "p_bet":         avg_p,
                "bet_amount":    per_combo,
                "payout_per100": payout_a,
                "is_hit":        int(is_hit),
                "profit":        profit,
            })
            total_invested += per_combo

    elif bet_type == "三連複" and n >= 3:
        # 流し: 1軸(確率最上位) × 相手 NAGASHI_AITE 頭から2頭選択
        n_aite    = min(NAGASHI_AITE, n - 1)
        aite_idxs = sorted_idx[1:n_aite + 1]
        n_combos  = calculate_bet_combinations(bet_type, 1, n_aite)
        if n_combos == 0:
            return records
        remaining = race_budget - total_invested
        if remaining < MIN_BET * n_combos:
            return records

        pairs   = list(itertools.combinations(range(n_aite), 2))
        p_vals  = [_harville_trio(prob_list, i1, aite_idxs[a], aite_idxs[b]) for a, b in pairs]
        avg_p   = float(np.mean(p_vals))

        total_kelly = kelly_bet(bankroll, avg_p, bet_type, ev_threshold, remaining)
        if total_kelly <= 0:
            return records

        per_combo = max(MIN_BET, min(MAX_BET_ABS, int(total_kelly / n_combos / 100) * 100))

        pmap = payouts.get(bet_type, {})
        for a, b in pairs:
            combo    = _combo_key(h1, horse_nums[aite_idxs[a]], horse_nums[aite_idxs[b]])
            payout_a = float(pmap.get(combo, 0))
            is_hit   = payout_a > 0
            profit   = (payout_a / 100.0 * per_combo) - per_combo if is_hit else -per_combo
            records.append({
                "race_id":       race_id,
                "date":          race_date,
                "bet_type":      bet_type,
                "ev_threshold":  ev_threshold,
                "combo":         combo,
                "p_bet":         avg_p,
                "bet_amount":    per_combo,
                "payout_per100": payout_a,
                "is_hit":        int(is_hit),
                "profit":        profit,
            })
            total_invested += per_combo

    return records


# ── 25パターン並行シミュレーションループ ──────────────────────────────────────
def run_simulation(
    conn: sqlite3.Connection,
    model: LGBMClassifier,
    iso: IsotonicRegression,
) -> dict[Pattern, SimResult]:
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

    results: dict[Pattern, SimResult] = {
        (bt, ev): SimResult(bet_type=bt, ev_threshold=ev)
        for bt in BET_TYPES
        for ev in EV_THRESHOLDS
    }

    for idx, (race_id, race_date) in enumerate(race_list):
        race_df_full = build_race_df(conn, race_id, race_date, include_rank=True)
        if race_df_full is None or len(race_df_full) < 2:
            continue

        feat_df = race_df_full.drop(columns=["rank"])
        probs   = predict_win_prob(model, iso, feat_df)
        payouts = get_payouts(conn, race_id)

        for (bt, ev), sim in results.items():
            if sim.bankroll < MIN_BET:
                sim.bl_history.append(0.0)
                continue
            bets = simulate_race_pattern(
                race_id, race_date, feat_df, probs, payouts,
                bt, ev, sim.bankroll,
            )
            if bets:
                net = sum(b["profit"] for b in bets)
                sim.bankroll      = max(sim.bankroll + net, 0.0)
                sim.peak_bankroll = max(sim.peak_bankroll, sim.bankroll)
                sim.records.extend(bets)
            sim.bl_history.append(sim.bankroll)

        if (idx + 1) % 500 == 0:
            logger.info("進行 %d/%d レース", idx + 1, len(race_list))

    return results


# ── 指標集計ヘルパー ───────────────────────────────────────────────────────────
def _payout_sum(records: list[dict]) -> float:
    return sum(
        r["payout_per100"] / 100.0 * r["bet_amount"] if r["is_hit"] else 0.0
        for r in records
    )


def calc_max_drawdown(bl_history: list[float]) -> float:
    if not bl_history:
        return 0.0
    peak   = bl_history[0]
    max_dd = 0.0
    for bl in bl_history:
        peak   = max(peak, bl)
        if peak > 0:
            dd     = (peak - bl) / peak * 100.0
            max_dd = max(max_dd, dd)
    return max_dd


def summarize_pattern(sim: SimResult) -> dict:
    recs      = sim.records
    n_bets    = len(recs)
    n_hits    = sum(r["is_hit"] for r in recs)
    total_in  = sum(r["bet_amount"] for r in recs)
    total_out = _payout_sum(recs)
    roi       = total_out / total_in * 100 if total_in > 0 else 0.0
    hit_rate  = n_hits / n_bets * 100 if n_bets > 0 else 0.0
    profit    = total_out - total_in
    mdd       = calc_max_drawdown(sim.bl_history)
    return {
        "bet_type":       sim.bet_type,
        "ev_threshold":   sim.ev_threshold,
        "n_bets":         n_bets,
        "n_hits":         n_hits,
        "hit_rate":       round(hit_rate, 1),
        "total_invest":   int(total_in),
        "total_return":   int(total_out),
        "net_profit":     int(profit),
        "roi":            round(roi, 1),
        "max_drawdown":   round(mdd, 1),
        "final_bankroll": int(sim.bankroll),
    }


# ── 成績マトリクス出力 ─────────────────────────────────────────────────────────
SEP = "=" * 100


def print_matrix(results: dict[Pattern, SimResult], cv_auc: float) -> pd.DataFrame:
    rows = [summarize_pattern(sim) for sim in results.values()]
    df   = pd.DataFrame(rows).sort_values(["bet_type", "ev_threshold"]).reset_index(drop=True)

    print(SEP)
    print("【Walk-Forward Backtest 2024-2025 — 25パターン 成績マトリクス】")
    print(f"  Train  : {TRAIN_FROM} ～ {TRAIN_TO}")
    print(f"  Test   : {TEST_FROM}  ～ {TEST_TO}")
    print(f"  CV AUC : {cv_auc:.4f}")
    print(f"  初期資金: ¥{INITIAL_BANKROLL:,.0f}  Kelly分数: {KELLY_FRACTION}")
    print(SEP)
    print()

    print("■ ROI(%) マトリクス")
    pivot_roi = df.pivot(index="bet_type", columns="ev_threshold", values="roi")
    pivot_roi = pivot_roi.reindex(BET_TYPES)
    print(pivot_roi.to_string(float_format=lambda x: f"{x:7.1f}%"))

    print()
    print("■ 的中率(%) マトリクス")
    pivot_hit = df.pivot(index="bet_type", columns="ev_threshold", values="hit_rate")
    pivot_hit = pivot_hit.reindex(BET_TYPES)
    print(pivot_hit.to_string(float_format=lambda x: f"{x:7.1f}%"))

    print()
    print("■ 最大ドローダウン(%) マトリクス")
    pivot_mdd = df.pivot(index="bet_type", columns="ev_threshold", values="max_drawdown")
    pivot_mdd = pivot_mdd.reindex(BET_TYPES)
    print(pivot_mdd.to_string(float_format=lambda x: f"{x:7.1f}%"))

    print()
    print("■ 純利益(円) マトリクス")
    pivot_pnl = df.pivot(index="bet_type", columns="ev_threshold", values="net_profit")
    pivot_pnl = pivot_pnl.reindex(BET_TYPES)
    print(pivot_pnl.to_string(float_format=lambda x: f"¥{x:+,.0f}"))

    print()
    print("■ 全25パターン 詳細テーブル")
    hdr = (
        f"{'券種':<8}{'EV閾値':>7}{'件数':>6}{'的中率':>8}"
        f"{'投資額':>12}{'回収額':>12}{'ROI':>8}{'損益':>12}{'最大DD':>8}{'最終残高':>12}"
    )
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


# ── 推奨ポートフォリオ ─────────────────────────────────────────────────────────
def recommend_portfolio(df: pd.DataFrame) -> str:
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

    # ROI × (1 - max_drawdown/100) でスコアリング
    profitable["score"]  = profitable["roi"] * (1.0 - profitable["max_drawdown"] / 100.0)
    total_score          = profitable["score"].sum()
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

    best = profitable.sort_values("score", ascending=False).iloc[0]
    lines.append("")
    lines.append("  【ポートフォリオ構築方針】")
    lines.append(f"  1. 主軸: {best['bet_type']} × EV≥{best['ev_threshold']:.1f} に資金の{best['weight']:.0f}%を投下")
    lines.append(f"     ROI={best['roi']:.1f}%  的中率={best['hit_rate']:.1f}%  最大DD={best['max_drawdown']:.1f}%")
    lines.append("")
    lines.append("  2. リスク分散: 推奨パターンを「score比率」で資金を按分する。")
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


# ── 結果保存 ───────────────────────────────────────────────────────────────────
def save_results(df: pd.DataFrame, cv_auc: float, portfolio_text: str) -> None:
    RULES_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "train":            [TRAIN_FROM, TRAIN_TO],
            "test":             [TEST_FROM,  TEST_TO],
            "cv_auc":           cv_auc,
            "kelly_fraction":   KELLY_FRACTION,
            "initial_bankroll": INITIAL_BANKROLL,
            "nagashi_aite":     NAGASHI_AITE,
            "generated_at":     "2026-05-23",
        },
        "patterns": df.to_dict(orient="records"),
    }
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON 保存: %s", OUT_JSON)

    # ── ポートフォリオ戦略を恒久ルールとして保存 ──────────────────────────────
    strategy_path = RULES_DIR / "portfolio_strategy_2024_2025.md"
    profitable = df[(df["roi"] > 100.0) & (df["n_bets"] >= 10)].copy()
    top5 = df.sort_values("roi", ascending=False).head(5)

    lines = [
        "# ポートフォリオ戦略 2024-2025 Walk-Forward バックテスト結果",
        "",
        f"**生成日**: 2026-05-23  ",
        f"**Train**: {TRAIN_FROM} ～ {TRAIN_TO}  ",
        f"**Test** : {TEST_FROM} ～ {TEST_TO}  ",
        f"**CV AUC**: {cv_auc:.4f}  ",
        f"**初期資金**: ¥{INITIAL_BANKROLL:,.0f}  ",
        f"**Kelly分数**: {KELLY_FRACTION} (1/{int(1/KELLY_FRACTION)} Kelly)  ",
        f"**流し相手頭数**: {NAGASHI_AITE}頭  ",
        "",
        "---",
        "",
        "## 25パターン ROI マトリクス",
        "",
        "| 券種 | EV≥1.0 | EV≥1.1 | EV≥1.2 | EV≥1.3 | EV≥1.5 |",
        "|------|--------|--------|--------|--------|--------|",
    ]
    for bt in BET_TYPES:
        sub = df[df["bet_type"] == bt].sort_values("ev_threshold")
        rois = [f"{row['roi']:.1f}%" for _, row in sub.iterrows()]
        lines.append(f"| {bt} | " + " | ".join(rois) + " |")

    lines += [
        "",
        "## ROI 上位5パターン",
        "",
        "| 券種 | EV閾値 | 件数 | 的中率 | ROI | 最大DD | 最終残高 |",
        "|------|--------|------|--------|-----|--------|----------|",
    ]
    for _, r in top5.iterrows():
        lines.append(
            f"| {r['bet_type']} | ≥{r['ev_threshold']:.1f} | {r['n_bets']} | "
            f"{r['hit_rate']:.1f}% | {r['roi']:.1f}% | {r['max_drawdown']:.1f}% | "
            f"¥{r['final_bankroll']:,} |"
        )

    lines += ["", "## 推奨ポートフォリオ方針", ""]
    lines.append(portfolio_text.strip())

    lines += [
        "",
        "---",
        "",
        "## 流し点数計算ルール（永続ルール）",
        "",
        "```",
        f"ワイド/馬連 流し(1軸×{NAGASHI_AITE}頭): {NAGASHI_AITE}通り = ¥{100*NAGASHI_AITE}以上",
        f"三連複 流し (1軸×{NAGASHI_AITE}頭):     {NAGASHI_AITE*(NAGASHI_AITE-1)//2}通り = "
        f"¥{100*NAGASHI_AITE*(NAGASHI_AITE-1)//2}以上",
        "実投資額 = ¥100 × 購入点数 × 単位数",
        "払戻: 的中コンボのみ払戻 / 非的中コンボは-¥100×単位数 の損失",
        "流動性チェック: 残予算 < ¥100×点数 の場合はスキップ",
        "```",
    ]

    strategy_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("ポートフォリオ戦略保存: %s", strategy_path)


# ── メイン ─────────────────────────────────────────────────────────────────────
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
    save_results(summary_df, cv_auc, portfolio_text)

    conn.close()
    logger.info("=== バックテスト完了 ===")


if __name__ == "__main__":
    main()
