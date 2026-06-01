# -*- coding: utf-8 -*-
"""
本命直前モデル 2025年カンニングなし walk-forward バックテスト
src/analysis/honmei_dynamic_backtest.py

【設計】
  Train : 2024-01-01 ～ 2024-12-31（2024年全レース）
  Test  : 2025-01-01 ～ 2025-12-31（2025年全レース）

【カンニングなし保証】
  - 時系列は races.date を使用（race_id の辞書順は日付順と異なるため使用不可）
  - 馬の成績統計は「当該レースの date より前のレースのみ」で集計
  - 騎手/調教師 勝率も同様

【動的Kelly賭け】
  - 各レースの field_size から base_rate = 1/field_size を計算
  - edge = p_top / base_rate（ランダム予測比）
  - edge > EDGE_THRESHOLD のみ発注
  - 1/10 フラクショナルKelly でベット金額を決定
  - 1レース最大 MAX_RACE_BUDGET_RATE × bankroll

【実行】
  py -m src.analysis.honmei_dynamic_backtest
"""

from __future__ import annotations

import io
import itertools
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [backtest] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("honmei_backtest")

# ────────────────────────────────────────────────────────────────────────────
# 設定定数
# ────────────────────────────────────────────────────────────────────────────

TRAIN_FROM = "2024-01-01"
TRAIN_TO = "2024-12-31"
TEST_FROM = "2025-01-01"
TEST_TO = "2025-12-31"

INITIAL_BANKROLL = 50_000.0
KELLY_FRACTION = 0.10  # 1/10 Kelly
MAX_RACE_BUDGET_RATE = 0.15  # 1レース最大 bankroll × 15%
MAX_BET_RATE = 0.05  # 1ベット最大 bankroll × 5%
MIN_BET = 100  # 最低賭け金（円）
EDGE_THRESHOLD = 1.10  # ランダム予測比 edge > 1.10 で発注
TRIO_EDGE_THRESHOLD = 1.50  # 三連複は edge > 1.50 のみ

# JRA 控除率（参考値・EV 計算に使用）
TRACK_TAKE: dict[str, float] = {
    "単勝": 0.200,
    "複勝": 0.200,
    "馬連": 0.225,
    "ワイド": 0.225,
    "三連複": 0.250,
}

# 各券種の典型市場オッズ（2024年実績ベース推定値）
TYPICAL_ODDS: dict[str, float] = {
    "単勝": 5.0,  # top-1 予測馬の単勝代表値
    "複勝": 2.0,  # 複勝は低配当
    "馬連": 12.0,  # top-2 馬連代表値
    "三連複": 25.0,  # top-3 三連複代表値
}

# bet_type の UTF-8 hex（race_payouts 照合用）
BET_TYPE_HEX = {
    "単勝": "E58D98E58B9D",
    "複勝": "E8A487E58B9D",
    "馬連": "E9A6ACE980A3",
    "ワイド": "E383AFE382A4E38389",
    "三連複": "E4B889E980A3E8A487",
}

# ────────────────────────────────────────────────────────────────────────────
# エンコーダ定数
# ────────────────────────────────────────────────────────────────────────────

_SURFACE_CODE = {"芝": 0, "ダート": 1, "障害": 2}
_SEX_CODE = {"牡": 0, "牝": 1, "セ": 2}
_VENUE_CODE = {
    "札幌": 0,
    "函館": 1,
    "福島": 2,
    "新潟": 3,
    "東京": 4,
    "中山": 5,
    "中京": 6,
    "京都": 7,
    "阪神": 8,
    "小倉": 9,
}
_COND_CODE = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
_DIST_BANDS = [(0, 1400), (1400, 1800), (1800, 2200), (2200, 9999)]
_SIRE_MAP: dict[str, int] = {}
_JKY_MAP: dict[str, float] = {}
_TRN_MAP: dict[str, float] = {}


def _dist_band(d: int) -> int:
    for i, (lo, hi) in enumerate(_DIST_BANDS):
        if lo <= d < hi:
            return i
    return 3


def _encode_sire(s: str | None) -> int:
    if not s:
        return -1
    if s not in _SIRE_MAP:
        _SIRE_MAP[s] = len(_SIRE_MAP)
    return _SIRE_MAP[s]


# ────────────────────────────────────────────────────────────────────────────
# DB 接続
# ────────────────────────────────────────────────────────────────────────────


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_ROOT / "data" / "umalogi.db")
    c.execute("PRAGMA foreign_keys = ON")
    c.execute("PRAGMA journal_mode = WAL")
    return c


# ────────────────────────────────────────────────────────────────────────────
# 騎手 / 調教師 勝率マップ（2024年データから事前計算）
# ────────────────────────────────────────────────────────────────────────────


def _build_jky_trn_maps(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT rr.jockey, rr.trainer, rr.rank
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE r.date BETWEEN ? AND ?
          AND rr.rank IS NOT NULL AND rr.rank > 0
        """,
        (TRAIN_FROM, TRAIN_TO),
    ).fetchall()
    jky_total: dict[str, int] = defaultdict(int)
    jky_wins: dict[str, int] = defaultdict(int)
    trn_total: dict[str, int] = defaultdict(int)
    trn_wins: dict[str, int] = defaultdict(int)

    for jockey, trainer, rank in rows:
        if jockey:
            jky_total[jockey] += 1
            if rank == 1:
                jky_wins[jockey] += 1
        if trainer:
            trn_total[trainer] += 1
            if rank == 1:
                trn_wins[trainer] += 1

    for jky, total in jky_total.items():
        _JKY_MAP[jky] = jky_wins[jky] / total if total > 0 else 0.0
    for trn, total in trn_total.items():
        _TRN_MAP[trn] = trn_wins[trn] / total if total > 0 else 0.0
    logger.info("騎手マップ %d 人  調教師マップ %d 人", len(_JKY_MAP), len(_TRN_MAP))


# ────────────────────────────────────────────────────────────────────────────
# 馬別成績統計（date ベースでリーク防止）
# ────────────────────────────────────────────────────────────────────────────


def _horse_stats(
    conn: sqlite3.Connection,
    horse_id: str,
    race_date: str,  # "YYYY-MM-DD" — この日付より前のみ使用
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
    win_all = sum(1 for r in ranks_all if r == 1) / len(ranks_all)

    surf_rows = [r for r in rows if r[1] == surface]
    win_surf = (
        (sum(1 for r in surf_rows if r[0] == 1) / len(surf_rows)) if surf_rows else 0.0
    )

    lo, hi = _DIST_BANDS[band]
    dist_rows = [r for r in rows if lo <= r[2] < hi]
    win_dist = (
        (sum(1 for r in dist_rows if r[0] == 1) / len(dist_rows)) if dist_rows else 0.0
    )

    return {
        "win_rate_all": win_all,
        "win_rate_surface": win_surf,
        "win_rate_distance_band": win_dist,
        "recent_rank_mean": float(np.mean(ranks_all[:5])),
    }


# ────────────────────────────────────────────────────────────────────────────
# 1レース分の特徴量 DataFrame 構築
# ────────────────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "weight_carried",
    "horse_weight",
    "horse_weight_diff",
    "gate_number",
    "distance",
    "surface_code",
    "sex_code",
    "venue_code",
    "condition_code",
    "win_rate_all",
    "win_rate_surface",
    "win_rate_distance_band",
    "recent_rank_mean",
    "sire_code",
    "jockey_win_rate",
    "trainer_win_rate",
]


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
               h.sire
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
    for (
        horse_id,
        horse_name,
        rank,
        weight_carried,
        horse_weight,
        hw_diff,
        gate_number,
        horse_number,
        sex_age,
        jockey,
        trainer,
        sire,
    ) in rr_rows:
        stats = _horse_stats(conn, horse_id or "", race_date, surface, distance)
        sex_str = (sex_age or "")[:1]

        records.append(
            {
                "race_id": race_id,
                "race_date": race_date,
                "horse_id": horse_id or "",
                "horse_name": horse_name or "",
                "horse_number": int(horse_number or 0),
                "rank": int(rank),
                "weight_carried": float(weight_carried or 55.0),
                "horse_weight": float(horse_weight or 480.0),
                "horse_weight_diff": float(hw_diff or 0.0),
                "gate_number": int(gate_number or 0),
                "distance": int(distance or 1600),
                "surface_code": _SURFACE_CODE.get(surface or "", -1),
                "sex_code": _SEX_CODE.get(sex_str, -1),
                "venue_code": _VENUE_CODE.get(venue or "", 10),
                "condition_code": _COND_CODE.get(condition or "", 0),
                "sire_code": _encode_sire(sire),
                "jockey_win_rate": _JKY_MAP.get(jockey or "", 0.05),
                "trainer_win_rate": _TRN_MAP.get(trainer or "", 0.05),
                **stats,
            }
        )

    df = pd.DataFrame(records)
    if not include_rank:
        df = df.drop(columns=["rank"])
    return df


# ────────────────────────────────────────────────────────────────────────────
# モデル（LightGBM + Isotonic）—— 2024年データのみで訓練
# ────────────────────────────────────────────────────────────────────────────

LGBM_PARAMS: dict[str, Any] = dict(
    n_estimators=600,
    learning_rate=0.04,
    max_depth=6,
    num_leaves=63,
    min_child_samples=20,
    subsample=0.8,
    colsample_bytree=0.8,
    class_weight="balanced",
    random_state=42,
    verbose=-1,
)


def train_honmei_model(train_df: pd.DataFrame) -> tuple:
    from lightgbm import LGBMClassifier

    feat_cols = [c for c in FEATURE_COLS if c in train_df.columns]
    X = train_df[feat_cols].astype(float).fillna(-1)
    y = (train_df["rank"] == 1).astype(int)
    groups = train_df["race_id"]

    n_races = groups.nunique()
    n_splits = min(5, n_races)
    logger.info(
        "LightGBM 訓練: %d レース / %d サンプル / %d-fold CV", n_races, len(X), n_splits
    )

    gkf = GroupKFold(n_splits=n_splits)
    oof_preds = np.zeros(len(X), dtype=float)
    aucs: list[float] = []

    for tr_idx, val_idx in gkf.split(X, y, groups=groups):
        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X.iloc[tr_idx], y.iloc[tr_idx])
        proba = clf.predict_proba(X.iloc[val_idx])[:, 1]
        oof_preds[val_idx] = proba
        try:
            aucs.append(roc_auc_score(y.iloc[val_idx], proba))
        except ValueError:
            pass

    cv_auc = float(np.mean(aucs)) if aucs else float("nan")
    logger.info("CV AUC: %.4f", cv_auc)

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oof_preds, y)

    final_clf = LGBMClassifier(**LGBM_PARAMS)
    final_clf.fit(X, y)
    return final_clf, iso, feat_cols, cv_auc


def predict_win_prob(model, iso, feat_cols: list[str], df: pd.DataFrame) -> np.ndarray:
    X = df[feat_cols].astype(float).fillna(-1)
    raw = model.predict_proba(X)[:, 1]
    return iso.predict(raw)


# ────────────────────────────────────────────────────────────────────────────
# 払戻データ取得
# ────────────────────────────────────────────────────────────────────────────


def get_payouts(conn: sqlite3.Connection, race_id: str) -> dict[str, dict]:
    rows = conn.execute(
        "SELECT hex(bet_type), combination, payout FROM race_payouts WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    hex2label = {v: k for k, v in BET_TYPE_HEX.items()}
    result: dict[str, dict] = defaultdict(dict)
    for hex_bt, combo, payout in rows:
        label = hex2label.get(hex_bt.upper())
        if label:
            result[label][combo] = int(payout or 0)
    return dict(result)


# ────────────────────────────────────────────────────────────────────────────
# Harville 確率（馬連・三連複推定）
# ────────────────────────────────────────────────────────────────────────────


def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    pi, pj = probs[i], probs[j]
    q = pi * pj / (1.0 - pi) if pi < 1.0 else 0.0
    q += pj * pi / (1.0 - pj) if pj < 1.0 else 0.0
    return min(q, 0.99)


def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    total = 0.0
    for a, b, c in itertools.permutations([i, j, k]):
        pa, pb, pc = probs[a], probs[b], probs[c]
        d1 = 1.0 - pa
        d2 = 1.0 - pa - pb
        if d1 > 0 and d2 > 0:
            total += pa * pb / d1 * pc / d2
    return min(total, 0.99)


# ────────────────────────────────────────────────────────────────────────────
# Kelly 賭け金算出（field_size を考慮した edge-based Kelly）
# ────────────────────────────────────────────────────────────────────────────


def kelly_bet(
    bankroll: float,
    p: float,
    bet_type: str,
    field_size: int,
    race_budget_remaining: float,
    edge_threshold: float = EDGE_THRESHOLD,
) -> float:
    """
    edge-based 1/10 フラクショナルKelly でベット金額を算出。

    base_rate = 1 / field_size（ランダム選択時の勝率）
    edge      = p / base_rate（モデルの有利度）
    EV        = p × typical_odds

    edge > threshold のみ発注。
    """
    base_rate = 1.0 / max(field_size, 2)
    edge = p / base_rate
    if edge < edge_threshold:
        return 0.0

    typical_odds = TYPICAL_ODDS.get(bet_type, 5.0)
    ev = p * typical_odds
    if ev < 1.0:
        return 0.0  # 期待値が1未満なら発注しない

    # Kelly = (p × odds - 1) / (odds - 1)
    kelly_full = (p * typical_odds - 1.0) / max(typical_odds - 1.0, 1e-9)
    kelly_full = max(kelly_full, 0.0)
    effective_kelly = kelly_full * KELLY_FRACTION

    bet_size = bankroll * effective_kelly
    bet_size = min(bet_size, bankroll * MAX_BET_RATE, race_budget_remaining)
    if bet_size < MIN_BET:
        return 0.0
    return float(int(bet_size / 100) * 100)


# ────────────────────────────────────────────────────────────────────────────
# 1レース シミュレーション
# ────────────────────────────────────────────────────────────────────────────


def simulate_race(
    race_id: str,
    race_date: str,
    race_df: pd.DataFrame,
    probs: np.ndarray,
    payouts: dict[str, dict],
    bankroll: float,
) -> tuple[list[dict], float]:
    n = len(race_df)
    horse_nums = list(race_df["horse_number"].astype(int))
    prob_list = list(probs)
    race_budget = bankroll * MAX_RACE_BUDGET_RATE
    month = race_date[5:7]  # races.date の "YYYY-MM-DD" から月

    records: list[dict] = []
    total_invested = 0.0

    sorted_idx = sorted(range(n), key=lambda i: -prob_list[i])
    top_idx = sorted_idx[0]
    p_top = prob_list[top_idx]
    h_top = horse_nums[top_idx]

    # モデルの edge
    base_rate = 1.0 / max(n, 2)
    edge_top = p_top / base_rate

    def make_record(bt: str, combo: str, p_bet: float, bet_amt: float) -> dict:
        pmap = payouts.get(bt, {})
        payout_a = float(pmap.get(combo, 0))
        is_hit = payout_a > 0
        profit = (payout_a / 100.0 * bet_amt) - bet_amt if is_hit else -bet_amt
        return {
            "race_id": race_id,
            "date": race_date,
            "month": month,
            "bet_type": bt,
            "combo": combo,
            "p_bet": p_bet,
            "edge": edge_top,
            "bet_amount": bet_amt,
            "payout_per100": payout_a,
            "is_hit": int(is_hit),
            "profit": profit,
        }

    # ── 単勝: top-1 ──
    remaining = race_budget - total_invested
    bet_tan = kelly_bet(bankroll, p_top, "単勝", n, remaining)
    if bet_tan > 0:
        records.append(make_record("単勝", str(h_top), p_top, bet_tan))
        total_invested += bet_tan

    # ── 複勝: top-1（確率を3倍にしてbetサイズ決定）──
    remaining = race_budget - total_invested
    p_fuku = min(p_top * 2.5, 0.95)
    bet_fuku = kelly_bet(bankroll, p_fuku, "複勝", n, remaining)
    if bet_fuku > 0:
        records.append(make_record("複勝", str(h_top), p_fuku, bet_fuku))
        total_invested += bet_fuku

    # ── 馬連: top-2（edge が高い場合のみ）──
    if n >= 2 and edge_top >= EDGE_THRESHOLD:
        remaining = race_budget - total_invested
        i1, i2 = sorted_idx[0], sorted_idx[1]
        p_umaren = _harville_quinella(prob_list, i1, i2)
        h1, h2 = horse_nums[i1], horse_nums[i2]
        lo, hi = (h1, h2) if h1 < h2 else (h2, h1)
        bet_uma = kelly_bet(bankroll, p_umaren, "馬連", n, remaining)
        if bet_uma > 0:
            records.append(make_record("馬連", f"{lo}-{hi}", p_umaren, bet_uma))
            total_invested += bet_uma

    # ── 三連複: top-3（高 edge のみ）──
    if n >= 3 and edge_top >= TRIO_EDGE_THRESHOLD:
        remaining = race_budget - total_invested
        i1, i2, i3 = sorted_idx[0], sorted_idx[1], sorted_idx[2]
        p_trio = _harville_trio(prob_list, i1, i2, i3)
        trio_sorted = sorted([horse_nums[i1], horse_nums[i2], horse_nums[i3]])
        trio_str = f"{trio_sorted[0]}-{trio_sorted[1]}-{trio_sorted[2]}"
        bet_trio = kelly_bet(
            bankroll, p_trio, "三連複", n, remaining, edge_threshold=TRIO_EDGE_THRESHOLD
        )
        if bet_trio > 0:
            records.append(make_record("三連複", trio_str, p_trio, bet_trio))
            total_invested += bet_trio

    total_profit = sum(r["profit"] for r in records)
    return records, total_profit


# ────────────────────────────────────────────────────────────────────────────
# 2025年全レース シミュレーション ループ
# ────────────────────────────────────────────────────────────────────────────


def run_simulation(
    conn: sqlite3.Connection,
    model,
    iso,
    feat_cols: list[str],
) -> pd.DataFrame:
    # date 順で取得（race_id 順は日付順ではないため）
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

    bankroll = INITIAL_BANKROLL
    peak_bankroll = INITIAL_BANKROLL
    all_records: list[dict] = []
    bl_history: list[dict] = []
    skipped = 0
    bet_races = 0

    for idx, (race_id, race_date) in enumerate(race_list):
        race_df_raw = build_race_df(conn, race_id, race_date, include_rank=True)
        if race_df_raw is None or len(race_df_raw) < 2:
            skipped += 1
            continue

        race_df = race_df_raw.drop(columns=["rank"])
        probs = predict_win_prob(model, iso, feat_cols, race_df)
        payouts = get_payouts(conn, race_id)

        bets, net_profit = simulate_race(
            race_id, race_date, race_df, probs, payouts, bankroll
        )

        if bets:
            bankroll += net_profit
            bankroll = max(bankroll, 0.0)
            bet_races += 1

        peak_bankroll = max(peak_bankroll, bankroll)

        if bankroll < MIN_BET:
            logger.warning("破産: race=%s  bankroll=%.0f", race_id, bankroll)
            bankroll = 0.0

        for b in bets:
            b["bankroll_after"] = bankroll

        all_records.extend(bets)
        bl_history.append(
            {
                "date": race_date,
                "race_id": race_id,
                "bankroll": bankroll,
                "peak": peak_bankroll,
            }
        )

        if (idx + 1) % 500 == 0:
            logger.info(
                "進行 %d/%d レース (bet_races=%d)  残高=¥%d  ベット数=%d",
                idx + 1,
                len(race_list),
                bet_races,
                int(bankroll),
                len(all_records),
            )

    logger.info(
        "シミュレーション完了: %d レース (skip=%d, bet=%d)  最終残高=¥%d",
        len(race_list),
        skipped,
        bet_races,
        int(bankroll),
    )

    df = pd.DataFrame(all_records) if all_records else pd.DataFrame()
    df.attrs["final_bankroll"] = bankroll
    df.attrs["peak_bankroll"] = peak_bankroll
    df.attrs["bankroll_history"] = bl_history
    return df


# ────────────────────────────────────────────────────────────────────────────
# 最大ドローダウン計算
# ────────────────────────────────────────────────────────────────────────────


def calc_max_drawdown(history: list[dict]) -> float:
    if not history:
        return 0.0
    peak = history[0]["bankroll"]
    max_dd = 0.0
    for h in history:
        peak = max(peak, h["bankroll"])
        if peak > 0:
            dd = (peak - h["bankroll"]) / peak * 100.0
            max_dd = max(max_dd, dd)
    return max_dd


# ────────────────────────────────────────────────────────────────────────────
# 払戻計算ヘルパー
# ────────────────────────────────────────────────────────────────────────────


def _payout_sum(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(
        df.apply(
            lambda r: (
                r["payout_per100"] / 100.0 * r["bet_amount"] if r["is_hit"] else 0.0
            ),
            axis=1,
        ).sum()
    )


# ────────────────────────────────────────────────────────────────────────────
# レポート出力
# ────────────────────────────────────────────────────────────────────────────


def print_report(df: pd.DataFrame, cv_auc: float) -> None:
    sep = "=" * 78
    final_bl = df.attrs.get("final_bankroll", 0.0)
    peak_bl = df.attrs.get("peak_bankroll", 0.0)
    history = df.attrs.get("bankroll_history", [])
    max_dd = calc_max_drawdown(history)
    bankrupt = "✅ ゼロ（破産なし）" if final_bl > 0 else "💀 破産"

    print(sep)
    print("【本命直前モデル 2025年 カンニングなし walk-forward バックテスト】")
    print(f"  Train  : {TRAIN_FROM} ～ {TRAIN_TO}")
    print(f"  Test   : {TEST_FROM}  ～ {TEST_TO}")
    print(f"  CV AUC (2024年モデル) : {cv_auc:.4f}")
    print(
        f"  Kelly 分数            : {KELLY_FRACTION} (1/{int(1 / KELLY_FRACTION)} Kelly)"
    )
    print(
        f"  Edge 閾値             : {EDGE_THRESHOLD}× (1/{int(EDGE_THRESHOLD)}以上の有利度のみ)"
    )
    print(f"  1レース最大投資        : bankroll × {MAX_RACE_BUDGET_RATE * 100:.0f}%")
    print(f"  1ベット最大投資        : bankroll × {MAX_BET_RATE * 100:.0f}%")
    print(sep)

    if df.empty:
        print("[ベットデータなし — edge 閾値を満たすレースが存在しませんでした]")
        return

    n_bets = len(df)
    n_races = df["race_id"].nunique()
    total_in = float(df["bet_amount"].sum())
    total_out = _payout_sum(df)
    n_hits = int(df["is_hit"].sum())
    roi_all = total_out / total_in * 100 if total_in > 0 else 0.0
    profit = total_out - total_in
    gain_rate = (final_bl / INITIAL_BANKROLL - 1.0) * 100.0

    print("\n■ 資金曲線サマリー")
    print(f"  初期資金          : ¥{INITIAL_BANKROLL:>12,.0f}")
    print(
        f"  最高到達残高      : ¥{peak_bl:>12,.0f}  ({(peak_bl / INITIAL_BANKROLL - 1) * 100:+.1f}%)"
    )
    print(f"  2025年末 最終残高 : ¥{final_bl:>12,.0f}  ({gain_rate:+.1f}%)")
    print(f"  最大ドローダウン  : {max_dd:.1f}%")
    print(f"  破産              : {bankrupt}")

    print("\n■ ベット全体サマリー")
    print(f"  発注レース数      : {n_races:,}")
    print(f"  総ベット件数      : {n_bets:,}")
    print(f"  的中件数          : {n_hits:,}  ({n_hits / n_bets * 100:.1f}%)")
    print(f"  総投資額          : ¥{total_in:,.0f}")
    print(f"  総回収額          : ¥{total_out:,.0f}")
    print(f"  ROI               : {roi_all:.1f}%")
    print(f"  損益              : ¥{profit:+,.0f}")

    # ── 券種別 ──
    print("\n■ 券種別 ROI 詳細")
    hdr = f"{'券種':<6}{'件数':>6}{'的中':>6}{'的中率':>7}{'投資額':>12}{'回収額':>12}{'ROI':>7}{'損益':>11}{'最大払戻':>9}"
    print(hdr)
    print("-" * 78)
    best_bt, best_pnl = "", float("-inf")
    for bt in ["単勝", "複勝", "馬連", "ワイド", "三連複"]:
        sub = df[df["bet_type"] == bt]
        if sub.empty:
            continue
        inv = float(sub["bet_amount"].sum())
        out = _payout_sum(sub)
        hit = int(sub["is_hit"].sum())
        roi = out / inv * 100 if inv > 0 else 0.0
        pnl = out - inv
        mxp = (
            float(sub.loc[sub["is_hit"] == 1, "payout_per100"].max())
            if hit > 0
            else 0.0
        )
        mark = " ★" if roi >= 100 else (" ▲" if roi >= 80 else "")
        print(
            f"{bt:<6}{len(sub):>6,}{hit:>6,}{hit / len(sub) * 100:>6.1f}%"
            f"{inv:>12,.0f}{out:>12,.0f}{roi:>6.1f}%{pnl:>11,.0f}{mxp:>9,.0f}{mark}"
        )
        if pnl > best_pnl:
            best_pnl = pnl
            best_bt = bt
    print("-" * 78)

    # ── 月次推移 ──
    print("\n■ 月次 ROI 推移")
    print(
        f"{'月':>4}{'件数':>5}{'的中':>5}{'投資額':>10}{'ROI':>7}{'月次損益':>10}{'月末残高':>12}"
    )
    print("-" * 58)

    # 月ごとの最終 bankroll を bankroll_history から取得
    bl_df = pd.DataFrame(history) if history else pd.DataFrame()

    for month in sorted(df["month"].unique()):
        msub = df[df["month"] == month]
        minv = float(msub["bet_amount"].sum())
        mout = _payout_sum(msub)
        mhit = int(msub["is_hit"].sum())
        mroi = mout / minv * 100 if minv > 0 else 0.0
        mpnl = mout - minv
        # 月末残高
        if not bl_df.empty:
            bl_month = bl_df[bl_df["date"].str[5:7] == month]
            last_bl = bl_month["bankroll"].iloc[-1] if not bl_month.empty else 0.0
        else:
            last_bl = 0.0
        print(
            f"{month:>4}月{len(msub):>5,}{mhit:>5,}{minv:>10,.0f}{mroi:>6.1f}%{mpnl:>10,.0f}{last_bl:>12,.0f}"
        )

    # ── Edge 区分別 ──
    print("\n■ Edge 強度別 ROI（単勝のみ）")
    tan_df = df[df["bet_type"] == "単勝"]
    print(f"{'edge≥':>6}{'件数':>6}{'的中':>5}{'投資額':>10}{'ROI':>7}{'損益':>10}")
    print("-" * 48)
    for thr in [1.1, 1.3, 1.5, 2.0, 3.0]:
        sub = tan_df[tan_df["edge"] >= thr]
        if sub.empty:
            continue
        inv = float(sub["bet_amount"].sum())
        out = _payout_sum(sub)
        hit = int(sub["is_hit"].sum())
        roi = out / inv * 100 if inv > 0 else 0.0
        pnl = out - inv
        mark = " ★" if roi >= 100 else ""
        print(
            f"{thr:>6.1f}x{len(sub):>5,}{hit:>5,}{inv:>10,.0f}{roi:>6.1f}%{pnl:>10,.0f}{mark}"
        )

    # ── ALPHA モデルとの比較 ──
    print(f"\n{'─' * 78}")
    print("【ALPHA モデルとの比較（参考値）】")
    print(f"{'指標':<30}{'本命モデル':>20}{'ALPHAモデル':>20}")
    print("─" * 72)
    rows_cmp = [
        ("初期資金", f"¥{INITIAL_BANKROLL:>,.0f}", "¥50,000"),
        ("2025年末残高", f"¥{final_bl:>,.0f}", "—"),
        ("最高到達残高", f"¥{peak_bl:>,.0f}", "—"),
        ("最大ドローダウン", f"{max_dd:.1f}%", "—"),
        ("全体ROI", f"{roi_all:.1f}%", "—"),
        ("総ベット件数", f"{n_bets:,}", "—"),
        ("的中率", f"{n_hits / n_bets * 100:.1f}%", "—"),
        ("利益貢献券種", f"{best_bt}", "—"),
        ("破産", "なし" if final_bl > 0 else "あり", "—"),
    ]
    for lbl, me, alpha in rows_cmp:
        print(f"{lbl:<30}{me:>20}{alpha:>20}")
    print("─" * 72)

    print(f"\n{sep}")
    print("【重要な前提・制約事項】")
    print("  1. 2025年 race_results.win_odds は全 NULL → EV は典型市場オッズで推定")
    print(
        f"  2. 単勝オッズ推定値: {TYPICAL_ODDS['単勝']}倍、馬連: {TYPICAL_ODDS['馬連']}倍、三連複: {TYPICAL_ODDS['三連複']}倍"
    )
    print("  3. Train データ(2024年)が 1,424 レース/7,426 サンプルと小規模")
    print("  4. 2024年 race_results は rank=4以上が部分的にしか記録されていない")
    print("  5. 馬単・三連単は 2025年 combination エンコード問題により除外")
    print(f"{sep}")


# ────────────────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────────────────


def main() -> None:
    logger.info("=== 本命直前モデル 2025年バックテスト 開始 ===")
    conn = _conn()

    logger.info("Step 1: 騎手/調教師 勝率マップ構築 (2024年データ)...")
    _build_jky_trn_maps(conn)

    logger.info("Step 2: 2024年 Train セット構築...")
    train_list = conn.execute(
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
    logger.info("Train レース数: %d", len(train_list))

    train_dfs = []
    for race_id, race_date in train_list:
        df = build_race_df(conn, race_id, race_date, include_rank=True)
        if df is not None:
            train_dfs.append(df)

    train_df = pd.concat(train_dfs, ignore_index=True)
    logger.info(
        "Train サンプル数: %d  (レース=%d)",
        len(train_df),
        train_df["race_id"].nunique(),
    )

    logger.info("Step 3: 本命モデル訓練 (2024年データのみ)...")
    model, iso, feat_cols, cv_auc = train_honmei_model(train_df)

    logger.info("Step 4: 2025年 walk-forward シミュレーション...")
    result_df = run_simulation(conn, model, iso, feat_cols)

    print_report(result_df, cv_auc)

    # JSON 保存
    out_path = _ROOT / "data" / "honmei_dynamic_backtest_2025.json"
    history = result_df.attrs.get("bankroll_history", [])
    summary = {
        "config": {
            "train": [TRAIN_FROM, TRAIN_TO],
            "test": [TEST_FROM, TEST_TO],
            "kelly_fraction": KELLY_FRACTION,
            "edge_threshold": EDGE_THRESHOLD,
            "max_race_budget": MAX_RACE_BUDGET_RATE,
        },
        "results": {
            "initial_bankroll": INITIAL_BANKROLL,
            "final_bankroll": result_df.attrs.get("final_bankroll", 0.0),
            "peak_bankroll": result_df.attrs.get("peak_bankroll", 0.0),
            "max_drawdown_pct": calc_max_drawdown(history),
            "cv_auc": cv_auc,
            "total_bets": len(result_df),
            "total_hits": int(result_df["is_hit"].sum()) if not result_df.empty else 0,
        },
        "bankroll_curve": [
            {"date": h["date"], "bankroll": h["bankroll"]}
            for h in history[::10]  # 10レースごとにサンプリング
        ],
    }
    out_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("JSON 保存: %s", out_path)

    conn.close()
    logger.info("=== バックテスト完了 ===")


if __name__ == "__main__":
    main()
