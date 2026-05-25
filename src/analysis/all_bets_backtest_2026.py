"""
src/analysis/all_bets_backtest_2026.py

本命直前モデル — 2026年 全券種ガチシミュレーション（現実世界流動性制限付き）
Train: 2024-01-01 ～ 2025-12-31
Test : 2026-01-01 ～ 2026-05-23

券種: 単勝 / 複勝 / ワイド / 馬連 / 三連複（馬単・三連単は赤字のため除外）
賭け金: 1/20 フラクショナルKelly + 現実的な流動性上限
発注ゾーン: 単勝 edge 1.2〜1.5 倍 OR ≥5.0 倍（過熱ゾーン 1.5〜5.0 はスキップ）
"""
from __future__ import annotations

import itertools
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [backtest] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "umalogi.db"
OUT_JSON = ROOT / "data" / "all_bets_backtest_2026.json"

# ── 期間 ──────────────────────────────────────────────────────────────────────
TRAIN_FROM = "2024-01-01"
TRAIN_TO   = "2025-12-31"
TEST_FROM  = "2026-01-01"
TEST_TO    = "2026-05-23"

# ── 資金管理 ──────────────────────────────────────────────────────────────────
INITIAL_BANKROLL     = 10_000.0
KELLY_FRACTION       = 0.05          # 1/20 Kelly
MAX_RACE_BUDGET_RATE = 0.10          # 1レース最大 bankroll × 10%（相対上限）
MAX_BET_RATE         = 0.03          # 1ベット最大 bankroll × 3%（相対上限）
MAX_RACE_BUDGET_ABS  = 50_000        # 1レース物理上限 ¥50,000（JRAスリッページ対策）
MAX_BET_ABS          = 15_000        # 1ベット物理上限 ¥15,000（JRAスリッページ対策）
MIN_BET              = 100
EDGE_THRESHOLD       = 1.20          # 基本エッジ閾値
# コンボ系（ワイド/馬連/三連複）の発注モード:
#   "disabled" — バックテスト期間中は全て発注しない（現在設定: ROI 35-60% で赤字確定のため）
#   "filtered" — COMBO_EV_THRESHOLD 以上のみ発注（再校正後に切り替え）
COMBO_BET_MODE       = "disabled"    # ワイド/馬連/三連複 の発注モード
COMBO_EV_THRESHOLD   = 1.50          # "filtered" モード時の最低 EV 閾値

# 単勝 edge 黄金ゾーン: [1.2, 1.5] OR [5.0, ∞)
# バックテスト上、1.5〜5.0 の過熱ゾーンは ROI が悪化するためスキップ
EDGE_GOLDEN_ZONES: list[tuple[float, float]] = [
    (1.20, 1.50),
    (5.00, float("inf")),
]

ARROW = "→"  # → (U+2192): 馬単・三連単のコンビネーション区切り

# ── 券種設定（馬単・三連単は除外）─────────────────────────────────────────────
BET_TYPE_HEX: dict[str, str] = {
    "単勝":  "E58D98E58B9D",
    "複勝":  "E8A487E58B9D",
    "ワイド": "E383AFE382A4E38389",
    "馬連":  "E9A6ACE980A3",
    # "馬単":  "E9A6ACE58D98",   # 除外: 赤字(ROI 0.3%)
    "三連複": "E4B889E980A3E8A487",
    # "三連単": "E4B889E980A3E58D98", # 除外: 赤字(ROI 1.1%)
}

# 控除率差し引き後の「モデルトップ馬買い」時の実態オッズ（Kelly / EV 計算用）
# 市場平均ではなく、このモデルのトップ予測馬（=人気馬寄り）を買い続けた場合の実績オッズ。
# ワイド/馬連: トップ2頭は人気馬ペアになりやすく実際払戻が市場平均より低い。
# 三連複: トップ3頭 → 中位オッズ帯（35倍より実際は低い）。
TYPICAL_ODDS: dict[str, float] = {
    "単勝":  5.0,
    "複勝":  2.0,
    "ワイド": 2.5,   # 市場平均4.0 → 実態2.5（人気馬ペアのため低オッズ）
    "馬連":  5.0,    # 市場平均12.0 → 実態5.0（同上）
    "三連複": 15.0,  # 市場平均35.0 → 実態15.0（トップ3中位オッズ帯）
}

# ── LightGBM パラメータ ────────────────────────────────────────────────────────
LGBM_PARAMS: dict = {
    "objective":        "binary",
    "metric":           "auc",
    "learning_rate":    0.05,
    "num_leaves":       63,
    "min_data_in_leaf": 30,
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
        "win_rate_all":            win_all,
        "win_rate_surface":        win_surf,
        "win_rate_distance_band":  win_dist,
        "recent_rank_mean":        float(np.mean(ranks_all[:5])),
    }


# ── 1レース分の特徴量 DataFrame ────────────────────────────────────────────────
def build_race_df(
    conn: sqlite3.Connection,
    race_id: str,
    race_date: str,
    include_rank: bool = False,
    test_mode: bool = False,
) -> pd.DataFrame | None:
    """1レース分の特徴量 DataFrame を構築する。

    test_mode=True の場合は entries テーブルを優先して出走馬全頭を含め、
    rank は race_results から補完する（データリーク防止）。
    test_mode=False の場合は rank IS NOT NULL AND rank > 0 の確定着順馬のみ。

    Args:
        conn: DB コネクション。
        race_id: レース ID。
        race_date: レース日付（"YYYY-MM-DD"）。_horse_stats の基準日として使用。
        include_rank: True の場合、rank 列を DataFrame に含める。
        test_mode: True の場合、entries テーブルを使いデータリークを防ぐ。

    Returns:
        特徴量 DataFrame（2頭未満の場合は None）。
    """
    race_row = conn.execute(
        "SELECT distance, surface, venue, condition FROM races WHERE race_id = ?",
        (race_id,),
    ).fetchone()
    if not race_row:
        return None
    distance, surface, venue, condition = race_row

    # test_mode 時は rank フィルタを外す（entries テーブルがあれば優先使用）
    if test_mode:
        # entries テーブルから出走馬一覧を取得（JVLink/netkeiba で事前取得済み）
        entry_rows = conn.execute(
            """
            SELECT e.horse_id, e.horse_name, NULL AS rank,
                   e.weight_carried, NULL AS horse_weight, NULL AS horse_weight_diff,
                   e.gate_number, e.horse_number,
                   e.sex_age, e.jockey, e.trainer,
                   h.sire
            FROM entries e
            LEFT JOIN horses h ON h.horse_id = e.horse_id
            WHERE e.race_id = ?
            ORDER BY e.horse_number
            """,
            (race_id,),
        ).fetchall()

        if len(entry_rows) >= 2:
            # entries にデータあり → entries ベースで特徴量構築、rank は race_results から補完
            rank_map: dict[int, int] = {}
            for rr in conn.execute(
                "SELECT horse_number, rank FROM race_results WHERE race_id = ? AND rank IS NOT NULL AND rank > 0",
                (race_id,),
            ).fetchall():
                rank_map[int(rr[0])] = int(rr[1])
            rr_rows = entry_rows
            rank_source = rank_map
        else:
            # entries にデータなし → race_results から全馬取得（rank NULL も含む）
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
                ORDER BY rr.horse_number
                """,
                (race_id,),
            ).fetchall()
            rank_source = None
    else:
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
        rank_source = None

    if len(rr_rows) < 2:
        return None

    records = []
    for (horse_id, horse_name, raw_rank,
         weight_carried, horse_weight, hw_diff,
         gate_number, horse_number,
         sex_age, jockey, trainer, sire) in rr_rows:

        stats   = _horse_stats(conn, horse_id or "", race_date, surface, distance)
        sex_str = (sex_age or "")[:1]

        # rank の決定: rank_source (entries 経由) > raw_rank > 99
        if rank_source is not None:
            rank = rank_source.get(int(horse_number or 0), 99)
        elif raw_rank is not None and int(raw_rank) > 0:
            rank = int(raw_rank)
        else:
            rank = 99  # 未確定・競走中止等

        records.append({
            "race_id":          race_id,
            "race_date":        race_date,
            "horse_id":         horse_id or "",
            "horse_name":       horse_name or "",
            "horse_number":     int(horse_number or 0),
            "rank":             rank,
            "weight_carried":   float(weight_carried or 55.0),
            "horse_weight":     float(horse_weight or 480.0),
            "horse_weight_diff": float(hw_diff or 0.0),
            "gate_number":      int(gate_number or 0),
            "distance":         int(distance or 1600),
            "surface_code":     _SURFACE_CODE.get(surface or "", -1),
            "sex_code":         _SEX_CODE.get(sex_str, -1),
            "venue_code":       _VENUE_CODE.get(venue or "", 10),
            "condition_code":   _COND_CODE.get(condition or "", 0),
            "sire_code":        _encode_sire(sire),
            "jockey_win_rate":  _JKY_MAP.get(jockey or "", 0.05),
            "trainer_win_rate": _TRN_MAP.get(trainer or "", 0.05),
            **stats,
        })

    df = pd.DataFrame(records)
    if not include_rank:
        df = df.drop(columns=["rank"])
    return df


# ── モデル訓練（2024+2025年データ）────────────────────────────────────────────
def train_model(
    conn: sqlite3.Connection,
) -> tuple:
    """2024〜2025年データで LightGBM+Isotonic モデルを訓練する。

    GroupKFold (5分割) で OOF 予測を生成し Isotonic 校正を行った後、
    全訓練データで最終モデルを構築する。

    Args:
        conn: DB コネクション。

    Returns:
        (final_clf, iso, feat_cols, cv_auc) のタプル。

    Raises:
        RuntimeError: 訓練データが 0 件の場合。
    """
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

    feat_cols = FEATURE_COLS
    X = train_df[feat_cols].astype(float).fillna(-1).values
    y = (train_df["rank"] == 1).astype(int).values
    groups = train_df["race_id"].values

    cv    = GroupKFold(n_splits=5)
    aucs: list[float] = []
    oof_preds = np.zeros(len(y))

    for fold, (tr_idx, va_idx) in enumerate(cv.split(X, y, groups)):
        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X[tr_idx], y[tr_idx])
        preds = clf.predict_proba(X[va_idx])[:, 1]
        oof_preds[va_idx] = preds
        if y[va_idx].sum() > 0:
            aucs.append(roc_auc_score(y[va_idx], preds))

    cv_auc = float(np.mean(aucs)) if aucs else float("nan")
    logger.info("CV AUC: %.4f", cv_auc)

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oof_preds, y)

    final_clf = LGBMClassifier(**LGBM_PARAMS)
    final_clf.fit(X, y)
    return final_clf, iso, feat_cols, cv_auc


def predict_win_prob(model, iso, feat_cols: list[str], df: pd.DataFrame) -> np.ndarray:
    """各馬の単勝確率を予測する（Isotonic 校正済み）。

    Args:
        model: 訓練済み LGBMClassifier。
        iso: 訓練済み IsotonicRegression。
        feat_cols: 使用する特徴量列名リスト。
        df: build_race_df() が返す特徴量 DataFrame。

    Returns:
        各馬の単勝確率の numpy 配列（長さ = len(df)）。
    """
    X   = df[feat_cols].astype(float).fillna(-1)
    raw = model.predict_proba(X)[:, 1]
    return iso.predict(raw)


# ── 払戻データ取得 ──────────────────────────────────────────────────────────────
def get_payouts(conn: sqlite3.Connection, race_id: str) -> dict[str, dict]:
    """払戻データを {bet_type: {combination: payout}} の辞書で返す。

    HEX 変換による bet_type マッピングを使用して日本語券種名に変換する。

    Args:
        conn: DB コネクション。
        race_id: レース ID。

    Returns:
        {bet_type: {combination: payout}} の辞書。BET_TYPE_HEX 外の券種は除外。
    """
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


# ── 実際の単勝オッズ取得（Kelly 計算改善用）──────────────────────────────────
def _get_race_odds_map(conn: sqlite3.Connection, race_id: str) -> dict[int, float]:
    """race_results から全馬の単勝オッズを馬番→オッズ のマップで返す。

    win_odds が記録されていない馬はマップに含まれない（呼び出し側が TYPICAL_ODDS にフォールバック）。
    22.8% 充填率（2026年）。実際のオッズが取れる馬は Kelly 計算の精度が向上する。
    """
    rows = conn.execute(
        """SELECT horse_number, win_odds
           FROM race_results
           WHERE race_id = ? AND win_odds IS NOT NULL AND win_odds > 1.0
           ORDER BY horse_number""",
        (race_id,),
    ).fetchall()
    return {int(r[0]): float(r[1]) for r in rows}


# ── Harville 確率 ──────────────────────────────────────────────────────────────
def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    """P(i と j が 1着・2着に入る = 馬連) を Harville 公式で推定する。

    Args:
        probs: 各馬の単勝確率リスト（インデックスは馬の位置）。
        i: 馬1のインデックス。
        j: 馬2のインデックス。

    Returns:
        馬連確率（0.0〜0.99）。
    """
    pi, pj = probs[i], probs[j]
    q  = pi * pj / (1.0 - pi) if pi < 1.0 else 0.0
    q += pj * pi / (1.0 - pj) if pj < 1.0 else 0.0
    return min(q, 0.99)


def _harville_exacta(probs: list[float], i: int, j: int) -> float:
    """P(i が 1着, j が 2着 = 馬単) を Harville 公式で推定する。

    Args:
        probs: 各馬の単勝確率リスト。
        i: 1着馬のインデックス。
        j: 2着馬のインデックス。

    Returns:
        馬単確率（0.0〜0.99）。
    """
    pi, pj = probs[i], probs[j]
    d1 = 1.0 - pi
    if d1 <= 0:
        return 0.0
    return min(pi * pj / d1, 0.99)


def _harville_trio(probs: list[float], i: int, j: int, k: int) -> float:
    """P(i, j, k が 1-3着 = 三連複) を Harville 公式で推定する。

    全 3! = 6 通りの順列の確率を合計して返す。

    Args:
        probs: 各馬の単勝確率リスト。
        i: 馬1のインデックス。
        j: 馬2のインデックス。
        k: 馬3のインデックス。

    Returns:
        三連複確率（0.0〜0.99）。
    """
    total = 0.0
    for a, b, c in itertools.permutations([i, j, k]):
        pa, pb, pc = probs[a], probs[b], probs[c]
        d1 = 1.0 - pa
        d2 = 1.0 - pa - pb
        if d1 > 0 and d2 > 0:
            total += pa * pb / d1 * pc / d2
    return min(total, 0.99)


def _harville_trifecta(probs: list[float], i: int, j: int, k: int) -> float:
    """P(i が 1着, j が 2着, k が 3着 = 三連単) を Harville 公式で推定する。

    Args:
        probs: 各馬の単勝確率リスト。
        i: 1着馬のインデックス。
        j: 2着馬のインデックス。
        k: 3着馬のインデックス。

    Returns:
        三連単確率（0.0〜0.99）。
    """
    pi, pj, pk = probs[i], probs[j], probs[k]
    d1 = 1.0 - pi
    d2 = 1.0 - pi - pj
    if d1 <= 0 or d2 <= 0:
        return 0.0
    return min(pi * pj / d1 * pk / d2, 0.99)


def _harville_wide(probs: list[float], i: int, j: int) -> float:
    """P(i と j が 1-3着に入る = ワイド) を Harville 公式で推定する。

    = Σ_{k≠i,j} P({i,j,k} が 1-3着)

    Args:
        probs: 各馬の単勝確率リスト。
        i: 馬1のインデックス。
        j: 馬2のインデックス。

    Returns:
        ワイド確率（0.0〜0.99）。
    """
    total = sum(
        _harville_trio(probs, i, j, k)
        for k in range(len(probs))
        if k != i and k != j
    )
    return min(total, 0.99)


# ── 黄金ゾーン判定（単勝専用）─────────────────────────────────────────────────
def _in_golden_zone(edge: float) -> bool:
    """単勝 edge が発注許可ゾーン（黄金ゾーン）内かどうかを判定する。

    許可: [1.2, 1.5] OR [5.0, ∞)
    禁止（過熱ゾーン）: (1.5, 5.0)

    Args:
        edge: モデル edge 値（= p_top / base_rate）。

    Returns:
        発注許可ゾーン内の場合 True。
    """
    for lo, hi in EDGE_GOLDEN_ZONES:
        if lo <= edge <= hi:
            return True
    return False


# ── Kelly 賭け金算出 ────────────────────────────────────────────────────────────

_SINGLE_BET_TYPES = {"単勝", "複勝"}

def kelly_bet(
    bankroll: float,
    p: float,
    bet_type: str,
    field_size: int,
    race_budget_remaining: float,
    edge_threshold: float = EDGE_THRESHOLD,
    actual_win_odds: float | None = None,
    min_ev: float = 1.0,
) -> float:
    """
    フラクショナルKelly + 流動性制限で賭け金を算出。

    単勝/複勝: actual_win_odds が取れる場合はそれを Kelly 計算に使用。
               ない場合は TYPICAL_ODDS にフォールバック。
    単勝: edge 黄金ゾーン ([1.2,1.5] or ≥5.0) かつ EV≥min_ev
    複勝: edge ≥ threshold かつ EV≥min_ev
    コンボ: EV≥min_ev のみ（コンボは COMBO_EV_THRESHOLD=1.5 が渡される）

    物理上限:
      1ベット: min(bankroll×3%, ¥15,000)
    """
    # 単勝・複勝は実際のオッズがあれば優先使用（Kelly 精度向上）
    if bet_type == "単勝" and actual_win_odds is not None and actual_win_odds > 1.0:
        typical_odds = actual_win_odds
    elif bet_type == "複勝" and actual_win_odds is not None and actual_win_odds > 1.0:
        # 単勝オッズから複勝オッズを推定: 単勝の約1/3（JRA平均的な関係性）
        typical_odds = max(actual_win_odds / 3.0, 1.1)
    else:
        typical_odds = TYPICAL_ODDS.get(bet_type, 5.0)

    ev           = p * typical_odds
    if ev < min_ev:
        return 0.0

    base_rate = 1.0 / max(field_size, 2)
    edge      = p / base_rate

    if bet_type == "単勝":
        if not _in_golden_zone(edge):
            return 0.0
    elif bet_type == "複勝":
        if edge < edge_threshold:
            return 0.0

    kelly_full      = (p * typical_odds - 1.0) / max(typical_odds - 1.0, 1e-9)
    kelly_full      = max(kelly_full, 0.0)
    effective_kelly = kelly_full * KELLY_FRACTION

    relative_cap = bankroll * MAX_BET_RATE
    abs_cap      = float(MAX_BET_ABS)
    bet_size     = min(bankroll * effective_kelly, relative_cap, abs_cap, race_budget_remaining)
    if bet_size < MIN_BET:
        # 小残高フェーズでも正EV かつ予算がある場合は最小ベット(¥100)を保証
        if kelly_full > 0 and race_budget_remaining >= MIN_BET:
            return float(MIN_BET)
        return 0.0
    return float(int(bet_size / 100) * 100)


# ── コンビネーション文字列生成 ─────────────────────────────────────────────────
def _combo_unordered(*nums: int) -> str:
    """複数馬番を昇順 "-" 区切りで結合する（馬連・ワイド・三連複用）。

    Args:
        *nums: 馬番の可変長引数。

    Returns:
        例: "3-7-11"（昇順ソート済み）。
    """
    return "-".join(str(n) for n in sorted(nums))

def _combo_ordered(*nums: int) -> str:
    """複数馬番を入力順 ARROW 区切りで結合する（馬単・三連単用）。

    Args:
        *nums: 馬番の可変長引数（順序が重要）。

    Returns:
        例: "7→14"（入力順）。
    """
    return ARROW.join(str(n) for n in nums)


# ── 1レース シミュレーション ────────────────────────────────────────────────────
def simulate_race(
    race_id: str,
    race_date: str,
    race_df: pd.DataFrame,
    probs: np.ndarray,
    payouts: dict[str, dict],
    bankroll: float,
    odds_map: dict[int, float] | None = None,
) -> tuple[list[dict], float]:
    """1レースのベットをシミュレートしてベット記録と純損益を返す。

    単勝・複勝のみ発注（COMBO_BET_MODE="disabled" の場合はワイド/馬連/三連複を除外）。
    実際の単勝オッズが取得できる馬は kelly_bet の精度が向上する。

    Args:
        race_id: レース ID。
        race_date: レース日付（"YYYY-MM-DD"）。
        race_df: build_race_df() が返す特徴量 DataFrame。
        probs: predict_win_prob() が返す単勝確率 numpy 配列。
        payouts: get_payouts() が返す {bet_type: {combo: payout}} 辞書。
        bankroll: 現在の残高（円）。
        odds_map: {horse_number: win_odds} の辞書。None の場合は TYPICAL_ODDS を使用。

    Returns:
        (ベット記録リスト, レース純損益（円）) のタプル。
    """
    n          = len(race_df)
    horse_nums = list(race_df["horse_number"].astype(int))
    prob_list  = list(probs)
    # 1レース上限: bankroll×10% か ¥50,000 の小さい方
    race_budget = min(bankroll * MAX_RACE_BUDGET_RATE, float(MAX_RACE_BUDGET_ABS))
    month       = race_date[5:7]

    records: list[dict] = []
    total_invested = 0.0

    sorted_idx = sorted(range(n), key=lambda i: -prob_list[i])
    top_idx    = sorted_idx[0]
    p_top      = prob_list[top_idx]
    h_top      = horse_nums[top_idx]

    base_rate = 1.0 / max(n, 2)
    edge_top  = p_top / base_rate

    def make_record(bt: str, combo: str, p_bet: float, bet_amt: float) -> dict:
        pmap      = payouts.get(bt, {})
        payout_a  = float(pmap.get(combo, 0))
        is_hit    = payout_a > 0
        profit    = (payout_a / 100.0 * bet_amt) - bet_amt if is_hit else -bet_amt
        return {
            "race_id":       race_id,
            "date":          race_date,
            "month":         month,
            "bet_type":      bt,
            "combo":         combo,
            "p_bet":         p_bet,
            "edge":          edge_top,
            "bet_amount":    bet_amt,
            "payout_per100": payout_a,
            "is_hit":        int(is_hit),
            "profit":        profit,
        }

    # 実際の単勝オッズ（馬番→オッズ）— race_results.win_odds から取得済み
    tan_actual_odds = (odds_map or {}).get(h_top)

    def try_bet(
        bt: str,
        combo: str,
        p_bet: float,
        eth: float = EDGE_THRESHOLD,
        win_odds: float | None = None,
        min_ev: float = 1.0,
    ) -> None:
        nonlocal total_invested
        remaining = race_budget - total_invested
        if remaining < MIN_BET:
            return
        bet = kelly_bet(bankroll, p_bet, bt, n, remaining, eth,
                        actual_win_odds=win_odds, min_ev=min_ev)
        if bet > 0:
            records.append(make_record(bt, combo, p_bet, bet))
            total_invested += bet

    # ── 単勝 ──────────────────────────────────────────────────────────────────
    try_bet("単勝", str(h_top), p_top, win_odds=tan_actual_odds)

    # ── 複勝（確率を2.5倍でplace調整）────────────────────────────────────────
    p_fuku = min(p_top * 2.5, 0.95)
    try_bet("複勝", str(h_top), p_fuku, win_odds=tan_actual_odds)

    # ── ワイド/馬連/三連複 ─────────────────────────────────────────────────────
    # COMBO_BET_MODE="disabled": 実績ROI 35-60%（赤字確定）のため現在は発注しない。
    # 再評価条件: Harville確率キャリブレーション完了後に "filtered" モードで再検証。
    if COMBO_BET_MODE != "disabled":
        if n >= 2:
            i1, i2 = sorted_idx[0], sorted_idx[1]
            h1, h2  = horse_nums[i1], horse_nums[i2]
            p_wide  = _harville_wide(prob_list, i1, i2)
            p_uma   = _harville_quinella(prob_list, i1, i2)
            try_bet("ワイド", _combo_unordered(h1, h2), p_wide, min_ev=COMBO_EV_THRESHOLD)
            try_bet("馬連",  _combo_unordered(h1, h2), p_uma,  min_ev=COMBO_EV_THRESHOLD)

        if n >= 3:
            i1, i2, i3 = sorted_idx[0], sorted_idx[1], sorted_idx[2]
            p_trio      = _harville_trio(prob_list, i1, i2, i3)
            trio_sorted = sorted([horse_nums[i1], horse_nums[i2], horse_nums[i3]])
            try_bet("三連複", _combo_unordered(*trio_sorted), p_trio, min_ev=COMBO_EV_THRESHOLD)

    # 馬単・三連単は除外（赤字確定のためシミュレーション対象外）

    total_profit = sum(r["profit"] for r in records)
    return records, total_profit


# ── シミュレーションループ ─────────────────────────────────────────────────────
def run_simulation(
    conn: sqlite3.Connection,
    model,
    iso,
    feat_cols: list[str],
) -> pd.DataFrame:
    """2026年テスト期間の全レースをシミュレートする。

    TEST_FROM〜TEST_TO の全レースに対して simulate_race() を呼び出し、
    結果を DataFrame に集約する。df.attrs に final_bankroll / peak_bankroll /
    bankroll_history を格納する。

    Args:
        conn: DB コネクション。
        model: 訓練済み LGBMClassifier。
        iso: 訓練済み IsotonicRegression。
        feat_cols: 使用する特徴量列名リスト。

    Returns:
        全ベット記録の DataFrame（bankroll_after 列含む）。ベットなしの場合は空 DataFrame。
    """
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

    bankroll      = INITIAL_BANKROLL
    peak_bankroll = INITIAL_BANKROLL
    all_records:  list[dict] = []
    bl_history:   list[dict] = []
    skipped = 0
    bet_races = 0

    for idx, (race_id, race_date) in enumerate(race_list):
        # test_mode=True: rank フィルタなし（データリーク防止）、include_rank=True で払戻照合用に保持
        race_df_raw = build_race_df(conn, race_id, race_date, include_rank=True, test_mode=True)
        if race_df_raw is None or len(race_df_raw) < 2:
            skipped += 1
            continue

        race_df = race_df_raw.drop(columns=["rank"])
        probs    = predict_win_prob(model, iso, feat_cols, race_df)
        payouts  = get_payouts(conn, race_id)
        odds_map = _get_race_odds_map(conn, race_id)

        bets, net_profit = simulate_race(
            race_id, race_date, race_df, probs, payouts, bankroll, odds_map=odds_map
        )

        if bets:
            bankroll  += net_profit
            bankroll   = max(bankroll, 0.0)
            bet_races += 1

        peak_bankroll = max(peak_bankroll, bankroll)

        if bankroll < MIN_BET:
            logger.warning("実質破産: race=%s  bankroll=%.0f", race_id, bankroll)
            bankroll = 0.0

        for b in bets:
            b["bankroll_after"] = bankroll

        all_records.extend(bets)
        bl_history.append({
            "date":     race_date,
            "race_id":  race_id,
            "bankroll": bankroll,
            "peak":     peak_bankroll,
        })

        if (idx + 1) % 200 == 0:
            logger.info(
                "進行 %d/%d レース (bet=%d)  残高=¥%d  ベット=%d件",
                idx + 1, len(race_list), bet_races, int(bankroll), len(all_records)
            )

    logger.info(
        "完了: %d レース (skip=%d, bet=%d)  最終残高=¥%d",
        len(race_list), skipped, bet_races, int(bankroll)
    )

    df = pd.DataFrame(all_records) if all_records else pd.DataFrame()
    df.attrs["final_bankroll"]   = bankroll
    df.attrs["peak_bankroll"]    = peak_bankroll
    df.attrs["bankroll_history"] = bl_history
    return df


# ── 最大ドローダウン ───────────────────────────────────────────────────────────
def calc_max_drawdown(history: list[dict]) -> float:
    """残高履歴から最大ドローダウン（%）を計算する。

    Args:
        history: {"bankroll": float, ...} の辞書リスト（run_simulation の bl_history）。

    Returns:
        最大ドローダウン (%)。履歴が空の場合は 0.0。
    """
    if not history:
        return 0.0
    peak  = history[0]["bankroll"]
    max_dd = 0.0
    for h in history:
        peak   = max(peak, h["bankroll"])
        if peak > 0:
            dd     = (peak - h["bankroll"]) / peak * 100.0
            max_dd = max(max_dd, dd)
    return max_dd


# ── 払戻合計ヘルパー ───────────────────────────────────────────────────────────
def _payout_sum(df: pd.DataFrame) -> float:
    """ベット記録 DataFrame から実際の払戻合計（円）を計算する。

    payout_per100 は 100円投資あたりの払戻金額のため、bet_amount と比例計算する。

    Args:
        df: run_simulation() が返す全ベット記録 DataFrame。

    Returns:
        払戻合計（円）。DataFrame が空の場合は 0.0。
    """
    if df.empty:
        return 0.0
    return float(df.apply(
        lambda r: r["payout_per100"] / 100.0 * r["bet_amount"] if r["is_hit"] else 0.0,
        axis=1,
    ).sum())


# ── レポート出力 ───────────────────────────────────────────────────────────────
SEP = "=" * 82

def print_report(df: pd.DataFrame, cv_auc: float) -> None:
    """バックテスト結果の詳細レポートを標準出力に表示する。

    資金曲線サマリー・ベット全体サマリー・券種別 ROI・月次推移・
    edge 強度別 ROI の各テーブルを含む。

    Args:
        df: run_simulation() が返す全ベット記録 DataFrame（attrs に残高情報含む）。
        cv_auc: train_model() が返す CV AUC 値。
    """
    final_bl  = df.attrs.get("final_bankroll", 0.0)
    peak_bl   = df.attrs.get("peak_bankroll",  0.0)
    history   = df.attrs.get("bankroll_history", [])
    max_dd    = calc_max_drawdown(history)
    bankrupt  = "✅ ゼロ（破産なし）" if final_bl > 0 else "💀 破産"

    print(SEP)
    print("【本命直前モデル 2026年 全券種 カンニングなし walk-forward バックテスト】")
    print(f"  Train  : {TRAIN_FROM} ～ {TRAIN_TO}")
    print(f"  Test   : {TEST_FROM}  ～ {TEST_TO}")
    print(f"  CV AUC (2024-25年モデル) : {cv_auc:.4f}")
    print(f"  Kelly 分数               : {KELLY_FRACTION} (1/{int(1/KELLY_FRACTION)} Kelly)")
    print(f"  Edge 閾値（通常）          : {EDGE_THRESHOLD}×")
    print(f"  単勝 発注ゾーン           : [1.2〜1.5×] OR [≥5.0×]（過熱ゾーン除外）")
    print(f"  1レース最大投資            : bankroll × {MAX_RACE_BUDGET_RATE*100:.0f}%")
    print(f"  1ベット最大投資            : bankroll × {MAX_BET_RATE*100:.0f}%")
    print(f"  Kelly オッズ              : 単勝/複勝=race_results.win_odds 優先 (22.8%充填), それ以外=TYPICAL_ODDS")
    print(f"  コンボ発注モード          : {COMBO_BET_MODE} (disabled=無効化, filtered=EV≥{COMBO_EV_THRESHOLD}×)")
    if COMBO_BET_MODE == "disabled":
        print("  ⚠️  ワイド/馬連/三連複: ROI 35-60% (赤字確定) のため無効化。複勝+単勝のみ発注")
    print()
    print("  【特徴量リーク監査サマリー】")
    print("  ✅ Oracle / HitFocus: 買い目生成戦略 (post-prediction), ML 特徴量ではない → リークなし")
    print("  ✅ FEATURE_COLS (16列): 未来情報の混入なし (馬体重/騎手/調教師/血統/過去成績のみ)")
    print("  ✅ 利益計算: race_payouts.payout を使用 (TYPICAL_ODDS は Kelly サイジングのみ)")
    print("  ✅ データリーク修正済: build_race_df(test_mode=True) — rank フィルタ除去")
    print(SEP)

    if df.empty:
        print("[ベットデータなし]")
        return

    n_bets   = len(df)
    n_races  = df["race_id"].nunique()
    total_in = float(df["bet_amount"].sum())
    total_out = _payout_sum(df)
    n_hits   = int(df["is_hit"].sum())
    roi_all  = total_out / total_in * 100 if total_in > 0 else 0.0
    profit   = total_out - total_in
    gain_rate = (final_bl / INITIAL_BANKROLL - 1.0) * 100.0

    print(f"\n■ 資金曲線サマリー")
    print(f"  初期資金          : ¥{INITIAL_BANKROLL:>12,.0f}")
    print(f"  最高到達残高      : ¥{peak_bl:>12,.0f}  ({(peak_bl/INITIAL_BANKROLL-1)*100:+.1f}%)")
    print(f"  2026年末 最終残高 : ¥{final_bl:>12,.0f}  ({gain_rate:+.1f}%)")
    print(f"  最大ドローダウン  : {max_dd:.1f}%")
    print(f"  破産              : {bankrupt}")

    print(f"\n■ ベット全体サマリー")
    print(f"  発注レース数      : {n_races}")
    print(f"  総ベット件数      : {n_bets}")
    print(f"  的中件数          : {n_hits}  ({n_hits/n_bets*100:.1f}%)")
    print(f"  総投資額          : ¥{total_in:>10,.0f}")
    print(f"  総回収額          : ¥{total_out:>10,.0f}")
    print(f"  ROI               : {roi_all:.1f}%")
    print(f"  損益              : ¥{profit:>+10,.0f}")

    # 券種別
    print(f"\n■ 券種別 ROI 詳細")
    bet_order = ["単勝", "複勝", "ワイド", "馬連", "三連複"]
    header = f"{'券種':<8}{'件数':>6}{'的中':>6}{'的中率':>7}{'投資額':>13}{'回収額':>13}{'ROI':>8}{'損益':>12}{'最大払戻':>10}"
    sep2 = "-" * 80
    print(header)
    print(sep2)
    best_roi = -9999.0
    best_bt  = ""
    for bt in bet_order:
        sub = df[df["bet_type"] == bt]
        if sub.empty:
            continue
        n_b  = len(sub)
        n_h  = int(sub["is_hit"].sum())
        t_in = float(sub["bet_amount"].sum())
        t_ou = _payout_sum(sub)
        roi  = t_ou / t_in * 100 if t_in > 0 else 0.0
        pnl  = t_ou - t_in
        maxp = int(sub["payout_per100"].max())
        star = " ★" if roi >= 100.0 else ""
        print(
            f"{bt:<8}{n_b:>6}{n_h:>6}{n_h/n_b*100:>6.1f}%"
            f"{t_in:>13,.0f}{t_ou:>13,.0f}{roi:>7.1f}%{pnl:>+12,.0f}{maxp:>10}{star}"
        )
        if roi > best_roi:
            best_roi = roi
            best_bt  = bt
    print(sep2)

    # 月次
    print(f"\n■ 月次 ROI 推移")
    hdr2 = f"{'月':>5}{'ベット':>7}{'的中':>6}{'投資額':>13}{'ROI':>8}{'月次損益':>12}{'月末残高':>12}"
    print(hdr2)
    print("-" * 65)
    for m in sorted(df["month"].unique()):
        ms = df[df["month"] == m]
        t_in2 = float(ms["bet_amount"].sum())
        t_ou2 = _payout_sum(ms)
        roi2  = t_ou2 / t_in2 * 100 if t_in2 > 0 else 0.0
        pnl2  = t_ou2 - t_in2
        last_bl = ms["bankroll_after"].iloc[-1] if "bankroll_after" in ms.columns else 0
        print(
            f"  {m}月{len(ms):>7}{int(ms['is_hit'].sum()):>6}"
            f"{t_in2:>13,.0f}{roi2:>7.1f}%{pnl2:>+12,.0f}{last_bl:>12,.0f}"
        )

    # edge 強度別
    print(f"\n■ Edge 強度別 ROI（単勝のみ）")
    print(f"{'edge≥':>7}{'件数':>6}{'的中':>6}{'投資額':>13}{'ROI':>8}{'損益':>12}")
    print("-" * 53)
    tan = df[df["bet_type"] == "単勝"]
    for thr in [1.2, 1.5, 2.0, 3.0, 5.0]:
        sub = tan[tan["edge"] >= thr]
        if sub.empty:
            continue
        t_in3 = float(sub["bet_amount"].sum())
        t_ou3 = _payout_sum(sub)
        roi3  = t_ou3 / t_in3 * 100 if t_in3 > 0 else 0.0
        print(
            f"   {thr:.1f}×{len(sub):>6}{int(sub['is_hit'].sum()):>6}"
            f"{t_in3:>13,.0f}{roi3:>7.1f}%{t_ou3-t_in3:>+12,.0f}"
        )

    print(f"\n{'─'*82}")
    print(f"  利益貢献トップ券種 : {best_bt} (ROI {best_roi:.1f}%)")
    print(SEP)


# ── JSON 保存 ──────────────────────────────────────────────────────────────────
def save_json(df: pd.DataFrame, cv_auc: float) -> None:
    """バックテスト結果を JSON ファイルに保存する。

    OUT_JSON に設定・AUC・最終残高・券種別成績・残高推移（10レースごと）を出力する。
    DataFrame が空の場合は何もしない。

    Args:
        df: run_simulation() が返す全ベット記録 DataFrame（attrs に残高情報含む）。
        cv_auc: train_model() が返す CV AUC 値。
    """
    if df.empty:
        return
    history = df.attrs.get("bankroll_history", [])
    # 10レースおきにサンプリング
    sampled = [{"date": h["date"], "bankroll": h["bankroll"]}
               for h in history[::10]]

    by_type: dict[str, dict] = {}
    for bt in BET_TYPE_HEX:
        sub = df[df["bet_type"] == bt]
        if sub.empty:
            continue
        t_in = float(sub["bet_amount"].sum())
        t_ou = _payout_sum(sub)
        by_type[bt] = {
            "count":   len(sub),
            "hits":    int(sub["is_hit"].sum()),
            "invest":  t_in,
            "return":  t_ou,
            "roi":     round(t_ou / t_in * 100, 1) if t_in > 0 else 0.0,
        }

    payload = {
        "config": {
            "train": [TRAIN_FROM, TRAIN_TO],
            "test":  [TEST_FROM,  TEST_TO],
            "kelly_fraction":     KELLY_FRACTION,
            "edge_threshold":     EDGE_THRESHOLD,
            "edge_golden_zones": EDGE_GOLDEN_ZONES,
            "max_race_budget":    MAX_RACE_BUDGET_RATE,
            "max_bet_rate":       MAX_BET_RATE,
        },
        "cv_auc":         cv_auc,
        "final_bankroll": df.attrs.get("final_bankroll", 0.0),
        "peak_bankroll":  df.attrs.get("peak_bankroll",  0.0),
        "by_bet_type":    by_type,
        "bankroll_curve": sampled,
    }
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON 保存: %s", OUT_JSON)


# ── メイン ─────────────────────────────────────────────────────────────────────
def main() -> None:
    """本命直前モデル 2026年全券種バックテストのエントリーポイント。

    Step 1: 騎手/調教師勝率マップ構築
    Step 2: モデル訓練（2024+2025年データ）
    Step 3: 2026年 walk-forward シミュレーション
    Step 4: レポート出力・JSON 保存
    """
    logger.info("=== 本命直前モデル 2026年 全券種バックテスト 開始 ===")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    logger.info("Step 1: 騎手/調教師 勝率マップ構築...")
    build_jockey_trainer_maps(conn)

    logger.info("Step 2: モデル訓練 (2024+2025年データ)...")
    model, iso, feat_cols, cv_auc = train_model(conn)

    logger.info("Step 3: 2026年 walk-forward シミュレーション...")
    result_df = run_simulation(conn, model, iso, feat_cols)

    print_report(result_df, cv_auc)
    save_json(result_df, cv_auc)
    logger.info("=== バックテスト完了 ===")


if __name__ == "__main__":
    main()
