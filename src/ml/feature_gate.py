"""
src/ml/feature_gate.py — 新特徴量「本番統合ゲートキーパー」（W-097 / 検証プロトコル標準化）

教訓（2026-06-15 W-096）:
  単一・少数 cutoff の OOS は標本ノイズで容易に符号反転する（prev_trouble_proxy は
  ある cutoff で +5.4pp と出たが、標本を変えると -15.6pp に反転＝ノイズだった）。
  以後、**いかなる新特徴量も「複数 cutoff ウォークフォワード検証」を PASS しない限り
  本番統合を許可しない**。本モジュールはその判定を一元化する単一ゲートである。

ゲート判定（既定ポリシー）:
  - ΔROI が **過半数以上の cutoff で正**（既定 win_rate >= 2/3）
  - かつ **平均 ΔROI >= +min_mean_delta_roi pp**（既定 +2.0pp）
  - かつ **平均 ΔAUC の悪化が許容内**（既定 -0.002 まで）
  上記をすべて満たして初めて PASS（本番統合の検討可）。1 つでも欠ければ FAIL。

設計:
  - 学習器・評価器（ROI/AUC）は本モジュールに内包（scripts への依存を持たない）。
  - 特徴量の組立は呼び出し側が `assemble_fn` / `encoder_fn` で注入（任意の研究特徴量に対応）。
  - 本番 FEATURE_COLS / predictions / モデル pkl には一切触れない（評価専用）。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class GatePolicy:
    """ゲート合格基準。"""

    min_win_rate: float = 2.0 / 3.0  # ΔROI>0 の cutoff 割合の下限
    min_mean_delta_roi: float = 2.0  # 平均 ΔROI(pp) の下限
    max_mean_auc_drop: float = 0.002  # 平均 ΔAUC の許容悪化幅（正の値）
    ev_threshold: float = 1.0  # 単勝EVベット閾値


@dataclass
class GateResult:
    """ゲート判定結果。"""

    feature: str
    n_cutoffs: int
    win_rate: float
    mean_delta_roi: float
    median_delta_roi: float
    mean_delta_auc: float
    per_cutoff: list[dict] = field(default_factory=list)
    passed: bool = False
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "feature": self.feature,
            "n_cutoffs": self.n_cutoffs,
            "win_rate": round(self.win_rate, 3),
            "mean_delta_roi": round(self.mean_delta_roi, 3),
            "median_delta_roi": round(self.median_delta_roi, 3),
            "mean_delta_auc": round(self.mean_delta_auc, 5),
            "passed": self.passed,
            "reason": self.reason,
            "per_cutoff": self.per_cutoff,
        }


def _train_lgbm(train: pd.DataFrame, cols: Sequence[str]) -> Any:
    """LightGBM 分類器を学習（is_win ターゲット）。"""
    import lightgbm as lgb

    x = train[list(cols)].apply(pd.to_numeric, errors="coerce").astype(float)
    y = train["is_win"].astype(int)
    model = lgb.LGBMClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        verbose=-1,
    )
    model.fit(x, y)
    return model


def _predict(model: Any, test: pd.DataFrame, cols: Sequence[str]) -> np.ndarray:
    x = test[list(cols)].apply(pd.to_numeric, errors="coerce").astype(float)
    return model.predict_proba(x)[:, 1]


def evaluate_roi_auc(
    test: pd.DataFrame, prob: np.ndarray, ev_threshold: float
) -> dict:
    """単勝 EV>閾値 のフラットベット(100円)で ROI・的中率・AUC を算出。"""
    from sklearn.metrics import roc_auc_score

    df = test[["race_id", "is_win", "win_odds"]].copy()
    df["p"] = prob
    df["p_norm"] = df.groupby("race_id")["p"].transform(
        lambda s: s / s.sum() if s.sum() > 0 else s
    )
    df["ev"] = df["p_norm"] * df["win_odds"]
    bets = df[df["ev"] >= ev_threshold]
    n = len(bets)
    stake = n * 100
    payout = float((bets["is_win"] * bets["win_odds"] * 100).sum())
    roi = (payout / stake * 100) if stake else 0.0
    hit = (bets["is_win"].sum() / n * 100) if n else 0.0
    try:
        auc = float(roc_auc_score(df["is_win"], df["p"]))
    except Exception:
        auc = float("nan")
    return {"auc": auc, "n_bets": n, "roi": roi, "hit": hit}


def add_months(cutoff: str, months: int) -> str:
    """'YYYY-MM-01' に months 加算した月初を返す。"""
    y, m = int(cutoff[:4]), int(cutoff[5:7])
    total = y * 12 + (m - 1) + months
    return f"{total // 12:04d}-{total % 12 + 1:02d}-01"


def walk_forward_gate(
    *,
    conn: Any,
    feature_name: str,
    base_cols: Sequence[str],
    candidate_cols: Sequence[str],
    race_ids_fn: Callable[[Any, str, str, int], list[str]],
    assemble_fn: Callable[[Any, list[str], Any], pd.DataFrame],
    encoder_fn: Callable[[Any, str], Any],
    cutoffs: Sequence[str],
    train_lo: str = "2024-01-01",
    test_months: int = 2,
    train_cap: int = 1500,
    test_cap: int = 600,
    policy: GatePolicy | None = None,
    log: Callable[[str], None] = print,
) -> GateResult:
    """候補特徴量を base に足して、複数 cutoff の OOS で ΔROI/ΔAUC を測りゲート判定する。

    Args:
        conn: DB 接続。
        feature_name: 候補特徴量名（表示用）。
        base_cols: ベースライン列。
        candidate_cols: base に追加して評価する候補列（1 つ以上）。
        race_ids_fn: (conn, lo, hi, cap) -> race_id list。
        assemble_fn: (conn, race_ids, encoder) -> 特徴量 DataFrame（is_win/race_id/win_odds と
            base_cols・candidate_cols を含むこと）。
        encoder_fn: (conn, cutoff) -> 血統TE 等の cutoff 前 fit 済みエンコーダ。
        cutoffs: train/test 分割日のリスト（各 cutoff で test=[cutoff, cutoff+test_months)）。
        policy: 合格基準。
    """
    policy = policy or GatePolicy()
    base = list(base_cols)
    full = base + [c for c in candidate_cols if c not in base]
    rows: list[dict] = []

    for cutoff in cutoffs:
        test_hi = add_months(cutoff, test_months)
        enc = encoder_fn(conn, cutoff)
        tr_ids = race_ids_fn(conn, train_lo, cutoff, train_cap)
        te_ids = race_ids_fn(conn, cutoff, test_hi, test_cap)
        train = assemble_fn(conn, tr_ids, enc)
        test = assemble_fn(conn, te_ids, enc)
        if train.empty or test.empty:
            log(f"{cutoff}: データ不足スキップ")
            continue

        m_base = _train_lgbm(train, base)
        m_full = _train_lgbm(train, full)
        r_base = evaluate_roi_auc(test, _predict(m_base, test, base), policy.ev_threshold)
        r_full = evaluate_roi_auc(test, _predict(m_full, test, full), policy.ev_threshold)
        d = {
            "cutoff": cutoff,
            "test_races": len(te_ids),
            "roi_base": round(r_base["roi"], 2),
            "roi_full": round(r_full["roi"], 2),
            "d_roi": round(r_full["roi"] - r_base["roi"], 2),
            "auc_base": round(r_base["auc"], 4),
            "auc_full": round(r_full["auc"], 4),
            "d_auc": round(r_full["auc"] - r_base["auc"], 5),
        }
        rows.append(d)
        log(
            f"{cutoff}: ROI {r_base['roi']:6.1f}% -> {r_full['roi']:6.1f}% "
            f"(d={d['d_roi']:+5.1f}pp) | dAUC {d['d_auc']:+.4f} | races={len(te_ids)}"
        )

    return summarize_gate(rows, feature_name, policy)


def summarize_gate(
    rows: list[dict], feature_name: str, policy: GatePolicy | None = None
) -> GateResult:
    """cutoff 別結果（d_roi/d_auc を含む dict のリスト）からゲート判定を行う純関数。

    LightGBM を呼ばないため決定的・高速で、テスト可能。walk_forward_gate から委譲される。
    """
    policy = policy or GatePolicy()
    if not rows:
        return GateResult(
            feature=feature_name,
            n_cutoffs=0,
            win_rate=0.0,
            mean_delta_roi=0.0,
            median_delta_roi=0.0,
            mean_delta_auc=0.0,
            passed=False,
            reason="有効な cutoff が無い（データ不足）",
        )

    df = pd.DataFrame(rows)
    n = len(df)
    win_rate = float((df["d_roi"] > 0).mean())
    mean_roi = float(df["d_roi"].mean())
    median_roi = float(df["d_roi"].median())
    mean_auc = float(df["d_auc"].mean())

    checks = {
        "win_rate": win_rate >= policy.min_win_rate,
        "mean_roi": mean_roi >= policy.min_mean_delta_roi,
        "auc": mean_auc >= -policy.max_mean_auc_drop,
    }
    passed = all(checks.values())
    if passed:
        reason = (
            f"PASS: 勝率 {win_rate:.0%}>={policy.min_win_rate:.0%} ・ "
            f"平均ΔROI {mean_roi:+.1f}pp>=+{policy.min_mean_delta_roi:.1f} ・ "
            f"平均ΔAUC {mean_auc:+.4f}"
        )
    else:
        fails = [k for k, ok in checks.items() if not ok]
        reason = (
            f"FAIL({','.join(fails)}): 勝率 {win_rate:.0%} ・ 平均ΔROI {mean_roi:+.1f}pp ・ "
            f"平均ΔAUC {mean_auc:+.4f}"
        )

    return GateResult(
        feature=feature_name,
        n_cutoffs=n,
        win_rate=win_rate,
        mean_delta_roi=mean_roi,
        median_delta_roi=median_roi,
        mean_delta_auc=mean_auc,
        per_cutoff=rows,
        passed=passed,
        reason=reason,
    )
