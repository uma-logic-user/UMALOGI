"""過去モデル資産の静的解析 — EVアンサンブル適合性評価。

リポジトリ内に保存されている全過去モデル（ALPHA / ALPHA-Payout / cascade /
sandbox / v2系 / pre69feat世代）を、リーク修正済み OOS キャッシュ
（data/ablation_cache.pkl の test フレーム・400R・2025-10〜2026-06）上で評価する。

評価軸:
  1. AUC(is_win)            — 予測のランキング品質（回帰系もランキングとして評価）
  2. Spearman vs 現行honmei — 低いほど多様性が高くアンサンブル価値がある
  3. Spearman vs 市場確率   — 市場の複製でないか（高すぎると情報追加なし）
  4. 荒れレースAUC          — 勝者人気4番以下のレースに限定した AUC

Usage:
    py scripts/analyze_legacy_models.py
"""

from __future__ import annotations

import pickle
import sqlite3
import sys
import warnings
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DB_PATH = _ROOT / "data" / "umalogi.db"
_CACHE = _ROOT / "data" / "ablation_cache.pkl"
_MODELS = _ROOT / "data" / "models"


def _auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """sklearn 依存を避けた rank ベース AUC。"""
    order = np.argsort(scores)
    ranks = np.empty(len(scores), dtype=float)
    ranks[order] = np.arange(1, len(scores) + 1)
    pos = labels == 1
    n_pos, n_neg = int(pos.sum()), int((~pos).sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    ra = pd.Series(a).rank().to_numpy()
    rb = pd.Series(b).rank().to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def _predict_69col(model: Any, df: pd.DataFrame) -> np.ndarray:
    """FEATURE_COLS 系モデル（_IsotonicModel / LGBM sklearn / Booster ラッパ）の予測。"""
    base = getattr(model, "base", model)
    fl = getattr(base, "feature_name_", None)
    if fl is None:
        fl = base.booster_.feature_name()
    x = df.reindex(columns=list(fl)).fillna(0.0)
    if hasattr(model, "predict_proba"):
        return np.asarray(model.predict_proba(x)[:, 1], dtype=float)
    return np.asarray(model.predict(x), dtype=float)


def _sandbox_features(df: pd.DataFrame, date_map: dict[str, str]) -> pd.DataFrame:
    """sandbox モデルの 10 特徴量をオッズ・レース属性から導出する。"""
    out = pd.DataFrame(index=df.index)
    odds = pd.to_numeric(df["win_odds"], errors="coerce").fillna(50.0).clip(lower=1.01)
    out["nb_win_odds"] = odds
    out["nb_implied_prob"] = 1.0 / odds
    out["nb_log_odds"] = np.log(odds)
    out["venue_code"] = df["race_id"].astype(str).str[4:6].astype(int)
    out["surface_code"] = df.get("surface_code", 0)
    out["condition_code"] = df.get("condition_code", 0)
    out["distance"] = df.get("distance", 0)
    out["race_number"] = df.get("race_number", 0)
    out["month"] = (
        df["race_id"]
        .astype(str)
        .map(lambda r: int(str(date_map.get(r, "0000-01"))[5:7]))
    )
    out["race_n_horses"] = df.groupby("race_id")["horse_number"].transform("count")
    return out


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    warnings.filterwarnings("ignore")

    from src.ml.market_blend_calibration import blend_with_market

    with open(_CACHE, "rb") as f:
        _, test_df = pickle.load(f)
    test_df = test_df.reset_index(drop=True)
    labels = test_df["is_win"].to_numpy(dtype=int)
    odds_all = (
        pd.to_numeric(test_df["win_odds"], errors="coerce").fillna(50.0).to_numpy()
    )

    # キャッシュの market_prob / popularity は全NaN（ablation 生成時に未充填）。
    # 最終 win_odds から決定的に再構成する（人気 = レース内オッズ昇順位）。
    inv = 1.0 / np.clip(odds_all, 1.01, None)
    test_df["market_prob"] = (
        inv
        / test_df.assign(_inv=inv)
        .groupby("race_id")["_inv"]
        .transform("sum")
        .to_numpy()
    )
    test_df["popularity"] = (
        test_df.groupby("race_id")["win_odds"].rank(method="first").astype(int)
    )

    conn = sqlite3.connect(str(_DB_PATH))
    rids = list(test_df["race_id"].unique())
    date_map = dict(
        conn.execute(
            "SELECT race_id, date FROM races WHERE race_id IN ({})".format(
                ",".join("?" * len(rids))
            ),
            rids,
        ).fetchall()
    )
    conn.close()

    # ── 基準: 現行 honmei Booster（blend 前 raw） ────────────────────────────
    with open(_MODELS / "honmei_model.pkl", "rb") as f:
        honmei = pickle.load(f)
    from src.ml.models import FEATURE_COLS

    x69 = test_df.reindex(columns=FEATURE_COLS).fillna(0.0)
    honmei_raw = np.asarray(honmei._Booster.predict(x69.values), dtype=float)
    honmei_blend = np.array(
        [blend_with_market(float(p), float(o)) for p, o in zip(honmei_raw, odds_all)]
    )
    market = test_df["market_prob"].to_numpy(dtype=float)

    # ── 荒れレース判定（勝者人気 >= 4） ──────────────────────────────────────
    winner_pop = (
        test_df[test_df["is_win"] == 1].groupby("race_id")["popularity"].first()
    )
    rough_rids = set(winner_pop[winner_pop >= 4].index)
    rough_mask = test_df["race_id"].isin(rough_rids).to_numpy()

    # ── 候補モデルの予測関数を構築 ────────────────────────────────────────────
    def load(p: str) -> Any:
        with open(_MODELS / p, "rb") as f:
            return pickle.load(f)

    candidates: list[tuple[str, Callable[[], np.ndarray]]] = []

    for name, path in [
        ("manji(現役/EV回帰)", "manji_model.pkl"),
        ("place(現役/複勝)", "place_model.pkl"),
        ("honmei_v2(Iso較正)", "honmei_model_v2.pkl"),
        ("manji_v2(EV回帰)", "manji_model_v2.pkl"),
        ("place_v2(Iso較正)", "place_model_v2.pkl"),
        ("honmei_pre69(世代)", "history/honmei_model_pre69feat_20260521_073034.pkl"),
        ("manji_pre69(世代)", "history/manji_model_pre69feat_20260521_073034.pkl"),
    ]:
        candidates.append((name, lambda p=path: _predict_69col(load(p), test_df)))

    def _alpha_pred() -> np.ndarray:
        from src.ml.alpha_model import AlphaModel

        m = AlphaModel.load(_MODELS / "alpha" / "alpha_model.pkl")
        df = test_df.copy()
        df["trainer"] = ""  # cache に trainer 名なし → 未知扱い(-1)
        return m.predict_win_prob(df).to_numpy()

    candidates.append(("ALPHA(勝率/オッズ歪み)", _alpha_pred))

    def _alpha_payout_pred() -> np.ndarray:
        from src.ml.alpha_payout_model import AlphaPayoutModel

        m = AlphaPayoutModel.load(_MODELS / "alpha_payout" / "alpha_payout_model.pkl")
        df = test_df.copy()
        df["trainer"] = ""
        return m.predict_payout_ev(df).to_numpy()

    candidates.append(("ALPHA-Payout(複勝EV)", _alpha_payout_pred))

    sandbox = load("sandbox/sandbox_models.pkl")
    sb_x = _sandbox_features(test_df, date_map)
    for key in ("honmei", "place", "ev"):
        m = sandbox[key]

        def _sb(m_: Any = m) -> np.ndarray:
            if hasattr(m_, "predict_proba"):
                return np.asarray(m_.predict_proba(sb_x)[:, 1], dtype=float)
            return np.asarray(m_.predict(sb_x), dtype=float)

        candidates.append((f"sandbox_{key}", _sb))

    # ── 評価 ──────────────────────────────────────────────────────────────────
    print("=" * 100)
    print(
        f" 過去モデル静的解析 — OOS {test_df['race_id'].nunique()}R / "
        f"{len(test_df)}頭 / 荒れレース {len(rough_rids)}R（勝者人気4+）"
    )
    print("=" * 100)
    hdr = (
        f" {'モデル':<22} {'AUC':>7} {'荒れAUC':>8} "
        f"{'ρ(honmei)':>10} {'ρ(market)':>10}  判定"
    )
    print(hdr)
    print("-" * 100)

    base_rows = [
        ("honmei(現行raw)", honmei_raw),
        ("honmei(blend後)", honmei_blend),
        ("market_prob(市場)", market),
    ]
    results: list[dict[str, Any]] = []
    for name, scores in base_rows:
        auc = _auc(labels, scores)
        r_auc = _auc(labels[rough_mask], scores[rough_mask])
        print(
            f" {name:<22} {auc:>7.4f} {r_auc:>8.4f} "
            f"{_spearman(scores, honmei_raw):>10.3f} "
            f"{_spearman(scores, market):>10.3f}  (基準)"
        )

    print("-" * 100)
    for name, fn in candidates:
        try:
            scores = fn()
        except Exception as exc:
            print(f" {name:<22} 評価不能: {type(exc).__name__}: {str(exc)[:50]}")
            continue
        if len(scores) != len(test_df) or np.std(scores) < 1e-12:
            print(f" {name:<22} 評価不能: 出力退化（分散ゼロ or 長さ不一致）")
            continue
        auc = _auc(labels, scores)
        r_auc = _auc(labels[rough_mask], scores[rough_mask])
        rho_h = _spearman(scores, honmei_raw)
        rho_m = _spearman(scores, market)
        verdict = ""
        if auc >= 0.70 and rho_h < 0.85:
            verdict = "◎ 採用候補（高AUC×多様性）"
        elif auc >= 0.70:
            verdict = "○ 高AUCだが現行と重複気味"
        elif auc >= 0.60 and rho_h < 0.60:
            verdict = "△ 多様性はあるが精度低"
        else:
            verdict = "× 不採用"
        print(
            f" {name:<22} {auc:>7.4f} {r_auc:>8.4f} "
            f"{rho_h:>10.3f} {rho_m:>10.3f}  {verdict}"
        )
        results.append(
            dict(
                name=name, auc=auc, rough_auc=r_auc, rho_honmei=rho_h, rho_market=rho_m
            )
        )

    print("=" * 100)
    print(" 注: cascade(stage2/3) は stage1 モデル未保存のため入力特徴量")
    print("     (own_rank1_prob 等) が再現不能 → 構造的に評価対象外。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
