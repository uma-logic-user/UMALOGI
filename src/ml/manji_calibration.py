"""
卍モデル confidence キャリブレーション（W-048 根本修正・P0-3）

【背景】
  卍の買い目 confidence は ``min(raw_prob * 係数, 1.0)`` で算出されており、
  ev_score の膨張や係数(×5〜×30)で **ほぼ常に 1.0 に飽和** していた。
  confidence=1.0 は「必勝」を意味し Kelly が全額投資 → 実現 ROI 26.9% に崩壊
  （DISABLE_MANJI_BETS=1 で停止中）。確定実績分析では卍の馬選択自体は勝っている
  （単勝 ROI 1000%超）ため、正解は「EV/confidence を信頼回復させ単複で復帰」。

【方針】
  - 単勝: 卍 ev_score → 実勝率 P(win) を **Isotonic Regression** で較正（確定実績で学習）。
  - 馬連/ワイド/馬単/三連複: Harville 由来の生確率をそのまま（係数で膨張させない）。
  - 学習済み較正器が無い場合も、**係数膨張を排した保守フォールバック** で 1.0 飽和を防ぐ。

  学習は WF バックテストの幻想 ROI ではなく **確定実績(race_results)** に基づく
  （recompute した ev_score と実着順を突合）。
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from pathlib import Path
from typing import Any

from src.ml.market_blend_calibration import blend_with_market

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models"
_WIN_CAL_PATH = _MODEL_DIR / "manji_win_calibrator.pkl"
# 複勝特化 Platt 較正器(2026-06-02): 卍 ev_score → P(複勝圏=3着内) を独立に較正する。
# 単勝(Isotonic)とは別インスタンス。卍を複勝のみ実弾へ昇格するため真の複勝EV算出に用いる。
_PLACE_CAL_PATH = _MODEL_DIR / "manji_place_calibrator.pkl"
# 複勝フォールバック上限（学習済み較正器が無くても暴騰を防ぐ）。複勝圏は base_rate が高い。
_FALLBACK_PLACE_CAP = 0.85

# フォールバック時の confidence 上限（学習済み較正器が無くても 1.0 飽和を防ぐ）
_FALLBACK_WIN_CAP = 0.6
# combo 生確率の上限（確率の性質を保持）
_COMBO_CAP = 0.99

# W-066 後継: EV_SANITY_CAP(=2.0) を廃止し market_blend_calibration.blend_with_market に移行。
# 旧 EV_SANITY_CAP は「EV を 2.0 に揃えてゲートを素通り」させる逆効果があった。
# 新ロジックは大穴ほど市場確率(0.80/odds)へ収縮させ、EV が 2.0 で飽和しない。


# 学習済み較正器のメモリキャッシュ（None=未ロード, False=ファイルなし）
# 型: IsotonicRegression インスタンス（サードパーティ・stub なし）のため Any 扱い。
_win_cal_cache: Any = None


def _load_win_cal() -> Any:
    """学習済み単勝較正器（IsotonicRegression）をロードする。失敗時は None。"""
    global _win_cal_cache
    if _win_cal_cache is not None:
        return _win_cal_cache if _win_cal_cache is not False else None
    try:
        with open(_WIN_CAL_PATH, "rb") as f:
            _win_cal_cache = pickle.load(f)
        logger.info("卍単勝較正器ロード: %s", _WIN_CAL_PATH.name)
        return _win_cal_cache
    except Exception:
        _win_cal_cache = False  # type: ignore[assignment]
        return None


def calibrate_win_prob(ev_score: float, odds: float) -> float:
    """卍 ev_score を較正済み P(win) に変換する（単勝 confidence / Kelly 用）。

    学習済み Isotonic 較正器があればそれを適用。無ければ係数膨張を排した
    保守フォールバック（implied prob を _FALLBACK_WIN_CAP で頭打ち）。

    Args:
        ev_score: ManjiModel.ev_score() の生出力（EV 比率）。
        odds: 当該馬の単勝オッズ（フォールバックの implied prob 計算に使用）。

    Returns:
        較正済み勝率（0.0〜1.0、飽和しない）。
    """
    cal = _load_win_cal()
    if cal is not None:
        try:
            p_iso = float(cal.predict([float(ev_score)])[0])
            p_clipped = min(max(p_iso, 0.0), 0.999)
            # 旧: _apply_ev_sanity_cap(P, odds) → EV を 2.0 に揃えるだけで大穴が素通り
            # 新: blend_with_market(P, odds) → 大穴ほど市場確率へ収縮（EV 暴騰を根絶）
            return blend_with_market(p_clipped, odds)
        except Exception as exc:  # noqa: BLE001
            logger.debug("卍較正器 predict 失敗（フォールバック）: %s", exc)

    # フォールバック: implied prob = EV / odds を上限 _FALLBACK_WIN_CAP で頭打ち
    o = max(float(odds), 1.0)
    implied = float(ev_score) / o if o > 1.0 else float(ev_score) / 10.0
    p = min(max(implied, 0.0), _FALLBACK_WIN_CAP)
    # フォールバック経路にも blend_with_market を適用（一貫性）。
    return blend_with_market(p, odds)


# ── 複勝特化 Platt 較正器（2026-06-02）─────────────────────────────────────────
# 単勝(Isotonic)とは独立したインスタンス。卍 ev_score → P(複勝圏=3着内) を
# Platt Scaling(ロジスティック回帰)で較正する。
_place_cal_cache: Any = None


def _load_place_cal() -> Any:
    """学習済み複勝較正器（Platt = LogisticRegression）をロードする。失敗時は None。"""
    global _place_cal_cache
    if _place_cal_cache is not None:
        return _place_cal_cache if _place_cal_cache is not False else None
    try:
        with open(_PLACE_CAL_PATH, "rb") as f:
            _place_cal_cache = pickle.load(f)
        logger.info("卍複勝較正器ロード: %s", _PLACE_CAL_PATH.name)
        return _place_cal_cache
    except Exception:
        _place_cal_cache = False  # type: ignore[assignment]
        return None


def calibrate_place_prob(ev_score: float) -> float:
    """卍 ev_score を較正済み P(複勝圏=3着内) に変換する（複勝 confidence / EV 用）。

    学習済み Platt(ロジスティック)較正器があれば適用。無ければ ev_score の
    単調変換による保守フォールバック（_FALLBACK_PLACE_CAP で頭打ち）。

    Args:
        ev_score: ManjiModel.ev_score() の生出力（EV 比率）。

    Returns:
        較正済み複勝圏確率（0.0〜1.0、飽和しない）。
    """
    cal = _load_place_cal()
    if cal is not None:
        try:
            # Platt は predict_proba[:,1] が P(複勝圏)。
            p = float(cal.predict_proba([[float(ev_score)]])[0][1])
            return min(max(p, 0.0), 0.999)
        except Exception as exc:  # noqa: BLE001
            logger.debug("卍複勝較正器 predict 失敗（フォールバック）: %s", exc)

    # フォールバック: ev_score を 0〜_FALLBACK_PLACE_CAP に潰した単調変換。
    ev = max(float(ev_score), 0.0)
    implied = ev / (ev + 1.5)  # ev=1.5→0.5, ev=3→0.67 と緩やかに飽和
    return min(max(implied, 0.0), _FALLBACK_PLACE_CAP)


def calibrate_combo_prob(raw_prob: float) -> float:
    """馬連/ワイド/馬単/三連複の Harville 生確率を confidence にする（膨張なし）。

    従来の ``min(raw * 係数, 1.0)`` の係数膨張を排し、生確率をそのまま
    [0, _COMBO_CAP] にクランプする（小さな現実的 confidence → Kelly 保守化）。

    Args:
        raw_prob: Harville 由来の生確率（組み合わせ確率）。

    Returns:
        confidence（0.0〜_COMBO_CAP）。
    """
    return min(max(float(raw_prob), 0.0), _COMBO_CAP)


def fit_manji_win_calibrator(
    conn: sqlite3.Connection,
    *,
    max_races: int = 250,
    min_samples: int = 150,
) -> dict[str, object]:
    """確定実績から卍 ev_score → P(win) の Isotonic 較正器を学習・永続化する。

    WF バックテストの幻想 ROI ではなく、過去レースで recompute した ev_score と
    実着順(rank==1)を突合して学習する。

    Args:
        conn: DB コネクション。
        max_races: 学習に使う直近確定レースの最大数。
        min_samples: 学習に必要な最小サンプル数（馬-行）。

    Returns:
        診断 dict（n_races / n_samples / fitted / path / sample_curve）。
    """
    from sklearn.isotonic import IsotonicRegression

    from src.ml.features import FeatureBuilder
    from src.ml.models import load_models

    _honmei, _place, manji = load_models()

    race_ids = [
        r[0]
        for r in conn.execute(
            """
            SELECT r.race_id
            FROM races r
            WHERE EXISTS (
                SELECT 1 FROM race_results rr
                WHERE rr.race_id = r.race_id AND rr.rank = 1
            )
            ORDER BY r.date DESC, r.race_id DESC
            LIMIT ?
            """,
            (max_races,),
        ).fetchall()
    ]

    xs: list[float] = []
    ys: list[int] = []
    n_races = 0
    for rid in race_ids:
        try:
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is None or df.empty:
                continue
            ev = manji.ev_score(df)
            winners = {
                row[0]
                for row in conn.execute(
                    "SELECT horse_name FROM race_results WHERE race_id = ? AND rank = 1",
                    (rid,),
                ).fetchall()
            }
            if not winners:
                continue
            n_races += 1
            for i, (_, hrow) in enumerate(df.iterrows()):
                if i >= len(ev):
                    break
                name = str(hrow.get("horse_name", ""))
                xs.append(float(ev.iloc[i]))
                ys.append(1 if name in winners else 0)
        except Exception as exc:  # noqa: BLE001 — 1レース失敗で全体を止めない
            logger.debug("較正学習 スキップ race_id=%s: %s", rid, exc)
            continue

    diag: dict[str, object] = {
        "n_races": n_races,
        "n_samples": len(xs),
        "fitted": False,
        "path": str(_WIN_CAL_PATH),
    }
    if len(xs) < min_samples or sum(ys) == 0:
        logger.warning(
            "卍較正器: サンプル不足（n=%d, wins=%d）→ 学習スキップ（保守フォールバック使用）",
            len(xs),
            sum(ys),
        )
        return diag

    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(xs, ys)

    _MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(_WIN_CAL_PATH, "wb") as f:
        pickle.dump(iso, f)

    global _win_cal_cache
    _win_cal_cache = iso

    # 診断: 代表点での較正値
    sample_curve = {
        round(v, 1): round(float(iso.predict([v])[0]), 4)
        for v in (0.5, 1.0, 1.5, 2.0, 3.0, 5.0)
    }
    diag.update(
        {
            "fitted": True,
            "base_rate": round(sum(ys) / len(ys), 4),
            "sample_curve": sample_curve,
        }
    )
    logger.info("卍単勝較正器を学習・保存: %s", diag)
    return diag


def _compute_ece(probs: list[float], labels: list[int], n_bins: int = 10) -> float:
    """Expected Calibration Error（小さいほど較正良好）を等幅ビンで算出する。"""
    if not probs:
        return float("nan")
    total = len(probs)
    ece = 0.0
    for b in range(n_bins):
        lo, hi = b / n_bins, (b + 1) / n_bins
        # 最終ビンは右端を含める
        idx = [
            i
            for i, p in enumerate(probs)
            if (lo <= p < hi) or (b == n_bins - 1 and p == hi)
        ]
        if not idx:
            continue
        conf = sum(probs[i] for i in idx) / len(idx)
        acc = sum(labels[i] for i in idx) / len(idx)
        ece += (len(idx) / total) * abs(conf - acc)
    return ece


def fit_manji_place_calibrator(
    conn: sqlite3.Connection,
    *,
    max_races: int = 400,
    min_samples: int = 200,
) -> dict[str, object]:
    """確定実績から卍 ev_score → P(複勝圏=3着内) の Platt 較正器を学習・永続化する。

    単勝(Isotonic)とは独立した複勝特化インスタンス。Platt Scaling=ロジスティック回帰で
    ev_score 1次元から複勝圏確率を較正し、ECE(較正誤差)を含む診断を返す。

    Args:
        conn: DB コネクション。
        max_races: 学習に使う直近確定レースの最大数。
        min_samples: 学習に必要な最小サンプル数（馬-行）。

    Returns:
        診断 dict（n_races / n_samples / fitted / base_rate / ece / ece_uncal /
        sample_curve / path）。
    """
    import numpy as np
    from sklearn.linear_model import LogisticRegression

    from src.ml.features import FeatureBuilder
    from src.ml.models import load_models

    _honmei, _place, manji = load_models()

    race_ids = [
        r[0]
        for r in conn.execute(
            """
            SELECT r.race_id
            FROM races r
            WHERE EXISTS (
                SELECT 1 FROM race_results rr
                WHERE rr.race_id = r.race_id AND rr.rank = 1
            )
            ORDER BY r.date DESC, r.race_id DESC
            LIMIT ?
            """,
            (max_races,),
        ).fetchall()
    ]

    xs: list[float] = []
    ys: list[int] = []
    n_races = 0
    for rid in race_ids:
        try:
            df = FeatureBuilder(conn).build_race_features(rid)
            if df is None or df.empty:
                continue
            ev = manji.ev_score(df)
            placed = {
                row[0]
                for row in conn.execute(
                    "SELECT horse_name FROM race_results "
                    "WHERE race_id = ? AND rank IS NOT NULL AND rank BETWEEN 1 AND 3",
                    (rid,),
                ).fetchall()
            }
            if not placed:
                continue
            n_races += 1
            for i, (_, hrow) in enumerate(df.iterrows()):
                if i >= len(ev):
                    break
                name = str(hrow.get("horse_name", ""))
                xs.append(float(ev.iloc[i]))
                ys.append(1 if name in placed else 0)
        except Exception as exc:  # noqa: BLE001 — 1レース失敗で全体を止めない
            logger.debug("複勝較正学習 スキップ race_id=%s: %s", rid, exc)
            continue

    diag: dict[str, object] = {
        "n_races": n_races,
        "n_samples": len(xs),
        "fitted": False,
        "path": str(_PLACE_CAL_PATH),
    }
    if len(xs) < min_samples or sum(ys) == 0 or sum(ys) == len(ys):
        logger.warning(
            "卍複勝較正器: サンプル不足/偏り（n=%d, placed=%d）→ 学習スキップ",
            len(xs),
            sum(ys),
        )
        return diag

    x_arr = np.asarray(xs, dtype=float).reshape(-1, 1)
    y_arr = np.asarray(ys, dtype=int)

    # Platt Scaling = 1次元ロジスティック回帰。
    platt = LogisticRegression(max_iter=1000)
    platt.fit(x_arr, y_arr)

    # 較正前後の ECE を比較（較正前は ev_score を 0-1 へ素朴正規化した値で代用）。
    cal_probs = [float(p) for p in platt.predict_proba(x_arr)[:, 1]]
    ev_min, ev_max = float(min(xs)), float(max(xs))
    span = (ev_max - ev_min) or 1.0
    uncal_probs = [min(max((v - ev_min) / span, 0.0), 1.0) for v in xs]
    ece_cal = _compute_ece(cal_probs, ys)
    ece_uncal = _compute_ece(uncal_probs, ys)

    _MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(_PLACE_CAL_PATH, "wb") as f:
        pickle.dump(platt, f)

    global _place_cal_cache
    _place_cal_cache = platt

    sample_curve = {
        round(v, 1): round(float(platt.predict_proba([[v]])[0][1]), 4)
        for v in (0.5, 1.0, 1.5, 2.0, 3.0, 5.0)
    }
    diag.update(
        {
            "fitted": True,
            "base_rate": round(sum(ys) / len(ys), 4),
            "ece": round(ece_cal, 4),
            "ece_uncal": round(ece_uncal, 4),
            "sample_curve": sample_curve,
        }
    )
    logger.info("卍複勝較正器(Platt)を学習・保存: %s", diag)
    return diag
