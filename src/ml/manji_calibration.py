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

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "models"
_WIN_CAL_PATH = _MODEL_DIR / "manji_win_calibrator.pkl"

# フォールバック時の confidence 上限（学習済み較正器が無くても 1.0 飽和を防ぐ）
_FALLBACK_WIN_CAP = 0.6
# combo 生確率の上限（確率の性質を保持）
_COMBO_CAP = 0.99

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
            p = float(cal.predict([float(ev_score)])[0])
            return min(max(p, 0.0), 0.999)
        except Exception as exc:  # noqa: BLE001
            logger.debug("卍較正器 predict 失敗（フォールバック）: %s", exc)

    # フォールバック: implied prob = EV / odds を上限 _FALLBACK_WIN_CAP で頭打ち
    o = max(float(odds), 1.0)
    implied = float(ev_score) / o if o > 1.0 else float(ev_score) / 10.0
    return min(max(implied, 0.0), _FALLBACK_WIN_CAP)


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
