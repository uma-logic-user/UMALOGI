"""過去モデル資産アンサンブル — 卍(EV回帰)を全券種EVエンジンの勝率推計に融合する。

【背景 / 静的解析（scripts/analyze_legacy_models.py・OOS 400R）】
  リポジトリ内の全過去モデル（ALPHA / ALPHA-Payout / cascade / sandbox /
  v2系 / pre69feat世代）を評価した結果:
    - 卍(現役EV回帰):  全体AUC 0.682 ながら荒れレースAUC 0.754（市場 0.612 を圧倒）。
                       ρ(honmei)=0.33 / ρ(market)=0.21 と多様性が最大 → 採用。
    - ALPHA / ALPHA-Payout: AUC 0.81 だが ρ(market)≈0.96-0.98 の実質市場複製 → 不採用
      （市場成分は blend_with_market が既に混合しており情報の重複追加になる）。
    - cascade: stage1 モデル未保存で入力特徴量が再現不能 → 構造的に不採用。
    - sandbox / pre69feat / v2系: AUC 不足 → 不採用。

【数式】
  p_h        = honmei raw P(win)（レースの全頭分・非正規化）
  implied_m  = clip(卍予測EV, 0) / odds          … 卍EV → 暗黙の勝率
  scaled_m   = implied_m / Σimplied_m × Σp_h     … p_h と同一スケールに正規化
  p_ens      = (1−w)·p_h + w·scaled_m

  w=0 で従来パイプラインと完全一致（恒等）。総和 Σp_ens = Σp_h を常に保存するため、
  後段の blend_with_market / scan_all_tickets の挙動スケールを変えない。

【重みの決定】
  MANJI_ENSEMBLE_WEIGHT は ablation_cache の train フレーム（cutoff 前・
  2024-01〜2025-10）でのグリッド探索で決定し、test フレーム（純OOS 400R）で
  一発検証する（scripts/backtest_all_tickets.py --manji-weight）。
  OOS で重みを選ぶことはリークであり禁止。
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_MANJI_PKL = _ROOT / "data" / "models" / "manji_model.pkl"

# train フレーム（cutoff前300R）グリッド探索 w∈{0,0.1,...,0.5} で合計ROIピーク
# （w=0.4: 97.5%）として決定し、OOS（test 400R）で一発検証した本番ウェイト。
#   OOS 結果: 三連複のみ適用で合計 ROI 110.0% → 119.2%（三連複 106.9% → 157.9%）
#             最大1的中除外でも 81.8% → 107.8% に改善（大穴依存の低減）。
#   三連単は w 適用で 110.0% → 93.5% に劣化したため適用対象外
#   （着順厳密予測は honmei の1着精度が支配的で、卍の複勝圏歪み情報はノイズ）。
# 変更時は scripts/backtest_all_tickets.py で OOS 再検証必須（W-080系）。
MANJI_ENSEMBLE_WEIGHT: float = 0.4

# アンサンブルを適用する券種。これ以外の券種は従来確率（w=0 恒等）を使うこと。
MANJI_ENSEMBLE_BET_TYPES: frozenset[str] = frozenset({"三連複"})


def ensemble_win_probs(
    p_honmei: np.ndarray,
    manji_ev: np.ndarray,
    odds: np.ndarray,
    weight: float = MANJI_ENSEMBLE_WEIGHT,
) -> np.ndarray:
    """honmei 勝率に卍EV回帰の暗黙勝率を重み w で融合する。

    w=0 のとき p_honmei をそのまま返す（従来パイプラインと恒等）。
    出力の総和は常に Σp_honmei に一致する（スケール保存）。

    Args:
        p_honmei: honmei raw P(win)（1レース全頭分）。
        manji_ev: 卍モデルの予測EV（同じ並び）。
        odds: 単勝オッズ（同じ並び）。
        weight: 卍成分の混合比 w ∈ [0, 1]。

    Returns:
        融合後の勝率配列（p_honmei と同じ長さ）。
    """
    p_h = np.asarray(p_honmei, dtype=float)
    if weight <= 0.0:
        return p_h.copy()

    w = min(float(weight), 1.0)
    ev = np.nan_to_num(np.asarray(manji_ev, dtype=float), nan=0.0)
    o = np.clip(np.nan_to_num(np.asarray(odds, dtype=float), nan=1.0), 1.01, None)

    implied = np.clip(ev, 0.0, None) / o
    s_implied = float(implied.sum())
    s_h = float(np.clip(p_h, 0.0, None).sum())
    if s_implied <= 0.0 or s_h <= 0.0:
        return p_h.copy()  # 卍が無情報 → フォールバック（恒等）

    scaled_m = implied / s_implied * s_h
    return (1.0 - w) * p_h + w * scaled_m


def predict_manji_ev(model: Any, feature_df: Any) -> np.ndarray:
    """卍モデル（LGBMRegressor / Booster ラッパ）で予測EVを返す。

    feature_df はモデルの feature_name_ に reindex して 0 埋めする
    （pre69feat 等の世代差にも安全）。
    """
    import pandas as pd

    base = getattr(model, "base", model)
    fl = getattr(base, "feature_name_", None)
    if fl is None:
        fl = base.booster_.feature_name()
    x = feature_df.reindex(columns=list(fl))
    x = x.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return np.asarray(model.predict(x), dtype=float)


class ManjiScoreSource:
    """本番用: race_id → {horse_number: 卍予測EV} を返す失敗安全ソース。

    predictions テーブルの卍レコードは買い目3頭のみ（同値スコア）保存のため
    使用できない。本クラスは卍 pkl を直接ロードし、FeatureBuilder で
    全頭特徴量を構築して推論する。いかなる失敗時も None を返し、
    呼び出し側は従来動作（honmei 単独）へフォールバックする。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._model: Any = None
        self._builder: Any = None
        self._load_failed = False

    def _ensure_loaded(self) -> bool:
        if self._load_failed:
            return False
        if self._model is not None:
            return True
        try:
            with open(_MANJI_PKL, "rb") as f:
                self._model = pickle.load(f)
            from src.ml.features import FeatureBuilder

            self._builder = FeatureBuilder(self._conn)
            return True
        except Exception as exc:
            logger.warning("卍アンサンブルソース初期化失敗（無効化）: %s", exc)
            self._load_failed = True
            return False

    def scores_for(self, race_id: str) -> dict[int, float] | None:
        """全頭の {馬番: 卍予測EV} を返す。失敗時は None（フォールバック）。"""
        if not self._ensure_loaded():
            return None
        try:
            df = self._builder.build_race_features(race_id)
            if df is None or df.empty or "horse_number" not in df.columns:
                return None
            ev = predict_manji_ev(self._model, df)
            return {
                int(hn): float(e)
                for hn, e in zip(df["horse_number"], ev)
                if hn is not None
            }
        except Exception as exc:
            logger.warning("卍アンサンブル推論失敗 race_id=%s: %s", race_id, exc)
            return None
