"""
src/ml/calibration.py — 本命モデルのキャリブレーション補正

概要:
  本命(直前)モデルは全 bin で実際の的中率を系統的に過小評価している
  (平均補正倍率 1.913)。scripts/calibration_kelly_audit.py の分析結果
  (1,726 サンプル, 2026-05-27 実施) に基づき bin 別の補正係数を定義する。

補正テーブル (actual/predicted ratio):
  bin 0.00-0.05 → 3.081
  bin 0.05-0.10 → 2.077
  bin 0.10-0.15 → 1.910
  bin 0.15-0.20 → 1.357
  bin 0.20-0.25 → 1.141
  bin 0.25+     → 1.000 (サンプル数5件のため保守的に補正なし)

利用方法:
  from src.ml.calibration import correct_honmei_score
  corrected_prob = correct_honmei_score(raw_model_score)

  # DataFrame 全体に適用する場合
  scores_corrected = honmei_scores.apply(correct_honmei_score)

注意:
  - DB に保存する model_score は RAW スコアを維持すること（監査追跡のため）。
  - 補正済みスコアは EV・Kelly 計算にのみ使用する。
  - 補正後の確率は 1.0 でキャップされる（確率の性質を保持）。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ── bin 別補正テーブル（calibration_kelly_audit.py の出力値）────────────────
# 形式: (上限値(exclusive), 補正倍率)
# スコア < 上限値 のサンプルに対してこの補正倍率を適用する
_HONMEI_CORRECTION_BINS: tuple[tuple[float, float], ...] = (
    (0.05, 3.081),  # bin 0.00-0.05: 予測1.7% vs 実績5.3%
    (0.10, 2.077),  # bin 0.05-0.10: 予測7.2% vs 実績14.9%
    (0.15, 1.910),  # bin 0.10-0.15: 予測12.1% vs 実績23.2%
    (0.20, 1.357),  # bin 0.15-0.20: 予測16.9% vs 実績23.0%
    (0.25, 1.141),  # bin 0.20-0.25: 予測20.9% vs 実績23.8%
)

# 0.25 以上のスコアはサンプル数 5 件のため補正なし（過学習防止）
_HONMEI_FALLBACK_FACTOR: float = 1.0

# 補正を適用する最大スコア上限
_CORRECTION_ENABLED: bool = True


def correct_honmei_score(raw_score: float) -> float:
    """本命モデルの raw model_score に bin 別補正倍率を適用する。

    calibration_kelly_audit.py で確認された系統的な過小評価（平均倍率 1.913）を
    bin 別に補正する。補正後確率は [0.0, 1.0] にクランプされる。

    Args:
        raw_score: HonmeiModel.predict() の生出力（0〜1 の確率）

    Returns:
        補正後の推定的中確率（0.0〜1.0）
    """
    if not _CORRECTION_ENABLED:
        return raw_score

    raw = float(raw_score)
    if raw <= 0.0:
        return 0.0

    for upper_bound, factor in _HONMEI_CORRECTION_BINS:
        if raw < upper_bound:
            corrected = raw * factor
            return min(corrected, 1.0)

    # 0.25 以上: 補正なし
    return min(raw * _HONMEI_FALLBACK_FACTOR, 1.0)


def correction_factor_for(raw_score: float) -> float:
    """raw_score に対応する補正倍率を返す（ログ・診断用）。"""
    if not _CORRECTION_ENABLED:
        return 1.0
    raw = float(raw_score)
    for upper_bound, factor in _HONMEI_CORRECTION_BINS:
        if raw < upper_bound:
            return factor
    return _HONMEI_FALLBACK_FACTOR


def apply_calibration_to_series(
    honmei_scores: "pd.Series",  # type: ignore[name-defined]  # noqa: F821
    log_prefix: str = "",
) -> "pd.Series":  # type: ignore[name-defined]  # noqa: F821
    """pandas Series 全体に correct_honmei_score を適用する。

    Args:
        honmei_scores: 生モデルスコアの Series
        log_prefix: ログメッセージのプレフィックス（race_id など）

    Returns:
        補正済みスコアの新規 Series（元の Series は変更しない）
    """

    corrected = honmei_scores.apply(correct_honmei_score)
    if log_prefix and logger.isEnabledFor(logging.DEBUG):
        raw_mean = honmei_scores.mean()
        cor_mean = corrected.mean()
        logger.debug(
            "[W-036 calibration] %s スコア補正: raw_mean=%.3f → corrected_mean=%.3f (ratio=×%.2f)",
            log_prefix,
            raw_mean,
            cor_mean,
            cor_mean / max(raw_mean, 1e-9),
        )
    return corrected
