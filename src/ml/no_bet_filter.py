"""
src/ml/no_bet_filter.py — 見送り（No-Bet）判定モデル【Fable 提案・収益強化】

「最も簡単に ROI を上げる方法は、負けやすいレースを買わないこと」。
プロ馬券師の鉄則をシステム化する。EV ゲートは「馬単位」のフィルタだが、
本モジュールは**レース単位**で「このレースは較正済み確率の信頼域の外にある＝
何を買っても分散だけ食う」状況を検知し、レースごと見送る判定を返す。

判定に使うシグナル（すべて発走前に確定する値・リークフリー）:

  1. **オッズエントロピー（混戦度）**: 市場確率の正規化エントロピー。
     1.0 に近いほど全馬均衡の混戦＝勝者予測の情報量が無い。
  2. **本命の弱さ**: 1番人気の市場確率。弱い本命のレースは波乱が構造的に多い。
  3. **オーバーラウンド異常**: Σ(1/odds) が通常域（控除率相当）から逸脱。
     低すぎ＝オッズ供給異常/データ欠損、高すぎ＝市場の歪み（インサイダー資金の
     痕跡を含む）であり、いずれも較正の前提が崩れている。
  4. **モデル-市場乖離の総量**: モデル確率と市場確率の Jensen-Shannon 距離。
     全馬で大きく食い違うのは「エッジ」ではなく特徴量欠損・データ異常の兆候
     （真のエッジは少数馬の局所的な歪みに現れる）。
  5. **少頭数・馬場**: 7頭以下（オッズ妙味が構造的に薄い）と不良馬場。

各シグナルを 0-1 のリスクに正規化し、加重和の chaos_score ≥ 閾値で見送り。
W-071 の教訓に従い**未検証の係数をモデルEVに乗じることはせず**、
「買う/買わない」の二値ゲートとしてのみ作用する（確率・EV は一切改変しない）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

# ── 既定パラメータ（OOS 再較正までの初期値・config で変更可）──────────────────
# 重み設計上、純粋な大混戦（entropy=1.0 + weak_favorite=1.0）で 0.50 に達する。
# 0.42 は「ほぼ満点の混戦シグナル or 混戦＋構造/異常の複合」で発火する水準。
DEFAULT_CHAOS_THRESHOLD: float = 0.42  # これ以上で見送り
# JRA 単勝オーバーラウンドの通常域（払戻率80% → Σ1/odds ≈ 1.25 近辺）
OVERROUND_NORMAL_LO: float = 1.05
OVERROUND_NORMAL_HI: float = 1.45
MIN_FIELD_SIZE: int = 8  # これ未満は少頭数リスク
WEAK_FAVORITE_PROB: float = 0.25  # 1番人気の市場確率がこれ未満で「弱い本命」

_WEIGHTS: dict[str, float] = {
    "entropy": 0.30,
    "weak_favorite": 0.20,
    "overround": 0.20,
    "divergence": 0.20,
    "structure": 0.10,  # 少頭数・馬場
}


@dataclass
class NoBetDecision:
    """見送り判定の結果（判定根拠つき）。"""

    race_id: str
    skip: bool
    chaos_score: float
    reasons: list[str] = field(default_factory=list)
    components: dict[str, float] = field(default_factory=dict)


def market_probs_from_odds(win_odds: np.ndarray | list[float]) -> np.ndarray:
    """単勝オッズから正規化済み市場確率を返す（不正値は微小確率に縮退）。"""
    o = np.asarray(win_odds, dtype=float)
    inv = np.where(o > 1.0, 1.0 / o, 1e-9)
    s = float(inv.sum())
    return inv / s if s > 0 else np.full(len(o), 1.0 / max(len(o), 1))


def normalized_entropy(probs: np.ndarray) -> float:
    """確率分布の正規化エントロピー（0=一点集中, 1=完全均衡）。"""
    p = np.asarray(probs, dtype=float)
    p = p[p > 0]
    n = len(p)
    if n <= 1:
        return 0.0
    h = float(-(p * np.log(p)).sum())
    return h / math.log(n)


def jensen_shannon_distance(p: np.ndarray, q: np.ndarray) -> float:
    """Jensen-Shannon 距離（0=同一分布, 1=完全乖離）。"""
    a = np.asarray(p, dtype=float) + 1e-12
    b = np.asarray(q, dtype=float) + 1e-12
    a, b = a / a.sum(), b / b.sum()
    m = 0.5 * (a + b)
    kl_am = float((a * np.log2(a / m)).sum())
    kl_bm = float((b * np.log2(b / m)).sum())
    return math.sqrt(max(0.5 * kl_am + 0.5 * kl_bm, 0.0))


@dataclass
class NoBetConfig:
    """見送り判定の設定。"""

    chaos_threshold: float = DEFAULT_CHAOS_THRESHOLD
    overround_lo: float = OVERROUND_NORMAL_LO
    overround_hi: float = OVERROUND_NORMAL_HI
    min_field_size: int = MIN_FIELD_SIZE
    weak_favorite_prob: float = WEAK_FAVORITE_PROB


def evaluate_no_bet(
    race_id: str,
    win_odds: list[float] | np.ndarray,
    model_win_probs: list[float] | np.ndarray | None = None,
    condition: str | None = None,
    config: NoBetConfig | None = None,
) -> NoBetDecision:
    """レース単位の見送り判定を行う純関数。

    Args:
        race_id: レース ID。
        win_odds: 全出走馬の単勝オッズ。
        model_win_probs: 自モデルの勝率推計（省略時は乖離シグナルを 0 扱い）。
        condition: 馬場状態（"不良"/"重" 等・省略可）。
        config: 設定。

    Returns:
        NoBetDecision（skip=True なら買わない）。
    """
    cfg = config or NoBetConfig()
    odds = np.asarray(win_odds, dtype=float)
    n = len(odds)
    reasons: list[str] = []
    comp: dict[str, float] = {}

    if n == 0:
        return NoBetDecision(race_id, True, 1.0, ["出走データなし"], {})

    p_market = market_probs_from_odds(odds)

    # 1. 混戦度（エントロピーは頭数が多いほど自然に高いので強めの非線形）
    ent = normalized_entropy(p_market)
    comp["entropy"] = float(np.clip((ent - 0.80) / 0.20, 0.0, 1.0))
    if comp["entropy"] >= 0.75:
        reasons.append(f"混戦（オッズエントロピー {ent:.2f}）")

    # 2. 本命の弱さ
    fav = float(p_market.max())
    comp["weak_favorite"] = float(
        np.clip((cfg.weak_favorite_prob - fav) / cfg.weak_favorite_prob, 0.0, 1.0)
    )
    if fav < cfg.weak_favorite_prob:
        reasons.append(f"弱い本命（1人気の市場確率 {fav:.0%}）")

    # 3. オーバーラウンド異常（通常域からの逸脱率）
    overround = float(np.where(odds > 1.0, 1.0 / odds, 0.0).sum())
    if overround < cfg.overround_lo:
        dev = (cfg.overround_lo - overround) / cfg.overround_lo
    elif overround > cfg.overround_hi:
        dev = (overround - cfg.overround_hi) / cfg.overround_hi
    else:
        dev = 0.0
    comp["overround"] = float(np.clip(dev * 3.0, 0.0, 1.0))
    if comp["overround"] > 0:
        reasons.append(f"オッズ供給異常（オーバーラウンド {overround:.2f}）")

    # 4. モデル-市場の全体乖離（JS距離）
    if model_win_probs is not None and len(model_win_probs) == n:
        jsd = jensen_shannon_distance(np.asarray(model_win_probs, float), p_market)
        comp["divergence"] = float(np.clip((jsd - 0.30) / 0.40, 0.0, 1.0))
        if comp["divergence"] >= 0.5:
            reasons.append(f"モデル-市場の全面乖離（JS距離 {jsd:.2f}・データ異常疑い）")
    else:
        comp["divergence"] = 0.0

    # 5. 構造リスク（少頭数・馬場）
    structure = 0.0
    if n < cfg.min_field_size:
        structure += 0.6
        reasons.append(f"少頭数（{n}頭）")
    if condition in ("不良", "重"):
        structure += 0.4
        reasons.append(f"渋った馬場（{condition}）")
    comp["structure"] = float(min(structure, 1.0))

    chaos = float(sum(_WEIGHTS[k] * comp[k] for k in _WEIGHTS))
    return NoBetDecision(
        race_id=race_id,
        skip=chaos >= cfg.chaos_threshold,
        chaos_score=round(chaos, 4),
        reasons=reasons,
        components={k: round(v, 4) for k, v in comp.items()},
    )
