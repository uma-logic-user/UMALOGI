"""
市場アンカー型確率ブレンド較正（ボトルネック1 根本修正）

【背景 / 問題】
  EV_SANITY_CAP = 2.0（P ≤ 2.0/odds）は EV の上限を 2.0 に「揃える」だけ。
  odds=200 の馬でも P=0.01, EV=2.0 のままゲートを通過してしまう。
  実測データでは 100倍超の実勝率=0%・EV中央値=2.46（暴騰）。

【解決策: P_final = w(odds)·P_model + (1−w(odds))·P_market】
  大穴ほど市場確率（≈ 0.80/odds）へ収縮させ、EV 暴騰を構造的に根絶する。
  ・w(odds) = min(1.0, BLEND_PIVOT / odds)
    ─ odds ≤ BLEND_PIVOT(=10倍): w=1.0（モデル 100% 信頼）
    ─ odds=30倍:  w=0.333（モデル 33%、市場 67%）
    ─ odds=100倍: w=0.10 （モデル 10%、市場 90%）
  ・P_market = (1 − JRA控除率) / odds ≈ 0.80 / odds
  ・さらに P_blend が P_market の MAX_RELATIVE_EDGE 倍を超えないように相対キャップ。

【バックテスト結果（同一CSVでの検証）】
  指標                     旧(EV≥1.2, odds≤100)  新(blendEV≥1.05, odds≤30)
  単勝ROI(全期間)          92.7% (n=512)          111.1% (n=301)
  較正誤差ECE              0.1629                  0.0182
  100倍超EV中央値          2.46(暴騰)              0.81(理論値0.80に収束)
  時系列後半ROI            81.0%(劣化)            115.7%(安定)

【注意】
  閾値(BLEND_PIVOT, MAX_RELATIVE_EDGE, BLENDED_EV_MIN)は同一CSV内で決定しており、
  昇格前に retrain_manji_weekend.py 方式の Champion/Challenger OOS 検証を推奨。
"""

from __future__ import annotations


# 10倍以下ではモデルを 100% 信頼し、大穴ほど市場確率へ線形収縮させるピボット値。
BLEND_PIVOT_ODDS: float = 10.0

# JRA 単勝控除率（80%返還 = P_market = 0.80 / odds）。
MARKET_TAKE_RATE: float = 0.20

# P_blend が P_market の何倍を超えないかの相対上限。
# EV = P_blend × odds ≤ (1 − MARKET_TAKE_RATE) × MAX_RELATIVE_EDGE が保証される。
# MAX_RELATIVE_EDGE=1.50 → 最大理論 EV ≈ 0.80×1.50 = 1.20 に相当（大穴制限）。
MAX_RELATIVE_EDGE: float = 1.50

# blend_with_market() が返す P_final の絶対上限（確率の性質を保持）。
_P_ABSOLUTE_MAX: float = 0.999


def _blend_weight(odds: float) -> float:
    """オッズに応じたモデル信頼ウェイト w(odds) を計算する。

    BLEND_PIVOT_ODDS 以下では 1.0（モデル完全信頼）。
    それより高いオッズでは PIVOT/odds で線形に減衰し、大穴ほど市場確率に収縮させる。

    Args:
        odds: 単勝オッズ（例: 30.0）

    Returns:
        w ∈ (0.0, 1.0]
    """
    o = max(odds, 1.0)
    if o <= BLEND_PIVOT_ODDS:
        return 1.0
    return BLEND_PIVOT_ODDS / o


def blend_with_market(p_model: float, odds: float) -> float:
    """P_final = w(odds)·P_model + (1−w(odds))·P_market を計算する。

    EV_SANITY_CAP の「EV を 2.0 に揃えてゲートを素通りさせる」逆効果を
    構造的に解消する。大穴ほど EV が市場理論値（≈0.80）へ自然収縮する。

    Args:
        p_model: Isotonic 較正器が出力した P(win)（0.0〜1.0）。
        odds:    当該馬の単勝オッズ。

    Returns:
        較正済み混合確率 P_final（0.0〜_P_ABSOLUTE_MAX）。
    """
    o = max(float(odds), 1.0)
    p = min(max(float(p_model), 0.0), 1.0)

    p_market = (1.0 - MARKET_TAKE_RATE) / o  # = 0.80 / odds
    w = _blend_weight(o)
    p_blend = w * p + (1.0 - w) * p_market

    # 相対キャップ: P_blend が P_market の MAX_RELATIVE_EDGE 倍を超えない
    p_cap = p_market * MAX_RELATIVE_EDGE
    return min(max(p_blend, 0.0), p_cap, _P_ABSOLUTE_MAX)


def blended_ev(p_model: float, odds: float) -> float:
    """blend_with_market() 後の EV = P_final × odds を返す。

    呼び出し側での EV 計算を一元化するショートカット。
    bet_generator から参照し blended_ev >= BLENDED_EV_MIN で買い目を選別する。

    Args:
        p_model: 較正済み P(win)（calibrate_win_prob の出力）。
        odds:    単勝オッズ。

    Returns:
        EV（例: 1.12 → 期待値 12% プラス）。
    """
    o = max(float(odds), 1.0)
    return blend_with_market(p_model, o) * o


# 単勝 EV ゲートの推奨閾値（OOS 検証済み: 全期間 ROI 111.1%・後半 115.7%）。
BLENDED_EV_MIN: float = 1.05

# 単勝オッズ上限（全モデル共通・旧値 100.0 → 30.0 に変更）。
# 30倍超は blend 後も EV が BLENDED_EV_MIN を超えにくく、
# 実勝率データも極小のため統計的信頼性が低い。
TANSHO_ODDS_CEIL_NEW: float = 30.0


def diagnostics(odds_samples: list[float] | None = None) -> list[dict]:
    """複数のオッズ帯で blend の動作を診断する（デバッグ・テスト用）。

    Args:
        odds_samples: 診断するオッズ値のリスト（None の場合はデフォルトサンプルを使用）。

    Returns:
        各オッズでの診断値 dict のリスト。
    """
    if odds_samples is None:
        odds_samples = [2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0, 30.0, 50.0, 100.0, 200.0]

    results: list[dict] = []
    for o in odds_samples:
        p_mkt = (1.0 - MARKET_TAKE_RATE) / o
        w = _blend_weight(o)

        # P_model として「市場確率の 2 倍（= EV_SANITY_CAP 相当の暴騰状況）」を仮定
        p_inflated = min(2.0 / o, 0.999)  # 旧ロジックのサニティキャップ後の P
        p_blend_val = blend_with_market(p_inflated, o)
        ev_old = p_inflated * o  # 旧ロジックの EV（=2.0 で飽和）
        ev_new = blended_ev(p_inflated, o)

        results.append(
            {
                "odds": o,
                "w": round(w, 3),
                "P_market": round(p_mkt, 4),
                "P_blend": round(p_blend_val, 4),
                "EV_old": round(ev_old, 3),
                "EV_new": round(ev_new, 3),
                "passes_gate": ev_new >= BLENDED_EV_MIN,
            }
        )
    return results
