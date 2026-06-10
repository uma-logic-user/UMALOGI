"""
src/ml/bankroll_manager.py — 破産確率最小化バンクロール管理（金融工学レイヤー）

既存の Kelly 実装（pure_ev_edge.kelly_stake / alpha_model.calc_kelly_bet 等）は
「1 ベット単位の fractional Kelly」しか見ておらず、以下の数学的な穴が残っていた:

  1. **同時ベットの合成リスク無視**: 同日に N 点買うと合計エクスポージャーが
     Σf となり、単発 Kelly の安全域を突き破る（同時 Kelly の過大賭け問題）。
  2. **静的バンクロール**: initial_bankroll=10万円 固定で、実損益を反映した
     複利運用（本来の Kelly の前提）になっていない。
  3. **破産確率の未定量化**: 「1/10 Kelly だから安全」は感覚であり、
     P(破産) を数値で監視していない。
  4. **ドローダウン時の無減速**: 連敗中も同じ比率で張り続ける。

本モジュールはこれらを純関数群として解決する**単一真実源**。
本番ベットフローへの結線は OOS 検証ゲートを通過してから行う
（[[feedback_ev_precision_safety_first]]・未検証ロジックの実弾直結禁止）。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

import numpy as np

# ── 既定パラメータ（コードロック・変更時は OOS 再検証必須）─────────────────
DEFAULT_KELLY_FRACTION: float = 0.10  # 1/10 Kelly（pure_ev_edge と整合）
DEFAULT_MAX_BET_PCT: float = 0.02  # 1点あたり上限（バンクロール比）
DEFAULT_MAX_DAILY_EXPOSURE_PCT: float = 0.10  # 1日合計エクスポージャー上限
MIN_STAKE_YEN: int = 100  # JRA 最低購入単位
# ドローダウン帯域 → Kelly 減速係数（ピーク比の下落率, 乗数）
DRAWDOWN_THROTTLE_BANDS: tuple[tuple[float, float], ...] = (
    (0.30, 0.0),  # -30% 超: 全停止（人間によるレビューを強制）
    (0.20, 0.25),  # -20%〜-30%: 1/4 に減速
    (0.10, 0.5),  # -10%〜-20%: 半減
)


@dataclass
class BankrollConfig:
    """バンクロール管理の設定。"""

    initial_bankroll: float = 100_000.0
    kelly_fraction: float = DEFAULT_KELLY_FRACTION
    max_bet_pct: float = DEFAULT_MAX_BET_PCT
    max_daily_exposure_pct: float = DEFAULT_MAX_DAILY_EXPOSURE_PCT
    min_stake: int = MIN_STAKE_YEN


@dataclass
class BetCandidate:
    """ステーク算出対象の 1 買い目（確率・オッズは較正済み前提）。"""

    race_id: str
    bet_type: str
    horse_number: int
    prob: float  # 較正済み的中確率
    odds: float  # 実効オッズ（複勝は推定場 odds）


@dataclass
class BetAllocation:
    """算出結果（推奨ステークと根拠）。"""

    candidate: BetCandidate
    full_kelly: float  # フル Kelly 比率 f*
    applied_fraction: float  # 減速・スケール適用後の実効比率
    stake: int  # 推奨購入額（円・100円単位）


@dataclass
class AllocationResult:
    """1 バッチ（同時ベット群）の配分結果。"""

    allocations: list[BetAllocation] = field(default_factory=list)
    bankroll: float = 0.0
    throttle: float = 1.0  # ドローダウン減速係数
    exposure_scale: float = 1.0  # 同時ベット縮約係数（1.0=縮約なし）

    @property
    def total_stake(self) -> int:
        return sum(a.stake for a in self.allocations)


# ── 基本 Kelly ───────────────────────────────────────────────────────────
def full_kelly(prob: float, odds: float) -> float:
    """フル Kelly 比率 f* = (p·odds − 1) / (odds − 1) を返す（負エッジは 0）。

    EV = p·odds とすると f* = (EV−1)/(odds−1) であり
    pure_ev_edge.kelly_stake と数学的に同一。
    """
    if odds <= 1.0 or prob <= 0.0:
        return 0.0
    f = (prob * odds - 1.0) / (odds - 1.0)
    return max(f, 0.0)


def drawdown_throttle(current_bankroll: float, peak_bankroll: float) -> float:
    """ピーク比ドローダウンに応じた Kelly 減速係数（0.0〜1.0）を返す。

    連敗中の賭け金を機械的に絞ることで、破産確率の裾を直接削る。
    """
    if peak_bankroll <= 0:
        return 1.0
    dd = max(0.0, 1.0 - current_bankroll / peak_bankroll)
    for threshold, multiplier in DRAWDOWN_THROTTLE_BANDS:
        if dd >= threshold:
            return multiplier
    return 1.0


# ── 動的バンクロール ─────────────────────────────────────────────────────
def effective_bankroll(
    conn: sqlite3.Connection, config: BankrollConfig | None = None
) -> tuple[float, float]:
    """確定実損益を反映した (現在バンクロール, ピークバンクロール) を返す。

    実弾の確定 profit を日付順に累積し、複利 Kelly の前提となる
    「いまの資金」と「過去最高資金」を一括算出する。
    """
    cfg = config or BankrollConfig()
    rows = conn.execute(
        """
        SELECT r.date, COALESCE(SUM(pr.profit), 0)
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON r.race_id = p.race_id
        WHERE pr.payout IS NOT NULL
          AND COALESCE(p.is_superseded, 0) = 0
        GROUP BY r.date
        ORDER BY r.date
        """
    ).fetchall()
    bankroll = cfg.initial_bankroll
    peak = bankroll
    for _, daily_profit in rows:
        bankroll += float(daily_profit or 0.0)
        peak = max(peak, bankroll)
    return max(bankroll, 0.0), peak


# ── 同時ベット配分（本体）────────────────────────────────────────────────
def allocate_stakes(
    candidates: list[BetCandidate],
    bankroll: float,
    peak_bankroll: float | None = None,
    config: BankrollConfig | None = None,
) -> AllocationResult:
    """同時ベット群に対する推奨ステークを一括算出する。

    手順:
      1. 各買い目のフル Kelly f* を算出し fractional Kelly を適用。
      2. ドローダウン減速係数を乗算。
      3. 1点上限 (max_bet_pct) でキャップ。
      4. 合計が 1日エクスポージャー上限を超える場合は比例縮約
         （同時 Kelly の過大賭けを防ぐ核心ステップ）。
      5. 100円単位へ丸め、最低購入額未満は 0。
    """
    cfg = config or BankrollConfig()
    throttle = (
        drawdown_throttle(bankroll, peak_bankroll) if peak_bankroll is not None else 1.0
    )
    result = AllocationResult(bankroll=bankroll, throttle=throttle)
    if bankroll <= 0 or throttle <= 0.0:
        result.allocations = [
            BetAllocation(c, full_kelly(c.prob, c.odds), 0.0, 0) for c in candidates
        ]
        return result

    raw: list[tuple[BetCandidate, float, float]] = []
    for c in candidates:
        f_star = full_kelly(c.prob, c.odds)
        f_eff = min(f_star * cfg.kelly_fraction * throttle, cfg.max_bet_pct)
        raw.append((c, f_star, max(f_eff, 0.0)))

    total_fraction = sum(f for _, _, f in raw)
    scale = 1.0
    if total_fraction > cfg.max_daily_exposure_pct > 0:
        scale = cfg.max_daily_exposure_pct / total_fraction
    result.exposure_scale = scale

    for c, f_star, f_eff in raw:
        stake = int(f_eff * scale * bankroll // cfg.min_stake * cfg.min_stake)
        if stake < cfg.min_stake:
            stake = 0
        result.allocations.append(BetAllocation(c, f_star, f_eff * scale, stake))
    return result


# ── 破産確率（モンテカルロ）──────────────────────────────────────────────
def estimate_ruin_probability(
    prob: float,
    odds: float,
    bet_fraction: float,
    n_bets: int = 1_000,
    n_paths: int = 5_000,
    ruin_threshold: float = 0.5,
    seed: int | None = 42,
) -> float:
    """代表的ベット (p, odds) を比率 bet_fraction で n_bets 回複利で張った場合の
    P(バンクロールが初期値の ruin_threshold 倍を下回る) をモンテカルロ推定する。

    ベクトル化された対数資産パスで高速計算。bet_fraction<=0 なら 0.0。
    """
    if bet_fraction <= 0.0 or prob <= 0.0 or odds <= 1.0:
        return 0.0
    f = min(bet_fraction, 0.99)
    rng = np.random.default_rng(seed)
    # 勝ち: ×(1 + f(odds-1)) / 負け: ×(1 - f)
    wins = rng.random((n_paths, n_bets)) < prob
    log_up = np.log1p(f * (odds - 1.0))
    log_dn = np.log1p(-f)
    log_path = np.cumsum(np.where(wins, log_up, log_dn), axis=1)
    ruined = (log_path <= np.log(ruin_threshold)).any(axis=1)
    return float(ruined.mean())


def recommend_kelly_fraction(
    prob: float,
    odds: float,
    target_ruin: float = 0.01,
    n_bets: int = 1_000,
    n_paths: int = 3_000,
    seed: int | None = 42,
) -> float:
    """P(破産) <= target_ruin を満たす最大の Kelly 分数（0.05刻み・上限1.0）を返す。

    フル Kelly 比率 f* に対する「分数」を返す点に注意
    （0.10 = 1/10 Kelly）。条件を満たす分数が無ければ 0.0。
    """
    f_star = full_kelly(prob, odds)
    if f_star <= 0.0:
        return 0.0
    best = 0.0
    for frac in np.arange(0.05, 1.0001, 0.05):
        ruin = estimate_ruin_probability(
            prob, odds, f_star * float(frac), n_bets, n_paths, seed=seed
        )
        if ruin <= target_ruin:
            best = float(frac)
        else:
            break  # 分数単調増加で破産確率も単調増加 → 早期打ち切り
    return round(best, 2)
