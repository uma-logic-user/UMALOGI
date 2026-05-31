"""
Pure_EV_Edge — 黒字化専用 単複ロジック（独立バリアント）

確定実績分析([[project_pnl_truth_strategy]])の結論「単勝に実エッジ・三連系は損失主因・
卍は唯一の勝ち頭だが停止中」を受け、**期待値(EV)のみを純粋追求する独立枠**として新設。
集客用・観賞用モデル(Oracle/HitFocus 等)のコードには一切手を付けない。

厳格にコードロックされたルール:
  1. 券種は「単勝」「複勝」のみ（多点買い・三連系を完全排除）。
  2. 卍モデルの Isotonic 較正済み確率をベースとした EV フィルタ（既定 EV>=1.15）。
  3. 資金管理は 1/10 Kelly（券種別 cap 付き・過大ベット防止）。
     1日/1週の損失上限（サーキットブレーカー）標準装備。

本モジュールは「EV 計算」「買い目選定」「Kelly」「サーキットブレーカー」を提供する
単一真実源。本番(prediction)・バックテスト(scripts)・健全性レポートが共有する。
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date

from src.ml.manji_calibration import calibrate_win_prob

logger = logging.getLogger(__name__)

# ── コードロックされた定数 ───────────────────────────────────────────────
PURE_EV_MODEL_NAME = "Pure_EV_Edge"  # model_type / UI 表示名
PURE_EV_THRESHOLD: float = 1.15  # 期待値フィルタ下限（安全マージン）
PURE_KELLY_FRACTION: float = 0.10  # 1/10 Kelly

_LOCKED_BET_TYPES: tuple[str, ...] = ("単勝", "複勝")
# 券種別 Kelly 上限（バンクロール比・過大ベット防止）
_KELLY_TYPE_CAP: dict[str, float] = {"単勝": 0.02, "複勝": 0.03}
_FUKUSHO_EV_CAP: float = 3.0  # 複勝 EV の上限（推定誤差の暴走防止）
_DEFAULT_PLACE_SCALE: float = 0.33  # 複勝場 odds 推定係数（win_odds から）

# 規律（原理的・結果非依存）: 較正確率がこれ未満の大穴は除外。単勝の実エッジは有力馬に在る。
_PROB_FLOOR: float = 0.06
_MAX_BETS_PER_RACE: int = 2


@dataclass
class PureEVConfig:
    """Pure_EV_Edge の設定（バンクロール・閾値・サーキットブレーカー）。"""

    initial_bankroll: float = 100_000.0
    ev_threshold: float = PURE_EV_THRESHOLD
    kelly_fraction: float = PURE_KELLY_FRACTION
    daily_loss_limit_pct: float = 0.05  # 1日損失上限（バンクロール比）
    weekly_loss_limit_pct: float = 0.12  # 1週損失上限
    prob_floor: float = _PROB_FLOOR
    max_bets_per_race: int = _MAX_BETS_PER_RACE


@dataclass
class CircuitBreakerStatus:
    """サーキットブレーカー判定結果。"""

    tripped: bool
    reason: str
    daily_loss: float = 0.0
    weekly_loss: float = 0.0


@dataclass
class PureEVBet:
    """Pure_EV_Edge の 1 買い目（常に単一馬・単勝 or 複勝）。"""

    race_id: str
    bet_type: str  # "単勝" / "複勝"
    horse_number: int
    horse_name: str
    odds: float  # 単勝オッズ（複勝は推定場 odds）
    prob: float  # 較正済み確率（単勝=P(win) / 複勝=P(place)）
    expected_value: float  # EV = prob * odds
    stake: int  # 1/10 Kelly 推奨購入額（円）


@dataclass
class PureEVRaceBets:
    """1 レースの Pure_EV_Edge 買い目。"""

    race_id: str
    bets: list[PureEVBet] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "race_id": self.race_id,
            "model_type": PURE_EV_MODEL_NAME,
            "bets": [
                {
                    "bet_type": b.bet_type,
                    "horse_number": b.horse_number,
                    "horse_name": b.horse_name,
                    "odds": b.odds,
                    "prob": round(b.prob, 4),
                    "expected_value": round(b.expected_value, 3),
                    "stake": b.stake,
                }
                for b in self.bets
            ],
        }


# ── 券種ロック ───────────────────────────────────────────────────────────
def is_locked_bet_type(bet_type: str) -> bool:
    """Pure_EV_Edge が許可する券種（単勝/複勝）か。"""
    return bet_type in _LOCKED_BET_TYPES


# ── EV 計算 ──────────────────────────────────────────────────────────────
def tansho_ev(manji_ev_score: float, win_odds: float) -> tuple[float, float]:
    """単勝の (較正済みP(win), EV) を返す。

    卍 ev_score を Isotonic 較正して勝率を得る（係数膨張なし・1.0 飽和なし）。

    Returns:
        ``(prob, ev)``。win_odds<=1.0 のときは ``(0.0, 0.0)``。
    """
    if win_odds <= 1.0:
        return 0.0, 0.0
    p = calibrate_win_prob(float(manji_ev_score), float(win_odds))
    return p, p * float(win_odds)


def fukusho_ev(
    place_prob: float,
    win_odds: float,
    place_payout_scale: float = _DEFAULT_PLACE_SCALE,
    place_odds: float | None = None,
) -> tuple[float, float]:
    """複勝の (P(place), EV) を返す。

    place_odds が与えられればそれを使い、無ければ win_odds から推定場 odds
    ``1 + (win_odds-1)*scale`` で近似する（EV は _FUKUSHO_EV_CAP で頭打ち）。
    """
    pp = float(place_prob)
    if place_odds is not None and place_odds > 0:
        return pp, pp * float(place_odds)
    eff = 1.0 + (max(float(win_odds), 1.0) - 1.0) * place_payout_scale
    return pp, min(pp * eff, _FUKUSHO_EV_CAP)


# ── 1/10 Kelly ───────────────────────────────────────────────────────────
def kelly_stake(
    ev: float,
    odds: float,
    bankroll: float,
    bet_type: str,
    kelly_fraction: float = PURE_KELLY_FRACTION,
) -> int:
    """1/10 Kelly の推奨購入額（円・100円単位・券種別 cap 付き）。

    Kelly f* = (EV-1)/(odds-1)（EV=prob*odds より標準 Kelly と一致）。
    負エッジ・不正オッズは 0。
    """
    if ev <= 1.0 or odds <= 1.0:
        return 0
    f = (ev - 1.0) / (odds - 1.0)
    if f <= 0:
        return 0
    raw = f * kelly_fraction * bankroll
    cap = bankroll * _KELLY_TYPE_CAP.get(bet_type, 0.02)
    stake = int(min(raw, cap) // 100 * 100)
    return stake if stake >= 100 else 0


# ── 買い目選定（単複のみ・EV フィルタ・厳選）───────────────────────────────
def select_pure_ev_bets(
    race_id: str,
    horses: list[dict],
    config: PureEVConfig | None = None,
) -> PureEVRaceBets:
    """1 レースの出走馬から Pure_EV_Edge 買い目（単複のみ）を選定する純関数。

    Args:
        race_id: レース ID。
        horses: ``{horse_number, horse_name, win_odds, manji_ev_score, place_prob}``
                の dict リスト。
        config: 設定（省略時は既定 PureEVConfig）。

    Returns:
        PureEVRaceBets（EV 降順・最大 max_bets_per_race 点）。
    """
    cfg = config or PureEVConfig()
    bets: list[PureEVBet] = []
    for h in horses:
        try:
            num = int(h["horse_number"])
            odds = float(h["win_odds"])
        except (KeyError, TypeError, ValueError):
            continue
        if num < 1 or odds <= 1.0:
            continue
        name = str(h.get("horse_name", num))
        mev = float(h.get("manji_ev_score", 0.0) or 0.0)
        pprob = float(h.get("place_prob", 0.0) or 0.0)

        # 単勝
        p_win, ev_win = tansho_ev(mev, odds)
        if p_win >= cfg.prob_floor and ev_win >= cfg.ev_threshold:
            stake = kelly_stake(
                ev_win, odds, cfg.initial_bankroll, "単勝", cfg.kelly_fraction
            )
            if stake >= 100:
                bets.append(
                    PureEVBet(
                        race_id,
                        "単勝",
                        num,
                        name,
                        round(odds, 1),
                        round(p_win, 4),
                        round(ev_win, 3),
                        stake,
                    )
                )

        # 複勝（有力馬=単勝確率フロアを満たす馬のみ）
        p_place, ev_place = fukusho_ev(pprob, odds)
        if p_win >= cfg.prob_floor and ev_place >= cfg.ev_threshold:
            podds = round(1.0 + (odds - 1.0) * _DEFAULT_PLACE_SCALE, 2)
            stake = kelly_stake(
                ev_place, podds, cfg.initial_bankroll, "複勝", cfg.kelly_fraction
            )
            if stake >= 100:
                bets.append(
                    PureEVBet(
                        race_id,
                        "複勝",
                        num,
                        name,
                        podds,
                        round(p_place, 4),
                        round(ev_place, 3),
                        stake,
                    )
                )

    bets.sort(key=lambda b: b.expected_value, reverse=True)
    return PureEVRaceBets(race_id, bets[: cfg.max_bets_per_race])


# ── サーキットブレーカー ─────────────────────────────────────────────────
def evaluate_circuit_breaker(
    daily_pnl: float, weekly_pnl: float, config: PureEVConfig
) -> CircuitBreakerStatus:
    """日次/週次損益から損失上限到達（サーキットブレーカー作動）を判定する。"""
    daily_limit = config.initial_bankroll * config.daily_loss_limit_pct
    weekly_limit = config.initial_bankroll * config.weekly_loss_limit_pct
    daily_loss = max(0.0, -float(daily_pnl))
    weekly_loss = max(0.0, -float(weekly_pnl))
    if daily_loss >= daily_limit:
        return CircuitBreakerStatus(
            True,
            f"日次損失上限 ¥{daily_limit:,.0f} 到達（損失 ¥{daily_loss:,.0f}）",
            daily_loss,
            weekly_loss,
        )
    if weekly_loss >= weekly_limit:
        return CircuitBreakerStatus(
            True,
            f"週次損失上限 ¥{weekly_limit:,.0f} 到達（損失 ¥{weekly_loss:,.0f}）",
            daily_loss,
            weekly_loss,
        )
    return CircuitBreakerStatus(False, "OK", daily_loss, weekly_loss)


def _iso_week_range(date_str: str) -> tuple[str, str]:
    """date_str(YYYY-MM-DD) の属する ISO 週の月曜〜日曜を返す。"""
    y, m, d = (int(x) for x in date_str[:10].split("-"))
    dt = date(y, m, d)
    monday = dt.fromordinal(dt.toordinal() - dt.weekday())
    sunday = dt.fromordinal(monday.toordinal() + 6)
    return monday.isoformat(), sunday.isoformat()


def get_period_pnl(conn: sqlite3.Connection, start_date: str, end_date: str) -> float:
    """期間内の Pure_EV_Edge 確定損益（profit 合計）を返す（他モデルは除外）。"""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(pr.profit), 0)
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON r.race_id = p.race_id
        WHERE p.model_type LIKE ?
          AND r.date BETWEEN ? AND ?
        """,
        (f"{PURE_EV_MODEL_NAME}%", start_date, end_date),
    ).fetchone()
    return float(row[0] or 0.0)


def circuit_breaker_status(
    conn: sqlite3.Connection, date_str: str, config: PureEVConfig | None = None
) -> CircuitBreakerStatus:
    """DB の確定損益から当日のサーキットブレーカー状態を判定する。"""
    cfg = config or PureEVConfig()
    daily_pnl = get_period_pnl(conn, date_str, date_str)
    wk_start, wk_end = _iso_week_range(date_str)
    weekly_pnl = get_period_pnl(conn, wk_start, wk_end)
    return evaluate_circuit_breaker(daily_pnl, weekly_pnl, cfg)
