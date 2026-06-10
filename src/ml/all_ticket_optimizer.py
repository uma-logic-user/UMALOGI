"""
src/ml/all_ticket_optimizer.py — 全券種 EV 最適化エンジン（着順分布 → オッズ歪み抽出）

各馬の勝率推計（Accuracy Model / 卍較正確率 等）を入力とし、
**割引 Harville（Benter 1994 流・Plackett-Luce 系）** で 1〜3 着の同時確率分布を
精緻に計算、全券種（馬連/馬単/ワイド/三連複/三連単）の的中確率を導出する。
実オッズと比較して「期待値が極めて高い＝市場に歪みがある」組み合わせのみを抽出し、
軸馬フォーメーションを自動生成する。

数学的背景:
  - 素朴な Harville は「2着・3着の条件付き確率」を勝率そのままで縮約するが、
    実データでは 2着以降の予測力は勝率より弱い（Benter 1994, Lo & Bacon-Shone 1994）。
    そこで P^λ に減衰させた割引 Harville を使う:
      P(A→B→C) = pA · qB/(Σq_{≠A}) · rC/(Σr_{≠A,B}),  q=p^λ2, r=p^λ3
    λ2=0.81, λ3=0.65 は Lo & Bacon-Shone の推定値に準拠（cfg で変更可）。
  - 検証用に Plackett-Luce 直接サンプリング（Gumbel-Max トリック）の
    モンテカルロ推定器も同梱（解析式とMCの一致をテストで担保）。

⚠️ 実弾ポリシー（bet_policy）は単勝・複勝のみを実弾とする。本モジュールの出力は
   **分析・観賞用・サブスク向け高付加価値コンテンツ**であり、実弾投票には結線しない。
   三連系の実弾解禁は OOS バックテストでの黒字実証が前提。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations, permutations

import numpy as np

from src.ml.ev_features import JRATakeoutRates

# ── 割引指数（Lo & Bacon-Shone 1994 の推定に準拠・OOS 再較正可）─────────────
LAMBDA_2ND: float = 0.81
LAMBDA_3RD: float = 0.65
# 計算爆発防止: 勝率上位 K 頭のみ着順分布の対象にする（K=8 で三連単 336 通り）
DEFAULT_TOP_K: int = 8
# 「極めて高い EV」の既定閾値（多点券種は推定誤差が乗るため単複より厳しく）
DEFAULT_EV_MIN: float = 1.30
DEFAULT_MAX_BETS: int = 6


@dataclass
class TicketCandidate:
    """1 つの券種×組み合わせ候補。"""

    bet_type: str  # 馬連/馬単/ワイド/三連複/三連単
    combo: tuple[int, ...]  # 馬番（馬単/三連単は着順順序付き）
    prob: float  # モデル確率（割引 Harville）
    odds: float  # 実オッズ（または市場推定オッズ）
    ev: float  # prob × odds

    @property
    def label(self) -> str:
        sep = "→" if self.bet_type in ("馬単", "三連単") else "-"
        return sep.join(str(n) for n in self.combo)


@dataclass
class OptimizerConfig:
    """全券種最適化の設定。"""

    top_k: int = DEFAULT_TOP_K
    lambda_2nd: float = LAMBDA_2ND
    lambda_3rd: float = LAMBDA_3RD
    ev_min: float = DEFAULT_EV_MIN
    max_bets: int = DEFAULT_MAX_BETS
    takeout: JRATakeoutRates = field(default_factory=JRATakeoutRates)


# ── 着順分布（割引 Harville = Plackett-Luce 系）──────────────────────────────
class FinishOrderModel:
    """勝率ベクトルから 1〜3 着の同時確率を計算する。

    horse_numbers と win_probs は同じ並び。win_probs は内部で正規化される。
    """

    def __init__(
        self,
        horse_numbers: list[int],
        win_probs: np.ndarray | list[float],
        lambda_2nd: float = LAMBDA_2ND,
        lambda_3rd: float = LAMBDA_3RD,
    ) -> None:
        p = np.asarray(win_probs, dtype=float)
        p = np.maximum(p, 1e-12)
        self.numbers = list(horse_numbers)
        self.p = p / p.sum()
        self.q = self.p**lambda_2nd  # 2着段の強さ
        self.r = self.p**lambda_3rd  # 3着段の強さ
        self._idx = {num: i for i, num in enumerate(self.numbers)}

    def prob_exacta(self, first: int, second: int) -> float:
        """馬単 (first→second) の確率。"""
        a, b = self._idx[first], self._idx[second]
        if a == b:
            return 0.0
        q_rest = float(self.q.sum() - self.q[a])
        if q_rest <= 0:
            return 0.0
        return float(self.p[a] * self.q[b] / q_rest)

    def prob_trifecta(self, first: int, second: int, third: int) -> float:
        """三連単 (first→second→third) の確率。"""
        a, b, c = self._idx[first], self._idx[second], self._idx[third]
        if len({a, b, c}) < 3:
            return 0.0
        q_rest = float(self.q.sum() - self.q[a])
        r_rest = float(self.r.sum() - self.r[a] - self.r[b])
        if q_rest <= 0 or r_rest <= 0:
            return 0.0
        return float(self.p[a] * (self.q[b] / q_rest) * (self.r[c] / r_rest))

    def prob_quinella(self, x: int, y: int) -> float:
        """馬連 {x, y}（1-2着順不同）の確率。"""
        return self.prob_exacta(x, y) + self.prob_exacta(y, x)

    def prob_trio(self, x: int, y: int, z: int) -> float:
        """三連複 {x, y, z}（1-3着順不同）の確率。"""
        return sum(self.prob_trifecta(a, b, c) for a, b, c in permutations((x, y, z)))

    def prob_wide(self, x: int, y: int, top_k: int = DEFAULT_TOP_K) -> float:
        """ワイド {x, y}（両馬とも3着以内）の確率。

        Σ_{z∈others} P(trio{x,y,z}) で厳密計算（z は上位 top_k に限定して近似）。
        """
        others = [n for n in self._top_numbers(top_k) if n not in (x, y)]
        return sum(self.prob_trio(x, y, z) for z in others)

    def _top_numbers(self, k: int) -> list[int]:
        order = np.argsort(-self.p)
        return [self.numbers[i] for i in order[:k]]

    # ── モンテカルロ検証器（Plackett-Luce 直接サンプリング）────────────────
    def simulate_finish_counts(
        self,
        n_sims: int = 20_000,
        seed: int | None = 42,
    ) -> dict[tuple[int, int, int], float]:
        """Gumbel-Max トリックで着順をサンプリングし三連単頻度を返す。

        2着・3着段にも同じ割引指数を適用するため、解析式（prob_trifecta）の
        近似誤差検証に使える（テストで一致を担保）。
        """
        rng = np.random.default_rng(seed)
        n = len(self.numbers)
        counts: dict[tuple[int, int, int], int] = {}
        log_p = np.log(self.p)
        log_q = np.log(self.q)
        log_r = np.log(self.r)
        for _ in range(n_sims):
            g1 = log_p + rng.gumbel(size=n)
            first = int(np.argmax(g1))
            g2 = log_q + rng.gumbel(size=n)
            g2[first] = -np.inf
            second = int(np.argmax(g2))
            g3 = log_r + rng.gumbel(size=n)
            g3[first] = g3[second] = -np.inf
            third = int(np.argmax(g3))
            key = (self.numbers[first], self.numbers[second], self.numbers[third])
            counts[key] = counts.get(key, 0) + 1
        return {k: v / n_sims for k, v in counts.items()}


# ── 市場オッズ推定（実オッズ未取得時のフォールバック）─────────────────────────
def estimate_combo_odds(
    prob_market: float,
    bet_type: str,
    takeout: JRATakeoutRates | None = None,
) -> float:
    """市場確率から控除率込みの想定オッズを推定する。

    odds ≈ (1 - takeout) / P_market。JVLink 全券種オッズ未取得レースの近似用で、
    **市場確率に自モデル確率を入れると EV が定義上 (1-t) に張り付く**ため、
    必ず市場系の確率（単勝オッズ由来の Shin 確率等から構成）を渡すこと。
    """
    t = takeout or JRATakeoutRates()
    rate = {
        "馬連": t.QUINELLA,
        "ワイド": t.WIDE,
        "馬単": t.EXACTA,
        "三連複": t.TRIO,
        "三連単": t.TRIFECTA,
    }.get(bet_type, 0.25)
    p = max(float(prob_market), 1e-9)
    return (1.0 - rate) / p


# ── 全券種 EV スキャン（本体）────────────────────────────────────────────────
def scan_all_tickets(
    horse_numbers: list[int],
    model_win_probs: np.ndarray | list[float],
    odds_map: dict[str, dict[tuple[int, ...], float]] | None = None,
    market_win_probs: np.ndarray | list[float] | None = None,
    config: OptimizerConfig | None = None,
) -> list[TicketCandidate]:
    """全券種の組み合わせを走査し EV ≥ ev_min の高歪み候補を抽出する。

    Args:
        horse_numbers: 馬番リスト。
        model_win_probs: 自モデルの勝率推計（Accuracy Model 等・正規化不要）。
        odds_map: 実オッズ {券種: {combo: odds}}。combo は馬単/三連単=順序付き、
                  馬連/ワイド/三連複=昇順ソート済み tuple。
        market_win_probs: 市場勝率（単勝オッズ由来 Shin 確率等）。実オッズが無い
                  券種のオッズ推定に使用。None なら実オッズの無い券種はスキップ。
        config: 設定。

    Returns:
        EV 降順の TicketCandidate（最大 max_bets 件）。
    """
    cfg = config or OptimizerConfig()
    model = FinishOrderModel(
        horse_numbers, model_win_probs, cfg.lambda_2nd, cfg.lambda_3rd
    )
    market_model = (
        FinishOrderModel(
            horse_numbers, market_win_probs, cfg.lambda_2nd, cfg.lambda_3rd
        )
        if market_win_probs is not None
        else None
    )
    top = model._top_numbers(cfg.top_k)
    odds_map = odds_map or {}

    def _odds_for(
        bet_type: str, combo: tuple[int, ...], market_prob_fn: object
    ) -> float | None:
        book = odds_map.get(bet_type, {})
        if combo in book and book[combo] > 1.0:
            return float(book[combo])
        if market_model is None:
            return None
        # 実オッズ未取得 → 市場確率からの推定オッズ（市場とモデルの分布差が EV 源泉）
        p_mkt = float(market_prob_fn())  # type: ignore[operator]
        if p_mkt <= 0:
            return None
        return estimate_combo_odds(p_mkt, bet_type, cfg.takeout)

    candidates: list[TicketCandidate] = []

    # 2頭系（馬連・ワイド・馬単）
    for x, y in combinations(top, 2):
        pairs: list[tuple[str, tuple[int, ...], float, object]] = [
            (
                "馬連",
                (x, y),
                model.prob_quinella(x, y),
                lambda x=x, y=y: (
                    market_model.prob_quinella(x, y) if market_model else 0.0
                ),
            ),
            (
                "ワイド",
                (x, y),
                model.prob_wide(x, y, cfg.top_k),
                lambda x=x, y=y: (
                    market_model.prob_wide(x, y, cfg.top_k) if market_model else 0.0
                ),
            ),
            (
                "馬単",
                (x, y),
                model.prob_exacta(x, y),
                lambda x=x, y=y: (
                    market_model.prob_exacta(x, y) if market_model else 0.0
                ),
            ),
            (
                "馬単",
                (y, x),
                model.prob_exacta(y, x),
                lambda x=x, y=y: (
                    market_model.prob_exacta(y, x) if market_model else 0.0
                ),
            ),
        ]
        for bet_type, combo, prob, mkt_fn in pairs:
            key = combo if bet_type == "馬単" else tuple(sorted(combo))
            odds = _odds_for(bet_type, key, mkt_fn)
            if odds is None or prob <= 0:
                continue
            ev = prob * odds
            if ev >= cfg.ev_min:
                candidates.append(TicketCandidate(bet_type, combo, prob, odds, ev))

    # 3頭系（三連複・三連単）
    for x, y, z in combinations(top, 3):
        combo3 = tuple(sorted((x, y, z)))
        prob_trio = model.prob_trio(x, y, z)
        odds = _odds_for(
            "三連複",
            combo3,
            lambda x=x, y=y, z=z: (
                market_model.prob_trio(x, y, z) if market_model else 0.0
            ),
        )
        if odds is not None and prob_trio > 0:
            ev = prob_trio * odds
            if ev >= cfg.ev_min:
                candidates.append(
                    TicketCandidate("三連複", combo3, prob_trio, odds, ev)
                )
        for a, b, c in permutations((x, y, z)):
            prob_tri = model.prob_trifecta(a, b, c)
            odds = _odds_for(
                "三連単",
                (a, b, c),
                lambda a=a, b=b, c=c: (
                    market_model.prob_trifecta(a, b, c) if market_model else 0.0
                ),
            )
            if odds is None or prob_tri <= 0:
                continue
            ev = prob_tri * odds
            if ev >= cfg.ev_min:
                candidates.append(
                    TicketCandidate("三連単", (a, b, c), prob_tri, odds, ev)
                )

    candidates.sort(key=lambda c: c.ev, reverse=True)
    return candidates[: cfg.max_bets]


# ── フォーメーション生成 ──────────────────────────────────────────────────────
@dataclass
class Formation:
    """軸馬フォーメーション（人間が買いやすい形への要約）。"""

    bet_type: str
    axis: list[int]  # 軸馬（全候補に共通して現れる馬）
    partners: list[int]  # 相手馬
    n_points: int  # 点数
    total_ev: float  # 候補 EV の合計
    candidates: list[TicketCandidate] = field(default_factory=list)


def build_formation(candidates: list[TicketCandidate]) -> list[Formation]:
    """券種別に高EV候補を軸-相手フォーメーションへ要約する。

    軸＝その券種の全候補に出現する馬。相手＝それ以外の出現馬。
    """
    formations: list[Formation] = []
    by_type: dict[str, list[TicketCandidate]] = {}
    for c in candidates:
        by_type.setdefault(c.bet_type, []).append(c)
    for bet_type, group in by_type.items():
        sets = [set(c.combo) for c in group]
        axis = sorted(set.intersection(*sets)) if sets else []
        partners = sorted(set.union(*sets) - set(axis)) if sets else []
        formations.append(
            Formation(
                bet_type=bet_type,
                axis=axis,
                partners=partners,
                n_points=len(group),
                total_ev=round(sum(c.ev for c in group), 3),
                candidates=group,
            )
        )
    formations.sort(key=lambda f: f.total_ev, reverse=True)
    return formations
