"""
src/ml/gachi_trifecta.py — 3連系「本気」アンサンブルモデル（Task3 / W-095）

目的:
  3連複・3連単・馬連・馬単を「ガチで当てにいく」買い目構築エンジン。
  単一モデルの EV 上位だけで流す従来の AlphaTrifectaStrategy と異なり、
  **役割の異なる2系統のスコアをアンサンブル**して軸と紐を選び分ける:

    ・軸（アンカー）: 本命モデル（勝率）× 複勝特化モデル（複勝率）の双方が高い
      = 「ちゃんと馬券圏に来る担保のある馬」。崩れにくさを最優先。
    ・紐（パートナー）: 卍 EV（期待値）が高い妙味馬（人気の盲点＝穴）。
      回収率の源泉。軸に絡めて配当を取りにいく。

  これにより「的中率（軸の信頼性）× 回収率（紐の妙味）」のバランスを最大化する。

設計方針:
  - 確率推定は Harville 公式（勝率ベクトルから順序付き確率を導出）。
  - EV は OddsEstimator 経由（オッズがあれば実 EV、無ければ確率ベースの相対評価）。
  - オッズ未確定（暫定）でも軸・紐選定と買い目構築は機能する（確率のみで動く）。
  - 既存の実弾単複ロック（bet_policy）とは独立した「3連系研究/プレミアム枠」。
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

import pandas as pd

from src.ml.bet_generator import (
    OddsEstimator,
    _harville_exacta,
    _harville_quinella,
    _harville_trifecta,
    _harville_trio,
    _name_map,
)


@dataclass
class GachiBet:
    """1つの3連系買い目推奨。"""

    bet_type: str  # 馬連 / 馬単 / 三連複 / 三連単
    combinations: list[tuple[int, ...]]
    horse_names: list[str]
    expected_value: float  # オッズがある場合の EV（無ければ 0.0）
    model_score: float  # 代表組の的中確率（Harville）
    confidence: float
    notes: str = ""


@dataclass
class GachiTrifectaBets:
    """1レースの3連系本気買い目一式。"""

    race_id: str
    model_type: str = "本気3連系"
    axis: list[int] = field(default_factory=list)
    partners: list[int] = field(default_factory=list)
    bets: list[GachiBet] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "race_id": self.race_id,
            "model_type": self.model_type,
            "axis": self.axis,
            "partners": self.partners,
            "bets": [
                {
                    "bet_type": b.bet_type,
                    "combinations": [list(c) for c in b.combinations],
                    "horse_names": b.horse_names,
                    "expected_value": round(b.expected_value, 3),
                    "model_score": round(b.model_score, 5),
                    "confidence": round(b.confidence, 3),
                    "notes": b.notes,
                }
                for b in self.bets
            ],
        }


# ── チューニング定数 ──────────────────────────────────────────────────────────
_N_AXIS = 2  # 軸頭数（◎〇相当の担保馬）
_N_PARTNERS = 4  # 紐頭数（高EV穴）
_MAX_UMAREN = 6
_MAX_UMATAN = 6
_MAX_TRIO = 8  # 三連複点数
_MAX_TRIFECTA = 12  # 三連単点数
# 軸の最低複勝率担保（これ未満は軸不適格）。0で無効化。
_AXIS_PLACE_FLOOR = 0.0


def _safe_series(s: pd.Series | None, n: int) -> list[float]:
    if s is None:
        return [0.0] * n
    out: list[float] = []
    for i in range(n):
        v = float(s.iloc[i]) if i < len(s) else 0.0
        out.append(0.0 if pd.isna(v) else v)
    return out


def _rank_desc(values: list[float]) -> dict[int, int]:
    """index -> 0始まり降順順位。"""
    order = sorted(range(len(values)), key=lambda i: values[i], reverse=True)
    return {idx: rank for rank, idx in enumerate(order)}


def select_axis_and_partners(
    df: pd.DataFrame,
    honmei_scores: pd.Series,
    place_scores: pd.Series | None,
    ev_scores: pd.Series | None,
    *,
    n_axis: int = _N_AXIS,
    n_partners: int = _N_PARTNERS,
) -> tuple[list[int], list[int]]:
    """アンサンブルで軸（担保馬）と紐（高EV穴）を選定する。

    軸: 本命勝率ランク + 複勝率ランクの合算が小さい（=両方上位）馬から n_axis 頭。
    紐: 軸を除いた中で卍EVが高い順に n_partners 頭（EV欠損時は本命勝率で代替）。

    Returns:
        (axis_horse_numbers, partner_horse_numbers)
    """
    df_reset = df.reset_index(drop=True)
    n = len(df_reset)
    if n == 0:
        return [], []
    nums = [int(r["horse_number"]) for _, r in df_reset.iterrows()]

    win = _safe_series(honmei_scores, n)
    place = _safe_series(place_scores, n) if place_scores is not None else win
    ev = _safe_series(ev_scores, n) if ev_scores is not None else win

    win_rank = _rank_desc(win)
    place_rank = _rank_desc(place)

    # 軸候補: 勝率ランク + 複勝率ランク（小さいほど信頼）。複勝率の最低担保を満たす馬のみ。
    axis_candidates = [i for i in range(n) if place[i] >= _AXIS_PLACE_FLOOR] or list(
        range(n)
    )
    axis_candidates.sort(key=lambda i: win_rank[i] + place_rank[i])
    axis_idx = axis_candidates[:n_axis]
    axis_nums = [nums[i] for i in axis_idx]

    # 紐: 軸以外を EV 降順（妙味＝穴）。EV が全て同値/0なら本命勝率で代替。
    rest = [i for i in range(n) if i not in axis_idx]
    use_ev = any(ev[i] != ev[rest[0]] for i in rest) if rest else False
    key = (lambda i: ev[i]) if use_ev else (lambda i: win[i])
    rest.sort(key=key, reverse=True)
    partner_idx = rest[:n_partners]
    partner_nums = [nums[i] for i in partner_idx]

    return axis_nums, partner_nums


def build_gachi_trifecta(
    race_id: str,
    df: pd.DataFrame,
    honmei_scores: pd.Series,
    place_scores: pd.Series | None = None,
    ev_scores: pd.Series | None = None,
    *,
    estimator: OddsEstimator | None = None,
) -> GachiTrifectaBets:
    """3連系本気アンサンブル買い目を構築する。

    Args:
        race_id: レースID。
        df: 出走馬 DataFrame（horse_number / win_odds / horse_name）。
        honmei_scores: 本命モデル勝率スコア（df 行と同順）。
        place_scores: 複勝特化スコア（無ければ honmei で代替）。
        ev_scores: 卍 EV スコア（紐の妙味判定用）。
        estimator: OddsEstimator（省略時デフォルト）。

    Returns:
        GachiTrifectaBets（馬連/馬単/三連複/三連単）。出走3頭未満なら空。
    """
    est = estimator or OddsEstimator()
    df_reset = df.reset_index(drop=True)
    n = len(df_reset)
    result = GachiTrifectaBets(race_id=race_id)
    if n < 3:
        return result

    nums = [int(r["horse_number"]) for _, r in df_reset.iterrows()]
    names = _name_map(df_reset)
    idx_of = {num: i for i, num in enumerate(nums)}

    # Harville 用の勝率ベクトル（本命勝率を正規化）。
    win = _safe_series(honmei_scores, n)
    total = sum(max(w, 0.0) for w in win)
    probs = [max(w, 0.0) / total if total > 0 else 1.0 / n for w in win]

    axis_nums, partner_nums = select_axis_and_partners(
        df_reset, honmei_scores, place_scores, ev_scores
    )
    result.axis = axis_nums
    result.partners = partner_nums
    if not axis_nums:
        return result

    primary = axis_nums[0]
    p_idx = idx_of[primary]

    def _axis_odds(num: int) -> float:
        if "win_odds" not in df_reset.columns:
            return 10.0
        sub = df_reset[df_reset["horse_number"] == num]["win_odds"]
        if sub.isna().all() or sub.empty:
            return 10.0
        return float(sub.iloc[0]) or 10.0

    axis_odds = _axis_odds(primary)
    # 軸＋紐の候補プール（重複排除・順序保持）。
    pool = list(dict.fromkeys(axis_nums + partner_nums))

    # ── 馬連: 軸(primary) × その他プール ──────────────────────────────────
    umaren: list[tuple[float, float, tuple[int, int]]] = []
    for other in pool:
        if other == primary:
            continue
        q = _harville_quinella(probs, p_idx, idx_of[other])
        ev_v = est.ev(q, "馬連", axis_odds)
        umaren.append((ev_v, q, tuple(sorted((primary, other)))))
    umaren.sort(reverse=True)
    if umaren:
        combos = [c for _, _, c in umaren[:_MAX_UMAREN]]
        result.bets.append(
            GachiBet(
                bet_type="馬連",
                combinations=combos,
                horse_names=[names.get(x, str(x)) for x in combos[0]],
                expected_value=umaren[0][0],
                model_score=umaren[0][1],
                confidence=min(umaren[0][1] * 3, 1.0),
                notes=f"軸{primary}（担保）×紐（高EV穴）馬連 {len(combos)}点",
            )
        )

    # ── 馬単: 軸 → 紐（軸1着固定）──────────────────────────────────────────
    umatan: list[tuple[float, float, tuple[int, int]]] = []
    for other in pool:
        if other == primary:
            continue
        ex = _harville_exacta(probs, p_idx, idx_of[other])
        ev_v = est.ev(ex, "馬単", axis_odds)
        umatan.append((ev_v, ex, (primary, other)))
    umatan.sort(reverse=True)
    if umatan:
        combos = [c for _, _, c in umatan[:_MAX_UMATAN]]
        result.bets.append(
            GachiBet(
                bet_type="馬単",
                combinations=combos,
                horse_names=[names.get(x, str(x)) for x in combos[0]],
                expected_value=umatan[0][0],
                model_score=umatan[0][1],
                confidence=min(umatan[0][1] * 3, 1.0),
                notes=f"軸{primary}→紐（高EV穴）馬単 {len(combos)}点",
            )
        )

    # ── 三連複: 軸固定 × 紐から2頭 ─────────────────────────────────────────
    others = [x for x in pool if x != primary]
    trio: list[tuple[float, float, tuple[int, int, int]]] = []
    for b, c in itertools.combinations(others, 2):
        tp = _harville_trio(probs, p_idx, idx_of[b], idx_of[c])
        ev_v = est.ev(tp, "三連複", axis_odds)
        trio.append((ev_v, tp, tuple(sorted((primary, b, c)))))
    trio.sort(reverse=True)
    if trio:
        combos = [c for _, _, c in trio[:_MAX_TRIO]]
        result.bets.append(
            GachiBet(
                bet_type="三連複",
                combinations=combos,
                horse_names=[names.get(x, str(x)) for x in combos[0]],
                expected_value=trio[0][0],
                model_score=trio[0][1],
                confidence=min(trio[0][1] * 5, 1.0),
                notes=f"軸{primary}固定×紐2頭の三連複フォーメーション {len(combos)}点",
            )
        )

    # ── 三連単: 軸1着固定 → 紐から2着3着（順列）──────────────────────────
    trifecta: list[tuple[float, float, tuple[int, int, int]]] = []
    for b, c in itertools.permutations(others, 2):
        tf = _harville_trifecta(probs, p_idx, idx_of[b], idx_of[c])
        ev_v = est.ev(tf, "三連単", axis_odds)
        trifecta.append((ev_v, tf, (primary, b, c)))
    trifecta.sort(reverse=True)
    if trifecta:
        combos = [c for _, _, c in trifecta[:_MAX_TRIFECTA]]
        result.bets.append(
            GachiBet(
                bet_type="三連単",
                combinations=combos,
                horse_names=[names.get(x, str(x)) for x in combos[0]],
                expected_value=trifecta[0][0],
                model_score=trifecta[0][1],
                confidence=min(trifecta[0][1] * 8, 1.0),
                notes=f"軸{primary}1着固定→紐の三連単軸流し {len(combos)}点",
            )
        )

    return result
