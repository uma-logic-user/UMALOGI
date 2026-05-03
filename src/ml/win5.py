"""
WIN5 予測エンジン

WIN5 は JRA が指定した5レースの勝馬を全て的中させる馬券。
払戻は「キャリーオーバー込みの売上の約70%」を的中票数で割った額になる。

アルゴリズム:
  1. 各レースの単勝オッズ (win_odds) から市場確率を推定
  2. モデル予測確率とブレンド（デフォルト: 50% ずつ）
  3. 5レースの勝率の積を期待確率として算出
  4. 推定払戻 = 1 / (5レース市場確率の積) × ターゲット係数
  5. 期待値 (EV) = 期待確率 × 推定払戻 / 100 (単位: 100円)
  6. EV > 1.0 の組み合わせを推奨買い目として返す

Usage:
    engine = Win5Engine(model=honmei_model)
    bets = engine.predict(conn, race_ids=["...", "...", "...", "...", "..."])
    for bet in bets[:10]:
        print(bet)
"""

from __future__ import annotations

import itertools
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .models import HonmeiModel

logger = logging.getLogger(__name__)

# WIN5 の理論払戻率（JRA 公式: 72.5% 返還）
_WIN5_RETURN_RATE = 0.725

# 市場確率とモデル確率のブレンド比率（model : market）
_MODEL_BLEND = 0.50
_MARKET_BLEND = 0.50

# 単勝オッズの最小値（0.0 防止用）
_MIN_ODDS = 1.1

# デフォルトの最大推奨買い目数
_DEFAULT_MAX_BETS = 20

# EV フィルター閾値（これ以上を推奨候補とする）
_EV_THRESHOLD = 1.0


@dataclass
class Win5HorsePick:
    """1レースで選んだ1頭の情報。"""
    race_id:     str
    horse_name:  str
    horse_number: int
    win_odds:    float
    market_prob: float    # オッズ逆数正規化後の確率
    model_prob:  float    # モデル予測の勝率
    blend_prob:  float    # ブレンド後の勝率


@dataclass
class Win5Combination:
    """5レース×1頭の買い目組み合わせ。"""
    picks:            list[Win5HorsePick]      # race_id の順に並ぶ
    combined_prob:    float                    # 5頭の勝率の積
    estimated_payout: float                   # 推定払戻（100円ベース）
    expected_value:   float                   # 期待値 = prob × payout / 100
    recommended_bet:  float = 100.0

    def __str__(self) -> str:
        horses = " / ".join(f"{p.horse_name}({p.win_odds:.1f}倍)" for p in self.picks)
        return (
            f"WIN5: {horses}\n"
            f"  推定払戻: ¥{self.estimated_payout:,.0f}  EV: {self.expected_value:.3f}"
        )


class Win5Engine:
    """
    WIN5 買い目生成・期待値計算エンジン。

    Args:
        model:        HonmeiModel。None の場合はオッズのみで確率を推定。
        max_bets:     返す買い目の最大数。
        ev_threshold: 推奨買い目の EV 最低値。
        model_blend:  モデル確率のブレンド比率（0〜1）。
    """

    def __init__(
        self,
        model: "HonmeiModel | None" = None,
        *,
        max_bets:     int   = _DEFAULT_MAX_BETS,
        ev_threshold: float = _EV_THRESHOLD,
        model_blend:  float = _MODEL_BLEND,
    ) -> None:
        self._model       = model
        self._max_bets    = max_bets
        self._ev_threshold = ev_threshold
        self._model_blend  = model_blend
        self._market_blend = 1.0 - model_blend

    # ── パブリック API ────────────────────────────────────────────

    def predict(
        self,
        conn: sqlite3.Connection,
        race_ids: list[str],
    ) -> list[Win5Combination]:
        """
        5レース分の race_id を受け取り、推奨 WIN5 買い目を返す。

        Args:
            conn:     DB コネクション
            race_ids: WIN5 対象の5レース ID リスト（順序通り）

        Returns:
            EV 降順の Win5Combination リスト
        """
        if len(race_ids) != 5:
            raise ValueError(f"WIN5 は5レース必要です (got {len(race_ids)})")

        # 各レースの候補馬リストを取得
        race_picks: list[list[Win5HorsePick]] = []
        for race_id in race_ids:
            picks = self._get_picks(conn, race_id)
            if not picks:
                logger.warning("race_id=%s: 出走馬データなし", race_id)
                return []
            race_picks.append(picks)

        # 全組み合わせを生成して EV 計算
        combinations = self._enumerate_combinations(race_picks)
        combinations.sort(key=lambda c: c.expected_value, reverse=True)

        filtered = [c for c in combinations if c.expected_value >= self._ev_threshold]
        logger.info(
            "WIN5 候補: 全%d件 / EV>=%.1f: %d件",
            len(combinations), self._ev_threshold, len(filtered),
        )
        return filtered[: self._max_bets]

    def predict_top_n(
        self,
        conn: sqlite3.Connection,
        race_ids: list[str],
        *,
        top_n_per_race: int = 3,
    ) -> list[Win5Combination]:
        """
        各レースで上位 top_n 頭のみ選んで組み合わせを限定する（高速版）。

        フルベット数 = top_n ^ 5 = 3^5 = 243 通り
        """
        if len(race_ids) != 5:
            raise ValueError(f"WIN5 は5レース必要です (got {len(race_ids)})")

        race_picks: list[list[Win5HorsePick]] = []
        for race_id in race_ids:
            picks = self._get_picks(conn, race_id)
            if not picks:
                return []
            # blend_prob 上位 N 頭に絞る
            race_picks.append(
                sorted(picks, key=lambda p: p.blend_prob, reverse=True)[:top_n_per_race]
            )

        combinations = self._enumerate_combinations(race_picks)
        combinations.sort(key=lambda c: c.expected_value, reverse=True)
        return combinations[: self._max_bets]

    # ── 内部実装 ──────────────────────────────────────────────────

    def _get_picks(
        self,
        conn: sqlite3.Connection,
        race_id: str,
    ) -> list[Win5HorsePick]:
        """エントリ or レース結果から出走馬リストと確率を生成する。"""
        # realtime_odds → race_results の順でフォールバック
        rows: list = conn.execute(
            """
            SELECT horse_name, horse_number, win_odds
            FROM realtime_odds
            WHERE race_id = ? AND win_odds IS NOT NULL AND win_odds > 0
            ORDER BY horse_number
            """,
            (race_id,),
        ).fetchall()

        if not rows:
            rows = conn.execute(
                """
                SELECT horse_name, horse_number, win_odds
                FROM race_results
                WHERE race_id = ? AND win_odds IS NOT NULL AND win_odds > 0
                ORDER BY horse_number
                """,
                (race_id,),
            ).fetchall()

        if not rows:
            return []

        # 市場確率: 1/odds を正規化
        names    = [r[0] for r in rows]
        numbers  = [r[1] for r in rows]
        odds_arr = [max(float(r[2]), _MIN_ODDS) for r in rows]
        raw_probs = [1.0 / o for o in odds_arr]
        total     = sum(raw_probs)
        market_probs = [p / total for p in raw_probs]

        # モデル予測確率（未訓練の場合は市場確率をそのまま使用）
        model_probs = self._get_model_probs(conn, race_id, names)

        picks = []
        for name, number, odds, mp, mdp in zip(
            names, numbers, odds_arr, market_probs, model_probs
        ):
            blend = self._model_blend * mdp + self._market_blend * mp
            picks.append(Win5HorsePick(
                race_id=race_id,
                horse_name=name,
                horse_number=number,
                win_odds=odds,
                market_prob=mp,
                model_prob=mdp,
                blend_prob=blend,
            ))
        return picks

    def _get_model_probs(
        self,
        conn: sqlite3.Connection,
        race_id: str,
        horse_names: list[str],
    ) -> list[float]:
        """
        モデルが利用可能な場合は予測スコアを正規化して返す。
        未訓練または列取得失敗時は等確率を返す。
        """
        n = len(horse_names)
        if n == 0:
            return []

        if self._model is None or not self._model.is_trained:
            return [1.0 / n] * n

        try:
            from .features import FeatureBuilder
            fb = FeatureBuilder(conn)
            df = fb.build_race_features(race_id)
            if df.empty:
                return [1.0 / n] * n

            scores = self._model.predict(df)
            # horse_name でインデックス合わせ
            score_map = dict(zip(df["horse_name"].tolist(), scores.tolist()))
            raw = [max(float(score_map.get(name, 1.0 / n)), 1e-6) for name in horse_names]
            total = sum(raw)
            return [v / total for v in raw]
        except Exception as e:
            logger.debug("モデル確率取得失敗: %s", e)
            return [1.0 / n] * n

    def _enumerate_combinations(
        self,
        race_picks: list[list[Win5HorsePick]],
    ) -> list[Win5Combination]:
        """全組み合わせを生成して期待値を計算する。"""
        # 各レースの全頭数の積が組み合わせ総数
        total = 1
        for picks in race_picks:
            total *= len(picks)
        logger.debug("WIN5 全組み合わせ数: %d", total)

        combos: list[Win5Combination] = []
        for picks_combo in itertools.product(*race_picks):
            combined_prob = 1.0
            for pick in picks_combo:
                combined_prob *= pick.blend_prob

            # 推定払戻 = 1 / combined_prob × WIN5 返還率
            # 実際には売上・キャリーオーバー次第だが期待値計算の基準として使用
            estimated_payout = (1.0 / max(combined_prob, 1e-10)) * _WIN5_RETURN_RATE * 100

            # 期待値 = 確率 × 払戻(円/100円) / 100
            ev = combined_prob * estimated_payout / 100.0

            combos.append(Win5Combination(
                picks=list(picks_combo),
                combined_prob=combined_prob,
                estimated_payout=estimated_payout,
                expected_value=ev,
            ))
        return combos
