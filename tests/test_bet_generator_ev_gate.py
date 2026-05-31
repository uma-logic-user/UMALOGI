"""ManjiStrategy の三連複 EV ゲートテスト。"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from src.ml.bet_generator import ManjiStrategy, OddsEstimator
from src.ml.models import FEATURE_COLS


def _make_df(n: int, win_odds: list[float]) -> pd.DataFrame:
    """FEATURE_COLS を含むダミー DataFrame を作成する。"""
    rows = []
    for i in range(n):
        row: dict = {col: 0.5 for col in FEATURE_COLS}
        row.update(
            {
                "horse_number": i + 1,
                "horse_id": f"h{i + 1:02d}",
                "horse_name": f"馬{i + 1}",
                "sex_age": "牡3",
                "weight_carried": 56.0,
                "horse_weight": 500,
                "popularity": i + 1,
                "win_odds": win_odds[i],
                "surface_code": 0,
                "sex_code": 0,
                "venue_encoded": 4,
                "sire_encoded": i + 1,
                "distance": 1600,
                "dist_band": "mile",
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _run_manji(
    df: pd.DataFrame,
    ev_scores: list[float],
    ev_override: float | None = None,
) -> list:
    """ManjiStrategy.generate() を呼び出して三連複だけ返す。

    Args:
        df:          出走馬 DataFrame
        ev_scores:   各馬の ev_score
        ev_override: None でない場合、OddsEstimator.ev() を常にこの値で返すようモックする
    """
    manji_scores = pd.Series(ev_scores, index=df.index)

    if ev_override is not None:
        with patch.object(OddsEstimator, "ev", return_value=ev_override):
            result = ManjiStrategy().generate(
                race_id="202601010101",
                df=df,
                manji_scores=manji_scores,
                bankroll=100_000.0,
            )
    else:
        result = ManjiStrategy().generate(
            race_id="202601010101",
            df=df,
            manji_scores=manji_scores,
            bankroll=100_000.0,
        )

    return [b for b in result.bets if b.bet_type == "三連複"]


class TestManjiTrioEvGate:
    """ManjiStrategy の三連複に EV ≥ 1.0 ゲートが機能することを検証する。"""

    def test_ev_below_threshold_is_excluded(self) -> None:
        """合成 EV < 1.0 の三連複は推奨しない。

        OddsEstimator.ev() を 0.5 に固定してモックすることで、
        どの組み合わせも EV < 1.0 の状況を作り出す。
        EV ゲート (ev_c < _TRIO_EV_MIN) により推奨が 0 件になることを確認する。
        """
        n = 5
        win_odds = [10.0, 15.0, 20.0, 25.0, 30.0]
        df = _make_df(n, win_odds)
        ev_scores = [1.5, 1.2, 1.0, 0.8, 0.5]

        # OddsEstimator.ev() を 0.5（EV < 1.0）に固定
        trio_bets = _run_manji(df, ev_scores, ev_override=0.5)
        assert len(trio_bets) == 0, (
            f"合成EV=0.5 < 1.0 なのに {len(trio_bets)} 件の三連複が生成された"
        )

    def test_ev_above_threshold_is_included(self) -> None:
        """合成 EV ≥ 1.0 の三連複は推奨する。

        EV = harville_prob × axis_odds × scale(30.0)
        win_odds=10.0 の場合、Harville prob ≈ 0.1 → EV = 0.1 × 10 × 30 = 30 → cap 5.0
        いずれも EV ≥ 1.0 を満たすため、推奨が 1 件以上生成されることを確認する。
        """
        n = 5
        win_odds = [10.0, 15.0, 20.0, 25.0, 30.0]
        df = _make_df(n, win_odds)
        ev_scores = [1.5, 1.5, 1.5, 1.5, 1.5]

        trio_bets = _run_manji(df, ev_scores)
        assert len(trio_bets) > 0, "EV≥1.0 なのに三連複が 0 件"
        for b in trio_bets:
            assert b.expected_value >= 1.0, (
                f"EV={b.expected_value:.3f} < 1.0 の三連複が漏れた"
            )

    def test_mixed_ev_only_high_included(self) -> None:
        """推奨された三連複は全て EV ≥ 1.0 であることを確認する。

        OddsEstimator.ev() を呼ぶたびに異なる値を返すようモックし、
        EV ゲートが「低 EV 組み合わせを除外し、高 EV 組み合わせを通過させる」
        ことを確認する。
        """
        n = 5
        win_odds = [10.0, 15.0, 20.0, 25.0, 30.0]
        df = _make_df(n, win_odds)
        ev_scores = [1.5, 1.5, 1.5, 1.5, 1.5]

        # ev() が順番に高EV→低EV→高EVを返すシーケンス
        ev_sequence = [2.0, 0.5, 1.5, 0.8, 0.3, 1.2, 0.4, 0.6, 0.7, 0.9]
        call_count = {"n": 0}

        def mock_ev(harville_prob: float, bet_type: str, axis_odds: float) -> float:
            if bet_type != "三連複":
                # 三連複以外は実際の計算を使用
                return (
                    OddsEstimator._DEFAULT_SCALE.get(bet_type, 1.0)
                    * harville_prob
                    * axis_odds
                )
            val = ev_sequence[call_count["n"] % len(ev_sequence)]
            call_count["n"] += 1
            return val

        with patch.object(OddsEstimator, "ev", side_effect=mock_ev):
            result = ManjiStrategy().generate(
                race_id="202601010101",
                df=df,
                manji_scores=pd.Series(ev_scores, index=df.index),
                bankroll=100_000.0,
            )

        trio_bets = [b for b in result.bets if b.bet_type == "三連複"]
        # 推奨された三連複は全て EV ≥ 1.0 であること
        for b in trio_bets:
            assert b.expected_value >= 1.0, (
                f"EV={b.expected_value:.3f} < 1.0 の三連複が推奨に漏れた"
            )
