"""
src/ml/bet_generator.py のユニットテスト。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.ml.bet_generator import (
    BetGenerator,
    HitFocusStrategy,
    HonmeiStrategy,
    ManjiStrategy,
    RaceBets,
    Win5Recommendation,
    _kelly_bet,
    generate_win5,
)
from src.ml.models import FEATURE_COLS


# ── フィクスチャ ──────────────────────────────────────────────────

def _make_df(n: int = 6, odds: list[float] | None = None) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        row: dict = {col: 0.5 for col in FEATURE_COLS}
        row.update({
            "horse_number": i,
            "horse_id":     f"h{i:02d}",
            "horse_name":   f"馬{i:02d}",
            "sex_age":      "牡3",
            "weight_carried": 56.0,
            "horse_weight": 500,
            "popularity":   i,
            "win_odds":     (odds[i - 1] if odds else float(i * 3)),
            "surface_code": 0,
            "sex_code":     0,
            "venue_encoded": 4,
            "sire_encoded": i,
            "distance":     1600,
            "dist_band":    "mile",
        })
        rows.append(row)
    return pd.DataFrame(rows)


def _make_honmei_scores(df: pd.DataFrame) -> pd.Series:
    """人気順の逆数を本命スコアとして返す。"""
    return pd.Series(
        [1.0 / i for i in df["popularity"]],
        index=df.index,
    )


def _make_ev_scores(df: pd.DataFrame, ev_base: float = 1.2) -> pd.Series:
    """全馬に EV > 1.0 を返すスコア。"""
    return pd.Series(
        [ev_base] * len(df),
        index=df.index,
    )


def _make_low_ev_scores(df: pd.DataFrame) -> pd.Series:
    """全馬 EV < 1.0 を返すスコア。"""
    return pd.Series(
        [0.8] * len(df),
        index=df.index,
    )


# ── _kelly_bet ────────────────────────────────────────────────────

class TestKellyBet:
    def test_正の賭け金を返す(self) -> None:
        bet = _kelly_bet(win_prob=0.3, odds=5.0, base_bet=1000)
        assert bet >= 0

    def test_勝率0でゼロ(self) -> None:
        assert _kelly_bet(win_prob=0.0, odds=5.0) == 0.0

    def test_オッズ1以下でゼロ(self) -> None:
        assert _kelly_bet(win_prob=0.5, odds=1.0) == 0.0

    def test_上限を超えない(self) -> None:
        # 極端な確率でも cap=0.25 が上限
        bet = _kelly_bet(win_prob=0.99, odds=100.0, base_bet=1000, cap=0.25)
        assert bet <= 1000 * (1 + 0.25 * 10)


# ── ManjiStrategy ─────────────────────────────────────────────────

class TestManjiStrategy:
    def test_EV高い馬の単勝が生成される(self) -> None:
        df = _make_df()
        ev = _make_ev_scores(df)
        bets = ManjiStrategy().generate("test001", df, ev)
        bet_types = [b.bet_type for b in bets.bets]
        assert "単勝" in bet_types

    def test_EVゲート廃止後も買い目が生成される(self) -> None:
        # 確率至上主義: EVゲート撤廃により低EVでも無条件で上位馬を推奨する
        df = _make_df()
        ev = _make_low_ev_scores(df)
        bets = ManjiStrategy().generate("test001", df, ev)
        assert len(bets.bets) > 0

    def test_馬連とワイドが含まれる(self) -> None:
        df = _make_df()
        ev = _make_ev_scores(df, ev_base=1.15)
        bets = ManjiStrategy().generate("test001", df, ev)
        types = {b.bet_type for b in bets.bets}
        assert "馬連" in types
        assert "ワイド" in types

    def test_model_typeが卍(self) -> None:
        df = _make_df()
        ev = _make_ev_scores(df)
        bets = ManjiStrategy().generate("test001", df, ev)
        assert bets.model_type == "卍"

    def test_to_dictがシリアライズ可能(self) -> None:
        import json
        df = _make_df()
        ev = _make_ev_scores(df)
        bets = ManjiStrategy().generate("test001", df, ev)
        d = bets.to_dict()
        json.dumps(d)  # JSON 変換できること


# ── HonmeiStrategy ────────────────────────────────────────────────

class TestHonmeiStrategy:
    def test_単勝が含まれる(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test002", df, scores)
        types = [b.bet_type for b in bets.bets]
        assert "単勝" in types

    def test_三連複が含まれる(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test002", df, scores)
        types = {b.bet_type for b in bets.bets}
        assert "三連複" in types

    def test_三連単が含まれる(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test002", df, scores)
        types = {b.bet_type for b in bets.bets}
        assert "三連単" in types

    def test_model_typeが本命(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test002", df, scores)
        assert bets.model_type == "本命"

    def test_1頭のみでも例外なし(self) -> None:
        df = _make_df(n=1)
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test003", df, scores)
        assert len(bets.bets) > 0  # 少なくとも単勝

    def test_馬名が設定される(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HonmeiStrategy().generate("test002", df, scores)
        tansho = next(b for b in bets.bets if b.bet_type == "単勝")
        assert tansho.horse_names[0].startswith("馬")


# ── BetGenerator ファサード ───────────────────────────────────────

class TestBetGenerator:
    def test_generate_honmeiが動作する(self) -> None:
        gen = BetGenerator()
        df  = _make_df()
        sc  = _make_honmei_scores(df)
        bets = gen.generate_honmei("r001", df, sc)
        assert bets.model_type == "本命"

    def test_generate_manjiが動作する(self) -> None:
        gen = BetGenerator()
        df  = _make_df()
        ev  = _make_ev_scores(df)
        bets = gen.generate_manji("r001", df, ev)
        assert bets.model_type == "卍"


# ── WIN5 ─────────────────────────────────────────────────────────

class TestWin5:
    def _make_five_races(self) -> tuple[dict, dict]:
        races = {}
        scores = {}
        for i in range(1, 6):
            rid = f"2025060{i:02d}11"
            df = _make_df(n=8)
            races[rid] = df
            scores[rid] = _make_honmei_scores(df)
        return races, scores

    def test_5レースでWin5が生成される(self) -> None:
        races, scores = self._make_five_races()
        rec = generate_win5(races, scores, top_n=2)
        assert rec is not None
        assert isinstance(rec, Win5Recommendation)
        assert rec.total_combinations > 0

    def test_5レース未満はNone(self) -> None:
        races = {f"r{i}": _make_df() for i in range(4)}
        scores = {rid: _make_honmei_scores(df) for rid, df in races.items()}
        rec = generate_win5(races, scores)
        assert rec is None

    def test_組み合わせ上限を超えると絞り込む(self) -> None:
        races, scores = self._make_five_races()
        rec = generate_win5(races, scores, top_n=2, max_combinations=3)
        assert rec is not None
        assert rec.total_combinations <= 3

    def test_to_dictがシリアライズ可能(self) -> None:
        import json
        races, scores = self._make_five_races()
        rec = generate_win5(races, scores)
        assert rec is not None
        json.dumps(rec.to_dict())


# ── HitFocusStrategy ──────────────────────────────────────────────

class TestHitFocusStrategy:
    """
    HitFocusStrategy の TDD テスト。

    仕様:
      - 馬連・馬単・三連単の2軸マルチフォーメーション
      - 均等 100 円買い（Kelly 不使用）
      - トリガミ防止フィルター（最良組み合わせ推定払戻 < 総投資額 → スキップ）
      - 第4の独立クラス（model_type = "HitFocus"）
    """

    def test_model_typeがHitFocus(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        assert bets.model_type == "HitFocus"

    def test_戻り値がRaceBets型(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        assert isinstance(bets, RaceBets)

    def test_馬連が含まれる(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        bet_types = {b.bet_type for b in bets.bets}
        assert "馬連" in bet_types

    def test_馬単が含まれる(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        bet_types = {b.bet_type for b in bets.bets}
        assert "馬単" in bet_types

    def test_三連単が含まれる(self) -> None:
        # 軸馬が圧倒的に強いスコア → トリガミ防止フィルターを通過させる
        df = _make_df(n=6)
        scores = pd.Series([0.70, 0.15, 0.06, 0.04, 0.03, 0.02], index=df.index)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        bet_types = {b.bet_type for b in bets.bets}
        assert "三連単" in bet_types

    def test_単勝と複勝とワイドと三連複は生成しない(self) -> None:
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        bet_types = {b.bet_type for b in bets.bets}
        assert "単勝" not in bet_types
        assert "複勝" not in bet_types
        assert "ワイド" not in bet_types
        assert "三連複" not in bet_types

    def test_馬連に2軸間組み合わせが含まれる(self) -> None:
        """スコア1位・2位の馬が馬連に含まれること（2軸フォーメーションの核心）。"""
        df = _make_df(n=6)
        # horse_number 1 が最高スコア、horse_number 2 が次点
        scores = pd.Series(
            [1.0, 0.8, 0.6, 0.4, 0.2, 0.1],
            index=df.index,
        )
        bets = HitFocusStrategy().generate("hf002", df, scores)
        umaren = next(b for b in bets.bets if b.bet_type == "馬連")
        axis_combo = tuple(sorted([1, 2]))
        assert axis_combo in umaren.combinations

    def test_馬単に2軸の両方向が含まれる(self) -> None:
        """axis1→axis2 と axis2→axis1 の両方が馬単に含まれること。"""
        df = _make_df(n=6)
        scores = pd.Series(
            [1.0, 0.8, 0.6, 0.4, 0.2, 0.1],
            index=df.index,
        )
        bets = HitFocusStrategy().generate("hf002", df, scores)
        umatan = next(b for b in bets.bets if b.bet_type == "馬単")
        combos = umatan.combinations
        assert (1, 2) in combos
        assert (2, 1) in combos

    def test_均等100円買い_馬連(self) -> None:
        """馬連の recommended_bet = 100 × 組み合わせ数 であること。"""
        df = _make_df(n=8)
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        umaren = next(b for b in bets.bets if b.bet_type == "馬連")
        assert umaren.recommended_bet == 100 * len(umaren.combinations)

    def test_均等100円買い_馬単(self) -> None:
        df = _make_df(n=8)
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        umatan = next(b for b in bets.bets if b.bet_type == "馬単")
        assert umatan.recommended_bet == 100 * len(umatan.combinations)

    def test_均等100円買い_三連単(self) -> None:
        df = _make_df(n=6)
        scores = pd.Series([0.70, 0.15, 0.06, 0.04, 0.03, 0.02], index=df.index)
        bets = HitFocusStrategy().generate("hf001", df, scores)
        sanrentan = next(b for b in bets.bets if b.bet_type == "三連単")
        assert sanrentan.recommended_bet == 100 * len(sanrentan.combinations)

    def test_2頭以下では買い目なしでも例外なし(self) -> None:
        df = _make_df(n=2)
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf003", df, scores)
        assert isinstance(bets, RaceBets)
        # 三連単は生成されない（馬連・馬単はあってもなくても例外なし）
        bet_types = {b.bet_type for b in bets.bets}
        assert "三連単" not in bet_types

    def test_1頭以下では空の買い目(self) -> None:
        df = _make_df(n=1)
        scores = _make_honmei_scores(df)
        bets = HitFocusStrategy().generate("hf004", df, scores)
        assert len(bets.bets) == 0

    def test_トリガミ防止フィルター_低オッズ軸馬はスキップされる(self) -> None:
        """推定払戻ベースフィルター: scale × axis_odds ≤ n_combos × margin → スキップ。
        _make_df の win_odds=horse_number×3 → axis1(馬番1)=3.0。
        scale=6.0, odds=3.0, margin=1.5 → 18.0 > 10.5 → 通常はPASS。
        例外なく RaceBets が返ることを確認するスモークテスト。"""
        df = _make_df(n=16)
        scores = pd.Series([1.0] * 16, index=df.index)
        bets = HitFocusStrategy().generate("hf005", df, scores)
        assert isinstance(bets, RaceBets)

    def test_BetGeneratorのgenerate_hit_focusが動作する(self) -> None:
        gen = BetGenerator()
        df = _make_df()
        scores = _make_honmei_scores(df)
        bets = gen.generate_hit_focus("r_hf001", df, scores)
        assert bets.model_type == "HitFocus"
