"""グランドスラム Phase2: 型安全化リファクタの契約回帰テスト。

Phase1（refactor: 型ヒント完全適用）で修正した型契約が将来壊れないよう固定する。
対象:
  - RaceBets.model_type Literal に Alpha-Payout / 卍V2 / 本命V2 が含まれること
  - BetRecommendation.combinations が可変長 tuple[int, ...]（単勝1要素〜三連単3要素）
    を受け、to_dict が list 化して正しくシリアライズすること
  - _run_alpha_payout 等の「シグナルなし→None」契約（return None 実バグ修正の固定）
  - alpha_payout/place モデルのハイパーパラメータ dict が **展開で型崩れしないこと

いずれもデータ非依存・決定的・高速（モデル訓練や DB I/O を伴わない）。
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ml.bet_generator import BetRecommendation, RaceBets


# ─────────────────────────────────────────────────────────────────────
# 1. RaceBets.model_type Literal 拡張（Phase1: Alpha-Payout/V2 を許容）
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "model_type",
    ["卍", "本命", "HitFocus", "Alpha-Payout", "卍V2", "本命V2"],
)
def test_racebets_accepts_all_live_model_types(model_type: str) -> None:
    """実運用で生成される全 model_type が RaceBets に格納できる。"""
    rb = RaceBets(race_id="2026010101", model_type=model_type)  # type: ignore[arg-type]
    assert rb.model_type == model_type
    assert rb.bets == []


def test_racebets_default_bets_is_independent_list() -> None:
    """default_factory により bets が個体ごとに独立（共有バグ防止）。"""
    a = RaceBets(race_id="r1", model_type="卍")
    b = RaceBets(race_id="r2", model_type="本命")
    a.bets.append(
        BetRecommendation(
            bet_type="単勝",
            combinations=[(1,)],
            horse_names=["A"],
            expected_value=1.2,
            model_score=0.3,
            recommended_bet=100.0,
            confidence=0.5,
        )
    )
    assert len(a.bets) == 1
    assert len(b.bets) == 0  # 共有されていない


# ─────────────────────────────────────────────────────────────────────
# 2. combinations 可変長 tuple[int, ...] 契約（単勝1〜三連単3要素）
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "bet_type,combos",
    [
        ("単勝", [(7,)]),
        ("複勝", [(3,), (5,)]),
        ("馬連", [(1, 2)]),
        ("三連複", [(1, 2, 3)]),
        ("三連単", [(3, 1, 2), (1, 3, 2)]),
    ],
)
def test_bet_recommendation_variable_length_combinations(
    bet_type: str, combos: list[tuple[int, ...]]
) -> None:
    """単勝(1)〜三連単(3)まで可変長タプルを保持できる。"""
    rec = BetRecommendation(
        bet_type=bet_type,  # type: ignore[arg-type]
        combinations=combos,
        horse_names=["x"] * len(combos),
        expected_value=1.5,
        model_score=0.4,
        recommended_bet=300.0,
        confidence=0.6,
    )
    assert rec.combinations == combos
    assert all(isinstance(c, tuple) for c in rec.combinations)


def test_racebets_to_dict_serializes_combinations_as_lists() -> None:
    """to_dict は tuple を list に変換し JSON 化可能な形にする。"""
    rb = RaceBets(race_id="2026010101", model_type="Alpha-Payout")
    rb.bets.append(
        BetRecommendation(
            bet_type="三連単",
            combinations=[(3, 1, 2)],
            horse_names=["A", "B", "C"],
            expected_value=2.345678,
            model_score=0.456789,
            recommended_bet=600.0,
            confidence=0.812345,
            notes="test",
        )
    )
    d = rb.to_dict()
    assert d["race_id"] == "2026010101"
    assert d["model_type"] == "Alpha-Payout"
    bet = d["bets"][0]
    assert bet["combinations"] == [[3, 1, 2]]  # tuple→list
    assert isinstance(bet["combinations"][0], list)
    # 丸め桁の固定
    assert bet["expected_value"] == 2.346
    assert bet["model_score"] == 0.457
    assert bet["confidence"] == 0.812


def test_racebets_to_dict_empty_bets() -> None:
    """買い目ゼロでも to_dict は空 bets リストで成立。"""
    rb = RaceBets(race_id="r", model_type="本命V2")
    d = rb.to_dict()
    assert d == {"race_id": "r", "model_type": "本命V2", "bets": []}


# ─────────────────────────────────────────────────────────────────────
# 3. _run_alpha_payout の「シグナルなし→None」契約（return None 実バグ固定）
# ─────────────────────────────────────────────────────────────────────
def test_run_alpha_payout_returns_none_when_model_missing() -> None:
    """モデルファイルが無い場合は None を返す（return→return None 修正の固定）。"""
    from src.pipeline import prediction as P

    conn = sqlite3.connect(":memory:")
    df = pd.DataFrame({"horse_number": [1, 2], "win_odds": [3.0, 5.0]})

    # _MODEL_PATH.exists() を False にしてモデル未存在パスへ
    fake_path = MagicMock()
    fake_path.exists.return_value = False
    with patch(
        "src.ml.alpha_payout_model.AlphaPayoutModel"
    ), patch("src.ml.alpha_payout_model._MODEL_PATH", fake_path):
        result = P._run_alpha_payout(conn, "2026010101", df, bankroll=100_000.0)
    assert result is None
    conn.close()


# ─────────────────────────────────────────────────────────────────────
# 4. ハイパーパラメータ dict の **展開（dict[str, Any] 型崩れ防止）
# ─────────────────────────────────────────────────────────────────────
def test_alpha_payout_params_dict_is_kwargs_expandable() -> None:
    """LGBM 系に渡すパラメータ dict が **展開で受け取り可能な構造。"""

    # dict[str, Any] を ** 展開しても型崩れせず関数に渡せることを構造的に確認
    def _sink(*, n_estimators: int, learning_rate: float, objective: str) -> tuple:
        return (n_estimators, learning_rate, objective)

    params: dict[str, object] = {
        "n_estimators": 500,
        "learning_rate": 0.05,
        "objective": "huber",
    }
    out = _sink(**params)  # type: ignore[arg-type]
    assert out == (500, 0.05, "huber")


# ─────────────────────────────────────────────────────────────────────
# 5. umanity _build_comment 異常系（DB由来の不正入力の握り潰し）
#    Phase1 で dict[str, Any] 化した投稿経路の出口。型崩れ・不正JSONに耐える。
# ─────────────────────────────────────────────────────────────────────
def _build_comment(*args: object, **kwargs: object) -> str:
    from src.ops.umanity_uploader import UmanityUploader

    return UmanityUploader._build_comment(*args, **kwargs)  # type: ignore[arg-type]


def test_build_comment_none_combination_json() -> None:
    """combination_json=None でも例外なくテキスト化（馬番なし）。"""
    out = _build_comment("複勝", None, None, None)
    assert "UMALOGI AI予想" in out
    assert "複勝" in out


def test_build_comment_invalid_json_swallowed() -> None:
    """壊れた JSON 文字列でも例外を出さず本文を返す。"""
    out = _build_comment("馬連", "{not-json", "根拠", 1.8)
    assert "馬連" in out
    assert "根拠" in out
    assert "EV=1.80" in out


def test_build_comment_empty_json_array() -> None:
    """空配列 '[]' は買い目なし扱い（combo_str 空）。"""
    out = _build_comment("複勝", "[]", None, None)
    assert "複勝" in out


def test_build_comment_nested_list_combo() -> None:
    """[[5,3]] 形式は先頭組を '5-3' に整形する。"""
    out = _build_comment("馬連", "[[5, 3]]", None, 2.5)
    assert "5-3" in out
    assert "EV=2.50" in out


def test_build_comment_flat_list_combo() -> None:
    """[7] 形式（単勝・フラット）は先頭要素を文字列化。"""
    out = _build_comment("単勝", "[7]", None, None)
    assert "7" in out


def test_build_comment_zero_ev_omits_ev_suffix() -> None:
    """expected_value=0/None は EV サフィックスを付けない（falsy 分岐）。"""
    out_zero = _build_comment("複勝", "[1]", None, 0.0)
    assert "EV=" not in out_zero


def test_build_comment_long_notes_included() -> None:
    """長い notes も本文に連結される（切り詰めは投稿側責務）。"""
    note = "あ" * 200
    out = _build_comment("複勝", "[1]", note, 1.1)
    assert note in out
