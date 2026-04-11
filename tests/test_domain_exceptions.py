"""
フェーズ3 ドメイン例外処理テスト

1. 真のオッズ回帰推定 (OddsEstimator)
2. ケリー基準ハードキャップ (BetConfig + BetGenerator._apply_caps)
3. 競馬ドメイン例外 (同着・返還・競走中止)
   - evaluator._is_hit()  の同着対応
   - reconcile._get_refund_set() の返還取得
   - evaluator._has_refund() の返還判定
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from src.evaluation.evaluator import _has_refund, _is_hit
from src.ml.bet_generator import (
    BetConfig,
    BetGenerator,
    HonmeiStrategy,
    ManjiStrategy,
    OddsEstimator,
)
from src.ml.models import FEATURE_COLS
from src.ml.reconcile import _get_refund_set


# ================================================================
# ヘルパー: 着順マップ
# ================================================================

def _normal_result() -> dict[str, int | None]:
    """通常着順（同着なし）。"""
    return {"馬A": 1, "馬B": 2, "馬C": 3, "馬D": 4, "馬E": 5}


def _dead_heat_2nd() -> dict[str, int | None]:
    """2着同着: 馬B・馬C が同着2着。"""
    return {"馬A": 1, "馬B": 2, "馬C": 2, "馬D": 4, "馬E": 5}


def _dead_heat_1st() -> dict[str, int | None]:
    """1着同着: 馬A・馬B が同着1着。"""
    return {"馬A": 1, "馬B": 1, "馬C": 3, "馬D": 4, "馬E": 5}


def _dead_heat_3rd() -> dict[str, int | None]:
    """3着同着: 馬C・馬D が同着3着。"""
    return {"馬A": 1, "馬B": 2, "馬C": 3, "馬D": 3, "馬E": 5}


def _with_scratch() -> dict[str, int | None]:
    """競走中止: 馬E の rank=None。"""
    return {"馬A": 1, "馬B": 2, "馬C": 3, "馬D": 4, "馬E": None}


# ================================================================
# 1. OddsEstimator テスト
# ================================================================

class TestOddsEstimator:
    """DB統計学習と EV 算出の検証。"""

    def test_conn_None_でデフォルトスケールが使われる(self) -> None:
        est = OddsEstimator(conn=None)
        assert est.scale("単勝") == pytest.approx(OddsEstimator._DEFAULT_SCALE["単勝"])
        assert est.scale("馬連") == pytest.approx(OddsEstimator._DEFAULT_SCALE["馬連"])
        assert est.scale("三連複") == pytest.approx(OddsEstimator._DEFAULT_SCALE["三連複"])

    def test_データ不足でフォールバック(self) -> None:
        """MIN_SAMPLES 未満のデータはデフォルトスケールを使用する。"""
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE race_payouts (
                race_id TEXT, bet_type TEXT, combination TEXT,
                payout INTEGER, popularity INTEGER
            );
            CREATE TABLE race_results (
                id INTEGER PRIMARY KEY,
                race_id TEXT, horse_name TEXT, horse_number INTEGER,
                rank INTEGER, win_odds REAL
            );
        """)
        # 10件のみ（MIN_SAMPLES=50 未満）
        for i in range(10):
            rid = f"r{i:03d}"
            conn.execute(
                "INSERT INTO race_results VALUES (?, ?, '馬A', 1, 1, 5.0)", (i, rid)
            )
            conn.execute(
                "INSERT INTO race_payouts VALUES (?, '単勝', '1', 500, 1)", (rid,)
            )
        conn.commit()

        est = OddsEstimator(conn=conn)
        assert est.scale("単勝") == pytest.approx(OddsEstimator._DEFAULT_SCALE["単勝"])

    def test_十分なデータでスケールを学習する(self) -> None:
        """50件以上のデータで median scale を学習すること。
        単勝オッズ=5.0, 払戻=500 → scale = 500/100/5.0 = 1.0 となるはず。
        """
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE race_payouts (
                race_id TEXT, bet_type TEXT, combination TEXT,
                payout INTEGER, popularity INTEGER
            );
            CREATE TABLE race_results (
                id INTEGER PRIMARY KEY,
                race_id TEXT, horse_name TEXT, horse_number INTEGER,
                rank INTEGER, win_odds REAL
            );
        """)
        for i in range(60):
            rid = f"r{i:03d}"
            conn.execute(
                "INSERT INTO race_results VALUES (?, ?, '馬A', 1, 1, 5.0)", (i, rid)
            )
            conn.execute(
                "INSERT INTO race_payouts VALUES (?, '単勝', '1', 500, 1)", (rid,)
            )
        conn.commit()

        est = OddsEstimator(conn=conn)
        assert est.scale("単勝") == pytest.approx(1.0, rel=0.01)

    def test_ev_が1超で期待値プラス_単勝(self) -> None:
        """P=0.25, odds=5.0 → EV=1.25 > 1.0 (有利)。"""
        est = OddsEstimator()
        assert est.ev(0.25, "単勝", 5.0) > 1.0

    def test_ev_が1未満で期待値マイナス_単勝(self) -> None:
        """P=0.1, odds=5.0 → EV=0.5 < 1.0 (不利)。"""
        est = OddsEstimator()
        assert est.ev(0.1, "単勝", 5.0) < 1.0

    def test_馬連EVが単勝より高い(self) -> None:
        """同じ Harville 確率でも馬連のほうが高オッズ → EV が高い。"""
        est = OddsEstimator()
        ev_tan = est.ev(0.1, "単勝", 5.0)
        ev_ren = est.ev(0.1, "馬連", 5.0)
        assert ev_ren > ev_tan

    def test_未知の券種でもクラッシュしない(self) -> None:
        est = OddsEstimator()
        ev = est.ev(0.1, "枠連", 5.0)  # 未定義券種
        assert isinstance(ev, float)


# ================================================================
# 2. BetConfig / ケリーキャップ テスト
# ================================================================

def _make_df(n: int = 6, base_odds: float = 3.0) -> pd.DataFrame:
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
            "win_odds":     float(i * base_odds),
            "surface_code": 0,
            "sex_code":     0,
            "venue_encoded": 4,
            "sire_encoded": i,
            "distance":     1600,
            "dist_band":    "mile",
        })
        rows.append(row)
    return pd.DataFrame(rows)


class TestBetCap:
    """ハードキャップの動作検証。"""

    def test_per_combo_キャップが全買い目に適用される(self) -> None:
        config = BetConfig(
            bankroll=1_000_000.0,
            max_bet_fraction=1.0,        # レース合計キャップは実質無効
            max_bet_per_combo=300.0,     # 1点 ≤ ¥300
        )
        gen = BetGenerator(config=config)
        df = _make_df()
        scores = pd.Series([1.0 / i for i in range(1, 7)], index=df.index)
        bets = gen.generate_honmei("r001", df, scores)
        for b in bets.bets:
            assert b.recommended_bet <= 300.0, (
                f"{b.bet_type}: recommended_bet={b.recommended_bet} > 300"
            )

    def test_per_race_合計キャップが適用される(self) -> None:
        config = BetConfig(
            bankroll=10_000.0,
            max_bet_fraction=0.05,       # max_race = ¥500
            max_bet_per_combo=100_000.0, # 1点キャップは実質無効
        )
        gen = BetGenerator(config=config)
        df = _make_df()
        ev = pd.Series([1.5] * 6, index=df.index)
        bets = gen.generate_manji("r001", df, ev)
        if bets.bets:
            total = sum(b.recommended_bet for b in bets.bets)
            max_race = config.max_race_bet
            # 100円丸め誤差 × 件数分の余裕を持たせる
            assert total <= max_race + 100 * len(bets.bets), (
                f"合計 {total} > max_race {max_race}"
            )

    def test_キャップなしより賭け金が小さいか等しい(self) -> None:
        default_gen = BetGenerator()
        capped_gen  = BetGenerator(config=BetConfig(max_bet_per_combo=200.0))
        df = _make_df()
        scores = pd.Series([1.0 / i for i in range(1, 7)], index=df.index)

        bets_default = default_gen.generate_honmei("r001", df, scores)
        bets_capped  = capped_gen.generate_honmei("r001", df, scores)

        for bd, bc in zip(bets_default.bets, bets_capped.bets):
            assert bc.recommended_bet <= bd.recommended_bet + 0.01

    def test_最低購入額100円を下回らない(self) -> None:
        config = BetConfig(
            bankroll=1.0,               # 極端に小さい資金
            max_bet_fraction=0.001,
            max_bet_per_combo=0.01,
        )
        gen = BetGenerator(config=config)
        df = _make_df()
        scores = pd.Series([1.0 / i for i in range(1, 7)], index=df.index)
        bets = gen.generate_honmei("r001", df, scores)
        for b in bets.bets:
            assert b.recommended_bet >= 100.0, (
                f"{b.bet_type}: recommended_bet={b.recommended_bet} < 100"
            )


# ================================================================
# 3. 同着（Dead Heat）テスト — evaluator._is_hit()
# ================================================================

class TestDeadHeat:
    """同着ケースの的中判定。"""

    # ── 馬連 ───────────────────────────────────────────────────────

    def test_馬連_通常的中(self) -> None:
        assert _is_hit("馬連", ["馬A", "馬B"], _normal_result()) is True

    def test_馬連_通常外れ(self) -> None:
        assert _is_hit("馬連", ["馬A", "馬C"], _normal_result()) is False

    def test_馬連_2着同着_軸馬と2着A_的中(self) -> None:
        # rank=1: 馬A, rank=2: 馬B・馬C
        assert _is_hit("馬連", ["馬A", "馬B"], _dead_heat_2nd()) is True

    def test_馬連_2着同着_軸馬と2着B_的中(self) -> None:
        assert _is_hit("馬連", ["馬A", "馬C"], _dead_heat_2nd()) is True

    def test_馬連_2着同着_2着同士_不的中(self) -> None:
        # 馬B・馬C は両方 rank=2 → 馬連の的中条件を満たさない
        assert _is_hit("馬連", ["馬B", "馬C"], _dead_heat_2nd()) is False

    def test_馬連_1着同着_的中(self) -> None:
        # rank=1: 馬A・馬B → 馬連 A-B は的中
        assert _is_hit("馬連", ["馬A", "馬B"], _dead_heat_1st()) is True

    def test_馬連_1着同着_3着馬含む_不的中(self) -> None:
        assert _is_hit("馬連", ["馬A", "馬C"], _dead_heat_1st()) is False

    # ── 三連複 ─────────────────────────────────────────────────────

    def test_三連複_通常的中(self) -> None:
        assert _is_hit("三連複", ["馬A", "馬B", "馬C"], _normal_result()) is True

    def test_三連複_通常外れ(self) -> None:
        assert _is_hit("三連複", ["馬A", "馬B", "馬D"], _normal_result()) is False

    def test_三連複_3着同着_馬C含む組合せ_的中(self) -> None:
        # rank=3: 馬C・馬D
        assert _is_hit("三連複", ["馬A", "馬B", "馬C"], _dead_heat_3rd()) is True

    def test_三連複_3着同着_馬D含む組合せ_的中(self) -> None:
        assert _is_hit("三連複", ["馬A", "馬B", "馬D"], _dead_heat_3rd()) is True

    def test_三連複_3着同着_圏外含む_不的中(self) -> None:
        assert _is_hit("三連複", ["馬A", "馬B", "馬E"], _dead_heat_3rd()) is False

    # ── 競走中止 ───────────────────────────────────────────────────

    def test_競走中止馬の単勝_不的中(self) -> None:
        assert _is_hit("単勝", ["馬E"], _with_scratch()) is False

    def test_競走中止馬を除いた単勝_的中(self) -> None:
        assert _is_hit("単勝", ["馬A"], _with_scratch()) is True

    def test_競走中止馬含む馬連_不的中(self) -> None:
        assert _is_hit("馬連", ["馬A", "馬E"], _with_scratch()) is False

    def test_競走中止馬含む三連複_不的中(self) -> None:
        assert _is_hit("三連複", ["馬A", "馬B", "馬E"], _with_scratch()) is False


# ================================================================
# 4. 返還（Refund）テスト
# ================================================================

@pytest.fixture
def refund_db() -> sqlite3.Connection:
    """返還馬を含むインメモリ DB。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE race_payouts (
            race_id TEXT, bet_type TEXT, combination TEXT,
            payout INTEGER, popularity INTEGER
        );
        CREATE TABLE race_results (
            id INTEGER PRIMARY KEY,
            race_id TEXT, horse_name TEXT, horse_number INTEGER,
            rank INTEGER, win_odds REAL
        );
    """)
    # 馬番5（馬C）が取消・返還
    conn.execute("INSERT INTO race_payouts VALUES ('r001', '返還', '5', 100, NULL)")
    conn.execute("INSERT INTO race_results VALUES (1, 'r001', '馬A', 1, 1, 5.0)")
    conn.execute("INSERT INTO race_results VALUES (2, 'r001', '馬B', 2, 2, 3.0)")
    conn.execute("INSERT INTO race_results VALUES (3, 'r001', '馬C', 5, NULL, 8.0)")
    conn.commit()
    return conn


class TestRefund:
    """返還判定の検証。"""

    def test_返還馬名セットが正しく取得される(self, refund_db: sqlite3.Connection) -> None:
        refund_set = _get_refund_set(refund_db, "r001")
        assert "馬C" in refund_set

    def test_返還なしレースは空セット(self, refund_db: sqlite3.Connection) -> None:
        refund_set = _get_refund_set(refund_db, "r999")
        assert refund_set == set()

    def test_返還DBエラーで空セット(self) -> None:
        """テーブルが存在しなくてもクラッシュしない。"""
        conn = sqlite3.connect(":memory:")
        result = _get_refund_set(conn, "r001")
        assert result == set()

    def test_has_refund_返還馬含む_True(self) -> None:
        horse_numbers = {"馬A": 1, "馬B": 2, "馬C": 5}
        refund_numbers = {5}
        assert _has_refund(["馬A", "馬C"], horse_numbers, refund_numbers) is True

    def test_has_refund_返還馬なし_False(self) -> None:
        horse_numbers = {"馬A": 1, "馬B": 2, "馬C": 5}
        refund_numbers = {5}
        assert _has_refund(["馬A", "馬B"], horse_numbers, refund_numbers) is False

    def test_has_refund_空セット_False(self) -> None:
        horse_numbers = {"馬A": 1, "馬B": 2}
        assert _has_refund(["馬A", "馬B"], horse_numbers, set()) is False

    def test_has_refund_馬番不明_False(self) -> None:
        """馬番が horse_numbers に存在しない場合は返還扱いにしない。"""
        horse_numbers: dict[str, int] = {}
        refund_numbers = {5}
        assert _has_refund(["馬C"], horse_numbers, refund_numbers) is False
