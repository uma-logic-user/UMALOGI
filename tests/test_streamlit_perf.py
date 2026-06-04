"""
web_streamlit/app.py パフォーマンス最適化のコードレベル検証テスト。

検証対象:
  1. _kelly_simulate_core の numpy 演算精度
  2. @st.cache_data 付き派生関数の存在
  3. @st.fragment デコレータの適用
  4. render_bias_panel の呼び出し重複排除（1 回のみ）
  5. iterrows 実コード残存ゼロ
"""

from __future__ import annotations

import ast
import sqlite3
import sys
import types
import unittest.mock as mock
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.database.schema import DDL_STATEMENTS

ROOT = Path(__file__).resolve().parent.parent
APP_PATH = ROOT / "web_streamlit" / "app.py"


# ── Streamlit モック（インポート時のランタイム依存を排除） ──────────


def _build_st_mock() -> types.ModuleType:
    """@st.cache_data / @st.cache_resource / @st.fragment をパススルーにするモック。"""
    st = mock.MagicMock(name="streamlit")

    def _cache_data(*args, **kwargs):
        if len(args) == 1 and callable(args[0]):
            return args[0]
        return lambda f: f

    st.cache_data = _cache_data
    st.cache_resource = lambda f: f
    st.fragment = lambda f: f
    return st


def _import_app() -> types.ModuleType:
    """web_streamlit/app.py を Streamlit モック下でインポートする。"""
    # 既存モジュールを退避し、テスト用モックに差し替え
    _orig_st = sys.modules.get("streamlit")
    _orig_app = sys.modules.pop("web_streamlit.app", None)

    sys.modules["streamlit"] = _build_st_mock()  # type: ignore[assignment]
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("web_streamlit.app", APP_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod
    finally:
        # 元の streamlit を復元（他テストへの影響を防ぐ）
        if _orig_st is not None:
            sys.modules["streamlit"] = _orig_st
        elif "streamlit" in sys.modules:
            del sys.modules["streamlit"]
        if _orig_app is not None:
            sys.modules["web_streamlit.app"] = _orig_app
        elif "web_streamlit.app" in sys.modules:
            del sys.modules["web_streamlit.app"]


@pytest.fixture(scope="module")
def app():
    return _import_app()


# ════════════════════════════════════════════════════════════════════
#  1. ソースコード構造テスト（パース不要・テキスト解析）
# ════════════════════════════════════════════════════════════════════


class TestSourceStructure:
    """app.py のソーステキストレベルの構造検証。"""

    @pytest.fixture(autouse=True)
    def _src(self):
        self.src = APP_PATH.read_text(encoding="utf-8")

    def test_ast_parse_clean(self):
        """構文エラーがないことを確認する。"""
        ast.parse(self.src)  # SyntaxError が出なければ合格

    def test_numpy_imported(self):
        assert "import numpy as np" in self.src

    def test_no_iterrows_in_runtime_code(self):
        """実行コードに iterrows() 呼び出しが残存しないことを確認する。

        docstring/コメント中の "iterrows" 説明文は除外する。
        実際の呼び出しは必ず iterrows() と括弧を伴うため "iterrows(" で判定する。
        """
        for line in self.src.splitlines():
            stripped = line.strip()
            if "iterrows(" in stripped and not stripped.startswith("#"):
                pytest.fail(f"実行コードに iterrows() が残存: {line!r}")

    def test_render_bias_panel_called_exactly_once(self):
        """render_bias_panel(selected_race_id) の呼び出しが 1 回だけであること。"""
        count = self.src.count("render_bias_panel(selected_race_id)")
        assert count == 1, f"render_bias_panel が {count} 回呼ばれている（期待: 1）"

    def test_render_analytics_has_fragment(self):
        """render_analytics の直前に @st.fragment があること。"""
        idx = self.src.find("def render_analytics(")
        assert idx > 0
        preceding = self.src[max(0, idx - 60) : idx]
        assert "@st.fragment" in preceding

    def test_render_hit_performance_has_fragment(self):
        """render_hit_performance の直前に @st.fragment があること。"""
        idx = self.src.find("def render_hit_performance(")
        assert idx > 0
        preceding = self.src[max(0, idx - 60) : idx]
        assert "@st.fragment" in preceding

    @pytest.mark.parametrize(
        "fn_name",
        [
            "_kelly_simulate_core",
            "_build_monthly_total",
            "_build_kelly_series",
            "_build_venue_stats",
        ],
    )
    def test_cache_functions_defined(self, fn_name: str):
        assert f"def {fn_name}(" in self.src

    def test_build_monthly_total_has_cache_data(self):
        idx = self.src.find("def _build_monthly_total(")
        pre = self.src[max(0, idx - 80) : idx]
        assert "@st.cache_data" in pre

    def test_build_kelly_series_has_cache_data(self):
        idx = self.src.find("def _build_kelly_series(")
        pre = self.src[max(0, idx - 80) : idx]
        assert "@st.cache_data" in pre

    def test_build_venue_stats_has_cache_data(self):
        idx = self.src.find("def _build_venue_stats(")
        pre = self.src[max(0, idx - 80) : idx]
        assert "@st.cache_data" in pre

    def test_bias_panel_placed_before_subtabs(self):
        """render_bias_panel がサブタブ宣言 (stab_prov) より前に来ること。"""
        bias_idx = self.src.find("render_bias_panel(selected_race_id)")
        stab_idx = self.src.find("stab_prov, stab_final")
        assert bias_idx < stab_idx, "render_bias_panel がサブタブ宣言より後ろにある"

    # ── 逆統合（サマリータブ）の構造検証 ───────────────────────
    @pytest.mark.parametrize(
        "fn_name",
        [
            "fetch_recent_results",
            "fetch_top_ev_horses",
            "fetch_model_roi",
            "_latest_prediction_date",
            "render_home",
        ],
    )
    def test_home_functions_defined(self, fn_name: str):
        assert f"def {fn_name}(" in self.src

    def test_main_has_summary_tab(self):
        """トップタブに「🏠 サマリー」が追加され 4 タブ構成であること。"""
        assert '"🏠 サマリー"' in self.src

    def test_render_home_called_once(self):
        """サマリータブ内で render_home() が 1 回だけ呼ばれること（def 定義を除く）。"""
        assert self.src.count("with main_tabs[0]:\n        render_home()") == 1

    def test_model_roi_uses_canonical_accounting(self):
        """モデル別ROIが正準ロジック compute_live_roi を再利用していること。"""
        assert "from src.ml.pnl_accounting import compute_live_roi" in self.src
        assert "compute_live_roi(_get_conn()" in self.src


# ════════════════════════════════════════════════════════════════════
#  4. サマリータブ データ関数テスト（旧 src/web/dashboard.py から移植）
# ════════════════════════════════════════════════════════════════════


def _seed_conn() -> sqlite3.Connection:
    """DDL_STATEMENTS で構築した空の一時 DB 接続を返す（row_factory=Row）。"""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    for ddl in DDL_STATEMENTS:
        c.execute(ddl)
    return c


def _rid(venue: str, race_no: int) -> str:
    """テスト用 race_id（12桁・SUBSTR(5,2)=会場 / SUBSTR(11,2)=R）を生成する。"""
    return f"2026{venue}0101{race_no:02d}"


def _add_race(c: sqlite3.Connection, race_id: str, date: str, name: str = "テストS") -> None:
    c.execute(
        "INSERT INTO races(race_id, race_name, date, venue, race_number, "
        "distance, surface) VALUES(?,?,?,?,?,?,?)",
        (race_id, name, date, "東京", int(race_id[10:12]), 1600, "芝"),
    )


def _add_result(c: sqlite3.Connection, race_id: str, name: str, rank: int | None,
                num: int, odds: float | None = None, pop: int | None = None) -> None:
    c.execute(
        "INSERT INTO race_results(race_id, horse_name, rank, horse_number, "
        "win_odds, popularity) VALUES(?,?,?,?,?,?)",
        (race_id, name, rank, num, odds, pop),
    )


def _add_pred(c: sqlite3.Connection, race_id: str, model: str, bet: str, ev: float,
              horse: str, *, superseded: int = 0, score: float = 1.0) -> int:
    cur = c.execute(
        "INSERT INTO predictions(race_id, model_type, bet_type, confidence, "
        "expected_value, is_superseded) VALUES(?,?,?,0.5,?,?)",
        (race_id, model, bet, ev, superseded),
    )
    pid = int(cur.lastrowid or 0)
    c.execute(
        "INSERT INTO prediction_horses(prediction_id, horse_name, predicted_rank, "
        "model_score, ev_score) VALUES(?,?,1,?,?)",
        (pid, horse, score, ev),
    )
    return pid


def _add_pred_result(c: sqlite3.Connection, pid: int, hit: int, payout: float,
                     profit: float) -> None:
    c.execute(
        "INSERT INTO prediction_results(prediction_id, is_hit, payout, profit) "
        "VALUES(?,?,?,?)",
        (pid, hit, payout, profit),
    )


class TestHomeDataFunctions:
    """fetch_recent_results / fetch_top_ev_horses / fetch_model_roi の振る舞い検証。

    app._get_conn を一時 DB に差し替え、実 SQL を検証する。
    """

    @pytest.fixture()
    def conn(self) -> Iterator[sqlite3.Connection]:
        c = _seed_conn()
        yield c
        c.close()

    @pytest.fixture(autouse=True)
    def _wire(self, app, conn, monkeypatch):
        monkeypatch.setattr(app, "_get_conn", lambda: conn)
        self.app = app
        self.c = conn

    # ── fetch_recent_results ──────────────────────────────────
    def test_recent_results_newest_first(self):
        _add_race(self.c, _rid("05", 11), "2026-05-30", "古いS")
        _add_result(self.c, _rid("05", 11), "オールド", 1, 5, 3.2, 1)
        _add_race(self.c, _rid("06", 11), "2026-05-31", "新しいS")
        _add_result(self.c, _rid("06", 11), "ニュー", 1, 7, 8.4, 4)
        self.c.commit()

        df = self.app.fetch_recent_results(limit=10)
        assert len(df) == 2
        assert df.iloc[0]["date"] == "2026-05-31"
        assert df.iloc[0]["winner"] == "ニュー"

    def test_recent_results_excludes_no_winner(self):
        _add_race(self.c, _rid("05", 1), "2026-05-31")
        _add_result(self.c, _rid("05", 1), "勝ち馬", 1, 3, 2.0, 1)
        _add_race(self.c, _rid("05", 2), "2026-05-31")
        _add_result(self.c, _rid("05", 2), "中止馬", None, 4)
        self.c.commit()

        df = self.app.fetch_recent_results(limit=10)
        assert set(df["winner"]) == {"勝ち馬"}

    def test_recent_results_respects_limit(self):
        for i in range(5):
            _add_race(self.c, _rid("05", i + 1), "2026-05-31")
            _add_result(self.c, _rid("05", i + 1), f"馬{i}", 1, i + 1, 2.0, 1)
        self.c.commit()

        assert len(self.app.fetch_recent_results(limit=3)) == 3

    # ── fetch_top_ev_horses ───────────────────────────────────
    def test_top_ev_horses_sorted_by_ev_desc(self):
        _add_race(self.c, _rid("05", 11), "2026-05-31")
        _add_race(self.c, _rid("06", 11), "2026-05-31")
        _add_pred(self.c, _rid("05", 11), "本命(直前)", "単勝", 1.20, "低EV馬")
        _add_pred(self.c, _rid("06", 11), "Pure_EV_Edge", "複勝", 1.85, "高EV馬")
        self.c.commit()

        df = self.app.fetch_top_ev_horses(target_date="2026-05-31", limit=10)
        assert list(df["horse_name"]) == ["高EV馬", "低EV馬"]

    def test_top_ev_horses_excludes_superseded(self):
        _add_race(self.c, _rid("05", 11), "2026-05-31")
        _add_pred(self.c, _rid("05", 11), "本命(直前)", "単勝", 2.5, "無効馬", superseded=1)
        _add_pred(self.c, _rid("05", 11), "卍(直前)", "複勝", 1.3, "有効馬")
        self.c.commit()

        df = self.app.fetch_top_ev_horses(target_date="2026-05-31", limit=10)
        assert list(df["horse_name"]) == ["有効馬"]

    def test_top_ev_horses_defaults_to_latest_date(self):
        _add_race(self.c, _rid("05", 11), "2026-05-30")
        _add_race(self.c, _rid("06", 11), "2026-05-31")
        _add_pred(self.c, _rid("05", 11), "本命(直前)", "単勝", 3.0, "昨日の馬")
        _add_pred(self.c, _rid("06", 11), "本命(直前)", "単勝", 1.1, "今日の馬")
        self.c.commit()

        df = self.app.fetch_top_ev_horses(limit=10)
        assert list(df["horse_name"]) == ["今日の馬"]

    # ── fetch_model_roi ───────────────────────────────────────
    def test_model_roi_per_model(self):
        _add_race(self.c, _rid("05", 11), "2026-05-31")
        pid = _add_pred(self.c, _rid("05", 11), "Pure_EV_Edge(直前)", "単勝", 1.5, "勝ち馬")
        _add_pred_result(self.c, pid, 1, 250.0, 150.0)  # cost=100, ROI=250%
        self.c.commit()

        df = self.app.fetch_model_roi(live_only=True)
        row = df[df["model_type"] == "Pure_EV_Edge(直前)"]
        assert len(row) == 1
        assert float(row.iloc[0]["roi"]) == 250.0
        assert float(row.iloc[0]["hit_rate"]) == 100.0

    def test_model_roi_excludes_appreciation_models(self):
        _add_race(self.c, _rid("05", 11), "2026-05-31")
        pid = _add_pred(self.c, _rid("05", 11), "Oracle(直前)", "単勝", 1.5, "観賞馬")
        _add_pred_result(self.c, pid, 0, 0.0, -100.0)
        self.c.commit()

        df = self.app.fetch_model_roi(live_only=True)
        assert df.empty or "Oracle(直前)" not in set(df["model_type"])

    def test_model_roi_empty_when_no_results(self):
        df = self.app.fetch_model_roi(live_only=True)
        assert df.empty


# ════════════════════════════════════════════════════════════════════
#  2. _kelly_simulate_core 純粋関数テスト
# ════════════════════════════════════════════════════════════════════


class TestKellySimulateCore:
    """numpy ベクトル化演算の精度・挙動を検証する。"""

    @pytest.fixture(autouse=True)
    def _mod(self, app):
        self.core = app._kelly_simulate_core

    def _run(
        self,
        bets: list,
        payouts: list,
        hits: list,
        dates: list | None = None,
        initial: float = 1_000_000.0,
    ) -> dict:
        n = len(bets)
        d = dates or ["2025-01-01"] * n
        return self.core(
            bets_arr=np.array(bets, dtype=np.float64),
            payouts_arr=np.array(payouts, dtype=np.float64),
            hits_arr=np.array(hits, dtype=bool),
            dates_arr=np.array(d, dtype=object),
            initial_bankroll=initial,
        )

    # ── 基本 ─────────────────────────────────────────────────

    def test_empty_returns_initial_bankroll(self):
        res = self._run([], [], [])
        assert res["total"] == 0
        assert res["wins"] == 0
        assert res["bankroll"] == pytest.approx(1_000_000.0)
        assert res["empty"] is False

    def test_single_win(self):
        # 払戻 3000 - 掛け金 1000 = +2000
        res = self._run([1000], [3000], [True])
        assert res["wins"] == 1
        assert res["total"] == 1
        assert res["bankroll"] == pytest.approx(1_002_000.0)

    def test_single_loss(self):
        res = self._run([1000], [0], [False])
        assert res["wins"] == 0
        assert res["bankroll"] == pytest.approx(999_000.0)

    def test_10pct_cap_applied(self):
        # bets[0]=200_000 > 1_000_000*0.10=100_000 → actual_bet=100_000, 損失=100_000
        res = self._run([200_000], [0], [False], initial=1_000_000.0)
        assert res["bankroll"] == pytest.approx(900_000.0)

    def test_10pct_cap_not_applied_when_small(self):
        # bets[0]=5000 < 100_000 → actual_bet=5000, 払戻=15000, 利益=10000
        res = self._run([5_000], [15_000], [True], initial=1_000_000.0)
        assert res["bankroll"] == pytest.approx(1_010_000.0)

    # ── ベクトル化集計 (wins) ──────────────────────────────────

    def test_wins_count_vectorized(self):
        """hits_arr.sum() でベクトル化された wins カウントを検証する。"""
        hits = [True, False, True, True, False]
        res = self._run([100] * 5, [300] * 5, hits)
        assert res["wins"] == 3
        assert res["total"] == 5

    def test_wins_all_false(self):
        res = self._run([100, 200], [0, 0], [False, False])
        assert res["wins"] == 0

    def test_wins_all_true(self):
        res = self._run([100, 200], [300, 600], [True, True])
        assert res["wins"] == 2

    # ── series_df ──────────────────────────────────────────────

    def test_series_df_shape(self):
        """n 件のベットに対し series_df は n+1 行になること。"""
        res = self._run([1000, 2000, 3000], [3000, 0, 6000], [True, False, True])
        assert isinstance(res["series_df"], pd.DataFrame)
        assert len(res["series_df"]) == 4  # n + 1

    def test_series_df_first_row_is_initial(self):
        res = self._run([1000], [2000], [True], initial=500_000.0)
        assert res["series_df"].iloc[0]["bankroll"] == pytest.approx(500_000.0)

    def test_series_df_last_row_equals_bankroll(self):
        res = self._run([1000, 2000], [3000, 0], [True, False])
        last_br = res["series_df"].iloc[-1]["bankroll"]
        assert last_br == pytest.approx(res["bankroll"])

    # ── max_dd ──────────────────────────────────────────────────

    def test_max_drawdown_nonzero_on_loss(self):
        """勝ち後に大損すると max_dd > 0 になること。"""
        res = self._run([1000, 10_000], [3000, 0], [True, False])
        assert res["max_dd"] > 0.0

    def test_max_drawdown_zero_on_all_wins(self):
        """一度も負けなければ max_dd は 0 のまま。"""
        res = self._run([1000, 2000], [3000, 6000], [True, True])
        assert res["max_dd"] == pytest.approx(0.0)

    # ── 戻り値キー ───────────────────────────────────────────

    def test_return_keys_complete(self):
        res = self._run([1000], [2000], [True])
        assert all(
            k in res
            for k in ("empty", "series_df", "bankroll", "max_dd", "wins", "total")
        )
        assert res["empty"] is False

    # ── ゼロ以下ベット除外 ─────────────────────────────────────

    def test_zero_bet_skipped(self):
        """actual_bet <= 0 のステップはスキップされ bankroll が変わらないこと。"""
        res = self._run([0], [5000], [True])
        assert res["bankroll"] == pytest.approx(1_000_000.0)
        # total はカウントされるが bankroll 変化なし
        assert res["total"] == 1


# ════════════════════════════════════════════════════════════════════
#  3. 派生 DataFrame キャッシュ関数のインターフェース検証
# ════════════════════════════════════════════════════════════════════


class TestCacheFunctionInterfaces:
    """_build_* 関数がモック DB でも正しい型を返すことを検証する。"""

    @pytest.fixture(autouse=True)
    def _mod(self, app):
        self.app = app

    def test_build_monthly_total_returns_tuple_of_dataframes(self, monkeypatch):
        empty_df = pd.DataFrame(
            columns=[
                "month",
                "model_type",
                "bet_type",
                "bets",
                "hits",
                "invested",
                "payout",
            ]
        )
        monkeypatch.setattr(self.app, "fetch_monthly_roi", lambda kind: empty_df)
        result = self.app._build_monthly_total(kind="all")
        assert isinstance(result, tuple) and len(result) == 2
        assert all(isinstance(r, pd.DataFrame) for r in result)

    def test_build_kelly_series_returns_empty_dict_on_empty(self, monkeypatch):
        empty_df = pd.DataFrame(
            columns=["recommended_bet", "payout", "is_hit", "created_at"]
        )
        monkeypatch.setattr(self.app, "fetch_kelly_simulation", lambda kind: empty_df)
        result = self.app._build_kelly_series(kind="all")
        assert result.get("empty") is True

    def test_build_venue_stats_returns_dataframe(self, monkeypatch):
        empty_df = pd.DataFrame()
        monkeypatch.setattr(self.app, "fetch_venue_performance", lambda kind: empty_df)
        result = self.app._build_venue_stats(kind="all")
        assert isinstance(result, pd.DataFrame)

    def test_build_monthly_total_with_data(self, monkeypatch):
        df = pd.DataFrame(
            {
                "month": ["2025-04", "2025-04", "2025-05"],
                "model_type": ["本命", "卍", "本命"],
                "bet_type": ["単勝", "複勝", "単勝"],
                "bets": [10, 5, 8],
                "hits": [3, 2, 4],
                "invested": [10_000, 5_000, 8_000],
                "payout": [15_000, 8_000, 20_000],
            }
        )
        monkeypatch.setattr(self.app, "fetch_monthly_roi", lambda kind: df)
        monthly_df, monthly_total = self.app._build_monthly_total(kind="all")
        assert "roi" in monthly_df.columns
        assert "hit_rate" in monthly_df.columns
        assert "roi" in monthly_total.columns
        assert len(monthly_total) == 2  # 2 月分に集約

    def test_build_kelly_series_with_data(self, monkeypatch):
        df = pd.DataFrame(
            {
                "recommended_bet": [1000.0, 2000.0, 1500.0],
                "payout": [3000.0, 0.0, 4500.0],
                "is_hit": [1, 0, 1],
                "created_at": ["2025-04-01"] * 3,
            }
        )
        monkeypatch.setattr(self.app, "fetch_kelly_simulation", lambda kind: df)
        result = self.app._build_kelly_series(kind="all")
        assert result["empty"] is False
        assert result["wins"] == 2
        assert result["total"] == 3
        assert isinstance(result["series_df"], pd.DataFrame)
        assert len(result["series_df"]) == 4  # n + 1
