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
import sys
import types
import unittest.mock as mock
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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
        preceding = self.src[max(0, idx - 60): idx]
        assert "@st.fragment" in preceding

    def test_render_hit_performance_has_fragment(self):
        """render_hit_performance の直前に @st.fragment があること。"""
        idx = self.src.find("def render_hit_performance(")
        assert idx > 0
        preceding = self.src[max(0, idx - 60): idx]
        assert "@st.fragment" in preceding

    @pytest.mark.parametrize("fn_name", [
        "_kelly_simulate_core",
        "_build_monthly_total",
        "_build_kelly_series",
        "_build_venue_stats",
    ])
    def test_cache_functions_defined(self, fn_name: str):
        assert f"def {fn_name}(" in self.src

    def test_build_monthly_total_has_cache_data(self):
        idx = self.src.find("def _build_monthly_total(")
        pre = self.src[max(0, idx - 80): idx]
        assert "@st.cache_data" in pre

    def test_build_kelly_series_has_cache_data(self):
        idx = self.src.find("def _build_kelly_series(")
        pre = self.src[max(0, idx - 80): idx]
        assert "@st.cache_data" in pre

    def test_build_venue_stats_has_cache_data(self):
        idx = self.src.find("def _build_venue_stats(")
        pre = self.src[max(0, idx - 80): idx]
        assert "@st.cache_data" in pre

    def test_bias_panel_placed_before_subtabs(self):
        """render_bias_panel がサブタブ宣言 (stab_prov) より前に来ること。"""
        bias_idx  = self.src.find("render_bias_panel(selected_race_id)")
        stab_idx  = self.src.find("stab_prov, stab_final")
        assert bias_idx < stab_idx, "render_bias_panel がサブタブ宣言より後ろにある"


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
            bets_arr    = np.array(bets,    dtype=np.float64),
            payouts_arr = np.array(payouts, dtype=np.float64),
            hits_arr    = np.array(hits,    dtype=bool),
            dates_arr   = np.array(d,       dtype=object),
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
        assert all(k in res for k in ("empty", "series_df", "bankroll", "max_dd", "wins", "total"))
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
        empty_df = pd.DataFrame(columns=["month", "model_type", "bet_type",
                                          "bets", "hits", "invested", "payout"])
        monkeypatch.setattr(self.app, "fetch_monthly_roi", lambda kind: empty_df)
        result = self.app._build_monthly_total(kind="all")
        assert isinstance(result, tuple) and len(result) == 2
        assert all(isinstance(r, pd.DataFrame) for r in result)

    def test_build_kelly_series_returns_empty_dict_on_empty(self, monkeypatch):
        empty_df = pd.DataFrame(columns=["recommended_bet", "payout", "is_hit", "created_at"])
        monkeypatch.setattr(self.app, "fetch_kelly_simulation", lambda kind: empty_df)
        result = self.app._build_kelly_series(kind="all")
        assert result.get("empty") is True

    def test_build_venue_stats_returns_dataframe(self, monkeypatch):
        empty_df = pd.DataFrame()
        monkeypatch.setattr(self.app, "fetch_venue_performance", lambda kind: empty_df)
        result = self.app._build_venue_stats(kind="all")
        assert isinstance(result, pd.DataFrame)

    def test_build_monthly_total_with_data(self, monkeypatch):
        df = pd.DataFrame({
            "month":      ["2025-04", "2025-04", "2025-05"],
            "model_type": ["本命", "卍", "本命"],
            "bet_type":   ["単勝", "複勝", "単勝"],
            "bets":       [10, 5, 8],
            "hits":       [3, 2, 4],
            "invested":   [10_000, 5_000, 8_000],
            "payout":     [15_000, 8_000, 20_000],
        })
        monkeypatch.setattr(self.app, "fetch_monthly_roi", lambda kind: df)
        monthly_df, monthly_total = self.app._build_monthly_total(kind="all")
        assert "roi" in monthly_df.columns
        assert "hit_rate" in monthly_df.columns
        assert "roi" in monthly_total.columns
        assert len(monthly_total) == 2  # 2 月分に集約

    def test_build_kelly_series_with_data(self, monkeypatch):
        df = pd.DataFrame({
            "recommended_bet": [1000.0, 2000.0, 1500.0],
            "payout":          [3000.0, 0.0,    4500.0],
            "is_hit":          [1,      0,       1],
            "created_at":      ["2025-04-01"] * 3,
        })
        monkeypatch.setattr(self.app, "fetch_kelly_simulation", lambda kind: df)
        result = self.app._build_kelly_series(kind="all")
        assert result["empty"] is False
        assert result["wins"] == 2
        assert result["total"] == 3
        assert isinstance(result["series_df"], pd.DataFrame)
        assert len(result["series_df"]) == 4  # n + 1
