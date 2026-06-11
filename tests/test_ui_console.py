"""tests/test_ui_console.py — CUI Rich化ラッパー（src/ui/console.py）のテスト。

rich がインストールされていない環境でも plain フォールバックで全機能が
動作することを保証する（本番常駐プロセスを絶対に殺さない）。
"""

from __future__ import annotations

import io
import logging

import pytest

from src.ui.console import (
    RICH_AVAILABLE,
    UmaConsole,
    create_rich_log_handler,
    get_console,
)


def _make(force_plain: bool = False) -> tuple[UmaConsole, io.StringIO]:
    buf = io.StringIO()
    return UmaConsole(file=buf, force_plain=force_plain, width=100), buf


# ── 基本出力 ─────────────────────────────────────────────────────────────
class TestBasicOutput:
    @pytest.mark.parametrize("plain", [False, True])
    def test_banner_contains_title_and_subtitle(self, plain: bool) -> None:
        con, buf = _make(force_plain=plain)
        con.banner("UMA-LOGIC", "週次オートパイロット")
        out = buf.getvalue()
        assert "UMA-LOGIC" in out
        assert "週次オートパイロット" in out

    @pytest.mark.parametrize("plain", [False, True])
    def test_section_renders_title(self, plain: bool) -> None:
        con, buf = _make(force_plain=plain)
        con.section("金曜夜間バッチ")
        assert "金曜夜間バッチ" in buf.getvalue()

    @pytest.mark.parametrize("plain", [False, True])
    def test_levels_render_message(self, plain: bool) -> None:
        con, buf = _make(force_plain=plain)
        con.success("同期完了")
        con.warning("オッズ欠損")
        con.error("JVLink タイムアウト")
        con.info("待機中")
        out = buf.getvalue()
        for s in ("同期完了", "オッズ欠損", "JVLink タイムアウト", "待機中"):
            assert s in out


# ── 高EVシグナル ─────────────────────────────────────────────────────────
class TestEvSignal:
    @pytest.mark.parametrize("plain", [False, True])
    def test_ev_signal_panel_contains_key_facts(self, plain: bool) -> None:
        con, buf = _make(force_plain=plain)
        con.ev_signal("東京11R", "三連複", "5-9-12", ev=1.42, odds=48.3, stake=600)
        out = buf.getvalue()
        assert "東京11R" in out
        assert "三連複" in out
        assert "5-9-12" in out
        assert "1.42" in out
        assert "EV" in out

    @pytest.mark.parametrize("plain", [False, True])
    def test_candidates_table_renders_rows(self, plain: bool) -> None:
        con, buf = _make(force_plain=plain)
        rows = [
            {
                "label": "5-9-12",
                "bet_type": "三連複",
                "prob": 0.031,
                "odds": 48.3,
                "ev": 1.42,
            },
            {
                "label": "5→9→12",
                "bet_type": "三連単",
                "prob": 0.008,
                "odds": 210.0,
                "ev": 1.61,
            },
        ]
        con.candidates_table("高EV候補", rows)
        out = buf.getvalue()
        assert "高EV候補" in out
        assert "三連複" in out
        assert "1.61" in out


# ── プログレス ───────────────────────────────────────────────────────────
class TestProgress:
    @pytest.mark.parametrize("plain", [False, True])
    def test_track_yields_all_items(self, plain: bool) -> None:
        con, _ = _make(force_plain=plain)
        items = list(con.track(range(5), description="取得中"))
        assert items == [0, 1, 2, 3, 4]

    @pytest.mark.parametrize("plain", [False, True])
    def test_progress_context_manager_advances(self, plain: bool) -> None:
        con, _ = _make(force_plain=plain)
        with con.progress("同期", total=3) as advance:
            for _ in range(3):
                advance(1)
        # 例外なく完走すれば OK（非TTYでは描画されない）


# ── ロギング統合 ─────────────────────────────────────────────────────────
class TestRichLogging:
    def test_create_rich_log_handler_returns_handler_or_none(self) -> None:
        handler = create_rich_log_handler(level=logging.INFO)
        if RICH_AVAILABLE:
            assert isinstance(handler, logging.Handler)
        else:
            assert handler is None

    def test_setup_logging_use_rich_attaches_console_handler(self, tmp_path) -> None:
        from src.ops.logger import setup_logging

        logger = setup_logging(
            "test_rich", "test_rich.log", log_dir=tmp_path, use_rich=True
        )
        logger.info("rich logging smoke")
        root = logging.getLogger()
        assert len(root.handlers) == 2  # file + console（rich or plain fallback）

    def test_setup_logging_default_keeps_plain_stream(self, tmp_path) -> None:
        from src.ops.logger import setup_logging

        setup_logging("test_plain", "test_plain.log", log_dir=tmp_path)
        root = logging.getLogger()
        kinds = [type(h).__name__ for h in root.handlers]
        assert "RichHandler" not in kinds


# ── シングルトン ─────────────────────────────────────────────────────────
def test_get_console_is_singleton() -> None:
    assert get_console() is get_console()
