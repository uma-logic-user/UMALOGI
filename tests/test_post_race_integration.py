"""today_auto_runner.py への敗因分析(Phase-A)組み込み検証。

重い常駐モジュールを import せず、ソーステキストを解析して
「週次レポート直後に非同期・best-effort で起動される」配線を担保する。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_RUNNER = Path(__file__).resolve().parents[1] / "scripts" / "today_auto_runner.py"


@pytest.fixture(scope="module")
def src() -> str:
    return _RUNNER.read_text(encoding="utf-8")


def test_ast_parse_clean(src: str) -> None:
    """構文エラーがないこと。"""
    ast.parse(src)


def test_threading_imported(src: str) -> None:
    assert "import threading" in src


def test_kick_function_defined(src: str) -> None:
    assert "def _kick_post_race_analysis(" in src


def test_kicked_after_weekly_report(src: str) -> None:
    """敗因分析は週次レポート送信の「後」に呼ばれること。"""
    wr = src.find("_send_weekly_report(target_date, dry_run)")
    kick = src.find("_kick_post_race_analysis(target_date, dry_run)")
    assert wr > 0 and kick > 0
    assert wr < kick, "敗因分析が週次レポートより前に配置されている"


def test_kick_is_async_daemon_and_guarded(src: str) -> None:
    """非同期(daemonスレッド)かつ例外内包(best-effort)であること。"""
    start = src.find("def _kick_post_race_analysis(")
    end = src.find("\ndef ", start + 1)
    body = src[start:end]
    assert "daemon=True" in body  # 非同期 daemon スレッド
    assert "threading.Thread(" in body
    assert "try:" in body and "except Exception" in body  # best-effort
    assert "run_post_race_analysis" in body  # 分析エンジン呼び出し


def test_dry_run_skips_kick(src: str) -> None:
    """dry_run 時は起動しない分岐があること。"""
    start = src.find("def _kick_post_race_analysis(")
    end = src.find("\ndef ", start + 1)
    body = src[start:end]
    assert "if dry_run:" in body
