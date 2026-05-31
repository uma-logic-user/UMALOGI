"""
src/ops/jvlink_dialog_handler.py のユニットテスト。

win32gui / win32api / win32con は Windows 専用のため unittest.mock でスタブ化する。
"""

from __future__ import annotations

import sys
import threading
import time
import types
from unittest.mock import MagicMock


# ── win32 モジュールのスタブをシステムに注入 ─────────────────────────────────
def _make_win32_stubs() -> None:
    """win32gui / win32api / win32con のスタブモジュールをインポート前に挿入する。"""

    # win32con 定数
    win32con = types.ModuleType("win32con")
    win32con.BM_CLICK = 0x00F5
    win32con.WM_COMMAND = 0x0111
    win32con.WM_KEYDOWN = 0x0100
    win32con.WM_KEYUP = 0x0101
    win32con.IDOK = 1
    win32con.VK_RETURN = 0x0D
    sys.modules.setdefault("win32con", win32con)

    win32gui = types.ModuleType("win32gui")
    win32gui.EnumWindows = MagicMock()
    win32gui.IsWindowVisible = MagicMock(return_value=True)
    win32gui.GetWindowText = MagicMock(return_value="")
    win32gui.GetClassName = MagicMock(return_value="")
    win32gui.EnumChildWindows = MagicMock()
    win32gui.SendMessage = MagicMock()
    sys.modules.setdefault("win32gui", win32gui)

    win32api = types.ModuleType("win32api")
    win32api.PostMessage = MagicMock()
    sys.modules.setdefault("win32api", win32api)


_make_win32_stubs()

# ── ここで初めてモジュールをインポート ────────────────────────────────────────
import importlib
import src.ops.jvlink_dialog_handler as _mod_orig


def _fresh() -> types.ModuleType:
    """テストごとにモジュール状態とモックをリセットしたコピーを返す。"""
    mod = importlib.reload(_mod_orig)
    mod._running = False
    mod._last_click.clear()
    mod._first_seen.clear()
    mod.stats["dialogs_dismissed"] = 0
    mod.stats["click_attempts"] = 0
    mod.stats["stubborn_dialogs"] = 0

    # 共有スタブのモック状態を毎回リセット（side_effect の汚染を防ぐ）
    import win32gui as _wg
    import win32api as _wa

    _wg.EnumWindows = MagicMock()
    _wg.IsWindowVisible = MagicMock(return_value=True)
    _wg.GetWindowText = MagicMock(return_value="")
    _wg.GetClassName = MagicMock(return_value="")
    _wg.EnumChildWindows = MagicMock()
    _wg.SendMessage = MagicMock()
    _wa.PostMessage = MagicMock()

    return mod


# ── _is_target_window ─────────────────────────────────────────────────────────


class TestIsTargetWindow:
    def test_jvlink_title(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("JVLink 設定") is True

    def test_jravan_title(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("JRA-VAN データ更新") is True

    def test_setup_title(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("Setup Wizard") is True

    def test_katakana_setup(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("セットアップ") is True

    def test_unrelated_title(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("メモ帳") is False

    def test_empty_title(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("") is False

    def test_case_insensitive(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("JVLINK VIEWER") is True

    def test_license(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("License Agreement") is True

    def test_update(self) -> None:
        mod = _fresh()
        assert mod._is_target_window("update available") is True


# ── _find_best_button ─────────────────────────────────────────────────────────


class TestFindBestButton:
    def test_ok_button_found(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(100, None)  # hwnd 100 = OK ボタン

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = "OK"

        result = mod._find_best_button(999)
        assert result == 100

    def test_priority_next_over_close(self) -> None:
        mod = _fresh()
        import win32gui

        buttons = {200: "閉じる", 201: "次へ"}

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(200, None)
            cb(201, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.side_effect = lambda h: buttons.get(h, "")

        result = mod._find_best_button(999)
        assert result == 201  # 次へ の方が優先順位が高い

    def test_no_button_returns_none(self) -> None:
        mod = _fresh()
        import win32gui

        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        result = mod._find_best_button(999)
        assert result is None

    def test_non_button_class_ignored(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(300, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Edit"  # ボタンでないクラス

        result = mod._find_best_button(999)
        assert result is None


# ── _dismiss_dialog ───────────────────────────────────────────────────────────


class TestDismissDialog:
    def test_bm_click_success(self) -> None:
        mod = _fresh()
        import win32gui
        import win32con

        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: cb(50, None)
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = "OK"
        win32gui.SendMessage.reset_mock()

        result = mod._dismiss_dialog(999, "JVLink 設定")
        assert result is True
        assert mod.stats["dialogs_dismissed"] == 1
        win32gui.SendMessage.assert_called_once_with(50, win32con.BM_CLICK, 0, 0)

    def test_cooldown_prevents_reclick(self) -> None:
        mod = _fresh()
        mod._last_click[999] = time.monotonic()  # 今クリック済みとみなす

        import win32gui

        win32gui.SendMessage.reset_mock()

        result = mod._dismiss_dialog(999, "JVLink 設定")
        assert result is False
        win32gui.SendMessage.assert_not_called()

    def test_fallback_to_wm_command(self) -> None:
        mod = _fresh()
        import win32api
        import win32con
        import win32gui

        # ボタン見つからない
        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        win32api.PostMessage.reset_mock()

        result = mod._dismiss_dialog(888, "設定ウィンドウ")
        assert result is True
        win32api.PostMessage.assert_called_once_with(
            888, win32con.WM_COMMAND, win32con.IDOK, 0
        )

    def test_fallback_to_vk_return_when_wm_command_raises(self) -> None:
        mod = _fresh()
        import win32api
        import win32con
        import win32gui

        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None

        call_count = [0]

        def _post(hwnd, msg, wp, lp):
            call_count[0] += 1
            if msg == win32con.WM_COMMAND:
                raise OSError("access denied")

        win32api.PostMessage.side_effect = _post

        result = mod._dismiss_dialog(777, "セットアップ")
        assert result is True
        # WM_KEYDOWN と WM_KEYUP の 2 回呼ばれるはず
        assert call_count[0] == 3  # WM_COMMAND(失敗) + WM_KEYDOWN + WM_KEYUP

    def test_stubborn_dialog_warning(self) -> None:
        mod = _fresh()
        import win32gui
        import win32api

        # 初回検出を 5 秒前に設定してすぐ頑固扱いにする
        mod._first_seen[555] = time.monotonic() - 5.0
        mod._last_click[555] = 0.0  # クールダウンリセット

        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        win32api.PostMessage.reset_mock()

        result = mod._dismiss_dialog(555, "JVLink 認証")
        assert result is True
        assert mod.stats["stubborn_dialogs"] == 1


# ── _scan_windows ─────────────────────────────────────────────────────────────


class TestScanWindows:
    def test_target_window_dismissed(self) -> None:
        mod = _fresh()
        import win32gui
        import win32api

        def _enum_top(cb, extra: object) -> None:
            cb(1001, None)

        win32gui.EnumWindows.side_effect = _enum_top
        win32gui.IsWindowVisible.return_value = True
        win32gui.GetWindowText.side_effect = lambda h: (
            "JVLink 設定" if h == 1001 else ""
        )
        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        win32api.PostMessage.reset_mock()

        mod._scan_windows()
        assert mod.stats["click_attempts"] == 1

    def test_non_target_window_ignored(self) -> None:
        mod = _fresh()
        import win32gui

        def _enum_top(cb, extra: object) -> None:
            cb(2001, None)

        win32gui.EnumWindows.side_effect = _enum_top
        win32gui.IsWindowVisible.return_value = True
        win32gui.GetWindowText.side_effect = lambda h: "Notepad" if h == 2001 else ""

        mod._scan_windows()
        assert mod.stats["click_attempts"] == 0

    def test_invisible_window_skipped(self) -> None:
        mod = _fresh()
        import win32gui

        def _enum_top(cb, extra: object) -> None:
            cb(3001, None)

        win32gui.EnumWindows.side_effect = _enum_top
        win32gui.IsWindowVisible.return_value = False
        win32gui.GetWindowText.return_value = "JVLink 設定"

        mod._scan_windows()
        assert mod.stats["click_attempts"] == 0

    def test_enum_windows_exception_does_not_crash(self) -> None:
        mod = _fresh()
        import win32gui

        win32gui.EnumWindows.side_effect = OSError("access denied")

        # 例外が伝播しないことを確認
        mod._scan_windows()


# ── start_dialog_handler / stop_dialog_handler ───────────────────────────────


class TestStartStopHandler:
    def test_start_returns_thread(self) -> None:
        mod = _fresh()
        t = mod.start_dialog_handler(interval=0.05)
        assert isinstance(t, threading.Thread)
        assert t.daemon is True
        mod.stop_dialog_handler()
        t.join(timeout=1.0)

    def test_double_start_returns_same_thread(self) -> None:
        mod = _fresh()
        t1 = mod.start_dialog_handler(interval=0.05)
        t2 = mod.start_dialog_handler(interval=0.05)
        assert t1 is t2
        mod.stop_dialog_handler()
        t1.join(timeout=1.0)

    def test_stop_terminates_loop(self) -> None:
        mod = _fresh()

        import win32gui

        win32gui.EnumWindows.side_effect = lambda cb, ex: None  # 何もしない

        t = mod.start_dialog_handler(interval=0.05)
        time.sleep(0.15)
        assert t.is_alive()
        mod.stop_dialog_handler()
        t.join(timeout=2.0)
        assert not t.is_alive()

    def test_no_win32_fallback_to_dummy_thread(self) -> None:
        """win32gui がない環境ではダミースレッドを返す。"""
        mod = _fresh()
        saved = sys.modules.get("win32gui")
        del sys.modules["win32gui"]
        try:
            t = mod.start_dialog_handler()
            assert isinstance(t, threading.Thread)
        finally:
            if saved is not None:
                sys.modules["win32gui"] = saved


# ── _is_setup_dialog ──────────────────────────────────────────────────────────


class TestIsSetupDialog:
    def test_katakana_setup_title(self) -> None:
        mod = _fresh()
        assert mod._is_setup_dialog("セットアップ") is True

    def test_english_setup_title(self) -> None:
        mod = _fresh()
        assert mod._is_setup_dialog("Setup Wizard") is True

    def test_jvlink_title_not_setup(self) -> None:
        mod = _fresh()
        assert mod._is_setup_dialog("JVLink 設定") is False

    def test_empty_not_setup(self) -> None:
        mod = _fresh()
        assert mod._is_setup_dialog("") is False


# ── _select_no_startkit_radio ─────────────────────────────────────────────────


class TestSelectNoStartkitRadio:
    def test_radio_found_by_持っていない(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(200, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = (
            "スタートキット（CD/DVD-ROM）を持っていない"
        )
        win32gui.SendMessage.reset_mock()

        result = mod._select_no_startkit_radio(999)
        assert result is True
        win32gui.SendMessage.assert_called_once_with(
            200, mod.BM_SETCHECK, mod.BST_CHECKED, 0
        )

    def test_radio_found_by_スタートキット(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(201, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = "スタートキットを使用しない"
        win32gui.SendMessage.reset_mock()

        result = mod._select_no_startkit_radio(999)
        assert result is True

    def test_no_matching_radio_returns_false(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(202, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = "キャンセル"

        result = mod._select_no_startkit_radio(999)
        assert result is False

    def test_non_button_class_ignored(self) -> None:
        mod = _fresh()
        import win32gui

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(203, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Static"
        win32gui.GetWindowText.return_value = "持っていない"
        win32gui.SendMessage.reset_mock()

        result = mod._select_no_startkit_radio(999)
        assert result is False
        win32gui.SendMessage.assert_not_called()

    def test_enum_exception_returns_false(self) -> None:
        mod = _fresh()
        import win32gui

        win32gui.EnumChildWindows.side_effect = OSError("access denied")

        result = mod._select_no_startkit_radio(999)
        assert result is False


# ── _dismiss_dialog (セットアップ専用) ───────────────────────────────────────


class TestDismissSetupDialog:
    def test_setup_dialog_selects_radio_then_clicks_ok(self) -> None:
        """セットアップダイアログはラジオ選択 → OK クリックの2段階で突破する。"""
        mod = _fresh()
        import win32gui
        import win32con

        radio_selected: list[int] = []
        ok_clicked: list[int] = []

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(300, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = (
            "スタートキット（CD/DVD-ROM）を持っていない"
        )

        def _send_msg(h, msg, wp, lp):
            if msg == mod.BM_SETCHECK:
                radio_selected.append(h)
            elif msg == win32con.BM_CLICK:
                ok_clicked.append(h)

        win32gui.SendMessage.side_effect = _send_msg

        result = mod._dismiss_dialog(999, "セットアップ")
        assert result is True
        assert len(radio_selected) == 1, "ラジオボタンが選択されていない"
        assert mod.stats["dialogs_dismissed"] == 1

    def test_setup_dialog_no_radio_still_clicks_ok(self) -> None:
        """ラジオボタンが見つからなくても OK クリックは試みる（フォールバック）。"""
        mod = _fresh()
        import win32api
        import win32gui

        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        win32api.PostMessage.reset_mock()

        result = mod._dismiss_dialog(888, "セットアップ")
        assert result is True  # WM_COMMAND IDOK で成功


# ── jvlink_guard ──────────────────────────────────────────────────────────────


class TestJvlinkGuard:
    def test_guard_starts_handler(self) -> None:
        """with jvlink_guard(): ブロック内でハンドラーが起動している。"""
        mod = _fresh()
        import win32gui

        win32gui.EnumWindows.side_effect = lambda cb, ex: None

        with mod.jvlink_guard(interval=0.05) as t:
            assert isinstance(t, threading.Thread)
            assert t.is_alive()
        mod.stop_dialog_handler()

    def test_guard_reentrant(self) -> None:
        """入れ子の jvlink_guard() は同一スレッドを返す（多重起動しない）。"""
        mod = _fresh()
        import win32gui

        win32gui.EnumWindows.side_effect = lambda cb, ex: None

        with mod.jvlink_guard(interval=0.05) as t1:
            with mod.jvlink_guard(interval=0.05) as t2:
                assert t1 is t2
        mod.stop_dialog_handler()
