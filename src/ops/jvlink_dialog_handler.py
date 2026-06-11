"""
JVLink ダイアログ自動突破ハンドラー

Windows デスクトップに出現する JVLink / 設定 / セットアップ系ダイアログを
0.3 秒間隔でスキャンし、OK・次へ・はい ボタンを自動クリックして消去する。

scheduler.py の run_daemon() から daemon スレッドとして起動される。
JVLink の GUI ブロックが発生しても、既存の 10 秒タイムアウト → netkeiba
フォールバック経路と共存し、二重の安全網として機能する。

動作フロー:
  1. EnumWindows でトップレベルウィンドウを列挙
  2. タイトルが _TARGET_TITLE_PATTERNS に一致するウィンドウを抽出
  3. 子ウィンドウを EnumChildWindows で走査し優先ボタンを探す
  4. 見つかれば BM_CLICK → なければ WM_COMMAND IDOK → なければ VK_RETURN
  5. クリック後 1.5 秒のクールダウンで同一 hwnd への連打を防止
  6. 3 秒経過後もウィンドウが残存する場合は「頑固なダイアログ」として警告ログ
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

# ── ターゲットウィンドウタイトルパターン（小文字で前方一致・部分一致）─────────
#
# 【重要】"設定" / "更新" 等の広すぎるパターンは Chrome/Edge のタブタイトルにも一致する。
# ブラウザウィンドウへの誤クリックを防ぐため、_EXCLUDED_WIN_CLASSES で除外すること。
#
_TARGET_TITLE_PATTERNS: tuple[str, ...] = (
    "jvlink",
    "jra-van",
    "jravan",
    "jvdtlab",
    "設定",
    "セットアップ",
    "setup",
    "target frontier",
    "targetfrontier",
    "認証",
    "ライセンス",
    "license",
    "使用許諾",
    "更新",
    "アップデート",
    "update",
    "競馬データ",
    "jvlink viewer",
)

# ── ブラウザ・UWP系ウィンドウのクラス名（小文字）— 誤操作防止で除外 ──────────
#
# これらのウィンドウクラスは Win32 ネイティブダイアログではなく、
# JVLink と無関係のアプリ（ブラウザ・UWP）なので絶対に操作しない。
#
_EXCLUDED_WIN_CLASSES: frozenset[str] = frozenset(
    {
        "chrome_widgetwin_1",  # Google Chrome / Microsoft Edge (Chromium)
        "chrome_widgetwin_0",  # Chrome / Edge サブウィンドウ
        "mozillawindowclass",  # Mozilla Firefox メインウィンドウ
        "mozilladialogclass",  # Firefox ダイアログ
        "ieframe",  # Internet Explorer / IE モード
        "applicationframewindow",  # Windows 10/11 UWP ホスト
        "windows.ui.core.corewindow",  # UWP コアウィンドウ
        "msoswp",  # Microsoft Office WebPane (IE内蔵)
        # ── ターミナル系（タイトルに作業内容が出るため "セットアップ" 等に誤一致する）──
        "consolewindowclass",  # conhost 古典コンソール（cmd / PowerShell）
        "cascadia_hosting_window_class",  # Windows Terminal
        "vscodemainwindow",  # VS Code（統合ターミナルのタイトル反映対策）
        # ── Delphi アプリ本体（TARGET frontier JV）────────────────────────────
        # TApplication は Delphi 製アプリのタスクバー用不可視ウィンドウで
        # ダイアログではない（IDOK を送っても消えず無限連打になる）。
        # TARGET の実ダイアログは #32770 / TForm 系なので影響しない。
        "tapplication",
    }
)

# ── ボタンテキスト優先順（小文字で部分一致）───────────────────────────────────
_BUTTON_PRIORITY: tuple[str, ...] = (
    "ok",
    "はい",
    "yes",
    "次へ",
    "next",
    "続行",
    "continue",
    "完了",
    "finish",
    "閉じる",
    "close",
)

# ── BM_SETCHECK 定数（ラジオボタン選択用）────────────────────────────────────
BM_SETCHECK: int = 0x00F1
BST_CHECKED: int = 1

# ── セットアップダイアログ判定パターン（小文字で部分一致）──────────────────
_SETUP_TITLE_PATTERNS: tuple[str, ...] = ("セットアップ", "setup")

# ── 「スタートキットを持っていない」ラジオボタン識別パターン（小文字部分一致）
_NO_STARTKIT_PATTERNS: tuple[str, ...] = (
    "持っていない",
    "持ってない",
    "スタートキット",
    "cd/dvd",
    "starterkit",
    "starter kit",
)

# ── クールダウン（秒）: 同一 hwnd への再クリックを防ぐ ─────────────────────────
_CLICK_COOLDOWN = 1.5

# ── hwnd → 最終クリック monotonic 時刻 ─────────────────────────────────────────
_last_click: dict[int, float] = {}

# ── hwnd → 初回検出 monotonic 時刻（頑固ダイアログ検出用）─────────────────────
_first_seen: dict[int, float] = {}

# ── 頑固ダイアログ判定閾値（秒）────────────────────────────────────────────────
_STUBBORN_THRESHOLD = 3.0

# ── 統計カウンター ────────────────────────────────────────────────────────────
stats: dict[str, int] = {
    "dialogs_dismissed": 0,
    "click_attempts": 0,
    "stubborn_dialogs": 0,
}

# ── スレッド制御 ──────────────────────────────────────────────────────────────
_running = False
_thread: threading.Thread | None = None


# ── ウィンドウ判定 ─────────────────────────────────────────────────────────────


def _is_target_window(title: str) -> bool:
    """タイトルがダイアログ対象パターンに一致するか判定する。

    Args:
        title: ウィンドウのタイトル文字列。

    Returns:
        _TARGET_TITLE_PATTERNS のいずれかとマッチする場合 True。
    """
    t = title.lower()
    return any(p in t for p in _TARGET_TITLE_PATTERNS)


def _is_setup_dialog(title: str) -> bool:
    """タイトルがセットアップダイアログかどうかを判定する。

    セットアップダイアログは2段階突破（ラジオ選択 + OK クリック）が必要なため
    通常のダイアログと区別する。

    Args:
        title: ウィンドウのタイトル文字列。

    Returns:
        _SETUP_TITLE_PATTERNS のいずれかとマッチする場合 True。
    """
    t = title.lower()
    return any(p in t for p in _SETUP_TITLE_PATTERNS)


def _select_no_startkit_radio(hwnd: int) -> bool:
    """
    セットアップダイアログの「スタートキットを持っていない」ラジオボタンを
    選択状態（BST_CHECKED）にする。

    Returns:
        True : ラジオボタンを見つけて選択完了
        False: ラジオボタンが見つからなかった
    """
    import win32gui

    found: list[int] = []

    def _enum_cb(child_hwnd: int, _: object) -> bool:
        try:
            if win32gui.GetClassName(child_hwnd) != "Button":
                return True
            text = win32gui.GetWindowText(child_hwnd).lower()
            for pat in _NO_STARTKIT_PATTERNS:
                if pat in text:
                    found.append(child_hwnd)
                    return False  # 最初の一致で列挙終了
        except Exception:
            pass
        return True

    try:
        win32gui.EnumChildWindows(hwnd, _enum_cb, None)
    except Exception:
        pass

    if not found:
        return False

    radio_hwnd = found[0]
    try:
        win32gui.SendMessage(radio_hwnd, BM_SETCHECK, BST_CHECKED, 0)
        btn_text = win32gui.GetWindowText(radio_hwnd)
        logger.info(
            "[DialogHandler] ✅ ラジオ選択完了: %r hwnd=%d", btn_text, radio_hwnd
        )
        return True
    except Exception as exc:
        logger.debug("[DialogHandler] BM_SETCHECK 失敗: %s", exc)
        return False


# ── ボタン検索 ────────────────────────────────────────────────────────────────


def _find_best_button(hwnd: int) -> int | None:
    """
    子ウィンドウ中から最優先ボタンの HWND を返す。
    優先順は _BUTTON_PRIORITY の順。見つからなければ None。
    """
    import win32gui

    candidates: list[tuple[int, int]] = []  # (child_hwnd, priority_index)

    def _enum_cb(child_hwnd: int, _: object) -> bool:
        try:
            if win32gui.GetClassName(child_hwnd) != "Button":
                return True
            text = win32gui.GetWindowText(child_hwnd).strip().lower()
            for i, pat in enumerate(_BUTTON_PRIORITY):
                if pat in text:
                    candidates.append((child_hwnd, i))
                    break
        except Exception:
            pass
        return True

    try:
        win32gui.EnumChildWindows(hwnd, _enum_cb, None)
    except Exception:
        pass

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]


# ── ダイアログ閉じ処理 ────────────────────────────────────────────────────────


def _dismiss_dialog(hwnd: int, title: str) -> bool:
    """
    ダイアログを閉じる。成功したら True を返す。

    セットアップダイアログ（タイトルに「セットアップ」等を含む）の場合は2段階突破:
      1. 「スタートキットを持っていない」ラジオボタンを BM_SETCHECK で選択
      2. OK ボタンを BM_CLICK

    通常ダイアログの試行順:
      1. 優先ボタンを見つけて BM_CLICK
      2. WM_COMMAND IDOK をポスト
      3. VK_RETURN キーイベントをポスト
    """
    import win32api
    import win32con
    import win32gui

    now = time.monotonic()
    if now - _last_click.get(hwnd, 0.0) < _CLICK_COOLDOWN:
        return False

    # 頑固ダイアログ検出
    if hwnd not in _first_seen:
        _first_seen[hwnd] = now
    elif now - _first_seen[hwnd] >= _STUBBORN_THRESHOLD:
        stats["stubborn_dialogs"] += 1
        logger.warning(
            "[DialogHandler] 頑固なダイアログ: %.1f 秒経過しても消えません "
            "title=%r hwnd=%d — netkeiba フォールバックに期待",
            now - _first_seen[hwnd],
            title,
            hwnd,
        )
        # 頑固でも諦めずに再クリックを試みる

    stats["click_attempts"] += 1
    _last_click[hwnd] = now

    # ── セットアップダイアログ専用: 2段階突破 ──────────────────────────────
    if _is_setup_dialog(title):
        radio_ok = _select_no_startkit_radio(hwnd)
        if radio_ok:
            logger.info(
                "[DialogHandler] セットアップダイアログ: ラジオ選択完了 hwnd=%d", hwnd
            )
        else:
            logger.warning(
                "[DialogHandler] セットアップダイアログ: ラジオボタン未検出 "
                "title=%r hwnd=%d — OKのみクリックを試みます",
                title,
                hwnd,
            )
    # ───────────────────────────────────────────────────────────────────────

    # 方法 1: 優先ボタンを探して BM_CLICK
    button_hwnd = _find_best_button(hwnd)
    if button_hwnd is not None:
        try:
            btn_text = win32gui.GetWindowText(button_hwnd)
            win32gui.SendMessage(button_hwnd, win32con.BM_CLICK, 0, 0)
            logger.info(
                "[DialogHandler] ✅ BM_CLICK: title=%r button=%r hwnd=%d",
                title,
                btn_text,
                hwnd,
            )
            stats["dialogs_dismissed"] += 1
            _first_seen.pop(hwnd, None)
            return True
        except Exception as exc:
            logger.debug("[DialogHandler] BM_CLICK 失敗: %s", exc)

    # 方法 2: WM_COMMAND IDOK をポスト
    try:
        win32api.PostMessage(hwnd, win32con.WM_COMMAND, win32con.IDOK, 0)
        logger.info("[DialogHandler] ✅ WM_COMMAND IDOK: title=%r hwnd=%d", title, hwnd)
        stats["dialogs_dismissed"] += 1
        _first_seen.pop(hwnd, None)
        return True
    except Exception as exc:
        logger.debug("[DialogHandler] WM_COMMAND IDOK 失敗: %s", exc)

    # 方法 3: VK_RETURN キーをポスト
    try:
        win32api.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        win32api.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0)
        logger.info("[DialogHandler] ✅ VK_RETURN: title=%r hwnd=%d", title, hwnd)
        stats["dialogs_dismissed"] += 1
        _first_seen.pop(hwnd, None)
        return True
    except Exception as exc:
        logger.debug("[DialogHandler] VK_RETURN 失敗: %s", exc)

    return False


# ── ウィンドウスキャン ────────────────────────────────────────────────────────


def _is_browser_window(hwnd: int) -> bool:
    """ウィンドウクラスがブラウザ / UWP系かどうかを判定する。

    Chrome/Edge/Firefox 等のウィンドウは JVLink ダイアログではないため
    タイトルがパターンに一致しても絶対に操作しない。
    """
    try:
        import win32gui

        cls = win32gui.GetClassName(hwnd).lower()
        return cls in _EXCLUDED_WIN_CLASSES
    except Exception:
        return False  # クラス取得失敗 → 安全策として操作対象とみなす（従来動作）


def _scan_windows() -> None:
    """可視トップレベルウィンドウを列挙してダイアログを検出・クリック。"""
    import win32gui

    found: list[tuple[int, str]] = []

    def _callback(hwnd: int, _: object) -> bool:
        try:
            if not win32gui.IsWindowVisible(hwnd):
                return True
            title = win32gui.GetWindowText(hwnd)
            if not title or not _is_target_window(title):
                return True
            # ブラウザ / UWP ウィンドウへの誤クリックを防ぐ
            if _is_browser_window(hwnd):
                logger.debug(
                    "[DialogHandler] ブラウザ/UWPウィンドウをスキップ: title=%r", title
                )
                return True
            found.append((hwnd, title))
        except Exception:
            pass
        return True

    try:
        win32gui.EnumWindows(_callback, None)
    except Exception as exc:
        logger.debug("[DialogHandler] EnumWindows 例外: %s", exc)
        return

    for hwnd, title in found:
        _dismiss_dialog(hwnd, title)

    # _last_click / _first_seen の定期クリーンアップ（無効 hwnd のメモリリーク防止）
    # 10 分 = 600 秒以上前のエントリーを削除
    cutoff = time.monotonic() - 600
    stale = [h for h, t in _last_click.items() if t < cutoff]
    for h in stale:
        _last_click.pop(h, None)
        _first_seen.pop(h, None)


# ── メインループ ─────────────────────────────────────────────────────────────


def _handler_loop(interval: float) -> None:
    """ダイアログ監視のメインループ。_running が False になるまで継続する。

    Args:
        interval: ウィンドウスキャン間隔（秒）。
    """
    logger.info(
        "[DialogHandler] 起動 — JVLink/設定ダイアログを %.1f 秒間隔で監視中",
        interval,
    )
    while _running:
        try:
            _scan_windows()
        except Exception as exc:
            logger.debug("[DialogHandler] スキャン例外（続行）: %s", exc)
        time.sleep(interval)
    logger.info("[DialogHandler] 停止")


# ── 公開 API ─────────────────────────────────────────────────────────────────


def start_dialog_handler(interval: float = 0.3) -> threading.Thread:
    """
    ダイアログ自動突破ハンドラーをデーモンスレッドとして起動する。

    pywin32 が未インストールの環境ではダミースレッドを返して何もしない。

    Args:
        interval: ウィンドウスキャン間隔（秒）。デフォルト 0.3 秒。

    Returns:
        起動した Thread オブジェクト。
    """
    global _running, _thread

    try:
        import win32gui  # noqa: F401
    except ImportError:
        logger.warning(
            "[DialogHandler] pywin32 が未インストールです。ダイアログハンドラーを無効化します。"
            " pip install pywin32 でインストールしてください。"
        )
        dummy = threading.Thread(target=lambda: None, daemon=True)
        dummy.start()
        return dummy

    if _running and _thread is not None and _thread.is_alive():
        logger.info("[DialogHandler] 既に稼働中です")
        return _thread

    _running = True
    _thread = threading.Thread(
        target=_handler_loop,
        args=(interval,),
        name="jvlink_dialog_handler",
        daemon=True,
    )
    _thread.start()
    return _thread


def stop_dialog_handler() -> None:
    """ダイアログハンドラーを停止する（テスト・シャットダウン用）。

    _running フラグを False に設定してループを抜けさせる。
    スレッドの終了を待機しないため、呼び出し直後にスレッドが即座に停止するとは限らない。
    """
    global _running
    _running = False


# ── Context Manager ───────────────────────────────────────────────────────────

from contextlib import contextmanager
from collections.abc import Iterator


@contextmanager
def jvlink_guard(interval: float = 0.3) -> Iterator[threading.Thread]:
    """
    JVLink ダイアログ自動突破ハンドラーを有効にする Context Manager。

    スケジューラー経由でない実行文脈（バックテスト・シミュレーション・
    直接実行スクリプト）で JVLink を使用する際にこれで包む。

    Usage:
        from src.ops.jvlink_dialog_handler import jvlink_guard

        with jvlink_guard():
            loader = JVDataLoader(sid=os.environ["JRAVAN_SID"])
            stats  = loader.load("RACE", ...)

    多重起動は安全（既存スレッドがあれば再利用する）。
    daemon スレッドのためプロセス終了時に自動停止する。
    """
    thread = start_dialog_handler(interval=interval)
    try:
        yield thread
    finally:
        pass  # daemon スレッドはプロセス終了時に自動停止
