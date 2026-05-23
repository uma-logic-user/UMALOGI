# JVLink セットアップダイアログ完全自動突破 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** JVLinkが出す2段階のセットアップGUIダイアログ（第1段:設定系ダイアログ→OK、第2段:「スタートキットを持っていない」ラジオ選択→OK）を0.3秒以内に完全自動突破し、スケジューラー外のすべてのJVLink呼び出し経路でもNetkeibaフォールバックが発生しない状態にする。

**Architecture:** `jvlink_dialog_handler.py`にセットアップダイアログ専用の2段階突破ロジック（ラジオボタン自動選択→OKクリック）を追加。`JVLinkClient._connect()`の先頭でハンドラーを自動起動することで、スケジューラー・バックテスト・シミュレーション等すべての実行文脈を保護する。`jvlink_guard()` Context Managerで外部スクリプトからも明示的に使用可能にする。

**Tech Stack:** Python 3.11+, pywin32 (win32gui/win32api/win32con), threading, contextlib

---

## File Map

| 操作 | ファイル | 変更内容 |
|------|---------|---------|
| 改修 | `src/ops/jvlink_dialog_handler.py` | 定数追加・`_is_setup_dialog()`・`_select_no_startkit_radio()`・`_dismiss_dialog()`修正・`jvlink_guard()` CM追加 |
| 改修 | `src/scraper/jravan_client.py` | `JVLinkClient._connect()`先頭にハンドラー自動起動フック追加 |
| 改修 | `tests/test_jvlink_dialog_handler.py` | セットアップダイアログ2段階突破テスト追加（約10ケース） |
| 新規 | `.claudecode/rules/jvlink_popup_management.md` | 恒久ルールファイル |
| 改修 | `docs/6_special_notes.md` | Changelog更新 |

---

## Task 1: `_is_setup_dialog()` と `_select_no_startkit_radio()` の追加

**Files:**
- Modify: `src/ops/jvlink_dialog_handler.py:28-82` (定数ブロック＋新関数)
- Test: `tests/test_jvlink_dialog_handler.py`

- [ ] **Step 1: 既存テストが全て通ることを確認**

```powershell
cd C:\dev\horse-racing-ai
py -m pytest tests/test_jvlink_dialog_handler.py -v 2>&1 | Select-String -Pattern "PASSED|FAILED|ERROR|passed|failed|error"
```

期待: 26件 PASSED

- [ ] **Step 2: セットアップダイアログ検出テストを追加（失敗するまで書く）**

`tests/test_jvlink_dialog_handler.py` の末尾に以下を追記:

```python
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
        import win32gui, win32con

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(200, None)

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.return_value = "Button"
        win32gui.GetWindowText.return_value = "スタートキット（CD/DVD-ROM）を持っていない"
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
        win32gui.GetClassName.return_value = "Static"  # Buttonではない
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
```

- [ ] **Step 3: テストを実行して失敗することを確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py::TestIsSetupDialog tests/test_jvlink_dialog_handler.py::TestSelectNoStartkitRadio -v 2>&1 | tail -20
```

期待: `AttributeError` または `module has no attribute '_is_setup_dialog'` で失敗

- [ ] **Step 4: `jvlink_dialog_handler.py` に定数と新関数を追加**

`src/ops/jvlink_dialog_handler.py` の既存の `_BUTTON_PRIORITY` ブロック（63行目付近）の直後に以下を挿入:

```python
# ── BM_SETCHECK 定数（ラジオボタン選択用）────────────────────────────────────
BM_SETCHECK: int = 0x00F1
BST_CHECKED: int = 1

# ── セットアップダイアログ判定パターン ──────────────────────────────────────
_SETUP_TITLE_PATTERNS: tuple[str, ...] = ("セットアップ", "setup")

# ── 「スタートキットを持っていない」ラジオボタン識別パターン（小文字部分一致）─
_NO_STARTKIT_PATTERNS: tuple[str, ...] = (
    "持っていない",
    "持ってない",
    "スタートキット",
    "cd/dvd",
    "starterkit",
    "starter kit",
)
```

その直後（`_CLICK_COOLDOWN`定義の前）に関数を追加:

```python
def _is_setup_dialog(title: str) -> bool:
    """タイトルがセットアップダイアログかどうかを判定する。"""
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
```

- [ ] **Step 5: テスト再実行して全件パスを確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py::TestIsSetupDialog tests/test_jvlink_dialog_handler.py::TestSelectNoStartkitRadio -v 2>&1 | tail -20
```

期待: `9 passed`

---

## Task 2: `_dismiss_dialog()` にセットアップ専用の2段階突破ロジックを追加

**Files:**
- Modify: `src/ops/jvlink_dialog_handler.py:134-209` (`_dismiss_dialog`関数)
- Test: `tests/test_jvlink_dialog_handler.py`

- [ ] **Step 1: セットアップ2段階突破テストを追加（失敗するまで書く）**

`tests/test_jvlink_dialog_handler.py` の `TestDismissDialog` クラスに以下を追加:

```python
    def test_setup_dialog_selects_radio_then_clicks_ok(self) -> None:
        """セットアップダイアログは ラジオ選択 → OK クリックの2段階で突破する。"""
        mod = _fresh()
        import win32gui, win32con

        radio_selected: list[int] = []
        ok_clicked: list[int] = []

        def _fake_enum(hwnd: int, cb, extra: object) -> None:
            cb(300, None)  # hwnd 300 = ラジオ or OK

        # GetClassNameを2回呼び出しに対応
        class_map = {300: "Button"}
        text_map = {300: "スタートキット（CD/DVD-ROM）を持っていない"}

        win32gui.EnumChildWindows.side_effect = _fake_enum
        win32gui.GetClassName.side_effect = lambda h: class_map.get(h, "")
        win32gui.GetWindowText.side_effect = lambda h: text_map.get(h, "")

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
        import win32gui, win32api, win32con

        # ラジオなし・ボタンなし → WM_COMMAND IDOK
        win32gui.EnumChildWindows.side_effect = lambda hwnd, cb, ex: None
        win32api.PostMessage.reset_mock()

        result = mod._dismiss_dialog(888, "セットアップ")
        assert result is True  # WM_COMMAND IDOK で成功
```

- [ ] **Step 2: 失敗を確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py::TestDismissDialog::test_setup_dialog_selects_radio_then_clicks_ok tests/test_jvlink_dialog_handler.py::TestDismissDialog::test_setup_dialog_no_radio_still_clicks_ok -v 2>&1 | tail -15
```

期待: FAIL（まだ実装がないため）

- [ ] **Step 3: `_dismiss_dialog()` にセットアップ専用分岐を追加**

`src/ops/jvlink_dialog_handler.py` の `_dismiss_dialog()` 関数（134行目付近）を以下に置き換え:

```python
def _dismiss_dialog(hwnd: int, title: str) -> bool:
    """
    ダイアログを閉じる。成功したら True を返す。

    セットアップダイアログの場合は2段階突破:
      1. 「スタートキットを持っていない」ラジオボタンを BM_SETCHECK で選択
      2. OK ボタンを BM_CLICK
    通常のダイアログの場合は従来通り優先ボタン → WM_COMMAND → VK_RETURN の順。

    試行順（通常）:
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
                title, btn_text, hwnd,
            )
            stats["dialogs_dismissed"] += 1
            _first_seen.pop(hwnd, None)
            return True
        except Exception as exc:
            logger.debug("[DialogHandler] BM_CLICK 失敗: %s", exc)

    # 方法 2: WM_COMMAND IDOK をポスト
    try:
        win32api.PostMessage(hwnd, win32con.WM_COMMAND, win32con.IDOK, 0)
        logger.info(
            "[DialogHandler] ✅ WM_COMMAND IDOK: title=%r hwnd=%d", title, hwnd
        )
        stats["dialogs_dismissed"] += 1
        _first_seen.pop(hwnd, None)
        return True
    except Exception as exc:
        logger.debug("[DialogHandler] WM_COMMAND IDOK 失敗: %s", exc)

    # 方法 3: VK_RETURN キーをポスト
    try:
        win32api.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        win32api.PostMessage(hwnd, win32con.WM_KEYUP,   win32con.VK_RETURN, 0)
        logger.info(
            "[DialogHandler] ✅ VK_RETURN: title=%r hwnd=%d", title, hwnd
        )
        stats["dialogs_dismissed"] += 1
        _first_seen.pop(hwnd, None)
        return True
    except Exception as exc:
        logger.debug("[DialogHandler] VK_RETURN 失敗: %s", exc)

    return False
```

- [ ] **Step 4: テスト全件パスを確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py -v 2>&1 | tail -20
```

期待: 全件 PASSED（既存26件 + 新規9件 = 35件以上）

---

## Task 3: `jvlink_guard()` Context Manager の追加

**Files:**
- Modify: `src/ops/jvlink_dialog_handler.py` (末尾に追加)
- Test: `tests/test_jvlink_dialog_handler.py`

- [ ] **Step 1: `jvlink_guard()` テストを追加（失敗するまで書く）**

`tests/test_jvlink_dialog_handler.py` に以下を追記:

```python
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
                assert t1 is t2  # 同一スレッドが返る
        mod.stop_dialog_handler()
```

- [ ] **Step 2: 失敗を確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py::TestJvlinkGuard -v 2>&1 | tail -10
```

期待: `AttributeError: module has no attribute 'jvlink_guard'`

- [ ] **Step 3: `jvlink_guard()` を `jvlink_dialog_handler.py` の末尾に追加**

`stop_dialog_handler()` の直後（ファイル末尾）に追記:

```python
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
    daemonスレッドのためプロセス終了時に自動停止する。
    """
    thread = start_dialog_handler(interval=interval)
    try:
        yield thread
    finally:
        pass  # daemon スレッドはプロセス終了時に自動停止
```

- [ ] **Step 4: テスト全件パスを確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py -v 2>&1 | tail -15
```

期待: 全件 PASSED

- [ ] **Step 5: Task 1〜3 の中間コミット**

```powershell
git add src/ops/jvlink_dialog_handler.py tests/test_jvlink_dialog_handler.py
git commit -m "feat: add setup-dialog 2-step breaker and jvlink_guard() CM"
```

---

## Task 4: `JVLinkClient._connect()` にダイアログハンドラー自動起動フックを追加

**Files:**
- Modify: `src/scraper/jravan_client.py:672-683` (`_connect`メソッド先頭)

- [ ] **Step 1: 現在の `_connect()` 先頭部分を確認**

```powershell
py -c "
import ast, sys
sys.stdout.reconfigure(encoding='utf-8')
src = open(r'src/scraper/jravan_client.py', encoding='utf-8').read()
tree = ast.parse(src)
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == '_connect':
        print(f'_connect() line {node.lineno}-{node.end_lineno}')
"
```

期待: `_connect() line 672-...` 等が表示される

- [ ] **Step 2: `_connect()` の先頭（`import win32com.client` の直前）にフックを挿入**

`src/scraper/jravan_client.py` の `_connect()` メソッド内、`"""COM オブジェクトを生成して...` のdocstring直後（674行目付近）の `try:` ブロックの前に以下を追加:

```python
        # JVLink 起動前にダイアログ自動突破ハンドラーを起動（多重起動安全）
        # スケジューラー外（バックテスト・直接実行）でも必ず保護される。
        try:
            from src.ops.jvlink_dialog_handler import start_dialog_handler
            start_dialog_handler(interval=0.3)
            logger.debug("[JVLinkClient] ダイアログハンドラー起動確認済み")
        except Exception as _dh_exc:
            logger.debug("[JVLinkClient] ダイアログハンドラー起動スキップ: %s", _dh_exc)
```

- [ ] **Step 3: モジュールのインポートが壊れていないことを確認**

```powershell
py -c "
import sys; sys.stdout.reconfigure(encoding='utf-8')
# win32com なしで import だけ確認
import importlib.util
spec = importlib.util.spec_from_file_location('jravan_client', r'src/scraper/jravan_client.py')
print('import spec OK:', spec is not None)
"
```

期待: `import spec OK: True`

- [ ] **Step 4: `_connect()` にフックが正しく入っているか目視確認**

```powershell
py -c "
import sys; sys.stdout.reconfigure(encoding='utf-8')
lines = open(r'src/scraper/jravan_client.py', encoding='utf-8').readlines()
for i, line in enumerate(lines[670:695], start=671):
    print(f'{i:4d}: {line}', end='')
"
```

期待: `start_dialog_handler` の呼び出し行が表示される

- [ ] **Step 5: コミット**

```powershell
git add src/scraper/jravan_client.py
git commit -m "feat: auto-start dialog handler in JVLinkClient._connect()"
```

---

## Task 5: 既存テスト全件パスの確認（回帰チェック）

**Files:** 変更なし（確認のみ）

- [ ] **Step 1: 全テストスイートを実行**

```powershell
py -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

期待: 既存26件 + 新規11件 = 37件以上 PASSED、FAILED 0件

- [ ] **Step 2: ハンドラーの動作を手動確認（pywin32がインストール済みの場合）**

```powershell
py -c "
import sys, time
sys.stdout.reconfigure(encoding='utf-8')
from src.ops.jvlink_dialog_handler import start_dialog_handler, stop_dialog_handler, stats
t = start_dialog_handler(interval=0.3)
print('スレッド起動:', t.is_alive())
time.sleep(0.5)
print('統計:', stats)
stop_dialog_handler()
t.join(timeout=1.0)
print('スレッド停止:', not t.is_alive())
"
```

期待: スレッドが起動・停止し、例外なし

---

## Task 6: 恒久ルールファイルの作成

**Files:**
- Create: `.claudecode/rules/jvlink_popup_management.md`

- [ ] **Step 1: `.claudecode/rules/` ディレクトリを作成**

```powershell
New-Item -ItemType Directory -Force -Path ".claudecode\rules" | Out-Null
Write-Host "OK"
```

- [ ] **Step 2: ルールファイルを作成**

`.claudecode/rules/jvlink_popup_management.md` を以下の内容で作成:

```markdown
# JVLink GUIポップアップ自動突破ルール

> **絶対厳守**：以下のルールを破壊・除外・無効化することは禁止。
> JVLink呼び出しを含むすべてのコードに適用される。

---

## 背景・問題

JVLink（JRA-VAN JV-Link COM コンポーネント）は初期化時に以下の2段階のGUIダイアログを表示する。
これが手動クリックなしに放置されると、データ取得が完全にブロックされ、
Netkeibaフォールバックが作動してJRA-VANデータの精度優位性が失われる。

### ダイアログ発生シーケンス

1. **第1段**（設定系ダイアログ）→ OKを押す
2. **第2段**（タイトル「セットアップ」）→「スタートキットを持っていない」ラジオを選択 → OKを押す

### なぜCREATE_NO_WINDOW/SW_HIDEでは不十分か

`CREATE_NO_WINDOW` と `SW_HIDE` は子プロセスのコンソールウィンドウを隠すが、
JVLink COMコンポーネントが自前で作成するモーダルGUIダイアログは抑制できない。
JVSetDialog(False) / ParentHWnd(0) は JVDTLab.JVLink.1 では動作しない。

---

## 実装（三重安全網）

```
Layer 1: COM API による抑制試行（JVSetDialog / ParentHWnd）
         → JVDTLab.JVLink.1では通常失敗するが試行は維持する

Layer 2: JVLinkDialogHandler（0.3秒間隔バックグラウンドスレッド）
         src/ops/jvlink_dialog_handler.py
         - EnumWindows でタイトルパターンを検出
         - セットアップダイアログは2段階突破:
             ① _select_no_startkit_radio() でラジオ選択（BM_SETCHECK）
             ② _find_best_button() でOKをBM_CLICK
         - JVLinkClient._connect() 先頭で自動起動（全実行文脈を保護）

Layer 3: タイムアウト（10秒）→ Killプロセス → Netkeibaフォールバック
         （フォールバックは許容だが、Layer 2 で回避するのが目標）
```

### 主要ファイルと役割

| ファイル | 役割 |
|---------|------|
| `src/ops/jvlink_dialog_handler.py` | ハンドラー本体（`start_dialog_handler()` / `jvlink_guard()` / `stop_dialog_handler()`） |
| `src/scraper/jravan_client.py:JVLinkClient._connect()` | JVLink呼び出しの全経路をカバーするフック |
| `scripts/scheduler.py:run_daemon()` | スケジューラー起動時の明示的な起動（二重起動は安全） |

---

## 開発者への鉄則

### 1. JVLink を呼ぶコードには必ずハンドラーを確保すること

`JVLinkClient._connect()` がハンドラーを自動起動するため、`JVLinkClient` を使う
限りは保護される。ただし、新たに `win32com.client.Dispatch("JVDTLab.JVLink.1")` を
直接呼ぶコードを書く場合は必ず以下を追加すること:

```python
from src.ops.jvlink_dialog_handler import start_dialog_handler
start_dialog_handler(interval=0.3)
```

または:

```python
from src.ops.jvlink_dialog_handler import jvlink_guard

with jvlink_guard():
    # JVLink処理
```

### 2. `jvlink_dialog_handler.py` の改修時の必須チェックリスト

- [ ] `_SETUP_TITLE_PATTERNS` に「セットアップ」が含まれているか
- [ ] `_NO_STARTKIT_PATTERNS` に「持っていない」「スタートキット」が含まれているか
- [ ] `_dismiss_dialog()` がセットアップダイアログで `_select_no_startkit_radio()` を呼ぶか
- [ ] `BM_SETCHECK = 0x00F1`, `BST_CHECKED = 1` が定義されているか
- [ ] `tests/test_jvlink_dialog_handler.py` の全件がパスするか

### 3. テストをスキップしないこと

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py -v
```

このコマンドが全件PASSすることを必ず確認してからコミットすること。

### 4. ハンドラーを無効化・削除してはならない

以下の変更は絶対禁止:
- `start_dialog_handler()` の呼び出しを削除・コメントアウト
- `_is_setup_dialog()` の判定条件を緩める
- `_select_no_startkit_radio()` を削除

### 5. ラジオボタンのテキストが変わった場合

JVLinkバージョンアップでダイアログのテキストが変わった場合は
`_NO_STARTKIT_PATTERNS` にパターンを追加して対応すること（削除は禁止）。

---

## デバッグ方法

### ハンドラーが動いているか確認

```python
from src.ops.jvlink_dialog_handler import stats
print(stats)
# {'dialogs_dismissed': N, 'click_attempts': M, 'stubborn_dialogs': 0}
```

### セットアップダイアログが残っていないか確認

```python
from src.ops.jvlink_dialog_handler import _scan_windows
_scan_windows()  # 手動スキャンで即座にハンドラーを走らせる
```

### ログ確認

```
[DialogHandler] セットアップダイアログ: ラジオ選択完了 hwnd=XXXXX
[DialogHandler] ✅ BM_CLICK: title='セットアップ' button='OK' hwnd=XXXXX
```

上記ログがあればセットアップダイアログの2段階突破成功。

---

## 変更履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-05-23 | 初版作成: 2段階セットアップダイアログ自動突破実装（アプローチA） |
```

- [ ] **Step 3: ファイルが正しく作成されたか確認**

```powershell
Get-Content ".claudecode\rules\jvlink_popup_management.md" | Select-Object -First 5
```

期待: ファイル先頭5行が表示される

---

## Task 7: ドキュメント更新

**Files:**
- Modify: `docs/6_special_notes.md`

- [ ] **Step 1: `docs/6_special_notes.md` のChangelogに今回の変更を追記**

`docs/6_special_notes.md` の更新履歴テーブルの先頭行として以下を追加:

```markdown
| 2026-05-23 | 【JVLinkセットアップダイアログ完全自動突破】jvlink_dialog_handler.py に `_select_no_startkit_radio()`（BM_SETCHECK）+ `_is_setup_dialog()` + `jvlink_guard()` CM を追加。JVLinkClient._connect() にハンドラー自動起動フックを挿入。全実行文脈（スケジューラー/バックテスト/直接実行）でGUI手動操作を完全排除。影響: src/ops/jvlink_dialog_handler.py, src/scraper/jravan_client.py |
```

---

## Task 8: 最終コミット

- [ ] **Step 1: 全テストが通ることを最終確認**

```powershell
py -m pytest tests/test_jvlink_dialog_handler.py -v 2>&1 | tail -10
```

期待: 全件 PASSED

- [ ] **Step 2: git status で変更ファイルを確認**

```powershell
git status
```

期待: 以下が表示される（他の意図しないファイルが含まれていないこと）
- `src/ops/jvlink_dialog_handler.py` (M)
- `src/scraper/jravan_client.py` (M)
- `tests/test_jvlink_dialog_handler.py` (M)
- `.claudecode/rules/jvlink_popup_management.md` (??)
- `docs/6_special_notes.md` (M)
- `docs/superpowers/specs/2026-05-23-jvlink-dialog-autobreak-design.md` (??)
- `docs/superpowers/plans/2026-05-23-jvlink-dialog-autobreak.md` (??)

- [ ] **Step 3: 全ファイルをステージングしてコミット**

```powershell
git add src/ops/jvlink_dialog_handler.py
git add src/scraper/jravan_client.py
git add tests/test_jvlink_dialog_handler.py
git add .claudecode/rules/jvlink_popup_management.md
git add docs/6_special_notes.md
git add docs/superpowers/specs/2026-05-23-jvlink-dialog-autobreak-design.md
git add docs/superpowers/plans/2026-05-23-jvlink-dialog-autobreak.md
git commit -m "$(cat <<'EOF'
feat: JVLink setup-dialog 2-step auto-break (radio select + OK click)

- _select_no_startkit_radio(): BM_SETCHECK でラジオを自動選択
- _is_setup_dialog(): セットアップダイアログ判定
- _dismiss_dialog(): セットアップ専用2段階突破分岐追加
- jvlink_guard(): バックテスト等向け Context Manager
- JVLinkClient._connect(): ハンドラー自動起動フック追加
- .claudecode/rules/jvlink_popup_management.md: 恒久ルール

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## 検証チェックリスト

実装完了後、以下を確認する:

- [ ] `py -m pytest tests/test_jvlink_dialog_handler.py -v` が全件 PASSED
- [ ] `jvlink_dialog_handler.py` に `BM_SETCHECK`, `BST_CHECKED`, `_is_setup_dialog`, `_select_no_startkit_radio`, `jvlink_guard` が存在する
- [ ] `jravan_client.py` の `_connect()` に `start_dialog_handler` の呼び出しが存在する
- [ ] `.claudecode/rules/jvlink_popup_management.md` が存在する
- [ ] `docs/6_special_notes.md` に 2026-05-23 のエントリが存在する
- [ ] `git log --oneline -3` で 2〜3件のコミットが確認できる
