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
`JVSetDialog(False)` / `ParentHWnd(0)` は JVDTLab.JVLink.1 では動作しない。

---

## 実装（三重安全網）

```
Layer 1: COM API による抑制試行（JVSetDialog / ParentHWnd / JVSetUIProperties）
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

### 重要な定数（削除・変更禁止）

```python
BM_SETCHECK: int = 0x00F1   # ラジオボタンをチェック状態にするWindowsメッセージ
BST_CHECKED: int = 1         # チェック状態フラグ

_SETUP_TITLE_PATTERNS = ("セットアップ", "setup")

_NO_STARTKIT_PATTERNS = (
    "持っていない",   # JVLinkの日本語ダイアログテキスト
    "持ってない",
    "スタートキット",
    "cd/dvd",
    "starterkit",
    "starter kit",
)
```

---

## 開発者への鉄則

### 1. JVLink を呼ぶコードには必ずハンドラーを確保すること

`JVLinkClient._connect()` がハンドラーを自動起動するため、`JVLinkClient` を使う
限りは保護される。新たに `win32com.client.Dispatch("JVDTLab.JVLink.1")` を
直接呼ぶコードを書く場合は必ず以下のいずれかを追加すること:

**方法1（推奨）: 関数内で起動**
```python
from src.ops.jvlink_dialog_handler import start_dialog_handler
start_dialog_handler(interval=0.3)
```

**方法2: Context Managerで包む**
```python
from src.ops.jvlink_dialog_handler import jvlink_guard

with jvlink_guard():
    loader = JVDataLoader(sid=os.environ["JRAVAN_SID"])
    stats  = loader.load("RACE", ...)
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

このコマンドが全件PASSすることを確認してからコミットすること。
**2026-05-23時点: 39件PASS。これ以下になったら問題。**

### 4. ハンドラーを無効化・削除してはならない

以下の変更は絶対禁止:
- `start_dialog_handler()` の呼び出しを削除・コメントアウト
- `_is_setup_dialog()` の判定条件を緩める・削除する
- `_select_no_startkit_radio()` を削除する
- `_NO_STARTKIT_PATTERNS` からパターンを削除する

### 5. ラジオボタンのテキストが変わった場合

JVLinkバージョンアップでダイアログのテキストが変わった場合は
`_NO_STARTKIT_PATTERNS` にパターンを**追加**して対応すること（既存パターンの削除は禁止）。

---

## デバッグ方法

### ハンドラーが動いているか確認

```python
from src.ops.jvlink_dialog_handler import stats
print(stats)
# {'dialogs_dismissed': N, 'click_attempts': M, 'stubborn_dialogs': 0}
```

### 手動スキャンで即座にテスト

```python
from src.ops.jvlink_dialog_handler import _scan_windows
_scan_windows()
```

### 期待されるログ（成功時）

```
[DialogHandler] セットアップダイアログ: ラジオ選択完了 hwnd=XXXXX
[DialogHandler] ✅ BM_CLICK: title='セットアップ' button='OK' hwnd=XXXXX
```

上記ログが出ていればセットアップダイアログの2段階突破成功。

### Netkeibaフォールバックが出た場合の調査手順

1. ログに `GUI_BLOCKED` が出ていないか確認
2. `stats["dialogs_dismissed"]` がインクリメントされているか確認
3. `stats["click_attempts"]` が0のまま → ハンドラーがタイトルを検出できていない
4. タイトルを確認して `_TARGET_TITLE_PATTERNS` / `_SETUP_TITLE_PATTERNS` に追加

---

## 変更履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-05-23 | 初版作成: 2段階セットアップダイアログ自動突破実装（アプローチA）|
