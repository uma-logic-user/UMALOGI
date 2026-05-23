# JVLink セットアップダイアログ完全自動突破設計

**日付**: 2026-05-23  
**ステータス**: 承認済み → 実装開始  

---

## 背景・問題

JVLinkは初期化時に2段階のGUIダイアログを表示し、手動クリックが発生しないとデータ取得が完全にブロックされる。

### ダイアログ発生シーケンス

1. **第1段ダイアログ**（タイトル不明 or「JVLink」「設定」等）→ OKを押す  
2. **第2段ダイアログ**（タイトル「セットアップ」）→「スタートキットを持っていない」ラジオボタンを選択 → OKを押す

### 現在の問題点

- 既存の`jvlink_dialog_handler.py`はOKボタンクリックのみ実装（ラジオボタン選択なし）
- スケジューラー以外のコンテキスト（バックテスト・`import_historical.py`等）ではハンドラー自体が起動されない
- `CREATE_NO_WINDOW` + `SW_HIDE`では抑制不可能

---

## 設計（アプローチA採用）

### 変更ファイル

| ファイル | 変更種別 | 内容 |
|---------|---------|------|
| `src/ops/jvlink_dialog_handler.py` | 改修 | ラジオボタン選択・Context Manager追加 |
| `src/scraper/jravan_client.py` | 軽微修正 | `_connect()`先頭でハンドラー自動起動 |
| `tests/test_jvlink_dialog_handler.py` | テスト追加 | セットアップ2段階突破テスト |
| `.claudecode/rules/jvlink_popup_management.md` | 新規 | 恒久ルール |

### `jvlink_dialog_handler.py` の改修仕様

#### 追加定数

```python
BM_SETCHECK = 0x00F1
BST_CHECKED = 1

_SETUP_TITLE_PATTERNS = ("セットアップ", "setup")
_NO_STARTKIT_PATTERNS = ("持っていない", "スタートキット", "cd/dvd", "starterkit")
```

#### 追加関数: `_select_no_startkit_radio(hwnd: int) -> bool`

1. `EnumChildWindows`でButtonクラスの子コントロールを列挙
2. `GetWindowText()`が`_NO_STARTKIT_PATTERNS`のいずれかを含むものを探索
3. 見つかったら`BM_SETCHECK`でチェック状態に設定
4. 成功したら`True`を返す

#### `_dismiss_dialog()`の修正

タイトルが`_SETUP_TITLE_PATTERNS`に一致する場合：
1. `_select_no_startkit_radio(hwnd)` でラジオを選択
2. `_find_best_button(hwnd)` でOKを探してBM_CLICK

#### 追加: `jvlink_guard()` Context Manager

```python
@contextmanager
def jvlink_guard(interval: float = 0.3) -> Iterator[threading.Thread]:
    thread = start_dialog_handler(interval=interval)
    yield thread
    # 停止しない（既存の常駐スレッドがあれば再利用）
```

### `jravan_client.py` の修正

`JVLinkClient._connect()`の先頭に追加：

```python
try:
    from src.ops.jvlink_dialog_handler import start_dialog_handler
    start_dialog_handler(interval=0.3)
except Exception:
    pass  # pywin32未インストール環境でも継続
```

---

## テスト方針

- `TestSelectNoStartkitRadio`: ラジオボタン選択の正常系・異常系
- `TestDismissSetupDialog`: セットアップダイアログ2段階突破のE2E
- `TestJvlinkGuard`: Context Manager の起動・再入・停止

---

## 成功条件

- ログに`[DialogHandler] セットアップダイアログ: ラジオ選択完了`が出力される
- `[DialogHandler] ✅ BM_CLICK: title='セットアップ'`が出力される
- Netkeibaフォールバック（GUI_BLOCKED -2）が発生しない
