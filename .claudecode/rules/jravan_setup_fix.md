# 永続ルール: JRA-VAN JVLink セットアップ安全規則

**策定日**: 2026-05-23  
**担当**: Claude Code (claude-sonnet-4-6)  
**根本原因調査に基づく恒久ルール**

---

## 発生した問題

JRA-VAN JVLink のセットアップ・COM 初期化処理が走る直前（`Dispatch()` 呼び出し時点）に、
ブラウザが勝手に「戻る（バック）」または「初期セッションへリダイレクト」される問題が発生。

ユーザーは物理的に一切画面に触れていなかった。

## 根本原因

### 原因 1: ダイアログハンドラーの誤一致

`src/ops/jvlink_dialog_handler.py` の `_TARGET_TITLE_PATTERNS` に含まれる
`"設定"`, `"更新"`, `"アップデート"`, `"update"` 等の広すぎるパターンが、
Chrome/Edge のウィンドウタイトル（例: "設定 - Microsoft Edge"）に一致し、
`WM_COMMAND IDOK` や `VK_RETURN` をブラウザウィンドウに送信していた。

### 原因 2: TARGET Frontier の自動再起動がブラウザを開く

`scripts/scheduler.py` の `_restart_target_jv()` が TARGET Frontier を
`DETACHED_PROCESS` で起動する際に `SW_SHOWMINIMIZED` を指定していなかったため、
TARGET Frontier が起動時にフォーカスを奪い、JRA-VAN 認証ページをブラウザで開いた。

## 適用した修正

### 修正 1: `src/ops/jvlink_dialog_handler.py`

- `_EXCLUDED_WIN_CLASSES` セットを追加（Chrome_WidgetWin_1, MozillaWindowClass 等）
- `_is_browser_window()` 関数を追加
- `_scan_windows()` のコールバックにブラウザ除外チェックを追加

### 修正 2: `scripts/scheduler.py`

- `_restart_target_jv()` に `STARTUPINFO(SW_SHOWMINIMIZED)` を追加
- TARGET Frontier を最小化状態で起動することでブラウザ誘導を抑制

### 修正 3: `scripts/setup_jvlink.py`

- COM 生成前にダイアログハンドラーを起動
- COM 生成直後に `ParentHWnd=0`, `JVSetDialog(False)`, `JVSetAutoDownload(True)` を設定
- ブラウザが開く可能性をユーザーに事前警告

### 修正 4: `src/utils/jravan_cli_initializer.py` (新規)

- ブラウザを一切開かないスタンドアロン CLI 初期化ツール
- 自動化スクリプト・スケジューラーから呼び出す際に使用
- exit code: 0=成功, 1=JVInit失敗, 2=COM失敗, 3=JVOpen失敗

## 恒久ルール

1. **ダイアログハンドラーは必ずウィンドウクラスで除外チェックすること**
   - `Chrome_WidgetWin_1`, `MozillaWindowClass` 等のブラウザクラスは絶対に操作しない

2. **GUI アプリ（TARGET Frontier 等）を自動起動する際は必ず `SW_SHOWMINIMIZED` を使うこと**
   - 自動化文脈でフォーカス奪取・ブラウザ開きを防ぐ

3. **JVLink COM 初期化は必ず `Dispatch()` 直後に抑制フラグを設定すること**
   - `ParentHWnd = 0`
   - `JVSetDialog(False)`
   - `JVSetAutoDownload(True)`

4. **自動化用のセットアップには `src/utils/jravan_cli_initializer.py` を使うこと**
   - `scripts/setup_jvlink.py` は対話型（ブラウザ開きを許容）
   - `jravan_cli_initializer.py` は完全非対話型（ブラウザなし）

## 影響ファイル

- `src/ops/jvlink_dialog_handler.py`
- `scripts/scheduler.py`
- `scripts/setup_jvlink.py`
- `src/utils/jravan_cli_initializer.py` (新規)
