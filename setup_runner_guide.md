# GitHub Actions Self-Hosted Runner セットアップガイド

## 概要

このWindows PCをGitHub Actionsの「self-hosted runner」として登録することで、
以下の問題を解決します：

- **DB永続化**: `data/umalogi.db` をローカルの固定パスで直接読み書き
- **JRA-VAN対応**: `py -3.14-32`（32bit COM）がそのまま動作
- **キャッシュ依存の排除**: `actions/cache` によるDB分断が発生しない
- **実行速度向上**: Python・依存パッケージのインストール不要（既存環境を利用）

---

## 前提条件

- Windows 10/11
- PowerShell 5.1 以上（管理者不要）
- Git インストール済み
- Python 3.12 以上インストール済み（`py -3.12` または `python` で起動できること）
- `C:\dev\horse-racing-ai` にリポジトリが存在すること

---

## Step 1: GitHub でランナー登録トークンを取得

1. ブラウザで以下を開く:
   ```
   https://github.com/{あなたのユーザー名}/horse-racing-ai/settings/actions/runners/new
   ```
   （Settings → Actions → Runners → New self-hosted runner）

2. OS: **Windows** を選択

3. 画面に表示される `--token xxxxxxxxxxxxx` の値をコピーしておく

---

## Step 2: ランナーのインストール

PowerShell を開き、以下を順番に実行します。

```powershell
# ランナー用ディレクトリを作成
mkdir C:\actions-runner
cd C:\actions-runner

# GitHub Actions Runner をダウンロード（バージョンは適宜最新に）
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v2.322.0/actions-runner-win-x64-2.322.0.zip -OutFile actions-runner-win-x64.zip

# 展開
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory("$PWD\actions-runner-win-x64.zip", "$PWD")
```

> **注意**: 最新バージョンは https://github.com/actions/runner/releases で確認してください。

---

## Step 3: ランナーの設定

```powershell
cd C:\actions-runner

# ランナーを登録（{OWNER} = GitHubユーザー名、{TOKEN} = Step1で取得したトークン）
.\config.cmd `
  --url https://github.com/{OWNER}/horse-racing-ai `
  --token {TOKEN} `
  --name "umalogi-windows-runner" `
  --labels "self-hosted,Windows,umalogi" `
  --work C:\actions-runner\_work `
  --runnergroup Default `
  --unattended
```

対話形式で聞かれた場合は Enter（デフォルト値）で進みます。

---

## Step 4: DB・モデルの永続パスを確認

ランナーが使う永続DBパスは `C:\dev\horse-racing-ai\data\umalogi.db` です。

```powershell
# DBファイルが存在することを確認
Test-Path "C:\dev\horse-racing-ai\data\umalogi.db"
# → True であればOK

# モデルディレクトリが存在することを確認
Test-Path "C:\dev\horse-racing-ai\data\models"
# → False の場合は作成
mkdir "C:\dev\horse-racing-ai\data\models" -Force
```

---

## Step 5: ランナーを起動

### 方法A: 手動起動（テスト用）

```powershell
cd C:\actions-runner
.\run.cmd
```

コンソールに `Listening for Jobs` と表示されれば成功です。

### 方法B: Windowsサービスとして常駐（本番推奨）

```powershell
# 管理者権限のPowerShellで実行
cd C:\actions-runner
.\svc.cmd install
.\svc.cmd start

# サービス状態を確認
.\svc.cmd status
# → Active: running であればOK
```

サービス名: `actions.runner.{OWNER}-horse-racing-ai.umalogi-windows-runner`

---

## Step 6: GitHub Secrets の設定

GitHub リポジトリの Settings → Secrets and variables → Actions で以下を登録：

| Secret名 | 値 |
|---|---|
| `DISCORD_WEBHOOK_URL` | Discord WebhookのURL（`.env` に記載の値） |

```
設定場所: https://github.com/{OWNER}/horse-racing-ai/settings/secrets/actions
```

---

## Step 7: 動作確認

GitHub リポジトリの Actions タブ → 「UMALOGI 自動予想パイプライン」→ 「Run workflow」で
手動実行を試みます。

ランナーのコンソールに以下が表示されれば成功：
```
Running job: レース直前予想
```

---

## ランナーの停止・削除

```powershell
# サービス停止
cd C:\actions-runner
.\svc.cmd stop

# サービス削除
.\svc.cmd uninstall

# ランナー登録解除（オプション）
.\config.cmd remove --token {TOKEN}
```

---

## トラブルシューティング

### `py -3.14-32` が見つからないエラー
JRA-VAN用の32bitPythonが必要です。以下からインストール：
```
https://www.python.org/downloads/windows/
→ Python 3.x.x - Windows installer (32-bit) を選択
```

### DB書き込み権限エラー
ランナーのサービスが実行するユーザーアカウントに `C:\dev\horse-racing-ai\data\` への
書き込み権限があることを確認してください。

```powershell
# 権限確認
icacls "C:\dev\horse-racing-ai\data"
```

### ランナーが `Offline` と表示される
```powershell
cd C:\actions-runner
.\run.cmd  # 手動起動して接続ログを確認
```

---

## 参考リンク

- GitHub Actions Self-hosted runners 公式ドキュメント:
  https://docs.github.com/ja/actions/hosting-your-own-runners
- ランナーの最新リリース:
  https://github.com/actions/runner/releases
