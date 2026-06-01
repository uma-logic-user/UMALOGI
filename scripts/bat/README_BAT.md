# UMALOGI 運用バッチ 操作マニュアル（README_BAT.md）

Windows 環境で UMALOGI の本番稼働プロセスを **ワンクリックで起動・停止** するための
バッチファイル群と、PC 起動時の自動実行手順をまとめたものです。

> 格納場所: `scripts/bat/`
> 対象 OS: Windows 10 / 11
> Python: システムの `py` ランチャー（仮想環境・Poetry は不使用。`py` = 64bit Python 3.14 が既定）

---

## 1. 構成プロセス（本番稼働スタック）

| # | プロセス | 役割 | 起動コマンド |
|---|---------|------|------------|
| 1 | **ダッシュボード** | 成果可視化 Streamlit UI（http://localhost:8501） | `py -m streamlit run web_streamlit/app.py` |
| 2 | **オートパイロット** | 週次自律運転（金曜夜→土日監視→週次レポート→翌週まで自動スリープ） | `py scripts/today_auto_runner.py --continuous` |
| 3 | **ウォッチドッグ** | オッズ欠損の自己修復番犬（5分間隔・JVLink異常時に再起動＋再同期） | `py scripts/watchdog.py --interval 5` |

> ℹ️ **JVLink について**: オートパイロット／スケジューラは 64bit Python で常駐し、
> JVLink COM 操作（32bit 制約）は内部で `py -3-32` の subprocess に自動委譲します。
> バッチ側で 32bit を意識する必要はありません。

---

## 2. ⚠️ 最重要：2つの運転方式は「排他」

UMALOGI の週次自動運転には **排他的な2実装** があります。**同時に動かしてはいけません**
（二重予想・二重 Discord 通知・予想レコードの汚染を招きます）。

| 方式 | 起動バッチ | 内容 |
|------|-----------|------|
| **オートパイロット方式（推奨・本番実態）** | `start_umalogi.bat` | `today_auto_runner.py --continuous` を常駐 |
| **scheduler 方式（代替）** | `start_scheduler_mode.bat` | `scripts/scheduler.py`（schedule ライブラリ）を常駐。内部で today_auto_runner を所定時刻に起動 |

各起動バッチには **排他ガード** が組み込まれており、もう一方が稼働中の場合は起動を中断します。
通常は **`start_umalogi.bat`（オートパイロット方式）** を使ってください。

> 📅 **週末凍結ルール（CLAUDE.md 条項2）**: 土・日は稼働と的中通知に専念します。
> 週末に新機能追加・再起動を伴う変更は原則行わないでください。

---

## 3. ファイル一覧

| ファイル | 用途 |
|---------|------|
| `start_umalogi.bat` | **本番ワンクリック起動**（ダッシュボード＋オートパイロット＋ウォッチドッグ） |
| `start_scheduler_mode.bat` | 代替起動（scheduler.py 方式・オートパイロットと排他） |
| `stop_umalogi.bat` | **UMALOGI 関連プロセスのみを安全停止**（無関係な Python は巻き込まない） |
| `README_BAT.md` | 本マニュアル |

---

## 4. 手動での起動・停止

### 起動
`scripts\bat\start_umalogi.bat` を **ダブルクリック**（または右クリック→管理者として実行）。

- 3つのプロセスがそれぞれ独立したウィンドウ（`UMALOGI_DASHBOARD` / `UMALOGI_AUTORUNNER` /
  `UMALOGI_WATCHDOG`）で起動します。
- ランチャーウィンドウは閉じても構いません（各プロセスは継続稼働）。
- 既に稼働中のプロセスは自動スキップされます（二重起動防止）。

起動確認:
- ブラウザで **http://localhost:8501** を開く（ダッシュボード）。
- コマンドプロンプトで稼働確認:
  ```cmd
  tasklist | findstr /i "python streamlit"
  ```

### 停止
`scripts\bat\stop_umalogi.bat` を **ダブルクリック**。

- コマンドラインに UMALOGI 固有のスクリプト名
  （`today_auto_runner.py` / `scheduler.py` / `watchdog.py` / `web_streamlit/app.py`）を
  含むプロセス **だけ** を PID 指定で停止します。
- 補助として `UMALOGI_*` タイトルのウィンドウをツリーごと停止します。
- **`taskkill /im python.exe` のような全 Python 一括 kill は行いません**ので、
  他作業の Python プロセスを誤って終了させません。

---

## 5. Windows スタートアップへの登録（ログオン時に自動起動）

最も簡単な方法。ログオンのたびに自動起動します。

1. `Win + R` を押し、`shell:startup` と入力して Enter
   → スタートアップフォルダ（`%AppData%\Microsoft\Windows\Start Menu\Programs\Startup`）が開きます。
2. `scripts\bat\start_umalogi.bat` を **右クリック →「ショートカットの作成」**。
3. 作成されたショートカットを、手順1で開いたスタートアップフォルダへ移動します。
4. （任意）ショートカットのプロパティ →「実行時の大きさ」を「最小化」にすると邪魔になりません。

> 解除したいときは、スタートアップフォルダ内のショートカットを削除するだけです。

---

## 6. タスクスケジューラでの自動実行（PC 起動／ログオン時）

スタートアップより堅牢で、権限昇格・遅延起動・失敗時の再試行などを制御できます。

### A. GUI 手順
1. スタート →「**タスク スケジューラ**」を起動。
2. 右ペイン「**基本タスクの作成**」をクリック。
3. 名前: `UMALOGI_Startup` → 次へ。
4. トリガー: 「**ログオン時**」を選択 → 次へ。
   （PC 起動直後に動かしたい場合は「コンピューターの起動時」。ただし UI ウィンドウ表示には
   ログオンセッションが必要なため、通常は「ログオン時」推奨）
5. 操作: 「**プログラムの開始**」→ 次へ。
6. プログラム/スクリプト: `C:\dev\horse-racing-ai\scripts\bat\start_umalogi.bat`
   開始（オプション）: `C:\dev\horse-racing-ai\scripts\bat`
7. 完了。

### B. コマンドで一発登録（管理者プロンプト）
```cmd
schtasks /Create /TN "UMALOGI_Startup" ^
  /TR "C:\dev\horse-racing-ai\scripts\bat\start_umalogi.bat" ^
  /SC ONLOGON /RL HIGHEST /F
```

- `/SC ONLOGON` … ログオン時に実行
- `/RL HIGHEST` … 最上位の権限で実行（taskkill / プロセス操作を確実化）
- `/F` … 既存タスクを上書き

登録確認 / 手動実行 / 削除:
```cmd
schtasks /Query  /TN "UMALOGI_Startup"
schtasks /Run    /TN "UMALOGI_Startup"
schtasks /Delete /TN "UMALOGI_Startup" /F
```

> 💡 PC 起動直後は DB ロックや JVLink 初期化が不安定な場合があります。タスクのプロパティ
> →「条件」「設定」で **30秒〜1分の遅延** を入れると安定します。

---

## 7. ログとトラブルシュート

| 事象 | 確認・対処 |
|------|-----------|
| ダッシュボードが開かない | `tasklist | findstr streamlit` で稼働確認 → http://localhost:8501。ポート 8501 が使用中なら別プロセスを停止 |
| オートパイロットの動作 | `data\scheduler.log`（today_auto_runner のログ） |
| ウォッチドッグの動作 | `data\watchdog.log` |
| 文字化け | バッチ冒頭で `chcp 65001`（UTF-8）を設定済み。古いコンソール設定の影響時は再起動 |
| 「排他です」で中断 | もう一方の方式が稼働中。`stop_umalogi.bat` で停止してから再起動 |
| プロセスが残る | `stop_umalogi.bat` を再実行。残存時は `tasklist` で PID を確認し個別 `taskkill /pid <PID> /f` |

---

## 8. 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-06-01 | 新規作成。本番実態（オートパイロット＋ウォッチドッグ＋ダッシュボード）に準拠した起動・停止バッチと運用手順を整備。scheduler 方式は排他ガード付き代替として併設。 |
