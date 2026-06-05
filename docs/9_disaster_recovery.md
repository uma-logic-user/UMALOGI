# UMALOGI 災害復旧手順書（Disaster Recovery / DR）

> 作成日: 2026-06-05 ／ 対象 VERSION: `1.4.5-dev`
> 目的: 物理PC全損・データ損失などの障害から、UMALOGI 本番無人運用を**最短で復旧**するための手順を定める。
> 関連: 起動は `startup_umalogi.bat` / `scripts/bat/start_umalogi.bat`（`scripts/bat/README_BAT.md`）、
>       バックアップは `scripts/backup_umalogi.py`（全資産ZIP）と `src/ops/backup.py:backup_db()`（DB世代）。

---

## 0. 復旧の優先順位（RTO の考え方）

| 障害 | 失うもの | 復旧の鍵 |
|---|---|---|
| 物理PC全損 | OS・コード・DB・環境変数すべて | GitHub（コード）＋ クラウド/外部のバックアップZIP（DB・モデル） |
| データ損失（DB破損） | `data/umalogi.db` | DBバックアップ（`data/backups/` or 全資産ZIP内の `data/umalogi.db`） |
| 設定喪失 | `.env` | 本書 §3 の環境変数リストから再構築 |

> ⚠️ **コードは GitHub にあるが、DB・モデル・`.env` は GitHub に存在しない**
> （`.gitignore` 済み: `data/umalogi.db` / `data/models/` / `backups/` / `.env`）。
> したがって DR の成否は **DB/モデル/`.env` を別媒体（クラウド同期 `CLOUD_BACKUP_DIR` or 外部ドライブ）に
> 退避できているか**に懸かる。`scripts/backup_umalogi.py` の出力ZIPを定期的にPC外へコピーすること。

---

## 1. 【物理PC全損時】最短復旧コマンド

新しい Windows PC（Python 3.x の `py` ランチャー導入済み・仮想環境は不使用）を前提とする。

```cmd
REM ① コードを GitHub から取得
cd C:\dev
git clone https://github.com/uma-logic-user/UMALOGI.git horse-racing-ai
cd horse-racing-ai

REM ② 依存パッケージのインストール（システム py へ）
py -m pip install -r requirements.txt
REM   Playwright を使う場合のみ（note自動下書き/Xスクレイピング）
REM   py -m playwright install chromium

REM ③ 環境変数を復元（§3 のリストから .env を作成）
copy .env.example .env
notepad .env            REM ← 各値を本物に書き換える（§3 参照）

REM ④ DB・モデルをバックアップから復元（§2 参照）
REM    全資産ZIP（backups\umalogi_backup_*.zip）または data\backups\ の .db を配置
REM    例: ZIP を展開して data\umalogi.db / data\models\ を戻す

REM ⑤ 本番稼働スタックを起動（自動復旧エントリポイント）
startup_umalogi.bat

REM ⑥（任意）PC再起動時の自動起動を登録（管理者プロンプト）
schtasks /Create /TN "UMALOGI_Startup" /TR "C:\dev\horse-racing-ai\startup_umalogi.bat" /SC ONLOGON /RL HIGHEST /F
```

> 💡 JVLink（JRA-VAN）を使う場合は別途 **TARGET frontier JV / JV-Link の再インストールと SID 認証**が必要
> （32bit COM）。JVLink が未復旧でも netkeiba フォールバックで EV 算出は継続する（CLAUDE.md データ戦略 §11）。

### 起動確認
```cmd
tasklist | findstr /i "python streamlit"
```
ブラウザで http://localhost:8501 （ダッシュボード）を開く。Discord #system に「スケジューラー起動」通知が来れば成功。

---

## 2. 【データ損失時】DBバックアップからの復元手順

### 2.1 バックアップの所在

| 種別 | 場所 | 作成元 |
|---|---|---|
| DB 世代バックアップ | `data/backups/umalogi_YYYYMMDD_HHMMSS.db` | `src/ops/backup.py:backup_db()`（毎日23:00・深夜保守04:00 の事前） |
| 全資産ZIP（DB含む） | `backups/umalogi_backup_YYYYMMDD_HHMMSS.zip` 内の `data/umalogi.db` | `scripts/backup_umalogi.py` |
| クラウド同期（任意） | `CLOUD_BACKUP_DIR` 配下 | `backup_db()` がコピー |

### 2.2 復元手順（⚠️ 稼働中プロセスを必ず停止してから）

```cmd
REM ① 全プロセスを安全停止（DBロックを解放）
scripts\bat\stop_umalogi.bat

REM ② 現状DBを念のため退避（上書き前の保全・CLAUDE.md 条項4）
copy data\umalogi.db data\umalogi.db.corrupt_%DATE:/=%

REM ③ 最新の正常バックアップを本体へ復元
REM    （data\backups\ の最新 .db、または ZIP 展開した data\umalogi.db）
copy data\backups\umalogi_YYYYMMDD_HHMMSS.db data\umalogi.db

REM ④ 整合性を確認（OKと件数が返ればDBは健全）
py -c "import sqlite3; c=sqlite3.connect('data/umalogi.db'); print(c.execute('PRAGMA integrity_check').fetchone()); print('predictions=', c.execute('SELECT COUNT(*) FROM predictions').fetchone())"

REM ⑤ 再起動
startup_umalogi.bat
```

> **絶対原則（CLAUDE.md 条項1・条項4）**: 復元後に過去 `predictions` を再生成・UPDATE/DELETE してはならない。
> 「UIに出ない＝データ消失」と即断せず、必ず上記 ④ で DB を直接 COUNT 確認してから判断すること
> （過去の誤リストア事故防止）。`.db-wal`/`.db-shm` はバックアップに含めない（揮発ファイル）。

---

## 3. 【環境変数】必要な環境変数リスト（`.env`）

値はプレースホルダ。`.env.example` をコピーして実値を設定する。**`.env` は GitHub に存在しない**ため DR では手動再構築が必要。

### 3.1 必須（これが無いと本番が機能しない）

| 変数 | 用途 | 例（プレースホルダ） |
|---|---|---|
| `DISCORD_WEBHOOK_URL` | 予想・的中結果の通常送信先（必須） | `https://discord.com/api/webhooks/XXXX/YYYY` |
| `JRAVAN_SID` | JRA-VAN Data Lab 加入者 SID | `SA000000` |
| `DB_PATH` | DB ファイルパス | `data/umalogi.db` |
| `INITIAL_BANKROLL` | Kelly 初期資金（円） | `100000` |

### 3.2 通知チャンネル（Discord・未設定時は DISCORD_WEBHOOK_URL へフォールバック）

| 変数 | 用途 |
|---|---|
| `DISCORD_WEBHOOK_HIT_FLASH` | 的中速報（Hit Flash）専用 |
| `DISCORD_WEBHOOK_SYSTEM` | システムアラート（例外/JVLink障害）専用 |
| `DISCORD_WEBHOOK_EV_ALERT` | EV≥1.5 激熱レース（@everyone） |
| `DISCORD_WEBHOOK_AB_TEST` | V1/V2 A/B 成績比較レポート |
| `DISCORD_WEBHOOK_NOTE_DRAFT` | note 下書き転送 / X コピペ告知 |
| `DISCORD_WEBHOOK_SNS` | **死活監視ハートビート / SNS 集客**（3時間おき生存報告） |

### 3.3 SNS 集客・外部連携

| 変数 | 用途 |
|---|---|
| `NOTE_MYPAGE_URL` / `NOTE_PROFILE_URL` / `X_ACCOUNT_URL` | 集客導線 URL |
| `NOTE_EMAIL` / `NOTE_PASSWORD` | note.com 自動下書きログイン |
| `UMANITY_EMAIL` / `UMANITY_PASSWORD` | Umanity 連携 |
| `ANTHROPIC_API_KEY` | X シグナルパーサー / note 記事生成（Claude API） |

### 3.4 運用フラグ・パス（任意/既定あり）

| 変数 | 用途 | 既定 |
|---|---|---|
| `TARGET_JV_PATH` | TARGET frontier JV 実行ファイルパス（JVLink 自動起動） | 既知パス探索 |
| `JVLINK_DISABLED` | JVLink 無効化（netkeiba 専用モード）。本番では設定しない | 未設定=有効 |
| `DISABLE_MANJI_BETS` | 卍 緊急停止フラグ | `0` |
| `ENABLE_PLAYWRIGHT_POST` / `X_PLAYWRIGHT_ENABLED` | X 自動投稿/スクレイピング | `0` |
| `IS_PREMIUM_NOTE` | note 有料記事投稿 | 未設定 |
| `BANKROLL_OVERRIDE` / `BANKROLL_RESET_DATE` | Kelly 資金リセット | 未設定 |
| `CLOUD_BACKUP_DIR` | バックアップのクラウド同期先 | 空 |

> 🔐 **セキュリティ**: `.env` は `.gitignore` 済み。API キー・パスワードを Git に絶対コミットしない（CLAUDE.md 開発ルール §9）。

---

## 4. ログとディスク保護（運用硬化）

- 本番デーモン（scheduler / today_auto_runner / watchdog）のログは `src/ops/logger.py:setup_logging()` により
  **日次ローテーション（毎0時）＋7日保持**（`TimedRotatingFileHandler` `backupCount=7`）。8日以上前は自動削除され、
  ログ肥大化によるディスク枯渇でシステムが止まる事故を防ぐ。
- 出力先: `data/scheduler.log` / `data/auto_runner.log` / `data/watchdog.log`（ローテーション後は `.YYYY-MM-DD` サフィックス）。
- DR 時にログ設定は自動適用される（コード追従のため別途設定不要）。

---

## 5. 復旧後チェックリスト

- [ ] `startup_umalogi.bat` 実行後、3プロセス（python/streamlit）が起動している
- [ ] http://localhost:8501 ダッシュボードが表示される
- [ ] `PRAGMA integrity_check` が `ok`、`predictions` 件数が想定どおり
- [ ] Discord #system に起動通知が届いた／3時間以内に🟢生存報告が届く
- [ ] `schtasks /Query /TN "UMALOGI_Startup"` で自動起動が登録されている
- [ ] （JVLink 使用時）SID 認証が通り、当日オッズが `realtime_odds` に蓄積される

---

> 本書はコード現状（v1.4.5-dev）に追従する。起動方式・バックアップ・環境変数に変更があれば本書も同一コミットで更新すること（CLAUDE.md 条項7）。
