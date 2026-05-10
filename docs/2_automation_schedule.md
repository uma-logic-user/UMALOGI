# UMALOGI 自動化スケジュール設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-10 | 初版作成。週次オートパイロットサイクル全体フロー記述 |

---

## 1. 週次サイクル全体フロー

```
【金曜 20:00】
  job_friday_sync()
    ├── JVLink RACE 同期 (OPT_NORMAL/STORED/SETUP 自動切替)
    ├── JVLink WOOD 同期 (調教タイム)
    ├── 土曜の暫定予想生成 (provisional_batch)
    └── 日曜の暫定予想生成 (provisional_batch)

【土曜 07:00】
  job_weekend_batch_pre()
    └── weekend_batch.py (エントリ確認・netkeiba 補完)

【土曜 07:30】
  job_morning_wood()
    └── JVLink WOOD 当日分同期

【土曜 08:30】
  job_today_auto_runner()
    └── today_auto_runner.py --continuous (監視ループ開始)

【土曜 09:00】
  job_win5_prediction()
    └── 当日 WIN5 対象5レース × EV計算・推奨買い目生成・Discord送信

【土曜 各レース発走 20分前】
  prerace_pipeline(race_id)
    ├── entries 取得 (JVLink → netkeiba fallback)
    ├── オッズ取得 (realtime_odds → netkeiba fallback)
    ├── 特徴量生成 → 3モデル予測
    ├── 買い目生成 (Kelly 計算)
    └── Discord 通知 🟦🟩🟥

【土曜 各レース発走 15分後】
  fetch_race_result(race_id)
    └── 結果取得 → 的中評価 → 的中カード生成

【土曜 13:00, 15:30】
  job_intraday_sync()
    └── JVLink RACE 中間同期 (確定成績・払戻の取込)

【土曜 17:30】
  job_post_race()
    └── 全レース結果の最終評価・Discord サマリー送信

【土曜 18:30】
  job_weekend_batch_post()
    └── weekend_batch.py --post (最終精算・ダッシュボード更新)

【土曜 20:00】
  job_evening_fetch (today_auto_runner --continuous 内部)
    ├── JVLink RACE/WOOD 同期 (日曜分最新化)
    └── 日曜の暫定予想再生成

【日曜】
  土曜と同じスケジュールで実行

【日曜 完了後】
  _send_weekly_report()
    └── 週次収支サマリーを Discord 送信 (的中率/払戻/損益)

【月曜 06:00】
  job_monday_masters()
    └── 週次モデル評価・メタデータ更新

【月曜 07:00】
  job_weekly_retrain()
    ├── HonmeiModel.train() (全学習データ)
    ├── ManjiModel.train()
    ├── AlphaPayoutModel.train()
    └── Champion/Challenger 評価 → 勝ったモデルのみ保存

【月曜 08:00】
  job_git_push()
    └── 変更コード・モデルを Git 自動コミット

【毎時 :00】
  job_heartbeat()
    └── 生存確認・Discord 通知 (DISCORD_SYSTEM_WEBHOOK_URL)

【毎日 23:00】
  job_daily_backup()
    └── umalogi.db バックアップ (data/backup/)
```

---

## 2. スクリプト構成

| スクリプト | 役割 |
|-----------|------|
| `scripts/scheduler.py` | メインスケジューラデーモン (`schedule` ライブラリ) |
| `scripts/today_auto_runner.py` | 1日分の prerace/postrace 監視ループ |
| `scripts/self_healing_monitor.py` | 5分ごとの自律データ品質監視・自己修復 |
| `scripts/watchdog.py` | scheduler/auto_runner プロセスの死活監視 |
| `scripts/weekend_batch.py` | 週末前後の一括データ処理 |

---

## 3. prerace / postrace の詳細タイムライン

```
発走推定時刻 = R1: 10:00  以降 30分間隔
  R1  10:00  R2  10:30  ...  R12  15:30

prerace  fire_at = 発走推定 - 20分  (デフォルト --fire-ahead-min 20)
postrace fire_at = 発走推定 + 15分  (デフォルト --result-after-min 15)

スキップ条件 (2026-05-10 修正):
  prerace  → 発走推定 + 30分 を過ぎたらスキップ (旧: +5分)
  postrace → スキップなし (最大8回・5分間隔でリトライ)
```

---

## 4. Self-Healing Monitor (`scripts/self_healing_monitor.py`)

5分ごとに以下を監視し、異常時は自動修復:

| 監視項目 | 修復アクション |
|---------|--------------|
| races.race_name 文字化け | `repair_race_data.py --date` 実行 |
| races.distance = 0 or surface = '' | `repair_race_data.py --date` 実行 |
| 当日レースの predictions = 0件 | `prerace_pipeline(race_id)` 実行 |

修復失敗時は `data_sync.py` にフォールバック。  
連続修復のスロットリング: 同一 race_id への再修復は 180秒クールダウン。

---

## 5. 環境変数 (.env)

```
DISCORD_WEBHOOK_URL=        # 予想チャンネル
DISCORD_SYSTEM_WEBHOOK_URL= # システムログチャンネル
JRAVAN_SID=                 # JRA-VAN サービス ID (JVLink 認証)
```

---

## 6. 常駐プロセス構成

```
scheduler.py (メインデーモン)
  └── watchdog.py (scheduler の死活監視、クラッシュ時に自動再起動)

self_healing_monitor.py (独立デーモン、5分ループ)

今後追加予定:
  - タスクスケジューラ登録 (install_autostart.py / install_watchdog_task.py)
  - PC 再起動時の自動起動
```
