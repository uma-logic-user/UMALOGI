# UMALOGI 自動化スケジュール設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-24 | 坂路調教スクレイプジョブ追加: `job_training_hillwork_scrape()` を scheduler.py に追加。木曜20:00・金曜18:00に今週末レースの race_id を取得し netkeiba 調教ページ（training.html）をスクレイプ。JVLink WOOD dataspec に WH レコードが含まれないため netkeiba で補完。既存バックフィルは調教ページがレース後削除のため不可（W-026 参照）。影響: scripts/scheduler.py, scripts/backfill_training_hillwork.py(新規) |
| 2026-05-23 | `job_friday_sync` を土曜20:00にも追加（日曜暫定予想の取りこぼし修正）: `register_schedules()` に `schedule.every().saturday.at("20:00").do(job_friday_sync)` を追加。`_JOB_SCHEDULES["job_friday_sync"]` に `(5, 20, 0)` を追記。`docs/automation_schedule.md` を新規作成（Claude Codeへの絶対指示付き）。影響: `scripts/scheduler.py`, `docs/automation_schedule.md`(新規) |
| 2026-05-23 | Oracle/HitFocus廃止: `prerace_pipeline()`からOracle/HitFocus生成呼び出し削除。`_save_predictions()`のOracle/HitFocusブロック削除。`notify_prerace_result()`のoracle_bets/hit_focus_bets引数削除。note_generatorを3モデル（本命・卍・ALPHA）合意スコアに変更。テスト573件PASS。影響: src/pipeline/prediction.py, src/notification/discord_notifier.py, src/notification/router.py, src/ops/note_generator.py |
| 2026-05-23 | note記事完全自動化ルーティン確立: `job_note_daily_article()` を scheduler.py に追加（土日 10:30）。Step1=記事生成(`note_generator.generate()`) → Step2=Discord note_draftチャンネルへ全文転送 → Step3=Discord systemチャンネルへ厳選レースEmbed送信 → Step4=`NOTE_DRAFT_AUTO_POST=1` かつ `.note_session.json` 存在時のみ `note_draft_publisher.save_draft()` でPlaywright自動保存。リカバリー窓3時間。CLI: `py scripts/scheduler.py --run-now note_article`。テスト15件PASS。影響: scripts/scheduler.py, .env(NOTE_DRAFT_AUTO_POST=0追加) |
| 2026-05-23 | note当日予想記事生成エンジン新設: `src/ops/note_generator.py` を実装。4モデル合意スコアで当日レースを採点し上位3〜5本を厳選したMarkdown記事を自動生成。CLI: `py -m src.ops.note_generator --date YYYYMMDD` → `outputs/note/YYYYMMDD_recommendations.md`。影響: src/ops/note_generator.py(新規), outputs/note/(新規) |
| 2026-05-21 | `generate_ab_report.py` に `_send_summary_to_discord(v1, v2, days)` 実装: ROI・純利益・勝者バッジを Discord Embed として `DISCORD_WEBHOOK_AB_TEST` へプッシュ送信（Webhook未設定時スキップ・HTTP4xx/5xx・OSError は WARNING 止まりで例外伝播なし）。`main()` 内で `_summary_row()` を追加呼び出しし、全文レポート送信後にコンパクト Embed も連続送信。`TestSendSummaryToDiscord` 8件追加（mock `urlopen`）。全スイート 486 PASS。影響: `scripts/generate_ab_report.py`, `tests/scripts/test_ab_report.py` |
| 2026-05-21 | V1 vs V2 A/B テスト週次レポート自動化完了: `scripts/generate_ab_report.py` を完全実装（対象レース数・ベット数・的中率・ROI・純利益・EV乖離MAE の V1/V2 比較 Markdown 生成、券種別詳細テーブル付き）。`scripts/scheduler.py` の `_JOB_SCHEDULES` / `_CATCHUP_HOURS` に `job_ab_report` を追加（日曜18:00・取りこぼし4時間）。`register_schedules()` にスケジュール登録済み。`_JOB_MAP_FULL` / `_JOB_MAP`（--run-now ab_report）でリカバリー・CLI対応。`tests/scripts/test_ab_report.py` 9件（総合サマリー/純利益/V2優勢判定/EV MAE/レース数/勝者バッジ検証を追加）/ `tests/test_scheduler_state.py` 4件（計13件 all PASS / 全スイート 478 PASS）。影響: `scripts/generate_ab_report.py`, `scripts/scheduler.py`, `tests/scripts/test_ab_report.py`, `tests/test_scheduler_state.py` |
| 2026-05-20 | note週次記事生成ロジック v3 マーケティング特化改修: ①全モデル合算ROI表示を廃止（赤字ROIがブランド毀損のため）。②`_fetch_winning_segments()` 実装: ROI≥100%セグメントを「完全勝利！」として自動選別、該当なし時は TOP5 ROI を「ベストパフォーマー」にフォールバック。③`_fetch_manbaiken_hits()` 実装: 払戻¥5,000以上を grade 分類（tokudai=¥100k+/manbaiken=¥10k+/kodai=¥5k+）し記事最上部に最大装飾で配置。④「次世代V2予告」セクションを固定追加（抽象表現のみ・実装詳細は非公開）。⑤`tests/test_generate_weekly_note.py` 新規作成 (35テスト全パス)。⑥`scripts/post_weekly_note_draft.py` 新規作成（記事生成→note.com下書き保存をワンコマンド化）。影響: `scripts/generate_weekly_note.py`, `scripts/post_weekly_note_draft.py`(新規), `tests/test_generate_weekly_note.py`(新規) |
| 2026-05-19 | note週次記事スクリプト大幅改修 v2: ①集計SQL修正（卍複勝72件 → 全モデル・全券種2,350件）。②モデル別比較テーブル（ALPHA/本命/Oracle/卍/HitFocus × 暫定+直前合算）。③QF推奨ハイライト（ワイド・馬連 562件独立集計）。④V1 vs V2 A/Bテスト比較枠（V2未稼働時はカウントダウン表示、稼働後は自動リアルタイム比較）。⑤損益符号バグ修正（-¥表示）。⑥重複レース排除。影響: `scripts/generate_weekly_note.py` |
| 2026-05-19 | note.com 週次まとめ記事自動生成: `scripts/generate_weekly_note.py` 新規作成（2部構成: Part1=先週の卍複勝成績/競馬場別内訳/注目的中例/W-004コラム、Part2=EV≥1.5 QF推奨ピック）。`docs/note_drafts/YYYY-MM-DD_weekly_note.md` に出力。`scheduler.py` の `job_weekend_batch_post()` に日曜のみ実行するフック追加（`date.today().weekday()==6`）。影響: `scripts/generate_weekly_note.py`(新規), `scripts/scheduler.py` |
| 2026-05-19 | 【V1/V2 並列稼働・週次再学習対応】①src/ml/models_v2.py 新設: HonmeiModelV2/ManjiModelV2/PlaceModelV2（pkl=*_v2.pkl、V1と独立した世代管理）。②BetGeneratorV2 追加（W-004大衆心理乖離+動的EV閾値+1/4 Kelly）。③prerace_pipeline() に model_version="v1"\|"v2" 引数追加、V2は{race_id}_v2.json へ出力。④today_auto_runner.py: V1 prerace 成功後に V2 も並列実行（_prerace_worker内）。⑤IncrementalTrainer.full_retrain() で V1 再学習後に V2(HonmeiModelV2/ManjiModelV2) も同時再学習→月次で独立した pkl に収束。⑥notify_discord.py: V2予想をメイン（★QF推奨+Kelly+動的EV）、V1を比較セクションで併記。影響: src/ml/models_v2.py, src/ml/bet_generator.py, src/ml/incremental.py, src/pipeline/prediction.py, src/main_pipeline.py, scripts/today_auto_runner.py, scripts/notify_discord.py |
| 2026-05-19 | 【W-022 動的EV閾値 完全実装】get_dynamic_ev_threshold(conn, lookback_days=28): 直近28日ROIを prediction_results から集計 → ROI≥150%→EV≥1.1 / 110-150%→1.2 / 80-110%→1.3 / <80%→1.5 に自動切替。calc_qf_kelly_bet(): implied_prob=EV/odds → 1/4 Kelly → ¥100単位ベット額。Discord通知に総資金比・Kelly%を自動表示。影響: src/ml/bet_generator.py, scripts/notify_discord.py |
| 2026-05-16 | 【today_auto_runner 耐障害性強化】①ThreadPoolExecutorを prerace/postrace に完全分離（umalogi-pre max=12, umalogi-post max=40）→ postrace 長期リトライが prerace 発火をブロックしない。②PIDファイルロック(_LOCK_FILE)で重複起動を防止。③例外ハンドラを「1回リトライしてbreak→プロセス死亡」から「無限continueリトライ」に修正。④postrace再試行を300s×8→120s×20に変更（最大40分同等）。⑤scheduler.pyのjob_today_auto_runner()に自動リスタートループ追加（rc!=0かつ19時前なら30秒後再起動）。影響: scripts/today_auto_runner.py, scripts/scheduler.py |
| 2026-05-13 | WIN5完全実装: job_win5_result_fetch()追加（土日17:15 netkeiba WIN5結果取得→win5_results保存）。job_win5_prediction()エラー時Discord🚨通知追加。start_ui.bat/start_ai.bat 分離。影響: scripts/scheduler.py, scripts/fetch_win5_result.py, src/database/schema.py, src/database/init_db.py |
| 2026-05-13 | WIN5沈黙バグ修正: job_win5_prediction()に_mark_job_done()追加（スキップ・成功時どちらも）。logger.info()の%,.0fフォーマットバグ修正→f-string化。原因特定: 5/9はno such column:win_oddsで失敗、5/10はPC停止でスケジューラー不在。影響: scripts/scheduler.py, src/pipeline/win5.py |
| 2026-05-12 | Day2 SRE: weekly_backup.py 追加（毎週月曜06:00、ZIP 12世代保持）。scheduler.py に job_weekly_backup() 登録。影響: scripts/weekly_backup.py, scripts/scheduler.py |
| 2026-05-11 | モバイルアクセス基盤: Tailscale VPN + HKCU Run 自動起動方式。install_autostart.ps1 をレジストリ方式に刷新。影響: scripts/install_autostart.ps1 |
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
