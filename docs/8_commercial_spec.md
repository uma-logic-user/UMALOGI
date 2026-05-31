# UMALOGI 商用化仕様書 v1.0

> **Week 1〜4 商用化ロードマップ完了版（2026-05-21 確定）**  
> 実装ベースコミット: `39d76066`

---

## 更新履歴

| 日付 | 内容 |
|------|------|
| 2026-06-01 | 【Note購入後の資金配分導線=おすすめ掛け金】読者が「予算内で各買い目にいくら賭けるか」で迷い販売導線(リピート購入)が詰まる課題を解消。`src/ops/sns_publisher.py` に EV 連動のおすすめ掛け金エンジンを追加: `recommended_unit_stake()`(EV<1.20→100円/1u安心投資, 1.20–1.40→300円/3u中勝負, 1.40+→500円/5u激熱勝負！)・`NoteBet`/`RecommendedBet`/`RecommendedBetPlan`・`calculate_recommended_note_bets()`・`format_recommended_bets_block()`。`scripts/generate_note_article.py` の買い目セクション直下へ「💰 AI推奨購入額（1点100円ベース換算）」ブロック(各点の掛け金＋想定総投資額＋倍率調整の免責)を自動挿入。テスト: tests/test_sns_publisher.py 拡張＋tests/test_note_article_recommended_bets.py 新設。影響ファイル: src/ops/sns_publisher.py, scripts/generate_note_article.py |
| 2026-06-01 | 【SNS集客→外部マネタイズ直結エンジン】隔離した観賞用モデル(Oracle/HitFocus)を集客資産として外部収益へ繋ぐ `src/ops/sns_publisher.py` を新設。X コピペ整形(`format_x_post`)・的中速報トリガー(`detect_and_flash`→Discord 集客ch Webhook)・週次 note 用 Markdown(`run_weekly_report`→`outputs/sns/weekly_report_YYYYMMDD.md`)の3経路を実装。実弾(本命/卍/Alpha)は集客統計から除外し会計と分離。詳細レイアウトは docs/4_ui_design.md 同日エントリ参照 |
| 2026-05-21 | 初版作成。Week1-4 商用化ロードマップ全完了・本番環境ロック確定を受け、実装実績を一本化 |

---

## 1. エグゼクティブサマリー

UMALOGI は JRA-VAN（JVLink）データ × LightGBM による **自律型・競馬予測プラットフォーム**。
2026-05-21 時点で Week1〜4 商用化ロードマップが完全完了し、実稼働フェーズへ移行した。

### 主要成果数値

| 指標 | 実績値 |
|------|--------|
| LightGBM モデル数 | 3 本（HonmeiModel / PlaceModel / ManjiModel）+ V2 系 2 本 |
| FEATURE_COLS | **69 列**（U-score 26 / x_consensus / EV 特徴量 7 / 騎手・調教師 / 基本特徴 ほか） |
| HonmeiModel CV AUC | **0.7677**（Challenger AUC: 0.9196） |
| PlaceModel CV AUC | **0.7293** |
| 学習データ | 84,930 行 × 90 列（6,138 レース分）Parquet cache |
| pytest | **466 テスト ALL GREEN** |
| E2E スループット | **5.12 秒**（prerace_pipeline 2.13 秒含む）|
| Discord チャンネル | 5 ch（prediction / system / ev_alert / ab_test / note_draft） |
| 週次スケジューラー | 10 ジョブ完全自動（金〜月） |

---

## 2. システムアーキテクチャ

```
【週次サイクル概要】

金曜 20:00  job_friday_sync
    │  JVLink / netkeiba → entries / realtime_odds / training_times
    ▼
土日 07:00  job_weekend_batch_pre
    │  暫定予想生成 → predictions テーブル → Discord prediction ch
    ▼
土日 08:30  job_today_auto_runner
    │  today_auto_runner.py 常駐ループ（直前予想・EV >= 1.5 → ev_alert）
    ▼
土日 09:00  job_win5_prediction
    │  WIN5 予想生成 → predictions テーブル
    ▼
土日 17:30  job_post_race
    │  JVLink RTD / netkeiba 払戻取得 → prediction_results 評価
    │  hit_flash → Discord prediction ch
    ▼
土日 18:30  job_weekend_batch_post
    │  週次 ROI レポート → ab_test ch / note_draft ch
    │  generate_note_article.py → post_weekly_note_draft.py
    ▼
月曜 07:00  job_weekly_retrain
    │  IncrementalTrainer.full_retrain()
    │  Champion/Challenger 比較 → data/models/ 更新
    ▼
月曜 08:00  job_git_push
    └─ git add & push → リモート自動同期
```

---

## 3. ML パイプライン仕様

### 3-1. モデル構成

| モデル | ファイル | 目的変数 | CV AUC | 特徴量数 |
|-------|---------|---------|--------|---------|
| HonmeiModel | `honmei_model.pkl` | `is_win` (1着=1) | **0.7677** | 69 |
| PlaceModel | `place_model.pkl` | `is_place` (3着以内=1) | **0.7293** | 69 |
| ManjiModel | `manji_model.pkl` | `ev_target` (払戻/賭金) | — (回帰) | 69 |
| HonmeiModelV2 | `honmei_model_v2.pkl` | `is_win` | 0.7679 | 69 |
| ManjiModelV2 | `manji_model_v2.pkl` | `ev_target` | — | 69 |

アルゴリズム: LightGBM + Isotonic Calibration + Platt Scaling（HonmeiModel）  
Champion/Challenger: 末尾 20% ホールドアウト AUC 比較、`challenger >= champion - 0.005` で更新

### 3-2. FEATURE_COLS 構成（69 列）

| カテゴリ | 列数 | 代表特徴量 |
|---------|------|-----------|
| U-score グループ A〜F | 26 | `u_score` / `uf_rank_trend` / `uf_jockey_win_rate` / `uf_crowd_bias` |
| X コンセンサス | 1 | `x_consensus_score` |
| EV 特化（買い目サイズ専用） | 7 | `shin_prob` / `harville_place_prob` / `odds_steam_flag` |
| 基本コース・騎手・調教師 | 35 | `distance` / `surface` / `jockey_id_enc` / `trainer_id_enc` / ほか |

> EV 特化 7 列は `_add_ev_features()` で生成されるが **FEATURE_COLS 外**（推論には使わず BetGenerator のサイジングに使用）

### 3-3. 学習データ・Parquet キャッシュ

- **生成**: `_build_train_df(conn)` → `data/processed/train_df_full.parquet`（4.35 MB）
- **初回生成**: 約 38 分（DB full scan）→ **キャッシュ使用時: 0.79 秒**（95% 短縮）
- **行数・列数**: 84,930 行 × 90 列（gitignore 対象・毎回再生成可能）

---

## 4. Discord 通知ルーティング設計（NotificationRouter）

### 4-1. チャンネルマップ

| チャンネル ID | 環境変数 | 用途 | EV 閾値 |
|--------------|---------|------|---------|
| `prediction` | `DISCORD_WEBHOOK_URL` | 買い目・的中結果（基本通知） | — |
| `system` | `DISCORD_WEBHOOK_SYSTEM` / 旧: `DISCORD_SYSTEM_WEBHOOK_URL` | スケジューラー例外・JVLink 障害 | — |
| `ev_alert` | `DISCORD_WEBHOOK_EV_ALERT` | EV ≥ 1.5 激熱アラート（@everyone） | 1.5 |
| `ab_test` | `DISCORD_WEBHOOK_AB_TEST` | V1/V2 成績比較レポート | — |
| `note_draft` | `DISCORD_WEBHOOK_NOTE_DRAFT` | note 下書き転送（チャンク分割 + X 告知） | — |

### 4-2. ルーティングロジック

```
notify_prerace_result(race_id, honmei_bets, manji_bets)
    │
    ├── prediction ch へ予想 embed 送信
    ├── 買い方ガイドテンプレート送信
    └── max_ev >= 1.5 かつ ev_alert ch 独立設定済み
            └── ev_alert ch へ @everyone + embed 送信

send_note_draft(title, body)
    │
    ├── note_draft ch へ 1800 字チャンク分割送信（ページング付き）
    └── X 告知ポスト案を自動生成して末尾送信

send_system_text(text)  ← スケジューラー例外ハンドラーから呼ばれる
    └── system ch（未設定時: prediction ch へフォールバック）
```

### 4-3. 本番環境確認状態（2026-05-21）

| 確認項目 | 状態 |
|---------|------|
| 全 5 チャンネル URL 設定 | ✅ |
| system チャンネル（旧変数名経由）| ✅ `DISCORD_SYSTEM_WEBHOOK_URL` でフォールバック確認 |
| E2E routing テスト | ✅ 12 テスト ALL PASS |
| 致命アラート → system ch パス | ✅ `test_system_alert_routes_to_system_channel` PASS |

---

## 5. 商用化パイプライン

### 5-1. note 記事自動生成フロー

```
job_weekend_batch_post (日曜 18:30)
    │
    ├── generate_performance_report.py  → 週次 ROI / 的中率集計
    ├── generate_ab_report.py           → V1 vs V2 A/B テスト比較
    ├── generate_note_article.py        → Markdown 記事生成
    │     --jackpot-only: JACKPOT（EV>=3.0）レース特化フォーマット
    │     IS_PREMIUM_NOTE=1: 有料記事フォーマット
    └── post_weekly_note_draft.py       → Discord note_draft へ転送
          ENABLE_PLAYWRIGHT_POST=1 時: note.com に Playwright 自動投稿
```

### 5-2. KPI 目標

| 指標 | 現在値 | Phase 2 目標 | Phase 3 目標 |
|------|--------|-------------|-------------|
| HonmeiModel CV AUC | 0.7677 | 0.80+ | 0.85+ |
| 複勝 ROI | 95.4% | 110%+ | 120%+ |
| note 有料読者数 | 0 | 50 | 100 |
| 月次 MRR | ¥0 | ¥30,000 | ¥100,000 |
| Discord フォロワー | — | 200 | 500 |

---

## 6. スケジューラー設計

### 6-1. ジョブ一覧

| ジョブ名 | 実行曜日・時刻 | 内容 | catchup 猶予 |
|---------|-------------|------|-------------|
| `job_friday_sync` | 金 20:00 | JVLink 同期・暫定予想 | 16h |
| `job_morning_wood` | 土日 07:30 | 調教タイム同期（32bit） | 4h |
| `job_weekend_batch_pre` | 土日 07:00 | 暫定予想生成 | 4h |
| `job_today_auto_runner` | 土日 08:30 | 直前予想ループ起動 | 3h |
| `job_win5_prediction` | 土日 09:00 | WIN5 予想 | 2h |
| `job_win5_result_fetch` | 土日 17:15 | WIN5 確定結果取得 | 4h |
| `job_post_race` | 土日 17:30 | 払戻同期・評価・通知 | 4h |
| `job_weekend_batch_post` | 土日 18:30 | ROI レポート・note 生成 | 4h |
| `job_monday_masters` | 月 06:00 | 馬・騎手マスタ更新 | 12h |
| `job_weekly_retrain` | 月 07:00 | LightGBM 週次再訓練 | 12h |
| `job_git_push` | 月 08:00 | リモート自動 push | 12h |

### 6-2. 例外ハンドリング

```python
# scheduler.py メインループ
try:
    schedule.run_pending()
except Exception as e:
    logger.critical("スケジューラー未処理例外: %s", e, exc_info=True)
    _send_discord(f"🚨 [UMALOGI] スケジューラー例外\n`{type(e).__name__}: {e}`")
```

`_send_discord()` → `NotificationRouter().send_system_text()` → `system` ch（フォールバック: `prediction` ch）

---

## 7. バックテスト・性能実績

### 7-1. Walk-Forward バックテスト（2025-01〜2026-05）

| 戦略 | ROI | 的中率 | 期間 |
|------|-----|--------|------|
| 本命 × ワイド（★QF推奨） | 805% | 47% | 17 窓 |
| 複勝 × 馬連（★QF推奨） | 963% | — | 17 窓 |
| 全 21 組み合わせ | すべて ROI 100%超 | — | 17 窓 |

### 7-2. ALPHA モデル（2024〜2025 実績）

| モデル | ROI | 備考 |
|-------|-----|------|
| 単勝 | 691.5% | 2024-01 win_odds データ期間 |
| 複勝 | 95.4% | EV ≥ 1.3 フィルタ適用 |
| 通算 | 202.8% | Phase 1 目標 110% 達成 |

### 7-3. E2E 本番シミュレーション（2026-05-21）

| ステップ | 結果 | 所要時間 |
|---------|------|---------|
| [1] prerace_pipeline | ✅ PASS | 2.13 秒 |
| [2] prediction ch | ✅ PASS | — |
| [3] ev_alert (EV=3.2) | ✅ PASS | — |
| [4] JACKPOT @everyone (EV=3.5) | ✅ PASS | — |
| [5] note_draft | ✅ PASS | — |
| [6] system ch | ✅ PASS | — |
| **総スループット** | **ALL PASS** | **5.12 秒** |

---

## 8. 本番環境構成

### 8-1. 環境変数（`.env`）

| キー | 必須 | 説明 |
|-----|------|------|
| `DISCORD_WEBHOOK_URL` | ✅ | prediction チャンネル（全通知フォールバック先） |
| `DISCORD_WEBHOOK_SYSTEM` | 推奨 | system アラート専用。未設定時 prediction へフォールバック |
| `DISCORD_SYSTEM_WEBHOOK_URL` | — | 旧変数名（後方互換、SYSTEM と同一チャンネルを指定） |
| `DISCORD_WEBHOOK_EV_ALERT` | ✅ | EV ≥ 1.5 激熱 @everyone |
| `DISCORD_WEBHOOK_AB_TEST` | — | A/B 比較レポート |
| `DISCORD_WEBHOOK_NOTE_DRAFT` | ✅ | note 下書き Discord 転送 |
| `JRAVAN_SID` | ✅ | JRA-VAN Data Lab 会員 SID |
| `NOTE_EMAIL` / `NOTE_PASSWORD` | ✅ | note.com 自動投稿用 |
| `ENABLE_PLAYWRIGHT_POST` | — | `1` で note.com 自動投稿（デフォルト: `0`） |
| `IS_PREMIUM_NOTE` | — | `1` で有料記事フォーマット（デフォルト: 無料） |
| `INITIAL_BANKROLL` | — | 初期バンクロール（デフォルト: 100000） |

### 8-2. データ永続化

| 種別 | パス | gitignore | 備考 |
|------|-----|-----------|------|
| メイン DB | `data/umalogi.db` | ✅ | SQLite WAL mode |
| 訓練済みモデル | `data/models/*.pkl` | ✅ | ~1MB/モデル |
| Parquet キャッシュ | `data/processed/train_df_full.parquet` | ✅ | 4.35 MB・再生成可能 |
| 予想 JSON | `data/predictions/*.json` | ✅ | 毎回再生成 |
| バックアップ | `data/backups/umalogi_*.db` | ✅ | 作業前に手動作成 |

### 8-3. Windows 自動起動

```
スタートアップフォルダ: %APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
配置ファイル: UMALOGI_Scheduler.vbs
内容:
  Set oShell = CreateObject("WScript.Shell")
  oShell.Run """C:\dev\horse-racing-ai\UMALOGI_SCHEDULER.bat""", 7, False
```

配置スクリプト: `scripts/setup_startup.py`（`--uninstall` でエントリ削除）

---

## 9. Week1-4 完了タスク総覧

| Week | タスク | コミット / 完了日 |
|------|-------|----------------|
| Week 1 | U-score 18 因子実装 (src/ml/u_score.py) | 2026-05-17 |
| Week 1 | JVLink 完全自動化 (setup_target_autostart.py) | 2026-05-17 |
| Week 1 | W-004 大衆心理乖離スコア (crowd_bias_ratio) | 2026-05-18 |
| Week 1 | 弱点管理台帳 (docs/7_weakness_ledger.md) 新設 | 2026-05-18 |
| Week 2 | X シグナル Phase A/B/C 実装 (x_scraper / x_signal_parser) | 2026-05-18〜20 |
| Week 2 | Walk-Forward バックテスト全 21 組 ROI 100%超 確認 | 2026-05-19 |
| Week 2 | V1/V2 A/B テスト分離 (models_v2.py) | 2026-05-19 |
| Week 2 | 動的 EV 閾値 + Kelly 資金管理 (W-022) | 2026-05-19 |
| Week 3 | EV 特化特徴量エンジン Phase 1 (ev_features.py / 71 テスト) | 2026-05-20 |
| Week 3 | Discord NotificationRouter 新設 W-028 完了 | 2026-05-20 |
| Week 3 | note 下書き転送・IS_PREMIUM_NOTE・週次 note 生成 | 2026-05-20 |
| Week 4 | DB 複合インデックス 6 件 (W-029 / migration #15) | 2026-05-21 |
| Week 4 | EV 特徴量 features.py 統合 (W-030) | 2026-05-21 |
| Week 4 | 69 FEATURE_COLS 全モデル完全再訓練 (AUC 0.7677) | 2026-05-21 |
| Week 4 | 466 テスト ALL GREEN + Parquet キャッシュ | 2026-05-21 |
| Week 4 | E2E 本番シミュレーション ALL PASS (5.12 秒) | `11803b33` |
| Week 4 | Production Lock 確定 + 致命アラートテスト 12 PASS | `39d76066` |

---

## 10. 次フェーズロードマップ（Phase 2 以降）

> 本番稼働フェーズ移行後。JVLink SID 制約解消を待って実施。

| 優先度 | タスク | 条件 |
|--------|-------|------|
| 高 | W-001 加速力スコア (上がり 3F) | JVLink ラップ DB カラム追加後 |
| 高 | W-020 FukushoElite 本番統合 | X シグナル再訓練後 (ROI 110%+ 目標) |
| 中 | W-002 PCI ペース変動指数 | W-001 と同時 |
| 中 | 歴史データ大規模取得（2023〜2025 3 年分） | SID 制約解消後 |
| 中 | note 有料記事毎週自動投稿 CI/CD | Phase 2 開始時 |
| 低 | W-023 破産確率 UI (Monte Carlo) | Web ダッシュボード改修時 |
