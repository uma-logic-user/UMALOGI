# UMALOGI 弱点・技術的負債 管理台帳

> **CLAUDE.md 条項「弱点管理ルール」に基づき、システムの弱点・技術的負債・未実装機能を**
> **一元管理するドキュメント。新規指示を受けた際は必ずこのファイルを冒頭に確認し、**
> **過去の弱点の改善状況を更新してから実装に進むこと。**

---

## 更新履歴

| 日付 | 更新内容 |
|------|---------|
| 2026-05-18 | 初版作成。社長指令「ビジョン再監査」を受け、U score ギャップ・インフラ・データ弱点を全面棚卸し |
| 2026-05-18 | 【W-004 実装完了】大衆心理乖離スコア (crowd_bias_ratio / uf_crowd_bias) を u_score.py・models.py・bet_generator.py に追加。ManjiGenerator・HonmeiGenerator の EV 調整まで統合完了 |
| 2026-05-19 | 【W-026 完了確認】_IsotonicModel プロキシ追加により増分学習 E2E 動作確認済み。フルモード WF バックテスト完走（OOM回避: expanding window + float32 + max_bin=127）。全21組み合わせ ROI 100%超。★QF推奨戦略（本命×ワイド ROI=805%/複勝×馬連 ROI=963%）を bet_generator.py・notify_discord.py に実装。W-022 部分対応: QF推奨 EV≥1.3 フィルタを実質的に適用 |
| 2026-05-19 | 【W-022 完全実装】動的EV閾値: get_dynamic_ev_threshold() で直近28日ROIから1.1/1.2/1.3/1.5を自動選択。Kelly資金管理: calc_qf_kelly_bet()実装。notify_discord.pyにDB接続→閾値・バンクロール自動取得・QF推奨セクションへの推奨ベット額・Kelly%・総資金比を表示統合。影響: src/ml/bet_generator.py / scripts/notify_discord.py |
| 2026-05-19 | 【V1/V2 モデル分離・週次再学習対応完了】models_v2.py 新設・BetGeneratorV2・prerace_pipeline model_version 引数・_archive_and_save() 命名バグ修正・IncrementalTrainer.full_retrain() V2 同時再学習対応。今週末より実弾 A/B テスト開始。影響: src/ml/models_v2.py / src/ml/incremental.py / src/pipeline/prediction.py |
| 2026-05-20 | 【商用化ロードマップ策定・全4週タスク完了】通知ルーター(W-028完了)・実績レポート自動化(generate_performance_report.py)・A/Bテスト自動比較(generate_ab_report.py)・note下書き転送・X信号統合Phase C(FEATURE_COLS)・有料JACKPOT記事フォーマット確立(generate_note_article.py --jackpot-only)・scheduler 月曜08:30/日曜18:00自動ジョブ登録 |
| 2026-05-20 | Discord 通知ルーター新設 (NotificationRouter): EV激熱アラート・note下書き転送・ENABLE_PLAYWRIGHT_POST トグル・IS_PREMIUM_NOTE 有料/無料出し分け・買い方テンプレート自動生成・2カ年バックテストシミュレーター・万馬券特化報告スクリプト実装。影響: src/notification/router.py, src/pipeline/prediction.py, scripts/post_weekly_note_draft.py, scripts/generate_weekly_note.py, scripts/run_2year_backtest.py, scripts/generate_result_note_draft.py |
| 2026-05-20 | EV 特化特徴量エンジン Phase 1 実装（71 テスト全 PASS）: JRATakeoutRates（控除率クラス定数）・Shin 1993 真確率推定・Harville 法・オッズ異常検知・np.cumprod Kelly バンクロールシミュレーター・Sharpe/MDD・グリッドサーチ・READ ONLY DB 監査スクリプト。W-029 (DB インデックス最適化) を Phase 2 として計上、承認待ち。|
| 2026-05-21 | 【Week1-4 商用化ロードマップ完全完了 + 本番環境ロック確定】① W-029 完了: DB 複合インデックス 6 件 (migration #15) 適用。idx_pred_model_ev/idx_pred_race_model/idx_tc_horse_date/idx_hc_horse_date/idx_rr_horse_race/idx_pr_pred_hit。② W-030 完了: EV 特化特徴量 7 本を features.py へ統合・try/except ガード付き安全実装。③ 69 FEATURE_COLS 全モデル完全再訓練: HonmeiModel CV AUC=**0.7677** (特徴量重要度 Top3: uf_rank_trend/uf_jockey_win_rate/u_score) / PlaceModel AUC=**0.7293** / ManjiModel 完了。Parquet cache 84,930 行×90 列で再学習 95% 短縮 (38分→2分)。466 テスト ALL GREEN。④ E2E 本番シミュレーション (scripts/e2e_production_sim.py) 全 6 ステップ ALL PASS: prerace_pipeline 2.13秒 / 全 Discord チャンネル routing 確認 / 総スループット **5.12秒**。⑤ 本番ロック確定: DISCORD_WEBHOOK_URL/EV_ALERT/AB_TEST/NOTE_DRAFT/DISCORD_SYSTEM_WEBHOOK_URL 全 URL 設定済み・JVLINK_DISABLED 未設定 (本番 JVLink 有効) / ENABLE_PLAYWRIGHT_POST=0 (X 自動投稿安全オフ) / DRY_RUN 未設定 (本番モード) / scheduler.py 全ジョブ dry_run=False 確認済み。⑥ system アラートテスト 2 件追加 (test_system_alert_routes_to_system_channel / test_legacy_system_webhook_url_accepted): 計 12 テスト PASS。⑦ .env.example 復旧 (全 13 キー完全文書化)。影響: tests/notification/test_router.py, .env.example(復旧) |

---

## ステータス凡例

| マーク | 意味 |
|--------|------|
| 🔴 未着手 | 対応を開始していない |
| 🟡 対応中 | 実装・調査が進行中 |
| 🟢 完了 | 本番反映済み・検証完了 |
| ⚪ 保留 | 意図的に対応を見送り中（理由を記載） |

---

## カテゴリ 1: U score — 完全体ビジョンとのギャップ

> **社長ビジョン**: 「1000以上の要素から厳選した30項目（加速力・PCI・不完全燃焼度・大衆心理ジレンマ排除・AIチーム目視分析）」  
> **現状**: Phase 1 として18因子実装済み（2026-05-17）  
> **目標差分**: **12因子** の追加実装が必要

### 現在実装済み（Phase 1: 18因子）

| # | 因子名 | グループ | 重み | ステータス |
|---|-------|---------|------|-----------|
| 1 | 通算勝率 | A: 能力指数 | 40% | 🟢 完了 |
| 2 | 馬場別勝率 | A: 能力指数 | - | 🟢 完了 |
| 3 | 距離帯別勝率 | A: 能力指数 | - | 🟢 完了 |
| 4 | 直近着順スコア | A: 能力指数 | - | 🟢 完了 |
| 5 | 着順改善トレンド | A: 能力指数 | - | 🟢 完了 |
| 6 | 前走休養日数 | A: 能力指数 | - | 🟢 完了 |
| 7 | 騎手直近勝率 (90日) | B: 人的要素 | 30% | 🟢 完了 |
| 8 | 調教師直近勝率 (90日) | B: 人的要素 | - | 🟢 完了 |
| 9 | 騎手×馬コンビ率 | B: 人的要素 | - | 🟢 完了 |
| 10 | 騎手×会場勝率 | B: 人的要素 | - | 🟢 完了 |
| 11 | 枠番適性 | C: コース適性 | 20% | 🟢 完了 |
| 12 | 会場別勝率 | C: コース適性 | - | 🟢 完了 |
| 13 | 美浦・栗東マッチ | C: コース適性 | - | 🟢 完了 |
| 14 | ウッドスピード指数 | D: 調教指数 | 7% | 🟢 完了 |
| 15 | 坂路スピード指数 | D: 調教指数 | - | 🟢 完了 |
| 16 | 父馬距離適性 | E: 血統適性 | 3% | 🟢 完了 |
| 17 | 母父馬場適性 | E: 血統適性 | - | 🟢 完了 |
| 18 | 父系統適性 | E: 血統適性 | - | 🟢 完了 |

---

### 未実装（Phase 2: 12因子 — 社長ビジョン完全体へ）

#### W-001: 加速力スコア (Acceleration Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「加速力」— 単なる着順でなく、上がり3Fと前半ラップの差分から"末脚の切れ味"を数値化 |
| **実装概要** | `race_results.finish_time` + JVLink ラップタイム（3F/4F）から算出。上がり3F≤34.0秒で高スコア。`v_race_mart` に `last3f_time` を追加後に算出可能 |
| **データ依存** | ラップタイム: JVLink RACE RA レコードから取得（現在未格納）|
| **追加SQL** | `training_times.time_3f` / 本番は `races` に `lap_3f` カラム追加が必要 |
| **担当フェーズ** | Phase 2-A |

#### W-002: ペース変動指数 (PCI: Pace Change Index)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「PCI」— レース前半と後半のペース差分。ハイペース流れ込み型か、スロー上がり勝負型かを数値化し、脚質との相性スコアに変換 |
| **実装概要** | PCI = (後半3F × 2) / 全体タイム。馬の脚質（先行/差し/追込）とのマッチング係数 `pci_style_match` として実装 |
| **データ依存** | レースラップタイム（上記 W-001 と同じ DB カラム追加が前提）|
| **担当フェーズ** | Phase 2-A（W-001 と同時実装可能）|

#### W-003: 不完全燃焼度スコア (Incomplete Combustion Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「不完全燃焼度」— 前走で力を出し切れなかった馬を発掘。"今回こそ爆発する"という隠れた期待値の源泉 |
| **実装概要** | 以下の条件で `uf_incompleteness` スコア (0〜1) を算出:<br>① 前走着順 < モデル予測順位（実力負け）<br>② 前走が不良馬場で今回は良馬場<br>③ 前走が大外枠で今回は内枠<br>④ 前走がハイペースで今回はスロー予想<br>各条件に重みを付けて合算 |
| **データ依存** | 前走 predictions テーブルのスコア + 馬場状態 + 枠番の変化 |
| **担当フェーズ** | Phase 2-B |

#### W-004: 大衆心理乖離スコア (Crowd Bias Removal Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🟢 完了（2026-05-18） |
| **社長ビジョン** | 「大衆心理のジレンマを排除した真の期待値算出」— 市場の過大/過小評価を定量化し、モデルの EV 計算に組み込む |
| **実装概要** | `crowd_bias_ratio = win_rate_all / market_implied_prob`（学習特徴量）<br>市場乖離 EV 倍率: crowd_bias > 1.3 → 最大 1.5x EV ブースト<br>crowd_bias < 0.7 → 最小 0.5x EV ペナルティ<br>bet_generator.py の ManjiGenerator / HonmeiGenerator 両方に適用済み |
| **実装ファイル** | `src/ml/u_score.py` (_calc_crowd_bias 新設・グループF追加・重み5%)<br>`src/ml/models.py` (FEATURE_COLS: uf_crowd_bias / crowd_bias_ratio 追加)<br>`src/ml/bet_generator.py` (_crowd_bias_ev_multiplier 新設・両Generator適用) |
| **データ依存** | `market_prob` 列（features.py で `1/min(win_odds, 80)` として既存） |
| **効果測定** | ドライラン再学習（2026-05-18）: HonmeiModel AUC **0.7591 → 0.7679**（+0.0088向上） |
| **担当フェーズ** | Phase 2-B ✅ |

#### W-005: X シグナルコンセンサス係数

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🟡 対応中（Phase A スクレイパー実装済み、Phase B 構造化未着手）|
| **社長ビジョン** | 「AIチームの目視分析」— 凄腕予想家の印を AI で構造化し、EV 計算の第4のファクターとして加算 |
| **実装概要** | `x_consensus_score = weighted_avg(x_signals.confidence)` by horse_number<br>重みは過去的中率で動的調整<br>FEATURE_COLS に `x_consensus_score` 追加 |
| **データ依存** | `x_signals` テーブル（DDL 作成済み）、Phase B: Claude Haiku 構造化 |
| **残作業** | Phase B: `src/ml/x_signal_parser.py` 作成<br>Phase C: FEATURE_COLS 統合 + モデル再訓練 |
| **担当フェーズ** | Phase B: 平日実装 |

#### W-006: オッズ動向スマートマネーシグナル

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟡 対応中（特徴量定義済み、実データ蓄積待ち）|
| **実装概要** | `odds_vs_morning`: 直前オッズ / 朝一オッズ（短縮=大口流入シグナル）<br>`odds_velocity`: 直近1時間のオッズ下落速度 |
| **データ依存** | `realtime_odds` の時系列蓄積（現在は当日1点のみ）|
| **残作業** | 朝一オッズ取得スケジュール追加（8:30 と 14:30 の2点保存）|
| **担当フェーズ** | Phase 2-C |

#### W-007: 斤量インパクト因子

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走比斤量変化（`weight_carried_diff`）と、その馬の斤量耐性（過去斤量55kg超のレースでの勝率）を組み合わせたスコア |
| **データ依存** | `race_results.weight_carried`（既存）|
| **担当フェーズ** | Phase 2-A（データ既存のため比較的容易）|

#### W-008: 馬場バイアス × 脚質マッチング

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | コース別馬場状態（良/稍重/重/不良）× 馬の脚質（先行/差し/追込）の過去勝率マトリクスを特徴量化。`uf_surface_style_match` スコア |
| **データ依存** | `races.condition`（既存）+ 脚質分類（前走着順推移から推定）|
| **担当フェーズ** | Phase 2-B |

#### W-009: 輸送疲れ係数

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走会場 → 今走会場の輸送距離（例: 函館→東京=長距離）に応じて `uf_transport_fatigue` スコアを減点。輸送歴がある馬の克服実績で補正 |
| **データ依存** | `races.venue`（既存）|
| **担当フェーズ** | Phase 2-C |

#### W-010: 相手関係指数 (Competition Strength Index)

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 今回の出走メンバーの `u_score` 平均値と対象馬の `u_score` の差分。強いメンバーでの高着順は評価アップ、弱いメンバーでの高着順は評価抑制 |
| **データ依存** | `u_score` の実装完了（Phase 1 済み）|
| **担当フェーズ** | Phase 2-B |

#### W-011: クラス昇降格インパクト

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走クラス（500万下/1000万下/オープン等）と今走クラスの差分。降格馬は高評価、昇格馬は割引。`uf_class_change` (-1〜+1) |
| **データ依存** | `races.grade`（既存）|
| **担当フェーズ** | Phase 2-A |

#### W-012: 非公開情報スコア (Proprietary Signal Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低（将来）|
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「1000以上の要素から厳選」の最終段階。X シグナル・厩舎コメント・前日オッズ異変など非定量情報を Claude で構造化 |
| **実装概要** | Phase B の x_signal_parser.py を拡張し、厩舎コメント（JVLink SE レコードの「調教コメント」）も取得・構造化 |
| **データ依存** | x_signal_parser.py Phase B 完成後 |
| **担当フェーズ** | Phase 3 以降 |

---

### U score ギャップサマリー

```
社長ビジョン 30因子
  ├─ 実装済み: 18因子 (Phase 1完了 ✓)
  ├─ Phase 2-A: W-001(加速力) / W-002(PCI) / W-007(斤量) / W-011(クラス変化) — 4因子
  ├─ Phase 2-B: W-003(不完全燃焼) / W-004(大衆心理) / W-008(馬場脚質) / W-010(相手関係) — 4因子
  ├─ Phase 2-C: W-006(オッズ動向) / W-009(輸送疲れ) — 2因子
  └─ Phase B連携: W-005(Xシグナル) / W-012(非公開情報) — 2因子

Phase 2-A 完了後: 22因子
Phase 2-B 完了後: 26因子
Phase 2-C+B完了後: 30因子 ← 社長ビジョン達成
```

---

## カテゴリ 2: データ弱点

#### W-013: win_odds 歴史データ欠損（JVLink SID 制約）

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留（外部制約）|
| **影響** | 2024-01 以前の単勝オッズが学習データに存在しない → EV 計算精度が2024年以降のデータに依存 |
| **対応方針** | `netkeiba_research.db` に 2024-2025 を 99.1% スクレイプ済み。本番 DB へのマージが完了次第、歴史オッズで再学習 |
| **解除条件** | CLAUDE.md §14「歴史データ大規模取得」参照 |

#### W-014: jockeys/trainers マスタ未充足

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留 |
| **影響** | 騎手・調教師のコード→名前変換に LabelEncode（名前文字列）を使用中。名前変更や新人への対応が不安定 |
| **対応方針** | JVLink DIFN KS/CH マスタを週次取得で充足。スケジューラの job_monday_masters() で実施中 |

#### W-015: ラップタイムデータ未格納

| 項目 | 内容 |
|------|------|
| **ステータス** | 🔴 未着手（W-001/W-002 の前提条件）|
| **影響** | 加速力・PCI 因子が実装できない。上がり3F が最重要指標なのに DB に存在しない |
| **対応方針** | JVLink RACE RA レコードから `RA_LAPS_*` フィールドを取得し `races` テーブルに `last3f_time` / `lap_data` カラムを追加 |
| **作業量** | jravan_client.py の RA パーサー拡張 + schema.py マイグレーション |

#### W-016: 2025年着順データの欠損バイアス

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟡 対応中 |
| **影響** | 2025年 race_results の rank データが有効行 11.5% のみ（残りは rank=0 or NULL）。本命/卍モデルのバックテストが無効 |
| **対応方針** | netkeiba_research.db から 2〜18 着を補完するスクリプトを作成 |

---

## カテゴリ 3: インフラ弱点

#### W-017: JVLink ダイアログ抑制の不確実性

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟢 完了（2026-05-18）|
| **修正内容** | 3段フォールバック（ParentHWnd → JVSetUIProperties → JVSetUI(0)）+ `_kill_stale_py32` 64bit 誤 kill 根治 + `_JVLINK_STARTUP_TIMEOUT` 60秒化 |
| **E2E 証明** | elapsed=2.78s で JVLINK_READY 受信確認。64bit outer 生存確認 |

#### W-018: オッズ取得の netkeiba 依存

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟡 対応中 |
| **影響** | JVLink realtime_odds が空の場合、netkeiba からフォールバック取得。netkeiba の利用規約・レート制限に依存 |
| **対応方針** | CLAUDE.md §11「JVLink 一次・netkeiba 二次」の二段構え維持。JVLink SID 制約解消後に移行 |

#### W-019: SQLite の並行書き込み競合

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留（現状スケール内）|
| **影響** | scheduler.py の複数ジョブが同時 DB 書き込みを試みると SQLite の write lock 競合が起きる可能性 |
| **対応方針** | WAL モード + `busy_timeout=10000ms` で対応済み（init_db.py）。スケール拡大時は PostgreSQL 移行を検討 |

---

## カテゴリ 4: モデル弱点

#### W-020: FukushoElite 本番未統合

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟡 対応中（実装済み・本番未結合）|
| **影響** | 複勝 ROI 95.4% → 110%+ 目標のモデルが本番 predictions テーブルに結合されていない |
| **対応方針** | CLAUDE.md §13 参照。X シグナル統合後に再訓練してから本番統合 |

#### W-021: WIN5 モデル精度不足

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低 |
| **ステータス** | 🔴 未着手 |
| **影響** | WIN5 ROI 22.8%（目標 110%+）。現在は等確率 + market 50/50 ブレンドで実質ランダム |
| **対応方針** | 本命モデルスコアを WIN5 エンジンに組み込む（CLAUDE.md §15 Plan B参照）|

#### W-022: 動的EV閾値の実装

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟢 完了（2026-05-19） |
| **実装内容** | `get_dynamic_ev_threshold(conn, lookback_days=28)` を `src/ml/bet_generator.py` に実装<br>直近28日の prediction_results ROI を計算し自動で閾値を選択:<br>ROI≥150% → 1.1(好調期) / ROI 110-150% → 1.2(通常期) / ROI 80-110% → 1.3(低調期) / ROI<80% → 1.5(不調期)<br>Kelly資金管理: `calc_qf_kelly_bet(ev_score, win_odds, bankroll)` で 1/4 Kelly ベット額を算出<br>Discord通知: `_get_threshold_and_bankroll()` でDB接続し動的取得。ヘッダーにモード・ROI・総資金を表示。★QF推奨セクションに推奨ベット額・Kelly%・総資金比を表示 |
| **実機検証** | 直近28日 ROI=62.2% → 不調期 → 閾値1.5 自動適用。Kelly算出正常確認（EV=1.5/odds=5.0 → 1/4 Kelly=3.12% → ¥300/点） |
| **影響ファイル** | `src/ml/bet_generator.py` (2関数追加) / `scripts/notify_discord.py` (全面改修) |

#### W-023: 破産確率 UI の未実装

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **影響** | Kelly バンクロールの破産リスクが可視化されていない |
| **対応方針** | Monte Carlo シミュレーション（CLAUDE.md §4-B）|

---

## カテゴリ 5: UI/UX 弱点

#### W-024: 的中率低下のリアルタイムアラート未実装

| 項目 | 内容 |
|------|------|
| **ステータス** | 🔴 未着手 |
| **影響** | ROI が急落しても Discord で自動アラートが来ない。目視での発見に依存 |
| **対応方針** | 週次バッチで ROI 計算 → 直近4週 ROI < 90% で Discord アラート |

#### W-025: Web ダッシュボードのオフライン耐性なし

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留 |
| **影響** | Next.js サーバーが落ちると UI が完全停止。Discord 通知のみ機能する |
| **対応方針** | 静的 JSON ファイルから直接表示するフォールバックページを追加（将来）|

#### W-028: Discord マルチチャンネル通知の統合管理（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-028 |
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-20） |
| **影響** | DiscordNotifier 直呼び出しが散在し、チャンネル管理・フォールバック制御が困難だった |
| **対応** | `NotificationRouter` 新設。5チャンネル（prediction/system/ev_alert/ab_test/note_draft）を EV 閾値・フォールバック付きで一元管理。全呼び出し元を Router 経由に統一 |
| **影響ファイル** | `src/notification/router.py`（新設）, `src/pipeline/prediction.py`, `scripts/scheduler.py`, `scripts/today_auto_runner.py`, `scripts/post_weekly_note_draft.py` |

---

#### W-029: DB クエリ性能 — 予想・評価クエリの複合インデックス未整備（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-029 |
| **優先度** | 中 |
| **ステータス** | 🟢 完了（2026-05-21） |
| **影響** | `predictions`/`race_results`/`training_times` の大量 JOIN クエリが full scan していた（EXPLAIN QUERY PLAN で SEARCH USING COVERING INDEX なし） |
| **対応** | migration #15 で 6 複合インデックス追加。`idx_pred_model_ev` (model_type, ev_score)・`idx_pred_race_model` (race_id, model_type)・`idx_tc_horse_date` (horse_id, date)・`idx_hc_horse_date` (horse_id, date)・`idx_rr_horse_race` (horse_id, race_id)・`idx_pr_pred_hit` (prediction_id, is_hit)。全クエリ SEARCH USING INDEX 確認済み |
| **影響ファイル** | `src/database/schema.py`, `src/database/init_db.py` |

---

#### W-030: EV 特化特徴量の本番統合（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-030 |
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-21） |
| **影響** | オッズから算出できる Shin 真確率・Harville 複勝確率・オッズ異常スコアが FEATURE_COLS に未統合で EV 算出精度に限界があった |
| **対応** | `src/ml/ev_features.py` で 7 特徴量エンジン実装（shin_prob / implied_prob_excess / harville_place_prob / odds_steam_flag / odds_reversal_score / field_strength_ev_adj / ev_rank_in_race）。`FeatureBuilder._add_ev_features()` で `build_race_features()` に統合。try/except ガード付き安全動作。EV 特徴量は FEATURE_COLS 外（買い目サイズ決定専用として機能）。全 69 FEATURE_COLS モデル再訓練完了: HonmeiModel CV AUC=**0.7677** / PlaceModel CV AUC=**0.7293** |
| **影響ファイル** | `src/ml/ev_features.py`（新設）, `src/ml/features.py`, `src/ml/models.py`（FEATURE_COLS 69 列）|

---

#### W-026: 増分学習 `_IsotonicModel.booster_` 属性エラー（→完了）

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟢 完了（2026-05-19） |
| **優先度** | 高（毎レース後に発生・モデル陳腐化リスク） |
| **影響** | W-004 実装時に Isotonic キャリブレーション層を HonmeiModel に導入したが、`incremental.py` の 194/217/218行目が `LGBMClassifier` 生メソッドに直接依存。毎レース後の増分学習が全件スキップされ、モデルが最新データを学習できなかった |
| **対応方針** | `_IsotonicModel`・`_PlattModel` に透過プロキシ3種を追加（`booster_` property / `_Booster` property+setter / `set_params()` メソッド） |
| **実装** | `src/ml/models.py` — 実機E2Eテスト（Booster取得・set_params・_Boosterセット）で全コードパス確認済み |

---

## 改善ロードマップ（優先順）

> **2026-05-21 更新: Week1-4 商用化フェーズ完了。本番稼働フェーズ移行。**

```
【完了済み（2026-05-21 本番確定）】
  W-004  大衆心理乖離スコア                    🟢 完了
  W-022  動的EV閾値 + Kelly資金管理            🟢 完了
  W-026  増分学習 _IsotonicModel 修正          🟢 完了
  W-028  Discord マルチチャンネル Router        🟢 完了
  W-029  DB 複合インデックス 6件               🟢 完了
  W-030  EV 特化特徴量統合 + 69列再訓練        🟢 完了
  E2E    本番シミュレーション 5.12秒 ALL PASS   🟢 完了

【本番稼働後・次フェーズ（JVLink SID 制約解消次第）】
  W-001  加速力スコア (上がり3F)               🔴 未着手
  W-002  PCI ペース変動指数                    🔴 未着手
  W-020  FukushoElite 本番統合                🔴 未着手
  W-023  破産確率 UI (Monte Carlo)             🔴 未着手

【中長期（歴史データ大規模取得後）】
  Phase 2-B (W-003/W-008/W-010: 不完全燃焼・馬場脚質・相手関係)
  W-015  ラップタイム DB 格納
  W-016  2025年着順データ補完（netkeiba 一括）
```

---

## チェックリスト（新規開発指示を受けた際に確認）

```
□ このファイルを開いて前回の弱点ステータスを確認したか？
□ 今回の作業で改善された弱点の ステータスを更新したか？
□ 新たに発見した弱点を追加したか？
□ 改善履歴を更新履歴テーブルに記載したか？
```
