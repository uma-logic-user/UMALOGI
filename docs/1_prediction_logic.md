# UMALOGI 予想ロジック設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-31 | 【推論精度強化 ステップ2-1/2-2（オーナー承認・条項2バイパス）】(2-1)特徴量の直前オーバーライド: `prerace_pipeline()` Step1c の `cached_odds==0` ガードを **直前モードでは解除**し毎回 `fetch_and_save_odds()` を実行、JRA-VAN速報の最新オッズ・馬体重・天候を推論直前に強制反映（馬体重/天候は entries/races を COALESCE/CASE で非破壊上書き）。(2-2)大口・オッズ歪み検知→危険馬フィルタ: 新規 `src/ml/odds_drift.py` が `realtime_odds` 朝→直前変動率を**レース中央値相対**で評価し plunge(大口流入)/abandoned(危険馬)を検知。`prerace_pipeline()` Step4c で危険馬を軸に含む買い目EVを `DANGER_EV_FACTOR=0.5` 減衰しEV<1.0を除外（本命/卍/Oracle/HitFocus/Alpha全モデル・保存前・暫定対象外）。閾値 `ABANDON=0.40`/`PLUNGE=0.25`/`_MIN_FIELD=4`。ライブ実証(東京2回12日8R): 系統+99%シフト下で誤検知12頭→意味のある2頭(11番2.3→1.3倍=大口/10番54→156倍=見限り)。テスト `tests/test_odds_drift.py` 6件＋全824 PASS。影響: `src/ml/odds_drift.py`(新規), `src/pipeline/prediction.py` |
| 2026-05-31 | 【Oracle/HitFocus 予想モデル復活（オーナー特別承認・条項2週末凍結バイパス）】2026-05-23 コミット9c53540bで配線除去されていた Oracle（VirtualOracleStrategy・的中確率最大の三連複/三連単）と HitFocus（HitFocusStrategy・2軸マルチフォーメーション・均等100円/Kelly不使用）を予想パイプライン・Discord通知・note記事採点・UIに再結線。ストラテジークラス本体は削除されておらず純粋に配線のみ復元。`_save_predictions()` に oracle_bets/hit_focus_bets 引数とOracle/HitFocus保存ブロックを復活、`prerace_pipeline` で generate_oracle/generate_hit_focus を呼び出し。note_generatorを4モデル（本命・卍・Oracle・ALPHA）合意採点へ復元。V2 suffix も自動適用（OracleV2/HitFocusV2(直前)）。隔離DB検証で本命/Oracle/HitFocus(直前)＋V2の保存を確認。卍は W-048（confidence=1.0固定・実現ROI26.9%）未解消のため `DISABLE_MANJI_BETS=1` を据え置き（オーナー判断）。テスト全761 PASS（既存失敗5件はFmtCombo/scraping mockで本変更と無関係）。影響: `src/pipeline/prediction.py`, `src/notification/discord_notifier.py`, `src/notification/router.py`, `src/ops/note_generator.py`, `web/src/components/HitHistory.tsx`, `web/src/components/DrillDownAnalytics.tsx` |
| 2026-05-31 | 【W-049 単勝EV抽出＋ワイド多点絞り込み追加（オーナー特別承認・IPAT自動投票は実装せず）】上記フィルタの残課題2点を実装。①単勝EVゲート: `TANSHO_EV_MIN=1.2` 未満の単勝を `_apply_odds_band_filter()` で除外（卍の無条件単勝でもEV<1.2なら除外）。②ワイド多点制限: `_limit_wide_points()` を新設しワイドをEV高い順（combinationsはスコア降順構築）に最大 `WIDE_MAX_POINTS=3` 点へ切り詰め、horse_names(1組2頭)も同期。E2E: 実レース202605021201で完走・卍単勝EV0.88除外・ワイド5点→3点を確認。テスト新規7件（計24件・bet系63件PASS）。影響: `src/ml/bet_generator.py`, `tests/test_bet_precision_filters.py` |
| 2026-05-31 | 【買い目精度向上フィルタ追加（オーナー特別承認スコープA・IPAT自動投票は実装せず手動投票維持）】期待値ロジックにノイズ帯の足切りを3点追加。①#2 単勝オッズ帯フィルタ: `TANSHO_ODDS_FLOOR=1.5`（以下）/`TANSHO_ODDS_CEIL=100.0`（以上）の単勝買い目を除外（過剰人気・大穴ノイズ）。狙うボリュームゾーンは5.0〜30.0倍。②#3 レース選定: `should_skip_race_for_betting()` を新設し新馬戦（「新馬」「メイクデビュー」）・障害戦（「障」「ジャンプ」）を `prerace_pipeline()` Step0bで出馬表/オッズ取得前に見送り（notify_skip→skipped返却）。③#4 ワイド専用EVゲート: ワイド買い目を `WIDE_EV_MIN=1.2`（AIワイド的中率×オッズ）未満で除外。フィルタは `BetGenerator._apply_odds_band_filter()` に集約し V1/V2 全 generate_*（honmei/manji/alpha_trifecta/hybrid）に配線。オッズ未取得時は単勝足切りをスキップ（買い目を消さない）。未実装メモ: 単勝のEV≥1.2抽出とワイド多点絞り込み(1〜3点)はスコープ外（台帳W-049）。テスト: 新規19件＋既存betテスト全PASS（計65件）。影響: `src/ml/bet_generator.py`, `src/pipeline/prediction.py`, `tests/test_bet_precision_filters.py`（新規） |
| 2026-05-24 | 【umasugi_engine Phase3 フル統合完了】3因子追加: ①オッズスパイク検知（_WINDOW=10/15%急落→+0.10/25%→+0.20）を odds_momentum.py に統合。②Discord /paddock コマンドでパドックコメントをDBへ保存・キーワード解析（±5%ブースト）・scorer.py 3%組み込み。③騎手・調教師コース別成績（jockey_stats/trainer_stats/3年バックフィル 1,048+1,817件）をscorerに3%/2%組み込み。ウェイト再編: legacy 0.57→0.50/training_grade 0.08→0.07/+paddock 0.03/+jockey_course 0.03/+trainer_course 0.02。バックテスト ROI: 75.7%(全体)→閾値0.50で**81.1%達成（目標80%超）**。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/paddock.py` `src/umasugi_engine/factors/jockey_trainer.py` `src/umasugi_engine/factors/odds_momentum.py` `src/database/schema.py` `src/database/init_db.py` `src/notification/discord_bot.py` `scripts/build_jockey_trainer_stats.py` |
| 2026-05-24 | 【的中報告レポートパイプライン追加】`src/ops/win_report.py` 新設。的中確認後に `data/results/YYYYMMDD/{race_id}_win_report.txt` 生成・Discord 予想ch へ Embed + X投稿テキスト送信・note.com 下書き保存を自動実行。`scripts/fetch_race_result.py` に `_try_publish_win_report()` を追加し `fetch_single_race()`・`fetch_for_date()` から呼び出す。影響ファイル: `src/ops/win_report.py`（新規）, `scripts/fetch_race_result.py` |
| 2026-05-24 | 【umasugi_engine Phase2完了】調教グレード (S〜E, 8%) + オッズモメンタム (5%) を追加。正規化JOINキー(horse_id[:4]+horse_id[4:9])によりtraining_times結合率45.6%達成。ウェイト再編: legacy 0.65→0.57 / turf 0.15 / track 0.10 / grade 0.08 / momentum 0.05 / crowd 0.05。`odds_timeseries` テーブル新設・毎分記録ジョブをschedulerに統合。バックテスト: ROI73.7%(閾値0.50/30日)。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/training_grade.py` `src/umasugi_engine/factors/odds_momentum.py` `scripts/record_odds_timeseries.py` `scripts/compute_training_grades.py` |
| 2026-05-24 | 【umasugi_engine Phase1・API・バックテスト完了】`src/umasugi_engine/` に小回り/洋芝/世論分析フィルターを実装。世論分析は legacy の正の相関を逆転させた負の相関（EV × (1−penalty)）で実装。バックテスト結果: Legacy 68.2% → Umasugi 73.6% (閾値0.50/30日)。最適ウェイト確定（turf=0.15/track=0.10）。`/api/compare/[race_id]` エンドポイント新設。影響: `src/umasugi_engine/*` `web/src/app/api/compare/[race_id]/route.ts` `scripts/backtest_umasugi.py` |
| 2026-05-24 | 【AIウマスギフィルター全出力先統合】`prerace_pipeline()` のStep4後に `[AIウマスギ] ROIフィルター適用完了` ログを追加。Discord通知に「🤖 AIウマスギフィルター適用済み」記述 + 本命三連単に「⚡条件付」バッジ。Discord: `notify_prerace_result()` の description/footer 更新。影響: `src/pipeline/prediction.py`, `src/notification/discord_notifier.py` |
| 2026-05-24 | 【ROIフィルター条件付き部分開放】本命モデルの三連単を「完全除外」→「個別EV≥1.5 の場合のみ許可」に変更。`_HONMEI_SANRENTAN_EV_MIN=1.5` 定数を追加。`_apply_roi_filter()` 内で `is_honmei + bet_type=="三連単"` の場合は `_ALLOWED_BET_TYPES` を経由せず EV 値で個別判定。許可時は「条件付き許可」ログを出力。根拠: 5月本番データで本命三連単 ROI272.7%（損失原因は廃止済み Oracle/HitFocus）。影響: `src/ml/bet_generator.py` |
| 2026-05-24 | 【ROIフィルター + 動的Kelly実装】本番実績ROI（2026-04〜05データ）分析結果を反映。`_ALLOWED_BET_TYPES` で本命モデルの馬単/馬連/ワイドを除外、単勝/複勝のみ許可（三連単は個別EV判定）。`get_dynamic_kelly_fraction()` 新設（ROI300%超=1/4Kelly、ROI200-300%=1/5Kelly、ROI100-200%=1/10Kelly、ROI<100%=購入禁止）。`get_model_bet_roi()` でDB照会ベースの ROI 算出関数追加。`BetGenerator._apply_roi_filter()` が `generate_honmei()` `generate_alpha_trifecta()` を自動フィルタリング。影響: `src/ml/bet_generator.py` |
| 2026-05-28 | 【卍モデル三連複EVゲート復活】`ManjiStrategy.generate()` の三連複生成ループに `_TRIO_EV_MIN=1.0` ゲートを追加。本番実績 ROI=46.7% の損失を招いた「確率至上主義・EVゲート撤廃」方針を廃止し、合成EV<1.0の組み合わせを除外する。影響ファイル: `src/ml/bet_generator.py` |
| 2026-05-10 | 初版作成。ALPHA/卍/本命 3モデル並列稼働・三連系生成ロジックを記述 |
| 2026-05-10 | 将来設計案「予測不変性（Prediction Immutability）」を追記 |
| 2026-05-19 | 動的EV閾値（W-022完全対応）実装: `get_dynamic_ev_threshold()` を bet_generator.py に追加。直近28日ROIから自動で1.1/1.2/1.3/1.5を選択（好調/通常/低調/不調）。Kelly資金管理: `calc_qf_kelly_bet()` を追加し、notify_discord.py のQF推奨セクションに推奨ベット額・Kelly%・総資金比を表示。影響ファイル: src/ml/bet_generator.py / scripts/notify_discord.py |
| 2026-05-23 | 【Discord通知完全リアル化】`DISCORD_WEBHOOK_HIT_FLASH` 環境変数を追加し的中速報を専用チャンネルへ分離。直前予想通知に購入単価×点数表示（`¥100×N点=¥XXX`）を追加。`_format_combo_card()` を馬番全表示版に刷新（省略撤廃・軸推奨スマート表記 `【推奨: 三連複流し 軸X - 相手A,B,C】`）。影響: `src/notification/discord_notifier.py` |
| 2026-05-20 | EV>=1.5 の激熱レースを DISCORD_WEBHOOK_EV_ALERT チャンネルへ自動追加送信。NotificationRouter 導入（マルチWebhook 5チャンネル対応）。買い方テンプレート自動送信 (_format_buying_guide)。影響: src/notification/router.py, src/pipeline/prediction.py |
| 2026-05-24 | 【バリデーター自動統合・BANKROLL_OVERRIDE新設】`prerace_pipeline()` の Step 2c として `data_validator.filter_sentinel_horses()` を自動統合（FeatureBuilder直後・モデル推論前）。`get_current_bankroll()` に `BANKROLL_OVERRIDE` 環境変数サポート追加（P&L累積を無視してバンクロールを直接指定、Kelly新規開始時に使用）。`BANKROLL_RESET_DATE` で移行日以降のP&Lのみ集計可能。影響: `src/pipeline/prediction.py` `src/ml/bet_generator.py` `.env.example` |
| 2026-05-24 | 【Kelly資金管理統合 + データバリデーター新設】`_KELLY_TYPE_CAPS` 辞書（券種別バンクロール上限: 複勝3%/馬連1.5%/三連複1%等）を新設。`calc_kelly_stake(bankroll, ev, win_odds, bet_type, n_combos)` 公開関数を追加（formula: f*=(EV-1)/(odds-1), stake=bankroll×min(f*,cap)×0.25）。ManjiGenerator・HonmeiStrategy・AlphaTrifectaStrategy の `recommended_bet` を全面Kelly化（複勝/馬連/ワイド/馬単/三連複）。`src/ml/data_validator.py` 新設: `validate_race_df()/filter_sentinel_horses()/validate_horse_for_axis()` でセンチネルオッズ(≥500)馬を排除。UIに Kelly理論 vs ¥100固定 比較パネル追加（WF 2025年実証ROI対比グラフ）。影響: `src/ml/bet_generator.py` `src/ml/data_validator.py` `web/src/components/FinancialDashboard.tsx` |
| 2026-05-24 | NOTE記事生成に有料仕切りロジック追加: `_build_paywall_separator()` を新設し、1レース目（無料）終了後に `🔒 ここから先は有料エリアです` ブロックを自動挿入。`generate()` の rank==1 後に挿入。影響: `src/ops/note_generator.py` |
| 2026-05-23 | Walk-Forward Backtest 2024-2025 実施: Train=2024-01〜05月 / Test=2025年全12ヶ月。5券種×EV閾値5段階=25パターン成績マトリクス生成（CV AUC=0.7494）。全パターンROI<80%（最高79.3%=単勝EV≥1.5）でランダム水準と判明。推奨ポートフォリオ方針（見送り・改善ロードマップ）を `.claudecode/rules/portfolio_strategy_2024_2025.md` に保存。影響ファイル: src/analysis/walk_forward_backtest_2024_2025.py |

---

## 1. システム概観

UMALOGI は **3モデル完全独立並列稼働** 方式を採用する。
各モデルは異なる目的変数・戦略で動作し、互いの出力を参照しない。

```
レース直前 (発走20分前)
    │
    ├── 🟦 Alpha-Payout モデル  → 複勝 + 三連複/三連単 (EV特化)
    ├── 🟩 卍 モデル           → 三連複/三連単       (回収率特化)
    ├── 🟥 本命 モデル         → 単勝/複勝/馬連      (勝率特化)
    ├── 🟨 Oracle 予想         → 三連系              (参考・非推奨買い目)
    └── 🔶 HitFocus 予想       → 馬連/馬単/三連単    (的中率特化)
```

---

## 2. モデル詳細

### 2-1. 🟥 本命モデル (HonmeiModel)

**ファイル**: `src/ml/models.py` — `HonmeiModel`  
**目的変数**: `is_win` (1着 = 1, 他 = 0)  
**アルゴリズム**: LightGBM (binary classification) + Isotonic Calibration + Platt Scaling  
**キャリブレーション**: OOF (Out-Of-Fold) Isotonic → 予測確率を信頼できる勝率に変換  
**Champion/Challenger**: 末尾20%ホールドアウトで旧モデルとAUCを比較し、上回った場合のみ更新  
**真のAUC**: 約 0.607 (is_win)  

**特徴量** (`FEATURE_COLS`、`src/ml/models.py` L37-93):

| カテゴリ | 特徴量名 | 説明 |
|---------|---------|------|
| 馬能力 | `weight_carried`, `horse_weight`, `horse_weight_diff` | 斤量・馬体重・前走差 |
| 勝率系 | `win_rate_all`, `win_rate_surface`, `win_rate_distance_band` | 通算・馬場別・距離帯別 |
| 直近成績 | `recent_rank_mean` | 直近5走平均着順 |
| カテゴリ | `surface_code`, `sex_code`, `venue_encoded`, `sire_encoded` | 馬場/性別/会場/父馬 |
| レース | `distance`, `gate_number`, `condition_code`, `race_number` | 距離・枠番・馬場状態 |
| 人的要素 | `jockey_code_encoded`, `trainer_code_encoded` | 騎手・調教師エンコード |
| 調教(ウッド) | `tc_4f`, `tc_lap`, `tc_accel_flag`, `tc_4f_diff` | 4Fタイム・ラスト・加速フラグ |
| 調教(坂路) | `hc_4f`, `hc_lap`, `hc_accel_flag`, `hc_4f_diff` | 坂路4Fタイム等 |
| レース内相対 | `*_rank`, `*_zscore` | レース内ランク・偏差値 |
| 当日バイアス | `today_inner_bias`, `today_front_bias`, `today_gate_match` | 当日馬場傾向 |
| オッズ時系列 | `odds_vs_morning`, `odds_velocity` | 朝一比・下落速度 |

**買い目生成** (`HonmeiStrategy`):
- Kelly 基準で推奨投資額を算出（動的バンクロール方式）
- 単勝・複勝・馬連・馬単・三連系を EV > 1.0 でフィルタ

---

### 2-2. 🟩 卍モデル (ManjiModel)

**ファイル**: `src/ml/models.py` — `ManjiModel`  
**目的変数**: `ev_target` = 払戻金 / 馬券代 (回収率直接最適化)  
**アルゴリズム**: LightGBM (regression) + Platt Scaling  
**真のAUC**: 約 0.724 (複勝目的変数)  
**特徴量**: HonmeiModel と同一 FEATURE_COLS を使用  

**買い目生成** (`ManjiStrategy`):
- EV スコア上位馬を軸に三連複・三連単を組む
- Harville 法: `P(A 1着 B 2着 C 3着) = P(A) × P(B)/(1-P(A)) × P(C)/(1-P(A)-P(B))`
- 合成EV = Harville確率 × 推定払戻 / 100

---

### 2-3. 🟦 Alpha-Payout モデル (AlphaPayoutModel)

**ファイル**: `src/ml/alpha_payout_model.py` — `AlphaPayoutModel`  
**目的変数**: `win_payout` (単勝払戻金、実際に得た円額)  
**アルゴリズム**: LightGBM (regression on payout × probability)  
**期待値計算**: `EV = P(win) × estimated_payout / 100` — 市場オッズから払戻推定  

**買い目生成** (`AlphaTrifectaStrategy` — `src/ml/bet_generator.py` L1564):
1. alpha_ev スコア上位5頭を選択
2. **三連複**: 軸馬(1着候補) × 相手上位組み合わせ (最大6点)
3. **三連単**: 軸 → 相手2頭の順列 (最大6点)
4. **複勝**: EV > 1.0 の上位馬 (最大3点)

---

### 2-4. 🟨 Oracle 予想

**実装**: `VirtualOracleStrategy` — 本命モデルのスコアで三連系を生成  
**用途**: 参考表示のみ。Kelly 推奨買い目には含まれない  
**model_type**: "Oracle"

---

### 2-5. 🔶 HitFocus 予想

**実装**: `HitFocusStrategy` — 的中率特化（EV より的中優先）  
**買い目**: 馬単・馬連・三連単  
**特徴**: 上位2頭固定で組み合わせ数を絞る

---

## 3. 予想パイプライン (`src/pipeline/prediction.py`)

```
prerace_pipeline(race_id)
  ├── Step 0: 締め切りチェック (直前モードのみ)
  ├── Step 1: entries キャッシュ確認
  │     └── entries = 0 → netkeiba フォールバック (fetch_entry_table)
  ├── Step 1c: オッズ取得
  │     ├── JVLink realtime_odds
  │     ├── → 空なら netkeiba fetch_odds_from_netkeiba()
  │     └── → fallback: 単勝オッズ推定
  ├── Step 2: 特徴量生成 (FeatureBuilder.build_race_features)
  ├── Step 2b: データ品質チェック (直前のみ)
  │     └── 欠損率 > 閾値 → provisional=True に降格
  ├── Step 3: モデル予測 (HonmeiModel + ManjiModel)
  ├── Step 4: 買い目生成 (BetGenerator.generate / generate_manji)
  │     └── 動的バンクロール × Kelly 計算
  ├── Step 4b: Alpha-Payout 三連系生成 (直前のみ)
  │     └── AlphaTrifectaStrategy.generate()
  ├── Step 5: DB 保存 (predictions テーブル)
  ├── Step 5c: WIN5 生成 (直前のみ・当日最終5レース)
  ├── Step 6: JSON ファイル出力 (data/predictions/{race_id}.json)
  └── Step 7: Discord 通知 (直前のみ)
        └── 3セクション Embed: 🟦 ALPHA / 🟩 卍 / 🟥 本命
```

---

## 4. WIN5 エンジン (`src/ml/win5.py`)

**EV 計算式** (C-01バグ修正済み 2026-05-10):
```
market_prob  = 各レースの (1/win_odds) 正規化積
model_prob   = blend_prob 積 (50% model + 50% market)
estimated_payout = (1 / market_prob) × 0.725 × 100  ← market_prob を使用
EV           = model_prob × estimated_payout / 100
             = (model_prob / market_prob) × 0.725
```

model_prob > market_prob のとき EV > 0.725 でエッジあり。  
旧バグ (model_prob で payout 計算) では EV = 0.725 固定の恒等式になっていた。

---

## 5. 期待値・Kelly 基準

```python
EV = モデル確率 × 推定払戻 / 100
推奨投資 = Kelly * bankroll  (Kelly = (EV - 1) / (オッズ - 1))
```

**判断基準**:
- EV >= 1.0 → 買い目に含める
- EV >= 1.5 → Discord で 🔥 アイコン表示
- EV >= 3.0 → Discord で ⚡ アイコン表示 (JACKPOT カラー)

---

## 6. モデルファイル管理

- 保存先: `data/models/{HonmeiModel,ManjiModel,AlphaPayoutModel}.pkl`
- 世代管理: `data/models/history/` (直近10世代保持)
- Champion/Challenger: AUC が旧モデルを上回った場合のみ置き換え

---

## 7. 将来設計案: 予測不変性（Prediction Immutability）

> **ステータス: 保留 (2026-05-10 時点)。社長承認待ち。コード変更なし。**

### 7-1. 問題の定義

現在の実装では、`predictions` テーブルに保存済みの予想データが後から上書きされる可能性がある。  
具体的には `main_pipeline prerace <race_id>` を複数回実行すると、`combination_json` / `expected_value` / `recommended_bet` が再計算結果で置き換わる。

これにより、Discord に送信した予想と DB の記録が乖離し、事後の回収率・EV 検証が不正確になるリスクがある。

### 7-2. 設計方針（案）

```
原則: prerace 通知後の predictions レコードは UPDATE 不可。再実行時は INSERT IGNORE。
```

実装イメージ:
```python
# predictions テーブルに "locked" フラグを追加
ALTER TABLE predictions ADD COLUMN locked INTEGER NOT NULL DEFAULT 0;

# Discord 通知後に locked = 1 にセット
UPDATE predictions SET locked = 1 WHERE race_id = ? AND model_type = ?;

# 再実行時: locked = 1 のレコードはスキップ
INSERT OR IGNORE INTO predictions (...) VALUES (...);  # UNIQUE制約で自然に弾く
```

または、既存レコードが存在する場合は INSERT を行わず警告を出す保守的な方針でもよい。

### 7-3. 実装コスト試算

| 作業 | コスト |
|------|--------|
| DDL: `locked` カラム追加 + マイグレーション | 小 |
| `bet_generator.py`: INSERT 前に locked チェック | 小 |
| `notify_prerace_result()`: 送信後に locked=1 UPDATE | 小 |
| バックテスト系スクリプトへの影響確認 | 中 |

### 7-4. 注意点

- `self_healing_monitor.py` が predictions = 0 を検出して再生成を試みる場合、locked チェックと競合する可能性がある。修復対象は locked=0 のレコードのみに制限する必要がある。
- バックテスト・simulate_year.py 系は DB を直接書き込む場合があり、locked 制約の例外処理が必要になる。
