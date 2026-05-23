# UMALOGI 予想ロジック設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-10 | 初版作成。ALPHA/卍/本命 3モデル並列稼働・三連系生成ロジックを記述 |
| 2026-05-10 | 将来設計案「予測不変性（Prediction Immutability）」を追記 |
| 2026-05-19 | 動的EV閾値（W-022完全対応）実装: `get_dynamic_ev_threshold()` を bet_generator.py に追加。直近28日ROIから自動で1.1/1.2/1.3/1.5を選択（好調/通常/低調/不調）。Kelly資金管理: `calc_qf_kelly_bet()` を追加し、notify_discord.py のQF推奨セクションに推奨ベット額・Kelly%・総資金比を表示。影響ファイル: src/ml/bet_generator.py / scripts/notify_discord.py |
| 2026-05-23 | 【Discord通知完全リアル化】`DISCORD_WEBHOOK_HIT_FLASH` 環境変数を追加し的中速報を専用チャンネルへ分離。直前予想通知に購入単価×点数表示（`¥100×N点=¥XXX`）を追加。`_format_combo_card()` を馬番全表示版に刷新（省略撤廃・軸推奨スマート表記 `【推奨: 三連複流し 軸X - 相手A,B,C】`）。影響: `src/notification/discord_notifier.py` |
| 2026-05-20 | EV>=1.5 の激熱レースを DISCORD_WEBHOOK_EV_ALERT チャンネルへ自動追加送信。NotificationRouter 導入（マルチWebhook 5チャンネル対応）。買い方テンプレート自動送信 (_format_buying_guide)。影響: src/notification/router.py, src/pipeline/prediction.py |
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
