# UMALOGI 機械学習ロードマップ

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-18 | 【U score 完全体ロードマップ追記（§6新設）】社長ビジョン「30因子完全体」への Phase 2-A〜C・Phase B のロードマップを §6 に追記。現在18因子(AUC 0.759)→目標30因子(AUC 0.80+)の具体的実装計画・弱点台帳(docs/7_weakness_ledger.md)との連携を明記。影響: docs/5_ml_roadmap.md |
| 2026-05-18 | 【X世論分析 Phase A 実装】src/scraper/x_scraper.py 新規作成（Playwright stealth-mode・RateLimiter・競馬関連フィルタ・x_signals保存）。scripts/x_targets.json アカウントマスタ新規作成。src/database/schema.py に x_accounts/x_signals テーブル DDL 追加（インデックス4件含む）。DB テーブル作成確認済み。Phase B: x_signal_parser.py（Claude Haiku API で構造化）は平日実装予定。Phase C: FEATURE_COLS への x_consensus_score 統合はモデル再訓練とセット。※社長明示指令により週末凍結ルール例外適用。影響: src/scraper/x_scraper.py(新規), scripts/x_targets.json(新規), src/database/schema.py |
| 2026-05-17 | 【U score 統合モデル ドライラン再学習完了】scripts/dry_run_retrain.py 実行。6,135レース/47,199サンプルで HonmeiModel・ManjiModel を U score 27列込みの 80特徴量で再学習。HonmeiModel CV AUC=**0.7591**（従来比 +0.152: 旧0.607→新0.759、U score 18因子の予測力向上を確認）。ManjiModel 正常完了（回帰モデルのためAUCなし）。エラー0件。総処理時間 60分（_build_train_df が3回呼ばれる設計上の制約）。モデルバイナリ: data/models/honmei_model.pkl (v20260517_232119) / data/models/manji_model.pkl (v20260517_234054)。影響: data/models/honmei_model.pkl, data/models/manji_model.pkl, data/models/history/ |
| 2026-05-17 | 【U score Phase 1 実装 + BugFix 2件】src/ml/u_score.py 新規作成。18因子（A:能力6/B:人的4/C:コース3/D:調教2/E:血統3）を DB バッチ SQL で算出し u_score 合成スコア（0〜1）を FEATURE_COLS に追加（計26列追加）。features.py に _add_u_score() 統合。①features.py BugFix: build_race_features_for_simulate/build_race_features の両関数で race_id 列未追加のため UScoreEngine が KeyError でスキップされていた（df["race_id"]=race_id を追加）。②u_score.py BugFix: _days_since_last_race_batch で horse_ids 用プレースホルダーを race_ids クエリに流用しバインディング不一致が発生（ph_race を別途算出）。Phase 2: X シグナルスコア・PCI加速力は CLAUDE.md §12 Phase C で実装予定。影響: src/ml/u_score.py(新規), src/ml/features.py, src/ml/models.py |
| 2026-05-15 | 厳密 Walk-Forward バックテスト実施 (scripts/run_strict_backtest.py 新規作成)。2024-2025 全モデル評価: ALPHA(複勝) EV≥1.3 が唯一の有効モデル（通算ROI=92.6%、2025H2窓ROI=102.3%）。本命/卍/PlaceModel は 2025年 race_results 着順欠損バイアスにより結果無効と判定。推奨EV閾値=1.3確定。影響: scripts/run_strict_backtest.py, data/strict_backtest_result.json |
| 2026-05-10 | 初版作成。現状スペック・再学習スケジュール・開発計画を記述 |

---

## 1. 現在のモデル構成（2026-05-10 時点）

| モデル | ファイル | 目的変数 | 真のAUC | 状態 |
|-------|---------|---------|--------|------|
| HonmeiModel | `data/models/HonmeiModel.pkl` | is_win (1着=1) | ~0.607 | 本番稼働中 |
| ManjiModel | `data/models/ManjiModel.pkl` | ev_target (払戻/賭金) | ~0.724 (複勝換算) | 本番稼働中 |
| AlphaPayoutModel | `data/models/AlphaPayoutModel.pkl` | win_payout (単勝払戻額) | — | 本番稼働中 |

**アルゴリズム**: 全モデル LightGBM ベース  
**特徴量数**: 37列 (`FEATURE_COLS` — `src/ml/models.py` L37-93)  
**学習データ**: `v_race_mart` ビュー経由 (全テーブル JOIN 済み)

---

## 2. 再学習スケジュール

### 2-1. 週次自動再学習（月曜 07:00）

`scripts/scheduler.py` — `job_weekly_retrain()` が自動実行:

```python
steps:
  1. HonmeiModel.train(conn)   # 全学習データで再訓練
  2. ManjiModel.train(conn)    # 同上
  3. AlphaPayoutModel.train()  # 同上
  4. Champion/Challenger 評価:
       ホールドアウト末尾20%の AUC を比較
       新モデル AUC > 旧モデル AUC → 保存・置き換え
       新モデル劣勢 → 破棄 (旧モデル継続)
```

### 2-2. 世代管理

- 保存先: `data/models/history/`
- 保持世代: 直近10世代
- ロールバック: `data/models/history/{model}_{timestamp}.pkl` を手動コピー

### 2-3. 再学習の学習データ構成

```
時系列分割 (_temporal_cv_split):
  fold 0: train=2024H1      val=2024H2
  fold 1: train=2024        val=2025H1
  fold 2: train=2024-2025H1 val=2025H2
  ...
  最終評価: 末尾20% ホールドアウト (Champion/Challenger)

注意: GroupKFold を廃止 → 時系列順分割で未来リークを完全防止
```

---

## 3. 現在の課題・制約

### 3-1. データ制約

| 項目 | 状況 |
|------|------|
| win_odds 歴史データ | 2024-01〜 のみ取得済み (JVLink SID 制約) |
| 調教タイム (WOOD) | 取得中 (fillna(-1) で欠損許容) |
| jockeys/trainers マスタ | 名前ベース LabelEncode で代用中 |
| 血統データ (BLOD) | sire_encoded のみ使用 |

### 3-2. JVLink SID 制約

JVLink は加入日以前の歴史データを取得できない。  
研究用には `scripts/scrape_research_data.py` で `netkeiba_research.db` を別途構築。

---

## 4. 開発計画

### Phase 4-A: 動的閾値（優先度: High）

**目標**: EV 閾値 1.0 固定をやめ、過去 N 週の実績に基づいて動的調整  
**実装イメージ**:
```python
threshold = 1.0 + 0.5 * (roi_30d < 0.9)   # ROI 低迷時に閾値引き上げ
```

**ファイル**: `src/ml/bet_generator.py` / `BetConfig`

---

### Phase 4-B: 破産確率 UI（優先度: High）

**目標**: Kelly バンクロールの破産確率をダッシュボードに表示  
**実装**: `scripts/monte_carlo_bankroll.py` の出力を `web/` に組込  
**計算**: モンテカルロ法 10,000 試行 / 損失確率を可視化

---

### Phase 4-C: FukushoElite モデル統合（優先度: Medium）

**背景**: `scripts/evaluate_elite_results.py` で運用中の複勝特化モデル  
**目標**: 本番 predictions テーブルへ統合 / Discord 通知セクション追加  
**現状**: 独立 CSV (`logs/fukusho_elite_monitor.csv`) で追跡中

---

### Phase 4-D: オッズ時系列特徴量の活性化（優先度: Medium）

**特徴量**:
- `odds_vs_morning`: 直前オッズ / 朝一オッズ（短縮=大口流入）
- `odds_velocity`: 直近1時間のオッズ下落速度

**現状**: 訓練データでは常に NaN → 実際の prerace データ蓄積後に再学習で有効化

---

### Phase 4-E: WIN5 モデル高度化（優先度: Low）

**現状**: model=None（等確率）+ market 50/50 ブレンド  
**目標**: 本命モデルのスコアを WIN5 エンジンに組込  
**課題**: WIN5 対象5レースが同会場とは限らない (3会場跨ぎ)

---

### Phase 4-F: 研究用 DB の歴史データ整備（優先度: Low）

**現状**: `netkeiba_research.db` に 2024-2025 を 99.1% スクレイプ済み  
**目標**: 本番 `umalogi.db` へマージして学習データ拡充

---

## 5. バックテスト基盤

| スクリプト | 説明 |
|-----------|------|
| `scripts/backtest_2024_2025.py` | 2024-2025 通年バックテスト |
| `scripts/walk_forward_backtest.py` | ウォークフォワード検証 |
| `scripts/simulate_year.py` | 年度別・会場別分解シミュレーション |
| `scripts/simulate_win_place.py` | 単複特化シミュレーション |

**評価指標**: ROI / 的中率 / シャープレシオ / 最大ドローダウン

---

## 6. U score 完全体ロードマップ（社長ビジョン 30 因子へ）

> **社長ビジョン**: 「1000以上の要素から厳選した30項目（加速力・PCI・不完全燃焼度など）、  
> AIチームの目視分析、大衆心理のジレンマを排除した真の期待値算出」  
> **現状**: Phase 1 として 18 因子実装・AUC 0.607 → **0.759** への向上を確認済み

### Phase 2-A: ラップ系因子（最優先）

**前提**: JVLink RA レコードから `last3f_time` / `lap_data` を `races` テーブルに格納

| 因子 | 概要 | EV への寄与 |
|------|------|-----------|
| **加速力スコア** | 上がり3F × 前半ペース → "末脚の切れ味"指数 | 直線末脚が生きるコースで大幅 EV 向上 |
| **PCI (Pace Change Index)** | (後半3F×2)/全体タイム × 脚質マッチング係数 | 脚質とレースペースの相性を EV に反映 |
| **斤量インパクト** | 前走比斤量変化 + 斤量耐性（過去実績）| 斤量減の馬を早期発見 |
| **クラス昇降格** | 前走クラス差分スコア | 降格馬の凡庸人気 → 隠れ期待値 |

**実装ファイル**: `src/ml/u_score.py` (`_calc_ability` 拡張) / `src/database/schema.py` (カラム追加)

---

### Phase 2-B: 心理・相性因子

| 因子 | 概要 | EV への寄与 |
|------|------|-----------|
| **不完全燃焼度** | 前走悪条件（不良馬場・大外枠・ハイペース）からの解放スコア | "今回こそ"の隠れた期待値を発掘 |
| **大衆心理乖離スコア** | `model_prob / market_implied_prob` の比率 | 市場過小評価馬への EV ブースト |
| **馬場バイアス × 脚質** | コース別馬場状態 × 脚質マッチングマトリクス | 不良馬場の先行有利を EV に反映 |
| **相手関係指数** | 出走メンバーの平均 u_score vs 対象馬 u_score | 強いメンバーでの高着順を過去より高評価 |

**実装ファイル**: `src/ml/u_score.py` (新グループ F: 心理・相性)

---

### Phase 2-C: 動向・環境因子

| 因子 | 概要 | EV への寄与 |
|------|------|-----------|
| **オッズ動向 (Smart Money)** | 朝一→直前オッズ変化率・速度 | 大口資金の流入方向を EV に加算 |
| **輸送疲れ係数** | 前走会場→今走会場の距離 + 克服実績 | 長距離輸送馬の過大評価を EV 修正 |

**実装ファイル**: `src/ml/u_score.py` + `realtime_odds` 朝一保存スケジュール追加

---

### Phase B (X シグナル連携): 目視 AI 分析因子

| 因子 | 概要 | EV への寄与 |
|------|------|-----------|
| **X コンセンサス係数** | 凄腕予想家の印（◎〇▲）を Claude Haiku で構造化 | "AIチーム目視分析"を定量化して EV に加算 |
| **非公開情報スコア** | 厩舎コメント・前日オッズ異変 の構造化 | 非公開シグナルの EV 貢献度を測定 |

**実装ファイル**: `src/ml/x_signal_parser.py`（Phase B）

---

### 30 因子達成タイムライン

```
現在: 18 因子 (AUC 0.759)
      ↓ Phase 2-A (4因子追加)
22 因子: 加速力・PCI・斤量・クラス変化
      ↓ Phase 2-B (4因子追加)
26 因子: 不完全燃焼・大衆心理・馬場脚質・相手関係
      ↓ Phase 2-C + Phase B (4因子追加)
30 因子: オッズ動向・輸送疲れ・Xコンセンサス・非公開情報
      ↓ モデル再訓練
目標AUC 0.80+ / ROI 300%+ (単勝換算)
```

**詳細な弱点管理と実装優先順位**: `docs/7_weakness_ledger.md` を参照
