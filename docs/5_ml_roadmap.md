# UMALOGI 機械学習ロードマップ

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
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
