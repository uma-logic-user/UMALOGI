# 2年間全モデル横断バックテスト機能 — 設計仕様

**作成日**: 2026-05-28  
**ステータス**: 承認済み  
**対象ブランチ**: feature/claude-design-migration

---

## 概要

UMALOGI の全4モデル（本命・卍・複勝・ALPHA）を対象に、2024年1月〜2025年12月の
2年間データを使って時系列分割バックテストを実行し、モデル横断でパフォーマンスを比較する
新規スクリプト `scripts/backtest_all_models.py` を追加する。

既存スクリプト（`scripts/simulate_year.py` など）への変更は一切行わない。

---

## 設計方針

### バックテスト方式: 時系列分割（Train/Test Split）

| フェーズ | 期間 | 用途 |
|---------|------|------|
| Train | 2024-01-01 〜 2024-12-31 | 全4モデルの再訓練 |
| Test  | 2025-01-01 〜 2025-12-31 | アウトオブサンプル評価（メイン指標） |

- **リーク完全防止**: Testフェーズでは2024以前のデータのみで訓練したモデルを使用
- **本番モデル保護**: 再訓練モデルは `data/models/backtest_tmp/` に一時書き出し。`data/models/`（本番）は無変更

---

## ファイル構成

```
scripts/
  backtest_all_models.py     ← 新規作成（本仕様の実装対象）

data/models/
  backtest_tmp/              ← 一時モデル保存ディレクトリ（実行時に自動作成）
    honmei/
    manji/
    place/
    alpha/
```

既存ファイルへの変更: **なし**

---

## 処理フロー

ALPHAモデルは独自の `run_backtest()` 関数でデータロード・訓練・評価を完結させる
設計になっているため、本命/卍/複勝の3モデルとは統合経路が異なる。

```
Phase 1: 検証前チェック
  - DB接続確認
  - 2024/2025データの件数確認（レース数・race_results・race_payouts）
  - データ不足の場合は早期終了してメッセージを表示

── 本命 / 卍 / 複勝 の3モデル ──────────────────────────────

Phase 2: Trainデータ準備（2024年）
  - FeatureBuilder.build_race_features_for_simulate() を全2024レースに実行
  - 特徴量DataFrameを結合してTrain用マスタDFを作成
  - 目的変数（is_win / ev_score / is_place）をアタッチ

Phase 3: 3モデル再訓練（本番モデルを上書きしない）
  - HonmeiModel: train(train_df) → data/models/backtest_tmp/honmei/
  - ManjiModel:  train(train_df) → data/models/backtest_tmp/manji/
  - PlaceModel:  train(train_df) → data/models/backtest_tmp/place/

Phase 4: Testフェーズ（2025年レース単位ループ）
  for race in 2025年全レース:
    df = FeatureBuilder.build_race_features_for_simulate(race_id)
    if df.empty: continue
    for strategy in [honmei_*, manji_*, place_*]:
      picks = strategy.select(df, trained_model)
      hit   = evaluator._is_hit(bet_type, picks, result_map)
      pay   = evaluator._lookup_payout(bet_type, comb_key, payouts)
      stats[strategy].add(hit, pay)

── ALPHAモデル ──────────────────────────────────────────────

Phase 3': ALPHAバックテスト（自己完結型）
  - alpha_model.run_backtest(conn, train_years=[2024], test_years=[2025], bet_type="単勝")
  - alpha_model.run_backtest(conn, train_years=[2024], test_years=[2025], bet_type="複勝")
  ※ ALPHAは run_backtest() 内でデータロード・訓練・評価・Kelly計算を完結
  ※ 追加特徴量（log_win_odds, nb_win_odds等）もALPHA内部で処理

Phase 5: 結果集計・統合表示
  - 本命/卍/複勝の StrategyStats + ALPHAの AlphaBacktestResult を統一フォーマットで出力
  - 全体サマリー（9戦略横断）
  - 月別ROI推移（本命/卍/複勝のみ集計可能）
  - 会場別・距離帯別内訳（本命/卍/複勝のみ）
  - --csv フラグ時は results/backtest_YYYYMMDD.csv に書き出し
```

---

## 比較対象戦略（全9戦略）

| 戦略ID | モデル | 券種 | フィルタ条件 | 備考 |
|--------|--------|------|------------|------|
| `honmei_tansho` | 本命 | 単勝 | スコアTop1 | |
| `honmei_umaren` | 本命 | 馬連 | スコアTop2 | |
| `honmei_sanrenpuku` | 本命 | 三連複 | スコアTop3 | |
| `manji_tansho` | 卍 | 単勝 | EV > 1.0 | 購入なし時はskip |
| `manji_fukusho` | 卍 | 複勝 | EV > 1.0 | 購入なし時はskip |
| `place_fukusho` | 複勝 | 複勝 | スコアTop1 | |
| `place_fukusho_top3` | 複勝 | 複勝 | スコアTop3（流し） | 3頭×100円/頭（1レース投資300円） |
| `alpha_tansho` | ALPHA | 単勝 | EV > 1.5 | 購入なし時はskip |
| `alpha_fukusho` | ALPHA | 複勝 | EV > 1.5 | 購入なし時はskip |

---

## 集計指標（StrategyStats）

各戦略について以下を集計:

| 指標 | 説明 |
|------|------|
| `races` | 買い目が発生したレース数 |
| `skipped` | EVフィルタで見送ったレース数 |
| `hits` | 的中数 |
| `hit_rate` | 的中率（%） |
| `invested` | 累計投資額（円） |
| `payout` | 累計回収額（円） |
| `roi` | 回収率（%）= payout / invested × 100 |
| `profit` | 純利益（円）= payout - invested |

Kelly基準シミュレーションは simulate_year.py と同じ `StrategyStats` クラスを
**再実装せず直接 import** して流用する。

---

## 出力フォーマット

### コンソール（必須）

```
================================================================
  UMALOGI AI  --  2-Year Backtest (Train:2024 / Test:2025)
================================================================

  DB   : data/umalogi.db
  Train: 2024-01-01 〜 2024-12-31  (N レース)
  Test : 2025-01-01 〜 2025-12-31  (N レース)

--- モデル再訓練 ---
  [OK] 本命モデル  AUC=0.xxx
  [OK] 卍モデル    AUC=0.xxx
  [OK] 複勝モデル  AUC=0.xxx
  [OK] ALPHAモデル AUC=0.xxx

--- 2025年 アウト・オブ・サンプル 結果 ---

+----------------------+--------+------+--------+---------+--------+--------+
| 戦略                 | レース | 的中 | 的中率  | 投資(円)| 回収(円)| ROI   |
+----------------------+--------+------+--------+---------+--------+--------+
| 本命・単勝(Top1)     |  1,240 |  350 | 28.2%  | 124,000 | 107,880|  87%  |
| ...                  |        |      |        |         |        |       |
+----------------------+--------+------+--------+---------+--------+--------+
  ★ 黒字戦略: 卍・単勝, ALPHA・単勝, ALPHA・複勝

--- 月別ROI推移（2025年） ---
  2025-01: 本命単勝= 82%  卍単勝=134%  複勝Top1= 95%  ALPHA単勝=162%
  ...

--- 会場別・距離帯別内訳 ---
  (既存 simulate_year.py と同形式)
```

### CSV（`--csv` フラグ時）

`results/backtest_YYYYMMDD_HHMMSS.csv` に以下のカラムで書き出し:

```
strategy,races,skipped,hits,hit_rate,invested,payout,roi,profit
```

---

## CLIインターフェース

```bash
# 基本実行（2024学習 → 2025テスト）
py scripts/backtest_all_models.py

# CSVも出力
py scripts/backtest_all_models.py --csv

# 一時モデルを実行後に自動削除
py scripts/backtest_all_models.py --cleanup

# DB指定
py scripts/backtest_all_models.py --db path/to/other.db

# 詳細ログ
py scripts/backtest_all_models.py --verbose

# データ件数確認のみ（予測・再訓練なし）
py scripts/backtest_all_models.py --dry-run
```

---

## 本番モデル保護の仕様

```
data/models/backtest_tmp/   ← このディレクトリ以外には一切書き込まない
```

- スクリプト起動時に `backtest_tmp/` が存在する場合は事前に削除（前回実行の残骸）
- `--cleanup` 指定時は評価完了後に `backtest_tmp/` を自動削除
- `--cleanup` 非指定時は残したまま終了（次回のデバッグ用に保持可能）

---

## 依存関係（既存コードの流用箇所）

| 流用元 | 利用機能 |
|--------|---------|
| `src/ml/models.py` | `HonmeiModel`, `ManjiModel`, `PlaceModel` のtrain/predict |
| `src/ml/alpha_model.py` | `run_backtest()` 関数（ALPHA専用の自己完結型バックテスト） |
| `src/ml/features.py` | `FeatureBuilder.build_race_features_for_simulate()` |
| `src/evaluation/evaluator.py` | `_is_hit`, `_fetch_payouts`, `_lookup_payout`, `_build_combination_key`, `_fetch_horse_numbers` |
| `src/database/init_db.py` | `init_db()`, `get_db_path()` |
| `scripts/simulate_year.py` | **import しない**。`StrategyStats` クラス相当を新スクリプト内に再実装（scripts/は非パッケージのためimport不可） |

---

## エラーハンドリング方針

- **特徴量生成失敗**: WARNING ログを出し、当該レースをスキップ（クラッシュさせない）
- **モデル訓練失敗**: ERROR ログを出し、該当モデルをスキップ。フォールバックモードで残りモデルは継続
- **払戻データ未取得**: 的中でも払戻0として記録し、WARNING
- **2024/2025データ不足**（レース数 < 100）: 実行前に警告してユーザー確認を促す

---

## テスト方針

- `--dry-run` でデータ件数確認のみできること
- 既存の pytest スイートが通ること（既存コードを変更しないため回帰なし）
- 手動で `py scripts/backtest_all_models.py --dry-run` を実行して動作確認

---

## ドキュメント更新（CLAUDE.md 条項準拠）

実装完了後に以下を更新:

| ファイル | 更新内容 |
|---------|---------|
| `docs/2_automation_schedule.md` | backtest_all_models.py の使用方法を追記 |
| `docs/7_weakness_ledger.md` | 本機能で解消する弱点（複勝/ALPHAのバックテスト未統合）をクローズ |
