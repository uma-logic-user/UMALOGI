# UMALOGI — 自律型競馬予測プラットフォーム

JRA-VAN Data Lab. と netkeiba を統合した、LightGBM による全券種対応の AI 競馬予測システム。
期待値（EV）ベースで買い目を自動生成し、照合・評価・再学習まで完全自動化する。

---

## システム概要

| 項目 | 内容 |
|---|---|
| データソース | JRA-VAN Data Lab. (JV-Link COM) + netkeiba スクレイピング |
| 予測モデル | LightGBM 2 本体制（本命モデル: 1着確率分類 / 卍モデル: EV 回帰） |
| 対応馬券 | 単勝・複勝・馬連・ワイド・馬単・三連複・三連単・WIN5 |
| データ蓄積先 | SQLite `data/umalogi.db`（約 20 テーブル + ビュー） |
| ダッシュボード | Streamlit (`web_streamlit/app.py`) — DB 直結、JSON 不要 |
| 通知 | Discord Webhook / X (Twitter) API |
| 自動化基盤 | GitHub Actions（土日 prerace / 月曜 retrain）+ ローカル常駐スケジューラー |

---

## ディレクトリ構造

```
UMALOGI/
├── src/
│   ├── database/
│   │   └── init_db.py          # DB 初期化・マイグレーション・CRUD ヘルパー
│   ├── scraper/
│   │   ├── netkeiba.py         # netkeiba 結果スクレイパー（HorseResult / RaceInfo）
│   │   ├── entry_table.py      # 出馬表・リアルタイムオッズ取得
│   │   ├── fetch_historical.py # 過去レース ID 一括取得
│   │   ├── jravan_client.py    # JV-Link COM クライアント（32bit Python 専用）
│   │   └── update_payouts.py   # 確定払戻の後追い取得
│   ├── ml/
│   │   ├── features.py         # FeatureBuilder（特徴量 DataFrame 生成）
│   │   ├── models.py           # HonmeiModel / ManjiModel（学習・推論・Platt Scaling）
│   │   ├── bet_generator.py    # BetGenerator（Harville 公式 + ケリー基準 + EV キャップ）
│   │   ├── reconcile.py        # 的中照合バッチ（同着・返還対応）
│   │   ├── incremental.py      # 増分学習（Champion-Challenger 方式）
│   │   └── win5.py             # WIN5 予測エンジン
│   ├── evaluation/
│   │   └── evaluator.py        # 的中判定（同着・返還・競走中止の例外処理）
│   ├── notification/
│   │   └── discord_notifier.py # Discord Webhook 通知
│   ├── ops/
│   │   ├── data_sync.py        # JRA-VAN 差分同期 (RACE/WOOD/DIFN/BLOD)
│   │   └── retrain_trigger.py  # 自動再学習トリガー
│   └── main_pipeline.py        # パイプライン統合（friday / prerace / train / reconcile）
├── scripts/
│   ├── scheduler.py            # 常駐スケジューラー（週次サイクル管理）
│   ├── run_prerace_auto.py     # 当日全レース直前予想バッチ（GitHub Actions 用）
│   ├── simulate_year.py        # 年間バックテストシミュレーション
│   ├── run_train.py            # モデル学習ラッパー
│   └── force_provisional_today.py # 本日分の暫定予想を即時生成
├── web_streamlit/
│   └── app.py                  # Streamlit ダッシュボード（Pro Investor UI）
├── tests/                      # pytest テストスイート（185 テスト）
├── data/
│   ├── umalogi.db              # SQLite メイン DB
│   ├── models/                 # 訓練済みモデル (.pkl)
│   │   └── history/            # 世代管理（直近 10 世代）
│   └── predictions/            # UI 用 JSON 出力（prerace が生成）
├── .claude/
│   ├── skills/                 # エージェント参照ドキュメント（db_schema.md 等）
│   └── agents/                 # Subagent 役割定義
├── requirements.txt
├── CLAUDE.md                   # 開発規約（AI エージェントへの指示書）
└── AI_CONTEXT.md               # 内部ロジック詳細（後任 AI 向け調教書）
```

---

## 環境構築

### 前提条件

- Python 3.14（64bit）— 通常処理用
- Python 3.14（32bit）— JV-Link COM 専用（`py -3.14-32`）
- Windows 10/11（JV-Link は Windows COM サーバーのため）
- JRA-VAN Data Lab. 会員登録済み + JV-Link インストール済み

### セットアップ

```bash
# 1. リポジトリクローン
git clone https://github.com/uma-logic-user/UMALOGI.git
cd UMALOGI

# 2. 依存ライブラリインストール
pip install -r requirements.txt

# 3. 環境変数設定（.env ファイルを作成）
#   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
#   TWITTER_BEARER_TOKEN=...（X 通知を使う場合）
#   JV_SDK_SID=...（JRA-VAN ソフトウェア ID）

# 4. DB 初期化（スキーマ作成・マイグレーション自動実行）
python -m src.database.init_db

# 5. Streamlit 起動確認
streamlit run web_streamlit/app.py
```

### 主要ライブラリ

| ライブラリ | バージョン | 用途 |
|---|---|---|
| pandas | ≥ 2.2.0 | データ処理 |
| lightgbm | ≥ 4.3.0 | 予測モデル |
| scikit-learn | ≥ 1.4.0 | Platt Scaling・評価 |
| beautifulsoup4 | ≥ 4.12.0 | netkeiba スクレイピング |
| requests | ≥ 2.31.0 | HTTP クライアント |
| streamlit | ≥ 1.35.0 | ダッシュボード |
| plotly | ≥ 5.22.0 | グラフ描画 |
| tenacity | ≥ 8.2.0 | リトライ処理 |
| schedule | ≥ 1.2.0 | ジョブスケジューリング |

---

## 運用コマンド一覧

### 1. 翌日の出馬表取得（金曜夜バッチ）

```bash
python -m src.main_pipeline friday
# 特定日を指定する場合
python -m src.main_pipeline friday --date 20260412
```

**処理内容**: netkeiba から翌日の全レース出馬表を取得し、`races` + `entries` テーブルへ保存。
スクレイピング失敗（0頭取得）時は Discord に `🚨【緊急】スクレイピング仕様変更の可能性` を通知。

---

### 2. 暫定予想の生成

```bash
python -m src.main_pipeline provisional
# または特定日
python -m src.main_pipeline provisional --date 20260412
# または手動で即時生成
python scripts/force_provisional_today.py
```

**処理内容**: オッズ・馬体重が未発表の状態でも LightGBM に NaN のまま推論させる。
`model_type` に `(暫定)` サフィックスを付与（例: `本命(暫定)`）。
Streamlit の「🔮 暫定予想」タブに表示される。

---

### 3. レース直前の本気予想（prerace）

```bash
# 単独レースを指定
python -m src.main_pipeline prerace 202605060511

# 当日全レースを自動実行（GitHub Actions / スケジューラー用）
python scripts/run_prerace_auto.py
python scripts/run_prerace_auto.py --date 20260412
```

**処理フロー（6ステップ）**:

| Step | 内容 |
|---|---|
| 0 | 締め切り時刻チェック（15分前超過で Discord 遅延警告） |
| 1 | リアルタイムオッズ取得 → `realtime_odds` テーブルへ保存 |
| 1b | 馬体重・馬場状態の当日更新 |
| 2 | `FeatureBuilder` で特徴量 DataFrame 生成 |
| 2b | データ品質チェック（馬体重欠損 > 50% or オッズ欠損 > 30% → 見送り通知） |
| 3 | 本命 / 卍モデルで予測スコア算出 |
| 4 | `BetGenerator` で Harville 公式 + ケリー基準で買い目生成 |
| 5 | `predictions` / `prediction_horses` へ保存。WIN5 対象日なら同時実行 |
| 6 | `data/predictions/<race_id>.json` へ UI 用 JSON 出力 |

`model_type` = `本命(直前)` / `卍(直前)` で保存。

---

### 4. 的中結果の照合（reconcile）

```bash
python -m src.main_pipeline reconcile <race_id>
# ドライラン（DB に書き込まない確認用）
python -m src.main_pipeline reconcile <race_id> --dry-run
```

**処理内容**: `race_payouts` テーブルの確定払戻と `predictions` を照合し、
`prediction_results` に的中フラグ・払戻額・利益・ROI を記録。
同着（dead heat）・返還（scratch）の特殊ケースに完全対応。

---

### 5. モデルの再学習（train）

```bash
python -m src.main_pipeline train
# または
python scripts/run_train.py
```

**処理内容**: `v_race_mart` ビューから全データを取得し、
`GroupKFold` でレース単位に分割してクロスバリデーション。
学習済みモデルを `data/models/honmei_model.pkl` / `manji_model.pkl` に保存。
旧モデルは `data/models/history/` に日付付きでアーカイブ（10世代管理）。

---

### 6. 年間バックテスト

```bash
python scripts/simulate_year.py --year 2024
python scripts/simulate_year.py --year 2024 --venue 中山
```

---

### 7. 常駐スケジューラー起動

```bash
python scripts/scheduler.py              # デーモン起動
python scripts/scheduler.py --run-now friday   # 即時テスト実行
```

**週次サイクル**:

| タイミング | 実行内容 |
|---|---|
| 金曜 20:00 | 出馬表取得 + JRA-VAN RACE 同期 |
| 土日 07:30 | JRA-VAN WOOD 同期（調教タイム） |
| 土日 09:00〜 | レース直前予想（prerace）× 全 R |
| 土日 16:00 | 払戻同期 + 照合 + 通知 + 増分学習 |
| 月曜 06:00 | マスタ差分更新（DIFN/BLOD） |
| 月曜 07:00 | 週次全件再学習 |
| 月曜 08:00 | GitHub 自動コミット・プッシュ |

---

## Streamlit ダッシュボード

```bash
streamlit run web_streamlit/app.py
```

**タブ構成**:

```
🏇 レース分析
  ├── 🔮 暫定予想    — 前日予想（オッズ欠損許容）
  ├── 🔍 直前予想    — 当日レース直前の本気予想
  ├── 📡 オッズ動向  — リアルタイムオッズ推移・大口シグナル
  ├── 📋 レース結果  — 着順・払戻（同着・返還表示対応）
  └── 🗂️ 予想アーカイブ — 全予想バリアント比較

📈 Analytics
  ├── 月次 ROI
  ├── ケリー基準 資金曲線・最大ドローダウン
  └── 会場別 ROI

🎯 的中実績
  └── EV ≥ 1.0 ベット追跡
```

---

## Discord 通知

### 設定方法

1. Discord サーバーで「サーバー設定 → 連携サービス → ウェブフック」から Webhook URL を作成
2. `.env` ファイルに設定:
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<id>/<token>
   ```

### 通知一覧

| メッセージ | タイミング | 重要度 |
|---|---|---|
| `[見送り] <会場R> データ不足: <理由>` | データ品質チェック失敗 | 通常 |
| `[遅延警告] <会場R> 予測処理が遅れています` | 締め切り 15 分前超過 | 警告 |
| `🚨【緊急】スクレイピング仕様変更の可能性` | 0頭取得 / 全オッズ NaN | **緊急** |
| `[WIN5] 推奨買い目 EV=X.XXX` | WIN5 EV ≥ 1.0 検出 | 情報 |
| 予想結果サマリー | 照合バッチ完了後 | 情報 |

`DISCORD_WEBHOOK_URL` が未設定の場合は WARNING ログのみ（サイレント失敗、パイプライン継続）。

---

## テスト実行

```bash
# 全テスト（185 件）
py -m pytest tests/ -q

# 特定モジュールのみ
py -m pytest tests/test_domain_exceptions.py -v  # 同着・返還・EV ロジック
py -m pytest tests/test_models.py -v             # モデル学習・推論
py -m pytest tests/test_bet_generator.py -v      # 買い目生成・ケリー基準
```

**既知の失敗**: `tests/test_v_race_mart.py::TestTrainingJoin` の 5 件は
`training_times` / `training_hillwork` の horse_id 形式不一致に起因する既存バグ。
JRA-VAN 調教データ未取得環境では再現する（本番データ投入後は解消）。

---

## JRA-VAN データ取得（JV-Link）

> **32bit Python が必要**。64bit Python では COM サーバーに接続できない。

```bash
# セットアップ（全データ一括取得）
py -3.14-32 -m src.scraper.jravan_client --option 2 --fromtime 20200101

# 差分更新（通常運用）
py -3.14-32 -m src.scraper.jravan_client --option 1

# デバッグ（生バイト表示）
py -3.14-32 -m src.scraper.jravan_client --debug --fromtime 20260101
```

---

## 環境変数一覧

| 変数名 | 必須 | 説明 |
|---|---|---|
| `DISCORD_WEBHOOK_URL` | 推奨 | Discord 通知先 Webhook URL |
| `TWITTER_BEARER_TOKEN` | 任意 | X (Twitter) Bearer Token |
| `TWITTER_API_KEY` | 任意 | X API Key |
| `TWITTER_API_SECRET` | 任意 | X API Secret |
| `TWITTER_ACCESS_TOKEN` | 任意 | X Access Token |
| `TWITTER_ACCESS_SECRET` | 任意 | X Access Token Secret |
| `JV_SDK_SID` | JV-Link 使用時 | JRA-VAN ソフトウェア ID |
