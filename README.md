# UMALOGI — 自律型競馬予測プラットフォーム

JRA-VAN Data Lab. と netkeiba を統合した、LightGBM による全券種対応の AI 競馬予測システム。  
期待値（EV）ベースで買い目を自動生成し、照合・評価・再学習まで完全自動化する。

---

## システム概要

| 項目 | 内容 |
|---|---|
| データソース | JRA-VAN Data Lab. (JV-Link COM) + netkeiba スクレイピング |
| 予測モデル | LightGBM 4本体制（本命 / 卍 / 複勝Elite / ALPHAモデル）|
| 対応馬券 | 単勝・複勝・馬連・ワイド・馬単・三連複・三連単・WIN5 |
| データ蓄積先 | SQLite `data/umalogi.db`（約 25 テーブル + ビュー）|
| ユーザー向け UI | **Next.js 15** `web/` — レース予想閲覧・当日購入ガイド |
| 運用者向け UI | **Streamlit** `web_streamlit/app.py` — DB 直結・高速分析ダッシュボード |
| 通知 | Discord Webhook（5チャンネル分離）/ X (Twitter) API |
| 自動化基盤 | ローカル常駐スケジューラー（週次サイクル管理）|

---

## 2 つのフロントエンドの役割分担

```
┌─────────────────────────────────────────────────────┐
│                   UMALOGI Backend                   │
│   SQLite DB ← pipeline → models/ → predictions/    │
└───────────────┬─────────────────────┬───────────────┘
                │                     │
    ┌───────────▼──────────┐  ┌───────▼────────────────┐
    │   Next.js (web/)     │  │  Streamlit (web_streamlit/)│
    │  ユーザー向け         │  │  運用者向け              │
    │  ─────────────────   │  │  ────────────────────    │
    │  ・レース一覧・詳細    │  │  ・月次ROI トレンド       │
    │  ・AI予想スコア表示    │  │  ・ケリー資金曲線         │
    │  ・当日購入ガイド      │  │  ・会場別 ROI 分析        │
    │  ・的中結果履歴        │  │  ・バイアスパネル         │
    │  ・収支KPIダッシュボード│  │  ・暫定予想タブ           │
    │  Port: 3000          │  │  Port: 8501              │
    └──────────────────────┘  └────────────────────────┘
```

---

## ディレクトリ構造

```
UMALOGI/
├── src/
│   ├── database/
│   │   └── init_db.py          # DB 初期化・マイグレーション・CRUD ヘルパー
│   ├── scraper/
│   │   ├── netkeiba.py         # netkeiba 結果スクレイパー
│   │   ├── entry_table.py      # 出馬表・リアルタイムオッズ取得
│   │   ├── jravan_client.py    # JV-Link COM クライアント（32bit Python 専用）
│   │   └── update_payouts.py   # 確定払戻の後追い取得
│   ├── ml/
│   │   ├── features.py         # FeatureBuilder（特徴量 DataFrame 生成・69列）
│   │   ├── models.py           # HonmeiModel / ManjiModel（学習・推論・Platt Scaling）
│   │   ├── alpha_model.py      # ALPHAモデル（EV特化・Harville公式）
│   │   ├── bet_generator.py    # BetGenerator（Harville + ケリー基準 + EVキャップ）
│   │   ├── u_score.py          # U-score スコアリングエンジン（18因子）
│   │   ├── ev_features.py      # EV特化特徴量（Shin/Harville/Kelly np.cumprod）
│   │   ├── reconcile.py        # 的中照合バッチ（同着・返還対応）
│   │   ├── incremental.py      # 増分学習（Champion-Challenger 方式）
│   │   └── win5.py             # WIN5 予測エンジン
│   ├── evaluation/
│   │   └── evaluator.py        # 的中判定（同着・返還・競走中止の例外処理）
│   ├── notification/
│   │   ├── discord_notifier.py # Discord Webhook 通知
│   │   └── router.py           # NotificationRouter（5チャンネル分離）
│   ├── ops/
│   │   ├── data_sync.py        # JRA-VAN 差分同期 (RACE/WOOD/DIFN/BLOD)
│   │   ├── note_generator.py   # note.com 記事自動生成
│   │   ├── note_draft_publisher.py # note.com Playwright 投稿
│   │   └── jvlink_dialog_handler.py # JVLink ダイアログ自動突破ハンドラー
│   └── main_pipeline.py        # パイプライン統合（friday / prerace / train / reconcile）
├── scripts/
│   ├── scheduler.py            # 常駐スケジューラー（週次サイクル管理）
│   ├── run_prerace_auto.py     # 当日全レース直前予想バッチ
│   ├── simulate_year.py        # 年間バックテストシミュレーション
│   ├── run_train.py            # モデル学習ラッパー
│   └── force_provisional_today.py # 本日分の暫定予想を即時生成
├── web/                        # Next.js 15 フロントエンド（ユーザー向け）
│   ├── src/
│   │   ├── app/
│   │   │   ├── api/            # Route Handlers（SQLite直接参照）
│   │   │   │   ├── races/      # レース一覧・詳細 API
│   │   │   │   ├── predictions/# 予想データ API
│   │   │   │   ├── financial/  # 収支 API
│   │   │   │   └── hits/       # 的中履歴 API
│   │   │   └── page.tsx        # メインページ
│   │   ├── components/
│   │   │   ├── RaceDetail.tsx  # レース詳細（5タブ構成）
│   │   │   ├── TodayBuyPanel.tsx # 当日購入ガイド（ケリー基準）
│   │   │   ├── PredictionsPanel.tsx # AI予想スコア表示
│   │   │   ├── FinancialDashboard.tsx # 収支KPIダッシュボード
│   │   │   └── RaceTree.tsx    # レースツリーナビ（カウントダウン付き）
│   │   ├── lib/
│   │   │   ├── kelly.ts        # ケリー基準ユーティリティ（純粋関数）
│   │   │   └── db.ts           # SQLite接続ヘルパー
│   │   └── types/
│   │       └── race.ts         # 型定義（RacePrediction / RaceResult 等）
│   ├── package.json
│   └── tsconfig.json
├── web_streamlit/
│   └── app.py                  # Streamlit ダッシュボード（キャッシュ最適化済み）
├── tests/                      # pytest テストスイート（466+ テスト）
│   ├── test_streamlit_perf.py  # Streamlitパフォーマンス検証（34テスト）
│   └── ...
├── data/
│   ├── umalogi.db              # SQLite メイン DB
│   ├── models/                 # 訓練済みモデル (.pkl)
│   │   └── history/            # 世代管理（直近 10 世代）
│   └── predictions/            # UI 用 JSON 出力（prerace が生成）
├── .claude/
│   ├── skills/                 # エージェント参照ドキュメント（db_schema.md 等）
│   └── agents/                 # Subagent 役割定義
├── requirements.txt
└── CLAUDE.md                   # 開発規約（AI エージェントへの指示書）
```

---

## 主要機能

### Next.js フロントエンド（Port 3000）

| 機能 | 説明 |
|---|---|
| レース一覧 / ツリーナビ | 開催日・会場・レース番号ツリー。推定発走時刻とカウントダウン表示 |
| AI 予想スコア | 本命 / 卍 / ALPHA 各モデルの EV・Kelly 推奨額・信頼度をカード表示 |
| **当日購入タブ** | ケリー基準（f*=(EV-1)/(odds-1)）で推奨購入金額を算出。単勝のみ対象（他券種は「オッズ確認要」）。総資金・Kelly係数をドロップダウンで変更可能。確定結果がある場合は合計投資額・払い戻し・回収率のKPIサマリーを表示。的中行ハイライト / 外れ行グレーダウン |
| 的中結果履歴 | AI的中照合結果を一覧表示。モデル別フィルター・ROI/EVソート対応 |
| 収支ダッシュボード | 月次ROI・Kelly vs フラット比較バー・会場別パフォーマンス |
| SNS コピー | X用（280字）/ NOTE用（Markdown）をワンクリックコピー |

### Streamlit ダッシュボード（Port 8501）

| タブ | 説明 |
|---|---|
| 🔮 暫定予想 | 前日予想（オッズ欠損許容） |
| 🔍 直前予想 | 当日レース直前の本気予想 |
| 📡 オッズ動向 | リアルタイムオッズ推移・大口シグナル |
| 📋 レース結果 | 着順・払戻（同着・返還表示対応） |
| 📈 Analytics | 月次ROI / ケリー資金曲線 / 会場別ROI（全てキャッシュ最適化済み） |
| 🎯 的中実績 | EV ≥ 1.0 ベット追跡 |

---

## パフォーマンス最適化（Streamlit）

`web_streamlit/app.py` には以下の最適化が実装されています。

### キャッシュ化（@st.cache_data）

```python
@st.cache_data(ttl=300)
def _build_monthly_total(kind: str) -> tuple[pd.DataFrame, pd.DataFrame]: ...

@st.cache_data(ttl=300)
def _build_kelly_series(kind: str) -> dict: ...

@st.cache_data(ttl=300)
def _build_venue_stats(kind: str) -> pd.DataFrame: ...
```

月次ROI・ケリーシリーズ・会場別ROIの派生DataFrameを5分間キャッシュし、
selectbox操作のたびに同一クエリを再実行するコストをゼロにする。

### フラグメント分離（@st.fragment）

```python
@st.fragment
def render_analytics(): ...   # Analytics タブ内 selectbox の変化がメインを再実行しない

@st.fragment
def render_hit_performance(): ... # 的中実績タブも同様
```

### numpy ベクトル化（iterrows 全廃）

```python
# Before: apply(axis=1) ループ
display["EV"] = display.apply(lambda r: "🔥 " + ..., axis=1)

# After: numpy ベクトル演算
display["EV"] = np.where(_ev_raw_na, "—",
    np.where(_ev_num >= 1.0, _ev_num.map(lambda x: f"🔥 {x:.2f}"),
             _ev_num.map(lambda x: f"{x:.2f}")))
```

全ての `.iterrows()` / `.apply(axis=1)` を `np.where` + マスク演算に置換済み。

---

## 環境構築

### 前提条件

- Python 3.11+（64bit）— 通常処理用
- Python 3.11+（32bit）— JV-Link COM 専用（`py -3.11-32`）
- Node.js 20+（Next.js フロントエンド用）
- Windows 10/11（JV-Link は Windows COM サーバーのため）
- JRA-VAN Data Lab. 会員登録済み + JV-Link インストール済み

### Python バックエンド セットアップ

```bash
# 1. リポジトリクローン
git clone https://github.com/uma-logic-user/UMALOGI.git
cd UMALOGI

# 2. 依存ライブラリインストール
pip install -r requirements.txt

# 3. 環境変数設定（.env ファイルを作成）
#   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
#   JV_SDK_SID=...（JRA-VAN ソフトウェア ID）

# 4. DB 初期化（スキーマ作成・マイグレーション自動実行）
python -m src.database.init_db
```

### Next.js フロントエンド 起動

```bash
cd web
npm install

# 開発サーバー（ホットリロード）
npm run dev            # → http://localhost:3000

# 本番ビルド + 起動
npm run build
npm start              # → http://0.0.0.0:3000（LAN 公開）

# モバイル対応（同一 LAN の iOS/Android からアクセス）
npm run dev:mobile     # → http://0.0.0.0:3000
```

### Streamlit ダッシュボード 起動

```bash
streamlit run web_streamlit/app.py
# → http://localhost:8501
```

### 主要 Python ライブラリ

| ライブラリ | バージョン | 用途 |
|---|---|---|
| pandas | ≥ 2.2.0 | データ処理 |
| numpy | ≥ 1.26.0 | ベクトル演算（iterrows 廃止）|
| lightgbm | ≥ 4.3.0 | 予測モデル |
| scikit-learn | ≥ 1.4.0 | Platt Scaling・評価 |
| beautifulsoup4 | ≥ 4.12.0 | netkeiba スクレイピング |
| streamlit | ≥ 1.35.0 | 分析ダッシュボード |
| plotly | ≥ 5.22.0 | グラフ描画 |
| playwright | ≥ 1.40.0 | note.com 自動投稿 |

---

## 運用コマンド一覧

### 1. 翌日の出馬表取得（金曜夜バッチ）

```bash
python -m src.main_pipeline friday
python -m src.main_pipeline friday --date 20260412
```

### 2. 暫定予想の生成

```bash
python -m src.main_pipeline provisional
python scripts/force_provisional_today.py
```

### 3. レース直前の本気予想（prerace）

```bash
python -m src.main_pipeline prerace 202605060511
python scripts/run_prerace_auto.py          # 当日全レース
python scripts/run_prerace_auto.py --date 20260412
```

**処理フロー（6ステップ）**:

| Step | 内容 |
|---|---|
| 0 | 締め切り時刻チェック（15分前超過で Discord 遅延警告）|
| 1 | リアルタイムオッズ取得 → `realtime_odds` テーブルへ保存 |
| 1b | 馬体重・馬場状態の当日更新 |
| 2 | `FeatureBuilder` で特徴量 DataFrame 生成（69列）|
| 3 | 本命 / 卍 / ALPHA モデルで予測スコア算出 |
| 4 | `BetGenerator` で Harville 公式 + ケリー基準で買い目生成 |
| 5 | `predictions` / `prediction_horses` へ保存 |
| 6 | `data/predictions/<race_id>.json` へ UI 用 JSON 出力 |

### 4. 的中結果の照合（reconcile）

```bash
python -m src.main_pipeline reconcile <race_id>
python -m src.main_pipeline reconcile <race_id> --dry-run
```

### 5. モデルの再学習（train）

```bash
python -m src.main_pipeline train
python scripts/run_train.py
```

学習済みモデルは `data/models/` に保存。旧モデルは `data/models/history/` に10世代管理。

### 6. 年間バックテスト

```bash
python scripts/simulate_year.py --year 2024
python scripts/simulate_year.py --year 2024 --venue 中山
```

### 7. 常駐スケジューラー起動

```bash
python scripts/scheduler.py              # デーモン起動
python scripts/scheduler.py --run-now friday   # 即時テスト実行
```

**週次サイクル**:

| タイミング | 実行内容 |
|---|---|
| 金曜 20:00 | 出馬表取得 + JRA-VAN RACE 同期 |
| 土日 07:30 | JRA-VAN WOOD 同期（調教タイム）|
| 土日 09:00〜 | レース直前予想（prerace）× 全 R |
| 土日 16:00 | 払戻同期 + 照合 + 通知 + 増分学習 |
| 月曜 06:00 | マスタ差分更新（DIFN/BLOD）|
| 月曜 07:00 | 週次全件再学習 |
| 月曜 08:00 | GitHub 自動コミット・プッシュ |

---

## テスト実行

### Python バックエンド

```bash
# 全テスト（466+ 件）
py -m pytest tests/ -q

# 特定モジュールのみ
py -m pytest tests/test_domain_exceptions.py -v  # 同着・返還・EV ロジック
py -m pytest tests/test_models.py -v             # モデル学習・推論
py -m pytest tests/test_streamlit_perf.py -v     # Streamlit パフォーマンス検証
```

### Next.js フロントエンド

```bash
cd web
npm test            # Jest + React Testing Library
npm run type-check  # TypeScript 型チェック
```

---

## Discord 通知

| 変数名 | チャンネル用途 |
|---|---|
| `DISCORD_WEBHOOK_URL` | 予想通知（メイン）|
| `DISCORD_SYSTEM_WEBHOOK_URL` | システムアラート |
| `DISCORD_EV_ALERT_WEBHOOK_URL` | EV ≥ 1.5 激熱アラート（@everyone）|
| `DISCORD_AB_TEST_WEBHOOK_URL` | A/B テスト結果 |
| `DISCORD_NOTE_DRAFT_WEBHOOK_URL` | note.com 下書き転送 |

### 通知一覧

| メッセージ | タイミング |
|---|---|
| `[見送り] <会場R> データ不足: <理由>` | データ品質チェック失敗 |
| `🚨【緊急】スクレイピング仕様変更の可能性` | 0頭取得 / 全オッズ NaN |
| `⚡ 激熱 EV=X.XX @everyone` | EV ≥ 1.5 検出 |
| 予想結果サマリー | 照合バッチ完了後 |

---

## JRA-VAN データ取得（JV-Link）

> **32bit Python が必要**。64bit Python では COM サーバーに接続できない。

```bash
# セットアップ（全データ一括取得）
py -3.11-32 -m src.scraper.jravan_client --option 2 --fromtime 20200101

# 差分更新（通常運用）
py -3.11-32 -m src.scraper.jravan_client --option 1
```

---

## 環境変数一覧

| 変数名 | 必須 | 説明 |
|---|---|---|
| `DISCORD_WEBHOOK_URL` | 推奨 | Discord 予想通知 Webhook URL |
| `DISCORD_SYSTEM_WEBHOOK_URL` | 任意 | システムアラート専用 |
| `DISCORD_EV_ALERT_WEBHOOK_URL` | 任意 | EV激熱アラート専用 |
| `JV_SDK_SID` | JV-Link 使用時 | JRA-VAN ソフトウェア ID |
| `NOTE_PROFILE_URL` | 任意 | note.com プロフィール URL（X投稿誘導用）|
