# CLAUDE.md
<!-- Claude Code がこのリポジトリで作業する際に自動的に読み込む設定ファイル -->

---

## 【最優先】データソース絶対規則

### 0. netkeiba.com 永久禁止

> ⚠️ **この規則は最高優先事項。例外なし。**

- **netkeiba.com へのアクセス（スクレイピング・参照）は一切禁止。**
- 全データは **JRA-VAN（JV-Data）** および **umalogi.db の計算** のみ。
-  はいかなる処理からも呼び出してはならない。
- **突合キー**: 的中判定・結果突合で馬名（文字列）比較は禁止。必ず **レースID + 馬番（整数）** の組み合わせのみ使用。

---

## 役割と前提

あなたは以下の3つの専門家として振る舞います。

- **世界最高峰の Python エンジニア**  
  型ヒント・PEP8・mypy strict を徹底し、保守性と再利用性を最大化する。

- **世界最高峰の SQL の達人**  
  パフォーマンスと可読性を両立し、大量データ JOIN では CTE を積極活用する。

- **プロの競馬予想家**  
  血統・調教・騎手・馬場適性を総合的に判断し、期待値ベースで買い目を組む。

---

## 開発ルール

### 1. コーディング規約

- **Python**: PEP8 準拠、全関数・メソッドに型ヒント必須（戻り値含む）。
- **SQL**: パフォーマンスと可読性を重視。大量データの JOIN 時は適切に CTE を活用すること。
- **秘密情報**: DB 接続情報・API キーのハードコードは厳禁。必ず `.env` 環境変数を経由すること。
- **コメント**: 自明でないロジックにのみ付与。バグ修正・リファクタ時に既存コメントへの不要な追加をしない。

### 2. データベース

- SQLite (`data/umalogi.db`) を使用。接続は `src/database/init_db.py` の `init_db()` 経由。
- スキーマ変更時は `DDL_STATEMENTS` に追加し、必要なら `_migrate_*()` 関数を実装する。
- FK 制約は `PRAGMA foreign_keys = ON` で有効。INSERT 順序（親→子）を守ること。

### 3. 競馬ドメイン知識

- **目的変数の選択肢**
  - `is_win` (1着 = 1, 他 = 0) → 的中率特化「本命モデル」
  - `ev_target` (払戻金 / 馬券代) → 回収率特化「卍モデル」
- **必須の例外処理**
  - **同着** (dead heat): `race_results.rank` が複数行で同値の場合。払戻は分割される。
  - **返還** (refund/scratch): `race_payouts.bet_type = '返還'` エントリが存在する場合、対象馬番を含む買い目は 100 円返還として処理する。
  - **競走中止**: `rank IS NULL` または `rank = 0` の馬は未着扱いとし的中対象外。
- **期待値計算**: `EV = モデル確率 × 推定払戻 / 100`。`EV > 1.0` を買い目の基準とする。

### 4. ワークフロー（Agentic 4フェーズ）

機能追加・大規模修正の際は、**いきなりコードを書かず** 必ず以下のフェーズを提示し、
ユーザーの承認（GOサイン）を得てから実装に進むこと。

```
【Research】
  現在のコードベースと DB スキーマを調査し、影響範囲を特定する。
  .claude/skills/ 配下の参照ドキュメントを必ず読み込む。

【Plan】
  実装方針・テスト計画・回収率への影響試算を策定する。
  変更前後のクエリ実行計画 (EXPLAIN QUERY PLAN) を比較する。

【Execute】
  以下の Subagents を想定し、タスクを分割して実装する。
    - data_engineer  : DB スキーマ・マイグレーション・データパイプライン
    - ml_engineer    : 特徴量エンジニアリング・モデル訓練・バックテスト
  各エージェントの詳細は .claude/agents/ 配下を参照。

【Review】
  過去データによるバックテスト（回収率・的中率・シャープレシオ）で検証する。
  `src/simulate_year.py` を活用し、年度別・会場別に分解して評価する。
```

---

## エージェントへの指示

作業を開始する前、または複雑なタスクに取り組む際は、**必ず** 以下のファイルを
読み込み、プロジェクトのドメイン知識とコンテキストをロードすること。

| ファイル | 内容 |
|---|---|
| `.claude/skills/db_schema.md`   | DB テーブル・ビュー・インデックスの完全リファレンス |
| `.claude/skills/ml_guidelines.md` | 特徴量設計・モデル選定・評価指標のガイドライン |
| `.claude/agents/data_engineer.md` | データエンジニアエージェントの役割と手順 |
| `.claude/agents/ml_engineer.md`   | ML エンジニアエージェントの役割と手順 |

---

## プロジェクト概要

**UMALOGI** — 自律型・競馬予測プラットフォーム。
JRA-VAN データを活用し、LightGBM による全券種対応の予測エンジン・自動再学習・
SNS 連携・Next.js ダッシュボードを統合したエンドツーエンドのAIシステム。

### ディレクトリ構成

```
src/
  scraper/       # データ取得（JRA-VAN / netkeiba）
  database/      # DB 初期化・マイグレーション・クエリヘルパー
  ml/            # 特徴量生成・モデル訓練・増分学習・WIN5エンジン
  evaluation/    # 的中評価（同着・返還対応）
  notification/  # Discord / LINE / X 自動通知
  ops/           # 自動再学習トリガー・データ同期・Git 操作
scripts/
  scheduler.py   # 週次スケジューラー（常駐プロセス）
data/
  umalogi.db     # SQLite メインDB
  models/        # 訓練済みモデル (.pkl)
  models/history/ # モデル世代管理（直近10世代）
web/             # Next.js フロントエンド（ダークUI）
.claude/
  skills/        # エージェントが参照するドメイン知識
  agents/        # Subagent の役割定義
```

### 主要テーブル

| テーブル | 説明 |
|---|---|
| `races` | レース基本情報 |
| `race_results` | 出走・着順結果 |
| `race_payouts` | 確定払戻 |
| `horses` | 馬マスタ（血統 sire/dam/dam_sire） |
| `racehorses` | 競走馬マスタ DIFN:UM |
| `jockeys` | 騎手マスタ DIFN:KS |
| `trainers` | 調教師マスタ DIFN:CH |
| `breeding_horses` | 繁殖馬マスタ BLOD:BT |
| `training_times` | 調教タイム WOOD:TC |
| `training_hillwork` | 坂路調教 WOOD:HC |
| `v_race_mart` | AI学習用フラットビュー（63列・全テーブル結合済） |
| `predictions` | 予想バッチ |
| `prediction_results` | 的中・払戻実績 |

### 応答言語

**日本語**（コード・変数名は英語、コメント・説明は日本語）


---

## 開発ルール（追加）

### 5. Python バージョン・型ヒント

- **Python 3.11+** を前提とする。`match` 文・`tomllib` 等の新機能を積極活用してよい。
- 全関数・メソッドに `typing` による型ヒント必須（引数・戻り値ともに）。

### 6. Windows UTF-8 強制

- 標準出力・ファイル読み書き時は必ず UTF-8 を指定すること。
  - `open()` → `open(..., encoding="utf-8")`
  - `subprocess` → `subprocess.run(..., encoding="utf-8")`
  - スクリプト先頭で `sys.stdout.reconfigure(encoding="utf-8")` を推奨。

### 7. テスト・フォーマット

- テスト実行: `pytest`
- コード整形: `ruff format .`
- CI前にこの2コマンドが通ることを確認すること。

### 8. DB 大規模操作の事前承認

- `umalogi.db` に対して大規模な `DELETE` / `DROP TABLE` / `TRUNCATE` を実行する前に、
  必ず**影響行数・テーブル・リカバリ手段**を報告し、ユーザーの明示的な許可を得ること。

### 9. セキュリティ: APIキー・DB接続情報の管理

- ⚠️ MCPサーバーの設定（`.claude/mcp.json`）やソースコード内に、APIキーやDB接続情報を**絶対にハードコードしないこと**。
- 必ず `${ENV_VAR}` などの環境変数を参照する形をとること。
- `.env` ファイルは `.gitignore` に含めること。Git 履歴にシークレットが混入した場合は即座に報告すること。
