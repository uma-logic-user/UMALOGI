# CLAUDE.md
<!-- Claude Code がこのリポジトリで作業する際に自動的に読み込む設定ファイル -->

---

## ⚠️ 最重要ルール：ドキュメント保守の絶対遵守

> **コードの追加・修正・バグフィックスを行った場合、作業終了時に必ず `docs/` ディレクトリ内の**
> **関連する Markdown ファイルを開き、一番上の「更新履歴」セクションに本日の日付で**
> **作業内容と仕様変更の事実を追記すること。ドキュメントとコードの乖離は絶対に許容されない。**

### 対象ドキュメントと担当領域

| ファイル | 更新が必要な作業 |
|---------|----------------|
| `docs/1_prediction_logic.md` | モデル変更・買い目ロジック変更・新戦略追加 |
| `docs/2_automation_schedule.md` | スケジューラ変更・新バッチ追加・タイミング変更 |
| `docs/3_data_schema.md` | DBスキーマ変更・データソース追加・取得ルール変更 |
| `docs/4_ui_design.md` | Discord通知レイアウト変更・ダッシュボード変更 |
| `docs/5_ml_roadmap.md` | 新モデル追加・特徴量変更・再学習ルール変更 |
| `docs/6_special_notes.md` | バグ修正・障害対応・手動リカバリ手順の追加 |

### 更新フォーマット（Changelog エントリ）

```markdown
| YYYY-MM-DD | 変更内容の要約（1行）。影響ファイル: src/... |
```

---


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

### 10. JVLink データ取得後の文字化けスクリーニング

JRA-VAN JVLink から取得したデータを DB に保存する際、**必ず文字化けスクリーニングを実施すること**。

#### 背景
JVLink COM は CP932 バイト列を Pattern 1（各バイトを U+0000-U+00FF の Unicode 文字として返す）または Pattern 2（正規の Unicode 日本語文字 U+3000+ として返す）で返す。両方が混在する場合、誤ったエンコード処理（`encode('cp932', errors='replace')`）を行うと C1 制御文字（U+0081, U+0083 等）が `?`（0x3F）に化け、レース名等が `?x???X?e?[?N?X` のように壊れる。

#### スクリーニング実装ルール

1. **`_to_bytes()` フォールバック**（`src/scraper/jravan_client.py`）  
   `encode('latin-1')` 失敗時は文字コードポイントで判定：
   - `ord(ch) <= 0xFF` → バイト値をそのまま使う（CP932 リードバイトを保持）
   - `ord(ch) >= 0x100` → `ch.encode('cp932')` を使う（Pattern 2 の正規 Unicode）

2. **保存前スクリーニング関数** `src/utils/text.py:sanitize_str()`  
   `[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]` にマッチする制御文字を除去する。  
   JVLink 由来の文字列は必ずこの関数を通してから DB に保存すること。

3. **文字化け検出チェック**  
   保存前に `?` (0x3F) が連続する文字列（例: `?x???X?`）は文字化けの兆候。  
   `_sanitize_check(s)` で以下を確認すること：
   ```python
   import re
   _GARBLED = re.compile(r'\?[^\s\?]{1,4}\?')  # ?X? パターン
   if _GARBLED.search(s):
       logger.warning("文字化け疑い: %r", s)
   ```

4. **DB 保存後の事後検証**  
   バッチ完了後に `races.race_name LIKE '%?%'` 等で残留文字化けを確認し、  
   文字化けが検出された場合は当該レコードを空文字にリセットして再取得を促す。

### 11. データ戦略: JVLink 一次・netkeiba 二次の二段構え

**原則**: JRA-VAN（JVLink）データは公式の真実であり、補完の最優先ソースとする。

#### レースエントリー / 出走情報
- **一次ソース**: JVLink （`src/scraper/jravan_client.py`）
- **二次ソース**: netkeiba (`src/scraper/entry_table.py`) ← JVLink 失敗時に自動フォールバック
- フォールバック実装箇所: `src/pipeline/prediction.py` の `_fetch_entries()` 付近

#### オッズ情報（直前予想で最重要）
- **一次ソース**: JVLink リアルタイムオッズ → `realtime_odds` テーブル経由 → `_latest_odds_map()`
- **二次ソース**: netkeiba オッズスクレイピング (`src/scraper/entry_table.py:fetch_odds_from_netkeiba()`)  
  ← `realtime_odds` が空（件数 = 0）の場合に自動フォールバックすること
- **必須実装**: `_latest_odds_map()` または直前予想パイプラインで、`realtime_odds` の該当 `race_id` の件数が 0 の場合、netkeiba からオッズを取得して一時的に使用する。

#### 確定結果 / 払戻
- **一次ソース**: JVLink RTD データ (`src/scraper/rtd_reader.py`)
- **二次ソース**: netkeiba 払戻ページ (`src/scraper/update_payouts.py`)
- JVLink が取得できなかった場合、当日中に netkeiba で補完すること。

#### ルール要約
```
オッズ取得: JVLink → realtime_odds 空 → netkeiba fetch_odds() → 再度 realtime_odds へ保存
エントリー: JVLink → 失敗 → netkeiba entry_table → entries テーブルへ保存
払戻:       JVLink RTD → 未取得 → netkeiba update_payouts → race_payouts へ保存
```
**「どちらかが死んでも必ず EV スコアが算出される状態」を維持すること。**
