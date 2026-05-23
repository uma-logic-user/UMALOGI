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
| `docs/7_weakness_ledger.md` | 弱点・技術的負債・未実装機能の記録と進捗更新 |

### 更新フォーマット（Changelog エントリ）

```markdown
| YYYY-MM-DD | 変更内容の要約（1行）。影響ファイル: src/... |
```

---

## ⚠️ Day2 本番運用 — 絶対行動規範（2026-05-11 策定）

> **以下の3条項は UMALOGI が本番稼働フェーズへ移行した時点から永続的に適用される。**
> **いかなる理由があっても、これらのルールを無断で破ることは許可されない。**

### 条項1: 予測データ不変性の担保

```
過去レースの predictions テーブルのレコードは、モデルを変更・再学習しても
絶対に UPDATE・DELETE・上書きを行ってはならない。

【許可】新しい race_id に対する INSERT
【禁止】既存 prediction_id に対するすべての UPDATE / DELETE
【禁止】--all フラグ等で過去レースの予想を再生成して上書きすること

理由: Discord 通知済みの予想と DB 記録が乖離すると、
      回収率・EV の事後検証が完全に無効化されるため。
```

### 条項2: 平日改修・週末凍結ルール

```
【月〜金】 新機能追加・リファクタリング・モデル変更・スキーマ変更 → OK
【土・日】 稼働と的中通知（Hit Flash）に専念する。
           実施可能: バグ修正（当日の予想・結果取得に影響するクリティカルのみ）
           実施禁止: 新機能追加 / 大規模リファクタ / DB スキーマ変更 /
                     モデル再学習 / 本番スクリプトの挙動変更

例外: 当日のレース取得・Hit Flash 送信が完全に停止した場合のみ
      最小限のホットフィックスを許可する（影響範囲を最小化すること）。
```

### 条項3: docs/ 完全同期（条項1を補完）

ALPHA / 卍 / 本命 モデルのロジック・特徴量・データ取得ルールを変更した場合、
必ず該当 `docs/` ファイルの Changelog に **日付・変更内容・影響ファイル** を記録すること。
ドキュメントとコードの乖離は「技術的負債」ではなく「障害」として扱う。

### 条項4: DB 物理削除禁止 ＆ 作業前バックアップ義務（2026-05-16 策定）

```
【原則】DBのレコードは物理削除（DELETE / DROP TABLE）を禁止する。
        変更が必要な場合は UPDATE による上書き、または is_deleted フラグ等の論理削除のみ許可。

【例外】ゴミデータの削除は影響行数・リカバリ手段を社長に報告し、明示的な許可を得ること。

【作業前バックアップ義務】
  DBスキーマ変更・大規模データ操作・モデル再学習を行う前に、必ず以下のいずれかを実行すること:
  1. data/backups/ に日付入りバックアップを作成:
     cp data/umalogi.db data/backups/umalogi_$(date +%Y%m%d_%H%M%S).db
  2. または git stash / git commit で作業前状態を保存する。

【事故事例（2026-05-16）】
  predictions テーブルは無事だったが、/api/predictions の limit=1000 が的中実績の全件表示を
  阻害し「データ消失」に見えた。根本原因の調査なしに DELETE/リストアを実施すると
  正常なデータを破壊する恐れがある。必ず現状調査（COUNT確認・バックアップ比較）を先に行うこと。

【事故事例（2026-05-17）】
  「的中実績がごっそり消えた」との報告が上がった。調査の結果:
  - predictions: 8,225件 (2026-04-11〜05-17) → 正常存在
  - prediction_results.is_hit=1: 782件 → 正常存在
  - /api/hits SQL (WHERE is_hit=1 + NO LIMIT) → 782件返却確認済み
  実際の原因: Next.jsサーバーが起動していなかった（または旧ビルドで稼働中）。
  DB データは完全無損傷。データ削除の事実なし。
  教訓: 「UIに出ない = データ消失」は誤り。必ずDB側を直接確認してから判断すること。
         py -c "import sqlite3; con=sqlite3.connect('data/umalogi.db'); print(con.execute('SELECT COUNT(*) FROM predictions').fetchone())"
         を実行してDBを直接確認する手順をまず踏むこと。

【Next.jsサーバー障害時のチェックリスト】
  1. DBのCOUNT確認（上記コマンド）
  2. ポート3000の疎通確認
  3. サーバー再起動: cd web && npm start
  4. ビルドが古い場合のみ再ビルド: cd web && npm run build && npm start
```

### 条項5: 弱点管理・改善トラッキングの恒久ルール（2026-05-18 策定）

```
【弱点の記録義務】
  システムの弱点・技術的負債・未実装機能を指摘された場合、または作業中に
  新たな問題を発見した場合は、必ず docs/7_weakness_ledger.md に記録すること。
  記録なしに「後で対応する」という対応は絶対に許されない。

【作業開始前の弱点確認義務】
  新たな開発指示を受けた際は、実装前に必ず docs/7_weakness_ledger.md を開き、
  以下を確認してから着手すること：
  1. 今回の作業で改善される弱点はあるか → ステータスを「完了」に更新
  2. 今回の作業で新たに発生する負債はないか → 追記
  3. 過去の弱点が意図せず悪化していないか → チェック

【台帳のフォーマット（必須）】
  各弱点エントリには以下を必ず記載：
  - ID (W-NNN 形式)
  - ステータス (🔴未着手/🟡対応中/🟢完了/⚪保留)
  - 優先度 (高/中/低)
  - 影響内容
  - 対応方針
  - 担当フェーズ

【完了の定義】
  「完了」は本番反映かつ E2E 検証または数値的改善が確認された場合のみ。
  コードが書かれただけでは「対応中」のまま維持すること。
```

---

## ⚠️ 最重要ルール：ドキュメント保守の絶対遵守

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

---

### 17. JVLink ダイアログ自動突破ハンドラー（2026-05-23 策定）

`scheduler.py` が起動すると、`src/ops/jvlink_dialog_handler.py` が **daemon スレッド** として自動起動し、
JVLink / 設定 / セットアップ系ダイアログを **0.3 秒以内** に自動クリックして消去する。

#### アーキテクチャ（三重安全網）

```
① COM フラグによるダイアログ生成抑制（_run_jvlink 内 SW_HIDE / CREATE_NO_WINDOW）
     ↓ 抑制しきれなかった場合
② JVLinkDialogHandler（scheduler daemon スレッド）
   - 0.3 秒間隔で EnumWindows → タイトルパターン照合
   - 検出したら BM_CLICK → WM_COMMAND IDOK → VK_RETURN の優先順で自動消去
   - 同一 hwnd への連打防止クールダウン 1.5 秒
   - 3 秒超残存で「頑固なダイアログ」として WARNING ログ
     ↓ 3 秒超残存した場合
③ _run_jvlink の 10 秒タイムアウト → Kill → netkeiba フォールバック
```

#### 検出ターゲットタイトルパターン

`jvlink` / `jra-van` / `jravan` / `jvdtlab` / `設定` / `セットアップ` /
`setup` / `target frontier` / `認証` / `ライセンス` / `license` / `使用許諾` /
`更新` / `アップデート` / `update` / `競馬データ` / `jvlink viewer`

#### ファイル

| ファイル | 役割 |
|---------|------|
| `src/ops/jvlink_dialog_handler.py` | ハンドラー本体（`start_dialog_handler()` / `stop_dialog_handler()`） |
| `scripts/scheduler.py:run_daemon()` | daemon スレッドとして起動（`start_dialog_handler(interval=0.3)`） |
| `tests/test_jvlink_dialog_handler.py` | 26 件のユニットテスト（win32 stub 使用） |

#### 注意事項

- `pywin32` が必須。未インストール環境ではダミースレッドで無害スキップ。
- **テスト用停止**: `from src.ops.jvlink_dialog_handler import stop_dialog_handler; stop_dialog_handler()` で停止可能。
- ダイアログ消去統計は `src.ops.jvlink_dialog_handler.stats` dict で参照できる。

---

### 16. 日本語エンコーディングの絶対遵守（2026-05-15 策定）

> **あらゆる入力ソース（JRA-VAN / JVLink, netkeiba, X/Twitter, 外部 API 等）から**
> **取得したデータは、DB 保存前に必ず UTF-8 であることを保証すること。**
> **Shift-JIS / CP932 / EUC-JP の生バイト列を変換せずに放置することは絶対禁止。**

#### 根拠
2026-05-15 調査で判明した文字化けパターン:
- `netkeiba.py` が EUC-JP 固定デコードを使用していたため、`db.netkeiba.com` の
  血統ページが Mac-Greek / Mac-Roman 等に誤検知され父馬・母馬名が化けていた。
- JVLink COM BSTR の CP932 リードバイト（U+0081-U+009F = C1 制御文字域）が
  `errors='replace'` で `?`（0x3F）に置換され `?A?h?}?C...` パターンが生成されていた。

#### 実装ルール（実施済み）

```
1. HTTP レスポンスのエンコーディング検知
   - Content-Type ヘッダーに charset が明示 → そのまま使用
   - 不明 → apparent_encoding を参照
   - apparent_encoding が 'mac'/'greek'/'iso-8859-7' 等を返した場合 → euc-jp にフォールバック
   - ソース: src/scraper/netkeiba.py:_detect_encoding()

2. DB 挿入前バリデーション
   - 血統情報 (sire/dam/dam_sire) 等のテキストフィールドは
     src/utils/text:ensure_clean() を必ず通すこと
   - ensure_clean() は文字化け検知・回復・最終ゲートの3層構造
   - ソース: src/database/init_db.py の horses INSERT 箇所

3. 文字化け検知 (src/utils/text:is_garbled)
   - ギリシャ文字/キリル文字の連続: Mac-Greek/Cyrillic 誤変換の痕跡
   - '?A?h?}' スタイルの ?X 繰り返し: JVLink CP932 リードバイト欠損の痕跡
   - 稀漢字 (窿/噬/穢 等) の出現: 不正な回復処理の痕跡
   - 上記のいずれかを検知したら ensure_clean() がフォールバック処理を起動

4. 定期クレンジング
   - scripts/cleanup_encoding.py を使って全テーブルをスキャン可能
   - 2026-05-15 実績: 7,562 件の文字化けを修正（racehorses 7,547 / races 15）

5. Web UI での文字化け表示の絶対禁止（2026-05-16 追加）
   - Next.js の /api/* ハンドラーは、DB から取得した文字列フィールドを
     レスポンスに含める前に文字化けチェックを実施すること。
   - チェック方法: Python 側の is_garbled() と同等のパターンを TypeScript で実装するか、
     あるいは API 呼び出し先の Python サービスで ensure_clean() を通した値のみを返すこと。
   - 文字化けを検知した場合の動作:
     a) 即座に scripts/cleanup_encoding.py を実行して DB 側を修復する
     b) レスポンスには空文字 "" または "（データ修復中）" を返す（文字化け文字列を絶対に返さない）
     c) システムログ・Discord #system チャンネルにアラートを送信する
   - 対象フィールド（最低限）: horse_name, jockey, trainer, race_name, sex_age
   - 判定パターン（TypeScript 簡易版）:
     /(\?[\x21-\x7e]){2,}/.test(s)   // JVLink ?X?X パターン
     || /[\\uFF61-\\uFF9F]/.test(s)   // 半角カタカナ混在
     || /[\\u0370-\\u03FF]{2,}/.test(s) // ギリシャ文字連続
```

---

## 次フェーズ ロードマップ（Ver2.0 候補）

> ⚠️ **重要な認識修正（2026-05-12 社長指令）**
> 「X（Twitter）連携」とは**自動ポスト機能ではない**。
> X 上で活動する凄腕予想家のポストをスクレイピングし、
> UMALOGI の **第4のファクター** として DB に取り込む戦略である。

### 12. X 予想データ抽出・構造化パイプライン（最優先）

**目的**: 一流競馬予想家の「印（◎〇▲）」「軸馬番号」「レース名」を
X 投稿から機械的に抽出し、`x_signals` テーブルに格納。
ALPHA/卍/本命の EV 計算時に **"専門家コンセンサス係数"** として加算する。

```
【アーキテクチャ案】

1. ターゲット選定
   - フォロワー1万人超・的中実績公開の凄腕予想家アカウントを選定
   - scripts/x_targets.json にアカウントリストを管理

2. スクレイピング（src/scraper/x_scraper.py）
   - Playwright + stealth-mode で X 検索・タイムラインを巡回
   - レース開催日の 前夜〜当日朝 の投稿を対象
   - 取得項目: tweet_id / posted_at / screen_name / raw_text

3. 構造化（src/ml/x_signal_parser.py）
   - 正規表現 + LLM（Claude Haiku）でテキスト → 構造化
     - race_name: "東京11R" / "NHKマイルC"
     - horse_nums: [5, 9, 3]
     - signal_type: "本命" / "穴" / "消し"
     - confidence: 0.0〜1.0（言語確信度から推定）

4. DB 格納（x_signals テーブル）
   schema:
     signal_id     INTEGER PRIMARY KEY
     race_id       TEXT     (races テーブルと突合)
     screen_name   TEXT
     horse_number  INTEGER
     signal_type   TEXT     ('honmei' / 'ana' / 'keshi')
     confidence    REAL
     raw_text      TEXT
     posted_at     TEXT
     fetched_at    TEXT

5. EV 計算への統合（src/ml/alpha_model.py）
   - x_consensus_score = weighted_avg(confidence) by horse_number
   - FEATURE_COLS に追加: 'x_consensus_score'
   - 重みは「アカウントの過去的中率」で動的調整

【実装優先順位】
  Phase A: x_scraper.py（Playwright）+ x_signals テーブル作成
  Phase B: x_signal_parser.py（Claude Haiku API で構造化）
  Phase C: FEATURE_COLS への統合・モデル再訓練・バックテスト
```

**実装開始前の必須確認事項**:
- X の利用規約（スクレイピング制限）の確認
- レート制限回避（1アカウントあたり 1時間あたり 15リクエスト以下）
- 個人情報保護: 収集データは非公開 DB のみに格納し、外部公開禁止

### 13. FukushoElite モデル本番統合

- 現状: `src/ml/` に実装済みだが本番パイプラインに未結合
- 複勝 ROI 95.4% → 110%+ 目標（X シグナル統合後に再訓練）

### 14. 歴史データ大規模取得（SID 制約解消後）

- JVLink SID が1日分以上のデータ取得に対応した時点で実行
- 2023〜2025 の 3年分データで ALPHA モデル再訓練 → ROI 250%+ 目標

### 15. WIN5 結果取得ソース移行計画（Plan B: JVLink 化）

**現状 (Plan A):** `scripts/fetch_win5_result.py` が netkeiba から WIN5 払戻・的中馬番を
スクレイピングして `win5_results` テーブルに保存する。

**将来移行先 (Plan B):** JVLink SID/32bit 制約が解消された時点で、
WIN5 公式結果データ（JVLink RACE データスペック内の払戻レコード）を直接取得する方式に切り替える。

```
移行条件:
  - JVLink SID がリアルタイム当日データの全取得に対応
  - 32bit Python COM 呼び出し不要の 64bit JVLink SDK（または代替 API）が利用可能

移行ファイル:
  - scripts/fetch_win5_result.py  → JVLink RTD / RACE 払戻パーサーを利用する実装に変更
  - src/scraper/rtd_reader.py     → WIN5 払戻レコードのパース拡張

移行後のメリット:
  - netkeiba スクレイピング依存をゼロ化（利用規約リスク解消）
  - 払戻確定タイミングが JVLink RTD で自動通知 → 取得遅延なし
```
