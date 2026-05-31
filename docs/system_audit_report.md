# UMALOGI システム全体監査レポート

> **実施日**: 2026-05-02  
> **監査者**: Claude Code (AI Code Review)  
> **対象バージョン**: master ブランチ (commit 061e8ce)  
> **方針**: コード修正なし。問題点の特定と改善提案のみ。

---

## 深刻度サマリー

| 深刻度 | 件数 | 概要 |
|--------|------|----|
| 🔴 Critical | 4件 | DB整合性破壊・モデル推論誤り・クエリ全件スキャン |
| 🟠 High | 7件 | N+1クエリ・例外サプレッション・テスト不在など |
| 🟡 Medium | 6件 | DDL重複・ログ競合・APIキャッシュなしなど |
| 🟢 Low | 3件 | 将来的な技術的負債 |
| **合計** | **20件** | |

---

## 観点1: データパイプラインの堅牢性

### [1-1] `_str()` 内のベア例外サプレッション
- **ファイル**: `src/scraper/jravan_client.py` (376–381行)
- **深刻度**: 🔴 Critical
- **内容**: `except Exception: return ''` でデコード失敗を完全に無音化。どのフィールドが失敗したか、なぜ失敗したかのログが一切残らない。`errors='replace'` による文字化けも事前にあるため、問題が二重にマスクされる。
- **影響**: JV-Link から受信した馬名・騎手名・レース名が空文字列になっても気づけない。DBに空文字が蓄積しサイレントにモデル精度が劣化する。
- **改善案**:
  ```python
  # Before
  except Exception:
      return ''
  # After
  except Exception as e:
      logger.debug("_str decode failed sl=%s encoding=%s: %s", sl, encoding, e)
      return ''
  ```

---

### [1-2] `_to_bytes()` の `errors='replace'` による無音データ破壊
- **ファイル**: `src/scraper/jravan_client.py` (353–373行)
- **深刻度**: 🟠 High
- **内容**: `latin-1` デコード失敗時に `cp932` へフォールバックするが、フォールバック時も `errors='replace'` で `?` 置換を行う。置換が起きてもログに記録されない。今朝の文字化けトラブルの直接原因の一つ。
- **影響**: 馬名・調教師名などの日本語マルチバイト文字列が `?` で置換されたままDBに格納される。
- **改善案**: フォールバック時に `logger.debug("_to_bytes fallback: %r → %d bytes replaced", com_str[:20], replaced_count)` を追加し、置換カウンターで異常検知する。

---

### [1-3] `insert_entries()` のFK制約無効化がリーク
- **ファイル**: `src/database/init_db.py` (1577–1614行)
- **深刻度**: 🔴 Critical
- **内容**: `PRAGMA foreign_keys = OFF` は `with conn:` ブロック内 (1578行) で設定されるが、`PRAGMA foreign_keys = ON` の復元は `with conn:` の**外側**に置かれている。ループ中に例外が発生した場合、`with conn:` がロールバックして終了するが、FK制約が無効なままセッション全体に残る。
- **影響**: その後の全INSERT操作でFK整合性チェックが機能しなくなる。孤立レコードがサイレントに挿入され、参照整合性が崩壊する。
- **改善案**:
  ```python
  conn.execute("PRAGMA foreign_keys = OFF")
  try:
      # ... insert処理 ...
  finally:
      conn.execute("PRAGMA foreign_keys = ON")
  ```

---

### [1-4] `_migrate_relax_model_type_check()` での `writable_schema` 直接操作
- **ファイル**: `src/database/init_db.py` (852–910行)
- **深刻度**: 🔴 Critical
- **内容**: `PRAGMA writable_schema = ON` を使って `sqlite_master` テーブルを直接 `UPDATE` している。SQLite公式ドキュメントで「専門家のみ使用」と明記された危険なオペレーション。途中中断でDBファイルが永久破損する。
- **影響**: マイグレーション実行中のプロセス終了・電源断でDBが開けない状態になる可能性がある。本番DBバックアップなしで実行するとデータ全損リスクがある。
- **改善案**: `writable_schema` を使わず、`ALTER TABLE ... RENAME TO ...` + `CREATE TABLE ... AS SELECT ...` パターンでスキーマ再構築する。

---

### [1-5] `JVDataLoader.load()` の全件メモリ蓄積によるOOMリスク
- **ファイル**: `src/scraper/jravan_client.py` (1926–2005行)
- **深刻度**: 🟡 Medium
- **内容**: `records = []` に全レコードを蓄積してから一括 `save_records_to_db()` を呼ぶ。年度一括同期時には数十万レコードがRAMに展開される。
- **影響**: 大規模同期時にメモリ不足 (OOM) でクラッシュ。Windowsの32bitサブプロセス文脈では2GBアドレス空間制限が特に厳しい。
- **改善案**: バッチサイズ (例: 500件) 毎に `save_records_to_db()` を呼ぶストリーミング処理に変更する。

---

### [1-6] `_save_tc()` の UNIQUE 制約キーに `direction` が常に空文字
- **ファイル**: `src/scraper/jravan_client.py` (1507–1529行)
- **深刻度**: 🟡 Medium
- **内容**: `ON CONFLICT(horse_id, training_date, course_type, direction)` と宣言しているが、INSERT の VALUES で `direction` は常にデフォルト空文字列。実際には `direction` による重複排除が機能しない。
- **影響**: 同じ馬・同日・同コースの調教データが複数 INSERT される可能性があり、重複行でモデルの特徴量計算が歪む。

---

### [1-7] `update_payouts.py` のリトライなし・指数バックオフなし
- **ファイル**: `src/scraper/update_payouts.py` (121行付近)
- **深刻度**: 🟡 Medium
- **内容**: レース単位の失敗を `logger.warning()` のみで継続。リトライ機構なし。`delay=2.0` 固定でnetkeiba側の503/429エラーに対する指数バックオフなし。
- **影響**: 一時的なネットワーク障害で払戻データが欠損したまま処理が正常終了したように見える。欠損払戻は的中評価を誤らせる。
- **改善案**:
  ```python
  from tenacity import retry, stop_after_attempt, wait_exponential
  @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
  def _fetch_payout(race_id: str) -> dict: ...
  ```

---

### [1-8] 複数プロセスが同一ログファイルに `RotatingFileHandler` を持つ競合
- **ファイル**: `scripts/scheduler.py` (70–76行) / `scripts/today_auto_runner.py` (75行)
- **深刻度**: 🟡 Medium
- **内容**: 両プロセスが同じ `data/scheduler.log` に対して独立した `RotatingFileHandler` を持つ。Windowsではファイルローテーション時に他プロセスが開いているファイルのリネームが競合し、`PermissionError` またはログエントリの消失が起きる。
- **改善案**: `scheduler.log` / `auto_runner.log` でファイルを分離する、またはQueueHandlerによるプロセス間ログ集約を検討する。

---

### [1-9] `backup.py` の不完全なバックアップへのクラウド同期リスク
- **ファイル**: `src/ops/backup.py` (107–155行)
- **深刻度**: 🟢 Low
- **内容**: `_hot_backup()` が中断されてもクラウド同期 `_cloud_sync()` が走り、不完全なバックアップがクラウドに上書きされる可能性がある。完了フラグ/チェックサム検証なし。
- **改善案**: バックアップ完了後に MD5/SHA256 チェックサムを書き込み、クラウド同期前に検証する。

---

## 観点2: パフォーマンスとスケーラビリティ

### [2-1] `v_race_mart` ビューの相関サブクエリが全件スキャンを誘発
- **ファイル**: `src/database/init_db.py` (681–698行)
- **深刻度**: 🔴 Critical
- **内容**: `v_race_mart` ビュー内に `SELECT MAX(training_date) FROM training_times WHERE horse_id = rr.horse_id AND training_date < r.date` という相関サブクエリが存在する。race_results の行数分（数十万行規模）でサブクエリが毎回実行される。
- **影響**: フルスキャンによる学習データ生成が現実的な時間内に完了しない。10万行の `race_results` に対して10万回のサブクエリ実行となり、数時間以上かかる可能性がある。
- **改善案**: `v_race_mart` をマテリアライズドビュー相当のテーブル（`race_mart_cache`）に置き換え、差分更新で管理する。または `training_times` に `(horse_id, training_date)` の複合インデックスを追加し相関サブクエリのコストを下げる。

---

### [2-2] `/api/predictions` の N+1 クエリ問題
- **ファイル**: `web/src/app/api/predictions/route.ts` (55–62行)
- **深刻度**: 🟠 High
- **内容**: `preds.map()` のループ内で `getHorses.all(pd.prediction_id)` を呼んでいる。デフォルト `limit=1000` の場合、1リクエストで最大1000回の個別SQLが発行される。
- **影響**: APIレスポンスタイムが予想件数に比例して劣化。フロントエンドの初期ロードが顕著に遅延する。
- **改善案**:
  ```typescript
  // 全prediction_idをIN句でまとめて取得
  const predIds = preds.map(p => p.prediction_id as number)
  const ph = predIds.length > 0
    ? db.prepare(`SELECT * FROM prediction_horses WHERE prediction_id IN (${predIds.map(() => '?').join(',')})`)
        .all(...predIds)
    : []
  const horsesByPred = Map.groupBy(ph, h => h.prediction_id)
  ```

---

### [2-3] `FeatureBuilder` の1レース最大108クエリ
- **ファイル**: `src/ml/features.py` (502–718行)
- **深刻度**: 🟠 High
- **内容**: `_get_horse_stats()` が馬1頭あたり4クエリ、`_get_training_stats()` が2クエリを発行。`build_race_features_for_simulate()` でこれを出走馬数分ループするため、18頭立てで108クエリ/レース。バックテスト全レース処理時は数万クエリになる。
- **影響**: バックテスト実行時間が非現実的になる。特に `_get_training_stats()` は調教テーブルへの全件スキャンを複数回行う可能性がある。
- **改善案**: レース単位でバルク取得するメソッド `_get_horse_stats_bulk(horse_ids)` を実装し、IN句で一括取得してメモリ上でマッピングする。

---

### [2-4] LightGBMモデルの毎回ディスク読み込み
- **ファイル**: `src/main_pipeline.py` (744行) / `src/ml/models.py` (666–684行)
- **深刻度**: 🟠 High
- **内容**: `load_models()` がpklファイルをディスクから毎回 `pickle.load()` する。`prerace_pipeline()` 内で `_try_win5()` と本予想の2箇所から呼ばれ、同一レース予想中にモデルが複数回ロードされる可能性がある。
- **影響**: 直前予想バッチ実行のたびにI/O負荷が発生し処理時間が増大する。モデルのメモリキャッシュがなく、同一モデルが複数回ロードされる。
- **改善案**:
  ```python
  # モジュールレベルでキャッシュ
  _MODEL_CACHE: dict[str, object] = {}
  
  def load_models(model_dir: Path) -> dict[str, object]:
      if model_dir not in _MODEL_CACHE:
          _MODEL_CACHE[model_dir] = _do_load(model_dir)
      return _MODEL_CACHE[model_dir]
  ```

---

### [2-5] Next.js APIルートのキャッシュ設定なし
- **ファイル**: `web/src/app/api/summary/route.ts` / `web/src/app/api/financial/route.ts` (全体)
- **深刻度**: 🟡 Medium
- **内容**: `export const dynamic` も `export const revalidate` も設定されていない。重い集計クエリが毎リクエスト実行される。`summary` と `financial` は変化頻度が低い（レース終了後のみ更新）。
- **改善案**:
  ```typescript
  // 1時間キャッシュ (レース終了後に自動更新)
  export const revalidate = 3600
  ```

---

### [2-6] `/api/financial` の4回独立集計クエリ
- **ファイル**: `web/src/app/api/financial/route.ts` (119–263行)
- **深刻度**: 🟡 Medium
- **内容**: 日次・月次・年次・レース粒度の4集計クエリが独立実行される。同一データを粒度違いで4回フルスキャン。
- **改善案**: `WITH` 句 (CTE) で共通テーブルを一度だけスキャンし、各粒度でのGROUP BYをサブクエリとして記述する。

---

### [2-7] `getDb()` の WAL モード下でのシングルトン管理
- **ファイル**: `web/src/lib/db.ts` (全体)
- **深刻度**: 🟢 Low
- **内容**: `let _db: Database.Database | null = null` でシングルトン管理しているが、Next.js の Hot Reload 時に複数インスタンスが生成される可能性がある。開発環境では `global._db` に退避するパターンが推奨される。
- **影響**: 開発環境での動作不安定（本番では影響なし）。

---

## 観点3: アーキテクチャと保守性

### [3-1] Discord 通知が6箇所に分散、通知クラスが形骸化
- **ファイル**: `scripts/scheduler.py` / `scripts/today_auto_runner.py` / `src/main_pipeline.py` / `src/ops/backup.py` + 2箇所以上
- **深刻度**: 🟠 High
- **内容**: Discord webhook送信が `requests` / `urllib.request` を使う独立実装として6箇所に散在している。環境変数名も `DISCORD_WEBHOOK_URL` と `DISCORD_WEBHOOK` が混在。`src/notification/dispatcher.py` という正規の通知クラスが存在するにもかかわらず使われていない。
- **影響**: webhook URLの変更・フォーマット変更時に全6箇所を修正する必要がある。`today_auto_runner.py` の実装は `except Exception: pass` で完全無音失敗し、通知漏れを検知できない。
- **改善案**: 全 Discord 送信を `src/notification/discord_notifier.py` の `DiscordNotifier` に集約し、他モジュールは全てそれを `import` して使う。

---

### [3-2] `sanitize()` / `rowToObj()` / `BET_ORDER` が全APIファイルにコピペ
- **ファイル**: `web/src/app/api/races/route.ts` / `predictions/route.ts` / `financial/route.ts` / `summary/route.ts` / `gachi/route.ts` / `win5/route.ts`
- **深刻度**: 🟡 Medium
- **内容**: `sanitize()` 関数、`rowToObj()` 関数、`BET_ORDER` 定数が複数ファイルに重複定義。`web/src/lib/` ディレクトリが既に存在し共通ライブラリの置き場として適切だが、活用されていない（`validateResponse.ts` を本日新規追加したが、`sanitize/rowToObj` は未統合）。
- **改善案**: `web/src/lib/dbHelpers.ts` に共通関数を移動し、全ルートから import する。

---

### [3-3] `_sire_map` が非永続化でモデル学習・推論間で不一致
- **ファイル**: `src/ml/features.py` (78行, 729–738行)
- **深刻度**: 🔴 Critical
- **内容**: `FeatureBuilder` インスタンスが生成されるたびに `_sire_map = {}` から動的再構築される。「ディープインパクト」が学習時に整数 `5` にエンコードされても、推論時の新しいインスタンスでは `0` や別の値になりうる。`_encode_jockey()` / `_encode_trainer()` はDBから固定値を読むのに `_sire_map` だけ動的再構築という不整合がある。
- **影響**: 学習データと推論データで同じ種牡馬が異なる整数値として扱われ、モデルが誤った特徴量を受け取る。回収率・的中率の劣化がサイレントに起きる可能性がある。
- **改善案**: 学習時に `sire_map` を `data/models/label_encoders.pkl` に一緒に保存し、推論時は必ずその保存済みマップから復元する（`cascade/label_encoders.pkl` と同じアプローチ）。

---

### [3-4] `main_pipeline.py` のゴッドファイル化（1400行超）
- **ファイル**: `src/main_pipeline.py` (全体)
- **深刻度**: 🟠 High
- **内容**: 1400行超の単一ファイルに、Discord通知・WIN5予想・エントリー保存・払戻取得・特徴量生成・予測実行・評価・暫定予想が全て混在している。単一責任原則 (SRP) を完全に違反している。
- **影響**: 新機能追加時の影響範囲が不明確。コードレビューが困難。テストが書けない。
- **改善案**:
  ```
  src/
    pipeline/
      prerace.py       # 直前予想パイプライン
      postrace.py      # レース後評価パイプライン
      provisional.py   # 暫定予想パイプライン
      win5.py          # WIN5予想 (既存 src/ml/win5.py と統合)
      entry_sync.py    # 出馬表同期
  ```

---

### [3-5] DDL定義の二重管理 (`init_db.py` と `jravan_client.py`)
- **ファイル**: `src/scraper/jravan_client.py` (1714–1874行) / `src/database/init_db.py` (全体)
- **深刻度**: 🟡 Medium
- **内容**: `_TRAINING_DDL` と `_MASTER_DDL` が `jravan_client.py` 内で再定義されており、`init_db.py` の `DDL_STATEMENTS` と二重管理になっている。どちらが権威あるスキーマ定義か不明確。
- **影響**: スキーマ変更時に両ファイルを更新しないと定義が乖離する。新規カラム追加がどちらかのDDLにしか反映されず、環境によって異なるスキーマになる。
- **改善案**: DDL定義を `src/database/schema.py` に一元化し、両ファイルが import して使う。

---

### [3-6] `today_auto_runner.py` の `subprocess.run()` にタイムアウトなし
- **ファイル**: `scripts/today_auto_runner.py` (182–198行)
- **深刻度**: 🟠 High
- **内容**: `_run_prerace()` と `_run_fetch_result()` の `subprocess.run()` に `timeout` パラメータがない。JV-Linkへの接続が無応答になった場合、プロセスが永久ブロックする。
- **影響**: 予想バッチプロセスがハングアップした場合、`today_auto_runner.py` 自体が永久ブロックし、スケジュールされた次のレース予想が実行されない。
- **改善案**:
  ```python
  result = subprocess.run(cmd, timeout=300, encoding='utf-8', ...)
  ```

---

### [3-7] テスト不在 — パイプラインの中核が無防備
- **ファイル**: `tests/` (全体)
- **深刻度**: 🟠 High
- **内容**: `tests/` に10個のテストファイルが存在するが、`main_pipeline.py`（最重要ファイル）、`scheduler.py`、`today_auto_runner.py` に対応するテストが一切ない。これらのファイルは全データフローの中心的オーケストレーターである。
- **影響**: パイプラインの回帰テストが不可能。リファクタリング・機能追加時に副作用を検知できない。
- **改善案**:
  - `prerace_pipeline()` の結合テスト（モックDB使用）を追加
  - `bet_generator.py` の全券種生成テストを追加
  - CI/CD で `pytest` を自動実行する

---

### [3-8] `_sanitize_for_discord()` が `sanitize_str()` を未使用
- **ファイル**: `src/main_pipeline.py` (103–105行) / `src/utils/text.py` (17–21行)
- **深刻度**: 🟢 Low
- **内容**: `main_pipeline.py` の `_sanitize_for_discord()` は `\x00` のみ除去する。`src/utils/text.py` に `_CTRL_RE` を使う `sanitize_str()` が存在するが利用されていない。
- **影響**: Discord通知に `\x01`〜`\x1f` の制御文字が混入した場合、Discord APIエラーまたは通知メッセージの文字化けが起きる。

---

### [3-9] `scheduler.py` の 32bit Python バージョンハードコード
- **ファイル**: `scripts/scheduler.py` (94行付近)
- **深刻度**: 🟡 Medium
- **内容**: `["py", "-3.14-32"]` で32bit Pythonのバージョンが具体的にハードコードされている。Python 3.14は執筆時点では開発中のバージョンであり、環境によっては存在しない。
- **影響**: JV-Linkサブプロセスが起動しない場合、全データ取得が無音で失敗する。
- **改善案**: `PY32_CMD = os.getenv("PY32_CMD", "py -3.13-32")` のように環境変数化する。

---

### [3-10] `backup.py` のクラウドパスのハードコード
- **ファイル**: `src/ops/backup.py` (97行)
- **深刻度**: 🟢 Low
- **内容**: `r"G:\マイドライブ\UMALOGI_backup"` がデフォルト値としてハードコードされている。このドライブレターは特定の実行環境にのみ存在する。
- **影響**: 別マシンや別Googleドライブマウントポイントでの実行時にバックアップが失敗する。
- **改善案**: `.env` に `CLOUD_BACKUP_PATH` を追加し、必須環境変数として起動時に検証する。

---

## 優先対応ロードマップ

### Phase 1 — 今週中（本番DB保護）
| 優先 | 対象 | 工数 |
|------|------|------|
| 1 | [1-3] FK制約 `finally` ブロックで確実に復元 | 30分 |
| 2 | [1-4] `writable_schema` 操作を安全なDDL再構築に置き換え | 2時間 |
| 3 | [3-3] `_sire_map` を `label_encoders.pkl` に永続化 | 2時間 |
| 4 | [3-6] `subprocess.run(timeout=300)` 追加 | 15分 |

### Phase 2 — 来週（パフォーマンス）
| 優先 | 対象 | 工数 |
|------|------|------|
| 5 | [2-1] `v_race_mart` を物理テーブル化（差分更新） | 1日 |
| 6 | [2-2] predictions N+1 → IN句一括取得 | 1時間 |
| 7 | [2-3] FeatureBuilder バルク取得メソッド実装 | 半日 |
| 8 | [2-4] LightGBMモデルキャッシュ実装 | 1時間 |

### Phase 3 — 今月中（アーキテクチャ整理）
| 優先 | 対象 | 工数 |
|------|------|------|
| 9 | [3-1] Discord通知を `DiscordNotifier` に集約 | 2時間 |
| 10 | [3-2] API共通関数を `web/src/lib/dbHelpers.ts` に抽出 | 1時間 |
| 11 | [3-4] `main_pipeline.py` を責務別にファイル分割 | 2日 |
| 12 | [3-5] DDL定義を `src/database/schema.py` に一元化 | 半日 |
| 13 | [3-7] `prerace_pipeline` の統合テスト追加 | 1日 |

---

## 付録: 設計改善の具体的ディレクトリ提案

```
src/
  database/
    schema.py          # DDL定義の一元管理 (新規)
    init_db.py         # スキーマ適用・マイグレーション
    queries.py         # 共通クエリヘルパー (新規)
  pipeline/            # main_pipeline.py の分割先 (新規)
    prerace.py
    postrace.py
    provisional.py
    entry_sync.py
  ml/
    features.py        # FeatureBuilderにバルク取得追加
    models.py          # モデルキャッシュ機構追加
    bet_generator.py
    win5.py
  notification/
    discord_notifier.py   # 全Discord送信の唯一の窓口
    dispatcher.py
  scraper/
    jravan_client.py   # エラーロギング強化
    update_payouts.py  # tenacity リトライ追加
  utils/
    text.py            # sanitize_str の統一適用
    encoding.py        # エンコーディング変換 (新規)
web/
  src/
    lib/
      db.ts
      dbHelpers.ts     # sanitize/rowToObj/BET_ORDER (新規)
      validateResponse.ts
```

---

*本レポートは 2026-05-02 時点のコードベースに基づく。コードの直接修正は含まない。*
