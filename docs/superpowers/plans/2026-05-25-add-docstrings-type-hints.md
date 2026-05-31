# Docstrings & Type Hints 一括追加 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `src/` 配下の全83ファイルにある全関数・クラス・メソッドへ Google スタイル Docstring と厳密な型ヒントを追加する。ロジックは一切変更しない。

**Architecture:** 83ファイルをモジュール単位で9バッチに分割し、各バッチを並列処理する。各エージェントは対象ファイルを Read → AST 解析 → 修正 → Edit/Write で保存する手順を踏む。

**Tech Stack:** Python typing / collections.abc、Google Docstring 形式、ruff によるフォーマット確認

---

## Google スタイル Docstring テンプレート

```python
def func(arg1: int, arg2: str = "") -> bool:
    """一行のサマリー（末尾ピリオド不要）。

    Args:
        arg1: 説明文。
        arg2: 説明文。デフォルトは空文字列。

    Returns:
        説明文。

    Raises:
        ValueError: 〇〇の場合に送出。
    """
```

**ルール:**
- サマリー行は1行目に収める（空行なし）
- 引数なし → `Args:` セクション省略
- 返り値 `None` → `Returns:` セクション省略
- 例外を送出しない → `Raises:` セクション省略
- 既存ドキュメントがある場合: Google スタイルに変換するだけ（内容は保持）

## 型ヒントルール

```
- 必須: 全引数・戻り値に型を付ける
- Python 3.11+: `list[str]` / `dict[str, int]` 小文字表記を優先
- Optional 引数: `str | None = None` 形式（`Optional[str]` は避ける）
- Union: `int | str` 形式（`Union[int, str]` は避ける）
- Any: `from typing import Any` で明示する
- 既存の型ヒントがある場合: そのまま維持（上書きしない）
```

---

## ファイル構成マップ

| バッチ | ファイル数 | 対象パス |
|--------|-----------|---------|
| Batch A | 6 | `src/__init__.py`, `src/run_scraper.py`, `src/simulate_year.py`, `src/main_pipeline.py`, `src/utils/text.py`, `src/utils/jravan_cli_initializer.py` |
| Batch B | 7 | `src/database/` 全ファイル |
| Batch C | 9 | `src/scraper/` 全ファイル |
| Batch D | 15 | `src/ml/` 全ファイル |
| Batch E | 9 | `src/notification/` 全ファイル |
| Batch F | 10 | `src/ops/` 全ファイル |
| Batch G | 7 | `src/pipeline/` 全ファイル |
| Batch H | 5 | `src/evaluation/` + `src/analysis/` 全ファイル |
| Batch I | 13 | `src/umasugi_engine/` 全ファイル（factors サブパッケージ含む） |

---

## Task 1: Batch A — utils/ + ルートファイル

**Files:**
- Modify: `src/__init__.py`
- Modify: `src/run_scraper.py`
- Modify: `src/simulate_year.py`
- Modify: `src/main_pipeline.py`
- Modify: `src/utils/text.py`
- Modify: `src/utils/jravan_cli_initializer.py`

- [ ] **Step 1: 各ファイルを読み込んで関数・クラスの一覧を把握する**

  各ファイルを Read ツールで読み込み、docstring・型ヒントが不足している箇所を特定する。

- [ ] **Step 2: `src/utils/text.py` を修正する**

  既存の docstring は概ね良好だが Google スタイルに統一する。型ヒントが不足している引数（特に戻り値）を追加する。

  ```python
  # 修正前の例
  def sanitize(v: object) -> object:
      """Remove control chars and strip. Pass through non-strings unchanged."""

  # 修正後の例（Google スタイル）
  def sanitize(v: object) -> object:
      """制御文字を除去してストリップする。文字列以外はそのまま返す。

      Args:
          v: 任意のオブジェクト。

      Returns:
          文字列の場合は制御文字除去・ストリップ済み文字列、それ以外は元の値。
      """
  ```

- [ ] **Step 3: `src/utils/jravan_cli_initializer.py` を修正する**

  Read で読み込み、全関数・クラスにドキュメントを追加する。

- [ ] **Step 4: `src/main_pipeline.py`, `src/run_scraper.py`, `src/simulate_year.py` を修正する**

  Read で読み込み、`main()` 関数等にドキュメントを追加する。

- [ ] **Step 5: `src/__init__.py` を確認する**

  モジュールレベル docstring がなければ追加する。

- [ ] **Step 6: ruff で構文チェックする**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/utils/ src/main_pipeline.py src/run_scraper.py src/simulate_year.py
  ```
  Expected: エラーなし（または型ヒント追加に無関係な既存警告のみ）

---

## Task 2: Batch B — database/

**Files:**
- Modify: `src/database/__init__.py`
- Modify: `src/database/schema.py`
- Modify: `src/database/init_db.py`
- Modify: `src/database/cleanup_old_data.py`
- Modify: `src/database/migrations/__init__.py`
- Modify: `src/database/migrations/add_training_grade.py`
- Modify: `src/database/migrations/add_odds_timeseries.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

  `init_db.py` は最大行数が多い可能性があるため `limit=200` などで分割して読む。

- [ ] **Step 2: `src/database/schema.py` を修正する**

  DDL 定数や関数があれば docstring を付与する。

- [ ] **Step 3: `src/database/init_db.py` を修正する**

  `init_db()`, `_migrate_*()` 等の全関数に docstring と型ヒントを追加する。

  ```python
  def init_db(db_path: str | Path = DB_PATH) -> sqlite3.Connection:
      """SQLite DB を初期化して接続を返す。

      スキーマが存在しない場合は DDL_STATEMENTS を実行して作成する。
      既存 DB にカラムが不足している場合は migration を自動実行する。

      Args:
          db_path: DB ファイルのパス。デフォルトは DB_PATH。

      Returns:
          初期化済みの sqlite3.Connection オブジェクト。
      """
  ```

- [ ] **Step 4: `src/database/cleanup_old_data.py`, migrations ファイル を修正する**

- [ ] **Step 5: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/database/
  ```

---

## Task 3: Batch C — scraper/

**Files:**
- Modify: `src/scraper/__init__.py`
- Modify: `src/scraper/jravan_client.py`
- Modify: `src/scraper/entry_table.py`
- Modify: `src/scraper/netkeiba.py`
- Modify: `src/scraper/rtd_reader.py`
- Modify: `src/scraper/update_payouts.py`
- Modify: `src/scraper/fetch_historical.py`
- Modify: `src/scraper/x_scraper.py`
- Modify: `src/scraper/training_scraper.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

  大きいファイルは `limit=200` で分割読み込みする。

- [ ] **Step 2: `src/scraper/jravan_client.py` を修正する**

  JVLink COM 操作の複雑な関数が多いため、以下のパターンで記述する：

  ```python
  def _run_jvlink(self, data_spec: str, start_key: str) -> list[str]:
      """JVLink COM 経由でデータを取得する。

      10秒タイムアウトで COM 呼び出しを実行し、
      ダイアログが出現した場合は自動的にクリックして消去する。

      Args:
          data_spec: JVLink データスペック文字列（例: "RACE"）。
          start_key: 取得開始キー（YYYYMMDD0000000000 形式）。

      Returns:
          取得したレコード文字列のリスト。COM 失敗時は空リスト。

      Raises:
          TimeoutError: 10秒以内に COM 呼び出しが完了しなかった場合。
      """
  ```

- [ ] **Step 3: `src/scraper/netkeiba.py`, `src/scraper/entry_table.py` を修正する**

- [ ] **Step 4: `src/scraper/rtd_reader.py`, `src/scraper/update_payouts.py` を修正する**

- [ ] **Step 5: `src/scraper/fetch_historical.py`, `src/scraper/x_scraper.py`, `src/scraper/training_scraper.py` を修正する**

- [ ] **Step 6: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/scraper/
  ```

---

## Task 4: Batch D — ml/

**Files:**
- Modify: `src/ml/__init__.py`
- Modify: `src/ml/features.py`
- Modify: `src/ml/models.py`
- Modify: `src/ml/models_v2.py`
- Modify: `src/ml/alpha_model.py`
- Modify: `src/ml/alpha_place_model.py`
- Modify: `src/ml/alpha_payout_model.py`
- Modify: `src/ml/bet_generator.py`
- Modify: `src/ml/data_validator.py`
- Modify: `src/ml/ev_features.py`
- Modify: `src/ml/incremental.py`
- Modify: `src/ml/narrative_generator.py`
- Modify: `src/ml/reconcile.py`
- Modify: `src/ml/u_score.py`
- Modify: `src/ml/win5.py`
- Modify: `src/ml/x_signal_parser.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

  `features.py`, `models.py`, `alpha_model.py` は特に大きいため分割読み込みする。

- [ ] **Step 2: `src/ml/features.py` の `FeatureBuilder` クラスを修正する**

  ```python
  class FeatureBuilder:
      """SQLite DB から機械学習用特徴量 DataFrame を生成するクラス。

      Attributes:
          conn: SQLite 接続オブジェクト。
          _sire_map: 父馬名 → ラベル整数のマッピング辞書。

      Example:
          conn = init_db(db_path)
          fb = FeatureBuilder(conn)
          df = fb.build(race_id="202604010801")
      """

  def build(self, race_id: str) -> pd.DataFrame:
      """指定レースの特徴量 DataFrame を構築して返す。

      Args:
          race_id: レース ID（YYYYMMDDJJRR 形式、例: "202604010801"）。

      Returns:
          出走馬を行、特徴量を列とする DataFrame。
          出走馬が存在しない場合は空の DataFrame を返す。
      """
  ```

- [ ] **Step 3: `src/ml/models.py`, `src/ml/models_v2.py` を修正する**

  LightGBM モデルの訓練・予測関数に型ヒントを追加する。

  ```python
  def train(
      X: pd.DataFrame,
      y: pd.Series,
      params: dict[str, Any] | None = None,
  ) -> lgb.Booster:
      """LightGBM モデルを訓練して返す。

      Args:
          X: 特徴量 DataFrame。
          y: 目的変数 Series（0/1 バイナリ）。
          params: LightGBM パラメータ辞書。None の場合はデフォルト値を使用。

      Returns:
          訓練済み LightGBM Booster オブジェクト。
      """
  ```

- [ ] **Step 4: `src/ml/bet_generator.py`, `src/ml/ev_features.py` を修正する**

- [ ] **Step 5: `src/ml/u_score.py`, `src/ml/alpha_model.py` 等を修正する**

- [ ] **Step 6: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/ml/
  ```

---

## Task 5: Batch E — notification/

**Files:**
- Modify: `src/notification/__init__.py`
- Modify: `src/notification/base.py`
- Modify: `src/notification/discord_bot.py`
- Modify: `src/notification/discord_notifier.py`
- Modify: `src/notification/dispatcher.py`
- Modify: `src/notification/image_builder.py`
- Modify: `src/notification/line_notifier.py`
- Modify: `src/notification/router.py`
- Modify: `src/notification/twitter_notifier.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

- [ ] **Step 2: `src/notification/base.py` の抽象基底クラスを修正する**

  ```python
  class BaseNotifier(ABC):
      """通知送信の抽象基底クラス。

      すべての通知チャンネル（Discord/LINE/X/note）はこのクラスを継承し、
      `send()` メソッドを実装しなければならない。
      """

  @abstractmethod
  def send(self, message: str, **kwargs: Any) -> bool:
      """通知を送信する。

      Args:
          message: 送信するメッセージ本文。
          **kwargs: チャンネル固有のオプション引数。

      Returns:
          送信成功時は True、失敗時は False。
      """
  ```

- [ ] **Step 3: `src/notification/discord_notifier.py`, `src/notification/discord_bot.py` を修正する**

- [ ] **Step 4: `src/notification/router.py`, `src/notification/dispatcher.py` を修正する**

- [ ] **Step 5: `src/notification/image_builder.py`, `src/notification/line_notifier.py`, `src/notification/twitter_notifier.py` を修正する**

- [ ] **Step 6: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/notification/
  ```

---

## Task 6: Batch F — ops/

**Files:**
- Modify: `src/ops/__init__.py`
- Modify: `src/ops/backup.py`
- Modify: `src/ops/data_sync.py`
- Modify: `src/ops/git_ops.py`
- Modify: `src/ops/jvlink_dialog_handler.py`
- Modify: `src/ops/note_draft_publisher.py`
- Modify: `src/ops/note_generator.py`
- Modify: `src/ops/retrain_trigger.py`
- Modify: `src/ops/umanity_uploader.py`
- Modify: `src/ops/win_report.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

- [ ] **Step 2: `src/ops/jvlink_dialog_handler.py` を修正する**

  Windows API 呼び出しを含む関数には特に詳細な docstring を追加する。

  ```python
  def start_dialog_handler(interval: float = 0.3) -> None:
      """JVLink ダイアログ自動突破ハンドラーを daemon スレッドとして起動する。

      EnumWindows でウィンドウを列挙し、JVLink/設定系ダイアログを
      検出したら BM_CLICK → WM_COMMAND IDOK → VK_RETURN の順に
      自動クリックして消去する。

      Args:
          interval: ダイアログ検索の実行間隔（秒）。デフォルトは 0.3 秒。

      Note:
          pywin32 が未インストールの環境ではダミースレッドで無害スキップする。
          停止には stop_dialog_handler() を呼び出す。
      """
  ```

- [ ] **Step 3: `src/ops/note_generator.py`, `src/ops/note_draft_publisher.py` を修正する**

- [ ] **Step 4: `src/ops/win_report.py`, `src/ops/retrain_trigger.py` を修正する**

- [ ] **Step 5: `src/ops/backup.py`, `src/ops/data_sync.py`, `src/ops/git_ops.py`, `src/ops/umanity_uploader.py` を修正する**

- [ ] **Step 6: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/ops/
  ```

---

## Task 7: Batch G — pipeline/

**Files:**
- Modify: `src/pipeline/__init__.py`
- Modify: `src/pipeline/_common.py`
- Modify: `src/pipeline/prediction.py`
- Modify: `src/pipeline/scraping.py`
- Modify: `src/pipeline/simulation.py`
- Modify: `src/pipeline/training.py`
- Modify: `src/pipeline/win5.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

- [ ] **Step 2: `src/pipeline/prediction.py` を修正する**

  メインパイプライン関数に詳細な docstring を追加する。

  ```python
  def run_prediction(race_id: str, db_path: str | Path = DB_PATH) -> dict[str, Any]:
      """単一レースの予測を実行して買い目辞書を返す。

      1. エントリー取得（JVLink → netkeiba フォールバック）
      2. FeatureBuilder で特徴量生成
      3. alpha_model / honmei_model で確率予測
      4. BetGenerator で買い目生成
      5. 結果を DB に保存

      Args:
          race_id: レース ID（YYYYMMDDJJRR 形式）。
          db_path: SQLite DB ファイルのパス。

      Returns:
          {'race_id': ..., 'bets': [...], 'ev_scores': {...}} 形式の辞書。

      Raises:
          ValueError: race_id の形式が不正な場合。
      """
  ```

- [ ] **Step 3: `src/pipeline/training.py`, `src/pipeline/simulation.py` を修正する**

- [ ] **Step 4: `src/pipeline/scraping.py`, `src/pipeline/win5.py`, `src/pipeline/_common.py` を修正する**

- [ ] **Step 5: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/pipeline/
  ```

---

## Task 8: Batch H — evaluation/ + analysis/

**Files:**
- Modify: `src/evaluation/__init__.py`
- Modify: `src/evaluation/evaluator.py`
- Modify: `src/analysis/alpha_backtest.py`
- Modify: `src/analysis/all_bets_backtest_2026.py`
- Modify: `src/analysis/walk_forward_backtest_2024_2025.py`
- Modify: `src/analysis/honmei_dynamic_backtest.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

- [ ] **Step 2: `src/evaluation/evaluator.py` を修正する**

  ```python
  def evaluate(
      race_id: str,
      bets: list[dict[str, Any]],
      conn: sqlite3.Connection,
  ) -> dict[str, Any]:
      """的中・払戻を評価して結果辞書を返す。

      同着・返還・競走中止の例外処理を行い、正確な払戻金額を算出する。

      Args:
          race_id: レース ID（YYYYMMDDJJRR 形式）。
          bets: 買い目リスト。各要素は {'bet_type': str, 'numbers': list[int], 'amount': int} 形式。
          conn: SQLite DB 接続オブジェクト。

      Returns:
          {'is_hit': bool, 'payout': int, 'roi': float} 形式の辞書。
      """
  ```

- [ ] **Step 3: analysis/ の各バックテストスクリプトを修正する**

- [ ] **Step 4: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/evaluation/ src/analysis/
  ```

---

## Task 9: Batch I — umasugi_engine/

**Files:**
- Modify: `src/umasugi_engine/__init__.py`
- Modify: `src/umasugi_engine/comparator.py`
- Modify: `src/umasugi_engine/engine.py`
- Modify: `src/umasugi_engine/ev_filter.py`
- Modify: `src/umasugi_engine/scorer.py`
- Modify: `src/umasugi_engine/factors/__init__.py`
- Modify: `src/umasugi_engine/factors/crowd_opinion.py`
- Modify: `src/umasugi_engine/factors/jockey_trainer.py`
- Modify: `src/umasugi_engine/factors/odds_momentum.py`
- Modify: `src/umasugi_engine/factors/paddock.py`
- Modify: `src/umasugi_engine/factors/track_style.py`
- Modify: `src/umasugi_engine/factors/training_grade.py`
- Modify: `src/umasugi_engine/factors/turf_type.py`

- [ ] **Step 1: 各ファイルを Read で読み込む**

- [ ] **Step 2: `src/umasugi_engine/engine.py` を修正する**

  メインエンジンクラスに詳細な docstring を追加する。

  ```python
  class UmasugiEngine:
      """UMASUGI スコアリングエンジン。

      18因子（A〜R グループ）を統合してレース内での各馬の相対スコアを算出し、
      EV フィルター適用後の買い目候補を返す。

      Attributes:
          factors: 登録済みファクターのリスト。
          conn: SQLite DB 接続オブジェクト。

      Example:
          engine = UmasugiEngine(conn)
          scores = engine.score(race_id="202604010801")
      """

  def score(self, race_id: str) -> pd.DataFrame:
      """指定レースの UMASUGI スコアを算出する。

      Args:
          race_id: レース ID（YYYYMMDDJJRR 形式）。

      Returns:
          馬番をインデックスとし、各ファクタースコアと総合スコアを列に持つ DataFrame。
      """
  ```

- [ ] **Step 3: `src/umasugi_engine/scorer.py`, `src/umasugi_engine/comparator.py`, `src/umasugi_engine/ev_filter.py` を修正する**

- [ ] **Step 4: `src/umasugi_engine/factors/` 配下の各ファクターファイルを修正する**

  各ファクターは同じインターフェースを持つので統一フォーマットを適用する：

  ```python
  class JockeyTrainerFactor:
      """騎手・調教師コース成績ファクター（グループ G）。

      指定レースの騎手・調教師について、同一競馬場・距離帯での
      過去勝率を算出し、スコアに変換する。

      Attributes:
          conn: SQLite DB 接続オブジェクト。
      """

  def compute(self, race_id: str) -> pd.Series:
      """騎手・調教師コース成績スコアを算出する。

      Args:
          race_id: レース ID（YYYYMMDDJJRR 形式）。

      Returns:
          馬番をインデックスとし、スコア（0.0〜1.0）を値とする Series。
      """
  ```

- [ ] **Step 5: ruff チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/umasugi_engine/
  ```

---

## Task 10: 最終検証

- [ ] **Step 1: ruff で全体チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m ruff check src/
  ```
  Expected: エラーなし（または docstring 関連以外の既存警告のみ）

- [ ] **Step 2: Python 構文チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -m py_compile src/utils/text.py src/database/init_db.py src/ml/features.py src/pipeline/prediction.py src/umasugi_engine/engine.py
  ```
  Expected: エラーなし（終了コード 0）

- [ ] **Step 3: import チェック**

  ```bash
  cd /c/dev/horse-racing-ai && python -c "import src.utils.text; import src.database.init_db; import src.ml.features; print('OK')"
  ```
  Expected: `OK`

- [ ] **Step 4: pytest 実行（既存テストが壊れていないことを確認）**

  ```bash
  cd /c/dev/horse-racing-ai && python -m pytest tests/ -x -q --timeout=30 2>&1 | head -50
  ```
  Expected: 既存テストがすべてパス

- [ ] **Step 5: git コミット**

  ```bash
  git add src/
  git commit -m "docs: Google スタイル Docstring と厳密な型ヒントを src/ 全体に追加"
  ```
