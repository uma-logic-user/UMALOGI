# Self-Healing Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** netkeiba 禁止を前提に、JRA-VAN データ欠損時の完全自動補完・リトライ・Graceful Degradation を実装し、手動介入ゼロのパイプラインを実現する。

**Architecture:**
- `scripts/infer_ranks_from_payouts.py` の `infer_ranks()` を `scheduler.py` の post_race / intraday_sync フローに組み込み、SEレコード未着でも払戻データから自動補完する。
- `scheduler.py` に Exponential Backoff リトライラッパー `_run_with_retry()` を追加し、JVLink 32bit 呼び出しを最大3回自動再試行する。
- `scraping.py` の `fetch_and_save_odds()` から netkeiba 依存を削除し RTD → DB フォールバック2段に整理。`prediction.py` のオッズ欠損時ハードストップを除去してフォールバック暫定モードに切り替える。

**Tech Stack:** Python 3.11+, SQLite, `schedule` ライブラリ, `scripts/infer_ranks_from_payouts.py` (既存), `src/ops/data_sync.py` (既存 3-Stage フォールバック)

---

## ファイル変更マップ

| ファイル | 変更内容 |
|---|---|
| `scripts/scheduler.py` | `_run_with_retry()` 追加・JVLink呼び出し置換・`job_post_race()` に infer_ranks ステップ追加・`job_intraday_sync()` に infer_ranks ステップ追加 |
| `src/ops/data_sync.py` | `sync_race_results()` の netkeiba Stage 4 フォールバックを `infer_ranks()` 呼び出しに置換 |
| `src/pipeline/scraping.py` | `fetch_and_save_odds()` から netkeiba Stage 1 を削除、DB既存オッズ確認を Stage 2 として追加 |
| `src/pipeline/prediction.py` | `prerace_pipeline()` のオッズ品質チェック失敗時を `{"skipped": True}` → 暫定モードフォールバックに変更 |
| `tests/test_self_healing.py` | 全3機能の単体テスト（新規作成） |

---

## Task 1: テストファイル骨格作成 + 失敗確認

**Files:**
- Create: `tests/test_self_healing.py`

- [ ] **Step 1: テストファイルを作成して失敗を確認する**

```python
"""自己修復パイプラインの単体テスト"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Task 2: infer_ranks 自動呼び出しテスト ─────────────────────────────────

def _make_test_db() -> sqlite3.Connection:
    """インメモリ DB にテスト用テーブルを作成して返す。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            date    TEXT,
            venue   TEXT DEFAULT '',
            race_number INTEGER DEFAULT 0
        );
        CREATE TABLE race_results (
            race_id      TEXT,
            horse_number INTEGER,
            rank         INTEGER
        );
        CREATE TABLE race_payouts (
            race_id     TEXT,
            bet_type    TEXT,
            combination TEXT,
            payout      INTEGER
        );
    """)
    return conn


def test_infer_ranks_fills_missing_rank_from_sanrentan() -> None:
    """三連単払戻から rank=1,2,3 が補完されることを確認する。"""
    from scripts.infer_ranks_from_payouts import infer_ranks

    conn = _make_test_db()
    conn.execute("INSERT INTO races VALUES ('R001', '2026-05-03', '東京', 1)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 3, NULL)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 7, NULL)")
    conn.execute("INSERT INTO race_results VALUES ('R001', 12, NULL)")
    conn.execute("INSERT INTO race_payouts VALUES ('R001', '三連単', '3→7→12', 15000)")
    conn.commit()

    stats = infer_ranks(conn, year_filter=None, dry_run=False)

    row = conn.execute(
        "SELECT horse_number, rank FROM race_results WHERE race_id='R001' ORDER BY horse_number"
    ).fetchall()
    assert {(3, 1), (7, 2), (12, 3)} == set(row)
    assert stats["rank1_set"] == 1


def test_infer_ranks_dry_run_does_not_write() -> None:
    """dry_run=True のとき DB を変更しないことを確認する。"""
    from scripts.infer_ranks_from_payouts import infer_ranks

    conn = _make_test_db()
    conn.execute("INSERT INTO races VALUES ('R002', '2026-05-03', '東京', 2)")
    conn.execute("INSERT INTO race_results VALUES ('R002', 5, NULL)")
    conn.execute("INSERT INTO race_payouts VALUES ('R002', '単勝', '5', 350)")
    conn.commit()

    infer_ranks(conn, year_filter=None, dry_run=True)

    rank_val = conn.execute(
        "SELECT rank FROM race_results WHERE race_id='R002'"
    ).fetchone()[0]
    assert rank_val is None


# ─── Task 3: _run_with_retry リトライテスト ──────────────────────────────────

def test_run_with_retry_succeeds_on_second_attempt() -> None:
    """1回失敗した後に2回目で成功する場合、最終的に 0 を返すことを確認する。"""
    from scripts.scheduler import _run_with_retry

    call_count = 0

    def fake_run(cmd: list, label: str, timeout: int = 3600) -> int:
        nonlocal call_count
        call_count += 1
        return 1 if call_count < 2 else 0

    with patch("scripts.scheduler._run", side_effect=fake_run):
        rc = _run_with_retry(["echo", "test"], "テスト", max_retries=3, base_delay=0)

    assert rc == 0
    assert call_count == 2


def test_run_with_retry_exhausts_all_retries() -> None:
    """全リトライ失敗時に非ゼロ rc を返すことを確認する。"""
    from scripts.scheduler import _run_with_retry

    with patch("scripts.scheduler._run", return_value=1):
        rc = _run_with_retry(["echo", "fail"], "テスト", max_retries=2, base_delay=0)

    assert rc != 0


# ─── Task 4: Graceful Degradation テスト ────────────────────────────────────

def test_fetch_and_save_odds_falls_back_to_db_existing() -> None:
    """RTD 失敗時に DB 既存オッズ件数を返すことを確認する。"""
    from src.pipeline.scraping import fetch_and_save_odds

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE entries (race_id TEXT, horse_number INTEGER, horse_name TEXT);
        CREATE TABLE realtime_odds (
            race_id TEXT, horse_number INTEGER,
            horse_name TEXT, win_odds REAL,
            fetched_at TEXT
        );
    """)
    conn.execute("INSERT INTO entries VALUES ('R003', 1, 'テスト馬')")
    conn.execute("INSERT INTO realtime_odds VALUES ('R003', 1, 'テスト馬', 3.5, '2026-05-03 10:00')")
    conn.commit()

    with patch("src.pipeline.scraping._fetch_odds_rtd", return_value=[]):
        result = fetch_and_save_odds(conn, "R003")

    assert result >= 1


def test_prerace_pipeline_does_not_skip_on_odds_failure() -> None:
    """オッズ取得ゼロでも skipped=True を返さず予測を続行することを確認する。"""
    from src.pipeline.prediction import prerace_pipeline

    with (
        patch("src.pipeline.prediction.init_db") as mock_db,
        patch("src.pipeline.prediction.FeatureBuilder") as mock_fb,
        patch("src.pipeline.prediction.load_models") as mock_lm,
        patch("src.pipeline.prediction.BetGenerator") as mock_bg,
        patch("src.pipeline.prediction.fetch_and_save_odds", return_value=0),
        patch("src.pipeline.prediction._save_predictions", return_value={}),
        patch("src.pipeline.prediction.try_win5"),
        patch("src.pipeline.prediction.save_json"),
        patch("src.pipeline.prediction.build_output_json", return_value={}),
    ):
        import pandas as pd
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        mock_db.return_value = mock_conn

        df = pd.DataFrame({
            "horse_number": [1, 2],
            "win_odds": [None, None],
            "horse_weight": [500, 490],
        })
        mock_fb.return_value.build_race_features.return_value = df

        import numpy as np
        mock_lm.return_value = (MagicMock(
            predict=lambda x: pd.Series([0.6, 0.4]),
            ev_predict=lambda x: pd.Series([1.1, 0.9]),
        ), MagicMock(ev_score=lambda x: pd.Series([1.1, 0.9])))
        mock_bg.return_value.generate_honmei.return_value = MagicMock(bets=[])
        mock_bg.return_value.generate_manji.return_value = MagicMock(bets=[])
        mock_bg.return_value.generate_oracle.return_value = MagicMock(bets=[])

        result = prerace_pipeline("202605030501", provisional=False)

    assert result.get("skipped") is not True
```

- [ ] **Step 2: テスト実行で失敗を確認する**

```
pytest tests/test_self_healing.py -v 2>&1 | head -50
```

期待: `ImportError` か `AssertionError` で FAILED（まだ実装がないため）

- [ ] **Step 3: コミット（スケルトンのみ）**

```bash
git add tests/test_self_healing.py
git commit -m "test: 自己修復パイプライン テストスケルトン追加"
```

---

## Task 2: `_run_with_retry()` を scheduler.py に追加

**Files:**
- Modify: `scripts/scheduler.py:99-134` (`_run()` の直後に追加)

- [ ] **Step 1: `_run_with_retry()` 関数を `_run()` の直後に挿入する**

`scripts/scheduler.py` の `_run()` 関数末尾（行134付近）の後に以下を追加:

```python
def _run_with_retry(
    cmd: list[str],
    label: str,
    timeout: int = 3600,
    max_retries: int = 3,
    base_delay: float = 60.0,
) -> int:
    """
    _run() を Exponential Backoff で最大 max_retries 回リトライするラッパー。

    - 成功（rc==0）した時点でリターン
    - 全試行失敗時は最後の rc を返す
    - base_delay=0 はテスト用（実運用は 60 以上を推奨）

    Backoff schedule (base_delay=60):
      試行1: 即時
      試行2: 60s 後
      試行3: 180s 後
      試行4: 600s 後（以降は cap=600）
    """
    import time as _time

    rc = _run(cmd, label, timeout=timeout)
    if rc == 0:
        return 0

    for attempt in range(1, max_retries + 1):
        delay = min(base_delay * (3 ** (attempt - 1)), 600.0)
        logger.warning(
            "[%s] 失敗(rc=%d) — %d秒後に再試行 (%d/%d)",
            label, rc, int(delay), attempt, max_retries,
        )
        if delay > 0:
            _time.sleep(delay)
        rc = _run(cmd, label, timeout=timeout)
        if rc == 0:
            logger.info("[%s] リトライ %d 回目で成功", label, attempt)
            return 0

    logger.error("[%s] 全リトライ失敗（%d 回試行）rc=%d", label, max_retries + 1, rc)
    return rc
```

- [ ] **Step 2: テスト実行（Task 1 の `test_run_with_retry_*` が通ること）**

```
pytest tests/test_self_healing.py::test_run_with_retry_succeeds_on_second_attempt tests/test_self_healing.py::test_run_with_retry_exhausts_all_retries -v
```

期待: 2 PASSED

- [ ] **Step 3: コミット**

```bash
git add scripts/scheduler.py
git commit -m "feat(scheduler): _run_with_retry() Exponential Backoff 追加"
```

---

## Task 3: scheduler.py の JVLink 呼び出しを `_run_with_retry()` に置換

**Files:**
- Modify: `scripts/scheduler.py` — `job_friday_sync()`, `job_morning_wood()`, `job_post_race()`, `job_intraday_sync()` 内の `_run(_PY32 + ...)` 呼び出し

- [ ] **Step 1: `job_friday_sync()` の JVLink 32bit 呼び出しを置換**

`scripts/scheduler.py` の `job_friday_sync()` 内の以下3箇所を変更:

```python
# 変更前
rc = _run(_PY32 + ["-m", "src.ops.data_sync", "friday"], "JVLink-RACE")
# 変更後
rc = _run_with_retry(_PY32 + ["-m", "src.ops.data_sync", "friday"], "JVLink-RACE")
```

```python
# 変更前
rc = _run(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD")
# 変更後
rc = _run_with_retry(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD")
```

```python
# 変更前
rc = _run(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters")
# 変更後
rc = _run_with_retry(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters")
```

- [ ] **Step 2: `job_morning_wood()` の JVLink 呼び出しを置換**

```python
# 変更前
rc = _run(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD朝")
# 変更後
rc = _run_with_retry(_PY32 + ["-m", "src.ops.data_sync", "wood"], "JVLink-WOOD朝")
```

- [ ] **Step 3: `job_post_race()` の JVLink 払戻同期を置換**

```python
# 変更前
rc = _run(
    _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
    "JVLink-払戻同期",
)
if rc != 0:
    logger.warning("[レース後処理] JVLink 払戻同期失敗（netkeiba フォールバックへ）: rc=%d", rc)
# 変更後
rc = _run_with_retry(
    _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
    "JVLink-払戻同期",
)
if rc != 0:
    logger.warning("[レース後処理] JVLink 払戻同期リトライ全滅: rc=%d — 払戻推論フォールバックへ", rc)
```

- [ ] **Step 4: `job_intraday_sync()` の JVLink 呼び出しを置換**

```python
# 変更前
rc = _run(
    _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
    "JVLink-中間結果同期",
)
# 変更後
rc = _run_with_retry(
    _PY32 + ["-m", "src.ops.data_sync", "race_results", "--date", date_yyyymmdd],
    "JVLink-中間結果同期",
)
```

- [ ] **Step 5: `job_monday_masters()` の JVLink 呼び出しを置換**

```python
# 変更前
rc = _run(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters月曜")
# 変更後
rc = _run_with_retry(_PY32 + ["-m", "src.ops.data_sync", "masters"], "JVLink-Masters月曜")
```

- [ ] **Step 6: 構文確認**

```
python -c "import scripts.scheduler; print('OK')" 2>&1
```

期待: `OK`

- [ ] **Step 7: コミット**

```bash
git add scripts/scheduler.py
git commit -m "feat(scheduler): JVLink 32bit 呼び出しを _run_with_retry に統一"
```

---

## Task 4: `job_post_race()` に払戻データからの着順自動補完を追加

**Files:**
- Modify: `scripts/scheduler.py` — `job_post_race()` 関数

- [ ] **Step 1: `job_post_race()` の Step 1（JVLink払戻同期）の直後に infer_ranks 呼び出しを追加**

`scripts/scheduler.py` の `job_post_race()` 内の "# Step 2: 評価 + 通知..." コメントの直前に以下を挿入:

```python
    # Step 1.5: 払戻データから着順自動補完（SEレコード未達対策）
    try:
        from src.database.init_db import init_db as _init_db
        from scripts.infer_ranks_from_payouts import infer_ranks as _infer_ranks
        _conn_infer = _init_db()
        _stats = _infer_ranks(_conn_infer, year_filter=None, dry_run=False)
        _conn_infer.close()
        if _stats["rank1_set"] > 0:
            logger.info(
                "[レース後処理] 払戻補完: rank1=%d rank2=%d rank3=%d (スキップ=%d)",
                _stats["rank1_set"], _stats["rank2_set"], _stats["rank3_set"], _stats["skipped"],
            )
    except Exception as _infer_exc:
        logger.warning("[レース後処理] 払戻補完失敗（続行）: %s", _infer_exc)
```

- [ ] **Step 2: `job_intraday_sync()` にも着順自動補完を追加**

`job_intraday_sync()` の `_run_with_retry()` 呼び出し直後に追加:

```python
    # 中間同期後: 払戻データから着順自動補完
    if rc == 0:
        try:
            from src.database.init_db import init_db as _init_db
            from scripts.infer_ranks_from_payouts import infer_ranks as _infer_ranks
            _conn_infer = _init_db()
            _stats = _infer_ranks(_conn_infer, year_filter=None, dry_run=False)
            _conn_infer.close()
            logger.info(
                "[中間結果同期] 払戻補完: rank1=%d rank2=%d rank3=%d",
                _stats["rank1_set"], _stats["rank2_set"], _stats["rank3_set"],
            )
        except Exception as _infer_exc:
            logger.warning("[中間結果同期] 払戻補完失敗（続行）: %s", _infer_exc)
```

- [ ] **Step 3: テスト実行（Task 1 の infer_ranks テストが通ること）**

```
pytest tests/test_self_healing.py::test_infer_ranks_fills_missing_rank_from_sanrentan tests/test_self_healing.py::test_infer_ranks_dry_run_does_not_write -v
```

期待: 2 PASSED

- [ ] **Step 4: コミット**

```bash
git add scripts/scheduler.py
git commit -m "feat(scheduler): post_race / intraday_sync に払戻データ着順自動補完を組み込み"
```

---

## Task 5: `data_sync.py` の netkeiba Stage 4 フォールバックを infer_ranks に置換

**Files:**
- Modify: `src/ops/data_sync.py:267-281` (`sync_race_results()` 内の Stage 4)

- [ ] **Step 1: netkeiba Stage 4 を infer_ranks 呼び出しに置き換える**

`src/ops/data_sync.py` の Stage 4 ブロック（行267〜281）を以下に置換:

```python
    # ── Stage 4: 払戻データから着順を逆引き補完（netkeiba 禁止のため代替） ─────────────
    # JVLink 全段階でも race_results.rank が埋まらない場合（HR払戻は取得済みが多い）
    if from_date and len(from_date) == 8:
        from_iso = f"{from_date[:4]}-{from_date[4:6]}-{from_date[6:8]}"
        null_rank_count: int = _get_conn().execute(
            """
            SELECT COUNT(*) FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.date = ? AND rr.rank IS NULL
            """,
            (from_iso,),
        ).fetchone()[0]
        if null_rank_count > 0:
            logger.info(
                "Stage4 払戻補完: rank=NULL が %d 件 → infer_ranks_from_payouts を実行",
                null_rank_count,
            )
            try:
                _ROOT_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
                sys.path.insert(0, str(_ROOT_SCRIPTS.parent))
                from scripts.infer_ranks_from_payouts import infer_ranks as _infer_ranks
                _infer_conn = _get_conn()
                _infer_stats = _infer_ranks(_infer_conn, year_filter=from_date[:4], dry_run=False)
                _infer_conn.close()
                logger.info(
                    "Stage4 払戻補完完了: rank1=%d rank2=%d rank3=%d skipped=%d",
                    _infer_stats["rank1_set"],
                    _infer_stats["rank2_set"],
                    _infer_stats["rank3_set"],
                    _infer_stats["skipped"],
                )
                saved += _infer_stats["rank1_set"]
            except Exception as _infer_exc:
                logger.error("Stage4 払戻補完失敗: %s", _infer_exc)
```

- [ ] **Step 2: `sync_results_from_netkeiba` 関数の `import sys` が既存かを確認し、なければ追加**

ファイル先頭の import ブロックに `import sys` があることを確認（すでにある）。

- [ ] **Step 3: 構文確認**

```
python -c "from src.ops.data_sync import sync_race_results; print('OK')" 2>&1
```

期待: `OK`

- [ ] **Step 4: コミット**

```bash
git add src/ops/data_sync.py
git commit -m "feat(data_sync): Stage4 netkeiba フォールバックを infer_ranks に置換"
```

---

## Task 6: `fetch_and_save_odds()` から netkeiba を除去し DB フォールバックを追加

**Files:**
- Modify: `src/pipeline/scraping.py:130-185` (`fetch_and_save_odds()`)

- [ ] **Step 1: 関数全体を以下に置き換える**

`src/pipeline/scraping.py` の `fetch_and_save_odds()` 関数（行130〜185）を以下に置換:

```python
def fetch_and_save_odds(conn: sqlite3.Connection, race_id: str) -> int:
    """realtime_odds が空のとき、RTD キャッシュ → DB 既存値 の順でオッズを確保する。

    フォールバック戦略（2段階）:
      Stage 1: JRA-VAN ローカル RTD キャッシュ — リアルタイムオッズ
      Stage 2: DB 内の既存 realtime_odds を確認して件数を返す（再取得なし）
               → 既存値が 0 でも予測パイプラインは暫定モードで続行する

    Returns:
        確保済みの頭数（0 の場合は暫定モードで続行）
    """
    from src.database.init_db import insert_realtime_odds

    name_map: dict[int, str] = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT horse_number, horse_name FROM entries WHERE race_id=?", (race_id,)
        ).fetchall()
    }

    # Stage 1: JRA-VAN ローカル RTD キャッシュ
    odds_list = _fetch_odds_rtd(race_id)
    if odds_list and any(o.win_odds for o in odds_list):
        n = insert_realtime_odds(conn, race_id, odds_list, name_map)
        logger.info("オッズ取得 [RTD キャッシュ]: %d 頭保存 (race_id=%s)", n, race_id)
        return n
    if odds_list is not None:
        logger.warning("RTD: オッズ取得なし or 全 NaN (race_id=%s)", race_id)

    # Stage 2: DB 内の既存 realtime_odds を確認
    existing: int = conn.execute(
        "SELECT COUNT(*) FROM realtime_odds WHERE race_id=?", (race_id,)
    ).fetchone()[0]
    if existing > 0:
        logger.info(
            "オッズ確保 [DB 既存]: %d 頭分 の realtime_odds を流用 (race_id=%s)",
            existing, race_id,
        )
        return existing

    logger.warning(
        "⚠️ オッズ全段取得失敗 (race_id=%s) — RTD/DB ともに 0。暫定モードで予測を続行します。",
        race_id,
    )
    return 0


def _fetch_odds_rtd(race_id: str) -> list | None:
    """JRA-VAN RTD キャッシュからオッズリストを返す。失敗時は None。"""
    try:
        from src.scraper.rtd_reader import read_rtd_for_race, rtd_odds_to_horse_odds
        rtd_info = read_rtd_for_race(race_id)
        if rtd_info and rtd_info.odds:
            return rtd_odds_to_horse_odds(rtd_info)
        return []
    except Exception as exc:
        logger.warning("RTD 読み込み失敗 (race_id=%s): %s", race_id, exc)
        return None
```

- [ ] **Step 2: テスト実行（Task 1 の `test_fetch_and_save_odds_falls_back_to_db_existing` が通ること）**

```
pytest tests/test_self_healing.py::test_fetch_and_save_odds_falls_back_to_db_existing -v
```

期待: 1 PASSED

- [ ] **Step 3: コミット**

```bash
git add src/pipeline/scraping.py
git commit -m "feat(scraping): fetch_and_save_odds から netkeiba 除去、RTD→DB フォールバック2段に整理"
```

---

## Task 7: `prerace_pipeline()` のオッズ品質チェック失敗時を Graceful Degradation に変更

**Files:**
- Modify: `src/pipeline/prediction.py:292-314` (データ品質チェックブロック)

- [ ] **Step 1: オッズ品質チェック失敗時のハードストップを暫定モードフォールバックに変更**

`src/pipeline/prediction.py` の以下のブロックを変更する:

変更前（行292〜314）:
```python
    # Step 2b: データ品質チェック（直前のみ）
    if not provisional:
        if "win_odds" in df.columns and df["win_odds"].isna().all():
            new_odds = conn.execute(
                "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
            ).fetchone()[0]
            if new_odds > 0:
                logger.info("オッズ保存済み(%d件) → DataFrame を再ビルド", new_odds)
                try:
                    df2 = FeatureBuilder(conn).build_race_features(race_id)
                    if not df2.empty and not df2["win_odds"].isna().all():
                        df = df2
                except Exception as e:
                    logger.warning("DataFrame 再ビルド失敗（続行）: %s", e)

            if "win_odds" in df.columns and df["win_odds"].isna().all():
                logger.warning("⚠️ 全馬の単勝オッズが NaN — オッズ未取得のまま推論: %s", race_id)

        ok, reason = _check_data_quality(df)
        if not ok:
            conn.close()
            _discord.notify_skip(race_id, reason)
            return {"skipped": True, "reason": reason, "race_id": race_id}
```

変更後:
```python
    # Step 2b: データ品質チェック（直前のみ）
    if not provisional:
        if "win_odds" in df.columns and df["win_odds"].isna().all():
            new_odds = conn.execute(
                "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
            ).fetchone()[0]
            if new_odds > 0:
                logger.info("オッズ保存済み(%d件) → DataFrame を再ビルド", new_odds)
                try:
                    df2 = FeatureBuilder(conn).build_race_features(race_id)
                    if not df2.empty and not df2["win_odds"].isna().all():
                        df = df2
                except Exception as e:
                    logger.warning("DataFrame 再ビルド失敗（続行）: %s", e)

            if "win_odds" in df.columns and df["win_odds"].isna().all():
                logger.warning("⚠️ 全馬の単勝オッズが NaN — オッズ未取得のまま推論: %s", race_id)

        ok, reason = _check_data_quality(df)
        if not ok:
            # オッズ欠損系の失敗はスキップせず暫定モードにフォールバック
            # 出馬表が 0 頭の場合（真の失敗）のみ中断する
            if "0 頭" in reason or df.empty:
                conn.close()
                _discord.notify_skip(race_id, reason)
                return {"skipped": True, "reason": reason, "race_id": race_id}
            logger.warning(
                "⚠️ データ品質チェック警告: %s → 暫定モードで強制続行 (race_id=%s)",
                reason, race_id,
            )
            _discord.send_text(
                f"⚠️ [オッズ欠損フォールバック] `{race_id}` — {reason}\n"
                f"暫定モードで予測を強制続行します。"
            )
            provisional = True
```

- [ ] **Step 2: テスト実行（Task 1 の `test_prerace_pipeline_does_not_skip_on_odds_failure` が通ること）**

```
pytest tests/test_self_healing.py::test_prerace_pipeline_does_not_skip_on_odds_failure -v
```

期待: 1 PASSED

- [ ] **Step 3: 全テストスイートを実行して既存テストへの回帰がないことを確認**

```
pytest tests/ -v --tb=short 2>&1 | tail -30
```

期待: 既存テストがすべて PASSED（または事前に SKIP/XFAIL だったものは変わらない）

- [ ] **Step 4: コミット**

```bash
git add src/pipeline/prediction.py
git commit -m "feat(prediction): オッズ品質チェック失敗時を暫定モード Graceful Degradation に変更"
```

---

## Task 8: 全テスト最終確認 + レポート

- [ ] **Step 1: 全テスト実行**

```
pytest tests/test_self_healing.py -v
```

期待: 6 PASSED

- [ ] **Step 2: `ruff format .` でコード整形**

```
ruff format scripts/scheduler.py src/ops/data_sync.py src/pipeline/scraping.py src/pipeline/prediction.py tests/test_self_healing.py
```

- [ ] **Step 3: 変更ファイルをまとめてコミット**

```bash
git add -p
git commit -m "chore: ruff format 整形"
```

- [ ] **Step 4: 最終確認 — scheduler.py に `_run_with_retry` が使われていること**

```
grep -n "_run_with_retry" scripts/scheduler.py
```

期待: `job_friday_sync`, `job_morning_wood`, `job_post_race`, `job_intraday_sync`, `job_monday_masters` の各JVLink呼び出し箇所に計5件以上

- [ ] **Step 5: 最終確認 — netkeiba 参照が残っていないこと**

```
grep -rn "netkeiba" src/pipeline/ src/ops/data_sync.py
```

期待: `data_sync.py` に `sync_results_from_netkeiba` 関数定義は残るが、呼び出し側からの参照はゼロ（関数自体は保守用に残す）

---

## Self-Review

### Spec Coverage

| 要件 | 対応タスク |
|---|---|
| 1. 払戻データからの自動補完（infer_ranks） | Task 4 (scheduler post_race / intraday_sync), Task 5 (data_sync Stage 4) |
| 2. JVLink 自動リトライ（Exponential Backoff） | Task 2 (_run_with_retry 追加), Task 3 (呼び出し置換) |
| 3. オッズ取得失敗時の Graceful Degradation | Task 6 (scraping RTD→DB), Task 7 (prediction 暫定フォールバック) |
| netkeiba 参照除去 | Task 5, Task 6 |

### Placeholder Scan
なし — 全ステップにコード記載済み。

### Type Consistency
- `infer_ranks(conn, year_filter, dry_run) -> dict[str, int]` — Task 4/5 で一貫して使用
- `_run_with_retry(cmd, label, timeout, max_retries, base_delay) -> int` — Task 2/3 で一貫
- `fetch_and_save_odds(conn, race_id) -> int` — Task 6/テストで一貫
- `_fetch_odds_rtd(race_id) -> list | None` — Task 6 内で定義・使用
