# Full Automation — Hands-Free E2E Pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** JVLinkスタンドアロン動作確認済み（Pattern A）を前提に、3つのギャップを埋めて「社長が何もしなくていい」E2Eパイプラインを完成させる。

**Architecture:**
- G-1: `weekend_batch.py --phase pre/post` を `scheduler.py` に統合（土日 07:00/18:30）
- G-2: `data/scheduler_state.json` でジョブ最終実行を追跡し、PC 再起動後の取りこぼしを起動時に自動リカバリー
- G-3: `scripts/install_autostart.py` で Windows タスクスケジューラにログオン時起動エントリを自動登録

**Tech Stack:** Python 3.11+, `schedule` ライブラリ, `schtasks.exe`（Windows ビルトイン）, pytest

---

## ファイルマップ

| ファイル | 変更種別 | 担当 |
|---|---|---|
| `scripts/scheduler.py` | **修正** | G-1, G-2 実装の中心 |
| `scripts/install_autostart.py` | **新規作成** | G-3 Windows 自動起動設定 |
| `data/scheduler_state.json` | **自動生成** | G-2 ジョブ状態追跡ファイル |
| `tests/test_scheduler_state.py` | **新規作成** | G-2 リカバリーロジックのユニットテスト |
| `scripts/_check_target_proc.ps1` | **削除** | 調査用一時ファイル |

---

## Task 1: 調査用一時ファイルの削除

**Files:**
- Delete: `scripts/_check_target_proc.ps1`

- [ ] **Step 1: 削除**

```bash
rm C:/dev/horse-racing-ai/scripts/_check_target_proc.ps1
```

- [ ] **Step 2: 確認**

```bash
ls C:/dev/horse-racing-ai/scripts/_check_target_proc.ps1 2>&1 || echo "削除確認"
```
Expected: "削除確認"

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: 調査用一時ファイル削除"
```

---

## Task 2: ジョブ状態管理のユニットテスト（Red phase）

ジョブ取りこぼし検出ロジック (`_should_recover`) のテストを先に書く。

**Files:**
- Create: `tests/test_scheduler_state.py`

- [ ] **Step 1: テストファイルを作成**

```python
# tests/test_scheduler_state.py
"""scheduler.py の取りこぼしリカバリーロジックのユニットテスト。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import pytest


def _should_recover(
    last_run: datetime | None,
    scheduled_today: datetime,
    now: datetime,
    catchup_hours: int,
) -> bool:
    """
    ジョブを今すぐ実行すべきかを判定する関数の期待仕様:

    Args:
        last_run:        最後に成功した実行時刻 (None=未実行)
        scheduled_today: 本日の予定実行時刻
        now:             現在時刻
        catchup_hours:   取りこぼし許容時間（この時間を超えたら再実行しない）

    Returns:
        True → 今すぐ実行すべき
    """
    # この関数は scheduler.py からインポートする想定
    from scripts.scheduler import _should_recover as _sr
    return _sr(last_run, scheduled_today, now, catchup_hours)


class TestShouldRecover:
    def _make(self, hour: int, minute: int = 0, date_offset: int = 0) -> datetime:
        base = datetime(2026, 5, 2, hour, minute)
        return base + timedelta(days=date_offset)

    def test_never_run_and_within_window(self) -> None:
        """未実行・許容窓内 → 再実行すべき"""
        scheduled = self._make(7, 30)    # 07:30
        now = self._make(9, 0)           # 09:00 (1.5h後)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is True

    def test_never_run_but_window_expired(self) -> None:
        """未実行・許容窓超過 → 再実行しない（時機を逸した）"""
        scheduled = self._make(7, 30)    # 07:30
        now = self._make(14, 0)          # 14:00 (6.5h後)
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False

    def test_already_run_today(self) -> None:
        """当日実行済み → 再実行しない"""
        scheduled = self._make(7, 30)
        now = self._make(9, 0)
        last_run = self._make(7, 31)     # 当日実行済み
        assert _should_recover(last_run, scheduled, now, catchup_hours=4) is False

    def test_ran_yesterday_within_window(self) -> None:
        """前日実行・本日まだ・許容窓内 → 再実行すべき"""
        scheduled = self._make(7, 30)
        now = self._make(9, 0)
        last_run = self._make(7, 31, date_offset=-1)  # 前日実行
        assert _should_recover(last_run, scheduled, now, catchup_hours=4) is True

    def test_scheduled_time_not_yet_passed(self) -> None:
        """スケジュール時刻まだ → 実行しない"""
        scheduled = self._make(20, 0)    # 20:00
        now = self._make(10, 0)          # 10:00
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False

    def test_exact_boundary_at_window_edge(self) -> None:
        """ちょうど窓の境界（=許容時間ちょうど）→ 実行しない（window は exclusive）"""
        scheduled = self._make(7, 30)
        now = self._make(11, 30)         # ちょうど4時間後
        assert _should_recover(None, scheduled, now, catchup_hours=4) is False
```

- [ ] **Step 2: テストが失敗することを確認（インポートエラーが出ればOK）**

```bash
cd C:/dev/horse-racing-ai && py -m pytest tests/test_scheduler_state.py -v 2>&1 | head -20
```
Expected: `ImportError` または `ModuleNotFoundError`（`_should_recover` 未定義）

---

## Task 3: scheduler.py — 状態管理・リカバリー関数を追加（Green phase）

**Files:**
- Modify: `scripts/scheduler.py` (行 96-100 の定数定義ブロックの直後に追加)

- [ ] **Step 1: 定数とインポートを追加**

`scheduler.py` の先頭 import 部分（`import time` の下）に追加:

```python
import json
from datetime import date, datetime, timedelta
```

（`date` と `datetime` はすでにインポートされているので `json` のみ追加）

実際には `scheduler.py` 先頭の import 群に `json` がなければ追加：

```python
import json  # ← 追加
```

- [ ] **Step 2: 状態ファイルパスと取りこぼし窓定数を追加**

`logger = logging.getLogger("scheduler")` の直後（行 86 付近）に追加:

```python
# ================================================================
# ジョブ状態管理（取りこぼしリカバリー用）
# ================================================================

_STATE_FILE: Path = _ROOT / "data" / "scheduler_state.json"

# ジョブ名 → 取りこぼし許容時間（時間）
# この時間を超えたら "時機を逸した" として再実行しない
_CATCHUP_HOURS: dict[str, int] = {
    "job_friday_sync":        16,  # Fri 20:00 → Sat 12:00 まで
    "job_morning_wood":        4,  # 07:30 → 11:30 まで
    "job_weekend_batch_pre":   4,  # 07:00 → 11:00 まで
    "job_today_auto_runner":   3,  # 08:30 → 11:30 まで
    "job_win5_prediction":     2,  # 09:00 → 11:00 まで
    "job_post_race":           4,  # 17:30 → 21:30 まで
    "job_weekend_batch_post":  4,  # 18:30 → 22:30 まで
    "job_monday_masters":     12,  # 06:00 → 18:00 まで
    "job_weekly_retrain":     12,  # 07:00 → 19:00 まで
    "job_git_push":           12,  # 08:00 → 20:00 まで
}

# 各ジョブのスケジュール定義（曜日コード: 0=月 … 6=日）
# (weekday_int, hour, minute)
_JOB_SCHEDULES: dict[str, list[tuple[int, int, int]]] = {
    "job_friday_sync":        [(4, 20,  0)],          # 金 20:00
    "job_morning_wood":       [(5,  7, 30), (6,  7, 30)],  # 土日 07:30
    "job_weekend_batch_pre":  [(5,  7,  0), (6,  7,  0)],  # 土日 07:00
    "job_today_auto_runner":  [(5,  8, 30), (6,  8, 30)],  # 土日 08:30
    "job_win5_prediction":    [(5,  9,  0), (6,  9,  0)],  # 土日 09:00
    "job_post_race":          [(5, 17, 30), (6, 17, 30)],  # 土日 17:30
    "job_weekend_batch_post": [(5, 18, 30), (6, 18, 30)],  # 土日 18:30
    "job_monday_masters":     [(0,  6,  0)],          # 月 06:00
    "job_weekly_retrain":     [(0,  7,  0)],          # 月 07:00
    "job_git_push":           [(0,  8,  0)],          # 月 08:00
}


def _load_job_state() -> dict[str, str]:
    """scheduler_state.json を読み込む。ファイルがなければ空辞書を返す。"""
    if not _STATE_FILE.exists():
        return {}
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_job_state(state: dict[str, str]) -> None:
    """scheduler_state.json に書き込む。"""
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("scheduler_state.json 書き込み失敗: %s", exc)


def _mark_job_done(job_name: str) -> None:
    """ジョブ成功時に state ファイルを更新する。"""
    state = _load_job_state()
    state[job_name] = datetime.now().isoformat(timespec="seconds")
    _save_job_state(state)


def _should_recover(
    last_run: datetime | None,
    scheduled_today: datetime,
    now: datetime,
    catchup_hours: int,
) -> bool:
    """
    ジョブを今すぐ実行すべきかを判定する。

    条件:
    1. スケジュール時刻がすでに過ぎている
    2. 取りこぼし許容窓内（scheduled_today + catchup_hours > now）
    3. 当日まだ実行されていない（last_run が今日より前 or None）
    """
    if now < scheduled_today:
        return False  # まだスケジュール時刻に達していない
    elapsed_hours = (now - scheduled_today).total_seconds() / 3600
    if elapsed_hours >= catchup_hours:
        return False  # 許容窓を超えた
    if last_run is None:
        return True
    # 当日実行済みかチェック
    return last_run.date() < now.date()


def _recover_missed_jobs(
    job_map: dict[str, object],
) -> None:
    """
    起動時に「本日実行すべきだったが取りこぼしたジョブ」を検出して即実行する。

    PC 再起動やスリープ復帰後に scheduler が起動した場合に対応する。
    """
    state = _load_job_state()
    now = datetime.now()
    weekday = now.weekday()  # 0=月 … 6=日

    for job_name, schedules in _JOB_SCHEDULES.items():
        fn = job_map.get(job_name)
        if fn is None:
            continue

        for wd, h, m in schedules:
            if wd != weekday:
                continue  # 今日が対象曜日でない

            scheduled_today = now.replace(hour=h, minute=m, second=0, microsecond=0)
            last_run_str = state.get(job_name)
            last_run: datetime | None = None
            if last_run_str:
                try:
                    last_run = datetime.fromisoformat(last_run_str)
                except ValueError:
                    pass

            catchup = _CATCHUP_HOURS.get(job_name, 4)
            if _should_recover(last_run, scheduled_today, now, catchup):
                logger.warning(
                    "[リカバリー] %s は %s に実行すべきでしたが取りこぼしを検出 → 今すぐ実行",
                    job_name,
                    scheduled_today.strftime("%H:%M"),
                )
                _send_discord(
                    f"⚠️ [UMALOGI] ジョブ取りこぼしをリカバリー: `{job_name}` "
                    f"（予定 {scheduled_today.strftime('%H:%M')} → 今実行）"
                )
                try:
                    fn()  # type: ignore[operator]
                    _mark_job_done(job_name)
                except Exception as exc:
                    logger.error("[リカバリー] %s 実行失敗: %s", job_name, exc)
```

- [ ] **Step 3: テストを実行して Green になることを確認**

```bash
cd C:/dev/horse-racing-ai && py -m pytest tests/test_scheduler_state.py -v
```
Expected: `6 passed`

---

## Task 4: scheduler.py — weekend_batch ジョブ関数を追加

**Files:**
- Modify: `scripts/scheduler.py` (`job_daily_backup` 関数の直前、行 735 付近に追加)

- [ ] **Step 1: 2つの新ジョブ関数を追加**

`job_daily_backup()` 関数定義の直前に以下を追加:

```python
def job_weekend_batch_pre() -> None:
    """
    土日 07:00: 週末バッチ Pre フェーズ

    金曜夜の暫定予想をもとに以下を自動実行:
      1. note.com に下書き記事を保存（Playwright）
      2. ウマニティ コロシアムに暫定予想を投稿
      3. X（Twitter）に本日予想のお知らせツイート

    NOTE_EMAIL / NOTE_PASSWORD が未設定の場合も note 下書き以外は続行する。
    """
    logger.info("=== [週末バッチ Pre] 開始 ===")
    rc = _run(
        _PY64 + ["scripts/weekend_batch.py", "--phase", "pre"],
        "週末バッチ-Pre",
        timeout=1800,  # 30分
    )
    if rc == 0:
        _mark_job_done("job_weekend_batch_pre")
        logger.info("=== [週末バッチ Pre] 完了 ===")
    else:
        logger.error("[週末バッチ Pre] 失敗: rc=%d", rc)
        _send_discord(f"🚨 [UMALOGI] 週末バッチ Pre 失敗 (rc={rc})")


def job_weekend_batch_post() -> None:
    """
    土日 18:30: 週末バッチ Post フェーズ

    レース確定後の成績を集計して以下を自動実行:
      1. 的中証拠画像（Pillow）を生成
      2. X（Twitter）に本日 P&L 結果ツイート（画像付き）
      3. batch_runs テーブルにログ記録

    prediction_results が空の場合は自動スキップ。
    """
    logger.info("=== [週末バッチ Post] 開始 ===")
    rc = _run(
        _PY64 + ["scripts/weekend_batch.py", "--phase", "post"],
        "週末バッチ-Post",
        timeout=1800,  # 30分
    )
    if rc == 0:
        _mark_job_done("job_weekend_batch_post")
        logger.info("=== [週末バッチ Post] 完了 ===")
    else:
        logger.error("[週末バッチ Post] 失敗: rc=%d", rc)
        _send_discord(f"🚨 [UMALOGI] 週末バッチ Post 失敗 (rc={rc})")
```

- [ ] **Step 2: import 追加確認**

`scheduler.py` の先頭に `import json` があることを確認（Task 3 で追加済み）。

- [ ] **Step 3: Commit**

```bash
cd C:/dev/horse-racing-ai && git add scripts/scheduler.py
git commit -m "feat: scheduler — 状態管理・リカバリー・weekend_batch ジョブ追加"
```

---

## Task 5: scheduler.py — register_schedules と run_daemon を更新

**Files:**
- Modify: `scripts/scheduler.py` の `register_schedules()` と `run_daemon()` と `_JOB_MAP`

- [ ] **Step 1: register_schedules() に新ジョブを追加**

`register_schedules()` 内の `# 土日朝: 調教タイム同期` の直前に追加:

```python
    # 土日朝: 週末バッチ Pre（暫定予想 → note 下書き + Umanity + X 告知）
    schedule.every().saturday.at("07:00").do(job_weekend_batch_pre)
    schedule.every().sunday.at("07:00").do(job_weekend_batch_pre)
```

`# 土日夕方: 払戻確定後のレース後処理` の直後に追加:

```python
    # 土日夜: 週末バッチ Post（P&L 集計 + 的中カード + X 結果報告）
    schedule.every().saturday.at("18:30").do(job_weekend_batch_post)
    schedule.every().sunday.at("18:30").do(job_weekend_batch_post)
```

- [ ] **Step 2: run_daemon() にリカバリー呼び出しを追加**

`run_daemon()` の `register_schedules()` 呼び出しの直後:

```python
def run_daemon() -> None:
    """スケジューラーをデーモンとして常駐させる。Ctrl+C で終了。"""
    register_schedules()

    # ── 起動時リカバリー: 取りこぼしジョブを検出して即実行 ────────
    _recover_missed_jobs(_JOB_MAP_FULL)    # ← 追加

    _send_discord(
        f"🤖 **[UMALOGI] スケジューラー起動**\n"
        ...
    )
```

`_JOB_MAP_FULL` はすべてのジョブを含む辞書（下記 Step 3 で定義）。

- [ ] **Step 3: _JOB_MAP と _JOB_MAP_FULL を更新**

現在の `_JOB_MAP` の直前に `_JOB_MAP_FULL` を追加し、`_JOB_MAP` は CLI 用のままにする:

```python
# リカバリー用フルマップ（関数名 → 関数オブジェクト）
_JOB_MAP_FULL: dict[str, object] = {
    "job_friday_sync":        job_friday_sync,
    "job_morning_wood":       job_morning_wood,
    "job_weekend_batch_pre":  job_weekend_batch_pre,
    "job_today_auto_runner":  job_today_auto_runner,
    "job_win5_prediction":    job_win5_prediction,
    "job_post_race":          job_post_race,
    "job_weekend_batch_post": job_weekend_batch_post,
    "job_monday_masters":     job_monday_masters,
    "job_weekly_retrain":     job_weekly_retrain,
    "job_git_push":           job_git_push,
}

# CLI --run-now 用マップ（短縮名 → 関数）
_JOB_MAP: dict[str, object] = {
    "friday":         job_friday_sync,
    "wood":           job_morning_wood,
    "batch_pre":      job_weekend_batch_pre,   # ← 追加
    "batch_post":     job_weekend_batch_post,  # ← 追加
    "win5":           job_win5_prediction,
    "umanity":        job_umanity_upload,
    "auto_runner":    job_today_auto_runner,
    "intraday_sync":  job_intraday_sync,
    "post_race":      job_post_race,
    "masters":        job_monday_masters,
    "retrain":        job_weekly_retrain,
    "git":            job_git_push,
}
```

- [ ] **Step 4: 既存ジョブにも _mark_job_done を追加**

主要ジョブの成功パスに `_mark_job_done("job_xxx")` を追加:

`job_friday_sync()` の `logger.info("=== [金曜バッチ] 完了（全ステップ正常）===")` の直前:
```python
        _mark_job_done("job_friday_sync")
```

`job_morning_wood()` の `logger.info("=== [朝調教同期] 完了 ===")` の直前:
```python
        _mark_job_done("job_morning_wood")
```

`job_post_race()` の最終行 `logger.info("=== [レース後処理] %s 終了 ===", target_date)` の直前:
```python
    _mark_job_done("job_post_race")
```

`job_monday_masters()` の `logger.info("=== [マスタ更新] 完了 ===")` の直前:
```python
            _mark_job_done("job_monday_masters")
```

`job_weekly_retrain()` の最終 `logger.info` の直前:
```python
    _mark_job_done("job_weekly_retrain")
```

`job_git_push()` の `logger.info("[Git プッシュ] %s", ...)` の直前:
```python
        _mark_job_done("job_git_push")
```

- [ ] **Step 5: テスト実行**

```bash
cd C:/dev/horse-racing-ai && py -m pytest tests/test_scheduler_state.py -v
```
Expected: `6 passed`

- [ ] **Step 6: スケジューラ構文確認**

```bash
cd C:/dev/horse-racing-ai && py scripts/scheduler.py --help
```
Expected: エラーなし、`choices` に `batch_pre` `batch_post` が含まれる

- [ ] **Step 7: Commit**

```bash
git add scripts/scheduler.py
git commit -m "feat: scheduler — weekend_batch 統合・register_schedules 更新・_mark_job_done 追加"
```

---

## Task 6: Windows 自動起動インストーラーの作成

**Files:**
- Create: `scripts/install_autostart.py`

- [ ] **Step 1: インストーラーを作成**

```python
# scripts/install_autostart.py
"""
UMALOGI スケジューラー Windows 自動起動インストーラー

PC 再起動後もスケジューラーが自動で起動するよう
Windows タスクスケジューラにエントリを登録する。

Usage:
    py scripts/install_autostart.py install    # 登録
    py scripts/install_autostart.py uninstall  # 削除
    py scripts/install_autostart.py status     # 確認
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_TASK_NAME = "UMALOGI-Scheduler"
_PY_CMD = sys.executable  # 現在の Python インタープリタ
_SCRIPT = str(_ROOT / "scripts" / "scheduler.py")


def _run_schtasks(args: list[str]) -> tuple[int, str]:
    """schtasks コマンドを実行して (returncode, output) を返す。"""
    result = subprocess.run(
        ["schtasks"] + args,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output


def install() -> None:
    """タスクスケジューラにログオン時起動エントリを登録する。"""
    print(f"タスク登録: {_TASK_NAME}")
    print(f"  インタープリタ: {_PY_CMD}")
    print(f"  スクリプト    : {_SCRIPT}")

    # 既存タスクを削除してから再登録（べき等）
    _run_schtasks(["/Delete", "/TN", _TASK_NAME, "/F"])

    # ログオン時に最低特権で起動、開始ディレクトリを _ROOT に設定
    rc, out = _run_schtasks([
        "/Create",
        "/TN", _TASK_NAME,
        "/TR", f'"{_PY_CMD}" "{_SCRIPT}"',
        "/SC", "ONLOGON",
        "/RL", "HIGHEST",          # 管理者権限
        "/F",                       # 強制上書き
        "/SD", str(_ROOT),
    ])

    if rc == 0:
        print("✅ 登録成功")
        print(f"   次回ログオン時に自動起動します: {_TASK_NAME}")
    else:
        print(f"❌ 登録失敗 (rc={rc})")
        print(out)
        sys.exit(1)


def uninstall() -> None:
    """タスクスケジューラからエントリを削除する。"""
    rc, out = _run_schtasks(["/Delete", "/TN", _TASK_NAME, "/F"])
    if rc == 0:
        print(f"✅ 削除完了: {_TASK_NAME}")
    else:
        print(f"❌ 削除失敗 (rc={rc}): {out}")


def status() -> None:
    """タスクの登録状態を確認する。"""
    rc, out = _run_schtasks(["/Query", "/TN", _TASK_NAME, "/FO", "LIST"])
    if rc == 0:
        print(f"✅ 登録済み: {_TASK_NAME}")
        print(out)
    else:
        print(f"⚠️  未登録: {_TASK_NAME}")
        print("   py scripts/install_autostart.py install  で登録できます。")


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI スケジューラー自動起動設定")
    parser.add_argument(
        "action",
        choices=["install", "uninstall", "status"],
        help="実行アクション",
    )
    args = parser.parse_args()
    actions = {"install": install, "uninstall": uninstall, "status": status}
    actions[args.action]()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 動作確認（status のみ）**

```bash
cd C:/dev/horse-racing-ai && py scripts/install_autostart.py status
```
Expected: 未登録なら "⚠️ 未登録: UMALOGI-Scheduler"、登録済みなら登録情報が表示される

- [ ] **Step 3: Commit**

```bash
git add scripts/install_autostart.py
git commit -m "feat: Windows タスクスケジューラ自動起動インストーラー追加"
```

---

## Task 7: 統合テストと最終確認

- [ ] **Step 1: 全テスト実行**

```bash
cd C:/dev/horse-racing-ai && py -m pytest tests/test_scheduler_state.py -v
```
Expected: `6 passed`

- [ ] **Step 2: scheduler.py 構文チェック**

```bash
cd C:/dev/horse-racing-ai && py -c "import scripts.scheduler; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: `--run-now batch_pre` のドライラン確認**

```bash
cd C:/dev/horse-racing-ai && py scripts/scheduler.py --run-now batch_pre 2>&1 | head -20
```
Expected: `[週末バッチ-Pre] 開始:` ログが出て weekend_batch.py が呼ばれる

- [ ] **Step 4: `--run-now batch_post` のドライラン確認**

```bash
cd C:/dev/horse-racing-ai && py scripts/scheduler.py --run-now batch_post 2>&1 | head -20
```
Expected: `[週末バッチ-Post] 開始:` ログが出る

- [ ] **Step 5: ruff format**

```bash
cd C:/dev/horse-racing-ai && ruff format scripts/scheduler.py scripts/install_autostart.py
```

- [ ] **Step 6: 最終 commit**

```bash
cd C:/dev/horse-racing-ai && git add -A
git commit -m "feat: E2Eフルオートメーション完成 — weekend_batch統合・取りこぼしリカバリー・Windows自動起動"
```

---

## Self-Review チェック

### Spec coverage
- [x] G-1: weekend_batch pre/post → Task 4, 5 で統合済み
- [x] G-2: sleep/wake 取りこぼし → Task 2, 3, 5 で実装済み
- [x] G-3: Windows 自動起動 → Task 6 で実装済み
- [x] 調査用一時ファイル削除 → Task 1

### Placeholder scan
- `_recover_missed_jobs` は Task 3 Step 2 で定義し、Task 5 Step 2 で使用 ✓
- `_mark_job_done` は Task 3 Step 2 で定義し、Task 4/5 で使用 ✓
- `_JOB_MAP_FULL` は Task 5 Step 3 で定義し、`run_daemon()` で使用 ✓

### Type consistency
- `_should_recover(last_run: datetime | None, scheduled_today: datetime, now: datetime, catchup_hours: int) -> bool`
- テスト (Task 2) と実装 (Task 3) でシグネチャが一致 ✓
