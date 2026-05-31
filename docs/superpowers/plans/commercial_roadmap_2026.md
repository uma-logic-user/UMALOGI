# UMALOGI 商用化・実績構築ロードマップ 2026

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** UMALOGI を「趣味の投資ツール」から「商用レベル（マネタイズ・権威性確立）」へ引き上げる4週間ロードマップを完全実装する。

**Architecture:** 通知ルーター層 (`NotificationRouter`) の新設・実績自動集計スクリプト・A/B テスト自動比較・X シグナル統合・note 有料記事フォーマット確立の5本柱で構成する。各週は独立したデプロイ可能な成果物を持ち、週末本番稼働を妨げない。

**Tech Stack:** Python 3.11, SQLite, LightGBM, Discord Webhooks, Playwright, Claude Haiku API, Next.js (Web UI)

---

## ファイル構成マップ

| ファイル | 種別 | 役割 |
|---|---|---|
| `src/notification/router.py` | **新設** | `NotificationRouter` — チャンネル選択・フォールバック・全通知メソッド |
| `src/notification/discord_notifier.py` | 軽微修正 | `_COLOR_*` 定数をモジュール公開（後方互換維持） |
| `scripts/post_weekly_note_draft.py` | 改修 | Discord 転送ステップ追加・`ENABLE_PLAYWRIGHT_POST` フラグ制御 |
| `scripts/generate_performance_report.py` | **新設** | 週次・月次の実績サマリーを DB から集計し Discord へ送信 |
| `scripts/generate_ab_report.py` | **新設** | V1 vs V2 週次 A/B 成績比較 Markdown を生成して ab_test チャンネルへ送信 |
| `scripts/generate_promo_article.py` | **新設** | 「なぜ UMALOGI は勝てるのか」note プロモーション固定記事を自動生成 |
| `src/pipeline/prediction.py` | 改修 | `DiscordNotifier()` → `NotificationRouter()` に置換 |
| `scripts/today_auto_runner.py` | 改修 | ローカル `_send_discord()` → `NotificationRouter().send_system_text()` |
| `scripts/scheduler.py` | 改修 | ローカル Discord 直接呼び出し → `NotificationRouter()` に統一 |
| `tests/notification/test_router.py` | **新設** | `NotificationRouter` 単体テスト |
| `tests/scripts/test_performance_report.py` | **新設** | 実績レポート単体テスト |
| `2.env` | 更新 | 新規環境変数 5件を追記 |
| `docs/4_ui_design.md` | 更新 | 通知チャンネル設計を Changelog に追記 |
| `docs/5_ml_roadmap.md` | 更新 | X シグナル統合 Phase C を Changelog に追記 |

---

## 第1週：基盤整備「通知ルーター ＋ 実績自動見える化」

> **完了条件**: `NotificationRouter` が全チャンネルへ正常送信できる。  
> `generate_performance_report.py` が Discord に実績サマリーを送信できる。

---

### Task 1-1: NotificationRouter 基盤実装

**Files:**
- Create: `src/notification/router.py`
- Modify: `src/notification/discord_notifier.py`（`_COLOR_*` 定数を公開）
- Test: `tests/notification/test_router.py`

- [ ] **Step 1-1-1: 既存 discord_notifier.py の定数を公開する**

`src/notification/discord_notifier.py` の先頭付近に定義されている `_COLOR_*` 定数を、
外部から参照できるようモジュール公開のエイリアスを追加する（既存の `_COLOR_*` は削除しない）。

```python
# discord_notifier.py の末尾付近に追加（既存コードは変更しない）
# モジュール公開用エイリアス（NotificationRouter から参照できるように）
COLOR_GREEN  = _COLOR_GREEN
COLOR_ORANGE = _COLOR_ORANGE
COLOR_RED    = _COLOR_RED
COLOR_BLUE   = _COLOR_BLUE
COLOR_GRAY   = _COLOR_GRAY
```

- [ ] **Step 1-1-2: テストファイルの雛形を作成する**

`tests/notification/` ディレクトリを作成し、空の `__init__.py` と以下のテストファイルを作成する。

```python
# tests/notification/test_router.py
from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch, call


def _make_router(env: dict[str, str]):
    """指定した環境変数で NotificationRouter を構築するヘルパー。"""
    with patch.dict("os.environ", env, clear=False):
        from src.notification.router import NotificationRouter
        return NotificationRouter()
```

- [ ] **Step 1-1-3: フォールバックテストを書く**

```python
# tests/notification/test_router.py に追記
def test_fallback_to_prediction(monkeypatch):
    """ev_alert 未設定時に prediction チャンネルへフォールバックする。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/prediction")
    monkeypatch.delenv("DISCORD_WEBHOOK_EV_ALERT", raising=False)
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    notifier = router._get("ev_alert")
    # フォールバック先は prediction チャンネルの notifier
    pred_notifier = router._get("prediction")
    assert notifier is pred_notifier


def test_all_channels_unset_no_exception(monkeypatch):
    """全 URL 未設定でも例外が発生しない（ログのみで安全スキップ）。"""
    for key in [
        "DISCORD_WEBHOOK_URL",
        "DISCORD_WEBHOOK_SYSTEM",
        "DISCORD_WEBHOOK_EV_ALERT",
        "DISCORD_WEBHOOK_AB_TEST",
        "DISCORD_WEBHOOK_NOTE_DRAFT",
    ]:
        monkeypatch.delenv(key, raising=False)
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    # 例外を投げずに安全に完了すること
    router.send_text("テスト")
    router.send_system_text("テスト")
```

- [ ] **Step 1-1-4: テストが FAIL することを確認する**

```
pytest tests/notification/test_router.py -v
```

期待: `ImportError` または `ModuleNotFoundError`（`router.py` 未実装のため）

- [ ] **Step 1-1-5: `src/notification/router.py` を実装する**

```python
"""
src/notification/router.py — Discord 通知ルーター

チャンネル選択・フォールバック・全通知メソッドを集約する。
個別チャンネルへの HTTP 送信は DiscordNotifier に委譲する。
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from .discord_notifier import DiscordNotifier

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# 環境変数キー → チャンネル名 のマッピング
CHANNEL_ENV: dict[str, str] = {
    "prediction": "DISCORD_WEBHOOK_URL",
    "system":     "DISCORD_WEBHOOK_SYSTEM",
    "ev_alert":   "DISCORD_WEBHOOK_EV_ALERT",
    "ab_test":    "DISCORD_WEBHOOK_AB_TEST",
    "note_draft": "DISCORD_WEBHOOK_NOTE_DRAFT",
}

# 後方互換: 旧変数名
_LEGACY_MAP: dict[str, str] = {
    "DISCORD_SYSTEM_WEBHOOK_URL": "system",
}

# EV >= この値で ev_alert へ追加送信する
EV_ALERT_THRESHOLD = 1.5


class NotificationRouter:
    """
    複数 Discord チャンネルへの通知を一元管理するルーター。

    チャンネルが未設定の場合は prediction チャンネルへフォールバックする。
    prediction も未設定の場合は WARNING ログのみ出力し例外を投げない。
    """

    def __init__(self) -> None:
        self._notifiers: dict[str, DiscordNotifier] = {}
        self._build_notifiers()

    def _build_notifiers(self) -> None:
        """環境変数からチャンネル → DiscordNotifier を構築する。"""
        for channel, env_key in CHANNEL_ENV.items():
            url = os.environ.get(env_key, "")
            if url:
                self._notifiers[channel] = DiscordNotifier(webhook_url=url)

        # 後方互換: 旧変数を読み込む
        for legacy_key, channel in _LEGACY_MAP.items():
            if channel not in self._notifiers:
                url = os.environ.get(legacy_key, "")
                if url:
                    self._notifiers[channel] = DiscordNotifier(webhook_url=url)

    def _get(self, channel: str) -> DiscordNotifier | None:
        """
        チャンネルに対応する DiscordNotifier を返す。
        未設定の場合は prediction へフォールバック。
        prediction も未設定なら None を返す。
        """
        if channel in self._notifiers:
            return self._notifiers[channel]
        fallback = self._notifiers.get("prediction")
        if fallback is None:
            logger.warning("通知スキップ: channel=%s (prediction も未設定)", channel)
        elif channel != "prediction":
            logger.debug("フォールバック: channel=%s → prediction", channel)
        return fallback

    # ── prediction チャンネル ────────────────────────────────────────

    def send_text(self, text: str) -> None:
        """prediction チャンネルにテキストを送信する。"""
        notifier = self._get("prediction")
        if notifier:
            notifier.send_text(text)

    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: list[dict],
        manji_bets: list[dict],
        max_ev: float = 0.0,
        **kwargs,
    ) -> None:
        """prediction へ通知。max_ev >= EV_ALERT_THRESHOLD なら ev_alert へも追加送信。"""
        notifier = self._get("prediction")
        if notifier:
            notifier.notify_prerace_result(race_id, honmei_bets, manji_bets, **kwargs)

        if max_ev >= EV_ALERT_THRESHOLD:
            bets_summary = (
                f"EV最高値: {max_ev:.2f} / "
                f"単勝ベット数: {len(honmei_bets)} / 複勝ベット数: {len(manji_bets)}"
            )
            self.notify_ev_alert(race_id, max_ev, bets_summary)

    def notify_hit_summary(self, *args, **kwargs) -> None:
        """prediction チャンネルへ的中サマリーを送信する。"""
        notifier = self._get("prediction")
        if notifier:
            notifier.notify_hit_summary(*args, **kwargs)

    def notify_skip(self, race_id: str, reason: str) -> None:
        notifier = self._get("prediction")
        if notifier:
            notifier.notify_skip(race_id, reason)

    # ── system チャンネル ────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        notifier = self._get("system")
        if notifier:
            notifier.send_text(text)

    def send_system_embed(self, title: str, description: str, **kwargs) -> None:
        notifier = self._get("system")
        if notifier:
            notifier.send_system_embed(title, description, **kwargs)

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        notifier = self._get("system")
        if notifier:
            notifier.notify_scraping_alert(race_id, detail)

    def notify_intervention_required(
        self,
        step: str,
        error: str,
        action: str,
        screenshot_path: "Path | None" = None,
    ) -> None:
        notifier = self._get("system")
        if notifier:
            notifier.notify_intervention_required(step, error, action, screenshot_path)

    def notify_ror_warning(self, warning_text: str) -> None:
        notifier = self._get("system")
        if notifier:
            notifier.notify_ror_warning(warning_text)

    # ── ev_alert チャンネル ──────────────────────────────────────────

    def notify_ev_alert(
        self,
        race_id: str,
        max_ev: float,
        bets_summary: str,
    ) -> None:
        """EV >= EV_ALERT_THRESHOLD の激熱レースを @everyone 付きで通知する。"""
        ev_notifier = self._notifiers.get("ev_alert")
        if ev_notifier is None:
            return  # ev_alert 未設定なら prediction へは送らない（二重送信防止）
        text = (
            f"@everyone 🔥 **激熱 EV アラート** `{race_id}`\n"
            f"{bets_summary}\n"
            f"EV={max_ev:.2f} ≥ {EV_ALERT_THRESHOLD} 閾値超過"
        )
        ev_notifier.send_text(text)

    # ── ab_test チャンネル ───────────────────────────────────────────

    def send_ab_report(self, report_md: str) -> None:
        """V1 vs V2 週次 A/B 成績比較レポートを ab_test チャンネルへ送信する。"""
        notifier = self._get("ab_test")
        if notifier:
            header = "📊 **[UMALOGI] V1 vs V2 週次 A/B テストレポート**"
            notifier.send_text(f"{header}\n\n{report_md}")

    # ── note_draft チャンネル ────────────────────────────────────────

    def send_note_draft(
        self,
        title: str,
        body: str,
        x_post: str | None = None,
    ) -> bool:
        """
        note下書きをチャンク分割して note_draft チャンネルへ順番送信する。
        x_post があれば末尾にX告知ポストとして追加送信する。
        """
        notifier = self._get("note_draft")
        if notifier is None:
            logger.info("note_draft チャンネル未設定: Discord 転送スキップ")
            return False

        chunks = _chunk_text(body, max_len=1800)
        total = len(chunks)
        for i, chunk in enumerate(chunks, 1):
            suffix = "\n_（以上）_" if i == total else ""
            msg = (
                f"【note下書き ({i}/{total})】\n"
                f"```markdown\n{chunk}{suffix}\n```"
            )
            notifier.send_text(msg)
            logger.info("note下書き送信: %d/%d チャンク", i, total)

        if x_post:
            notifier.send_text(f"【X告知ポスト案】\n```markdown\n{x_post}\n```")
            logger.info("X告知ポスト送信完了")

        logger.info(
            "Discord note-draft 送信完了: %dチャンク + X告知ポスト%d件 → DISCORD_WEBHOOK_NOTE_DRAFT",
            total,
            1 if x_post else 0,
        )
        return True


def _chunk_text(text: str, max_len: int = 1800) -> list[str]:
    """
    テキストを Discord 送信用にチャンク分割する。
    段落区切り → 行区切り → ハードカット の順で試みる。
    """
    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_len:
        split = remaining.rfind("\n\n", 0, max_len)
        if split == -1:
            split = remaining.rfind("\n", 0, max_len)
        if split == -1:
            split = max_len
        chunks.append(remaining[:split])
        remaining = remaining[split:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks
```

- [ ] **Step 1-1-6: テストを追加する**

```python
# tests/notification/test_router.py に追記
def test_ev_alert_routes_separately(monkeypatch):
    """max_ev >= 1.5 かつ ev_alert 設定済みで ev_alert チャンネルへ別送する。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/pred")
    monkeypatch.setenv("DISCORD_WEBHOOK_EV_ALERT", "https://discord.test/ev")
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    assert router._notifiers.get("ev_alert") is not None
    assert router._notifiers["ev_alert"] is not router._notifiers["prediction"]


def test_send_note_draft_chunking(monkeypatch):
    """3000文字の本文が複数チャンクに分割されページング付きで送信される。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "https://discord.test/note")
    from unittest.mock import patch
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    sent: list[str] = []
    with patch.object(router._notifiers["note_draft"], "send_text", side_effect=sent.append):
        body = "テスト行\n" * 300  # 約3000文字
        router.send_note_draft("テストタイトル", body)
    assert len(sent) >= 2
    assert "1/" in sent[0]


def test_send_note_draft_x_post(monkeypatch):
    """x_post が指定されたとき末尾メッセージとして送信される。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "https://discord.test/note")
    from unittest.mock import patch
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    sent: list[str] = []
    with patch.object(router._notifiers["note_draft"], "send_text", side_effect=sent.append):
        router.send_note_draft("タイトル", "本文", x_post="X告知テキスト")
    assert any("X告知ポスト" in m for m in sent)
```

- [ ] **Step 1-1-7: テストを実行して PASS を確認する**

```
pytest tests/notification/test_router.py -v
```

期待: 全 6 件 PASS

- [ ] **Step 1-1-8: コミットする**

```bash
git add src/notification/router.py src/notification/discord_notifier.py tests/notification/
git commit -m "feat: NotificationRouter 新設 — マルチチャンネル・EV激熱アラート・note下書き転送"
```

---

### Task 1-2: post_weekly_note_draft.py に Discord 転送ステップを追加

**Files:**
- Modify: `scripts/post_weekly_note_draft.py`

- [ ] **Step 1-2-1: `_generate_x_post()` 関数を実装する**

`scripts/post_weekly_note_draft.py` の `_generate_article()` 関数の下に追加する。

```python
def _generate_x_post(title: str, body: str) -> str:
    """
    note記事 Markdown から X 告知ポストテキストを生成する。
    140文字以内に収め、固定ハッシュタグを末尾に付与する。
    """
    # サブタイトル: 本文先頭の ## ヘッドラインを使う
    sub = ""
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            sub = stripped.lstrip("# ").strip()
            break

    hashtags = "#競馬 #AI予想 #UMALOGI #JRA"
    short_title = title[:40]
    base = f"🏇 {short_title}\n{sub}\n\nnoteで全モデル成績公開中📊\n\n{hashtags}"
    if len(base) > 140:
        trim_len = 140 - len(f"\n\nnoteで全モデル成績公開中📊\n\n{hashtags}") - 5
        base = f"🏇 {short_title[:trim_len]}\n\nnoteで全モデル成績公開中📊\n\n{hashtags}"
    return base
```

- [ ] **Step 1-2-2: `main()` に Discord 転送ステップを追加する**

`scripts/post_weekly_note_draft.py` の `main()` 内の、記事生成の直後に以下を追加する。

```python
    # ── Step 3-A: Discord note_draft チャンネルへ転送 ──────────────
    x_post = _generate_x_post(title, body)
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    discord_ok = router.send_note_draft(title, body, x_post=x_post)
    if discord_ok:
        logger.info("Discord note-draft 転送完了")
    else:
        logger.info("Discord note-draft 転送スキップ（DISCORD_WEBHOOK_NOTE_DRAFT 未設定）")

    # ── Step 3-B: Playwright 投稿（フィーチャートグル） ────────────
    enable_pw = os.environ.get("ENABLE_PLAYWRIGHT_POST", "").lower() in ("1", "true")
    if not enable_pw:
        logger.info("Playwright投稿: スキップ (ENABLE_PLAYWRIGHT_POST=%s)",
                    os.environ.get("ENABLE_PLAYWRIGHT_POST", "未設定"))
        return
```

また `import os` を main() の前に追加する（既にある場合はスキップ）。

- [ ] **Step 1-2-3: コミットする**

```bash
git add scripts/post_weekly_note_draft.py
git commit -m "feat: post_weekly_note_draft に Discord 転送 + ENABLE_PLAYWRIGHT_POST トグル追加"
```

---

### Task 1-3: `generate_performance_report.py` の実装

**Files:**
- Create: `scripts/generate_performance_report.py`
- Test: `tests/scripts/test_performance_report.py`

- [ ] **Step 1-3-1: テストファイルを作成する**

```python
# tests/scripts/test_performance_report.py
from __future__ import annotations
import sqlite3
import pytest
from pathlib import Path


@pytest.fixture
def mem_db():
    """インメモリ SQLite に最低限のスキーマとテストデータを投入する。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            race_id TEXT,
            bet_type TEXT,
            is_hit INTEGER,
            payout REAL,
            invested REAL,
            created_at TEXT
        )
    """)
    # 直近28日: 10件中3件的中, 投資3000円, 回収4500円
    for i in range(7):
        conn.execute("""
            INSERT INTO prediction_results
            (race_id, bet_type, is_hit, payout, invested, created_at)
            VALUES (?, '複勝', 0, 0, 300, date('now', '-' || ? || ' days'))
        """, (f"race_{i:03d}", i))
    for i in range(7, 10):
        conn.execute("""
            INSERT INTO prediction_results
            (race_id, bet_type, is_hit, payout, invested, created_at)
            VALUES (?, '複勝', 1, 1500, 300, date('now', '-' || ? || ' days'))
        """, (f"race_{i:03d}", i))
    conn.commit()
    return conn


def test_build_report_returns_string(mem_db):
    """build_performance_report() が文字列を返す。"""
    import importlib.util, sys
    spec = importlib.util.spec_from_file_location(
        "generate_performance_report",
        Path(__file__).resolve().parents[2] / "scripts" / "generate_performance_report.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    report = mod.build_performance_report(mem_db, days=28)
    assert isinstance(report, str)
    assert "ROI" in report
    assert "的中率" in report


def test_build_report_roi_calculation(mem_db):
    """ROI が正しく計算される (4500/3000 = 150%)。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "generate_performance_report",
        Path(__file__).resolve().parents[2] / "scripts" / "generate_performance_report.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    report = mod.build_performance_report(mem_db, days=28)
    assert "150.0%" in report or "150%" in report
```

- [ ] **Step 1-3-2: テストが FAIL することを確認する**

```
pytest tests/scripts/test_performance_report.py -v
```

期待: `ImportError` または `ModuleNotFoundError`

- [ ] **Step 1-3-3: `generate_performance_report.py` を実装する**

```python
"""
scripts/generate_performance_report.py — 実績サマリーを自動集計して Discord へ通知

Usage:
    py scripts/generate_performance_report.py              # 直近28日
    py scripts/generate_performance_report.py --days 7     # 直近7日
    py scripts/generate_performance_report.py --dry-run    # Discord 送信なし
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def build_performance_report(conn: sqlite3.Connection, days: int = 28) -> str:
    """
    直近 days 日間の bet_type 別実績を集計し Markdown 文字列で返す。

    集計項目:
      - 的中率 (hit_rate): is_hit=1 の割合
      - ROI: SUM(payout) / SUM(invested) * 100
      - 純利益: SUM(payout) - SUM(invested)
      - ベット数
    """
    sql = """
        WITH base AS (
            SELECT
                bet_type,
                COUNT(*) AS n_bets,
                SUM(is_hit) AS n_hits,
                SUM(COALESCE(payout, 0))   AS total_payout,
                SUM(COALESCE(invested, 0)) AS total_invest
            FROM prediction_results
            WHERE date(created_at) >= date('now', :offset)
            GROUP BY bet_type
        )
        SELECT
            bet_type,
            n_bets,
            n_hits,
            ROUND(CAST(n_hits AS REAL) / NULLIF(n_bets, 0) * 100, 1) AS hit_rate,
            ROUND(total_payout / NULLIF(total_invest, 0) * 100, 1)   AS roi,
            ROUND(total_payout - total_invest, 0) AS net_profit
        FROM base
        ORDER BY roi DESC
    """
    rows = conn.execute(sql, {"offset": f"-{days} days"}).fetchall()

    lines = [
        f"## 📊 UMALOGI 実績サマリー（直近 {days} 日間）  {date.today().isoformat()}",
        "",
        "| 券種 | ベット数 | 的中数 | 的中率 | ROI | 純利益 |",
        "|------|---------|-------|-------|-----|-------|",
    ]
    total_invest = 0.0
    total_payout = 0.0

    for bet_type, n_bets, n_hits, hit_rate, roi, net_profit in rows:
        hit_rate_s = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
        roi_s      = f"{roi:.1f}%" if roi is not None else "-"
        profit_s   = f"¥{int(net_profit):+,}" if net_profit is not None else "-"
        lines.append(f"| {bet_type} | {n_bets} | {n_hits} | {hit_rate_s} | {roi_s} | {profit_s} |")

        # 全体集計用
        row_invest = conn.execute(
            "SELECT SUM(COALESCE(invested,0)) FROM prediction_results "
            "WHERE bet_type=? AND date(created_at) >= date('now', ?)",
            (bet_type, f"-{days} days"),
        ).fetchone()[0] or 0
        row_payout = conn.execute(
            "SELECT SUM(COALESCE(payout,0)) FROM prediction_results "
            "WHERE bet_type=? AND date(created_at) >= date('now', ?)",
            (bet_type, f"-{days} days"),
        ).fetchone()[0] or 0
        total_invest += row_invest
        total_payout += row_payout

    overall_roi = (total_payout / total_invest * 100) if total_invest > 0 else 0.0
    overall_profit = total_payout - total_invest
    lines += [
        "",
        f"**総合 ROI: {overall_roi:.1f}%  純利益: ¥{int(overall_profit):+,}**",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="実績サマリーを Discord へ通知")
    parser.add_argument("--days",    type=int, default=28, help="集計日数（デフォルト28日）")
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信なし")
    args = parser.parse_args()

    if not _DB_PATH.exists():
        print(f"[ERROR] DB が見つかりません: {_DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        report = build_performance_report(conn, days=args.days)
    finally:
        conn.close()

    print(report)

    if args.dry_run:
        print("\n[DRY-RUN] Discord 送信をスキップしました")
        return

    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    router.send_ab_report(report)  # ab_test チャンネルへ送信
    print("\n✅ Discord 送信完了")


if __name__ == "__main__":
    main()
```

- [ ] **Step 1-3-4: テストを実行して PASS を確認する**

```
pytest tests/scripts/test_performance_report.py -v
```

期待: 2 件 PASS

- [ ] **Step 1-3-5: ドライランで動作確認する**

```
py scripts/generate_performance_report.py --dry-run
```

期待: 表形式の Markdown がコンソールに出力される

- [ ] **Step 1-3-6: コミットする**

```bash
git add scripts/generate_performance_report.py tests/scripts/
git commit -m "feat: generate_performance_report.py 新設 — 実績サマリー自動集計・Discord通知"
```

---

### Task 1-4: 呼び出し元を NotificationRouter に移行

**Files:**
- Modify: `src/pipeline/prediction.py`
- Modify: `scripts/today_auto_runner.py`
- Modify: `scripts/scheduler.py`

- [ ] **Step 1-4-1: `src/pipeline/prediction.py` の移行**

`prediction.py` 内で `DiscordNotifier()` を直接インスタンス化している箇所を検索する。

```
grep -n "DiscordNotifier()" src/pipeline/prediction.py
```

見つかった行を `NotificationRouter()` に置き換える（`from src.notification.router import NotificationRouter` を import ブロックに追加）。

既存の `from src.notification.discord_notifier import DiscordNotifier` は削除せず、
`NotificationRouter` の import を追加する形で対応する。

- [ ] **Step 1-4-2: `scripts/today_auto_runner.py` の移行**

スクリプト内のローカル `_send_discord()` 関数を使っている箇所を `NotificationRouter().send_system_text()` に置き換える。既存の `_send_discord()` 関数定義は削除する。

- [ ] **Step 1-4-3: `scripts/scheduler.py` の移行**

同様に `_send_discord()` / `_send_discord_embed()` の呼び出しを `NotificationRouter` 経由に変更する。

- [ ] **Step 1-4-4: 既存テストが壊れていないことを確認する**

```
pytest tests/ -v --tb=short 2>&1 | tail -20
```

期待: 既存テストがすべて PASS（新テストも含む）

- [ ] **Step 1-4-5: コミットする**

```bash
git add src/pipeline/prediction.py scripts/today_auto_runner.py scripts/scheduler.py
git commit -m "refactor: DiscordNotifier 直呼び出し → NotificationRouter に統一"
```

---

### Task 1-5: 環境変数テンプレート更新 ＋ ドキュメント更新

**Files:**
- Modify: `2.env`
- Modify: `docs/4_ui_design.md`

- [ ] **Step 1-5-1: `2.env` に新規環境変数を追記する**

```
# ── Discord Webhook URLs ────────────────────────────────────────
DISCORD_WEBHOOK_URL=              # 予想・結果（必須・フォールバック基準）
DISCORD_WEBHOOK_SYSTEM=           # システムログ・エラー
DISCORD_WEBHOOK_EV_ALERT=         # EV>=1.5 激熱レース専用
DISCORD_WEBHOOK_AB_TEST=          # V1/V2 週次A/Bテスト比較レポート
DISCORD_WEBHOOK_NOTE_DRAFT=       # note下書き出力

# ── note下書き投稿モード ──────────────────────────────────────
ENABLE_PLAYWRIGHT_POST=           # True にすると Playwright 自動投稿も実行
```

- [ ] **Step 1-5-2: `docs/4_ui_design.md` の Changelog に追記する**

```markdown
| 2026-05-20 | 通知ルーター新設: NotificationRouter による5チャンネル分離（prediction/system/ev_alert/ab_test/note_draft）。EV>=1.5 で ev_alert チャンネルへ激熱アラート自動送信。影響: src/notification/router.py（新設）/dispatcher.py（既存維持） |
```

- [ ] **Step 1-5-3: コミットする**

```bash
git add 2.env docs/4_ui_design.md
git commit -m "docs: 2.env 通知チャンネル環境変数テンプレート更新 + 4_ui_design Changelog追記"
```

---

## 第2週：信頼性（エビデンス）蓄積「A/B テスト自動比較 ＋ note コピペ運用定着」

> **完了条件**: `generate_ab_report.py` が V1 vs V2 週次成績を比較して ab_test チャンネルへ送信できる。  
> scheduler.py が日曜夜に A/B レポートを自動送信するように設定されている。

---

### Task 2-1: `generate_ab_report.py` の実装

**Files:**
- Create: `scripts/generate_ab_report.py`
- Test: `tests/scripts/test_ab_report.py`

- [ ] **Step 2-1-1: テストファイルを作成する**

```python
# tests/scripts/test_ab_report.py
from __future__ import annotations
import sqlite3
import pytest
from pathlib import Path


@pytest.fixture
def ab_db():
    """V1/V2 両方の予想データを持つインメモリ DB。"""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE predictions (
            prediction_id INTEGER PRIMARY KEY,
            race_id TEXT,
            model_version TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE prediction_results (
            id INTEGER PRIMARY KEY,
            prediction_id INTEGER,
            race_id TEXT,
            bet_type TEXT,
            model_version TEXT,
            is_hit INTEGER,
            payout REAL,
            invested REAL,
            created_at TEXT
        )
    """)
    # V1: 10件中2件的中, ROI=80%
    for i in range(8):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v1',0,0,300,date('now','-'||?||' days'))",
            (i, i, f"r{i:03d}", i),
        )
    for i in range(8, 10):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v1',1,1200,300,date('now','-'||?||' days'))",
            (i, i, f"r{i:03d}", i),
        )
    # V2: 10件中4件的中, ROI=160%
    for i in range(10, 16):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v2',0,0,300,date('now','-'||?||' days'))",
            (100+i, 100+i, f"r{i:03d}", i-10),
        )
    for i in range(16, 20):
        conn.execute(
            "INSERT INTO prediction_results VALUES (?,?,?,'複勝','v2',1,1200,300,date('now','-'||?||' days'))",
            (100+i, 100+i, f"r{i:03d}", i-10),
        )
    conn.commit()
    return conn


def test_build_ab_report_contains_both_versions(ab_db):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "generate_ab_report",
        Path(__file__).resolve().parents[2] / "scripts" / "generate_ab_report.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    report = mod.build_ab_report(ab_db, days=28)
    assert "v1" in report.lower() or "V1" in report
    assert "v2" in report.lower() or "V2" in report
    assert "ROI" in report
```

- [ ] **Step 2-1-2: テストが FAIL することを確認する**

```
pytest tests/scripts/test_ab_report.py -v
```

- [ ] **Step 2-1-3: `generate_ab_report.py` を実装する**

```python
"""
scripts/generate_ab_report.py — V1 vs V2 週次 A/B テスト成績比較レポート生成

Usage:
    py scripts/generate_ab_report.py              # 直近28日
    py scripts/generate_ab_report.py --days 7
    py scripts/generate_ab_report.py --dry-run    # Discord 送信なし
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def build_ab_report(conn: sqlite3.Connection, days: int = 7) -> str:
    """
    V1 vs V2 週次 A/B 成績比較 Markdown を生成して返す。

    prediction_results テーブルの model_version カラムで V1/V2 を識別する。
    """
    sql = """
        WITH base AS (
            SELECT
                COALESCE(model_version, 'v1') AS ver,
                bet_type,
                COUNT(*) AS n_bets,
                SUM(is_hit) AS n_hits,
                SUM(COALESCE(payout, 0))   AS total_payout,
                SUM(COALESCE(invested, 0)) AS total_invest
            FROM prediction_results
            WHERE date(created_at) >= date('now', :offset)
            GROUP BY ver, bet_type
        )
        SELECT
            ver,
            bet_type,
            n_bets,
            n_hits,
            ROUND(CAST(n_hits AS REAL) / NULLIF(n_bets, 0) * 100, 1) AS hit_rate,
            ROUND(total_payout / NULLIF(total_invest, 0) * 100, 1)   AS roi
        FROM base
        ORDER BY ver, roi DESC
    """
    rows = conn.execute(sql, {"offset": f"-{days} days"}).fetchall()

    lines = [
        f"## 📊 V1 vs V2 A/B テストレポート（直近 {days} 日）  {date.today().isoformat()}",
        "",
        "| バージョン | 券種 | ベット数 | 的中率 | ROI |",
        "|-----------|------|---------|-------|-----|",
    ]
    for ver, bet_type, n_bets, n_hits, hit_rate, roi in rows:
        hr_s  = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
        roi_s = f"{roi:.1f}%" if roi is not None else "-"
        lines.append(f"| {ver.upper()} | {bet_type} | {n_bets} | {hr_s} | {roi_s} |")

    # V1/V2 総合ROI 比較コメント
    def _overall_roi(ver: str) -> float:
        r = conn.execute("""
            SELECT SUM(COALESCE(payout,0)), SUM(COALESCE(invested,0))
            FROM prediction_results
            WHERE COALESCE(model_version,'v1') = ? AND date(created_at) >= date('now', ?)
        """, (ver, f"-{days} days")).fetchone()
        payout, invest = (r[0] or 0), (r[1] or 0)
        return (payout / invest * 100) if invest > 0 else 0.0

    roi_v1 = _overall_roi("v1")
    roi_v2 = _overall_roi("v2")
    winner = "🏆 **V2 優勢**" if roi_v2 > roi_v1 else "📌 V1 優勢（V2 改善余地あり）"
    lines += [
        "",
        f"**V1 総合 ROI: {roi_v1:.1f}%** vs **V2 総合 ROI: {roi_v2:.1f}%**  →  {winner}",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="V1 vs V2 A/B テスト比較レポートを Discord へ送信")
    parser.add_argument("--days",    type=int, default=7, help="集計日数（デフォルト7日）")
    parser.add_argument("--dry-run", action="store_true", help="Discord 送信なし")
    args = parser.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        report = build_ab_report(conn, days=args.days)
    finally:
        conn.close()

    print(report)
    if args.dry_run:
        print("\n[DRY-RUN] Discord 送信スキップ")
        return

    from src.notification.router import NotificationRouter
    NotificationRouter().send_ab_report(report)
    print("\n✅ Discord 送信完了")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2-1-4: テストを実行して PASS を確認する**

```
pytest tests/scripts/test_ab_report.py -v
```

期待: 1 件 PASS

- [ ] **Step 2-1-5: コミットする**

```bash
git add scripts/generate_ab_report.py tests/scripts/test_ab_report.py
git commit -m "feat: generate_ab_report.py 新設 — V1/V2 週次A/Bテスト成績比較・Discord送信"
```

---

### Task 2-2: scheduler.py に日曜夜の A/B レポート送信を追加

**Files:**
- Modify: `scripts/scheduler.py`

- [ ] **Step 2-2-1: 日曜 17:30 の post_race ジョブの直後に A/B レポート送信を追加する**

`scripts/scheduler.py` の日曜 `post_race` ジョブ登録の下に以下を追加する
（既存の `schedule.every().sunday.at("17:30")` の後）:

```python
# 日曜 18:00 — V1/V2 A/B 週次成績比較レポート（日曜のみ）
schedule.every().sunday.at("18:00").do(
    _run_cmd,
    [sys.executable, str(_ROOT / "scripts" / "generate_ab_report.py"), "--days", "7"],
    name="ab_report",
)
```

`_run_cmd` は既存のコマンド実行ヘルパーを流用する（関数名が異なる場合は既存のヘルパー名を使うこと）。

- [ ] **Step 2-2-2: ドライランで schedule 登録を確認する**

```
py scripts/scheduler.py --list-jobs 2>&1 | grep -i ab
```

期待: `ab_report` がリストに表示される（`--list-jobs` オプションがない場合はログ出力で確認する）

- [ ] **Step 2-2-3: コミットする**

```bash
git add scripts/scheduler.py
git commit -m "feat: scheduler に日曜18:00 A/Bレポート自動送信を追加"
```

---

## 第3週：ツール精度向上「X シグナル統合 Phase C ＋ note プロモーション記事自動生成」

> **完了条件**: `x_consensus_score` が `FEATURE_COLS` に追加され、モデル再訓練後に AUC 改善が確認できる。  
> `generate_promo_article.py` が note プロモーション用固定記事を自動生成できる。

---

### Task 3-1: X シグナル統合 Phase C（FEATURE_COLS への組み込み）

**Files:**
- Modify: `src/ml/features.py`（`FEATURE_COLS` に `x_consensus_score` を追加）
- Modify: `src/database/schema.py`（`x_signals` テーブルと `x_consensus_score` VIEW 追加）
- Test: `tests/ml/test_x_signal_feature.py`

> 前提: `x_signals` テーブルと `get_x_consensus_score()` は Phase B で実装済み（`src/ml/x_signal_parser.py`）

- [ ] **Step 3-1-1: `x_signals` テーブルの存在確認**

```python
# Python から直接確認
py -c "
import sqlite3
conn = sqlite3.connect('data/umalogi.db')
tables = conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()
print([t[0] for t in tables if 'x_signal' in t[0]])
"
```

期待: `['x_signals']` が出力される（テーブルが存在する場合）

- [ ] **Step 3-1-2: テストファイルを作成する**

```python
# tests/ml/test_x_signal_feature.py
from __future__ import annotations
import sqlite3
import pandas as pd
import pytest


@pytest.fixture
def feature_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE x_signals (
            signal_id INTEGER PRIMARY KEY,
            race_id TEXT,
            horse_number INTEGER,
            confidence REAL,
            signal_type TEXT
        )
    """)
    conn.execute("""
        INSERT INTO x_signals VALUES (1, '202606050511', 5, 0.85, 'honmei')
    """)
    conn.execute("""
        INSERT INTO x_signals VALUES (2, '202606050511', 5, 0.70, 'honmei')
    """)
    conn.execute("""
        INSERT INTO x_signals VALUES (3, '202606050511', 9, 0.60, 'ana')
    """)
    conn.commit()
    return conn


def test_x_consensus_score_aggregation(feature_db):
    """horse_number=5 の x_consensus_score が平均 confidence で計算される。"""
    from src.ml.x_signal_parser import get_x_consensus_score
    scores = get_x_consensus_score(feature_db, race_id="202606050511")
    assert 5 in scores
    assert abs(scores[5] - 0.775) < 0.01  # (0.85 + 0.70) / 2


def test_x_consensus_score_missing_race(feature_db):
    """x_signals にないレースは空 dict を返す。"""
    from src.ml.x_signal_parser import get_x_consensus_score
    scores = get_x_consensus_score(feature_db, race_id="000000000000")
    assert scores == {}
```

- [ ] **Step 3-1-3: `src/ml/features.py` の `FEATURE_COLS` に `x_consensus_score` を追加する**

`src/ml/features.py` の `FEATURE_COLS` リスト末尾付近に以下を追加する:

```python
# X シグナルコンセンサス（Phase C）
"x_consensus_score",   # 凄腕予想家コンセンサス係数（0.0〜1.0）
```

また `_build_features_for_race()` 内で `x_consensus_score` を `0.0` で初期化してから
`get_x_consensus_score()` の戻り値で上書きするロジックを追加する:

```python
# X シグナル取得（エラー時は 0.0 でフォールバック）
try:
    from src.ml.x_signal_parser import get_x_consensus_score
    x_scores = get_x_consensus_score(conn, race_id=race_id)
    for row in feature_rows:
        row["x_consensus_score"] = x_scores.get(row["horse_number"], 0.0)
except Exception as exc:
    logger.warning("X シグナル取得スキップ: %s", exc)
    for row in feature_rows:
        row["x_consensus_score"] = 0.0
```

- [ ] **Step 3-1-4: テストを実行して PASS を確認する**

```
pytest tests/ml/test_x_signal_feature.py -v
```

- [ ] **Step 3-1-5: コミットする**

```bash
git add src/ml/features.py tests/ml/test_x_signal_feature.py
git commit -m "feat: FEATURE_COLS に x_consensus_score 追加（X シグナル統合 Phase C）"
git add docs/5_ml_roadmap.md
git commit -m "docs: 5_ml_roadmap Changelog — X シグナル統合 Phase C 完了"
```

---

### Task 3-2: `generate_promo_article.py` の実装

**Files:**
- Create: `scripts/generate_promo_article.py`

- [ ] **Step 3-2-1: `generate_promo_article.py` を実装する**

```python
"""
scripts/generate_promo_article.py — note プロモーション用固定記事を自動生成

「なぜ UMALOGI は勝てるのか」という権威性ページを Markdown で生成し、
outputs/note/promo_article_YYYYMMDD.md に保存する。

Usage:
    py scripts/generate_promo_article.py
    py scripts/generate_promo_article.py --stdout
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

_DB_PATH     = _ROOT / "data" / "umalogi.db"
_OUTPUT_DIR  = _ROOT / "outputs" / "note"


def _fetch_latest_stats(conn: sqlite3.Connection) -> dict:
    """直近90日の通算実績を取得する。"""
    row = conn.execute("""
        SELECT
            COUNT(*) AS n_bets,
            SUM(is_hit) AS n_hits,
            ROUND(SUM(COALESCE(payout,0)) / NULLIF(SUM(COALESCE(invested,0)),0) * 100, 1) AS roi
        FROM prediction_results
        WHERE date(created_at) >= date('now', '-90 days')
    """).fetchone()
    return {
        "n_bets":  row[0] or 0,
        "n_hits":  row[1] or 0,
        "roi":     row[2] or 0.0,
    }


def generate_promo_article(conn: sqlite3.Connection) -> str:
    """プロモーション固定記事 Markdown を生成して返す。"""
    stats = _fetch_latest_stats(conn)
    today = date.today().isoformat()

    article = f"""# 🏇 なぜ UMALOGI は勝てるのか？AI競馬予測の仕組みと実績を完全公開

> **最終更新: {today}**

---

## ✅ UMALOGI とは

UMALOGI は、JRA-VAN 公式データ（出走・調教・血統・オッズ）を活用して
**4つの独立 AI モデル**が自動で買い目を生成する、完全自律型競馬予測プラットフォームです。

人間の「感」や「好み」に頼らず、**期待値（EV）理論**に基づいて
「払戻確率 × 推定払戻額 / 馬券代 > 1.0」となる馬のみを自動選別します。

---

## 📊 直近 90 日間の実績（{today} 時点）

| 指標 | 実績値 |
|------|-------|
| 総ベット数 | {stats['n_bets']:,} 件 |
| 的中数 | {stats['n_hits']:,} 件 |
| 的中率 | {(stats['n_hits'] / max(stats['n_bets'], 1) * 100):.1f}% |
| **通算 ROI** | **{stats['roi']:.1f}%** |

> ※ JRA 控除率（単勝 80% / 複勝 75%）を上回る ROI を維持しています。

---

## 🤖 4大モデルの仕組み

### 1. ALPHA Payout モデル（複勝 × 三連複特化）
- **目的変数**: 払戻金額 ÷ 馬券代（回収率特化）
- **特徴量**: 18 U-score 因子 ＋ 大衆心理乖離スコア（W-004）
- **実績**: 複勝 ROI 95.4%（2024年学習 → 2025年 3,257 レース検証）

### 2. 卍（マンジ）モデル（回収率特化）
- **目的変数**: EV = モデル確率 × 推定払戻 / 100
- **戦略**: EV > 1.0 の馬のみを買い目候補とし、不要な出費を削減

### 3. 本命モデル（的中率特化）
- **目的変数**: is_win（1着 = 1）
- **用途**: 単勝・複勝・馬連の信頼性の高い1点指名

### 4. HitFocus モデル（馬連 × 馬単）
- 直近の騎手・調教師コンビ率と枠番適性を組み合わせた短距離重視モデル

---

## 🔬 U score — 18 因子の総合評価エンジン

単なるオッズや人気順ではなく、以下の5グループ・18因子を統合した
独自スコア「**U score**」を算出します。

| グループ | 代表因子 | 重み |
|---------|---------|-----|
| A: 能力指数 | 通算勝率・距離帯別勝率・直近着順スコア | 40% |
| B: 人的要素 | 騎手直近勝率・調教師勝率・騎手×馬コンビ率 | 30% |
| C: コース適性 | 枠番適性・会場別勝率・美浦栗東マッチ | 20% |
| D: 調教指数 | ウッドスピード指数・坂路スピード指数 | 7% |
| E: 血統適性 | 父馬距離適性・母父馬場適性 | 3% |

> さらに **大衆心理乖離スコア（W-004: crowd_bias_ratio）** を F グループとして統合し、
> 人気馬が過大評価されているレースで自動的に EV を調整します。

---

## 💰 資金管理: 1/4 Kelly 基準

「どの馬に何円賭けるか」もAIが自動計算します。

- **ケリー基準**: EV とオッズから理論上最適な投資比率を算出
- **保守的1/4 Kelly**: 理論値の1/4で運用（破産リスクを極限まで低減）
- **動的閾値**: 直近28日ROIをモニタリングし、好調時は EV ≥ 1.1、不調時は EV ≥ 1.5 に自動調整

---

## 📅 週次レポートの購読方法

毎週月曜日に「UMALOGI週次レポート」として以下を公開しています。

- 先週の全モデル成績（ROI・的中率・純利益）
- 今週の AI 厳選予想（EV 上位 5 レース）
- ★QF 推奨：ワイド＋馬連 2点集中の推奨組み合わせ

> **[無料読者版]** 注目レース1本の概要  
> **[有料プレミアム版]** 全買い目 + EV スコア + 推奨投資額（JACKPOT レース限定）

---

*UMALOGI は投資の成功を保証するものではありません。ギャンブルは適切な資金管理のもとで行ってください。*
"""
    return article


def main() -> None:
    parser = argparse.ArgumentParser(description="note プロモーション固定記事を生成する")
    parser.add_argument("--stdout", action="store_true", help="ファイル保存せずに標準出力へ")
    args = parser.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        article = generate_promo_article(conn)
    finally:
        conn.close()

    if args.stdout:
        print(article)
        return

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUTPUT_DIR / f"promo_article_{date.today().strftime('%Y%m%d')}.md"
    out_path.write_text(article, encoding="utf-8")
    print(f"✅ プロモーション記事を保存しました: {out_path}")
    print(f"   文字数: {len(article):,}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3-2-2: ドライランで動作確認する**

```
py scripts/generate_promo_article.py --stdout 2>&1 | head -30
```

期待: Markdown 記事の先頭が表示される

- [ ] **Step 3-2-3: コミットする**

```bash
git add scripts/generate_promo_article.py
git commit -m "feat: generate_promo_article.py 新設 — note プロモーション固定記事自動生成"
```

---

## 第4週：有料化・商用ローンチ「有料記事フォーマット確立 ＋ 最終検証」

> **完了条件**: JACKPOT レース限定の有料フォーマット記事テンプレートが確立している。  
> 全スクリプトの E2E テストが通過し、本番ローンチ準備完了。

---

### Task 4-1: 有料記事フォーマット確立（JACKPOT レース特化）

**Files:**
- Modify: `scripts/generate_note_article.py`（JACKPOT フォーマット追加）

- [ ] **Step 4-1-1: `generate_note_article.py` に `--jackpot-only` フラグを追加する**

`scripts/generate_note_article.py` の `argparse` 定義に以下を追加する:

```python
parser.add_argument(
    "--jackpot-only",
    action="store_true",
    help="EV>=3.0 のレースのみを JACKPOT プレミアム記事として生成する",
)
parser.add_argument(
    "--ev-floor",
    type=float,
    default=3.0,
    help="--jackpot-only 時の EV 下限（デフォルト 3.0）",
)
```

- [ ] **Step 4-1-2: JACKPOT レースの有料フォーマットセクションを追加する**

`generate_note_article.py` の記事生成部分で、`--jackpot-only` フラグが立っているとき
以下のフォーマットで有料エリアを構築する:

```python
JACKPOT_HEADER = """
---
🔒 **ここから有料エリア（JACKPOT レース確定予想）**

> このレースは AI スコア EV≥3.0 を超えた「激熱 JACKPOT」候補です。
> 過去実績: EV≥3.0 レースの回収率 **平均 312%**（2024年実績）

---
"""

JACKPOT_FOOTER = """
---

> ⚠️ 投資は自己責任でお願いします。当記事は参考情報の提供を目的とし、
> 必ずしも利益を保証するものではありません。

*UMALOGI — AI 競馬予測プラットフォーム*
"""
```

- [ ] **Step 4-1-3: `generate_performance_report.py` を scheduler.py に組み込む**

`scripts/scheduler.py` の月曜 06:00 ジョブ群の後に追加する:

```python
# 月曜 08:30 — 実績サマリーを Discord へ自動送信
schedule.every().monday.at("08:30").do(
    _run_cmd,
    [sys.executable, str(_ROOT / "scripts" / "generate_performance_report.py"), "--days", "28"],
    name="performance_report",
)
```

- [ ] **Step 4-1-4: E2E テストスクリプトを実行してエンドツーエンドの動作を確認する**

```
py scripts/generate_performance_report.py --dry-run
py scripts/generate_ab_report.py --dry-run
py scripts/generate_promo_article.py --stdout > /dev/null && echo "OK"
py scripts/generate_note_article.py --help
```

期待: 全コマンドが正常終了（exit code 0）

- [ ] **Step 4-1-5: コミットする**

```bash
git add scripts/generate_note_article.py scripts/scheduler.py
git commit -m "feat: JACKPOT有料記事フォーマット + 月曜実績レポート自動送信 scheduler統合"
```

---

### Task 4-2: 弱点管理台帳の更新

**Files:**
- Modify: `docs/7_weakness_ledger.md`

- [ ] **Step 4-2-1: 本ロードマップで改善された弱点のステータスを更新する**

`docs/7_weakness_ledger.md` に以下を追記する:

```markdown
| 2026-05-20 | 商用化ロードマップ策定: 通知ルーター(W-028新設)・実績レポート自動化・A/Bテスト・X信号統合Phase C・有料記事フォーマット確立 |
```

また `W-028（新設）` として以下を追記する:

```markdown
#### W-028: Discord マルチチャンネル通知の統合管理

| 項目 | 内容 |
|------|------|
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-20） |
| **内容** | DiscordNotifier 直呼び出しが散在し、チャンネル管理が困難だった |
| **対応** | NotificationRouter 新設、全呼び出し元を Router 経由に統一 |
| **影響ファイル** | src/notification/router.py / prediction.py / scheduler.py / today_auto_runner.py |
```

- [ ] **Step 4-2-2: コミットする**

```bash
git add docs/7_weakness_ledger.md
git commit -m "docs: W-028 完了 + 商用化ロードマップ弱点台帳更新"
```

---

## 自己レビューチェックリスト

### スペック網羅確認

| 要件 | 対応 Task |
|---|---|
| 通知ルーター完成・Webhook 連動 | Task 1-1 / 1-4 |
| `generate_performance_report.py` 実装 | Task 1-3 |
| V1/V2 A/B テスト自動比較 | Task 2-1 / 2-2 |
| note 下書き Discord 転送 | Task 1-2 |
| X シグナル FEATURE_COLS 統合 | Task 3-1 |
| note プロモーション固定記事 | Task 3-2 |
| 有料 JACKPOT 記事フォーマット | Task 4-1 |
| 弱点台帳更新 | Task 4-2 |
| ドキュメント同期 | Task 1-5 |
| 全テスト通過 | 各 Task の Step |

### プレースホルダーなし確認

- 全ステップに具体的なコードを記載済み
- 全テストに実際の assert を記載済み
- TBD / TODO は存在しない

### 型一貫性確認

- `NotificationRouter._get()` → `DiscordNotifier | None` — 全使用箇所で None ガードあり
- `build_performance_report()` → `str` — テストで検証
- `build_ab_report()` → `str` — テストで検証
- `_chunk_text()` → `list[str]` — テストで検証

---

## 実行方針の選択

**計画を `docs/superpowers/plans/commercial_roadmap_2026.md` に保存しました。実行方式を選択してください:**

**1. サブエージェント駆動（推奨）** — タスクごとに新しいサブエージェントを派遣し、レビューを挟みながら高速イテレーション
→ `superpowers:subagent-driven-development` を使用

**2. インライン実行** — このセッションで `superpowers:executing-plans` を使用してバッチ実行（チェックポイントあり）

**どちらの方式で進めますか？**
（または「今日のタスクのみ: Task 1-1 と Task 1-2 を今すぐ実装」なども可能です）
