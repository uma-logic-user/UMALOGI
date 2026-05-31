# Discord 通知ルーター & note下書き転送 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `NotificationRouter` を新設してマルチチャンネル Discord 通知を一元管理し、`post_weekly_note_draft.py` に Discord 転送と Playwright トグルを追加する。

**Architecture:** `DiscordNotifier` はそのまま保持し、`NotificationRouter` がその上にチャンネル振り分け層を追加する。`prediction.py` / `today_auto_runner.py` / `scheduler.py` の呼び出し元を `NotificationRouter` に差し替える。`post_weekly_note_draft.py` はステップ 3-A (Discord 転送) を追加し、ステップ 3-B (Playwright) を `ENABLE_PLAYWRIGHT_POST` で ON/OFF できるようにする。

**Tech Stack:** Python 3.11+, requests, python-dotenv, pytest, unittest.mock

---

## File Map

| 操作 | ファイル | 変更内容 |
|------|---------|---------|
| 新設 | `src/notification/router.py` | `NotificationRouter` + `_chunk_text` + `_generate_x_post` |
| 新設 | `tests/notification/__init__.py` | 空 |
| 新設 | `tests/notification/test_router.py` | ルーター & Playwright トグルの単体テスト |
| 改修 | `src/notification/discord_notifier.py` | `send_prediction_embed()` を追加（scheduler 用） |
| 改修 | `src/notification/__init__.py` | `NotificationRouter` を再エクスポート |
| 改修 | `scripts/post_weekly_note_draft.py` | `_should_publish_playwright` + `_generate_x_post` + Discord 転送ステップを追加 |
| 改修 | `src/pipeline/prediction.py` | `DiscordNotifier()` → `NotificationRouter()` |
| 改修 | `scripts/today_auto_runner.py` | ローカル `_send_discord*` 関数を削除し `NotificationRouter` に統一 |
| 改修 | `scripts/scheduler.py` | ローカル `_send_discord*` 関数を削除し `NotificationRouter` に統一 |
| 改修 | `2.env` | 新規環境変数を追記 |

---

## Task 1: テストファイルを作成する（TDD 最初のステップ）

**Files:**
- Create: `tests/notification/__init__.py`
- Create: `tests/notification/test_router.py`

- [ ] **Step 1-1: テストディレクトリとファイルを作成する**

```python
# tests/notification/__init__.py
# (空ファイル)
```

```python
# tests/notification/test_router.py
"""NotificationRouter + post_weekly_note_draft の単体テスト。"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ── ヘルパー ──────────────────────────────────────────────────────────────────

def _mock_post(sent: list) -> object:
    """requests.post のモックを返す。呼び出しごとに sent に URL を追加する。"""
    def _post(url, **kwargs):
        sent.append({"url": url, "json": kwargs.get("json", {})})
        m = MagicMock()
        m.status_code = 204
        return m
    return _post


# ── NotificationRouter テスト ─────────────────────────────────────────────────

class TestNotificationRouter:
    def test_fallback_to_prediction(self, monkeypatch):
        """DISCORD_WEBHOOK_SYSTEM 未設定時にシステム通知が予想チャンネルへフォールバックする。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://fake-prediction")
        monkeypatch.delenv("DISCORD_WEBHOOK_SYSTEM", raising=False)
        monkeypatch.delenv("DISCORD_SYSTEM_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("DISCORD_WEBHOOK_EV_ALERT", raising=False)
        monkeypatch.delenv("DISCORD_WEBHOOK_AB_TEST", raising=False)
        monkeypatch.delenv("DISCORD_WEBHOOK_NOTE_DRAFT", raising=False)

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.send_system_text("system msg")

        assert any("fake-prediction" in e["url"] for e in sent), \
            f"prediction へのフォールバックが発生していない: {sent}"

    def test_ev_alert_routes_separately(self, monkeypatch):
        """max_ev >= 1.5 かつ ev_alert URL 設定済みで ev_alert チャンネルへ別送される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://fake-prediction")
        monkeypatch.setenv("DISCORD_WEBHOOK_EV_ALERT", "http://fake-ev-alert")
        monkeypatch.delenv("DISCORD_WEBHOOK_SYSTEM", raising=False)
        monkeypatch.delenv("DISCORD_SYSTEM_WEBHOOK_URL", raising=False)

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.notify_ev_alert("2026050105050101", 2.0, "EV=2.00 テスト")

        urls = [e["url"] for e in sent]
        assert "http://fake-ev-alert" in urls, "ev_alert URL への送信がない"
        assert "http://fake-prediction" not in urls, \
            "prediction チャンネルへ二重送信されている（ev_alert は prediction へ fallback しない）"

    def test_send_note_draft_chunking(self, monkeypatch):
        """3000文字の本文が複数チャンクに分割され、ページング番号付きで送信される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "http://fake-draft")
        monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            result = router.send_note_draft("テストタイトル", "あ" * 3000)

        assert result is True, "send_note_draft が False を返した"
        assert len(sent) >= 2, f"チャンク数が1以下: {len(sent)}"
        first_content = sent[0]["json"]["content"]
        last_content = sent[-2]["json"]["content"]   # 最後から2番目 = 最終チャンク（x_post の前）
        assert "(1/" in first_content, f"ページング番号がない: {first_content[:80]}"
        assert "_（以上）_" in last_content or "_（以上）_" in sent[-1]["json"].get("content", ""), \
            "終端マーカーがない"

    def test_send_note_draft_x_post(self, monkeypatch):
        """x_post が指定されたとき末尾メッセージとして送信される。"""
        monkeypatch.setenv("DISCORD_WEBHOOK_NOTE_DRAFT", "http://fake-draft")
        monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)

        sent: list = []
        with patch("requests.post", _mock_post(sent)):
            from src.notification.router import NotificationRouter
            router = NotificationRouter()
            router.send_note_draft("タイトル", "短い本文", x_post="X告知テキストXX")

        assert any("X告知テキストXX" in e["json"].get("content", "") for e in sent), \
            "X 告知テキストが送信されていない"

    def test_all_channels_unset(self, monkeypatch):
        """全チャンネル URL 未設定でも例外が発生しない。"""
        for key in [
            "DISCORD_WEBHOOK_URL", "DISCORD_WEBHOOK_SYSTEM",
            "DISCORD_WEBHOOK_EV_ALERT", "DISCORD_WEBHOOK_AB_TEST",
            "DISCORD_WEBHOOK_NOTE_DRAFT", "DISCORD_SYSTEM_WEBHOOK_URL",
        ]:
            monkeypatch.delenv(key, raising=False)

        from src.notification.router import NotificationRouter
        router = NotificationRouter()
        router.send_text("t")
        router.send_system_text("t")
        router.notify_ev_alert("2026050105050101", 2.0, "e")
        router.send_ab_report("r")
        result = router.send_note_draft("title", "body")
        assert result is False


# ── post_weekly_note_draft のトグルテスト ─────────────────────────────────────

class TestPlaywrightToggle:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_PLAYWRIGHT_POST", raising=False)
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_off_with_zero(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "0")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_off_with_false_str(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "False")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is False

    def test_on_with_one(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "1")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is True

    def test_on_with_true_str(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PLAYWRIGHT_POST", "True")
        from scripts.post_weekly_note_draft import _should_publish_playwright
        assert _should_publish_playwright() is True
```

- [ ] **Step 1-2: テストが FAIL することを確認する**

```
pytest tests/notification/test_router.py -v
```

期待出力: `ImportError: No module named 'src.notification.router'` または `ImportError: cannot import name '_should_publish_playwright'`

- [ ] **Step 1-3: コミット**

```bash
git add tests/notification/__init__.py tests/notification/test_router.py
git commit -m "test: NotificationRouter & Playwright トグルの failing テスト追加"
```

---

## Task 2: `src/notification/router.py` を実装する

**Files:**
- Create: `src/notification/router.py`

- [ ] **Step 2-1: router.py を作成する**

```python
# src/notification/router.py
"""
マルチチャンネル Discord 通知ルーター。

チャンネルマップ:
  prediction  → DISCORD_WEBHOOK_URL          (必須・フォールバック基準)
  system      → DISCORD_WEBHOOK_SYSTEM       (旧: DISCORD_SYSTEM_WEBHOOK_URL 後方互換)
  ev_alert    → DISCORD_WEBHOOK_EV_ALERT     (EV>=1.5 激熱)
  ab_test     → DISCORD_WEBHOOK_AB_TEST      (V1/V2 A/B比較)
  note_draft  → DISCORD_WEBHOOK_NOTE_DRAFT   (note下書き)
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .discord_notifier import (
    DiscordNotifier,
    _COLOR_JACKPOT,
    _format_race_label,
)

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)

logger = logging.getLogger(__name__)

_EV_ALERT_THRESHOLD = 1.5
_CHUNK_MAX = 1800  # 2000 制限からヘッダー/ページング分を引いたマージン


# ── テキストチャンク分割 ────────────────────────────────────────────────────────

def _chunk_text(text: str, max_len: int = _CHUNK_MAX) -> list[str]:
    """テキストを max_len 文字以内のチャンクに分割する。

    分割優先順位: 段落区切り(\\n\\n) → 行区切り(\\n) → ハードカット
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break
        idx = remaining.rfind("\n\n", 0, max_len)
        if idx > 0:
            chunks.append(remaining[:idx])
            remaining = remaining[idx + 2:]
            continue
        idx = remaining.rfind("\n", 0, max_len)
        if idx > 0:
            chunks.append(remaining[:idx])
            remaining = remaining[idx + 1:]
            continue
        chunks.append(remaining[:max_len])
        remaining = remaining[max_len:]

    return [c for c in chunks if c.strip()]


# ── X 告知ポスト生成 ────────────────────────────────────────────────────────────

def _generate_x_post(title: str, body: str) -> str:
    """note記事タイトル・本文から X 告知ポストを生成する（140文字以内）。"""
    short_title = title[:40]
    match = re.search(r"^##\s+(.+)$", body, re.MULTILINE)
    subtitle = match.group(1)[:40] if match else ""

    tags = "#競馬 #AI予想 #UMALOGI #JRA"
    lines = [f"🏇 {short_title}"]
    if subtitle:
        lines.append(subtitle)
    lines.append("")
    lines.append("noteで全モデル成績公開中📊")
    lines.append("")
    lines.append(tags)

    post = "\n".join(lines)
    if len(post) > 140:
        available = 140 - len(f"\n\nnoteで全モデル成績公開中📊\n\n{tags}") - 4
        post = f"🏇 {title[:available]}...\n\nnoteで全モデル成績公開中📊\n\n{tags}"
    return post


# ── NotificationRouter ─────────────────────────────────────────────────────────

class NotificationRouter:
    """マルチチャンネル Discord 通知ルーター。

    使用方法:
        router = NotificationRouter()
        router.notify_prerace_result(race_id, honmei_bets, manji_bets, alpha_bets=...)
        router.send_system_text("起動完了")
        router.send_note_draft(title, body)
    """

    def __init__(self, *, enabled: bool = True) -> None:
        self._enabled = enabled

        pred_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
        sys_url = (
            os.environ.get("DISCORD_WEBHOOK_SYSTEM")
            or os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL")
            or ""
        )
        self._notifier = DiscordNotifier(
            webhook_url=pred_url,
            system_url=sys_url,
            enabled=enabled,
        )

        self._ev_alert_url: str = os.environ.get("DISCORD_WEBHOOK_EV_ALERT", "")
        self._ab_test_url: str = os.environ.get("DISCORD_WEBHOOK_AB_TEST", "")
        self._note_draft_url: str = os.environ.get("DISCORD_WEBHOOK_NOTE_DRAFT", "")

    # ── 内部ヘルパー ───────────────────────────────────────────────────────────

    def _post_to(self, url: str, payload: dict[str, Any]) -> bool:
        """指定 URL に JSON を POST する。失敗しても例外を出さない。"""
        if not url or not self._enabled:
            return False
        try:
            import requests
            resp = requests.post(url, json=payload, timeout=10)
            return resp.status_code in (200, 204)
        except Exception as exc:
            logger.warning("[Router] POST 失敗: %s", exc)
            return False

    # ── prediction チャンネル ──────────────────────────────────────────────────

    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: object,
        manji_bets: object,
        **kwargs: Any,
    ) -> None:
        """直前予想を prediction チャンネルへ送信し、EV>=1.5 なら ev_alert にも追加送信。"""
        self._notifier.notify_prerace_result(race_id, honmei_bets, manji_bets, **kwargs)

        if not self._ev_alert_url:
            return

        all_bets: list[object] = []
        for rb in [
            kwargs.get("alpha_bets"),
            manji_bets,
            honmei_bets,
            kwargs.get("oracle_bets"),
            kwargs.get("hit_focus_bets"),
        ]:
            if rb is not None:
                all_bets.extend(getattr(rb, "bets", []))

        max_ev = max(
            (getattr(b, "expected_value", 0.0) for b in all_bets), default=0.0
        )
        if max_ev >= _EV_ALERT_THRESHOLD:
            self.notify_ev_alert(race_id, max_ev, f"最高EV: {max_ev:.2f}")

    def notify_hit_summary(self, *args: Any, **kwargs: Any) -> None:
        self._notifier.notify_hit_summary(*args, **kwargs)

    def notify_skip(self, race_id: str, reason: str) -> None:
        self._notifier.notify_skip(race_id, reason)

    def send_text(self, text: str) -> None:
        self._notifier.send_text(text)

    # ── system チャンネル ──────────────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        self._notifier.send_system_text(text)

    def send_system_embed(
        self, title: str, description: str, **kwargs: Any
    ) -> None:
        self._notifier.send_system_embed(title, description, **kwargs)

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        self._notifier.notify_scraping_alert(race_id, detail)

    def notify_intervention_required(
        self,
        step: str,
        error: str,
        action: str,
        screenshot_path: Path | None = None,
    ) -> None:
        self._notifier.notify_intervention_required(
            step, error, action, screenshot_path
        )

    def notify_ror_warning(self, warning_text: str) -> None:
        self._notifier.notify_ror_warning(warning_text)

    # ── ev_alert チャンネル ────────────────────────────────────────────────────

    def notify_ev_alert(
        self, race_id: str, max_ev: float, bets_summary: str
    ) -> None:
        """EV >= 1.5 の激熱レースを @everyone 付きで ev_alert チャンネルへ送信する。"""
        if not self._ev_alert_url:
            return
        label = _format_race_label(race_id)
        payload: dict[str, Any] = {
            "content": "@everyone",
            "embeds": [
                {
                    "title": f"🔥 EV激熱アラート — {label}",
                    "description": f"**{bets_summary}**",
                    "color": _COLOR_JACKPOT,
                }
            ],
        }
        self._post_to(self._ev_alert_url, payload)

    # ── ab_test チャンネル ─────────────────────────────────────────────────────

    def send_ab_report(self, report_md: str) -> None:
        """V1/V2 週次 A/B 成績レポートを ab_test チャンネルへ送信する。"""
        if not self._ab_test_url:
            logger.info("DISCORD_WEBHOOK_AB_TEST 未設定 — A/B レポートをスキップ")
            return
        self._post_to(self._ab_test_url, {"content": report_md[:2000]})

    # ── note_draft チャンネル ──────────────────────────────────────────────────

    def send_note_draft(
        self, title: str, body: str, x_post: str | None = None
    ) -> bool:
        """note下書きをチャンク分割して note_draft チャンネルへ順番送信する。

        x_post が None の場合は _generate_x_post() で自動生成する。
        Returns True if 送信成功, False if note_draft URL 未設定。
        """
        if not self._note_draft_url:
            logger.warning(
                "DISCORD_WEBHOOK_NOTE_DRAFT 未設定 — Discord 転送をスキップ"
            )
            return False

        chunks = _chunk_text(body)
        total = len(chunks)

        for i, chunk in enumerate(chunks, 1):
            suffix = "\n_（以上）_" if i == total else ""
            content = (
                f"【note下書き ({i}/{total})】\n"
                f"```markdown\n{chunk}{suffix}\n```"
            )
            self._post_to(self._note_draft_url, {"content": content})

        post_text = x_post if x_post is not None else _generate_x_post(title, body)
        self._post_to(
            self._note_draft_url,
            {"content": f"```markdown\n{post_text}\n```"},
        )

        logger.info(
            "[Discord] note-draft 送信完了: %dチャンク + X告知1件 → DISCORD_WEBHOOK_NOTE_DRAFT",
            total,
        )
        return True

    # ── prediction embed (scheduler 互換) ─────────────────────────────────────

    def send_prediction_embed(self, embeds: list[dict[str, Any]]) -> None:
        """予想チャンネルに生 Embed リストを送信する（scheduler の暫定予想サマリー用）。"""
        self._notifier.send_prediction_embed(embeds)
```

- [ ] **Step 2-2: テストを実行して PASS することを確認する**

```
pytest tests/notification/test_router.py -v
```

期待出力: `6 passed` (TestNotificationRouter 5件 + TestPlaywrightToggle はまだ FAIL)

- [ ] **Step 2-3: コミット**

```bash
git add src/notification/router.py
git commit -m "feat: NotificationRouter 新設 — マルチチャンネルルーター層"
```

---

## Task 3: `DiscordNotifier` に `send_prediction_embed` を追加し `__init__.py` を更新する

**Files:**
- Modify: `src/notification/discord_notifier.py`
- Modify: `src/notification/__init__.py`

- [ ] **Step 3-1: `DiscordNotifier` に `send_prediction_embed` を追加する**

`src/notification/discord_notifier.py` の `notify_hit_summary` メソッドの直前（line 266 付近）に以下を挿入する:

```python
    def send_prediction_embed(self, embeds: list[dict[str, Any]]) -> None:
        """予想チャンネルに生 Embed リストを送信する（スケジューラーの暫定予想サマリー用）。"""
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL 未設定 — prediction embed 送信スキップ")
            return
        self._post(self._url, {"embeds": embeds})
```

- [ ] **Step 3-2: `__init__.py` に `NotificationRouter` を追加する**

```python
# src/notification/__init__.py
"""Notification package — Discord / LINE / X への自動投稿。"""
from .dispatcher import NotificationDispatcher, NotifyLevel
from .router import NotificationRouter

__all__ = ["NotificationDispatcher", "NotifyLevel", "NotificationRouter"]
```

- [ ] **Step 3-3: テストが引き続き PASS することを確認する**

```
pytest tests/notification/test_router.py -v
```

- [ ] **Step 3-4: コミット**

```bash
git add src/notification/discord_notifier.py src/notification/__init__.py
git commit -m "feat: DiscordNotifier に send_prediction_embed 追加 + __init__ 更新"
```

---

## Task 4: `post_weekly_note_draft.py` を改修する

**Files:**
- Modify: `scripts/post_weekly_note_draft.py`

- [ ] **Step 4-1: `_should_publish_playwright` と Discord 転送ロジックを追加する**

現在の `post_weekly_note_draft.py` の `import` 直後（`_DB_PATH` 定数の前）に以下を追加し、`main()` を改修する:

```python
# ── 追加 import (ファイル先頭の import セクションへ) ──
import os
from src.notification.router import NotificationRouter


# ── 新規ヘルパー関数 (_DB_PATH 定数の直前に追加) ───────────────────────────────

def _should_publish_playwright() -> bool:
    """ENABLE_PLAYWRIGHT_POST 環境変数が True/1 のときのみ True を返す。"""
    val = os.environ.get("ENABLE_PLAYWRIGHT_POST", "").strip().lower()
    return val in ("1", "true")
```

次に `main()` 関数を以下のように改修する（Step 2 と Step 3 の間に Step 3-A を挿入し、Step 3-B を条件分岐に変える）:

```python
def main() -> None:
    p = argparse.ArgumentParser(description="週次まとめ記事を note.com に下書き投稿する")
    p.add_argument("--week-offset", type=int, default=1,
                   help="何週前を振り返るか（デフォルト1=先週）")
    p.add_argument("--login-only", action="store_true",
                   help="ログインのみ（下書き投稿はしない）")
    p.add_argument("--no-headless", action="store_true",
                   help="ブラウザ画面を表示する（デバッグ用）")
    args = p.parse_args()

    headless = not args.no_headless

    from src.ops.note_draft_publisher import login_and_save_session, save_draft

    # ── Step 1: セッション確認 / ログイン ──────────────────────────
    if not _SESSION_FILE.exists():
        print()
        print("=" * 60)
        print("  note.com セッションが見つかりません。")
        print("  ブラウザが開くので、ログインしてください。")
        print("  reCAPTCHA が出た場合は手動で解決してください。")
        print("  ログイン完了後、自動で下書き投稿に進みます。")
        print("=" * 60)
        print()
        ok = login_and_save_session()
        if not ok:
            logger.error("ログイン失敗。スクリプトを終了します。")
            sys.exit(1)
        logger.info("ログイン完了。セッション保存: %s", _SESSION_FILE)
    else:
        logger.info("既存セッションを使用: %s", _SESSION_FILE)

    if args.login_only:
        print("\n✅ ログイン完了（--login-only のため下書き投稿はスキップ）")
        return

    # ── Step 2: 記事生成 ────────────────────────────────────────────
    logger.info("週次記事を生成中 (week_offset=%d)...", args.week_offset)
    title, body = _generate_article(args.week_offset)
    logger.info("記事生成完了: %d 文字", len(body))
    print(f"\n  タイトル: {title}")
    print(f"  本文文字数: {len(body):,} 文字\n")

    # ── Step 3-A: Discord note_draft チャンネルへ転送 ───────────────
    router = NotificationRouter()
    discord_ok = router.send_note_draft(title, body)
    if discord_ok:
        logger.info("Discord note-draft 転送完了")
    else:
        logger.info("Discord note-draft 転送スキップ（DISCORD_WEBHOOK_NOTE_DRAFT 未設定）")

    # ── Step 3-B: note.com に下書き保存（ENABLE_PLAYWRIGHT_POST=True 時のみ） ──
    if not _should_publish_playwright():
        logger.info(
            "Playwright 投稿: スキップ (ENABLE_PLAYWRIGHT_POST=%s)",
            os.environ.get("ENABLE_PLAYWRIGHT_POST", "未設定"),
        )
        print()
        print("=" * 60)
        print("  ✅ Discord 転送完了 (Playwright 投稿はスキップ)")
        print("  ENABLE_PLAYWRIGHT_POST=1 にすると note.com にも投稿します。")
        print("=" * 60)
        return

    logger.info("note.com に下書き保存中 (headless=%s)...", headless)
    ok = save_draft(
        title=title,
        body=body,
        tags=["競馬", "AI予想", "UMALOGI", "JRA", "期待値", "競馬AI"],
        headless=headless,
    )

    print()
    print("=" * 60)
    if ok:
        print("  ✅ note.com 下書き保存 完了!")
        print(f"  タイトル: {title}")
        print("  note.com の下書き一覧を確認してください。")
    else:
        print("  ❌ note.com 下書き保存 失敗")
        print("  outputs/debug/ のスクリーンショットを確認してください。")
        print("  セッション期限切れの場合: --login-only で再ログインしてください。")
    print("=" * 60)
    print()

    if not ok:
        sys.exit(1)
```

- [ ] **Step 4-2: Playwright トグルテストが PASS することを確認する**

```
pytest tests/notification/test_router.py::TestPlaywrightToggle -v
```

期待出力: `5 passed`

- [ ] **Step 4-3: コミット**

```bash
git add scripts/post_weekly_note_draft.py
git commit -m "feat: post_weekly_note_draft に Discord 転送 + ENABLE_PLAYWRIGHT_POST トグル追加"
```

---

## Task 5: `src/pipeline/prediction.py` を `NotificationRouter` に移行する

**Files:**
- Modify: `src/pipeline/prediction.py:28-35`

- [ ] **Step 5-1: import と初期化を差し替える**

変更前:
```python
from src.notification.discord_notifier import DiscordNotifier
...
_discord = DiscordNotifier()
```

変更後:
```python
from src.notification.router import NotificationRouter
...
_discord = NotificationRouter()
```

`prediction.py` の import セクション（line 28）と初期化（line 35）を以下の手順で編集する:

line 28 を:
```python
from src.notification.router import NotificationRouter
```

line 35 を:
```python
_discord = NotificationRouter()
```

- [ ] **Step 5-2: 動作確認（import エラーがないこと）**

```
python -c "from src.pipeline.prediction import prerace_pipeline; print('OK')"
```

期待出力: `OK`

- [ ] **Step 5-3: コミット**

```bash
git add src/pipeline/prediction.py
git commit -m "refactor: prediction.py を NotificationRouter に移行"
```

---

## Task 6: `scripts/today_auto_runner.py` を `NotificationRouter` に移行する

**Files:**
- Modify: `scripts/today_auto_runner.py`

- [ ] **Step 6-1: import を追加し、ローカル関数を削除して module-level router を追加する**

ファイル先頭の import セクション（`from dotenv import load_dotenv` の直後）に追加:
```python
from src.notification.router import NotificationRouter
```

`load_dotenv(...)` の直後（現在 line 52 付近）に追加:
```python
_router = NotificationRouter()
```

次に **line 54-87 の `_send_discord` と `_send_discord_race` 関数定義を完全削除する**。

- [ ] **Step 6-2: `_send_discord(...)` の呼び出しをすべて `_router.send_system_text(...)` に差し替える**

以下の箇所を確認してすべて置換する（`grep -n "_send_discord(" scripts/today_auto_runner.py` で箇所を列挙してから編集）:

```python
# 変更前
_send_discord("テキスト")

# 変更後
_router.send_system_text("テキスト")
```

- [ ] **Step 6-3: `_send_discord_race(...)` の呼び出しをすべて `_router.send_text(...)` に差し替える**

```python
# 変更前
_send_discord_race("テキスト")

# 変更後
_router.send_text("テキスト")
```

- [ ] **Step 6-4: 動作確認（import エラーがないこと）**

```
python -c "import scripts.today_auto_runner; print('OK')"
```

期待出力: `OK`

- [ ] **Step 6-5: コミット**

```bash
git add scripts/today_auto_runner.py
git commit -m "refactor: today_auto_runner.py を NotificationRouter に移行"
```

---

## Task 7: `scripts/scheduler.py` を `NotificationRouter` に移行する

**Files:**
- Modify: `scripts/scheduler.py`

- [ ] **Step 7-1: import を追加し、module-level router を追加する**

ファイル先頭の import セクション（`from dotenv import load_dotenv` 付近）に追加:
```python
from src.notification.router import NotificationRouter
```

`load_dotenv(...)` の直後（line 62 付近）に追加:
```python
_router = NotificationRouter()
```

- [ ] **Step 7-2: ローカルの `_send_discord` / `_send_discord_embed` を削除して差し替える**

line 476-518 の `_send_discord` と `_send_discord_embed` 関数定義を削除する。

```python
# 削除対象（line 476-518）
def _send_discord(text: str) -> None:
    ...

def _send_discord_embed(embeds: list[dict]) -> None:
    ...
```

呼び出し箇所を以下のように差し替える:
```python
# 変更前
_send_discord("テキスト")

# 変更後
_router.send_system_text("テキスト")
```

```python
# 変更前
_send_discord_embed([embed])

# 変更後
_router.send_prediction_embed([embed])
```

- [ ] **Step 7-3: `_notify_provisional_summary` 内の `_send_discord_embed` 呼び出しを確認・差し替える**

`scripts/scheduler.py` の `_notify_provisional_summary` 関数（line 521 付近）:
```python
# 変更前
_send_discord_embed([embed])

# 変更後
_router.send_prediction_embed([embed])
```

- [ ] **Step 7-4: 動作確認**

```
python -c "import scripts.scheduler; print('OK')"
```

期待出力: `OK`

- [ ] **Step 7-5: コミット**

```bash
git add scripts/scheduler.py
git commit -m "refactor: scheduler.py を NotificationRouter に移行"
```

---

## Task 8: `2.env` を更新する

**Files:**
- Modify: `2.env`

- [ ] **Step 8-1: 新規環境変数を追記する**

`2.env` の内容を以下に完全置換する:

```
# ── Discord Webhook URLs ─────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL=              # 予想・結果・週次レポート（必須・フォールバック基準）
DISCORD_WEBHOOK_SYSTEM=           # システムログ・エラー（旧: DISCORD_SYSTEM_WEBHOOK_URL）
DISCORD_WEBHOOK_EV_ALERT=         # EV>=1.5 激熱レース専用（未設定時は prediction へ fallback なし・スキップ）
DISCORD_WEBHOOK_AB_TEST=          # V1/V2 週次A/Bテスト比較レポート（未設定時はスキップ）
DISCORD_WEBHOOK_NOTE_DRAFT=       # note下書き出力（未設定時はスキップ）

# ── 旧変数（後方互換のため継続サポート） ──────────────────────────────────────
# DISCORD_SYSTEM_WEBHOOK_URL=     # → DISCORD_WEBHOOK_SYSTEM として読み込み

# ── SNS 通知設定 ─────────────────────────────────────────────────────────────
LINE_NOTIFY_TOKEN=
X_API_KEY=
X_API_SECRET=
X_ACCESS_TOKEN=
X_ACCESS_TOKEN_SECRET=

# ── 通知有効フラグ ────────────────────────────────────────────────────────────
NOTIFY_DISCORD=1
NOTIFY_LINE=1
NOTIFY_TWITTER=0

# ── note下書き投稿モード ──────────────────────────────────────────────────────
ENABLE_PLAYWRIGHT_POST=           # True/1 にすると Playwright 自動投稿も実行（デフォルト OFF）
```

- [ ] **Step 8-2: コミット**

```bash
git add 2.env
git commit -m "chore: 2.env に Discord マルチチャンネル環境変数を追加"
```

---

## Task 9: 全テスト実行 & ドキュメント更新

- [ ] **Step 9-1: 全テストを実行する**

```
pytest tests/notification/ -v
```

期待出力: `10 passed` (TestNotificationRouter 5件 + TestPlaywrightToggle 5件)

- [ ] **Step 9-2: `docs/1_prediction_logic.md` の Changelog に記録する**

```markdown
| 2026-05-20 | NotificationRouter 新設・DiscordNotifier をルーター層でラップ。prediction.py/today_auto_runner.py/scheduler.py を移行。影響ファイル: src/notification/router.py, src/pipeline/prediction.py |
```

- [ ] **Step 9-3: `docs/2_automation_schedule.md` の Changelog に記録する**

```markdown
| 2026-05-20 | post_weekly_note_draft.py に Discord 転送(Step 3-A)と ENABLE_PLAYWRIGHT_POST トグルを追加。影響ファイル: scripts/post_weekly_note_draft.py |
```

- [ ] **Step 9-4: `docs/7_weakness_ledger.md` の W-021（Discord通知拡張）を完了に更新する**

W-021 のステータスを `🟡対応中` → `🟢完了` に更新し、以下を記録する:
```
完了日: 2026-05-20 / NotificationRouter 実装・全呼び出し元移行完了
```

- [ ] **Step 9-5: 最終コミット**

```bash
git add docs/1_prediction_logic.md docs/2_automation_schedule.md docs/7_weakness_ledger.md
git commit -m "docs: 通知ルーター実装完了 — Changelog & 弱点台帳更新"
```

---

## Self-Review チェック

### Spec カバレッジ確認

| Spec 要件 | 対応タスク |
|----------|----------|
| NotificationRouter 新設 (`src/notification/router.py`) | Task 2 |
| チャンネルマップ (prediction/system/ev_alert/ab_test/note_draft) | Task 2 Step 2-1 |
| DISCORD_SYSTEM_WEBHOOK_URL 後方互換 | Task 2 Step 2-1 (`_notifier` コンストラクタ) |
| フォールバック規則（system → prediction） | Task 2 Step 2-1 (DiscordNotifier 内部の `_sys_url()`) |
| ev_alert フォールバックなし（スキップ） | Task 2 Step 2-1 (`_ev_alert_url` 未設定時 `return`) |
| EV >= 1.5 で ev_alert 追加送信 | Task 2 Step 2-1 (`notify_prerace_result`) |
| @everyone 付き EV激熱アラート | Task 2 Step 2-1 (`notify_ev_alert`) |
| send_ab_report | Task 2 Step 2-1 |
| send_note_draft チャンク分割 | Task 2 Step 2-1 (`_chunk_text`) |
| ページング表記 `【note下書き (N/M)】` | Task 2 Step 2-1 |
| 最終チャンク `_（以上）_` | Task 2 Step 2-1 |
| X 告知ポスト自動生成 | Task 2 Step 2-1 (`_generate_x_post`) |
| ENABLE_PLAYWRIGHT_POST トグル | Task 4 |
| prediction.py 移行 | Task 5 |
| today_auto_runner.py 移行 | Task 6 |
| scheduler.py 移行 | Task 7 |
| 2.env 環境変数テンプレート | Task 8 |
| テスト 6件 | Task 1 |
| DiscordNotifier 既存メソッド変更なし | Task 3（send_prediction_embed 追加のみ） |
| web/ ファイルへの変更ゼロ | 全タスク |
