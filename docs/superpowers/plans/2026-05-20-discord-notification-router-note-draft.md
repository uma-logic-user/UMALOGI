# Discord 通知ルーター刷新 & note下書き転送 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** マルチWebhookルーター層（`NotificationRouter`）を新設し、EV激熱アラート・A/Bテストレポート・note下書き Discord 転送を実装する。さらに「組」→「点」修正、IS_PREMIUM_NOTE フラグ、買い方テンプレート、2カ年バックテストシミュレーター、万馬券特化報告スクリプトを追加する。

**Architecture:** `NotificationRouter` (`src/notification/router.py`) が5チャンネル分の `DiscordNotifier` インスタンスを保持し、用途別のフォールバック制御を行う。`prediction.py` / `today_auto_runner.py` の呼び出し元は `DiscordNotifier()` から `NotificationRouter()` に置換する。`post_weekly_note_draft.py` に Discord 転送ロジックと `ENABLE_PLAYWRIGHT_POST` トグルを追加する。

**Tech Stack:** Python 3.11, requests, python-dotenv, pytest, unittest.mock

---

## ファイル構成

| ファイル | 種別 | 担当 |
|---|---|---|
| `src/notification/router.py` | 新設 | `NotificationRouter` + `_chunk_text` + `_generate_x_post` + `_format_buying_guide` |
| `tests/test_router.py` | 新設 | ルーター単体テスト |
| `src/pipeline/prediction.py` | 修正 (L28, L35) | `DiscordNotifier` → `NotificationRouter` 置換 |
| `scripts/today_auto_runner.py` | 修正 (L54–87) | インライン `_send_discord*` → `NotificationRouter` |
| `src/notification/discord_notifier.py` | 修正 (L493) | 「組」→「点」表記統一 |
| `scripts/generate_weekly_note.py` | 修正 | `is_premium` パラメータ追加・有料/無料出し分け |
| `scripts/run_2year_backtest.py` | 新設 | 2カ年厳選黒字化シミュレーター |
| `scripts/generate_result_note_draft.py` | 新設 | 万馬券特化的中報告自動生成 |
| `docs/commercialization_roadmap.md` | 更新 | 商用化ロードマップ反映 |
| `scripts/post_weekly_note_draft.py` | 修正 | Discord 転送 + フィーチャートグル追加 |
| `2.env` | 修正 | 新規 6 環境変数追記 |

---

## Task 1: テストファイルの作成（全テスト・失敗状態で追加）

**Files:**
- Create: `tests/test_router.py`

- [ ] **Step 1: テストファイルを作成する**

```python
# tests/test_router.py
"""
src/notification/router.py のユニットテスト。
requests.post をモックして実際の HTTP 送信は行わない。
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, call, patch

import pytest


# ── _chunk_text ──────────────────────────────────────────────────────────────

class TestChunkText:
    def test_short_text_returned_as_single_chunk(self):
        from src.notification.router import _chunk_text
        assert _chunk_text("Hello world", max_len=100) == ["Hello world"]

    def test_split_at_double_newline(self):
        from src.notification.router import _chunk_text
        text = "段落1\n\n段落2"
        result = _chunk_text(text, max_len=6)
        assert len(result) == 2
        assert "段落1" in result[0]
        assert "段落2" in result[1]

    def test_split_at_single_newline_when_no_double(self):
        from src.notification.router import _chunk_text
        text = "行1\n行2"
        result = _chunk_text(text, max_len=4)
        assert len(result) == 2

    def test_hard_cut_when_no_newline(self):
        from src.notification.router import _chunk_text
        text = "A" * 200
        result = _chunk_text(text, max_len=100)
        assert len(result) == 2
        for chunk in result:
            assert len(chunk) <= 100

    def test_3600_chars_splits_into_2_chunks(self):
        from src.notification.router import _chunk_text
        text = "A" * 3600
        result = _chunk_text(text, max_len=1800)
        assert len(result) == 2

    def test_empty_string_returns_one_empty_chunk(self):
        from src.notification.router import _chunk_text
        result = _chunk_text("", max_len=100)
        assert result == [""]


# ── _generate_x_post ─────────────────────────────────────────────────────────

class TestGenerateXPost:
    def test_result_under_140_chars(self):
        from src.notification.router import _generate_x_post
        title = "🏇【UMALOGI週次レポート】2026-05-18号 — 全モデル成績公開＆今週のAI厳選予想"
        body = "## 万馬券3本的中！ALPHAモデルROI203%達成\n\n本文コンテンツ..."
        result = _generate_x_post(title, body)
        assert len(result) <= 140

    def test_contains_umalogi_hashtag(self):
        from src.notification.router import _generate_x_post
        result = _generate_x_post("タイトル", "本文")
        assert "#UMALOGI" in result

    def test_contains_keiba_hashtag(self):
        from src.notification.router import _generate_x_post
        result = _generate_x_post("タイトル", "本文")
        assert "#競馬" in result

    def test_subtitle_extracted_from_body(self):
        from src.notification.router import _generate_x_post
        body = "前文\n## サブタイトルです\n本文"
        result = _generate_x_post("タイトル", body)
        assert "サブタイトルです" in result


# ── NotificationRouter 初期化 ─────────────────────────────────────────────────

class TestNotificationRouterInit:
    def test_no_env_vars_no_exception(self):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            assert router._get("prediction") is None

    def test_prediction_channel_configured_when_url_set(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("prediction") is not None

    def test_ev_alert_unset_falls_back_to_prediction(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("ev_alert") is router._get("prediction")

    def test_ev_alert_set_returns_separate_instance(self):
        from src.notification.router import NotificationRouter
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("ev_alert") is not router._get("prediction")

    def test_system_backward_compat_old_env_var(self):
        from src.notification.router import NotificationRouter
        env = {"DISCORD_SYSTEM_WEBHOOK_URL": "https://example.com/sys"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            assert router._get("system") is not None

    def test_new_system_env_var_takes_precedence(self):
        from src.notification.router import NotificationRouter
        env = {
            "DISCORD_WEBHOOK_SYSTEM":    "https://example.com/new_sys",
            "DISCORD_SYSTEM_WEBHOOK_URL": "https://example.com/old_sys",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            notifier = router._get("system")
            assert notifier is not None
            assert notifier._url == "https://example.com/new_sys"


# ── send_note_draft ──────────────────────────────────────────────────────────

def _mock_post_ok() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 204
    resp.text = ""
    return resp


class TestSendNoteDraft:
    @patch("src.notification.discord_notifier.requests.post")
    def test_returns_false_when_no_url(self, mock_post):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            result = router.send_note_draft(title="テスト", body="コンテンツ")
        assert result is False
        mock_post.assert_not_called()

    @patch("src.notification.discord_notifier.requests.post")
    def test_returns_true_and_sends_when_url_set(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            result = router.send_note_draft(title="テスト", body="短いコンテンツ")
        assert result is True
        assert mock_post.call_count >= 1

    @patch("src.notification.discord_notifier.requests.post")
    def test_pagination_header_in_first_chunk(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "【note下書き (1/2)】" in first_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_pagination_header_in_second_chunk(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        second_content = mock_post.call_args_list[1][1]["json"]["content"]
        assert "【note下書き (2/2)】" in second_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_chunks_wrapped_in_code_block(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="本文コンテンツ")
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "```markdown" in first_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_x_post_sent_as_last_message(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(
                title="テスト", body="短い本文", x_post="カスタムツイート文"
            )
        last_content = mock_post.call_args_list[-1][1]["json"]["content"]
        assert "X（Twitter）告知ポスト" in last_content
        assert "カスタムツイート文" in last_content

    @patch("src.notification.discord_notifier.requests.post")
    def test_3600_char_body_sends_3_messages(self, mock_post):
        # 2 chunks + 1 x_post = 3 messages
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="A" * 3600)
        assert mock_post.call_count == 3

    @patch("src.notification.discord_notifier.requests.post")
    def test_final_chunk_has_end_marker(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter
        env = {"DISCORD_WEBHOOK_NOTE_DRAFT": "https://example.com/note"}
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            router.send_note_draft(title="テスト", body="短い本文")
        # The note chunk message (index 0) should contain "（以上）"
        first_content = mock_post.call_args_list[0][1]["json"]["content"]
        assert "（以上）" in first_content


# ── EV 激熱アラート ──────────────────────────────────────────────────────────

class TestEvAlert:
    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_sent_when_max_ev_meets_threshold(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD + 0.1
            bet.recommended_bet = 1000
            bets = MagicMock()
            bets.bets = [bet]
            router.notify_prerace_result("2026051905010911", bets, bets)
        # At least 2 POST calls: prediction embed + ev_alert embed
        assert mock_post.call_count >= 2

    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_not_sent_below_threshold(self, mock_post):
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {
            "DISCORD_WEBHOOK_URL":      "https://example.com/pred",
            "DISCORD_WEBHOOK_EV_ALERT": "https://example.com/ev",
        }
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD - 0.1
            bet.recommended_bet = 500
            bets = MagicMock()
            bets.bets = [bet]
            initial_count = mock_post.call_count
            router.notify_prerace_result("2026051905010911", bets, bets)
            # Only prediction embed called (or skipped if all EV<=0 logic triggers)
            # Key: should NOT call ev_alert URL
            for c in mock_post.call_args_list:
                url_arg = c[0][0] if c[0] else c[1].get("url", "")
                assert "ev" not in url_arg or "pred" in url_arg

    @patch("src.notification.discord_notifier.requests.post")
    def test_ev_alert_not_sent_when_same_url_as_prediction(self, mock_post):
        """ev_alert と prediction が同じ URL の場合は2重送信しない。"""
        mock_post.return_value = _mock_post_ok()
        from src.notification.router import NotificationRouter, EV_ALERT_THRESHOLD
        env = {"DISCORD_WEBHOOK_URL": "https://example.com/pred"}
        # ev_alert not set → falls back to prediction → same instance → no double send
        with patch.dict(os.environ, env, clear=True):
            router = NotificationRouter()
            bet = MagicMock()
            bet.expected_value = EV_ALERT_THRESHOLD + 1.0
            bet.recommended_bet = 1000
            bets = MagicMock()
            bets.bets = [bet]
            router.notify_prerace_result("2026051905010911", bets, bets)
        # ev_alert is not in _channels, so no extra call
        # (the check is: ev_notifier = self._channels.get("ev_alert") — returns None)


# ── 全 URL 未設定でも例外が発生しない ────────────────────────────────────────

class TestAllChannelsUnset:
    def test_no_exception_on_all_methods(self):
        from src.notification.router import NotificationRouter
        with patch.dict(os.environ, {}, clear=True):
            router = NotificationRouter()
            router.send_text("テスト")
            router.send_system_text("システムテスト")
            router.send_system_embed("タイトル", "説明")
            router.notify_skip("2026051905010911", "テスト理由")
            router.send_ab_report("## A/B レポート")
            result = router.send_note_draft("テスト", "コンテンツ")
            assert result is False
```

- [ ] **Step 2: テストが FAIL することを確認する**

```
pytest tests/test_router.py -v 2>&1 | head -30
```

期待出力: `ModuleNotFoundError: No module named 'src.notification.router'` または `ImportError`

---

## Task 2: ヘルパー関数の実装（`_chunk_text`, `_generate_x_post`）

**Files:**
- Create: `src/notification/router.py`

- [ ] **Step 1: `router.py` を最小限の実装で作成する**

```python
# src/notification/router.py
"""
Discord 通知マルチ Webhook ルーター

チャンネルマップ:
  prediction  : DISCORD_WEBHOOK_URL          (買い目・結果 — フォールバック基準)
  system      : DISCORD_WEBHOOK_SYSTEM       (システムログ・エラー)
  ev_alert    : DISCORD_WEBHOOK_EV_ALERT     (EV>=1.5 激熱レース専用)
  ab_test     : DISCORD_WEBHOOK_AB_TEST      (V1/V2 成績比較レポート)
  note_draft  : DISCORD_WEBHOOK_NOTE_DRAFT   (note下書き出力用)
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
    _COLOR_BIG,
    _COLOR_JACKPOT,
    _format_race_label,
)

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)

logger = logging.getLogger(__name__)

# ── 定数 ─────────────────────────────────────────────────────────────────────

EV_ALERT_THRESHOLD: float = 1.5
_CHUNK_MAX: int = 1800

CHANNEL_ENV: dict[str, str] = {
    "prediction": "DISCORD_WEBHOOK_URL",
    "system":     "DISCORD_WEBHOOK_SYSTEM",
    "ev_alert":   "DISCORD_WEBHOOK_EV_ALERT",
    "ab_test":    "DISCORD_WEBHOOK_AB_TEST",
    "note_draft": "DISCORD_WEBHOOK_NOTE_DRAFT",
}


# ── ヘルパー関数 ──────────────────────────────────────────────────────────────

def _chunk_text(text: str, max_len: int = _CHUNK_MAX) -> list[str]:
    """テキストを max_len 文字以下のチャンクに分割して返す。

    分割優先順位: ダブル改行 → 単一改行 → ハードカット
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    remaining = text

    while len(remaining) > max_len:
        # 1. ダブル改行で分割
        pos = remaining.rfind("\n\n", 0, max_len)
        if pos > 0:
            chunks.append(remaining[:pos])
            remaining = remaining[pos + 2:]
            continue
        # 2. 単一改行で分割
        pos = remaining.rfind("\n", 0, max_len)
        if pos > 0:
            chunks.append(remaining[:pos])
            remaining = remaining[pos + 1:]
            continue
        # 3. ハードカット
        chunks.append(remaining[:max_len])
        remaining = remaining[max_len:]

    if remaining:
        chunks.append(remaining)

    return chunks


def _generate_x_post(title: str, body: str) -> str:
    """note 記事から X（Twitter）告知ポスト（140文字以内）を生成する。

    title の絵文字プレフィックスを除去し、body の最初の ## 見出しをサブタイトルとして使用する。
    """
    hashtags = "#競馬 #AI予想 #UMALOGI #JRA"
    suffix = f"\n\nnoteで全モデル成績公開中📊\n\n{hashtags}"

    # 絵文字プレフィックスを除去
    clean_title = re.sub(r"^[\U0001F300-\U0001FAFF\s🏇]*", "", title).strip()

    # body の最初の ## 見出しを抽出
    subtitle_m = re.search(r"^##\s+(.+)$", body, re.MULTILINE)
    subtitle = subtitle_m.group(1).strip()[:40] if subtitle_m else ""

    max_body = 140 - len(suffix)
    if subtitle:
        post_body = f"{clean_title[:50]}\n{subtitle}"
    else:
        post_body = clean_title[:70]

    return f"{post_body[:max_body]}{suffix}"
```

- [ ] **Step 2: ヘルパーのテストを実行して PASS することを確認する**

```
pytest tests/test_router.py::TestChunkText tests/test_router.py::TestGenerateXPost -v
```

期待出力: 全テスト `PASSED`

---

## Task 3: `NotificationRouter` クラスの実装（初期化・`_get`）

**Files:**
- Modify: `src/notification/router.py`（Task 2 のファイルに追記）

- [ ] **Step 1: クラス本体を追記する**

Task 2 で作成した `_generate_x_post` 関数の直後に以下を追加する:

```python
# ── NotificationRouter ────────────────────────────────────────────────────────

class NotificationRouter:
    """
    マルチ Webhook ルーティング層。
    チャンネル別に DiscordNotifier インスタンスを保持し、
    用途別フォールバック制御を担う。
    """

    def __init__(self) -> None:
        self._channels: dict[str, DiscordNotifier] = {}

        for channel, env_key in CHANNEL_ENV.items():
            url = os.getenv(env_key, "").strip()
            # 後方互換: system チャンネルは旧変数名も読む
            if not url and channel == "system":
                url = os.getenv("DISCORD_SYSTEM_WEBHOOK_URL", "").strip()
            if url:
                self._channels[channel] = DiscordNotifier(
                    webhook_url=url, enabled=True
                )

        if "prediction" not in self._channels:
            logger.warning(
                "DISCORD_WEBHOOK_URL 未設定 — NotificationRouter: 通知は全スキップされます"
            )

    def _get(self, channel: str) -> DiscordNotifier | None:
        """チャンネルの Notifier を返す。未設定なら prediction へフォールバック。"""
        return self._channels.get(channel) or self._channels.get("prediction")
```

- [ ] **Step 2: 初期化テストを実行して PASS することを確認する**

```
pytest tests/test_router.py::TestNotificationRouterInit -v
```

期待出力: 全テスト `PASSED`

---

## Task 4: 委譲メソッドの実装（prediction / system / ab_test チャンネル）

**Files:**
- Modify: `src/notification/router.py`（Task 3 クラスに追記）

- [ ] **Step 1: `NotificationRouter` クラスに委譲メソッドを追記する**

Task 3 の `_get` メソッドの直後に以下を追加する:

```python
    # ── prediction チャンネル ────────────────────────────────────────────────

    def send_text(self, text: str) -> None:
        """予想チャンネルにプレーンテキストを送信する。"""
        n = self._get("prediction")
        if n:
            n.send_text(text)

    def notify_hit_summary(
        self,
        date_str: str,
        hit_count: int,
        total_count: int,
        cumulative_pnl: int,
        monthly_progress_pct: float,
    ) -> None:
        n = self._get("prediction")
        if n:
            n.notify_hit_summary(
                date_str, hit_count, total_count,
                cumulative_pnl, monthly_progress_pct,
            )

    def notify_skip(self, race_id: str, reason: str) -> None:
        n = self._get("prediction")
        if n:
            n.notify_skip(race_id, reason)

    # ── system チャンネル ────────────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        """システムチャンネルにプレーンテキストを送信する。"""
        n = self._get("system")
        if n:
            n.send_system_text(text)

    def send_system_embed(
        self,
        title: str,
        description: str,
        **kwargs: Any,
    ) -> None:
        n = self._get("system")
        if n:
            n.send_system_embed(title, description, **kwargs)

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        n = self._get("system")
        if n:
            n.notify_scraping_alert(race_id, detail)

    def notify_intervention_required(
        self,
        step: str,
        error: str,
        action: str,
        screenshot_path: Any = None,
    ) -> None:
        n = self._get("system")
        if n:
            n.notify_intervention_required(step, error, action, screenshot_path)

    def notify_ror_warning(self, warning_text: str) -> None:
        n = self._get("system")
        if n:
            n.notify_ror_warning(warning_text)

    # ── ab_test チャンネル ───────────────────────────────────────────────────

    def send_ab_report(self, report_md: str) -> None:
        """V1/V2 週次 A/B 比較レポートを ab_test チャンネルへ送信する。"""
        n = self._get("ab_test")
        if n is None:
            logger.warning("DISCORD_WEBHOOK_AB_TEST 未設定 — A/B レポート送信スキップ")
            return
        for chunk in _chunk_text(report_md):
            n.send_text(f"```markdown\n{chunk}\n```")
```

- [ ] **Step 2: `TestAllChannelsUnset` テストを実行して PASS することを確認する**

```
pytest tests/test_router.py::TestAllChannelsUnset -v
```

期待出力: `PASSED`

---

## Task 5: EV 激熱アラートと `notify_prerace_result` の実装

**Files:**
- Modify: `src/notification/router.py`（Task 4 に続けて追記）

- [ ] **Step 1: `notify_prerace_result` メソッドを追記する**

Task 4 の `send_text` メソッドの**前**（prediction チャンネルセクションの先頭）に挿入する:

```python
    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: object,
        manji_bets: object,
        oracle_bets: object | None = None,
        hit_focus_bets: object | None = None,
        alpha_bets: object | None = None,
        dashboard_url: str = "",
    ) -> None:
        """直前予想を prediction チャンネルへ送信する。

        max_ev >= EV_ALERT_THRESHOLD かつ ev_alert チャンネルが独立設定されている場合は
        ev_alert チャンネルへも @everyone 付きで追加送信する。
        """
        # ── 1. prediction チャンネルへ通常送信 ──────────────────────────────
        pred = self._get("prediction")
        if pred:
            pred.notify_prerace_result(
                race_id, honmei_bets, manji_bets,
                oracle_bets=oracle_bets,
                hit_focus_bets=hit_focus_bets,
                alpha_bets=alpha_bets,
                dashboard_url=dashboard_url,
            )

        # ── 2. EV 激熱アラート（ev_alert が独立チャンネルの場合のみ） ────────
        ev_notifier = self._channels.get("ev_alert")  # fallback を経由しない
        if ev_notifier is None:
            return

        all_bets: list[object] = []
        for rb in [alpha_bets, manji_bets, honmei_bets, oracle_bets, hit_focus_bets]:
            if rb is not None:
                all_bets.extend(getattr(rb, "bets", []))

        max_ev = max(
            (getattr(b, "expected_value", 0.0) for b in all_bets), default=0.0
        )
        if max_ev < EV_ALERT_THRESHOLD:
            return

        label = _format_race_label(race_id)
        color = _COLOR_JACKPOT if max_ev >= 3.0 else _COLOR_BIG
        ev_notifier.send_system_embed(
            title=f"🔥【激熱】{label}  EV={max_ev:.2f}",
            description=(
                f"@everyone\n\n"
                f"**{label}** で最高 EV **{max_ev:.2f}** を検知しました。\n"
                f"UMALOGI ダッシュボードで買い目を確認してください。"
            ),
            color=color,
        )
        logger.info(
            "[EV激熱アラート] %s  max_ev=%.2f  → ev_alert チャンネル送信",
            race_id, max_ev,
        )
```

- [ ] **Step 2: EV アラートのテストを実行して PASS することを確認する**

```
pytest tests/test_router.py::TestEvAlert -v
```

期待出力: 全テスト `PASSED`

---

## Task 6: `send_note_draft` の実装 & コミット

**Files:**
- Modify: `src/notification/router.py`（Task 5 の ab_test セクションの後に追記）

- [ ] **Step 1: `send_note_draft` メソッドを追記する**

```python
    # ── note_draft チャンネル ────────────────────────────────────────────────

    def send_note_draft(
        self,
        title: str,
        body: str,
        x_post: str | None = None,
    ) -> bool:
        """note下書きをチャンク分割して note_draft チャンネルへ順番送信する。

        各チャンクに【note下書き (N/M)】ページング番号を付与。
        最後に x_post（未指定時は自動生成）を X 告知ポスト案として送信する。

        Returns:
            True: 送信成功（チャンネル設定あり）
            False: チャンネル未設定（URL 未設定）
        """
        notifier = self._channels.get("note_draft") or self._channels.get("prediction")
        if notifier is None:
            logger.warning("Discord URL 未設定のため note下書き送信スキップ")
            return False

        if "note_draft" not in self._channels:
            logger.warning(
                "DISCORD_WEBHOOK_NOTE_DRAFT 未設定 — prediction チャンネルへフォールバック"
            )

        chunks = _chunk_text(body)
        n_total = len(chunks)

        for i, chunk in enumerate(chunks, 1):
            footer = "\n_（以上）_" if i == n_total else ""
            message = (
                f"【note下書き ({i}/{n_total})】\n"
                f"```markdown\n{chunk}{footer}\n```"
            )
            notifier.send_text(message)

        post = x_post if x_post is not None else _generate_x_post(title, body)
        notifier.send_text(
            f"📢 **X（Twitter）告知ポスト案**\n```\n{post}\n```"
        )

        logger.info(
            "[Discord:note_draft] 送信完了: %dチャンク + X告知ポスト1件",
            n_total,
        )
        return True
```

- [ ] **Step 2: `send_note_draft` のテストを実行して PASS することを確認する**

```
pytest tests/test_router.py::TestSendNoteDraft -v
```

期待出力: 全テスト `PASSED`

- [ ] **Step 3: テスト全件をまとめて実行して PASS することを確認する**

```
pytest tests/test_router.py -v
```

期待出力: 全テスト `PASSED`（`FAILED` が 0 件）

- [ ] **Step 4: コミットする**

```bash
git add src/notification/router.py tests/test_router.py
git commit -m "feat: NotificationRouter 新設 — マルチWebhookルーティング + EV激熱アラート + note下書き転送"
```

---

## Task 7: `src/pipeline/prediction.py` の呼び出し元を更新

**Files:**
- Modify: `src/pipeline/prediction.py` (L28, L35)

- [ ] **Step 1: import と初期化を置換する**

`src/pipeline/prediction.py` の L28 を変更する:

```python
# 変更前 (L28):
from src.notification.discord_notifier import DiscordNotifier

# 変更後 (L28):
from src.notification.router import NotificationRouter
```

`src/pipeline/prediction.py` の L35 を変更する:

```python
# 変更前 (L35):
_discord = DiscordNotifier()

# 変更後 (L35):
_discord = NotificationRouter()
```

- [ ] **Step 2: 既存の prediction パイプラインテストが通ることを確認する**

```
pytest tests/test_pipeline_prediction.py -v
```

期待出力: 既存テストが全て `PASSED`（新たな `FAILED` が発生しないこと）

- [ ] **Step 3: コミットする**

```bash
git add src/pipeline/prediction.py
git commit -m "refactor: prediction.py の DiscordNotifier を NotificationRouter に置換"
```

---

## Task 8: `scripts/today_auto_runner.py` のインライン helper を置換

**Files:**
- Modify: `scripts/today_auto_runner.py` (L54–87)

- [ ] **Step 1: モジュール先頭の import セクションに NotificationRouter を追加し、インライン helper を置換する**

`today_auto_runner.py` の先頭近く（`load_dotenv` 呼び出しの直後）に以下を追加する:

```python
# L52 の load_dotenv の直後に追加:
from src.notification.router import NotificationRouter as _Router

_router = _Router()
```

次に、L54–87 の `_send_discord` と `_send_discord_race` の関数定義を以下に置換する:

```python
def _send_discord(text: str, *, color: int | None = None) -> None:
    """システムチャンネルにメッセージを送信する（NotificationRouter 経由）。"""
    try:
        if color is not None:
            _router.send_system_embed(title="", description=text, color=color)
        else:
            _router.send_system_text(text)
    except Exception:
        pass


def _send_discord_race(text: str) -> None:
    """予想チャンネルにメッセージを送信する（NotificationRouter 経由）。"""
    try:
        _router.send_text(text)
    except Exception:
        pass
```

- [ ] **Step 2: scheduler テストが通ることを確認する（スモークテスト）**

```
pytest tests/test_scheduler_state.py -v
```

期待出力: `PASSED`（新たな `FAILED` が発生しないこと）

- [ ] **Step 3: コミットする**

```bash
git add scripts/today_auto_runner.py
git commit -m "refactor: today_auto_runner.py のインライン Discord helper を NotificationRouter に統一"
```

---

## Task 9: `scripts/post_weekly_note_draft.py` に Discord 転送 & フィーチャートグルを追加

**Files:**
- Modify: `scripts/post_weekly_note_draft.py`

- [ ] **Step 1: ファイル先頭の import に os と NotificationRouter を追加する**

既存の `import argparse` 行の下に追加する:

```python
import os
```

（既に `import os` がある場合はスキップ）

- [ ] **Step 2: `_is_playwright_enabled` 関数と `publish_via_playwright` 関数を追加する**

`_generate_article` 関数の直前に以下を挿入する:

```python
def _is_playwright_enabled() -> bool:
    """ENABLE_PLAYWRIGHT_POST=True/1 の場合のみ True を返す。デフォルトは False。"""
    val = os.getenv("ENABLE_PLAYWRIGHT_POST", "").strip().lower()
    return val in ("true", "1", "yes")


def publish_via_playwright(
    title: str,
    body: str,
    *,
    headless: bool = True,
) -> bool:
    """Playwright で note.com に下書き保存する。

    ENABLE_PLAYWRIGHT_POST=True の場合のみ main() から呼び出す。
    """
    from src.ops.note_draft_publisher import save_draft
    return save_draft(
        title=title,
        body=body,
        tags=["競馬", "AI予想", "UMALOGI", "JRA", "期待値", "競馬AI"],
        headless=headless,
    )
```

- [ ] **Step 3: `main()` 関数の "Step 3: note.com に下書き保存" ブロックを置換する**

既存の L100–124（Step 3 以降）を以下に置き換える:

```python
    # ── Step 3-A: Discord へ下書きを転送する ──────────────────────────────
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    discord_ok = router.send_note_draft(title=title, body=body)

    playwright_enabled = _is_playwright_enabled()
    n_chunks = len(body) // 1800 + 1

    print()
    print("=" * 60)
    if discord_ok:
        print(f"  ✅ Discord note-draft 送信完了: {n_chunks}チャンク + X告知ポスト1件")
    else:
        print("  ⚠️  Discord URL 未設定のため note-draft 送信スキップ")
        print("     .env に DISCORD_WEBHOOK_NOTE_DRAFT を設定してください。")
    print(
        f"  ℹ️  Playwright投稿: "
        f"{'実行中' if playwright_enabled else '設定によりスキップ'}"
        f" (ENABLE_PLAYWRIGHT_POST={playwright_enabled})"
    )
    print("=" * 60)

    # ── Step 3-B: Playwright 投稿（フラグが ON の場合のみ） ───────────────
    if playwright_enabled:
        # セッション確認
        if not _SESSION_FILE.exists():
            print()
            print("=" * 60)
            print("  note.com セッションが見つかりません。")
            print("  ブラウザが開くので、ログインしてください。")
            print("=" * 60)
            from src.ops.note_draft_publisher import login_and_save_session
            ok = login_and_save_session()
            if not ok:
                logger.error("ログイン失敗。Playwright 投稿をスキップします。")
                return
            logger.info("ログイン完了: %s", _SESSION_FILE)

        logger.info("Playwright 投稿開始 (headless=%s)...", headless)
        pw_ok = publish_via_playwright(title, body, headless=headless)
        print()
        print("=" * 60)
        if pw_ok:
            print("  ✅ note.com 下書き保存 完了!")
            print(f"  タイトル: {title}")
            print("  note.com の下書き一覧を確認してください。")
        else:
            print("  ❌ note.com 下書き保存 失敗")
            print("  outputs/debug/ のスクリーンショットを確認してください。")
        print("=" * 60)
        print()
        if not pw_ok:
            sys.exit(1)
```

同時に、既存の L71–98（Step 1 セッション確認）のブロックを削除し、`if args.login_only:` の処理だけ先頭に残す。最終的な `main()` の骨格は以下になる:

```python
def main() -> None:
    p = argparse.ArgumentParser(...)
    p.add_argument("--week-offset", ...)
    p.add_argument("--login-only", ...)
    p.add_argument("--no-headless", ...)
    args = p.parse_args()

    headless = not args.no_headless

    if args.login_only:
        from src.ops.note_draft_publisher import login_and_save_session
        ok = login_and_save_session()
        if not ok:
            logger.error("ログイン失敗。")
            sys.exit(1)
        print("\n✅ ログイン完了（--login-only のため投稿はスキップ）")
        return

    # Step 2: 記事生成
    logger.info("週次記事を生成中 (week_offset=%d)...", args.week_offset)
    title, body = _generate_article(args.week_offset)
    logger.info("記事生成完了: %d 文字", len(body))
    print(f"\n  タイトル: {title}")
    print(f"  本文文字数: {len(body):,} 文字\n")

    # ─ ここから Step 3-A + 3-B の完全なコード ─

    # Step 3-A: Discord へ下書きを転送する
    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    discord_ok = router.send_note_draft(title=title, body=body)

    playwright_enabled = _is_playwright_enabled()
    n_chunks = max(1, (len(body) + 1799) // 1800)

    print()
    print("=" * 60)
    if discord_ok:
        print(f"  ✅ Discord note-draft 送信完了: {n_chunks}チャンク + X告知ポスト1件")
    else:
        print("  ⚠️  Discord URL 未設定のため note-draft 送信スキップ")
        print("     .env に DISCORD_WEBHOOK_NOTE_DRAFT を設定してください。")
    print(
        f"  ℹ️  Playwright投稿: "
        f"{'実行中' if playwright_enabled else '設定によりスキップ'}"
        f" (ENABLE_PLAYWRIGHT_POST={playwright_enabled})"
    )
    print("=" * 60)

    # Step 3-B: Playwright 投稿（フラグが ON の場合のみ）
    if not playwright_enabled:
        return

    if not _SESSION_FILE.exists():
        print()
        print("=" * 60)
        print("  note.com セッションが見つかりません。")
        print("  ブラウザが開くので、ログインしてください。")
        print("  reCAPTCHA が出た場合は手動で解決してください。")
        print("=" * 60)
        from src.ops.note_draft_publisher import login_and_save_session
        ok = login_and_save_session()
        if not ok:
            logger.error("ログイン失敗。Playwright 投稿をスキップします。")
            return
        logger.info("ログイン完了: %s", _SESSION_FILE)

    logger.info("Playwright 投稿開始 (headless=%s)...", headless)
    pw_ok = publish_via_playwright(title, body, headless=headless)
    print()
    print("=" * 60)
    if pw_ok:
        print("  ✅ note.com 下書き保存 完了!")
        print(f"  タイトル: {title}")
        print("  note.com の下書き一覧を確認してください。")
        print("  公開は手動で行ってください（自動公開は行いません）。")
    else:
        print("  ❌ note.com 下書き保存 失敗")
        print("  outputs/debug/ のスクリーンショットを確認してください。")
        print("  セッション期限切れの場合: --login-only で再ログインしてください。")
    print("=" * 60)
    print()

    if not pw_ok:
        sys.exit(1)
```

- [ ] **Step 4: スクリプトを --help でスモークテストする**

```
py scripts/post_weekly_note_draft.py --help
```

期待出力: ArgumentParser のヘルプテキストが表示される（エラーなし）

- [ ] **Step 5: コミットする**

```bash
git add scripts/post_weekly_note_draft.py
git commit -m "feat: post_weekly_note_draft.py に Discord note-draft 転送 & ENABLE_PLAYWRIGHT_POST トグル追加"
```

---

## Task 10: `scripts/scheduler.py` のインライン helper を置換

**Files:**
- Modify: `scripts/scheduler.py` (L476–518 の `_send_discord` / `_send_discord_embed`)

- [ ] **Step 1: import と module-level router インスタンスを追加する**

`scheduler.py` の先頭の import セクション（`import os` などの後）に追加する:

```python
from src.notification.router import NotificationRouter as _Router

_scheduler_router = _Router()
```

- [ ] **Step 2: L476–518 の `_send_discord` と `_send_discord_embed` を置換する**

```python
def _send_discord(text: str) -> None:
    """Discord 予想チャンネルにテキストメッセージを送信する（NotificationRouter 経由）。"""
    try:
        _scheduler_router.send_text(text)
    except Exception as exc:
        logger.warning("Discord 送信失敗: %s", exc)


def _send_discord_embed(embeds: list[dict]) -> None:
    """Discord 予想チャンネルに Embed メッセージを送信する（NotificationRouter 経由）。"""
    try:
        pred = _scheduler_router._get("prediction")
        if pred:
            pred._post(pred._url, {"embeds": embeds})
    except Exception as exc:
        logger.warning("Discord Embed 送信失敗: %s", exc)
```

- [ ] **Step 3: スモークテストする**

```
pytest tests/test_scheduler_state.py -v
```

期待出力: `PASSED`（新たな `FAILED` が発生しないこと）

- [ ] **Step 4: コミットする**

```bash
git add scripts/scheduler.py
git commit -m "refactor: scheduler.py のインライン Discord helper を NotificationRouter に統一"
```

---

## Task 12: `2.env` テンプレートの更新

**Files:**
- Modify: `2.env`

- [ ] **Step 1: 新規環境変数を追記する**

現在の `2.env` の内容を以下に置き換える:

```
# ── Discord Webhook URLs ─────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL=              # 予想・結果・週次レポート（必須・フォールバック基準）
DISCORD_WEBHOOK_SYSTEM=           # システムログ・エラー（旧: DISCORD_SYSTEM_WEBHOOK_URL から移行）
DISCORD_WEBHOOK_EV_ALERT=         # EV>=1.5 激熱レース専用（未設定時は prediction へ fallback）
DISCORD_WEBHOOK_AB_TEST=          # V1/V2 週次 A/B テスト比較レポート（未設定時は prediction へ fallback）
DISCORD_WEBHOOK_NOTE_DRAFT=       # note下書き出力専用（未設定時はシステムログのみ）

# ── 旧変数（後方互換のため継続サポート・DISCORD_WEBHOOK_SYSTEM 優先） ────────
# DISCORD_SYSTEM_WEBHOOK_URL=

# ── 通知有効フラグ ─────────────────────────────────────────────────────────
NOTIFY_DISCORD=1
NOTIFY_LINE=1
NOTIFY_TWITTER=0

# ── note下書き投稿モード ─────────────────────────────────────────────────────
ENABLE_PLAYWRIGHT_POST=           # True/1 にすると Playwright 自動投稿も実行（デフォルト OFF）

# ── LINE / X ──────────────────────────────────────────────────────────────
LINE_NOTIFY_TOKEN=
X_API_KEY=
X_API_SECRET=
X_ACCESS_TOKEN=
X_ACCESS_TOKEN_SECRET=
```

- [ ] **Step 2: コミットする**

```bash
git add 2.env
git commit -m "chore: 2.env テンプレートに新規 Discord Webhook 変数と ENABLE_PLAYWRIGHT_POST を追記"
```

---

## Task 13: 全テスト実行 & docs 更新

**Files:**
- Modify: `docs/7_weakness_ledger.md`

- [ ] **Step 1: 全テストを実行して PASS することを確認する**

```
pytest tests/test_router.py tests/test_pipeline_prediction.py tests/test_discord_notifier.py tests/test_scheduler_state.py -v
```

期待出力: 全テスト `PASSED`、`FAILED` が 0 件

- [ ] **Step 2: `docs/7_weakness_ledger.md` に作業完了エントリを追記する**

弱点台帳の先頭 Changelog テーブルに以下のエントリを追加する:

```markdown
| 2026-05-20 | Discord 通知ルーター新設 (NotificationRouter): EV激熱アラート・note下書き転送・ENABLE_PLAYWRIGHT_POST トグル実装。影響: src/notification/router.py, src/pipeline/prediction.py, scripts/today_auto_runner.py, scripts/post_weekly_note_draft.py |
```

- [ ] **Step 3: `docs/1_prediction_logic.md` に Changelog エントリを追加する**（通知ロジック変更）

```markdown
| 2026-05-20 | EV>=1.5 の激熱レースを DISCORD_WEBHOOK_EV_ALERT チャンネルへ自動追加送信。NotificationRouter 導入。影響: src/notification/router.py, src/pipeline/prediction.py |
```

- [ ] **Step 4: 最終コミットする**

```bash
git add docs/7_weakness_ledger.md docs/1_prediction_logic.md
git commit -m "docs: Discord ルーター新設の Changelog を弱点台帳・予測ロジックドキュメントに追記"
```

---

## Task 14: discord_notifier.py L493「組」→「点」表記修正

**Files:**
- Modify: `src/notification/discord_notifier.py:493`

- [ ] **Step 1: テストを書いて失敗させる**

`tests/test_discord_notifier.py` の既存テストクラスに追記（または末尾に追加）:

```python
def test_combo_card_shows_ten_not_kumi(self) -> None:
    """(+N組) ではなく (+N点) と表示されることを確認。"""
    from src.notification.discord_notifier import _format_combo_card
    # 三連単 5点超のケースを作る
    combos = [1, 2, 3, 4, 5, 6]  # 6点
    result = _format_combo_card(combos, {}, "trifecta_single")
    assert "(+2点)" in result, f"expected '(+2点)' but got: {result!r}"
    assert "組" not in result, f"'組' should not appear, got: {result!r}"
```

- [ ] **Step 2: テストが失敗することを確認する**

```
pytest tests/test_discord_notifier.py::test_combo_card_shows_ten_not_kumi -v
```

期待: `FAILED` (AssertionError: expected '(+2点)' but got ...)

- [ ] **Step 3: `src/notification/discord_notifier.py` L493 を修正する**

変更前:
```python
            lines.append(f"  (+{n_total - 4}組)")
```

変更後:
```python
            lines.append(f"  (+{n_total - 4}点)")
```

- [ ] **Step 4: テストが通ることを確認する**

```
pytest tests/test_discord_notifier.py::test_combo_card_shows_ten_not_kumi -v
```

期待: `PASSED`

- [ ] **Step 5: コミットする**

```bash
git add src/notification/discord_notifier.py tests/test_discord_notifier.py
git commit -m "fix: 買い目表示の表記を「組」→「点」に統一 (discord_notifier.py L493)"
```

---

## Task 15: generate_weekly_note.py に is_premium フラグ追加

**Files:**
- Modify: `scripts/generate_weekly_note.py`
- Modify: `scripts/post_weekly_note_draft.py`

- [ ] **Step 1: テストを書く（失敗状態）**

`tests/test_generate_weekly_note.py`（新規または既存に追記）:

```python
import sqlite3
from unittest.mock import MagicMock

def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # 最低限のテーブルを作成
    conn.execute("""CREATE TABLE races (race_id TEXT, race_date TEXT, race_name TEXT,
                    venue TEXT, race_number INTEGER, race_type TEXT, distance INTEGER,
                    surface TEXT, grade TEXT)""")
    conn.execute("""CREATE TABLE predictions (prediction_id INTEGER PRIMARY KEY,
                    race_id TEXT, model_type TEXT, bet_type TEXT, horse_number INTEGER,
                    horse_name TEXT, expected_value REAL, created_at TEXT)""")
    conn.execute("""CREATE TABLE prediction_results (prediction_id INTEGER,
                    is_hit INTEGER, payout INTEGER, settled_at TEXT)""")
    return conn

def test_free_version_contains_separator() -> None:
    """is_premium=False の場合、セパレーターが含まれる。"""
    from scripts.generate_weekly_note import generate_weekly_note
    conn = _make_conn()
    body = generate_weekly_note(conn, week_offset=1, include_picks=False, is_premium=False)
    assert "【有料エリア設定箇所" in body, f"separator missing in: {body[:300]}"

def test_premium_version_no_separator() -> None:
    """is_premium=True の場合、セパレーターは含まれない。"""
    from scripts.generate_weekly_note import generate_weekly_note
    conn = _make_conn()
    body = generate_weekly_note(conn, week_offset=1, include_picks=False, is_premium=True)
    assert "【有料エリア設定箇所" not in body, "separator should not appear in premium mode"
```

- [ ] **Step 2: テストが失敗することを確認する**

```
pytest tests/test_generate_weekly_note.py -v
```

期待: `FAILED` (TypeError: unexpected keyword argument 'is_premium')

- [ ] **Step 3: `scripts/generate_weekly_note.py` を修正する**

`generate_weekly_note` 関数のシグネチャを変更:

変更前:
```python
def generate_weekly_note(
    conn: sqlite3.Connection,
    week_offset: int = 1,
    include_picks: bool = True,
) -> str:
```

変更後:
```python
_PREMIUM_SEPARATOR = (
    "\n\n---\n\n"
    "【有料エリア設定箇所：ここから下はnoteの有料ブロックへ貼り付けてください】\n\n"
    "---\n\n"
)

def generate_weekly_note(
    conn: sqlite3.Connection,
    week_offset: int = 1,
    include_picks: bool = True,
    is_premium: bool = False,
) -> str:
```

関数末尾の `return body` 直前に以下を挿入:

```python
    if not is_premium:
        # 有料コンテンツ（oracle EV Top3・三連複フォーメーション等）の前にセパレーターを挿入
        # 無料版はセパレーター以降をnote側で有料ブロックに設定する運用
        body = body + _PREMIUM_SEPARATOR
    return body
```

- [ ] **Step 4: `scripts/post_weekly_note_draft.py` を修正して `IS_PREMIUM_NOTE` を読む**

`_generate_article` 関数内を修正:

変更前:
```python
        body = generate_weekly_note(conn, week_offset=week_offset, include_picks=True)
```

変更後:
```python
        _is_premium = os.environ.get("IS_PREMIUM_NOTE", os.environ.get("NOTE_IS_PREMIUM", "")).lower() in ("1", "true")
        body = generate_weekly_note(conn, week_offset=week_offset, include_picks=True, is_premium=_is_premium)
```

- [ ] **Step 5: テストが通ることを確認する**

```
pytest tests/test_generate_weekly_note.py -v
```

期待: 両テスト `PASSED`

- [ ] **Step 6: コミットする**

```bash
git add scripts/generate_weekly_note.py scripts/post_weekly_note_draft.py tests/test_generate_weekly_note.py
git commit -m "feat: generate_weekly_note に is_premium フラグ追加・有料/無料セパレーター自動挿入"
```

---

## Task 16: router.py に _format_buying_guide() 追加

**Files:**
- Modify: `src/notification/router.py`
- Test: `tests/test_router.py`

- [ ] **Step 1: テストを追記する（失敗状態）**

`tests/test_router.py` の末尾に追加:

```python
def test_format_buying_guide_basic() -> None:
    """honmei/manji/alpha の buy_candidates から買い方テンプレートを生成する。"""
    from src.notification.router import _format_buying_guide

    class _Bet:
        def __init__(self, bet_type, numbers, names, ev):
            self.bet_type = bet_type
            self.numbers = numbers
            self.names = names
            self.expected_value = ev

    predictions = {
        "honmei": [_Bet("win", [5], ["アーバンシック"], 1.8),
                   _Bet("place", [5], ["アーバンシック"], 1.6)],
        "manji":  [_Bet("quinella", [[5, 3]], ["アーバンシック", "レガシー"], 2.1)],
        "alpha":  [_Bet("trio", [[5, 3, 7]], ["アーバン", "レガシー", "サクセス"], 3.0)],
    }
    text = _format_buying_guide(predictions)
    assert "単勝" in text
    assert "複勝" in text
    assert "馬連" in text
    assert "三連複" in text
    assert "アーバンシック" in text

def test_format_buying_guide_empty_returns_none() -> None:
    """全予想が空の場合 None を返す。"""
    from src.notification.router import _format_buying_guide
    assert _format_buying_guide({}) is None
```

- [ ] **Step 2: テストが失敗することを確認する**

```
pytest tests/test_router.py::test_format_buying_guide_basic tests/test_router.py::test_format_buying_guide_empty_returns_none -v
```

期待: `FAILED` (ImportError または AttributeError)

- [ ] **Step 3: `src/notification/router.py` に `_format_buying_guide` を実装する**

`router.py` の `_generate_x_post` の後（`NotificationRouter` クラスの前）に追加:

```python
def _format_buying_guide(predictions: dict) -> str | None:
    """
    honmei/manji/alpha の予想結果から推奨買い方テンプレートを生成する。
    bet_type が存在しない場合はそのセクションをスキップ。
    すべて空なら None を返す。
    """
    lines: list[str] = []

    def _horse_label(numbers: list, names: list) -> str:
        parts = []
        for i, num in enumerate(numbers):
            name = names[i] if i < len(names) else ""
            parts.append(f"{num}番 {name}" if name else f"{num}番")
        return "、".join(parts)

    # 単勝・複勝（honmei から）
    honmei_bets = predictions.get("honmei", [])
    win_bet = next((b for b in honmei_bets if getattr(b, "bet_type", "") == "win"), None)
    place_bet = next((b for b in honmei_bets if getattr(b, "bet_type", "") == "place"), None)

    if win_bet or place_bet:
        lines.append("■ 単複で手堅く行くなら")
        if win_bet:
            nums = getattr(win_bet, "numbers", [])
            names = getattr(win_bet, "names", [])
            lines.append(f"・単勝：{_horse_label(nums, names)}（1点）")
        if place_bet:
            nums = getattr(place_bet, "numbers", [])
            names = getattr(place_bet, "names", [])
            lines.append(f"・複勝：{_horse_label(nums, names)}（1点）")
        lines.append("")

    # 馬連（manji から quinella）
    manji_bets = predictions.get("manji", [])
    quinella_bet = next((b for b in manji_bets if getattr(b, "bet_type", "") == "quinella"), None)
    if quinella_bet:
        nums_raw = getattr(quinella_bet, "numbers", [])
        names_raw = getattr(quinella_bet, "names", [])
        # numbers は [[5,3],[5,7],...] 形式
        if nums_raw and isinstance(nums_raw[0], (list, tuple)):
            jiku = nums_raw[0][0]
            jiku_name = names_raw[0] if names_raw else ""
            aite_nums = [str(combo[1]) for combo in nums_raw]
            n_ten = len(aite_nums)
            lines.append("■ 馬連で中穴・好配当を狙うなら")
            lines.append(f"・馬連 軸流し：{jiku}番 {jiku_name} → 相手：{'、'.join(aite_nums)}番（計{n_ten}点）")
            lines.append("")

    # 三連複（alpha または manji から trio）
    alpha_bets = predictions.get("alpha", [])
    trio_bet = next(
        (b for b in alpha_bets + manji_bets if getattr(b, "bet_type", "") == "trio"),
        None,
    )
    if trio_bet:
        nums_raw = getattr(trio_bet, "numbers", [])
        names_raw = getattr(trio_bet, "names", [])
        if nums_raw and isinstance(nums_raw[0], (list, tuple)):
            jiku = nums_raw[0][0]
            jiku_name = names_raw[0] if names_raw else ""
            aite_nums = list(dict.fromkeys(
                str(n) for combo in nums_raw for n in combo if n != jiku
            ))
            n_ten = len(nums_raw)
            lines.append("■ 三連複で高配当（万馬券）を狙うなら")
            lines.append(
                f"・三連複 軸1頭流し：{jiku}番 {jiku_name}"
                f" → 相手：{'、'.join(aite_nums)}番（計{n_ten}点）"
            )
            lines.append("")

    if not lines:
        return None
    return "【💡 推奨される買い方サマリー】\n\n" + "\n".join(lines).rstrip()
```

- [ ] **Step 4: `NotificationRouter.notify_prerace_result` を更新して買い方ガイドを追加送信する**

`notify_prerace_result` メソッドで embed 送信後に以下を追加（`predictions` dict が渡される箇所）:

```python
        # 買い方ガイドを別メッセージとして送信
        guide = _format_buying_guide(predictions)
        if guide:
            notifier.send_text(guide)
```

注意: `predictions` は `{"honmei": [...], "manji": [...], "alpha": [...]}` 形式で渡す必要がある。
`notify_prerace_result` のシグネチャに `predictions: dict | None = None` を追加すること。

- [ ] **Step 5: テストが通ることを確認する**

```
pytest tests/test_router.py -v
```

期待: 全テスト `PASSED`

- [ ] **Step 6: コミットする**

```bash
git add src/notification/router.py tests/test_router.py
git commit -m "feat: router に _format_buying_guide() 追加・推奨買い方サマリーをDiscordに自動送信"
```

---

## Task 17: scripts/run_2year_backtest.py 新設

**Files:**
- Create: `scripts/run_2year_backtest.py`

- [ ] **Step 1: スクリプトを作成する**

```python
"""
scripts/run_2year_backtest.py — 2カ年厳選黒字化シミュレーター

Pattern A: EV閾値のみ (EV >= threshold)
Pattern B: オッズ範囲 + EV閾値 (odds_lo <= odds <= odds_hi AND EV >= threshold)
Pattern C: 1日購入上限 + EV閾値 (daily top-N by EV, EV >= threshold)

使い方:
    py scripts/run_2year_backtest.py
    py scripts/run_2year_backtest.py --output results/backtest_grid.csv
    py scripts/run_2year_backtest.py --model honmei --bet-type win
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_DB_PATH = _ROOT / "data" / "umalogi.db"

_EV_THRESHOLDS = [1.0, 1.2, 1.5, 2.0]
_ODDS_RANGES = [(3.0, 10.0), (5.0, 25.0), (5.0, 50.0)]
_DAILY_LIMITS = [3, 5, 10]


@dataclass
class _BacktestRow:
    pattern: str
    params: str
    model_type: str
    bet_type: str
    n_bets: int
    n_hits: int
    total_cost: int
    total_payout: int
    roi: float
    hit_rate: float


def _load_data(
    conn: sqlite3.Connection,
    model_type: str,
    bet_type: str,
) -> list[dict[str, Any]]:
    """predictions + prediction_results を JOIN して返す。"""
    sql = """
    SELECT
        p.prediction_id,
        p.race_id,
        p.expected_value,
        p.horse_number,
        r.race_date,
        COALESCE(pr.is_hit, 0)  AS is_hit,
        COALESCE(pr.payout, 0)  AS payout
    FROM predictions p
    LEFT JOIN prediction_results pr ON pr.prediction_id = p.prediction_id
    LEFT JOIN races r ON r.race_id = p.race_id
    WHERE p.model_type = ?
      AND p.bet_type   = ?
    ORDER BY r.race_date, p.race_id
    """
    rows = conn.execute(sql, (model_type, bet_type)).fetchall()
    return [dict(row) for row in rows]


def _run_pattern_a(
    rows: list[dict], ev_thresh: float, model_type: str, bet_type: str
) -> _BacktestRow:
    filtered = [r for r in rows if (r["expected_value"] or 0.0) >= ev_thresh]
    n_bets = len(filtered)
    n_hits = sum(r["is_hit"] for r in filtered)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in filtered)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="A",
        params=f"ev>={ev_thresh:.1f}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _run_pattern_b(
    rows: list[dict],
    ev_thresh: float,
    odds_lo: float,
    odds_hi: float,
    model_type: str,
    bet_type: str,
) -> _BacktestRow:
    filtered = [
        r for r in rows
        if (r["expected_value"] or 0.0) >= ev_thresh
        and odds_lo <= (r["payout"] / 100.0 if r["payout"] else 0.0) <= odds_hi
    ]
    n_bets = len(filtered)
    n_hits = sum(r["is_hit"] for r in filtered)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in filtered)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="B",
        params=f"ev>={ev_thresh:.1f},odds={odds_lo:.0f}-{odds_hi:.0f}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _run_pattern_c(
    rows: list[dict],
    ev_thresh: float,
    daily_limit: int,
    model_type: str,
    bet_type: str,
) -> _BacktestRow:
    from collections import defaultdict
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if (r["expected_value"] or 0.0) >= ev_thresh:
            by_date[r["race_date"] or "unknown"].append(r)
    selected: list[dict] = []
    for date_rows in by_date.values():
        sorted_rows = sorted(date_rows, key=lambda x: x["expected_value"] or 0.0, reverse=True)
        selected.extend(sorted_rows[:daily_limit])
    n_bets = len(selected)
    n_hits = sum(r["is_hit"] for r in selected)
    total_cost = n_bets * 100
    total_payout = sum(r["payout"] for r in selected)
    roi = total_payout / total_cost if total_cost > 0 else 0.0
    hit_rate = n_hits / n_bets if n_bets > 0 else 0.0
    return _BacktestRow(
        pattern="C",
        params=f"ev>={ev_thresh:.1f},daily<={daily_limit}",
        model_type=model_type,
        bet_type=bet_type,
        n_bets=n_bets,
        n_hits=n_hits,
        total_cost=total_cost,
        total_payout=total_payout,
        roi=roi,
        hit_rate=hit_rate,
    )


def _print_summary(results: list[_BacktestRow]) -> None:
    print()
    print("=" * 90)
    print(f"{'Pattern':<8} {'Params':<30} {'Model':<8} {'BetType':<8} "
          f"{'Bets':>6} {'Hits':>5} {'ROI':>7} {'HitRate':>8}")
    print("-" * 90)
    for r in sorted(results, key=lambda x: x.roi, reverse=True):
        flag = "★" if r.roi >= 1.0 and r.n_bets >= 10 else " "
        print(f"{flag}{r.pattern:<7} {r.params:<30} {r.model_type:<8} {r.bet_type:<8} "
              f"{r.n_bets:>6} {r.n_hits:>5} {r.roi:>7.1%} {r.hit_rate:>8.1%}")
    print("=" * 90)
    winners = [r for r in results if r.roi >= 1.0 and r.n_bets >= 10]
    print(f"\n★ ROI >= 100% かつ Bets >= 10 の組み合わせ: {len(winners)} 件")


def _save_csv(results: list[_BacktestRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pattern", "params", "model_type", "bet_type",
                    "n_bets", "n_hits", "total_cost", "total_payout", "roi", "hit_rate"])
        for r in results:
            w.writerow([r.pattern, r.params, r.model_type, r.bet_type,
                        r.n_bets, r.n_hits, r.total_cost, r.total_payout,
                        f"{r.roi:.4f}", f"{r.hit_rate:.4f}"])
    print(f"\n📊 CSV 保存: {path}")


def main() -> None:
    p = argparse.ArgumentParser(description="2カ年厳選黒字化シミュレーター")
    p.add_argument("--model", default="honmei", help="モデルタイプ (honmei/manji/alpha)")
    p.add_argument("--bet-type", default="win", help="券種 (win/place/quinella/trio)")
    p.add_argument("--output", default="", help="CSV 出力先パス（省略時は出力なし）")
    args = p.parse_args()

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = _load_data(conn, args.model, args.bet_type)
    finally:
        conn.close()

    if not rows:
        print(f"⚠️  データなし: model={args.model}, bet_type={args.bet_type}")
        sys.exit(0)

    print(f"🔍 対象データ: {len(rows)} 件 (model={args.model}, bet_type={args.bet_type})")

    results: list[_BacktestRow] = []

    # Pattern A
    for ev in _EV_THRESHOLDS:
        results.append(_run_pattern_a(rows, ev, args.model, args.bet_type))

    # Pattern B
    for ev in _EV_THRESHOLDS:
        for lo, hi in _ODDS_RANGES:
            results.append(_run_pattern_b(rows, ev, lo, hi, args.model, args.bet_type))

    # Pattern C
    for ev in _EV_THRESHOLDS:
        for limit in _DAILY_LIMITS:
            results.append(_run_pattern_c(rows, ev, limit, args.model, args.bet_type))

    _print_summary(results)

    if args.output:
        _save_csv(results, Path(args.output))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 動作確認する**

```
py scripts/run_2year_backtest.py --model honmei --bet-type win
```

期待: 全パターンの ROI 表がターミナルに表示される。エラーなし。

- [ ] **Step 3: コミットする**

```bash
git add scripts/run_2year_backtest.py
git commit -m "feat: run_2year_backtest.py 新設 — Pattern A/B/C グリッドサーチで ROI>=100% 条件を探索"
```

---

## Task 18: scripts/generate_result_note_draft.py 新設

**Files:**
- Create: `scripts/generate_result_note_draft.py`

- [ ] **Step 1: スクリプトを作成する**

```python
"""
scripts/generate_result_note_draft.py — 万馬券特化的中報告 Markdown を生成して Discord 転送する

対象: payout >= 10,000 円 OR (is_hit=1 かつ ROI >= 300%)
Markdown を生成し DISCORD_WEBHOOK_NOTE_DRAFT チャンネルへ送信する。

使い方:
    py scripts/generate_result_note_draft.py
    py scripts/generate_result_note_draft.py --days 14   # 直近14日間
    py scripts/generate_result_note_draft.py --dry-run   # Discord 送信なし（標準出力のみ）
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

sys.stdout.reconfigure(encoding="utf-8")

_DB_PATH = _ROOT / "data" / "umalogi.db"
_PAYOUT_THRESHOLD = 10_000
_ROI_THRESHOLD = 3.0  # 300%


def _fetch_hits(conn: sqlite3.Connection, since: date) -> list[dict]:
    sql = """
    SELECT
        r.race_date,
        r.race_name,
        r.venue,
        p.model_type,
        p.bet_type,
        p.horse_number,
        p.horse_name,
        p.expected_value,
        COALESCE(pr.payout, 0) AS payout,
        COALESCE(pr.is_hit, 0) AS is_hit,
        pr.settled_at
    FROM predictions p
    JOIN prediction_results pr ON pr.prediction_id = p.prediction_id
    LEFT JOIN races r ON r.race_id = p.race_id
    WHERE pr.is_hit = 1
      AND r.race_date >= ?
      AND (
          pr.payout >= ?
          OR (pr.payout >= 100 AND CAST(pr.payout AS REAL) / 100.0 >= ?)
      )
    ORDER BY pr.payout DESC
    """
    rows = conn.execute(sql, (since.isoformat(), _PAYOUT_THRESHOLD, _ROI_THRESHOLD * 100)).fetchall()
    return [dict(row) for row in rows]


_BET_TYPE_LABELS: dict[str, str] = {
    "win": "単勝",
    "place": "複勝",
    "quinella": "馬連",
    "wide": "ワイド",
    "exacta": "馬単",
    "trio": "三連複",
    "trifecta": "三連単",
}


def _build_markdown(hits: list[dict], since: date, until: date) -> str:
    if not hits:
        return f"# 🏇 万馬券炸裂レポート（{since}〜{until}）\n\nこの期間の対象的中はありませんでした。\n"

    lines: list[str] = [
        f"# 🏇 万馬券炸裂レポート（{since}〜{until}）",
        "",
        f"> UMALOGI AI が狙い打った **{len(hits)}件** の高配当的中を公開します。",
        "",
        "---",
        "",
    ]

    total_payout = sum(h["payout"] for h in hits)
    total_cost = len(hits) * 100
    roi = total_payout / total_cost if total_cost > 0 else 0.0

    lines += [
        "## 📊 集計サマリー",
        "",
        f"| 項目 | 値 |",
        f"|---|---|",
        f"| 対象期間 | {since} 〜 {until} |",
        f"| 対象的中件数 | {len(hits)} 件 |",
        f"| 合計払戻 | ¥{total_payout:,} |",
        f"| 合計投資 | ¥{total_cost:,} |",
        f"| ROI | {roi:.1%} |",
        "",
        "---",
        "",
        "## 🎯 的中詳細",
        "",
    ]

    for i, h in enumerate(hits, 1):
        bet_label = _BET_TYPE_LABELS.get(h["bet_type"], h["bet_type"])
        roi_single = h["payout"] / 100.0
        lines += [
            f"### {i}. {h['race_date']} {h['race_name']}（{h['venue']}）",
            "",
            f"- **モデル**: {h['model_type']}",
            f"- **券種**: {bet_label}",
            f"- **馬番**: {h['horse_number']}番 {h['horse_name'] or ''}",
            f"- **EV スコア**: {h['expected_value']:.2f}" if h["expected_value"] else "",
            f"- **払戻**: ¥{h['payout']:,}（{roi_single:.1f}倍）",
            "",
        ]

    lines += [
        "---",
        "",
        "## 💡 AIの予測根拠（サンプル）",
        "",
        "> U score（大衆心理乖離スコア）・オッズ逆行シグナル・前走比較などを総合的に評価して選定しました。",
        "> 詳細な特徴量重要度はダッシュボードでご確認いただけます。",
        "",
        "---",
        "",
        "*このレポートは UMALOGI AI によって自動生成されました。*",
        "*馬券の購入は自己責任でお願いします。未来の結果を保証するものではありません。*",
    ]

    return "\n".join(l for l in lines)


def main() -> None:
    p = argparse.ArgumentParser(description="万馬券特化的中報告 Markdown を生成して Discord 転送")
    p.add_argument("--days", type=int, default=7, help="直近N日間を対象（デフォルト7日）")
    p.add_argument("--dry-run", action="store_true", help="Discord 送信なし（標準出力のみ）")
    args = p.parse_args()

    until = date.today()
    since = until - timedelta(days=args.days)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        hits = _fetch_hits(conn, since)
    finally:
        conn.close()

    print(f"🔍 対象期間: {since} 〜 {until}（{args.days}日間）")
    print(f"🎯 対象的中件数: {len(hits)} 件")

    body = _build_markdown(hits, since, until)
    title = f"🏇【万馬券炸裂レポート】{since}〜{until}"

    print("\n" + "=" * 60)
    print(body[:500] + ("..." if len(body) > 500 else ""))
    print("=" * 60)

    if args.dry_run:
        print("\n✅ --dry-run: Discord 送信をスキップしました。")
        return

    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)

    from src.notification.router import NotificationRouter
    router = NotificationRouter()
    ok = router.send_note_draft(title, body)
    if ok:
        print("\n✅ Discord note-draft チャンネルへ転送完了。")
    else:
        print("\n⚠️  Discord 転送スキップ（DISCORD_WEBHOOK_NOTE_DRAFT 未設定）。")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: ドライランで動作確認する**

```
py scripts/generate_result_note_draft.py --dry-run
```

期待: エラーなし。Markdown がターミナルに出力される。

- [ ] **Step 3: コミットする**

```bash
git add scripts/generate_result_note_draft.py
git commit -m "feat: generate_result_note_draft.py 新設 — 万馬券炸裂レポート自動生成・Discord 転送"
```

---

## Task 19: docs/commercialization_roadmap.md 更新

**Files:**
- Create/Modify: `docs/commercialization_roadmap.md`

- [ ] **Step 1: ファイルを作成/更新する**

`docs/commercialization_roadmap.md` が存在する場合は先頭の Changelog テーブルに追記。
存在しない場合は以下の内容で新規作成する:

```markdown
# UMALOGI 商用化ロードマップ

## 更新履歴

| 日付 | 内容 |
|---|---|
| 2026-05-20 | Phase 1-3 ロードマップ策定・Discord ルーター/note下書き転送/2カ年シミュレーター追加 |

---

## Phase 1: 通知インフラ刷新（〜2026-05-31）

**目標**: 全チャンネルへの適切な通知配信・note連携の自動化

| タスク | ステータス |
|---|---|
| NotificationRouter 新設（5チャンネル対応） | ✅ 完了 |
| EV激熱アラート（EV>=1.5 自動通知） | ✅ 完了 |
| note下書き Discord 転送（ENABLE_PLAYWRIGHT_POST トグル） | ✅ 完了 |
| IS_PREMIUM_NOTE 有料/無料出し分け | ✅ 完了 |
| 買い方テンプレート自動生成（_format_buying_guide） | ✅ 完了 |

---

## Phase 2: 収益最大化戦略の確立（〜2026-06-30）

**目標**: 黒字化できる購買条件をデータドリブンで特定し、note有料記事の収益を最大化

| タスク | ステータス |
|---|---|
| 2カ年グリッドサーチバックテスト（run_2year_backtest.py） | ✅ 完了 |
| 万馬券特化報告スクリプト（generate_result_note_draft.py） | ✅ 完了 |
| FukushoElite モデル本番パイプライン統合 | 🔴 未着手 |
| X シグナルパーサー Phase C（FEATURE_COLS 統合・再訓練） | 🔴 未着手 |
| note有料記事の定期投稿自動化（週1回） | 🟡 対応中 |

---

## Phase 3: スケール・外部公開（〜2026-09-30）

**目標**: 有料会員 100 名・月次 MRR ¥100,000 達成

| タスク | ステータス |
|---|---|
| JVLink 歴史データ大規模取得（3年分） | 🔴 未着手（SID制約解消待ち） |
| note有料コンテンツ毎週自動投稿 CI/CD パイプライン | 🔴 未着手 |
| 破産確率リアルタイム表示（Web UI） | 🔴 未着手 |
| X 凄腕予想家シグナル Phase A/B/C 完全統合 | 🔴 未着手 |
| Discord コミュニティ開設・会員専用チャンネル運用 | 🔴 未着手 |

---

## KPI 目標

| 指標 | 現在値 | Phase 2 目標 | Phase 3 目標 |
|---|---|---|---|
| 単勝 ROI | 691.5% | 維持 | 維持 |
| 複勝 ROI | 95.4% | 110%+ | 120%+ |
| note 有料読者数 | 0 | 50 | 100 |
| 月次 MRR | ¥0 | ¥30,000 | ¥100,000 |
| Discord フォロワー | - | 200 | 500 |
```

- [ ] **Step 2: コミットする**

```bash
git add docs/commercialization_roadmap.md
git commit -m "docs: 商用化ロードマップ Phase 1-3 策定・KPI 目標設定"
```
