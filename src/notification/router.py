"""
src/notification/router.py — Discord 通知ルーター

チャンネル選択・フォールバック・全通知メソッドを集約する。
個別チャンネルへの HTTP 送信は DiscordNotifier に委譲する。
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from .discord_notifier import DiscordNotifier

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
        honmei_bets: object,
        manji_bets: object,
        max_ev: float = 0.0,
        **kwargs,
    ) -> None:
        """prediction へ通知。max_ev >= EV_ALERT_THRESHOLD なら ev_alert へも追加送信。"""
        notifier = self._get("prediction")
        if notifier:
            notifier.notify_prerace_result(race_id, honmei_bets, manji_bets, **kwargs)

        if max_ev >= EV_ALERT_THRESHOLD:
            bets_summary = f"EV最高値: {max_ev:.2f}"
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
        screenshot_path: Path | None = None,
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
        notifier = self._notifiers.get("note_draft")
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
            "Discord note-draft 送信完了: %dチャンク + X告知ポスト%d件",
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
