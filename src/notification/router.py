"""
src/notification/router.py — Discord 通知マルチ Webhook ルーター

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
    "system": "DISCORD_WEBHOOK_SYSTEM",
    "ev_alert": "DISCORD_WEBHOOK_EV_ALERT",
    "ab_test": "DISCORD_WEBHOOK_AB_TEST",
    "note_draft": "DISCORD_WEBHOOK_NOTE_DRAFT",
}

# 後方互換: 旧変数名
_LEGACY_MAP: dict[str, str] = {
    "DISCORD_SYSTEM_WEBHOOK_URL": "system",
}


# ── ヘルパー関数 ──────────────────────────────────────────────────────────────


def _chunk_text(text: str, max_len: int = _CHUNK_MAX) -> list[str]:
    """テキストを max_len 文字以下のチャンクに分割して返す。

    分割優先順位: ダブル改行 → 単一改行 → ハードカット。
    Discord のメッセージ上限（2000文字）に対してデフォルト 1800 文字で制限する。

    Args:
        text: 分割対象のテキスト。
        max_len: チャンクの最大文字数。デフォルトは 1800。

    Returns:
        max_len 文字以下に分割されたチャンクのリスト。
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
            remaining = remaining[pos + 2 :]
            continue
        # 2. 単一改行で分割
        pos = remaining.rfind("\n", 0, max_len)
        if pos > 0:
            chunks.append(remaining[:pos])
            remaining = remaining[pos + 1 :]
            continue
        # 3. ハードカット
        chunks.append(remaining[:max_len])
        remaining = remaining[max_len:]

    if remaining:
        chunks.append(remaining)

    return chunks


def _generate_x_post(title: str, body: str) -> str:
    """note 記事から X（Twitter）告知ポスト（140文字以内）を生成する。

    タイトルの絵文字プレフィックスを除去し、本文の最初の ## 見出しを抽出して
    ハッシュタグ付きのコンパクトなポスト文を生成する。

    Args:
        title: note 記事のタイトル。
        body: note 記事の本文（Markdown 形式）。

    Returns:
        140文字以内の X 投稿テキスト。
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


def _format_buying_guide(predictions: dict) -> str | None:
    """honmei/manji/alpha の予想結果から推奨買い方テンプレートを生成する。

    各モデルの bet_type に応じて単複・馬連・三連複のセクションを生成する。
    bet_type が存在しないセクションはスキップする。

    Args:
        predictions: モデル名をキーとする RaceBets リストの辞書。
                     キー: 'honmei' / 'manji' / 'alpha'。

    Returns:
        推奨買い方テンプレート文字列、またはすべて空の場合は None。
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
    win_bet = next(
        (b for b in honmei_bets if getattr(b, "bet_type", "") == "win"), None
    )
    place_bet = next(
        (b for b in honmei_bets if getattr(b, "bet_type", "") == "place"), None
    )

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
    quinella_bet = next(
        (b for b in manji_bets if getattr(b, "bet_type", "") == "quinella"), None
    )
    if quinella_bet:
        nums_raw = getattr(quinella_bet, "numbers", [])
        names_raw = getattr(quinella_bet, "names", [])
        if nums_raw and isinstance(nums_raw[0], (list, tuple)):
            jiku = nums_raw[0][0]
            jiku_name = names_raw[0] if names_raw else ""
            aite_nums = [str(combo[1]) for combo in nums_raw]
            n_ten = len(aite_nums)
            lines.append("■ 馬連で中穴・好配当を狙うなら")
            lines.append(
                f"・馬連 軸流し：{jiku}番 {jiku_name} → 相手：{'、'.join(aite_nums)}番（計{n_ten}点）"
            )
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
            aite_nums = list(
                dict.fromkeys(str(n) for combo in nums_raw for n in combo if n != jiku)
            )
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


# ── NotificationRouter ────────────────────────────────────────────────────────


class NotificationRouter:
    """マルチ Webhook ルーティング層。

    チャンネル別に DiscordNotifier インスタンスを保持し、
    用途別のフォールバック制御を担う。

    チャンネルマップ:
        prediction: 買い目・結果の主要チャンネル。
        system:     システムログ・エラー通知。
        ev_alert:   EV >= EV_ALERT_THRESHOLD の激熱レース専用。
        ab_test:    V1/V2 成績比較レポート。
        note_draft: note 下書き出力用。
    """

    def __init__(self) -> None:
        """初期化。環境変数からチャンネル Notifier を構築する。"""
        self._channels: dict[str, DiscordNotifier] = {}
        self._build_channels()

    # チャンネル識別子 → ログラベル
    _CHANNEL_LABELS: dict[str, str] = {
        "prediction": "予想",
        "system": "システム",
        "ev_alert": "EV激熱",
        "ab_test": "A/Bテスト",
        "note_draft": "note下書き",
    }

    def _build_channels(self) -> None:
        """環境変数からチャンネル名 → DiscordNotifier の辞書を構築する。

        CHANNEL_ENV マップを走査し、URL が設定されているチャンネルのみ
        DiscordNotifier インスタンスを生成する。
        system チャンネルは旧変数名 DISCORD_SYSTEM_WEBHOOK_URL も後方互換で参照する。
        """
        for channel, env_key in CHANNEL_ENV.items():
            url = os.environ.get(env_key, "").strip()
            # 後方互換: system チャンネルは旧変数名も読む
            if not url and channel == "system":
                url = os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "").strip()
            if url:
                label = self._CHANNEL_LABELS.get(channel, channel)
                self._channels[channel] = DiscordNotifier(
                    webhook_url=url, enabled=True, channel_label=label
                )

        if "prediction" not in self._channels:
            logger.warning(
                "DISCORD_WEBHOOK_URL 未設定 — NotificationRouter: 通知は全スキップされます"
            )

    def _get(self, channel: str) -> DiscordNotifier | None:
        """チャンネルの Notifier を返す。未設定なら prediction チャンネルへフォールバック。

        Args:
            channel: チャンネル識別子（'prediction' / 'system' / 'ev_alert' 等）。

        Returns:
            対応する DiscordNotifier、または prediction フォールバック。
            両方とも未設定の場合は None。
        """
        return self._channels.get(channel) or self._channels.get("prediction")

    # ── prediction チャンネル ────────────────────────────────────────────────

    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: object,
        manji_bets: object,
        oracle_bets: object | None = None,
        hit_focus_bets: object | None = None,
        alpha_bets: object | None = None,
        dashboard_url: str = "",
        predictions: dict | None = None,
    ) -> None:
        """直前予想を prediction チャンネルへ送信する。

        max_ev >= EV_ALERT_THRESHOLD かつ ev_alert チャンネルが独立設定されている場合は
        ev_alert チャンネルへも @everyone 付きで追加送信する。
        """
        # ── 1. prediction チャンネルへ通常送信 ──────────────────────────────
        pred = self._get("prediction")
        if pred:
            pred.notify_prerace_result(
                race_id,
                honmei_bets,
                manji_bets,
                oracle_bets=oracle_bets,
                hit_focus_bets=hit_focus_bets,
                alpha_bets=alpha_bets,
                dashboard_url=dashboard_url,
            )

        # ── 2. 買い方ガイドを別メッセージとして送信 ──────────────────────────
        if predictions and pred:
            guide = _format_buying_guide(predictions)
            if guide:
                pred.send_text(guide)

        # ── 3. EV 激熱アラート（ev_alert が独立チャンネルの場合のみ） ────────
        ev_notifier = self._channels.get("ev_alert")  # fallback を経由しない
        if ev_notifier is None:
            return

        all_bets: list[object] = []
        for rb in [alpha_bets, manji_bets, honmei_bets, oracle_bets, hit_focus_bets]:
            if rb is not None:
                all_bets.extend(getattr(rb, "bets", []))

        max_ev = max((getattr(b, "expected_value", 0.0) for b in all_bets), default=0.0)
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
            race_id,
            max_ev,
        )

    def send_text(self, text: str) -> None:
        """予想チャンネルにプレーンテキストを送信する。

        Args:
            text: 送信するプレーンテキスト。
        """
        n = self._get("prediction")
        if n:
            n.send_text(text)

    def send_prediction_embed(self, embeds: list[dict]) -> None:
        """予想チャンネルに生 embed リストを送信する（scheduler 週次サマリー用）。

        Args:
            embeds: Discord embed オブジェクトのリスト。
        """
        n = self._get("prediction")
        if n:
            n.send_prediction_embed(embeds)

    # ── ev_alert チャンネル ──────────────────────────────────────────────────

    def notify_ev_alert(
        self,
        race_id: str,
        max_ev: float,
        bets_summary: str,
    ) -> None:
        """EV >= EV_ALERT_THRESHOLD の激熱レースを @everyone 付きで ev_alert チャンネルへ送信する。

        ev_alert チャンネルが未設定の場合は何もしない（prediction への二重送信を防ぐ）。

        Args:
            race_id: 対象レースの ID。
            max_ev: 最大期待値。
            bets_summary: 買い目サマリーテキスト。
        """
        ev_notifier = self._channels.get("ev_alert")  # fallback 経由しない
        if ev_notifier is None:
            return
        text = (
            f"@everyone 🔥 **激熱 EV アラート** `{race_id}`\n"
            f"{bets_summary}\n"
            f"EV={max_ev:.2f} ≥ {EV_ALERT_THRESHOLD} 閾値超過"
        )
        ev_notifier.send_text(text)

    def notify_pure_ev_edge(self, race_id: str, pure_ev_bets: object) -> None:
        """Pure_EV_Edge（黒字化専用・単複限定）の買い目を prediction チャンネルへ送信する。

        ev_alert チャンネルが独立設定で最大EVが閾値超なら ev_alert へも追加送信する。

        Args:
            race_id: 対象レース ID。
            pure_ev_bets: PureEVRaceBets 互換（.bets に PureEVBet のリスト）。
        """
        bets = list(getattr(pure_ev_bets, "bets", None) or [])
        if not bets:
            return
        notifier = self._get("prediction")
        if notifier is None:
            return
        lines = [f"💎 **Pure_EV_Edge（黒字化専用・単複）** `{race_id}`"]
        for b in bets:
            lines.append(
                f"・{getattr(b, 'bet_type', '?')} "
                f"{getattr(b, 'horse_number', '?')}番 {getattr(b, 'horse_name', '')}  "
                f"EV={getattr(b, 'expected_value', 0.0):.2f} "
                f"P={getattr(b, 'prob', 0.0):.0%} "
                f"(1/10Kelly ¥{int(getattr(b, 'stake', 0)):,})"
            )
        notifier.send_text("\n".join(lines))

        max_ev = max((getattr(b, "expected_value", 0.0) for b in bets), default=0.0)
        ev_notifier = self._channels.get("ev_alert")
        if ev_notifier is not None and max_ev >= EV_ALERT_THRESHOLD:
            ev_notifier.send_text(
                f"@everyone 💎 **Pure_EV_Edge 激熱** `{race_id}` 最大EV={max_ev:.2f}\n"
                + "\n".join(lines[1:])
            )

    def notify_prerace_15min(
        self,
        race_id: str,
        max_ev: float,
        message: str,
    ) -> None:
        """発走15分前アラートを ev_alert チャンネルへ送信する。

        ev_alert チャンネルが未設定の場合は prediction チャンネルへフォールバックする
        （直前予想通知と同じ見え方になるが、情報が届くことを優先する）。

        Args:
            race_id: 対象レースの ID。
            max_ev: 最大期待値。
            message: 送信するアラートメッセージ本文。
        """
        notifier = self._get("ev_alert")  # ev_alert → prediction フォールバック
        if notifier is None:
            logger.warning(
                "[発走前アラート] Discord URL 未設定 — %s 通知スキップ", race_id
            )
            return
        notifier.send_text(message)
        logger.info("[発走前アラート] %s 送信完了 (max_ev=%.2f)", race_id, max_ev)

    def notify_hit_summary(
        self,
        date_str: str,
        hit_count: int,
        total_count: int,
        cumulative_pnl: int,
        monthly_progress_pct: float,
    ) -> None:
        """的中サマリーを予想チャンネルへ委譲送信する。

        Args:
            date_str: 対象日付の文字列（例: '2026-05-25'）。
            hit_count: 当日の的中件数。
            total_count: 当日の全買い目件数。
            cumulative_pnl: 累積損益（円）。
            monthly_progress_pct: 月次目標進捗率（%）。
        """
        n = self._get("prediction")
        if n:
            n.notify_hit_summary(
                date_str,
                hit_count,
                total_count,
                cumulative_pnl,
                monthly_progress_pct,
            )

    def notify_skip(self, race_id: str, reason: str) -> None:
        """予想見送りを予想チャンネルの Notifier に委譲する。

        Args:
            race_id: 見送り対象のレース ID。
            reason: 見送り理由。
        """
        n = self._get("prediction")
        if n:
            n.notify_skip(race_id, reason)

    # ── system チャンネル ────────────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        """システムチャンネルにプレーンテキストを送信する。

        Args:
            text: 送信するプレーンテキスト。
        """
        n = self._get("system")
        if n:
            n.send_text(text)

    def send_system_embed(
        self,
        title: str,
        description: str,
        **kwargs: Any,
    ) -> None:
        """システムチャンネルに Embed を送信する。

        Args:
            title: Embed のタイトル。
            description: Embed の本文。
            **kwargs: DiscordNotifier.send_system_embed に渡す追加キーワード引数
                      （color / fields / footer）。
        """
        n = self._get("system")
        if n:
            n.send_system_embed(title, description, **kwargs)

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        """スクレイピング異常をシステムチャンネルへ委譲送信する。

        Args:
            race_id: 異常が発生したレース ID。
            detail: 異常の詳細説明。
        """
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
        """手動介入要請をシステムチャンネルへ委譲送信する。

        Args:
            step: 失敗したステップ名。
            error: エラーの詳細文字列。
            action: 推奨される対応アクション。
            screenshot_path: スクリーンショット画像のパス（任意）。
        """
        n = self._get("system")
        if n:
            n.notify_intervention_required(step, error, action, screenshot_path)

    def notify_ror_warning(self, warning_text: str) -> None:
        """RoR 警告をシステムチャンネルへ委譲送信する。

        Args:
            warning_text: 警告の詳細テキスト。
        """
        n = self._get("system")
        if n:
            n.notify_ror_warning(warning_text)

    # ── ab_test チャンネル ───────────────────────────────────────────────────

    def send_ab_report(self, report_md: str) -> None:
        """V1/V2 週次 A/B 比較レポートを ab_test チャンネルへ送信する。

        テキストを _chunk_text で分割し、各チャンクを Markdown コードブロックで送信する。
        DISCORD_WEBHOOK_AB_TEST 未設定時は警告ログを出してスキップする。

        Args:
            report_md: Markdown 形式のレポート本文。
        """
        n = self._get("ab_test")
        if n is None:
            logger.warning("DISCORD_WEBHOOK_AB_TEST 未設定 — A/B レポート送信スキップ")
            return
        for chunk in _chunk_text(report_md):
            n.send_text(f"```markdown\n{chunk}\n```")

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
                f"【note下書き ({i}/{n_total})】\n```markdown\n{chunk}{footer}\n```"
            )
            notifier.send_text(message)

        post = x_post if x_post is not None else _generate_x_post(title, body)
        notifier.send_text(f"📢 **X（Twitter）告知ポスト案**\n```\n{post}\n```")

        logger.info(
            "[Discord:note下書き] 送信完了: %dチャンク + X告知ポスト1件",
            n_total,
        )
        return True
