"""
Discord Webhook ノーティファイア  — デュアルチャンネル対応版

環境変数:
  DISCORD_WEBHOOK_URL         : 買い目・結果・週次レポート用チャンネル
  DISCORD_SYSTEM_WEBHOOK_URL  : システムログ・エラー・稼働状態用チャンネル

送信先マトリクス
  予想チャンネル : notify_prerace_result / notify_hit_summary / send_text
  システムチャンネル: notify_scraping_alert / notify_intervention_required
                      notify_ror_warning / send_system_text / send_system_embed
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from .base import BaseNotifier, NotifyMessage
from .embed_builder import (
    COLOR_DEFAULT,
    build_axis_partner_fields,
    dynamic_color,
    infer_grade,
    stake_bar,
)
from src.utils.text import sanitize_str

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)

logger = logging.getLogger(__name__)

# ── embed カラー ────────────────────────────────────────────────────────────
_COLOR_NORMAL = 0x00C8FF  # シアン（通常）
_COLOR_BIG = 0xFFD700  # 金（高配当）
_COLOR_JACKPOT = 0xFF4500  # 赤橙（万馬券）
_COLOR_SYSTEM = 0x5865F2  # Discord Blurple（システム）
_COLOR_WARNING = 0xFF9800  # オレンジ（警告）
_COLOR_ERROR = 0xFF4444  # 赤（エラー）
_COLOR_OK = 0x43B581  # 緑（正常完了）

# 競馬場コード → 名称
_JYO: dict[str, str] = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}

# EV >= 1.0 のとき 🔥、それ以外は空白
_FIRE = "🔥"
_NONE = "　"  # 全角スペース（モノスペース揃え用）


def _format_race_label(race_id: str) -> str:
    """race_id を '東京 11R' 形式の表示文字列に変換する。

    Args:
        race_id: 12桁のレース ID（例: 202608031001）。

    Returns:
        '競馬場名 NNR' 形式の文字列（例: '東京 11R'）。
    """
    venue_code = race_id[4:6] if len(race_id) >= 6 else "??"
    venue = _JYO.get(venue_code, venue_code)
    race_num = str(int(race_id[10:12])) + "R" if len(race_id) >= 12 else race_id
    return f"{venue} {race_num}"


def _s(text: str) -> str:
    """制御文字を除去して Discord 送信安全な文字列を返す。

    Args:
        text: サニタイズ対象の文字列。

    Returns:
        制御文字を除去した文字列。
    """
    return sanitize_str(text)


class DiscordNotifier(BaseNotifier):
    """Discord Webhook を通じて通知を送る（デュアルチャンネル対応）。

    予想チャンネル (DISCORD_WEBHOOK_URL):
      send_text / notify_prerace_result / notify_hit_summary

    システムチャンネル (DISCORD_SYSTEM_WEBHOOK_URL):
      send_system_text / send_system_embed
      notify_scraping_alert / notify_intervention_required / notify_ror_warning
    """

    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        system_url: str | None = None,
        hit_flash_url: str | None = None,
        note_draft_url: str | None = None,
        enabled: bool = True,
        channel_label: str = "予想",
    ) -> None:
        """初期化。

        Args:
            webhook_url: 予想チャンネルの Webhook URL。
                         未指定時は DISCORD_WEBHOOK_URL 環境変数を使用。
            system_url: システムチャンネルの Webhook URL。
                        未指定時は DISCORD_SYSTEM_WEBHOOK_URL 環境変数を使用。
            hit_flash_url: 的中速報チャンネルの Webhook URL。
                           未指定時は DISCORD_WEBHOOK_HIT_FLASH 環境変数を使用。
            note_draft_url: note下書き / SNS 告知チャンネルの Webhook URL。
                            未指定時は DISCORD_WEBHOOK_NOTE_DRAFT 環境変数を使用。
            enabled: False に設定すると全送信をスキップする。
            channel_label: ログ出力に使用するチャンネルラベル。
        """
        super().__init__(enabled=enabled)
        self._url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")
        self._system_url = system_url or os.environ.get(
            "DISCORD_SYSTEM_WEBHOOK_URL", ""
        )
        self._hit_flash_url = hit_flash_url or os.environ.get(
            "DISCORD_WEBHOOK_HIT_FLASH", ""
        )
        self._note_draft_url = note_draft_url or os.environ.get(
            "DISCORD_WEBHOOK_NOTE_DRAFT", ""
        )
        self._label = channel_label
        if enabled and not self._url:
            logger.warning(
                "DISCORD_WEBHOOK_URL が設定されていません（予想通知が届きません）"
            )
        if enabled and not self._system_url:
            logger.warning(
                "DISCORD_SYSTEM_WEBHOOK_URL が設定されていません（システム通知は予想チャンネルへ fallback します）"
            )

    # ────────────────────────────────────────────────────────────────────────
    # 内部ヘルパー
    # ────────────────────────────────────────────────────────────────────────

    def _sanitize(self, s: str) -> str:
        """文字列を Discord 送信用にサニタイズする（null バイト除去・前後空白除去）。

        Args:
            s: サニタイズ対象の文字列。

        Returns:
            制御文字を除去した文字列。
        """
        return _s(s)

    def _post(
        self,
        url: str,
        payload: dict[str, Any],
        image_path: str | Path | None = None,
    ) -> bool:
        """指定 URL に payload を POST する。失敗しても例外を外に出さない。

        Args:
            url: Discord Webhook の URL。
            payload: 送信する JSON ペイロード。
            image_path: 添付する画像ファイルのパス（任意）。

        Returns:
            True = 送信成功 (HTTP 200/204), False = 失敗またはURL未設定。
        """
        if not url:
            return False
        try:
            if image_path and Path(image_path).exists():
                with open(image_path, "rb") as fp:
                    resp = requests.post(
                        url,
                        data={"payload_json": json.dumps(payload)},
                        files={"file": (Path(image_path).name, fp, "image/png")},
                        timeout=10,
                    )
            else:
                resp = requests.post(url, json=payload, timeout=10)
        except Exception as exc:
            logger.warning("[Discord] POST 例外: %s", exc)
            return False

        if resp.status_code in (200, 204):
            return True
        logger.warning(
            "[Discord] POST 失敗 status=%d: %s", resp.status_code, resp.text[:200]
        )
        return False

    def _sys_url(self) -> str:
        """システム通知先 URL を返す。SYSTEM_URL 未設定時は予想チャンネルへ fallback。

        Returns:
            システムチャンネル URL。未設定の場合は予想チャンネル URL。
        """
        return self._system_url or self._url

    # ────────────────────────────────────────────────────────────────────────
    # BaseNotifier 実装（予想チャンネル）
    # ────────────────────────────────────────────────────────────────────────

    def _send(self, message: NotifyMessage) -> bool:
        """BaseNotifier の抽象メソッド実装。予想チャンネルへ Embed を送信する。

        タイトルに「万馬券」「爆裂」が含まれる場合は赤橙、「高配当」は金色、
        それ以外はシアン色の Embed を送信する。

        Args:
            message: 送信するメッセージ。

        Returns:
            True = 送信成功, False = 失敗。
        """
        color = (
            _COLOR_JACKPOT
            if "万馬券" in message.title or "爆裂" in message.title
            else _COLOR_BIG
            if "高配当" in message.title
            else _COLOR_NORMAL
        )
        embed: dict[str, Any] = {
            "title": _s(message.title),
            "description": _s(message.body),
            "color": color,
        }
        if message.url:
            embed["url"] = message.url
        return self._post(self._url, {"embeds": [embed]}, message.image_path)

    # ────────────────────────────────────────────────────────────────────────
    # 予想チャンネル送信
    # ────────────────────────────────────────────────────────────────────────

    def send_text(self, text: str) -> None:
        """プレーンテキストを予想チャンネルへ送信する。

        ログラベルは channel_label に従う。
        DISCORD_WEBHOOK_URL 未設定時は警告ログを出してスキップする。

        Args:
            text: 送信するプレーンテキスト。
        """
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL 未設定のため送信スキップ")
            return
        self._post(self._url, {"content": _s(text)})
        logger.info("[Discord:%s] 送信: %s", self._label, text[:60])

    def send_prediction_embed(self, embeds: list[dict[str, Any]]) -> None:
        """予想チャンネルに生 embed リストを送信する（scheduler の週次サマリー用）。

        Args:
            embeds: Discord embed オブジェクトのリスト。
        """
        if not self._url:
            return
        self._post(self._url, {"embeds": embeds})

    def notify_gachi_x_post(
        self,
        race_id: str,
        x_text: str,
        note_path: str | None = None,
    ) -> bool:
        """厳選レースの X（Twitter）コピペ投稿テキストを Discord へ送信する。

        送信先は note下書き/SNS 告知チャンネル（DISCORD_WEBHOOK_NOTE_DRAFT）。
        未設定時は予想チャンネル（DISCORD_WEBHOOK_URL）へフォールバックする。
        コードブロックで囲み、そのままコピペして X に貼り付けられる形で流す。

        Args:
            race_id: 対象レース ID（ラベル表示に使用）。
            x_text: X 投稿用テキスト（ハッシュタグ含む）。
            note_path: 生成済み note Markdown のローカルパス（任意）。

        Returns:
            True = 送信成功, False = 失敗または URL 未設定。
        """
        url = self._note_draft_url or self._url
        if not url:
            logger.warning("Discord URL 未設定のため厳選X投稿スキップ: %s", race_id)
            return False

        label = _format_race_label(race_id)
        content = (
            f"📢 **【厳選レース】X投稿用コピペ — {label}**\n"
            f"下記をそのまま X に貼り付けられます👇\n"
            f"```\n{_s(x_text)}\n```"
        )
        if note_path:
            content += f"\n📝 note貼り付け用Markdown: `{note_path}`"

        ok = self._post(url, {"content": content})
        ch = "note下書き" if self._note_draft_url else "予想(fallback)"
        logger.info(
            "[Discord:%s] 厳選X投稿 %s: %s",
            ch,
            "送信完了" if ok else "送信失敗",
            race_id,
        )
        return ok

    # ────────────────────────────────────────────────────────────────────────
    # システムチャンネル送信
    # ────────────────────────────────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        """システムチャンネルにプレーンテキストを送信する。

        DISCORD_SYSTEM_WEBHOOK_URL 未設定時は予想チャンネルへ fallback する。

        Args:
            text: 送信するプレーンテキスト。
        """
        url = self._sys_url()
        if not url:
            logger.warning("Discord URL 未設定のためシステム通知スキップ")
            return
        self._post(url, {"content": _s(text)})
        logger.info("[Discord:システム] 送信: %s", text[:60])

    def send_system_embed(
        self,
        title: str,
        description: str,
        color: int = _COLOR_SYSTEM,
        fields: list[dict] | None = None,
        footer: str | None = None,
    ) -> None:
        """システムチャンネルに Embed を送信する。

        Args:
            title: Embed のタイトル。
            description: Embed の本文。
            color: Embed の左端カラー（16進数整数）。デフォルトは Discord Blurple。
            fields: 追加フィールドのリスト。各要素は ``name``/``value``/``inline`` キーを持つ dict。
            footer: Embed のフッターテキスト（任意）。
        """
        url = self._sys_url()
        if not url:
            return
        embed: dict[str, Any] = {
            "title": _s(title),
            "description": _s(description),
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if fields:
            embed["fields"] = [
                {
                    "name": _s(f["name"]),
                    "value": _s(f["value"]),
                    "inline": f.get("inline", False),
                }
                for f in fields
            ]
        if footer:
            embed["footer"] = {"text": _s(footer)}
        self._post(url, {"embeds": [embed]})

    # ────────────────────────────────────────────────────────────────────────
    # パイプライン固有: 予想見送り（ログのみ）
    # ────────────────────────────────────────────────────────────────────────

    def notify_skip(self, race_id: str, reason: str) -> None:
        """予想見送りをログに記録する（Discord への送信は行わない）。

        Args:
            race_id: 見送り対象のレース ID。
            reason: 見送り理由。
        """
        label = _format_race_label(race_id)
        logger.warning("[見送り] %s (%s): %s", label, race_id, reason)

    # ────────────────────────────────────────────────────────────────────────
    # パイプライン固有: システムチャンネル通知
    # ────────────────────────────────────────────────────────────────────────

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        """スクレイピング異常をシステムチャンネルに緊急通知する。

        Args:
            race_id: 異常が発生したレース ID。
            detail: 異常の詳細説明。
        """
        label = _format_race_label(race_id)
        logger.error("[スクレイピング異常] %s: %s", race_id, detail)
        self.send_system_embed(
            title=f"🚨 スクレイピング異常 — {label}",
            description=(
                f"**対象**: `{race_id}`\n"
                f"**詳細**: {detail}\n\n"
                f"→ netkeiba / JRA-VAN の HTML 構造変更を確認してください"
            ),
            color=_COLOR_ERROR,
        )

    def notify_intervention_required(
        self,
        step: str,
        error: str,
        action: str,
        screenshot_path: Path | None = None,
    ) -> None:
        """手動介入要請をシステムチャンネルに送信する。

        Args:
            step: 失敗したステップ名（例: 'エントリー取得'）。
            error: エラーの詳細文字列（400文字以内に切り詰め）。
            action: 推奨される対応アクション。
            screenshot_path: スクリーンショット画像のパス（任意）。
        """
        url = self._sys_url()
        if not url:
            logger.warning("Discord URL 未設定のため介入要請通知スキップ: %s", step)
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_body = _s(error[:400])
        payload = {
            "embeds": [
                {
                    "title": _s(f"⚠️ UMALOGI 手動介入要請 — {step}"),
                    "description": (
                        f"**失敗ステップ**: {_s(step)}\n\n"
                        f"**エラー内容**\n```\n{error_body}\n```\n\n"
                        f"**対応アクション**\n{_s(action)}\n\n"
                        f"**発生時刻**: {now}"
                    ),
                    "color": _COLOR_ERROR,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ]
        }
        image_path = (
            str(screenshot_path)
            if screenshot_path and Path(screenshot_path).exists()
            else None
        )
        ok = self._post(url, payload, image_path)
        logger.info(
            "[Discord:システム] 介入要請 %s: %s", "送信完了" if ok else "送信失敗", step
        )

    def notify_ror_warning(self, warning_text: str) -> None:
        """RoR（破産確率）警告をシステムチャンネルに送信する。

        Args:
            warning_text: 警告の詳細テキスト。
        """
        self.send_system_embed(
            title="⚠️ UMALOGI 資金管理警告",
            description=warning_text,
            color=_COLOR_WARNING,
        )
        logger.info("[Discord:システム] RoR 警告通知完了")

    # ────────────────────────────────────────────────────────────────────────
    # パイプライン固有: 予想チャンネル通知
    # ────────────────────────────────────────────────────────────────────────

    def notify_hit_summary(
        self,
        date_str: str,
        hit_count: int,
        total_count: int,
        cumulative_pnl: int,
        monthly_progress_pct: float,
    ) -> None:
        """的中サマリーを予想チャンネルに送信する。

        Args:
            date_str: 対象日付の文字列（例: '2026-05-25'）。
            hit_count: 当日の的中件数。
            total_count: 当日の全買い目件数。
            cumulative_pnl: 累積損益（円）。
            monthly_progress_pct: 月次目標進捗率（%）。
        """
        if not self._url:
            return
        color = _COLOR_BIG if hit_count > 0 else _COLOR_NORMAL
        pnl_sign = "+" if cumulative_pnl >= 0 else ""
        hit_label = (
            f"✅ 的中 {hit_count}/{total_count}"
            if hit_count > 0
            else f"❌ 的中なし (0/{total_count})"
        )
        payload = {
            "embeds": [
                {
                    "title": f"🏇 {date_str} レース結果",
                    "description": _s(
                        f"**{hit_label}**\n"
                        f"累積P&L: {pnl_sign}¥{cumulative_pnl:,}\n"
                        f"月次進捗: {monthly_progress_pct:.1f}%"
                    ),
                    "color": color,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ]
        }
        hit_url = self._hit_flash_url or self._url
        ok = self._post(hit_url, payload)
        ch_label = "的中速報" if self._hit_flash_url else "予想(fallback)"
        logger.info(
            "[Discord:%s] 結果サマリー %s: %s",
            ch_label,
            "送信完了" if ok else "失敗",
            date_str,
        )

    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: object,
        manji_bets: object,
        oracle_bets: object | None = None,
        hit_focus_bets: object | None = None,
        alpha_bets: object | None = None,
        dashboard_url: str = "",
        race_name: str = "",
        confidence: float | None = None,
        bankroll: float | None = None,
    ) -> None:
        """直前予想を「🟦 ALPHA / 🟩 卍 / 🟥 本命」の3セクション分離 Embed で送信する。

        各モデルは完全独立セクションとして表示。馬番+馬名を必ず明示。
        スマホでも一行ずつ読めるカード形式。文字数超過時は自動折りたたみ。
        全モデルで EV <= 0 の場合は送信をスキップする。

        プレミアム装飾（embed_builder 連携）:
          - Embed カラーは「万馬券級 EV > 格付け(G1=青/G2=赤/G3=緑) > 自信度
            グラデーション」の優先順で動的決定。
          - 最高 EV の買い目を「軸馬｜相手馬｜EV/想定オッズ」の 3 カラム
            グリッドでトップに表示。
          - EV>=1.0 の推奨投資合計を ████░░░░░░ 40% のインジケーターで表現。

        Args:
            race_id: 対象レースの ID（12桁）。
            honmei_bets: 本命モデルの RaceBets オブジェクト。
            manji_bets: 卍モデルの RaceBets オブジェクト。
            oracle_bets: Oracle（的中確率最大）の RaceBets オブジェクト（任意）。
            hit_focus_bets: HitFocus（2軸マルチ）の RaceBets オブジェクト（任意）。
            alpha_bets: ALPHA モデルの RaceBets オブジェクト（任意）。
            dashboard_url: ダッシュボードの URL（フッターに付与）。
            race_name: レース名（格付けカラー推定に使用・任意）。
            confidence: モデル自信度 0.0〜1.0（カラーグラデーションに使用・任意）。
            bankroll: 投資比率バーの分母となるバンクロール（円）。
                      未指定時は UMALOGI_BANKROLL 環境変数 → 100,000 円。
        """
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL 未設定のため通知スキップ: %s", race_id)
            return

        label = _format_race_label(race_id)

        # ── 全モデル最大EV と投資合計を集計 ────────────────────────────────
        all_bets_flat: list[object] = []
        for rb in [alpha_bets, manji_bets, honmei_bets, oracle_bets, hit_focus_bets]:
            if rb is not None:
                all_bets_flat.extend(getattr(rb, "bets", []))

        if not any(getattr(b, "expected_value", 0) > 0 for b in all_bets_flat):
            logger.info("全 EV <= 0 — Discord 通知スキップ: %s", race_id)
            return

        max_ev = max(
            (getattr(b, "expected_value", 0.0) for b in all_bets_flat), default=0.0
        )
        # カラー優先順: 万馬券級EV > 格付け > 自信度。どれも無ければ従来の EV 閾値。
        grade = infer_grade(race_name)
        color = dynamic_color(grade=grade, confidence=confidence, max_ev=max_ev)
        if color == COLOR_DEFAULT and confidence is None:
            color = (
                _COLOR_JACKPOT
                if max_ev >= 3.0
                else _COLOR_BIG
                if max_ev >= 1.5
                else _COLOR_NORMAL
            )
        total_invest = sum(
            getattr(b, "recommended_bet", 0) or 0
            for b in all_bets_flat
            if getattr(b, "expected_value", 0) >= 1.0
        )

        # ── セクション定義: (アイコン, ラベル, カラー名, RaceBets, 最大表示件数) ──
        sections: list[tuple[str, str, object | None, int]] = [
            ("🟦", "ALPHA 予想  (期待値特化)", alpha_bets, 3),
            ("🟩", "卍 予想  (回収率特化)", manji_bets, 3),
            ("🟥", "本命 予想  (勝率特化)", honmei_bets, 3),
        ]
        if oracle_bets is not None and getattr(oracle_bets, "bets", []):
            sections.append(("🟨", "Oracle 予想  (的中確率最大)", oracle_bets, 2))
        if hit_focus_bets is not None and getattr(hit_focus_bets, "bets", []):
            sections.append(("🔶", "HitFocus 予想  (2軸マルチ)", hit_focus_bets, 2))

        fields: list[dict[str, Any]] = []

        # ── ベストシグナル: 最高 EV の買い目を軸/相手/EV の 3 カラムグリッドで先頭表示 ──
        best_bet = max(
            (b for b in all_bets_flat if getattr(b, "expected_value", 0.0) >= 1.0),
            key=lambda b: getattr(b, "expected_value", 0.0),
            default=None,
        )
        if best_bet is not None:
            axis, partners = _extract_axis_partners(best_bet)
            if axis or partners:
                odds_val = getattr(best_bet, "odds", None)
                odds_f = float(odds_val) if isinstance(odds_val, (int, float)) else None
                fields.append(
                    {
                        "name": _s(
                            f"⚡ ベストシグナル — {getattr(best_bet, 'bet_type', '?')}"
                        ),
                        "value": "​",
                        "inline": False,
                    }
                )
                fields.extend(
                    build_axis_partner_fields(
                        axis,
                        partners,
                        float(getattr(best_bet, "expected_value", 0.0)),
                        odds_f,
                    )
                )

        for icon, section_label, rb, max_bets in sections:
            if rb is None:
                continue
            bets = sorted(
                getattr(rb, "bets", []),
                key=lambda b: getattr(b, "expected_value", 0.0),
                reverse=True,
            )
            if not bets:
                continue

            # セクションヘッダー（区切り線）
            fields.append(
                {
                    "name": f"{icon} **__{section_label}__**",
                    "value": "​",  # zero-width space（空フィールドにしない）
                    "inline": False,
                }
            )

            for bet in bets[:max_bets]:
                ev = getattr(bet, "expected_value", 0.0)
                bet_type = getattr(bet, "bet_type", "?")
                rec_bet = int(getattr(bet, "recommended_bet", 0) or 0)
                fire = _FIRE if ev >= 1.0 else "　"
                combos = getattr(bet, "combinations", []) or []
                n_combos = len(combos)
                cost_str = (
                    f"¥100×{n_combos}点=¥{n_combos * 100:,}"
                    if n_combos > 0
                    else f"¥{rec_bet:,}"
                )

                # 本命×三連単 (EV≥1.5で条件付き許可) は識別マークを付与
                model_type = getattr(rb, "model_type", "")
                cond_badge = (
                    " ⚡条件付"
                    if ("本命" in model_type and bet_type == "三連単" and ev >= 1.5)
                    else ""
                )

                # フィールド name: "🔥 三連複  EV=2.13 | ¥100×4点=¥400"
                field_name = f"{fire} {bet_type}{cond_badge}  EV={ev:.2f} | {cost_str}"

                # フィールド value: 馬番+馬名 カード形式
                field_value = _format_combo_card(bet)

                fields.append(
                    {
                        "name": _s(field_name[:256]),
                        "value": _s(field_value[:1024]),
                        "inline": False,
                    }
                )

            if len(fields) >= 20:  # Discord 上限 25、グリッド+投資バー込みで余裕を持つ
                break

        if not fields:
            logger.info("有効フィールドなし — Discord 通知スキップ: %s", race_id)
            return

        # ── 推奨投資比率インジケーター（bankroll_manager 連携）──────────────
        bank = (
            float(bankroll)
            if bankroll and bankroll > 0
            else float(os.environ.get("UMALOGI_BANKROLL", "") or 100_000)
        )
        fields.append(
            {
                "name": "💰 推奨投資比率（バンクロール比）",
                "value": (
                    f"`{stake_bar(total_invest / bank)}`\n"
                    f"投資 ¥{int(total_invest):,} ／ 資金 ¥{int(bank):,}"
                ),
                "inline": False,
            }
        )

        invest_str = f"¥{int(total_invest):,}" if total_invest > 0 else "なし"
        footer_text = f"🤖 AIウマスギフィルター適用済み | EV≥1.0 推奨投資 {invest_str}"
        if dashboard_url:
            footer_text += f" | 詳細 → {dashboard_url}"

        grade_badge = f" 〔{grade}〕" if grade else ""
        title_race_name = f"\n{race_name}" if race_name else ""
        embed: dict[str, Any] = {
            "title": _s(f"🏇  {label}{grade_badge}  直前予想"),
            "description": _s(
                f"{title_race_name.strip()}\n"
                f"最高EV: **{max_ev:.2f}**  |  モデル3系統独立稼働  |  🤖 ROIフィルター適用済み"
            ).strip(),
            "color": color,
            "fields": fields,
            "footer": {"text": footer_text},
            "timestamp": datetime.utcnow().isoformat(),
        }

        ok = self._post(self._url, {"embeds": [embed]})
        logger.info(
            "[Discord:予想] 直前予想 %s: %s  (sections=%d  fields=%d  max_ev=%.2f)",
            "送信完了" if ok else "送信失敗",
            race_id,
            len(sections),
            len(fields),
            max_ev,
        )


# ────────────────────────────────────────────────────────────────────────────
# 組み合わせカード形式フォーマッター（スマホ対応・馬番+馬名必須）
# ────────────────────────────────────────────────────────────────────────────


def _extract_axis_partners(
    bet: object,
) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    """買い目から (軸馬リスト, 相手馬リスト) を抽出する。

    全組み合わせに含まれる馬番を「軸」、それ以外を「相手」と判定する
    （_format_combo_card と同一のロジック）。馬名は先頭組み合わせの脚順と
    horse_names の対応から逆引きする。

    Args:
        bet: RaceBet オブジェクト（combinations / horse_names 属性を持つ）。

    Returns:
        ([(馬番, 馬名), ...], [(馬番, 馬名), ...]) のタプル。買い目なしは空リスト。
    """
    combos = getattr(bet, "combinations", []) or []
    names = getattr(bet, "horse_names", []) or []
    if not combos:
        return [], []

    try:
        combo_lists = [
            [int(n) for n in (c if isinstance(c, (list, tuple)) else [c])]
            for c in combos
        ]
    except (TypeError, ValueError):
        return [], []
    if not combo_lists or not combo_lists[0]:
        return [], []
    name_by_num: dict[int, str] = {}
    for i, leg in enumerate(combo_lists[0]):
        if i < len(names) and names[i]:
            name_by_num[leg] = str(names[i])

    all_nums = sorted({n for c in combo_lists for n in c})
    axis_nums = [n for n in all_nums if all(n in c for c in combo_lists)]
    partner_nums = [n for n in all_nums if n not in axis_nums]
    return (
        [(n, name_by_num.get(n, "")) for n in axis_nums],
        [(n, name_by_num.get(n, "")) for n in partner_nums],
    )


def _format_combo_card(bet: object) -> str:
    """買い目をスマホ対応カード形式にフォーマットする。馬番を省略せず全表示。

    券種に応じて以下の形式で出力する:
    - 単勝/複勝: 「⬛ N番 馬名」の行リスト
    - 三連単/馬単: 軸マルチ/1着固定/ベタ展開を自動判定
    - 馬連/ワイド/三連複: 軸流し/ボックスを自動判定

    出力例（三連複 軸1頭流し）::

        【推奨: 三連複流し 軸5 - 相手3,7,9,12】
        ▶ 軸: 5番 アーバンシック
          相手: 3番 / 7番 / 9番 / 12番
          計4点

    Args:
        bet: RaceBet オブジェクト（bet_type / combinations / horse_names 属性を持つ）。

    Returns:
        Discord field.value として使用できる書式化済み文字列（最大 ~900 文字）。
    """
    from collections import Counter

    _BUDGET = 900  # Discord field.value 上限 1024 に余裕を持たせたバジェット

    bt: str = getattr(bet, "bet_type", "")
    combos: list = getattr(bet, "combinations", []) or []
    names: list = getattr(bet, "horse_names", []) or []

    if not combos:
        return "　(買い目なし)"

    n_total = len(combos)

    # horse_number → 馬名 逆引きマップ
    name_by_num: dict[int, str] = {}

    if bt in ("単勝", "複勝"):
        for i, combo in enumerate(combos):
            num = combo[0] if isinstance(combo, (list, tuple)) else combo
            if i < len(names) and names[i]:
                name_by_num[int(num)] = str(names[i])
    else:
        first = combos[0]
        first_legs = list(first) if isinstance(first, (list, tuple)) else [first]
        for i, leg in enumerate(first_legs):
            if i < len(names) and names[i]:
                name_by_num[int(leg)] = str(names[i])

    def _label(num: int) -> str:
        n = name_by_num.get(int(num), "")
        return f"{num}番 {n}" if n else f"{num}番"

    def _fit(lines: list[str], budget: int = _BUDGET) -> str:
        """行リストを budget 文字以内に収める。超える場合は末尾に省略行を追記。"""
        out: list[str] = []
        used = 0
        for line in lines:
            if used + len(line) + 1 > budget:
                remaining = n_total - len(out)
                if remaining > 0:
                    out.append(f"  …(残り{remaining}点 省略)")
                break
            out.append(line)
            used += len(line) + 1
        return "\n".join(out)

    # ── 単勝・複勝 ──────────────────────────────────────────────────────────
    if bt in ("単勝", "複勝"):
        nums = [c[0] if isinstance(c, (list, tuple)) else c for c in combos]
        lines = [f"⬛ {_label(n)}" for n in nums]
        return _fit(lines)

    # ── 馬単・三連単（順序あり）: マルチ・軸固定を自動判定 ────────────────
    if bt in ("馬単", "三連単"):
        all_nums = sorted({int(n) for combo in combos for n in combo})
        # マルチ判定: 軸馬が位置不問ですべての組み合わせに含まれる
        always_in = [n for n in all_nums if all(n in list(c) for c in combos)]
        if always_in:
            others = [n for n in all_nums if n not in always_in]
            axis_str = ",".join(str(a) for a in always_in)
            opp_str = ",".join(str(o) for o in others)
            smart = f"【推奨: {bt}マルチ 軸{axis_str} - 相手{opp_str}】\n計{n_total}点"
            if len(smart) <= _BUDGET:
                return smart
        # 1着固定判定
        firsts = sorted(
            {int(c[0]) for c in combos if isinstance(c, (list, tuple)) and c}
        )
        if len(firsts) <= 2:
            others = [n for n in all_nums if n not in firsts]
            axis_str = ",".join(str(f) for f in firsts)
            opp_str = ",".join(str(o) for o in others)
            smart = f"【推奨: {bt} 軸{axis_str}(1着) - 相手{opp_str}】\n計{n_total}点"
            if len(smart) <= _BUDGET:
                return smart
        # ベタ展開（上記に該当しないフォーメーション）
        lines = []
        for combo in combos:
            legs = list(combo) if isinstance(combo, (list, tuple)) else [combo]
            lines.append(f"▶ {'→'.join(str(n) for n in legs)}")
        return _fit(lines)

    # ── 馬連・ワイド・三連複（軸流し or ボックス）──────────────────────────
    first = combos[0]
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        flat = [int(n) for combo in combos for n in combo]
        cnt = Counter(flat)
        axis_set = {num for num, c in cnt.items() if c == n_total}

        if axis_set:
            # 軸あり: 全相手馬を省略なしで表示
            axes = sorted(axis_set)
            others = sorted({int(n) for combo in combos for n in combo} - axis_set)
            axis_str = ",".join(str(a) for a in axes)
            axis_label = " / ".join(_label(a) for a in axes[:2])
            if others:
                opp_nums = ",".join(str(o) for o in others)
                opp_label = " / ".join(_label(o) for o in others)
                smart_str = f"【推奨: {bt}流し 軸{axis_str} - 相手{opp_nums}】"
                detail = f"▶ 軸: {axis_label}\n  相手: {opp_label}\n  計{n_total}点"
            else:
                # 全馬が軸 = 組み合わせ1通り
                smart_str = f"【推奨: {bt} {axis_str}番】"
                detail = f"▶ {axis_label}\n計{n_total}点"
            result = smart_str + "\n" + detail
            if len(result) <= _BUDGET:
                return result
            # 超える場合は番号のみのコンパクト表記
            return f"{smart_str}\n計{n_total}点"
        else:
            # ボックス: 全馬番を省略なしで表示
            nums_all = sorted({int(n) for combo in combos for n in combo})
            all_labels = " / ".join(_label(n) for n in nums_all)
            box_str = f"ボックス: {all_labels}\n計{n_total}点"
            if len(box_str) <= _BUDGET:
                return box_str
            # 超える場合は番号のみ
            nums_str = ",".join(str(n) for n in nums_all)
            return f"ボックス: {nums_str}\n計{n_total}点"

    return f"計{n_total}点"


def _summarize_combos(bet: object) -> str:
    """後方互換エイリアス: _format_combo_card に委譲する。

    Args:
        bet: RaceBet オブジェクト。

    Returns:
        _format_combo_card の戻り値と同一。
    """
    return _format_combo_card(bet)
