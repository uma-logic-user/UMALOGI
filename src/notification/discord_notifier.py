"""
Discord Webhook ノーティファイア

環境変数:
  DISCORD_WEBHOOK_URL  : Discord Incoming Webhook URL

embed を使った見やすいフォーマットで送信する。
画像がある場合は multipart/form-data でアップロード。
パイプライン固有の通知（見送り・スクレイピング異常・直前予想まとめ）も提供する。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from .base import BaseNotifier, NotifyMessage
from src.utils.text import sanitize_str

# プロジェクトルートの .env を読み込む
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)

logger = logging.getLogger(__name__)

# embed カラー（的中レベルに応じて変える）
_COLOR_NORMAL  = 0x00FF88   # 緑
_COLOR_BIG     = 0xFFD700   # 金
_COLOR_JACKPOT = 0xFF4500   # 赤金

# 競馬場コード → 名称
_JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


def _format_race_label(race_id: str) -> str:
    """race_id から "東京 11R" のような表示文字列を生成する。"""
    venue_code = race_id[4:6] if len(race_id) >= 6 else "??"
    venue = _JYO.get(venue_code, venue_code)
    race_num = str(int(race_id[10:12])) + "R" if len(race_id) >= 12 else race_id
    return f"{venue} {race_num}"


class DiscordNotifier(BaseNotifier):
    """Discord Webhook を通じて通知を送る。

    基本通知 (send_text / _send) に加え、パイプライン固有の高レベルメソッドを提供する:
      - notify_skip()             : 予想見送り通知
      - notify_scraping_alert()   : スクレイピング異常の緊急通知
      - notify_prerace_result()   : 直前予想の全券種まとめ Embed 通知
    """

    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")
        if enabled and not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません")

    @staticmethod
    def _sanitize(s: str) -> str:
        """制御文字を全除去してDiscord送信安全な文字列に変換する。"""
        return sanitize_str(s)

    # ------------------------------------------------------------------ #
    # BaseNotifier 実装
    # ------------------------------------------------------------------ #

    def _send(self, message: NotifyMessage) -> bool:
        if not self._url:
            return False

        color = _COLOR_JACKPOT if "万馬券" in message.title or "爆裂" in message.title \
                else _COLOR_BIG if "高配当" in message.title \
                else _COLOR_NORMAL

        embed: dict = {
            "title":       self._sanitize(message.title),
            "description": self._sanitize(message.body),
            "color": color,
        }
        if message.url:
            embed["url"] = message.url

        payload = {"embeds": [embed]}

        try:
            if message.image_path and Path(message.image_path).exists():
                with open(message.image_path, "rb") as fp:
                    resp = requests.post(
                        self._url,
                        data={"payload_json": json.dumps(payload)},
                        files={"file": (Path(message.image_path).name, fp, "image/png")},
                        timeout=10,
                    )
            else:
                resp = requests.post(self._url, json=payload, timeout=10)
        except Exception as exc:
            logger.warning("[Discord] 送信例外: %s", exc)
            return False

        if resp.status_code in (200, 204):
            logger.info("[Discord] 送信成功: %s", message.title)
            return True
        logger.warning("[Discord] 送信失敗 status=%d: %s", resp.status_code, resp.text[:200])
        return False

    # ------------------------------------------------------------------ #
    # テキスト送信（シンプルなプレーンテキスト用）
    # ------------------------------------------------------------------ #

    def send_text(self, text: str) -> None:
        """プレーンテキストを Discord に送信する。URL 未設定時はログ警告のみ。"""
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が未設定のため Discord 通知をスキップします")
            return
        try:
            resp = requests.post(
                self._url,
                json={"content": self._sanitize(text)},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("Discord 送信完了: HTTP %d", resp.status_code)
        except Exception as exc:
            logger.warning("Discord 送信失敗: %s", exc)

    # ------------------------------------------------------------------ #
    # パイプライン固有の通知メソッド
    # ------------------------------------------------------------------ #

    def notify_skip(self, race_id: str, reason: str) -> None:
        """予想見送りを Discord に通知する。"""
        label = _format_race_label(race_id)
        text = f"[見送り] {label} (`{race_id}`) データ不足: {reason}"
        logger.warning("[見送り] %s: %s", race_id, reason)
        self.send_text(text)

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        """スクレイピング異常（0頭取得・全オッズ NaN 等）を Discord に緊急通知する。"""
        label = _format_race_label(race_id)
        text = (
            f"🚨【緊急】スクレイピング仕様変更の可能性\n"
            f"対象: {label} (`{race_id}`)\n"
            f"詳細: {detail}\n"
            f"→ netkeiba / JRA-VAN の HTML 構造変更を確認してください"
        )
        logger.error("[スクレイピング異常] %s: %s", race_id, detail)
        self.send_text(text)

    def notify_prerace_result(
        self,
        race_id: str,
        honmei_bets: object,
        manji_bets: object,
    ) -> None:
        """直前予想の全券種買い目を 1 つの Embed にまとめて送信する。

        EV >= 1.0 の買い目には 🔥、それ以外は空白で区別する。
        全 EV <= 0 の場合は送信しない。
        """
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL 未設定のため通知スキップ: %s", race_id)
            return

        label = _format_race_label(race_id)

        def _combo_str(bet: object) -> str:
            bt     = bet.bet_type       # type: ignore[attr-defined]
            combos = bet.combinations   # type: ignore[attr-defined]
            names  = bet.horse_names    # type: ignore[attr-defined]
            if not combos:
                return "—"
            first = combos[0]
            if bt in ("馬単", "三連単"):
                nums     = " → ".join(str(n) for n in first)
                name_str = " / ".join(names[:len(first)]) if names else ""
            else:
                if names and len(names) >= len(first):
                    paired   = sorted(zip(first, names[:len(first)]), key=lambda x: x[0])
                    nums     = " - ".join(str(p[0]) for p in paired)
                    name_str = " / ".join(p[1] for p in paired)
                else:
                    nums     = " - ".join(str(n) for n in sorted(first))
                    name_str = " / ".join(names[:3]) if names else ""
            suffix = f"（+{len(combos)-1}組）" if len(combos) > 1 else ""
            return f"{nums}{suffix}  ({name_str})"

        def _build_section(race_bets: object) -> str:
            bets = sorted(
                race_bets.bets,  # type: ignore[attr-defined]
                key=lambda b: b.expected_value,
                reverse=True,
            )
            if not bets:
                return "  (推奨なし)\n"
            lines = []
            for b in bets:
                ev    = b.expected_value
                flag  = "🔥" if ev >= 1.0 else "  "
                bet_y = f"¥{int(b.recommended_bet):,}" if b.recommended_bet else "—"
                combo = _combo_str(b)
                lines.append(f"{flag} **{b.bet_type}** EV={ev:.2f}  {bet_y}\n    └ {combo}")
            return "\n".join(lines)

        all_bets = list(honmei_bets.bets) + list(manji_bets.bets)  # type: ignore[attr-defined]
        if not any(b.expected_value > 0 for b in all_bets):
            logger.info("全 EV <= 0 のため Discord 通知をスキップ: %s", race_id)
            return

        total_bet = sum(
            b.recommended_bet for b in all_bets
            if b.expected_value >= 1.0 and b.recommended_bet
        )
        total_str = f"¥{int(total_bet):,}" if total_bet > 0 else "なし"

        max_ev = max((b.expected_value for b in all_bets), default=0.0)
        color  = _COLOR_JACKPOT if max_ev >= 3.0 else (_COLOR_BIG if max_ev >= 1.5 else _COLOR_NORMAL)

        description = (
            f"**本命モデル**\n{_build_section(honmei_bets)}\n\n"
            f"**卍モデル**\n{_build_section(manji_bets)}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"EV >= 1.0 推奨合計: {total_str}"
        )
        if len(description) > 4000:
            description = description[:3997] + "..."

        payload = {
            "embeds": [{
                "title":       self._sanitize(f"🏇 {label}  直前予想  (`{race_id}`)"),
                "description": self._sanitize(description),
                "color": color,
            }]
        }
        try:
            resp = requests.post(self._url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Discord 直前予想通知 送信完了: %s HTTP %d", race_id, resp.status_code)
        except Exception as exc:
            logger.warning("Discord 直前予想通知 送信失敗: %s", exc)
