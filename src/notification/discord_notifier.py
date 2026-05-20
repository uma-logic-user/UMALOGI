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
from src.utils.text import sanitize_str

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)

logger = logging.getLogger(__name__)

# ── embed カラー ────────────────────────────────────────────────────────────
_COLOR_NORMAL   = 0x00C8FF   # シアン（通常）
_COLOR_BIG      = 0xFFD700   # 金（高配当）
_COLOR_JACKPOT  = 0xFF4500   # 赤橙（万馬券）
_COLOR_SYSTEM   = 0x5865F2   # Discord Blurple（システム）
_COLOR_WARNING  = 0xFF9800   # オレンジ（警告）
_COLOR_ERROR    = 0xFF4444   # 赤（エラー）
_COLOR_OK       = 0x43B581   # 緑（正常完了）

# 競馬場コード → 名称
_JYO: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}

# EV >= 1.0 のとき 🔥、それ以外は空白
_FIRE = "🔥"
_NONE = "　"  # 全角スペース（モノスペース揃え用）


def _format_race_label(race_id: str) -> str:
    """race_id → '東京 11R' 形式の表示文字列。"""
    venue_code = race_id[4:6] if len(race_id) >= 6 else "??"
    venue = _JYO.get(venue_code, venue_code)
    race_num = str(int(race_id[10:12])) + "R" if len(race_id) >= 12 else race_id
    return f"{venue} {race_num}"


def _s(text: str) -> str:
    """制御文字を除去して Discord 送信安全な文字列を返す。"""
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
        webhook_url:    str | None = None,
        system_url:     str | None = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._url        = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")
        self._system_url = system_url  or os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "")
        if enabled and not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません（予想通知が届きません）")
        if enabled and not self._system_url:
            logger.warning("DISCORD_SYSTEM_WEBHOOK_URL が設定されていません（システム通知は予想チャンネルへ fallback します）")

    # ────────────────────────────────────────────────────────────────────────
    # 内部ヘルパー
    # ────────────────────────────────────────────────────────────────────────

    def _sanitize(self, s: str) -> str:
        """文字列を Discord 送信用にサニタイズする（null バイト除去・前後空白除去）。"""
        return _s(s)

    def _post(self, url: str, payload: dict[str, Any], image_path: str | None = None) -> bool:
        """指定 URL に payload を POST する。失敗しても例外を外に出さない。"""
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
        logger.warning("[Discord] POST 失敗 status=%d: %s", resp.status_code, resp.text[:200])
        return False

    def _sys_url(self) -> str:
        """システム通知先 URL。SYSTEM_URL 未設定時は予想チャンネルへ fallback。"""
        return self._system_url or self._url

    # ────────────────────────────────────────────────────────────────────────
    # BaseNotifier 実装（予想チャンネル）
    # ────────────────────────────────────────────────────────────────────────

    def _send(self, message: NotifyMessage) -> bool:
        color = (
            _COLOR_JACKPOT if "万馬券" in message.title or "爆裂" in message.title
            else _COLOR_BIG if "高配当" in message.title
            else _COLOR_NORMAL
        )
        embed: dict[str, Any] = {
            "title":       _s(message.title),
            "description": _s(message.body),
            "color":       color,
        }
        if message.url:
            embed["url"] = message.url
        return self._post(self._url, {"embeds": [embed]}, message.image_path)

    # ────────────────────────────────────────────────────────────────────────
    # 予想チャンネル送信
    # ────────────────────────────────────────────────────────────────────────

    def send_text(self, text: str) -> None:
        """予想チャンネルにプレーンテキストを送信する。"""
        if not self._url:
            logger.warning("DISCORD_WEBHOOK_URL 未設定のため送信スキップ")
            return
        self._post(self._url, {"content": _s(text)})
        logger.info("[Discord:予想] 送信: %s", text[:60])

    def send_prediction_embed(self, embeds: list[dict[str, Any]]) -> None:
        """予想チャンネルに生 embed リストを送信する（scheduler の週次サマリー用）。"""
        if not self._url:
            return
        self._post(self._url, {"embeds": embeds})

    # ────────────────────────────────────────────────────────────────────────
    # システムチャンネル送信
    # ────────────────────────────────────────────────────────────────────────

    def send_system_text(self, text: str) -> None:
        """システムチャンネルにプレーンテキストを送信する。"""
        url = self._sys_url()
        if not url:
            logger.warning("Discord URL 未設定のためシステム通知スキップ")
            return
        self._post(url, {"content": _s(text)})
        logger.info("[Discord:システム] 送信: %s", text[:60])

    def send_system_embed(
        self,
        title:       str,
        description: str,
        color:       int       = _COLOR_SYSTEM,
        fields:      list[dict] | None = None,
        footer:      str | None = None,
    ) -> None:
        """システムチャンネルに Embed を送信する。"""
        url = self._sys_url()
        if not url:
            return
        embed: dict[str, Any] = {
            "title":       _s(title),
            "description": _s(description),
            "color":       color,
            "timestamp":   datetime.utcnow().isoformat(),
        }
        if fields:
            embed["fields"] = [
                {"name": _s(f["name"]), "value": _s(f["value"]), "inline": f.get("inline", False)}
                for f in fields
            ]
        if footer:
            embed["footer"] = {"text": _s(footer)}
        self._post(url, {"embeds": [embed]})

    # ────────────────────────────────────────────────────────────────────────
    # パイプライン固有: 予想見送り（ログのみ）
    # ────────────────────────────────────────────────────────────────────────

    def notify_skip(self, race_id: str, reason: str) -> None:
        label = _format_race_label(race_id)
        logger.warning("[見送り] %s (%s): %s", label, race_id, reason)

    # ────────────────────────────────────────────────────────────────────────
    # パイプライン固有: システムチャンネル通知
    # ────────────────────────────────────────────────────────────────────────

    def notify_scraping_alert(self, race_id: str, detail: str) -> None:
        """スクレイピング異常をシステムチャンネルに緊急通知する。"""
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
        step:            str,
        error:           str,
        action:          str,
        screenshot_path: Path | None = None,
    ) -> None:
        """手動介入要請をシステムチャンネルに送信する。"""
        url = self._sys_url()
        if not url:
            logger.warning("Discord URL 未設定のため介入要請通知スキップ: %s", step)
            return

        now        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_body = _s(error[:400])
        payload = {
            "embeds": [{
                "title":       _s(f"⚠️ UMALOGI 手動介入要請 — {step}"),
                "description": (
                    f"**失敗ステップ**: {_s(step)}\n\n"
                    f"**エラー内容**\n```\n{error_body}\n```\n\n"
                    f"**対応アクション**\n{_s(action)}\n\n"
                    f"**発生時刻**: {now}"
                ),
                "color":     _COLOR_ERROR,
                "timestamp": datetime.utcnow().isoformat(),
            }]
        }
        image_path = str(screenshot_path) if screenshot_path and Path(screenshot_path).exists() else None
        ok = self._post(url, payload, image_path)
        logger.info("[Discord:システム] 介入要請 %s: %s", "送信完了" if ok else "送信失敗", step)

    def notify_ror_warning(self, warning_text: str) -> None:
        """RoR 警告をシステムチャンネルに送信する。"""
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
        date_str:             str,
        hit_count:            int,
        total_count:          int,
        cumulative_pnl:       int,
        monthly_progress_pct: float,
    ) -> None:
        """的中サマリーを予想チャンネルに送信する。"""
        if not self._url:
            return
        color    = _COLOR_BIG if hit_count > 0 else _COLOR_NORMAL
        pnl_sign = "+" if cumulative_pnl >= 0 else ""
        hit_label = (
            f"✅ 的中 {hit_count}/{total_count}"
            if hit_count > 0
            else f"❌ 的中なし (0/{total_count})"
        )
        payload = {
            "embeds": [{
                "title": f"🏇 {date_str} レース結果",
                "description": _s(
                    f"**{hit_label}**\n"
                    f"累積P&L: {pnl_sign}¥{cumulative_pnl:,}\n"
                    f"月次進捗: {monthly_progress_pct:.1f}%"
                ),
                "color":     color,
                "timestamp": datetime.utcnow().isoformat(),
            }]
        }
        ok = self._post(self._url, payload)
        logger.info("[Discord:予想] 結果サマリー %s: %s", "送信完了" if ok else "失敗", date_str)

    def notify_prerace_result(
        self,
        race_id:        str,
        honmei_bets:    object,
        manji_bets:     object,
        oracle_bets:    object | None = None,
        hit_focus_bets: object | None = None,
        alpha_bets:     object | None = None,
        dashboard_url:  str = "",
    ) -> None:
        """
        直前予想を「🟦 ALPHA / 🟩 卍 / 🟥 本命」の3セクション分離 Embed で送信する。

        各モデルは完全独立セクションとして表示。馬番+馬名を必ず明示。
        スマホでも一行ずつ読めるカード形式。文字数超過時は自動折りたたみ。
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

        max_ev = max((getattr(b, "expected_value", 0.0) for b in all_bets_flat), default=0.0)
        color  = (
            _COLOR_JACKPOT if max_ev >= 3.0
            else _COLOR_BIG   if max_ev >= 1.5
            else _COLOR_NORMAL
        )
        total_invest = sum(
            getattr(b, "recommended_bet", 0) or 0
            for b in all_bets_flat
            if getattr(b, "expected_value", 0) >= 1.0
        )

        # ── セクション定義: (アイコン, ラベル, カラー名, RaceBets, 最大表示件数) ──
        sections: list[tuple[str, str, object | None, int]] = [
            ("🟦", "ALPHA 予想  (期待値特化)",  alpha_bets,    3),
            ("🟩", "卍 予想  (回収率特化)",      manji_bets,    3),
            ("🟥", "本命 予想  (勝率特化)",      honmei_bets,   3),
        ]
        if oracle_bets is not None and getattr(oracle_bets, "bets", []):
            sections.append(("🟨", "Oracle 予想", oracle_bets, 2))
        if hit_focus_bets is not None and getattr(hit_focus_bets, "bets", []):
            sections.append(("🔶", "HitFocus 予想", hit_focus_bets, 2))

        fields: list[dict[str, Any]] = []

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
            fields.append({
                "name":   f"{icon} **__{section_label}__**",
                "value":  "​",   # zero-width space（空フィールドにしない）
                "inline": False,
            })

            for bet in bets[:max_bets]:
                ev        = getattr(bet, "expected_value", 0.0)
                bet_type  = getattr(bet, "bet_type", "?")
                rec_bet   = int(getattr(bet, "recommended_bet", 0) or 0)
                fire      = _FIRE if ev >= 1.0 else "　"

                # フィールド name: "🔥 三連複  EV=2.13  ¥800"
                field_name = f"{fire} {bet_type}  EV={ev:.2f}  ¥{rec_bet:,}"

                # フィールド value: 馬番+馬名 カード形式
                field_value = _format_combo_card(bet)

                fields.append({
                    "name":   _s(field_name[:256]),
                    "value":  _s(field_value[:1024]),
                    "inline": False,
                })

            if len(fields) >= 23:   # Discord 上限 25、ヘッダー込みで余裕を持つ
                break

        if not fields:
            logger.info("有効フィールドなし — Discord 通知スキップ: %s", race_id)
            return

        invest_str   = f"¥{int(total_invest):,}" if total_invest > 0 else "なし"
        footer_text  = f"EV≥1.0 推奨投資 {invest_str}"
        if dashboard_url:
            footer_text += f" | 詳細 → {dashboard_url}"

        embed: dict[str, Any] = {
            "title":       _s(f"🏇  {label}  直前予想"),
            "description": _s(f"最高EV: **{max_ev:.2f}**  |  モデル3系統独立稼働"),
            "color":       color,
            "fields":      fields,
            "footer":      {"text": footer_text},
            "timestamp":   datetime.utcnow().isoformat(),
        }

        ok = self._post(self._url, {"embeds": [embed]})
        logger.info(
            "[Discord:予想] 直前予想 %s: %s  (sections=%d  fields=%d  max_ev=%.2f)",
            "送信完了" if ok else "送信失敗",
            race_id, len(sections), len(fields), max_ev,
        )


# ────────────────────────────────────────────────────────────────────────────
# 組み合わせカード形式フォーマッター（スマホ対応・馬番+馬名必須）
# ────────────────────────────────────────────────────────────────────────────

def _format_combo_card(bet: object) -> str:
    """
    買い目をスマホ対応カード形式にフォーマット。馬番・馬名を必ず表示。

    出力例:
      複勝:
        ⬛ 5番 アーバンシック
        ⬛ 9番 キタノオウジ

      三連複 (軸1頭流し 4点):
        ▶ 軸: 5番 アーバンシック
          相手: 3番 レガシー / 7番 サクセス / 9番 オウジ / 12番 ホープ

      三連単 (軸1頭→2頭 4組):
        ▶ 5番 アーバン → 9番 オウジ → 3番 レガシー
        ▶ 5番 アーバン → 3番 レガシー → 9番 オウジ
        ▶ 5番 アーバン → 7番 サクセス → 3番 レガシー
        (+1組)
    """
    from collections import Counter

    bt: str      = getattr(bet, "bet_type", "")
    combos: list = getattr(bet, "combinations", []) or []
    names: list  = getattr(bet, "horse_names",  []) or []

    if not combos:
        return "　(買い目なし)"

    n_total = len(combos)

    # horse_number → 馬名 逆引きマップ
    name_by_num: dict[int, str] = {}

    if bt in ("単勝", "複勝"):
        # 単勝/複勝: combo は1頭ずつ、names[i] が i 番目の combo の馬名
        for i, combo in enumerate(combos):
            num = combo[0] if isinstance(combo, (list, tuple)) else combo
            if i < len(names) and names[i]:
                name_by_num[int(num)] = str(names[i])
    else:
        # 多馬券: names は最初の combo に対応する順序
        first = combos[0]
        first_legs = list(first) if isinstance(first, (list, tuple)) else [first]
        for i, leg in enumerate(first_legs):
            if i < len(names) and names[i]:
                name_by_num[int(leg)] = str(names[i])

    def _label(num: int) -> str:
        n = name_by_num.get(int(num), "")
        return f"{num}番 {n}" if n else f"{num}番"

    # ── 単勝・複勝 ──────────────────────────────────────────────────────────
    if bt in ("単勝", "複勝"):
        nums = [c[0] if isinstance(c, (list, tuple)) else c for c in combos]
        lines = [f"⬛ {_label(n)}" for n in nums[:5]]
        if n_total > 5:
            lines.append(f"  … 他{n_total-5}頭")
        return "\n".join(lines)

    # ── 馬単・三連単（順序あり）────────────────────────────────────────────
    if bt in ("馬単", "三連単"):
        lines = []
        for combo in combos[:4]:
            legs = list(combo) if isinstance(combo, (list, tuple)) else [combo]
            arrow_str = " → ".join(_label(n) for n in legs)
            # 三連単は馬名がname_by_numに全馬分ないので combo 順で名前を補完
            lines.append(f"▶ {arrow_str}")
        if n_total > 4:
            lines.append(f"  (+{n_total - 4}点)")
        return "\n".join(lines)

    # ── 馬連・ワイド・三連複（軸流し or ボックス）──────────────────────────
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        flat     = [int(n) for combo in combos for n in combo]
        cnt      = Counter(flat)
        axis_set = {num for num, c in cnt.items() if c == n_total}

        if axis_set:
            # 軸あり: 軸馬を先頭、相手馬を列挙
            axes   = sorted(axis_set)
            others = sorted({int(n) for combo in combos for n in combo} - axis_set)
            axis_str = " / ".join(_label(a) for a in axes[:2])
            opp_str  = " / ".join(_label(o) for o in others[:6])
            if len(others) > 6:
                opp_str += f" … +{len(others)-6}頭"
            return (
                f"▶ 軸: {axis_str}\n"
                f"  相手: {opp_str}\n"
                f"  計{n_total}点"
            )
        else:
            # ボックス
            nums_all = sorted({int(n) for combo in combos for n in combo})
            box_str  = " / ".join(_label(n) for n in nums_all[:6])
            if len(nums_all) > 6:
                box_str += f" … +{len(nums_all)-6}頭"
            return f"ボックス: {box_str}\n計{n_total}点"

    return f"計{n_total}点"


def _summarize_combos(bet: object) -> str:
    """後方互換: _format_combo_card の旧名エイリアス。"""
    return _format_combo_card(bet)
