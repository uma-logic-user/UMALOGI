"""
通知ディスパッチャー

EvaluationResult を受け取り、結果の重要度に応じて
Discord / LINE / X (Twitter) へ振り分けて送信する。

通知レベル:
  JACKPOT  : 払戻 >= 100,000 円 または ROI >= 500%
             → 全チャンネルへ証拠画像付き
  BIG      : 払戻 >= 10,000 円 (万馬券) または ROI >= 300%
             → 全チャンネルへ証拠画像付き
  NORMAL   : 的中 + ROI >= 100%
             → Discord / LINE のみ（テキスト）
  MISS     : 不的中 → 送信しない

環境変数による有効/無効の切替:
  NOTIFY_DISCORD=1  / NOTIFY_LINE=1  / NOTIFY_TWITTER=1
"""

from __future__ import annotations

import logging
import os
import tempfile
from enum import Enum, auto
from pathlib import Path

from ..evaluation.evaluator import BetHitDetail, EvaluationResult
from .base import BaseNotifier, NotifyMessage
from .discord_notifier import DiscordNotifier
from .image_builder import build_hit_image
from .line_notifier import LineNotifier
from .twitter_notifier import TwitterNotifier

logger = logging.getLogger(__name__)


class NotifyLevel(Enum):
    MISS    = auto()
    NORMAL  = auto()
    BIG     = auto()
    JACKPOT = auto()


def _classify(hit: BetHitDetail) -> NotifyLevel:
    """1買い目の払戻・ROI から通知レベルを判定する。"""
    if not hit.is_hit:
        return NotifyLevel.MISS
    if hit.payout >= 100_000 or hit.roi >= 500:
        return NotifyLevel.JACKPOT
    if hit.payout >= 10_000 or hit.roi >= 300:
        return NotifyLevel.BIG
    if hit.roi >= 100:
        return NotifyLevel.NORMAL
    return NotifyLevel.MISS   # 的中でも回収率が100%未満なら通知不要


def _build_body(result: EvaluationResult, hit: BetHitDetail) -> str:
    lines = [
        f"🏇 {result.race_name}  {result.date}",
        f"📋 券種: {hit.bet_type}",
        f"🎯 予想: {' / '.join(hit.combination)}",
        f"🏆 実績: {' / '.join(hit.actual_winners[:3])}",
        f"",
        f"💰 払戻: ¥{hit.payout:,.0f}",
        f"📈 回収率: {hit.roi:.1f}%",
        f"💸 購入: ¥{hit.invested:,.0f}  利益: ¥{hit.profit:,.0f}",
    ]
    if result.is_refund_race:
        lines.append("⚠️ 返還馬券あり")
    return "\n".join(lines)


def _build_title(hit: BetHitDetail, level: NotifyLevel) -> str:
    if level == NotifyLevel.JACKPOT:
        return f"🎆 爆裂的中！ {hit.bet_type}  ROI {hit.roi:.0f}%"
    if level == NotifyLevel.BIG:
        return f"🥇 万馬券的中！ {hit.bet_type}  ¥{hit.payout:,.0f}"
    return f"✅ 的中！ {hit.bet_type}  ROI {hit.roi:.0f}%"


class NotificationDispatcher:
    """
    的中結果を受け取り、レベルに応じたチャンネルへ自動送信する。

    Usage:
        dispatcher = NotificationDispatcher()
        dispatcher.dispatch(evaluation_result)
    """

    def __init__(
        self,
        notifiers: list[BaseNotifier] | None = None,
        *,
        image_dir: Path | None = None,
        min_level: NotifyLevel = NotifyLevel.NORMAL,
    ) -> None:
        """
        Args:
            notifiers: 使用するノーティファイアのリスト。
                       None の場合は環境変数から自動構築。
            image_dir: 証拠画像の保存先ディレクトリ。
            min_level: この以上のレベルのみ通知する。
        """
        self._notifiers = notifiers if notifiers is not None else self._auto_notifiers()
        self._image_dir = image_dir or Path(
            os.environ.get("NOTIFY_IMAGE_DIR", "/tmp/umalogi_images")
        )
        self._min_level = min_level

    @staticmethod
    def _auto_notifiers() -> list[BaseNotifier]:
        notifiers: list[BaseNotifier] = []
        if os.environ.get("NOTIFY_DISCORD", "1") == "1":
            notifiers.append(DiscordNotifier())
        if os.environ.get("NOTIFY_LINE", "0") == "1":
            notifiers.append(LineNotifier())
        if os.environ.get("NOTIFY_TWITTER", "0") == "1":
            notifiers.append(TwitterNotifier())
        return notifiers

    def dispatch(self, result: EvaluationResult) -> list[BetHitDetail]:
        """
        EvaluationResult の全買い目を評価し、基準を満たすものを通知する。

        Returns:
            通知を送った BetHitDetail のリスト
        """
        notified: list[BetHitDetail] = []

        for hit in result.hits:
            level = _classify(hit)
            if level.value < self._min_level.value:
                continue

            title = _build_title(hit, level)
            body  = _build_body(result, hit)
            image_path: Path | None = None

            # BIG / JACKPOT は証拠画像を生成
            if level in (NotifyLevel.BIG, NotifyLevel.JACKPOT):
                img_file = self._image_dir / f"{result.race_id}_{hit.prediction_id}.png"
                image_path = build_hit_image(
                    race_name=result.race_name,
                    date=result.date,
                    bet_type=hit.bet_type,
                    combination=hit.combination,
                    payout=hit.payout,
                    roi=hit.roi,
                    invested=hit.invested,
                    out_path=img_file,
                )

            message = NotifyMessage(title=title, body=body, image_path=image_path)

            for notifier in self._notifiers:
                # NORMAL は画像なし、Twitter は BIG+ のみ
                if isinstance(notifier, TwitterNotifier) and level == NotifyLevel.NORMAL:
                    continue
                notifier.send(message)

            notified.append(hit)
            logger.info(
                "通知送信: race=%s bet=%s ROI=%.1f%% level=%s",
                result.race_id, hit.bet_type, hit.roi, level.name,
            )

        return notified
