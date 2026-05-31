# -*- coding: utf-8 -*-
"""
Hit Flash Dry-Run テスト

3パターンの擬似 EvaluationResult を生成し、Discord 予想チャンネルへ
テスト通知を送信する。本番DBには一切書き込まない。

Usage:
    py scripts/test_hit_flash.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=False)

sys.stdout.reconfigure(encoding="utf-8")


# ── BetHitDetail / EvaluationResult の最小モック ──────────────────────────


@dataclass
class _BetHitDetail:
    prediction_id: int
    bet_type: str
    is_hit: bool
    is_refund: bool
    payout: float
    invested: float
    profit: float
    roi: float
    combination: list[str]
    actual_winners: list[str]


@dataclass
class _EvaluationResult:
    race_id: str
    race_name: str
    date: str
    hits: list[_BetHitDetail]
    total_invested: float
    total_payout: float
    roi: float
    has_manbaken: bool
    max_single_roi: float
    is_refund_race: bool
    errors: list[str] = field(default_factory=list)

    @property
    def hit_count(self) -> int:
        return sum(1 for h in self.hits if h.is_hit)

    @property
    def net_profit(self) -> float:
        return self.total_payout - self.total_invested


# ── テストケース定義 ───────────────────────────────────────────────────────


def _case_manbaken() -> tuple[_EvaluationResult, str]:
    """万馬券的中シナリオ（¥100,000超）"""
    hits = [
        _BetHitDetail(
            prediction_id=1,
            bet_type="三連複",
            is_hit=True,
            is_refund=False,
            payout=128_400,
            invested=500,
            profit=127_900,
            roi=25_680.0,
            combination=[
                "5番 ダイヤモンドノット",
                "9番 カヴァレリッツォ",
                "3番 シルバーステップ",
            ],
            actual_winners=[
                "5番 ダイヤモンドノット",
                "9番 カヴァレリッツォ",
                "3番 シルバーステップ",
            ],
        ),
        _BetHitDetail(
            prediction_id=2,
            bet_type="複勝",
            is_hit=True,
            is_refund=False,
            payout=780,
            invested=500,
            profit=280,
            roi=156.0,
            combination=["5番 ダイヤモンドノット"],
            actual_winners=["5番 ダイヤモンドノット"],
        ),
        _BetHitDetail(
            prediction_id=3,
            bet_type="馬連",
            is_hit=False,
            is_refund=False,
            payout=0,
            invested=500,
            profit=-500,
            roi=0.0,
            combination=["5番 ダイヤモンドノット", "7番 グランドラッシュ"],
            actual_winners=["5番 ダイヤモンドノット", "9番 カヴァレリッツォ"],
        ),
    ]
    result = _EvaluationResult(
        race_id="202605100511",
        race_name="ＮＨＫマイルカップ（G1）",
        date="2026-05-10",
        hits=hits,
        total_invested=1_500,
        total_payout=129_180,
        roi=8_612.0,
        has_manbaken=True,
        max_single_roi=25_680.0,
        is_refund_race=False,
    )
    return result, "【DRY-RUN】万馬券的中シナリオ"


def _case_normal_hit() -> tuple[_EvaluationResult, str]:
    """通常的中シナリオ（¥3,800 / 単勝＋複勝）"""
    hits = [
        _BetHitDetail(
            prediction_id=4,
            bet_type="単勝",
            is_hit=True,
            is_refund=False,
            payout=2_400,
            invested=1_000,
            profit=1_400,
            roi=240.0,
            combination=["3番 ユーレイクイーン"],
            actual_winners=["3番 ユーレイクイーン"],
        ),
        _BetHitDetail(
            prediction_id=5,
            bet_type="複勝",
            is_hit=True,
            is_refund=False,
            payout=1_400,
            invested=1_000,
            profit=400,
            roi=140.0,
            combination=["3番 ユーレイクイーン"],
            actual_winners=["3番 ユーレイクイーン"],
        ),
    ]
    result = _EvaluationResult(
        race_id="202605100508",
        race_name="東京8R 3歳上2勝クラス",
        date="2026-05-10",
        hits=hits,
        total_invested=2_000,
        total_payout=3_800,
        roi=190.0,
        has_manbaken=False,
        max_single_roi=240.0,
        is_refund_race=False,
    )
    return result, "【DRY-RUN】通常的中シナリオ（単複）"


def _case_miss() -> tuple[_EvaluationResult, str]:
    """的中なしシナリオ（→ システムチャンネルへ流れることを確認）"""
    hits = [
        _BetHitDetail(
            prediction_id=6,
            bet_type="三連単",
            is_hit=False,
            is_refund=False,
            payout=0,
            invested=600,
            profit=-600,
            roi=0.0,
            combination=[
                "2番 アシストブレイズ",
                "7番 コーラルウィンド",
                "11番 ナイトリペア",
            ],
            actual_winners=[
                "5番 ダイヤモンドノット",
                "9番 カヴァレリッツォ",
                "3番 シルバーステップ",
            ],
        ),
    ]
    result = _EvaluationResult(
        race_id="202605100503",
        race_name="東京3R 3歳未勝利",
        date="2026-05-10",
        hits=hits,
        total_invested=600,
        total_payout=0,
        roi=0.0,
        has_manbaken=False,
        max_single_roi=0.0,
        is_refund_race=False,
    )
    return result, "【DRY-RUN】的中なし（システムchへ流れるはず）"


# ── エントリポイント ───────────────────────────────────────────────────────


def main() -> None:
    # fetch_race_result の _send_hit_flash を直接インポート
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "fetch_race_result",
        _ROOT / "scripts" / "fetch_race_result.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    send_hit_flash = mod._send_hit_flash

    cases = [_case_manbaken(), _case_normal_hit(), _case_miss()]
    for result, label in cases:
        print(f"\n送信中: {label}")
        send_hit_flash(result, result.race_name)
        print(f"  → 送信完了 (的中数={result.hit_count}, ROI={result.roi:.1f}%)")

    print("\n✅ 全3パターンの送信完了。Discord を確認してください。")


if __name__ == "__main__":
    main()
