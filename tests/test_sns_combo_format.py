"""src/ops/sns_publisher.py のフェーズ0修正（買い目表記バグ・週次レイアウト）の回帰テスト。"""

from __future__ import annotations

from datetime import date

from src.ops.sns_publisher import (
    ModelWeeklyStat,
    _format_combo,
    export_weekly_report,
)


# ── _format_combo（買い目表記の可読化）─────────────────────────────────


def test_format_combo_flat_single() -> None:
    assert _format_combo("[3]") == "3"


def test_format_combo_flat_pair() -> None:
    assert _format_combo("[3,5]") == "3-5"


def test_format_combo_axis_flow() -> None:
    """全組合せに共通する軸がある場合は「軸→相手」表記にする。"""
    assert _format_combo("[[6,8],[6,16],[6,2]]") == "軸6→相手2,8,16（3点）"


def test_format_combo_box_no_axis() -> None:
    """軸がない場合はボックス表記（a-b / c-d …）。"""
    out = _format_combo("[[6,8],[8,16],[6,16]]")
    assert out == "6-8 / 8-16 / 6-16（3点）"


def test_format_combo_garbage_fallback() -> None:
    assert _format_combo("not json") == "not json"


def test_format_combo_empty() -> None:
    assert _format_combo("") == ""
    assert _format_combo(None) == ""


def test_format_combo_no_raw_json_leak() -> None:
    """旧バグ（生JSON断片 '6, 8], [8, 16' の露出）が再発しないこと。"""
    out = _format_combo("[[6,8],[8,16],[6,16],[2,8],[2,6],[8,14],[6,14]]")
    assert "[" not in out
    assert "]," not in out
    assert "/" in out  # ボックス表記になっている


# ── 週次レポートのレイアウト（最高配当・的中率を主役に）──────────────────


def test_weekly_report_highlights_payout_and_hitrate(tmp_path) -> None:
    stats = [
        ModelWeeklyStat(
            model_name="HitFocus",
            n_bets=122,
            n_hits=14,
            total_stake=12200,
            total_return=12400,  # ROI 101.6%
            best_payout=1670,
            best_payout_desc="三連単",
        )
    ]
    path = export_weekly_report(
        stats,
        period_label="2026-05-25 〜 2026-05-31",
        out_dir=tmp_path,
        report_date=date(2026, 5, 31),
    )
    body = path.read_text(encoding="utf-8")

    # 最高配当ハイライト（見出し＋強調）
    assert "🏆" in body
    assert "**¥1,670**" in body
    # 的中率は強調（14/122 = 11.5%）
    assert "**11.5%**" in body
    # 回収率は「参考」かつ太字にしない
    assert "回収率(参考)" in body
    assert "**101.6%**" not in body
