"""tests/test_embed_builder.py — Discord Embed プレミアム化ビルダーのテスト。"""

from __future__ import annotations

import pytest

from src.notification.embed_builder import (
    COLOR_G1,
    COLOR_G2,
    COLOR_G3,
    build_axis_partner_fields,
    confidence_color,
    dynamic_color,
    grade_color,
    infer_grade,
    stake_bar,
)


# ── 格付け推定 ───────────────────────────────────────────────────────────
class TestInferGrade:
    @pytest.mark.parametrize(
        ("race_name", "expected"),
        [
            ("安田記念(G1)", "G1"),
            ("安田記念（GⅠ）", "G1"),
            ("第91回 東京優駿（GI）", "G1"),
            ("京都新聞杯(G2)", "G2"),
            ("エプソムC（GⅢ）", "G3"),
            ("メルボルンT(L)", "L"),
            ("3歳上1勝クラス", None),
            ("", None),
        ],
    )
    def test_infer(self, race_name: str, expected: str | None) -> None:
        assert infer_grade(race_name) == expected


# ── カラー決定 ───────────────────────────────────────────────────────────
class TestColors:
    def test_grade_colors_are_distinct(self) -> None:
        assert len({COLOR_G1, COLOR_G2, COLOR_G3}) == 3
        assert grade_color("G1") == COLOR_G1
        assert grade_color("G2") == COLOR_G2
        assert grade_color("G3") == COLOR_G3

    def test_unknown_grade_returns_none(self) -> None:
        assert grade_color(None) is None
        assert grade_color("XX") is None

    def test_confidence_gradient_endpoints(self) -> None:
        low = confidence_color(0.0)
        high = confidence_color(1.0)
        assert low != high
        assert 0 <= low <= 0xFFFFFF
        assert 0 <= high <= 0xFFFFFF

    def test_confidence_clamps_out_of_range(self) -> None:
        assert confidence_color(-1.0) == confidence_color(0.0)
        assert confidence_color(2.0) == confidence_color(1.0)

    def test_dynamic_color_priority_grade_over_confidence(self) -> None:
        assert dynamic_color(grade="G1", confidence=0.2) == COLOR_G1

    def test_dynamic_color_jackpot_ev_overrides_all(self) -> None:
        c = dynamic_color(grade="G1", confidence=0.9, max_ev=3.5)
        assert c != COLOR_G1  # 万馬券級 EV は格付けより優先

    def test_dynamic_color_falls_back_to_confidence(self) -> None:
        assert dynamic_color(confidence=0.8) == confidence_color(0.8)

    def test_dynamic_color_default(self) -> None:
        assert isinstance(dynamic_color(), int)


# ── 推奨投資比率インジケーター ───────────────────────────────────────────
class TestStakeBar:
    def test_forty_percent(self) -> None:
        assert stake_bar(0.4) == "████░░░░░░ 40%"

    def test_zero_and_full(self) -> None:
        assert stake_bar(0.0) == "░░░░░░░░░░ 0%"
        assert stake_bar(1.0) == "██████████ 100%"

    def test_clamps(self) -> None:
        assert stake_bar(1.7) == "██████████ 100%"
        assert stake_bar(-0.3) == "░░░░░░░░░░ 0%"

    def test_small_fraction_rounds_visibly(self) -> None:
        # 1% 台でも 0 ブロックの "見えないバー" にしつつ % は表示する
        assert stake_bar(0.014) == "░░░░░░░░░░ 1.4%"


# ── 軸・相手グリッド ─────────────────────────────────────────────────────
class TestAxisPartnerFields:
    def test_grid_layout_inline_fields(self) -> None:
        fields = build_axis_partner_fields(
            axis=[(5, "アーバンシック")],
            partners=[(3, ""), (9, ""), (12, "")],
            ev=1.42,
            odds=48.3,
        )
        assert len(fields) == 3
        assert all(f["inline"] for f in fields)
        names = [f["name"] for f in fields]
        assert any("軸" in n for n in names)
        assert any("相手" in n for n in names)
        joined = " ".join(str(f["value"]) for f in fields)
        assert "アーバンシック" in joined
        assert "1.42" in joined
        assert "48.3" in joined

    def test_no_odds_shows_placeholder(self) -> None:
        fields = build_axis_partner_fields(
            axis=[(1, "")], partners=[], ev=1.1, odds=None
        )
        joined = " ".join(str(f["value"]) for f in fields)
        assert "—" in joined or "推定" in joined
