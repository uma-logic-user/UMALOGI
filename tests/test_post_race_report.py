"""
tests/test_post_race_report.py — 事後報告ジェネレーターの TDD テスト

テスト対象:
  - BetResult dataclass (sns_publisher)
  - generate_x_hit_tweet(result) -> str | None
  - generate_post_race_report(results, *, date, note_url) -> str
  - write_daily_reports(results, *, date, out_dir) -> Path
  - generate_note_draft の有料ライン追加（note_generator）

保証する不変条件:
  1. 外れ買い目は X 速報を生成しない
  2. 的中 X 速報は常に 140 文字以内
  3. 日次総括は空結果でもプレースホルダを含む有効な Markdown を返す
  4. 総括 ROI は総払戻 / 総投資で計算される
  5. write_daily_reports は的中数分の X ファイルを生成する
  6. generate_note_draft の有料ラインは予算配分表の前に入る
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.ops.money_management import allocate_budget
from src.ops.note_generator import generate_note_draft
from src.ops.sns_publisher import (
    BetResult,
    generate_post_race_report,
    generate_x_hit_tweet,
    write_daily_reports,
)
from src.ops.sns_publisher import NoteBet  # type: ignore[attr-defined]


# ─────────────────────────────────────────────────────────────────────
# フィクスチャ
# ─────────────────────────────────────────────────────────────────────


def _hit(
    race_name: str = "11R",
    venue: str = "東京",
    bet_type: str = "複勝",
    horse_desc: str = "3番",
    ev: float = 1.5,
    stake: int = 100,
    payout: int = 250,
    date: str = "20260603",
) -> BetResult:
    return BetResult(
        race_name=race_name,
        venue=venue,
        bet_type=bet_type,
        horse_desc=horse_desc,
        ev=ev,
        stake=stake,
        payout=payout,
        is_hit=True,
        date=date,
    )


def _miss(
    race_name: str = "9R",
    venue: str = "京都",
    bet_type: str = "単勝",
    horse_desc: str = "7番",
    ev: float = 1.3,
    stake: int = 100,
    date: str = "20260603",
) -> BetResult:
    return BetResult(
        race_name=race_name,
        venue=venue,
        bet_type=bet_type,
        horse_desc=horse_desc,
        ev=ev,
        stake=stake,
        payout=0,
        is_hit=False,
        date=date,
    )


# ─────────────────────────────────────────────────────────────────────
# BetResult dataclass
# ─────────────────────────────────────────────────────────────────────


def test_bet_result_roi_for_hit():
    """的中時 ROI = 100 × 払戻 / 投資。"""
    r = _hit(stake=100, payout=250)
    assert r.roi == pytest.approx(250.0)


def test_bet_result_roi_for_miss():
    """外れ時 ROI = 0。"""
    r = _miss(stake=100)
    assert r.roi == pytest.approx(0.0)


def test_bet_result_profit():
    """profit = 払戻 - 投資。"""
    r = _hit(stake=100, payout=350)
    assert r.profit == 250


def test_bet_result_miss_profit_is_negative():
    """外れ時の profit は負。"""
    r = _miss(stake=100)
    assert r.profit == -100


# ─────────────────────────────────────────────────────────────────────
# generate_x_hit_tweet
# ─────────────────────────────────────────────────────────────────────


def test_x_hit_tweet_returns_none_for_miss():
    """外れ買い目は None を返す。"""
    assert generate_x_hit_tweet(_miss()) is None


def test_x_hit_tweet_returns_string_for_hit():
    """的中買い目は文字列を返す。"""
    result = generate_x_hit_tweet(_hit())
    assert isinstance(result, str)
    assert result.strip()


def test_x_hit_tweet_within_140_chars():
    """X 速報は常に 140 文字以内。"""
    assert len(generate_x_hit_tweet(_hit())) <= 140


def test_x_hit_tweet_long_race_name_still_within_140():
    """長いレース名でも 140 字以内に収まる。"""
    r = _hit(race_name="帝王賞2026年春季特別GIレース", venue="大井競馬場")
    result = generate_x_hit_tweet(r)
    assert result is not None
    assert len(result) <= 140


def test_x_hit_tweet_contains_payout():
    """払戻額が含まれる。"""
    result = generate_x_hit_tweet(_hit(payout=1500))
    assert "1,500" in result or "1500" in result


def test_x_hit_tweet_contains_bet_type():
    """券種が含まれる。"""
    result = generate_x_hit_tweet(_hit(bet_type="馬連"))
    assert "馬連" in result


def test_x_hit_tweet_contains_note_url():
    """note.com への誘導 URL が含まれる。"""
    result = generate_x_hit_tweet(_hit())
    assert "note.com" in result


# ─────────────────────────────────────────────────────────────────────
# generate_post_race_report
# ─────────────────────────────────────────────────────────────────────


def test_post_race_report_empty_results_has_placeholder():
    """結果 0 件でもプレースホルダを含む非空 Markdown を返す。"""
    result = generate_post_race_report([], date="20260603")
    assert result.strip()
    assert "#" in result  # markdown header


def test_post_race_report_empty_results_no_error():
    """結果 0 件で例外を発生させない。"""
    try:
        generate_post_race_report([])
    except Exception as e:
        pytest.fail(f"空結果で例外: {e}")


def test_post_race_report_contains_date_in_title():
    """タイトルに日付が含まれる。"""
    result = generate_post_race_report([], date="20260603")
    assert "2026" in result and "06" in result and "03" in result


def test_post_race_report_title_format():
    """タイトルに '結果報告' が含まれる。"""
    result = generate_post_race_report([], date="20260603")
    assert "結果報告" in result


def test_post_race_report_roi_calculated_correctly():
    """回収率 = 100 × 総払戻 / 総投資。"""
    results = [
        _hit(stake=100, payout=300),  # 300%
        _miss(stake=100),  # 0%
    ]
    report = generate_post_race_report(results, date="20260603")
    # total_payout=300, total_stake=200 → ROI=150%
    assert "150" in report


def test_post_race_report_shows_hit_count():
    """的中数が含まれる。"""
    results = [_hit(), _miss(), _hit()]
    report = generate_post_race_report(results, date="20260603")
    # 的中2件
    assert "2" in report


def test_post_race_report_hit_list_contains_race_info():
    """的中一覧に買い目情報が含まれる。"""
    r = _hit(race_name="日本ダービー", bet_type="馬連", horse_desc="1-3", payout=8000)
    report = generate_post_race_report([r], date="20260603")
    assert "馬連" in report
    assert "1-3" in report


def test_post_race_report_all_miss_shows_roi():
    """全外れでも回収率 0% が表示される。"""
    results = [_miss(), _miss()]
    report = generate_post_race_report(results, date="20260603")
    assert "0" in report  # 0% のいずれかが含まれる


# ─────────────────────────────────────────────────────────────────────
# write_daily_reports
# ─────────────────────────────────────────────────────────────────────


def test_write_daily_reports_creates_note_file(tmp_path: Path):
    """Note 総括ファイルが作成される。"""
    path = write_daily_reports([], date="20260603", out_dir=tmp_path)
    assert path.exists()


def test_write_daily_reports_note_filename(tmp_path: Path):
    """Note ファイル名は note_report_YYYYMMDD.md。"""
    path = write_daily_reports([], date="20260603", out_dir=tmp_path)
    assert path.name == "note_report_20260603.md"


def test_write_daily_reports_creates_hit_tweet_per_hit(tmp_path: Path):
    """的中 1 件につき X ファイルが 1 つ作られる。"""
    results = [_hit(), _miss()]
    write_daily_reports(results, date="20260603", out_dir=tmp_path)
    x_files = list(tmp_path.glob("x_hit_*.txt"))
    assert len(x_files) == 1


def test_write_daily_reports_no_x_file_for_all_miss(tmp_path: Path):
    """全外れなら X ファイルは作られない。"""
    results = [_miss(), _miss()]
    write_daily_reports(results, date="20260603", out_dir=tmp_path)
    x_files = list(tmp_path.glob("x_hit_*.txt"))
    assert len(x_files) == 0


def test_write_daily_reports_multiple_hits_multiple_files(tmp_path: Path):
    """的中 3 件なら X ファイルが 3 つ作られる。"""
    results = [_hit(), _hit(race_name="9R"), _hit(race_name="10R")]
    write_daily_reports(results, date="20260603", out_dir=tmp_path)
    x_files = list(tmp_path.glob("x_hit_*.txt"))
    assert len(x_files) == 3


def test_write_daily_reports_note_is_valid_markdown(tmp_path: Path):
    """Note ファイルの内容が Markdown（# ヘッダーあり）。"""
    results = [_hit(), _miss()]
    path = write_daily_reports(results, date="20260603", out_dir=tmp_path)
    content = path.read_text(encoding="utf-8")
    assert "#" in content
    assert "結果報告" in content


def test_write_daily_reports_x_file_within_140_chars(tmp_path: Path):
    """X ファイルの内容が 140 文字以内。"""
    results = [_hit()]
    write_daily_reports(results, date="20260603", out_dir=tmp_path)
    x_file = next(tmp_path.glob("x_hit_*.txt"))
    content = x_file.read_text(encoding="utf-8").strip()
    assert len(content) <= 140


# ─────────────────────────────────────────────────────────────────────
# 有料ライン追加（generate_note_draft へのペイウォール挿入）
# ─────────────────────────────────────────────────────────────────────


def test_note_draft_has_paywall_marker():
    """買い目あり時、有料エリアマーカー（🔒）が含まれる。"""
    bets = [NoteBet("複勝", "3番", ev=1.5)]
    allocs = allocate_budget(bets)
    result = generate_note_draft(bets, allocs, date="20260603")
    assert "🔒" in result or "有料" in result


def test_note_draft_paywall_precedes_budget_table():
    """ペイウォールが予算配分表（💰）より前に挿入されている。"""
    bets = [NoteBet("複勝", "3番", ev=1.5)]
    allocs = allocate_budget(bets)
    result = generate_note_draft(bets, allocs, date="20260603")
    paywall_idx = result.find("🔒")
    budget_idx = result.find("💰")
    assert 0 <= paywall_idx < budget_idx
