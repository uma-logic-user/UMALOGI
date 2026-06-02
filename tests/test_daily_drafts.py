"""
tests/test_daily_drafts.py — 日次下書き生成の TDD テスト

テスト対象:
  - generate_note_draft(bets, allocations, *, date, total_budget) -> str
  - generate_x_promo_tweet(bets, *, note_url) -> str
  - write_daily_drafts(bets, allocations, *, date, note_url, out_dir) -> (Path, Path)

保証する不変条件:
  1. Note 下書きは空買い目でもプレースホルダを含む有効な Markdown を返す
  2. X ツイートは常に 140 文字以内
  3. 指定 URL が X ツイートに含まれる
  4. ファイル出力は note_pre_YYYYMMDD.md / x_pre_YYYYMMDD.txt の命名規則に従う
  5. 実弾処理（auto_runner）は一切ブロックしない（例外セーフ）
"""

from __future__ import annotations

from pathlib import Path

from src.ops.money_management import BetAllocation, allocate_budget
from src.ops.note_generator import (
    generate_note_draft,
    generate_x_promo_tweet,
    write_daily_drafts,
)
from src.ops.sns_publisher import NoteBet


# ─────────────────────────────────────────────────────────────────────
# ヘルパー
# ─────────────────────────────────────────────────────────────────────


def _alloc(bets: list[NoteBet], budget: int = 10_000) -> list[BetAllocation]:
    return allocate_budget(bets, total_budget=budget)


def _single_bet(ev: float = 1.5) -> tuple[list[NoteBet], list[BetAllocation]]:
    bets = [NoteBet(bet_type="複勝", horse_desc="3番", ev=ev)]
    return bets, _alloc(bets)


# ─────────────────────────────────────────────────────────────────────
# generate_note_draft — 空買い目
# ─────────────────────────────────────────────────────────────────────


def test_note_draft_empty_bets_returns_nonempty_string():
    """買い目 0 件でも非空の文字列を返す（プレースホルダあり）。"""
    result = generate_note_draft([], [], date="20260602")
    assert result.strip()


def test_note_draft_empty_bets_has_placeholder():
    """買い目 0 件のときプレースホルダ文字列が含まれる。"""
    result = generate_note_draft([], [], date="20260602")
    assert (
        "予想データ" in result or "データがありません" in result or "準備中" in result
    )


def test_note_draft_empty_bets_is_valid_markdown():
    """空買い目でもマークダウンヘッダー (#) を含む。"""
    result = generate_note_draft([], [], date="20260602")
    assert "#" in result


# ─────────────────────────────────────────────────────────────────────
# generate_note_draft — 日付
# ─────────────────────────────────────────────────────────────────────


def test_note_draft_contains_year_in_title():
    result = generate_note_draft([], [], date="20260602")
    assert "2026" in result


def test_note_draft_contains_month_in_title():
    result = generate_note_draft([], [], date="20260602")
    assert "06" in result


def test_note_draft_contains_day_in_title():
    result = generate_note_draft([], [], date="20260602")
    assert "02" in result


# ─────────────────────────────────────────────────────────────────────
# generate_note_draft — 買い目内容
# ─────────────────────────────────────────────────────────────────────


def test_note_draft_contains_bet_type():
    bets, allocs = _single_bet()
    result = generate_note_draft(bets, allocs, date="20260602")
    assert "複勝" in result


def test_note_draft_contains_horse_desc():
    bets, allocs = _single_bet()
    result = generate_note_draft(bets, allocs, date="20260602")
    assert "3番" in result


def test_note_draft_contains_ev_value():
    bets, allocs = _single_bet(ev=1.5)
    result = generate_note_draft(bets, allocs, date="20260602")
    assert "1.5" in result or "EV" in result


def test_note_draft_single_bet_full_budget_displayed():
    """1 件買い目のとき推奨配分 10,000 円が表示される。"""
    bets, allocs = _single_bet()
    result = generate_note_draft(bets, allocs, date="20260602")
    assert "10,000" in result or "10000" in result


def test_note_draft_multiple_bets_all_included():
    """複数買い目がすべて（または選定された）買い目が含まれる。"""
    bets = [
        NoteBet("単勝", "1番", ev=2.0),
        NoteBet("複勝", "3番", ev=1.3),
    ]
    allocs = _alloc(bets)
    result = generate_note_draft(bets, allocs, date="20260602")
    assert "1番" in result
    assert "3番" in result


# ─────────────────────────────────────────────────────────────────────
# generate_x_promo_tweet
# ─────────────────────────────────────────────────────────────────────


def test_x_tweet_always_within_140_chars():
    bets, _ = _single_bet()
    result = generate_x_promo_tweet(bets)
    assert len(result) <= 140


def test_x_tweet_high_ev_within_140_chars():
    """高 EV (>= 1.4) でも 140 字に収まる。"""
    bets = [NoteBet("単勝", "1番", ev=1.8)]
    result = generate_x_promo_tweet(bets)
    assert len(result) <= 140


def test_x_tweet_contains_custom_note_url():
    bets, _ = _single_bet()
    custom_url = "https://note.com/test_custom_url"
    result = generate_x_promo_tweet(bets, note_url=custom_url)
    assert custom_url in result


def test_x_tweet_contains_default_note_url():
    """note_url 未指定のとき note.com ドメインが含まれる。"""
    bets, _ = _single_bet()
    result = generate_x_promo_tweet(bets)
    assert "note.com" in result


def test_x_tweet_empty_bets_nonempty_and_within_limit():
    result = generate_x_promo_tweet([])
    assert result.strip()
    assert len(result) <= 140


def test_x_tweet_long_url_still_within_140_chars():
    """URL が長くても 140 字に収まる（切り詰め保護）。"""
    bets, _ = _single_bet()
    long_url = "https://note.com/" + "a" * 80
    result = generate_x_promo_tweet(bets, note_url=long_url)
    assert len(result) <= 140


# ─────────────────────────────────────────────────────────────────────
# write_daily_drafts
# ─────────────────────────────────────────────────────────────────────


def test_write_daily_drafts_creates_both_files(tmp_path: Path):
    bets, allocs = _single_bet()
    note_path, x_path = write_daily_drafts(
        bets, allocs, date="20260602", out_dir=tmp_path
    )
    assert note_path.exists()
    assert x_path.exists()


def test_write_daily_drafts_note_filename(tmp_path: Path):
    bets, allocs = _single_bet()
    note_path, _ = write_daily_drafts(bets, allocs, date="20260602", out_dir=tmp_path)
    assert note_path.name == "note_pre_20260602.md"


def test_write_daily_drafts_x_filename(tmp_path: Path):
    bets, allocs = _single_bet()
    _, x_path = write_daily_drafts(bets, allocs, date="20260602", out_dir=tmp_path)
    assert x_path.name == "x_pre_20260602.txt"


def test_write_daily_drafts_note_content_is_markdown(tmp_path: Path):
    bets, allocs = _single_bet()
    note_path, _ = write_daily_drafts(bets, allocs, date="20260602", out_dir=tmp_path)
    content = note_path.read_text(encoding="utf-8")
    assert "#" in content
    assert "複勝" in content


def test_write_daily_drafts_x_content_within_140(tmp_path: Path):
    bets, allocs = _single_bet()
    _, x_path = write_daily_drafts(bets, allocs, date="20260602", out_dir=tmp_path)
    content = x_path.read_text(encoding="utf-8").strip()
    assert len(content) <= 140


def test_write_daily_drafts_empty_bets_creates_files(tmp_path: Path):
    """空買い目でもファイルが作成される（エラーを出さない）。"""
    note_path, x_path = write_daily_drafts([], [], date="20260602", out_dir=tmp_path)
    assert note_path.exists()
    assert x_path.exists()


def test_write_daily_drafts_uses_today_when_no_date(tmp_path: Path):
    """date 未指定のとき本日の日付でファイル名が作られる。"""
    from datetime import date

    note_path, _ = write_daily_drafts([], [], out_dir=tmp_path)
    today = date.today().strftime("%Y%m%d")
    assert today in note_path.name


def test_write_daily_drafts_utf8_encoding(tmp_path: Path):
    """日本語を含む内容が UTF-8 で正常に読み書きできる。"""
    bets, allocs = _single_bet()
    note_path, _ = write_daily_drafts(bets, allocs, date="20260602", out_dir=tmp_path)
    content = note_path.read_text(encoding="utf-8")
    assert "UMALOGI" in content
