"""src/nar/note_adapter.py — NAR データを既存 Note/X 生成基盤へ流し込む
ラッパーのテスト。

既存の src.ops.money_management / note_generator を再利用し、
NAR 由来の買い目（NarBet）が NoteBet 互換として正しく扱えることを検証する。
"""

from __future__ import annotations

from src.nar.note_adapter import (
    NarBet,
    generate_nar_note_markdown,
    to_note_bet,
    to_note_bets,
    write_nar_drafts,
)
from src.ops.money_management import allocate_budget
from src.ops.sns_publisher import NoteBet


def _sample_bets() -> list[NarBet]:
    return [
        NarBet(bet_type="複勝", horse_desc="3番", ev=1.45, venue="大井"),
        NarBet(bet_type="単勝", horse_desc="1番", ev=1.20, venue="大井"),
        NarBet(bet_type="ワイド", horse_desc="1-3", ev=0.90, venue="大井"),
    ]


def test_to_note_bet_preserves_core_fields() -> None:
    """NarBet → NoteBet で券種・対象・EV が保持される。"""
    nb = to_note_bet(NarBet(bet_type="複勝", horse_desc="5番", ev=1.6, venue="船橋"))
    assert isinstance(nb, NoteBet)
    assert (nb.bet_type, nb.horse_desc, nb.ev) == ("複勝", "5番", 1.6)


def test_to_note_bets_maps_all_in_order() -> None:
    """リスト変換は順序と件数を保持する。"""
    note_bets = to_note_bets(_sample_bets())
    assert [b.bet_type for b in note_bets] == ["複勝", "単勝", "ワイド"]
    assert all(isinstance(b, NoteBet) for b in note_bets)


def test_nar_bets_are_compatible_with_allocate_budget() -> None:
    """互換性の核: NAR 由来 NoteBet が既存 allocate_budget で配分される。"""
    note_bets = to_note_bets(_sample_bets())
    allocs = allocate_budget(note_bets, total_budget=10_000)
    assert allocs  # 正 EV が存在するので配分結果は非空
    assert sum(a.allocated_yen for a in allocs) == 10_000


def test_generate_nar_note_markdown_contains_venue_and_paywall() -> None:
    """生成 Markdown に NAR 会場名・有料ライン・資金配分表が含まれる。"""
    md = generate_nar_note_markdown(
        _sample_bets(), date="20260603", venue="大井", total_budget=10_000
    )
    assert "大井" in md
    assert "地方競馬" in md
    assert "🔒" in md  # 有料ライン
    assert "資金配分" in md
    assert "¥10,000" in md


def test_generate_nar_note_markdown_handles_empty_bets() -> None:
    """買い目ゼロでもプレースホルダ付き Markdown を返す（例外を出さない）。"""
    md = generate_nar_note_markdown([], date="20260603", venue="川崎")
    assert "川崎" in md
    assert md.strip()  # 非空


def test_write_nar_drafts_creates_note_and_x_files(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """write_nar_drafts は note(.md)・X(.txt) の下書きを指定先に出力する。"""
    note_path, x_path = write_nar_drafts(
        _sample_bets(),
        date="20260603",
        venue="大井",
        out_dir=tmp_path,
    )
    assert note_path.exists() and note_path.suffix == ".md"
    assert x_path.exists() and x_path.suffix == ".txt"
    assert note_path.read_text(encoding="utf-8").strip()
    assert x_path.read_text(encoding="utf-8").strip()
