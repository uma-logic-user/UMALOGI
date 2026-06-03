"""src/nar/note_adapter.py — 地方競馬（NAR）データを既存 Note/X 生成基盤へ橋渡しする。

既存の中央競馬（JRA）向け Note/X 自動生成資産を **再利用** するためのアダプタ層。
NAR 由来の買い目（``NarBet``）を ``src.ops.sns_publisher.NoteBet`` 互換に変換し、
``src.ops.money_management.allocate_budget``（予算配分ロジック）および
``src.ops.note_generator.generate_note_draft``（有料ライン挿入付き Markdown 生成）
へそのまま流し込めるようにする。

設計方針:
  - 既存モジュールは一切変更しない（読み取り再利用のみ）。
  - NAR 固有の文脈（地方競馬・ナイター・会場名）はヘッダー/プロモ文に付与する。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _dt_date
from pathlib import Path

from src.ops.money_management import BetAllocation, allocate_budget
from src.ops.note_generator import generate_note_draft
from src.ops.sns_publisher import NoteBet

# NAR 集客の既定導線（環境変数で上書きする運用は本番結線時に追加）。
_DEFAULT_NOTE_URL = "https://note.com/umalogi"


@dataclass
class NarBet:
    """地方競馬 1 買い目（NoteBet 互換 + 会場メタ）。

    ``NoteBet`` と同一の (bet_type, horse_desc, ev) を持ちつつ、
    NAR 固有の会場情報を保持する。
    """

    bet_type: str
    horse_desc: str
    ev: float
    venue: str = ""


def to_note_bet(nar_bet: NarBet) -> NoteBet:
    """NarBet を既存基盤互換の NoteBet へ変換する。

    Args:
        nar_bet: 変換元の地方競馬買い目。

    Returns:
        bet_type / horse_desc / ev を引き継いだ NoteBet。
    """
    return NoteBet(
        bet_type=nar_bet.bet_type,
        horse_desc=nar_bet.horse_desc,
        ev=nar_bet.ev,
    )


def to_note_bets(nar_bets: list[NarBet]) -> list[NoteBet]:
    """NarBet リストを NoteBet リストへ順序保持で変換する。"""
    return [to_note_bet(b) for b in nar_bets]


def _normalize_date(date: str | None) -> str:
    """日付を YYYYMMDD に正規化する（省略時は本日）。"""
    return (date or _dt_date.today().strftime("%Y%m%d")).replace("-", "")


def _nar_header(date_ds: str, venue: str, n_bets: int) -> list[str]:
    """NAR 記事冒頭（地方競馬の文脈ヘッダー）を生成する。"""
    y, m, d = date_ds[:4], date_ds[4:6], date_ds[6:]
    place = venue or "地方競馬"
    return [
        f"# 🏇【地方競馬AI予想】{y}年{m}月{d}日 {place}｜期待値ベース買い目",
        "",
        f"> **地方競馬（NAR）{place}開催分**　by UMALOGI AI予測システム",
        "",
        (
            "中央競馬（JRA）で実績を積んだ UMALOGI の期待値（EV）エンジンを、"
            "365日開催の **地方競馬** へ展開した AI 予想です。  \n"
            f"本日は **{place}** の妙味ある買い目を厳選しました（全 {n_bets} 点）。"
        ),
        "",
        "---",
        "",
    ]


def generate_nar_note_markdown(
    nar_bets: list[NarBet],
    *,
    date: str | None = None,
    venue: str = "",
    total_budget: int = 10_000,
) -> str:
    """NAR 買い目から有料ライン付き Note 記事 Markdown を生成する。

    既存の ``allocate_budget`` で資金配分し、``generate_note_draft`` で
    有料ライン挿入付き本文を生成、その前に NAR 文脈ヘッダーを付与する。

    Args:
        nar_bets:     地方競馬の買い目リスト（空でもプレースホルダ付きで返す）。
        date:         対象日 YYYYMMDD / YYYY-MM-DD（省略時: 本日）。
        venue:        会場名（記事ヘッダーに表示）。
        total_budget: 表示用の総予算基準（円）。

    Returns:
        note.com 貼り付け用 Markdown 文字列。
    """
    ds = _normalize_date(date)
    note_bets = to_note_bets(nar_bets)
    allocs: list[BetAllocation] = allocate_budget(note_bets, total_budget=total_budget)
    body = generate_note_draft(note_bets, allocs, date=ds, total_budget=total_budget)
    header = "\n".join(_nar_header(ds, venue, len(nar_bets)))
    return f"{header}\n{body}"


def generate_nar_x_promo(
    nar_bets: list[NarBet],
    *,
    venue: str = "",
    note_url: str | None = None,
) -> str:
    """地方競馬向け X（Twitter）集客ツイートを生成する（≤140 文字保証）。

    Args:
        nar_bets: 買い目リスト。
        venue:    会場名（本文に挿入）。
        note_url: 誘導先 note URL（未指定時は既定 URL）。

    Returns:
        140 文字以内のツイートテキスト。
    """
    url = note_url or _DEFAULT_NOTE_URL
    place = venue or "地方競馬"
    tags = "#地方競馬 #競馬予想 #UMALOGI #期待値競馬"

    pos = [b for b in nar_bets if b.ev > 1.0]
    if pos:
        max_ev = max(b.ev for b in pos)
        body = (
            f"🌃本日の{place}！AIが期待値{max_ev:.2f}の妙味を検知。"
            "推奨買い目と¥1万配分はNoteで公開👇"
        )
    else:
        body = f"本日の{place}予想を準備中！AI厳選の買い目はNoteにて👇"

    core = f"\n{url}\n{tags}"
    tweet = body + core
    if len(tweet) > 140:
        tweet = body[: max(140 - len(core), 0)] + core
    return tweet[:140]


def write_nar_drafts(
    nar_bets: list[NarBet],
    *,
    date: str | None = None,
    venue: str = "",
    out_dir: Path,
    note_url: str | None = None,
    total_budget: int = 10_000,
) -> tuple[Path, Path]:
    """NAR の Note/X 下書きをファイルへ書き出し (note_path, x_path) を返す。

    既存 JRA 版（``src.ops.note_generator.write_daily_drafts``）と同一の
    入出力契約に揃えつつ、NAR 専用のファイル名・本文で出力する。

    Args:
        nar_bets:     買い目リスト。
        date:         対象日 YYYYMMDD / YYYY-MM-DD（省略時: 本日）。
        venue:        会場名。
        out_dir:      出力先ディレクトリ（テスト時は tmp_path を渡す）。
        note_url:     X ツイートに含める note URL。
        total_budget: Note 記事の総予算基準。

    Returns:
        (nar_note_pre_YYYYMMDD.md の Path, nar_x_pre_YYYYMMDD.txt の Path)
    """
    ds = _normalize_date(date)
    out_dir.mkdir(parents=True, exist_ok=True)

    note_md = generate_nar_note_markdown(
        nar_bets, date=ds, venue=venue, total_budget=total_budget
    )
    x_text = generate_nar_x_promo(nar_bets, venue=venue, note_url=note_url)

    note_path = out_dir / f"nar_note_pre_{ds}.md"
    x_path = out_dir / f"nar_x_pre_{ds}.txt"
    note_path.write_text(note_md, encoding="utf-8")
    x_path.write_text(x_text, encoding="utf-8")
    return note_path, x_path
