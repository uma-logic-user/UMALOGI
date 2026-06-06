"""
tests/test_encoding_guards.py — 文字化けガードレールの TDD テスト

テスト対象:
  1. is_garbled_name()     : 名前フィールド専用の高感度文字化け検知（text.py）
  2. _sjis_name()          : JVLink 名前フィールドのガード付きデコード（jravan_client.py）
  3. _parse_entry_rows()   : netkeiba HTML パース時の置換文字ガード（entry_table.py）
  4. clean_mojibake.py     : DB 文字化けデータの一括浄化スクリプト

背景:
  JVLink COM API が CP932 2バイト文字のリードバイトを '?' (U+003F) に化けた状態で
  返すケースがある（2026-05-15、2026-06-06 などで実例確認済み）。
  「?X」「?e」「?N」のように '?' + ASCII文字が連続するパターンが文字化けの証拠。
  本テストはこのパターンを確実に遮断することを保証する。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────
# Group 1: is_garbled_name() — 名前フィールド専用検知
# ─────────────────────────────────────────────────────────────────────


def test_is_garbled_name_detects_question_ascii_pair():
    """単発の ?X パターン（JVLink CP932 リードバイト脱落）を検出する。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("?X?eー?N?X") is True  # ポートアイランドステークス化け


def test_is_garbled_name_detects_single_question_ascii():
    """単発 '?A' でも名前フィールドでは文字化けと判定する。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("?W?F?[?香@") is True  # 今日の馬名の実例


def test_is_garbled_name_detects_halfwidth_katakana():
    """半角カタカナ（JVLink 文字化けアーティファクト）を検出する。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("000'ｺ?") is True


def test_is_garbled_name_detects_replacement_char():
    """Unicode 置換文字 U+FFFD を文字化けと判定する。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("馬名�") is True


def test_is_garbled_name_detects_curly_quote_in_name():
    """CP932 バイトがカーリークォートに化けたパターンを検出する。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("000‘c?") is True


def test_is_garbled_name_passes_clean_katakana():
    """正常なカタカナ馬名を文字化けと誤検知しない。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("ポートアイランドステークス") is False


def test_is_garbled_name_passes_clean_hiragana():
    """正常なひらがなを文字化けと誤検知しない。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("すみれステークス") is False


def test_is_garbled_name_passes_kanji_name():
    """正常な漢字・カタカナ混合名を誤検知しない。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("武豊") is False
    assert is_garbled_name("三木ホースランドパークジャンプステークス") is False


def test_is_garbled_name_passes_empty_string():
    """空文字は文字化けと判定しない。"""
    from src.utils.text import is_garbled_name

    assert is_garbled_name("") is False
    assert is_garbled_name(None) is False  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────
# Group 2: _sjis_name() — JVLink 名前フィールドのガード付きデコード
# ─────────────────────────────────────────────────────────────────────


def test_sjis_name_returns_empty_for_garbled_bytes():
    """?X?e パターンに化けた CP932 バイト列を受け取ったとき空文字を返す。"""
    from src.scraper.jravan_client import _sjis_name

    # 0x3F=? 0x58=X 0x3F=? 0x65=e（JVLink が返す破損バイト列）
    garbled = bytes([0x3F, 0x58, 0x3F, 0x65]) + b"\x20" * 32
    assert _sjis_name(garbled, slice(0, 4)) == ""


def test_sjis_name_returns_correct_for_valid_cp932():
    """正常な CP932 バイト列（すみれ）を正しくデコードして返す。"""
    from src.scraper.jravan_client import _sjis_name

    name = "すみれ"
    raw = name.encode("cp932") + b"\x20" * 30
    result = _sjis_name(raw, slice(0, len(name.encode("cp932"))))
    assert result == name


def test_sjis_name_returns_correct_for_katakana_stakes():
    """カタカナのステークス名（CP932）を正しくデコードする。"""
    from src.scraper.jravan_client import _sjis_name

    name = "アイランドステークス"
    raw = name.encode("cp932") + b"\x20" * 40
    result = _sjis_name(raw, slice(0, len(name.encode("cp932"))))
    assert result == name


def test_sjis_name_returns_empty_for_halfwidth_katakana():
    """半角カタカナを含む文字化けバイト列に対して空文字を返す。"""
    from src.scraper.jravan_client import _sjis_name

    # ｺ (U+FF7A, half-width katakana KO) を含む文字列
    garbled_with_hwk = "000ｺ?".encode("utf-8") + b"\x20" * 20
    result = _sjis_name(garbled_with_hwk, slice(0, 10))
    # 半角カタカナが含まれるため空文字で保護
    assert result == ""


# ─────────────────────────────────────────────────────────────────────
# Group 3: entry_table.py — netkeiba HTML パース時の置換文字ガード
# ─────────────────────────────────────────────────────────────────────


def test_entry_parse_replaces_fffd_horse_name_with_empty():
    """U+FFFD を含む馬名を空文字で保護する。"""
    from bs4 import BeautifulSoup
    from src.scraper.entry_table import _parse_entry_rows

    html = """
    <table class="Shutuba_Table">
      <tr class="HorseList">
        <td>1</td><td>1</td><td></td>
        <td class="HorseInfo"><a href="/horse/2021001234/">���</a></td>
        <td>牡3</td><td>56.0</td><td>武豊</td><td>藤沢</td>
        <td></td><td></td>
      </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = _parse_entry_rows(soup)
    assert len(entries) == 1
    assert entries[0].horse_name == ""  # 置換文字 → 空文字で保護


def test_entry_parse_preserves_clean_horse_name():
    """正常な馬名はそのまま通過する。"""
    from bs4 import BeautifulSoup
    from src.scraper.entry_table import _parse_entry_rows

    html = """
    <table class="Shutuba_Table">
      <tr class="HorseList">
        <td>1</td><td>1</td><td></td>
        <td class="HorseInfo"><a href="/horse/2021001234/">すみれ</a></td>
        <td>牝3</td><td>54.0</td><td>川田将雅</td><td>高野友和</td>
        <td></td><td></td>
      </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = _parse_entry_rows(soup)
    assert len(entries) == 1
    assert entries[0].horse_name == "すみれ"


# ─────────────────────────────────────────────────────────────────────
# Group 4: clean_mojibake.py — DB 文字化けデータの一括浄化
# ─────────────────────────────────────────────────────────────────────


def _make_test_db(path: Path) -> None:
    """テスト用 SQLite DB を作成する。"""
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            race_name TEXT,
            venue TEXT
        );
        CREATE TABLE entries (
            race_id TEXT,
            horse_number INTEGER,
            horse_name TEXT,
            jockey TEXT,
            PRIMARY KEY (race_id, horse_number)
        );
        CREATE TABLE race_results (
            race_id TEXT,
            horse_number INTEGER,
            horse_name TEXT,
            jockey TEXT,
            PRIMARY KEY (race_id, horse_number)
        );
        INSERT INTO races VALUES ('202609030111', '?吹c?X?eー?N?X', '阪神');
        INSERT INTO races VALUES ('202609010108', 'すみれステークス', '阪神');
        INSERT INTO entries VALUES ('202609030111', 1, '?W?F?[?香@', '');
        INSERT INTO entries VALUES ('202609030111', 2, 'アイランドS', '武豊');
        INSERT INTO race_results VALUES ('202609030111', 1, '?~?W?V??', '');
        INSERT INTO race_results VALUES ('202609010108', 1, 'すみれ', '武豊');
    """)
    conn.commit()
    conn.close()


def test_clean_mojibake_clears_garbled_race_name(tmp_path: Path):
    """文字化け races.race_name を空文字にクリアする。"""
    db_path = tmp_path / "test.db"
    _make_test_db(db_path)

    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "scripts/clean_mojibake.py", "--db", str(db_path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0

    conn = sqlite3.connect(str(db_path))
    garbled = conn.execute(
        "SELECT race_name FROM races WHERE race_id='202609030111'"
    ).fetchone()[0]
    clean = conn.execute(
        "SELECT race_name FROM races WHERE race_id='202609010108'"
    ).fetchone()[0]
    conn.close()

    assert garbled == ""  # 文字化け → 空文字
    assert clean == "すみれステークス"  # 正常 → 変更なし


def test_clean_mojibake_clears_garbled_entries_horse_name(tmp_path: Path):
    """文字化け entries.horse_name を空文字にクリアする。"""
    db_path = tmp_path / "test.db"
    _make_test_db(db_path)

    import subprocess
    import sys

    subprocess.run(
        [sys.executable, "scripts/clean_mojibake.py", "--db", str(db_path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    conn = sqlite3.connect(str(db_path))
    garbled = conn.execute(
        "SELECT horse_name FROM entries WHERE race_id='202609030111' AND horse_number=1"
    ).fetchone()[0]
    clean = conn.execute(
        "SELECT horse_name FROM entries WHERE race_id='202609030111' AND horse_number=2"
    ).fetchone()[0]
    conn.close()

    assert garbled == ""  # 文字化け → 空文字
    assert clean == "アイランドS"  # 正常 → 変更なし


def test_clean_mojibake_dry_run_does_not_modify_db(tmp_path: Path):
    """--dry-run では DB を変更しない。"""
    db_path = tmp_path / "test.db"
    _make_test_db(db_path)

    import subprocess
    import sys

    subprocess.run(
        [
            sys.executable,
            "scripts/clean_mojibake.py",
            "--db",
            str(db_path),
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    conn = sqlite3.connect(str(db_path))
    garbled_still = conn.execute(
        "SELECT race_name FROM races WHERE race_id='202609030111'"
    ).fetchone()[0]
    conn.close()

    assert garbled_still != ""  # --dry-run なので元の文字化けが残っている
