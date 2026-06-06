"""Universal Text Sanitizer -- DB insert/API response purification utility."""

from __future__ import annotations
import re

# Remove: NUL + C0 control chars + C1 control chars (keep tab/newline/CR)
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# Garbling detection: non-ASCII, non-CJK, non-Latin-Extended outlier chars
# U+0300-U+036F: combining marks  U+0370-U+03FF: Greek  U+0400-U+04FF: Cyrillic
# U+2000-U+206F: general punctuation  U+2100-U+214F: letterlike symbols
_GARBLED_BLOCK_RE = re.compile("[̀-ӿ -⁯℀-⅏←-⇿∞∂∆∏∑]")

# Greek letters repeated 2+ consecutive (e.g., "ΞΔΞΛΞκ")
_GARBLED_REPEAT_RE = re.compile("[Ͱ-Ͽ]{2,}")

# Special punctuation marks appearing 2+ consecutively (bullet-pattern garbling)
# U+2022=bullet U+2018/9=curly single quotes U+201C/D=curly double quotes
# U+2013=en-dash U+2014=em-dash
_BULLET_PATTERN_RE = re.compile("[•‘’“”–—]{2,}")

# JVLink CP932 lead-byte replaced by '?' (U+003F) — artifact pattern
# e.g., "?A?h?}?C???e?‰" where each "?X" was originally a double-byte SJIS char
# Also covers halfwidth katakana trail bytes (U+FF61-FF9F) appearing after '?'
_JVLINK_QUESTION_RE = re.compile(r"(?:\?[\x21-\x7e\xa6-\xdf\x80-\x9f｡-ﾟ]){2,}")

# Name-field–specific patterns (horse_name / race_name / jockey):
# Even a single '?' followed by an ASCII letter is garbling in Japanese names.
# ─ '?A' through '?z' : CP932 2-byte lead byte → '?' + trail byte (ASCII)
# ─ half-width katakana (U+FF61-U+FF9F): illegitimate in Japanese proper names
# ─ curly quotes U+2018/2019/201C/201D, †, ‡ : CP932 bytes misread as punctuation
_NAME_GARBLED_RE = re.compile(
    r"\?[A-Za-z]"  # ?X: JVLink CP932 リードバイト脱落
    r"|[｡-ﾟ]"  # 半角カタカナ (U+FF61-U+FF9F)
    r"|[‘-‟†‡]"  # カーリークォート・ダガー (CP932誤読)
    r"|�"  # Unicode 置換文字
)

# Half-width katakana (U+FF61-FF9F) mixed with curly-quotes or ASCII '?' — JVLink artifact
# Proper Japanese text uses full-width katakana; half-width only appears in garbled CP932 strings
_HALFWIDTH_MIXED_RE = re.compile(r"[｡-ﾟ].*[\"\"''\x3f†]|[\"\"''\x3f†].*[｡-ﾟ]")

# Rare CJK characters that appear as incorrect recovery artifacts
_GARBLED_KANJI_RE = re.compile(
    "[窿噼穢磚穩窺侶頯煢]"
    # 窿噬穢磚穩窺侶頷煢 — none of these appear in Japanese horse names
)

# Valid Japanese character range for recovery quality check
_JAPANESE_RE = re.compile("[ぁ-ヿ一-鿿！-￯]")

# Encoding recovery paths: (wrong decoding used, correct decoding)
# mac_greek -> euc-jp: EUC-JP bytes misread as Mac Greek characters
# mac_roman -> euc-jp: EUC-JP bytes misread as Mac Roman characters
# latin-1   -> cp932:  CP932 bytes stored as ISO-8859-1
# cp1251    -> cp932:  CP932 bytes misread as Windows Cyrillic
_RECOVERY_PATHS: list[tuple[str, str]] = [
    ("mac_greek", "euc-jp"),
    ("mac_roman", "euc-jp"),
    ("latin-1", "cp932"),
    ("cp1252", "cp932"),
    ("cp1251", "cp932"),
]


def sanitize(v: object) -> object:
    """制御文字を除去してストリップする。文字列以外はそのまま返す。

    Args:
        v: サニタイズ対象の値。文字列以外の型はそのまま返す。

    Returns:
        文字列の場合は制御文字除去・strip 済みの文字列。それ以外は入力値をそのまま返す。
    """
    if not isinstance(v, str):
        return v
    return _CTRL_RE.sub("", v).strip()


def sanitize_str(v: str, fallback: str = "") -> str:
    """文字列専用のサニタイズ関数。None または空の場合は fallback を返す。

    Args:
        v: サニタイズ対象の文字列。
        fallback: ``v`` が None または空文字の場合に返すデフォルト値。デフォルトは空文字列。

    Returns:
        制御文字除去・strip 済みの文字列。空になった場合は ``fallback`` を返す。
    """
    if not v:
        return fallback
    return _CTRL_RE.sub("", v).strip() or fallback


def is_garbled(s: str) -> bool:
    """文字列が文字化けしているかどうかを判定する。

    以下のパターンを検出する:

    - ギリシャ文字 2 文字以上の連続（mac_greek → euc-jp 誤読アーティファクト）
    - 箇条書き記号・カーリークォート等の特殊句読点 2 文字以上の連続（mac_roman アーティファクト）
    - ``?X?X?X`` パターン（JVLink CP932 リードバイトが ``?`` に置換されたバグ）
    - 不正な encoding 回復処理によってのみ出現する稀な CJK 文字
    - U+0300-U+04FF / U+2000-U+21FF 範囲の文字が 30% 超

    Args:
        s: 判定対象の文字列。

    Returns:
        文字化けが疑われる場合は ``True``、それ以外は ``False``。
    """
    if not s:
        return False
    if _GARBLED_REPEAT_RE.search(s):
        return True
    if _BULLET_PATTERN_RE.search(s):
        return True
    if _JVLINK_QUESTION_RE.search(s):
        return True
    if _GARBLED_KANJI_RE.search(s):
        return True
    if _HALFWIDTH_MIXED_RE.search(s):
        return True
    suspicious = len(_GARBLED_BLOCK_RE.findall(s))
    return suspicious > 0 and suspicious / len(s) > 0.30


def is_garbled_name(s: str | None) -> bool:
    """馬名・競走名・騎手名フィールド専用の高感度文字化け検知。

    ``is_garbled()`` より厳格で、単発の ``?A`` パターン（JVLink CP932 リードバイトが
    ``?`` に置換されたもの）や半角カタカナだけでも文字化けと判定する。
    日本語の正規な名前にこれらのパターンは絶対に出現しない。

    Args:
        s: 判定対象の文字列。None または空文字は ``False`` を返す。

    Returns:
        名前フィールドとして不正な文字が含まれる場合 ``True``。
    """
    if not s:
        return False
    return bool(_NAME_GARBLED_RE.search(s))


def try_recover_encoding(s: str) -> str:
    """複数のエンコーディングパスを試して文字化け文字列の回復を試みる。

    ``_RECOVERY_PATHS`` に定義された ``(誤エンコード, 正エンコード)`` の各ペアを試す。
    以下の条件をすべて満たす場合のみ回復結果として採用する:

    - 結果に置換文字 U+FFFD が含まれない
    - 結果に日本語文字が 2 文字以上含まれる
    - 回復結果自体が文字化けしていない

    Args:
        s: 回復対象の文字列。文字化けしていない場合はそのまま返す。

    Returns:
        回復に成功した場合は回復済み文字列。失敗した場合は空文字列。
    """
    if not s or not is_garbled(s):
        return s

    best: str = ""
    best_score: int = 0

    for src_enc, tgt_enc in _RECOVERY_PATHS:
        try:
            recovered = s.encode(src_enc, errors="ignore").decode(
                tgt_enc, errors="replace"
            )
            # Reject partial recoveries that still contain replacement char
            if "�" in recovered:
                continue
            jp_count = len(_JAPANESE_RE.findall(recovered))
            # Require >= 2 Japanese chars and no residual garbling
            if jp_count >= 2 and not is_garbled(recovered) and jp_count > best_score:
                best = recovered.strip()
                best_score = jp_count
        except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
            continue

    return best


def ensure_clean(s: str | None, fallback: str = "") -> str:
    """DB 挿入前の最終ゲート処理を行う。

    以下の順序で処理する:

    1. ``None`` または空文字の場合は ``fallback`` を返す。
    2. 制御文字を除去して strip する。
    3. 文字化けが検出された場合は回復を試みる。回復不能な場合は ``fallback`` を返す。

    Args:
        s: クリーニング対象の文字列。``None`` も受け付ける。
        fallback: 空・文字化け不能時に返すデフォルト値。デフォルトは空文字列。

    Returns:
        クリーニング済みの文字列、または回復不能な場合は ``fallback``。
    """
    if not s:
        return fallback
    cleaned = _CTRL_RE.sub("", s).strip()
    if not cleaned:
        return fallback
    if is_garbled(cleaned):
        recovered = try_recover_encoding(cleaned)
        return recovered if recovered else fallback
    return cleaned


def try_recover_sjis(raw_bytes: bytes) -> str:
    """latin-1 として誤保存された SJIS データを CP932 としてデコードして回復する。

    JV-Link COM 文字列 → latin-1 バイト列 → cp932 デコードのパスを想定する。
    回復不能な文字は置換文字（U+FFFD）に置き換えられる。

    Args:
        raw_bytes: latin-1 として保存された CP932 バイト列。

    Returns:
        CP932 デコード済みの文字列（strip 済み）。デコード失敗時は空文字列。
    """
    try:
        return raw_bytes.decode("cp932", errors="replace").strip()
    except Exception:
        return ""
