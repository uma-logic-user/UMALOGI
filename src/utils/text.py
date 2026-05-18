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
    """Remove control chars and strip. Pass through non-strings unchanged."""
    if not isinstance(v, str):
        return v
    return _CTRL_RE.sub("", v).strip()


def sanitize_str(v: str, fallback: str = "") -> str:
    """str-only version. Returns fallback for None/empty."""
    if not v:
        return fallback
    return _CTRL_RE.sub("", v).strip() or fallback


def is_garbled(s: str) -> bool:
    """
    Return True if the string appears to be garbled text.

    Detects:
      - 2+ consecutive Greek characters (mac_greek->euc-jp misread artifact)
      - 2+ consecutive special punctuation like bullet/curly-quotes (mac_roman artifact)
      - "?X?X?X" pattern where '?' replaced Shift-JIS lead bytes (JVLink CP932 bug)
      - Rare CJK chars that appear only from incorrect encoding recovery
      - >30% of chars in U+0300-U+04FF / U+2000-U+21FF range
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


def try_recover_encoding(s: str) -> str:
    """
    Attempt to recover a garbled string by trying multiple encoding paths.

    Tries each (wrong_encoding, correct_encoding) pair. Accepts a recovery only
    when:
      - The result contains no replacement character U+FFFD
      - The result contains at least 2 Japanese characters
      - The result itself is not garbled

    Returns the best recovery found, or empty string if none works.
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
    """
    Final gate before DB insertion.
    1. None / empty -> fallback
    2. Strip control characters
    3. If garbled: attempt recovery; if unrecoverable -> fallback
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
    """
    Attempt to recover SJIS data that was incorrectly stored as latin-1.

    JV-Link COM string -> latin-1 bytes -> cp932 decode
    Unrecoverable characters are replaced with the substitution character.
    """
    try:
        return raw_bytes.decode("cp932", errors="replace").strip()
    except Exception:
        return ""
