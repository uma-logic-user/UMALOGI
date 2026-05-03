"""Universal Text Sanitizer — 全DB挿入・API応答で必ず通す浄化ユーティリティ。"""

from __future__ import annotations
import re

# 除去対象: NUL + C0制御文字 + C1制御文字 (タブ・改行・CRは保持)
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def sanitize(v: object) -> object:
    """文字列ならば制御文字を除去してtrimする。非文字列はそのまま返す。"""
    if not isinstance(v, str):
        return v
    return _CTRL_RE.sub("", v).strip()


def sanitize_str(v: str, fallback: str = "") -> str:
    """str専用版。Noneや空文字のときはfallbackを返す。"""
    if not v:
        return fallback
    return _CTRL_RE.sub("", v).strip() or fallback


def try_recover_sjis(raw_bytes: bytes) -> str:
    """
    latin-1誤解釈で格納されたSJISデータの回復を試みる。

    JV-Link COM文字列 → latin-1バイト列 → cp932デコード
    回復できない文字は置換文字（）で代替する。
    """
    try:
        return raw_bytes.decode("cp932", errors="replace").strip()
    except Exception:
        return ""
