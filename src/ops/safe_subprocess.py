"""
src/ops/safe_subprocess.py — CP932 デコード事故に強い subprocess ラッパー

背景（2026-06-14 障害 / v1.16.0-dev）:
  `subprocess.run(..., stdout=PIPE, text=True, encoding="utf-8")` を **errors 未指定
  （= "strict"）**で使うと、子プロセス（JVLink 32bit ワーカー等）が CP932 のリードバイト
  （例: 0x83）を吐いた瞬間に Python のリーダースレッド `_readerthread` が
  `UnicodeDecodeError` で死亡する。リーダースレッドが死ぬとパイプが drain されず、
  子プロセスが書き込みでブロック → 親は communicate() で待ち続け → タイムアウト発火を
  毎周期くり返す（＝サイレント・ハング）。

  本ラッパーは「捕捉する subprocess は必ず errors='replace' を強制する」ことで、
  この事故クラス全体を構造的に封じる。CLAUDE.md 第6/10/16条（UTF-8 強制・文字化け
  スクリーニング）の subprocess 境界版。
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def safe_run(
    cmd: Sequence[str],
    *,
    cwd: str | Path | None = None,
    timeout: float | None = None,
    capture: bool = True,
    check: bool = False,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """子プロセス出力を **必ず errors='replace' の UTF-8 テキスト**で捕捉して実行する。

    CP932 を吐く子プロセスでもリーダースレッドが落ちず、パイプが確実に drain される。

    Args:
        cmd: 実行コマンド（リスト）。
        cwd: 作業ディレクトリ。
        timeout: タイムアウト秒。
        capture: True なら stdout/stderr を捕捉（既定）。False なら親へ継承。
        check: True なら非ゼロ終了で CalledProcessError を送出。
        **kwargs: subprocess.run への追加引数。encoding/errors/text は上書きされる。

    Returns:
        CompletedProcess（capture=True なら stdout/stderr は str）。
    """
    # 捕捉時はテキスト復号を errors='replace' で固定（呼び出し側の指定を無視して強制）。
    kwargs.pop("encoding", None)
    kwargs.pop("errors", None)
    kwargs.pop("text", None)
    kwargs.pop("universal_newlines", None)

    if capture:
        kwargs.setdefault("stdout", subprocess.PIPE)
        kwargs.setdefault("stderr", subprocess.PIPE)

    return subprocess.run(  # noqa: S603 — 呼び出し側が信頼できるコマンドを渡す前提
        list(cmd),
        cwd=str(cwd) if cwd is not None else None,
        timeout=timeout,
        check=check,
        encoding="utf-8",
        errors="replace",
        text=True,
        **kwargs,
    )
