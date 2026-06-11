"""src/ui — CUI（ターミナル）装飾レイヤー。

rich ベースの「ハッカーのコックピット」風コンソール出力ラッパーを提供する。
rich 未インストール環境でも plain フォールバックで完全動作する。
"""

from src.ui.console import RICH_AVAILABLE, UmaConsole, get_console

__all__ = ["RICH_AVAILABLE", "UmaConsole", "get_console"]
