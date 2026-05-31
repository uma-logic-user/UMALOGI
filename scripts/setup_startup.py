"""
scripts/setup_startup.py — Windows スタートアップへ UMALOGI_SCHEDULER.bat を登録

実行方法:
  py scripts/setup_startup.py            # インストール（デフォルト）
  py scripts/setup_startup.py --install  # インストール（明示）
  py scripts/setup_startup.py --uninstall # 削除

動作:
  --install : スタートアップフォルダに UMALOGI_Scheduler.vbs を配置。
              PC 再起動時に UMALOGI_SCHEDULER.bat を最小化状態（windowStyle=7）で
              自動起動する。

  --uninstall: UMALOGI_Scheduler.vbs をスタートアップフォルダから削除する。

Windows スタートアップフォルダ:
  %APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# ── パス定義 ──────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
BAT_PATH = REPO_ROOT / "UMALOGI_SCHEDULER.bat"

STARTUP_DIR = Path(
    os.path.expandvars(r"%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup")
)
VBS_NAME = "UMALOGI_Scheduler.vbs"
VBS_PATH = STARTUP_DIR / VBS_NAME


# ── VBScript テンプレート ──────────────────────────────────────────────────────


def _vbs_content(bat_path: Path) -> str:
    """BAT を最小化（windowStyle=7）で起動する VBScript を生成する。"""
    # 7 = Minimized / False = ウィンドウ完了を待たない（非同期）
    return f'Set oShell = CreateObject("WScript.Shell")\r\noShell.Run """{bat_path}""", 7, False\r\n'


# ── サブコマンド ──────────────────────────────────────────────────────────────


def install() -> None:
    """VBScript をスタートアップフォルダへ配置する。"""
    if not BAT_PATH.exists():
        print(f"[ERROR] BAT ファイルが見つかりません: {BAT_PATH}")
        sys.exit(1)

    STARTUP_DIR.mkdir(parents=True, exist_ok=True)

    content = _vbs_content(BAT_PATH)
    VBS_PATH.write_text(content, encoding="utf-8")

    print("[OK] スタートアップ登録完了")
    print(f"     VBS  : {VBS_PATH}")
    print(f"     BAT  : {BAT_PATH}")
    print()
    print("     PC 再起動時に UMALOGI_SCHEDULER.bat が最小化で自動起動します。")
    print("     削除する場合: py scripts/setup_startup.py --uninstall")


def uninstall() -> None:
    """スタートアップフォルダから VBScript を削除する。"""
    if VBS_PATH.exists():
        VBS_PATH.unlink()
        print(f"[OK] スタートアップ登録を削除しました: {VBS_PATH}")
    else:
        print(f"[SKIP] VBS ファイルが見つかりません（未登録）: {VBS_PATH}")


def status() -> None:
    """現在の登録状態を表示する。"""
    print("=== UMALOGI スタートアップ登録状態 ===")
    print(f"  VBS  : {VBS_PATH}")
    print(f"  BAT  : {BAT_PATH}")
    print()
    if VBS_PATH.exists():
        print("  状態: [登録済み] PC 起動時に自動実行されます")
        print()
        print("  --- VBS 内容 ---")
        print(VBS_PATH.read_text(encoding="utf-8"))
    else:
        print("  状態: [未登録] `py scripts/setup_startup.py` で登録できます")

    if not BAT_PATH.exists():
        print(f"\n  [WARNING] BAT ファイルが見つかりません: {BAT_PATH}")


# ── エントリーポイント ────────────────────────────────────────────────────────

if __name__ == "__main__":
    arg = sys.argv[1].lower() if len(sys.argv) > 1 else "--install"

    if arg in ("--install", "install"):
        install()
    elif arg in ("--uninstall", "uninstall"):
        uninstall()
    elif arg in ("--status", "status"):
        status()
    else:
        print(f"[ERROR] 不明な引数: {arg}")
        print("使用方法: py scripts/setup_startup.py [--install|--uninstall|--status]")
        sys.exit(1)
