"""
UMALOGI スケジューラー Windows 自動起動インストーラー

PC 再起動後もスケジューラーが自動で起動するよう
Windows タスクスケジューラにエントリを登録する。

Usage:
    py scripts/install_autostart.py install    # 登録
    py scripts/install_autostart.py uninstall  # 削除
    py scripts/install_autostart.py status     # 確認
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
_TASK_NAME = "UMALOGI-Scheduler"
_PY_CMD = sys.executable
_SCRIPT = str(_ROOT / "scripts" / "scheduler.py")


def _run_schtasks(args: list[str]) -> tuple[int, str]:
    """schtasks コマンドを実行して (returncode, output) を返す。"""
    result = subprocess.run(
        ["schtasks"] + args,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output


def install() -> None:
    """タスクスケジューラにログオン時起動エントリを登録する。"""
    print(f"タスク登録: {_TASK_NAME}")
    print(f"  インタープリタ: {_PY_CMD}")
    print(f"  スクリプト    : {_SCRIPT}")

    # 既存タスクを削除してから再登録（べき等）
    _run_schtasks(["/Delete", "/TN", _TASK_NAME, "/F"])

    rc, out = _run_schtasks(
        [
            "/Create",
            "/TN",
            _TASK_NAME,
            "/TR",
            f'"{_PY_CMD}" "{_SCRIPT}"',
            "/SC",
            "ONLOGON",
            "/RL",
            "HIGHEST",
            "/F",
        ]
    )

    if rc == 0:
        print("✅ 登録成功")
        print(f"   次回ログオン時に自動起動します: {_TASK_NAME}")
    else:
        print(f"❌ 登録失敗 (rc={rc})")
        print(out)
        sys.exit(1)


def uninstall() -> None:
    """タスクスケジューラからエントリを削除する。"""
    rc, out = _run_schtasks(["/Delete", "/TN", _TASK_NAME, "/F"])
    if rc == 0:
        print(f"✅ 削除完了: {_TASK_NAME}")
    else:
        print(f"❌ 削除失敗 (rc={rc}): {out}")


def status() -> None:
    """タスクの登録状態を確認する。"""
    rc, out = _run_schtasks(["/Query", "/TN", _TASK_NAME, "/FO", "LIST"])
    if rc == 0:
        print(f"✅ 登録済み: {_TASK_NAME}")
        print(out)
    else:
        print(f"⚠️  未登録: {_TASK_NAME}")
        print("   py scripts/install_autostart.py install  で登録できます。")


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI スケジューラー自動起動設定")
    parser.add_argument(
        "action",
        choices=["install", "uninstall", "status"],
        help="実行アクション",
    )
    args = parser.parse_args()
    actions: dict[str, object] = {
        "install": install,
        "uninstall": uninstall,
        "status": status,
    }
    actions[args.action]()  # type: ignore[operator]


if __name__ == "__main__":
    main()
