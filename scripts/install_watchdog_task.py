# -*- coding: utf-8 -*-
"""
UMALOGI watchdog を Windows タスクスケジューラに登録する。

【標準実行（現在ユーザー権限）】
    py scripts/install_watchdog_task.py

【最上位権限での登録（推奨）】
    右クリック→「管理者として実行」で開いたターミナルで:
    py scripts/install_watchdog_task.py

タスク仕様:
  - 名前        : UMALOGI_Watchdog
  - 実行ファイル : 現在の Python (64bit)
  - スクリプト  : scripts/watchdog.py --interval 5
  - 起動条件    : 5分おき（毎分監視・重複起動なし）
  - 実行権限    : 管理者ありの場合 = HighestAvailable、なし = CurrentUser
  - 失敗時再起動: 1分後に最大 999 回
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
_PYTHON = sys.executable
_SCRIPT = _ROOT / "scripts" / "watchdog.py"
_TASK = "UMALOGI_Watchdog"
_LOG_DIR = _ROOT / "logs"
_LOG_DIR.mkdir(exist_ok=True)


def _is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())  # type: ignore[attr-defined]
    except Exception:
        return False


def _register_via_schtasks(highest: bool) -> bool:
    """schtasks コマンドでタスクを登録する。"""
    import datetime

    now = datetime.datetime.now().strftime("%H:%M")

    # 既存タスクを削除
    subprocess.run(
        ["schtasks", "/Delete", "/TN", _TASK, "/F"],
        capture_output=True,
        encoding="cp932",
        errors="replace",
    )

    cmd = [
        "schtasks",
        "/Create",
        "/TN",
        _TASK,
        "/SC",
        "MINUTE",
        "/MO",
        "5",
        "/ST",
        now,
        "/TR",
        f'{_PYTHON} "{_SCRIPT}" --interval 5',
        "/F",
    ]
    if highest:
        cmd += ["/RL", "HIGHEST"]

    result = subprocess.run(
        cmd,
        capture_output=True,
        encoding="cp932",
        errors="replace",
        timeout=20,
    )
    if result.returncode == 0:
        print(f"  schtasks 登録成功 (最上位権限={highest})")
        return True
    print(f"  schtasks 失敗 rc={result.returncode}: {result.stderr.strip()[:200]}")
    return False


def _register_via_powershell() -> bool:
    """PowerShell Register-ScheduledTask で登録（BootTrigger + Repetition 対応）。"""
    xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>UMALOGI 自己修復ウォッチドッグ: 5分おきOdds欠損を監視・自動補完</Description>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
      <Delay>PT2M</Delay>
      <Repetition>
        <Interval>PT5M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
    </BootTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
    <StartWhenAvailable>true</StartWhenAvailable>
  </Settings>
  <Actions>
    <Exec>
      <Command>{_PYTHON}</Command>
      <Arguments>"{_SCRIPT}" --interval 5</Arguments>
      <WorkingDirectory>{_ROOT}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""

    with tempfile.NamedTemporaryFile(
        suffix=".xml",
        delete=False,
        mode="w",
        encoding="utf-16",
    ) as f:
        f.write(xml)
        tmp_path = f.name

    try:
        # 既存タスク削除
        subprocess.run(
            [
                "powershell",
                "-NonInteractive",
                "-Command",
                f'Unregister-ScheduledTask -TaskName "{_TASK}" -Confirm:$false -ErrorAction SilentlyContinue',
            ],
            capture_output=True,
            timeout=15,
        )
        result = subprocess.run(
            ["schtasks", "/Create", "/TN", _TASK, "/XML", tmp_path, "/F"],
            capture_output=True,
            encoding="cp932",
            errors="replace",
            timeout=30,
        )
        if result.returncode == 0:
            print("  XML登録成功 (BootTrigger + 5分おき Repetition)")
            return True
        print(f"  XML登録失敗 rc={result.returncode}: {result.stderr.strip()[:200]}")
        return False
    finally:
        os.unlink(tmp_path)


def main() -> None:
    admin = _is_admin()

    print("=" * 55)
    print("  UMALOGI watchdog タスクスケジューラ登録")
    print("=" * 55)
    print(f"  Python  : {_PYTHON}")
    print(f"  Script  : {_SCRIPT}")
    print(f"  WorkDir : {_ROOT}")
    print(
        f"  管理者  : {'はい (最上位権限で登録)' if admin else 'いいえ (標準権限で登録)'}"
    )
    print()

    # 管理者ありなら XML+BootTrigger で完全版を試みる
    ok = False
    if admin:
        print("[1/2] XML方式 (BootTrigger + HighestAvailable) を試みます...")
        ok = _register_via_powershell()
    if not ok:
        print("[2/2] schtasks 方式 (5分間隔) を試みます...")
        ok = _register_via_schtasks(highest=admin)

    if not ok:
        print()
        print("❌ 登録失敗。管理者権限のターミナルから再実行してください:")
        print(f'   py "{__file__}"')
        sys.exit(1)

    # 即時起動
    run_result = subprocess.run(
        ["schtasks", "/Run", "/TN", _TASK],
        capture_output=True,
        encoding="cp932",
        errors="replace",
        timeout=15,
    )
    if run_result.returncode == 0:
        print("  watchdog を即時起動しました ✓")
    else:
        print(f"  即時起動: {run_result.stderr.strip()[:100]}")

    print()
    print("=" * 55)
    if admin:
        print("✅ 完全登録完了 (最上位権限 / PC起動時+5分おき)")
    else:
        print("✅ 標準登録完了 (5分おき)")
        print()
        print(">>> 最上位権限で登録するには:")
        print("    1. スタートメニューで「cmd」を右クリック →「管理者として実行」")
        print(f'    2. py "{__file__}"')
    print("=" * 55)

    # 登録確認
    query = subprocess.run(
        ["schtasks", "/Query", "/TN", _TASK, "/FO", "LIST"],
        capture_output=True,
        encoding="cp932",
        errors="replace",
        timeout=10,
    )
    if query.returncode == 0:
        for line in query.stdout.splitlines()[:8]:
            print(" ", line)


if __name__ == "__main__":
    main()
