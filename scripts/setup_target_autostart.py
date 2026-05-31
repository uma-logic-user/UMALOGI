"""
TARGET frontier JV 完全自動起動セットアップ

実行するだけで以下をすべて自動化する:
  1. TARGET frontier JV 実行ファイルを自動探索
  2. Windows タスクスケジューラに「ログオン時自動起動」を登録
  3. Windows スタートアップフォルダにショートカットを配置（二重保険）
  4. JVLink 疎通確認 (JVInit=0 を検証)

Usage:
    py scripts/setup_target_autostart.py
    py scripts/setup_target_autostart.py --path "C:\\...\\TargetFrontierJV.exe"
    py scripts/setup_target_autostart.py --check-only
    py scripts/setup_target_autostart.py --uninstall
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Windows コンソールの文字化け防止
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parents[1]
_TASK_NAME = "UMALOGI_TargetFrontierJV"

# 既知インストールパス（優先度順）
# TARGET frontier JV が未インストールの場合は JVLinkAgent.exe（JRA-VAN Data Lab）を使用
_KNOWN_PATHS: list[Path] = [
    Path(r"C:\Program Files\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
    Path(r"C:\Program Files (x86)\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
    Path(r"C:\TARGET\TargetFrontierJV\TargetFrontierJV.exe"),
    Path(r"C:\JRA-VAN\TargetFrontierJV\TargetFrontierJV.exe"),
    Path(r"C:\Program Files\JRAVAN\TargetFrontierJV\TargetFrontierJV.exe"),
    # フォールバック: JRA-VAN Data Lab の JVLink エージェント
    Path(r"C:\Program Files (x86)\JRA-VAN\Data Lab\JVLinkAgent.exe"),
]

_STARTUP_DIR = (
    Path(os.environ.get("APPDATA", ""))
    / "Microsoft"
    / "Windows"
    / "Start Menu"
    / "Programs"
    / "Startup"
)


# ─────────────────────────────────────────────────────────────────────
# 探索
# ─────────────────────────────────────────────────────────────────────


def find_target_exe(override: str | None = None) -> Path | None:
    """TARGET frontier JV 実行ファイルを探索する。"""
    if override:
        p = Path(override)
        if p.exists():
            print(f"[OK] 指定パスを使用: {p}")
            return p
        print(f"[ERROR] 指定パスが存在しません: {p}", file=sys.stderr)
        return None

    # 既知パスを試す
    for path in _KNOWN_PATHS:
        if path.exists():
            print(f"[OK] TARGET JV 発見: {path}")
            return path

    # レジストリから検索
    try:
        import winreg

        search_keys = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\TARGET\TargetFrontierJV"),
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\WOW6432Node\TARGET\TargetFrontierJV",
            ),
            (winreg.HKEY_CURRENT_USER, r"SOFTWARE\TARGET\TargetFrontierJV"),
        ]
        for hive, key_path in search_keys:
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    for val_name in ("InstallPath", "Path", "ExePath", ""):
                        try:
                            install_dir, _ = winreg.QueryValueEx(key, val_name)
                            exe = Path(install_dir)
                            if exe.suffix.lower() != ".exe":
                                exe = exe / "TargetFrontierJV.exe"
                            if exe.exists():
                                print(f"[OK] レジストリから発見: {exe}")
                                return exe
                        except (FileNotFoundError, OSError):
                            continue
            except (FileNotFoundError, OSError):
                continue
    except ImportError:
        pass

    # where コマンドで PATH 検索
    try:
        result = subprocess.run(
            ["where", "TargetFrontierJV.exe"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            p = Path(result.stdout.strip().splitlines()[0])
            if p.exists():
                print(f"[OK] PATH から発見: {p}")
                return p
    except Exception:
        pass

    return None


# ─────────────────────────────────────────────────────────────────────
# タスクスケジューラ登録
# ─────────────────────────────────────────────────────────────────────


def register_task_scheduler(exe: Path) -> bool:
    """Windows タスクスケジューラにログオン時自動起動を登録する（冪等）。"""
    print(f"\n[タスクスケジューラ] 登録中: {_TASK_NAME}")

    # 既存タスクを削除して再登録（冪等性）
    subprocess.run(
        ["schtasks", "/Delete", "/TN", _TASK_NAME, "/F"],
        capture_output=True,
    )

    result = subprocess.run(
        [
            "schtasks",
            "/Create",
            "/TN",
            _TASK_NAME,
            "/TR",
            str(exe),
            "/SC",
            "ONLOGON",
            "/RL",
            "HIGHEST",
            "/DELAY",
            "0001:00",  # ログオン後1分待機（デスクトップ完全起動を待つ）
            "/F",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode == 0:
        print(f"[OK] タスクスケジューラ登録成功: {_TASK_NAME}")
        print("     トリガー: ログオン時 / 遅延: 1分 / 権限: 最高")
        return True
    else:
        stderr = result.stderr.strip() or result.stdout.strip()
        print(
            f"[WARN] タスクスケジューラ登録失敗 (管理者権限が必要な場合があります): {stderr}"
        )
        return False


def unregister_task_scheduler() -> bool:
    """タスクスケジューラから登録を削除する。"""
    result = subprocess.run(
        ["schtasks", "/Delete", "/TN", _TASK_NAME, "/F"],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode == 0:
        print(f"[OK] タスクスケジューラ削除: {_TASK_NAME}")
        return True
    else:
        print(f"[WARN] 削除失敗（登録されていない可能性）: {result.stderr.strip()}")
        return False


# ─────────────────────────────────────────────────────────────────────
# スタートアップショートカット
# ─────────────────────────────────────────────────────────────────────


def create_startup_shortcut(exe: Path) -> bool:
    """Windows スタートアップフォルダにショートカットを配置する（二重保険）。"""
    print(f"\n[スタートアップ] ショートカット作成中: {_STARTUP_DIR}")
    _STARTUP_DIR.mkdir(parents=True, exist_ok=True)

    # win32com による .lnk 作成を試みる
    try:
        import win32com.client  # type: ignore[import-untyped]

        shortcut_path = str(_STARTUP_DIR / "TargetFrontierJV.lnk")
        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(shortcut_path)
        shortcut.Targetpath = str(exe)
        shortcut.WorkingDirectory = str(exe.parent)
        shortcut.Description = "TARGET frontier JV (UMALOGI 自動起動)"
        shortcut.save()
        print(f"[OK] ショートカット作成: {shortcut_path}")
        return True
    except (ImportError, Exception) as e:
        print(f"[INFO] win32com 未インストール → .bat フォールバック: {e}")

    # フォールバック: .bat をスタートアップに配置
    try:
        bat_path = _STARTUP_DIR / "start_target_jv.bat"
        bat_path.write_text(
            f'@echo off\r\ntimeout /t 60 /nobreak > NUL\r\nstart "" "{exe}"\r\n',
            encoding="utf-8",
        )
        print(f"[OK] スタートアップ bat 作成: {bat_path}")
        return True
    except Exception as bat_exc:
        print(f"[ERROR] スタートアップ bat 作成失敗: {bat_exc}")
        return False


def remove_startup_shortcut() -> None:
    """スタートアップフォルダからショートカット・batを削除する。"""
    for name in ["TargetFrontierJV.lnk", "start_target_jv.bat"]:
        p = _STARTUP_DIR / name
        if p.exists():
            p.unlink()
            print(f"[OK] スタートアップ削除: {p}")


# ─────────────────────────────────────────────────────────────────────
# .env への TARGET_JV_PATH 記録
# ─────────────────────────────────────────────────────────────────────


def save_exe_to_env(exe: Path) -> None:
    """TARGET_JV_PATH を .env に追記して scheduler.py のウォッチドッグが参照できるようにする。"""
    env_path = _ROOT / ".env"
    key = "TARGET_JV_PATH"
    value = str(exe).replace("\\", "/")

    try:
        lines: list[str] = []
        if env_path.exists():
            lines = env_path.read_text(encoding="utf-8").splitlines()

        # 既存の TARGET_JV_PATH 行を更新または追加
        updated = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}=") or line.startswith(f"# {key}="):
                lines[i] = f"{key}={value}"
                updated = True
                break
        if not updated:
            lines.append(f"{key}={value}")

        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[OK] .env に {key}={value} を記録しました")
    except Exception as e:
        print(f"[WARN] .env 更新失敗: {e}")


# ─────────────────────────────────────────────────────────────────────
# JVLink 疎通確認
# ─────────────────────────────────────────────────────────────────────


def check_jvlink(launch_if_needed: bool = False, exe: Path | None = None) -> int:
    """
    JVLink 疎通確認。

    Returns:
        0  : JVInit=0 (成功)
        -4 : TARGET JV 未起動
        -99: 確認失敗（win32com等の問題）
    """
    print("\n[JVLink] 疎通確認中...")

    try:
        result = subprocess.run(
            [
                "py",
                "-3.14-32",
                "-c",
                (
                    "import win32com.client, sys;"
                    "jv=win32com.client.Dispatch('JVDTLab.JVLink.1');"
                    "ret=jv.JVInit('UNKNOWN');"
                    "print(f'JVInit={ret}');"
                    "jv.JVClose();"
                    "sys.exit(0 if ret==0 else abs(ret))"
                ),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
        stdout = result.stdout.strip()
        print(f"[JVLink] 出力: {stdout or result.stderr.strip()}")

        if "JVInit=0" in stdout:
            print("[OK] JVLink 認証成功 ✓")
            return 0
        elif "JVInit=-4" in stdout:
            print("[WARN] JVInit=-4 → TARGET frontier JV が起動していません")
            if launch_if_needed and exe and exe.exists():
                print(f"[INFO] TARGET JV を起動します: {exe}")
                subprocess.Popen([str(exe)])
                print("[INFO] 30秒後に再確認...")
                time.sleep(30)
                return check_jvlink(launch_if_needed=False)
            return -4
        else:
            print(f"[WARN] JVLink 応答不明: {stdout}")
            return -99
    except subprocess.TimeoutExpired:
        print("[ERROR] JVLink 確認タイムアウト（15秒）")
        return -99
    except Exception as e:
        print(f"[ERROR] JVLink 確認失敗: {e}")
        return -99


# ─────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TARGET frontier JV 自動起動完全セットアップ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "例:\n"
            "  py scripts/setup_target_autostart.py              # 自動検索して登録\n"
            "  py scripts/setup_target_autostart.py --check-only # JVLink 疎通確認のみ\n"
            "  py scripts/setup_target_autostart.py --uninstall  # 登録解除\n"
        ),
    )
    parser.add_argument("--path", help="TARGET JV 実行ファイルの明示パス")
    parser.add_argument(
        "--check-only", action="store_true", help="JVLink 疎通確認のみ実行"
    )
    parser.add_argument(
        "--uninstall", action="store_true", help="自動起動登録を解除する"
    )
    args = parser.parse_args()

    if args.uninstall:
        unregister_task_scheduler()
        remove_startup_shortcut()
        print("\n[完了] 自動起動登録を解除しました。")
        return

    if args.check_only:
        code = check_jvlink()
        sys.exit(0 if code == 0 else 1)

    print("=" * 62)
    print("  TARGET frontier JV 完全自動起動セットアップ")
    print("=" * 62)

    exe = find_target_exe(args.path)
    if exe is None:
        print(
            "\n[ERROR] TARGET frontier JV の実行ファイルが見つかりません。\n"
            "  --path オプションで明示的に指定してください:\n"
            '  py scripts/setup_target_autostart.py --path "C:\\...\\TargetFrontierJV.exe"',
            file=sys.stderr,
        )
        sys.exit(1)

    # .env に記録（ウォッチドッグが参照する）
    save_exe_to_env(exe)

    # タスクスケジューラ登録
    ok_task = register_task_scheduler(exe)

    # スタートアップショートカット（二重保険）
    ok_startup = create_startup_shortcut(exe)

    # JVLink 疎通確認（TARGET JV が起動していない場合は自動起動を試みる）
    code = check_jvlink(launch_if_needed=True, exe=exe)

    print("\n" + "=" * 62)
    print("  セットアップ結果")
    print("=" * 62)
    print(
        f"  タスクスケジューラ登録 : {'✓ 成功' if ok_task else '✗ 失敗（手動設定が必要）'}"
    )
    print(f"  スタートアップ配置     : {'✓ 成功' if ok_startup else '✗ 失敗'}")
    print(
        f"  JVLink 疎通             : {'✓ OK (JVInit=0)' if code == 0 else f'✗ 失敗 (code={code})'}"
    )
    print()

    if ok_task or ok_startup:
        print("→ 次回 Windows ログオン時から TARGET frontier JV が自動起動します。")
        print("  ウォッチドッグ機能が稼働中にクラッシュしても自動再起動されます。")
    else:
        print("→ 自動起動の設定に失敗しました。管理者として実行してください:")
        print("  runas /user:Administrator 'py scripts/setup_target_autostart.py'")

    sys.exit(0 if (ok_task or ok_startup) else 1)


if __name__ == "__main__":
    main()
