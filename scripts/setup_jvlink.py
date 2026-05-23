"""
JVLink 初期設定スクリプト v3

【使い方】
  py -3.14-32 scripts/setup_jvlink.py

  ※ 必ず 32bit Python で実行すること（JVLink COM は 32bit 専用）。
  ※ ダイアログが表示されたら操作を完了させてください。

【UAC について】
  このスクリプトは UAC 自動昇格を行いません。
  JVLink の設定は HKCU（現ユーザー）に書き込まれるため管理者権限は不要です。
  万一 HKLM への書き込みが必要な場合は、コマンドプロンプトを管理者として開いて実行してください。
"""

from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path

# UTF-8 強制（クラッシュしない方法）
try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
except Exception:
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass  # 最悪そのまま続行


def _p(msg: str = "") -> None:
    print(msg, flush=True)


# ── 32bit チェック ────────────────────────────────────────────────────────
if sys.maxsize > 2**32:
    _p("=" * 60)
    _p("ERROR: 32bit Python で実行してください。")
    _p("  py -3.14-32 scripts/setup_jvlink.py")
    _p("=" * 60)
    sys.exit(1)

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv  # type: ignore[import]
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

_SID = os.getenv("JRAVAN_SID", "")

# ── レジストリ探索パス ─────────────────────────────────────────────────────
try:
    import winreg  # type: ignore[import]
    _HKCU = winreg.HKEY_CURRENT_USER
    _HKLM = winreg.HKEY_LOCAL_MACHINE
    _REG_PATHS = [
        (_HKCU, r"Software\JVDTLab\JVLink"),
        (_HKCU, r"Software\JVLink"),
        (_HKCU, r"Software\JRA-VAN\JVLink"),
        (_HKLM, r"SOFTWARE\JVDTLab\JVLink"),
        (_HKLM, r"SOFTWARE\WOW6432Node\JVDTLab\JVLink"),
        (_HKLM, r"SOFTWARE\JVLink"),
        (_HKLM, r"SOFTWARE\WOW6432Node\JVLink"),
    ]
    _HIVE = {_HKCU: "HKCU", _HKLM: "HKLM"}
    _HAS_WINREG = True
except ImportError:
    _HAS_WINREG = False


def _scan_registry() -> dict[str, dict[str, str]]:
    if not _HAS_WINREG:
        return {}
    results: dict[str, dict[str, str]] = {}
    for hive, path in _REG_PATHS:
        label = f"{_HIVE[hive]}\\{path}"
        try:
            key = winreg.OpenKey(hive, path)
            vals: dict[str, str] = {}
            i = 0
            while True:
                try:
                    name, data, _ = winreg.EnumValue(key, i)
                    vals[name] = str(data)
                    i += 1
                except OSError:
                    break
            winreg.CloseKey(key)
            results[label] = vals
        except FileNotFoundError:
            results[label] = {}
        except PermissionError:
            results[label] = {"_perm": "権限なし"}
        except Exception as e:
            results[label] = {"_err": str(e)}
    return results


def _has_data(reg: dict[str, dict[str, str]]) -> bool:
    return any(
        bool(v) and not all(k.startswith("_") for k in v)
        for v in reg.values()
    )


def _print_reg(reg: dict[str, dict[str, str]], label: str) -> None:
    _p(label)
    found = False
    for path, vals in reg.items():
        real_vals = {k: v for k, v in vals.items() if not k.startswith("_")}
        if real_vals:
            found = True
            _p(f"  ✅ [{path}]")
            for k, v in real_vals.items():
                _p(f"     {k} = {v!r}")
        elif vals:
            _p(f"  ⚠️  [{path}] → {next(iter(vals.values()))}")
    if not found:
        _p("  （全パスでキーなし）")


def _jvinit_msg(code: int) -> str:
    return {
        0:    "✅ 成功",
        -1:   "❌ COM エラー（JVLink未インストール？）",
        -2:   "❌ SID が空またはフォーマット不正",
        -3:   "❌ SID 未登録（利用キー設定が必要）",
        -4:   "❌ TARGET frontier JV 未起動/未ログイン",
        -5:   "❌ JVLink バージョン古い",
        -9:   "❌ ライセンスエラー",
        -202: "❌ サービス未登録",
    }.get(code, f"❓ 不明 (code={code})")


def main() -> None:
    _p("=" * 60)
    _p("JVLink 初期設定スクリプト v3")
    _p("=" * 60)
    _p(f"Python : {sys.version.split()[0]} ({'32bit' if sys.maxsize <= 2**32 else '64bit(エラー)'})")
    _p(f"SID    : {repr(_SID) if _SID else '（未設定）'}")
    _p()

    # ── SID 確認 ──────────────────────────────────────────────────
    sid = _SID
    if not sid:
        _p("ERROR: .env に JRAVAN_SID が未設定です。")
        _p("       .env に JRAVAN_SID=あなたのSID を追記して再実行してください。")
        sys.exit(1)

    # ── 事前レジストリ ─────────────────────────────────────────────
    _p("─" * 60)
    _p("[Step 1/4] 事前レジストリスキャン")
    _p("─" * 60)
    reg_before = _scan_registry()
    _print_reg(reg_before, "現在のレジストリ状態:")
    _p()

    # ── ダイアログハンドラー事前起動 ───────────────────────────────────
    _p("─" * 60)
    _p("[Step 1.5/4] JVLink ダイアログハンドラー起動")
    _p("─" * 60)
    _p("  COM 生成前にダイアログ自動突破ハンドラーを起動します。")
    _p("  ⚠️  注意: JVLink が「ブラウザ」を開く場合があります。")
    _p("       → JRA-VAN 認証ページが開いたらログインを完了してブラウザを閉じてください。")
    _p("       → 手動でブラウザを閉じた後、このターミナルで Enter を押してください。")
    _p("       → UMALOGI ダッシュボードが表示中のブラウザは別ウィンドウにしておくと安全です。")
    _p()
    try:
        # sys.path にプロジェクトルートが入っていることを前提
        from src.ops.jvlink_dialog_handler import start_dialog_handler
        start_dialog_handler(interval=0.3)
        _p("  ダイアログハンドラー: 起動済み（Win32ダイアログを自動突破）")
    except Exception as _dh_exc:
        _p(f"  ダイアログハンドラー: スキップ（{_dh_exc}）")
    _p()

    # ── COM 生成 ───────────────────────────────────────────────────
    _p("─" * 60)
    _p("[Step 2/4] JVLink COM 生成")
    _p("─" * 60)
    try:
        import win32com.client  # type: ignore[import]
    except ImportError:
        _p("ERROR: pywin32 が未インストールです。")
        _p("  py -3.14-32 -m pip install pywin32")
        sys.exit(1)

    _p("  JVDTLab.JVLink.1 を生成中...", )
    try:
        jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
        _p("  COM 生成: OK")
    except Exception as e:
        _p(f"  COM 生成: FAILED → {e}")
        _p()
        _p("  TARGET frontier JV が正しくインストールされているか確認してください。")
        sys.exit(1)

    # COM 生成直後にダイアログ抑制フラグを可能な限り設定する
    # （JVInit 呼び出し前に実行することで初期化中のポップアップを抑制）
    for _api, _call in [
        ("ParentHWnd", lambda: setattr(jvl, "ParentHWnd", 0)),
        ("JVSetDialog(False)", lambda: jvl.JVSetDialog(False)),
        ("JVSetAutoDownload(True)", lambda: jvl.JVSetAutoDownload(True)),
    ]:
        try:
            _call()
            _p(f"  {_api}: OK（ダイアログ抑制）")
        except Exception:
            pass  # 非対応バージョンは無視
    _p()

    # ── JVInit（ダイアログ許可）────────────────────────────────────
    _p("─" * 60)
    _p("[Step 3/4] JVInit 実行（ダイアログ許可モード）")
    _p("─" * 60)
    _p("  Win32ダイアログが表示されたら操作を完了させてください。")
    _p("  ブラウザが開いた場合はログイン完了後にブラウザを閉じてください。")
    _p(f"  JVInit(sid={sid!r}) 呼び出し中... ← ここで止まる場合はダイアログを操作してください")
    _p()

    jvinit_ret = jvl.JVInit(sid)
    _p(f"  JVInit 戻り値: {jvinit_ret} → {_jvinit_msg(jvinit_ret)}")

    if jvinit_ret == -4:
        _p()
        _p("  ★ TARGET frontier JV を起動してログインしてから再実行してください。")
        _p("    再実行: py -3.14-32 scripts/setup_jvlink.py")
        sys.exit(1)
    elif jvinit_ret == -3 or jvinit_ret == -202:
        _p()
        _p("  ★ JRA-VAN のウェブサイトでサービスIDを確認・登録してください。")
        _p("    https://jra-van.jp/")
        sys.exit(1)
    elif jvinit_ret != 0:
        _p()
        _p(f"  ★ JVInit 失敗。原因を確認して再実行してください。")
        _p("    再実行: py -3.14-32 scripts/setup_jvlink.py")
        sys.exit(1)

    _p()
    _p("  ダイアログが表示された場合は操作完了後に続けてください。")
    input("  【確認】操作が完了したら Enter を押してください: ")
    _p()

    # ── 事後レジストリ + JVOpen テスト ─────────────────────────────
    _p("─" * 60)
    _p("[Step 4/4] 動作確認")
    _p("─" * 60)

    time.sleep(0.5)
    reg_after = _scan_registry()
    diffs = {
        path: vals for path, vals in reg_after.items()
        if vals != reg_before.get(path, {}) and vals and not all(k.startswith("_") for k in vals)
    }
    if diffs:
        _p("  ✅ レジストリに変化を検出:")
        for path, vals in diffs.items():
            _p(f"     [{path}]")
            for k, v in vals.items():
                _p(f"       {k} = {v!r}")
    else:
        _print_reg(reg_after, "  事後レジストリ状態:")

    _p()
    _p("  JVOpen テスト中（OPT_STORED + RACE）...")
    jvlink_ok = False
    try:
        from datetime import date, timedelta
        fromtime = (date.today() - timedelta(days=14)).strftime("%Y%m%d000000")
        open_ret = jvl.JVOpen("RACE", fromtime, 4, 0, 0, "")
        _p(f"  JVOpen 戻り値: {open_ret}")
        if open_ret >= -1:  # 0=OK, -1=データなし（どちらも正常）
            _p("  ✅ JVOpen 成功 — JVLink が動作しています。")
            jvl.JVClose()
            jvlink_ok = True
        else:
            _p(f"  ⚠️  JVOpen 失敗 (ret={open_ret})")
    except Exception as e:
        _p(f"  JVOpen テスト失敗: {e}")

    # ── 最終判定（厳格）──────────────────────────────────────────
    _p()
    _p("=" * 60)
    if jvinit_ret == 0 and jvlink_ok:
        _p("✅ JVLink 初期設定 完了")
        _p()
        _p("  次のステップ: .env の JVLINK_DISABLED=1 を削除または 0 に変更してください。")
        _p("  → その後 scheduler.py が自動的に JVLink を使用します。")
    elif jvinit_ret == 0 and not jvlink_ok:
        _p("ERROR: JVInit は成功しましたが JVOpen が失敗しました。")
        _p()
        _p("  考えられる原因:")
        _p("  1. TARGET frontier JV のログインが必要")
        _p("  2. インターネット接続なし")
        _p("  → TARGET frontier JV を起動してログイン後、再実行してください。")
        sys.exit(1)
    else:
        _p(f"ERROR: JVLink 初期設定 失敗 (JVInit ret={jvinit_ret})")
        sys.exit(1)
    _p("=" * 60)


if __name__ == "__main__":
    main()
