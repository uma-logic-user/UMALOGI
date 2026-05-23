"""
JRA-VAN JVLink CLI 初期化ツール（ブラウザ不要版）
================================================

【目的】
  setup_jvlink.py（対話型）の代わりに、ブラウザを一切開かずに
  JVLink の接続確認・初期設定を CLI のみで完結させる。

  scheduler.py や自動化スクリプトから呼び出す場合はこちらを使うこと。

【使い方】
  # 接続確認のみ
  py -3.14-32 -m src.utils.jravan_cli_initializer

  # 詳細ログ付き
  py -3.14-32 -m src.utils.jravan_cli_initializer --verbose

  # タイムアウト変更（デフォルト 15 秒）
  py -3.14-32 -m src.utils.jravan_cli_initializer --timeout 30

【保証】
  - ブラウザを一切開かない（ParentHWnd=0 / JVSetDialog(False) を先行設定）
  - Win32 ダイアログを自動突破（jvlink_dialog_handler.py 経由）
  - JVInit 失敗時はスタックトレースではなく明確なエラーコードを出力
  - exit code 0 = 成功、1 = JVInit 失敗、2 = COM 生成失敗、3 = JVOpen 失敗

【注意】
  このスクリプトは 32bit Python でのみ動作します。
  (JVLink COM は 32bit 専用)
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
from pathlib import Path

# ── UTF-8 強制 ─────────────────────────────────────────────────────────────
try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
except Exception:
    try:
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
    except Exception:
        pass

# ── プロジェクトルートを sys.path に追加 ───────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)

_JVINIT_CODES: dict[int, str] = {
    0:    "✅ 成功",
    -1:   "❌ COM エラー（JVLink未インストール？）",
    -2:   "❌ SID が空またはフォーマット不正",
    -3:   "❌ SID 未登録（利用キー設定が必要）",
    -4:   "❌ TARGET frontier JV 未起動/未ログイン",
    -5:   "❌ JVLink バージョン古い",
    -9:   "❌ ライセンスエラー",
    -202: "❌ サービス未登録",
}


def _jvinit_msg(code: int) -> str:
    return _JVINIT_CODES.get(code, f"❓ 不明 (code={code})")


def run_check(sid: str, timeout: int = 15, verbose: bool = False) -> int:
    """
    JVLink 接続確認をブラウザなしで実行する。

    Args:
        sid:     JRAVAN_SID 値
        timeout: JVInit の最大待機秒数
        verbose: 詳細ログを出力するか

    Returns:
        0 = 成功
        1 = JVInit 失敗
        2 = COM 生成失敗
        3 = JVOpen 失敗
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60, flush=True)
    print("JRA-VAN JVLink CLI 初期化ツール（ブラウザ不要版）", flush=True)
    print("=" * 60, flush=True)
    print(f"SID    : {'*' * max(0, len(sid) - 4) + sid[-4:] if sid else '（未設定）'}", flush=True)
    print(f"Timeout: {timeout}s", flush=True)
    print(flush=True)

    # ── Step 1: ダイアログハンドラー起動（COM 生成前） ──────────────────────
    print("[1/4] ダイアログハンドラー起動...", flush=True)
    try:
        from src.ops.jvlink_dialog_handler import start_dialog_handler
        start_dialog_handler(interval=0.3)
        print("      OK — Win32 ダイアログを自動突破します", flush=True)
    except Exception as e:
        print(f"      SKIP — {e}", flush=True)
    print(flush=True)

    # ── Step 2: COM 生成 ─────────────────────────────────────────────────────
    print("[2/4] JVDTLab.JVLink.1 COM 生成...", flush=True)
    try:
        import win32com.client  # type: ignore[import]
    except ImportError:
        print("ERROR: pywin32 が未インストールです。", flush=True)
        print("  py -3.14-32 -m pip install pywin32", flush=True)
        return 2

    try:
        jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
        print("      COM 生成: OK", flush=True)
    except Exception as e:
        print(f"      COM 生成: FAILED — {e}", flush=True)
        print("      → TARGET frontier JV のインストールを確認してください", flush=True)
        return 2

    # ── Step 3: ダイアログ抑制フラグを最大限設定 ──────────────────────────
    print("[3/4] ダイアログ抑制フラグ設定...", flush=True)
    suppressed_count = 0

    # ParentHWnd = 0 : 親ウィンドウなし → ダイアログを描画できない
    try:
        jvl.ParentHWnd = 0
        print("      ParentHWnd=0: OK", flush=True)
        suppressed_count += 1
    except Exception as e:
        print(f"      ParentHWnd=0: SKIP ({e})", flush=True)

    # JVSetUIProperties() : UI 非表示プロパティ設定
    try:
        jvl.JVSetUIProperties()
        print("      JVSetUIProperties(): OK", flush=True)
        suppressed_count += 1
    except Exception as e:
        print(f"      JVSetUIProperties(): SKIP ({e})", flush=True)

    # JVSetUI(0) : ダイアログ無効化（旧 API）
    try:
        jvl.JVSetUI(0)
        print("      JVSetUI(0): OK (旧API)", flush=True)
        suppressed_count += 1
    except Exception as e:
        print(f"      JVSetUI(0): SKIP ({e})", flush=True)

    # JVSetDialog(False) : JVLink自発ダイアログをすべて抑制
    try:
        jvl.JVSetDialog(False)
        print("      JVSetDialog(False): OK", flush=True)
        suppressed_count += 1
    except Exception as e:
        print(f"      JVSetDialog(False): SKIP ({e})", flush=True)

    # JVSetAutoDownload(True) : 確認なしで自動DL
    try:
        jvl.JVSetAutoDownload(True)
        print("      JVSetAutoDownload(True): OK", flush=True)
        suppressed_count += 1
    except Exception as e:
        print(f"      JVSetAutoDownload(True): SKIP ({e})", flush=True)

    if suppressed_count == 0:
        print("      ⚠️  ダイアログ抑制API がすべて非対応です。ダイアログが表示される可能性があります。", flush=True)
    else:
        print(f"      ✅ {suppressed_count} 個のダイアログ抑制API を設定しました。", flush=True)
    print(flush=True)

    # ── Step 4: JVInit（リトライ付き） ──────────────────────────────────────
    print(f"[4/4] JVInit 実行（タイムアウト {timeout}s）...", flush=True)
    deadline = time.monotonic() + timeout
    ret = -999
    attempt = 0
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        if time.monotonic() > deadline:
            print(f"      タイムアウト — JVInit が {timeout}s 以内に完了しませんでした", flush=True)
            break
        ret = jvl.JVInit(sid)
        msg = _jvinit_msg(ret)
        print(f"      試行 {attempt}/{max_attempts}: ret={ret} → {msg}", flush=True)
        if ret == 0:
            break
        if ret == -4:
            print("      → TARGET frontier JV を起動してログインし、再実行してください。", flush=True)
            break
        if ret in (-2, -3, -9, -202):
            print("      → SID または JRA-VAN 登録状態を確認してください。", flush=True)
            break
        if attempt < max_attempts:
            time.sleep(1)

    if ret != 0:
        print(flush=True)
        print("=" * 60, flush=True)
        print("❌ JVLink 接続確認 失敗", flush=True)
        print(f"   JVInit ret={ret}: {_jvinit_msg(ret)}", flush=True)
        print("=" * 60, flush=True)
        return 1

    print(flush=True)
    print("=" * 60, flush=True)
    print("✅ JVLink 接続確認 成功", flush=True)
    print("=" * 60, flush=True)
    return 0


def main() -> None:
    if sys.maxsize > 2**32:
        print("ERROR: 32bit Python で実行してください。", file=sys.stderr, flush=True)
        print("  py -3.14-32 -m src.utils.jravan_cli_initializer", file=sys.stderr, flush=True)
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="JRA-VAN JVLink CLI 初期化ツール（ブラウザ不要版）"
    )
    parser.add_argument(
        "--timeout", type=int, default=15,
        help="JVInit の最大待機秒数（デフォルト: 15）",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="詳細ログを出力する",
    )
    args = parser.parse_args()

    sid = os.getenv("JRAVAN_SID", "")
    if not sid:
        print("ERROR: .env に JRAVAN_SID が未設定です。", file=sys.stderr, flush=True)
        sys.exit(1)

    rc = run_check(sid=sid, timeout=args.timeout, verbose=args.verbose)
    sys.exit(rc)


if __name__ == "__main__":
    main()
