# -*- coding: utf-8 -*-
"""
JVLink スタンドアロン認証テスト (32bit Python 専用)

TARGET frontier を閉じた状態で JVLink が認証・接続できるかを検証する。

Usage:
    py -3.14-32 scripts/test_jvlink_standalone.py
"""

from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass


def main() -> int:
    sid = os.getenv("JRAVAN_SID", "")
    print(f"[test] JRAVAN_SID={'*' * len(sid) if sid else '(未設定)'}")
    if not sid:
        print("[FAIL] .env に JRAVAN_SID が設定されていません")
        return 1

    try:
        import win32com.client  # type: ignore[import]
    except ImportError:
        print("[FAIL] pywin32 が見つかりません。py -3.14-32 -m pip install pywin32")
        return 1

    print("[test] JV-Link COM オブジェクト生成中...")
    try:
        jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
    except Exception as e:
        print(f"[FAIL] COM 初期化失敗: {e}")
        return 1
    print("[test] COM 生成 OK")

    print(f"[test] JVInit 呼び出し中 (sid={sid!r})...")
    ret = jvl.JVInit(sid)
    if ret != 0:
        print(f"[FAIL] JVInit 失敗 code={ret}")
        print("       SID が正しいか、JRA-VAN ライセンスが有効か確認してください。")
        return 1
    print(f"[OK]   JVInit 完了 (code={ret})")

    # OPT_TODAY(3) で当日 RACE データを試みる
    from src.scraper.jravan_client import OPT_TODAY, OPT_STORED
    today = time.strftime("%Y%m%d") + "000000"

    print(f"\n[test] JVOpen RACE fromtime={today} option=OPT_TODAY(3)...")
    try:
        result = jvl.JVOpen("RACE", today, OPT_TODAY, 0, "")
        code = result[0] if isinstance(result, (tuple, list)) else int(result)
    except Exception:
        try:
            result = jvl.JVOpen("RACE", today, OPT_TODAY)
            code = result[0] if isinstance(result, (tuple, list)) else int(result)
        except Exception as e:
            print(f"[FAIL] JVOpen 例外: {e}")
            jvl.JVClose()
            return 1

    if code >= 0:
        print(f"[OK]   JVOpen 成功 code={code} (files={result[1] if isinstance(result, (tuple,list)) and len(result)>1 else '?'})")
        jvl.JVClose()
        print("\n[RESULT] TARGET frontier なしで JVLink 認証・接続 成功")
        return 0
    elif code == -1:
        print(f"[WARN] JVOpen code=-1 (本日は開催なし/データなし) — 認証自体は成功")
        jvl.JVClose()
        print("\n[RESULT] TARGET frontier なしで JVLink 認証 成功（当日データなし）")
        return 0
    else:
        print(f"[FAIL] JVOpen 失敗 code={code}")
        jvl.JVClose()
        return 1


if __name__ == "__main__":
    sys.exit(main())
