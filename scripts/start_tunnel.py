# -*- coding: utf-8 -*-
"""
UMALOGI スマホ外部アクセス トンネリングスクリプト

ローカルの Next.js ダッシュボード (port 3000) を HTTPS で外部公開する。
cloudflared (Cloudflare Quick Tunnel) を優先使用。未インストール時は自動ダウンロード。

使い方:
    py scripts/start_tunnel.py            # ダッシュボード起動 + トンネル開通
    py scripts/start_tunnel.py --port 3000
    py scripts/start_tunnel.py --tunnel-only   # Next.js は別途起動済みの場合
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
_BIN = _ROOT / "bin"
_BIN.mkdir(exist_ok=True)

_CLOUDFLARED_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-windows-amd64.exe"
)
_CLOUDFLARED_EXE = _BIN / "cloudflared.exe"


def _download_cloudflared() -> Path:
    """cloudflared.exe を bin/ にダウンロードする。"""
    if _CLOUDFLARED_EXE.exists():
        return _CLOUDFLARED_EXE
    print(f"cloudflared をダウンロード中: {_CLOUDFLARED_URL}")
    print("(約 30MB — しばらくお待ちください)")
    try:
        urllib.request.urlretrieve(_CLOUDFLARED_URL, str(_CLOUDFLARED_EXE))
        print(f"ダウンロード完了: {_CLOUDFLARED_EXE}")
        return _CLOUDFLARED_EXE
    except Exception as exc:
        print(f"ダウンロード失敗: {exc}")
        sys.exit(1)


def _find_cloudflared() -> Path | None:
    """PATH または bin/ から cloudflared を探す。"""
    for candidate in ["cloudflared", "cloudflared.exe"]:
        try:
            r = subprocess.run([candidate, "--version"], capture_output=True, timeout=5)
            if r.returncode == 0:
                return Path(candidate)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
    if _CLOUDFLARED_EXE.exists():
        return _CLOUDFLARED_EXE
    return None


def start_tunnel(port: int) -> None:
    """cloudflared Quick Tunnel を起動して HTTPS URL を表示する。"""
    exe = _find_cloudflared()
    if exe is None:
        print("cloudflared が見つかりません。自動ダウンロードします...")
        exe = _download_cloudflared()

    print(f"\n{'=' * 55}")
    print("  UMALOGI トンネル起動")
    print(f"  ローカル: http://localhost:{port}")
    print(f"{'=' * 55}")
    print("Cloudflare Quick Tunnel を開通中...")
    print("(初回は10〜20秒かかります)\n")

    proc = subprocess.Popen(
        [str(exe), "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    url_found = False
    url_pattern = re.compile(r"https://[a-z0-9\-]+\.trycloudflare\.com")

    try:
        for line in proc.stdout:  # type: ignore[union-attr]
            line = line.rstrip()
            if line:
                print(f"[cloudflared] {line}")
            m = url_pattern.search(line)
            if m and not url_found:
                url = m.group(0)
                url_found = True
                print()
                print("=" * 55)
                print("  スマホからアクセスできる URL:")
                print()
                print(f"  >> {url} <<")
                print()
                print("  QRコード (カメラアプリで読み込み):")
                _print_qr(url)
                print("=" * 55)
                print("Ctrl+C でトンネルを停止します")
                print()

        proc.wait()
    except KeyboardInterrupt:
        print("\nトンネルを停止しました。")
        proc.terminate()


def _print_qr(url: str) -> None:
    """QR コードをターミナルに ASCII 表示する（qrcode ライブラリがある場合）。"""
    try:
        import qrcode  # type: ignore[import]

        qr = qrcode.QRCode(border=1)
        qr.add_data(url)
        qr.make(fit=True)
        qr.print_ascii(tty=True)
    except ImportError:
        print("  (qrcode ライブラリなし — ブラウザで直接入力してください)")


def start_nextjs(port: int) -> subprocess.Popen:
    """Next.js 開発サーバーをバックグラウンドで起動する。"""
    web_dir = _ROOT / "web"
    print(f"Next.js 起動中 (port={port})...")
    proc = subprocess.Popen(
        ["npm", "run", "dev", "--", "--port", str(port)],
        cwd=str(web_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    )
    # 起動完了を待機（最大30秒）
    started = False
    for _ in range(30):
        if proc.poll() is not None:
            print("Next.js 起動失敗")
            break
        try:
            line = proc.stdout.readline()  # type: ignore[union-attr]
            if "localhost" in line or "ready" in line.lower():
                started = True
                break
        except Exception:
            pass
        time.sleep(1)
    if started:
        print(f"Next.js 起動完了: http://localhost:{port}")
    else:
        print("Next.js 起動中（バックグラウンド）...")
    return proc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="UMALOGI ダッシュボード トンネリング (Cloudflare Quick Tunnel)"
    )
    parser.add_argument("--port", type=int, default=3000, help="Next.js のポート番号")
    parser.add_argument(
        "--tunnel-only",
        action="store_true",
        help="Next.js を起動せず、トンネルのみ開通する",
    )
    args = parser.parse_args()

    nextjs_proc: subprocess.Popen | None = None
    if not args.tunnel_only:
        nextjs_proc = start_nextjs(args.port)

    try:
        start_tunnel(args.port)
    finally:
        if nextjs_proc and nextjs_proc.poll() is None:
            nextjs_proc.terminate()
            print("Next.js を停止しました。")


if __name__ == "__main__":
    main()
