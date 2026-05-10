# -*- coding: utf-8 -*-
"""
UMALOGI Cloudflare Named Tunnel セットアップスクリプト

Quick Tunnel（毎回URLが変わる）→ Named Tunnel（固定URL）へ移行する。
初回のみ実行する。以降は install_tunnel_service.ps1 で自動起動する。

実行手順:
    1. py scripts/setup_named_tunnel.py --check   # cloudflared の確認
    2. cloudflared login                           # ブラウザでCFログイン（手動）
    3. py scripts/setup_named_tunnel.py --create  # トンネル作成 + 設定ファイル生成
    4. py scripts/setup_named_tunnel.py --run     # テスト起動
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

_ROOT     = Path(__file__).resolve().parents[1]
_BIN      = _ROOT / "bin"
_CF_EXE   = _BIN / "cloudflared.exe"
_CF_DIR   = _ROOT / ".cloudflare"
_CF_CONF  = _CF_DIR / "config.yml"
_TUNNEL_NAME = "umalogi"


def _cf() -> str:
    """cloudflared の実行パスを返す。"""
    for exe in [str(_CF_EXE), "cloudflared", "cloudflared.exe"]:
        try:
            r = subprocess.run([exe, "--version"], capture_output=True, timeout=5)
            if r.returncode == 0:
                return exe
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
    print("cloudflared が見つかりません。bin/cloudflared.exe にダウンロードしてください。")
    print("  → py scripts/start_tunnel.py を一度実行すると自動DLされます。")
    sys.exit(1)


def cmd_check() -> None:
    """cloudflared のバージョンと認証状態を確認する。"""
    cf = _cf()
    r = subprocess.run([cf, "--version"], capture_output=True, encoding="utf-8")
    print(f"cloudflared バージョン: {r.stdout.strip()}")

    # 認証トークン確認
    token_file = Path.home() / ".cloudflared" / "cert.pem"
    if token_file.exists():
        print("✅ Cloudflare 認証済み (cert.pem 存在)")
    else:
        print("⚠️  未認証です。次のコマンドを実行してください:")
        print(f"  {cf} login")
        print("  ブラウザが開くので、Cloudflare アカウントでログインしてください。")


def cmd_create() -> None:
    """Named Tunnel を作成して設定ファイルを生成する。"""
    cf = _cf()
    _CF_DIR.mkdir(exist_ok=True)

    # トンネル作成
    print(f"Named Tunnel '{_TUNNEL_NAME}' を作成中...")
    r = subprocess.run(
        [cf, "tunnel", "create", _TUNNEL_NAME],
        capture_output=True, encoding="utf-8",
    )
    if r.returncode != 0:
        if "already exists" in r.stderr or "already exists" in r.stdout:
            print(f"トンネル '{_TUNNEL_NAME}' は既に存在します。")
        else:
            print(f"作成失敗:\n{r.stderr}")
            sys.exit(1)
    else:
        print(r.stdout)

    # トンネルID を取得
    r2 = subprocess.run(
        [cf, "tunnel", "list", "--output", "json"],
        capture_output=True, encoding="utf-8",
    )
    tunnels = json.loads(r2.stdout) if r2.stdout.strip() else []
    tunnel_id = next(
        (t["id"] for t in tunnels if t.get("name") == _TUNNEL_NAME),
        None
    )
    if not tunnel_id:
        print("トンネルIDの取得に失敗しました。手動で確認してください: cloudflared tunnel list")
        tunnel_id = "<YOUR_TUNNEL_ID>"

    # config.yml 生成
    config_content = f"""# UMALOGI Cloudflare Named Tunnel 設定
# 生成日: {__import__('datetime').date.today()}
# Tunnel ID: {tunnel_id}

tunnel: {tunnel_id}
credentials-file: {Path.home() / '.cloudflared' / f'{tunnel_id}.json'}

# ローカルの Next.js ダッシュボードをトンネル経由で公開
ingress:
  - hostname: {_TUNNEL_NAME}.workers.dev
    service: http://localhost:3000
  # マッチしないリクエストはここで処理
  - service: http_status:404

# ログ設定
loglevel: warn
logfile: {_ROOT / 'data' / 'tunnel.log'}
"""
    _CF_CONF.write_text(config_content, encoding="utf-8")
    print(f"✅ 設定ファイル生成: {_CF_CONF}")
    print()
    print("次のステップ:")
    print(f"  1. Cloudflare ダッシュボード → Workers & Pages → {_TUNNEL_NAME}.workers.dev を確認")
    print(f"  2. py scripts/setup_named_tunnel.py --run  でテスト起動")
    print(f"  3. py scripts/install_tunnel_service.ps1  で自動起動登録")


def cmd_run() -> None:
    """Named Tunnel をフォアグラウンドで起動（テスト用）。"""
    cf = _cf()
    if not _CF_CONF.exists():
        print(f"設定ファイルが見つかりません: {_CF_CONF}")
        print("先に --create を実行してください。")
        sys.exit(1)

    print(f"Named Tunnel 起動: {_TUNNEL_NAME}")
    print(f"設定ファイル: {_CF_CONF}")
    print("Ctrl+C で停止\n")

    subprocess.run(
        [cf, "tunnel", "--config", str(_CF_CONF), "run", _TUNNEL_NAME],
        cwd=str(_ROOT),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI Named Tunnel セットアップ")
    parser.add_argument("--check",  action="store_true", help="cloudflared の確認")
    parser.add_argument("--create", action="store_true", help="Named Tunnel を作成")
    parser.add_argument("--run",    action="store_true", help="トンネルを起動（テスト）")
    args = parser.parse_args()

    if args.check:
        cmd_check()
    elif args.create:
        cmd_create()
    elif args.run:
        cmd_run()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
