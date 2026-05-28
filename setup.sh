#!/usr/bin/env bash
# UMALOGI 初回セットアップスクリプト (Mac/Linux)
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo ""
echo " ============================================================"
echo "   UMALOGI  初回セットアップ"
echo " ============================================================"
echo ""

# ── [1/4] .env 確認 ────────────────────────────────────────────
echo " [1/4] .env ファイルを確認中..."
if [ -f "$ROOT/.env" ]; then
    echo "       .env は既に存在します。スキップします。"
else
    if [ -f "$ROOT/.env.example" ]; then
        cp "$ROOT/.env.example" "$ROOT/.env"
        echo "       .env.example から .env を作成しました。"
        echo ""
        echo " *** 重要 ***"
        echo " $ROOT/.env を開き、以下の項目を設定してください:"
        echo "   - DISCORD_WEBHOOK_URL"
        echo "   - JRAVAN_SID"
        echo "   - INITIAL_BANKROLL"
        echo "   - NOTE_EMAIL / NOTE_PASSWORD (任意)"
        echo ""
        read -p " 設定後、Enter キーを押して続行してください..."
    else
        echo " [警告] .env.example が見つかりません。.env を手動で作成してください。"
    fi
fi

# ── [2/4] Python 依存インストール ──────────────────────────────
echo " [2/4] Python パッケージをインストール中..."
echo "       pip install -r requirements.txt"
python3 -m pip install -r requirements.txt
echo "       Python パッケージのインストール完了。"

# ── [3/4] npm install ──────────────────────────────────────────
echo ""
echo " [3/4] Node.js パッケージをインストール中..."
echo "       cd web && npm install"
cd "$ROOT/web"
npm install
cd "$ROOT"
echo "       Node.js パッケージのインストール完了。"

# ── [4/4] DB 初期化 ─────────────────────────────────────────────
echo ""
echo " [4/4] データベースを初期化中..."
echo "       python -m src.database.init_db"
python3 -m src.database.init_db
echo "       DB 初期化完了。"

# ── 完了 ────────────────────────────────────────────────────────
echo ""
echo " ============================================================"
echo "   セットアップ完了！"
echo ""
echo "   次のコマンドで各サービスを起動できます:"
echo "     ./start_nextjs.sh    : Next.js ダッシュボード"
echo "     ./start_streamlit.sh : Streamlit 運用ダッシュボード"
echo "     ./start_docker.sh    : Docker 一括起動"
echo " ============================================================"
echo ""
