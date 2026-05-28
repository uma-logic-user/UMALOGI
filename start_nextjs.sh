#!/usr/bin/env bash
# UMALOGI Next.js ダッシュボード起動 (Mac/Linux)
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo " ============================================================"
echo "   UMALOGI  Next.js ダッシュボード起動"
echo " ============================================================"
echo ""

# ビルド済みか確認し、なければビルドする
if [ ! -d "$ROOT/web/.next" ]; then
    echo " [INFO] ビルドが存在しません。npm run build を実行します..."
    cd "$ROOT/web"
    npm run build
fi

echo " 起動中: http://localhost:3000"
echo " 停止:   Ctrl+C"
echo ""

cd "$ROOT/web"
npm start
