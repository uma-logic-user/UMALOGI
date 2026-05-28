#!/usr/bin/env bash
# UMALOGI Docker 一括起動 (Mac/Linux)

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo ""
echo " ============================================================"
echo "   UMALOGI  Docker 一括起動"
echo " ============================================================"
echo ""
echo " ※ JVLink / AI スケジューラーは Docker 外で別途起動してください。"
echo ""

# Docker が起動しているか確認
if ! docker info > /dev/null 2>&1; then
    echo " [エラー] Docker が起動していません。Docker Desktop を起動してから再実行してください。"
    exit 1
fi

ACTION="${1:-start}"

case "$ACTION" in
    stop|down)
        echo " [停止] docker compose down"
        docker compose down
        echo " 停止しました。"
        ;;
    build)
        echo " [ビルド] docker compose up -d --build"
        docker compose up -d --build
        ;;
    logs)
        echo " [ログ] Ctrl+C で終了"
        docker compose logs -f
        ;;
    *)
        echo " [起動] docker compose up -d"
        docker compose up -d
        echo ""
        echo " ============================================================"
        echo "   起動完了"
        echo ""
        echo "   Next.js ダッシュボード : http://localhost:3000"
        echo "   Streamlit ダッシュボード: http://localhost:8501"
        echo ""
        echo "   停止: ./start_docker.sh stop"
        echo "   ログ: ./start_docker.sh logs"
        echo " ============================================================"
        echo ""
        ;;
esac
