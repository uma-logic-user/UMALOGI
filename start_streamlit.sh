#!/usr/bin/env bash
# UMALOGI Streamlit 運用ダッシュボード起動 (Mac/Linux)
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo " ============================================================"
echo "   UMALOGI  Streamlit 運用ダッシュボード起動"
echo " ============================================================"
echo ""
echo " 起動中: http://localhost:8501"
echo " 停止:   Ctrl+C"
echo ""

cd "$ROOT"
python3 -m streamlit run web_streamlit/app.py \
    --server.port 8501 \
    --server.address localhost
