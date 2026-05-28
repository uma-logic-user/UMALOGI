@echo off
chcp 65001 > nul
title UMALOGI - Streamlit 運用ダッシュボード

set ROOT=%~dp0

echo.
echo  ============================================================
echo    UMALOGI  Streamlit 運用ダッシュボード起動
echo  ============================================================
echo.
echo  起動中: http://localhost:8501
echo  停止:   このウィンドウを閉じる か Ctrl+C
echo.

cd /d "%ROOT%"
py -m streamlit run web_streamlit/app.py --server.port 8501 --server.address localhost
