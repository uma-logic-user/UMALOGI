@echo off
chcp 65001 > nul
title UMALOGI Dashboard

cd /d C:\dev\horse-racing-ai

echo.
echo ============================================
echo   UMALOGI Pro Dashboard
echo   http://localhost:8555
echo ============================================
echo.

py -m streamlit run web_streamlit/app.py ^
    --server.port 8555 ^
    --server.headless false ^
    --browser.gatherUsageStats false

pause
