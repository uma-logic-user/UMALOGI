@echo off
chcp 65001 > nul
title UMALOGI 週次オートパイロット

cd /d C:\dev\horse-racing-ai

echo ============================================================
echo   UMALOGI 自律運転モード 起動中...
echo   終了するには Ctrl+C を押してください
echo ============================================================
echo.

py scripts/today_auto_runner.py --continuous

echo.
echo ============================================================
echo   UMALOGI オートパイロット 停止
echo ============================================================
pause
