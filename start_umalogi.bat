@echo off
chcp 65001 > nul
title UMALOGI Startup Monitor

cd /d "%~dp0"

echo.
echo  ============================================================
echo    _   _ __  __    _    _     ___   ____ ___
echo   | | | |  \/  |  / \  | |   / _ \ / ___|_ _|
echo   | | | | |\/| | / _ \ | |  | | | | |  _ | |
echo   | |_| | |  | |/ ___ \| |__| |_| | |_| || |
echo    \___/|_|  |_/_/   \_\_____\___/ \____|___|
echo.
echo    Ver 1.0  --  Autonomous Racing Prediction
echo  ============================================================
echo.

echo  [1/2] Next.js Dashboard を起動中...
echo        http://localhost:3000
start "UMALOGI_UI" cmd /k "cd /d "%~dp0web" && npm run start"

echo.
echo        5秒待機中...
timeout /t 5 /nobreak > nul

echo.
echo  [2/2] AI スケジューラーを起動中...
echo        scripts/scheduler.py
start "UMALOGI_AI" cmd /k "cd /d "%~dp0" && python scripts/scheduler.py"

echo.
echo  ============================================================
echo    ALL SYSTEMS GO
echo.
echo    Dashboard (PC)     : http://localhost:3000
echo    Dashboard (Mobile) : http://100.108.246.20:3000
echo    AI Engine          : UMALOGI_AI ウィンドウを確認
echo  ============================================================
echo.
echo  このウィンドウは閉じても構いません。
echo  各プロセスは独立したウィンドウで継続稼働します。
echo.
echo  30秒後に自動クローズします... (Ctrl+C で即時終了)
timeout /t 30 /nobreak
