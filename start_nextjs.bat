@echo off
chcp 65001 > nul
title UMALOGI - Next.js Dashboard

set ROOT=%~dp0

echo.
echo  ============================================================
echo    UMALOGI  Next.js ダッシュボード起動
echo  ============================================================
echo.

:: ビルド済みか確認し、なければビルドする
if not exist "%ROOT%web\.next" (
    echo  [INFO] ビルドが存在しません。npm run build を実行します...
    cd /d "%ROOT%web"
    call npm run build
    if %ERRORLEVEL% neq 0 (
        echo  [エラー] ビルドに失敗しました。
        pause
        exit /b 1
    )
)

echo  起動中: http://localhost:3000
echo  停止:   このウィンドウを閉じる か Ctrl+C
echo.

cd /d "%ROOT%web"
npm start
