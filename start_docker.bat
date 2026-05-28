@echo off
chcp 65001 > nul
title UMALOGI - Docker 一括起動

set ROOT=%~dp0

echo.
echo  ============================================================
echo    UMALOGI  Docker 一括起動
echo  ============================================================
echo.
echo  ※ JVLink / AI スケジューラーは Docker 外（Windows 側）で別途起動してください。
echo    → UMALOGI_SCHEDULER.bat を使用してください。
echo.

:: Docker が起動しているか確認
docker info > nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo  [エラー] Docker が起動していません。
    echo  Docker Desktop を起動してから再実行してください。
    pause
    exit /b 1
)

cd /d "%ROOT%"

:: 引数で操作を選択
if "%1"=="stop"  goto STOP
if "%1"=="down"  goto STOP
if "%1"=="build" goto BUILD
if "%1"=="logs"  goto LOGS

:: デフォルト: 起動
:START
echo  [起動] docker compose up -d
docker compose up -d
if %ERRORLEVEL% neq 0 (
    echo  [エラー] Docker コンテナの起動に失敗しました。
    echo  ヒント: 初回またはイメージ更新時は start_docker.bat build を実行してください。
    pause
    exit /b 1
)
echo.
echo  ============================================================
echo    起動完了
echo.
echo    Next.js ダッシュボード : http://localhost:3000
echo    Streamlit ダッシュボード: http://localhost:8501
echo.
echo    停止: start_docker.bat stop
echo    ログ: start_docker.bat logs
echo  ============================================================
echo.
goto END

:BUILD
echo  [ビルド] docker compose up -d --build
docker compose up -d --build
goto END

:STOP
echo  [停止] docker compose down
docker compose down
echo  停止しました。
goto END

:LOGS
echo  [ログ] Ctrl+C で終了
docker compose logs -f
goto END

:END
pause
