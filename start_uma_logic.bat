@echo off
chcp 65001 >nul
cd /d "C:\dev\horse-racing-ai"

:MENU
cls
echo ===================================================
echo        UMALOGI AI 起動メニュー
echo ===================================================
echo.
echo  [1] 週末準備のみ実行 (prepare_weekend.bat)
echo  [2] Next.js フロントエンドのみ起動
echo  [3] スケジューラのみ起動 (py -3.14-32)
echo  [4] 全サービス起動 (Next.js + スケジューラ)
echo  [5] 全サービス起動 + 週末準備
echo  [Q] 終了
echo.

:: ── ポート 3000 の稼働状況を事前チェック ──
for /f "tokens=*" %%p in ('netstat -ano ^| find ":3000" ^| find "LISTENING" 2^>nul') do (
    echo [INFO] ポート 3000 はすでに使用中です（Next.js が起動済みの可能性）。
    echo.
    goto :PROMPT
)

:PROMPT
set /p CHOICE="選択してください: "

if /i "%CHOICE%"=="1" goto :PREPARE_ONLY
if /i "%CHOICE%"=="2" goto :NEXTJS_ONLY
if /i "%CHOICE%"=="3" goto :SCHEDULER_ONLY
if /i "%CHOICE%"=="4" goto :ALL_SERVICES
if /i "%CHOICE%"=="5" goto :ALL_WITH_PREPARE
if /i "%CHOICE%"=="Q" goto :EXIT
if /i "%CHOICE%"=="q" goto :EXIT

echo [ERROR] 無効な選択です。もう一度入力してください。
pause
goto :MENU

:: ─────────────────────────────────────────────────────
:PREPARE_ONLY
echo.
echo ===== 週末準備を起動します =====
call prepare_weekend.bat
goto :MENU

:: ─────────────────────────────────────────────────────
:NEXTJS_ONLY
echo.
echo ===== Next.js フロントエンドを起動します =====
call :START_NEXTJS
echo [OK] Next.js を別ウィンドウで起動しました。
echo      ブラウザで http://localhost:3000 を開いてください。
pause
goto :MENU

:: ─────────────────────────────────────────────────────
:SCHEDULER_ONLY
echo.
echo ===== スケジューラを起動します =====
start "UMALOGI Scheduler" cmd /k "cd /d C:\dev\horse-racing-ai && py -3.14-32 scripts\scheduler.py"
echo [OK] スケジューラを別ウィンドウで起動しました。
pause
goto :MENU

:: ─────────────────────────────────────────────────────
:ALL_SERVICES
echo.
echo ===== 全サービスを起動します =====
call :START_NEXTJS
start "UMALOGI Scheduler" cmd /k "cd /d C:\dev\horse-racing-ai && py -3.14-32 scripts\scheduler.py"
echo [OK] Next.js + スケジューラを起動しました。
echo      ブラウザで http://localhost:3000 を開いてください。
pause
goto :MENU

:: ─────────────────────────────────────────────────────
:ALL_WITH_PREPARE
echo.
echo ===== 全サービス起動 + 週末準備を開始します =====
call :START_NEXTJS
start "UMALOGI Scheduler" cmd /k "cd /d C:\dev\horse-racing-ai && py -3.14-32 scripts\scheduler.py"
echo [OK] Next.js + スケジューラを起動しました。
echo.
echo ===== 週末準備を実行します =====
call prepare_weekend.bat
goto :MENU

:: ─────────────────────────────────────────────────────
:: サブルーチン: Next.js 起動（ビルド済みなら npm start、なければ npm run dev）
:START_NEXTJS
if exist "web\.next\BUILD_ID" (
    start "UMALOGI Next.js" cmd /k "cd /d C:\dev\horse-racing-ai\web && npm start"
) else (
    echo [INFO] .next ビルドが見つかりません。npm run dev で起動します。
    start "UMALOGI Next.js (dev)" cmd /k "cd /d C:\dev\horse-racing-ai\web && npm run dev"
)
exit /b 0

:: ─────────────────────────────────────────────────────
:EXIT
echo.
echo 終了します。
exit /b 0
