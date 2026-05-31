@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion

:: ============================================================
:: UMALOGI 金曜夜バッチ
:: JRA-VAN同期（32bit） → AI予想（64bit） → 通知 → バックアップ
::
:: 【異常検知】Step4（AI予想）でエラーが発生した場合は
::   Discord に SOS通知を送信して後続処理を中止します。
::   Steps 1-3（JRA-VAN同期）は -303 等の既知エラーが出やすいため
::   WARN として処理を継続します。
:: ============================================================

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

echo.
echo ================================================================
echo   UMALOGI 金曜夜バッチ  %DATE% %TIME%
echo ================================================================
echo.

:: .env を読み込む（Python 側でも load_dotenv するが念のため確認）
if not exist ".env" (
    echo [WARN] .env が見つかりません
)

set BATCH_ERROR=0

:: ---- Step 1: JRA-VAN RACE 同期（32bit Python 必須）--------------
echo [1/6] JRA-VAN RACE データ同期（出馬表・32bit）...
py -3-32 -m src.ops.data_sync friday
if %ERRORLEVEL% neq 0 (
    echo [WARN] RACE 同期でエラーが発生しました（処理続行）
) else (
    echo [OK] RACE 同期完了
)

:: ---- Step 2: JRA-VAN WOOD 同期（32bit Python 必須）--------------
echo.
echo [2/6] JRA-VAN WOOD（調教タイム）同期（32bit）...
py -3-32 -m src.ops.data_sync wood
if %ERRORLEVEL% neq 0 (
    echo [WARN] WOOD 同期でエラーが発生しました（処理続行）
) else (
    echo [OK] WOOD 同期完了
)

:: ---- Step 3: マスタ差分更新（32bit Python 必須）-----------------
echo.
echo [3/6] マスタデータ差分更新（32bit）...
py -3-32 -m src.ops.data_sync masters
if %ERRORLEVEL% neq 0 (
    echo [WARN] マスタ更新でエラーが発生しました（処理続行）
) else (
    echo [OK] マスタ更新完了
)

:: ---- Step 4: AI 暫定予想生成（64bit Python）---------------------
echo.
echo [4/6] AI 暫定予想生成（64bit）...
py -m src.main_pipeline provisional
if %ERRORLEVEL% neq 0 (
    echo [CRITICAL] 予想生成に失敗しました — SOS通知送信中...
    set BATCH_ERROR=1
    py -m scripts.test_notification --sos "[UMALOGI][緊急] 金曜バッチ Step4 AI予想生成が異常終了しました。サーバーを確認してください。(%DATE% %TIME%)"
    goto :BACKUP
)
echo [OK] 予想生成完了

:: ---- Step 5: Discord 通知（64bit Python）------------------------
echo.
echo [5/6] Discord 通知送信（64bit）...
py -m scripts.test_notification --channel discord
if %ERRORLEVEL% neq 0 (
    echo [WARN] 通知送信に失敗しました（処理続行）
) else (
    echo [OK] 通知送信完了
)

:: ---- Step 6: DB バックアップ（64bit Python）---------------------
:BACKUP
echo.
echo [6/6] DB バックアップ（ローカル + クラウド）...
py -m src.ops.backup
if %ERRORLEVEL% neq 0 (
    echo [WARN] バックアップに失敗しました
) else (
    echo [OK] バックアップ完了
)

echo.
if %BATCH_ERROR% equ 1 (
    echo ================================================================
    echo   [NG] UMALOGI 金曜夜バッチ 異常終了  %DATE% %TIME%
    echo   Step4 AI予想生成でエラー。Discord に SOS通知済み。
    echo ================================================================
    endlocal
    exit /b 1
)

echo ================================================================
echo   [OK] UMALOGI 金曜夜バッチ完了  %DATE% %TIME%
echo ================================================================
echo.

endlocal
exit /b 0
