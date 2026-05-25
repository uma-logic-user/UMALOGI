@echo off
chcp 65001 > nul
:: ================================================================
:: UMALOGI JVLinkAgent タスクスケジューラ登録
:: ダブルクリックするだけで UAC が出て管理者権限で登録される
:: ================================================================

:: 管理者権限チェック
net session >nul 2>&1
if %errorlevel% == 0 goto :ADMIN_OK

:: 管理者でなければ UAC で自己昇格
echo [INFO] UAC で管理者昇格中...
powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\"' -Verb RunAs -Wait"
exit /b

:ADMIN_OK
echo.
echo ======================================================
echo   UMALOGI JVLinkAgent タスクスケジューラ登録
echo   （管理者権限で実行中）
echo ======================================================
echo.

set TASK_NAME=UMALOGI_TargetFrontierJV
set EXE_PATH=C:\Program Files (x86)\JRA-VAN\Data Lab\JVLinkAgent.exe

:: 存在確認
if not exist "%EXE_PATH%" (
    echo [ERROR] 実行ファイルが見つかりません:
    echo   %EXE_PATH%
    echo.
    pause
    exit /b 1
)
echo [OK] 実行ファイル確認: %EXE_PATH%

:: 既存タスク削除（冪等）
schtasks /Delete /TN "%TASK_NAME%" /F >nul 2>&1
echo [INFO] 既存タスクをリセット

:: タスク登録
schtasks /Create ^
  /TN  "%TASK_NAME%" ^
  /TR  "\"%EXE_PATH%\"" ^
  /SC  ONLOGON ^
  /RL  HIGHEST ^
  /DELAY "0001:00" ^
  /F

if %errorlevel% == 0 (
    echo.
    echo [SUCCESS] タスクスケジューラ登録完了！
    echo   タスク名 : %TASK_NAME%
    echo   トリガー : ログオン時（1分遅延）
    echo   実行権限 : 最高（管理者）
    echo.
    echo 次回 Windows ログオン時から JVLinkAgent が自動起動します。
) else (
    echo [ERROR] 登録に失敗しました。
)

echo.
pause
