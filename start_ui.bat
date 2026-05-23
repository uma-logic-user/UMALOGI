@echo off
:: ポート3000を占有している既存プロセスを自動で強制終了してから起動する
echo [UMALOGI UI] ポート3000 解放チェック中...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3000 " 2^>nul') do (
    echo   旧プロセス PID=%%a を強制終了します...
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul

echo [UMALOGI UI] Next.js サーバーを起動します (0.0.0.0:3000)
cd /d "%~dp0web"
start "UMALOGI_UI" cmd /k "npx next start -H 0.0.0.0 -p 3000"
