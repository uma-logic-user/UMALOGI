chcp 65001
cd /d C:\dev\horse-racing-ai

echo [1/2] JSON generating...
py web\generate_data.py
if %ERRORLEVEL% neq 0 (
    echo ERROR: generate_data.py failed. Check umalogi.db.
    pause
    exit /b 1
)
echo [1/2] Done.

echo [2/2] Starting Next.js dev server...
echo Open: http://localhost:3000  ^(Ctrl+C to stop^)
cd web
call npm run dev
pause
