@echo off
setlocal EnableDelayedExpansion
title UMALOGI Launcher

REM ============================================================
REM  UMALOGI production stack one-click launcher (canonical).
REM    1) Streamlit ops dashboard       web_streamlit/app.py  (port 8501)
REM    2) Weekly autonomous autopilot   scripts/today_auto_runner.py --continuous
REM    3) Self-healing watchdog         scripts/watchdog.py --interval 5
REM    4) Next.js Web UI                web/ npm start  (port 3000)
REM
REM  NOTE: today_auto_runner (autopilot) and scheduler.py are two
REM  mutually-exclusive implementations of the same weekly automation.
REM  Production uses the autopilot. Never run both at once
REM  (double predictions / double Discord notifications).
REM  Use start_scheduler_mode.bat only to switch to scheduler mode.
REM
REM  W-085: This file MUST stay 100% ASCII. Japanese text saved as
REM  UTF-8 is misparsed by cmd.exe in a fresh console (initial CP932):
REM  trailing bytes of multibyte chars act as CP932 lead bytes, eat
REM  the following byte (%, quotes, even CR/LF), corrupt variable
REM  expansion and merge lines. "chcp 65001" inside the bat does NOT
REM  reliably fix parsing. Japanese docs belong in README_BAT.md.
REM ============================================================

REM -- Resolve project root (this bat lives in scripts\bat\) --
pushd "%~dp0..\.."
set "ROOT=%CD%"
popd

echo.
echo  ============================================================
echo    UMALOGI production stack launcher
echo    ROOT: %ROOT%
echo  ============================================================
echo.

REM -- Detect what is already running (double-start guard) --
call :count "today_auto_runner.py"  AUTO_RUNNING
call :count "watchdog.py"           WD_RUNNING
call :count "web_streamlit"         DASH_RUNNING
call :count "scheduler.py"          SCHED_RUNNING
call :countport 3000                WEB_RUNNING

REM -- Abort if the exclusive scheduler.py mode is active --
if not "%SCHED_RUNNING%"=="0" (
    echo  [ABORT] scheduler.py is running ^(%SCHED_RUNNING% process^).
    echo          Autopilot and scheduler are mutually exclusive.
    echo          Run stop_umalogi.bat first, then retry.
    echo.
    pause
    endlocal & exit /b 1
)

REM -- 1) Streamlit dashboard --
if "%DASH_RUNNING%"=="0" (
    echo  [1/4] Starting Streamlit dashboard... http://localhost:8501
    start "UMALOGI_DASHBOARD" /D "%ROOT%" cmd /k "py -m streamlit run web_streamlit\app.py --server.port 8501 --browser.gatherUsageStats false"
) else (
    echo  [1/4] Streamlit dashboard already running ^(%DASH_RUNNING% process^). Skip.
)
timeout /t 3 /nobreak > nul

REM -- 2) Weekly autonomous autopilot --
if "%AUTO_RUNNING%"=="0" (
    echo  [2/4] Starting autopilot... scripts\today_auto_runner.py --continuous
    start "UMALOGI_AUTORUNNER" /D "%ROOT%" cmd /k "py scripts\today_auto_runner.py --continuous"
) else (
    echo  [2/4] Autopilot already running ^(%AUTO_RUNNING% process^). Skip.
)
timeout /t 2 /nobreak > nul

REM -- 3) Self-healing watchdog --
if "%WD_RUNNING%"=="0" (
    echo  [3/4] Starting watchdog... scripts\watchdog.py --interval 5
    start "UMALOGI_WATCHDOG" /D "%ROOT%" cmd /k "py scripts\watchdog.py --interval 5"
) else (
    echo  [3/4] Watchdog already running ^(%WD_RUNNING% process^). Skip.
)
timeout /t 2 /nobreak > nul

REM -- 4) Next.js Web UI (port 3000, hit results / premium reports) --
if "%WEB_RUNNING%"=="0" (
    echo  [4/4] Starting Next.js Web UI... http://localhost:3000
    if not exist "%ROOT%\web\.next" (
        echo        No build found. Running npm run build first...
        pushd "%ROOT%\web"
        call npm run build
        popd
    )
    start "UMALOGI_WEBUI" /D "%ROOT%\web" cmd /k "npm start"
) else (
    echo  [4/4] Web UI already listening on port 3000. Skip.
)

echo.
echo  ============================================================
echo    ALL SYSTEMS GO
echo.
echo    Streamlit dashboard : http://localhost:8501
echo    Next.js Web UI      : http://localhost:3000  ^(mobile: http://100.108.246.20:3000^)
echo    Autopilot           : UMALOGI_AUTORUNNER window
echo    Watchdog            : UMALOGI_WATCHDOG window
echo    Logs                : data\scheduler.log / data\watchdog.log
echo.
echo    Stop                : stop_umalogi.bat
echo  ============================================================
echo.
echo  You may close this launcher window. Each process keeps
echo  running in its own window.
echo.
timeout /t 20 /nobreak
endlocal
exit /b 0

REM -- Subroutine: count python processes whose command line matches %1 --
REM    Name limited to python-like images so bash/cmd/editors never match.
REM    Pattern must not contain backslashes (cmd-to-PowerShell quoting).
REM    Temp-file + set /p avoids for/f pipe quoting issues.
:count
set "%~2=0"
powershell -NoProfile -Command "(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|^py' -and $_.CommandLine -and ($_.CommandLine -match '%~1') } | Measure-Object).Count" > "%TEMP%\_umalogi_cnt_%~2.txt" 2>nul
set /p %~2=<"%TEMP%\_umalogi_cnt_%~2.txt"
del "%TEMP%\_umalogi_cnt_%~2.txt" 2>nul
exit /b 0

REM -- Subroutine: set %2 to non-zero when TCP port %1 is LISTENing --
REM    Port state is the single source of truth: matching node.exe by
REM    name would false-positive on npm/build helper processes.
:countport
set "%~2=0"
powershell -NoProfile -Command "(Get-NetTCPConnection -LocalPort %~1 -State Listen -ErrorAction SilentlyContinue | Measure-Object).Count" > "%TEMP%\_umalogi_cnt_%~2.txt" 2>nul
set /p %~2=<"%TEMP%\_umalogi_cnt_%~2.txt"
del "%TEMP%\_umalogi_cnt_%~2.txt" 2>nul
exit /b 0
