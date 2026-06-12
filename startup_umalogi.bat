@echo off
title UMALOGI Auto-Recovery Launcher

REM ============================================================
REM  startup_umalogi.bat
REM    Auto-recovery entry point for PC boot / logon. Brings the
REM    production stack back with zero human action after forced
REM    reboots (e.g. Windows Update).
REM
REM    Register in Task Scheduler or the Startup folder.
REM    (See scripts\bat\README_BAT.md sections 5/6.)
REM
REM  Production stack (implemented in scripts\bat\start_umalogi.bat):
REM    1) Streamlit dashboard      web_streamlit/app.py   (port 8501)
REM    2) Weekly autopilot         today_auto_runner.py --continuous
REM    3) Self-healing watchdog    watchdog.py --interval 5
REM    4) Next.js Web UI           web/ npm start         (port 3000)
REM
REM  W-085: This file MUST stay 100% ASCII. Japanese text saved as
REM  UTF-8 breaks cmd.exe parsing in a fresh console (CP932 default).
REM ============================================================

REM -- Resolve project root from this bat's location --
set "ROOT=%~dp0"
cd /d "%ROOT%"

echo.
echo  ============================================================
echo    UMALOGI auto-recovery  (%DATE% %TIME%)
echo    ROOT: %ROOT%
echo  ============================================================

REM -- Stabilization grace period (DB locks / JVLink init / network) --
echo  [wait] Waiting 45 seconds for system stabilization...
timeout /t 45 /nobreak > nul

REM -- Python environment: no venv/Poetry by design; system py launcher --
REM    (py = 64bit Python 3.14). Auto-activate only if a venv appears.
if exist "%ROOT%.venv\Scripts\activate.bat" (
    echo  [env] .venv detected. Activating virtualenv.
    call "%ROOT%.venv\Scripts\activate.bat"
) else (
    echo  [env] No venv. Using system py launcher.
)

REM -- Launch production stack via the canonical launcher --
REM    start_umalogi.bat owns the double-start guard and the
REM    scheduler.py exclusivity guard, so re-running after a reboot
REM    is safe even when some processes survived.
if exist "%ROOT%scripts\bat\start_umalogi.bat" (
    echo  [run] Launching production stack via scripts\bat\start_umalogi.bat ...
    call "%ROOT%scripts\bat\start_umalogi.bat"
) else (
    echo  [warn] scripts\bat\start_umalogi.bat not found. Falling back to direct launch.
    start "UMALOGI_DASHBOARD"  /D "%ROOT%" cmd /k "py -m streamlit run web_streamlit\app.py --server.port 8501 --browser.gatherUsageStats false"
    timeout /t 3 /nobreak > nul
    start "UMALOGI_AUTORUNNER" /D "%ROOT%" cmd /k "py scripts\today_auto_runner.py --continuous"
    timeout /t 3 /nobreak > nul
    start "UMALOGI_WATCHDOG"   /D "%ROOT%" cmd /k "py scripts\watchdog.py --interval 5"
    timeout /t 3 /nobreak > nul
    start "UMALOGI_WEBUI"      /D "%ROOT%web" cmd /k "npm start"
)

echo.
echo  [done] UMALOGI auto-recovery sequence finished.
echo         Check: http://localhost:8501 / http://localhost:3000
echo.
