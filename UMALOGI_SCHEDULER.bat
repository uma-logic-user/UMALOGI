@echo off
title UMALOGI Scheduler Supervisor

REM ============================================================
REM  UMALOGI weekly autopilot supervisor (self-heal loop).
REM
REM  Runs today_auto_runner.py --continuous in the foreground and
REM  restarts it 30 seconds after any exit (crash or manual kill).
REM  Double-start safety is enforced by the runner's own PID lock
REM  (data/auto_runner.pid, zombie-PID auto-clear included).
REM
REM  Launched at logon by the Startup-folder UMALOGI_Scheduler.vbs.
REM
REM  W-085: This file MUST stay 100% ASCII. Japanese text saved as
REM  UTF-8 breaks cmd.exe parsing in a fresh console (CP932 default).
REM
REM  Child stderr is appended to data\supervisor_stderr.log so that
REM  early-startup crashes (before file logging is configured) are
REM  never lost when the console window is minimized or closed.
REM ============================================================

cd /d C:\dev\horse-racing-ai

echo.
echo ============================================================
echo   UMALOGI weekly autopilot supervisor
echo ============================================================
echo.
echo  Cycle: Fri 20:00 sync + provisional picks
echo         Sat/Sun 08:30 pre-race picks + result watch loop
echo         Sun weekly P+L report to Discord, then sleep to Friday
echo.
echo  Logs:  data\auto_runner.log / data\scheduler.log
echo         data\supervisor_stderr.log (child startup errors)
echo  Stop:  close this window or Ctrl+C
echo.
echo ============================================================
echo.

:loop
echo [%date% %time%] starting today_auto_runner --continuous >> data\supervisor_stderr.log
py -3 scripts/today_auto_runner.py --continuous 2>> data\supervisor_stderr.log
echo.
echo [%date% %time%] runner exited (code %ERRORLEVEL%). Restarting in 30 seconds...
echo [%date% %time%] runner exited (code %ERRORLEVEL%) >> data\supervisor_stderr.log
timeout /t 30 /nobreak
echo.
echo restarting...
goto loop
