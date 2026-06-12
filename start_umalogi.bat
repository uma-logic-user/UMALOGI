@echo off
title UMALOGI Launcher (delegating)

REM ============================================================
REM  start_umalogi.bat (repo root - delegation shim)
REM
REM  W-085: The old implementation here started Next.js plus
REM  scripts/scheduler.py directly. scheduler.py is the legacy,
REM  mutually-exclusive twin of the autopilot
REM  (today_auto_runner --continuous); running both causes double
REM  predictions and double Discord notifications. On 2026-06-12 the
REM  dangerous direct launch was removed and this file now delegates
REM  to the canonical, guard-protected scripts\bat\start_umalogi.bat.
REM
REM  The Startup-folder shortcut "UMALOGI Kidou.lnk" points at this
REM  file, so it is kept as a shim instead of being deleted.
REM  This file MUST stay 100% ASCII (cmd.exe CP932 parsing trap).
REM ============================================================

echo  [delegate] Calling scripts\bat\start_umalogi.bat (canonical launcher)...
call "%~dp0scripts\bat\start_umalogi.bat"
exit /b %ERRORLEVEL%
