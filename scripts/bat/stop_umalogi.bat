@echo off
setlocal
title UMALOGI Stop

REM ============================================================
REM  Safely stop UMALOGI processes only.
REM
REM  Targets (matched by script name in the command line):
REM    - scripts/today_auto_runner.py   (autopilot)
REM    - scripts/scheduler.py           (scheduler mode)
REM    - scripts/watchdog.py            (self-healing watchdog)
REM    - web_streamlit/app.py           (Streamlit dashboard)
REM    - the node process LISTENing on port 3000 (Next.js Web UI)
REM
REM  Safety design (never hit unrelated python/node):
REM    1) Stop-Process by PID, only for processes whose command line
REM       contains one of the script names above.
REM    2) Additionally taskkill window trees titled UMALOGI_*.
REM  -> Never do a blanket "taskkill /im python.exe" or node.exe.
REM
REM  W-085: This file MUST stay 100% ASCII (cmd.exe CP932 parsing trap).
REM ============================================================

echo.
echo  ============================================================
echo    Stopping UMALOGI processes safely
echo  ============================================================
echo.

echo  [1/3] Stopping UMALOGI python processes (matched by command line)...
powershell -NoProfile -Command ^
  "$pat='today_auto_runner.py|scheduler.py|watchdog.py|web_streamlit';" ^
  "$procs = Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|^py' -and $_.CommandLine -and ($_.CommandLine -match $pat) };" ^
  "if (-not $procs) { Write-Host '      no matching process found.' }" ^
  "else { foreach ($p in $procs) { Write-Host ('      stop PID=' + $p.ProcessId + '  ' + $p.Name); Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } }"

echo.
echo  [2/3] Stopping Next.js Web UI (only the node listening on port 3000)...
powershell -NoProfile -Command ^
  "Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue | ForEach-Object {" ^
  "  $p = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue;" ^
  "  if ($p -and $p.ProcessName -eq 'node') { Write-Host ('      stop PID=' + $p.Id + '  node (Next.js)'); Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } }"

echo.
echo  [3/3] Killing UMALOGI cmd wrapper windows (process tree)...
taskkill /FI "WINDOWTITLE eq UMALOGI_AUTORUNNER*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_SCHEDULER*"  /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_WATCHDOG*"   /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_DASHBOARD*"  /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_WEBUI*"      /T /F >nul 2>&1

echo.
echo  ============================================================
echo    Stop sequence finished.
echo    Verify:  tasklist ^| findstr /i "python streamlit node"
echo  ============================================================
echo.
timeout /t 8 /nobreak
endlocal
