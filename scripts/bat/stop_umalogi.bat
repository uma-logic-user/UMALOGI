@echo off
chcp 65001 > nul
setlocal
title UMALOGI - 停止

REM ============================================================
REM  UMALOGI 関連プロセスのみを安全に停止する。
REM
REM  対象（コマンドラインにスクリプト名を含むものだけ）:
REM    - scripts/today_auto_runner.py   (オートパイロット)
REM    - scripts/scheduler.py           (scheduler方式)
REM    - scripts/watchdog.py            (自己修復番犬)
REM    - web_streamlit/app.py           (Streamlitダッシュボード)
REM
REM  安全設計（無関係な Python を巻き込まないため）:
REM    1) 上記スクリプト名を CommandLine に含むプロセスのみを
REM       PID 指定で Stop-Process する。
REM    2) 補助として UMALOGI_ で始まるタイトルのウィンドウを
REM       プロセスツリーごと(/T) taskkill する。
REM  → "taskkill /im python.exe" のような全 Python 一括killは行わない。
REM ============================================================

echo.
echo  ============================================================
echo    UMALOGI 関連プロセスを安全に停止します
echo  ============================================================
echo.

echo  [1/2] UMALOGI スクリプト実行中の python プロセスを検索・停止中...
REM  安全のため Name を python 系に限定し、bash/cmd/エディタ等が同名文字列を
REM  含んでいても巻き込まない。パターンはバックスラッシュ非使用。
powershell -NoProfile -Command ^
  "$pat='today_auto_runner.py|scheduler.py|watchdog.py|web_streamlit';" ^
  "$procs = Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|^py' -and $_.CommandLine -and ($_.CommandLine -match $pat) };" ^
  "if (-not $procs) { Write-Host '      対象プロセスは見つかりませんでした。' }" ^
  "else { foreach ($p in $procs) { Write-Host ('      停止 PID=' + $p.ProcessId + '  ' + $p.Name); Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } }"

echo.
echo  [2/2] UMALOGI ウィンドウ(cmd ラッパー)をツリーごと停止中...
taskkill /FI "WINDOWTITLE eq UMALOGI_AUTORUNNER*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_SCHEDULER*"  /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_WATCHDOG*"   /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq UMALOGI_DASHBOARD*"  /T /F >nul 2>&1

echo.
echo  ============================================================
echo    停止処理が完了しました。
echo    残存確認:  tasklist ^| findstr /i "python streamlit"
echo  ============================================================
echo.
timeout /t 8 /nobreak
endlocal
