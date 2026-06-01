@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion
title UMALOGI - scheduler方式(代替)

REM ============================================================
REM  UMALOGI 代替起動: schedule ライブラリ方式 (scripts/scheduler.py)
REM
REM  通常運用は start_umalogi.bat（オートパイロット方式）を使う。
REM  本batは scheduler.py を常駐マスターとして使いたい場合の代替。
REM  scheduler.py は内部で today_auto_runner を所定時刻に起動するため、
REM  オートパイロット(today_auto_runner --continuous)とは「排他」。
REM
REM  起動内容:
REM    1) Streamlit ダッシュボード  web_streamlit/app.py
REM    2) scheduler.py（64bit常駐・JVLink操作は32bit subprocessへ委譲）
REM    3) watchdog.py（自己修復番犬）
REM ============================================================

pushd "%~dp0..\.."
set "ROOT=%CD%"
popd

call :count "today_auto_runner.py"  AUTO_RUNNING
call :count "scheduler.py"          SCHED_RUNNING
call :count "watchdog.py"           WD_RUNNING
call :count "web_streamlit"         DASH_RUNNING

REM ── オートパイロットが動いていれば排他のため中断 ──
if not "%AUTO_RUNNING%"=="0" (
    echo  [中断] today_auto_runner が稼働中です（%AUTO_RUNNING% プロセス）。
    echo         scheduler 方式とオートパイロットは排他です。
    echo         先に stop_umalogi.bat で停止してください。
    echo.
    pause
    endlocal & exit /b 1
)

echo.
echo  ============================================================
echo    UMALOGI scheduler 方式 起動   ROOT: %ROOT%
echo  ============================================================
echo.

if "%DASH_RUNNING%"=="0" (
    echo  [1/3] ダッシュボード起動... http://localhost:8501
    start "UMALOGI_DASHBOARD" /D "%ROOT%" cmd /k "py -m streamlit run web_streamlit\app.py --server.port 8501 --browser.gatherUsageStats false"
) else ( echo  [1/3] ダッシュボードは既に稼働中。スキップ。 )
timeout /t 3 /nobreak > nul

if "%SCHED_RUNNING%"=="0" (
    echo  [2/3] scheduler.py 起動...
    start "UMALOGI_SCHEDULER" /D "%ROOT%" cmd /k "py scripts\scheduler.py"
) else ( echo  [2/3] scheduler は既に稼働中。スキップ。 )
timeout /t 2 /nobreak > nul

if "%WD_RUNNING%"=="0" (
    echo  [3/3] watchdog.py 起動...
    start "UMALOGI_WATCHDOG" /D "%ROOT%" cmd /k "py scripts\watchdog.py --interval 5"
) else ( echo  [3/3] watchdog は既に稼働中。スキップ。 )

echo.
echo  ============================================================
echo    起動完了。停止は stop_umalogi.bat。
echo  ============================================================
echo.
timeout /t 20 /nobreak
endlocal
exit /b 0

REM Name を python 系に限定＋バックスラッシュ非使用＋一時ファイル経由（堅牢方式）。
:count
set "%~2=0"
powershell -NoProfile -Command "(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|^py' -and $_.CommandLine -and ($_.CommandLine -match '%~1') } | Measure-Object).Count" > "%TEMP%\_umalogi_cnt.txt" 2>nul
set /p %~2=<"%TEMP%\_umalogi_cnt.txt"
del "%TEMP%\_umalogi_cnt.txt" 2>nul
exit /b 0
