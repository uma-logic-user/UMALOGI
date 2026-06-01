@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion
title UMALOGI - 起動ランチャー

REM ============================================================
REM  UMALOGI 本番稼働ワンクリック起動（本番実態スタック）
REM    1) Streamlit 運用ダッシュボード      web_streamlit/app.py
REM    2) 週次自律オートパイロット(常駐)     scripts/today_auto_runner.py --continuous
REM    3) 自己修復ウォッチドッグ(常駐)       scripts/watchdog.py --interval 5
REM
REM  ※ オートパイロット(today_auto_runner)と scheduler.py は
REM    同一の週次自動運転の「排他的2実装」。本batは本番実態である
REM    オートパイロット方式で起動する。schedule ライブラリ方式
REM    (scheduler.py) を使いたい場合は start_scheduler_mode.bat を使う。
REM    両方を同時に動かすと二重予想・二重通知になるため厳禁。
REM ============================================================

REM ── プロジェクトルートを絶対パスで解決（このbatは scripts\bat\ 配下） ──
pushd "%~dp0..\.."
set "ROOT=%CD%"
popd

echo.
echo  ============================================================
echo    UMALOGI 本番稼働 起動
echo    ROOT: %ROOT%
echo  ============================================================
echo.

REM ── 稼働状況の事前検出（コマンドライン照合・二重起動防止） ──
call :count "today_auto_runner.py"  AUTO_RUNNING
call :count "watchdog.py"           WD_RUNNING
call :count "web_streamlit"         DASH_RUNNING
call :count "scheduler.py"          SCHED_RUNNING

REM ── scheduler.py が動いていれば排他のため中断 ──
if not "%SCHED_RUNNING%"=="0" (
    echo  [中断] scheduler.py が稼働中です（%SCHED_RUNNING% プロセス）。
    echo         オートパイロットと scheduler は排他です。
    echo         先に stop_umalogi.bat で停止してから再実行してください。
    echo.
    pause
    endlocal & exit /b 1
)

REM ── 1) Streamlit ダッシュボード ──
if "%DASH_RUNNING%"=="0" (
    echo  [1/3] Streamlit ダッシュボードを起動中... http://localhost:8501
    start "UMALOGI_DASHBOARD" /D "%ROOT%" cmd /k "py -m streamlit run web_streamlit\app.py --server.port 8501 --browser.gatherUsageStats false"
) else (
    echo  [1/3] ダッシュボードは既に稼働中（%DASH_RUNNING% プロセス）。スキップ。
)
timeout /t 3 /nobreak > nul

REM ── 2) 週次自律オートパイロット ──
if "%AUTO_RUNNING%"=="0" (
    echo  [2/3] オートパイロットを起動中... scripts\today_auto_runner.py --continuous
    start "UMALOGI_AUTORUNNER" /D "%ROOT%" cmd /k "py scripts\today_auto_runner.py --continuous"
) else (
    echo  [2/3] オートパイロットは既に稼働中（%AUTO_RUNNING% プロセス）。スキップ。
)
timeout /t 2 /nobreak > nul

REM ── 3) 自己修復ウォッチドッグ ──
if "%WD_RUNNING%"=="0" (
    echo  [3/3] ウォッチドッグを起動中... scripts\watchdog.py --interval 5
    start "UMALOGI_WATCHDOG" /D "%ROOT%" cmd /k "py scripts\watchdog.py --interval 5"
) else (
    echo  [3/3] ウォッチドッグは既に稼働中（%WD_RUNNING% プロセス）。スキップ。
)

echo.
echo  ============================================================
echo    ALL SYSTEMS GO
echo.
echo    ダッシュボード   : http://localhost:8501
echo    オートパイロット : UMALOGI_AUTORUNNER ウィンドウ
echo    ウォッチドッグ   : UMALOGI_WATCHDOG   ウィンドウ
echo    ログ             : data\scheduler.log / data\watchdog.log
echo.
echo    停止             : stop_umalogi.bat を実行
echo  ============================================================
echo.
echo  このランチャーウィンドウは閉じても構いません。
echo  各プロセスは独立ウィンドウで継続稼働します。
echo.
timeout /t 20 /nobreak
endlocal
exit /b 0

REM ── サブルーチン: 指定スクリプトを実行中の python プロセス数を変数へ ──
REM   ・Name を python 系に限定（bash/cmd/エディタ等の誤検出を排除）
REM   ・パターンはバックスラッシュ非使用（cmd→PowerShell の \ 取りこぼし回避）
REM   ・for/f のパイプ問題を避けるため一時ファイル経由で set /p 読み取り
:count
set "%~2=0"
powershell -NoProfile -Command "(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|^py' -and $_.CommandLine -and ($_.CommandLine -match '%~1') } | Measure-Object).Count" > "%TEMP%\_umalogi_cnt.txt" 2>nul
set /p %~2=<"%TEMP%\_umalogi_cnt.txt"
del "%TEMP%\_umalogi_cnt.txt" 2>nul
exit /b 0
