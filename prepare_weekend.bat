@echo off
chcp 65001 >nul
cd /d "C:\dev\horse-racing-ai"

echo ===================================================
echo        UMALOGI 週末準備バッチ
echo ===================================================
echo.

:: ─────────────────────────────────────────────────────
:: 日付計算
::   月曜(0)〜木曜(3): 次の土日を計算（金曜扱い相当）
::   金曜(4)          : 翌日(土) + 翌々日(日)
::   土曜(5)          : 当日(土) + 翌日(日)
::   日曜(6)          : 当日のみ（日曜単独実行）
:: ─────────────────────────────────────────────────────
for /f "tokens=1,2" %%a in ('py -c "import datetime; t=datetime.date.today(); w=t.weekday(); d1=t+datetime.timedelta(days=(1 if w==4 else (0 if w==5 else (6-w if w<4 else 0)))); d2=d1+datetime.timedelta(days=1); print(d1.strftime('%%Y-%%m-%%d'), d2.strftime('%%Y-%%m-%%d'))"') do (
    set DATE1=%%a
    set DATE2=%%b
)

:: YYYYMMDD 形式（暫定予想スクリプト用）
for /f "tokens=*" %%a in ('py -c "import datetime; t=datetime.date.today(); w=t.weekday(); d1=t+datetime.timedelta(days=(1 if w==4 else (0 if w==5 else (6-w if w<4 else 0)))); print(d1.strftime('%%Y%%m%%d'))"') do set DATE1_NODASH=%%a
for /f "tokens=*" %%a in ('py -c "import datetime; t=datetime.date.today(); w=t.weekday(); d1=t+datetime.timedelta(days=(1 if w==4 else (0 if w==5 else (6-w if w<4 else 0)))); d2=d1+datetime.timedelta(days=1); print(d2.strftime('%%Y%%m%%d'))"') do set DATE2_NODASH=%%a

echo 対象日: %DATE1% (%DATE1_NODASH%) ／ %DATE2% (%DATE2_NODASH%)
echo.

:: ─────────────────────────────────────────────────────
:: Step 1: 出馬表補完（netkeiba）
:: ─────────────────────────────────────────────────────
echo ===== Step 1: 出馬表補完 (netkeiba) =====
py scripts\refetch_entries_from_netkeiba.py --date %DATE1% %DATE2%
if %ERRORLEVEL% neq 0 (
    echo [ERROR] 出馬表補完に失敗しました。
    exit /b 1
)
echo [OK] 出馬表補完 完了
echo.

:: ─────────────────────────────────────────────────────
:: Step 2: 暫定予想生成（土曜分）
:: ─────────────────────────────────────────────────────
echo ===== Step 2: 暫定予想生成（土曜: %DATE1_NODASH%）=====
py scripts\force_provisional_today.py %DATE1_NODASH%
if %ERRORLEVEL% neq 0 (
    echo [ERROR] 暫定予想生成（土曜）に失敗しました。
    exit /b 1
)
echo [OK] 暫定予想（土曜）完了
echo.

:: ─────────────────────────────────────────────────────
:: Step 3: 暫定予想生成（日曜分）
:: ─────────────────────────────────────────────────────
echo ===== Step 3: 暫定予想生成（日曜: %DATE2_NODASH%）=====
py scripts\force_provisional_today.py %DATE2_NODASH%
if %ERRORLEVEL% neq 0 (
    echo [ERROR] 暫定予想生成（日曜）に失敗しました。
    exit /b 1
)
echo [OK] 暫定予想（日曜）完了
echo.

:: ─────────────────────────────────────────────────────
:: Step 4: DB 件数確認レポート
:: ─────────────────────────────────────────────────────
echo ===== Step 4: DB 件数確認 =====
py -c "import sqlite3; con=sqlite3.connect('data/umalogi.db'); d1='%DATE1%'; d2='%DATE2%'; races=con.execute('SELECT COUNT(*) FROM races WHERE date IN (?,?)', (d1,d2)).fetchone()[0]; entries=con.execute('SELECT COUNT(*) FROM entries WHERE race_id IN (SELECT race_id FROM races WHERE date IN (?,?))', (d1,d2)).fetchone()[0]; preds=con.execute('SELECT COUNT(*) FROM predictions WHERE race_id IN (SELECT race_id FROM races WHERE date IN (?,?))', (d1,d2)).fetchone()[0]; print(f'  レース数     : {races}件'); print(f'  エントリー数  : {entries}件'); print(f'  暫定予想数    : {preds}件')"
echo.

echo ===================================================
echo        週末準備 完了！
echo ===================================================
pause
