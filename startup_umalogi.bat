@echo off
chcp 65001 > nul
title UMALOGI - PC起動時 自動復旧ランチャー

REM ============================================================
REM  startup_umalogi.bat
REM    PC 起動 / ログオン時の「自動復旧」エントリポイント。
REM    Windows Update 等による強制再起動の後、本番稼働スタックを
REM    人手ゼロで復帰させるためのバッチ。
REM
REM    タスクスケジューラ or スタートアップフォルダへ登録して使う。
REM    （登録手順は scripts\bat\README_BAT.md の §5/§6 を参照）
REM
REM  本番稼働スタック（実体は scripts\bat\start_umalogi.bat に集約）:
REM    1) Streamlit ダッシュボード   web_streamlit/app.py
REM    2) 週次自律オートパイロット   today_auto_runner.py --continuous
REM    3) 自己修復ウォッチドッグ     watchdog.py --interval 5
REM ============================================================

REM ── プロジェクトルートをこのバッチの位置から解決 ──
set "ROOT=%~dp0"
cd /d "%ROOT%"

echo.
echo  ============================================================
echo    UMALOGI 自動復旧起動  (%DATE% %TIME%)
echo    ROOT: %ROOT%
echo  ============================================================

REM ── 起動直後の安定化待機（DBロック / JVLink初期化 / ネットワーク確立の猶予） ──
echo  [待機] システム安定化のため 45 秒待機します...
timeout /t 45 /nobreak > nul

REM ── Python 実行環境 ─────────────────────────────────────────
REM  本プロジェクトは仮想環境(venv/Poetry)を「不使用」で、システムの py ランチャー
REM  (py = 64bit Python 3.14) を使う。将来 venv を導入した場合のみ自動で activate する。
if exist "%ROOT%.venv\Scripts\activate.bat" (
    echo  [env] .venv を検出。仮想環境をアクティベートします。
    call "%ROOT%.venv\Scripts\activate.bat"
) else (
    echo  [env] venv 不使用。システムの py ランチャーを使用します。
)

REM ── 本番稼働スタックの起動（正本バッチへ委譲・ロジック二重化を避ける） ──
REM  start_umalogi.bat 側に「二重起動ガード」「scheduler.py との排他ガード」が
REM  実装済みのため、再起動後に既存プロセスが生き残っていても安全に再開できる。
if exist "%ROOT%scripts\bat\start_umalogi.bat" (
    echo  [起動] 本番稼働スタックを起動します（オートパイロット方式）...
    call "%ROOT%scripts\bat\start_umalogi.bat"
) else (
    echo  [警告] scripts\bat\start_umalogi.bat が見つかりません。フォールバックで直接起動します。
    start "UMALOGI_DASHBOARD"  /D "%ROOT%" cmd /k "py -m streamlit run web_streamlit\app.py --server.port 8501 --browser.gatherUsageStats false"
    timeout /t 3 /nobreak > nul
    start "UMALOGI_AUTORUNNER" /D "%ROOT%" cmd /k "py scripts\today_auto_runner.py --continuous"
    timeout /t 3 /nobreak > nul
    start "UMALOGI_WATCHDOG"   /D "%ROOT%" cmd /k "py scripts\watchdog.py --interval 5"
)

echo.
echo  [完了] UMALOGI 自動復旧シーケンスを実行しました。
echo         稼働確認: http://localhost:8501 ／ tasklist ^| findstr /i python
echo.
