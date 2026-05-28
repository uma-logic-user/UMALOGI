@echo off
chcp 65001 > nul
title UMALOGI セットアップ

echo.
echo  ============================================================
echo    UMALOGI  初回セットアップ
echo  ============================================================
echo.

set ROOT=%~dp0

:: ── [1/4] .env 確認 ────────────────────────────────────────────
echo  [1/4] .env ファイルを確認中...
if exist "%ROOT%.env" (
    echo        .env は既に存在します。スキップします。
) else (
    if exist "%ROOT%.env.example" (
        copy "%ROOT%.env.example" "%ROOT%.env" > nul
        echo        .env.example から .env を作成しました。
        echo.
        echo  *** 重要 ***
        echo  %ROOT%.env を開き、以下の項目を設定してください:
        echo    - DISCORD_WEBHOOK_URL
        echo    - JRAVAN_SID
        echo    - INITIAL_BANKROLL
        echo    - NOTE_EMAIL / NOTE_PASSWORD (任意)
        echo  設定後、このスクリプトを再実行するか続行してください。
        echo.
        pause
    ) else (
        echo  [警告] .env.example が見つかりません。.env を手動で作成してください。
    )
)

:: ── [2/4] Python 依存インストール ──────────────────────────────
echo  [2/4] Python パッケージをインストール中...
echo        pip install -r requirements.txt
cd /d "%ROOT%"
py -m pip install -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo  [エラー] pip install に失敗しました。Python のインストールを確認してください。
    pause
    exit /b 1
)
echo        Python パッケージのインストール完了。

:: ── [3/4] npm install ──────────────────────────────────────────
echo.
echo  [3/4] Node.js パッケージをインストール中...
echo        cd web ^&^& npm install
cd /d "%ROOT%web"
call npm install
if %ERRORLEVEL% neq 0 (
    echo  [エラー] npm install に失敗しました。Node.js のインストールを確認してください。
    pause
    exit /b 1
)
echo        Node.js パッケージのインストール完了。

:: ── [4/4] DB 初期化 ─────────────────────────────────────────────
echo.
echo  [4/4] データベースを初期化中...
echo        python -m src.database.init_db
cd /d "%ROOT%"
py -m src.database.init_db
if %ERRORLEVEL% neq 0 (
    echo  [エラー] DB 初期化に失敗しました。
    pause
    exit /b 1
)
echo        DB 初期化完了。

:: ── 完了 ────────────────────────────────────────────────────────
echo.
echo  ============================================================
echo    セットアップ完了！
echo.
echo    次のコマンドで各サービスを起動できます:
echo      start_nextjs.bat   : Next.js ダッシュボード
echo      start_streamlit.bat: Streamlit 運用ダッシュボード
echo      start_umalogi.bat  : Next.js + AI スケジューラー
echo      start_docker.bat   : Docker 一括起動
echo  ============================================================
echo.
pause
