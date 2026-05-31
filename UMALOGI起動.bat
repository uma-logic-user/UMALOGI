@echo off
chcp 65001 > nul
echo ===================================================
echo       UMALOGI 投資ダッシュボード 起動システム
echo ===================================================
echo.

:: プロジェクトフォルダへ移動
cd /d C:\dev\horse-racing-ai

echo [1/2] データベースへの直結ルートを確認中...
echo.

echo [2/2] UIサーバーを起動しています（この画面は閉じないでください）...
echo ※サーバー起動後、ブラウザが自動で立ち上がります。
echo.

:: StreamlitをPython経由で確実に起動する
py -m streamlit run web_streamlit/app.py --server.port 8501

pause