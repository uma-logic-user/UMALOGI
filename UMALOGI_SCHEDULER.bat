@echo off
chcp 65001 > nul
title UMALOGI Scheduler - 週次オートパイロット

cd /d C:\dev\horse-racing-ai

echo.
echo ============================================================
echo   UMALOGI 週次オートパイロット スケジューラー
echo ============================================================
echo.
echo  動作サイクル:
echo    金曜 20:00  JVLink同期 + 土日両日の暫定予想生成
echo    土曜 08:30  直前予想 + 結果速報 監視ループ
echo    土曜 20:00  JVLink再同期 + 日曜暫定予想更新
echo    日曜 08:30  直前予想 + 結果速報 監視ループ (WIN5含む)
echo    日曜 完了後 週次収支レポートをDiscordへ送信
echo    月〜木      完全スリープ (次週金曜まで自動復帰)
echo.
echo  ログ: data\scheduler.log
echo  停止: このウィンドウを閉じる か Ctrl+C
echo.
echo ============================================================
echo.

:loop
py -3 scripts/today_auto_runner.py --continuous
echo.
echo [%date% %time%] スケジューラーが終了しました。30秒後に再起動します...
echo.
timeout /t 30 /nobreak
echo.
echo 再起動中...
goto loop
