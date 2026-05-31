@echo off
echo 血統データ(BLOD)を取得中...
py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --dataspec BLOD
echo マスタデータ(DICT)を取得中...
py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --dataspec DICT
echo すべての取得が完了しました。
pause