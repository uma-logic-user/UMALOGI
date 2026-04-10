import subprocess
import time
from datetime import date, timedelta

# 取得期間の設定（2024年1月1日〜今日）
start_date = date(2024, 1, 1)
end_date = date.today()

print("=== 過去の週末（土日）全レース一括取得を開始します ===")

curr = start_date
while curr <= end_date:
    # weekday() は 5:土曜日, 6:日曜日
    if curr.weekday() in (5, 6):
        date_str = curr.strftime("%Y%m%d")
        print(f"\n>>> [{date_str}] の全レースを取得中...")
        
        # 日別モードでコマンドを実行
        subprocess.run([
            "py", "-m", "src.scraper.fetch_historical", 
            "--date", date_str, 
            "--grade", "all"
        ])
        
        # 相手サーバーに優しく（BAN対策）
        time.sleep(3)
        
    curr += timedelta(days=1)

print("\n=== 全日程の取得が完了しました！ ===")