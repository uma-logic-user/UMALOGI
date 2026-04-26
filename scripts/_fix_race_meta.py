# -*- coding: utf-8 -*-
"""本日のracesテーブルにレース名・距離・コースを補完する（netkeiba scrape）"""
import re
import sys
import time
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from src.database.init_db import init_db

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://race.netkeiba.com/",
}
SHUTUBA_URL = "https://race.netkeiba.com/race/shutuba.html"


def parse_meta(soup: BeautifulSoup) -> tuple[str, int, str, str]:
    race_name = ""
    distance  = 0
    surface   = ""
    direction = ""

    # タイトルからレース名 ("3歳未勝利 | 2026年4月25日 福島1R..." → "3歳未勝利")
    title_tag = soup.find("title")
    if title_tag:
        m = re.match(r"^(.+?)\s*[|｜]", title_tag.get_text(strip=True))
        if m:
            race_name = m.group(1).strip()

    # div.RaceData01 → 距離・コース (例: "09:45発走 / ダ1700m (右) / 天候:晴 / 馬場:良")
    data01 = soup.select_one("div.RaceData01")
    if data01:
        txt = data01.get_text(" ", strip=True)
        # 距離: 数字3〜4桁 + m
        dm = re.search(r"(\d{3,4})m", txt)
        if dm:
            distance = int(dm.group(1))
        # コース: 芝/ダ/障 を直前の文字で判定
        sm = re.search(r"(芝|ダ|障)\s*\d{3,4}m", txt)
        if sm:
            c = sm.group(1)
            if c == "芝":
                surface = "芝"
            elif c == "障":
                surface = "障害"
            else:
                surface = "ダート"
        # 回り (右/左)
        dm2 = re.search(r"[（(](右|左|直線)[）)]", txt)
        if dm2:
            direction = dm2.group(1)

    return race_name, distance, surface, direction


def main() -> None:
    conn = init_db()
    today_races = [
        r[0]
        for r in conn.execute(
            "SELECT race_id FROM races WHERE date='2026-04-25' ORDER BY race_id"
        ).fetchall()
    ]
    print(f"処理対象: {len(today_races)} レース")

    updated = errors = 0
    for i, rid in enumerate(today_races):
        try:
            resp = requests.get(
                SHUTUBA_URL, params={"race_id": rid}, headers=HEADERS, timeout=10
            )
            resp.encoding = "euc-jp"
            soup = BeautifulSoup(resp.text, "html.parser")
            race_name, distance, surface, direction = parse_meta(soup)
            conn.execute(
                "UPDATE races SET race_name=?, distance=?, surface=?, track_direction=?"
                " WHERE race_id=?",
                (race_name, distance, surface, direction, rid),
            )
            conn.commit()
            updated += 1
            print(f"  [{i+1:02d}/36] {rid}: 【{race_name}】{surface}{distance}m {direction}")
            time.sleep(0.7)
        except Exception as e:
            errors += 1
            print(f"  NG {rid}: {e}")

    conn.close()
    print(f"\n完了: {updated} 件成功 / {errors} 件失敗")


if __name__ == "__main__":
    main()
