# -*- coding: utf-8 -*-
import io
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
resp = requests.get(
    "https://race.netkeiba.com/race/shutuba.html",
    params={"race_id": "202603010501"},
    headers=HEADERS,
    timeout=10,
)
resp.encoding = "utf-8"
soup = BeautifulSoup(resp.text, "html.parser")
data01 = soup.select_one("div.RaceData01")
if data01:
    txt = data01.get_text(" ", strip=True)
    # Print hex codes for first 60 chars
    print("HEX dump:")
    for i, ch in enumerate(txt[:60]):
        print(f"  [{i:02d}] U+{ord(ch):04X}  {repr(ch)}")
