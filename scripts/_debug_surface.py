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
    print("TEXT:", repr(txt[:300]))
    # Check each character around 'm'
    for i, ch in enumerate(txt):
        if txt[i:i+1] in ("m", "ｍ"):
            print(f"  pos {i}: ...{repr(txt[max(0,i-10):i+5])}...")
else:
    print("div.RaceData01 not found")
    # Try to find what's in the page
    for tag in soup.find_all(class_=lambda c: c and "Race" in c)[:10]:
        print(f"  Found: {tag.name}.{tag.get('class')} -> {repr(tag.get_text(' ', strip=True)[:80])}")
