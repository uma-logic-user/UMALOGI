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
print("Content-Type header:", resp.headers.get("Content-Type"))
print("requests detected encoding:", resp.encoding)
print("apparent_encoding:", resp.apparent_encoding)

# Try EUC-JP
try:
    txt_euc = resp.content.decode("euc-jp", errors="replace")
    soup = BeautifulSoup(txt_euc, "html.parser")
    data01 = soup.select_one("div.RaceData01")
    if data01:
        print("\nEUC-JP decode:", data01.get_text(" ", strip=True)[:100])
except Exception as e:
    print("EUC-JP failed:", e)

# Try CP932
try:
    txt_cp932 = resp.content.decode("cp932", errors="replace")
    soup = BeautifulSoup(txt_cp932, "html.parser")
    data01 = soup.select_one("div.RaceData01")
    if data01:
        print("\nCP932 decode:", data01.get_text(" ", strip=True)[:100])
except Exception as e:
    print("CP932 failed:", e)
