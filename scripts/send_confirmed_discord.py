"""本日の確定予想をDiscordへ送信するワンショットスクリプト。"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

TARGET_DATE = "2026-05-09"

conn = sqlite3.connect(str(_ROOT / "data" / "umalogi.db"))

total_direct = conn.execute(
    "SELECT COUNT(DISTINCT p.race_id) FROM predictions p "
    "JOIN races r ON r.race_id = p.race_id "
    "WHERE r.date = ? AND p.model_type LIKE ?",
    (TARGET_DATE, "%(直前)%"),
).fetchone()[0]

ev_high = conn.execute(
    """
    SELECT p.race_id, r.race_name, p.bet_type, p.recommended_bet, p.expected_value
    FROM predictions p
    JOIN races r ON r.race_id = p.race_id
    WHERE r.date = ?
      AND p.model_type LIKE '%(直前)%'
      AND p.expected_value IS NOT NULL
      AND p.expected_value >= 2.0
    ORDER BY p.expected_value DESC
    LIMIT 10
    """,
    (TARGET_DATE,),
).fetchall()

conn.close()

lines = [
    f"🎯 **【UMALOGI 確定推論 完了】** {TARGET_DATE}",
    f"全 **{total_direct}R** の直前予想が確定しました（netkeibaオッズ充填済）",
    "",
    "📊 **EV上位買い目:**",
]
for r in ev_high:
    name = (r[1] or r[0][-4:])[:10]
    amt = int(float(r[3])) if r[3] else 0
    lines.append(f"  `{r[0]}` {name} | {r[2]} | ¥{amt} | **EV={float(r[4]):.2f}**")

lines += ["", "🤖 自動修復・確定化 by UMALOGI watchdog"]
msg = "\n".join(lines)

webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
if webhook_url:
    import requests  # type: ignore[import]
    resp = requests.post(webhook_url, json={"content": msg}, timeout=10)
    print(f"Discord送信: HTTP {resp.status_code}")
else:
    print("DISCORD_WEBHOOK_URL 未設定 — メッセージ内容:")
print(msg)
