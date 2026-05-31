
import sys, os, io
sys.path.insert(0, r"C:\dev\horse-racing-ai")
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from dotenv import load_dotenv
load_dotenv(r"C:\dev\horse-racing-ai\.env", override=False)
from src.scraper.jravan_client import JVLinkClient, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING, _int

sid = os.getenv("JRAVAN_SID", "UMALOGI00")
se_count = 0
target_se = [480, 490, 495, 500, 505, 510]  # FILE-22の最初あたり

with JVLinkClient(sid) as c:
    code = c.open("RACE", "20260503", 1)
    print(f"JVOpen code={code}")
    while True:
        code, data = c.read_record()
        if code == 0: break
        if code == -1: continue
        if code == -3:
            import time; time.sleep(1); continue
        if code < 0: break
        if not data: continue
        rec_type = data[0:2].decode("ascii","replace") if len(data)>=2 else ""
        if rec_type == "SE":
            se_count += 1
            if se_count in target_se:
                uma_ban = _int(data, slice(28,30))
                data_cat = chr(data[2])
                # オフセット 190-230 のダンプ
                chunk = data[190:230] if len(data)>230 else data[190:]
                # ASCII部分を探す
                ascii_hits = [(i, data[i:i+4]) for i in range(180, 220) if all(48<=b<=57 for b in data[i:i+4]) and any(b>48 for b in data[i:i+4])]
                print(f"SE#{se_count} uma={uma_ban} cat={data_cat} len={len(data)}")
                print(f"  bytes[190:230]={chunk!r}")
                print(f"  ASCII数字列4桁({180}-{220}): {ascii_hits[:5]}")

print("done, total SE processed:", se_count)
