
import sys, os
sys.path.insert(0, r"C:\dev\horse-racing-ai")
from dotenv import load_dotenv
load_dotenv(r"C:\dev\horse-racing-ai\.env", override=False)
from src.scraper.jravan_client import (
    JVLinkClient, JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING, parse_record, _int
)
import io, os
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sid = os.getenv("JRAVAN_SID", "UMALOGI00")
se_count = 0
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
        if rec_type == "SE" and se_count < 5:
            uma_ban = _int(data, slice(28,30))
            rank_bytes = data[202:206] if len(data) > 206 else b"?"
            rank_val   = _int(data, slice(202,204))
            fin_bytes  = data[211:215] if len(data) > 215 else b"?"
            print(f"SE#{se_count+1} uma={uma_ban} rank_raw={rank_bytes!r} rank_val={rank_val} fin_raw={fin_bytes!r} len={len(data)}")
            # 190-230範囲をダンプ
            chunk = data[190:230] if len(data)>230 else data[190:]
            print(f"  bytes[190:230]={chunk!r}")
            se_count += 1
        if se_count >= 5: break
print("done")
