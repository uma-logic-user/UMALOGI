"""
SEレコードのrankオフセットを高速に特定するスクリプト。

OPT_STORED で取得したSEレコードと race_payouts の単勝組み合わせ（=1着馬番）を
クロス参照し、どのオフセットが馬番に対応する "01" を示すかを数え上げる。

実行: py -3.14-32 scripts/find_se_rank_fast.py
"""
from __future__ import annotations
import os, sys, time
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVLinkClient, OPT_STORED,
    JVREAD_EOF, JVREAD_FILECHANGE, JVREAD_DOWNLOADING,
    _make_race_id, _int,
)
from src.database.init_db import init_db

# ── 設定 ─────────────────────────────────────────────────────────
FROMTIME    = "20240101"  # 2024年1月から
MAX_RACES   = 100         # 最大レース数
SEARCH_RANGE = range(195, 260)  # rankが存在すると思われるオフセット範囲

env_path = _ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text("utf-8", errors="replace").splitlines():
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

sid = os.environ.get("JRAVAN_SID", "")
if not sid:
    print("ERROR: JRAVAN_SID 未設定"); sys.exit(1)

# ── DBから払戻データ（単勝1着馬番）を事前読み込み ─────────────────
conn = init_db()
payout_winners: dict[str, int] = {}
rows = conn.execute("""
    SELECT race_id, combination FROM race_payouts
    WHERE bet_type = '単勝' AND race_id LIKE '2024%'
    AND combination IS NOT NULL
    AND CAST(combination AS INTEGER) BETWEEN 1 AND 18
    LIMIT 3000
""").fetchall()
for race_id, combo in rows:
    try:
        horse_num = int(combo.strip())
        if 1 <= horse_num <= 18:
            payout_winners[race_id] = horse_num
    except Exception:
        pass
conn.close()
print(f"払戻から{len(payout_winners)}レースの1着馬番を取得")

# ── JVLinkからSEレコードを取得 ────────────────────────────────────
# offset → {count_correct, count_total} で一致率を集計
hit_counts: dict[int, int]   = defaultdict(int)
total_counts: dict[int, int] = defaultdict(int)

se_by_race: dict[str, list[bytes]] = defaultdict(list)   # race_id → SE記録リスト
n_races = 0

print(f"JVLink OPT_STORED で SE レコード取得中 (fromtime={FROMTIME})...")
try:
    with JVLinkClient(sid) as client:
        ret = client.open("RACE", FROMTIME, OPT_STORED)
        print(f"JVOpen: ret={ret}")
        if ret < 0:
            print(f"JVOpen 失敗 (ret={ret})"); sys.exit(1)

        for _ in range(20_000_000):
            code, data = client.read_record()
            if code == JVREAD_EOF:
                break
            if code == JVREAD_FILECHANGE:
                # ファイル境界: これまで収集したデータを解析
                for race_id, se_list in se_by_race.items():
                    if race_id not in payout_winners:
                        continue
                    winner_horse = payout_winners[race_id]
                    for off in SEARCH_RANGE:
                        for se_raw in se_list:
                            if off + 2 > len(se_raw):
                                continue
                            horse_num = _int(se_raw, slice(28, 30)) or 0
                            val_raw = se_raw[off:off+2]
                            try:
                                val = int(val_raw.decode("ascii", errors="replace").strip())
                            except Exception:
                                val = -1
                            total_counts[off] += 1
                            if horse_num == winner_horse and val == 1:
                                hit_counts[off] += 1
                se_by_race.clear()
                n_races += 1
                if n_races >= MAX_RACES:
                    break
                continue
            if code == JVREAD_DOWNLOADING:
                time.sleep(0.5); continue
            if code < 0 or not data or len(data) < 50:
                continue

            rec_type = data[:2].decode("ascii", errors="replace")
            if rec_type == "SE":
                race_id = _make_race_id(data)
                if race_id:
                    se_by_race[race_id].append(bytes(data))

except KeyboardInterrupt:
    print("\n中断")

# ── 結果表示 ─────────────────────────────────────────────────────
print(f"\n処理レース数: {n_races}")
print(f"\n=== オフセット別 一致率 (上位20) ===")
scored: list[tuple[float, int, int, int]] = []
for off in SEARCH_RANGE:
    total = total_counts[off]
    hit   = hit_counts[off]
    if total > 5:
        rate = hit / total * 100
        scored.append((rate, hit, total, off))
scored.sort(reverse=True)
for rate, hit, total, off in scored[:20]:
    marker = " ← ★推奨" if rate >= 50 else ""
    print(f"  offset {off:3d}: {hit:4d}/{total:4d} = {rate:5.1f}%{marker}")

if scored:
    best_off = scored[0][3]
    print(f"\n推奨オフセット: slice({best_off}, {best_off+2})")
    print(f"現在の設定:    slice(202, 204)")
    if best_off != 202:
        print(f"→ jravan_client.py の _SE_RANK を slice({best_off}, {best_off+2}) に変更してください")
    else:
        print("→ 現在の設定は正しいです")
