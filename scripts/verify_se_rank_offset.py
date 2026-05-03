"""
SEレコードの着順(rank)バイトオフセット確定スクリプト。

【戦略】
  払戻HRレコードの「単勝・組合せ」= 1着馬番（確実）。
  同一 race_id の SEレコード群を走査し、
  どのバイトオフセットが「単勝馬番」と一致する馬のみで "01" を示すかを数え上げる。
  最多一致のオフセットが _SE_RANK の正解。

【実行方法】
  py -3.14-32 scripts/verify_se_rank_offset.py
  ※ JV-Link は 32bit COM のため 32bit Python 必須

【出力】
  候補オフセット → 一致率ランキングを表示
  最終行に推奨オフセットを表示
"""
from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.scraper.jravan_client import (
    JVLinkClient,
    OPT_NORMAL,
    JVREAD_EOF,
    JVREAD_FILECHANGE,
    JVREAD_DOWNLOADING,
    _make_race_id,
    _PAYOUT_SPECS,
)

# 2025年確定データを OPT_NORMAL (1) で取得する
# JVOpen に渡す fromtime は 14桁 "YYYYMMDDhhmmss" が正式。
# 8桁のみ渡すと jravan_client.open() 内で "000000" を自動付与する。
FROMTIME = "20250101000000"


# ── SE オフセット候補範囲 ────────────────────────────────────────────────────
# 現在の推定: slice(211, 213)
# 探索範囲: 150〜280 (JV-Data 4.5.2 SE レコード後半の合理的な範囲)
SEARCH_START = 150
SEARCH_END   = 280

# 調査するレース数の上限
MAX_RACES = 500


def _extract_tan_winner(hr_data: bytes) -> int | None:
    """HRレコードから単勝1着馬番を取り出す。offset=27, combo=2bytes, amount=5bytes."""
    if len(hr_data) < 34:
        return None
    try:
        combo_raw  = hr_data[27:29]
        amount_raw = hr_data[29:34]
        amount = int(amount_raw.decode("ascii", errors="replace").strip())
        if amount <= 0:
            return None
        horse_num_str = combo_raw.decode("ascii", errors="replace").strip().lstrip("0")
        return int(horse_num_str) if horse_num_str.isdigit() else None
    except Exception:
        return None


def main() -> None:
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text("utf-8", errors="replace").splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    sid = os.environ.get("JRAVAN_SID", "")
    if not sid:
        print("ERROR: JRAVAN_SID 未設定"); sys.exit(1)

    # race_id → {winner_horse_num: int, se_records: list[bytes]}
    # SE レコードは HR より先にストリームに来ることがあるため全件バッファリング
    race_data: dict[str, dict] = {}
    all_se: dict[str, list[bytes]] = {}  # race_id → SE レコードリスト

    print(f"JVLinkからデータ読み込み中 (OPT_NORMAL, fromtime={FROMTIME}, 最大{MAX_RACES}レース)...")

    with JVLinkClient(sid) as client:
        code = client.open("RACE", FROMTIME, OPT_NORMAL)
        if code < 0:
            print(f"JVOpen 失敗 code={code}"); return

        scanned = 0
        for _ in range(10_000_000):
            ret, data = client.read_record()
            if ret == JVREAD_EOF:
                break
            if ret == JVREAD_FILECHANGE:
                continue
            if ret == JVREAD_DOWNLOADING:
                time.sleep(1); continue
            if ret < 0:
                break
            if not data or len(data) < 10:
                continue

            rec_type = data[:2].decode("ascii", errors="replace")
            data_cat = chr(data[2])

            # HR(確定払戻 cat='1') → 単勝馬番を記録
            if rec_type == "HR" and data_cat == "1":
                race_id = _make_race_id(data)
                winner = _extract_tan_winner(data)
                if winner and race_id:
                    race_data[race_id] = {
                        "winner": winner,
                        "se_list": all_se.get(race_id, []),
                    }
                    scanned += 1
                    if scanned % 50 == 0:
                        print(f"  HR確定(cat=1): {scanned} レース")
                    if scanned >= MAX_RACES:
                        break

            # SE → 全件バッファリング（HR より先に来る場合があるため）
            elif rec_type == "SE":
                race_id = _make_race_id(data)
                all_se.setdefault(race_id, []).append(data)
                # すでに HR が来ていれば race_data にも追加
                if race_id in race_data:
                    race_data[race_id]["se_list"].append(data)

    print(f"\nHR(cat=1)確定レース数: {len(race_data)}")
    print(f"SE バッファ済みレース数: {len(all_se)}")

    # all_se で race_data の se_list を補完（HR より後に SE が来た場合に備えて）
    for rid, v in race_data.items():
        if not v["se_list"] and rid in all_se:
            v["se_list"] = all_se[rid]

    # 有効レース (winner + SE 両方ある) を抽出
    valid_races = {
        rid: v for rid, v in race_data.items()
        if v.get("winner") and v.get("se_list")
    }
    print(f"有効レース数 (winner + SE 両方): {len(valid_races)}")

    if not valid_races:
        print("\n有効レースが0件。原因:")
        print("  - cat='1' HR が0件 → OPT_NORMAL で fromtime 以降の確定データが存在しない可能性")
        print("  - fromtime を過去に遡って再試行してください（例: 20241001000000）")
        print(f"  - all_se にあるレース数: {len(all_se)}")
        return

    # オフセット別スコア集計
    # score[offset] = (一致件数, 総件数)
    score: dict[int, list[int]] = defaultdict(lambda: [0, 0])

    for rid, v in valid_races.items():
        winner = v["winner"]
        winner_str_2 = f"{winner:02d}"  # "01"〜"18"

        for se in v["se_list"]:
            if len(se) < SEARCH_END + 2:
                continue

            # 馬番フィールド(slice 28:30)を読んで、この馬が1着候補かを判定
            try:
                uma_ban = int(se[28:30].decode("ascii", errors="replace").strip())
            except Exception:
                continue

            is_winner_horse = (uma_ban == winner)

            for off in range(SEARCH_START, min(SEARCH_END, len(se) - 1)):
                chunk = se[off:off+2]
                try:
                    val_str = chunk.decode("ascii", errors="replace").strip()
                    if not val_str:
                        continue
                    val = int(val_str.lstrip("0") or "0")
                except Exception:
                    continue

                score[off][1] += 1  # 総件数

                # 期待: winner_horse → rank=01(=1), 他の馬 → rank!=01
                if is_winner_horse and val == 1:
                    score[off][0] += 1
                elif not is_winner_horse and val != 1:
                    score[off][0] += 1

    # スコアでソート
    results = []
    for off, (hits, total) in score.items():
        if total > 0:
            rate = hits / total * 100
            results.append((rate, off, hits, total))
    results.sort(reverse=True)

    print(f"\n{'='*60}")
    print(f"オフセット別 着順一致率 TOP 20")
    print(f"{'='*60}")
    print(f"{'Offset':>8}  {'一致率':>8}  {'一致件数':>8}  {'総件数':>8}")
    print(f"{'-'*60}")
    for rate, off, hits, total in results[:20]:
        marker = " ← 推奨" if off == results[0][1] else ""
        print(f"  [{off:3d}:{off+2:3d}]  {rate:7.1f}%  {hits:>8}  {total:>8}{marker}")

    if results:
        best_off = results[0][1]
        best_rate = results[0][0]
        print(f"\n{'='*60}")
        print(f"【推奨】_SE_RANK = slice({best_off}, {best_off+2})  一致率 {best_rate:.1f}%")
        print(f"【現在】_SE_RANK = slice(211, 213)")
        if best_off == 211:
            print("→ 現在のオフセットが正しい。")
        else:
            print(f"→ 現在値 slice(211,213) は不正確。slice({best_off},{best_off+2}) に修正が必要。")
        print(f"{'='*60}")

    # 実データサンプル確認（上位5レース）
    print(f"\n=== 実データ確認（先頭5レース）===")
    shown = 0
    for rid, v in list(valid_races.items())[:5]:
        winner = v["winner"]
        print(f"  race_id={rid}  単勝1着={winner:02d}番")
        for se in v["se_list"][:6]:
            try:
                uma_ban = int(se[28:30].decode("ascii", "replace").strip())
            except Exception:
                continue
            if results:
                best = results[0][1]
                rank_raw = se[best:best+2].decode("ascii", "replace").strip()
                rank_211 = se[211:213].decode("ascii", "replace").strip() if len(se) >= 213 else "??"
                marker = " ← 1着！" if uma_ban == winner else ""
                print(f"    馬番={uma_ban:2d}  offset{best}={rank_raw!r:6s}  offset211={rank_211!r:6s}{marker}")
        shown += 1


if __name__ == "__main__":
    main()
