"""scripts/nar_live_demo.py — 地方競馬（NAR）ライブ取得 → Note Markdown 生成デモ。

nar.netkeiba.com から本日の地方競馬の実データ（出馬表・オッズ）を取得し、
EV 比例の予算配分付き Note 用 Markdown を生成して標準出力＋ファイルへ出力する。

⚠️ 本スクリプトは feature/nar-support の基盤デモ。EV 値はデモ用の暫定値であり、
   実際の予想モデル出力ではない（NAR モデルは次フェーズ）。

Usage:
    py scripts/nar_live_demo.py                 # 本日の NAR 開催から自動選択
    py scripts/nar_live_demo.py --race-id 202630060301
"""

from __future__ import annotations

import argparse
import datetime
import logging
import re
import sys
from pathlib import Path

import requests

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.nar.data_fetcher import (  # noqa: E402
    NetkeibaNarFetcher,
    is_nar_race_id,
)
from src.nar.note_adapter import (  # noqa: E402
    NarBet,
    generate_nar_note_markdown,
)

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_OUT_DIR = _ROOT / "outputs" / "nar"


def _find_today_nar_race_id() -> str | None:
    """本日の地方競馬開催から最初の NAR race_id を 1 件返す（取得不可なら None）。"""
    today = datetime.date.today().strftime("%Y%m%d")
    url = f"https://nar.netkeiba.com/top/race_list_sub.html?kaisai_date={today}"
    try:
        resp = requests.get(url, timeout=10, headers=_UA)
        resp.encoding = resp.apparent_encoding
        ids = sorted(set(re.findall(r"race_id=(\d{12})", resp.text)))
        nar_ids = [i for i in ids if is_nar_race_id(i)]
        return nar_ids[0] if nar_ids else None
    except Exception as exc:  # noqa: BLE001
        logging.warning("本日の NAR race_id 取得失敗: %s", exc)
        return None


def _demo_bets(fetcher: NetkeibaNarFetcher, race_id: str) -> tuple[list[NarBet], str]:
    """ライブ取得した出走馬から、デモ用 EV を付与した NarBet リストを生成する。"""
    meta = fetcher.fetch_race_meta(race_id)
    entries = fetcher.fetch_entries(race_id)
    if not entries:
        return [], meta.venue

    # 単勝オッズ昇順（人気順）に並べ、上位馬へデモ EV を付与する。
    ranked = sorted(entries, key=lambda e: e.win_odds or 9_999.0)
    venue = meta.venue

    def _desc(e) -> str:  # type: ignore[no-untyped-def]
        return f"{e.horse_number}番 {e.horse_name}"

    bets: list[NarBet] = []
    bets.append(NarBet("単勝", _desc(ranked[0]), ev=1.30, venue=venue))
    bets.append(NarBet("複勝", _desc(ranked[0]), ev=1.45, venue=venue))
    if len(ranked) >= 2:
        bets.append(NarBet("複勝", _desc(ranked[1]), ev=1.15, venue=venue))
    if len(ranked) >= 3:
        bets.append(
            NarBet(
                "ワイド",
                f"{ranked[0].horse_number}-{ranked[2].horse_number}",
                ev=0.95,
                venue=venue,
            )
        )
    return bets, venue


def main() -> int:
    ap = argparse.ArgumentParser(description="NAR ライブ取得 → Note Markdown デモ")
    ap.add_argument(
        "--race-id", default=None, help="対象 race_id（省略時=本日の NAR から自動）"
    )
    args = ap.parse_args()

    race_id = args.race_id or _find_today_nar_race_id()
    if not race_id:
        print("⚠️ 本日の NAR 開催が見つかりませんでした。--race-id で指定してください。")
        return 1
    if not is_nar_race_id(race_id):
        print(f"⚠️ {race_id} は NAR の race_id ではありません。")
        return 1

    print(f"対象 race_id: {race_id}")
    fetcher = NetkeibaNarFetcher()
    bets, venue = _demo_bets(fetcher, race_id)
    if not bets:
        print("⚠️ 出走馬を取得できませんでした（開催前/構造変更の可能性）。")
        return 1

    today = datetime.date.today().strftime("%Y%m%d")
    md = generate_nar_note_markdown(bets, date=today, venue=venue, total_budget=10_000)

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"nar_note_live_{race_id}.md"
    out_path.write_text(md, encoding="utf-8")

    print(f"✅ 生成完了 → {out_path}（{len(md)} 文字 / 買い目 {len(bets)} 点）")
    print("=" * 60)
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
