"""
WOOD dataspec の WH/HC レコード診断スクリプト

JVLink から小規模な WOOD データを取得し、実際に WH レコードが
存在するか・バイトオフセットが正しいかを確認する。

使用方法:
    py scripts/diagnose_wood_wh.py
"""

from __future__ import annotations

import sqlite3
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "data/umalogi.db"


def check_existing_data() -> None:
    """DB の現状を確認する。"""
    conn = sqlite3.connect(DB_PATH)

    tc_count = conn.execute("SELECT COUNT(*) FROM training_times").fetchone()[0]
    hc_count = conn.execute("SELECT COUNT(*) FROM training_hillwork").fetchone()[0]

    print(f"training_times (WC→TC): {tc_count:,} 件")
    print(f"training_hillwork (WH→HC): {hc_count:,} 件")

    if tc_count > 0:
        sample = conn.execute(
            "SELECT horse_id, training_date, time_4f FROM training_times ORDER BY training_date DESC LIMIT 3"
        ).fetchall()
        print("training_times サンプル:")
        for row in sample:
            print(f"  horse_id={row[0]}, date={row[1]}, 4f={row[2]}")

    conn.close()


def check_jvlink_raw_record_types() -> None:
    """
    JVLink の WOOD dataspec から直接バイト列を取得し、
    実際のレコードタイプ分布を確認する。

    JVLink の COM インターフェースを直接叩くため、32bit Python が必要。
    """
    print()
    print("=== JVLink 直接診断 (32bit が必要) ===")

    try:
        import win32com.client  # type: ignore

        jv = win32com.client.Dispatch("JVDTLab.JVLink.1")

        # 短期間のみ取得（直近1週間）
        from datetime import datetime, timedelta

        fromtime = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d%H%M%S")

        ret = jv.JVOpen("WOOD", fromtime, 1, 0, None, None)
        print(f"JVOpen WOOD (1週間): ret={ret}")

        if ret < 0:
            print(f"JVOpen 失敗: {ret}")
            return

        rec_counts: dict[str, int] = {}
        total = 0

        while True:
            buff = ""
            size = 0
            filename = ""
            rc = jv.JVRead(buff, size, filename)
            if rc == 0:
                break
            if rc < 0:
                print(f"JVRead エラー: {rc}")
                break

            raw = buff.encode("latin-1") if isinstance(buff, str) else buff
            if len(raw) >= 2:
                rec_type = raw[:2].decode("ascii", errors="replace")
                rec_counts[rec_type] = rec_counts.get(rec_type, 0) + 1
                total += 1

            if total > 10000:
                print("10,000件サンプリング完了")
                break

        jv.JVClose()

        print(f"総レコード数: {total:,}")
        print("レコードタイプ分布:")
        for rt, cnt in sorted(rec_counts.items(), key=lambda x: -x[1]):
            print(f"  {rt}: {cnt:,} 件")

        if "WH" not in rec_counts and "HC" not in rec_counts:
            print()
            print("⚠️  WH/HC レコードが存在しません")
            print(
                "   → WOOD dataspec では坂路調教(hillwork)データが配信されていない可能性"
            )
            print("   → 対策: netkeiba の調教ページからスクレイピングが必要")

    except ImportError:
        print("win32com 未インストール (このスクリプトは 32bit JVLink 環境専用)")
    except Exception as exc:
        print(f"JVLink 接続エラー: {exc}")
        print(
            "64bit Python では JVLink COM に接続できません。32bit で実行してください。"
        )


def suggest_netkeiba_scraper() -> None:
    """netkeiba の調教欄スクレイプ代替案を提示する。"""
    print()
    print("=== netkeiba 坂路調教データの代替取得方法 ===")
    print()
    print("JVLink WOOD dataspec に WH レコードが含まれない場合、")
    print("netkeiba.com の調教ページから取得可能:")
    print()
    print("  URL: https://race.netkeiba.com/race/training.html?race_id=<race_id>")
    print()
    print("取得できるデータ:")
    print("  - 坂路タイム（4F・3F・2F・1F）")
    print("  - 調教コース種別（坂路 / 美南W / 美東W / 栗W 等）")
    print("  - 騎乗者")
    print("  - 馬体重（出馬表発表前から確認可能）")
    print()
    print("実装案: src/scraper/training_scraper.py を新設して")
    print("  fetch_training_table(race_id) → 坂路/コース調教データを返す")
    print("  → training_hillwork テーブルへ INSERT")


if __name__ == "__main__":
    check_existing_data()
    check_jvlink_raw_record_types()
    suggest_netkeiba_scraper()
