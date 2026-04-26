"""
JVLink 生データ調査スクリプト
==============================
目的: 4/18・4/19 のデータがどのレコード種別でキャッシュに存在するかを特定する。

実行方法 (32bit Python 必須):
    py -3-32 scripts/inspect_raw_records.py
    py -3-32 scripts/inspect_raw_records.py --option 1   # OPT_NORMAL
    py -3-32 scripts/inspect_raw_records.py --option 4   # OPT_STORED
    py -3-32 scripts/inspect_raw_records.py --fromtime 20260404  # from_dt 指定
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=True)
except ImportError:
    pass

# ── 定数 ────────────────────────────────────────────────────────────
TARGET_DATES = {"20260418", "20260419"}
FROMTIME_DEFAULT = "20260404000000"   # 14 日前 (4/19 - 14d)
MAX_RECORDS = 100_000                 # 無限ループ防止

# JVLink が保持する全レコード種別の既知一覧（JV-Data 仕様書より）
KNOWN_TYPES = {
    "RA": "レース詳細（確定出馬表）",
    "SE": "馬毎レース情報",
    "HR": "払戻（全馬券種）",
    "WH": "単勝/複勝払戻",
    "WF": "枠連払戻",
    "WE": "ワイド払戻",
    "WQ": "馬連払戻",
    "WM": "馬単払戻",
    "WT": "三連複払戻",
    "WS": "三連単払戻",
    "JG": "出馬投票（暫定出馬表）",
    "TK": "特別登録",
    "CH": "調教師マスタ",
    "KS": "騎手マスタ",
    "UM": "競走馬マスタ",
    "BT": "繁殖馬マスタ",
    "HN": "産駒マスタ",
    "TC": "調教タイム（旧）",
    "HC": "坂路調教（旧）",
    "WC": "調教タイム（実）",
    "WH2":"坂路調教（実）",
    "CS": "特別競走登録",
    "YS": "レース変更",
    "O1": "単勝・複勝オッズ",
    "O2": "枠連オッズ",
    "O3": "馬連オッズ",
    "O4": "ワイドオッズ",
    "O5": "馬単オッズ",
    "O6": "三連複オッズ",
    "O7": "三連単オッズ",
}


def _to_bytes(com_str: str) -> bytes:
    try:
        return com_str.encode("latin-1")
    except (UnicodeEncodeError, AttributeError):
        return bytes(ord(c) & 0xFF for c in com_str)


def _extract_dates_from_record(raw: bytes, rec_type: str) -> list[str]:
    """
    レコードバイト列から YYYYMMDD 形式の日付候補を全て抽出する。

    JV-Data の主な日付フィールド位置:
      共通ヘッダー    [2:10]  = データ作成年月日 YYYYMMDD
      レースキー      [21:29] = 開催年月日 YYYYMMDD   (RA/SE/HR/JG 共通)
      JG 特有         [11:19] = 開催年月日 (YYYYMMDD) ← 仮説の核心
    """
    dates: list[str] = []

    def try_extract(start: int, end: int) -> None:
        if len(raw) >= end:
            chunk = raw[start:end]
            try:
                s = chunk.decode("ascii", errors="replace").strip()
                if len(s) == 8 and s.isdigit() and s.startswith("202"):
                    dates.append(s)
            except Exception:
                pass

    # 共通ヘッダー: データ作成日
    try_extract(2, 10)

    if rec_type in ("RA", "SE", "HR", "WH", "WF", "WE", "WQ", "WM", "WT", "WS"):
        # レースキー内の開催日 [21:29]
        try_extract(21, 29)

    elif rec_type == "JG":
        # JG: 出馬投票レコード。仕様書上の日付位置を複数箇所試す
        for s, e in [(11, 19), (21, 29), (29, 37), (37, 45)]:
            try_extract(s, e)

    elif rec_type in ("TK", "CS", "YS"):
        for s, e in [(10, 18), (18, 26), (21, 29)]:
            try_extract(s, e)

    else:
        # 未知レコード: 8バイト刻みで全体を走査
        for offset in range(2, min(len(raw), 80), 4):
            try_extract(offset, offset + 8)

    return list(set(dates))  # 重複除去


def _dump_hex(raw: bytes, label: str = "", max_bytes: int = 96) -> None:
    print(f"  ─── HEX DUMP {label} ({len(raw)} bytes) ───")
    data = raw[:max_bytes]
    for i in range(0, len(data), 16):
        chunk = data[i:i + 16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        asc_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
        print(f"    {i:4d}  {hex_part:<48s}  {asc_part}")


def main() -> None:
    parser = argparse.ArgumentParser(description="JVLink 生データ調査 (32bit Python 専用)")
    parser.add_argument("--option",   type=int, default=4,
                        help="JVOpen オプション: 1=NORMAL, 2=SETUP, 3=TODAY, 4=STORED (デフォルト:4)")
    parser.add_argument("--dataspec", default="RACE",
                        help="データ種別 (デフォルト: RACE)")
    parser.add_argument("--fromtime", default=FROMTIME_DEFAULT,
                        help=f"from_time YYYYMMDD[hhmmss] (デフォルト: {FROMTIME_DEFAULT})")
    parser.add_argument("--dump-target", action="store_true",
                        help="TARGET_DATES に一致したレコードの HEX ダンプを出力する")
    args = parser.parse_args()

    sid = os.getenv("JRAVAN_SID", "")
    if not sid:
        print("[ERROR] JRAVAN_SID が .env に設定されていません。", file=sys.stderr)
        sys.exit(1)

    from_time = args.fromtime
    if len(from_time) == 8:
        from_time += "000000"

    print("=" * 70)
    print("JVLink 生データ調査スクリプト")
    print(f"  dataspec : {args.dataspec}")
    print(f"  fromtime : {from_time}")
    print(f"  option   : {args.option}")
    print(f"  調査日付 : {sorted(TARGET_DATES)}")
    print("=" * 70)

    try:
        import win32com.client  # type: ignore[import]
    except ImportError:
        print("[ERROR] pywin32 が見つかりません。32bit Python で実行してください。", file=sys.stderr)
        sys.exit(1)

    jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
    ret = jvl.JVInit(sid)
    if ret != 0:
        print(f"[ERROR] JVInit 失敗 (code={ret})", file=sys.stderr)
        sys.exit(1)

    BUFF_SIZE = 1_000_000
    buff  = " " * BUFF_SIZE
    fname = " " * 256

    # JVOpen
    try:
        result = jvl.JVOpen(args.dataspec, from_time, args.option, 0, "")
        open_code = result[0] if isinstance(result, (tuple, list)) else int(result)
    except Exception:
        result = jvl.JVOpen(args.dataspec, from_time, args.option)
        open_code = result[0] if isinstance(result, (tuple, list)) else int(result)

    print(f"\nJVOpen → code={open_code}")
    if open_code < 0:
        print(f"[WARN] JVOpen が負値 ({open_code}) を返しました。データなし / -303 の可能性。")
        jvl.JVClose()
        sys.exit(0)

    # ── 集計データ構造 ─────────────────────────────────────────────
    # total_by_type[rec_type] = 総件数
    total_by_type: dict[str, int] = defaultdict(int)
    # target_by_type[rec_type][date] = 件数
    target_by_type: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # サンプルバイト列（最初の1件）
    samples: dict[str, bytes] = {}

    read_count = 0
    import time

    print("\n読み込み中 (Ctrl+C で中断)...")
    try:
        while read_count < MAX_RECORDS:
            result = jvl.JVRead(buff, BUFF_SIZE, fname)
            if isinstance(result, (tuple, list)):
                code    = int(result[0])
                raw_str = result[1] if len(result) > 1 else buff
                size    = int(result[2]) if len(result) > 2 else 0
            else:
                code    = int(result)
                raw_str = buff
                size    = 0

            if code == 0:    # EOF
                break
            if code == -1:   # ファイル切り替わり
                continue
            if code == -3:   # ダウンロード中
                time.sleep(0.5)
                continue
            if code < 0:
                print(f"[ERROR] JVRead code={code}")
                break

            raw = _to_bytes(raw_str)
            if size > 0:
                raw = raw[:size]
            raw = raw.rstrip(b"\x00")

            if len(raw) < 2:
                continue

            rec_type = raw[:2].decode("ascii", errors="replace")
            total_by_type[rec_type] += 1
            read_count += 1

            # サンプル保存（最初の1件）
            if rec_type not in samples:
                samples[rec_type] = raw

            # 日付抽出 → TARGET_DATES と照合
            dates = _extract_dates_from_record(raw, rec_type)
            for d in dates:
                if d in TARGET_DATES:
                    target_by_type[rec_type][d] += 1

            if read_count % 1000 == 0:
                print(f"  {read_count} レコード処理済み...")

    except KeyboardInterrupt:
        print("\n[中断]")
    finally:
        jvl.JVClose()

    # ── 結果出力 ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"読み込み総数: {read_count} レコード")
    print("=" * 70)

    print("\n【全レコード種別 集計】")
    print(f"  {'種別':<6} {'件数':>8}  説明")
    print(f"  {'──':<6} {'──':>8}  ──")
    for rt, cnt in sorted(total_by_type.items(), key=lambda x: -x[1]):
        desc = KNOWN_TYPES.get(rt, "不明")
        print(f"  {rt:<6} {cnt:>8}  {desc}")

    print("\n【TARGET_DATES に一致したレコード】")
    any_found = False
    for rt, date_counts in sorted(target_by_type.items()):
        for dt, cnt in sorted(date_counts.items()):
            desc = KNOWN_TYPES.get(rt, "不明")
            print(f"  ★ 種別={rt}  日付={dt}  件数={cnt:>4}  ({desc})")
            any_found = True
    if not any_found:
        print("  (該当なし — 4/18・4/19 の関連データは存在しません)")

    # ── 未知種別のサンプル HEX ダンプ ─────────────────────────────
    unknown_types = [rt for rt in samples if rt not in KNOWN_TYPES]
    if unknown_types:
        print("\n【未知レコード種別 HEX ダンプ (先頭96バイト)】")
        for rt in sorted(unknown_types):
            _dump_hex(samples[rt], f"種別={rt}", 96)

    # ── TARGET_DATES レコードの HEX ダンプ ──────────────────────
    if args.dump_target and any_found:
        print("\n【TARGET_DATES 一致レコード HEX ダンプ (再スキャン)】")
        print("※ 再スキャンには時間がかかります。Ctrl+C で中断可。")
        # 再スキャン
        try:
            result2 = jvl.JVOpen(args.dataspec, from_time, args.option, 0, "")
            open2   = result2[0] if isinstance(result2, (tuple, list)) else int(result2)
        except Exception:
            result2 = jvl.JVOpen(args.dataspec, from_time, args.option)
            open2   = result2[0] if isinstance(result2, (tuple, list)) else int(result2)

        if open2 >= 0:
            dumped: set[str] = set()
            try:
                while True:
                    r2 = jvl.JVRead(buff, BUFF_SIZE, fname)
                    c2 = int(r2[0]) if isinstance(r2, (tuple, list)) else int(r2)
                    if c2 <= 0:
                        break
                    raw2 = _to_bytes(r2[1] if isinstance(r2, (tuple, list)) else buff)
                    sz2  = int(r2[2]) if isinstance(r2, (tuple, list)) and len(r2) > 2 else 0
                    if sz2 > 0:
                        raw2 = raw2[:sz2]
                    raw2 = raw2.rstrip(b"\x00")
                    if len(raw2) < 2:
                        continue
                    rt2 = raw2[:2].decode("ascii", errors="replace")
                    dates2 = _extract_dates_from_record(raw2, rt2)
                    for d2 in dates2:
                        if d2 in TARGET_DATES:
                            key = f"{rt2}_{d2}"
                            if key not in dumped:
                                _dump_hex(raw2, f"種別={rt2} 日付={d2}", 96)
                                dumped.add(key)
            except KeyboardInterrupt:
                pass
            finally:
                jvl.JVClose()

    print("\n" + "=" * 70)
    print("調査完了")

    # ── 仮説検証サマリー ────────────────────────────────────────
    print("\n【仮説検証サマリー】")
    ra_target = sum(target_by_type.get("RA", {}).values())
    jg_target = sum(target_by_type.get("JG", {}).values())
    se_target = sum(target_by_type.get("SE", {}).values())
    other_target = {
        rt: sum(v.values())
        for rt, v in target_by_type.items()
        if rt not in ("RA", "JG", "SE")
    }

    if jg_target > 0 and ra_target == 0:
        print(f"  → 仮説【正しい】: RA=0件, JG={jg_target}件")
        print("    4/18・4/19 の出馬表は JG（出馬投票）として存在します。")
        print("    TARGET は JG を解釈して出馬表を表示。")
        print("    UMALOGI は RA のみを対象とするため空振りします。")
        print()
        print("  ■ 対策: jravan_client.py に JG レコードパーサーを追加し、")
        print("           JG → races/entries へ変換保存する必要があります。")
    elif ra_target > 0:
        print(f"  → 仮説【不正確】: RA={ra_target}件 存在します。")
        print("    RA レコードが取得できているのに保存されていない別の原因があります。")
        print("    日付フォーマット・migration ロジックを再確認してください。")
    elif jg_target == 0 and ra_target == 0 and not other_target:
        print("  → 4/18・4/19 に関するデータがキャッシュに一切存在しません。")
        print("    JVLink のローカルキャッシュが古い状態です。")
        print("    TARGET frontier JV を起動してデータを同期してください。")
    else:
        print(f"  → 他種別でデータ確認: {dict(other_target)}")
        print("    レコード種別ごとに個別対応が必要です。")


if __name__ == "__main__":
    main()
