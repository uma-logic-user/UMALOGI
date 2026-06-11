"""
JRA-VAN ローカルキャッシュ (.rtd) リーダー

TARGET frontier JV が `C:\\ProgramData\\JRA-VAN\\Data Lab\\cache\\` に保存する
リアルタイムオッズキャッシュファイル (.rtd) を直接解析して
馬番・単勝オッズ・人気を抽出する。

JVLink COM サーバー不要・64bit Python 対応。
レース開始後はファイルが削除されるため、prerace_pipeline での利用を前提とする。

ファイル名フォーマット:
    0B30{YYYYMMDD}{JYO(2)}{KAI(2)}{NICHI(2)}{RACE(2)}.rtd
    例: 0B302026041903010401.rtd
        → 2026-04-19 福島(03) 1回(01) 4日目(04) 1R(01)
        → race_id = 202603010401

O1 レコード構造 (zlib 解凍後テキスト):
    [0:2]   "O1"              レコード種別
    [2:3]   データ区分         "1" = 速報
    [3:11]  データ作成年月日   YYYYMMDD
    [11:19] 開催年月日         YYYYMMDD
    [19:21] 場コード           "03" = 福島
    [21:23] 開催回             "01"
    [23:25] 開催日次           "04"
    [25:27] レース番号         "01"
    [27:29] 登録頭数           (2桁)
    [29:31] 出走頭数           (2桁)
    [31:43] プール情報         単勝集計金(6) + 複勝集計金(6)
    [43:] 馬別データ          8文字 × 出走頭数
          [0:2] 馬番
          [2:6] 単勝オッズ × 10 (例: "0046" = 4.6)
          [6:8] 人気順位
"""

from __future__ import annotations

import logging
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.entry_table import HorseOdds

logger = logging.getLogger(__name__)

_RTD_DIR = Path(r"C:\ProgramData\JRA-VAN\Data Lab\cache")

# O1 レコードの固定オフセット
_HORSE_SECTION_START = 43  # 馬別データ開始位置
_BYTES_PER_HORSE = 8  # 馬番(2) + 単勝オッズ×10(4) + 人気(2)


@dataclass
class RtdOdds:
    horse_number: int
    win_odds: float | None  # 単勝オッズ (None = 取得不可)
    popularity: int | None  # 人気順位


@dataclass
class RtdRaceInfo:
    race_id: str
    head_count: int  # 出走頭数
    odds: list[RtdOdds]  # 馬別オッズ（空リストの場合あり）


def _race_id_from_filename(stem: str) -> str:
    """ファイルのステム名から race_id を導出する。

    旧フォーマット (20文字): "0B30{YYYYMMDD}{JYO}{KAI}{NICHI}{RACE}"
      → "{YYYY}{JYO}{KAI}{NICHI}{RACE}"
    新フォーマット (16文字): "0B12{YYYYMMDD}{JYO}{RACE}"
      → KAI/NICHI 不明のため "{YYYY}{JYO}0000{RACE}" (DB 突合で補完)

    Args:
        stem: RTD ファイルの拡張子なしファイル名。

    Returns:
        12桁の race_id 文字列。
    """
    if len(stem) == 16:  # 新フォーマット: 0BXX + YYYYMMDD(8) + JYO(2) + RACE(2)
        year = stem[4:8]
        jyo = stem[12:14]
        race = stem[14:16]
        rid = f"{year}{jyo}0000{race}"
        return rid if rid.isdigit() else ""
    if len(stem) != 20 or not stem[4:].isdigit():
        # 想定外のファイル名からゴミ race_id を生成して DB を汚染しない
        logger.warning("RTD 想定外ファイル名: %r", stem)
        return ""
    # 旧フォーマット (20文字)
    date8 = stem[4:12]
    venue = stem[12:14]
    kai = stem[14:16]
    nichi = stem[16:18]
    race = stem[18:20]
    return date8[:4] + venue + kai + nichi + race


def _parse_o1(text: str, race_id: str) -> RtdRaceInfo:
    """
    O1 レコードテキストを解析して RtdRaceInfo を返す。

    オッズ形式: 4 桁整数 / 10.0 = 実オッズ (例: "0046" → 4.6)
    解析失敗した馬は win_odds=None として扱いスキップしない。
    """
    if len(text) < 31:
        return RtdRaceInfo(race_id=race_id, head_count=0, odds=[])

    # 出走頭数
    head_count_str = text[29:31]
    try:
        head_count = int(head_count_str)
    except ValueError:
        logger.warning(
            "RTD 出走頭数パース失敗 (race_id=%s): %r", race_id, head_count_str
        )
        return RtdRaceInfo(race_id=race_id, head_count=0, odds=[])

    if head_count < 1 or head_count > 28:
        return RtdRaceInfo(race_id=race_id, head_count=0, odds=[])

    horse_section = text[_HORSE_SECTION_START:]
    odds_list: list[RtdOdds] = []

    for i in range(head_count):
        offset = i * _BYTES_PER_HORSE
        chunk = horse_section[offset : offset + _BYTES_PER_HORSE]
        if len(chunk) < _BYTES_PER_HORSE:
            break

        try:
            horse_no = int(chunk[0:2])
            odds_raw = int(chunk[2:6])  # オッズ × 10
            popularity = int(chunk[6:8])
        except ValueError:
            logger.debug("RTD 馬データパース失敗 (race_id=%s) chunk=%r", race_id, chunk)
            continue

        win_odds = round(odds_raw / 10.0, 1) if odds_raw > 0 else None

        odds_list.append(
            RtdOdds(
                horse_number=horse_no,
                win_odds=win_odds,
                popularity=popularity if popularity > 0 else None,
            )
        )

    return RtdRaceInfo(race_id=race_id, head_count=head_count, odds=odds_list)


def read_rtd_for_race(race_id: str) -> RtdRaceInfo | None:
    """
    指定 race_id に対応する .rtd ファイルを読み込んで RtdRaceInfo を返す。

    2つのファイルフォーマットに対応:
      新: 0B12{YYYYMMDD}{JYO(2)}{RACE(2)}.rtd  (16文字 stem) ← JRA-VAN 現行
      旧: 0B30{YYYYMMDD}{JYO(2)}{KAI(2)}{NICHI(2)}{RACE(2)}.rtd  (20文字 stem)

    ファイルが存在しない（レース開始後は削除される）場合は None を返す。
    """
    jyo = race_id[4:6]
    kai = race_id[6:8]
    nichi = race_id[8:10]
    race = race_id[10:12]

    candidates: list[Path] = []

    # 新フォーマット: 0B12*{jyo}{race}.rtd (KAI/NICHIなし — 任意のプレフィックスにも対応)
    for p in _RTD_DIR.glob(f"0B??*{jyo}{race}.rtd"):
        stem = p.stem
        if len(stem) == 16:  # 新フォーマット長のみ受け入れ
            candidates.append(p)

    # 旧フォーマット: 0B30*{jyo}{kai}{nichi}{race}.rtd (20文字)
    for p in _RTD_DIR.glob(f"0B30*{jyo}{kai}{nichi}{race}.rtd"):
        if len(p.stem) == 20 and p not in candidates:
            candidates.append(p)

    if not candidates:
        logger.debug("RTD ファイル未存在 (race_id=%s)", race_id)
        return None

    # 最も新しいファイルを使用。レース発走後はファイルが削除されるため、
    # glob と stat の間に消えるレースコンディション（TOCTOU）を許容する。
    def _safe_mtime(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    rtd_path = max(candidates, key=_safe_mtime)

    try:
        raw = rtd_path.read_bytes()
        dec = zlib.decompress(raw)
        text = dec.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning(
            "RTD 解凍失敗 (race_id=%s, file=%s): %s", race_id, rtd_path.name, exc
        )
        return None

    if not text.startswith("O1"):
        logger.warning("RTD 非O1レコード (race_id=%s): %r", race_id, text[:10])
        return None

    info = _parse_o1(text, race_id)
    logger.info(
        "RTD 読み込み完了: race_id=%s file=%s 出走頭数=%d オッズ取得=%d頭",
        race_id,
        rtd_path.name,
        info.head_count,
        len(info.odds),
    )
    return info


def read_all_rtd_for_date(target_date: str) -> dict[str, RtdRaceInfo]:
    """
    指定日の全 .rtd ファイルを読み込んで {race_id: RtdRaceInfo} を返す。

    新フォーマット (0B12...) と旧フォーマット (0B30...) の両方を走査する。

    Args:
        target_date: "YYYYMMDD" 形式

    Returns:
        race_id → RtdRaceInfo のマッピング（ファイルが存在するレースのみ）
    """
    # 両フォーマットを検索 (0B?? = 任意のプレフィックス)
    files = sorted(
        set(_RTD_DIR.glob(f"0B12{target_date}*.rtd"))
        | set(_RTD_DIR.glob(f"0B30{target_date}*.rtd"))
    )

    if not files:
        logger.info("RTD ファイルなし (date=%s)", target_date)
        return {}

    result: dict[str, RtdRaceInfo] = {}
    for f in files:
        race_id = _race_id_from_filename(f.stem)
        if not race_id:
            continue
        try:
            raw = f.read_bytes()
            dec = zlib.decompress(raw)
            text = dec.decode("cp932", errors="replace")
        except Exception as exc:
            logger.warning("RTD 解凍失敗 %s: %s", f.name, exc)
            continue

        if not text.startswith("O1"):
            continue

        info = _parse_o1(text, race_id)
        if info.head_count > 0:
            result[race_id] = info

    logger.info(
        "RTD 一括読み込み: %d ファイル → %d レース (date=%s)",
        len(files),
        len(result),
        target_date,
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# JVRTOpen 速報 O1（リアルタイムオッズ）対応
#   .rtd ファイル版とは O1 ヘッダ長が異なる（速報版は発表月日時分 8桁を持つ）。
#   2026-05-31 のライブ実データ（東京2回12日1R・16頭）で以下を実証済み:
#     [37:39] 出走頭数 / 単勝配列 start=43 / entry=8 (馬番2 + オッズ×10 4 + 人気2)
# ─────────────────────────────────────────────────────────────────────────────
_RT_O1_HEAD_COUNT_SLICE = slice(37, 39)  # 出走頭数（速報版）
_RT_O1_HORSE_START = 43  # 単勝配列の開始位置
_RT_O1_BYTES_PER_HORSE = 8  # 馬番(2) + 単勝オッズ×10(4) + 人気(2)


def build_rt_race_key(race_id: str, date8: str) -> str:
    """race_id(12桁) と開催日(YYYYMMDD) から 16桁の速報レースキーを構築する。

    JVRTOpen の key 形式: ``{YYYYMMDD}{JYO}{KAI}{NICHI}{RR}``

    Args:
        race_id: 12桁の race_id（YYYY+JYO+KAI+NICHI+RR）。
        date8:   開催日 "YYYYMMDD"。

    Returns:
        16桁の速報レースキー。引数が不正な場合は空文字。
    """
    if len(race_id) != 12 or len(date8) != 8 or not date8.isdigit():
        return ""
    jyo, kai, nichi, rr = race_id[4:6], race_id[6:8], race_id[8:10], race_id[10:12]
    return f"{date8}{jyo}{kai}{nichi}{rr}"


def parse_o1_realtime(text: str, race_id: str) -> RtdRaceInfo | None:
    """JVRTOpen("0B30") 由来の速報 O1 レコードを解析して RtdRaceInfo を返す。

    .rtd ファイル版の :func:`_parse_o1` とはヘッダ長が異なるため別実装。
    オッズ形式: 4桁整数 / 10.0 = 実オッズ（例: "0019" → 1.9）。

    Args:
        text: cp932 デコード済みの O1 レコード文字列。
        race_id: 対象 race_id。

    Returns:
        RtdRaceInfo。O1 レコードでない/壊れている場合は None。
    """
    if not text or not text.startswith("O1") or len(text) < _RT_O1_HORSE_START:
        return None

    try:
        head_count = int(text[_RT_O1_HEAD_COUNT_SLICE])
    except ValueError:
        logger.warning(
            "速報O1 出走頭数パース失敗 (race_id=%s): %r", race_id, text[37:39]
        )
        return None
    if head_count < 1 or head_count > 28:
        return None

    horse_section = text[_RT_O1_HORSE_START:]
    odds_list: list[RtdOdds] = []
    for i in range(head_count):
        chunk = horse_section[
            i * _RT_O1_BYTES_PER_HORSE : (i + 1) * _RT_O1_BYTES_PER_HORSE
        ]
        if len(chunk) < _RT_O1_BYTES_PER_HORSE:
            break
        try:
            horse_no = int(chunk[0:2])
            odds_raw = int(chunk[2:6])
            popularity = int(chunk[6:8])
        except ValueError:
            logger.debug(
                "速報O1 馬データパース失敗 (race_id=%s) chunk=%r", race_id, chunk
            )
            continue
        odds_list.append(
            RtdOdds(
                horse_number=horse_no,
                win_odds=round(odds_raw / 10.0, 1) if odds_raw > 0 else None,
                popularity=popularity if popularity > 0 else None,
            )
        )

    return RtdRaceInfo(race_id=race_id, head_count=head_count, odds=odds_list)


# ─────────────────────────────────────────────────────────────────────────────
# JVRTOpen 速報 WH（馬体重）対応
#   2026-05-31 のライブ実データ（東京2回12日1R）でレイアウト実証済み（バイト基準）:
#     ヘッダ: [0:2]"WH" [2:3]区分 [3:11]作成日 [11:27]レースキー [27:35]発表月日時分
#     馬体重情報配列 start=35 / stride=45:
#       馬番[0:2] 馬名[2:38](36バイト) 馬体重[38:41] 増減符号[41:42] 増減差[42:45]
#   例: 馬番01 体重482 符号"-" 差002 → 482kg(-2) / 馬番02 体重492 "+" 012 → 492kg(+12)
# ─────────────────────────────────────────────────────────────────────────────
_RT_WH_HORSE_START = 35
_RT_WH_STRIDE = 45


def parse_wh_realtime(data: bytes, race_id: str) -> dict[int, dict[str, int | None]]:
    """JVRTOpen("0B11") 由来の速報 WH（馬体重）レコードを解析する。

    馬名に半角文字が混在しても安全なよう、デコード済み文字列ではなく
    生バイト列を固定幅で走査する。

    Args:
        data: WH レコードの生バイト列（JVRead 取得・NULL 除去済み）。
        race_id: 対象 race_id（ログ用）。

    Returns:
        {馬番: {"weight": kg|None, "weight_diff": 増減kg|None}}。WH でなければ空 dict。
    """
    if not data or not data.startswith(b"WH") or len(data) < _RT_WH_HORSE_START:
        return {}

    result: dict[int, dict[str, int | None]] = {}
    section = data[_RT_WH_HORSE_START:]
    n = len(section) // _RT_WH_STRIDE
    for i in range(n):
        chunk = section[i * _RT_WH_STRIDE : (i + 1) * _RT_WH_STRIDE]
        if len(chunk) < _RT_WH_STRIDE:
            break
        try:
            horse_no = int(chunk[0:2])
        except ValueError:
            continue
        if horse_no < 1 or horse_no > 28:
            continue

        weight_raw = chunk[38:41].decode("ascii", "replace").strip()
        sign = chunk[41:42].decode("ascii", "replace")
        diff_raw = chunk[42:45].decode("ascii", "replace").strip()

        weight: int | None
        try:
            w = int(weight_raw)
            weight = w if w > 0 else None
        except ValueError:
            weight = None

        weight_diff: int | None
        if sign in ("+", "-") and diff_raw.isdigit():
            weight_diff = int(diff_raw) * (-1 if sign == "-" else 1)
        else:
            weight_diff = None

        result[horse_no] = {"weight": weight, "weight_diff": weight_diff}

    return result


def rtd_odds_to_horse_odds(rtd_info: RtdRaceInfo) -> list["HorseOdds"]:
    """RtdRaceInfo を entry_table.HorseOdds のリストに変換する。

    prerace_pipeline で insert_realtime_odds に渡すために使用。

    Args:
        rtd_info: RTD ファイルから読み込んだレース情報。

    Returns:
        HorseOdds のリスト（horse_number > 0 の馬のみ）。
    """
    from src.scraper.entry_table import HorseOdds

    return [
        HorseOdds(
            horse_number=o.horse_number,
            win_odds=o.win_odds,
            place_odds_min=None,
            place_odds_max=None,
            popularity=o.popularity,
        )
        for o in rtd_info.odds
        if o.horse_number > 0
    ]
