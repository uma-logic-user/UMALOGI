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

logger = logging.getLogger(__name__)

_RTD_DIR = Path(r"C:\ProgramData\JRA-VAN\Data Lab\cache")

# O1 レコードの固定オフセット
_HORSE_SECTION_START = 43   # 馬別データ開始位置
_BYTES_PER_HORSE     =  8   # 馬番(2) + 単勝オッズ×10(4) + 人気(2)


@dataclass
class RtdOdds:
    horse_number: int
    win_odds: float | None       # 単勝オッズ (None = 取得不可)
    popularity: int | None       # 人気順位


@dataclass
class RtdRaceInfo:
    race_id: str
    head_count: int              # 出走頭数
    odds: list[RtdOdds]         # 馬別オッズ（空リストの場合あり）


def _race_id_from_filename(stem: str) -> str:
    """
    ファイルのステム名から race_id を導出する。

    "0B302026041903010401" → "202603010401"
    (YEAR(4) + JYO(2) + KAI(2) + NICHI(2) + RACE(2))
    """
    date8 = stem[4:12]   # YYYYMMDD
    venue  = stem[12:14]  # JYO
    kai    = stem[14:16]  # KAI
    nichi  = stem[16:18]  # NICHI
    race   = stem[18:20]  # RACE
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
        logger.warning("RTD 出走頭数パース失敗 (race_id=%s): %r", race_id, head_count_str)
        return RtdRaceInfo(race_id=race_id, head_count=0, odds=[])

    if head_count < 1 or head_count > 28:
        return RtdRaceInfo(race_id=race_id, head_count=0, odds=[])

    horse_section = text[_HORSE_SECTION_START:]
    odds_list: list[RtdOdds] = []

    for i in range(head_count):
        offset = i * _BYTES_PER_HORSE
        chunk  = horse_section[offset : offset + _BYTES_PER_HORSE]
        if len(chunk) < _BYTES_PER_HORSE:
            break

        try:
            horse_no   = int(chunk[0:2])
            odds_raw   = int(chunk[2:6])          # オッズ × 10
            popularity = int(chunk[6:8])
        except ValueError:
            logger.debug("RTD 馬データパース失敗 (race_id=%s) chunk=%r", race_id, chunk)
            continue

        win_odds = round(odds_raw / 10.0, 1) if odds_raw > 0 else None

        odds_list.append(RtdOdds(
            horse_number=horse_no,
            win_odds=win_odds,
            popularity=popularity if popularity > 0 else None,
        ))

    return RtdRaceInfo(race_id=race_id, head_count=head_count, odds=odds_list)


def read_rtd_for_race(race_id: str) -> RtdRaceInfo | None:
    """
    指定 race_id に対応する .rtd ファイルを読み込んで RtdRaceInfo を返す。

    ファイルが存在しない（レース開始後は削除される）場合は None を返す。

    Args:
        race_id: 12 桁の race_id (例: "202603010401")

    Returns:
        RtdRaceInfo、またはファイル未存在の場合 None
    """
    # race_id → ファイル名へ変換
    # race_id = YYYY(4)+JYO(2)+KAI(2)+NICHI(2)+RACE(2)
    year  = race_id[0:4]
    jyo   = race_id[4:6]
    kai   = race_id[6:8]
    nichi = race_id[8:10]
    race  = race_id[10:12]
    date8 = race_id[0:8]   # YYYYMMDD ではない！ race_id の先頭8文字

    # race_id 先頭4文字は YEAR のみ。日付は外部から取得できないため
    # ファイル名パターン検索で代替する
    pattern = f"0B30*{jyo}{kai}{nichi}{race}.rtd"
    candidates = list(_RTD_DIR.glob(pattern))

    if not candidates:
        logger.debug("RTD ファイル未存在 (race_id=%s, pattern=%s)", race_id, pattern)
        return None

    # 最も新しいファイルを使用
    rtd_path = max(candidates, key=lambda p: p.stat().st_mtime)

    try:
        raw  = rtd_path.read_bytes()
        dec  = zlib.decompress(raw)
        text = dec.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning("RTD 解凍失敗 (race_id=%s, file=%s): %s", race_id, rtd_path.name, exc)
        return None

    if not text.startswith("O1"):
        logger.warning("RTD 非O1レコード (race_id=%s): %r", race_id, text[:10])
        return None

    info = _parse_o1(text, race_id)
    logger.info(
        "RTD 読み込み完了: race_id=%s 出走頭数=%d オッズ取得=%d頭",
        race_id, info.head_count, len(info.odds),
    )
    return info


def read_all_rtd_for_date(target_date: str) -> dict[str, RtdRaceInfo]:
    """
    指定日の全 .rtd ファイルを読み込んで {race_id: RtdRaceInfo} を返す。

    Args:
        target_date: "YYYYMMDD" 形式

    Returns:
        race_id → RtdRaceInfo のマッピング（ファイルが存在するレースのみ）
    """
    pattern = f"0B30{target_date}*.rtd"
    files   = sorted(_RTD_DIR.glob(pattern))

    if not files:
        logger.info("RTD ファイルなし (date=%s)", target_date)
        return {}

    result: dict[str, RtdRaceInfo] = {}
    for f in files:
        race_id = _race_id_from_filename(f.stem)
        try:
            raw  = f.read_bytes()
            dec  = zlib.decompress(raw)
            text = dec.decode("cp932", errors="replace")
        except Exception as exc:
            logger.warning("RTD 解凍失敗 %s: %s", f.name, exc)
            continue

        if not text.startswith("O1"):
            continue

        info = _parse_o1(text, race_id)
        if info.head_count > 0:
            result[race_id] = info

    logger.info("RTD 一括読み込み: %d ファイル → %d レース (date=%s)", len(files), len(result), target_date)
    return result


def rtd_odds_to_horse_odds(rtd_info: RtdRaceInfo) -> list:
    """
    RtdRaceInfo を entry_table.HorseOdds のリストに変換する。

    prerace_pipeline で insert_realtime_odds に渡すために使用。
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
