"""
JRA-VAN Data Lab. (JV-Link) Python クライアント
================================================

【重要】このモジュールは 32bit Python でのみ動作します。
  JV-Link は 32bit COM サーバーのため 64bit Python からは利用不可。

  実行コマンド:
    py -3.14-32 -m src.scraper.jravan_client --help
    py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --option 1

【依存】
  pip install pywin32  (32bit Python 用)

【JV-Link フロー】
  1. JVInit(sid)         - ソフトウェア認証
  2. JVOpen(spec, ...)   - データストリームをオープン
  3. JVRead(buff, ...)   - レコードを逐次読み込み（ループ）
  4. JVClose()           - ストリームをクローズ

【対応レコード種別 (JV-Data 仕様書 Ver.4.5.2)】
  [RACE dataspec]
  RA : レース詳細           (860 bytes)
  SE : 馬毎レース情報        (532 bytes ※近似)
  HR : 払戻金（全馬券種）
  WH/WF/WE/WQ/WM/WT/WS : 個別払戻（旧仕様）

  [WOOD dataspec]
  TC : 調教タイム
  HC : 坂路調教

  [BLOD dataspec]
  BT : 繁殖馬マスタ
  HN : 産駒マスタ

  [DIFN dataspec]
  UM : 競走馬マスタ
  KS : 騎手マスタ
  CH : 調教師マスタ

【フィールドオフセット注記】
  - 「確定」: 仕様書で確認済み
  - 「推定」: 仕様書から計算した近似値。実データで要検証
  --debug フラグで生レコードをダンプできます。
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────────
# JVOpen / JVRead 定数
# ────────────────────────────────────────────────────────────────────────────

# データ種別コード (JVOpen dataspec)
DATASPEC_RACE = "RACE"    # レース系: RA/SE/HR(払戻) など
DATASPEC_WOOD = "WOOD"    # 調教タイム・坂路調教: TC/HC
DATASPEC_SNAP = "SNAP"    # リアルタイムオッズ
DATASPEC_BLOD  = "BLOD"   # 血統データ(差分): BT(繁殖馬)/HN(産駒)
DATASPEC_DIFN  = "DIFN"   # マスタデータ(差分): UM(競走馬)/KS(騎手)/CH(調教師)
DATASPEC_SETUP = "SETUP"  # マスタ一括初期取得: BLOD+DIFN相当を一括配信 (option=2 推奨)

# JVOpen オプション
OPT_NORMAL  = 1  # 通常: fromtime 以降の差分データ
OPT_SETUP   = 2  # セットアップ: 全データ再取得（時間がかかる）
OPT_TODAY   = 3  # 当日データのみ
OPT_STORED  = 4  # 蓄積: ローカルキャッシュから取得

# JVRead 戻り値 (JRA-VAN 公式仕様)
# code > 0  : 正常読み取り（読み込んだバイト数）
JVREAD_EOF         =  0   # 全ファイル読み取り完了 → ループ終了
JVREAD_FILECHANGE  = -1   # ファイル切り替わり → スキップして次の JVRead へ
JVREAD_DOWNLOADING = -3   # ダウンロード中 → 1秒待機して再試行
# code < -1 かつ code != -3 : エラー → 中断

# ────────────────────────────────────────────────────────────────────────────
# コード変換テーブル
# ────────────────────────────────────────────────────────────────────────────

_JYO_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

_TRACK_CODES = {
    "1": "芝", "2": "ダート", "3": "障害",
}

# 内外コード → 方向文字列
_COURSE_CODES = {
    "1": "右", "2": "左", "3": "直線",
    "4": "右外", "5": "左外", "0": "",
}

_WEATHER_CODES = {
    "1": "晴", "2": "曇", "3": "小雨",
    "4": "雨", "5": "小雪", "6": "雪", "7": "霧", "0": "",
}

_CONDITION_CODES = {
    "1": "良", "2": "稍重", "3": "重", "4": "不良", "0": "",
}

_SEX_CODES = {
    "1": "牡", "2": "牝", "3": "騸",
}

_GEAR_CODES = {
    "1": "馬なり", "2": "強め", "3": "一杯", "4": "追切",
    "5": "軽め",   "0": "",
}

# 生産国コード（UM/BT/HN 共通）[推定]
_COUNTRY_CODES = {
    "01": "日本", "02": "アメリカ", "03": "フランス", "04": "イギリス",
    "05": "アイルランド", "06": "ドイツ", "07": "イタリア", "08": "カナダ",
    "09": "オーストラリア", "10": "ニュージーランド",
    "11": "アルゼンチン", "12": "ブラジル", "13": "その他",
}

# 東西所属コード（KS/CH 共通）[推定]
_EAST_WEST_CODES = {
    "11": "美浦", "12": "栗東",
    "21": "地方", "31": "外国", "00": "",
}

# 毛色コード（UM/BT/HN 共通）[推定]
_COAT_CODES = {
    "01": "栗毛", "02": "栗粕毛", "03": "鹿毛", "04": "黒鹿毛",
    "05": "青鹿毛", "06": "青毛", "07": "芦毛", "08": "白毛",
    "09": "栃栗毛", "10": "パロミノ", "11": "クリーム毛", "00": "",
}

# 払戻レコード種別 → (馬券種名, 最大組合せ数, 組合せバイト長)
# combo_bytes: 単・複=2, 2頭組み合わせ=4, 3頭組み合わせ=6
#
# HR : 払戻金（RACEデータスペックのメイン払戻レコード）
#       全馬券種の払戻を1レコードに順番に収録。offset 29 から以下の順に並ぶ:
#       単勝(3)→複勝(5)→枠連(3)→馬連(3)→ワイド(7)→馬単(6)→三連複(3)→三連単(6)
# WH/WF/… : 個別払戻レコード（旧仕様または別データスペック）
_PAYOUT_SPECS: dict[str, list[tuple[str, int, int]]] = {
    "HR": [
        ("単勝",   3, 2),
        ("複勝",   5, 2),
        ("枠連",   3, 4),
        ("馬連",   3, 4),
        ("ワイド", 7, 4),
        ("馬単",   6, 4),
        ("三連複", 3, 6),
        ("三連単", 6, 6),
    ],
    "WH": [("単勝", 3, 2), ("複勝", 5, 2)],
    "WF": [("枠連", 3, 4)],
    "WE": [("ワイド", 7, 4)],
    "WQ": [("馬連", 3, 4)],
    "WM": [("馬単", 6, 4)],
    "WT": [("三連複", 3, 6)],
    "WS": [("三連単", 6, 6)],
}

# 1エントリあたりの払戻金バイト数
_PAYOUT_AMOUNT_BYTES  = 8  # "00001500" = ¥1500
_PAYOUT_POP_BYTES     = 2  # 人気 2桁

# ────────────────────────────────────────────────────────────────────────────
# バイトスライス定義 (JV-Data 仕様書 Ver.4.5.2)
# ────────────────────────────────────────────────────────────────────────────

# ── 共通ヘッダー (全レコード共通) ──────────────────────────────
_H_REC_TYPE   = slice(0, 2)     # レコード種別 "RA"/"SE" etc.  [確定]
_H_DATA_DATE  = slice(2, 10)    # データ作成年月日 YYYYMMDD      [確定]

# ── レースキー (RA/SE/WH/WF/WE/WQ/WM/WT/WS 共通) ──────────────
_RK_JYO       = slice(10, 12)   # 場コード 01-10               [確定]
_RK_YEAR      = slice(12, 16)   # 開催年 YYYY                  [確定]
_RK_KAI       = slice(16, 17)   # 開催回 1-9                   [確定]
_RK_NICHI     = slice(17, 19)   # 開催日 01-08                 [確定]
_RK_RACE_NO   = slice(19, 21)   # レース番号 01-12             [確定]
_RK_KAISAI_DT = slice(21, 29)   # 開催年月日 YYYYMMDD          [確定]

# ── RA: レース詳細 ──────────────────────────────────────────────
# race_name 以降は仕様書から逆算した推定値。実データで確認してください。
_RA_RACE_NAME  = slice(29, 89)  # 競走名(漢字) 60バイト SJIS   [確定]
_RA_GRADE      = slice(192, 194) # グレードコード "A3"/"A2"    [推定]
_RA_DISTANCE   = slice(244, 248) # 距離 "2000"                 [推定]
_RA_TRACK      = slice(248, 249) # 芝ダート 1=芝,2=ダート,3=障害[推定]
_RA_COURSE     = slice(249, 250) # 内外 1=右,2=左,3=直,4=右外,5=左外[推定]
_RA_WEATHER    = slice(313, 314) # 天候コード                   [推定 ※要検証]
_RA_CONDITION  = slice(314, 315) # 芝馬場状態コード             [推定 ※要検証]
_RA_COND_DIRT  = slice(315, 316) # ダート馬場状態コード         [推定 ※要検証]

# ── SE: 馬毎レース情報 ─────────────────────────────────────────
_SE_WAKU_BAN   = slice(29, 30)   # 枠番 "1"-"8"               [確定]
_SE_UMA_BAN    = slice(30, 32)   # 馬番 "01"-"18"              [確定] offset 31-33 (1-based)
_SE_HORSE_ID   = slice(32, 42)   # 血統登録番号 10桁           [確定]
_SE_HORSE_NM   = slice(42, 78)   # 馬名(漢字) 36バイト SJIS   [確定]
_SE_SEX        = slice(78, 79)   # 性別 1=牡,2=牝,3=騸        [確定]
_SE_AGE        = slice(79, 81)   # 馬齢 "03"-"16"              [確定]
_SE_JOCKEY_CD  = slice(81, 86)   # 騎手コード 5桁              [確定]
_SE_JOCKEY_NM  = slice(86, 106)  # 騎手名 20バイト SJIS        [確定]
_SE_LOAD       = slice(107, 110) # 斤量 ×10 "580"=58.0kg      [推定]
_SE_TRAINER_CD = slice(110, 115) # 調教師コード 5桁             [推定]
_SE_TRAINER_NM = slice(115, 135) # 調教師名 20バイト SJIS       [推定]
# 以下は推定値。実データと照合して src/scraper/jravan_client.py の
# スライス定数を修正してください。
_SE_RANK       = slice(213, 215) # 着順 "01"-"18" (0=除外/取消) [推定]
_SE_WIN_ODDS   = slice(215, 220) # 単勝オッズ ×10 "01500"=1.5倍 [推定]
_SE_POPULARITY = slice(220, 222) # 人気 "01"-"18"               [推定]
_SE_FINISH_T   = slice(222, 226) # タイム ×10秒 "0915"=91.5秒   [推定]
_SE_MARGIN     = slice(226, 231) # 着差 SJIS                    [推定]
_SE_HORSE_WT   = slice(231, 234) # 馬体重 "480"                 [推定]
_SE_HORSE_DIFF = slice(234, 237) # 増減 "+4 " or "-12"          [推定]

# ── WC: 調教タイム（WOOD dataspec の実レコードタイプは WC） ──────
# 実データから確認済みのオフセット（103バイト + CRLF）
# ヘッダー: [0:2]=WC, [2:3]=データ区分(1桁), [3:11]=データ年月日
# [11:12]=調教場コード(1桁), [12:20]=調教年月日, [20:22]=時刻(時)
# [22:23]=コース種別コード(1桁), [23:33]=blood_id(10桁)
_WC_DATA_DATE   = slice(3, 11)   # データ年月日 YYYYMMDD         [実測]
_WC_JYO         = slice(11, 12)  # 調教場コード 1桁              [実測]
_WC_TRAINING_DT = slice(12, 20)  # 調教年月日 YYYYMMDD           [実測]
_WC_HOUR        = slice(20, 22)  # 調教時刻(時) HH               [実測]
_WC_COURSE_CD   = slice(22, 23)  # コース種別コード 1桁           [実測]
_WC_HORSE_ID    = slice(23, 33)  # blood_id 10桁                 [実測]
_WC_TIME_1F     = slice(44, 48)  # ラスト1Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_2F     = slice(48, 52)  # ラスト2Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_3F     = slice(52, 56)  # ラスト3Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_4F     = slice(56, 60)  # ラスト4Fタイム ×0.01秒        [実測位置・単位推定]
_WC_LAP_TIME    = slice(60, 64)  # ラップタイム ×0.01秒           [実測位置・単位推定]

# ── WH: 坂路調教（WOOD dataspec の坂路レコードタイプ） ──────────
# WH レコードは WC と同様のヘッダー構造と推定（実データ未確認）
_WH_DATA_DATE   = slice(3, 11)
_WH_JYO         = slice(11, 12)
_WH_TRAINING_DT = slice(12, 20)
_WH_HOUR        = slice(20, 22)
_WH_HORSE_ID    = slice(23, 33)
_WH_TIME_1F     = slice(44, 48)
_WH_TIME_2F     = slice(48, 52)
_WH_TIME_3F     = slice(52, 56)
_WH_TIME_4F     = slice(56, 60)
_WH_LAP_TIME    = slice(60, 64)

# ── TC/HC: 旧レコードタイプ（後方互換のため保持・現在未使用）───
_TC_TRAINING_DT = slice(10, 18)
_TC_HORSE_ID    = slice(20, 30)
_TC_COURSE_TYPE = slice(66, 68)
_TC_TIME_4F     = slice(68, 72)
_TC_TIME_3F     = slice(72, 76)
_TC_TIME_2F     = slice(76, 80)
_TC_TIME_1F     = slice(80, 84)
_TC_LAP_TIME    = slice(84, 88)
_TC_GEAR        = slice(88, 89)
_HC_TRAINING_DT = slice(10, 18)
_HC_HORSE_ID    = slice(18, 28)
_HC_TIME_4F     = slice(64, 68)
_HC_TIME_3F     = slice(68, 72)
_HC_TIME_2F     = slice(72, 76)
_HC_TIME_1F     = slice(76, 80)
_HC_LAP_TIME    = slice(80, 84)
_HC_GEAR        = slice(84, 85)

# ── BT: 繁殖馬マスタ ──────────────────────────────────────────
# ※ 以下はすべて [推定]。--debug で実データを確認して修正してください。
_BT_HORSE_ID    = slice(10, 20)   # 血統登録番号 10桁
_BT_HORSE_NM    = slice(20, 56)   # 馬名(漢字) 36バイト SJIS
_BT_HORSE_KANA  = slice(56, 92)   # 馬名(カナ) 36バイト SJIS
_BT_COUNTRY     = slice(92, 94)   # 生産国コード 2桁
_BT_SEX         = slice(94, 95)   # 性別コード 1桁
_BT_BIRTH_YEAR  = slice(95, 99)   # 生年 YYYY
_BT_BIRTH_MONTH = slice(99, 101)  # 生月 MM
_BT_COAT        = slice(101, 103) # 毛色コード 2桁
_BT_FATHER_ID   = slice(103, 113) # 父馬 血統登録番号
_BT_FATHER_NM   = slice(113, 149) # 父馬名 SJIS
_BT_MOTHER_ID   = slice(149, 159) # 母馬 血統登録番号
_BT_MOTHER_NM   = slice(159, 195) # 母馬名 SJIS

# ── HN: 産駒マスタ ────────────────────────────────────────────
# ※ 以下はすべて [推定]。BT と構造が類似しているが異なる場合がある。
_HN_HORSE_ID    = slice(10, 20)
_HN_HORSE_NM    = slice(20, 56)
_HN_HORSE_KANA  = slice(56, 92)
_HN_COUNTRY     = slice(92, 94)
_HN_SEX         = slice(94, 95)
_HN_BIRTH_YEAR  = slice(95, 99)
_HN_BIRTH_MONTH = slice(99, 101)
_HN_COAT        = slice(101, 103)
_HN_FATHER_ID   = slice(103, 113)
_HN_MOTHER_ID   = slice(113, 123)

# ── UM: 競走馬マスタ ──────────────────────────────────────────
# ※ 以下はすべて [推定]。
_UM_HORSE_ID    = slice(10, 20)   # 血統登録番号
_UM_HORSE_NM    = slice(20, 56)   # 馬名(漢字) SJIS
_UM_HORSE_KANA  = slice(56, 92)   # 馬名(カナ) SJIS
_UM_COUNTRY     = slice(92, 94)   # 生産国コード
_UM_SEX         = slice(94, 95)   # 性別コード
_UM_BIRTH_YEAR  = slice(95, 99)   # 生年 YYYY
_UM_BIRTH_MONTH = slice(99, 101)  # 生月 MM
_UM_COAT        = slice(101, 103) # 毛色コード
_UM_FATHER_ID   = slice(103, 113) # 父馬 血統登録番号
_UM_FATHER_NM   = slice(113, 149) # 父馬名 SJIS
_UM_MOTHER_ID   = slice(149, 159) # 母馬 血統登録番号
_UM_MOTHER_NM   = slice(159, 195) # 母馬名 SJIS
_UM_GRANDSIRE_ID = slice(195, 205) # 母父馬 血統登録番号
_UM_GRANDSIRE_NM = slice(205, 241) # 母父馬名 SJIS
_UM_TRAINER_CD  = slice(241, 246) # 調教師コード 5桁
_UM_TRAINER_NM  = slice(246, 266) # 調教師名 SJIS 20バイト
_UM_OWNER_CD    = slice(266, 271) # 馬主コード 5桁
_UM_OWNER_NM    = slice(271, 311) # 馬主名 SJIS 40バイト
_UM_EAST_WEST   = slice(311, 313) # 東西所属コード

# ── KS: 騎手マスタ ────────────────────────────────────────────
# ※ 以下はすべて [推定]。
_KS_CODE        = slice(10, 15)   # 騎手コード 5桁
_KS_NAME        = slice(15, 35)   # 騎手名(漢字) SJIS 20バイト
_KS_NAME_KANA   = slice(35, 55)   # 騎手名(カナ) SJIS 20バイト
_KS_EAST_WEST   = slice(55, 57)   # 東西所属コード
_KS_BIRTH_YEAR  = slice(57, 61)   # 生年 YYYY
_KS_BIRTH_MONTH = slice(61, 63)   # 生月 MM
_KS_BIRTH_DAY   = slice(63, 65)   # 生日 DD
_KS_LIC_YEAR    = slice(65, 69)   # 免許取得年 YYYY

# ── CH: 調教師マスタ ──────────────────────────────────────────
# ※ 以下はすべて [推定]。
_CH_CODE        = slice(10, 15)   # 調教師コード 5桁
_CH_NAME        = slice(15, 35)   # 調教師名(漢字) SJIS 20バイト
_CH_NAME_KANA   = slice(35, 55)   # 調教師名(カナ) SJIS 20バイト
_CH_EAST_WEST   = slice(55, 57)   # 東西所属コード
_CH_BIRTH_YEAR  = slice(57, 61)   # 生年 YYYY
_CH_BIRTH_MONTH = slice(61, 63)   # 生月 MM
_CH_BIRTH_DAY   = slice(63, 65)   # 生日 DD
_CH_LIC_YEAR    = slice(65, 69)   # 免許取得年 YYYY
_CH_STABLE_NM   = slice(69, 109)  # 厩舎名 SJIS 40バイト

# ────────────────────────────────────────────────────────────────────────────
# バイト解析ユーティリティ
# ────────────────────────────────────────────────────────────────────────────

def _to_bytes(com_str: str) -> bytes:
    """
    win32com が返す COM 文字列をバイト列に変換する。
    JV-Link は Shift-JIS データを COM BSTR として返すため、
    各文字が 1バイト値に対応する。
    """
    try:
        return com_str.encode('latin-1')
    except (UnicodeEncodeError, AttributeError):
        return bytes(ord(c) & 0xFF for c in com_str)


def _str(raw: bytes, sl: slice, encoding: str = 'ascii') -> str:
    """指定スライスをデコードして空白トリムして返す。"""
    try:
        return raw[sl].decode(encoding, errors='replace').strip()
    except Exception:
        return ''


def _sjis(raw: bytes, sl: slice) -> str:
    """Shift-JIS (cp932) フィールド用デコード。"""
    return _str(raw, sl, 'cp932')


def _safe_int_val(val: object, default: int = 0) -> int:
    """
    任意の値（COM 戻り値・文字列・整数）を安全に int に変換する。
    空文字列・空白のみ・None はすべて default を返す。
    """
    if val is None:
        return default
    try:
        s = str(val).strip()
        return int(s) if s else default
    except (ValueError, TypeError):
        return default


def _int(raw: bytes, sl: slice, default: int = 0) -> int:
    try:
        s = raw[sl].decode('ascii', errors='replace').strip()
        return int(s) if s else default
    except (ValueError, IndexError):
        return default


def _float(raw: bytes, sl: slice, divisor: float = 1.0) -> Optional[float]:
    """整数として読んで divisor で割る。0 は None 扱い。"""
    try:
        s = raw[sl].decode('ascii', errors='replace').strip()
        v = int(s) if s else 0
        return round(v / divisor, 1) if v > 0 else None
    except (ValueError, IndexError):
        return None


def _tenths_to_time(raw: bytes, sl: slice) -> Optional[str]:
    """× 10 秒整数 → "M:SS.s" 文字列。0 または空白は None。"""
    try:
        s = raw[sl].decode('ascii', errors='replace').strip()
        if not s:
            return None
        tenths = int(s)
        if tenths == 0:
            return None
        mins, rem = divmod(tenths, 600)
        secs, frac = divmod(rem, 10)
        return f"{mins}:{secs:02d}.{frac}" if mins else f"{secs}.{frac}"
    except (ValueError, IndexError):
        return None


def _signed_int(raw: bytes, sl: slice) -> Optional[int]:
    """"+4 " / "-12" 形式のバイト列を符号付き整数に変換。"""
    try:
        s = raw[sl].decode('ascii', errors='replace').strip()
        return int(s) if s else None
    except (ValueError, IndexError):
        return None


def _make_race_id(raw: bytes) -> str:
    """
    レースキーから DB 用 race_id (12桁) を生成する。

    JV-Data: JYO(2) + YEAR(4) + KAI(1) + NICHI(2) + RACE_NO(2) = offset 10-21
    DB形式:  YEAR(4) + JYO(2) + KAI(02d) + NICHI(2) + RACE_NO(2) = 12桁

    例: 中山2025年5回8日目11R → "202506050811"
    """
    year    = _str(raw, _RK_YEAR)
    jyo     = _str(raw, _RK_JYO)
    kai     = _str(raw, _RK_KAI).zfill(2)
    nichi   = _str(raw, _RK_NICHI)
    race_no = _str(raw, _RK_RACE_NO)
    return f"{year}{jyo}{kai}{nichi}{race_no}"


def _kaisai_date_to_db(raw: bytes) -> str:
    """YYYYMMDD → YYYY-MM-DD (ISO 8601)"""
    d = _str(raw, _RK_KAISAI_DT)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else ''


def _format_combo(raw_combo: bytes, combo_bytes: int) -> str:
    """
    組み合わせバイト列 → "3-7" / "1-3-7" 形式。
    2バイトごとに 1頭の番号として解釈する。
    """
    nums = []
    for i in range(0, combo_bytes, 2):
        chunk = raw_combo[i:i+2]
        try:
            n = int(chunk.decode('ascii', errors='replace').strip())
            if n > 0:
                nums.append(str(n))
        except ValueError:
            pass
    return '-'.join(nums)


def dump_record(raw: bytes, label: str = '') -> None:
    """
    デバッグ用: レコードを16進と ASCII で出力する。

    使い方:
        dump_record(data)               # 全体
        dump_record(data[:60], "RA先頭") # 先頭60バイトのみ
    """
    print(f"=== RECORD DUMP {label} ({len(raw)} bytes) ===")
    for i in range(0, len(raw), 16):
        chunk = raw[i:i+16]
        hex_part = ' '.join(f'{b:02x}' for b in chunk)
        asc_part = ''.join(chr(b) if 0x20 <= b < 0x7F else '.' for b in chunk)
        print(f"  {i:4d}  {hex_part:<48s}  {asc_part}")


# ────────────────────────────────────────────────────────────────────────────
# JV-Link COM ラッパー
# ────────────────────────────────────────────────────────────────────────────

class JVLinkClient:
    """
    JV-Link COM オブジェクト (JVDTLab.JVLink.1) の Python ラッパー。

    使い方:
        with JVLinkClient(sid="YOUR_SID") as client:
            client.open(DATASPEC_RACE, "20240101000000", OPT_NORMAL)
            while True:
                code, data = client.read_record()
                if code == JVREAD_EOF:
                    break
                if code > 0:
                    process(data)
    """

    # JVRead のバッファサイズ（最大レコード長より十分大きく確保）
    _BUFF_SIZE = 1_000_000

    def __init__(self, sid: str) -> None:
        self._sid   = sid
        self._jvl   = None              # COM オブジェクト
        self._buff  = ' ' * self._BUFF_SIZE
        self._fname = ' ' * 256

    # ── コンテキストマネージャ ──────────────────────────────────

    def __enter__(self) -> "JVLinkClient":
        self._connect()
        return self

    def __exit__(self, *_) -> None:
        try:
            self.close()
        except Exception:
            pass
        self._jvl = None

    # ── 接続・初期化 ────────────────────────────────────────────

    def _connect(self) -> None:
        """COM オブジェクトを生成して JVInit を実行する。"""
        try:
            import win32com.client  # type: ignore[import]
        except ImportError:
            raise RuntimeError(
                "pywin32 が見つかりません。\n"
                "  py -3.14-32 -m pip install pywin32\n"
                "で 32bit Python 用をインストールしてください。"
            )
        try:
            self._jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
        except Exception as e:
            raise RuntimeError(
                f"JV-Link COM 初期化失敗: {e}\n"
                "JV-Link がインストールされているか確認してください。\n"
                "また 32bit Python で実行しているか確認してください。"
            ) from e

        ret = self._jvl.JVInit(self._sid)
        if ret != 0:
            raise RuntimeError(f"JVInit 失敗 (code={ret}): SID を確認してください。")
        logger.info("JVInit 完了 sid=%s", self._sid)

    # ── JVOpen ──────────────────────────────────────────────────

    def open(self, dataspec: str, fromtime: str, option: int) -> int:
        """
        JVOpen を呼び出してデータストリームをオープンする。

        Args:
            dataspec: "RACE" / "WOOD" など
            fromtime: 読み込み開始時刻 "YYYYMMDDhhmmss"
            option:   OPT_NORMAL=1, OPT_SETUP=2, OPT_TODAY=3, OPT_STORED=4

        Returns:
            正の値: ダウンロード予定ファイル数
            0: ダウンロード不要（キャッシュ使用）
        """
        if len(fromtime) == 8:
            fromtime += "000000"   # YYYYMMDD → YYYYMMDDhhmmss

        try:
            result = self._jvl.JVOpen(dataspec, fromtime, option)
        except Exception:
            # JVOpen の引数形式が異なる場合のフォールバック
            try:
                result = self._jvl.JVOpen(dataspec, fromtime, option, 0, "")
            except Exception as e:
                raise RuntimeError(f"JVOpen 失敗: {e}") from e

        code = result[0] if isinstance(result, (tuple, list)) else _safe_int_val(result, default=-1)
        if isinstance(code, str):
            code = _safe_int_val(code, default=-1)
        if code < 0:
            raise RuntimeError(f"JVOpen エラーコード: {code}")

        dl_count = result[1] if isinstance(result, (tuple, list)) and len(result) > 1 else 0
        logger.info(
            "JVOpen: dataspec=%s fromtime=%s option=%d → code=%d dl=%s",
            dataspec, fromtime, option, code, dl_count,
        )
        return code

    # ── JVRead ──────────────────────────────────────────────────

    def read_record(self) -> tuple[int, bytes]:
        """
        1レコードを読み込む。

        Returns:
            (return_code, record_bytes)

            return_code:
              JVREAD_DATA=1        データあり
              JVREAD_FILECHANGE=2  ファイル切り替わり（データあり）
              JVREAD_EOF=0         読み込み完了
              JVREAD_NEEDS_CLOSE=-1 JVClose が必要
              その他負値           エラー
        """
        # 正しい引数順序: JVRead(BSTR *buff, long *size, BSTR *fname)
        # ※ 第2引数は size (long)、第3引数は fname (BSTR)
        try:
            result = self._jvl.JVRead(self._buff, self._BUFF_SIZE, self._fname)
        except Exception as e:
            logger.error("JVRead COM 呼び出し例外: %s", e)
            return -999, b''

        # win32com は BYREF パラメータをタプルで返す:
        #   result[0] = 戻り値 (LONG)
        #   result[1] = buff   (BSTR) — 読み込んだデータ
        #   result[2] = size   (LONG) — 実際に書き込まれたバイト数
        #   result[3] = fname  (BSTR) — ファイル名
        try:
            if isinstance(result, (tuple, list)):
                code    = _safe_int_val(result[0], default=-999)
                raw_str = result[1] if len(result) > 1 else self._buff
                size    = _safe_int_val(result[2]) if len(result) > 2 else 0
            else:
                code    = _safe_int_val(result, default=-999)
                raw_str = self._buff
                size    = 0
        except Exception as e:
            logger.error("JVRead 戻り値パース失敗: %s (result=%r)", e, result)
            return -999, b''

        if code > 0 and raw_str:
            data = _to_bytes(raw_str)
            if size > 0:
                data = data[:size]
            # ヌルバイトのみ除去（rstrip() で空白除去すると TC/HC の末尾フィールドが
            # 全スペースの場合に削除され、レコード長が短くなってパース失敗する）
            data = data.rstrip(b'\x00')
            return code, data

        return code, b''

    # ── JVClose ─────────────────────────────────────────────────

    def close(self) -> None:
        """JVClose を呼び出してストリームをクローズする。"""
        if self._jvl is not None:
            try:
                self._jvl.JVClose()
                logger.info("JVClose 完了")
            except Exception as e:
                logger.warning("JVClose エラー (無視): %s", e)

    # ── ステータス確認 ──────────────────────────────────────────

    def status(self) -> dict:
        """JVStatus を呼び出してダウンロード進捗を返す。"""
        try:
            result = self._jvl.JVStatus()
            return {"raw": result}
        except Exception as e:
            return {"error": str(e)}


# ────────────────────────────────────────────────────────────────────────────
# レコードパーサー
# ────────────────────────────────────────────────────────────────────────────

def parse_record(raw: bytes, debug: bool = False) -> Optional[dict]:
    """
    レコード種別を判定して適切なパーサーに振り分ける。

    Returns:
        パース結果 dict。不明種別・パース失敗は None。
        dict の "_record_type" キーで種別を判定できる。
    """
    if len(raw) < 2:
        return None

    rec_type = raw[:2].decode('ascii', errors='replace')

    if debug:
        dump_record(raw[:80], f"[{rec_type}]")

    if rec_type == 'RA':
        return _parse_ra(raw)
    if rec_type == 'SE':
        return _parse_se(raw)
    if rec_type in _PAYOUT_SPECS:
        return _parse_payout(raw, rec_type)
    if rec_type == 'WC':
        return _parse_wc(raw)
    if rec_type == 'WH':
        return _parse_wh(raw)
    if rec_type == 'TC':
        return _parse_tc(raw)
    if rec_type == 'HC':
        return _parse_hc(raw)
    if rec_type == 'BT':
        return _parse_bt(raw)
    if rec_type == 'HN':
        return _parse_hn(raw)
    if rec_type == 'UM':
        return _parse_um(raw)
    if rec_type == 'KS':
        return _parse_ks(raw)
    if rec_type == 'CH':
        return _parse_ch(raw)

    logger.debug("未対応レコード種別: %s", rec_type)
    return None


# ── RA: レース詳細 ──────────────────────────────────────────────

def _parse_ra(raw: bytes) -> Optional[dict]:
    """RA レース詳細レコードをパースして races テーブル用 dict を返す。"""
    if len(raw) < 29:
        return None

    race_id    = _make_race_id(raw)
    if not race_id or race_id == '000000000000':
        return None

    race_name  = _sjis(raw, _RA_RACE_NAME)
    kaisai_dt  = _kaisai_date_to_db(raw)
    jyo_code   = _str(raw, _RK_JYO)
    venue      = _JYO_NAMES.get(jyo_code, jyo_code)
    race_no    = _int(raw, _RK_RACE_NO)

    # 推定フィールド群
    dist_raw   = _str(raw, _RA_DISTANCE)
    distance   = _safe_int_val(dist_raw)

    track_raw  = _str(raw, _RA_TRACK)
    surface    = _TRACK_CODES.get(track_raw, '')

    course_raw = _str(raw, _RA_COURSE)
    direction  = _COURSE_CODES.get(course_raw, '')

    weather_raw = _str(raw, _RA_WEATHER)
    weather     = _WEATHER_CODES.get(weather_raw, '')

    cond_raw   = _str(raw, _RA_CONDITION)
    dirt_raw   = _str(raw, _RA_COND_DIRT)
    condition  = (
        _CONDITION_CODES.get(cond_raw, '') if surface in ('芝', '障害')
        else _CONDITION_CODES.get(dirt_raw, '')
    )

    return {
        '_record_type': 'RA',
        'race_id':         race_id,
        'race_name':       race_name,
        'date':            kaisai_dt,
        'venue':           venue,
        'race_number':     race_no,
        'distance':        distance,
        'surface':         surface,
        'track_direction': direction,
        'weather':         weather,
        'condition':       condition,
    }


# ── SE: 馬毎レース情報 ─────────────────────────────────────────

def _parse_se(raw: bytes) -> Optional[dict]:
    """SE 馬毎レース情報をパースして race_results + horses 用 dict を返す。"""
    if len(raw) < 42:
        return None

    race_id    = _make_race_id(raw)
    uma_ban    = _int(raw, _SE_UMA_BAN)
    waku_ban   = _int(raw, _SE_WAKU_BAN)
    horse_id   = _str(raw, _SE_HORSE_ID)
    horse_name = _sjis(raw, _SE_HORSE_NM)

    sex_raw    = _str(raw, _SE_SEX)
    age_raw    = _str(raw, _SE_AGE)
    sex_age    = _SEX_CODES.get(sex_raw, '') + age_raw.lstrip('0')

    jockey_nm  = _sjis(raw, _SE_JOCKEY_NM)
    trainer_nm = _sjis(raw, _SE_TRAINER_NM)

    load_raw   = _str(raw, _SE_LOAD)
    weight_car = _safe_int_val(load_raw) / 10.0

    # 推定フィールド（実データで要検証）
    rank       = _int(raw, _SE_RANK) or None
    win_odds   = _float(raw, _SE_WIN_ODDS, divisor=10.0)
    popularity = _int(raw, _SE_POPULARITY) or None
    finish_t   = _tenths_to_time(raw, _SE_FINISH_T)
    margin     = _sjis(raw, _SE_MARGIN) or None
    horse_wt   = _int(raw, _SE_HORSE_WT) or None
    horse_diff = _signed_int(raw, _SE_HORSE_DIFF)

    return {
        '_record_type':    'SE',
        'race_id':         race_id,
        'horse_id':        horse_id if horse_id else None,
        'horse_name':      horse_name,
        'rank':            rank,
        'gate_number':     waku_ban or None,
        'horse_number':    uma_ban or None,
        'sex_age':         sex_age,
        'weight_carried':  weight_car,
        'jockey':          jockey_nm,
        'trainer':         trainer_nm,
        'finish_time':     finish_t,
        'margin':          margin,
        'popularity':      popularity,
        'win_odds':        win_odds,
        'horse_weight':    horse_wt,
        'horse_weight_diff': horse_diff,
    }


# ── W*: 払戻レコード ───────────────────────────────────────────

def _parse_payout(raw: bytes, rec_type: str) -> Optional[dict]:
    """
    払戻レコード (WH/WF/WE/WQ/WM/WT/WS) をパースして
    race_payouts テーブル用リストを含む dict を返す。
    """
    race_id = _make_race_id(raw)
    specs   = _PAYOUT_SPECS.get(rec_type, [])
    payouts: list[dict] = []

    offset = 29   # レースキー直後からデータ開始
    for bet_type, max_entries, combo_bytes in specs:
        entry_len = combo_bytes + _PAYOUT_AMOUNT_BYTES + _PAYOUT_POP_BYTES
        for _ in range(max_entries):
            if offset + entry_len > len(raw):
                break
            combo_raw  = raw[offset : offset + combo_bytes]
            amount_raw = raw[offset + combo_bytes : offset + combo_bytes + _PAYOUT_AMOUNT_BYTES]
            pop_raw    = raw[offset + combo_bytes + _PAYOUT_AMOUNT_BYTES : offset + entry_len]

            combo  = _format_combo(combo_raw, combo_bytes)
            amount = _int(raw, slice(
                offset + combo_bytes,
                offset + combo_bytes + _PAYOUT_AMOUNT_BYTES
            ))
            pop    = _int(raw, slice(
                offset + combo_bytes + _PAYOUT_AMOUNT_BYTES,
                offset + entry_len
            )) or None

            if combo and amount > 0:
                payouts.append({
                    'bet_type':    bet_type,
                    'combination': combo,
                    'payout':      amount,
                    'popularity':  pop,
                })

            offset += entry_len

    if not payouts:
        return None

    return {
        '_record_type': rec_type,
        'race_id':  race_id,
        'payouts':  payouts,
    }


# ── WC: 調教タイム（実レコードタイプ）────────────────────────────

def _parse_wc(raw: bytes) -> Optional[dict]:
    """WC 調教タイムレコードをパースして training_times テーブル用 dict を返す。
    実データ確認済みオフセット使用。タイムは ×0.01秒単位（推定）。
    """
    if len(raw) < 64:
        return None

    horse_id = _str(raw, _WC_HORSE_ID)
    if not horse_id or horse_id == '0000000000':
        return None

    training_dt = _str(raw, _WC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8 else ''
    )
    if not training_date:
        return None

    return {
        '_record_type':  'TC',  # training_times テーブルに保存
        'horse_id':       horse_id,
        'horse_name':     '',
        'training_date':  training_date,
        'venue_code':     _str(raw, _WC_JYO),
        'course_type':    _str(raw, _WC_COURSE_CD),
        'time_4f':        _float(raw, _WC_TIME_4F, 100.0),
        'time_3f':        _float(raw, _WC_TIME_3F, 100.0),
        'time_2f':        _float(raw, _WC_TIME_2F, 100.0),
        'time_1f':        _float(raw, _WC_TIME_1F, 100.0),
        'lap_time':       _float(raw, _WC_LAP_TIME, 100.0),
        'gear':           '',
        'jockey_code':    '',
        'jockey_name':    '',
        'data_date':      _str(raw, _WC_DATA_DATE),
    }


# ── WH: 坂路調教（実レコードタイプ）────────────────────────────

def _parse_wh(raw: bytes) -> Optional[dict]:
    """WH 坂路調教レコードをパースして training_hillwork テーブル用 dict を返す。
    WH レコードは WC と同様のヘッダー構造と推定。
    """
    if len(raw) < 64:
        return None

    horse_id = _str(raw, _WH_HORSE_ID)
    if not horse_id or horse_id == '0000000000':
        return None

    training_dt = _str(raw, _WH_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8 else ''
    )
    if not training_date:
        return None

    return {
        '_record_type':  'HC',  # training_hillwork テーブルに保存
        'horse_id':       horse_id,
        'horse_name':     '',
        'training_date':  training_date,
        'time_4f':        _float(raw, _WH_TIME_4F, 100.0),
        'time_3f':        _float(raw, _WH_TIME_3F, 100.0),
        'time_2f':        _float(raw, _WH_TIME_2F, 100.0),
        'time_1f':        _float(raw, _WH_TIME_1F, 100.0),
        'lap_time':       _float(raw, _WH_LAP_TIME, 100.0),
        'gear':           '',
        'jockey_code':    '',
        'jockey_name':    '',
        'data_date':      _str(raw, _WH_DATA_DATE),
    }


# ── TC/HC: 旧レコードタイプ（現在の JVLink では発生しないが後方互換）──

def _parse_tc(raw: bytes) -> Optional[dict]:
    """TC 調教タイムレコード（旧形式）をパースする。"""
    if len(raw) < 90:
        return None
    horse_id = _str(raw, _TC_HORSE_ID)
    if not horse_id:
        return None
    training_dt = _str(raw, _TC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8 else ''
    )
    return {
        '_record_type':  'TC',
        'horse_id':       horse_id,
        'horse_name':     '',
        'training_date':  training_date,
        'venue_code':     '',
        'course_type':    _str(raw, _TC_COURSE_TYPE),
        'time_4f':        _float(raw, _TC_TIME_4F, 10.0),
        'time_3f':        _float(raw, _TC_TIME_3F, 10.0),
        'time_2f':        _float(raw, _TC_TIME_2F, 10.0),
        'time_1f':        _float(raw, _TC_TIME_1F, 10.0),
        'lap_time':       _float(raw, _TC_LAP_TIME, 10.0),
        'gear':           _GEAR_CODES.get(_str(raw, _TC_GEAR), ''),
        'jockey_code':    '',
        'jockey_name':    '',
        'data_date':      _str(raw, _H_DATA_DATE),
    }


# ── HC: 坂路調教（旧レコードタイプ、後方互換）──────────────────

def _parse_hc(raw: bytes) -> Optional[dict]:
    """HC 坂路調教レコード（旧形式）をパースする。"""
    if len(raw) < 90:
        return None
    horse_id = _str(raw, _HC_HORSE_ID)
    if not horse_id:
        return None
    training_dt = _str(raw, _HC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8 else ''
    )
    return {
        '_record_type':  'HC',
        'horse_id':       horse_id,
        'horse_name':     '',
        'training_date':  training_date,
        'time_4f':        _float(raw, _HC_TIME_4F, 10.0),
        'time_3f':        _float(raw, _HC_TIME_3F, 10.0),
        'time_2f':        _float(raw, _HC_TIME_2F, 10.0),
        'time_1f':        _float(raw, _HC_TIME_1F, 10.0),
        'lap_time':       _float(raw, _HC_LAP_TIME, 10.0),
        'gear':           _GEAR_CODES.get(_str(raw, _HC_GEAR), ''),
        'jockey_code':    '',
        'jockey_name':    '',
        'data_date':      _str(raw, _H_DATA_DATE),
    }


# ── BT: 繁殖馬マスタ ──────────────────────────────────────────

def _parse_bt(raw: bytes) -> Optional[dict]:
    """BT 繁殖馬マスタをパースして breeding_horses テーブル用 dict を返す。"""
    if len(raw) < 20:
        return None
    horse_id = _str(raw, _BT_HORSE_ID)
    if not horse_id:
        return None
    return {
        '_record_type':  'BT',
        'horse_id':       horse_id,
        'horse_name':     _sjis(raw, _BT_HORSE_NM),
        'horse_name_kana': _sjis(raw, _BT_HORSE_KANA),
        'country':        _COUNTRY_CODES.get(_str(raw, _BT_COUNTRY), _str(raw, _BT_COUNTRY)),
        'sex':            _SEX_CODES.get(_str(raw, _BT_SEX), ''),
        'birth_year':     _safe_int_val(_str(raw, _BT_BIRTH_YEAR)) or None,
        'birth_month':    _safe_int_val(_str(raw, _BT_BIRTH_MONTH)) or None,
        'coat_color':     _COAT_CODES.get(_str(raw, _BT_COAT), ''),
        'father_id':      _str(raw, _BT_FATHER_ID),
        'father_name':    _sjis(raw, _BT_FATHER_NM),
        'mother_id':      _str(raw, _BT_MOTHER_ID),
        'mother_name':    _sjis(raw, _BT_MOTHER_NM),
        'data_date':      _str(raw, _H_DATA_DATE),
    }


# ── HN: 産駒マスタ ────────────────────────────────────────────

def _parse_hn(raw: bytes) -> Optional[dict]:
    """HN 産駒マスタをパースして foals テーブル用 dict を返す。"""
    if len(raw) < 20:
        return None
    horse_id = _str(raw, _HN_HORSE_ID)
    if not horse_id:
        return None
    return {
        '_record_type':  'HN',
        'horse_id':       horse_id,
        'horse_name':     _sjis(raw, _HN_HORSE_NM),
        'horse_name_kana': _sjis(raw, _HN_HORSE_KANA),
        'country':        _COUNTRY_CODES.get(_str(raw, _HN_COUNTRY), _str(raw, _HN_COUNTRY)),
        'sex':            _SEX_CODES.get(_str(raw, _HN_SEX), ''),
        'birth_year':     _safe_int_val(_str(raw, _HN_BIRTH_YEAR)) or None,
        'birth_month':    _safe_int_val(_str(raw, _HN_BIRTH_MONTH)) or None,
        'coat_color':     _COAT_CODES.get(_str(raw, _HN_COAT), ''),
        'father_id':      _str(raw, _HN_FATHER_ID),
        'mother_id':      _str(raw, _HN_MOTHER_ID),
        'data_date':      _str(raw, _H_DATA_DATE),
    }


# ── UM: 競走馬マスタ ──────────────────────────────────────────

def _parse_um(raw: bytes) -> Optional[dict]:
    """UM 競走馬マスタをパースして racehorses テーブル用 dict を返す。"""
    if len(raw) < 20:
        return None
    horse_id = _str(raw, _UM_HORSE_ID)
    if not horse_id:
        return None
    return {
        '_record_type':   'UM',
        'horse_id':        horse_id,
        'horse_name':      _sjis(raw, _UM_HORSE_NM),
        'horse_name_kana': _sjis(raw, _UM_HORSE_KANA),
        'country':         _COUNTRY_CODES.get(_str(raw, _UM_COUNTRY), _str(raw, _UM_COUNTRY)),
        'sex':             _SEX_CODES.get(_str(raw, _UM_SEX), ''),
        'birth_year':      _safe_int_val(_str(raw, _UM_BIRTH_YEAR)) or None,
        'birth_month':     _safe_int_val(_str(raw, _UM_BIRTH_MONTH)) or None,
        'coat_color':      _COAT_CODES.get(_str(raw, _UM_COAT), ''),
        'father_id':       _str(raw, _UM_FATHER_ID),
        'father_name':     _sjis(raw, _UM_FATHER_NM),
        'mother_id':       _str(raw, _UM_MOTHER_ID),
        'mother_name':     _sjis(raw, _UM_MOTHER_NM),
        'grandsire_id':    _str(raw, _UM_GRANDSIRE_ID),
        'grandsire_name':  _sjis(raw, _UM_GRANDSIRE_NM),
        'trainer_code':    _str(raw, _UM_TRAINER_CD),
        'trainer_name':    _sjis(raw, _UM_TRAINER_NM),
        'owner_code':      _str(raw, _UM_OWNER_CD),
        'owner_name':      _sjis(raw, _UM_OWNER_NM),
        'east_west':       _EAST_WEST_CODES.get(_str(raw, _UM_EAST_WEST), ''),
        'data_date':       _str(raw, _H_DATA_DATE),
    }


# ── KS: 騎手マスタ ────────────────────────────────────────────

def _parse_ks(raw: bytes) -> Optional[dict]:
    """KS 騎手マスタをパースして jockeys テーブル用 dict を返す。"""
    if len(raw) < 15:
        return None
    jockey_code = _str(raw, _KS_CODE)
    if not jockey_code:
        return None

    by = _safe_int_val(_str(raw, _KS_BIRTH_YEAR))
    bm = _safe_int_val(_str(raw, _KS_BIRTH_MONTH))
    bd = _safe_int_val(_str(raw, _KS_BIRTH_DAY))
    birth_date = (
        f"{by:04d}/{bm:02d}/{bd:02d}"
        if by and bm and bd else ''
    )
    return {
        '_record_type':   'KS',
        'jockey_code':     jockey_code,
        'jockey_name':     _sjis(raw, _KS_NAME),
        'jockey_name_kana': _sjis(raw, _KS_NAME_KANA),
        'east_west':       _EAST_WEST_CODES.get(_str(raw, _KS_EAST_WEST), ''),
        'birth_date':      birth_date,
        'license_year':    _safe_int_val(_str(raw, _KS_LIC_YEAR)) or None,
        'data_date':       _str(raw, _H_DATA_DATE),
    }


# ── CH: 調教師マスタ ──────────────────────────────────────────

def _parse_ch(raw: bytes) -> Optional[dict]:
    """CH 調教師マスタをパースして trainers テーブル用 dict を返す。"""
    if len(raw) < 15:
        return None
    trainer_code = _str(raw, _CH_CODE)
    if not trainer_code:
        return None

    by = _safe_int_val(_str(raw, _CH_BIRTH_YEAR))
    bm = _safe_int_val(_str(raw, _CH_BIRTH_MONTH))
    bd = _safe_int_val(_str(raw, _CH_BIRTH_DAY))
    birth_date = (
        f"{by:04d}/{bm:02d}/{bd:02d}"
        if by and bm and bd else ''
    )
    return {
        '_record_type':    'CH',
        'trainer_code':     trainer_code,
        'trainer_name':     _sjis(raw, _CH_NAME),
        'trainer_name_kana': _sjis(raw, _CH_NAME_KANA),
        'east_west':        _EAST_WEST_CODES.get(_str(raw, _CH_EAST_WEST), ''),
        'birth_date':       birth_date,
        'license_year':     _safe_int_val(_str(raw, _CH_LIC_YEAR)) or None,
        'stable_name':      _sjis(raw, _CH_STABLE_NM),
        'data_date':        _str(raw, _H_DATA_DATE),
    }


# ────────────────────────────────────────────────────────────────────────────
# DB 保存
# ────────────────────────────────────────────────────────────────────────────

def save_records_to_db(
    records: list[dict],
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """
    パース済みレコードリストを DB に一括保存する。

    Returns:
        {"ra": 保存RA数, "se": 保存SE数, "payout": 保存払戻数,
         "tc": 保存TC数, "hc": 保存HC数, "skipped": スキップ数}
    """
    stats = {"ra": 0, "se": 0, "payout": 0, "tc": 0, "hc": 0,
             "bt": 0, "hn": 0, "um": 0, "ks": 0, "ch": 0, "skipped": 0}

    # RA → SE → その他 の順に保存して race_payouts の FK 制約を確実に満たす
    _ORDER = {'RA': 0, 'SE': 1}
    records = sorted(records, key=lambda r: _ORDER.get(r.get('_record_type', ''), 9))

    for rec in records:
        rt = rec.get('_record_type', '')
        try:
            if rt == 'RA':
                _save_ra(conn, rec)
                stats['ra'] += 1
            elif rt == 'SE':
                _save_se(conn, rec)
                stats['se'] += 1
            elif rt in _PAYOUT_SPECS:
                _save_payout(conn, rec)
                stats['payout'] += len(rec.get('payouts', []))
            elif rt == 'TC':
                _save_tc(conn, rec)
                stats['tc'] += 1
            elif rt == 'HC':
                _save_hc(conn, rec)
                stats['hc'] += 1
            elif rt == 'BT':
                _save_bt(conn, rec)
                stats['bt'] += 1
            elif rt == 'HN':
                _save_hn(conn, rec)
                stats['hn'] += 1
            elif rt == 'UM':
                _save_um(conn, rec)
                stats['um'] += 1
            elif rt == 'KS':
                _save_ks(conn, rec)
                stats['ks'] += 1
            elif rt == 'CH':
                _save_ch(conn, rec)
                stats['ch'] += 1
            else:
                stats['skipped'] += 1
        except sqlite3.IntegrityError as e:
            if 'FOREIGN KEY' in str(e):
                # 親レコード(races)が存在しない場合は静かにスキップ
                logger.debug("FK スキップ %s race_id=%s", rt, rec.get('race_id', '?'))
            else:
                logger.warning("保存失敗(整合性) %s race_id=%s: %s", rt, rec.get('race_id', '?'), e)
            stats['skipped'] += 1
        except Exception as e:
            logger.warning("保存失敗 %s race_id=%s: %s", rt, rec.get('race_id', '?'), e)
            stats['skipped'] += 1

    return stats


def _save_ra(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (r['race_id'], r['race_name'], r['date'], r['venue'], r['race_number'],
             r['distance'], r['surface'], r['track_direction'], r['weather'], r['condition']),
        )


def _save_se(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        # horses テーブル (horse_id がある場合のみ)
        if r.get('horse_id'):
            conn.execute(
                """
                INSERT INTO horses (horse_id, horse_name)
                VALUES (?, ?)
                ON CONFLICT(horse_id) DO UPDATE SET
                    horse_name = excluded.horse_name,
                    updated_at = datetime('now', 'localtime')
                """,
                (r['horse_id'], r['horse_name']),
            )

        # blood_id: JRA-VAN の血統登録番号（SE レコードの horse_id フィールド）
        # training_times.horse_id と同形式のため、調教データとの JOIN キーに使う
        blood_id = r.get('horse_id') if r.get('horse_id') and r.get('horse_id', '').strip('0') else None
        conn.execute(
            """
            INSERT INTO race_results
                (race_id, horse_id, horse_name, rank,
                 gate_number, horse_number,
                 sex_age, weight_carried, jockey, trainer,
                 finish_time, margin, popularity, win_odds,
                 horse_weight, horse_weight_diff, blood_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(race_id, horse_name) DO UPDATE SET
                gate_number       = excluded.gate_number,
                horse_number      = excluded.horse_number,
                horse_weight_diff = COALESCE(excluded.horse_weight_diff, race_results.horse_weight_diff),
                blood_id          = COALESCE(excluded.blood_id, race_results.blood_id)
            """,
            (r['race_id'], r.get('horse_id'), r['horse_name'], r.get('rank'),
             r.get('gate_number'), r.get('horse_number'),
             r.get('sex_age', ''), r.get('weight_carried', 0),
             r.get('jockey', ''), r.get('trainer', ''),
             r.get('finish_time'), r.get('margin'),
             r.get('popularity'), r.get('win_odds'),
             r.get('horse_weight'), r.get('horse_weight_diff'), blood_id),
        )


def _save_payout(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        for p in r.get('payouts', []):
            conn.execute(
                """
                INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(race_id, bet_type, combination) DO UPDATE SET
                    payout     = excluded.payout,
                    popularity = excluded.popularity
                """,
                (r['race_id'], p['bet_type'], p['combination'], p['payout'], p.get('popularity')),
            )


def _save_tc(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO training_times
                (horse_id, horse_name, training_date, venue_code, course_type,
                 time_4f, time_3f, time_2f, time_1f, lap_time,
                 gear, jockey_code, jockey_name, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id, training_date, course_type, direction) DO UPDATE SET
                time_4f   = excluded.time_4f,
                time_3f   = excluded.time_3f,
                time_2f   = excluded.time_2f,
                time_1f   = excluded.time_1f,
                lap_time  = excluded.lap_time,
                gear      = excluded.gear
            """,
            (r['horse_id'], r['horse_name'], r['training_date'],
             r.get('venue_code', ''), r.get('course_type', ''),
             r.get('time_4f'), r.get('time_3f'), r.get('time_2f'),
             r.get('time_1f'), r.get('lap_time'), r.get('gear', ''),
             r.get('jockey_code', ''), r.get('jockey_name', ''), r.get('data_date', '')),
        )


def _save_hc(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO training_hillwork
                (horse_id, horse_name, training_date,
                 time_4f, time_3f, time_2f, time_1f, lap_time,
                 gear, jockey_code, jockey_name, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id, training_date) DO UPDATE SET
                time_4f  = excluded.time_4f,
                time_3f  = excluded.time_3f,
                time_2f  = excluded.time_2f,
                time_1f  = excluded.time_1f,
                lap_time = excluded.lap_time,
                gear     = excluded.gear
            """,
            (r['horse_id'], r['horse_name'], r['training_date'],
             r.get('time_4f'), r.get('time_3f'), r.get('time_2f'),
             r.get('time_1f'), r.get('lap_time'), r.get('gear', ''),
             r.get('jockey_code', ''), r.get('jockey_name', ''), r.get('data_date', '')),
        )


def _save_bt(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO breeding_horses
                (horse_id, horse_name, horse_name_kana, country, sex,
                 birth_year, birth_month, coat_color,
                 father_id, father_name, mother_id, mother_name, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name      = excluded.horse_name,
                horse_name_kana = excluded.horse_name_kana,
                country         = excluded.country,
                sex             = excluded.sex,
                birth_year      = excluded.birth_year,
                birth_month     = excluded.birth_month,
                coat_color      = excluded.coat_color,
                father_id       = excluded.father_id,
                father_name     = excluded.father_name,
                mother_id       = excluded.mother_id,
                mother_name     = excluded.mother_name,
                data_date       = excluded.data_date,
                updated_at      = datetime('now', 'localtime')
            """,
            (r['horse_id'], r.get('horse_name', ''), r.get('horse_name_kana', ''),
             r.get('country', ''), r.get('sex', ''),
             r.get('birth_year'), r.get('birth_month'), r.get('coat_color', ''),
             r.get('father_id', ''), r.get('father_name', ''),
             r.get('mother_id', ''), r.get('mother_name', ''), r.get('data_date', '')),
        )


def _save_hn(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO foals
                (horse_id, horse_name, horse_name_kana, country, sex,
                 birth_year, birth_month, coat_color,
                 father_id, mother_id, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name      = excluded.horse_name,
                horse_name_kana = excluded.horse_name_kana,
                country         = excluded.country,
                sex             = excluded.sex,
                birth_year      = excluded.birth_year,
                birth_month     = excluded.birth_month,
                coat_color      = excluded.coat_color,
                father_id       = excluded.father_id,
                mother_id       = excluded.mother_id,
                data_date       = excluded.data_date,
                updated_at      = datetime('now', 'localtime')
            """,
            (r['horse_id'], r.get('horse_name', ''), r.get('horse_name_kana', ''),
             r.get('country', ''), r.get('sex', ''),
             r.get('birth_year'), r.get('birth_month'), r.get('coat_color', ''),
             r.get('father_id', ''), r.get('mother_id', ''), r.get('data_date', '')),
        )


def _save_um(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO racehorses
                (horse_id, horse_name, horse_name_kana, country, sex,
                 birth_year, birth_month, coat_color,
                 father_id, father_name, mother_id, mother_name,
                 grandsire_id, grandsire_name,
                 trainer_code, trainer_name,
                 owner_code, owner_name, east_west, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name      = excluded.horse_name,
                horse_name_kana = excluded.horse_name_kana,
                country         = excluded.country,
                sex             = excluded.sex,
                birth_year      = excluded.birth_year,
                birth_month     = excluded.birth_month,
                coat_color      = excluded.coat_color,
                father_id       = excluded.father_id,
                father_name     = excluded.father_name,
                mother_id       = excluded.mother_id,
                mother_name     = excluded.mother_name,
                grandsire_id    = excluded.grandsire_id,
                grandsire_name  = excluded.grandsire_name,
                trainer_code    = excluded.trainer_code,
                trainer_name    = excluded.trainer_name,
                owner_code      = excluded.owner_code,
                owner_name      = excluded.owner_name,
                east_west       = excluded.east_west,
                data_date       = excluded.data_date,
                updated_at      = datetime('now', 'localtime')
            """,
            (r['horse_id'], r.get('horse_name', ''), r.get('horse_name_kana', ''),
             r.get('country', ''), r.get('sex', ''),
             r.get('birth_year'), r.get('birth_month'), r.get('coat_color', ''),
             r.get('father_id', ''), r.get('father_name', ''),
             r.get('mother_id', ''), r.get('mother_name', ''),
             r.get('grandsire_id', ''), r.get('grandsire_name', ''),
             r.get('trainer_code', ''), r.get('trainer_name', ''),
             r.get('owner_code', ''), r.get('owner_name', ''),
             r.get('east_west', ''), r.get('data_date', '')),
        )


def _save_ks(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO jockeys
                (jockey_code, jockey_name, jockey_name_kana,
                 east_west, birth_date, license_year, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(jockey_code) DO UPDATE SET
                jockey_name      = excluded.jockey_name,
                jockey_name_kana = excluded.jockey_name_kana,
                east_west        = excluded.east_west,
                birth_date       = excluded.birth_date,
                license_year     = excluded.license_year,
                data_date        = excluded.data_date,
                updated_at       = datetime('now', 'localtime')
            """,
            (r['jockey_code'], r.get('jockey_name', ''), r.get('jockey_name_kana', ''),
             r.get('east_west', ''), r.get('birth_date', ''),
             r.get('license_year'), r.get('data_date', '')),
        )


def _save_ch(conn: sqlite3.Connection, r: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO trainers
                (trainer_code, trainer_name, trainer_name_kana,
                 east_west, birth_date, license_year, stable_name, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trainer_code) DO UPDATE SET
                trainer_name      = excluded.trainer_name,
                trainer_name_kana = excluded.trainer_name_kana,
                east_west         = excluded.east_west,
                birth_date        = excluded.birth_date,
                license_year      = excluded.license_year,
                stable_name       = excluded.stable_name,
                data_date         = excluded.data_date,
                updated_at        = datetime('now', 'localtime')
            """,
            (r['trainer_code'], r.get('trainer_name', ''), r.get('trainer_name_kana', ''),
             r.get('east_west', ''), r.get('birth_date', ''),
             r.get('license_year'), r.get('stable_name', ''), r.get('data_date', '')),
        )


# ────────────────────────────────────────────────────────────────────────────
# DB スキーマ拡張（調教テーブル追加）
# ────────────────────────────────────────────────────────────────────────────

_TRAINING_DDL = [
    # training_times: JV-Data TC レコード（調教タイム）
    """
    CREATE TABLE IF NOT EXISTS training_times (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        horse_id       TEXT    NOT NULL,
        horse_name     TEXT    NOT NULL DEFAULT '',
        training_date  TEXT    NOT NULL,           -- YYYY-MM-DD (ISO 8601)
        venue_code     TEXT    NOT NULL DEFAULT '', -- 調教場コード
        course_type    TEXT    NOT NULL DEFAULT '', -- W=ウッド, P=ポリ 等
        direction      TEXT    NOT NULL DEFAULT '', -- 左/右
        time_4f        REAL,                        -- 4ハロン (秒)
        time_3f        REAL,                        -- 3ハロン (秒)
        time_2f        REAL,                        -- 2ハロン (秒)
        time_1f        REAL,                        -- ラスト1ハロン (秒)
        lap_time       REAL,                        -- 全体タイム (秒)
        gear           TEXT    NOT NULL DEFAULT '', -- 馬なり/強め/一杯/追切
        jockey_code    TEXT    NOT NULL DEFAULT '',
        jockey_name    TEXT    NOT NULL DEFAULT '',
        data_date      TEXT    NOT NULL DEFAULT '', -- JVデータ作成日 YYYYMMDD
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(horse_id, training_date, course_type, direction)
    )
    """,

    # training_hillwork: JV-Data HC レコード（坂路調教）
    """
    CREATE TABLE IF NOT EXISTS training_hillwork (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        horse_id       TEXT    NOT NULL,
        horse_name     TEXT    NOT NULL DEFAULT '',
        training_date  TEXT    NOT NULL,           -- YYYY-MM-DD (ISO 8601)
        time_4f        REAL,
        time_3f        REAL,
        time_2f        REAL,
        time_1f        REAL,                        -- ラスト1ハロン (秒)
        lap_time       REAL,
        gear           TEXT    NOT NULL DEFAULT '',
        jockey_code    TEXT    NOT NULL DEFAULT '',
        jockey_name    TEXT    NOT NULL DEFAULT '',
        data_date      TEXT    NOT NULL DEFAULT '',
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(horse_id, training_date)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_training_times_horse ON training_times(horse_id)",
    "CREATE INDEX IF NOT EXISTS idx_training_times_date  ON training_times(training_date)",
    "CREATE INDEX IF NOT EXISTS idx_hillwork_horse       ON training_hillwork(horse_id)",
    "CREATE INDEX IF NOT EXISTS idx_hillwork_date        ON training_hillwork(training_date)",
]


_MASTER_DDL = [
    # ── breeding_horses: JV-Data BT レコード（繁殖馬マスタ）──────
    """
    CREATE TABLE IF NOT EXISTS breeding_horses (
        horse_id        TEXT    PRIMARY KEY,
        horse_name      TEXT    NOT NULL DEFAULT '',
        horse_name_kana TEXT    NOT NULL DEFAULT '',
        country         TEXT    NOT NULL DEFAULT '',
        sex             TEXT    NOT NULL DEFAULT '',
        birth_year      INTEGER,
        birth_month     INTEGER,
        coat_color      TEXT    NOT NULL DEFAULT '',
        father_id       TEXT    NOT NULL DEFAULT '',
        father_name     TEXT    NOT NULL DEFAULT '',
        mother_id       TEXT    NOT NULL DEFAULT '',
        mother_name     TEXT    NOT NULL DEFAULT '',
        data_date       TEXT    NOT NULL DEFAULT '',
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ── foals: JV-Data HN レコード（産駒マスタ）─────────────────
    """
    CREATE TABLE IF NOT EXISTS foals (
        horse_id        TEXT    PRIMARY KEY,
        horse_name      TEXT    NOT NULL DEFAULT '',
        horse_name_kana TEXT    NOT NULL DEFAULT '',
        country         TEXT    NOT NULL DEFAULT '',
        sex             TEXT    NOT NULL DEFAULT '',
        birth_year      INTEGER,
        birth_month     INTEGER,
        coat_color      TEXT    NOT NULL DEFAULT '',
        father_id       TEXT    NOT NULL DEFAULT '',
        mother_id       TEXT    NOT NULL DEFAULT '',
        data_date       TEXT    NOT NULL DEFAULT '',
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ── racehorses: JV-Data UM レコード（競走馬マスタ）───────────
    """
    CREATE TABLE IF NOT EXISTS racehorses (
        horse_id        TEXT    PRIMARY KEY,
        horse_name      TEXT    NOT NULL DEFAULT '',
        horse_name_kana TEXT    NOT NULL DEFAULT '',
        country         TEXT    NOT NULL DEFAULT '',
        sex             TEXT    NOT NULL DEFAULT '',
        birth_year      INTEGER,
        birth_month     INTEGER,
        coat_color      TEXT    NOT NULL DEFAULT '',
        father_id       TEXT    NOT NULL DEFAULT '',
        father_name     TEXT    NOT NULL DEFAULT '',
        mother_id       TEXT    NOT NULL DEFAULT '',
        mother_name     TEXT    NOT NULL DEFAULT '',
        grandsire_id    TEXT    NOT NULL DEFAULT '',
        grandsire_name  TEXT    NOT NULL DEFAULT '',
        trainer_code    TEXT    NOT NULL DEFAULT '',
        trainer_name    TEXT    NOT NULL DEFAULT '',
        owner_code      TEXT    NOT NULL DEFAULT '',
        owner_name      TEXT    NOT NULL DEFAULT '',
        east_west       TEXT    NOT NULL DEFAULT '',
        data_date       TEXT    NOT NULL DEFAULT '',
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ── jockeys: JV-Data KS レコード（騎手マスタ）────────────────
    """
    CREATE TABLE IF NOT EXISTS jockeys (
        jockey_code      TEXT    PRIMARY KEY,
        jockey_name      TEXT    NOT NULL DEFAULT '',
        jockey_name_kana TEXT    NOT NULL DEFAULT '',
        east_west        TEXT    NOT NULL DEFAULT '',
        birth_date       TEXT    NOT NULL DEFAULT '',
        license_year     INTEGER,
        data_date        TEXT    NOT NULL DEFAULT '',
        created_at       TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at       TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ── trainers: JV-Data CH レコード（調教師マスタ）─────────────
    """
    CREATE TABLE IF NOT EXISTS trainers (
        trainer_code      TEXT    PRIMARY KEY,
        trainer_name      TEXT    NOT NULL DEFAULT '',
        trainer_name_kana TEXT    NOT NULL DEFAULT '',
        east_west         TEXT    NOT NULL DEFAULT '',
        birth_date        TEXT    NOT NULL DEFAULT '',
        license_year      INTEGER,
        stable_name       TEXT    NOT NULL DEFAULT '',
        data_date         TEXT    NOT NULL DEFAULT '',
        created_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # インデックス
    "CREATE INDEX IF NOT EXISTS idx_racehorses_name    ON racehorses(horse_name)",
    "CREATE INDEX IF NOT EXISTS idx_racehorses_trainer ON racehorses(trainer_code)",
    "CREATE INDEX IF NOT EXISTS idx_breeding_father    ON breeding_horses(father_id)",
    "CREATE INDEX IF NOT EXISTS idx_foals_father       ON foals(father_id)",
    "CREATE INDEX IF NOT EXISTS idx_jockeys_name       ON jockeys(jockey_name)",
    "CREATE INDEX IF NOT EXISTS idx_trainers_name      ON trainers(trainer_name)",
]


def extend_db_schema(conn: sqlite3.Connection) -> None:
    """
    既存 DB に調教テーブル・マスタテーブルを追加する。
    既存テーブルには影響しない。
    """
    with conn:
        for ddl in _TRAINING_DDL + _MASTER_DDL:
            conn.execute(ddl)
    logger.info(
        "DB スキーマ拡張完了 "
        "(training_times / training_hillwork / "
        "breeding_horses / foals / racehorses / jockeys / trainers)"
    )


# ────────────────────────────────────────────────────────────────────────────
# 高レベルローダー
# ────────────────────────────────────────────────────────────────────────────

class JVDataLoader:
    """
    JVLinkClient + パーサー + DB 保存を一体化した高レベルインターフェース。

    使い方:
        loader = JVDataLoader(sid="YOUR_SID")
        stats  = loader.load(
            dataspec=DATASPEC_RACE,
            fromtime="20240101",
            option=OPT_NORMAL,
        )
        print(stats)
    """

    def __init__(
        self,
        sid: str,
        db_path: Optional[Path] = None,
        debug: bool = False,
    ) -> None:
        self._sid     = sid
        self._db_path = db_path
        self._debug   = debug

    def _get_conn(self) -> sqlite3.Connection:
        from src.database.init_db import init_db
        conn = init_db(self._db_path)
        extend_db_schema(conn)
        return conn

    def load(
        self,
        dataspec: str,
        fromtime: str,
        option: int = OPT_NORMAL,
    ) -> dict:
        """
        指定データ種別のレコードを JV-Link から全件読み込んで DB に保存する。

        Args:
            dataspec: DATASPEC_RACE / DATASPEC_WOOD 等
            fromtime: 開始日時 "YYYYMMDD" または "YYYYMMDDhhmmss"
            option:   OPT_NORMAL / OPT_SETUP / OPT_TODAY / OPT_STORED

        Returns:
            保存件数統計 dict
        """
        import time

        conn    = self._get_conn()
        records = []
        read_count = 0

        with JVLinkClient(self._sid) as client:
            client.open(dataspec, fromtime, option)

            while True:
                code, data = client.read_record()

                if code == JVREAD_EOF:
                    # 全ファイルの読み取り完了
                    break

                if code == JVREAD_FILECHANGE:
                    # ファイル切り替わり: データなし、次の JVRead へ
                    continue

                if code == JVREAD_DOWNLOADING:
                    # バックグラウンドダウンロード中: 1秒待機して再試行
                    logger.debug("ダウンロード待機中 (code=-3)…")
                    time.sleep(1)
                    continue

                if code < 0:
                    # その他の負値はエラー
                    raise RuntimeError(f"JVRead エラー: code={code}")

                # code > 0: 正常読み取り（バイト数）
                if data:
                    rec = parse_record(data, debug=self._debug)
                    if rec:
                        records.append(rec)
                read_count += 1
                if read_count % 500 == 0:
                    logger.info("読み込み中: %d レコード処理済み", read_count)

        stats = save_records_to_db(records, conn)
        conn.close()

        stats['total_read'] = read_count
        logger.info(
            "JV-Data 取得完了: read=%d "
            "RA=%d SE=%d payout=%d TC=%d HC=%d "
            "BT=%d HN=%d UM=%d KS=%d CH=%d skip=%d",
            read_count,
            stats['ra'], stats['se'], stats['payout'],
            stats['tc'], stats['hc'],
            stats['bt'], stats['hn'], stats['um'],
            stats['ks'], stats['ch'], stats['skipped'],
        )
        return stats

    def load_race(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """レース系データ (RA/SE/払戻) を取得・保存する。"""
        return self.load(DATASPEC_RACE, fromtime, option)

    def load_training(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """調教データ (TC/HC) を取得・保存する。"""
        return self.load(DATASPEC_WOOD, fromtime, option)

    def load_blod(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """血統データ (BT/HN) を取得・保存する。"""
        return self.load(DATASPEC_BLOD, fromtime, option)

    def load_difn(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """マスタデータ (UM/KS/CH) を取得・保存する。"""
        return self.load(DATASPEC_DIFN, fromtime, option)


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "JRA-VAN Data Lab. (JV-Link) データ取得ツール\n"
            "\n"
            "【重要】32bit Python で実行してください:\n"
            "  py -3.14-32 -m src.scraper.jravan_client [オプション]"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 2024年以降のレースデータを通常取得
  py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --dataspec RACE

  # 今日のレースデータのみ
  py -3.14-32 -m src.scraper.jravan_client --option 3 --dataspec RACE

  # 2024年以降の調教データ
  py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --dataspec WOOD

  # 血統データ（繁殖馬/産駒マスタ）
  py -3.14-32 -m src.scraper.jravan_client --fromtime 20200101 --dataspec BLOD

  # マスタデータ差分（競走馬/騎手/調教師）
  py -3.14-32 -m src.scraper.jravan_client --fromtime 20200101 --dataspec DIFN

  # マスタ一括初期取得（セットアップ: 全マスタを一度に取得）
  py -3.14-32 -m src.scraper.jravan_client --option 2 --dataspec SETUP

  # 全データ再取得（時間がかかる）
  py -3.14-32 -m src.scraper.jravan_client --option 2 --dataspec RACE

  # デバッグ: 生レコードをダンプしながら取得
  py -3.14-32 -m src.scraper.jravan_client --fromtime 20240101 --debug

【注意】
  --option 2 (セットアップ) は全データを再取得するため数時間かかる場合があります。
  通常運用は --option 1 (通常) を使用してください。
""",
    )
    parser.add_argument(
        '--sid',
        default='UMALOGI00',
        help='JRA-VAN ソフトウェアID (デフォルト: UMALOGI00)',
    )
    parser.add_argument(
        '--fromtime',
        default='20240101',
        metavar='YYYYMMDD',
        help='読み込み開始日 (デフォルト: 20240101)',
    )
    parser.add_argument(
        '--dataspec',
        choices=['RACE', 'WOOD', 'SNAP', 'BLOD', 'DIFN', 'SETUP'],
        default='RACE',
        help=(
            'データ種別 (デフォルト: RACE)。'
            'RACE=レース系, WOOD=調教, BLOD=血統差分, DIFN=マスタ差分, '
            'SETUP=マスタ一括初期取得 (--option 2 と併用)'
        ),
    )
    parser.add_argument(
        '--option',
        type=int,
        choices=[1, 2, 3, 4],
        default=1,
        help='取得オプション: 1=通常 2=セットアップ 3=今日 4=蓄積 (デフォルト: 1)',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='生レコードの先頭80バイトをダンプする (バイトオフセット確認用)',
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    # 32bit チェック
    if sys.maxsize > 2**32:
        logger.warning(
            "64bit Python で実行されています。"
            "JV-Link は 32bit COM のため動作しません。\n"
            "  py -3.14-32 -m src.scraper.jravan_client で再実行してください。"
        )

    args = _parse_args()

    loader = JVDataLoader(
        sid=args.sid,
        debug=args.debug,
    )

    logger.info(
        "取得開始: dataspec=%s fromtime=%s option=%d",
        args.dataspec, args.fromtime, args.option,
    )

    stats = loader.load(args.dataspec, args.fromtime, args.option)

    print(
        f"\n取得完了:\n"
        f"  読み込みレコード数 : {stats.get('total_read', 0):,}\n"
        f"  RA (レース)       : {stats['ra']:,}\n"
        f"  SE (馬毎結果)     : {stats['se']:,}\n"
        f"  払戻              : {stats['payout']:,}\n"
        f"  TC (調教タイム)   : {stats['tc']:,}\n"
        f"  HC (坂路調教)     : {stats['hc']:,}\n"
        f"  スキップ          : {stats['skipped']:,}"
    )


if __name__ == '__main__':
    main()
