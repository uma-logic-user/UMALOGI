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
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _kill_stale_py32(exclude_pid: int | None = None) -> int:
    """自分以外の 32bit Python プロセスを終了し、JVLink ストリーム競合を防ぐ。

    ⚠️ 重要: wmic の ExecutablePath で "-32" / "(x86)" を含む真の 32bit プロセス
    だけを対象とする。メモリ使用量ヒューリスティックは廃止済み（64bit Python を
    誤 kill する致命的バグがあったため）。

    親プロセスチェーンも保護対象に含め、呼び出し元 64bit Python を kill しない。

    Args:
        exclude_pid: 終了対象から除外するPID（通常は os.getpid()）。

    Returns:
        終了したプロセス数。
    """
    my_pid = exclude_pid if exclude_pid is not None else os.getpid()

    # 保護対象PIDセット: 自分 + 直接の親のみ（wmic を最小限に抑えて高速化）
    protected_pids: set[int] = {my_pid}
    try:
        # 親チェーンは 3 段まで（10段は wmic コストが高すぎる）
        _cur = my_pid
        for _ in range(3):
            ppid_res = subprocess.run(
                [
                    "wmic",
                    "process",
                    "where",
                    f"ProcessId={_cur}",
                    "get",
                    "ParentProcessId",
                ],
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                timeout=3,
            )
            for _ln in ppid_res.stdout.splitlines():
                _ln = _ln.strip()
                if _ln and _ln.isdigit():
                    _parent = int(_ln)
                    if _parent in protected_pids or _parent <= 4:
                        break
                    protected_pids.add(_parent)
                    _cur = _parent
                    break
            else:
                break
    except Exception:
        pass

    killed = 0
    try:
        # wmic で全 python.exe の実行パスを取得し、32bit プロセスのみ対象にする
        result = subprocess.run(
            [
                "wmic",
                "process",
                "where",
                "Name='python.exe'",
                "get",
                "ProcessId,ExecutablePath",
            ],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or "ProcessId" in line or "ExecutablePath" in line:
                continue
            # 行形式: "C:\Python314-32\python.exe  12345"
            # (ExecutablePath が空の場合は "  12345" になる)
            parts = line.rsplit(None, 1)
            if len(parts) < 1:
                continue
            pid_str = parts[-1]
            exe_path = parts[0] if len(parts) == 2 else ""
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if pid in protected_pids:
                continue
            # 32bit Python かどうかを実行パスで判定
            # パスが空 (System / Protected プロセス) はスキップ
            if not exe_path:
                continue
            exe_lower = exe_path.lower()
            is_32bit = (
                "(x86)" in exe_lower
                or "-32" in exe_lower
                or "32bit" in exe_lower
                or "\\python3" in exe_lower
                and exe_lower.endswith("-32\\python.exe")
            )
            if not is_32bit:
                continue  # 64bit Python は絶対に保護
            try:
                subprocess.run(
                    ["taskkill", "/F", "/PID", str(pid)],
                    capture_output=True,
                    timeout=5,
                )
                logger.debug(
                    "古い 32bit Python セッション終了: PID=%d path=%s", pid, exe_path
                )
                killed += 1
            except Exception:
                pass
    except Exception as exc:
        logger.debug("セッションクリーンアップスキップ: %s", exc)

    if killed:
        logger.info("古い 32bit Python セッション %d 件を終了しました", killed)
        time.sleep(1)  # OS がソケット/COM ハンドルを解放するまで待機
    return killed


# ────────────────────────────────────────────────────────────────────────────
# JVOpen / JVRead 定数
# ────────────────────────────────────────────────────────────────────────────

# データ種別コード (JVOpen dataspec)
DATASPEC_RACE = "RACE"  # レース系: RA/SE/HR(払戻) など
DATASPEC_WOOD = "WOOD"  # 調教タイム・坂路調教: TC/HC
DATASPEC_SNAP = "SNAP"  # リアルタイムオッズ
DATASPEC_BLOD = "BLOD"  # 血統データ(差分): BT(繁殖馬)/HN(産駒)
DATASPEC_DIFN = "DIFN"  # マスタデータ(差分): UM(競走馬)/KS(騎手)/CH(調教師)
DATASPEC_SETUP = "SETUP"  # マスタ一括初期取得: BLOD+DIFN相当を一括配信 (option=2 推奨)

# JVOpen オプション
OPT_NORMAL = 1  # 通常: fromtime 以降の差分データ
OPT_SETUP = 2  # セットアップ: 全データ再取得（時間がかかる）
OPT_TODAY = 3  # 当日データのみ
OPT_STORED = 4  # 蓄積: ローカルキャッシュから取得

# JVRead 戻り値 (JRA-VAN 公式仕様)
# code > 0  : 正常読み取り（読み込んだバイト数）
JVREAD_EOF = 0  # 全ファイル読み取り完了 → ループ終了
JVREAD_FILECHANGE = -1  # ファイル切り替わり → スキップして次の JVRead へ
JVREAD_DOWNLOADING = -3  # ダウンロード中 → 1秒待機して再試行
# code < -1 かつ code != -3 : エラー → 中断

# ────────────────────────────────────────────────────────────────────────────
# コード変換テーブル
# ────────────────────────────────────────────────────────────────────────────

_JYO_NAMES = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}

_TRACK_CODES = {
    "1": "芝",
    "2": "ダート",
    "3": "障害",
}

# 内外コード → 方向文字列
_COURSE_CODES = {
    "1": "右",
    "2": "左",
    "3": "直線",
    "4": "右外",
    "5": "左外",
    "0": "",
}

_WEATHER_CODES = {
    "1": "晴",
    "2": "曇",
    "3": "小雨",
    "4": "雨",
    "5": "小雪",
    "6": "雪",
    "7": "霧",
    "0": "",
}

_CONDITION_CODES = {
    "1": "良",
    "2": "稍重",
    "3": "重",
    "4": "不良",
    "0": "",
}

_SEX_CODES = {
    "1": "牡",
    "2": "牝",
    "3": "騸",
}

_GEAR_CODES = {
    "1": "馬なり",
    "2": "強め",
    "3": "一杯",
    "4": "追切",
    "5": "軽め",
    "0": "",
}

# 生産国コード（UM/BT/HN 共通）[推定]
_COUNTRY_CODES = {
    "01": "日本",
    "02": "アメリカ",
    "03": "フランス",
    "04": "イギリス",
    "05": "アイルランド",
    "06": "ドイツ",
    "07": "イタリア",
    "08": "カナダ",
    "09": "オーストラリア",
    "10": "ニュージーランド",
    "11": "アルゼンチン",
    "12": "ブラジル",
    "13": "その他",
}

# 東西所属コード（KS/CH 共通）[推定]
_EAST_WEST_CODES = {
    "11": "美浦",
    "12": "栗東",
    "21": "地方",
    "31": "外国",
    "00": "",
}

# 毛色コード（UM/BT/HN 共通）[推定]
_COAT_CODES = {
    "01": "栗毛",
    "02": "栗粕毛",
    "03": "鹿毛",
    "04": "黒鹿毛",
    "05": "青鹿毛",
    "06": "青毛",
    "07": "芦毛",
    "08": "白毛",
    "09": "栃栗毛",
    "10": "パロミノ",
    "11": "クリーム毛",
    "00": "",
}

# 払戻レコード種別 → (馬券種名, 最大組合せ数, 組合せバイト長, 人気バイト長, 1数字あたりバイト数)
# JV-Data 4.5.2 公式仕様に基づく値:
#   combo_bytes: 単/複=2(馬番2桁), 枠連=2(枠番1桁×2), 馬連/ワイド/馬単=4(馬番2桁×2),
#                三連複/三連単=6(馬番2桁×3)
#   pop_bytes:   通常=2桁, 三連複/三連単=3桁 (最大999通り)
#   chunk_size:  馬番=2(ASCII 2桁 "01"-"18"), 枠番=1(ASCII 1桁 "1"-"8")
#
# HR : 全馬券種の払戻を1レコードに順番に収録。offset 27 から以下の順に並ぶ:
#       単勝(3)→複勝(5)→枠連(3)→馬連(3)→ワイド(7)→馬単(6)→三連複(3)→三連単(6)
#
# 【2025/04/28 確定】枠連 combo_bytes=2 が正しい。JV-Data 4.5.2公式: 枠番1桁×2=2bytes。
#   combo_bytes=4 にすると枠連以降のオフセットが+6ずれ、馬連以降の組合せが
#   "3-91-11" "21-21-50" などの異常値になることをDBデータで確認。
_PAYOUT_SPECS: dict[str, list[tuple[str, int, int, int, int]]] = {
    "HR": [
        ("単勝", 3, 2, 2, 2),  # combo=2(馬番2桁×1), pop=2, chunk=2
        ("複勝", 5, 2, 2, 2),
        ("枠連", 3, 2, 2, 1),  # combo=2(枠番1桁×2), pop=2, chunk=1 [JV-Data 4.5.2 公式]
        ("馬連", 3, 4, 2, 2),  # combo=4(馬番2桁×2), pop=2, chunk=2
        ("ワイド", 7, 4, 2, 2),
        ("馬単", 6, 4, 2, 2),
        ("三連複", 3, 6, 3, 2),  # pop=3桁 (最大999通り) [JV-Data 4.5.2 公式仕様]
        ("三連単", 6, 6, 3, 2),  # pop=3桁 [JV-Data 4.5.2 公式仕様]
    ],
    "WH": [("単勝", 3, 2, 2, 2), ("複勝", 5, 2, 2, 2)],
    "WF": [("枠連", 3, 2, 2, 1)],
    "WE": [("ワイド", 7, 4, 2, 2)],
    "WQ": [("馬連", 3, 4, 2, 2)],
    "WM": [("馬単", 6, 4, 2, 2)],
    "WT": [("三連複", 3, 6, 3, 2)],
    "WS": [("三連単", 6, 6, 3, 2)],
}

# 1エントリあたりの払戻金バイト数
# JV-Data実測確定: 払戻金額は5桁ASCII (例: "16000" = ¥16,000)
_PAYOUT_AMOUNT_BYTES = 5  # "16000" = ¥16,000 [実データ hexdump 2025/04/28 確定]

# ────────────────────────────────────────────────────────────────────────────
# バイトスライス定義 (JV-Data 仕様書 Ver.4.5.2)
# ────────────────────────────────────────────────────────────────────────────

# ── 共通ヘッダー (全レコード共通) ──────────────────────────────
_H_REC_TYPE = slice(0, 2)  # レコード種別 "RA"/"SE" etc.  [確定]
_H_DATA_CAT = slice(2, 3)  # データ区分 "1"=通常等         [確定: 実データ検証済み]
_H_DATA_DATE = slice(3, 11)  # データ作成年月日 YYYYMMDD      [確定: 実データ検証済み]

# ── レースキー (RA/SE/HR/WH/WF/WE/WQ/WM/WT/WS 共通) ────────────
# 実データ検証済み: bytes[0:27] = type(2)+cat(1)+data_date(8)+kaisai_dt(8)+JYO(2)+KAI(2)+NICHI(2)+RACE_NO(2)
_RK_KAISAI_DT = slice(11, 19)  # 開催年月日 YYYYMMDD (場コードより前) [確定]
_RK_JYO = slice(19, 21)  # 場コード 01-10               [確定]
_RK_KAI = slice(21, 23)  # 開催回 "01"-"06" (2桁)       [確定]
_RK_NICHI = slice(23, 25)  # 開催日 "01"-"08"             [確定]
_RK_RACE_NO = slice(25, 27)  # レース番号 "01"-"12"         [確定]

# ── RA: レース詳細 ──────────────────────────────────────────────
_RA_RACE_NAME = slice(27, 87)  # 競走名(漢字) 60バイト SJIS   [確定]
_RA_GRADE = slice(190, 192)  # グレードコード "A3"/"A2"    [推定]
_RA_DISTANCE = slice(242, 246)  # 距離 "2000"                 [推定]
_RA_TRACK = slice(246, 247)  # 芝ダート 1=芝,2=ダート,3=障害[推定]
_RA_COURSE = slice(247, 248)  # 内外 1=右,2=左,3=直,4=右外,5=左外[推定]
_RA_WEATHER = slice(311, 312)  # 天候コード                   [推定 ※要検証]
_RA_CONDITION = slice(312, 313)  # 芝馬場状態コード             [推定 ※要検証]
_RA_COND_DIRT = slice(313, 314)  # ダート馬場状態コード         [推定 ※要検証]

# ── SE: 馬毎レース情報 ─────────────────────────────────────────
_SE_WAKU_BAN = slice(27, 28)  # 枠番 "1"-"8"               [確定]
_SE_UMA_BAN = slice(28, 30)  # 馬番 "01"-"18"              [確定]
_SE_HORSE_ID = slice(30, 40)  # 血統登録番号 10桁           [確定]
_SE_HORSE_NM = slice(40, 76)  # 馬名(漢字) 36バイト SJIS   [確定]
_SE_SEX = slice(78, 79)  # 性別 1=牡,2=牝,3=騸        [暫定 - 牝馬レースでのみ検証]
_SE_AGE = slice(80, 82)  # 馬齢                        [未確定 - 要調査]
_SE_TRAINER_EW = slice(84, 85)  # 調教師 東西所属 1=美浦/2=栗東 [W-076確定]
_SE_TRAINER_CD = slice(85, 90)  # 調教師コード 5桁(下5桁)      [W-076確定: CHマスタ5桁と一致]
_SE_TRAINER_NM = slice(90, 98)  # 調教師名 8バイト SJIS (4文字)[実測確定]
_SE_JOCKEY_CD = slice(296, 301)  # 騎手コード 5桁              [実測確定]
_SE_JOCKEY_NM = slice(306, 314)  # 騎手名 8バイト SJIS (4文字) [実測確定]
_SE_LOAD = slice(288, 291)  # 斤量 ×0.1kg: "550"→55.0kg  [実測確定]
_SE_RANK = slice(202, 204)  # 着順 "01"-"18" (0=除外/取消) [推定: 旧211-9ずれ補正]
_SE_WIN_ODDS = slice(204, 209)  # 単勝オッズ ×10 "01500"=1.5倍 [推定]
_SE_POPULARITY = slice(209, 211)  # 人気 "01"-"18"               [推定]
_SE_FINISH_T = slice(211, 215)  # タイム ×10秒 "0915"=91.5秒   [推定]
_SE_MARGIN = slice(215, 220)  # 着差 SJIS                    [推定]
_SE_HORSE_WT = slice(220, 223)  # 馬体重 "480"                 [推定]
_SE_HORSE_DIFF = slice(223, 226)  # 増減 "+4 " or "-12"          [推定]

# ── WC: 調教タイム（WOOD dataspec の実レコードタイプは WC） ──────
# 実データから確認済みのオフセット（103バイト + CRLF）
# ヘッダー: [0:2]=WC, [2:3]=データ区分(1桁), [3:11]=データ年月日
# [11:12]=調教場コード(1桁), [12:20]=調教年月日, [20:22]=時刻(時)
# [22:23]=コース種別コード(1桁), [23:33]=blood_id(10桁)
_WC_DATA_DATE = slice(3, 11)  # データ年月日 YYYYMMDD         [実測]
_WC_JYO = slice(11, 12)  # 調教場コード 1桁              [実測]
_WC_TRAINING_DT = slice(12, 20)  # 調教年月日 YYYYMMDD           [実測]
_WC_HOUR = slice(20, 22)  # 調教時刻(時) HH               [実測]
_WC_COURSE_CD = slice(22, 23)  # コース種別コード 1桁           [実測]
_WC_HORSE_ID = slice(23, 33)  # blood_id 10桁                 [実測]
_WC_TIME_1F = slice(44, 48)  # ラスト1Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_2F = slice(48, 52)  # ラスト2Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_3F = slice(52, 56)  # ラスト3Fタイム ×0.01秒        [実測位置・単位推定]
_WC_TIME_4F = slice(56, 60)  # ラスト4Fタイム ×0.01秒        [実測位置・単位推定]
_WC_LAP_TIME = slice(60, 64)  # ラップタイム ×0.01秒           [実測位置・単位推定]

# ── WH: 坂路調教（WOOD dataspec の坂路レコードタイプ） ──────────
# WH レコードは WC と同様のヘッダー構造と推定（実データ未確認）
_WH_DATA_DATE = slice(3, 11)
_WH_JYO = slice(11, 12)
_WH_TRAINING_DT = slice(12, 20)
_WH_HOUR = slice(20, 22)
_WH_HORSE_ID = slice(23, 33)
_WH_TIME_1F = slice(44, 48)
_WH_TIME_2F = slice(48, 52)
_WH_TIME_3F = slice(52, 56)
_WH_TIME_4F = slice(56, 60)
_WH_LAP_TIME = slice(60, 64)

# ── TC/HC: 旧レコードタイプ（後方互換のため保持・現在未使用）───
_TC_TRAINING_DT = slice(10, 18)
_TC_HORSE_ID = slice(20, 30)
_TC_COURSE_TYPE = slice(66, 68)
_TC_TIME_4F = slice(68, 72)
_TC_TIME_3F = slice(72, 76)
_TC_TIME_2F = slice(76, 80)
_TC_TIME_1F = slice(80, 84)
_TC_LAP_TIME = slice(84, 88)
_TC_GEAR = slice(88, 89)
_HC_TRAINING_DT = slice(10, 18)
_HC_HORSE_ID = slice(18, 28)
_HC_TIME_4F = slice(64, 68)
_HC_TIME_3F = slice(68, 72)
_HC_TIME_2F = slice(72, 76)
_HC_TIME_1F = slice(76, 80)
_HC_LAP_TIME = slice(80, 84)
_HC_GEAR = slice(84, 85)

# ── BT: 繁殖馬マスタ ──────────────────────────────────────────
# ※ 以下はすべて [推定]。--debug で実データを確認して修正してください。
_BT_HORSE_ID = slice(10, 20)  # 血統登録番号 10桁
_BT_HORSE_NM = slice(20, 56)  # 馬名(漢字) 36バイト SJIS
_BT_HORSE_KANA = slice(56, 92)  # 馬名(カナ) 36バイト SJIS
_BT_COUNTRY = slice(92, 94)  # 生産国コード 2桁
_BT_SEX = slice(94, 95)  # 性別コード 1桁
_BT_BIRTH_YEAR = slice(95, 99)  # 生年 YYYY
_BT_BIRTH_MONTH = slice(99, 101)  # 生月 MM
_BT_COAT = slice(101, 103)  # 毛色コード 2桁
_BT_FATHER_ID = slice(103, 113)  # 父馬 血統登録番号
_BT_FATHER_NM = slice(113, 149)  # 父馬名 SJIS
_BT_MOTHER_ID = slice(149, 159)  # 母馬 血統登録番号
_BT_MOTHER_NM = slice(159, 195)  # 母馬名 SJIS

# ── HN: 産駒マスタ ────────────────────────────────────────────
# ※ 以下はすべて [推定]。BT と構造が類似しているが異なる場合がある。
_HN_HORSE_ID = slice(10, 20)
_HN_HORSE_NM = slice(20, 56)
_HN_HORSE_KANA = slice(56, 92)
_HN_COUNTRY = slice(92, 94)
_HN_SEX = slice(94, 95)
_HN_BIRTH_YEAR = slice(95, 99)
_HN_BIRTH_MONTH = slice(99, 101)
_HN_COAT = slice(101, 103)
_HN_FATHER_ID = slice(103, 113)
_HN_MOTHER_ID = slice(113, 123)

# ── UM: 競走馬マスタ ──────────────────────────────────────────
# 【2026-06-07 実バイト検証で全面是正 / W-074】
# JV-Data ヘッダーは 11 バイト（種別2＋区分1＋作成日8）。旧定義は header 後の
# 全フィールドが誤配置（馬名を slice(20,56) 等）で racehorses 全列がゴミ化し、
# horse_id 名前空間も race_results と不一致だった。以下は実 UM レコード
# (1609B, 血統登録番号2016100752) の hex ダンプから確定した正規オフセット。
_UM_HORSE_ID = slice(11, 21)  # 血統登録番号 10桁  [確定: 実データ検証済み]
_UM_DEL_FLAG = slice(21, 22)  # 競走馬抹消区分 1
_UM_REG_DATE = slice(22, 30)  # 登録年月日 YYYYMMDD
_UM_DEL_DATE = slice(30, 38)  # 抹消年月日 YYYYMMDD
_UM_BIRTH_DATE = slice(38, 46)  # 生年月日 YYYYMMDD  [確定]
_UM_BIRTH_YEAR = slice(38, 42)  # 生年 YYYY          [確定]
_UM_BIRTH_MONTH = slice(42, 44)  # 生月 MM            [確定]
_UM_HORSE_NM = slice(46, 82)  # 馬名(漢字) 36バイト SJIS [確定]
_UM_HORSE_KANA = slice(82, 118)  # 馬名(半角カナ) 36バイト  [確定]
_UM_HORSE_EN = slice(118, 178)  # 馬名(欧字) 60バイト 末尾(XXX)に生産国 [確定]
_UM_UMAKIGO = slice(198, 200)  # 馬記号コード 2
_UM_SEX = slice(200, 201)  # 性別コード 1       [確定: 1=牡]
_UM_BREED = slice(201, 202)  # 品種コード 1
_UM_COAT = slice(202, 204)  # 毛色コード 2       [確定: 04=黒鹿毛]
# 3代血統(14頭): 各 [繁殖登録番号10 + 繁殖馬名36] = 46B。父=1,母=2,母父=5。
_UM_FATHER_ID = slice(204, 214)  # 父 繁殖登録番号    [確定]
_UM_FATHER_NM = slice(214, 250)  # 父名 SJIS         [確定]
_UM_MOTHER_ID = slice(250, 260)  # 母 繁殖登録番号    [確定]
_UM_MOTHER_NM = slice(260, 296)  # 母名 SJIS         [確定]
_UM_GRANDSIRE_ID = slice(388, 398)  # 母父 繁殖登録番号 [確定]
_UM_GRANDSIRE_NM = slice(398, 434)  # 母父名 SJIS      [確定]
# ※ 調教師/馬主/東西所属は post-pedigree 領域(byte848+)に存在するが、公式仕様
#    での精密確定が未了のため誤マッピング回避で空とする（W-074 残課題）。

# ── KS: 騎手マスタ ────────────────────────────────────────────
# 【2026-06-08 実バイト検証で是正 / W-075】UM と同様に旧定義は全フィールド誤配置で
# jockeys.name が数値ゴミ化し race_results.jockey と結合 0 件だった。実 KS レコード
# (4173B, コード00666=武豊) の hex ダンプで確定。レイアウト: header11 + コード5 +
# 抹消区分1 + 免許交付8 + 免許抹消8 + 生年月日8 + 騎手名漢字34。
# 騎手名漢字は姓名間に全角空白を含む（"武　豊"）が、race_results.jockey は SE 8バイト
# 名（"武豊"・空白無し）のため、_parse_ks で全角空白を除去して結合キーに合わせる。
_KS_CODE = slice(11, 16)  # 騎手コード 5桁         [確定]
_KS_BIRTH_DATE = slice(33, 41)  # 生年月日 YYYYMMDD     [確定]
_KS_BIRTH_YEAR = slice(33, 37)  # 生年 YYYY
_KS_BIRTH_MONTH = slice(37, 39)  # 生月 MM
_KS_BIRTH_DAY = slice(39, 41)  # 生日 DD
_KS_NAME = slice(41, 75)  # 騎手名(漢字) 34バイト SJIS [確定]
# ※ 半角カナ・東西所属・免許年は名漢字後の位置が KS/CH で不一致のため未マッピング
#    （誤マッピング回避・W-075 残課題）。

# ── CH: 調教師マスタ ──────────────────────────────────────────
# 【2026-06-08 実バイト検証で是正 / W-075】KS と同一レイアウト。実 CH レコード
# (3862B, コード00399=国枝栄) で確定。調教師名漢字も全角空白を除去して結合キー化。
_CH_CODE = slice(11, 16)  # 調教師コード 5桁       [確定]
_CH_BIRTH_DATE = slice(33, 41)  # 生年月日 YYYYMMDD     [確定]
_CH_BIRTH_YEAR = slice(33, 37)  # 生年 YYYY
_CH_BIRTH_MONTH = slice(37, 39)  # 生月 MM
_CH_BIRTH_DAY = slice(39, 41)  # 生日 DD
_CH_NAME = slice(41, 75)  # 調教師名(漢字) 34バイト SJIS [確定]

# ────────────────────────────────────────────────────────────────────────────
# バイト解析ユーティリティ
# ────────────────────────────────────────────────────────────────────────────


def _to_bytes(com_str: str) -> bytes:
    """
    win32com が返す COM 文字列をバイト列に変換する。
    JV-Link は Shift-JIS データを COM BSTR として返す。

    パターン1: BSTRが生SJISバイト列を保持 (各バイトをU+0000-U+00FF として格納)
               → encode('latin-1') でそのまま復元可能
    パターン2: JV-Linkが正規Unicodeを返す (U+3000以上の日本語文字)
               → encode('cp932') でSJIS変換可能

    【バグ修正 2026-05-08】
    latin-1 失敗時に encode('cp932', errors='replace') を使うと、
    Pattern 1 のリードバイト (U+0081-U+009F = C1制御文字) が CP932 未定義のため
    全て '?' (0x3F) に置換され競走名が文字化けする。
    修正: U+0000-U+00FF は直接バイト値として扱い (Pattern 1 保持)、
          U+0100+ のみ cp932 エンコードで変換 (Pattern 2 対応)。
    """
    if not isinstance(com_str, str):
        return b""
    try:
        return com_str.encode("latin-1")
    except (UnicodeEncodeError, AttributeError):
        # latin-1 失敗 → Pattern 1 と Pattern 2 が混在している可能性がある。
        # U+0000-U+00FF: バイト値をそのまま使用（CP932 リードバイト U+0083/U+0081 を保持）
        # U+0100+: cp932 でエンコード（正規Unicode日本語文字を CP932 バイト列へ）
        buf = bytearray()
        for ch in com_str:
            o = ord(ch)
            if o <= 0xFF:
                buf.append(o)
            else:
                try:
                    buf.extend(ch.encode("cp932"))
                except (UnicodeEncodeError, ValueError):
                    buf.append(0x3F)
        return bytes(buf)


def _str(raw: bytes, sl: slice, encoding: str = "ascii") -> str:
    """指定スライスをデコードして空白トリムして返す。制御文字は全除去。

    Args:
        raw:      元バイト列。
        sl:       抽出するスライス。
        encoding: デコードに使う文字エンコーディング。

    Returns:
        デコード・トリム・制御文字除去後の文字列。エラー時は空文字列。
    """
    try:
        from src.utils.text import sanitize_str

        return sanitize_str(raw[sl].decode(encoding, errors="replace"))
    except Exception as _exc:
        logger.debug(
            "_str decode error sl=%s enc=%s len=%d: %s", sl, encoding, len(raw), _exc
        )
        return ""


def _sjis(raw: bytes, sl: slice) -> str:
    """Shift-JIS (cp932) フィールド用デコード。制御文字は全除去。

    Args:
        raw: 元バイト列。
        sl:  抽出するスライス。

    Returns:
        cp932 デコード・トリム後の文字列。
    """
    return _str(raw, sl, "cp932")


def _sjis_name(raw: bytes, sl: slice) -> str:
    """名前フィールド（馬名・競走名・騎手名）専用の Shift-JIS デコード。

    ``_sjis()`` でデコード後に ``is_garbled_name()`` で文字化けを検査し、
    JVLink が破損した CP932 データを返した場合は空文字で保護する。
    これにより ``?X?eー?N?X`` 等の文字化けが DB に混入するのを防ぐ。

    Args:
        raw: 元バイト列。
        sl:  抽出するスライス。

    Returns:
        正常にデコードできた文字列。文字化け検出時は空文字列。
    """
    from src.utils.text import is_garbled_name
    from src.utils.discord_alert import send_mojibake_alert

    result = _sjis(raw, sl)
    if result and is_garbled_name(result):
        logger.warning(
            "[JVLink] 名前フィールド文字化け検出 → 空文字で保護: %r", result[:30]
        )
        send_mojibake_alert("JVLink", "name_field", result)
        return ""
    return result


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


# 欧字馬名末尾の生産国略称 → 日本語表記（UM 専用コード列が未確定のため代替）。
_EN_COUNTRY_CODES = {
    "JPN": "日本",
    "USA": "アメリカ",
    "FR": "フランス",
    "GB": "イギリス",
    "IRE": "アイルランド",
    "GER": "ドイツ",
    "ITY": "イタリア",
    "CAN": "カナダ",
    "AUS": "オーストラリア",
    "NZ": "ニュージーランド",
    "ARG": "アルゼンチン",
    "BRZ": "ブラジル",
}


def _extract_country_from_en(en_name: str) -> str:
    """欧字馬名末尾の "(JPN)" 等から生産国を抽出して日本語表記で返す。

    Args:
        en_name: 欧字馬名（例: "Meiner Virtus(JPN)"）。

    Returns:
        日本語の生産国名。判定不能時は空文字列。
    """
    import re

    m = re.search(r"\(([A-Z]{2,3})\)\s*$", en_name.strip())
    if not m:
        return ""
    code = m.group(1)
    return _EN_COUNTRY_CODES.get(code, code)


def _int(raw: bytes, sl: slice, default: int = 0) -> int:
    """指定スライスを ASCII 整数としてパースする。

    Args:
        raw:     元バイト列。
        sl:      抽出するスライス。
        default: パース失敗時のデフォルト値。

    Returns:
        パース結果の整数。失敗時は default。
    """
    try:
        s = raw[sl].decode("ascii", errors="replace").strip()
        return int(s) if s else default
    except (ValueError, IndexError):
        return default


def _float(raw: bytes, sl: slice, divisor: float = 1.0) -> Optional[float]:
    """整数として読んで divisor で割る。0 は None 扱い。

    Args:
        raw:     元バイト列。
        sl:      抽出するスライス。
        divisor: 除数（例: 10.0 → × 0.1 変換）。

    Returns:
        変換後の浮動小数点数。0 または変換不可は None。
    """
    try:
        s = raw[sl].decode("ascii", errors="replace").strip()
        v = int(s) if s else 0
        return round(v / divisor, 1) if v > 0 else None
    except (ValueError, IndexError):
        return None


def _tenths_to_time(raw: bytes, sl: slice) -> Optional[str]:
    """× 10 秒整数 → "M:SS.s" 文字列。0 または空白は None。

    Args:
        raw: 元バイト列。
        sl:  抽出するスライス。

    Returns:
        "M:SS.s" または "SS.s" 形式のタイム文字列。0 または空は None。
    """
    try:
        s = raw[sl].decode("ascii", errors="replace").strip()
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
    """ "+4 " / "-12" 形式のバイト列を符号付き整数に変換。

    Args:
        raw: 元バイト列。
        sl:  抽出するスライス。

    Returns:
        符号付き整数。空文字列または変換不可は None。
    """
    try:
        s = raw[sl].decode("ascii", errors="replace").strip()
        return int(s) if s else None
    except (ValueError, IndexError):
        return None


def _make_race_id(raw: bytes) -> str:
    """
    レースキーから DB 用 race_id (12桁) を生成する。

    JV-Data 実測構造: type(2)+cat(1)+date(8)+kaisai_dt(8)+JYO(2)+KAI(2)+NICHI(2)+RACE_NO(2)
    DB形式: YEAR(4) + JYO(2) + KAI(2) + NICHI(2) + RACE_NO(2) = 12桁
    YEARは開催年月日(kaisai_dt)の先頭4桁から取得。

    例: 中山2025年5回8日目11R → "202506050811"
    """
    kaisai = _str(raw, _RK_KAISAI_DT)  # "YYYYMMDD"
    year = kaisai[:4]
    jyo = _str(raw, _RK_JYO)
    kai = _str(raw, _RK_KAI)
    nichi = _str(raw, _RK_NICHI)
    race_no = _str(raw, _RK_RACE_NO)
    return f"{year}{jyo}{kai}{nichi}{race_no}"


def _kaisai_date_to_db(raw: bytes) -> str:
    """開催年月日バイト列を ISO 8601 形式に変換する。

    Args:
        raw: レースキーを含む元バイト列。

    Returns:
        "YYYY-MM-DD" 形式の日付文字列。パース失敗時は空文字列。
    """
    d = _str(raw, _RK_KAISAI_DT)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else ""


def _format_combo(raw_combo: bytes, combo_bytes: int, chunk_size: int = 2) -> str:
    """
    組み合わせバイト列 → "3-7" / "1-3-7" 形式。

    chunk_size=2: 馬番 "01"-"18" (2桁ASCII × n頭)
    chunk_size=1: 枠番 "1"-"8"  (1桁ASCII × 2枠, 枠連専用)
    """
    # chunk_size=1 → 枠番(1-8), chunk_size=2 → 馬番(1-18)
    max_num = 8 if chunk_size == 1 else 18
    nums = []
    for i in range(0, combo_bytes, chunk_size):
        chunk = raw_combo[i : i + chunk_size]
        try:
            n = int(chunk.decode("ascii", errors="replace").strip())
            if 0 < n <= max_num:
                nums.append(str(n))
        except ValueError:
            pass
    return "-".join(nums)


def dump_record(raw: bytes, label: str = "") -> None:
    """
    デバッグ用: レコードを16進と ASCII で出力する。

    使い方:
        dump_record(data)               # 全体
        dump_record(data[:60], "RA先頭") # 先頭60バイトのみ
    """
    print(f"=== RECORD DUMP {label} ({len(raw)} bytes) ===")
    for i in range(0, len(raw), 16):
        chunk = raw[i : i + 16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        asc_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
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

    _MAX_RECONNECT = 3  # JVInit 失敗時の最大再試行回数

    def __init__(self, sid: str) -> None:
        self._sid = sid
        # JVLink COM オブジェクト（接続前は None・実行時は win32com ディスパッチ）。
        self._jvl: Any = None
        self._buff = " " * self._BUFF_SIZE
        self._fname = " " * 256

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
        """COM オブジェクトを生成して JVInit を実行する（失敗時は自動再試行）。"""
        # JVLink 起動前にダイアログ自動突破ハンドラーを起動（多重起動安全）
        # スケジューラー外（バックテスト・直接実行）でも必ず保護される。
        try:
            from src.ops.jvlink_dialog_handler import start_dialog_handler

            start_dialog_handler(interval=0.3)
            logger.debug("[JVLinkClient] ダイアログハンドラー起動確認済み")
        except Exception as _dh_exc:
            logger.debug("[JVLinkClient] ダイアログハンドラー起動スキップ: %s", _dh_exc)

        try:
            import win32com.client  # type: ignore[import]
        except ImportError:
            raise RuntimeError(
                "pywin32 が見つかりません。\n"
                "  py -3.14-32 -m pip install pywin32\n"
                "で 32bit Python 用をインストールしてください。"
            )

        # JVOpen 前に古い 32bit Python セッションを終了してストリーム競合を防ぐ
        _kill_stale_py32(exclude_pid=os.getpid())

        try:
            self._jvl = win32com.client.Dispatch("JVDTLab.JVLink.1")
        except Exception as e:
            raise RuntimeError(
                f"JV-Link COM 初期化失敗: {e}\n"
                "JV-Link がインストールされているか確認してください。\n"
                "また 32bit Python で実行しているか確認してください。"
            ) from e

        # ────────────────────────────────────────────────────────────────
        # ダイアログ完全抑制
        #
        # 旧 JVLink (JVLink.1) には JVSetUI(hwnd) メソッドが存在したが、
        # 新 JVLink (JVDTLab.JVLink.1) では廃止され AttributeError になる。
        # 代わりに ParentHWnd(hwnd) メソッドと JVSetUIProperties() を使う。
        #
        # ParentHWnd(0) = 親ウィンドウなし → JVLink が UI を表示しようとしても
        # 描画先のウィンドウがないため実質的にダイアログがブロッキングしない。
        # ────────────────────────────────────────────────────────────────
        _ui_suppressed = False

        # Step A: 新 API — ParentHWnd プロパティを 0 に設定してヘッドレス化
        # COM Property なので object.Prop = value 形式でセット（メソッド呼び出しではない）
        try:
            self._jvl.ParentHWnd = 0  # 親ウィンドウなし → ダイアログが描画できない
            logger.debug("ParentHWnd=0 完了 — ダイアログ親ウィンドウなし設定")
            _ui_suppressed = True
        except Exception as _hwnd_exc:
            logger.debug("ParentHWnd=0 非対応: %s", _hwnd_exc)

        # Step B: 新 API — JVSetUIProperties() でUI非表示プロパティ設定（引数なし）
        try:
            self._jvl.JVSetUIProperties()
            logger.debug("JVSetUIProperties() 完了")
            _ui_suppressed = True
        except Exception as _prop_exc:
            logger.debug("JVSetUIProperties() 非対応: %s", _prop_exc)

        # Step C: 旧 API — JVSetUI(0) (フォールバック)
        if not _ui_suppressed:
            try:
                self._jvl.JVSetUI(0)
                logger.debug("JVSetUI(0) 完了 — ダイアログ無効化 (旧API)")
                _ui_suppressed = True
            except Exception as _ui_exc:
                logger.warning(
                    "ダイアログ抑制API (ParentHWnd/JVSetUIProperties/JVSetUI) がすべて"
                    "失敗しました。UI表示を抑制できない可能性があります: %s",
                    _ui_exc,
                )

        # Step D: JVSetDialog(False) — JVLink が自発的に起動するダイアログをすべて抑制
        try:
            self._jvl.JVSetDialog(False)
            logger.info("JVSetDialog(False) 完了 — JVLink自発ダイアログ無効化")
        except Exception as _dlg_exc:
            logger.debug("JVSetDialog 非対応: %s", _dlg_exc)

        # Step E: JVSetAutoDownload(True) — ダウンロード確認ダイアログなしで自動DL
        try:
            self._jvl.JVSetAutoDownload(True)
            logger.info("JVSetAutoDownload(True) 完了 — 自動ダウンロード有効化")
        except Exception as _ald_exc:
            logger.debug("JVSetAutoDownload 非対応: %s", _ald_exc)

        if _ui_suppressed:
            logger.info("JVLink GUIダイアログ抑制完了 (Steps A-E)")

        for attempt in range(1, self._MAX_RECONNECT + 1):
            ret = self._jvl.JVInit(self._sid)
            if ret == 0:
                logger.info("JVInit 完了 sid=%s (attempt=%d)", self._sid, attempt)
                # 親プロセスへ「JVLink初期化成功」を通知（GUIダイアログブロック検出用）
                print("JVLINK_READY", flush=True)
                return
            logger.warning(
                "JVInit 失敗 code=%d sid=%s (attempt=%d/%d)",
                ret,
                self._sid,
                attempt,
                self._MAX_RECONNECT,
            )
            if attempt < self._MAX_RECONNECT:
                time.sleep(1)  # 3s→1s: タイムアウト10秒以内に収めるため短縮

        # 全リトライ消化 → 親プロセスへ「JVLink初期化失敗」を通知して即時終了
        # GUI_BLOCKED タイムアウトを待たずに Netkeiba フォールバックへ切り替えさせる
        print("JVLINK_FAILED", flush=True)
        raise RuntimeError(
            f"JVInit 失敗 (全{self._MAX_RECONNECT}回): SID={self._sid!r} を確認してください。\n"
            "TARGET frontier を起動してログイン後に再実行、もしくは .env の JRAVAN_SID を確認してください。"
        )

    def _reconnect(self) -> None:
        """セッション切れ時に JVClose → JVInit を再実行して接続を回復する。

        Raises:
            RuntimeError: JVInit が全リトライ後も失敗した場合。
        """
        logger.info("JVLink 再接続を試みます...")
        try:
            if self._jvl is not None:
                self._jvl.JVClose()
        except Exception:
            pass
        self._connect()

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
            fromtime += "000000"  # YYYYMMDD → YYYYMMDDhhmmss

        # COM [in,out] パラメータ形式に対応するため 5引数形式を優先して試みる
        result = None
        for call_args in [
            (dataspec, fromtime, option, 0, ""),  # 5引数 ([in,out] 形式)
            (dataspec, fromtime, option),  # 3引数 ([out] 形式フォールバック)
        ]:
            try:
                result = self._jvl.JVOpen(*call_args)
                _code = (
                    result[0]
                    if isinstance(result, (tuple, list))
                    else _safe_int_val(result, default=-1)
                )
                if isinstance(_code, str):
                    _code = _safe_int_val(_code, default=-1)
                if _code >= 0 or len(call_args) == 3:
                    # 成功 or 最後のフォールバック → ループ終了
                    break
                # -1 だった場合は次の形式を試す
                logger.debug(
                    "JVOpen %d引数で%d → 次の形式を試みます", len(call_args), _code
                )
            except Exception as e:
                if len(call_args) == 3:
                    raise RuntimeError(f"JVOpen 失敗: {e}") from e
                logger.debug(
                    "JVOpen %d引数で例外 → 次の形式を試みます: %s", len(call_args), e
                )

        code = (
            result[0]
            if isinstance(result, (tuple, list))
            else _safe_int_val(result, default=-1)
        )
        if isinstance(code, str):
            code = _safe_int_val(code, default=-1)

        # -2: JVInit 未呼び出し（セッション切れ）→ 自動再接続して1回だけリトライ
        if code == -2:
            logger.warning("JVOpen code=-2: セッション切れ → 自動再接続します")
            self._reconnect()
            result = None
            for call_args in [
                (dataspec, fromtime, option, 0, ""),
                (dataspec, fromtime, option),
            ]:
                try:
                    result = self._jvl.JVOpen(*call_args)
                    _code = (
                        result[0]
                        if isinstance(result, (tuple, list))
                        else _safe_int_val(result, default=-1)
                    )
                    if isinstance(_code, str):
                        _code = _safe_int_val(_code, default=-1)
                    if _code >= 0 or len(call_args) == 3:
                        break
                except Exception as e:
                    if len(call_args) == 3:
                        raise RuntimeError(f"JVOpen 再接続後に失敗: {e}") from e
            code = (
                result[0]
                if isinstance(result, (tuple, list))
                else _safe_int_val(result, default=-1)
            )
            if isinstance(code, str):
                code = _safe_int_val(code, default=-1)

        # -3: ストリーム多重オープン → 致命的エラー
        if code == -3:
            raise RuntimeError(
                f"JVOpen 致命的エラーコード: {code} (ストリーム多重オープン)"
            )

        # その他の負値 (-1=パラメータエラー/-303=データなし等) はデータ取得不可として扱う
        if code < 0:
            logger.warning(
                "JVOpen: dataspec=%s fromtime=%s → code=%d (データなし/取得不可, スキップ)",
                dataspec,
                fromtime,
                code,
            )
            return code  # 呼び出し元で負値チェックして処理をスキップさせる

        dl_count = (
            result[1] if isinstance(result, (tuple, list)) and len(result) > 1 else 0
        )
        logger.info(
            "JVOpen: dataspec=%s fromtime=%s option=%d → code=%d dl=%s",
            dataspec,
            fromtime,
            option,
            code,
            dl_count,
        )
        return code

    # ── JVRTOpen（速報リアルタイム）─────────────────────────────

    def rt_open(self, dataspec: str, key: str) -> int:
        """JVRTOpen を呼び出して速報（リアルタイム）データストリームをオープンする。

        速報系 dataspec 例:
          0B30 速報オッズ（単複枠）/ 0B11 速報馬名表 / 0B42 速報天候馬場
        key 形式: ``{YYYYMMDD}{JYO}{KAI}{NICHI}{RR}`` (16桁レースキー) 等。

        オープン後は :meth:`read_record` で逐次読み込みできる（JVRead 共用）。

        Args:
            dataspec: 速報データ種別コード。
            key:      速報キー。

        Returns:
            0=成功 / 負値=エラー（-1 該当データなし, -114 キー書式不正 等）。
            呼び出し元は負値をデータ取得不可として扱うこと。
        """
        try:
            result = self._jvl.JVRTOpen(dataspec, key)
        except Exception as e:
            logger.warning("JVRTOpen 例外: dataspec=%s key=%s err=%s", dataspec, key, e)
            return -999
        code = (
            result[0]
            if isinstance(result, (tuple, list))
            else _safe_int_val(result, default=-1)
        )
        if isinstance(code, str):
            code = _safe_int_val(code, default=-1)
        if code < 0:
            logger.info(
                "JVRTOpen: dataspec=%s key=%s → code=%d (取得不可)", dataspec, key, code
            )
        else:
            logger.info("JVRTOpen: dataspec=%s key=%s → code=%d", dataspec, key, code)
        return code

    # ── JVRead ──────────────────────────────────────────────────

    def read_record(self) -> tuple[int, bytes]:
        """
        1レコードを読み込む。

        Returns:
            (return_code, record_bytes)

            return_code:
              JVREAD_DATA=N>0      データあり（Nバイト読み込み）
              JVREAD_EOF=0         全ファイル読み取り完了
              JVREAD_FILECHANGE=-1 ファイル切り替わり（JVRead を再呼び出し）
              JVREAD_DOWNLOADING=-3 ダウンロード中（1秒待機して再呼び出し）
              その他負値           JVLink エラー（セッション再起動が必要）
        """
        # 正しい引数順序: JVRead(BSTR *buff, long *size, BSTR *fname)
        # ※ 第2引数は size (long)、第3引数は fname (BSTR)
        try:
            result = self._jvl.JVRead(self._buff, self._BUFF_SIZE, self._fname)
        except Exception as e:
            logger.error("JVRead COM 呼び出し例外: %s", e)
            return -999, b""

        # win32com は BYREF パラメータをタプルで返す:
        #   result[0] = 戻り値 (LONG)
        #   result[1] = buff   (BSTR) — 読み込んだデータ
        #   result[2] = size   (LONG) — 実際に書き込まれたバイト数
        #   result[3] = fname  (BSTR) — ファイル名
        try:
            if isinstance(result, (tuple, list)):
                code = _safe_int_val(result[0], default=-999)
                raw_str = result[1] if len(result) > 1 else self._buff
                size = _safe_int_val(result[2]) if len(result) > 2 else 0
            else:
                code = _safe_int_val(result, default=-999)
                raw_str = self._buff
                size = 0
        except Exception as e:
            logger.error("JVRead 戻り値パース失敗: %s (result=%r)", e, result)
            return -999, b""

        if code > 0 and raw_str:
            data = _to_bytes(raw_str)
            if size > 0:
                data = data[:size]
            # ヌルバイトのみ除去（rstrip() で空白除去すると TC/HC の末尾フィールドが
            # 全スペースの場合に削除され、レコード長が短くなってパース失敗する）
            data = data.rstrip(b"\x00")
            return code, data

        return code, b""

    # ── JVClose ─────────────────────────────────────────────────

    def close(self) -> None:
        """JVClose を呼び出してストリームをクローズする。

        エラーが発生しても例外は送出せず警告ログのみ出力する。
        """
        if self._jvl is not None:
            try:
                self._jvl.JVClose()
                logger.info("JVClose 完了")
            except Exception as e:
                logger.warning("JVClose エラー (無視): %s", e)

    # ── ステータス確認 ──────────────────────────────────────────

    def status(self) -> dict:
        """JVStatus を呼び出してダウンロード進捗を返す。

        Returns:
            {"raw": JVStatus 戻り値} または {"error": エラーメッセージ}。
        """
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

    data_cat フィルタリング:
      '1' = 確定データ → 通常処理
      '2' = 速報/暫定データ → 払戻レコード(HR/WH等)はスキップ。
            速報払戻には 16,000 等のプレースホルダー値が格納されており
            DB を汚染する。RA/SE は速報でも出走・着順情報として使用可。
      '3' = 削除レコード → RA/SE/払戻をスキップ

    Returns:
        パース結果 dict。不明種別・パース失敗・フィルタ済みは None。
        dict の "_record_type" キーで種別を判定できる。
    """
    if len(raw) < 3:
        return None

    rec_type = raw[:2].decode("ascii", errors="replace")
    data_cat = chr(raw[2])

    if debug:
        dump_record(raw[:80], f"[{rec_type}] cat={data_cat}")

    # cat='3' = 削除レコード → RA/SE/払戻はスキップ
    if data_cat == "3" and rec_type in (*_PAYOUT_SPECS, "RA", "SE"):
        logger.debug("削除レコードスキップ: %s cat=%s", rec_type, data_cat)
        return None

    if rec_type == "RA":
        return _parse_ra(raw)
    if rec_type == "SE":
        return _parse_se(raw)
    if rec_type == "JG":
        return _parse_jg(raw)
    if rec_type in _PAYOUT_SPECS:
        # cat='1'(確定) および cat='2'(速報) を受け入れる。
        # レース終了後に JRA-VAN が cat='2' で配信するケースがあるため。
        # _parse_payout 内で amount >= 100 の検証を行うため
        # プレースホルダー値 (0, 負値) は自動除外される。
        # cat='3'(削除) は上位の削除レコードチェックで既にスキップ済み。
        if data_cat not in ("1", "2"):
            logger.debug("速報払戻スキップ (cat=%s): %s", data_cat, rec_type)
            return None
        return _parse_payout(raw, rec_type)
    if rec_type == "WC":
        return _parse_wc(raw)
    if rec_type == "WH":
        return _parse_wh(raw)
    if rec_type == "TC":
        return _parse_tc(raw)
    if rec_type == "HC":
        return _parse_hc(raw)
    if rec_type == "BT":
        return _parse_bt(raw)
    if rec_type == "HN":
        return _parse_hn(raw)
    if rec_type == "UM":
        return _parse_um(raw)
    if rec_type == "KS":
        return _parse_ks(raw)
    if rec_type == "CH":
        return _parse_ch(raw)

    logger.debug("未対応レコード種別: %s", rec_type)
    return None


# ── RA: レース詳細 ──────────────────────────────────────────────


def _parse_ra(raw: bytes) -> Optional[dict]:
    """RA レース詳細レコードをパースして races テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        races テーブル用フィールドを含む dict。パース失敗・無効データは None。
    """
    if len(raw) < 29:
        return None

    race_id = _make_race_id(raw)
    if not race_id or race_id == "000000000000":
        return None

    # 先頭5文字は賞金等級コード（"10000" 等）なので除去し、末尾の全角スペース・置換文字を除去
    _rn_raw = _sjis_name(raw, _RA_RACE_NAME)
    race_name = re.sub(r"^\d{5}", "", _rn_raw).replace("�", "").strip("　 　").strip()
    kaisai_dt = _kaisai_date_to_db(raw)
    jyo_code = _str(raw, _RK_JYO)
    venue = _JYO_NAMES.get(jyo_code, jyo_code)
    race_no = _int(raw, _RK_RACE_NO)

    # 推定フィールド群
    dist_raw = _str(raw, _RA_DISTANCE)
    distance = _safe_int_val(dist_raw)

    track_raw = _str(raw, _RA_TRACK)
    surface = _TRACK_CODES.get(track_raw, "")

    course_raw = _str(raw, _RA_COURSE)
    direction = _COURSE_CODES.get(course_raw, "")

    weather_raw = _str(raw, _RA_WEATHER)
    weather = _WEATHER_CODES.get(weather_raw, "")

    cond_raw = _str(raw, _RA_CONDITION)
    dirt_raw = _str(raw, _RA_COND_DIRT)
    condition = (
        _CONDITION_CODES.get(cond_raw, "")
        if surface in ("芝", "障害")
        else _CONDITION_CODES.get(dirt_raw, "")
    )

    return {
        "_record_type": "RA",
        "race_id": race_id,
        "race_name": race_name,
        "date": kaisai_dt,
        "venue": venue,
        "race_number": race_no,
        "distance": distance,
        "surface": surface,
        "track_direction": direction,
        "weather": weather,
        "condition": condition,
    }


# ── SE: 馬毎レース情報 ─────────────────────────────────────────


def _parse_se(raw: bytes) -> Optional[dict]:
    """SE 馬毎レース情報をパースして race_results + horses 用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        race_results / horses テーブル用フィールドを含む dict。
        パース失敗・無効データは None。
    """
    if len(raw) < 42:
        return None

    race_id = _make_race_id(raw)
    uma_ban = _int(raw, _SE_UMA_BAN)
    waku_ban = _int(raw, _SE_WAKU_BAN)
    horse_id = _str(raw, _SE_HORSE_ID)
    horse_name = _sjis_name(raw, _SE_HORSE_NM)

    sex_raw = _str(raw, _SE_SEX)
    age_raw = _str(raw, _SE_AGE)
    sex_age = _SEX_CODES.get(sex_raw, "") + age_raw.lstrip("0")

    jockey_nm = _sjis_name(raw, _SE_JOCKEY_NM).strip("　 ")
    trainer_nm = _sjis_name(raw, _SE_TRAINER_NM).strip("　 ")

    # W-076: 騎手/調教師コード（マスタ結合用）。氏名は8バイト切り詰めで結合不能のため
    # コードを直接保存する。jockey=5桁・trainer=5桁(6桁フィールドの下5桁=東西除く)。
    jockey_cd = _str(raw, _SE_JOCKEY_CD)
    trainer_cd = _str(raw, _SE_TRAINER_CD)
    jockey_cd = jockey_cd if jockey_cd and jockey_cd.strip("0") else None
    trainer_cd = trainer_cd if trainer_cd and trainer_cd.strip("0") else None

    load_raw = _str(raw, _SE_LOAD)
    load_int = _safe_int_val(load_raw)
    # 斤量は3桁ASCII×0.1kg: "550"→55.0kg, "520"→52.0kg (実測確定)
    weight_car = load_int / 10.0 if load_int > 0 else 0.0

    # 推定フィールド（実データで要検証）
    rank = _int(raw, _SE_RANK) or None
    win_odds = _float(raw, _SE_WIN_ODDS, divisor=10.0)
    popularity = _int(raw, _SE_POPULARITY) or None
    finish_t = _tenths_to_time(raw, _SE_FINISH_T)
    margin = _sjis(raw, _SE_MARGIN) or None
    horse_wt = _int(raw, _SE_HORSE_WT) or None
    horse_diff = _signed_int(raw, _SE_HORSE_DIFF)

    return {
        "_record_type": "SE",
        "race_id": race_id,
        "horse_id": horse_id if horse_id else None,
        "horse_name": horse_name,
        "rank": rank,
        "gate_number": waku_ban or None,
        "horse_number": uma_ban or None,
        "sex_age": sex_age,
        "weight_carried": weight_car,
        "jockey": jockey_nm,
        "trainer": trainer_nm,
        "jockey_code": jockey_cd,
        "trainer_code": trainer_cd,
        "finish_time": finish_t,
        "margin": margin,
        "popularity": popularity,
        "win_odds": win_odds,
        "horse_weight": horse_wt,
        "horse_weight_diff": horse_diff,
    }


# ── JG: 出馬投票（暫定出馬表）──────────────────────────────────
# JGレコードは RA/SE と異なる独自レイアウト (実データで確認済み):
#   [0:2]  = "JG"
#   [2:3]  = データ区分 "1"=正常 "2"=取消 等 (RA/SEにはないフィールド)
#   [3:11] = データ作成年月日 YYYYMMDD
#   [11:19] = 開催年月日 YYYYMMDD (kaisai_date = レース日)
#   [19:21] = 場コード (JYO)
#   [21:23] = 開催回 (KAI, 2バイト・ゼロパディング)
#   [23:25] = 開催日次 (NICHI, 2バイト)
#   [25:27] = レース番号 (RACE_NO, 2バイト; "00"は未確定→スキップ)
#   [27:37] = 血統登録番号 (blood_id, 10バイト)
#   [37:]   = 馬名 (SJIS) 以降
_JG_KAISAI_DT = slice(11, 19)
_JG_JYO = slice(19, 21)
_JG_KAI = slice(21, 23)
_JG_NICHI = slice(23, 25)
_JG_RACE_NO = slice(25, 27)
_JG_BLOOD_ID = slice(27, 37)


def _make_race_id_jg(raw: bytes) -> str:
    """JGレコード専用の race_id 生成 (YEAR+JYO+KAI+NICHI+RACE_NO)。

    Args:
        raw: JGレコードの生バイト列。

    Returns:
        12桁の race_id 文字列。
    """
    d8 = _str(raw, _JG_KAISAI_DT)  # YYYYMMDD
    year = d8[:4]
    jyo = _str(raw, _JG_JYO)
    kai = _str(raw, _JG_KAI)
    nichi = _str(raw, _JG_NICHI)
    race_no = _str(raw, _JG_RACE_NO)
    return f"{year}{jyo}{kai}{nichi}{race_no}"


def _parse_jg(raw: bytes) -> Optional[dict]:
    """JG 出馬投票レコード → races(placeholder)用 dict を返す。

    JGはレース番号の確定前(race_no="00")レコードが存在するため、
    race_no が"00"のものはスキップする。
    horse_number(馬番)はJGに含まれないため entries は populateしない。
    """
    if len(raw) < 37:
        return None

    # データ区分 "3"=削除はスキップ
    data_type = _str(raw, slice(2, 3))
    if data_type == "3":
        return None

    kaisai_d8 = _str(raw, _JG_KAISAI_DT)
    if len(kaisai_d8) != 8 or not kaisai_d8.isdigit():
        return None
    kaisai_dt = f"{kaisai_d8[:4]}-{kaisai_d8[4:6]}-{kaisai_d8[6:8]}"

    jyo_code = _str(raw, _JG_JYO)
    # JRA 会場コードのみ (01-10); NAR は 11以上
    try:
        if not (1 <= int(jyo_code) <= 10):
            return None
    except ValueError:
        return None

    race_no_str = _str(raw, _JG_RACE_NO)
    if race_no_str == "00" or not race_no_str.isdigit():
        return None

    race_id = _make_race_id_jg(raw)
    if not race_id or "0" * 12 == race_id:
        return None

    venue = _JYO_NAMES.get(jyo_code, jyo_code)
    race_no = int(race_no_str)
    blood_id: str | None = _str(raw, _JG_BLOOD_ID).strip()
    if not blood_id or not blood_id.isdigit() or blood_id.lstrip("0") == "":
        blood_id = None

    return {
        "_record_type": "JG",
        "race_id": race_id,
        "date": kaisai_dt,
        "venue": venue,
        "race_number": race_no,
        "blood_id": blood_id,
    }


# ── W*: 払戻レコード ───────────────────────────────────────────


def _parse_payout(raw: bytes, rec_type: str) -> Optional[dict]:
    """
    払戻レコード (HR/WH/WF/WE/WQ/WM/WT/WS) をパースして
    race_payouts テーブル用リストを含む dict を返す。
    """
    race_id = _make_race_id(raw)
    specs = _PAYOUT_SPECS.get(rec_type, [])
    payouts: list[dict] = []

    offset = 27  # レースキー直後からデータ開始 (type2+cat1+date8+kaisai8+JYO2+KAI2+NICHI2+RACE_NO2=27)
    for bet_type, max_entries, combo_bytes, pop_bytes, chunk_size in specs:
        entry_len = combo_bytes + _PAYOUT_AMOUNT_BYTES + pop_bytes
        for _ in range(max_entries):
            if offset + entry_len > len(raw):
                break
            combo_raw = raw[offset : offset + combo_bytes]
            raw[offset + combo_bytes : offset + combo_bytes + _PAYOUT_AMOUNT_BYTES]
            raw[offset + combo_bytes + _PAYOUT_AMOUNT_BYTES : offset + entry_len]

            combo = _format_combo(combo_raw, combo_bytes, chunk_size)
            amount = _int(
                raw,
                slice(
                    offset + combo_bytes, offset + combo_bytes + _PAYOUT_AMOUNT_BYTES
                ),
            )
            pop = (
                _int(
                    raw,
                    slice(
                        offset + combo_bytes + _PAYOUT_AMOUNT_BYTES, offset + entry_len
                    ),
                )
                or None
            )

            if combo and amount >= 100:  # ¥100未満は無効エントリ (JRA最小払戻=¥100)
                payouts.append(
                    {
                        "bet_type": bet_type,
                        "combination": combo,
                        "payout": amount,
                        "popularity": pop,
                    }
                )

            offset += entry_len

    if not payouts:
        return None

    return {
        "_record_type": rec_type,
        "race_id": race_id,
        "payouts": payouts,
    }


# ── WC: 調教タイム（実レコードタイプ）────────────────────────────


def _parse_wc(raw: bytes) -> Optional[dict]:
    """WC 調教タイムレコードをパースして training_times テーブル用 dict を返す。

    実データ確認済みオフセット使用。タイムは ×0.01秒単位（推定）。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        training_times テーブル用フィールドを含む dict。
        horse_id 未存在・日付不正は None。
    """
    if len(raw) < 64:
        return None

    horse_id = _str(raw, _WC_HORSE_ID)
    if not horse_id or horse_id == "0000000000":
        return None

    training_dt = _str(raw, _WC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8
        else ""
    )
    if not training_date:
        return None

    return {
        "_record_type": "TC",  # training_times テーブルに保存
        "horse_id": horse_id,
        "horse_name": "",
        "training_date": training_date,
        "venue_code": _str(raw, _WC_JYO),
        "course_type": _str(raw, _WC_COURSE_CD),
        "time_4f": _float(raw, _WC_TIME_4F, 100.0),
        "time_3f": _float(raw, _WC_TIME_3F, 100.0),
        "time_2f": _float(raw, _WC_TIME_2F, 100.0),
        "time_1f": _float(raw, _WC_TIME_1F, 100.0),
        "lap_time": _float(raw, _WC_LAP_TIME, 100.0),
        "gear": "",
        "jockey_code": "",
        "jockey_name": "",
        "data_date": _str(raw, _WC_DATA_DATE),
    }


# ── WH: 坂路調教（実レコードタイプ）────────────────────────────


def _parse_wh(raw: bytes) -> Optional[dict]:
    """WH 坂路調教レコードをパースして training_hillwork テーブル用 dict を返す。

    WH レコードは WC と同様のヘッダー構造と推定。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        training_hillwork テーブル用フィールドを含む dict。
        horse_id 未存在・日付不正は None。
    """
    if len(raw) < 64:
        return None

    horse_id = _str(raw, _WH_HORSE_ID)
    if not horse_id or horse_id == "0000000000":
        return None

    training_dt = _str(raw, _WH_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8
        else ""
    )
    if not training_date:
        return None

    return {
        "_record_type": "HC",  # training_hillwork テーブルに保存
        "horse_id": horse_id,
        "horse_name": "",
        "training_date": training_date,
        "time_4f": _float(raw, _WH_TIME_4F, 100.0),
        "time_3f": _float(raw, _WH_TIME_3F, 100.0),
        "time_2f": _float(raw, _WH_TIME_2F, 100.0),
        "time_1f": _float(raw, _WH_TIME_1F, 100.0),
        "lap_time": _float(raw, _WH_LAP_TIME, 100.0),
        "gear": "",
        "jockey_code": "",
        "jockey_name": "",
        "data_date": _str(raw, _WH_DATA_DATE),
    }


# ── TC/HC: 旧レコードタイプ（現在の JVLink では発生しないが後方互換）──


def _parse_tc(raw: bytes) -> Optional[dict]:
    """TC 調教タイムレコード（旧形式）をパースする。

    Args:
        raw: JVRead で取得した生バイト列（旧 TC 形式）。

    Returns:
        training_times テーブル用フィールドを含む dict。
        horse_id 未存在は None。
    """
    if len(raw) < 90:
        return None
    horse_id = _str(raw, _TC_HORSE_ID)
    if not horse_id:
        return None
    training_dt = _str(raw, _TC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8
        else ""
    )
    return {
        "_record_type": "TC",
        "horse_id": horse_id,
        "horse_name": "",
        "training_date": training_date,
        "venue_code": "",
        "course_type": _str(raw, _TC_COURSE_TYPE),
        "time_4f": _float(raw, _TC_TIME_4F, 10.0),
        "time_3f": _float(raw, _TC_TIME_3F, 10.0),
        "time_2f": _float(raw, _TC_TIME_2F, 10.0),
        "time_1f": _float(raw, _TC_TIME_1F, 10.0),
        "lap_time": _float(raw, _TC_LAP_TIME, 10.0),
        "gear": _GEAR_CODES.get(_str(raw, _TC_GEAR), ""),
        "jockey_code": "",
        "jockey_name": "",
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── HC: 坂路調教（旧レコードタイプ、後方互換）──────────────────


def _parse_hc(raw: bytes) -> Optional[dict]:
    """HC 坂路調教レコード（旧形式）をパースする。

    Args:
        raw: JVRead で取得した生バイト列（旧 HC 形式）。

    Returns:
        training_hillwork テーブル用フィールドを含む dict。
        horse_id 未存在は None。
    """
    if len(raw) < 90:
        return None
    horse_id = _str(raw, _HC_HORSE_ID)
    if not horse_id:
        return None
    training_dt = _str(raw, _HC_TRAINING_DT)
    training_date = (
        f"{training_dt[:4]}-{training_dt[4:6]}-{training_dt[6:8]}"
        if len(training_dt) == 8
        else ""
    )
    return {
        "_record_type": "HC",
        "horse_id": horse_id,
        "horse_name": "",
        "training_date": training_date,
        "time_4f": _float(raw, _HC_TIME_4F, 10.0),
        "time_3f": _float(raw, _HC_TIME_3F, 10.0),
        "time_2f": _float(raw, _HC_TIME_2F, 10.0),
        "time_1f": _float(raw, _HC_TIME_1F, 10.0),
        "lap_time": _float(raw, _HC_LAP_TIME, 10.0),
        "gear": _GEAR_CODES.get(_str(raw, _HC_GEAR), ""),
        "jockey_code": "",
        "jockey_name": "",
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── BT: 繁殖馬マスタ ──────────────────────────────────────────


def _parse_bt(raw: bytes) -> Optional[dict]:
    """BT 繁殖馬マスタをパースして breeding_horses テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        breeding_horses テーブル用フィールドを含む dict。
        horse_id 未存在は None。
    """
    if len(raw) < 20:
        return None
    horse_id = _str(raw, _BT_HORSE_ID)
    if not horse_id:
        return None
    return {
        "_record_type": "BT",
        "horse_id": horse_id,
        "horse_name": _sjis(raw, _BT_HORSE_NM),
        "horse_name_kana": _sjis(raw, _BT_HORSE_KANA),
        "country": _COUNTRY_CODES.get(_str(raw, _BT_COUNTRY), _str(raw, _BT_COUNTRY)),
        "sex": _SEX_CODES.get(_str(raw, _BT_SEX), ""),
        "birth_year": _safe_int_val(_str(raw, _BT_BIRTH_YEAR)) or None,
        "birth_month": _safe_int_val(_str(raw, _BT_BIRTH_MONTH)) or None,
        "coat_color": _COAT_CODES.get(_str(raw, _BT_COAT), ""),
        "father_id": _str(raw, _BT_FATHER_ID),
        "father_name": _sjis(raw, _BT_FATHER_NM),
        "mother_id": _str(raw, _BT_MOTHER_ID),
        "mother_name": _sjis(raw, _BT_MOTHER_NM),
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── HN: 産駒マスタ ────────────────────────────────────────────


def _parse_hn(raw: bytes) -> Optional[dict]:
    """HN 産駒マスタをパースして foals テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        foals テーブル用フィールドを含む dict。horse_id 未存在は None。
    """
    if len(raw) < 20:
        return None
    horse_id = _str(raw, _HN_HORSE_ID)
    if not horse_id:
        return None
    return {
        "_record_type": "HN",
        "horse_id": horse_id,
        "horse_name": _sjis(raw, _HN_HORSE_NM),
        "horse_name_kana": _sjis(raw, _HN_HORSE_KANA),
        "country": _COUNTRY_CODES.get(_str(raw, _HN_COUNTRY), _str(raw, _HN_COUNTRY)),
        "sex": _SEX_CODES.get(_str(raw, _HN_SEX), ""),
        "birth_year": _safe_int_val(_str(raw, _HN_BIRTH_YEAR)) or None,
        "birth_month": _safe_int_val(_str(raw, _HN_BIRTH_MONTH)) or None,
        "coat_color": _COAT_CODES.get(_str(raw, _HN_COAT), ""),
        "father_id": _str(raw, _HN_FATHER_ID),
        "mother_id": _str(raw, _HN_MOTHER_ID),
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── UM: 競走馬マスタ ──────────────────────────────────────────


def _parse_um(raw: bytes) -> Optional[dict]:
    """UM 競走馬マスタをパースして racehorses テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        racehorses テーブル用フィールドを含む dict。horse_id 未存在は None。
    """
    # 馬名漢字 [46:82] まで読むため最低 82 バイトを要求（旧 20 は短すぎた）。
    if len(raw) < 82:
        return None
    horse_id = _str(raw, _UM_HORSE_ID)
    # 血統登録番号は 10 桁数字。ゼロ埋め/空は無効。
    if not horse_id or not horse_id.isdigit() or horse_id.lstrip("0") == "":
        return None

    birth_raw = _str(raw, _UM_BIRTH_DATE)  # "YYYYMMDD"
    birth_date = ""
    if len(birth_raw) == 8 and birth_raw.isdigit() and birth_raw != "00000000":
        birth_date = f"{birth_raw[0:4]}/{birth_raw[4:6]}/{birth_raw[6:8]}"

    return {
        "_record_type": "UM",
        "horse_id": horse_id,
        "horse_name": _sjis_name(raw, _UM_HORSE_NM),
        "horse_name_kana": _sjis(raw, _UM_HORSE_KANA),
        # 生産国は欧字馬名末尾の "(JPN)" 等から抽出（専用コード列が未確定のため）。
        "country": _extract_country_from_en(_sjis(raw, _UM_HORSE_EN)),
        "sex": _SEX_CODES.get(_str(raw, _UM_SEX), ""),
        "birth_year": _safe_int_val(_str(raw, _UM_BIRTH_YEAR)) or None,
        "birth_month": _safe_int_val(_str(raw, _UM_BIRTH_MONTH)) or None,
        "birth_date": birth_date,
        "coat_color": _COAT_CODES.get(_str(raw, _UM_COAT), ""),
        "father_id": _str(raw, _UM_FATHER_ID),
        "father_name": _sjis_name(raw, _UM_FATHER_NM),
        "mother_id": _str(raw, _UM_MOTHER_ID),
        "mother_name": _sjis_name(raw, _UM_MOTHER_NM),
        "grandsire_id": _str(raw, _UM_GRANDSIRE_ID),
        "grandsire_name": _sjis_name(raw, _UM_GRANDSIRE_NM),
        # 調教師/馬主/東西所属は post-pedigree 領域の精密確定が未了（W-074 残課題）。
        "trainer_code": "",
        "trainer_name": "",
        "owner_code": "",
        "owner_name": "",
        "east_west": "",
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── KS: 騎手マスタ ────────────────────────────────────────────


def _parse_ks(raw: bytes) -> Optional[dict]:
    """KS 騎手マスタをパースして jockeys テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        jockeys テーブル用フィールドを含む dict。jockey_code 未存在は None。
    """
    # 騎手名漢字 [41:75] まで読むため最低 75 バイトを要求。
    if len(raw) < 75:
        return None
    jockey_code = _str(raw, _KS_CODE)
    if not jockey_code:
        return None

    by = _safe_int_val(_str(raw, _KS_BIRTH_YEAR))
    bm = _safe_int_val(_str(raw, _KS_BIRTH_MONTH))
    bd = _safe_int_val(_str(raw, _KS_BIRTH_DAY))
    birth_date = f"{by:04d}/{bm:02d}/{bd:02d}" if by and bm and bd else ""
    # 騎手名漢字は姓名間に全角空白を含む（"武　豊"）。race_results.jockey は
    # SE 8バイト名（"武豊"・空白無し）のため、結合キーに合わせて全角空白を除去。
    jockey_name = _sjis_name(raw, _KS_NAME).replace("　", "")
    return {
        "_record_type": "KS",
        "jockey_code": jockey_code,
        "jockey_name": jockey_name,
        # 半角カナ・東西所属・免許年は位置未確定のため空（W-075 残課題）。
        "jockey_name_kana": "",
        "east_west": "",
        "birth_date": birth_date,
        "license_year": None,
        "data_date": _str(raw, _H_DATA_DATE),
    }


# ── CH: 調教師マスタ ──────────────────────────────────────────


def _parse_ch(raw: bytes) -> Optional[dict]:
    """CH 調教師マスタをパースして trainers テーブル用 dict を返す。

    Args:
        raw: JVRead で取得した生バイト列。

    Returns:
        trainers テーブル用フィールドを含む dict。trainer_code 未存在は None。
    """
    # 調教師名漢字 [41:75] まで読むため最低 75 バイトを要求。
    if len(raw) < 75:
        return None
    trainer_code = _str(raw, _CH_CODE)
    if not trainer_code:
        return None

    by = _safe_int_val(_str(raw, _CH_BIRTH_YEAR))
    bm = _safe_int_val(_str(raw, _CH_BIRTH_MONTH))
    bd = _safe_int_val(_str(raw, _CH_BIRTH_DAY))
    birth_date = f"{by:04d}/{bm:02d}/{bd:02d}" if by and bm and bd else ""
    # 調教師名漢字も全角空白を除去して race_results.trainer に合わせる。
    trainer_name = _sjis_name(raw, _CH_NAME).replace("　", "")
    return {
        "_record_type": "CH",
        "trainer_code": trainer_code,
        "trainer_name": trainer_name,
        # 半角カナ・東西所属・免許年・厩舎名は位置未確定のため空（W-075 残課題）。
        "trainer_name_kana": "",
        "east_west": "",
        "birth_date": birth_date,
        "license_year": None,
        "stable_name": "",
        "data_date": _str(raw, _H_DATA_DATE),
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
    stats = {
        "ra": 0,
        "jg": 0,
        "se": 0,
        "payout": 0,
        "tc": 0,
        "hc": 0,
        "bt": 0,
        "hn": 0,
        "um": 0,
        "ks": 0,
        "ch": 0,
        "skipped": 0,
    }

    # RA/JG(races作成) → SE → その他 の順に保存して FK 制約を確実に満たす
    _ORDER = {"RA": 0, "JG": 0, "SE": 1}
    records = sorted(records, key=lambda r: _ORDER.get(r.get("_record_type", ""), 9))

    for rec in records:
        rt = rec.get("_record_type", "")
        try:
            if rt == "RA":
                _save_ra(conn, rec)
                stats["ra"] += 1
            elif rt == "JG":
                _save_jg(conn, rec)
                stats["jg"] += 1
            elif rt == "SE":
                _save_se(conn, rec)
                stats["se"] += 1
            elif rt in _PAYOUT_SPECS:
                _save_payout(conn, rec)
                stats["payout"] += len(rec.get("payouts", []))
            elif rt == "TC":
                _save_tc(conn, rec)
                stats["tc"] += 1
            elif rt == "HC":
                _save_hc(conn, rec)
                stats["hc"] += 1
            elif rt == "BT":
                _save_bt(conn, rec)
                stats["bt"] += 1
            elif rt == "HN":
                _save_hn(conn, rec)
                stats["hn"] += 1
            elif rt == "UM":
                _save_um(conn, rec)
                stats["um"] += 1
            elif rt == "KS":
                _save_ks(conn, rec)
                stats["ks"] += 1
            elif rt == "CH":
                _save_ch(conn, rec)
                stats["ch"] += 1
            else:
                stats["skipped"] += 1
        except sqlite3.IntegrityError as e:
            if "FOREIGN KEY" in str(e):
                # 親レコード(races)が存在しない場合は静かにスキップ
                logger.debug("FK スキップ %s race_id=%s", rt, rec.get("race_id", "?"))
            else:
                logger.warning(
                    "保存失敗(整合性) %s race_id=%s: %s", rt, rec.get("race_id", "?"), e
                )
            stats["skipped"] += 1
        except Exception as e:
            logger.warning("保存失敗 %s race_id=%s: %s", rt, rec.get("race_id", "?"), e)
            stats["skipped"] += 1

    return stats


def _save_ra(conn: sqlite3.Connection, r: dict) -> None:
    """RA パース結果を races テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_ra() が返した dict。
    """
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["race_id"],
                r["race_name"],
                r["date"],
                r["venue"],
                r["race_number"],
                r["distance"],
                r["surface"],
                r["track_direction"],
                r["weather"],
                r["condition"],
            ),
        )


def _save_se(conn: sqlite3.Connection, r: dict) -> None:
    """SE パース結果を race_results / horses / entries テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_se() が返した dict。
    """
    with conn:
        # horses テーブル (horse_id がある場合のみ)
        if r.get("horse_id"):
            conn.execute(
                """
                INSERT INTO horses (horse_id, horse_name)
                VALUES (?, ?)
                ON CONFLICT(horse_id) DO UPDATE SET
                    horse_name = excluded.horse_name,
                    updated_at = datetime('now', 'localtime')
                """,
                (r["horse_id"], r["horse_name"]),
            )

        # blood_id: JRA-VAN の血統登録番号（SE レコードの horse_id フィールド）
        # training_times.horse_id と同形式のため、調教データとの JOIN キーに使う
        blood_id = (
            r.get("horse_id")
            if r.get("horse_id") and r.get("horse_id", "").strip("0")
            else None
        )
        horse_number = r.get("horse_number")

        # 【2段 UPSERT】
        # Step1: horse_number で既存行を探してUPDATE（cat='2'→cat='1'の上書きに対応）
        #        rank は確定データ(cat='1')で常に上書き。COALESCE を使わない。
        updated = 0
        if horse_number and horse_number > 0:
            updated = conn.execute(
                """
                UPDATE race_results SET
                    horse_id          = COALESCE(?, horse_id),
                    blood_id          = COALESCE(?, blood_id),
                    gate_number       = ?,
                    rank              = CASE WHEN ? IS NOT NULL THEN ? ELSE rank END,
                    sex_age           = ?,
                    weight_carried    = ?,
                    jockey            = ?,
                    trainer           = ?,
                    finish_time       = COALESCE(?, finish_time),
                    margin            = COALESCE(?, margin),
                    popularity        = COALESCE(?, popularity),
                    win_odds          = CASE WHEN ? > 0 THEN ? ELSE win_odds END,
                    horse_weight      = COALESCE(?, horse_weight),
                    horse_weight_diff = COALESCE(?, horse_weight_diff),
                    jockey_code       = COALESCE(?, jockey_code),
                    trainer_code      = COALESCE(?, trainer_code)
                WHERE race_id = ? AND horse_number = ?
                """,
                (
                    r.get("horse_id"),
                    blood_id,
                    r.get("gate_number"),
                    r.get("rank"),
                    r.get("rank"),
                    r.get("sex_age", ""),
                    r.get("weight_carried", 0),
                    r.get("jockey", ""),
                    r.get("trainer", ""),
                    r.get("finish_time"),
                    r.get("margin"),
                    r.get("popularity"),
                    r.get("win_odds"),
                    r.get("win_odds"),
                    r.get("horse_weight"),
                    r.get("horse_weight_diff"),
                    r.get("jockey_code"),
                    r.get("trainer_code"),
                    r["race_id"],
                    horse_number,
                ),
            ).rowcount

        # Step2: 既存行がなければ INSERT。horse_number が確定している場合は
        # UNIQUE(race_id, horse_number) で upsert。NULL の場合は INSERT OR IGNORE。
        if updated == 0:
            _params = (
                r["race_id"],
                r.get("horse_id"),
                r["horse_name"],
                r.get("rank"),
                r.get("gate_number"),
                horse_number,
                r.get("sex_age", ""),
                r.get("weight_carried", 0),
                r.get("jockey", ""),
                r.get("trainer", ""),
                r.get("finish_time"),
                r.get("margin"),
                r.get("popularity"),
                r.get("win_odds"),
                r.get("horse_weight"),
                r.get("horse_weight_diff"),
                blood_id,
                r.get("jockey_code"),
                r.get("trainer_code"),
            )
            _insert_cols = """INSERT INTO race_results
                    (race_id, horse_id, horse_name, rank,
                     gate_number, horse_number,
                     sex_age, weight_carried, jockey, trainer,
                     finish_time, margin, popularity, win_odds,
                     horse_weight, horse_weight_diff, blood_id,
                     jockey_code, trainer_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            if horse_number is not None:
                # W-086: UNIQUE は部分インデックス idx_rr_unique_horsenum
                # (WHERE horse_number IS NOT NULL) のため、conflict target にも
                # 同一の WHERE 句が必須（無いと SQLite が
                # "ON CONFLICT clause does not match..." で保存全滅する）。
                conn.execute(
                    f"""
                    {_insert_cols}
                    ON CONFLICT(race_id, horse_number) WHERE horse_number IS NOT NULL DO UPDATE SET
                        gate_number       = excluded.gate_number,
                        horse_name        = CASE WHEN excluded.horse_name != '' THEN excluded.horse_name ELSE race_results.horse_name END,
                        rank              = CASE WHEN excluded.rank IS NOT NULL THEN excluded.rank ELSE race_results.rank END,
                        finish_time       = COALESCE(excluded.finish_time, race_results.finish_time),
                        margin            = COALESCE(excluded.margin, race_results.margin),
                        popularity        = COALESCE(excluded.popularity, race_results.popularity),
                        win_odds          = CASE WHEN excluded.win_odds > 0 THEN excluded.win_odds ELSE race_results.win_odds END,
                        horse_weight      = COALESCE(excluded.horse_weight, race_results.horse_weight),
                        horse_weight_diff = COALESCE(excluded.horse_weight_diff, race_results.horse_weight_diff),
                        blood_id          = COALESCE(excluded.blood_id, race_results.blood_id),
                        jockey_code       = COALESCE(excluded.jockey_code, race_results.jockey_code),
                        trainer_code      = COALESCE(excluded.trainer_code, race_results.trainer_code)
                    """,
                    _params,
                )
            else:
                conn.execute(f"{_insert_cols} ON CONFLICT DO NOTHING", _params)

        # entries テーブル: FeatureBuilder が出馬表データとして参照する
        # JVLink SE レコードから直接書き込むことで netkeiba スクレイピングを不要にする
        if horse_number and horse_number > 0:
            conn.execute(
                """
                INSERT INTO entries
                    (race_id, horse_number, gate_number, horse_id, horse_name,
                     sex_age, weight_carried, jockey, trainer,
                     horse_weight, horse_weight_diff, jockey_code, trainer_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_id, horse_number) DO UPDATE SET
                    gate_number       = excluded.gate_number,
                    horse_id          = COALESCE(excluded.horse_id,         entries.horse_id),
                    horse_name        = excluded.horse_name,
                    sex_age           = excluded.sex_age,
                    weight_carried    = excluded.weight_carried,
                    jockey            = excluded.jockey,
                    trainer           = excluded.trainer,
                    horse_weight      = COALESCE(excluded.horse_weight,      entries.horse_weight),
                    horse_weight_diff = COALESCE(excluded.horse_weight_diff, entries.horse_weight_diff),
                    jockey_code       = COALESCE(excluded.jockey_code,       entries.jockey_code),
                    trainer_code      = COALESCE(excluded.trainer_code,      entries.trainer_code)
                """,
                (
                    r["race_id"],
                    horse_number,
                    r.get("gate_number") or 0,
                    r.get("horse_id"),
                    r["horse_name"],
                    r.get("sex_age", ""),
                    r.get("weight_carried", 0),
                    r.get("jockey", ""),
                    r.get("trainer", ""),
                    r.get("horse_weight"),
                    r.get("horse_weight_diff"),
                    r.get("jockey_code"),
                    r.get("trainer_code"),
                ),
            )


def _save_jg(conn: sqlite3.Connection, r: dict) -> None:
    """JG → races テーブルにプレースホルダー行を作成する (INSERT OR IGNORE)。

    JGレコードには馬番・騎手・調教師が含まれないため entries は書かない。
    race_name/distance/surface は RA レコード上書き時に補完される。
    """
    with conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["race_id"],
                "",
                r["date"],
                r["venue"],
                r["race_number"],
                0,
                "",
                "",
                "",
                "",
            ),
        )


def _save_payout(conn: sqlite3.Connection, r: dict) -> None:
    """払戻パース結果を race_payouts テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_payout() が返した dict（'payouts' キーにリストを持つ）。
    """
    with conn:
        for p in r.get("payouts", []):
            conn.execute(
                """
                INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(race_id, bet_type, combination) DO UPDATE SET
                    payout     = excluded.payout,
                    popularity = excluded.popularity
                """,
                (
                    r["race_id"],
                    p["bet_type"],
                    p["combination"],
                    p["payout"],
                    p.get("popularity"),
                ),
            )


def _save_tc(conn: sqlite3.Connection, r: dict) -> None:
    """TC/WC パース結果を training_times テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_tc() または _parse_wc() が返した dict。
    """
    with conn:
        conn.execute(
            """
            INSERT INTO training_times
                (horse_id, horse_name, training_date, venue_code, course_type, direction,
                 time_4f, time_3f, time_2f, time_1f, lap_time,
                 gear, jockey_code, jockey_name, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id, training_date, course_type, direction) DO UPDATE SET
                time_4f   = excluded.time_4f,
                time_3f   = excluded.time_3f,
                time_2f   = excluded.time_2f,
                time_1f   = excluded.time_1f,
                lap_time  = excluded.lap_time,
                gear      = excluded.gear
            """,
            (
                r["horse_id"],
                r["horse_name"],
                r["training_date"],
                r.get("venue_code", ""),
                r.get("course_type", ""),
                r.get("direction", ""),
                r.get("time_4f"),
                r.get("time_3f"),
                r.get("time_2f"),
                r.get("time_1f"),
                r.get("lap_time"),
                r.get("gear", ""),
                r.get("jockey_code", ""),
                r.get("jockey_name", ""),
                r.get("data_date", ""),
            ),
        )


def _save_hc(conn: sqlite3.Connection, r: dict) -> None:
    """HC/WH パース結果を training_hillwork テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_hc() または _parse_wh() が返した dict。
    """
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
            (
                r["horse_id"],
                r["horse_name"],
                r["training_date"],
                r.get("time_4f"),
                r.get("time_3f"),
                r.get("time_2f"),
                r.get("time_1f"),
                r.get("lap_time"),
                r.get("gear", ""),
                r.get("jockey_code", ""),
                r.get("jockey_name", ""),
                r.get("data_date", ""),
            ),
        )


def _save_bt(conn: sqlite3.Connection, r: dict) -> None:
    """BT パース結果を breeding_horses テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_bt() が返した dict。
    """
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
            (
                r["horse_id"],
                r.get("horse_name", ""),
                r.get("horse_name_kana", ""),
                r.get("country", ""),
                r.get("sex", ""),
                r.get("birth_year"),
                r.get("birth_month"),
                r.get("coat_color", ""),
                r.get("father_id", ""),
                r.get("father_name", ""),
                r.get("mother_id", ""),
                r.get("mother_name", ""),
                r.get("data_date", ""),
            ),
        )


def _save_hn(conn: sqlite3.Connection, r: dict) -> None:
    """HN パース結果を foals テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_hn() が返した dict。
    """
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
            (
                r["horse_id"],
                r.get("horse_name", ""),
                r.get("horse_name_kana", ""),
                r.get("country", ""),
                r.get("sex", ""),
                r.get("birth_year"),
                r.get("birth_month"),
                r.get("coat_color", ""),
                r.get("father_id", ""),
                r.get("mother_id", ""),
                r.get("data_date", ""),
            ),
        )


def _save_um(conn: sqlite3.Connection, r: dict) -> None:
    """UM パース結果を racehorses テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_um() が返した dict。
    """
    with conn:
        conn.execute(
            """
            INSERT INTO racehorses
                (horse_id, horse_name, horse_name_kana, country, sex,
                 birth_year, birth_month, birth_date, coat_color,
                 father_id, father_name, mother_id, mother_name,
                 grandsire_id, grandsire_name,
                 trainer_code, trainer_name,
                 owner_code, owner_name, east_west, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name      = excluded.horse_name,
                horse_name_kana = excluded.horse_name_kana,
                country         = excluded.country,
                sex             = excluded.sex,
                birth_year      = excluded.birth_year,
                birth_month     = excluded.birth_month,
                birth_date      = excluded.birth_date,
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
            (
                r["horse_id"],
                r.get("horse_name", ""),
                r.get("horse_name_kana", ""),
                r.get("country", ""),
                r.get("sex", ""),
                r.get("birth_year"),
                r.get("birth_month"),
                r.get("birth_date", ""),
                r.get("coat_color", ""),
                r.get("father_id", ""),
                r.get("father_name", ""),
                r.get("mother_id", ""),
                r.get("mother_name", ""),
                r.get("grandsire_id", ""),
                r.get("grandsire_name", ""),
                r.get("trainer_code", ""),
                r.get("trainer_name", ""),
                r.get("owner_code", ""),
                r.get("owner_name", ""),
                r.get("east_west", ""),
                r.get("data_date", ""),
            ),
        )


def _save_ks(conn: sqlite3.Connection, r: dict) -> None:
    """KS パース結果を jockeys テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_ks() が返した dict。
    """
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
            (
                r["jockey_code"],
                r.get("jockey_name", ""),
                r.get("jockey_name_kana", ""),
                r.get("east_west", ""),
                r.get("birth_date", ""),
                r.get("license_year"),
                r.get("data_date", ""),
            ),
        )


def _save_ch(conn: sqlite3.Connection, r: dict) -> None:
    """CH パース結果を trainers テーブルに UPSERT する。

    Args:
        conn: SQLite コネクション。
        r:    _parse_ch() が返した dict。
    """
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
            (
                r["trainer_code"],
                r.get("trainer_name", ""),
                r.get("trainer_name_kana", ""),
                r.get("east_west", ""),
                r.get("birth_date", ""),
                r.get("license_year"),
                r.get("stable_name", ""),
                r.get("data_date", ""),
            ),
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
        birth_date      TEXT    NOT NULL DEFAULT '',
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
        # 既存 racehorses への birth_date 列追加（W-074 / 2026-06-07）。
        # CREATE TABLE IF NOT EXISTS は既存テーブルに列を足さないため明示 ALTER。
        cols = {row[1] for row in conn.execute("PRAGMA table_info(racehorses)")}
        if "birth_date" not in cols:
            conn.execute(
                "ALTER TABLE racehorses ADD COLUMN birth_date TEXT NOT NULL DEFAULT ''"
            )
            logger.info("racehorses.birth_date 列を追加しました (W-074)")
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
        self._sid = sid
        self._db_path = db_path
        self._debug = debug

    def _get_conn(self) -> sqlite3.Connection:
        """DB コネクションを初期化してスキーマを拡張して返す。

        Returns:
            スキーマ拡張済みの SQLite コネクション。
        """
        from src.database.init_db import init_db

        conn = init_db(self._db_path)
        extend_db_schema(conn)
        return conn

    # JVREAD_DOWNLOADING が続く最大秒数（この時間を超えたらセッション再起動）
    _MAX_DOWNLOAD_WAIT_SEC: int = 300
    # セッション再起動の最大リトライ回数
    _MAX_RETRIES: int = 3

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
            保存件数統計 dict。``open_code`` キーで JVOpen の戻り値を確認できる。
            open_code < 0 の場合はデータなし/-303 等でスキップ済み。
        """
        import time

        _BATCH_SIZE = 500

        conn = self._get_conn()

        _EMPTY_STATS: dict[str, int] = {
            "ra": 0,
            "jg": 0,
            "se": 0,
            "payout": 0,
            "tc": 0,
            "hc": 0,
            "bt": 0,
            "hn": 0,
            "um": 0,
            "ks": 0,
            "ch": 0,
            "skipped": 0,
        }

        try:
            for attempt in range(self._MAX_RETRIES):
                batch: list[dict] = []
                stats: dict[str, int] = dict(_EMPTY_STATS)
                read_count = 0
                open_code = -1

                try:
                    with JVLinkClient(self._sid) as client:
                        open_code = client.open(dataspec, fromtime, option)
                        if open_code < 0:
                            logger.info(
                                "JVOpen %s fromtime=%s option=%d → code=%d のためスキップ",
                                dataspec,
                                fromtime,
                                option,
                                open_code,
                            )
                            return {
                                **_EMPTY_STATS,
                                "total_read": 0,
                                "open_code": open_code,
                            }

                        download_wait_sec = 0

                        while True:
                            code, data = client.read_record()

                            if code == JVREAD_EOF:
                                break

                            if code == JVREAD_FILECHANGE:
                                download_wait_sec = 0
                                continue

                            if code == JVREAD_DOWNLOADING:
                                download_wait_sec += 1
                                if download_wait_sec >= self._MAX_DOWNLOAD_WAIT_SEC:
                                    raise TimeoutError(
                                        f"JVLink ダウンロード待機 {download_wait_sec}s 超過 "
                                        f"(dataspec={dataspec})"
                                    )
                                logger.debug(
                                    "ダウンロード待機中 (code=-3) … %ds/%ds",
                                    download_wait_sec,
                                    self._MAX_DOWNLOAD_WAIT_SEC,
                                )
                                time.sleep(1)
                                continue

                            download_wait_sec = 0

                            if code < 0:
                                raise RuntimeError(f"JVRead エラー: code={code}")

                            if data:
                                rec = parse_record(data, debug=self._debug)
                                if rec:
                                    batch.append(rec)
                            read_count += 1

                            if len(batch) >= _BATCH_SIZE:
                                partial = save_records_to_db(batch, conn)
                                for k in stats:
                                    stats[k] += partial.get(k, 0)
                                batch = []
                                logger.info(
                                    "バッチ保存完了: 累計 %d レコード読み込み済み",
                                    read_count,
                                )

                        if batch:
                            partial = save_records_to_db(batch, conn)
                            for k in stats:
                                stats[k] += partial.get(k, 0)

                    # with 正常終了 → リトライ不要
                    break

                except (TimeoutError, RuntimeError) as e:
                    logger.warning(
                        "JVLink エラー (attempt %d/%d): %s",
                        attempt + 1,
                        self._MAX_RETRIES,
                        e,
                    )
                    if attempt < self._MAX_RETRIES - 1:
                        logger.info(
                            "JVLink セッションを再起動して再試行します … 10秒待機"
                        )
                        time.sleep(10)
                    else:
                        logger.error("JVLink リトライ上限到達。処理を中断します: %s", e)
                        raise

        finally:
            conn.close()

        stats["total_read"] = read_count
        stats["open_code"] = open_code
        logger.info(
            "JV-Data 取得完了: read=%d "
            "RA=%d JG=%d SE=%d payout=%d TC=%d HC=%d "
            "BT=%d HN=%d UM=%d KS=%d CH=%d skip=%d",
            read_count,
            stats["ra"],
            stats["jg"],
            stats["se"],
            stats["payout"],
            stats["tc"],
            stats["hc"],
            stats["bt"],
            stats["hn"],
            stats["um"],
            stats["ks"],
            stats["ch"],
            stats["skipped"],
        )
        return stats

    def load_race(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """レース系データ (RA/SE/払戻) を取得・保存する。

        Args:
            fromtime: 開始日時 "YYYYMMDD" または "YYYYMMDDhhmmss"。
            option:   OPT_NORMAL / OPT_SETUP / OPT_TODAY / OPT_STORED。

        Returns:
            保存件数統計 dict（load() の戻り値と同形式）。
        """
        return self.load(DATASPEC_RACE, fromtime, option)

    def load_training(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """調教データ (TC/HC) を取得・保存する。

        Args:
            fromtime: 開始日時 "YYYYMMDD" または "YYYYMMDDhhmmss"。
            option:   OPT_NORMAL / OPT_SETUP / OPT_TODAY / OPT_STORED。

        Returns:
            保存件数統計 dict（load() の戻り値と同形式）。
        """
        return self.load(DATASPEC_WOOD, fromtime, option)

    def load_blod(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """血統データ (BT/HN) を取得・保存する。

        Args:
            fromtime: 開始日時 "YYYYMMDD" または "YYYYMMDDhhmmss"。
            option:   OPT_NORMAL / OPT_SETUP / OPT_TODAY / OPT_STORED。

        Returns:
            保存件数統計 dict（load() の戻り値と同形式）。
        """
        return self.load(DATASPEC_BLOD, fromtime, option)

    def load_difn(self, fromtime: str, option: int = OPT_NORMAL) -> dict:
        """マスタデータ (UM/KS/CH) を取得・保存する。

        Args:
            fromtime: 開始日時 "YYYYMMDD" または "YYYYMMDDhhmmss"。
            option:   OPT_NORMAL / OPT_SETUP / OPT_TODAY / OPT_STORED。

        Returns:
            保存件数統計 dict（load() の戻り値と同形式）。
        """
        return self.load(DATASPEC_DIFN, fromtime, option)


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────


def main() -> None:
    """CLI エントリポイント。引数を解析して JVDataLoader を実行する。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
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
        args.dataspec,
        args.fromtime,
        args.option,
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


def _parse_args() -> argparse.Namespace:
    """CLI 引数をパースして Namespace を返す。

    Returns:
        解析済みの argparse.Namespace。
    """
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
        "--sid",
        default="UMALOGI00",
        help="JRA-VAN ソフトウェアID (デフォルト: UMALOGI00)",
    )
    parser.add_argument(
        "--fromtime",
        default="20240101",
        metavar="YYYYMMDD",
        help="読み込み開始日 (デフォルト: 20240101)",
    )
    parser.add_argument(
        "--dataspec",
        choices=["RACE", "WOOD", "SNAP", "BLOD", "DIFN", "SETUP"],
        default="RACE",
        help=(
            "データ種別 (デフォルト: RACE)。"
            "RACE=レース系, WOOD=調教, BLOD=血統差分, DIFN=マスタ差分, "
            "SETUP=マスタ一括初期取得 (--option 2 と併用)"
        ),
    )
    parser.add_argument(
        "--option",
        type=int,
        choices=[1, 2, 3, 4],
        default=1,
        help="取得オプション: 1=通常 2=セットアップ 3=今日 4=蓄積 (デフォルト: 1)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="生レコードの先頭80バイトをダンプする (バイトオフセット確認用)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
