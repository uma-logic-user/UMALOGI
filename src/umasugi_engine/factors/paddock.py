"""
パドック気配スコア — AIウマスギ拡張因子 (Phase3)

paddock_notes テーブルから当該レース・馬番のコメントを取得し、
キーワード辞書でポジティブ/ネガティブを判定して boost_factor を返す。

Discord /paddock コマンド経由でコメントが保存された場合のみ有効。
データなし = 0.5（中立）。

出力列: paddock_score (0.0〜1.0, 0.5 = 中立)
"""

from __future__ import annotations

import logging
import re
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT = 0.5

# ポジティブキーワード → +0.05
_POSITIVE_KEYWORDS: list[str] = [
    "気配良",
    "スムーズ",
    "落ち着き",
    "集中",
    "輝き",
    "汗なし",
    "デキ良",
    "好気配",
    "毛艶",
    "覇気",
    "上昇",
    "力強",
    "リラックス",
    "余裕",
]

# ネガティブキーワード → -0.05
_NEGATIVE_KEYWORDS: list[str] = [
    "イライラ",
    "発汗",
    "かかり",
    "ぼんやり",
    "元気なし",
    "煩い",
    "行きたがり",
    "チャカつ",
    "落ち着かない",
    "消耗",
    "気難し",
    "興奮",
    "暴れ",
    "汗だく",
    "ガリガリ",
    "疲れ",
]

_RE_POSITIVE = re.compile("|".join(re.escape(k) for k in _POSITIVE_KEYWORDS))
_RE_NEGATIVE = re.compile("|".join(re.escape(k) for k in _NEGATIVE_KEYWORDS))


def analyze_comment(comment: str) -> float:
    """コメントをキーワード解析して boost_factor を返す。

    ポジティブキーワードとネガティブキーワードを照合し、
    一方のみ含む場合にブースト値を返す。両方含む場合と
    どちらも含まない場合は中立（0.0）を返す。

    Args:
        comment: パドックコメント文字列。

    Returns:
        ブースト値（-0.05〜+0.05）。複数キーワードは累積しない。
    """
    pos = len(_RE_POSITIVE.findall(comment)) > 0
    neg = len(_RE_NEGATIVE.findall(comment)) > 0

    if pos and not neg:
        return 0.05
    if neg and not pos:
        return -0.05
    # 混在または両方なし → 中立
    return 0.0


def calc_paddock_score(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """パドック気配スコアを DataFrame に追加して返す。

    paddock_notes テーブルから boost_factor を取得し、
    馬番一致 → レース全体メモの優先順でスコアを割り当てる。
    複数メモがある場合は平均値を使用する。

    Args:
        df: 必須列として race_id, horse_number を含む DataFrame。
        conn: umalogi.db 接続。

    Returns:
        元 df に paddock_score 列（0.0〜1.0、データなし = 0.5）を
        追加した DataFrame。
    """
    if df.empty:
        df["paddock_score"] = pd.Series(dtype=float)
        return df

    df = df.copy()

    race_ids = df["race_id"].unique().tolist()
    ph = ",".join("?" * len(race_ids))

    rows = conn.execute(
        f"""
        SELECT race_id, horse_number, boost_factor
        FROM paddock_notes
        WHERE race_id IN ({ph})
        """,
        race_ids,
    ).fetchall()

    if not rows:
        df["paddock_score"] = _DEFAULT
        return df

    # (race_id, horse_number) → boost の平均（複数メモがある場合は平均）
    boost_map: dict[tuple[str, int | None], list[float]] = {}
    for rid, hn, bf in rows:
        key: tuple[str, int | None] = (str(rid), int(hn) if hn is not None else None)
        boost_map.setdefault(key, []).append(float(bf))

    def _get_score(row: pd.Series) -> float:
        rid = str(row["race_id"])
        hn = int(row["horse_number"]) if pd.notna(row.get("horse_number")) else None
        # 馬番一致のメモを優先、なければレース全体メモ（horse_number=NULL）
        boosts = boost_map.get((rid, hn)) or boost_map.get((rid, None)) or []
        if not boosts:
            return _DEFAULT
        avg_boost = sum(boosts) / len(boosts)
        return round(min(1.0, max(0.0, _DEFAULT + avg_boost)), 4)

    df["paddock_score"] = df.apply(_get_score, axis=1)
    return df


def save_paddock_note(
    conn: sqlite3.Connection,
    race_id: str,
    horse_number: int | None,
    comment: str,
    source: str = "discord",
) -> float:
    """パドックコメントを paddock_notes テーブルに保存する。

    Args:
        conn: umalogi.db 接続。
        race_id: 対象レース ID（YYYYMMDDJJRR 形式）。
        horse_number: 馬番。None の場合はレース全体メモとして保存する。
        comment: パドックコメント文字列。
        source: コメントソース識別子。デフォルトは "discord"。

    Returns:
        analyze_comment() が算出した boost_factor（-0.05〜+0.05）。
    """
    boost = analyze_comment(comment)
    conn.execute(
        """
        INSERT INTO paddock_notes (race_id, horse_number, comment, boost_factor, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        (race_id, horse_number, comment, boost, source),
    )
    conn.commit()
    logger.info(
        "paddock_note 保存: race_id=%s horse=%s boost=%.2f comment=%r",
        race_id,
        horse_number,
        boost,
        comment[:40],
    )
    return boost
