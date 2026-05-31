"""
直前 異常検知 — 出走取消 / 競走除外 / 騎手変更。

レース直前（発走数分前）に確定する出走取消・騎手変更を検知し、
該当レースの再推論（買い目再計算）トリガーを返す。

検知ロジック:
  - 取消/除外 : ``entries`` に存在するが、最新 ``realtime_odds`` スナップショット
                （JRA-VAN 速報 or netkeiba の直前 feed）に居ない馬。
                feed が極端に欠落しているとき（取得失敗）は誤検知防止で空集合を返す。
  - 騎手変更   : ``entries.jockey`` と直前 entry テーブル（netkeiba・W-053 の
                グローバルレート制限つき HTTP クライアント経由）の騎手を比較。

検知のみを行い、DB 反映（entries.jockey の UPDATE）と再推論の発火は
呼び出し側（today_auto_runner / check_race_anomalies）が担う。
"""

from __future__ import annotations

import logging
import sqlite3
import unicodedata
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# feed がこの頭数未満なら「取得失敗」とみなし取消検知をスキップ（誤検知防止）
_MIN_PRESENT_FOR_SCRATCH: int = 5


@dataclass
class RaceAnomalies:
    """1 レース分の異常検知結果。"""

    scratched: set[int] = field(default_factory=set)  # 取消/除外の馬番
    jockey_changes: dict[int, tuple[str, str]] = field(
        default_factory=dict
    )  # 馬番 -> (旧騎手, 新騎手)

    @property
    def has_changes(self) -> bool:
        """再推論が必要な変化があるか。"""
        return bool(self.scratched or self.jockey_changes)


def _norm_name(s: str | None) -> str:
    """騎手名比較用に正規化する（NFKC・空白除去）。"""
    if not s:
        return ""
    return unicodedata.normalize("NFKC", s).replace(" ", "").replace("　", "").strip()


def detect_scratches(
    entry_horses: set[int],
    present_horses: set[int],
    *,
    min_present: int = _MIN_PRESENT_FOR_SCRATCH,
) -> set[int]:
    """entries に居て最新ライブ feed に居ない馬（取消/除外候補）を返す。

    Args:
        entry_horses: entries テーブルの馬番集合。
        present_horses: 最新 feed（realtime_odds 最新スナップショット）の馬番集合。
        min_present: present がこの数未満なら feed 取得失敗とみなし空集合を返す。

    Returns:
        取消/除外候補の馬番集合。
    """
    if len(present_horses) < min_present:
        return set()
    return {h for h in entry_horses if h not in present_horses}


def detect_jockey_changes(
    entry_jockeys: dict[int, str],
    fresh_jockeys: dict[int, str],
) -> dict[int, tuple[str, str]]:
    """entries の騎手と直前 entry テーブルの騎手を比較して変更を返す。

    Args:
        entry_jockeys: ``{馬番: 既存騎手名}``。
        fresh_jockeys: ``{馬番: 最新騎手名}``。

    Returns:
        ``{馬番: (旧騎手, 新騎手)}``。両者とも非空かつ正規化後に不一致のときのみ。
    """
    changes: dict[int, tuple[str, str]] = {}
    for hn, fresh in fresh_jockeys.items():
        old = entry_jockeys.get(hn)
        if not old or not fresh:
            continue
        if _norm_name(old) != _norm_name(fresh):
            changes[hn] = (old, fresh)
    return changes


def _latest_present_horses(conn: sqlite3.Connection, race_id: str) -> set[int]:
    """最新 realtime_odds スナップショットに居る馬番集合を返す。"""
    rows = conn.execute(
        """
        SELECT horse_number
        FROM realtime_odds
        WHERE race_id = ?
          AND win_odds IS NOT NULL
          AND recorded_at = (
              SELECT MAX(recorded_at) FROM realtime_odds WHERE race_id = ?
          )
        """,
        (race_id, race_id),
    ).fetchall()
    return {int(r[0]) for r in rows}


def _fetch_fresh_jockeys(race_id: str) -> dict[int, str]:
    """netkeiba 直前 entry テーブルから ``{馬番: 騎手名}`` を取得する（best-effort）。

    W-053 のグローバルレート制限つき HTTP クライアントを共有する
    ``fetch_entry_table`` を経由するため、自己 DoS にはならない。
    取得失敗時は空 dict を返す。
    """
    try:
        from src.scraper.entry_table import fetch_entry_table

        tbl = fetch_entry_table(race_id, delay=1.5)
        return {
            int(e.horse_number): (e.jockey or "")
            for e in tbl.entries
            if int(e.horse_number) > 0
        }
    except Exception as exc:  # noqa: BLE001 — 取得失敗は異常検知を止めない
        logger.warning("直前騎手の取得失敗 (race_id=%s): %s", race_id, exc)
        return {}


def check_race_anomalies(
    conn: sqlite3.Connection,
    race_id: str,
    *,
    check_jockey: bool = True,
) -> RaceAnomalies:
    """レースの取消・騎手変更を検知し、騎手変更は entries に反映する。

    取消検知はネットワーク不要（realtime_odds の最新スナップショット利用）。
    騎手変更検知は netkeiba 直前 entry を取得して比較し、変更分は
    ``entries.jockey`` を UPDATE する（再推論で特徴量に反映させるため）。

    Args:
        conn: umalogi.db 接続。
        race_id: 対象レース ID。
        check_jockey: 騎手変更検知（netkeiba 取得）を行うか。

    Returns:
        :class:`RaceAnomalies`。
    """
    entry_rows = conn.execute(
        "SELECT horse_number, jockey FROM entries WHERE race_id = ?", (race_id,)
    ).fetchall()
    entry_horses = {int(r[0]) for r in entry_rows}
    entry_jockeys = {int(r[0]): (r[1] or "") for r in entry_rows}

    present = _latest_present_horses(conn, race_id)
    scratched = detect_scratches(entry_horses, present)

    jockey_changes: dict[int, tuple[str, str]] = {}
    if check_jockey:
        fresh = _fetch_fresh_jockeys(race_id)
        # 取消馬は騎手変更判定から除外する
        for hn in scratched:
            fresh.pop(hn, None)
        jockey_changes = detect_jockey_changes(entry_jockeys, fresh)
        if jockey_changes:
            for hn, (_old, new) in jockey_changes.items():
                conn.execute(
                    "UPDATE entries SET jockey = ? WHERE race_id = ? AND horse_number = ?",
                    (new, race_id, hn),
                )
            conn.commit()

    if scratched or jockey_changes:
        logger.info(
            "異常検知 (race_id=%s): 取消=%s 騎手変更=%s",
            race_id,
            sorted(scratched),
            {hn: chg for hn, chg in jockey_changes.items()},
        )
    return RaceAnomalies(scratched=scratched, jockey_changes=jockey_changes)
