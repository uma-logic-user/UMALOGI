"""
src/data/jravan_pipeline.py — JRA-VAN データ 単一真実源(SSOT)パイプライン facade

【目的】
  JRA-VAN（JVLink / 速報 JVRTOpen）から取得する以下4種のデータの取得・DB同期を
  ひとつの facade に集約し、UMALOGI 全体が「同じ入口」を通すようにする。
    1. リアルタイムオッズ（realtime_odds）
    2. 時系列オッズ（realtime_odds への複数スナップショット追記）
    3. 直前情報（馬体重・天候馬場）
    4. 確定結果・払戻（race_results / race_payouts）

【単一真実源(SSOT)】
  オッズの SSOT は **realtime_odds テーブル**（W-055 で統一済み）。
  本 facade は新しい取得ロジックを再実装せず、検証済みの既存実装に委譲する:
    - オッズ/馬体重/天候 : src.pipeline.scraping.fetch_and_save_odds
                           （JRA-VAN速報 JVRTOpen → RTD → netkeiba の三段フォールバック）
    - 時系列スナップショット : scripts.record_odds_timeseries.capture_today_odds
    - 確定結果           : scripts.fetch_race_result
    - 払戻補完           : src.scraper.update_payouts

【「オッズ時系列の空問題」再発防止（最重要）】
  過去、odds_timeseries が空（本日0/24）になり odds_drift / odds_momentum が
  最低2点を得られず死んだ。本 facade は取得「後」に必ず
  ``odds_snapshot_health`` でスナップショット数を**検証**し、
  ``coverage_report`` で「2点以上=健全 / 1点 / 0点」をレース横断で可視化する。
  取得したつもりで空のまま進む事故を構造的に検知できるようにする。
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)

#: odds_drift / odds_momentum が動作するために必要な最低スナップショット点数。
MIN_HEALTHY_SNAPSHOTS: int = 2


# ── オッズ SSOT ヘルスチェック（純粋・DBのみ依存）─────────────────────────────


@dataclass(frozen=True)
class OddsHealth:
    """1レースのオッズ時系列健全性。"""

    race_id: str
    n_snapshots: int  # distinct recorded_at の数（時系列の点数）
    n_horses: int  # オッズが入っている馬数（最新スナップショット）
    is_healthy: bool  # n_snapshots >= MIN_HEALTHY_SNAPSHOTS

    @property
    def status(self) -> str:
        if self.n_snapshots == 0:
            return "empty"
        if self.n_snapshots == 1:
            return "single"
        return "healthy"


def odds_snapshot_health(conn: sqlite3.Connection, race_id: str) -> OddsHealth:
    """realtime_odds から当該レースのオッズ時系列健全性を集計する。

    Args:
        conn: DB コネクション。
        race_id: 対象レース ID。

    Returns:
        OddsHealth（スナップショット点数・馬数・健全フラグ）。
    """
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT recorded_at) AS n_snap,
               COUNT(DISTINCT horse_number) AS n_horse
        FROM realtime_odds
        WHERE race_id = ?
        """,
        (race_id,),
    ).fetchone()
    n_snap = int(row[0]) if row and row[0] is not None else 0
    n_horse = int(row[1]) if row and row[1] is not None else 0
    return OddsHealth(
        race_id=race_id,
        n_snapshots=n_snap,
        n_horses=n_horse,
        is_healthy=n_snap >= MIN_HEALTHY_SNAPSHOTS,
    )


@dataclass(frozen=True)
class CoverageReport:
    """ある日付のオッズ時系列カバレッジ（odds空問題の自動検知用）。"""

    date: str
    n_races: int
    healthy: int  # 2点以上（drift稼働可）
    single: int  # 1点のみ
    empty: int  # 0点
    empty_race_ids: tuple[str, ...]
    single_race_ids: tuple[str, ...]

    @property
    def is_ok(self) -> bool:
        """全レースが健全（2点以上）なら True。"""
        return self.n_races > 0 and self.empty == 0 and self.single == 0

    def summary(self) -> str:
        return (
            f"{self.date}: {self.n_races}R 中 健全(2点+)={self.healthy} "
            f"1点={self.single} 空={self.empty}"
        )


def coverage_report(conn: sqlite3.Connection, date: str) -> CoverageReport:
    """指定日の全レースについてオッズ時系列カバレッジを集計する。

    「12/24 が1点」「odds_timeseries 空」のような劣化を一目で検知するための
    SSOT ヘルスレポート。

    Args:
        conn: DB コネクション。
        date: 対象日（YYYY-MM-DD）。

    Returns:
        CoverageReport。
    """
    race_ids = [
        r[0]
        for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id", (date,)
        ).fetchall()
    ]
    healthy = single = empty = 0
    empties: list[str] = []
    singles: list[str] = []
    for rid in race_ids:
        h = odds_snapshot_health(conn, rid)
        if h.n_snapshots == 0:
            empty += 1
            empties.append(rid)
        elif h.n_snapshots == 1:
            single += 1
            singles.append(rid)
        else:
            healthy += 1
    return CoverageReport(
        date=date,
        n_races=len(race_ids),
        healthy=healthy,
        single=single,
        empty=empty,
        empty_race_ids=tuple(empties),
        single_race_ids=tuple(singles),
    )


# ── 同期 facade（既存実装へ委譲）─────────────────────────────────────────────


@dataclass
class SyncResult:
    """同期処理の結果。"""

    ok: bool
    race_id: str
    n_records: int
    detail: str = ""


def sync_odds(
    conn: sqlite3.Connection,
    race_id: str,
    *,
    fetcher: Callable[[sqlite3.Connection, str], int] | None = None,
    verify: bool = True,
) -> SyncResult:
    """1レースのオッズ・馬体重・天候を取得し realtime_odds に同期する。

    JRA-VAN速報(JVRTOpen) → RTD → netkeiba の三段フォールバックは
    fetch_and_save_odds が担う。取得後に realtime_odds を再確認し、
    1件も入っていなければ警告する（オッズ空問題の再発防止）。

    Args:
        conn: DB コネクション。
        race_id: 対象レース ID。
        fetcher: 取得関数（テスト差し替え用）。None なら fetch_and_save_odds。
        verify: True なら取得後に realtime_odds の件数を検証する。

    Returns:
        SyncResult（ok / 取得馬数 / 詳細）。
    """
    if fetcher is None:
        from src.pipeline.scraping import fetch_and_save_odds

        fetcher = fetch_and_save_odds

    try:
        n = int(fetcher(conn, race_id) or 0)
    except Exception as exc:  # noqa: BLE001 — 1レース失敗で全体を止めない
        logger.error("オッズ同期失敗 race_id=%s: %s", race_id, exc)
        return SyncResult(ok=False, race_id=race_id, n_records=0, detail=str(exc))

    if verify:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM realtime_odds WHERE race_id = ?", (race_id,)
        ).fetchone()[0]
        if cnt == 0:
            logger.warning(
                "⚠️ オッズ同期後も realtime_odds が空: race_id=%s（速報/RTD/netkeiba 全滅）",
                race_id,
            )
            return SyncResult(
                ok=False, race_id=race_id, n_records=0, detail="取得後も realtime_odds 空"
            )
    return SyncResult(ok=True, race_id=race_id, n_records=n, detail="OK")


def sync_odds_timeseries(
    date: str | None = None,
    *,
    capture: Callable[[str | None], int] | None = None,
) -> int:
    """発走前ウィンドウのレースに対しオッズスナップショットを追記する。

    時系列の「2点目以降」を確保するための定期取得。capture_today_odds に委譲。

    Args:
        date: 未使用（capture_today_odds は当日を見る）。後方互換のため受ける。
        capture: 取得関数（テスト差し替え用）。None なら capture_today_odds。

    Returns:
        スナップショットを取得したレース数。
    """
    if capture is None:
        from scripts.record_odds_timeseries import capture_today_odds

        capture = capture_today_odds  # type: ignore[assignment]
    try:
        return int(capture(None) or 0)  # type: ignore[misc]
    except Exception as exc:  # noqa: BLE001
        logger.error("オッズ時系列同期失敗: %s", exc)
        return 0


def sync_results(
    race_id: str,
    *,
    fetcher: Callable[[str], object] | None = None,
) -> SyncResult:
    """確定結果（着順・払戻）を取得して race_results / race_payouts に同期する。

    JRA-VAN RTD を一次、netkeiba を二次とする既存実装に委譲する。

    Args:
        race_id: 対象レース ID。
        fetcher: 取得関数（テスト差し替え用）。None なら fetch_race_result.fetch_one。

    Returns:
        SyncResult。
    """
    if fetcher is None:
        try:
            from scripts.fetch_race_result import fetch_one  # type: ignore

            fetcher = fetch_one  # type: ignore[assignment]
        except Exception as exc:  # noqa: BLE001
            logger.error("確定結果取得関数のロード失敗: %s", exc)
            return SyncResult(ok=False, race_id=race_id, n_records=0, detail=str(exc))
    try:
        res = fetcher(race_id)  # type: ignore[misc]
        n = int(res) if isinstance(res, int) else 0
        return SyncResult(ok=True, race_id=race_id, n_records=n, detail="OK")
    except Exception as exc:  # noqa: BLE001
        logger.error("確定結果同期失敗 race_id=%s: %s", race_id, exc)
        return SyncResult(ok=False, race_id=race_id, n_records=0, detail=str(exc))
