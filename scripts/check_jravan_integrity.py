"""JRA-VAN 過去データ整合性チェック（read-only）。

`races`（開催スケジュール）に対し `race_results`（確定着順）の充足状況を
年・月・日・会場の粒度でスキャンし、期間的な欠損（結果未取得の月/日）を
自動検出する。欠損が見つかった場合は JVLink での再取得（Setup/Update）コマンドを
提案する（自動実行はせず提案に留める＝本番非破壊・条項4 の精神）。

使い方::

    py scripts/check_jravan_integrity.py                 # 全期間レポート
    py scripts/check_jravan_integrity.py --since 2023-01 # 期間限定
    py scripts/check_jravan_integrity.py --json          # JSON 出力

設計: DB は読み取り専用（mode=ro 相当の SELECT のみ）。集計関数は in-memory DB で
テスト可能なように conn を受け取る純粋関数として実装する。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# 月の結果充足率がこれ未満なら「低充足」として警告
_LOW_COVERAGE_RATIO = 0.5
# 低充足判定の最小スケジュール件数（少数開催の偽陽性を避ける）
_MIN_SCHEDULED_FOR_FLAG = 10


@dataclass
class IntegrityReport:
    """整合性スキャン結果。"""

    total_races: int
    total_with_results: int
    overall_coverage: float
    month_coverage: dict[str, tuple[int, int]]  # "YYYY-MM" -> (scheduled, with_results)
    missing_months: list[str]  # scheduled>0 だが results=0
    low_coverage_months: list[str]  # 0 < ratio < しきい値
    venues: list[str]
    suggested_jvlink_ranges: list[tuple[str, str]] = field(default_factory=list)

    def is_healthy(self) -> bool:
        return not self.missing_months and not self.low_coverage_months


def _result_race_ids(conn: sqlite3.Connection) -> set[str]:
    """確定着順（rank IS NOT NULL）を持つ race_id 集合。"""
    return {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT race_id FROM race_results WHERE rank IS NOT NULL"
        ).fetchall()
    }


def scan_integrity(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
    until: str | None = None,
    today: str | None = None,
) -> IntegrityReport:
    """`races` vs `race_results` の充足を月粒度でスキャンする。

    Args:
        conn: DB 接続（読み取りのみ）。
        since: 開始 "YYYY-MM" or "YYYY-MM-DD"（含む）。None=全期間。
        until: 終了（含む）。None=全期間。
        today: 「未来日」判定の基準日 "YYYY-MM-DD"（既定=実日付）。未来の月は
               欠損判定から除外する（まだ開催されていないため）。

    Returns:
        :class:`IntegrityReport`。
    """
    today = today or date.today().isoformat()
    cur_month = today[:7]

    where = ["date IS NOT NULL", "date != ''"]
    params: list[str] = []
    if since:
        where.append("date >= ?")
        params.append(since if len(since) > 7 else since + "-01")
    if until:
        where.append("date <= ?")
        params.append(until if len(until) > 7 else until + "-31")
    clause = " AND ".join(where)

    rows = conn.execute(
        f"SELECT race_id, date, venue FROM races WHERE {clause}", params
    ).fetchall()
    result_ids = _result_race_ids(conn)

    month_sched: dict[str, int] = {}
    month_done: dict[str, int] = {}
    venues: set[str] = set()
    total = 0
    total_done = 0
    for race_id, d, venue in rows:
        month = str(d)[:7]
        month_sched[month] = month_sched.get(month, 0) + 1
        total += 1
        if venue:
            venues.add(str(venue))
        if race_id in result_ids:
            month_done[month] = month_done.get(month, 0) + 1
            total_done += 1

    month_coverage: dict[str, tuple[int, int]] = {
        m: (month_sched[m], month_done.get(m, 0)) for m in sorted(month_sched)
    }

    missing: list[str] = []
    low: list[str] = []
    for m, (sched, done) in month_coverage.items():
        if m >= cur_month:  # 当月以降は未確定があり得るので欠損判定しない
            continue
        if sched > 0 and done == 0:
            missing.append(m)
        elif (
            sched >= _MIN_SCHEDULED_FOR_FLAG and (done / sched) < _LOW_COVERAGE_RATIO
        ):
            low.append(m)

    return IntegrityReport(
        total_races=total,
        total_with_results=total_done,
        overall_coverage=(total_done / total) if total else 0.0,
        month_coverage=month_coverage,
        missing_months=missing,
        low_coverage_months=low,
        venues=sorted(venues),
        suggested_jvlink_ranges=_suggest_ranges(missing + low),
    )


def _suggest_ranges(months: list[str]) -> list[tuple[str, str]]:
    """欠損月リストを連続レンジ (fromtime, totime YYYYMMDD) に畳む。"""
    if not months:
        return []
    months = sorted(set(months))
    ranges: list[tuple[str, str]] = []
    start = prev = months[0]
    for m in months[1:]:
        if _next_month(prev) == m:
            prev = m
            continue
        ranges.append((start.replace("-", "") + "01", _month_end(prev)))
        start = prev = m
    ranges.append((start.replace("-", "") + "01", _month_end(prev)))
    return ranges


def _next_month(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{y + 1}-01" if m == 12 else f"{y}-{m + 1:02d}"


def _month_end(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    nxt = _next_month(ym)
    ny, nm = int(nxt[:4]), int(nxt[5:7])
    from datetime import date as _d
    from datetime import timedelta

    last = _d(ny, nm, 1) - timedelta(days=1)
    return last.strftime("%Y%m%d")


def format_report(rep: IntegrityReport) -> str:
    """人間可読レポート文字列。"""
    lines = [
        "=" * 60,
        "JRA-VAN 過去データ整合性レポート",
        "=" * 60,
        f"総レース(スケジュール): {rep.total_races:,}",
        f"確定結果あり          : {rep.total_with_results:,} "
        f"({rep.overall_coverage * 100:.1f}%)",
        f"会場                  : {', '.join(rep.venues)}",
        "",
        f"🚨 結果ゼロの月: {rep.missing_months or 'なし'}",
        f"⚠️ 低充足の月  : {rep.low_coverage_months or 'なし'}",
    ]
    if rep.suggested_jvlink_ranges:
        lines.append("")
        lines.append("【JVLink 再取得の提案（自動実行はしない）】")
        for fr, to in rep.suggested_jvlink_ranges:
            lines.append(
                f"  py -3-32 -m src.scraper.jravan_client --option 2 "
                f"--fromtime {fr}  # 〜{to}"
            )
    lines.append("")
    lines.append("✅ 健全" if rep.is_healthy() else "❌ 欠損あり（上記を確認）")
    return "\n".join(lines)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass
    p = argparse.ArgumentParser(description="JRA-VAN 過去データ整合性チェック")
    p.add_argument("--since", help="開始 YYYY-MM or YYYY-MM-DD")
    p.add_argument("--until", help="終了 YYYY-MM or YYYY-MM-DD")
    p.add_argument("--json", action="store_true", help="JSON 出力")
    args = p.parse_args()

    from src.database.init_db import init_db

    conn = init_db()
    try:
        rep = scan_integrity(conn, since=args.since, until=args.until)
    finally:
        conn.close()

    if args.json:
        print(json.dumps(asdict(rep), ensure_ascii=False, indent=2))
    else:
        print(format_report(rep))
    return 0 if rep.is_healthy() else 1


if __name__ == "__main__":
    raise SystemExit(main())
