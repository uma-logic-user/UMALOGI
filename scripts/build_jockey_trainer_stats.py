"""
騎手・調教師 コース別成績バックフィルスクリプト

race_results JOIN races から過去3年分を集計し、
jockey_stats / trainer_stats テーブルを構築する。

使用方法:
    py scripts/build_jockey_trainer_stats.py            # 過去3年分
    py scripts/build_jockey_trainer_stats.py --years 1  # 直近1年
    py scripts/build_jockey_trainer_stats.py --force     # 既存データをクリアして再構築
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "data/umalogi.db"


def _compute_stats(
    conn: sqlite3.Connection,
    cutoff_date: str,
    cutoff_30d: str,
) -> tuple[list[tuple], list[tuple]]:
    """
    jockey_stats / trainer_stats に格納するタプルリストを返す。

    Returns
    -------
    (jockey_rows, trainer_rows)
    各行: (name, venue, surface, total_races, wins, win_rate, place_rate,
           last_30d_races, last_30d_wins, last_30d_win_rate)
    """
    # ── 騎手統計 ─────────────────────────────────────────────────────────
    sql_jockey = """
    WITH base AS (
        SELECT
            rr.jockey                           AS name,
            r.venue,
            r.surface,
            (rr.rank <= 3)                      AS placed,
            (rr.rank = 1)                       AS won,
            r.date
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.jockey IS NOT NULL
          AND rr.jockey != ''
          AND rr.rank IS NOT NULL
          AND rr.rank > 0
          AND r.date >= ?
    )
    SELECT
        name,
        venue,
        surface,
        COUNT(*)                                            AS total_races,
        SUM(won)                                           AS wins,
        ROUND(CAST(SUM(won)    AS REAL) / COUNT(*), 4)    AS win_rate,
        ROUND(CAST(SUM(placed) AS REAL) / COUNT(*), 4)    AS place_rate,
        SUM(CASE WHEN date >= ? THEN 1 ELSE 0 END)         AS last_30d_races,
        SUM(CASE WHEN date >= ? AND won  THEN 1 ELSE 0 END) AS last_30d_wins,
        ROUND(
            CAST(SUM(CASE WHEN date >= ? AND won  THEN 1 ELSE 0 END) AS REAL)
            / NULLIF(SUM(CASE WHEN date >= ? THEN 1 ELSE 0 END), 0),
            4
        )                                                   AS last_30d_win_rate
    FROM base
    GROUP BY name, venue, surface
    HAVING total_races >= 3
    """
    jockey_rows = conn.execute(
        sql_jockey,
        (cutoff_date, cutoff_30d, cutoff_30d, cutoff_30d, cutoff_30d),
    ).fetchall()

    # ── 調教師統計 ─────────────────────────────────────────────────────
    sql_trainer = (
        sql_jockey.replace("rr.jockey", "rr.trainer")
        .replace("rr.jockey IS NOT NULL", "rr.trainer IS NOT NULL")
        .replace("rr.jockey != ''", "rr.trainer != ''")
    )
    trainer_rows = conn.execute(
        sql_trainer,
        (cutoff_date, cutoff_30d, cutoff_30d, cutoff_30d, cutoff_30d),
    ).fetchall()

    return jockey_rows, trainer_rows


def build_stats(
    db_path: str = DB_PATH,
    years: int = 3,
    force: bool = False,
) -> None:
    conn = sqlite3.connect(db_path)
    cutoff_date = (date.today() - timedelta(days=365 * years)).isoformat()
    cutoff_30d = (date.today() - timedelta(days=30)).isoformat()

    if force:
        conn.execute("DELETE FROM jockey_stats")
        conn.execute("DELETE FROM trainer_stats")
        conn.commit()
        print("既存データをクリアしました")

    print(f"集計期間: {cutoff_date} 〜 {date.today().isoformat()}")
    print("統計計算中...", flush=True)

    jockey_rows, trainer_rows = _compute_stats(conn, cutoff_date, cutoff_30d)
    print(f"  騎手データ: {len(jockey_rows):,} 行")
    print(f"  調教師データ: {len(trainer_rows):,} 行")

    # ── jockey_stats に保存 ────────────────────────────────────────────
    conn.executemany(
        """
        INSERT OR REPLACE INTO jockey_stats
            (jockey_name, venue, surface,
             total_races, wins, win_rate, place_rate,
             last_30d_races, last_30d_wins, last_30d_win_rate,
             updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
        """,
        jockey_rows,
    )

    # ── trainer_stats に保存 ───────────────────────────────────────────
    conn.executemany(
        """
        INSERT OR REPLACE INTO trainer_stats
            (trainer_name, venue, surface,
             total_races, wins, win_rate, place_rate,
             last_30d_races, last_30d_wins, last_30d_win_rate,
             updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
        """,
        trainer_rows,
    )

    conn.commit()

    # ── 確認サンプル ──────────────────────────────────────────────────
    jc = conn.execute("SELECT COUNT(*) FROM jockey_stats").fetchone()[0]
    tc = conn.execute("SELECT COUNT(*) FROM trainer_stats").fetchone()[0]
    print(f"\n保存完了: jockey_stats={jc:,}件, trainer_stats={tc:,}件")

    top3 = conn.execute(
        """
        SELECT jockey_name, venue, surface, total_races, win_rate
        FROM jockey_stats
        WHERE total_races >= 10
        ORDER BY win_rate DESC LIMIT 3
        """
    ).fetchall()
    print("\n勝率トップ3（騎手・コース別）:")
    for r in top3:
        print(f"  {r[0]} @ {r[1]} ({r[2]}) : {r[3]}戦 勝率{r[4] * 100:.1f}%")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, default=3, help="集計年数 (デフォルト3年)")
    parser.add_argument(
        "--force", action="store_true", help="既存データをクリアして再構築"
    )
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()
    build_stats(db_path=args.db, years=args.years, force=args.force)
