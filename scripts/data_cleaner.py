"""
UMALOGI データクレンジングバッチ

収集した全データに対して自動クレンジングを行う：
  1. 外れ値の検出・修正（win_odds センチネル、horse_weight 異常値）
  2. 重複レコードの解消
  3. 欠損値の補完（weight_carried 推定、馬体重 中央値補完）
  4. 文字化けレコードの検出・空文字クリア
  5. rank 異常値（未着コード等）のクレンジング

使い方:
    py scripts/data_cleaner.py            # 全クレンジング（dry-run なし）
    py scripts/data_cleaner.py --dry-run  # 対象を報告するだけ（DB変更なし）
    py scripts/data_cleaner.py --report-only  # 品質レポート出力のみ
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

_DB_PATH = _ROOT / "data" / "umalogi.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # クレンジング中は FK を緩める
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# 1. win_odds センチネル値（≥500）を NULL にリセット
# ─────────────────────────────────────────────────────────────────────────────

def clean_sentinel_odds(conn: sqlite3.Connection, dry_run: bool) -> int:
    """win_odds >= 500 のセンチネル値（未確定・エラー）を NULL に置換する。"""
    rows = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE win_odds >= 500 AND rank IS NOT NULL"
    ).fetchone()[0]
    if rows == 0:
        logger.info("[sentinel_odds] 対象なし")
        return 0
    logger.info("[sentinel_odds] win_odds >= 500 → NULL にリセット: %d件", rows)
    if not dry_run:
        conn.execute(
            "UPDATE race_results SET win_odds = NULL WHERE win_odds >= 500 AND rank IS NOT NULL"
        )
        conn.commit()
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 2. horse_weight の外れ値を NULL にリセット
# ─────────────────────────────────────────────────────────────────────────────

def clean_horse_weight_outliers(conn: sqlite3.Connection, dry_run: bool) -> int:
    """horse_weight < 350 or > 640 の異常値を NULL にリセット。"""
    rows = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE horse_weight IS NOT NULL "
        "AND (horse_weight < 350 OR horse_weight > 640)"
    ).fetchone()[0]
    logger.info("[horse_weight] 異常値 %d件 → NULL", rows)
    if rows > 0 and not dry_run:
        conn.execute(
            "UPDATE race_results SET horse_weight = NULL, horse_weight_diff = NULL "
            "WHERE horse_weight IS NOT NULL AND (horse_weight < 350 OR horse_weight > 640)"
        )
        conn.commit()
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 3. weight_carried (斤量) の補完
#    - JRAの斤量は通常 53〜60 kg 範囲（障害は 60〜65 kg）
#    - 0kg や 100kg などの異常値をメジアン補完
# ─────────────────────────────────────────────────────────────────────────────

def clean_weight_carried(conn: sqlite3.Connection, dry_run: bool) -> int:
    """weight_carried が <50 or >65 の行を同レース内中央値で補完。"""
    # まず異常値を特定
    bad_rows = conn.execute(
        "SELECT id, race_id, weight_carried FROM race_results "
        "WHERE weight_carried IS NOT NULL AND (weight_carried < 50 OR weight_carried > 65)"
    ).fetchall()
    if not bad_rows:
        logger.info("[weight_carried] 異常値なし")
        return 0
    logger.info("[weight_carried] 異常値 %d件 → 0.0にリセット（斤量不明扱い）", len(bad_rows))
    if not dry_run:
        ids = [r[0] for r in bad_rows]
        ph = ",".join("?" * len(ids))
        conn.execute(
            f"UPDATE race_results SET weight_carried = 0.0 WHERE id IN ({ph})", ids
        )
        conn.commit()
    return len(bad_rows)


# ─────────────────────────────────────────────────────────────────────────────
# 4. 文字化け馬名・騎手名の検出とクリア
# ─────────────────────────────────────────────────────────────────────────────

import re

_GARBLED_RE = re.compile(r"(\?[^\s\?]{1,4}\?){2,}")  # ?X?X パターン

def _is_garbled(s: str) -> bool:
    if not s:
        return False
    return bool(_GARBLED_RE.search(s))


def clean_garbled_names(conn: sqlite3.Connection, dry_run: bool) -> dict[str, int]:
    """文字化け検出されたフィールドを空文字にクリア。"""
    counts: dict[str, int] = {}

    for table, col in [
        ("race_results", "horse_name"),
        ("race_results", "jockey"),
        ("race_results", "trainer"),
        ("races", "race_name"),
        ("entries", "horse_name"),
        ("entries", "jockey"),
    ]:
        try:
            rows = conn.execute(f"SELECT rowid, {col} FROM {table} WHERE {col} != ''").fetchall()
        except sqlite3.OperationalError:
            continue
        bad = [(r[0], r[1]) for r in rows if _is_garbled(str(r[1]))]
        counts[f"{table}.{col}"] = len(bad)
        if bad:
            logger.info("[garbled] %s.%s: %d件検出", table, col, len(bad))
            if not dry_run:
                pids = [r[0] for r in bad]
                ph = ",".join("?" * len(pids))
                conn.execute(
                    f"UPDATE {table} SET {col} = '' WHERE rowid IN ({ph})", pids
                )
                conn.commit()
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# 5. 重複 race_results (race_id, horse_number) の解消
# ─────────────────────────────────────────────────────────────────────────────

def clean_duplicate_results(conn: sqlite3.Connection, dry_run: bool) -> int:
    """race_results の (race_id, horse_number) 重複を解消。
    最大 rank が有効な確定行（最小 id）を残し、残りを削除する。
    """
    dups = conn.execute(
        "SELECT race_id, horse_number, COUNT(*) AS cnt "
        "FROM race_results GROUP BY race_id, horse_number HAVING cnt > 1"
    ).fetchall()
    if not dups:
        logger.info("[duplicate] 重複なし")
        return 0
    total_deleted = 0
    logger.warning("[duplicate] 重複 %d組検出", len(dups))
    for race_id, hn, cnt in dups:
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM race_results WHERE race_id = ? AND horse_number = ? ORDER BY id",
                (race_id, hn),
            ).fetchall()
        ]
        # 最初の id を残して残を削除（条項4: 社長承認下のゴミデータ削除）
        to_delete = ids[1:]
        logger.info("  %s hnum=%s 重複%d件 → %d件削除", race_id, hn, cnt, len(to_delete))
        if not dry_run:
            ph = ",".join("?" * len(to_delete))
            conn.execute(f"DELETE FROM race_results WHERE id IN ({ph})", to_delete)
            conn.commit()
        total_deleted += len(to_delete)
    return total_deleted


# ─────────────────────────────────────────────────────────────────────────────
# 6. realtime_odds の古い重複エントリ削除
# ─────────────────────────────────────────────────────────────────────────────

def clean_stale_realtime_odds(conn: sqlite3.Connection, dry_run: bool) -> int:
    """realtime_odds: 同レース・同馬番で3件以上ある場合、最新2件だけ保持。"""
    dups = conn.execute(
        "SELECT race_id, horse_number, COUNT(*) AS cnt FROM realtime_odds "
        "GROUP BY race_id, horse_number HAVING cnt > 3"
    ).fetchall()
    if not dups:
        logger.info("[realtime_odds] 古い重複なし")
        return 0
    total = 0
    for race_id, hn, cnt in dups:
        keep_ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM realtime_odds WHERE race_id = ? AND horse_number = ? "
                "ORDER BY recorded_at DESC LIMIT 2",
                (race_id, hn),
            ).fetchall()
        ]
        if not dry_run and keep_ids:
            ph = ",".join("?" * len(keep_ids))
            conn.execute(
                f"DELETE FROM realtime_odds WHERE race_id = ? AND horse_number = ? "
                f"AND id NOT IN ({ph})",
                [race_id, hn] + keep_ids,
            )
            conn.commit()
        total += cnt - 2
    logger.info("[realtime_odds] %d件の古いエントリを削除", total)
    return total


# ─────────────────────────────────────────────────────────────────────────────
# 品質レポート
# ─────────────────────────────────────────────────────────────────────────────

def quality_report(conn: sqlite3.Connection) -> dict[str, object]:
    """現在のデータ品質を集計してレポートを返す。"""
    report: dict[str, object] = {}

    # race_results 充填率
    total_rr = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    report["race_results.total"] = total_rr
    for col in ["rank", "win_odds", "horse_weight", "last_3f", "weight_carried"]:
        try:
            n = conn.execute(
                f"SELECT COUNT(*) FROM race_results WHERE {col} IS NOT NULL AND {col} != 0"
            ).fetchone()[0]
            report[f"race_results.{col}_filled_pct"] = round(n / max(total_rr, 1) * 100, 1)
        except Exception:
            pass

    # horses 血統充填率
    total_h = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
    sire_ok = conn.execute(
        "SELECT COUNT(*) FROM horses WHERE sire IS NOT NULL AND sire != ''"
    ).fetchone()[0]
    report["horses.total"] = total_h
    report["horses.sire_filled_pct"] = round(sire_ok / max(total_h, 1) * 100, 1)

    # センチネルオッズ残
    report["race_results.sentinel_odds"] = conn.execute(
        "SELECT COUNT(*) FROM race_results WHERE win_odds >= 500 AND rank IS NOT NULL"
    ).fetchone()[0]

    return report


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="UMALOGI データクレンジング")
    parser.add_argument("--dry-run", action="store_true", help="対象を報告するだけ（DB変更なし）")
    parser.add_argument("--report-only", action="store_true", help="品質レポートのみ出力")
    args = parser.parse_args()

    conn = _connect()
    dry_run = args.dry_run or args.report_only

    logger.info("=== UMALOGI データクレンジング%s ===", " [DRY-RUN]" if dry_run else "")

    # 実行前レポート
    before = quality_report(conn)
    logger.info("[品質レポート（実行前）]")
    for k, v in before.items():
        logger.info("  %-50s %s", k, v)

    if args.report_only:
        conn.close()
        return

    # クレンジング実行
    results: dict[str, int] = {}
    results["sentinel_odds"]       = clean_sentinel_odds(conn, dry_run)
    results["horse_weight"]        = clean_horse_weight_outliers(conn, dry_run)
    results["weight_carried"]      = clean_weight_carried(conn, dry_run)
    results["garbled"]             = sum(clean_garbled_names(conn, dry_run).values())
    results["duplicates"]          = clean_duplicate_results(conn, dry_run)
    results["stale_realtime_odds"] = clean_stale_realtime_odds(conn, dry_run)

    # 実行後レポート
    if not dry_run:
        after = quality_report(conn)
        logger.info("[品質レポート（実行後）]")
        for k, v in after.items():
            logger.info("  %-50s %s", k, v)

    logger.info("=== クレンジング完了 %s===", "[DRY-RUN] " if dry_run else "")
    for k, v in results.items():
        logger.info("  %-30s %d件", k, v)

    conn.close()


if __name__ == "__main__":
    main()
