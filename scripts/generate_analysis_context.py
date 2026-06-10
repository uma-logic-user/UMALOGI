"""
UMA-Logic 分析用コンテキストCSV生成スクリプト
出力: ./data/uma_analysis_context.csv

カラム構成:
  race_id, date, venue, distance, surface, condition,
  horse_name, win_odds, popularity, actual_rank,
  predicted_rank, ev_score, confidence,
  model_type, bet_type, is_hit, payout, roi
"""

import csv
import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "umalogi.db"
OUT_PATH = Path(__file__).parent.parent / "data" / "uma_analysis_context.csv"

QUERY = """
WITH base AS (
    SELECT
        p.id          AS prediction_id,
        p.race_id,
        p.model_type,
        p.bet_type,
        p.confidence,
        p.expected_value,
        pr.is_hit,
        pr.payout,
        pr.roi,
        r.date,
        r.venue,
        r.distance,
        r.surface,
        r.condition
    FROM predictions p
    JOIN races r
        ON r.race_id = p.race_id
    LEFT JOIN prediction_results pr
        ON pr.prediction_id = p.id
    WHERE p.is_superseded = 0
      OR  p.is_superseded IS NULL
)
SELECT
    b.race_id,
    b.date,
    b.venue,
    b.distance,
    b.surface,
    b.condition,
    ph.horse_name,
    rr.win_odds,
    rr.popularity,
    rr.rank             AS actual_rank,
    ph.predicted_rank,
    ph.ev_score,
    b.confidence,
    b.model_type,
    b.bet_type,
    b.is_hit,
    b.payout,
    b.roi
FROM base b
JOIN prediction_horses ph
    ON ph.prediction_id = b.prediction_id
LEFT JOIN race_results rr
    ON  rr.race_id   = b.race_id
    AND rr.horse_name = ph.horse_name
ORDER BY b.date DESC, b.race_id, ph.predicted_rank
"""

FIELDNAMES = [
    "race_id", "date", "venue", "distance", "surface", "condition",
    "horse_name", "win_odds", "popularity", "actual_rank",
    "predicted_rank", "ev_score", "confidence",
    "model_type", "bet_type", "is_hit", "payout", "roi",
]


def _safe(value: object, default: str = "") -> str:
    """None / NaN を空文字に変換する。"""
    if value is None:
        return default
    try:
        s = str(value)
        return s if s.lower() not in ("none", "nan") else default
    except Exception:
        return default


def generate(db_path: Path = DB_PATH, out_path: Path = OUT_PATH) -> int:
    """CSVを生成して書き込み行数を返す。"""
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("DB接続: %s", db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    try:
        cur = con.cursor()
        log.info("クエリ実行中...")
        cur.execute(QUERY)
        rows = cur.fetchall()
        log.info("取得行数: %d", len(rows))
    except sqlite3.Error as exc:
        log.error("SQLエラー: %s", exc)
        raise
    finally:
        con.close()

    written = 0
    skipped = 0

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for row in rows:
            try:
                record: dict[str, str] = {col: _safe(row[col]) for col in FIELDNAMES}

                # 必須フィールドが欠損している行はスキップ
                if not record["race_id"] or not record["horse_name"]:
                    skipped += 1
                    continue

                writer.writerow(record)
                written += 1

            except (KeyError, TypeError, ValueError) as exc:
                log.warning("行スキップ (変換エラー): %s | row=%s", exc, dict(row))
                skipped += 1
                continue

    log.info("書き込み完了: %d 行 (スキップ: %d 行)", written, skipped)
    log.info("出力先: %s", out_path.resolve())
    return written


def verify(out_path: Path = OUT_PATH) -> None:
    """生成されたCSVの先頭3行をログ出力して内容を確認する。"""
    if not out_path.exists():
        log.error("出力ファイルが存在しません: %s", out_path)
        return

    size_kb = out_path.stat().st_size / 1024
    log.info("ファイルサイズ: %.1f KB", size_kb)

    with open(out_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        log.info("カラム: %s", reader.fieldnames)
        for i, row in enumerate(reader):
            if i >= 3:
                break
            log.info("  row[%d]: %s", i, dict(row))


if __name__ == "__main__":
    start = datetime.now()
    log.info("=== UMA-Logic 分析用CSV生成 開始 ===")

    try:
        n = generate()
        verify()
        elapsed = (datetime.now() - start).total_seconds()
        log.info("=== 完了 (%.1f 秒 / %d 行) ===", elapsed, n)
        sys.exit(0)
    except FileNotFoundError as exc:
        log.error("ファイルが見つかりません: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("予期せぬエラー: %s", exc)
        sys.exit(2)
