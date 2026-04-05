"""
SQLite データベース初期化スクリプト

テーブル構成:
  races        - レース基本情報
  horses       - 馬マスタ（血統情報含む）
  race_results - レースごとの出走・着順結果
"""

import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# デフォルトDB パス（環境変数で上書き可能）
DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "umalogi.db"


DDL_STATEMENTS: list[str] = [
    # ------------------------------------------------------------------
    # races: レース基本情報
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS races (
        race_id     TEXT        PRIMARY KEY,   -- netkeiba レース ID
        race_name   TEXT        NOT NULL,      -- レース名
        date        TEXT        NOT NULL,      -- 開催日 (YYYY/MM/DD)
        venue       TEXT        NOT NULL,      -- 開催場所
        race_number INTEGER     NOT NULL,      -- 第N競走
        distance    INTEGER     NOT NULL,      -- 距離 (m)
        surface     TEXT        NOT NULL,      -- 芝 / ダート
        weather     TEXT        NOT NULL DEFAULT '',  -- 天候
        condition   TEXT        NOT NULL DEFAULT '',  -- 馬場状態
        created_at  TEXT        NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ------------------------------------------------------------------
    # horses: 馬マスタ（血統情報）
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS horses (
        horse_id    TEXT        PRIMARY KEY,   -- netkeiba 馬 ID
        horse_name  TEXT        NOT NULL,      -- 馬名
        sire        TEXT,                      -- 父
        dam         TEXT,                      -- 母
        dam_sire    TEXT,                      -- 母父
        created_at  TEXT        NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at  TEXT        NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ------------------------------------------------------------------
    # race_results: 出走・着順結果（races × horses の関連テーブル）
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS race_results (
        id              INTEGER     PRIMARY KEY AUTOINCREMENT,
        race_id         TEXT        NOT NULL REFERENCES races(race_id),
        horse_id        TEXT        REFERENCES horses(horse_id),
        horse_name      TEXT        NOT NULL,      -- 馬名（馬IDなしでも保存できるよう）
        rank            INTEGER,                   -- 着順（失格等は NULL）
        sex_age         TEXT        NOT NULL DEFAULT '',  -- 性齢 (例: 牡3)
        weight_carried  REAL        NOT NULL DEFAULT 0,   -- 斤量 (kg)
        jockey          TEXT        NOT NULL DEFAULT '',  -- 騎手名
        finish_time     TEXT,                      -- タイム (例: 1:33.5)
        margin          TEXT,                      -- 着差
        popularity      INTEGER,                   -- 人気
        win_odds        REAL,                      -- 単勝オッズ
        horse_weight    INTEGER,                   -- 馬体重 (kg)
        created_at      TEXT        NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(race_id, horse_name)                -- 同一レースの同一馬名は1行のみ
    )
    """,

    # インデックス
    "CREATE INDEX IF NOT EXISTS idx_race_results_race_id ON race_results(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_race_results_horse_id ON race_results(horse_id)",
    "CREATE INDEX IF NOT EXISTS idx_races_date ON races(date)",
]


def get_db_path() -> Path:
    """環境変数 DB_PATH を優先し、なければデフォルトパスを返す。"""
    env_path = os.environ.get("DB_PATH")
    return Path(env_path) if env_path else DEFAULT_DB_PATH


def init_db(db_path: Path | None = None) -> sqlite3.Connection:
    """
    SQLite DB を初期化してコネクションを返す。

    既に存在する場合はテーブルを再作成せずスキップする（IF NOT EXISTS）。

    Args:
        db_path: DB ファイルパス。None の場合は get_db_path() を使用。

    Returns:
        sqlite3.Connection（呼び出し側でクローズすること）
    """
    path = db_path or get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    with conn:
        for ddl in DDL_STATEMENTS:
            conn.execute(ddl)

    logger.info("DB 初期化完了: %s", path)
    return conn


def insert_race(conn: sqlite3.Connection, race: "RaceInfo") -> None:  # type: ignore[name-defined]
    """
    RaceInfo をトランザクション内で races / horses / race_results に保存する。

    Args:
        conn: sqlite3 コネクション
        race: スクレイパーが返した RaceInfo オブジェクト
    """
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number, distance, surface, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                race.race_id,
                race.race_name,
                race.date,
                race.venue,
                race.race_number,
                race.distance,
                race.surface,
                race.weather,
                race.condition,
            ),
        )

        for r in race.results:
            # 馬マスタ upsert（血統情報があれば更新）
            if r.horse_id:
                conn.execute(
                    """
                    INSERT INTO horses (horse_id, horse_name, sire, dam, dam_sire)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(horse_id) DO UPDATE SET
                        horse_name = excluded.horse_name,
                        sire       = COALESCE(excluded.sire, sire),
                        dam        = COALESCE(excluded.dam, dam),
                        dam_sire   = COALESCE(excluded.dam_sire, dam_sire),
                        updated_at = datetime('now', 'localtime')
                    """,
                    (
                        r.horse_id,
                        r.horse_name,
                        r.pedigree.sire,
                        r.pedigree.dam,
                        r.pedigree.dam_sire,
                    ),
                )

            conn.execute(
                """
                INSERT OR IGNORE INTO race_results
                    (race_id, horse_id, horse_name, rank, sex_age, weight_carried,
                     jockey, finish_time, margin, popularity, win_odds, horse_weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    race.race_id,
                    r.horse_id,
                    r.horse_name,
                    r.rank,
                    r.sex_age,
                    r.weight_carried,
                    r.jockey,
                    r.finish_time,
                    r.margin,
                    r.popularity,
                    r.win_odds,
                    r.horse_weight,
                ),
            )

    logger.info("DB 保存完了: race_id=%s, %d 頭", race.race_id, len(race.results))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = init_db()
    conn.close()
    print("umalogi.db の初期化が完了しました。")
