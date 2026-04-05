"""
SQLite データベース初期化スクリプト

テーブル構成:
  ── データ層 ──────────────────────────────────────────────────
  races              - レース基本情報
  horses             - 馬マスタ（血統情報含む）
  race_results       - レースごとの出走・着順結果
  entries            - 出馬表（レース前の出走登録情報）
  realtime_odds      - リアルタイムオッズ履歴

  ── 予想層 ────────────────────────────────────────────────────
  predictions        - 卍/本命モデルの予想バッチ（1レース×1馬券種）
  prediction_horses  - 予想に含まれる馬と個別スコア
  prediction_results - 的中・払戻の実績

  ── 集計層 ────────────────────────────────────────────────────
  model_performance  - モデル別 年/月/会場 累積成績

  ── ビュー ────────────────────────────────────────────────────
  v_prediction_summary - 予想 × レース × 結果の結合ビュー
"""

import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "umalogi.db"


DDL_STATEMENTS: list[str] = [

    # ================================================================
    # ── データ層 ────────────────────────────────────────────────────
    # ================================================================

    """
    CREATE TABLE IF NOT EXISTS races (
        race_id     TEXT    PRIMARY KEY,
        race_name   TEXT    NOT NULL,
        date        TEXT    NOT NULL,       -- YYYY/MM/DD
        venue       TEXT    NOT NULL,
        race_number INTEGER NOT NULL,
        distance    INTEGER NOT NULL,
        surface     TEXT    NOT NULL,       -- 芝 / ダート
        weather     TEXT    NOT NULL DEFAULT '',
        condition   TEXT    NOT NULL DEFAULT '',
        created_at  TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS horses (
        horse_id   TEXT PRIMARY KEY,
        horse_name TEXT NOT NULL,
        sire       TEXT,                   -- 父
        dam        TEXT,                   -- 母
        dam_sire   TEXT,                   -- 母父
        created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS race_results (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id        TEXT    NOT NULL REFERENCES races(race_id),
        horse_id       TEXT    REFERENCES horses(horse_id),
        horse_name     TEXT    NOT NULL,
        rank           INTEGER,
        sex_age        TEXT    NOT NULL DEFAULT '',
        weight_carried REAL    NOT NULL DEFAULT 0,
        jockey         TEXT    NOT NULL DEFAULT '',
        finish_time    TEXT,
        margin         TEXT,
        popularity     INTEGER,
        win_odds       REAL,
        horse_weight   INTEGER,
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(race_id, horse_name)
    )
    """,

    # ================================================================
    # ── 出馬表・オッズ層 ─────────────────────────────────────────
    # ================================================================

    # entries: レース前の出走登録情報（出馬表）
    """
    CREATE TABLE IF NOT EXISTS entries (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id            TEXT    NOT NULL REFERENCES races(race_id) ON DELETE CASCADE,
        horse_number       INTEGER NOT NULL,  -- 馬番
        gate_number        INTEGER NOT NULL DEFAULT 0,  -- 枠番
        horse_id           TEXT    REFERENCES horses(horse_id),
        horse_name         TEXT    NOT NULL,
        sex_age            TEXT    NOT NULL DEFAULT '',
        weight_carried     REAL    NOT NULL DEFAULT 0,
        jockey             TEXT    NOT NULL DEFAULT '',
        trainer            TEXT    NOT NULL DEFAULT '',
        horse_weight       INTEGER,           -- 馬体重（kg）
        horse_weight_diff  INTEGER,           -- 前走比
        scraped_at         TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(race_id, horse_number)
    )
    """,

    # realtime_odds: 単勝・複勝オッズの時系列スナップショット
    """
    CREATE TABLE IF NOT EXISTS realtime_odds (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id          TEXT    NOT NULL REFERENCES races(race_id) ON DELETE CASCADE,
        horse_number     INTEGER NOT NULL,
        horse_name       TEXT    NOT NULL DEFAULT '',
        win_odds         REAL,               -- 単勝オッズ
        place_odds_min   REAL,               -- 複勝オッズ（下限）
        place_odds_max   REAL,               -- 複勝オッズ（上限）
        popularity       INTEGER,            -- 人気順
        recorded_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # entries インデックス
    "CREATE INDEX IF NOT EXISTS idx_entries_race_id      ON entries(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_entries_horse_id     ON entries(horse_id)",

    # realtime_odds インデックス
    "CREATE INDEX IF NOT EXISTS idx_odds_race_id         ON realtime_odds(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_odds_recorded_at     ON realtime_odds(race_id, recorded_at)",

    # ================================================================
    # ── 予想層 ────────────────────────────────────────────────────
    # ================================================================

    # predictions: 1レース × 1モデル × 1馬券種 の予想バッチ
    """
    CREATE TABLE IF NOT EXISTS predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id         TEXT    NOT NULL REFERENCES races(race_id),
        model_type      TEXT    NOT NULL
                            CHECK(model_type IN ('卍', '本命')),
                                             -- 卍=回収率特化 / 本命=的中率特化
        bet_type        TEXT    NOT NULL,    -- 単勝/複勝/馬連/馬単/三連複/三連単/WIN5
        confidence      REAL,               -- モデル信頼度 0.0〜1.0
        expected_value  REAL,               -- 期待値（卍モデルの主指標）
        recommended_bet REAL,               -- 推奨購入金額（Kelly最適化後）
        notes           TEXT,               -- 根拠メモ（血統・オッズ歪み等）
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # prediction_horses: 予想に含まれる馬と個別スコア
    """
    CREATE TABLE IF NOT EXISTS prediction_horses (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_id  INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
        horse_id       TEXT    REFERENCES horses(horse_id),
        horse_name     TEXT    NOT NULL,
        predicted_rank INTEGER,             -- 1=本命 2=対抗 3=単穴 …
        model_score    REAL,               -- モデルのスコア（高いほど有力）
        ev_score       REAL,               -- 期待値スコア（卍モデル用）
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # prediction_results: レース終了後に照合し的中・払戻を記録
    """
    CREATE TABLE IF NOT EXISTS prediction_results (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_id INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
        is_hit        INTEGER NOT NULL DEFAULT 0,  -- 0=外れ 1=的中
        payout        REAL    DEFAULT 0,            -- 払戻金額（円）
        profit        REAL    DEFAULT 0,            -- 利益（払戻 - 購入）
        roi           REAL,                         -- 回収率（%）= payout/bet*100
        recorded_at   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,

    # ================================================================
    # ── 集計層 ──────────────────────────────────────────────────────
    # ================================================================

    # model_performance: 定期バッチで集計・更新するモデル累積成績
    """
    CREATE TABLE IF NOT EXISTS model_performance (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        model_type     TEXT    NOT NULL CHECK(model_type IN ('卍', '本命')),
        bet_type       TEXT    NOT NULL DEFAULT 'ALL',
        year           INTEGER NOT NULL,
        month          INTEGER NOT NULL DEFAULT 0,  -- 0 = 年間集計
        venue          TEXT    NOT NULL DEFAULT '', -- '' = 全場集計
        total_bets     INTEGER NOT NULL DEFAULT 0,
        hits           INTEGER NOT NULL DEFAULT 0,
        hit_rate       REAL,
        total_invested REAL    NOT NULL DEFAULT 0,
        total_payout   REAL    NOT NULL DEFAULT 0,
        roi            REAL,
        updated_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(model_type, bet_type, year, month, venue)
    )
    """,

    # ================================================================
    # ── インデックス ─────────────────────────────────────────────────
    # ================================================================

    # データ層 ---
    "CREATE INDEX IF NOT EXISTS idx_races_date        ON races(date)",
    "CREATE INDEX IF NOT EXISTS idx_races_venue       ON races(venue)",
    # 年・会場の複合インデックス（年別・会場別アーカイブ用）
    "CREATE INDEX IF NOT EXISTS idx_races_year_venue  ON races(substr(date,1,4), venue)",

    "CREATE INDEX IF NOT EXISTS idx_results_race_id   ON race_results(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_results_horse_id  ON race_results(horse_id)",
    "CREATE INDEX IF NOT EXISTS idx_results_rank      ON race_results(rank)",

    # 予想層 ---
    "CREATE INDEX IF NOT EXISTS idx_pred_race_id      ON predictions(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_model_type   ON predictions(model_type)",
    "CREATE INDEX IF NOT EXISTS idx_pred_created_at   ON predictions(created_at)",
    # 年・会場をまたいだ予想検索（レース情報と JOIN して使う）
    "CREATE INDEX IF NOT EXISTS idx_pred_bet_type     ON predictions(bet_type)",

    "CREATE INDEX IF NOT EXISTS idx_pred_h_pred_id    ON prediction_horses(prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_h_horse_id   ON prediction_horses(horse_id)",

    "CREATE INDEX IF NOT EXISTS idx_pred_r_pred_id    ON prediction_results(prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_r_is_hit     ON prediction_results(is_hit)",

    # 集計層 ---
    "CREATE INDEX IF NOT EXISTS idx_mperf_type_year   ON model_performance(model_type, year, month)",
    "CREATE INDEX IF NOT EXISTS idx_mperf_venue       ON model_performance(model_type, venue)",

    # ================================================================
    # ── ビュー ───────────────────────────────────────────────────────
    # ================================================================

    # 予想 × レース × 的中実績 の結合ビュー（ダッシュボード用）
    """
    CREATE VIEW IF NOT EXISTS v_prediction_summary AS
    SELECT
        p.id              AS prediction_id,
        p.race_id,
        r.race_name,
        r.date,
        substr(r.date, 1, 4)  AS year,
        r.venue,
        r.surface,
        r.distance,
        p.model_type,
        p.bet_type,
        p.confidence,
        p.expected_value,
        p.recommended_bet,
        pr.is_hit,
        pr.payout,
        pr.profit,
        pr.roi,
        pr.recorded_at
    FROM predictions p
    JOIN  races r              ON p.race_id = r.race_id
    LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
    """,

    # 各モデルの年別サマリービュー
    """
    CREATE VIEW IF NOT EXISTS v_model_annual_summary AS
    SELECT
        mp.model_type,
        mp.year,
        mp.venue,
        mp.bet_type,
        mp.total_bets,
        mp.hits,
        mp.hit_rate,
        mp.total_invested,
        mp.total_payout,
        mp.roi,
        mp.updated_at
    FROM model_performance mp
    ORDER BY mp.year DESC, mp.model_type, mp.venue
    """,
]


# ================================================================
# ── DB 操作関数 ──────────────────────────────────────────────────
# ================================================================

def get_db_path() -> Path:
    """環境変数 DB_PATH を優先し、なければデフォルトパスを返す。"""
    env_path = os.environ.get("DB_PATH")
    return Path(env_path) if env_path else DEFAULT_DB_PATH


def init_db(db_path: Path | None = None) -> sqlite3.Connection:
    """
    SQLite DB を初期化してコネクションを返す。

    既存テーブルは IF NOT EXISTS で保護する（データ削除なし）。

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
    """
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                race.race_id, race.race_name, race.date, race.venue,
                race.race_number, race.distance, race.surface,
                race.weather, race.condition,
            ),
        )

        for r in race.results:
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
                    (r.horse_id, r.horse_name,
                     r.pedigree.sire, r.pedigree.dam, r.pedigree.dam_sire),
                )

            conn.execute(
                """
                INSERT OR IGNORE INTO race_results
                    (race_id, horse_id, horse_name, rank, sex_age,
                     weight_carried, jockey, finish_time, margin,
                     popularity, win_odds, horse_weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    race.race_id, r.horse_id, r.horse_name, r.rank,
                    r.sex_age, r.weight_carried, r.jockey,
                    r.finish_time, r.margin, r.popularity,
                    r.win_odds, r.horse_weight,
                ),
            )

    logger.info("DB 保存完了: race_id=%s, %d 頭", race.race_id, len(race.results))


def insert_prediction(
    conn: sqlite3.Connection,
    race_id: str,
    model_type: str,
    bet_type: str,
    horses: list[dict],
    *,
    confidence: float | None = None,
    expected_value: float | None = None,
    recommended_bet: float | None = None,
    notes: str | None = None,
) -> int:
    """
    予想を predictions + prediction_horses に保存して prediction_id を返す。

    Args:
        race_id:          対象レース ID
        model_type:       '卍' または '本命'
        bet_type:         '単勝' / '馬連' / '三連複' / 'WIN5' 等
        horses:           [{"horse_id": ..., "horse_name": ...,
                            "predicted_rank": 1, "model_score": 0.85,
                            "ev_score": 1.23}, ...]
        confidence:       モデル信頼度 0.0〜1.0
        expected_value:   期待値（卍モデルの主指標）
        recommended_bet:  推奨購入金額（円）
        notes:            根拠メモ

    Returns:
        新規 prediction.id
    """
    if model_type not in ("卍", "本命"):
        raise ValueError(f"model_type は '卍' または '本命' を指定してください: {model_type!r}")

    with conn:
        cur = conn.execute(
            """
            INSERT INTO predictions
                (race_id, model_type, bet_type, confidence,
                 expected_value, recommended_bet, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (race_id, model_type, bet_type, confidence,
             expected_value, recommended_bet, notes),
        )
        prediction_id = cur.lastrowid

        for h in horses:
            conn.execute(
                """
                INSERT INTO prediction_horses
                    (prediction_id, horse_id, horse_name,
                     predicted_rank, model_score, ev_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    h.get("horse_id"),
                    h["horse_name"],
                    h.get("predicted_rank"),
                    h.get("model_score"),
                    h.get("ev_score"),
                ),
            )

    logger.info(
        "予想保存: prediction_id=%d race_id=%s model=%s bet=%s",
        prediction_id, race_id, model_type, bet_type,
    )
    return prediction_id  # type: ignore[return-value]


def record_prediction_result(
    conn: sqlite3.Connection,
    prediction_id: int,
    is_hit: bool,
    payout: float = 0.0,
    recommended_bet: float | None = None,
) -> None:
    """
    予想の的中・払戻実績を prediction_results に記録する。

    Args:
        prediction_id:    対象 prediction.id
        is_hit:           的中したか
        payout:           払戻金額（円）
        recommended_bet:  購入金額（None の場合 predictions.recommended_bet を参照）
    """
    if recommended_bet is None:
        row = conn.execute(
            "SELECT recommended_bet FROM predictions WHERE id = ?",
            (prediction_id,),
        ).fetchone()
        recommended_bet = (row[0] or 0.0) if row else 0.0

    profit = payout - recommended_bet
    roi    = (payout / recommended_bet * 100) if recommended_bet else None

    with conn:
        conn.execute(
            """
            INSERT INTO prediction_results
                (prediction_id, is_hit, payout, profit, roi)
            VALUES (?, ?, ?, ?, ?)
            """,
            (prediction_id, int(is_hit), payout, profit, roi),
        )

    logger.info(
        "実績記録: prediction_id=%d is_hit=%s payout=%.0f roi=%s%%",
        prediction_id, is_hit, payout,
        f"{roi:.1f}" if roi is not None else "N/A",
    )


def refresh_model_performance(
    conn: sqlite3.Connection,
    model_type: str,
    year: int,
    month: int | None = None,
    venue: str | None = None,
    bet_type: str = "ALL",
) -> None:
    """
    model_performance テーブルを集計・更新する。

    Args:
        model_type: '卍' または '本命'
        year:       集計対象年
        month:      集計対象月（None = 年間集計）
        venue:      開催場所（None = 全場集計）
        bet_type:   馬券種（'ALL' = 全種別合算）
    """
    # 集計クエリ
    # None → センチネル値（0 / ''）に正規化してクエリ・UPSERT 両方で統一
    month_key = month if month is not None else 0
    venue_key = venue if venue is not None else ""

    where_clauses = ["p.model_type = ?", "substr(r.date,1,4) = ?"]
    params: list = [model_type, str(year)]

    if month_key:
        where_clauses.append("CAST(substr(r.date,6,2) AS INTEGER) = ?")
        params.append(month_key)
    if venue_key:
        where_clauses.append("r.venue = ?")
        params.append(venue_key)
    if bet_type != "ALL":
        where_clauses.append("p.bet_type = ?")
        params.append(bet_type)

    where = " AND ".join(where_clauses)

    row = conn.execute(
        f"""
        SELECT
            COUNT(pr.id)              AS total_bets,
            SUM(pr.is_hit)            AS hits,
            SUM(p.recommended_bet)    AS total_invested,
            SUM(pr.payout)            AS total_payout
        FROM predictions p
        JOIN races r             ON p.race_id = r.race_id
        LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE {where}
        """,
        params,
    ).fetchone()

    if not row or row[0] == 0:
        return

    total_bets, hits, invested, payout = row
    hit_rate = (hits / total_bets * 100) if total_bets else None
    roi      = (payout / invested * 100)  if invested  else None

    with conn:
        conn.execute(
            """
            INSERT INTO model_performance
                (model_type, bet_type, year, month, venue,
                 total_bets, hits, hit_rate,
                 total_invested, total_payout, roi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_type, bet_type, year, month, venue) DO UPDATE SET
                total_bets     = excluded.total_bets,
                hits           = excluded.hits,
                hit_rate       = excluded.hit_rate,
                total_invested = excluded.total_invested,
                total_payout   = excluded.total_payout,
                roi            = excluded.roi,
                updated_at     = datetime('now', 'localtime')
            """,
            (model_type, bet_type, year, month_key, venue_key,
             total_bets, hits, hit_rate, invested, payout, roi),
        )

    logger.info(
        "成績更新: model=%s year=%d month=%s venue=%s "
        "hit_rate=%.1f%% roi=%.1f%%",
        model_type, year, month, venue,
        hit_rate or 0, roi or 0,
    )


def insert_entries(
    conn: sqlite3.Connection,
    race_id: str,
    entries: list["EntryHorse"],  # type: ignore[name-defined]
) -> int:
    """
    出馬表データを entries テーブルに保存する（UPSERT）。

    Args:
        conn:    DB コネクション
        race_id: 対象レース ID
        entries: EntryHorse のリスト

    Returns:
        保存した件数
    """
    count = 0
    with conn:
        for e in entries:
            conn.execute(
                """
                INSERT INTO entries
                    (race_id, horse_number, gate_number, horse_id, horse_name,
                     sex_age, weight_carried, jockey, trainer,
                     horse_weight, horse_weight_diff)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_id, horse_number) DO UPDATE SET
                    gate_number       = excluded.gate_number,
                    horse_id          = COALESCE(excluded.horse_id, horse_id),
                    horse_name        = excluded.horse_name,
                    sex_age           = excluded.sex_age,
                    weight_carried    = excluded.weight_carried,
                    jockey            = excluded.jockey,
                    trainer           = excluded.trainer,
                    horse_weight      = excluded.horse_weight,
                    horse_weight_diff = excluded.horse_weight_diff,
                    scraped_at        = datetime('now', 'localtime')
                """,
                (
                    race_id,
                    e.horse_number,
                    e.gate_number,
                    e.horse_id,
                    e.horse_name,
                    e.sex_age,
                    e.weight_carried,
                    e.jockey,
                    e.trainer,
                    e.horse_weight,
                    e.horse_weight_diff,
                ),
            )
            count += 1

    logger.info("出馬表保存: race_id=%s, %d 頭", race_id, count)
    return count


def insert_realtime_odds(
    conn: sqlite3.Connection,
    race_id: str,
    odds_list: list["HorseOdds"],  # type: ignore[name-defined]
    horse_name_map: dict[int, str] | None = None,
) -> int:
    """
    リアルタイムオッズのスナップショットを realtime_odds テーブルに追記する。

    各呼び出しごとに新規行を追加する（上書きではなく履歴保持）。

    Args:
        conn:           DB コネクション
        race_id:        対象レース ID
        odds_list:      HorseOdds のリスト
        horse_name_map: {馬番: 馬名} の辞書（entries テーブルから引いておくと確実）

    Returns:
        保存した件数
    """
    horse_name_map = horse_name_map or {}
    count = 0
    with conn:
        for o in odds_list:
            conn.execute(
                """
                INSERT INTO realtime_odds
                    (race_id, horse_number, horse_name,
                     win_odds, place_odds_min, place_odds_max, popularity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    race_id,
                    o.horse_number,
                    horse_name_map.get(o.horse_number, ""),
                    o.win_odds,
                    o.place_odds_min,
                    o.place_odds_max,
                    o.popularity,
                ),
            )
            count += 1

    logger.info("オッズ保存: race_id=%s, %d 頭", race_id, count)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = init_db()
    conn.close()
    print("umalogi.db の初期化が完了しました。")
