"""
UMALOGI DB スキーマ定義

全テーブル・インデックス・ビューの DDL をここに一元管理する。
init_db.py はこのリストを参照して CREATE TABLE / CREATE INDEX / CREATE VIEW を実行する。

変更手順:
  1. このファイルに DDL を追記／修正する。
  2. 既存テーブルの変更は init_db.py に _migrate_*() 関数を追加して実行する。
  3. ビューの再定義は _migrate_recreate_mart_view() が自動で行う。
"""

DDL_STATEMENTS: list[str] = [

    # ================================================================
    # ── データ層 ────────────────────────────────────────────────────
    # ================================================================

    """
    CREATE TABLE IF NOT EXISTS races (
        race_id         TEXT    PRIMARY KEY,
        race_name       TEXT    NOT NULL,
        date            TEXT    NOT NULL,       -- YYYY-MM-DD (ISO 8601)
        venue           TEXT    NOT NULL,
        race_number     INTEGER NOT NULL,
        distance        INTEGER NOT NULL,
        surface         TEXT    NOT NULL,       -- 芝 / ダート
        track_direction TEXT    NOT NULL DEFAULT '',  -- 右 / 左 / 右外 / 左外 / 直線
        weather         TEXT    NOT NULL DEFAULT '',
        condition       TEXT    NOT NULL DEFAULT '',
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
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
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id           TEXT    NOT NULL REFERENCES races(race_id),
        horse_id          TEXT    REFERENCES horses(horse_id),
        horse_name        TEXT    NOT NULL,
        rank              INTEGER,
        gate_number       INTEGER,                        -- 枠番
        horse_number      INTEGER,                        -- 馬番
        sex_age           TEXT    NOT NULL DEFAULT '',
        weight_carried    REAL    NOT NULL DEFAULT 0,
        jockey            TEXT    NOT NULL DEFAULT '',
        trainer           TEXT    NOT NULL DEFAULT '',    -- 調教師
        finish_time       TEXT,
        margin            TEXT,
        popularity        INTEGER,
        win_odds          REAL,
        horse_weight      INTEGER,
        horse_weight_diff INTEGER,                        -- 馬体重増減（例: +2, -4）
        created_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
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
        model_type      TEXT    NOT NULL,
                                             -- 卍/本命 + オプション suffix (暫定)/(直前)
        bet_type        TEXT    NOT NULL,    -- 単勝/複勝/馬連/馬単/三連複/三連単/WIN5
        confidence      REAL,               -- モデル信頼度 0.0〜1.0
        expected_value  REAL,               -- 期待値（卍モデルの主指標）
        recommended_bet REAL,               -- 推奨購入金額（Kelly最適化後）
        notes           TEXT,               -- 根拠メモ（血統・オッズ歪み等）
        combination_json TEXT,              -- 買い目組合せ JSON [[1,5],[1,7],...]
        created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(race_id, model_type, bet_type)
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
        model_type     TEXT    NOT NULL,  -- 卍/本命 + オプション suffix (暫定)/(直前)
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
    "CREATE INDEX IF NOT EXISTS idx_races_year_venue  ON races(substr(date,1,4), venue)",

    "CREATE INDEX IF NOT EXISTS idx_results_race_id   ON race_results(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_results_horse_id  ON race_results(horse_id)",
    "CREATE INDEX IF NOT EXISTS idx_results_rank      ON race_results(rank)",

    # 予想層 ---
    "CREATE INDEX IF NOT EXISTS idx_pred_race_id      ON predictions(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_model_type   ON predictions(model_type)",
    "CREATE INDEX IF NOT EXISTS idx_pred_created_at   ON predictions(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_pred_bet_type     ON predictions(bet_type)",

    "CREATE INDEX IF NOT EXISTS idx_pred_h_pred_id    ON prediction_horses(prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_h_horse_id   ON prediction_horses(horse_id)",

    "CREATE INDEX IF NOT EXISTS idx_pred_r_pred_id    ON prediction_results(prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_pred_r_is_hit     ON prediction_results(is_hit)",

    # 集計層 ---
    "CREATE INDEX IF NOT EXISTS idx_mperf_type_year   ON model_performance(model_type, year, month)",
    "CREATE INDEX IF NOT EXISTS idx_mperf_venue       ON model_performance(model_type, venue)",

    # ================================================================
    # ── 払戻層 ────────────────────────────────────────────────────────
    # ================================================================

    # race_payouts: レース確定払戻（netkeiba pay_table_01 から取得）
    """
    CREATE TABLE IF NOT EXISTS race_payouts (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id     TEXT    NOT NULL REFERENCES races(race_id),
        bet_type    TEXT    NOT NULL,
        combination TEXT    NOT NULL,  -- "14" / "7-14" / "14→7→16"
        payout      INTEGER NOT NULL,  -- 払戻金額（100円あたり）
        popularity  INTEGER,           -- 人気（複勝/ワイドは複数行あり）
        UNIQUE(race_id, bet_type, combination)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_payouts_race_id  ON race_payouts(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_payouts_bet_type ON race_payouts(race_id, bet_type)",

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

    # ================================================================
    # ── JRA-VAN マスタ層 (WOOD / BLOD / DIFN dataspec) ─────────────
    # ================================================================

    """
    CREATE TABLE IF NOT EXISTS training_times (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        horse_id       TEXT    NOT NULL,
        horse_name     TEXT    NOT NULL DEFAULT '',
        training_date  TEXT    NOT NULL,
        venue_code     TEXT    NOT NULL DEFAULT '',
        course_type    TEXT    NOT NULL DEFAULT '',
        direction      TEXT    NOT NULL DEFAULT '',
        time_4f        REAL,
        time_3f        REAL,
        time_2f        REAL,
        time_1f        REAL,
        lap_time       REAL,
        gear           TEXT    NOT NULL DEFAULT '',
        jockey_code    TEXT    NOT NULL DEFAULT '',
        jockey_name    TEXT    NOT NULL DEFAULT '',
        data_date      TEXT    NOT NULL DEFAULT '',
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(horse_id, training_date, course_type, direction)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS training_hillwork (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        horse_id       TEXT    NOT NULL,
        horse_name     TEXT    NOT NULL DEFAULT '',
        training_date  TEXT    NOT NULL,
        time_4f        REAL,
        time_3f        REAL,
        time_2f        REAL,
        time_1f        REAL,
        lap_time       REAL,
        gear           TEXT    NOT NULL DEFAULT '',
        jockey_code    TEXT    NOT NULL DEFAULT '',
        jockey_name    TEXT    NOT NULL DEFAULT '',
        data_date      TEXT    NOT NULL DEFAULT '',
        created_at     TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
        UNIQUE(horse_id, training_date)
    )
    """,

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

    """
    CREATE TABLE IF NOT EXISTS racehorses (
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

    # ================================================================
    # ── PERFORMANCE_INDEXES（AI特徴量生成クエリ最適化）──────────────
    # ================================================================

    "CREATE INDEX IF NOT EXISTS idx_races_date_venue    ON races(date, venue)",
    "CREATE INDEX IF NOT EXISTS idx_races_surface_dist  ON races(surface, distance)",

    "CREATE INDEX IF NOT EXISTS idx_rr_horse_raceid     ON race_results(horse_id, race_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rr_jockey_raceid    ON race_results(jockey, race_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rr_trainer_raceid   ON race_results(trainer, race_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rr_race_rank        ON race_results(race_id, rank)",

    "CREATE INDEX IF NOT EXISTS idx_rp_race_bet         ON race_payouts(race_id, bet_type)",

    "CREATE INDEX IF NOT EXISTS idx_racehorses_father   ON racehorses(father_id)",
    "CREATE INDEX IF NOT EXISTS idx_racehorses_name     ON racehorses(horse_name)",
    "CREATE INDEX IF NOT EXISTS idx_jockeys_name        ON jockeys(jockey_name)",
    "CREATE INDEX IF NOT EXISTS idx_trainers_name       ON trainers(trainer_name)",
    "CREATE INDEX IF NOT EXISTS idx_tc_horse_date       ON training_times(horse_id, training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hc_horse_date       ON training_hillwork(horse_id, training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_tc_norm  ON training_times(substr(horse_id,2,9), training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hc_norm  ON training_hillwork(substr(horse_id,2,9), training_date DESC)",
    # v_race_mart の相関サブクエリ用部分インデックス（小型化して応答改善）
    "CREATE INDEX IF NOT EXISTS idx_tc_mart ON training_times(substr(horse_id,2,9), training_date DESC) WHERE training_date != ''",
    "CREATE INDEX IF NOT EXISTS idx_hc_mart ON training_hillwork(substr(horse_id,2,9), training_date DESC) WHERE training_date != ''",
    "CREATE INDEX IF NOT EXISTS idx_foals_father        ON foals(father_id)",

    # ================================================================
    # ── v_race_mart: AI学習用フラットビュー ─────────────────────────
    # ================================================================
    """
    CREATE VIEW IF NOT EXISTS v_race_mart AS
    SELECT
        r.race_id,
        r.date,
        substr(r.date, 1, 4)    AS year,
        substr(r.date, 6, 2)    AS month,
        r.venue,
        r.race_number,
        r.distance,
        r.surface,
        r.track_direction,
        r.condition,
        r.weather,

        rr.id                   AS result_id,
        rr.horse_id,
        rr.horse_number,
        rr.gate_number,
        rr.horse_name,
        rr.sex_age,
        rr.rank,
        rr.win_odds,
        rr.popularity,
        rr.finish_time,
        rr.horse_weight,
        rr.horse_weight_diff,
        rr.weight_carried,
        rr.jockey,
        rr.trainer,

        rp_tan.payout           AS payout_tansho,
        rp_fuk.payout           AS payout_fukusho,

        h.sire,
        h.dam,
        h.dam_sire,

        um.birth_year,
        um.sex                  AS um_sex,
        um.coat_color,
        um.country,
        um.father_id,
        um.father_name,
        um.grandsire_id,
        um.grandsire_name,
        um.east_west            AS horse_east_west,

        ks.jockey_code,
        ks.east_west            AS jockey_east_west,
        ks.license_year         AS jockey_license_year,

        ch.trainer_code,
        ch.east_west            AS trainer_east_west,
        ch.stable_name,

        bt.country              AS father_country,
        bt.birth_year           AS father_birth_year,
        bt.father_id            AS father_sire_id,
        bt.father_name          AS father_sire_name,
        bt.mother_id            AS father_dam_id,
        bt.mother_name          AS father_dam_name,

        tc.training_date        AS last_tc_date,
        tc.time_4f              AS last_tc_4f,
        tc.time_3f              AS last_tc_3f,
        tc.lap_time             AS last_tc_lap,
        tc.course_type          AS last_tc_course,
        tc.gear                 AS last_tc_gear,

        hc.training_date        AS last_hc_date,
        hc.time_4f              AS last_hc_4f,
        hc.time_3f              AS last_hc_3f,
        hc.lap_time             AS last_hc_lap,
        hc.gear                 AS last_hc_gear

    FROM races r

    JOIN  race_results rr
          ON  rr.race_id = r.race_id

    LEFT JOIN race_payouts rp_tan
          ON  rp_tan.race_id    = r.race_id
          AND rp_tan.bet_type   = '単勝'
          AND rp_tan.combination = CAST(rr.horse_number AS TEXT)

    LEFT JOIN race_payouts rp_fuk
          ON  rp_fuk.race_id    = r.race_id
          AND rp_fuk.bet_type   = '複勝'
          AND rp_fuk.combination = CAST(rr.horse_number AS TEXT)

    LEFT JOIN horses h
          ON  h.horse_id = rr.horse_id

    LEFT JOIN racehorses um
          ON  um.horse_id = rr.horse_id

    LEFT JOIN jockeys ks
          ON  ks.jockey_name = rr.jockey

    LEFT JOIN trainers ch
          ON  ch.trainer_name = rr.trainer

    LEFT JOIN breeding_horses bt
          ON  bt.horse_id = um.father_id
          AND um.father_id != ''

    LEFT JOIN training_times tc
          ON  rr.horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
          AND substr(tc.horse_id,2,9) = substr(rr.horse_id,1,4)||substr(rr.horse_id,5,5)
          AND tc.training_date = (
              SELECT MAX(t2.training_date)
              FROM   training_times t2
              WHERE  substr(t2.horse_id,2,9) = substr(rr.horse_id,1,4)||substr(rr.horse_id,5,5)
              AND    t2.training_date < r.date
              AND    t2.training_date != ''
          )

    LEFT JOIN training_hillwork hc
          ON  rr.horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
          AND substr(hc.horse_id,2,9) = substr(rr.horse_id,1,4)||substr(rr.horse_id,5,5)
          AND hc.training_date = (
              SELECT MAX(h2.training_date)
              FROM   training_hillwork h2
              WHERE  substr(h2.horse_id,2,9) = substr(rr.horse_id,1,4)||substr(rr.horse_id,5,5)
              AND    h2.training_date < r.date
              AND    h2.training_date != ''
          )
    """,
]
