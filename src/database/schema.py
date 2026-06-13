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
        post_time       TEXT    NOT NULL DEFAULT '',  -- 実発走時刻 HH:MM（空=推定にフォールバック）
        grade           TEXT    NOT NULL DEFAULT '',  -- グレード/クラス: G1/G2/G3/OP/L/3勝/2勝/1勝/未勝利/新馬（W-088・U scoreクラス変化）
        status          TEXT    NOT NULL DEFAULT 'valid',  -- valid / error（破損・幽霊行の論理隔離。W-089・条項4論理削除）
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
        last_3f           REAL,                           -- 上がり3F秒数（migration additive同期）
        created_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
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
        jockey_code        TEXT,              -- 騎手コード（W-076・マスタ結合用）
        trainer_code       TEXT,              -- 調教師コード（W-076・マスタ結合用）
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
        is_superseded   INTEGER NOT NULL DEFAULT 0,  -- 1=再推論で論理無効化（評価/ROI除外）
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
        shap_json      TEXT,               -- SHAP根拠JSON（migration #18 と同期）
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
    # win5_results: WIN5 確定結果（的中馬番5つ＋払戻金額）
    """
    CREATE TABLE IF NOT EXISTS win5_results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        race_date       TEXT    NOT NULL UNIQUE,  -- YYYY-MM-DD
        race_ids        TEXT    NOT NULL,          -- JSON ["202501010101", ...]
        winning_numbers TEXT    NOT NULL,          -- JSON [3, 7, 12, 1, 9] (各レース1着馬番)
        payout          INTEGER NOT NULL DEFAULT 0, -- 払戻金額（100円あたり・円）
        scraped_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_win5r_date ON win5_results(race_date)",
    # ================================================================
    # ── ビュー ───────────────────────────────────────────────────────
    # ================================================================
    # 階層型収支分析ビュー（Analytics ドリルダウン用）
    # 粒度: 日 × モデル × 券種
    """
    CREATE VIEW IF NOT EXISTS v_analytics AS
    SELECT
        r.date,
        substr(r.date, 1, 4)  AS year,
        substr(r.date, 6, 2)  AS month,
        substr(r.date, 9, 2)  AS day,
        r.venue,
        r.surface,
        p.model_type,
        p.bet_type,
        COUNT(*)                                              AS total_bets,
        SUM(CASE WHEN pr.is_hit = 1 THEN 1 ELSE 0 END)      AS hits,
        SUM(COALESCE(pr.payout,  0))                         AS total_payout,
        SUM(COALESCE(pr.profit,  0))                         AS total_profit
    FROM predictions p
    JOIN  races r              ON r.race_id = p.race_id
    LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
    WHERE pr.is_hit IS NOT NULL
    GROUP BY r.date, p.model_type, p.bet_type
    """,
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
    # horse_number による部分ユニークインデックス（UNIQUE(race_id, horse_name)の後継）
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_rr_unique_horsenum ON race_results(race_id, horse_number) WHERE horse_number IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_rp_race_bet         ON race_payouts(race_id, bet_type)",
    # ── 確定P&L集計 / W-057 シャドーA/B / 実弾ROI（pnl_accounting・evaluator）最適化 ──
    # predictions の WHERE(is_superseded, created_at) + GROUP(model_type, bet_type) を1本でカバー。
    "CREATE INDEX IF NOT EXISTS idx_pred_ab ON predictions(is_superseded, created_at, model_type, bet_type)",
    # prediction_results を JOIN キー込みのカバリングインデックス化（payout/profit/is_hit を
    # テーブル本体に触れず読み出し、数千〜数万行の集計を高速化）。
    "CREATE INDEX IF NOT EXISTS idx_pred_r_cover ON prediction_results(prediction_id, payout, profit, is_hit)",
    # 速報オッズの「レース×馬番ごと最新スナップショット」(_latest_odds_map / Pure_EV) 取得用。
    "CREATE INDEX IF NOT EXISTS idx_odds_race_horse_rec ON realtime_odds(race_id, horse_number, recorded_at DESC)",
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
    # ================================================================
    # ── 運用層 ────────────────────────────────────────────────────
    # ================================================================
    # batch_runs: 週末バッチの実行ログ（Pre/Post 両フェーズ）
    """
    CREATE TABLE IF NOT EXISTS batch_runs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date    TEXT    NOT NULL,               -- YYYY-MM-DD
        phase       TEXT    NOT NULL,               -- 'pre' | 'post'
        status      TEXT    NOT NULL DEFAULT 'running',  -- 'running'|'success'|'partial'|'failed'
        note_ok     INTEGER NOT NULL DEFAULT 0,     -- note 下書き保存成功
        umanity_ok  INTEGER NOT NULL DEFAULT 0,     -- ウマニティ投稿成功件数
        x_ok        INTEGER NOT NULL DEFAULT 0,     -- X 投稿成功件数
        error_msg   TEXT,
        started_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
        finished_at TEXT,
        UNIQUE (run_date, phase)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_batch_runs_date ON batch_runs(run_date)",
    # ================================================================
    # ── X（旧Twitter）世論分析層（2026-05-18 Phase A）
    # ── 社長明示指令により日曜実装（週末凍結ルール例外適用）
    # ================================================================
    # x_accounts: 監視対象の競馬予想家アカウントマスタ
    """
    CREATE TABLE IF NOT EXISTS x_accounts (
        screen_name     TEXT    PRIMARY KEY,
        display_name    TEXT    NOT NULL DEFAULT '',
        follower_count  INTEGER NOT NULL DEFAULT 0,
        hit_rate_30d    REAL,                       -- 直近30日的中率（0〜1）
        weight          REAL    NOT NULL DEFAULT 1.0,  -- EV計算時の重み（的中率で動的調整）
        is_active       INTEGER NOT NULL DEFAULT 1, -- 監視対象フラグ
        last_scraped_at TEXT,
        created_at      TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
        updated_at      TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
    )
    """,
    # x_signals: 予想家ポストから抽出した馬番シグナル
    """
    CREATE TABLE IF NOT EXISTS x_signals (
        signal_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id        TEXT    NOT NULL UNIQUE,    -- X の tweet ID（重複排除）
        race_id         TEXT    REFERENCES races(race_id),  -- races テーブルと突合後に設定
        screen_name     TEXT    NOT NULL REFERENCES x_accounts(screen_name),
        horse_number    INTEGER,                    -- 抽出した馬番（NULL=未抽出）
        signal_type     TEXT,                       -- 'honmei'|'ana'|'keshi'|NULL
        confidence      REAL    NOT NULL DEFAULT 0.5,  -- 0.0〜1.0（LLM推定）
        race_name_raw   TEXT,                       -- 元テキストから抽出したレース名
        raw_text        TEXT    NOT NULL,
        posted_at       TEXT    NOT NULL,           -- ISO 8601
        fetched_at      TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
        parsed          INTEGER NOT NULL DEFAULT 0  -- x_signal_parser 処理済みフラグ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_x_signals_race_id     ON x_signals(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_x_signals_posted_at   ON x_signals(posted_at)",
    "CREATE INDEX IF NOT EXISTS idx_x_signals_screen_name ON x_signals(screen_name)",
    "CREATE INDEX IF NOT EXISTS idx_x_signals_parsed      ON x_signals(parsed) WHERE parsed=0",
    # x_accounts_history: アカウント別予想精度の蓄積テーブル（Phase C 2026-05-20）
    # signal 1件ごとに評価結果を記録し、アカウント重みの動的更新に使用する。
    # FK は参照整合性より可用性を優先しアプリ側で担保する（テスト容易性向上）。
    """
    CREATE TABLE IF NOT EXISTS x_accounts_history (
        history_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        screen_name     TEXT    NOT NULL,           -- x_accounts.screen_name 参照（アプリ側で整合）
        race_id         TEXT    NOT NULL,           -- races.race_id 参照
        horse_number    INTEGER NOT NULL,
        signal_type     TEXT    NOT NULL,           -- 'honmei'/'ana'/'keshi'
        confidence      REAL    NOT NULL DEFAULT 0.5,
        consensus_score REAL,                       -- get_x_consensus_score() の出力値
        win_odds        REAL,                       -- 発走時単勝オッズ
        final_rank      INTEGER,                    -- 着順（NULL=未確定）
        is_hit          INTEGER,                    -- 1=的中/0=外れ/NULL=未評価
        payout          REAL,                       -- 実際の払戻金額
        roi             REAL,                       -- 払戻 / (自己 confidence × 想定賭け金)
        evaluated_at    TEXT,                       -- 評価完了日時 (ISO 8601)
        created_at      TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_x_ah_screen_name ON x_accounts_history(screen_name)",
    "CREATE INDEX IF NOT EXISTS idx_x_ah_race_id     ON x_accounts_history(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_x_ah_evaluated   ON x_accounts_history(evaluated_at) WHERE evaluated_at IS NOT NULL",
    # ── Phase 2 EV インデックス（等値述語列レフトモスト準拠） ─────────────────────────
    # model_type（=）+ expected_value 降順 → EV閾値フィルタ・ソートを完全カバー
    "CREATE INDEX IF NOT EXISTS idx_pred_model_ev    ON predictions(model_type, expected_value DESC)",
    # race_id（=）+ model_type → プレレース通知の予想検索を高速化
    "CREATE INDEX IF NOT EXISTS idx_pred_race_model  ON predictions(race_id, model_type)",
    # horse_id（=）+ training_date 降順 → 特徴量生成の調教データ取得を高速化
    "CREATE INDEX IF NOT EXISTS idx_tc_horse_date    ON training_times(horse_id, training_date DESC)",
    # horse_id（=）+ training_date 降順 → 坂路調教データ取得を高速化
    "CREATE INDEX IF NOT EXISTS idx_hc_horse_date    ON training_hillwork(horse_id, training_date DESC)",
    # horse_id（=）+ race_id → 馬の過去成績履歴取得を高速化
    "CREATE INDEX IF NOT EXISTS idx_rr_horse_race    ON race_results(horse_id, race_id)",
    # prediction_id（=）+ is_hit → 回収率・的中統計クエリを高速化
    "CREATE INDEX IF NOT EXISTS idx_pr_pred_hit      ON prediction_results(prediction_id, is_hit)",
    # ── umasugi_engine Phase2: オッズ時系列テーブル ──────────────────────────────────
    """
CREATE TABLE IF NOT EXISTS odds_timeseries (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        TEXT    NOT NULL,
    horse_number   INTEGER NOT NULL,
    win_odds       REAL,
    place_odds_min REAL,
    place_odds_max REAL,
    popularity     INTEGER,
    recorded_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_ots_race_horse ON odds_timeseries(race_id, horse_number)",
    "CREATE INDEX IF NOT EXISTS idx_ots_recorded_at ON odds_timeseries(recorded_at)",
    # ── umasugi_engine Phase3: パドック気配メモ ──────────────────────────────────
    """
CREATE TABLE IF NOT EXISTS paddock_notes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id       TEXT    NOT NULL,
    horse_number  INTEGER,            -- NULL = レース全体へのメモ
    comment       TEXT    NOT NULL,
    boost_factor  REAL    NOT NULL DEFAULT 0.0,  -- -0.05〜+0.05 (キーワード解析結果)
    source        TEXT    NOT NULL DEFAULT 'discord',
    created_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_pn_race_id      ON paddock_notes(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_pn_race_horse   ON paddock_notes(race_id, horse_number)",
    # ── umasugi_engine Phase3: 騎手コース別成績 ─────────────────────────────────
    """
CREATE TABLE IF NOT EXISTS jockey_stats (
    jockey_name       TEXT    NOT NULL,
    venue             TEXT    NOT NULL,  -- 東京 / 中山 / 阪神 等
    surface           TEXT    NOT NULL,  -- 芝 / ダート
    total_races       INTEGER NOT NULL DEFAULT 0,
    wins              INTEGER NOT NULL DEFAULT 0,
    win_rate          REAL    NOT NULL DEFAULT 0.0,
    place_rate        REAL    NOT NULL DEFAULT 0.0,  -- 3着内率
    last_30d_races    INTEGER NOT NULL DEFAULT 0,
    last_30d_wins     INTEGER NOT NULL DEFAULT 0,
    last_30d_win_rate REAL    NOT NULL DEFAULT 0.0,
    updated_at        TEXT    NOT NULL DEFAULT (date('now')),
    PRIMARY KEY (jockey_name, venue, surface)
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_js_jockey ON jockey_stats(jockey_name)",
    # ── umasugi_engine Phase3: 調教師コース別成績 ────────────────────────────────
    """
CREATE TABLE IF NOT EXISTS trainer_stats (
    trainer_name      TEXT    NOT NULL,
    venue             TEXT    NOT NULL,
    surface           TEXT    NOT NULL,
    total_races       INTEGER NOT NULL DEFAULT 0,
    wins              INTEGER NOT NULL DEFAULT 0,
    win_rate          REAL    NOT NULL DEFAULT 0.0,
    place_rate        REAL    NOT NULL DEFAULT 0.0,
    last_30d_races    INTEGER NOT NULL DEFAULT 0,
    last_30d_wins     INTEGER NOT NULL DEFAULT 0,
    last_30d_win_rate REAL    NOT NULL DEFAULT 0.0,
    updated_at        TEXT    NOT NULL DEFAULT (date('now')),
    PRIMARY KEY (trainer_name, venue, surface)
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_ts_trainer ON trainer_stats(trainer_name)",
    # ── マルチ券種オッズ履歴 ─────────────────────────────────────────────
    """
CREATE TABLE IF NOT EXISTS multi_odds (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id      TEXT    NOT NULL,
    bet_type     TEXT    NOT NULL CHECK (bet_type IN ('枠連','馬連','ワイド','馬単','三連複','三連単')),
    combination  TEXT    NOT NULL,
    odds         REAL,
    odds_max     REAL,
    popularity   INTEGER,
    recorded_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE (race_id, bet_type, combination, recorded_at)
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_mo_race_bet ON multi_odds(race_id, bet_type)",
    "CREATE INDEX IF NOT EXISTS idx_mo_race_id  ON multi_odds(race_id)",
    # ── 調教評価（netkeiba oikiri.html 由来） ─────────────────────────────────────
    # W-068/W-072: training_scraper.py が取得する調教グレード(A/B/C)・寸評テキスト
    """
CREATE TABLE IF NOT EXISTS training_evaluations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id      TEXT    NOT NULL,
    horse_id     TEXT    NOT NULL,
    horse_name   TEXT    NOT NULL DEFAULT '',
    horse_number INTEGER NOT NULL DEFAULT 0,
    eval_text    TEXT    NOT NULL DEFAULT '',   -- 寸評テキスト
    eval_grade   TEXT    NOT NULL DEFAULT '',   -- A / B / C / D
    source_date  TEXT    NOT NULL DEFAULT '',   -- 取得日 YYYY-MM-DD
    created_at   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(race_id, horse_id)
)
    """,
    "CREATE INDEX IF NOT EXISTS idx_te_race_id  ON training_evaluations(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_te_horse_id ON training_evaluations(horse_id)",
]
