"""
SQLite データベース初期化スクリプト

テーブル構成:
  ── データ層 ──────────────────────────────────────────────────
  races              - レース基本情報
  horses             - 馬マスタ（血統情報含む）
  race_results       - レースごとの出走・着順結果
  race_payouts       - レース確定払戻
  entries            - 出馬表（レース前の出走登録情報）
  realtime_odds      - リアルタイムオッズ履歴

  ── JRA-VAN マスタ層 ───────────────────────────────────────
  training_times     - 調教タイム (TC / WOOD dataspec)
  training_hillwork  - 坂路調教  (HC / WOOD dataspec)
  breeding_horses    - 繁殖馬マスタ (BT / BLOD dataspec)
  foals              - 産駒マスタ   (HN / BLOD dataspec)
  racehorses         - 競走馬マスタ (UM / DIFN dataspec)
  jockeys            - 騎手マスタ   (KS / DIFN dataspec)
  trainers           - 調教師マスタ (CH / DIFN dataspec)

  ── 予想層 ────────────────────────────────────────────────────
  predictions        - 卍/本命モデルの予想バッチ（1レース×1馬券種）
  prediction_horses  - 予想に含まれる馬と個別スコア
  prediction_results - 的中・払戻の実績

  ── 集計層 ────────────────────────────────────────────────────
  model_performance  - モデル別 年/月/会場 累積成績

  ── ビュー ────────────────────────────────────────────────────
  v_prediction_summary - 予想 × レース × 結果の結合ビュー
  v_race_mart          - AI学習用フラットビュー（レース×馬×マスタ JOIN）
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
    # jravan_client.py の extend_db_schema() も同じ DDL を持つが、
    # CREATE TABLE IF NOT EXISTS は冪等なので二重実行しても安全。

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

    # ── races ──────────────────────────────────────────────────────
    # 特徴量生成では期間×会場絞り込みが最頻出
    "CREATE INDEX IF NOT EXISTS idx_races_date_venue    ON races(date, venue)",
    "CREATE INDEX IF NOT EXISTS idx_races_surface_dist  ON races(surface, distance)",

    # ── race_results ───────────────────────────────────────────────
    # 馬の直近N走取得: horse_id で絞り race_id 降順ソート
    "CREATE INDEX IF NOT EXISTS idx_rr_horse_raceid     ON race_results(horse_id, race_id DESC)",
    # 騎手・調教師の近走成績集計
    "CREATE INDEX IF NOT EXISTS idx_rr_jockey_raceid    ON race_results(jockey, race_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rr_trainer_raceid   ON race_results(trainer, race_id DESC)",
    # 着順フィルタ（的中評価・上位馬抽出）
    "CREATE INDEX IF NOT EXISTS idx_rr_race_rank        ON race_results(race_id, rank)",

    # ── race_payouts ───────────────────────────────────────────────
    # 単勝・複勝払戻の個別取得（v_race_mart の LEFT JOIN で使用）
    "CREATE INDEX IF NOT EXISTS idx_rp_race_bet         ON race_payouts(race_id, bet_type)",

    # ── マスタ層 ───────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_racehorses_father   ON racehorses(father_id)",
    "CREATE INDEX IF NOT EXISTS idx_racehorses_name     ON racehorses(horse_name)",
    "CREATE INDEX IF NOT EXISTS idx_jockeys_name        ON jockeys(jockey_name)",
    "CREATE INDEX IF NOT EXISTS idx_trainers_name       ON trainers(trainer_name)",
    "CREATE INDEX IF NOT EXISTS idx_tc_horse_date       ON training_times(horse_id, training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hc_horse_date       ON training_hillwork(horse_id, training_date DESC)",
    # 正規化キー（先頭1桁プレフィックスを除いた9桁）による調教結合用
    # race_results.horse_id (YYYY+SSSSSS) → substr(tc.horse_id,2,9) = YYYY+SSSSS で結合
    "CREATE INDEX IF NOT EXISTS idx_tc_norm  ON training_times(substr(horse_id,2,9), training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hc_norm  ON training_hillwork(substr(horse_id,2,9), training_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_foals_father        ON foals(father_id)",

    # ================================================================
    # ── v_race_mart: AI学習用フラットビュー ─────────────────────────
    # ================================================================
    # races × race_results × 払戻 × horses × 競走馬マスタ × 騎手 ×
    # 調教師 × 繁殖馬マスタ(父) × 直近調教 を 1行=1頭 に展開した
    # 平坦ビュー。LightGBM 特徴量生成の起点として使う。
    #
    # ┌─ JOIN キー設計 ───────────────────────────────────────────┐
    # │ テーブル          結合キー             補足               │
    # │ races             race_id (PK)                            │
    # │ race_results      race_id → races.race_id                 │
    # │ race_payouts      race_id + bet_type + 馬番(CAST TEXT)    │
    # │ horses            horse_id → race_results.horse_id        │
    # │ racehorses (um)   horse_id → race_results.horse_id        │
    # │ jockeys (ks)      jockey_name → race_results.jockey       │
    # │                   ※ race_results に jockey_code 列なし   │
    # │                   ※ jockey_code は SELECT で露出          │
    # │ trainers (ch)     trainer_name → race_results.trainer     │
    # │                   ※ trainer_code は SELECT で露出         │
    # │ breeding_horses   horse_id → racehorses.father_id         │
    # │   (bt)            父の繁殖情報(産地/生年/祖父/BMS)を取得  │
    # │ training_times    substr(horse_id,2,9) = YYYY+seq5で結合   │
    # │ training_hillwork 同上                                    │
    # └───────────────────────────────────────────────────────────┘
    #
    # 定義変更時は _migrate_recreate_mart_view() を呼ぶこと（init_db内で自動実行）。
    """
    CREATE VIEW IF NOT EXISTS v_race_mart AS
    SELECT
        -- ── レース情報 (races) ──────────────────────────────────
        r.race_id,
        r.date,
        substr(r.date, 1, 4)    AS year,
        substr(r.date, 6, 2)    AS month,
        r.venue,
        r.race_number,
        r.distance,
        r.surface,              -- 芝 / ダート / 障害
        r.track_direction,      -- 右 / 左 / 直線 etc.
        r.condition,            -- 良 / 稍重 / 重 / 不良
        r.weather,

        -- ── 出走馬情報 (race_results) ──────────────────────────
        rr.id                   AS result_id,   -- 行を一意に識別するキー
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

        -- ── 払戻 (race_payouts) ────────────────────────────────
        -- 馬番(horse_number)を TEXT にキャストして combination と照合
        rp_tan.payout           AS payout_tansho,
        rp_fuk.payout           AS payout_fukusho,

        -- ── 血統情報 (horses / netkeiba scraper) ─────────────
        -- horse_id で結合。DIFN 未取得の馬もここで父/母/母父名を保持する
        h.sire,                 -- 父名（文字列）
        h.dam,                  -- 母名（文字列）
        h.dam_sire,             -- 母父名（文字列）

        -- ── 競走馬マスタ (racehorses / DIFN:UM) ───────────────
        -- horse_id で結合。DIFN 取得後に充填される
        um.birth_year,
        um.sex                  AS um_sex,
        um.coat_color,
        um.country,
        um.father_id,           -- 父の blood_id（breeding_horses との JOIN キー）
        um.father_name,         -- 父名（マスタ由来）
        um.grandsire_id,        -- 母父 ID (maternal grandsire)
        um.grandsire_name,      -- 母父名
        um.east_west            AS horse_east_west,  -- 美浦 / 栗東

        -- ── 騎手マスタ (jockeys / DIFN:KS) ──────────────────
        -- race_results.jockey(名前) → jockeys.jockey_name で結合
        -- jockey_code は ML の label encoding に利用
        ks.jockey_code,
        ks.east_west            AS jockey_east_west,
        ks.license_year         AS jockey_license_year,

        -- ── 調教師マスタ (trainers / DIFN:CH) ────────────────
        -- race_results.trainer(名前) → trainers.trainer_name で結合
        -- trainer_code は ML の label encoding に利用
        ch.trainer_code,
        ch.east_west            AS trainer_east_west,
        ch.stable_name,

        -- ── 繁殖馬マスタ・父 (breeding_horses / BLOD:BT) ──────
        -- racehorses.father_id → breeding_horses.horse_id で結合
        -- BLOD 取得前は全列 NULL（LEFT JOIN のため影響なし）
        bt.country              AS father_country,   -- 父の産地（国産/外国産）
        bt.birth_year           AS father_birth_year,-- 父の生年（種牡馬年齢推算）
        bt.father_id            AS father_sire_id,   -- 父の父 ID（3代血統）
        bt.father_name          AS father_sire_name, -- 父の父名
        bt.mother_id            AS father_dam_id,    -- 父の母 ID (BMS 系統)
        bt.mother_name          AS father_dam_name,  -- 父の母名 (BMS)

        -- ── 直近調教タイム (training_times / WOOD:TC) ─────────
        -- レース日 (r.date) より前の最新 training_date を相関サブクエリで特定
        -- idx_tc_norm(substr(horse_id,2,9), training_date DESC) でカバリングスキャン
        -- JOINキー: race_results.horse_id(YYYY+SSSSSS) の先頭9桁
        --          = training_times.horse_id(D+YYYY+SSSSS) の2文字目以降9桁
        tc.training_date        AS last_tc_date,
        tc.time_4f              AS last_tc_4f,
        tc.time_3f              AS last_tc_3f,
        tc.lap_time             AS last_tc_lap,
        tc.course_type          AS last_tc_course,
        tc.gear                 AS last_tc_gear,

        -- ── 直近坂路調教 (training_hillwork / WOOD:HC) ────────
        -- idx_hc_horse_date(horse_id, training_date DESC) でカバリングスキャン
        hc.training_date        AS last_hc_date,
        hc.time_4f              AS last_hc_4f,
        hc.time_3f              AS last_hc_3f,
        hc.lap_time             AS last_hc_lap,
        hc.gear                 AS last_hc_gear

    FROM races r

    -- ── 必須 JOIN: 1レース × N頭 に展開 ───────────────────────
    JOIN  race_results rr
          ON  rr.race_id = r.race_id

    -- ── 単勝払戻: horse_number を TEXT 変換して combination と照合 ──
    LEFT JOIN race_payouts rp_tan
          ON  rp_tan.race_id    = r.race_id
          AND rp_tan.bet_type   = '単勝'
          AND rp_tan.combination = CAST(rr.horse_number AS TEXT)

    -- ── 複勝払戻: 同上 ────────────────────────────────────────
    LEFT JOIN race_payouts rp_fuk
          ON  rp_fuk.race_id    = r.race_id
          AND rp_fuk.bet_type   = '複勝'
          AND rp_fuk.combination = CAST(rr.horse_number AS TEXT)

    -- ── 血統マスタ (horses): horse_id で結合 ─────────────────
    LEFT JOIN horses h
          ON  h.horse_id = rr.horse_id

    -- ── 競走馬マスタ (racehorses): horse_id で結合 ───────────
    LEFT JOIN racehorses um
          ON  um.horse_id = rr.horse_id

    -- ── 騎手マスタ (jockeys): 騎手名で結合 ──────────────────
    -- ※ race_results に jockey_code 列はなく名前のみのため名前結合
    --   DIFN 取得後は idx_jockeys_name が効く
    LEFT JOIN jockeys ks
          ON  ks.jockey_name = rr.jockey

    -- ── 調教師マスタ (trainers): 調教師名で結合 ──────────────
    -- ※ 同上。DIFN 取得後は idx_trainers_name が効く
    LEFT JOIN trainers ch
          ON  ch.trainer_name = rr.trainer

    -- ── 繁殖馬マスタ・父 (breeding_horses): father_id で結合 ─
    -- racehorses.father_id が埋まった後（BLOD 取得後）に有効になる
    -- 父の産地・生年・3代目血統(父の父/父の母)を取得
    LEFT JOIN breeding_horses bt
          ON  bt.horse_id = um.father_id
          AND um.father_id != ''

    -- ── 直近調教タイム: horse_id の年+連番5桁キーで結合 ─────────
    -- race_results.horse_id = YYYY+SSSSSS (4桁年 + 6桁連番)
    -- training_times.horse_id = D+YYYY+SSSSS (1桁プレフィックス + 4桁年 + 5桁連番)
    -- 共通キー(9桁): substr(rr.horse_id,1,4)||substr(rr.horse_id,5,5)
    --              = substr(tc.horse_id,2,9)
    -- GLOB フィルタで文字化けIDを除外し、クリーンな10桁数値IDのみ対象にする
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

    -- ── 直近坂路調教: 同上 ────────────────────────────────────
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


# ================================================================
# ── 日付ユーティリティ ────────────────────────────────────────────
# ================================================================

def normalize_race_date(date_str: str) -> str:
    """任意の日付文字列を YYYY-MM-DD (ISO 8601) 形式に正規化する。

    対応フォーマット:
      - ``YYYYMMDD``    (コンパクト形式、スクレイパー引数など)
      - ``YYYY/MM/DD``  (旧 DB 格納形式)
      - ``YYYY-MM-DD``  (ISO 8601、パススルー)

    腐敗データの判定:
      - 年が ``'20'`` 以外で始まる (2001〜2099 年以外)
      - 月が 1〜12 の範囲外
      - 日が 1〜31 の範囲外

    Raises:
        ValueError: 認識できないフォーマットまたは腐敗データの場合

    Returns:
        ``YYYY-MM-DD`` 形式の文字列
    """
    s = (date_str or "").strip()
    if len(s) == 10 and s[4] == '-' and s[7] == '-':
        y, mo, d = s[0:4], s[5:7], s[8:10]
    elif len(s) == 10 and s[4] == '/' and s[7] == '/':
        y, mo, d = s[0:4], s[5:7], s[8:10]
    elif len(s) == 8 and s.isdigit():
        y, mo, d = s[0:4], s[4:6], s[6:8]
    else:
        raise ValueError(f"未知の日付フォーマット: {date_str!r}")

    if not y.startswith('20'):
        raise ValueError(f"腐敗データ（2000年以前または3000年以降）: {date_str!r}")
    try:
        mo_i, d_i = int(mo), int(d)
    except ValueError:
        raise ValueError(f"腐敗データ（月日が数値でない）: {date_str!r}")
    if not (1 <= mo_i <= 12 and 1 <= d_i <= 31):
        raise ValueError(f"腐敗データ（月日が範囲外）: {date_str!r}")

    return f"{y}-{mo}-{d}"


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

    # ── パフォーマンス PRAGMA ─────────────────────────────────────
    # journal_mode=WAL : 書き込みと並行読み取りを両立（必須）
    conn.execute("PRAGMA journal_mode = WAL")
    # synchronous=NORMAL : WAL 使用時は NORMAL で十分安全かつ高速
    conn.execute("PRAGMA synchronous  = NORMAL")
    # cache_size=-65536  : 64 MB ページキャッシュ（負値=KB単位）
    conn.execute("PRAGMA cache_size   = -65536")
    # temp_store=MEMORY  : ソート・集計の一時領域をメモリに置く
    conn.execute("PRAGMA temp_store   = MEMORY")
    # mmap_size=256MB    : メモリマップ I/O で大規模 SELECT を高速化
    conn.execute("PRAGMA mmap_size    = 268435456")
    # foreign_keys=ON    : FK 制約を有効化
    conn.execute("PRAGMA foreign_keys = ON")

    with conn:
        for ddl in DDL_STATEMENTS:
            conn.execute(ddl)

    # 既存 DB への列追加マイグレーション（順序厳守）
    _migrate_add_combination_json(conn)          # 1. combination_json 列追加
    _migrate_races_new_columns(conn)             # 2. races 新列追加
    _migrate_race_results_new_columns(conn)      # 3. race_results 新列追加
    # model_type CHECK 制約を除去（(暫定)/(直前) suffix 対応）
    _migrate_relax_model_type_check(conn)        # 4. CHECK 制約除去
    # predictions UNIQUE 制約追加（Create-Insert-Drop）
    _migrate_predictions_unique_constraint(conn) # 5. UNIQUE 制約
    # races.date / training_date を YYYY-MM-DD に標準化
    _migrate_standardize_race_dates(conn)        # 6. 日付フォーマット統一
    _migrate_standardize_training_dates(conn)    # 7. 調教日付フォーマット統一
    # ビュー定義を常に最新に保つ（DDL 変更時に自動反映）
    _migrate_recreate_mart_view(conn)            # 8. v_race_mart 再作成

    logger.info("DB 初期化完了: %s", path)
    return conn


def _migrate_add_combination_json(conn: sqlite3.Connection) -> None:
    """predictions に combination_json 列を追加する（既存 DB マイグレーション）。"""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()]
    if "combination_json" not in cols:
        with conn:
            conn.execute("ALTER TABLE predictions ADD COLUMN combination_json TEXT")
        logger.info("マイグレーション: predictions.combination_json 列を追加しました")


def _migrate_races_new_columns(conn: sqlite3.Connection) -> None:
    """races に track_direction 列を追加する（既存 DB マイグレーション）。"""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(races)").fetchall()]
    if "track_direction" not in cols:
        with conn:
            conn.execute(
                "ALTER TABLE races ADD COLUMN track_direction TEXT NOT NULL DEFAULT ''"
            )
        logger.info("マイグレーション: races.track_direction 列を追加しました")


def _migrate_recreate_mart_view(conn: sqlite3.Connection) -> None:
    """v_race_mart ビューを DROP → CREATE で最新定義に再構築する。

    DDL_STATEMENTS 内の v_race_mart CREATE 文を正として、既存ビューを
    無条件に上書きする。init_db() から毎回呼ばれるため、DDL を変更した
    だけで既存 DB に自動反映される。
    """
    mart_ddl = next(
        ddl for ddl in DDL_STATEMENTS
        if "v_race_mart" in ddl and "CREATE VIEW" in ddl
    )
    with conn:
        conn.execute("DROP VIEW IF EXISTS v_race_mart")
        conn.execute(mart_ddl)
    logger.info("マイグレーション: v_race_mart ビューを再作成しました")


def _migrate_relax_model_type_check(conn: sqlite3.Connection) -> None:
    """predictions / model_performance テーブルの model_type CHECK 制約を除去する。

    CHECK(model_type IN ('卍', '本命')) が残っていると (暫定)/(直前) suffix を
    持つ値を INSERT できないため、PRAGMA writable_schema でスキーマを直接書き換える。
    ALTER TABLE RENAME TO を使うと prediction_horses の FK 参照が壊れるため採用しない。
    """
    needs_fix = False
    for table in ("predictions", "model_performance"):
        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if schema and "CHECK(model_type IN" in schema[0]:
            needs_fix = True
            break

    # prediction_horses が壊れた FK を参照しているかも確認
    ph_schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='prediction_horses'"
    ).fetchone()
    if ph_schema and "_predictions_old" in ph_schema[0]:
        needs_fix = True

    if not needs_fix:
        return

    logger.info("マイグレーション: model_type CHECK 制約 / FK 参照を writable_schema で修正します")
    conn.execute("PRAGMA writable_schema = ON")
    try:
        with conn:
            # 1. predictions / model_performance の CHECK 除去
            for table in ("predictions", "model_performance"):
                row = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if row and "CHECK(model_type IN" in row[0]:
                    new_sql = row[0].replace(
                        "CHECK(model_type IN ('卍', '本命'))", ""
                    ).replace(
                        " CHECK(model_type IN ('卍', '本命'))", ""
                    )
                    conn.execute(
                        "UPDATE sqlite_master SET sql = ? WHERE type='table' AND name=?",
                        (new_sql, table),
                    )
                    logger.info("マイグレーション: %s の CHECK 制約を除去しました", table)

            # 2. prediction_horses の壊れた FK 参照を修正
            if ph_schema and "_predictions_old" in ph_schema[0]:
                fixed_sql = ph_schema[0].replace(
                    'REFERENCES "_predictions_old"(id)', "REFERENCES predictions(id)"
                )
                conn.execute(
                    "UPDATE sqlite_master SET sql = ? WHERE type='table' AND name='prediction_horses'",
                    (fixed_sql,),
                )
                logger.info("マイグレーション: prediction_horses の FK 参照を修正しました")
    finally:
        conn.execute("PRAGMA writable_schema = OFF")


def _migrate_race_results_new_columns(conn: sqlite3.Connection) -> None:
    """race_results に gate_number / horse_number / trainer / horse_weight_diff / blood_id を追加する。"""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(race_results)").fetchall()]
    additions = [
        ("gate_number",       "ALTER TABLE race_results ADD COLUMN gate_number       INTEGER"),
        ("horse_number",      "ALTER TABLE race_results ADD COLUMN horse_number      INTEGER"),
        ("trainer",           "ALTER TABLE race_results ADD COLUMN trainer           TEXT NOT NULL DEFAULT ''"),
        ("horse_weight_diff", "ALTER TABLE race_results ADD COLUMN horse_weight_diff INTEGER"),
        # JRA-VAN 血統登録番号（10桁）。training_times.horse_id との JOIN キー
        ("blood_id",          "ALTER TABLE race_results ADD COLUMN blood_id          TEXT"),
    ]
    with conn:
        for col_name, sql in additions:
            if col_name not in cols:
                conn.execute(sql)
                logger.info("マイグレーション: race_results.%s 列を追加しました", col_name)


def _migrate_predictions_unique_constraint(conn: sqlite3.Connection) -> None:
    """predictions テーブルに UNIQUE(race_id, model_type, bet_type) 制約を追加する。

    SQLite は既存テーブルへの UNIQUE 制約追加が不可のため Create-Insert-Drop で対応。
    重複行は id が最大（最新）のものを残し、孤立した prediction_horses /
    prediction_results 行も合わせて削除する。prediction_id は保持されるため
    FK 参照は維持される。
    """
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='predictions'"
    ).fetchone()
    if not schema or "UNIQUE(race_id, model_type, bet_type)" in schema[0]:
        return

    # 残留テーブルのクリーンアップ（前回の失敗マイグレーション対策）
    conn.execute("DROP TABLE IF EXISTS predictions_new")

    # 現在のカラム一覧（combination_json が追加済みであることを前提）
    cols = [row[1] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()]
    cols_str = ", ".join(cols)

    logger.info("マイグレーション: predictions UNIQUE(race_id, model_type, bet_type) 制約追加開始")

    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        with conn:
            # 0. RENAME 時に SQLite がビュー定義を検証するため、壊れた参照を持つ
            #    ビューは事前に DROP して migration 後に再作成する。
            #    v_race_mart は _migrate_recreate_mart_view() が再作成するため省略。
            for _view in ("v_prediction_summary", "v_model_annual_summary"):
                conn.execute(f"DROP VIEW IF EXISTS {_view}")

            # 1. UNIQUE 制約つき新テーブルを作成
            conn.execute("""
                CREATE TABLE predictions_new (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id         TEXT    NOT NULL REFERENCES races(race_id),
                    model_type      TEXT    NOT NULL,
                    bet_type        TEXT    NOT NULL,
                    confidence      REAL,
                    expected_value  REAL,
                    recommended_bet REAL,
                    notes           TEXT,
                    combination_json TEXT,
                    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
                    UNIQUE(race_id, model_type, bet_type)
                )
            """)

            # 2. 重複排除: (race_id, model_type, bet_type) ごとに最新 id のみ残す
            conn.execute(f"""
                INSERT INTO predictions_new ({cols_str})
                SELECT {cols_str}
                FROM predictions
                WHERE id IN (
                    SELECT MAX(id)
                    FROM   predictions
                    GROUP BY race_id, model_type, bet_type
                )
            """)

            # 3. 孤立した子テーブル行を削除
            conn.execute("""
                DELETE FROM prediction_horses
                WHERE prediction_id NOT IN (SELECT id FROM predictions_new)
            """)
            conn.execute("""
                DELETE FROM prediction_results
                WHERE prediction_id NOT IN (SELECT id FROM predictions_new)
            """)

            # 4. 旧テーブル削除・新テーブルをリネーム
            conn.execute("DROP TABLE predictions")
            conn.execute("ALTER TABLE predictions_new RENAME TO predictions")

            # 5. インデックスを再作成
            for idx_sql in (
                "CREATE INDEX IF NOT EXISTS idx_pred_race_id    ON predictions(race_id)",
                "CREATE INDEX IF NOT EXISTS idx_pred_model_type ON predictions(model_type)",
                "CREATE INDEX IF NOT EXISTS idx_pred_created_at ON predictions(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_pred_bet_type   ON predictions(bet_type)",
            ):
                conn.execute(idx_sql)

            # 6. ビューを再作成（DDL_STATEMENTS から定義を取り出して実行）
            for _ddl in DDL_STATEMENTS:
                if "CREATE VIEW" in _ddl and (
                    "v_prediction_summary" in _ddl or "v_model_annual_summary" in _ddl
                ):
                    # DROP して CREATE（IF NOT EXISTS でも既にないので安全）
                    conn.execute(_ddl)

        logger.info("マイグレーション: predictions UNIQUE 制約追加完了")
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_standardize_race_dates(conn: sqlite3.Connection) -> None:
    """races.date を YYYY-MM-DD (ISO 8601) に統一し、腐敗レコードを削除する。

    腐敗データの定義:
      - race_id が ``'20__________'`` (12 桁) パターンに合わない
      - date が ``'20??-??-??'`` でも ``'20??/??/??'`` でもない
      - 会場コード (race_id[4:6]) が ``'01'`` 〜 ``'10'`` の範囲外

    変換ロジック:
      YYYY/MM/DD → YYYY-MM-DD (SQLite の SUBSTR + 連結で変換)
    """
    slash_count: int = conn.execute(
        "SELECT COUNT(*) FROM races WHERE date LIKE '____/__/__'"
    ).fetchone()[0]
    corrupt_count: int = conn.execute(
        """
        SELECT COUNT(*) FROM races
        WHERE race_id NOT LIKE '20__________'
           OR (date NOT LIKE '20__/__/__' AND date NOT LIKE '20__-__-__')
           OR CAST(SUBSTR(race_id, 5, 2) AS INTEGER) NOT BETWEEN 1 AND 10
        """
    ).fetchone()[0]

    if slash_count == 0 and corrupt_count == 0:
        return

    logger.info(
        "マイグレーション: races.date 標準化 (スラッシュ形式: %d件, 腐敗データ: %d件)",
        slash_count, corrupt_count,
    )

    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        with conn:
            # 1. 腐敗レコードの特定
            corrupt_ids: list[str] = [
                r[0] for r in conn.execute(
                    """
                    SELECT race_id FROM races
                    WHERE race_id NOT LIKE '20__________'
                       OR (date NOT LIKE '20__/__/__' AND date NOT LIKE '20__-__-__')
                       OR CAST(SUBSTR(race_id, 5, 2) AS INTEGER) NOT BETWEEN 1 AND 10
                    """
                ).fetchall()
            ]
            if corrupt_ids:
                ph = ",".join("?" * len(corrupt_ids))
                # FK=OFF のため手動で子テーブルを先に削除
                for child in ("race_results", "race_payouts", "entries",
                               "realtime_odds", "predictions"):
                    conn.execute(f"DELETE FROM {child} WHERE race_id IN ({ph})", corrupt_ids)
                conn.execute(f"DELETE FROM races WHERE race_id IN ({ph})", corrupt_ids)
                logger.info("腐敗レコード削除: %d 件", len(corrupt_ids))

            # 2. YYYY/MM/DD → YYYY-MM-DD に変換
            if slash_count > 0:
                conn.execute(
                    """
                    UPDATE races
                    SET date =
                        SUBSTR(date, 1, 4) || '-' ||
                        SUBSTR(date, 6, 2) || '-' ||
                        SUBSTR(date, 9, 2)
                    WHERE date LIKE '____/__/__'
                    """
                )
                logger.info("races.date 変換完了: %d 件", slash_count)
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_standardize_training_dates(conn: sqlite3.Connection) -> None:
    """training_times / training_hillwork の training_date を YYYY-MM-DD に統一する。

    UNIQUE 制約 (horse_id, training_date, ...) は同一論理日の変換では衝突しないため
    DROP/RENAME 不要。UPDATE のみで完結する。
    """
    for table in ("training_times", "training_hillwork"):
        count: int = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE training_date LIKE '____/__/__'"
        ).fetchone()[0]
        if count > 0:
            with conn:
                conn.execute(
                    f"""
                    UPDATE {table}
                    SET training_date =
                        SUBSTR(training_date, 1, 4) || '-' ||
                        SUBSTR(training_date, 6, 2) || '-' ||
                        SUBSTR(training_date, 9, 2)
                    WHERE training_date LIKE '____/__/__'
                    """
                )
            logger.info("マイグレーション: %s.training_date 変換完了 %d 件", table, count)


def insert_race(conn: sqlite3.Connection, race: "RaceInfo") -> None:  # type: ignore[name-defined]
    """
    RaceInfo をトランザクション内で races / horses / race_results に保存する。
    """
    try:
        normalized_date = normalize_race_date(race.date)
    except ValueError:
        logger.warning(
            "不正な日付のため races INSERT をスキップ: race_id=%s date=%r",
            race.race_id, race.date,
        )
        return

    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                race.race_id, race.race_name, normalized_date, race.venue,
                race.race_number, race.distance, race.surface,
                getattr(race, "track_direction", ""),
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
                    (race_id, horse_id, horse_name, rank,
                     gate_number, horse_number,
                     sex_age, weight_carried, jockey, trainer,
                     finish_time, margin, popularity, win_odds,
                     horse_weight, horse_weight_diff)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    race.race_id, r.horse_id, r.horse_name, r.rank,
                    getattr(r, "gate_number", None),
                    getattr(r, "horse_number", None),
                    r.sex_age, r.weight_carried, r.jockey,
                    getattr(r, "trainer", ""),
                    r.finish_time, r.margin, r.popularity,
                    r.win_odds, r.horse_weight,
                    getattr(r, "horse_weight_diff", None),
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
    combination_json: str | None = None,
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
    _VALID_BASE_TYPES = {"卍", "本命", "WIN5"}
    base = model_type.split("(")[0]
    if base not in _VALID_BASE_TYPES:
        raise ValueError(f"model_type のベースは '卍' / '本命' / 'WIN5' を指定してください: {model_type!r}")

    with conn:
        cur = conn.execute(
            """
            INSERT OR REPLACE INTO predictions
                (race_id, model_type, bet_type, confidence,
                 expected_value, recommended_bet, notes, combination_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (race_id, model_type, bet_type, confidence,
             expected_value, recommended_bet, notes, combination_json),
        )
        prediction_id = cur.lastrowid

        for h in horses:
            # horse_id が horses テーブルに未登録の場合（暫定予想など）は先に登録
            hid = h.get("horse_id")
            if hid:
                conn.execute(
                    "INSERT OR IGNORE INTO horses (horse_id, horse_name) VALUES (?, ?)",
                    (hid, h.get("horse_name", "")),
                )
            conn.execute(
                """
                INSERT INTO prediction_horses
                    (prediction_id, horse_id, horse_name,
                     predicted_rank, model_score, ev_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    hid,
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
    # entries.horse_id は netkeiba 形式（例: 2021103333）で、
    # horses テーブルの JRA-VAN 形式（例: 00000000AB）と一致しないため
    # FK 制約を一時的に無効化してバルク挿入する。
    with conn:
        conn.execute("PRAGMA foreign_keys = OFF")
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
        conn.execute("PRAGMA foreign_keys = ON")

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


def insert_race_payouts(
    conn: sqlite3.Connection,
    race_id: str,
    payouts: list[dict],
) -> int:
    """
    レース払戻データを race_payouts テーブルに保存する（UPSERT）。

    Args:
        conn:    DB コネクション
        race_id: 対象レース ID
        payouts: [{"bet_type": "単勝", "combination": "14",
                   "payout": 380, "popularity": 1}, ...]

    Returns:
        保存した件数
    """
    count = 0
    with conn:
        for p in payouts:
            conn.execute(
                """
                INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(race_id, bet_type, combination) DO UPDATE SET
                    payout     = excluded.payout,
                    popularity = excluded.popularity
                """,
                (race_id, p["bet_type"], p["combination"], p["payout"], p.get("popularity")),
            )
            count += 1
    logger.info("払戻保存: race_id=%s, %d 件", race_id, count)
    return count


def query_mart(
    conn: sqlite3.Connection,
    *,
    race_id:   str | None = None,
    year:      str | None = None,
    venue:     str | None = None,
    surface:   str | None = None,
    date_from: str | None = None,
    date_to:   str | None = None,
) -> list[sqlite3.Row]:
    """
    v_race_mart から条件に合う行を返す。

    すべての引数はキーワード専用。フィルタを指定しない場合は全行を返す。
    SQL はパラメータバインドのみで構築するため SQL インジェクションは発生しない。

    Args:
        conn:      init_db() が返した sqlite3.Connection
        race_id:   特定レース ID (完全一致)
        year:      開催年 (例: "2024")
        venue:     開催場所 (例: "東京")
        surface:   馬場種別 (例: "芝" / "ダート")
        date_from: 開催日の下限 (YYYY-MM-DD, 含む)
        date_to:   開催日の上限 (YYYY-MM-DD, 含む)

    Returns:
        list[sqlite3.Row] — 辞書ライクアクセス可 (row["horse_name"] 等)
    """
    conn.row_factory = sqlite3.Row

    clauses: list[str] = []
    params:  list[str] = []

    if race_id is not None:
        clauses.append("race_id = ?")
        params.append(race_id)
    if year is not None:
        clauses.append("year = ?")
        params.append(year)
    if venue is not None:
        clauses.append("venue = ?")
        params.append(venue)
    if surface is not None:
        clauses.append("surface = ?")
        params.append(surface)
    if date_from is not None:
        clauses.append("date >= ?")
        params.append(date_from)
    if date_to is not None:
        clauses.append("date <= ?")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT * FROM v_race_mart {where} ORDER BY date, race_id, horse_number"
    return conn.execute(sql, params).fetchall()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = init_db()
    conn.close()
    print("umalogi.db の初期化が完了しました。")
