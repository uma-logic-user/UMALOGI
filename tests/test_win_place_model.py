"""単複特化モデル修正のテスト: リーク排除・PlaceModel"""
from __future__ import annotations
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _make_restored_db() -> sqlite3.Connection:
    """restore_results_from_payouts と同様に rank=1/2/3 のみ設定された DB を模倣する。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE races (
            race_id TEXT PRIMARY KEY,
            date TEXT,
            venue TEXT DEFAULT '東京',
            race_number INTEGER DEFAULT 5,
            distance INTEGER DEFAULT 1600,
            surface TEXT DEFAULT '芝',
            condition TEXT DEFAULT '良'
        );
        CREATE TABLE race_results (
            race_id TEXT,
            horse_number INTEGER,
            horse_id TEXT,
            horse_name TEXT,
            sex_age TEXT DEFAULT '牡3',
            weight_carried REAL DEFAULT 55.0,
            gate_number INTEGER DEFAULT 1,
            horse_weight REAL,
            horse_weight_diff REAL,
            jockey TEXT DEFAULT '',
            trainer TEXT DEFAULT '',
            win_odds REAL,
            popularity INTEGER,
            rank INTEGER
        );
        CREATE TABLE race_payouts (
            race_id TEXT, bet_type TEXT, combination TEXT, payout INTEGER
        );
        CREATE TABLE entries (
            race_id TEXT, horse_number INTEGER, horse_id TEXT, horse_name TEXT,
            sex_age TEXT, weight_carried REAL, gate_number INTEGER,
            horse_weight REAL, horse_weight_diff REAL, jockey TEXT, trainer TEXT
        );
        CREATE TABLE horses (horse_id TEXT PRIMARY KEY, sire TEXT);
        CREATE TABLE jockeys (jockey_name TEXT, jockey_code TEXT);
        CREATE TABLE trainers (trainer_name TEXT, trainer_code TEXT);
        CREATE TABLE realtime_odds (race_id TEXT, horse_number INTEGER, horse_name TEXT, win_odds REAL, fetched_at TEXT, popularity INTEGER, recorded_at TEXT);
        CREATE TABLE training_times (horse_id TEXT, training_date TEXT, time_4f REAL, lap_time REAL);
        CREATE TABLE training_hillwork (horse_id TEXT, training_date TEXT, time_4f REAL, lap_time REAL);
    """)
    # 1レース: 5頭 rank=1/2/3のみ設定 (他はNULL) — 復元後の状態
    conn.execute("INSERT INTO races VALUES ('R001','2025-06-01','東京',5,1600,'芝','良')")
    for i in range(1, 6):
        rank_val = i if i <= 3 else None
        conn.execute(
            "INSERT INTO race_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ('R001', i, None, f'馬{i}', '牡3', 55.0, i, 500.0, 0.0, '', '',
             float(i * 5), i, rank_val)
        )
    conn.execute("INSERT INTO race_payouts VALUES ('R001','単勝','1',500)")
    conn.commit()
    return conn


def test_build_train_df_includes_all_horses_not_just_ranked() -> None:
    """_build_train_df が rank=NULL の馬も含んで is_winner=0 とすること。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    df = _build_train_df(conn)
    # 5頭全員が含まれるはず (rank=NULL → is_winner=0)
    assert len(df) == 5, f"期待5頭, 実際{len(df)}頭"
    assert df["is_winner"].sum() == 1, "勝者は1頭のみ"


def test_build_train_df_ev_target_capped_at_10000() -> None:
    """ev_target が 10,000 を超えないこと。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    # 超高オッズ馬を追加 (win_odds=150, rank=NULL)
    conn.execute(
        "INSERT INTO race_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ('R001', 6, None, '超穴馬', '牡3', 55.0, 6, 500.0, 0.0, '', '',
         150.0, 6, None)
    )
    conn.commit()
    df = _build_train_df(conn)
    max_ev = df["ev_target"].max()
    assert max_ev <= 10000, f"EV上限超え: {max_ev}"


def test_build_train_df_is_placed_created() -> None:
    """is_placed 列（rank<=3=1）が存在すること。"""
    from src.ml.models import _build_train_df
    conn = _make_restored_db()
    df = _build_train_df(conn)
    assert "is_placed" in df.columns
    # rank=1,2,3 の3頭は is_placed=1, NULL2頭は0
    assert df["is_placed"].sum() == 3, f"期待3頭, 実際{df['is_placed'].sum()}頭"


def test_place_model_trains_on_is_placed() -> None:
    """PlaceModel が is_placed を学習できること。"""
    from src.ml.models import PlaceModel
    conn = _make_restored_db()
    model = PlaceModel()
    result = model.train(conn)
    assert result["n_samples"] > 0
    assert model.is_trained


def test_place_model_predict_returns_series() -> None:
    """PlaceModel.predict() が pd.Series を返すこと。"""
    import pandas as pd
    from src.ml.models import PlaceModel, FEATURE_COLS
    conn = _make_restored_db()
    model = PlaceModel()
    model.train(conn)
    # 最小限の特徴量 DataFrame を作成
    df = pd.DataFrame({col: [0.0] * 3 for col in FEATURE_COLS})
    result = model.predict(df)
    assert isinstance(result, pd.Series)
    assert len(result) == 3
