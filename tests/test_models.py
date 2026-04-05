"""
src/ml/models.py のユニットテスト。
DB は in-memory で動作させる。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.database.init_db import init_db, insert_race
from src.ml.models import (
    FEATURE_COLS,
    HonmeiModel,
    ManjiModel,
    _build_train_df,
    load_models,
    train_all,
)
from src.scraper.netkeiba import HorseResult, PedigreeInfo, RaceInfo


# ── フィクスチャ ──────────────────────────────────────────────────

def _make_race(race_id: str, n_horses: int = 6) -> RaceInfo:
    results = []
    for i in range(1, n_horses + 1):
        results.append(
            HorseResult(
                rank=i,
                horse_name=f"テスト馬{i:02d}",
                horse_id=f"h{race_id}{i:02d}",
                sex_age="牡3",
                weight_carried=56.0,
                jockey="テスト騎手",
                finish_time="2:00.0",
                margin=None if i == 1 else "0.1",
                popularity=i,
                win_odds=float(i * 2),
                horse_weight=500,
                pedigree=PedigreeInfo(sire=f"父{i}", dam="母A", dam_sire="母父A"),
            )
        )
    return RaceInfo(
        race_id=race_id,
        race_name=f"テストレース{race_id}",
        date=f"2024/0{(int(race_id[-2:]) % 9) + 1}/01",
        venue="東京",
        race_number=int(race_id[-2:]),
        distance=1600,
        surface="芝",
        weather="晴",
        condition="良",
        results=results,
    )


@pytest.fixture()
def db_many() -> sqlite3.Connection:
    """複数レースのデータが入った DB（学習用）。"""
    conn = init_db(db_path=Path(":memory:"))
    # 35 レース挿入（_MIN_TRAIN_RACES=30 を超える）
    for i in range(1, 36):
        insert_race(conn, _make_race(f"20240101{i:02d}"))
    yield conn
    conn.close()


@pytest.fixture()
def db_empty() -> sqlite3.Connection:
    conn = init_db(db_path=Path(":memory:"))
    yield conn
    conn.close()


@pytest.fixture()
def dummy_df() -> pd.DataFrame:
    """predict() に渡すダミー特徴量 DataFrame（6頭）。"""
    rows = []
    for i in range(1, 7):
        row: dict = {col: 0.0 for col in FEATURE_COLS}
        row.update({
            "horse_number": i,
            "horse_id":     f"h{i:02d}",
            "horse_name":   f"テスト馬{i:02d}",
            "popularity":   i,
            "win_odds":     float(i * 2),
            "surface_code": 0,
            "sex_code":     0,
            "venue_encoded": 4,
            "sire_encoded": i,
            "distance":     1600,
            "dist_band":    "mile",
        })
        rows.append(row)
    return pd.DataFrame(rows)


# ── _build_train_df ───────────────────────────────────────────────

class TestBuildTrainDf:
    def test_データありで非空DataFrame(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert not df.empty

    def test_is_winnerカラムが存在する(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert "is_winner" in df.columns

    def test_ev_targetカラムが存在する(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert "ev_target" in df.columns

    def test_データなしで空DataFrame(self, db_empty: sqlite3.Connection) -> None:
        df = _build_train_df(db_empty)
        assert df.empty


# ── HonmeiModel ───────────────────────────────────────────────────

class TestHonmeiModel:
    def test_初期状態は未訓練(self) -> None:
        m = HonmeiModel()
        assert not m.is_trained

    def test_フォールバック予測は正常動作(self, dummy_df: pd.DataFrame) -> None:
        m = HonmeiModel()
        scores = m.predict(dummy_df)
        assert len(scores) == len(dummy_df)
        assert (scores >= 0).all()

    def test_訓練後は訓練済みフラグがTrue(self, db_many: sqlite3.Connection) -> None:
        m = HonmeiModel()
        m.train(db_many)
        assert m.is_trained

    def test_訓練後predictが正常動作(
        self, db_many: sqlite3.Connection, dummy_df: pd.DataFrame
    ) -> None:
        m = HonmeiModel()
        m.train(db_many)
        scores = m.predict(dummy_df)
        assert len(scores) == len(dummy_df)
        assert (scores >= 0).all()
        assert (scores <= 1).all()

    def test_データなしで訓練スキップ(self, db_empty: sqlite3.Connection) -> None:
        m = HonmeiModel()
        result = m.train(db_empty)
        assert result["n_races"] == 0
        assert not m.is_trained


# ── ManjiModel ────────────────────────────────────────────────────

class TestManjiModel:
    def test_初期状態は未訓練(self) -> None:
        m = ManjiModel()
        assert not m.is_trained

    def test_フォールバック予測が人気に基づく(self, dummy_df: pd.DataFrame) -> None:
        m = ManjiModel()
        scores = m.predict(dummy_df)
        assert len(scores) == len(dummy_df)
        assert (scores >= 0).all()

    def test_ev_scoreは100で割られる(
        self, db_many: sqlite3.Connection, dummy_df: pd.DataFrame
    ) -> None:
        m = ManjiModel()
        m.train(db_many)
        ev = m.ev_score(dummy_df)
        raw = m.predict(dummy_df)
        # ev = raw / 100
        import numpy as np
        assert np.allclose(ev.values, raw.values / 100.0)

    def test_訓練後は訓練済みフラグがTrue(self, db_many: sqlite3.Connection) -> None:
        m = ManjiModel()
        m.train(db_many)
        assert m.is_trained


# ── train_all ─────────────────────────────────────────────────────

class TestTrainAll:
    def test_結果にn_racesが含まれる(self, db_many: sqlite3.Connection, tmp_path) -> None:
        # モデルの保存先を tmp_path に変更
        import src.ml.models as _mod
        orig = _mod._MODEL_DIR
        _mod._MODEL_DIR = tmp_path
        try:
            result = train_all(db_many)
        finally:
            _mod._MODEL_DIR = orig

        assert "honmei" in result
        assert "manji" in result
        assert result["honmei"]["n_races"] > 0


# ── load_models ───────────────────────────────────────────────────

class TestLoadModels:
    def test_モデルファイルなしでも例外なし(self) -> None:
        honmei, manji = load_models()
        assert isinstance(honmei, HonmeiModel)
        assert isinstance(manji, ManjiModel)

    def test_未訓練モデルでフォールバック動作(self, dummy_df: pd.DataFrame) -> None:
        honmei, manji = load_models()
        h_scores = honmei.predict(dummy_df)
        m_scores = manji.predict(dummy_df)
        assert len(h_scores) == len(dummy_df)
        assert len(m_scores) == len(dummy_df)
