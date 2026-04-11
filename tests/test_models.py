"""
src/ml/models.py のユニットテスト。
DB は in-memory で動作させる。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
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
                gate_number=((i - 1) // 2) + 1,   # 1〜8 の枠番を均等に割り当て
                horse_number=i,
                sex_age="牡3",
                weight_carried=56.0,
                jockey="テスト騎手",
                trainer="テスト調教師",
                finish_time="2:00.0",
                margin=None if i == 1 else "0.1",
                popularity=i,
                win_odds=float(i * 2),
                horse_weight=500,
                horse_weight_diff=0,
                pedigree=PedigreeInfo(sire=f"父{i}", dam="母A", dam_sire="母父A"),
            )
        )
    return RaceInfo(
        race_id=race_id,
        race_name=f"テストレース{race_id}",
        date=f"2024-0{(int(race_id[-2:]) % 9) + 1}-01",
        venue="東京",
        race_number=int(race_id[-2:]),
        distance=1600,
        surface="芝",
        track_direction="左",
        weather="晴",
        condition="良",
        results=results,
    )


@pytest.fixture()
def db_many() -> sqlite3.Connection:
    """複数レースのデータが入った DB（学習用）。35 レース = _MIN_TRAIN_RACES(30) 超え。"""
    conn = init_db(db_path=Path(":memory:"))
    for i in range(1, 36):
        insert_race(conn, _make_race(f"20240101{i:02d}"))
    yield conn
    conn.close()


@pytest.fixture()
def db_with_payouts(db_many: sqlite3.Connection) -> sqlite3.Connection:
    """単勝払戻データを 1 レース分追加した DB。ev_target 検証用。"""
    # レース 20240101_01: 馬番 1 の単勝払戻 = 250 円
    with db_many:
        db_many.execute(
            "INSERT OR IGNORE INTO race_payouts (race_id, bet_type, combination, payout, popularity) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2024010101", "単勝", "1", 250, 1),
        )
    return db_many


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
            "horse_number":   i,
            "horse_id":       f"h{i:02d}",
            "horse_name":     f"テスト馬{i:02d}",
            "popularity":     i,
            "win_odds":       float(i * 2),
            "market_prob":    1.0 / float(i * 2),
            "surface_code":   0,
            "sex_code":       0,
            "venue_encoded":  4,
            "sire_encoded":   i,
            "distance":       1600,
            "dist_band":      "mile",
            "gate_number":    ((i - 1) // 2) + 1,
            "condition_code": 0,
            "race_number":    i,
        })
        rows.append(row)
    return pd.DataFrame(rows)


# ── FEATURE_COLS の整合性 ─────────────────────────────────────────

class TestFeatureCols:
    def test_列数が39(self) -> None:
        assert len(FEATURE_COLS) == 39

    def test_追加3列が含まれる(self) -> None:
        # win_odds / market_prob は過学習防止のため除外済み
        added = {"horse_weight_diff", "gate_number", "condition_code", "race_number"}
        assert added.issubset(set(FEATURE_COLS))

    def test_オッズ列が除外されている(self) -> None:
        assert "win_odds"    not in FEATURE_COLS
        assert "market_prob" not in FEATURE_COLS


# ── _build_train_df ───────────────────────────────────────────────

class TestBuildTrainDf:
    def test_データありで非空DataFrame(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert not df.empty

    def test_is_winnerカラムが存在する(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert "is_winner" in df.columns

    def test_is_placedカラムが存在する(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert "is_placed" in df.columns

    def test_ev_targetカラムが存在する(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert "ev_target" in df.columns

    def test_データなしで空DataFrame(self, db_empty: sqlite3.Connection) -> None:
        df = _build_train_df(db_empty)
        assert df.empty

    def test_is_winner_は0か1のみ(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert set(df["is_winner"].unique()).issubset({0, 1})

    def test_is_placed_は0か1のみ(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        assert set(df["is_placed"].unique()).issubset({0, 1})

    def test_is_winner_はis_placed_のサブセット(self, db_many: sqlite3.Connection) -> None:
        """1着馬は必ず3着以内でもある。"""
        df = _build_train_df(db_many)
        winners = df[df["is_winner"] == 1]
        assert (winners["is_placed"] == 1).all()

    def test_is_placed_比率は約50パーセント(self, db_many: sqlite3.Connection) -> None:
        """6頭立てレースなら 3/6 = 50% が is_placed=1 になる。"""
        df = _build_train_df(db_many)
        rate = df["is_placed"].mean()
        assert 0.45 <= rate <= 0.55

    def test_ev_targetはpayout_tanshoを優先する(
        self, db_with_payouts: sqlite3.Connection
    ) -> None:
        """payout_tansho が存在するレースでは ev_target = payout_tansho になる。"""
        df = _build_train_df(db_with_payouts)
        # race_id="2024010101" の 1着馬 (horse_name="テスト馬01", win_odds=2.0)
        winner = df[(df["race_id"] == "2024010101") & (df["is_winner"] == 1)]
        assert not winner.empty
        # payout_tansho=250 が存在するので ev_target=250
        assert float(winner["ev_target"].iloc[0]) == pytest.approx(250.0)

    def test_ev_targetフォールバックはoddsx100(self, db_many: sqlite3.Connection) -> None:
        """払戻データなし & 1着の場合: ev_target = win_odds × 100。"""
        df = _build_train_df(db_many)
        winners = df[df["is_winner"] == 1]
        assert not winners.empty
        # win_odds=2.0 の1着馬: ev_target = 200.0
        first = winners[winners["win_odds"] == 2.0]
        if not first.empty:
            assert float(first["ev_target"].iloc[0]) == pytest.approx(200.0)

    def test_外れ馬のev_targetは0(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        losers = df[df["is_winner"] == 0]
        # payout_tansho が NULL かつ 1着以外 → ev_target = 0
        no_payout = losers[losers["payout_tansho"].isna()]
        assert (no_payout["ev_target"] == 0).all()

    def test_新特徴量がDataFrameに含まれる(self, db_many: sqlite3.Connection) -> None:
        df = _build_train_df(db_many)
        for col in ["horse_weight_diff", "gate_number", "condition_code", "market_prob", "race_number"]:
            assert col in df.columns, f"{col} が DataFrame に存在しない"

    def test_market_probはwin_oddsの逆数(self, db_many: sqlite3.Connection) -> None:
        """market_prob = 1 / win_odds（win_odds が NULL 以外の行で検証）。"""
        df = _build_train_df(db_many)
        valid = df[df["win_odds"].notna() & df["market_prob"].notna()]
        assert not valid.empty
        expected = (1.0 / valid["win_odds"].clip(upper=80.0)).values
        np.testing.assert_allclose(valid["market_prob"].values, expected, rtol=1e-5)

    def test_リーク排除で未来成績が混入しない(self, db_many: sqlite3.Connection) -> None:
        """
        新馬戦（最初のレースに出走した馬）の win_rate_all は None であるべき。
        _get_horse_stats(exclude_race_id=...) により、
        その馬の初レース時点では過去成績が 0 件 → None が返る。
        """
        df = _build_train_df(db_many)
        # 最初のレースの出走馬は全て初出走（過去成績なし）なので win_rate_all が NULL
        first_race = df[df["race_id"] == df["race_id"].min()]
        assert first_race["win_rate_all"].isna().all(), (
            "初レースの win_rate_all に値が入っている（リークの可能性）"
        )


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

    def test_train_はcv_auc_meanを返す(self, db_many: sqlite3.Connection) -> None:
        """GroupKFold CV の結果として cv_auc_mean が返却されること。"""
        m = HonmeiModel()
        result = m.train(db_many)
        assert "cv_auc_mean" in result
        assert "cv_auc_std" in result
        # AUC は 0〜1 の範囲（nan は十分なデータがない場合に許容）
        if not np.isnan(result["cv_auc_mean"]):
            assert 0.0 <= result["cv_auc_mean"] <= 1.0

    def test_train_はn_samples_を返す(self, db_many: sqlite3.Connection) -> None:
        m = HonmeiModel()
        result = m.train(db_many)
        assert result["n_samples"] > 0
        assert result["n_races"] > 0

    def test_特徴量重要度の長さがFEATURE_COLS一致(
        self, db_many: sqlite3.Connection
    ) -> None:
        """訓練後の feature_importances_ の長さが FEATURE_COLS と同じであること。
        _model は CalibratedClassifierCV のため _base_lgbm から取得する。"""
        m = HonmeiModel()
        m.train(db_many)
        assert len(m._base_lgbm.feature_importances_) == len(FEATURE_COLS)


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
        assert np.allclose(ev.values, raw.values / 100.0)

    def test_訓練後は訓練済みフラグがTrue(self, db_many: sqlite3.Connection) -> None:
        m = ManjiModel()
        m.train(db_many)
        assert m.is_trained


# ── train_all ─────────────────────────────────────────────────────

class TestTrainAll:
    def test_結果にn_racesが含まれる(self, db_many: sqlite3.Connection, tmp_path) -> None:
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

    def test_train_all_でcv_aucが返る(
        self, db_many: sqlite3.Connection, tmp_path
    ) -> None:
        import src.ml.models as _mod
        orig = _mod._MODEL_DIR
        _mod._MODEL_DIR = tmp_path
        try:
            result = train_all(db_many)
        finally:
            _mod._MODEL_DIR = orig

        assert "cv_auc_mean" in result["honmei"]


# ── load_models ───────────────────────────────────────────────────

class TestLoadModels:
    def test_モデルファイルなしでも例外なし(self, tmp_path) -> None:
        import src.ml.models as _mod
        orig = _mod._MODEL_DIR
        _mod._MODEL_DIR = tmp_path  # 空ディレクトリ → モデルファイルなし
        try:
            honmei, manji = load_models()
        finally:
            _mod._MODEL_DIR = orig
        assert isinstance(honmei, HonmeiModel)
        assert isinstance(manji, ManjiModel)

    def test_未訓練モデルでフォールバック動作(self, dummy_df: pd.DataFrame, tmp_path) -> None:
        """モデルファイルが存在しない場合、フォールバック予測が返ること。"""
        import src.ml.models as _mod
        orig = _mod._MODEL_DIR
        _mod._MODEL_DIR = tmp_path  # 空ディレクトリ → モデルファイルなし → フォールバック
        try:
            honmei, manji = load_models()
            h_scores = honmei.predict(dummy_df)
            m_scores = manji.predict(dummy_df)
        finally:
            _mod._MODEL_DIR = orig
        assert len(h_scores) == len(dummy_df)
        assert len(m_scores) == len(dummy_df)
