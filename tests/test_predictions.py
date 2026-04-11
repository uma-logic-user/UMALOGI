"""
predictions / prediction_horses / prediction_results / model_performance
テーブルの CRUD と集計ロジックを検証する。
"""

import sqlite3
from pathlib import Path

import pytest

from src.database.init_db import (
    init_db,
    insert_prediction,
    insert_race,
    record_prediction_result,
    refresh_model_performance,
)
from src.scraper.netkeiba import HorseResult, PedigreeInfo, RaceInfo


# ── フィクスチャ ──────────────────────────────────────────────────

@pytest.fixture()
def db() -> sqlite3.Connection:
    conn = init_db(db_path=Path(":memory:"))
    yield conn
    conn.close()


@pytest.fixture()
def seeded_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """有馬記念ダミーデータを投入済みの DB を返す。"""
    race = RaceInfo(
        race_id="202506050811",
        race_name="第70回有馬記念(GI)",
        date="2025-12-28",
        venue="中山",
        race_number=5,
        distance=2500,
        surface="芝",
        track_direction="右",
        weather="晴",
        condition="良",
        results=[
            HorseResult(
                rank=1, horse_name="ミュージアムマイル",
                horse_id="2022105081", sex_age="牡3",
                gate_number=3, horse_number=5,
                weight_carried=56.0, jockey="Ｃ．デム",
                trainer="国枝栄",
                finish_time="2:31.5", margin=None,
                popularity=3, win_odds=3.8, horse_weight=502,
                horse_weight_diff=2,
                pedigree=PedigreeInfo(sire="リオンディーズ",
                                      dam="ミュージアムヒル",
                                      dam_sire="ハーツクライ"),
            ),
            HorseResult(
                rank=4, horse_name="レガレイラ",
                horse_id="2021105898", sex_age="牝4",
                gate_number=7, horse_number=13,
                weight_carried=55.0, jockey="横山武史",
                trainer="木村哲也",
                finish_time="2:31.7", margin="2",
                popularity=1, win_odds=3.3, horse_weight=482,
                horse_weight_diff=-4,
                pedigree=PedigreeInfo(sire="スワーヴリチャード",
                                      dam="ロカ", dam_sire="ハービンジャー"),
            ),
        ],
    )
    insert_race(db, race)
    return db


# ── insert_prediction ────────────────────────────────────────────

class TestInsertPrediction:
    def test_予想を保存してIDを返す(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db,
            race_id="202506050811",
            model_type="卍",
            bet_type="単勝",
            horses=[{"horse_name": "ミュージアムマイル",
                     "horse_id": "2022105081",
                     "predicted_rank": 1,
                     "model_score": 0.72,
                     "ev_score": 1.45}],
            confidence=0.72,
            expected_value=1.45,
            recommended_bet=1000.0,
        )
        assert isinstance(pid, int)
        assert pid > 0

    def test_prediction_horsesに馬が保存される(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db,
            race_id="202506050811",
            model_type="本命",
            bet_type="馬連",
            horses=[
                {"horse_name": "ミュージアムマイル", "predicted_rank": 1, "model_score": 0.80},
                {"horse_name": "レガレイラ",         "predicted_rank": 2, "model_score": 0.65},
            ],
        )
        rows = seeded_db.execute(
            "SELECT horse_name, predicted_rank FROM prediction_horses WHERE prediction_id=? ORDER BY predicted_rank",
            (pid,),
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "ミュージアムマイル"
        assert rows[1][0] == "レガレイラ"

    def test_不正なmodel_typeでValueError(self, seeded_db: sqlite3.Connection) -> None:
        with pytest.raises(ValueError, match="model_type"):
            insert_prediction(
                seeded_db,
                race_id="202506050811",
                model_type="unknown",
                bet_type="単勝",
                horses=[],
            )

    def test_卍と本命の両方を保存できる(self, seeded_db: sqlite3.Connection) -> None:
        for model in ("卍", "本命"):
            insert_prediction(
                seeded_db,
                race_id="202506050811",
                model_type=model,
                bet_type="単勝",
                horses=[{"horse_name": "ミュージアムマイル"}],
            )
        count = seeded_db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        assert count == 2


# ── record_prediction_result ─────────────────────────────────────

class TestRecordPredictionResult:
    def test_的中を記録できる(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db, "202506050811", "卍", "単勝",
            horses=[{"horse_name": "ミュージアムマイル"}],
            recommended_bet=1000.0,
        )
        record_prediction_result(seeded_db, pid, is_hit=True, payout=3800.0)

        row = seeded_db.execute(
            "SELECT is_hit, payout, profit, roi FROM prediction_results WHERE prediction_id=?",
            (pid,),
        ).fetchone()
        assert row[0] == 1
        assert row[1] == pytest.approx(3800.0)
        assert row[2] == pytest.approx(2800.0)   # 3800 - 1000
        assert row[3] == pytest.approx(380.0)    # 3800/1000*100

    def test_外れを記録できる(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db, "202506050811", "本命", "単勝",
            horses=[{"horse_name": "レガレイラ"}],
            recommended_bet=500.0,
        )
        record_prediction_result(seeded_db, pid, is_hit=False, payout=0.0)

        row = seeded_db.execute(
            "SELECT is_hit, profit FROM prediction_results WHERE prediction_id=?",
            (pid,),
        ).fetchone()
        assert row[0] == 0
        assert row[1] == pytest.approx(-500.0)

    def test_購入金額をpredictionsから自動取得(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db, "202506050811", "卍", "単勝",
            horses=[{"horse_name": "ミュージアムマイル"}],
            recommended_bet=2000.0,
        )
        # recommended_bet を明示しない → predictions テーブルから取得
        record_prediction_result(seeded_db, pid, is_hit=True, payout=7600.0)

        row = seeded_db.execute(
            "SELECT profit FROM prediction_results WHERE prediction_id=?", (pid,)
        ).fetchone()
        assert row[0] == pytest.approx(5600.0)   # 7600 - 2000


# ── v_prediction_summary ビュー ───────────────────────────────────

class TestPredictionSummaryView:
    def test_ビューにレース名と結果が結合される(self, seeded_db: sqlite3.Connection) -> None:
        pid = insert_prediction(
            seeded_db, "202506050811", "卍", "単勝",
            horses=[{"horse_name": "ミュージアムマイル"}],
            recommended_bet=1000.0,
        )
        record_prediction_result(seeded_db, pid, is_hit=True, payout=3800.0)

        row = seeded_db.execute(
            "SELECT race_name, venue, model_type, is_hit, roi FROM v_prediction_summary WHERE prediction_id=?",
            (pid,),
        ).fetchone()
        assert row[0] == "第70回有馬記念(GI)"
        assert row[1] == "中山"
        assert row[2] == "卍"
        assert row[3] == 1
        assert row[4] == pytest.approx(380.0)


# ── refresh_model_performance ────────────────────────────────────

class TestRefreshModelPerformance:
    def _seed_results(self, db: sqlite3.Connection) -> None:
        """的中1件・外れ1件のデータを投入する。
        bet_type を別々にする（同 race_id+model_type+bet_type は UNIQUE 制約で1件に絞られるため）。
        """
        for model, horse, bet_type, hit, payout in [
            ("卍", "ミュージアムマイル", "単勝", True,  3800.0),
            ("卍", "レガレイラ",        "複勝", False, 0.0),
        ]:
            pid = insert_prediction(
                db, "202506050811", model, bet_type,
                horses=[{"horse_name": horse}],
                recommended_bet=1000.0,
            )
            record_prediction_result(db, pid, is_hit=hit, payout=payout)

    def test_回収率と的中率が正しく計算される(self, seeded_db: sqlite3.Connection) -> None:
        self._seed_results(seeded_db)
        refresh_model_performance(seeded_db, "卍", 2025)

        row = seeded_db.execute(
            "SELECT total_bets, hits, hit_rate, roi FROM model_performance WHERE model_type='卍' AND year=2025",
        ).fetchone()
        assert row[0] == 2                        # 2回購入
        assert row[1] == 1                        # 1回的中
        assert row[2] == pytest.approx(50.0)      # 50%
        assert row[3] == pytest.approx(190.0)     # 3800 / 2000 * 100

    def test_再集計でUPSERTされる(self, seeded_db: sqlite3.Connection) -> None:
        self._seed_results(seeded_db)
        refresh_model_performance(seeded_db, "卍", 2025)
        refresh_model_performance(seeded_db, "卍", 2025)  # 2回目は上書き

        count = seeded_db.execute(
            "SELECT COUNT(*) FROM model_performance WHERE model_type='卍' AND year=2025"
        ).fetchone()[0]
        assert count == 1   # 重複しない
