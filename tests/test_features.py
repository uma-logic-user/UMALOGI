"""
src/ml/features.py の FeatureBuilder ユニットテスト

リークテストも含む:
  build_race_features_for_simulate() が rank / finish_time / margin を
  出力 DataFrame に含めないことを検証する。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.database.init_db import init_db, insert_race
from src.ml.features import FeatureBuilder, _distance_band, _parse_sex
from src.scraper.netkeiba import HorseResult, PedigreeInfo, RaceInfo

# レース確定後にしか判明しない「リーク禁止列」
_LEAK_COLS: frozenset[str] = frozenset({"rank", "finish_time", "margin"})


# ── リークテスト用ヘルパー ─────────────────────────────────────────


def _make_leak_test_race(race_id: str, n_horses: int = 6) -> RaceInfo:
    """リークテスト用のシンプルなレースデータを生成する（YYYY-MM-DD 形式の日付）。"""
    results = [
        HorseResult(
            rank=i,
            horse_name=f"馬{i:02d}",
            horse_id=f"h{race_id}{i:02d}",
            gate_number=((i - 1) // 2) + 1,
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
        for i in range(1, n_horses + 1)
    ]
    return RaceInfo(
        race_id=race_id,
        race_name=f"テストレース{race_id}",
        date="2024-05-01",
        venue="東京",
        race_number=1,
        distance=1600,
        surface="芝",
        track_direction="左",
        weather="晴",
        condition="良",
        results=results,
    )


@pytest.fixture()
def db_single_leak() -> sqlite3.Connection:
    """1レース分の race_results が入った in-memory DB（リークテスト専用）。"""
    conn = init_db(db_path=Path(":memory:"))
    insert_race(conn, _make_leak_test_race("2024010101"))
    yield conn
    conn.close()


# ── リーク防止テスト ──────────────────────────────────────────────


class TestLeakPrevention:
    """
    build_race_features_for_simulate() がレース確定後情報を含まないことを検証する。

    以下の列はレース終了後にしか判明しない「未来データ」であり、
    特徴量 DataFrame に絶対に混入してはならない:
      - rank        (着順)
      - finish_time (タイム)
      - margin      (着差)
    """

    def test_rank列が含まれない(self, db_single_leak: sqlite3.Connection) -> None:
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        assert not df.empty, "特徴量 DataFrame が空です"
        assert "rank" not in df.columns, "'rank' 列がリークしています"

    def test_finish_time列が含まれない(self, db_single_leak: sqlite3.Connection) -> None:
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        assert "finish_time" not in df.columns, "'finish_time' 列がリークしています"

    def test_margin列が含まれない(self, db_single_leak: sqlite3.Connection) -> None:
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        assert "margin" not in df.columns, "'margin' 列がリークしています"

    def test_リーク列が一切含まれない(self, db_single_leak: sqlite3.Connection) -> None:
        """_LEAK_COLS のいずれも含まれないことを一括検証する。"""
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        leaked = _LEAK_COLS & set(df.columns)
        assert not leaked, f"リーク列が検出されました: {leaked}"

    def test_必須特徴量列が存在する(self, db_single_leak: sqlite3.Connection) -> None:
        """リーク除外後も非リーク列が揃っていることを確認。"""
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        required: frozenset[str] = frozenset(
            {"horse_name", "horse_number", "win_odds", "weight_carried"}
        )
        missing = required - set(df.columns)
        assert not missing, f"必須列が欠落しています: {missing}"

    def test_行数が出走頭数と一致する(self, db_single_leak: sqlite3.Connection) -> None:
        """6頭立てレースなら 6 行の DataFrame が返ること。"""
        fb = FeatureBuilder(db_single_leak)
        df = fb.build_race_features_for_simulate("2024010101")
        assert len(df) == 6

    def test_複数レース全てリーク無し(self) -> None:
        """複数レースを投入した DB でも全レース分リーク無しを検証する。"""
        conn = init_db(db_path=Path(":memory:"))
        race_ids: list[str] = []
        for i in range(1, 6):
            rid = f"20240101{i:02d}"
            insert_race(conn, _make_leak_test_race(rid))
            race_ids.append(rid)

        fb = FeatureBuilder(conn)
        for rid in race_ids:
            df = fb.build_race_features_for_simulate(rid)
            leaked = _LEAK_COLS & set(df.columns)
            assert not leaked, f"レース {rid} でリーク列が検出されました: {leaked}"
        conn.close()


# ── フィクスチャ ──────────────────────────────────────────────────

@pytest.fixture()
def db() -> sqlite3.Connection:
    conn = init_db(db_path=Path(":memory:"))
    yield conn
    conn.close()


@pytest.fixture()
def seeded_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """有馬記念ダミーデータ（races + horses + race_results + entries）を投入済み DB。"""
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
                horse_id="2022105081",
                gate_number=1, horse_number=1,
                sex_age="牡3",
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
                rank=2, horse_name="レガレイラ",
                horse_id="2021105898",
                gate_number=2, horse_number=2,
                sex_age="牝4",
                weight_carried=55.0, jockey="横山武史",
                trainer="木村哲也",
                finish_time="2:31.7", margin="0.2",
                popularity=1, win_odds=3.3, horse_weight=482,
                horse_weight_diff=-4,
                pedigree=PedigreeInfo(sire="スワーヴリチャード",
                                      dam="ロカ", dam_sire="ハービンジャー"),
            ),
        ],
    )
    insert_race(db, race)

    # entries テーブルに手動挿入
    with db:
        db.executemany(
            """
            INSERT INTO entries
                (race_id, horse_number, gate_number, horse_id, horse_name,
                 sex_age, weight_carried, jockey, trainer,
                 horse_weight, horse_weight_diff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("202506050811", 1, 1, "2022105081", "ミュージアムマイル",
                 "牡3", 56.0, "Ｃ．デム", "国枝栄", 502, 2),
                ("202506050811", 2, 2, "2021105898", "レガレイラ",
                 "牝4", 55.0, "横山武史", "木村哲也", 482, -4),
            ],
        )
    return db


# ── ユーティリティ関数 ────────────────────────────────────────────

class TestDistanceBand:
    def test_スプリント(self) -> None:
        assert _distance_band(1200) == "sprint"

    def test_マイル境界(self) -> None:
        assert _distance_band(1400) == "mile"

    def test_中距離(self) -> None:
        assert _distance_band(2000) == "intermediate"

    def test_長距離(self) -> None:
        assert _distance_band(2500) == "long"


class TestParseSex:
    def test_牡(self) -> None:
        assert _parse_sex("牡3") == "牡"

    def test_牝(self) -> None:
        assert _parse_sex("牝4") == "牝"

    def test_セン馬(self) -> None:
        assert _parse_sex("セ5") == "セ"

    def test_不正文字列(self) -> None:
        assert _parse_sex("unknown") == ""


# ── FeatureBuilder ────────────────────────────────────────────────

class TestFeatureBuilder:
    def test_DataFrameの行数は出走頭数(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        assert len(df) == 2

    def test_識別子列が存在する(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        assert "horse_number" in df.columns
        assert "horse_name" in df.columns

    def test_数値特徴量が含まれる(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        for col in ("weight_carried", "horse_weight"):
            assert col in df.columns

    def test_カテゴリ特徴量が整数(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        # 芝=0
        assert df["surface_code"].iloc[0] == 0
        # 中山=5
        assert df["venue_encoded"].iloc[0] == 5

    def test_性別コード牡0牝1(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        sex_codes = df.set_index("horse_name")["sex_code"]
        assert sex_codes["ミュージアムマイル"] == 0  # 牡
        assert sex_codes["レガレイラ"] == 1          # 牝

    def test_win_rate_allが計算される(self, seeded_db: sqlite3.Connection) -> None:
        """過去成績がある馬は win_rate_all が 0〜1 の範囲。"""
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        row = df[df["horse_name"] == "ミュージアムマイル"].iloc[0]
        assert row["win_rate_all"] is not None
        assert 0.0 <= row["win_rate_all"] <= 1.0

    def test_存在しないrace_idでValueError(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        with pytest.raises(ValueError, match="race_id"):
            fb.build_race_features("999999999999")

    def test_sire_encodedが異なる父に異なる整数を返す(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        codes = df["sire_encoded"].tolist()
        # 2頭いて父が異なる → エンコード値も異なる
        assert codes[0] != codes[1]

    def test_realtime_oddsなしでもwin_oddsはNone(self, seeded_db: sqlite3.Connection) -> None:
        fb = FeatureBuilder(seeded_db)
        df = fb.build_race_features("202506050811")
        # realtime_odds テーブルが空なので None になる
        assert df["win_odds"].iloc[0] is None or df["win_odds"].isna().all()


# ── insert_entries / insert_realtime_odds の DB テスト ──────────

class TestInsertEntries:
    def test_出馬表を保存してカウントを返す(self, db: sqlite3.Connection) -> None:
        # entries テーブルには races FK が必要なので先にレースを挿入
        with db:
            db.execute(
                "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface) "
                "VALUES ('test001', 'テスト', '2025/01/01', '東京', 1, 1600, '芝')"
            )

        from src.database.init_db import insert_entries
        from src.scraper.entry_table import EntryHorse

        entries = [
            EntryHorse(
                horse_number=1, gate_number=1, horse_id=None,
                horse_name="テスト馬A", sex_age="牡3",
                weight_carried=56.0, jockey="テスト騎手",
                trainer="テスト調教師", horse_weight=500,
                horse_weight_diff=0,
            ),
        ]
        count = insert_entries(db, "test001", entries)
        assert count == 1

        row = db.execute(
            "SELECT horse_name FROM entries WHERE race_id='test001' AND horse_number=1"
        ).fetchone()
        assert row[0] == "テスト馬A"

    def test_UPSERT同一馬番を上書き(self, db: sqlite3.Connection) -> None:
        with db:
            db.execute(
                "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface) "
                "VALUES ('test002', 'テスト2', '2025/01/01', '東京', 2, 1600, '芝')"
            )

        from src.database.init_db import insert_entries
        from src.scraper.entry_table import EntryHorse

        base = EntryHorse(
            horse_number=1, gate_number=1, horse_id=None,
            horse_name="テスト馬A", sex_age="牡3",
            weight_carried=56.0, jockey="騎手A",
            trainer="調教師A", horse_weight=500, horse_weight_diff=0,
        )
        insert_entries(db, "test002", [base])

        updated = EntryHorse(
            horse_number=1, gate_number=1, horse_id=None,
            horse_name="テスト馬A", sex_age="牡3",
            weight_carried=57.0, jockey="騎手B",
            trainer="調教師A", horse_weight=500, horse_weight_diff=0,
        )
        insert_entries(db, "test002", [updated])

        row = db.execute(
            "SELECT weight_carried, jockey FROM entries WHERE race_id='test002' AND horse_number=1"
        ).fetchone()
        assert row[0] == 57.0
        assert row[1] == "騎手B"
        count = db.execute("SELECT COUNT(*) FROM entries WHERE race_id='test002'").fetchone()[0]
        assert count == 1  # 重複なし


class TestInsertRealtimeOdds:
    def test_オッズスナップショットを保存する(self, db: sqlite3.Connection) -> None:
        with db:
            db.execute(
                "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface) "
                "VALUES ('test003', 'テスト3', '2025/01/01', '中山', 3, 2000, '芝')"
            )

        from src.database.init_db import insert_realtime_odds
        from src.scraper.entry_table import HorseOdds

        odds = [
            HorseOdds(horse_number=1, win_odds=3.8, place_odds_min=2.0,
                      place_odds_max=3.5, popularity=3),
            HorseOdds(horse_number=2, win_odds=5.1, place_odds_min=1.5,
                      place_odds_max=2.8, popularity=1),
        ]
        count = insert_realtime_odds(db, "test003", odds,
                                     horse_name_map={1: "馬A", 2: "馬B"})
        assert count == 2

        row = db.execute(
            "SELECT win_odds, place_odds_min, popularity FROM realtime_odds "
            "WHERE race_id='test003' AND horse_number=1"
        ).fetchone()
        assert row[0] == pytest.approx(3.8)
        assert row[1] == pytest.approx(2.0)
        assert row[2] == 3

    def test_複数回保存で履歴が積まれる(self, db: sqlite3.Connection) -> None:
        with db:
            db.execute(
                "INSERT INTO races (race_id, race_name, date, venue, race_number, distance, surface) "
                "VALUES ('test004', 'テスト4', '2025/01/01', '阪神', 4, 1800, 'ダート')"
            )

        from src.database.init_db import insert_realtime_odds
        from src.scraper.entry_table import HorseOdds

        odds = [HorseOdds(horse_number=1, win_odds=3.8, place_odds_min=None,
                          place_odds_max=None, popularity=1)]
        insert_realtime_odds(db, "test004", odds)
        insert_realtime_odds(db, "test004", odds)  # 2 回目

        count = db.execute(
            "SELECT COUNT(*) FROM realtime_odds WHERE race_id='test004'"
        ).fetchone()[0]
        assert count == 2  # 履歴が2行ある
