"""
v_race_mart ビューの動作を検証するユニットテスト。

カバー範囲:
  - ビューが init_db() 後に存在すること
  - 列数が 63 であること
  - 基本的なレース・出走データが正しく取得できること
  - 払戻 (payout_tansho / payout_fukusho) が horse_number で正しく結合されること
  - training_times / training_hillwork の直近取得ロジック
      - レース日より前の最新レコードが取得されること
      - training_date = '' の行はマッチしないこと（空文字ガード）
      - レース日以降の調教は取得されないこと
  - query_mart() ヘルパーのフィルタ動作
"""

import sqlite3
from pathlib import Path

import pytest

from src.database.init_db import init_db, query_mart


# ── フィクスチャ ──────────────────────────────────────────────────────────────

@pytest.fixture()
def db() -> sqlite3.Connection:
    conn = init_db(db_path=Path(":memory:"))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture()
def seeded_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """races / race_results / race_payouts を投入済みの DB を返す。"""
    with db:
        db.execute(
            """
            INSERT INTO races
                (race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("202501010101", "テスト新馬戦", "2025/01/05",
             "中山", 1, 1600, "芝", "右", "晴", "良"),
        )
        # horses 親レコードを先に挿入（race_results.horse_id FK のため必須）
        db.execute(
            "INSERT INTO horses (horse_id, horse_name, sire, dam, dam_sire) VALUES (?, ?, ?, ?, ?)",
            ("2023105001", "テスト馬A", "ディープインパクト", "テスト母A", "キングカメハメハ"),
        )
        db.execute(
            "INSERT INTO horses (horse_id, horse_name, sire, dam, dam_sire) VALUES (?, ?, ?, ?, ?)",
            ("2023105002", "テスト馬B", "ハーツクライ", "テスト母B", "サンデーサイレンス"),
        )
        # 馬1: horse_number=3（1着）
        db.execute(
            """
            INSERT INTO race_results
                (race_id, horse_id, horse_name, rank, gate_number, horse_number,
                 sex_age, weight_carried, jockey, trainer, finish_time,
                 popularity, win_odds, horse_weight, horse_weight_diff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("202501010101", "2023105001", "テスト馬A", 1, 2, 3,
             "牡2", 54.0, "武豊", "藤原英昭", "1:35.0", 1, 2.5, 460, 2),
        )
        # 馬2: horse_number=7（2着）
        db.execute(
            """
            INSERT INTO race_results
                (race_id, horse_id, horse_name, rank, gate_number, horse_number,
                 sex_age, weight_carried, jockey, trainer, finish_time,
                 popularity, win_odds, horse_weight, horse_weight_diff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("202501010101", "2023105002", "テスト馬B", 2, 4, 7,
             "牝2", 54.0, "川田将雅", "矢作芳人", "1:35.2", 2, 4.0, 448, -4),
        )
        # 払戻: 単勝3番・複勝3番・複勝7番
        db.execute(
            "INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity) "
            "VALUES (?, ?, ?, ?, ?)",
            ("202501010101", "単勝", "3", 250, 1),
        )
        db.execute(
            "INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity) "
            "VALUES (?, ?, ?, ?, ?)",
            ("202501010101", "複勝", "3", 120, 1),
        )
        db.execute(
            "INSERT INTO race_payouts (race_id, bet_type, combination, payout, popularity) "
            "VALUES (?, ?, ?, ?, ?)",
            ("202501010101", "複勝", "7", 180, 2),
        )
    return db


# ── ビュー存在・構造テスト ─────────────────────────────────────────────────────

class TestViewStructure:
    def test_ビューが存在する(self, db: sqlite3.Connection) -> None:
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='v_race_mart'"
        ).fetchone()
        assert row is not None, "v_race_mart ビューが存在しない"

    def test_列数が63(self, db: sqlite3.Connection) -> None:
        """ビュー定義の列数が仕様通り 63 であることを確認する。"""
        cols = db.execute("PRAGMA table_info(v_race_mart)").fetchall()
        assert len(cols) == 63, f"列数が期待値(63)と異なる: {len(cols)}"

    def test_必須列がすべて存在する(self, db: sqlite3.Connection) -> None:
        col_names = {row["name"] for row in db.execute("PRAGMA table_info(v_race_mart)")}
        required = {
            # races
            "race_id", "date", "year", "month", "venue", "race_number",
            "distance", "surface", "track_direction", "condition", "weather",
            # race_results
            "result_id", "horse_id", "horse_number", "gate_number", "horse_name",
            "sex_age", "rank", "win_odds", "popularity", "finish_time",
            "horse_weight", "horse_weight_diff", "weight_carried", "jockey", "trainer",
            # race_payouts
            "payout_tansho", "payout_fukusho",
            # horses
            "sire", "dam", "dam_sire",
            # racehorses
            "birth_year", "um_sex", "coat_color", "country",
            "father_id", "father_name", "grandsire_id", "grandsire_name", "horse_east_west",
            # jockeys
            "jockey_code", "jockey_east_west", "jockey_license_year",
            # trainers
            "trainer_code", "trainer_east_west", "stable_name",
            # breeding_horses
            "father_country", "father_birth_year",
            "father_sire_id", "father_sire_name", "father_dam_id", "father_dam_name",
            # training_times
            "last_tc_date", "last_tc_4f", "last_tc_3f", "last_tc_lap",
            "last_tc_course", "last_tc_gear",
            # training_hillwork
            "last_hc_date", "last_hc_4f", "last_hc_3f", "last_hc_lap", "last_hc_gear",
        }
        missing = required - col_names
        assert not missing, f"ビューに必須列が不足: {missing}"


# ── 基本データ取得テスト ──────────────────────────────────────────────────────

class TestBasicJoin:
    def test_行数がrace_results件数と一致する(self, seeded_db: sqlite3.Connection) -> None:
        rows = seeded_db.execute("SELECT * FROM v_race_mart").fetchall()
        assert len(rows) == 2

    def test_レース情報が正しく取得できる(self, seeded_db: sqlite3.Connection) -> None:
        row = seeded_db.execute(
            "SELECT * FROM v_race_mart WHERE horse_name = 'テスト馬A'"
        ).fetchone()
        assert row is not None
        assert row["race_id"]   == "202501010101"
        assert row["venue"]     == "中山"
        assert row["distance"]  == 1600
        assert row["surface"]   == "芝"
        assert row["year"]      == "2025"
        assert row["month"]     == "01"

    def test_出走馬情報が正しく取得できる(self, seeded_db: sqlite3.Connection) -> None:
        row = seeded_db.execute(
            "SELECT * FROM v_race_mart WHERE horse_name = 'テスト馬A'"
        ).fetchone()
        assert row["rank"]          == 1
        assert row["horse_number"]  == 3
        assert row["jockey"]        == "武豊"
        assert row["win_odds"]      == pytest.approx(2.5)

    def test_払戻が馬番で正しく結合される(self, seeded_db: sqlite3.Connection) -> None:
        rows = {
            r["horse_name"]: r
            for r in seeded_db.execute("SELECT * FROM v_race_mart").fetchall()
        }
        # 馬A (horse_number=3): 単勝250 / 複勝120
        assert rows["テスト馬A"]["payout_tansho"]  == 250
        assert rows["テスト馬A"]["payout_fukusho"] == 120
        # 馬B (horse_number=7): 単勝なし / 複勝180
        assert rows["テスト馬B"]["payout_tansho"]  is None
        assert rows["テスト馬B"]["payout_fukusho"] == 180


# ── 調教データ結合テスト ──────────────────────────────────────────────────────

class TestTrainingJoin:
    """直近調教タイムの相関サブクエリを検証する。"""

    RACE_DATE = "2025/01/05"
    HORSE_ID  = "2023105001"

    def _insert_tc(
        self,
        db: sqlite3.Connection,
        training_date: str,
        time_3f: float = 35.0,
    ) -> None:
        with db:
            db.execute(
                """
                INSERT OR IGNORE INTO training_times
                    (horse_id, horse_name, training_date, course_type, direction,
                     time_4f, time_3f, lap_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (self.HORSE_ID, "テスト馬A", training_date, "坂路", "左",
                 50.0, time_3f, 12.0),
            )

    def _insert_hc(
        self,
        db: sqlite3.Connection,
        training_date: str,
        time_3f: float = 37.0,
    ) -> None:
        with db:
            db.execute(
                """
                INSERT OR IGNORE INTO training_hillwork
                    (horse_id, horse_name, training_date, time_4f, time_3f, lap_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.HORSE_ID, "テスト馬A", training_date, 52.0, time_3f, 12.5),
            )

    def test_レース日前の最新調教タイムが取得される(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        # 古い調教 (2025/01/02) と直前調教 (2025/01/04) を追加
        self._insert_tc(seeded_db, "2025/01/02", time_3f=36.0)
        self._insert_tc(seeded_db, "2025/01/04", time_3f=34.5)  # こちらが取得されるべき

        row = seeded_db.execute(
            "SELECT last_tc_date, last_tc_3f FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        assert row["last_tc_date"] == "2025/01/04"
        assert row["last_tc_3f"]   == pytest.approx(34.5)

    def test_レース日以降の調教は取得されない(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        # レース当日 (2025/01/05) と前日 (2025/01/04) を追加
        self._insert_tc(seeded_db, "2025/01/05", time_3f=33.0)  # レース当日: 除外
        self._insert_tc(seeded_db, "2025/01/04", time_3f=35.5)  # こちらが取得されるべき

        row = seeded_db.execute(
            "SELECT last_tc_date FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        assert row["last_tc_date"] == "2025/01/04"

    def test_調教なし馬はNULL(self, seeded_db: sqlite3.Connection) -> None:
        row = seeded_db.execute(
            "SELECT last_tc_date, last_tc_3f FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        assert row["last_tc_date"] is None
        assert row["last_tc_3f"]   is None

    def test_training_date空文字はマッチしない(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        """空文字ガード: training_date='' の行は最新日として採用されない。"""
        self._insert_tc(seeded_db, "",           time_3f=99.9)  # 除外されるべき
        self._insert_tc(seeded_db, "2025/01/03", time_3f=35.0)  # こちらが取得されるべき

        row = seeded_db.execute(
            "SELECT last_tc_date, last_tc_3f FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        # 空文字行がマッチしていれば last_tc_3f = 99.9 になる（バグ検出）
        assert row["last_tc_date"] == "2025/01/03"
        assert row["last_tc_3f"]   == pytest.approx(35.0)

    def test_hillwork_空文字はマッチしない(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        """坂路調教でも空文字ガードが有効か確認する。"""
        self._insert_hc(seeded_db, "",           time_3f=88.8)  # 除外されるべき
        self._insert_hc(seeded_db, "2025/01/03", time_3f=37.5)  # こちらが取得されるべき

        row = seeded_db.execute(
            "SELECT last_hc_date, last_hc_3f FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        assert row["last_hc_date"] == "2025/01/03"
        assert row["last_hc_3f"]   == pytest.approx(37.5)

    def test_hillwork_レース日前の最新が取得される(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        self._insert_hc(seeded_db, "2025/01/01", time_3f=38.0)
        self._insert_hc(seeded_db, "2025/01/04", time_3f=36.8)  # こちらが取得されるべき

        row = seeded_db.execute(
            "SELECT last_hc_date, last_hc_3f FROM v_race_mart WHERE horse_name='テスト馬A'"
        ).fetchone()
        assert row["last_hc_date"] == "2025/01/04"
        assert row["last_hc_3f"]   == pytest.approx(36.8)


# ── query_mart() ヘルパーテスト ────────────────────────────────────────────────

class TestQueryMart:
    def test_全行取得(self, seeded_db: sqlite3.Connection) -> None:
        rows = query_mart(seeded_db)
        assert len(rows) == 2

    def test_year_フィルタ(self, seeded_db: sqlite3.Connection) -> None:
        assert len(query_mart(seeded_db, year="2025")) == 2
        assert len(query_mart(seeded_db, year="2024")) == 0

    def test_venue_フィルタ(self, seeded_db: sqlite3.Connection) -> None:
        assert len(query_mart(seeded_db, venue="中山")) == 2
        assert len(query_mart(seeded_db, venue="東京")) == 0

    def test_surface_フィルタ(self, seeded_db: sqlite3.Connection) -> None:
        assert len(query_mart(seeded_db, surface="芝"))    == 2
        assert len(query_mart(seeded_db, surface="ダート")) == 0

    def test_date_from_to_フィルタ(self, seeded_db: sqlite3.Connection) -> None:
        assert len(query_mart(seeded_db, date_from="2025/01/01", date_to="2025/01/31")) == 2
        assert len(query_mart(seeded_db, date_from="2025/02/01")) == 0

    def test_結果はSQLiteRow型(self, seeded_db: sqlite3.Connection) -> None:
        rows = query_mart(seeded_db)
        assert isinstance(rows[0], sqlite3.Row)
