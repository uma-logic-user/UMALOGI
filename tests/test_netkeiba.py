"""
src/scraper/netkeiba.py の単体テスト

実際の HTTP リクエストは pytest-mock でモックし、オフラインで実行可能。
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.scraper.netkeiba import (
    HorseResult,
    PedigreeInfo,
    RaceInfo,
    _parse_float,
    _parse_int,
    _parse_rank,
    _parse_results_table,
    fetch_race_results,
)
from src.database.init_db import init_db, insert_race


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------
SAMPLE_RACE_HTML = """
<html><body>
<dl class="racedata fc">
  11 R 東京優駿（日本ダービー）(GI) 芝左2400m&nbsp;/&nbsp;
  天候 : 晴&nbsp;/&nbsp;
  芝 : 良&nbsp;/&nbsp;
  発走 : 15:40
</dl>
<p class="smalltxt">2023年5月28日 5回東京8日目 3歳オープン(国際)(指)(定量)</p>
<table class="race_table_01">
  <tr>
    <th>着順</th><th>枠</th><th>馬番</th><th>馬名</th><th>性齢</th>
    <th>斤量</th><th>騎手</th><th>タイム</th><th>着差</th>
    <th>x</th><th>x</th><th>x</th><th>x</th><th>x</th><th>x</th><th>x</th>
    <th>単勝</th><th>人気</th><th>馬体重</th><th>x</th><th>x</th>
    <th>x</th><th>x</th><th>x</th><th>x</th>
  </tr>
  <tr>
    <td>1</td><td>5</td><td>9</td>
    <td><a href="/horse/2020104451/">タスティエーラ</a></td>
    <td>牡3</td><td>57</td><td>松山弘平</td>
    <td>2:25.2</td><td></td>
    <td>**</td><td>**</td><td>**</td><td>**</td><td>**</td><td>**</td><td>**</td>
    <td>6.1</td><td>3</td><td>476(+2)</td><td></td><td></td>
    <td></td><td></td><td></td><td></td>
  </tr>
  <tr>
    <td>2</td><td>7</td><td>14</td>
    <td><a href="/horse/2020104785/">ソールオリエンス</a></td>
    <td>牡3</td><td>57</td><td>横山武史</td>
    <td>2:25.3</td><td>クビ</td>
    <td>**</td><td>**</td><td>**</td><td>**</td><td>**</td><td>**</td><td>**</td>
    <td>2.7</td><td>1</td><td>488(0)</td><td></td><td></td>
    <td></td><td></td><td></td><td></td>
  </tr>
  <tr>
    <td>除外</td><td>1</td><td>1</td>
    <td><a href="/horse/2020104999/">テストウマ</a></td>
    <td>牡3</td><td>57</td><td>福永祐一</td>
    <td></td><td></td>
    <td></td><td></td><td></td><td></td><td></td><td></td><td></td>
    <td></td><td></td><td></td><td></td><td></td>
    <td></td><td></td><td></td><td></td>
  </tr>
</table>
</body></html>
"""

# blood_table: 32行の実構造を模倣
# row[0].td[0] rowspan=16 → 父、row[16].td[0] rowspan=16 → 母、row[16].td[1] rowspan=8 → 母父
_EMPTY_ROWS = "\n".join(f"  <tr><td>pad{i}</td></tr>" for i in range(1, 16))
SAMPLE_PEDIGREE_HTML = f"""
<html><body>
<table class="blood_table detail">
  <tr>
    <td rowspan="16"><a href="/horse/xxx/">キタサンブラック</a> 2012 黒鹿毛</td>
    <td rowspan="8"><a href="/horse/yyy/">ブラックタイド</a></td>
    <td rowspan="4"><a href="/horse/aaa/">サンデーサイレンス</a></td>
    <td rowspan="2"><a href="/horse/bbb/">Halo</a></td>
    <td><a href="/horse/ccc/">Hail to Reason</a></td>
  </tr>
  {_EMPTY_ROWS}
  <tr>
    <td rowspan="16"><a href="/horse/zzz/">ラヴズオンリーミー</a> 2008 栗毛</td>
    <td rowspan="8"><a href="/horse/www/">Storm Cat</a> 1983 黒鹿毛</td>
    <td rowspan="4"><a href="/horse/vvv/">Storm Bird</a></td>
    <td rowspan="2"><a href="/horse/uuu/">Northern Dancer</a></td>
    <td><a href="/horse/ttt/">Nearctic</a></td>
  </tr>
</table>
</body></html>
"""


@pytest.fixture()
def in_memory_db() -> sqlite3.Connection:
    """テスト用インメモリ DB を返す。"""
    conn = init_db(db_path=Path(":memory:"))
    yield conn
    conn.close()


@pytest.fixture()
def sample_race() -> RaceInfo:
    """DB・保存テスト用のサンプル RaceInfo を返す。"""
    return RaceInfo(
        race_id="202305021211",
        race_name="東京優駿（日本ダービー）",
        date="2023-05-28",
        venue="東京",
        race_number=11,
        distance=2400,
        surface="芝",
        track_direction="左",
        weather="晴",
        condition="良",
        results=[
            HorseResult(
                rank=1,
                horse_name="タスティエーラ",
                horse_id="2020104451",
                gate_number=5,
                horse_number=9,
                sex_age="牡3",
                weight_carried=57.0,
                jockey="松山弘平",
                trainer="堀宣行",
                finish_time="2:25.2",
                margin=None,
                popularity=3,
                win_odds=6.1,
                horse_weight=476,
                horse_weight_diff=2,
                pedigree=PedigreeInfo(
                    sire="サトノクラウン",
                    dam="パルティトゥーラ",
                    dam_sire="Oratorio",
                ),
            ),
        ],
    )


# ---------------------------------------------------------------------------
# _parse_rank
# ---------------------------------------------------------------------------
class TestParseRank:
    def test_正常な着順を返す(self) -> None:
        assert _parse_rank("1") == 1
        assert _parse_rank("18") == 18

    def test_除外は_None(self) -> None:
        assert _parse_rank("除外") is None

    def test_失格は_None(self) -> None:
        assert _parse_rank("失格") is None

    def test_空文字は_None(self) -> None:
        assert _parse_rank("") is None


# ---------------------------------------------------------------------------
# _parse_float
# ---------------------------------------------------------------------------
class TestParseFloat:
    def test_通常の浮動小数(self) -> None:
        assert _parse_float("6.1") == pytest.approx(6.1)

    def test_カンマ区切り(self) -> None:
        assert _parse_float("1,234.5") == pytest.approx(1234.5)

    def test_変換不可は_None(self) -> None:
        assert _parse_float("---") is None

    def test_空文字は_None(self) -> None:
        assert _parse_float("") is None


# ---------------------------------------------------------------------------
# _parse_int
# ---------------------------------------------------------------------------
class TestParseInt:
    def test_馬体重のみ(self) -> None:
        assert _parse_int("476(+2)") == 476

    def test_変動なし(self) -> None:
        assert _parse_int("488(0)") == 488

    def test_変換不可は_None(self) -> None:
        assert _parse_int("計不") is None


# ---------------------------------------------------------------------------
# _parse_results_table
# ---------------------------------------------------------------------------
class TestParseResultsTable:
    def test_サンプルHTMLから2頭を解析(self) -> None:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_RACE_HTML, "lxml")
        rows = _parse_results_table(soup)
        assert len(rows) == 3

        name, horse_id, cells = rows[0]
        assert name == "タスティエーラ"
        assert horse_id == "2020104451"
        assert cells[0] == "1"   # 着順

    def test_除外馬の着順セルが文字列(self) -> None:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_RACE_HTML, "lxml")
        rows = _parse_results_table(soup)
        _, _, cells = rows[2]
        assert _parse_rank(cells[0]) is None

    def test_テーブルなしは空リスト(self) -> None:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        assert _parse_results_table(soup) == []


# ---------------------------------------------------------------------------
# fetch_race_results（HTTP モック）
# ---------------------------------------------------------------------------
class TestFetchRaceResults:
    def test_不正なrace_idで_ValueError(self) -> None:
        with pytest.raises(ValueError, match="不正なレース ID"):
            fetch_race_results("invalid-id")

    def test_空文字のrace_idで_ValueError(self) -> None:
        with pytest.raises(ValueError):
            fetch_race_results("")

    @patch("src.scraper.netkeiba._fetch_html")
    def test_モックHTMLから結果を取得(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = SAMPLE_RACE_HTML

        race = fetch_race_results("202305021211", fetch_pedigree=False)

        assert race.race_id == "202305021211"
        assert race.surface == "芝"
        assert race.distance == 2400
        assert race.weather == "晴"
        assert race.condition == "良"
        assert len(race.results) == 3

        first = race.results[0]
        assert first.horse_name == "タスティエーラ"
        assert first.rank == 1
        assert first.win_odds == pytest.approx(6.1)
        assert first.horse_weight == 476

        second = race.results[1]
        assert second.rank == 2
        assert second.margin == "クビ"

    @patch("requests.get")
    def test_リトライ後に成功(self, mock_get: MagicMock) -> None:
        # 1回目: 接続エラー、2回目: 正常レスポンス
        ok_resp = MagicMock()
        ok_resp.raise_for_status = MagicMock()
        ok_resp.apparent_encoding = "utf-8"
        ok_resp.text = SAMPLE_RACE_HTML

        mock_get.side_effect = [
            requests.ConnectionError("接続エラー"),
            ok_resp,
        ]

        with patch("time.sleep"):  # テスト時のスリープをスキップ
            race = fetch_race_results("202305021211", fetch_pedigree=False, delay=0)

        assert race.race_id == "202305021211"
        assert mock_get.call_count == 2

    @patch("src.scraper.netkeiba._fetch_html")
    def test_リトライ上限超過で例外(self, mock_fetch: MagicMock) -> None:
        mock_fetch.side_effect = requests.ConnectionError("接続エラー")

        with patch("time.sleep"):
            with pytest.raises(requests.RequestException):
                fetch_race_results("202305021211", fetch_pedigree=False, max_retries=2)

    @patch("src.scraper.netkeiba._fetch_pedigree")
    @patch("src.scraper.netkeiba._fetch_html")
    def test_血統情報を取得(
        self, mock_fetch: MagicMock, mock_pedigree: MagicMock
    ) -> None:
        mock_fetch.return_value = SAMPLE_RACE_HTML
        mock_pedigree.return_value = PedigreeInfo(
            sire="キタサンブラック", dam="ラヴズオンリーミー", dam_sire="Storm Cat"
        )

        race = fetch_race_results("202305021211", fetch_pedigree=True)

        assert race.results[0].pedigree.sire == "キタサンブラック"
        assert race.results[0].pedigree.dam == "ラヴズオンリーミー"


# ---------------------------------------------------------------------------
# init_db / insert_race
# ---------------------------------------------------------------------------
class TestDatabase:
    def test_テーブルが存在する(self, in_memory_db: sqlite3.Connection) -> None:
        tables = {
            row[0]
            for row in in_memory_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"races", "horses", "race_results"}.issubset(tables)

    def test_レース結果を保存できる(
        self,
        in_memory_db: sqlite3.Connection,
        sample_race: RaceInfo,
    ) -> None:
        insert_race(in_memory_db, sample_race)

        row = in_memory_db.execute(
            "SELECT race_name, distance FROM races WHERE race_id = ?",
            (sample_race.race_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "東京優駿（日本ダービー）"
        assert row[1] == 2400

    def test_着順と単勝オッズが保存される(
        self,
        in_memory_db: sqlite3.Connection,
        sample_race: RaceInfo,
    ) -> None:
        insert_race(in_memory_db, sample_race)

        row = in_memory_db.execute(
            "SELECT rank, win_odds, horse_weight FROM race_results WHERE race_id = ?",
            (sample_race.race_id,),
        ).fetchone()
        assert row[0] == 1
        assert row[1] == pytest.approx(6.1)
        assert row[2] == 476

    def test_血統情報が馬マスタに保存される(
        self,
        in_memory_db: sqlite3.Connection,
        sample_race: RaceInfo,
    ) -> None:
        insert_race(in_memory_db, sample_race)

        row = in_memory_db.execute(
            "SELECT sire, dam, dam_sire FROM horses WHERE horse_id = ?",
            ("2020104451",),
        ).fetchone()
        assert row[0] == "サトノクラウン"
        assert row[1] == "パルティトゥーラ"
        assert row[2] == "Oratorio"

    def test_同一レースの二重挿入は無視される(
        self,
        in_memory_db: sqlite3.Connection,
        sample_race: RaceInfo,
    ) -> None:
        insert_race(in_memory_db, sample_race)
        insert_race(in_memory_db, sample_race)  # 2回目は無視

        count = in_memory_db.execute(
            "SELECT COUNT(*) FROM race_results WHERE race_id = ?",
            (sample_race.race_id,),
        ).fetchone()[0]
        assert count == 1
