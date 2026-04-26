"""
スクレイパーバリデーション自動テスト

以下の欠損データをスクレイパーが出力しないことを検証する:
  - 距離 0m
  - 頭数 4 頭未満
  - 1着馬不在（rank=1 なし）
  - 距離・馬場種別・コース方向の正常パース
  - 障害レース・外回りコース等の特殊フォーマット対応

すべてのテストはオフラインで動作する（HTTP リクエストなし / patch 使用）。

注意:
  _parse_results_table は table.RaceTable01 + tr.HorseList を使用（2025年版フォーマット）。
  _parse_race_info は div.RaceData01 / div.RaceData02 を使用。
  track_direction は "芝右2000m" 形式（距離前置）で抽出。
  "(右)" 形式（距離後置・括弧内）は現在の正規表現では抽出されない（既知の制限）。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.scraper.netkeiba import (
    HorseResult,
    PedigreeInfo,
    RaceInfo,
    _parse_race_info,
    fetch_race_results,
)


# ---------------------------------------------------------------------------
# テスト用 HTML ファクトリ
# ---------------------------------------------------------------------------

def _make_race_html(
    data01_text: str = "09:45発走 / 芝右2000m / 天候:晴 / 馬場:良",
    data02_text: str = "1回 中山 3日目",
    horses: list[tuple[int, str, float]] | None = None,
) -> str:
    """
    テスト用の最小 HTML を生成する。

    table.RaceTable01 + tr.HorseList を使用（2025年版 netkeiba フォーマット）。

    Args:
        data01_text: RaceData01 div のテキスト（距離・天候・馬場を含む）
        data02_text: RaceData02 div のテキスト（開催情報）
        horses: [(着順, 馬名, 単勝オッズ), ...] のリスト
    """
    if horses is None:
        horses = [
            (1, "テスト馬A", 2.5),
            (2, "テスト馬B", 3.1),
            (3, "テスト馬C", 5.0),
            (4, "テスト馬D", 8.0),
            (5, "テスト馬E", 12.0),
        ]

    rows = ""
    for rank, name, odds in horses:
        rows += f"""
  <tr class="HorseList">
    <td>{rank}</td><td>1</td><td>{rank}</td>
    <td><a href="/horse/202000000{rank}/">{name}</a></td>
    <td>牡3</td><td>57</td><td>テスト騎手</td><td>テスト調教師</td>
    <td>2:00.0</td><td></td>
    <td></td><td></td><td></td><td></td><td></td><td></td><td></td>
    <td>{odds}</td><td>{rank}</td><td>480(0)</td><td></td><td></td>
    <td></td><td></td><td></td><td></td>
  </tr>"""

    return f"""
<html><body>
<div class="RaceName">テストレース</div>
<div class="RaceData01">{data01_text}</div>
<div class="RaceData02">{data02_text}</div>
<table class="RaceTable01">
  <tr>
    <th>着順</th><th>枠</th><th>馬番</th><th>馬名</th><th>性齢</th>
    <th>斤量</th><th>騎手</th><th>調教師</th><th>タイム</th><th>着差</th>
    <th>x</th><th>x</th><th>x</th><th>x</th><th>x</th><th>x</th><th>x</th>
    <th>単勝</th><th>人気</th><th>馬体重</th><th>x</th><th>x</th>
    <th>x</th><th>x</th><th>x</th><th>x</th>
  </tr>
  {rows}
</table>
</body></html>
"""


def _parse(data01_text: str) -> RaceInfo:
    """data01_text だけを変えて _parse_race_info を呼ぶヘルパー。"""
    from bs4 import BeautifulSoup
    html = _make_race_html(data01_text=data01_text)
    soup = BeautifulSoup(html, "lxml")
    return _parse_race_info(soup, "202304050601")


# ---------------------------------------------------------------------------
# 距離パース検証
# ---------------------------------------------------------------------------
class TestDistanceParse:
    """_parse_race_info による距離パースの正確性を検証する。"""

    def test_芝2000m(self) -> None:
        info = _parse("09:45発走 / 芝右2000m / 天候:晴 / 馬場:良")
        assert info.distance == 2000
        assert info.surface == "芝"
        assert info.track_direction == "右"

    def test_ダート1700m(self) -> None:
        info = _parse("09:45発走 / ダ1700m / 天候:晴 / 馬場:良")
        assert info.distance == 1700
        assert info.surface == "ダート"

    def test_ダート表記バリエーション(self) -> None:
        info = _parse("09:45発走 / ダート左1400m / 天候:曇 / 馬場:稍重")
        assert info.distance == 1400
        assert info.surface == "ダート"
        assert info.track_direction == "左"

    def test_障害2970m(self) -> None:
        """障害レースは '障' プレフィックス。surface='障害' で保存される。"""
        info = _parse("09:45発走 / 障右2970m / 天候:晴 / 馬場:良")
        assert info.distance == 2970
        assert info.surface == "障害"

    def test_障害フルスペル(self) -> None:
        info = _parse("09:45発走 / 障害右3170m / 天候:晴 / 馬場:良")
        assert info.distance == 3170
        assert info.surface == "障害"

    def test_右外コース(self) -> None:
        """外回りコースは '右外' または '右 外' で表記される。"""
        info = _parse("09:45発走 / 芝右外1600m / 天候:晴 / 馬場:良")
        assert info.distance == 1600
        assert info.track_direction == "右外"

    def test_左外コース(self) -> None:
        info = _parse("09:45発走 / 芝左外1800m / 天候:晴 / 馬場:良")
        assert info.distance == 1800
        assert info.track_direction == "左外"

    def test_距離が0にならない_正常HTML(self) -> None:
        """正常な HTML では距離は必ず 0 以外になる。"""
        info = _parse("09:45発走 / 芝左2400m / 天候:晴 / 馬場:良")
        assert info.distance != 0, "距離が 0 になってはいけない"
        assert info.distance > 0

    def test_短距離1000m_直線(self) -> None:
        info = _parse("10:00発走 / 芝直線1000m / 天候:晴 / 馬場:良")
        assert info.distance == 1000
        assert info.surface == "芝"

    def test_右外スペース入り(self) -> None:
        """'右 外' 形式（スペース入り）でも方向が正しくパースされる。"""
        info = _parse("09:45発走 / 芝右 外1600m / 天候:晴 / 馬場:良")
        assert info.distance == 1600
        assert info.track_direction == "右外"  # スペースは除去される

    def test_天候パース(self) -> None:
        info = _parse("09:45発走 / 芝右2000m / 天候:曇 / 馬場:稍重")
        assert info.weather == "曇"

    def test_馬場状態パース(self) -> None:
        info = _parse("09:45発走 / 芝右2000m / 天候:晴 / 馬場:重")
        assert info.condition == "重"


# ---------------------------------------------------------------------------
# fetch_race_results バリデーション検証
# ---------------------------------------------------------------------------
class TestFetchRaceResultsValidation:
    """fetch_race_results が返す RaceInfo のバリデーション条件を検証する。"""

    @patch("src.scraper.netkeiba._fetch_html")
    def test_通常レースに1着馬が存在する(self, mock_fetch: MagicMock) -> None:
        """通常レースは必ず rank=1 の馬が存在する。"""
        mock_fetch.return_value = _make_race_html()
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        assert any(h.rank == 1 for h in race.results), "1着馬が存在しない"

    @patch("src.scraper.netkeiba._fetch_html")
    def test_通常レースの頭数は4頭以上(self, mock_fetch: MagicMock) -> None:
        """本番レースの出走頭数は最低 4 頭以上。"""
        mock_fetch.return_value = _make_race_html()
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        ranked = [h for h in race.results if h.rank is not None]
        assert len(ranked) >= 4, f"頭数が不足: {len(ranked)}頭"

    @patch("src.scraper.netkeiba._fetch_html")
    def test_距離が0mにならない(self, mock_fetch: MagicMock) -> None:
        """正常な HTML では距離フィールドは 0 以外でなければならない。"""
        mock_fetch.return_value = _make_race_html(
            data01_text="09:45発走 / 芝右2000m / 天候:晴 / 馬場:良"
        )
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        assert race.distance != 0, f"距離が 0m になった (race_id={race.race_id})"
        assert race.distance > 0

    @patch("src.scraper.netkeiba._fetch_html")
    def test_障害レース距離が正しくパース(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = _make_race_html(
            data01_text="09:45発走 / 障右2970m / 天候:晴 / 馬場:良"
        )
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        assert race.distance == 2970
        assert race.surface == "障害"

    @patch("src.scraper.netkeiba._fetch_html")
    def test_外回りコースのtrack_direction(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = _make_race_html(
            data01_text="09:45発走 / 芝右外1600m / 天候:晴 / 馬場:良"
        )
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        assert race.track_direction == "右外", (
            f"外回り方向が正しくない: {race.track_direction!r}"
        )

    @patch("src.scraper.netkeiba._fetch_html")
    def test_除外馬は着順がNone(self, mock_fetch: MagicMock) -> None:
        """除外・失格・競走中止馬は rank=None で返される。"""
        horses_normal = [
            (1, "勝ち馬", 2.5),
            (2, "2着馬", 3.0),
            (3, "3着馬", 5.0),
            (4, "4着馬", 8.0),
        ]
        base_html = _make_race_html(horses=horses_normal)
        # 除外馬行を HorseList として追加
        scratch_row = """
  <tr class="HorseList">
    <td>除外</td><td>1</td><td>5</td>
    <td><a href="/horse/2020000005/">除外馬</a></td>
    <td>牡3</td><td>57</td><td>テスト騎手</td><td>テスト調教師</td>
    <td></td><td></td>
    <td></td><td></td><td></td><td></td><td></td><td></td><td></td>
    <td></td><td></td><td></td><td></td><td></td>
    <td></td><td></td><td></td><td></td>
  </tr>
</table>"""
        scratch_html = base_html.replace("</table>", scratch_row)
        mock_fetch.return_value = scratch_html

        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        scratch = next((h for h in race.results if h.horse_name == "除外馬"), None)
        assert scratch is not None, "除外馬がパース結果に含まれていない"
        assert scratch.rank is None, f"除外馬の rank は None であるべき: {scratch.rank}"

    @patch("src.scraper.netkeiba._fetch_html")
    def test_出走頭数と結果件数が一致(self, mock_fetch: MagicMock) -> None:
        """HTML に含まれる HorseList 行数と results の件数が一致する。"""
        horses = [(i + 1, f"馬{i + 1}", 2.0 + i) for i in range(12)]
        mock_fetch.return_value = _make_race_html(horses=horses)
        race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)

        assert len(race.results) == 12, f"12頭のはずが {len(race.results)}頭"


# ---------------------------------------------------------------------------
# バリデーション関数（data_sync の防護壁ロジック）単体検証
# ---------------------------------------------------------------------------
class TestValidationLogic:
    """
    src/ops/data_sync.py に組み込んだバリデーション条件を
    RaceInfo オブジェクトに対して直接検証する。
    """

    def _make_race_info(
        self,
        distance: int = 2000,
        n_horses: int = 8,
        has_winner: bool = True,
    ) -> RaceInfo:
        results: list[HorseResult] = []
        for i in range(n_horses):
            if has_winner:
                rank = i + 1
            else:
                rank = i + 2  # 全馬 2着以下（1着不在）
            results.append(
                HorseResult(
                    rank=rank,
                    horse_name=f"テスト馬{i + 1}",
                    horse_id=f"202000000{i + 1}",
                    gate_number=(i % 8) + 1,
                    horse_number=i + 1,
                    sex_age="牡3",
                    weight_carried=57.0,
                    jockey="テスト騎手",
                    trainer="テスト調教師",
                    finish_time="2:00.0",
                    margin=None,
                    popularity=i + 1,
                    win_odds=2.5 + i,
                    horse_weight=480,
                    horse_weight_diff=0,
                )
            )
        return RaceInfo(
            race_id="202304050601",
            race_name="テストレース",
            date="2023-04-05",
            venue="阪神",
            race_number=6,
            distance=distance,
            surface="芝",
            track_direction="右",
            weather="晴",
            condition="良",
            results=results,
        )

    def _is_valid(self, race: RaceInfo) -> tuple[bool, str]:
        """data_sync.py のバリデーション条件を再現する。"""
        if len(race.results) < 4:
            return False, f"頭数不足: {len(race.results)}頭"
        if not any(h.rank == 1 for h in race.results):
            return False, "1着馬なし"
        if race.distance == 0:
            return False, "距離0m"
        return True, "OK"

    def test_正常なレースはバリデーション通過(self) -> None:
        race = self._make_race_info(distance=2000, n_horses=8, has_winner=True)
        ok, reason = self._is_valid(race)
        assert ok, f"バリデーション失敗（想定外）: {reason}"

    def test_距離0mはバリデーション失敗(self) -> None:
        race = self._make_race_info(distance=0, n_horses=8, has_winner=True)
        ok, reason = self._is_valid(race)
        assert not ok, "距離0mがバリデーションを通過してしまった"
        assert "距離" in reason

    def test_頭数3頭はバリデーション失敗(self) -> None:
        race = self._make_race_info(distance=2000, n_horses=3, has_winner=True)
        ok, reason = self._is_valid(race)
        assert not ok, "3頭レースがバリデーションを通過してしまった"
        assert "頭数" in reason

    def test_頭数4頭はバリデーション通過(self) -> None:
        """ちょうど 4 頭はボーダーライン上で通過すること。"""
        race = self._make_race_info(distance=2000, n_horses=4, has_winner=True)
        ok, _ = self._is_valid(race)
        assert ok, "4頭レースはバリデーションを通過するべき"

    def test_1着馬なしはバリデーション失敗(self) -> None:
        race = self._make_race_info(distance=2000, n_horses=8, has_winner=False)
        ok, reason = self._is_valid(race)
        assert not ok, "1着馬なしがバリデーションを通過してしまった"
        assert "1着" in reason

    def test_空レースはバリデーション失敗(self) -> None:
        race = self._make_race_info(distance=1600, n_horses=0, has_winner=False)
        ok, reason = self._is_valid(race)
        assert not ok
        assert "頭数" in reason

    def test_距離1mはバリデーション失敗しない_距離は0のみ禁止(self) -> None:
        """1m でも距離 != 0 なら通過（距離妥当性チェックは別途）。"""
        race = self._make_race_info(distance=1, n_horses=8, has_winner=True)
        ok, _ = self._is_valid(race)
        assert ok

    def test_1着馬がrank1であることを確認(self) -> None:
        """has_winner=True のとき、必ず rank==1 の馬が存在する。"""
        race = self._make_race_info(n_horses=8, has_winner=True)
        assert any(h.rank == 1 for h in race.results)

    def test_1着馬不在のときrank1は存在しない(self) -> None:
        """has_winner=False のとき、rank==1 の馬は存在しない。"""
        race = self._make_race_info(n_horses=8, has_winner=False)
        assert not any(h.rank == 1 for h in race.results)


# ---------------------------------------------------------------------------
# race_id バリデーション検証
# ---------------------------------------------------------------------------
class TestRaceIdValidation:
    """
    fetch_race_results の race_id 入力バリデーションを検証する。

    現行バリデーション仕様:
      - 空文字 → ValueError
      - 非数字文字を含む → ValueError
      - 全数字の任意長 → ValueError は発生しない（桁数チェックなし）
    """

    def test_空文字で例外(self) -> None:
        with pytest.raises(ValueError):
            fetch_race_results("")

    def test_不正文字を含むidで例外(self) -> None:
        with pytest.raises(ValueError):
            fetch_race_results("XXXXXXXXXXXXXXXX")

    def test_ハイフン含むidで例外(self) -> None:
        with pytest.raises(ValueError):
            fetch_race_results("2023-04-05-06-01")

    def test_正常な12桁idは例外なし(self) -> None:
        """12桁の数字であれば ValueError は送出されない（HTTP は別途モック）。"""
        with patch("src.scraper.netkeiba._fetch_html") as m:
            m.return_value = _make_race_html()
            race = fetch_race_results("202304050601", fetch_pedigree=False, delay=0)
        assert race.race_id == "202304050601"

    def test_全数字ならidの長さ問わずValueError不送出(self) -> None:
        """現行仕様: 桁数チェックはなく、全数字であれば ValueError は発生しない。"""
        with patch("src.scraper.netkeiba._fetch_html") as m:
            m.return_value = _make_race_html()
            # 8桁でも全数字なら例外なし（バリデーション改善余地として記録）
            race = fetch_race_results("20230405", fetch_pedigree=False, delay=0)
        assert race.race_id == "20230405"
