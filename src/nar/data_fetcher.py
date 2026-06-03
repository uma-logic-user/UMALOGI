"""src/nar/data_fetcher.py — 地方競馬（NAR）データ取得基盤（プロトタイプ）。

地方競馬の出馬表・オッズ・レース結果を取得するための抽象インターフェースと、
2 つの実装を提供する。

  - ``DummyNarFetcher``    : 決定的なダミー NAR データ（開発・テスト・E2E 雛形用）。
  - ``NetkeibaNarFetcher`` : netkeiba 地方競馬ページ（nar.netkeiba.com）を一次ソースと
                             する取得器。URL 契約のみ確定済みで、ライブ HTML パースは
                             未実装の明示スタブ（誠実なプロトタイプ境界）。

⚠️ 本モジュールは中央競馬（JRA）本番パイプラインから完全に隔離されており、
   既存 DB・実弾投票へ副作用を持たない。地方競馬の会場コード・ナイター発走時刻
   など NAR 固有のドメイン差分をここに集約する。

データ戦略（CLAUDE.md 条項11 の NAR 版・将来方針）:
    出馬表/オッズ/結果: nar.netkeiba.com を一次ソースとする。
    （JVLink は地方競馬を提供しないため、NAR では netkeiba が一次となる点が JRA と異なる）
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

# ── NAR 会場コード（netkeiba 地方競馬場の 2 桁プレースコード） ─────────────────
# race_id の 5〜6 文字目（0-indexed [4:6]）が会場コードに対応する。
NAR_VENUES: dict[str, str] = {
    "30": "門別",
    "35": "盛岡",
    "36": "水沢",
    "42": "浦和",
    "43": "船橋",
    "44": "大井",
    "45": "川崎",
    "46": "金沢",
    "47": "笠松",
    "48": "名古屋",
    "50": "園田",
    "51": "姫路",
    "54": "高知",
    "55": "佐賀",
    "65": "帯広",
}

# JRA（中央）会場コード。NAR 判定の除外集合として保持する。
_JRA_VENUES: frozenset[str] = frozenset(
    {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
)


def venue_code(race_id: str) -> str:
    """race_id から会場コード（2 桁）を抽出する。

    Args:
        race_id: 12 桁の race_id（YYYY + 会場2桁 + 回次2桁 + 日2桁 + R2桁）。

    Returns:
        会場コード文字列。race_id が短い場合は空文字。
    """
    return race_id[4:6] if len(race_id) >= 6 else ""


def is_nar_race_id(race_id: str) -> bool:
    """race_id が地方競馬（NAR）のものかを会場コードで判定する。

    Args:
        race_id: 判定対象の race_id。

    Returns:
        NAR 会場コードを含む場合 True、JRA 等それ以外は False。
    """
    return venue_code(race_id) in NAR_VENUES


# ── NAR ドメインデータ構造 ─────────────────────────────────────────────────


@dataclass(frozen=True)
class NarRaceMeta:
    """地方競馬 1 レースの基本情報。"""

    race_id: str
    date: str  # YYYY-MM-DD
    venue: str  # 例: "大井"
    race_number: int
    distance: int  # m
    surface: str  # 地方競馬はほぼ "ダート"
    post_time: str  # ナイター対応の発走時刻 "HH:MM"


@dataclass(frozen=True)
class NarHorseEntry:
    """地方競馬の出走馬 1 頭ぶんのエントリー情報。"""

    horse_number: int
    horse_name: str
    sex_age: str  # 例: "牡4"
    jockey: str
    trainer: str
    win_odds: float | None = None
    popularity: int | None = None


@dataclass(frozen=True)
class NarRaceResult:
    """地方競馬 1 レースの確定結果。"""

    race_id: str
    ranking: list[int]  # 着順順の馬番（[0] が 1 着）
    payouts: dict[str, int] = field(default_factory=dict)  # 券種 -> 払戻(円)


# ── 抽象インターフェース ───────────────────────────────────────────────────


class NarDataFetcher(ABC):
    """地方競馬データ取得器の抽象基底。

    取得ソース（netkeiba / ダミー / 将来の他ソース）を差し替え可能にする。
    """

    @abstractmethod
    def fetch_race_meta(self, race_id: str) -> NarRaceMeta:
        """レース基本情報を取得する。"""

    @abstractmethod
    def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
        """出走馬一覧を馬番昇順で取得する。"""

    @abstractmethod
    def fetch_odds(self, race_id: str) -> dict[int, float]:
        """馬番 → 単勝オッズの辞書を取得する。"""

    @abstractmethod
    def fetch_results(self, race_id: str) -> NarRaceResult:
        """確定結果を取得する。"""


# ── ダミー実装（決定的） ───────────────────────────────────────────────────

_DUMMY_HORSE_NAMES: tuple[str, ...] = (
    "ハクサンリュウ",
    "トウカイファイン",
    "ナンゴクテイオー",
    "ベイサイドキング",
    "リンドウブルーム",
    "サザンクロスター",
    "ヤマトダイヤモンド",
    "コスモアルタイル",
    "ミナミノフウジン",
    "ゴールデンアロー",
    "シルクロードボス",
    "アオイレヴァンテ",
    "テンリュウマサムネ",
    "カガヤキノオト",
    "ホクトショウグン",
    "エトワールノクチュルヌ",
)
_DUMMY_JOCKEYS: tuple[str, ...] = (
    "的場文男",
    "森泰斗",
    "笹川翼",
    "御神本訓史",
    "矢野貴之",
    "本田正重",
)
_DUMMY_TRAINERS: tuple[str, ...] = (
    "荒山勝徳",
    "森下淳平",
    "佐藤賢二",
    "渡邉和雄",
    "藤原智行",
)
_DUMMY_SEX: tuple[str, ...] = ("牡", "牝", "セ")
_DUMMY_DISTANCES: tuple[int, ...] = (1200, 1400, 1600, 1800, 2000)
# 地方競馬はナイター開催が多い（夕方〜夜）。
_DUMMY_POST_TIMES: tuple[str, ...] = (
    "15:10",
    "16:00",
    "17:25",
    "18:40",
    "19:55",
    "20:50",
)


class DummyNarFetcher(NarDataFetcher):
    """race_id から決定的にダミー NAR データを生成する取得器。

    ネットワーク不要・再現可能。開発・テスト・E2E パイプライン雛形に用いる。
    実データ取得は ``NetkeibaNarFetcher`` のライブ実装完了後に差し替える。
    """

    def _rng(self, race_id: str, salt: str = "") -> random.Random:
        """race_id（+salt）に対し決定的な乱数生成器を返す。"""
        return random.Random(f"{race_id}:{salt}")

    def _entry_count(self, race_id: str) -> int:
        """race_id から決定的に出走頭数（8〜12 頭）を導く。"""
        tail = race_id[-2:] if len(race_id) >= 2 else "0"
        digits = int("".join(ch for ch in tail if ch.isdigit()) or "0")
        return 8 + (digits % 5)

    def fetch_race_meta(self, race_id: str) -> NarRaceMeta:
        rng = self._rng(race_id, "meta")
        venue = NAR_VENUES.get(venue_code(race_id), "大井")
        race_no = int(race_id[-2:]) if race_id[-2:].isdigit() else 1
        race_no = max(1, race_no)
        date = (
            f"{race_id[:4]}-01-01"
            if len(race_id) >= 4 and race_id[:4].isdigit()
            else "2026-01-01"
        )
        return NarRaceMeta(
            race_id=race_id,
            date=date,
            venue=venue,
            race_number=race_no,
            distance=rng.choice(_DUMMY_DISTANCES),
            surface="ダート",
            post_time=rng.choice(_DUMMY_POST_TIMES),
        )

    def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
        rng = self._rng(race_id, "entries")
        n = self._entry_count(race_id)
        names = list(_DUMMY_HORSE_NAMES)
        rng.shuffle(names)
        odds = self.fetch_odds(race_id)
        # 単勝オッズ昇順 → 人気順を付与
        pop_order = sorted(odds, key=lambda hn: odds[hn])
        popularity = {hn: i + 1 for i, hn in enumerate(pop_order)}
        entries: list[NarHorseEntry] = []
        for i in range(1, n + 1):
            entries.append(
                NarHorseEntry(
                    horse_number=i,
                    horse_name=names[(i - 1) % len(names)],
                    sex_age=f"{rng.choice(_DUMMY_SEX)}{rng.randint(2, 7)}",
                    jockey=rng.choice(_DUMMY_JOCKEYS),
                    trainer=rng.choice(_DUMMY_TRAINERS),
                    win_odds=odds[i],
                    popularity=popularity[i],
                )
            )
        return entries

    def fetch_odds(self, race_id: str) -> dict[int, float]:
        rng = self._rng(race_id, "odds")
        n = self._entry_count(race_id)
        return {i: round(rng.uniform(1.5, 50.0), 1) for i in range(1, n + 1)}

    def fetch_results(self, race_id: str) -> NarRaceResult:
        rng = self._rng(race_id, "results")
        n = self._entry_count(race_id)
        ranking = list(range(1, n + 1))
        rng.shuffle(ranking)
        win, second, third = ranking[0], ranking[1], ranking[2]
        payouts = {
            "単勝": rng.choice([180, 320, 540, 760, 1230]),
            "馬連": rng.choice([850, 1640, 3200, 5800]),
            "三連複": rng.choice([1200, 4300, 9800, 24500]),
        }
        # 払戻に着順情報の一貫性メモを残す（プロトタイプ用）
        _ = (win, second, third)
        return NarRaceResult(race_id=race_id, ranking=ranking, payouts=payouts)


# ── netkeiba 地方競馬 実装（URL 契約確定・ライブパースは未実装スタブ） ─────────


class NetkeibaNarFetcher(NarDataFetcher):
    """nar.netkeiba.com を一次ソースとする地方競馬データ取得器。

    URL 契約は確定済みでテスト固定されている。ライブ HTML パースは未実装で、
    検証できないダミー成功を返さないため明示的に NotImplementedError を送出する。
    ライブ実装時は ``DummyNarFetcher`` を差し替え対象とし、本クラスの
    parse 層（将来追加）に BeautifulSoup パーサを実装する。

    Args:
        http_get: HTML 取得関数の注入口（テスト/将来実装用）。
                  None の場合、ライブ取得メソッドは未実装スタブとして振る舞う。
    """

    _BASE = "https://nar.netkeiba.com"

    def __init__(self, http_get: Callable[[str], str] | None = None) -> None:
        self._http_get = http_get

    # ── URL ビルダー（純関数・契約確定） ──────────────────────────────
    @staticmethod
    def build_entry_url(race_id: str) -> str:
        """出馬表ページ URL を返す。"""
        return f"{NetkeibaNarFetcher._BASE}/race/shutuba.html?race_id={race_id}"

    @staticmethod
    def build_odds_url(race_id: str) -> str:
        """単勝オッズページ URL を返す。"""
        return f"{NetkeibaNarFetcher._BASE}/odds/index.html?type=b1&race_id={race_id}"

    @staticmethod
    def build_result_url(race_id: str) -> str:
        """レース結果ページ URL を返す。"""
        return f"{NetkeibaNarFetcher._BASE}/race/result.html?race_id={race_id}"

    # ── ライブ取得（プロトタイプ未実装スタブ） ────────────────────────
    def _not_implemented(self, what: str) -> NotImplementedError:
        return NotImplementedError(
            f"NetkeibaNarFetcher.{what} はプロトタイプ未実装です。"
            "ライブ取得は HTML パーサ実装後に有効化されます。"
            "現段階の検証・E2E には DummyNarFetcher を使用してください。"
        )

    def fetch_race_meta(self, race_id: str) -> NarRaceMeta:
        raise self._not_implemented("fetch_race_meta")

    def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
        raise self._not_implemented("fetch_entries")

    def fetch_odds(self, race_id: str) -> dict[int, float]:
        raise self._not_implemented("fetch_odds")

    def fetch_results(self, race_id: str) -> NarRaceResult:
        raise self._not_implemented("fetch_results")
