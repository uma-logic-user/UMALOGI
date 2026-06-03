"""src/nar/data_fetcher.py — 地方競馬（NAR）データ取得基盤。

地方競馬の出馬表・オッズ・レース結果を取得するための抽象インターフェースと、
2 つの実装を提供する。

  - ``DummyNarFetcher``    : 決定的なダミー NAR データ（開発・テスト・E2E 雛形用）。
  - ``NetkeibaNarFetcher`` : netkeiba 地方競馬ページ（nar.netkeiba.com）を一次ソースと
                             する取得器。出馬表ページから ``NarRaceMeta`` /
                             ``NarHorseEntry`` / 単勝オッズを **ライブ取得**する
                             （`requests` + `BeautifulSoup`・EUC-JP 確定・マナー実装済み）。
                             確定結果（fetch_results）は次フェーズ。

⚠️ 本モジュールは中央競馬（JRA）本番パイプラインから完全に隔離されており、
   既存 DB・実弾投票へ副作用を持たない。地方競馬の会場コード・ナイター発走時刻
   など NAR 固有のドメイン差分をここに集約する。

データ戦略（CLAUDE.md 条項11 の NAR 版・将来方針）:
    出馬表/オッズ/結果: nar.netkeiba.com を一次ソースとする。
    （JVLink は地方競馬を提供しないため、NAR では netkeiba が一次となる点が JRA と異なる）
"""

from __future__ import annotations

import logging
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# 実通信時の HTTP ヘッダー・タイムアウト・リクエスト間ウェイト（マナー）。
_HTTP_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}
_HTTP_TIMEOUT: float = 10.0
_REQUEST_DELAY_SEC: float = 1.0

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


# ── netkeiba 出馬表 HTML パーサ（純関数・テスト可能） ─────────────────────────

# 性齢（牡牝セ騸 + 数字）判定パターン。
_SEX_AGE_RE = re.compile(r"[牡牝セせ騸騙セン]\d+")
# 地域接頭辞（NAR 調教師セルの「北海道 沼澤英知」等）。
_REGION_PREFIX_RE = re.compile(
    r"^(北海道|岩手|浦和|船橋|大井|川崎|金沢|笠松|愛知|"
    r"名古屋|兵庫|園田|高知|佐賀|南関東|地方)\s+"
)


def _to_float(text: str | None) -> float | None:
    """数値文字列を float に変換する（"---.-" 等の非数値は None）。"""
    if not text:
        return None
    m = re.search(r"\d+(?:\.\d+)?", text)
    return float(m.group()) if m else None


def _to_int(text: str | None) -> int | None:
    """数値文字列を int に変換する（非数値は None）。"""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group()) if m else None


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "html.parser")


def parse_shutuba_meta(html: str, race_id: str) -> NarRaceMeta:
    """出馬表 HTML からレース基本情報をパースする（欠損は安全に既定値補完）。

    Args:
        html:    出馬表ページの HTML。
        race_id: 対象 race_id（会場・レース番号の補完に使用）。

    Returns:
        NarRaceMeta。要素が見つからない場合も race_id ベースの既定値で返す。
    """
    venue = NAR_VENUES.get(venue_code(race_id), "大井")
    race_no = _to_int(race_id[-2:]) or 1
    distance = 0
    surface = "ダート"
    post_time = ""
    date = ""

    try:
        soup = _soup(html)
        data01 = soup.select_one(".RaceData01")
        text = data01.get_text(" ", strip=True) if data01 else ""
        if text:
            mt = re.search(r"(\d{1,2}:\d{2})", text)
            if mt:
                post_time = mt.group(1)
            md = re.search(r"(\d{3,4})m", text)
            if md:
                distance = int(md.group(1))
            if "芝" in text:
                surface = "芝"
            elif "障" in text:
                surface = "障害"
            elif "ダ" in text:
                surface = "ダート"
        # 開催日はタイトルの "YYYY年M月D日" から復元する。
        title = soup.title.get_text(strip=True) if soup.title else ""
        dm = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
        if dm:
            date = (
                f"{int(dm.group(1)):04d}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            )
    except Exception as exc:  # noqa: BLE001 — パース失敗でもクラッシュさせない
        logger.warning("NAR race_meta パース失敗 %s: %s", race_id, exc)

    return NarRaceMeta(
        race_id=race_id,
        date=date,
        venue=venue,
        race_number=race_no,
        distance=distance,
        surface=surface,
        post_time=post_time,
    )


def _clean_trainer(text: str) -> str:
    """調教師セルから地域接頭辞を除去して名前のみ返す。"""
    name = _REGION_PREFIX_RE.sub("", text.strip())
    # 「北海道 沼澤英知」のようにスペース区切りで地域+名前なら名前側を採用。
    if " " in name or "　" in name:
        parts = re.split(r"[\s　]+", name)
        name = parts[-1] if parts else name
    return name


def parse_shutuba_entries(html: str) -> list[NarHorseEntry]:
    """出馬表 HTML から出走馬一覧（NarHorseEntry）を抽出する。

    DOM 要素が欠損した行は安全にスキップし、システムをクラッシュさせない。

    Args:
        html: 出馬表ページの HTML。

    Returns:
        馬番昇順の NarHorseEntry リスト。行が無ければ空リスト。
    """
    entries: list[NarHorseEntry] = []
    try:
        soup = _soup(html)
        rows = soup.select("tr.HorseList") or soup.select(".HorseList")
        for row in rows:
            try:
                num_td = row.select_one('td[class^="Umaban"]')
                horse_number = _to_int(num_td.get_text(strip=True) if num_td else None)
                if horse_number is None:
                    continue

                info = row.select_one(".HorseInfo")
                name_a = info.select_one("a") if info else None
                horse_name = (
                    name_a.get_text(strip=True)
                    if name_a
                    else (info.get_text(strip=True) if info else "")
                )

                # 性齢: HorseInfo の次 td（class 無し）。無ければ行テキストから正規表現抽出。
                sex_age = ""
                if info is not None:
                    sib = info.find_next_sibling("td")
                    if sib is not None:
                        cand = sib.get_text(strip=True)
                        if _SEX_AGE_RE.fullmatch(cand) or _SEX_AGE_RE.match(cand):
                            sex_age = cand
                if not sex_age:
                    msa = _SEX_AGE_RE.search(row.get_text(" ", strip=True))
                    sex_age = msa.group() if msa else ""

                jockey_td = row.select_one(".Jockey")
                jockey = jockey_td.get_text(strip=True) if jockey_td else ""
                trainer_td = row.select_one(".Trainer")
                trainer = (
                    _clean_trainer(trainer_td.get_text(" ", strip=True))
                    if trainer_td
                    else ""
                )

                odds_td = row.select_one("td.Popular.Txt_R")
                win_odds = _to_float(odds_td.get_text(strip=True) if odds_td else None)
                pop_td = row.select_one("td.Popular.Txt_C")
                popularity = _to_int(pop_td.get_text(strip=True) if pop_td else None)

                entries.append(
                    NarHorseEntry(
                        horse_number=horse_number,
                        horse_name=horse_name,
                        sex_age=sex_age,
                        jockey=jockey,
                        trainer=trainer,
                        win_odds=win_odds,
                        popularity=popularity,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — 1 行の異常で全体を止めない
                logger.warning("NAR 出走馬行のパース失敗（スキップ）: %s", exc)
                continue
    except Exception as exc:  # noqa: BLE001
        logger.warning("NAR 出馬表パース失敗: %s", exc)
        return []
    entries.sort(key=lambda e: e.horse_number)
    return entries


def parse_shutuba_odds(html: str) -> dict[int, float]:
    """出馬表 HTML から馬番→単勝オッズ辞書を抽出する。

    netkeiba の単独オッズページは JS 描画のため、確実に取得できる出馬表ページ
    埋め込みオッズ（直前確定値）を一次とする。

    Args:
        html: 出馬表ページの HTML。

    Returns:
        {馬番: 単勝オッズ}。オッズ未確定の馬は含めない。
    """
    return {
        e.horse_number: e.win_odds
        for e in parse_shutuba_entries(html)
        if e.win_odds is not None
    }


# ── netkeiba 地方競馬 実装（ライブパーサ） ───────────────────────────────────


def _resolve_encoding(resp: object) -> str:
    """netkeiba レスポンスのエンコーディングを確定する（既定 EUC-JP）。

    CLAUDE.md §16 準拠: Content-Type charset 優先、apparent_encoding が
    mac/greek 等に誤検知した場合は netkeiba 既定の euc-jp にフォールバックする。
    """
    headers = getattr(resp, "headers", {}) or {}
    ct = str(headers.get("Content-Type", "")).lower()
    if "utf-8" in ct or "utf8" in ct:
        return "utf-8"
    if "euc" in ct:
        return "euc-jp"
    if "shift" in ct or "sjis" in ct:
        return "cp932"
    apparent = str(getattr(resp, "apparent_encoding", "") or "").lower()
    if "utf" in apparent:
        return "utf-8"
    if "euc" in apparent:
        return "euc-jp"
    # mac-greek / iso-8859 等の誤検知を含め、netkeiba 既定の euc-jp に倒す。
    return "euc-jp"


class NetkeibaNarFetcher(NarDataFetcher):
    """nar.netkeiba.com を一次ソースとする地方競馬データ取得器（ライブ実装）。

    出馬表ページ（/race/shutuba.html）から ``NarRaceMeta`` / ``NarHorseEntry`` /
    単勝オッズを取得する。単独オッズページは JS 描画のため、オッズも出馬表ページ
    埋め込み値から取得する。

    堅牢性:
      - HTTP timeout=10s、リクエスト間に 1.0s 以上のウェイト（対象サイトへのマナー）。
      - 通信失敗・DOM 欠損時は例外で停止せず、WARNING ログを出して空/既定値を返す。

    Args:
        http_get: HTML 取得関数の注入口（テスト時にモック HTML を返す）。
                  None の場合は requests による実通信を行う。
        delay:    連続リクエスト間の最小ウェイト秒（既定 1.0s）。
        timeout:  HTTP タイムアウト秒（既定 10s）。
    """

    _BASE = "https://nar.netkeiba.com"

    def __init__(
        self,
        http_get: Callable[[str], str] | None = None,
        *,
        delay: float = _REQUEST_DELAY_SEC,
        timeout: float = _HTTP_TIMEOUT,
    ) -> None:
        self._http_get = http_get
        self._delay = delay
        self._timeout = timeout

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

    # ── HTML 取得（マナー: ウェイト + timeout + エンコード確定） ──────────
    def _get(self, url: str) -> str:
        """URL の HTML を取得する。http_get 注入時はそれを使う（テスト用・ウェイト無し）。"""
        if self._http_get is not None:
            return self._http_get(url)
        import requests  # 実通信時のみ依存（テストはモック注入で回避）

        time.sleep(max(self._delay, 0.0))  # 対象サイトへの負荷軽減
        resp = requests.get(url, timeout=self._timeout, headers=_HTTP_HEADERS)
        resp.raise_for_status()
        resp.encoding = _resolve_encoding(resp)
        return resp.text

    # ── ライブ取得（失敗時も停止せず空/既定値を返す） ──────────────────
    def fetch_race_meta(self, race_id: str) -> NarRaceMeta:
        try:
            html = self._get(self.build_entry_url(race_id))
        except Exception as exc:  # noqa: BLE001
            logger.warning("NAR race_meta 取得失敗 %s: %s", race_id, exc)
            html = ""  # race_id ベースの既定値で補完
        return parse_shutuba_meta(html, race_id)

    def fetch_entries(self, race_id: str) -> list[NarHorseEntry]:
        try:
            html = self._get(self.build_entry_url(race_id))
        except Exception as exc:  # noqa: BLE001
            logger.warning("NAR 出馬表取得失敗 %s: %s", race_id, exc)
            return []
        return parse_shutuba_entries(html)

    def fetch_odds(self, race_id: str) -> dict[int, float]:
        try:
            html = self._get(self.build_entry_url(race_id))
        except Exception as exc:  # noqa: BLE001
            logger.warning("NAR オッズ取得失敗 %s: %s", race_id, exc)
            return {}
        return parse_shutuba_odds(html)

    def fetch_results(self, race_id: str) -> NarRaceResult:
        # 結果ページ（/race/result.html）は別 DOM 構造。基盤段階では未対応を明示し、
        # 確定結果の取り込みは次フェーズ（docs/5_nar_integration_spec.md §10.5）で実装する。
        raise NotImplementedError(
            "NetkeibaNarFetcher.fetch_results は次フェーズで実装予定です。"
            "現段階の結果系検証には DummyNarFetcher を使用してください。"
        )
