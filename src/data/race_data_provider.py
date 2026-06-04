"""src/data/race_data_provider.py — JRA/NAR 統合データプロバイダ。

中央競馬（JRA）と地方競馬（NAR）のデータ取得を `datasource` で切り替える上位層。
docs/5_nar_integration_spec.md §3 の Provider パターンの実体化であり、
取得結果をすべて `datasource`（'jra' | 'nar'）列付きの正規化 DataFrame で返すことで、
DB 保存・特徴量生成の段階で両データが混ざらないようガードする。

経路:
  - datasource='nar' → src/nar/data_fetcher.NetkeibaNarFetcher（ライブ取得）
  - datasource='jra' → 既存 DB（entries / realtime_odds / race_results / race_payouts）読み取り
                       （JVLink/netkeiba が取り込んだ正本を参照。JRA 既存コードは一切変更しない）

⚠️ 本モジュールは src/ops・src/ml 等の JRA 本番コードを変更しない（読み取り再利用のみ）。
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from src.nar.data_fetcher import (
    NarDataFetcher,
    NetkeibaNarFetcher,
    is_nar_race_id,
)

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DB = _ROOT / "data" / "umalogi.db"

#: 受理する datasource 値。
VALID_DATASOURCES: tuple[str, ...] = ("jra", "nar")

#: 正規化 DataFrame の列定義（datasource 横断で統一）。
ENTRY_COLUMNS: list[str] = [
    "race_id",
    "datasource",
    "horse_number",
    "horse_name",
    "sex_age",
    "jockey",
    "trainer",
    "win_odds",
    "popularity",
]
RESULT_COLUMNS: list[str] = [
    "race_id",
    "datasource",
    "rank",
    "horse_number",
    "horse_name",
]
PAYOUT_COLUMNS: list[str] = [
    "race_id",
    "datasource",
    "bet_type",
    "combination",
    "payout",
]


# ── 混在ガード ───────────────────────────────────────────────────────────────


def assert_single_datasource(df: pd.DataFrame, expected: str | None = None) -> None:
    """DataFrame が単一 datasource のみで構成されることを保証する。

    DB 保存・特徴量生成の直前に呼び、JRA と NAR のデータ混在を防ぐガード。

    Args:
        df:       検査対象（'datasource' 列を持つこと）。
        expected: 指定時は、その datasource と一致することも要求する。

    Raises:
        ValueError: 'datasource' 列が無い / 複数 datasource が混在 / expected と不一致。
    """
    if "datasource" not in df.columns:
        raise ValueError("DataFrame に 'datasource' 列がありません（混在ガード不能）")
    uniq = set(df["datasource"].dropna().unique())
    if len(uniq) > 1:
        raise ValueError(f"datasource が混在しています: {sorted(uniq)}")
    if expected is not None and uniq and uniq != {expected}:
        raise ValueError(f"datasource 不一致: 期待={expected!r} 実際={sorted(uniq)}")


# ── プロバイダ本体 ───────────────────────────────────────────────────────────


class RaceDataProvider:
    """JRA/NAR を `datasource` で切り替えるデータプロバイダ。

    Args:
        datasource:  'jra'（既定）または 'nar'。
        nar_fetcher: NAR 取得器の注入口（テスト/差し替え用）。既定は NetkeibaNarFetcher。
        db_path:     JRA 経路で参照する SQLite パス（既定 data/umalogi.db）。
    """

    def __init__(
        self,
        datasource: str = "jra",
        *,
        nar_fetcher: NarDataFetcher | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        if datasource not in VALID_DATASOURCES:
            raise ValueError(
                f"未知の datasource: {datasource!r}（{VALID_DATASOURCES} のいずれか）"
            )
        self.datasource = datasource
        self._nar: NarDataFetcher = nar_fetcher or NetkeibaNarFetcher()
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB

    # ── 公開 API ────────────────────────────────────────────────────────
    def get_entries(self, race_id: str) -> pd.DataFrame:
        """出馬表を datasource 列付き正規化 DataFrame で返す。"""
        if self.datasource == "nar":
            df = self._nar_entries(race_id)
        else:
            df = self._jra_entries(race_id)
        return self._finalize(df, ENTRY_COLUMNS)

    def get_results(self, race_id: str) -> pd.DataFrame:
        """確定着順を datasource 列付き正規化 DataFrame で返す。"""
        if self.datasource == "nar":
            df = self._nar_results(race_id)
        else:
            df = self._jra_results(race_id)
        return self._finalize(df, RESULT_COLUMNS)

    def get_payouts(self, race_id: str) -> pd.DataFrame:
        """確定払戻を datasource 列付き正規化 DataFrame で返す。"""
        if self.datasource == "nar":
            df = self._nar_payouts(race_id)
        else:
            df = self._jra_payouts(race_id)
        return self._finalize(df, PAYOUT_COLUMNS)

    # ── 仕上げ（datasource 付与 + 列整形 + ガード） ──────────────────────
    def _finalize(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """datasource 列を強制付与し、列順を整え、混在しないことを保証する。"""
        if df.empty:
            return pd.DataFrame(columns=columns)
        df = df.copy()
        df["datasource"] = self.datasource  # 経路に応じて一律タグ付け（上書き）
        df = df.reindex(columns=columns)
        assert_single_datasource(df, expected=self.datasource)
        return df.reset_index(drop=True)

    # ── NAR 経路（NetkeibaNarFetcher） ──────────────────────────────────
    def _nar_entries(self, race_id: str) -> pd.DataFrame:
        rows = [
            {
                "race_id": race_id,
                "horse_number": e.horse_number,
                "horse_name": e.horse_name,
                "sex_age": e.sex_age,
                "jockey": e.jockey,
                "trainer": e.trainer,
                "win_odds": e.win_odds,
                "popularity": e.popularity,
            }
            for e in self._nar.fetch_entries(race_id)
        ]
        return pd.DataFrame(rows)

    def _nar_results(self, race_id: str) -> pd.DataFrame:
        res = self._nar.fetch_results(race_id)
        rows = [
            {
                "race_id": race_id,
                "rank": r.rank,
                "horse_number": r.horse_number,
                "horse_name": r.horse_name,
            }
            for r in res.results
        ]
        return pd.DataFrame(rows)

    def _nar_payouts(self, race_id: str) -> pd.DataFrame:
        res = self._nar.fetch_results(race_id)
        rows = [
            {
                "race_id": race_id,
                "bet_type": p.bet_type,
                "combination": p.combination,
                "payout": p.amount,
            }
            for p in res.payouts
        ]
        return pd.DataFrame(rows)

    # ── JRA 経路（既存 DB 読み取り・JRA コード非改変） ──────────────────
    def _query(self, sql: str, params: tuple[Any, ...]) -> list[sqlite3.Row]:
        """JRA DB に対する読み取りクエリ（失敗時は WARNING + 空）。"""
        if not self._db_path.exists():
            logger.warning("JRA DB が見つかりません: %s", self._db_path)
            return []
        try:
            con = sqlite3.connect(str(self._db_path))
            con.row_factory = sqlite3.Row
            try:
                return con.execute(sql, params).fetchall()
            finally:
                con.close()
        except sqlite3.Error as exc:
            logger.warning("JRA DB 読み取り失敗 (%s): %s", self._db_path, exc)
            return []

    def _jra_entries(self, race_id: str) -> pd.DataFrame:
        rows = self._query(
            """
            SELECT e.horse_number, e.horse_name, e.sex_age, e.jockey, e.trainer,
                   o.win_odds, o.popularity
              FROM entries e
              LEFT JOIN realtime_odds o
                ON o.race_id = e.race_id AND o.horse_number = e.horse_number
             WHERE e.race_id = ?
             ORDER BY e.horse_number
            """,
            (race_id,),
        )
        return pd.DataFrame([dict(r) for r in rows])

    def _jra_results(self, race_id: str) -> pd.DataFrame:
        rows = self._query(
            """
            SELECT rank, horse_number, horse_name
              FROM race_results
             WHERE race_id = ? AND rank IS NOT NULL AND rank > 0
             ORDER BY rank
            """,
            (race_id,),
        )
        return pd.DataFrame([dict(r) for r in rows])

    def _jra_payouts(self, race_id: str) -> pd.DataFrame:
        rows = self._query(
            """
            SELECT bet_type, combination, payout
              FROM race_payouts
             WHERE race_id = ? AND bet_type != '返還'
            """,
            (race_id,),
        )
        return pd.DataFrame([dict(r) for r in rows])


def provider_for_race(
    race_id: str,
    *,
    nar_fetcher: NarDataFetcher | None = None,
    db_path: str | Path | None = None,
) -> RaceDataProvider:
    """race_id の会場コードから datasource を自動判定してプロバイダを生成する。

    Args:
        race_id:     対象 race_id。
        nar_fetcher: NAR 取得器の注入口。
        db_path:     JRA DB パス。

    Returns:
        NAR 会場なら datasource='nar'、それ以外は 'jra' の RaceDataProvider。
    """
    datasource = "nar" if is_nar_race_id(race_id) else "jra"
    return RaceDataProvider(
        datasource=datasource, nar_fetcher=nar_fetcher, db_path=db_path
    )
