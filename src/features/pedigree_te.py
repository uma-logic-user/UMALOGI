"""血統 Target Encoding（W-070続き / タスク1・プラン2）。

サイアー（父）・母父（dam_sire）ごとの「複勝率（rank<=3）」を、**cutoff 日より前の
結果のみ**で学習（リークフリー）し、ベイズ・スムージング付きで数値化する。未知/欠損
サイアーは全体平均にフォールバックする（死に特徴量・NaN 混入を防ぐ）。

⚠️ リーク対策:
  - TE は必ず予測対象レースの日付より前の結果だけで fit する（時系列分割）。
  - 同一馬の今回結果は学習に含めない（cutoff で自然に除外）。
  - スムージング: te = (n*mean_sire + m*global) / (n + m)。少数産駒は全体平均へ収縮。

⚠️ 非破壊: 稼働中モデルの FEATURE_COLS は変更しない。再学習で明示採用するまで並行計算。
"""

from __future__ import annotations

import sqlite3

import pandas as pd

PEDIGREE_FEATURE_COLS: list[str] = [
    "sire_place_te",  # 父の複勝率TE（リークフリー・スムージング済）
    "dam_sire_place_te",  # 母父の複勝率TE
]

_DEFAULT_SMOOTHING: float = 20.0


class _ColumnEncoder:
    """単一血統列（sire または dam_sire）の複勝率 Target Encoder。"""

    def __init__(self, column: str, smoothing: float) -> None:
        self.column = column
        self.smoothing = smoothing
        self.global_mean: float = 0.0
        self._map: dict[str, float] = {}

    def fit(
        self, conn: sqlite3.Connection, cutoff_date: str, surface: str | None
    ) -> None:
        """cutoff_date より前・指定 surface の結果のみで複勝率を学習する。"""
        surf_clause = "AND r.surface = ?" if surface else ""
        params: list[object] = [cutoff_date]
        if surface:
            params.append(surface)
        rows = conn.execute(
            f"""
            SELECT h.{self.column} AS sire,
                   AVG(CASE WHEN rr.rank <= 3 THEN 1.0 ELSE 0.0 END) AS place_rate,
                   COUNT(*) AS n
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            JOIN horses h ON rr.horse_id = h.horse_id
            WHERE r.date < ? {surf_clause}
              AND rr.rank IS NOT NULL AND rr.rank > 0
              AND h.{self.column} IS NOT NULL AND h.{self.column} != ''
            GROUP BY h.{self.column}
            """,
            params,
        ).fetchall()

        # 全体平均（cutoff前・surface限定の複勝基準率）
        gm = conn.execute(
            f"""
            SELECT AVG(CASE WHEN rr.rank <= 3 THEN 1.0 ELSE 0.0 END)
            FROM race_results rr JOIN races r ON rr.race_id = r.race_id
            WHERE r.date < ? {surf_clause}
              AND rr.rank IS NOT NULL AND rr.rank > 0
            """,
            params,
        ).fetchone()
        self.global_mean = float(gm[0]) if gm and gm[0] is not None else 0.0

        m = self.smoothing
        self._map = {}
        for sire, place_rate, n in rows:
            if sire is None:
                continue
            pr = float(place_rate) if place_rate is not None else self.global_mean
            cnt = float(n)
            # ベイズ・スムージング: 少数産駒は global_mean へ収縮
            te = (
                (cnt * pr + m * self.global_mean) / (cnt + m)
                if (cnt + m) > 0
                else self.global_mean
            )
            self._map[str(sire)] = te

    def encode(self, sire: str | None) -> float:
        """サイアー名を TE 値へ変換。未知・欠損は global_mean にフォールバック。"""
        if not sire:
            return self.global_mean
        return self._map.get(str(sire), self.global_mean)


class SireEncoder:
    """父・母父の複勝率 Target Encoder（リークフリー・スムージング付き）。"""

    def __init__(self, smoothing: float = _DEFAULT_SMOOTHING) -> None:
        self.smoothing = smoothing
        self._sire = _ColumnEncoder("sire", smoothing)
        self._dam_sire = _ColumnEncoder("dam_sire", smoothing)

    @property
    def global_mean(self) -> float:
        return self._sire.global_mean

    def fit(
        self, conn: sqlite3.Connection, cutoff_date: str, surface: str | None = None
    ) -> "SireEncoder":
        self._sire.fit(conn, cutoff_date, surface)
        self._dam_sire.fit(conn, cutoff_date, surface)
        return self

    def encode(self, sire: str | None) -> float:
        """父サイアーの TE 値（テスト互換の簡易アクセサ）。"""
        return self._sire.encode(sire)

    def encode_dam_sire(self, dam_sire: str | None) -> float:
        return self._dam_sire.encode(dam_sire)


def build_pedigree_features(
    conn: sqlite3.Connection, race_id: str, encoder: SireEncoder
) -> pd.DataFrame:
    """1レース分の血統TE特徴量（sire_place_te / dam_sire_place_te）を返す。

    encoder は対象レース日より前で fit 済みであることを前提とする（リークフリー）。
    horses マスタに血統が無い馬は global_mean で埋める（NaN を出さない）。

    Returns:
        columns = ["horse_number", *PEDIGREE_FEATURE_COLS] の DataFrame。
    """
    cols = ["horse_number", *PEDIGREE_FEATURE_COLS]
    runners = conn.execute(
        """
        SELECT rr.horse_number, h.sire, h.dam_sire
        FROM race_results rr
        LEFT JOIN horses h ON rr.horse_id = h.horse_id
        WHERE rr.race_id = ? AND rr.horse_number IS NOT NULL
        ORDER BY rr.horse_number
        """,
        (race_id,),
    ).fetchall()
    if not runners:
        return pd.DataFrame(columns=cols)

    records = [
        {
            "horse_number": hn,
            "sire_place_te": encoder.encode(sire),
            "dam_sire_place_te": encoder.encode_dam_sire(dam_sire),
        }
        for hn, sire, dam_sire in runners
    ]
    return pd.DataFrame(records, columns=cols)
