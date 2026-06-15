"""
src/features/research_assemblers.py — ゲート検証用の研究アセンブラ・レジストリ（W-098）

validate_feature.py / feature_gate が使う `assemble_fn(conn, ids, enc)` の差し替え可能な
実装群。標準の `_assemble`（6列ベース＋前走系＋血統TE）に研究特徴量を付与する。

inner_bias 版: x_trouble_inner の三項クロス（前走不利 × 内枠 × 内枠複勝バイアスz）を付与。
  x_trouble_inner_today = prev_trouble_proxy × inner_draw(枠1-3) × today_inner_bias
  x_trouble_inner_yest  = prev_trouble_proxy × inner_draw(枠1-3) × yesterday_inner_bias

リーク防止: 内枠バイアスの基準(μ,σ)は全検証cutoffより前の固定参照 `_BASELINE_REF_HI` から算出
（today/yesterday の率は本質的に既走・前日のみ参照のためリークフリー）。
"""

from __future__ import annotations

import sqlite3
from typing import Any

import numpy as np
import pandas as pd

from scripts.backtest_v2_oos import _assemble
from src.features.inner_bias import DailyInnerIndex, build_daily_inner_index

# 索引の集計範囲と基準参照上限（検証 cutoff 2025-10 より前 → test 期間に対しリークフリー）。
_INDEX_LO = "2024-01-01"
_INDEX_HI = "2027-01-01"
_BASELINE_REF_HI = "2025-10-01"

_index_cache: DailyInnerIndex | None = None


def _get_index(conn: sqlite3.Connection) -> DailyInnerIndex:
    global _index_cache
    if _index_cache is None:
        _index_cache = build_daily_inner_index(
            conn, _INDEX_LO, _INDEX_HI, reference_hi=_BASELINE_REF_HI
        )
    return _index_cache


def assemble_with_inner_bias(
    conn: sqlite3.Connection, ids: list[str], enc: Any
) -> pd.DataFrame:
    """_assemble に inner_bias 三項クロスを付与して返す。"""
    df = _assemble(conn, ids, enc)
    if df.empty:
        return df
    idx = _get_index(conn)

    # race_id -> (date, race_number)
    meta: dict[str, tuple[str, int]] = {}
    if ids:
        ph = ",".join("?" * len(ids))
        for rid, d, rn in conn.execute(
            f"SELECT race_id, date, race_number FROM races WHERE race_id IN ({ph})",
            ids,
        ).fetchall():
            meta[str(rid)] = (str(d), int(rn) if rn is not None else 0)

    rid_col = df["race_id"].astype(str)
    today = np.array([idx.today_bias_z(*meta.get(r, ("", 0))) for r in rid_col])
    yest = np.array([idx.yesterday_bias_z(meta.get(r, ("", 0))[0]) for r in rid_col])

    trouble = pd.to_numeric(df.get("prev_trouble_proxy"), errors="coerce").fillna(0.0)
    gate = pd.to_numeric(df.get("gate_number"), errors="coerce")
    inner = (gate <= 3).astype(float).fillna(0.0).to_numpy()

    df["today_inner_bias"] = today
    df["yesterday_inner_bias"] = yest
    df["x_trouble_inner_today"] = trouble.to_numpy() * inner * today
    df["x_trouble_inner_yest"] = trouble.to_numpy() * inner * yest
    return df


# validate_feature.py / 研究スクリプトが参照するレジストリ。
RESEARCH_ASSEMBLERS: dict[str, Any] = {
    "default": _assemble,
    "inner_bias": assemble_with_inner_bias,
}


def reset_index_cache() -> None:
    """テスト用: 索引キャッシュをクリアする。"""
    global _index_cache
    _index_cache = None
