"""再シミュレーション(v2) 骨子 — 新特徴量(PCI/加速力)を交えた学習データ生成の検証。

⚠️ 骨子（skeleton）。本番モデル学習(v1.2.0)には一切結線しない。
バックフィルされた `race_results.last_3f` を基に、各レースの加速力特徴量が
正しく計算され、FEATURE_COLS(69列・不変) に **非破壊で** 連結できることを確認する。

使い方::

    py scripts/run_backtest_v2.py --limit 20          # 確定レースを20件サンプル検証
    py scripts/run_backtest_v2.py --race-id 202605...  # 単一レース

本スクリプトは「学習データセット(DataFrame)生成の前処理モック」であり、
モデル fit/predict は行わない（次フェーズ v1.4.0 で実装）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3  # noqa: E402

import pandas as pd  # noqa: E402


def _base_frame_for_race(conn: sqlite3.Connection, race_id: str) -> pd.DataFrame:
    """学習基底 DataFrame のモック（horse_number と確定 rank のみ）。

    本来は FeatureBuilder が 69 列を生成するが、骨子では結合検証のため
    horse_number / rank の最小フレームを用いる（FEATURE_COLS には非干渉）。
    """
    rows = conn.execute(
        "SELECT horse_number, rank FROM race_results "
        "WHERE race_id = ? AND horse_number IS NOT NULL ORDER BY horse_number",
        (race_id,),
    ).fetchall()
    return pd.DataFrame(rows, columns=["horse_number", "rank"])


def assemble_v2_frame(
    conn: sqlite3.Connection, race_ids: list[str]
) -> pd.DataFrame:
    """複数レースの加速力特徴量付きフレームを縦結合して返す（再学習用前処理モック）。"""
    from src.features.backtest_v2 import attach_acceleration_features

    frames: list[pd.DataFrame] = []
    for rid in race_ids:
        base = _base_frame_for_race(conn, rid)
        if base.empty:
            continue
        merged = attach_acceleration_features(base, conn, rid)
        merged["race_id"] = rid
        frames.append(merged)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass
    p = argparse.ArgumentParser(description="再シミュレーション v2 骨子（学習データ生成検証）")
    p.add_argument("--race-id", help="単一レース ID")
    p.add_argument("--limit", type=int, default=20, help="確定レースのサンプル件数")
    args = p.parse_args()

    from src.database.init_db import init_db
    from src.features.backtest_v2 import build_feature_cols_v2
    from src.ml.models import FEATURE_COLS

    conn = init_db()
    try:
        if args.race_id:
            race_ids = [args.race_id]
        else:
            race_ids = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT race_id FROM race_results "
                    "WHERE rank IS NOT NULL AND last_3f IS NOT NULL "
                    "ORDER BY race_id DESC LIMIT ?",
                    (args.limit,),
                ).fetchall()
            ]
        df = assemble_v2_frame(conn, race_ids)
    finally:
        conn.close()

    cols_v2 = build_feature_cols_v2(FEATURE_COLS)
    print("=" * 60)
    print("再シミュレーション v2 骨子レポート")
    print("=" * 60)
    print(f"FEATURE_COLS (本番・不変): {len(FEATURE_COLS)} 列")
    print(f"FEATURE_COLS_V2 (評価用) : {len(cols_v2)} 列 (+{len(cols_v2) - len(FEATURE_COLS)})")
    print(f"対象レース               : {len(race_ids)}")
    print(f"組立行数                 : {len(df)}")
    if not df.empty:
        have = [c for c in ("last_3f_sec", "pci", "acceleration_score") if c in df.columns]
        print(f"加速力特徴量列           : {have}")
        print(f"last_3f 充填率           : {df['last_3f_sec'].notna().mean() * 100:.0f}%")
    print("※ 本番モデル(v1.2.0)には非結線。次フェーズで再学習に採用予定。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
