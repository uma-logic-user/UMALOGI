"""W-003 不完全燃焼度スコア（Incomplete Combustion Score）実現可能性 調査プロトタイプ。

仮説: 「強い上がり3F（レース内で速い末脚）を使ったのに着順が悪かった馬」は脚を余した
／不利な競馬だった＝能力を出し切れていない＝次走で改善しやすい。通過順位データが無くても
（W-073でDB未保有）、`last_3f のレース内順位` と `着順` の乖離で代替計算できる。

incomplete_combustion = max(0, finish_rank - last3f_rank_in_race)
  例) 上がり最速(last3f_rank=1)なのに6着(finish_rank=6) → 5（脚を余した＝高）
      着順=上がり順 → 0（出し切った）

検証（リークフリー）: 各馬の「前走」での combustion を説明変数、「今走」の複勝(rank<=3)を
目的変数として、combustion バケットごとの今走複勝率を比較する。前走→今走の時系列で
リークは無い。combustion が高いほど今走で巻き返す傾向があれば因子として有効。

本スクリプトは調査専用（本番非結線）。
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3  # noqa: E402

import pandas as pd  # noqa: E402


def compute_combustion_for_race(
    conn: sqlite3.Connection, race_id: str
) -> dict[str, float]:
    """1レースの各馬の不完全燃焼度を {horse_id: score} で返す（そのレース結果ベース）。"""
    rows = conn.execute(
        """
        SELECT horse_id, rank, last_3f FROM race_results
        WHERE race_id=? AND rank>0 AND last_3f IS NOT NULL AND horse_id IS NOT NULL
        """,
        (race_id,),
    ).fetchall()
    if len(rows) < 3:
        return {}
    # 上がり3Fのレース内順位（速い=小さい=1位）
    ranked = sorted(rows, key=lambda r: r[2])
    last3f_rank = {r[0]: i + 1 for i, r in enumerate(ranked)}
    out: dict[str, float] = {}
    for hid, fin_rank, _l3 in rows:
        out[hid] = max(0.0, float(fin_rank) - float(last3f_rank[hid]))
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    from src.database.init_db import init_db

    conn = init_db()
    try:
        # 2025-01〜2026-05 の確定レースを対象（サンプル）
        race_rows = conn.execute(
            """
            SELECT DISTINCT r.race_id, r.date FROM races r
            WHERE r.date >= '2025-01-01' AND r.date < '2026-06-01'
              AND EXISTS (SELECT 1 FROM race_results rr
                          WHERE rr.race_id=r.race_id AND rr.rank>0 AND rr.last_3f IS NOT NULL)
            ORDER BY r.date
            """
        ).fetchall()
        print(f"対象レース: {len(race_rows)}")

        # 全レースの combustion を計算しキャッシュ
        comb_by_race: dict[str, dict[str, float]] = {}
        for rid, _d in race_rows:
            c = compute_combustion_for_race(conn, rid)
            if c:
                comb_by_race[rid] = c

        # 各馬の (日付, race_id, finish_rank) 履歴を取得し「前走 combustion → 今走複勝」を作る
        hist = pd.read_sql(
            """
            SELECT rr.horse_id, r.date, rr.race_id, rr.rank
            FROM race_results rr JOIN races r ON rr.race_id=r.race_id
            WHERE r.date >= '2025-01-01' AND r.date < '2026-06-01'
              AND rr.rank>0 AND rr.horse_id IS NOT NULL
            ORDER BY rr.horse_id, r.date
            """,
            conn,
        )
        samples = []
        for hid, grp in hist.groupby("horse_id"):
            grp = grp.sort_values("date")
            prev_rid = None
            for _, row in grp.iterrows():
                if prev_rid is not None:
                    comb = comb_by_race.get(prev_rid, {}).get(hid)
                    if comb is not None:
                        samples.append(
                            {
                                "prev_combustion": comb,
                                "this_place": 1 if row["rank"] <= 3 else 0,
                                "this_win": 1 if row["rank"] == 1 else 0,
                            }
                        )
                prev_rid = row["race_id"]

        df = pd.DataFrame(samples)
        print(f"前走→今走サンプル数: {len(df)}")
        if df.empty:
            return 1

        base_place = df["this_place"].mean()
        base_win = df["this_win"].mean()
        print(f"\n全体 複勝率={base_place * 100:.1f}%  勝率={base_win * 100:.1f}%")
        print("\n=== 前走 不完全燃焼度 バケット別 今走成績（リークフリー）===")
        print(f"{'combustion':<14}{'n':>8}{'複勝率%':>10}{'対全体':>10}{'勝率%':>9}")
        print("-" * 52)
        bins = [(-0.1, 0.1), (0.1, 2.1), (2.1, 4.1), (4.1, 6.1), (6.1, 99)]
        labels = ["0(出切)", "1-2", "3-4", "5-6", "7+(余力大)"]
        for (lo, hi), lab in zip(bins, labels):
            sub = df[(df["prev_combustion"] > lo) & (df["prev_combustion"] <= hi)]
            if len(sub) < 20:
                print(f"{lab:<14}{len(sub):>8}  (サンプル不足)")
                continue
            pr = sub["this_place"].mean()
            wr = sub["this_win"].mean()
            lift = (pr - base_place) * 100
            print(
                f"{lab:<14}{len(sub):>8}{pr * 100:>10.1f}{lift:>+10.1f}{wr * 100:>9.1f}"
            )

        # 相関
        corr_place = df["prev_combustion"].corr(df["this_place"])
        corr_win = df["prev_combustion"].corr(df["this_win"])
        print(f"\n相関係数: combustion×今走複勝={corr_place:+.4f} / ×今走勝利={corr_win:+.4f}")
        print(
            "\n判定: バケットが上がるほど複勝率が単調増加し相関>0 なら、"
            "W-003不完全燃焼度は有効因子（次走巻き返し）。逆なら因子として弱い。"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
