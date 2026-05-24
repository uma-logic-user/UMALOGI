"""training_times 全行に training_grade を計算して更新するバッチスクリプト"""
from __future__ import annotations

import sqlite3
import sys

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

GRADE_QUANTILES: dict[str, float] = {
    "S": 0.05,
    "A": 0.15,
    "B": 0.35,
    "C": 0.65,
    "D": 0.85,
}  # E は残り全て


def assign_grade(time_4f: float, thresholds: dict[str, float]) -> str:
    """time_4f (秒・小さいほど速い) からグレードを返す"""
    for grade, q in GRADE_QUANTILES.items():
        if time_4f <= thresholds[grade]:
            return grade
    return "E"


def run(db_path: str = "data/umalogi.db") -> None:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT id, course_type, time_4f FROM training_times WHERE time_4f > 0",
        conn,
    )
    if df.empty:
        print("time_4f データなし")
        conn.close()
        return

    # course_type 別に分位数を計算
    thresholds_by_course: dict[str, dict[str, float]] = {}
    for ct, grp in df.groupby("course_type"):
        qs = grp["time_4f"].quantile(list(GRADE_QUANTILES.values())).to_dict()
        thresholds_by_course[str(ct)] = dict(zip(GRADE_QUANTILES.keys(), qs.values()))

    df["training_grade"] = df.apply(
        lambda r: assign_grade(
            r["time_4f"],
            thresholds_by_course.get(str(r["course_type"]), {k: 999 for k in GRADE_QUANTILES}),
        ),
        axis=1,
    )

    # バッチ UPDATE
    updates = list(zip(df["training_grade"].tolist(), df["id"].tolist()))
    conn.executemany("UPDATE training_times SET training_grade = ? WHERE id = ?", updates)
    conn.commit()

    grade_dist = df["training_grade"].value_counts().sort_index()
    print(f"グレード付与完了: {len(df):,} 件")
    print(grade_dist.to_string())
    conn.close()


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "data/umalogi.db")
