"""月末一斉メンテナンス（P0-1/P1-4/P1-5/P0-3）の回帰テスト。"""

from __future__ import annotations

import sqlite3

from src.ml.manji_calibration import calibrate_combo_prob, calibrate_win_prob
from src.pipeline.prediction import _supersede_prior_predictions


# ── P0-3: 卍 confidence 飽和の解消 ──────────────────────────────────
def test_manji_confidence_no_saturation() -> None:
    # 旧実装は min(ev/odds,1.0) で ev>=odds のとき 1.0 飽和。較正後は飽和しない。
    for ev, odds in [(5.0, 3.0), (10.0, 2.0), (3.0, 1.5)]:
        p = calibrate_win_prob(ev, odds)
        assert 0.0 <= p < 1.0
        assert p <= 0.7  # 較正/フォールバックいずれでも 1.0 飽和しない


def test_manji_combo_prob_no_inflation() -> None:
    # 旧実装は min(raw*係数,1.0)。較正後は生確率をそのままクランプ（膨張なし）。
    assert calibrate_combo_prob(0.02) == 0.02
    assert calibrate_combo_prob(0.5) == 0.5
    assert calibrate_combo_prob(1.5) == 0.99  # 上限クランプ
    assert calibrate_combo_prob(-0.1) == 0.0


# ── P1-4: is_superseded による二重計上防止 ────────────────────────
def _pred_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE predictions ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, race_id TEXT, model_type TEXT, "
        "bet_type TEXT, is_superseded INTEGER NOT NULL DEFAULT 0)"
    )
    return conn


def test_supersede_marks_only_same_variant() -> None:
    conn = _pred_conn()
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type, bet_type) VALUES (?,?,?)",
        [
            ("R1", "本命(直前)", "単勝"),
            ("R1", "卍(直前)", "複勝"),
            ("R1", "Alpha-Payout(直前)", "三連複"),
            ("R1", "本命V2(直前)", "単勝"),  # V2 は巻き込まない
            ("R1", "本命(暫定)", "単勝"),  # 暫定は対象外
            ("R2", "本命(直前)", "単勝"),  # 別レースは対象外
        ],
    )
    conn.commit()

    n = _supersede_prior_predictions(conn, "R1", "(直前)")
    assert n == 3  # 本命/卍/Alpha の直前（V2・暫定・別レースを除く）

    superseded = {
        r[0]
        for r in conn.execute(
            "SELECT model_type FROM predictions WHERE is_superseded = 1"
        ).fetchall()
    }
    assert superseded == {"本命(直前)", "卍(直前)", "Alpha-Payout(直前)"}


def test_supersede_v2_variant_only() -> None:
    conn = _pred_conn()
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type, bet_type) VALUES (?,?,?)",
        [("R1", "本命(直前)", "単勝"), ("R1", "本命V2(直前)", "単勝")],
    )
    conn.commit()
    n = _supersede_prior_predictions(conn, "R1", "V2(直前)")
    assert n == 1
    row = conn.execute(
        "SELECT is_superseded FROM predictions WHERE model_type='本命(直前)'"
    ).fetchone()
    assert row[0] == 0  # 非V2は無効化されない


# ── P1-5: 発走時刻の動的取得（post_time 優先・フォールバック）──────
def test_estimate_start_prefers_post_time() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "tar_runner", "scripts/today_auto_runner.py"
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    # post_time 優先
    assert m._estimate_start("2026-05-31", 11, "15:40").strftime("%H:%M") == "15:40"
    # 空 → R1=10:00+30分×(11-1)=15:00 推定
    assert m._estimate_start("2026-05-31", 11, "").strftime("%H:%M") == "15:00"
    # 不正 → 推定にフォールバック
    assert m._estimate_start("2026-05-31", 1, "99:99").strftime("%H:%M") == "10:00"
