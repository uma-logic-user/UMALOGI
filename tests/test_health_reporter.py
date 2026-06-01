"""W-058 日次ヘルスレポート（src/ops/health_reporter.py）のテスト。"""

from __future__ import annotations

import sqlite3

from src.ops.health_reporter import collect_health, format_report_text


def _build_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT, date TEXT);
        CREATE TABLE predictions (race_id TEXT, model_type TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE realtime_odds (race_id TEXT, win_odds REAL, recorded_at TEXT);
        CREATE TABLE race_results (race_id TEXT, rank INTEGER);
        """
    )
    # 4レース
    conn.executemany(
        "INSERT INTO races VALUES (?, '2026-05-31')",
        [("R1",), ("R2",), ("R3",), ("R4",)],
    )
    # 予想: R1〜R3 に直前あり / R4 は無し（カバー率 3/4）
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type) VALUES (?, ?)",
        [("R1", "本命(直前)"), ("R2", "卍(直前)"), ("R3", "本命(直前)")],
    )
    # R2 は論理無効化のみ → 予想なし扱いにはしない（別レコードがある想定だが今回はR2を無効化で消す）
    conn.execute("UPDATE predictions SET is_superseded=1 WHERE race_id='R2'")
    # オッズ: R1=2点, R2=1点, R3=0点, R4=3点
    conn.executemany(
        "INSERT INTO realtime_odds VALUES (?, 5.0, ?)",
        [
            ("R1", "10:00"),
            ("R1", "14:00"),
            ("R2", "10:00"),
            ("R4", "10:00"),
            ("R4", "12:00"),
            ("R4", "14:00"),
        ],
    )
    # 結果: R1,R2,R4 あり / R3 欠損（欠損1）
    conn.executemany(
        "INSERT INTO race_results VALUES (?, 1)",
        [("R1",), ("R2",), ("R4",)],
    )
    conn.commit()
    return conn


def test_collect_health_metrics() -> None:
    conn = _build_conn()
    r = collect_health(conn, "2026-05-31")
    assert r.n_races == 4
    assert r.n_predicted == 2  # R1,R3（R2は論理無効化で除外）
    assert r.n_chokuzen == 2
    assert (r.n_odds_ge2, r.n_odds_1, r.n_odds_0) == (2, 1, 1)  # R1,R4 / R2 / R3
    assert r.n_results == 3
    assert r.n_results_missing == 1
    # 欠損>0 → crit
    assert r.severity == "crit"


def test_severity_ok_when_perfect() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT, date TEXT);
        CREATE TABLE predictions (race_id TEXT, model_type TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE realtime_odds (race_id TEXT, win_odds REAL, recorded_at TEXT);
        CREATE TABLE race_results (race_id TEXT, rank INTEGER);
        """
    )
    conn.execute("INSERT INTO races VALUES ('R1', '2026-05-31')")
    # 完璧ケース＝全実弾モデル（本命/卍/Alpha-Payout/Pure_EV_Edge）が直前予想を生成（W-064）
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type) VALUES ('R1', ?)",
        [("本命(直前)",), ("卍(直前)",), ("Alpha-Payout(直前)",), ("Pure_EV_Edge(直前)",)],
    )
    conn.executemany(
        "INSERT INTO realtime_odds VALUES ('R1', 5.0, ?)", [("10:00",), ("14:00",)]
    )
    conn.execute("INSERT INTO race_results VALUES ('R1', 1)")
    conn.commit()
    r = collect_health(conn, "2026-05-31")
    assert r.zero_live_models == []
    assert r.severity == "ok"
    assert "✅" in format_report_text(r)


def test_no_races_is_ok() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT, date TEXT);
        CREATE TABLE predictions (race_id TEXT, model_type TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE realtime_odds (race_id TEXT, win_odds REAL, recorded_at TEXT);
        CREATE TABLE race_results (race_id TEXT, rank INTEGER);
        """
    )
    r = collect_health(conn, "2026-06-02")
    assert r.n_races == 0
    assert r.severity == "ok"


def _build_model_count_conn() -> sqlite3.Connection:
    """実弾モデル別の直前予想生成件数を検証するための DB。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT, date TEXT);
        CREATE TABLE predictions (race_id TEXT, model_type TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE realtime_odds (race_id TEXT, win_odds REAL, recorded_at TEXT);
        CREATE TABLE race_results (race_id TEXT, rank INTEGER);
        """
    )
    conn.executemany("INSERT INTO races VALUES (?, '2026-05-31')", [("R1",), ("R2",)])
    # 本命/卍/Alpha-Payout は直前予想あり、Pure_EV_Edge は 0 件（=サイレント障害）
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type) VALUES (?, ?)",
        [
            ("R1", "本命(直前)"),
            ("R2", "本命(直前)"),
            ("R1", "卍(直前)"),
            ("R1", "Alpha-Payout(直前)"),
            ("R1", "Oracle(直前)"),  # 観賞用は実弾カウント対象外
        ],
    )
    # 各レースに完全なオッズ・結果（他の crit/warn 要因を排除）
    for rid in ("R1", "R2"):
        conn.executemany(
            "INSERT INTO realtime_odds VALUES (?, 5.0, ?)",
            [(rid, "10:00"), (rid, "14:00")],
        )
        conn.execute("INSERT INTO race_results VALUES (?, 1)", (rid,))
    conn.commit()
    return conn


def test_model_counts_populated() -> None:
    """直前予想の実弾モデル別生成件数（distinct race）が集計される。"""
    r = collect_health(_build_model_count_conn(), "2026-05-31")
    assert r.model_counts["本命"] == 2
    assert r.model_counts["卍"] == 1
    assert r.model_counts["Alpha-Payout"] == 1
    assert r.model_counts["Pure_EV_Edge"] == 0  # サイレント障害


def test_zero_live_model_triggers_warn() -> None:
    """開催日に生成0件の実弾モデルがあれば zero_live_models に載り warn 以上へ昇格する。"""
    r = collect_health(_build_model_count_conn(), "2026-05-31")
    assert "Pure_EV_Edge" in r.zero_live_models
    assert "本命" not in r.zero_live_models
    # 他に crit 要因が無くても 0件モデルがあれば warn 以上
    assert r.severity in ("warn", "crit")
    assert "Pure_EV_Edge" in format_report_text(r)


def test_no_zero_models_when_all_present() -> None:
    """全実弾モデルが生成されていれば zero_live_models は空。"""
    conn = _build_model_count_conn()
    conn.executemany(
        "INSERT INTO predictions (race_id, model_type) VALUES (?, ?)",
        [
            ("R1", "Pure_EV_Edge(直前)"),
            ("R2", "卍(直前)"),
            ("R2", "Alpha-Payout(直前)"),
        ],
    )
    conn.commit()
    r = collect_health(conn, "2026-05-31")
    assert r.zero_live_models == []


def test_no_races_no_zero_model_alert() -> None:
    """非開催日は 0件モデルアラートを出さない（false positive 防止）。"""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT, date TEXT);
        CREATE TABLE predictions (race_id TEXT, model_type TEXT, is_superseded INTEGER DEFAULT 0);
        CREATE TABLE realtime_odds (race_id TEXT, win_odds REAL, recorded_at TEXT);
        CREATE TABLE race_results (race_id TEXT, rank INTEGER);
        """
    )
    r = collect_health(conn, "2026-06-02")
    assert r.zero_live_models == []
    assert r.severity == "ok"
