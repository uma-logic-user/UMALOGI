"""過去データ整合性チェック・last_3f バックフィル・v2 特徴量アセンブリのテスト。

本番モデル(v1.2.0)の FEATURE_COLS(69列) を破壊しないことも担保する。
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from scripts.bulk_backfill_features import (
    _upsert_race_meta,
    backfill_last_3f,
    find_backfill_targets,
)
from scripts.check_jravan_integrity import scan_integrity
from src.features.backtest_v2 import (
    ACCEL_FEATURE_COLS,
    attach_acceleration_features,
    build_feature_cols_v2,
)


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE races (race_id TEXT PRIMARY KEY, date TEXT, venue TEXT, distance INTEGER);
        CREATE TABLE race_results (
            race_id TEXT, horse_number INTEGER, rank INTEGER,
            finish_time TEXT, last_3f REAL
        );
        """
    )


# ── 整合性チェック ──────────────────────────────────────────────────────────
def test_integrity_detects_missing_month() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    # 2025-01: 結果あり / 2025-02: スケジュールのみ結果ゼロ（欠損）
    conn.executemany(
        "INSERT INTO races VALUES (?, ?, '東京', 1600)",
        [("A1", "2025-01-05"), ("A2", "2025-01-12"), ("B1", "2025-02-09")],
    )
    conn.execute("INSERT INTO race_results VALUES ('A1', 1, 1, '1:36.0', NULL)")
    conn.execute("INSERT INTO race_results VALUES ('A2', 1, 1, '1:36.0', NULL)")
    # B1 は結果なし
    conn.commit()
    rep = scan_integrity(conn, today="2025-03-01")
    assert "2025-02" in rep.missing_months
    assert "2025-01" not in rep.missing_months
    assert not rep.is_healthy()
    assert rep.suggested_jvlink_ranges  # 再取得提案がある


def test_integrity_ignores_future_months() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('F1', '2026-12-05', '東京', 1600)")
    conn.commit()
    # today が 2026-06 → 2026-12 は未来なので欠損扱いしない
    rep = scan_integrity(conn, today="2026-06-01")
    assert "2026-12" not in rep.missing_months
    assert rep.is_healthy()


def test_integrity_healthy_when_all_covered() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('A1', '2025-01-05', '東京', 1600)")
    conn.execute("INSERT INTO race_results VALUES ('A1', 1, 1, '1:36.0', NULL)")
    conn.commit()
    rep = scan_integrity(conn, today="2025-03-01")
    assert rep.is_healthy()
    assert rep.overall_coverage == pytest.approx(1.0)


# ── バックフィル ────────────────────────────────────────────────────────────
def _backfill_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.executemany(
        "INSERT INTO races VALUES (?, ?, '東京', 1600)",
        [("R23", "2023-05-01"), ("R25", "2025-05-01"), ("R22", "2022-05-01")],
    )
    # R23/R25: 確定だが last_3f NULL（対象）/ R22: 期間外 / R25b: 既に充填済
    conn.executemany(
        "INSERT INTO race_results VALUES (?, ?, ?, ?, ?)",
        [
            ("R23", 1, 1, "1:36.0", None),
            ("R25", 1, 1, "1:36.0", None),
            ("R22", 1, 1, "1:36.0", None),  # since=2023 で除外
        ],
    )
    conn.commit()
    return conn


def test_find_backfill_targets_respects_range_and_null() -> None:
    conn = _backfill_conn()
    targets = find_backfill_targets(conn, since="2023-01-01", until="2025-12-31")
    assert "R23" in targets and "R25" in targets
    assert "R22" not in targets  # 期間外


def test_find_backfill_targets_skips_already_filled() -> None:
    conn = _backfill_conn()
    conn.execute("UPDATE race_results SET last_3f=34.5 WHERE race_id='R23'")
    conn.commit()
    targets = find_backfill_targets(conn, since="2023-01-01")
    assert "R23" not in targets  # 充填済みは冪等にスキップ


def test_backfill_uses_injected_fetcher_and_sleeps() -> None:
    conn = _backfill_conn()
    calls: list[str] = []
    slept: list[float] = []

    def fake_fetcher(rid: str, c: sqlite3.Connection) -> int:
        calls.append(rid)
        c.execute("UPDATE race_results SET last_3f=34.0 WHERE race_id=?", (rid,))
        return 1

    targets = find_backfill_targets(conn, since="2023-01-01")
    stats = backfill_last_3f(
        conn, targets, sleep_sec=1.0, fetcher=fake_fetcher, sleeper=slept.append
    )
    assert stats["targets"] == len(targets)
    assert stats["filled"] == len(targets)
    assert set(calls) == set(targets)
    # レース間 sleep（最後の1回は省く）
    assert len(slept) == len(targets) - 1
    assert all(s >= 1.0 for s in slept)


class _FakeInfo:
    def __init__(self, distance: int, surface: str = "") -> None:
        self.distance = distance
        self.surface = surface


def test_upsert_race_meta_fills_missing_distance() -> None:
    """distance が 0/NULL のとき netkeiba 値で補填（PCI 用）・既存有効値は非破壊。"""
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('R1', '2025-05-01', '東京', 0)")
    conn.execute("INSERT INTO races VALUES ('R2', '2025-05-01', '東京', 1600)")
    conn.commit()
    _upsert_race_meta(conn, "R1", _FakeInfo(2000))
    _upsert_race_meta(conn, "R2", _FakeInfo(9999))  # 既存1600は上書きしない
    assert conn.execute("SELECT distance FROM races WHERE race_id='R1'").fetchone()[0] == 2000
    assert conn.execute("SELECT distance FROM races WHERE race_id='R2'").fetchone()[0] == 1600


def test_upsert_race_meta_ignores_invalid_distance() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('R1', '2025-05-01', '東京', 0)")
    conn.commit()
    _upsert_race_meta(conn, "R1", _FakeInfo(0))  # 取得値も無効 → 何もしない
    assert conn.execute("SELECT distance FROM races WHERE race_id='R1'").fetchone()[0] == 0


def test_backfill_dry_run_does_not_fetch() -> None:
    conn = _backfill_conn()
    called = []
    stats = backfill_last_3f(
        conn, ["R23"], dry_run=True, fetcher=lambda r, c: called.append(r) or 1
    )
    assert stats["saved"] == 0
    assert called == []


def test_backfill_continues_on_error() -> None:
    conn = _backfill_conn()

    def flaky(rid: str, c: sqlite3.Connection) -> int:
        if rid == "R23":
            raise RuntimeError("network")
        c.execute("UPDATE race_results SET last_3f=34.0 WHERE race_id=?", (rid,))
        return 1

    stats = backfill_last_3f(
        conn, ["R23", "R25"], sleep_sec=0.0, fetcher=flaky, sleeper=lambda s: None
    )
    assert stats["errors"] == 1
    assert stats["filled"] == 1  # R25 は成功


# ── v2 特徴量アセンブリ（FEATURE_COLS 非破壊） ──────────────────────────────
def test_feature_cols_v2_appends_without_mutating_base() -> None:
    from src.ml.models import FEATURE_COLS

    base = list(FEATURE_COLS)
    v2 = build_feature_cols_v2(FEATURE_COLS)
    # 本番 FEATURE_COLS は不変（コピーされ変更されていない）
    assert FEATURE_COLS == base
    assert len(FEATURE_COLS) == 69
    # v2 = 本番 + 加速力特徴量（W-001 3列 + W-002 race_pci）
    assert v2[: len(FEATURE_COLS)] == list(FEATURE_COLS)
    assert v2[len(FEATURE_COLS) :] == ACCEL_FEATURE_COLS
    assert "race_pci" in ACCEL_FEATURE_COLS  # W-002
    assert len(v2) == 69 + len(ACCEL_FEATURE_COLS)


def test_feature_cols_v2_idempotent_no_dup() -> None:
    once = build_feature_cols_v2(["a", "pci"])
    assert once.count("pci") == 1


def test_attach_acceleration_features_merges_and_preserves_base() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('R1', '2025-05-01', '東京', 1600)")
    conn.executemany(
        "INSERT INTO race_results VALUES ('R1', ?, ?, ?, ?)",
        [(1, 1, "1:36.0", 34.0), (2, 2, "1:36.5", 33.0)],
    )
    conn.commit()
    base = pd.DataFrame({"horse_number": [1, 2], "dummy": [10, 20]})
    out = attach_acceleration_features(base, conn, "R1")
    # base は不変
    assert "pci" not in base.columns
    # 結合列が付与される
    for c in ("pci", "acceleration_score", "last_3f_sec"):
        assert c in out.columns
    assert out["pci"].notna().all()


def test_attach_acceleration_features_safe_without_last3f() -> None:
    conn = sqlite3.connect(":memory:")
    _schema(conn)
    conn.execute("INSERT INTO races VALUES ('R1', '2025-05-01', '東京', 1600)")
    conn.executemany(
        "INSERT INTO race_results VALUES ('R1', ?, ?, ?, ?)",
        [(1, 1, "1:36.0", None), (2, 2, "1:36.5", None)],
    )
    conn.commit()
    base = pd.DataFrame({"horse_number": [1, 2]})
    out = attach_acceleration_features(base, conn, "R1")
    assert out["pci"].isna().all()  # last_3f 無し → PCI NaN（安全）
    assert (out["acceleration_score"] == 0.0).all()
