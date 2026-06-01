"""src.ops.db_optimize（VACUUM/ANALYZE）および深夜保守ジョブの回帰テスト。"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest import mock

import pytest

from src.ops.db_optimize import optimize_db


def _make_bloated_db(path: Path) -> None:
    """断片化した DB を作る: 大量行を INSERT → 大半を DELETE（VACUUM で縮む状態）。"""
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE blob_t (id INTEGER PRIMARY KEY, payload TEXT)")
    conn.executemany(
        "INSERT INTO blob_t (payload) VALUES (?)",
        [("x" * 2048,) for _ in range(2000)],
    )
    conn.commit()
    # 9 割削除 → 大量のフリーページが発生し VACUUM で回収可能になる
    conn.execute("DELETE FROM blob_t WHERE id % 10 != 0")
    conn.commit()
    conn.close()


def test_vacuum_shrinks_file(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _make_bloated_db(db)
    before = db.stat().st_size

    result = optimize_db(db, vacuum=True, analyze=True)

    assert result["ok"] is True
    assert result["vacuumed"] is True
    assert result["analyzed"] is True
    assert result["before_bytes"] == before
    # VACUUM 後はフリーページが回収されファイルが縮小する
    assert result["after_bytes"] < before
    assert result["saved_bytes"] > 0
    assert db.stat().st_size == result["after_bytes"]


def test_analyze_creates_stat_table(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _make_bloated_db(db)

    optimize_db(db, vacuum=False, analyze=True)

    conn = sqlite3.connect(str(db))
    has_stat = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'"
    ).fetchone()[0]
    conn.close()
    assert has_stat == 1


def test_analyze_only_does_not_vacuum(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    _make_bloated_db(db)
    before = db.stat().st_size

    result = optimize_db(db, vacuum=False, analyze=True)

    assert result["vacuumed"] is False
    assert result["analyzed"] is True
    # VACUUM していないのでサイズはほぼ不変（縮小していないこと）
    assert result["after_bytes"] >= before


def test_wal_mode_db_optimizes_cleanly(tmp_path: Path) -> None:
    """WAL モードでも VACUUM が autocommit エラーなく完走すること。"""
    db = tmp_path / "wal.db"
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.executemany(
        "INSERT INTO t (v) VALUES (?)", [("y" * 512,) for _ in range(1000)]
    )
    conn.commit()
    conn.execute("DELETE FROM t WHERE id % 2 = 0")
    conn.commit()
    conn.close()

    result = optimize_db(db, vacuum=True, analyze=True)
    assert result["ok"] is True
    assert result["vacuumed"] is True


def test_missing_db_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        optimize_db(tmp_path / "nope.db")


# ── 深夜保守ジョブ（scheduler）の挙動 ────────────────────────────────
def test_nightly_maintenance_backs_up_then_optimizes() -> None:
    """job_nightly_maintenance はバックアップ → 最適化の順で両方を呼ぶ。"""
    import scripts.scheduler as sched

    ok_result = {
        "ok": True,
        "vacuumed": True,
        "analyzed": True,
        "before_bytes": 100,
        "after_bytes": 80,
        "saved_bytes": 20,
        "elapsed_sec": 0.1,
    }
    with (
        mock.patch("src.ops.backup.backup_db", return_value=Path("b.db")) as m_bak,
        mock.patch("src.ops.db_optimize.optimize_db", return_value=ok_result) as m_opt,
        mock.patch.object(sched, "_send_discord") as m_disc,
        mock.patch.object(sched, "_mark_job_done") as m_done,
    ):
        sched.job_nightly_maintenance()

    m_bak.assert_called_once()
    m_opt.assert_called_once()
    m_disc.assert_not_called()  # 正常時は Discord 警告なし
    m_done.assert_called_once_with("job_nightly_maintenance")


def test_nightly_maintenance_alerts_on_optimize_error() -> None:
    """VACUUM/ANALYZE がエラー (ok=False) なら Discord 警告を送る。"""
    import scripts.scheduler as sched

    ng_result = {
        "ok": False,
        "vacuumed": False,
        "analyzed": True,
        "before_bytes": 100,
        "after_bytes": 100,
        "saved_bytes": 0,
        "elapsed_sec": 0.1,
    }
    with (
        mock.patch("src.ops.backup.backup_db", return_value=Path("b.db")),
        mock.patch("src.ops.db_optimize.optimize_db", return_value=ng_result),
        mock.patch.object(sched, "_send_discord") as m_disc,
        mock.patch.object(sched, "_mark_job_done") as m_done,
    ):
        sched.job_nightly_maintenance()

    m_disc.assert_called_once()
    m_done.assert_called_once_with("job_nightly_maintenance")


def test_nightly_maintenance_handles_backup_failure() -> None:
    """バックアップが例外でも握りつぶさず Discord 通知し、job 完了マークは打つ。"""
    import scripts.scheduler as sched

    with (
        mock.patch("src.ops.backup.backup_db", side_effect=RuntimeError("disk full")),
        mock.patch("src.ops.db_optimize.optimize_db") as m_opt,
        mock.patch.object(sched, "_send_discord") as m_disc,
        mock.patch.object(sched, "_mark_job_done") as m_done,
    ):
        sched.job_nightly_maintenance()

    m_opt.assert_not_called()  # バックアップ失敗時は最適化に進まない
    m_disc.assert_called_once()
    m_done.assert_called_once_with("job_nightly_maintenance")
