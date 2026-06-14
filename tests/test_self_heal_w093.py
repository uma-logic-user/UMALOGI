"""
W-093 / v1.16.0-dev — サイレント停止根治＋自己修復の回帰テスト

検証対象:
  1. safe_subprocess.safe_run が CP932 リードバイト（0x83）を吐く子プロセスでも
     UnicodeDecodeError でリーダースレッドを落とさず、パイプを drain して完走する
     （2026-06-14 障害の根本原因クラスの再発防止）。
  2. heartbeat の書き込み・経過秒・鮮度判定が期待通り動く（ハング検知の土台）。
"""

from __future__ import annotations

import sys
import time

from src.ops import heartbeat
from src.ops.safe_subprocess import safe_run


def test_safe_run_survives_cp932_lead_byte(tmp_path) -> None:
    """子プロセスが CP932 バイト(0x83 等)を stdout に吐いても safe_run は完走する。

    旧実装（text=True, encoding='utf-8', errors未指定）では _readerthread が
    UnicodeDecodeError で死に、パイプ詰まり→ハングまたは例外になっていた。
    """
    # stdout に生の CP932 バイト列（"ソ" の一部 0x83 0x5c 等）を書く子プロセス。
    code = (
        "import sys; "
        "sys.stdout.buffer.write(bytes([0x83,0x5c,0x83,0x65,0x83,0x4e])); "
        "sys.stdout.buffer.flush()"
    )
    result = safe_run([sys.executable, "-c", code], timeout=30)
    assert result.returncode == 0
    # errors='replace' なので例外なく文字列が得られる（中身は置換文字でも可）。
    assert isinstance(result.stdout, str)


def test_safe_run_drains_large_cp932_output() -> None:
    """大量の CP932 バイト出力でもパイプ詰まりせず drain される（デッドロック回避）。"""
    code = (
        "import sys; "
        "sys.stdout.buffer.write(bytes([0x83,0x5c])*100000); "
        "sys.stdout.buffer.flush()"
    )
    # 旧実装ならリーダースレッド死亡→パイプ満杯→子ブロック→timeout だった。
    result = safe_run([sys.executable, "-c", code], timeout=30)
    assert result.returncode == 0


def test_heartbeat_roundtrip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(heartbeat, "_HEARTBEAT_DIR", tmp_path)
    name = "unit_test_runner"
    assert heartbeat.heartbeat_age_seconds(name) is None  # 未鼓動
    assert heartbeat.is_stale(name, max_age_seconds=1) is False  # 欠損は誤検知しない

    heartbeat.write_heartbeat(name, note="phase-x")
    age = heartbeat.heartbeat_age_seconds(name)
    assert age is not None and age < 5
    assert heartbeat.is_stale(name, max_age_seconds=60) is False


def test_heartbeat_detects_staleness(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(heartbeat, "_HEARTBEAT_DIR", tmp_path)
    name = "stale_runner"
    heartbeat.write_heartbeat(name)
    # 過去の時刻を直接書き込んでハングをシミュレート。
    path = tmp_path / f"{name}.heartbeat"
    old_epoch = time.time() - 3600
    path.write_text(f"{old_epoch:.0f}\told\t", encoding="utf-8")

    age = heartbeat.heartbeat_age_seconds(name)
    assert age is not None and age > 3000
    assert heartbeat.is_stale(name, max_age_seconds=900) is True


def test_heartbeat_clear(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(heartbeat, "_HEARTBEAT_DIR", tmp_path)
    name = "clear_runner"
    heartbeat.write_heartbeat(name)
    assert (tmp_path / f"{name}.heartbeat").exists()
    heartbeat.clear_heartbeat(name)
    assert not (tmp_path / f"{name}.heartbeat").exists()


def test_heartbeat_corrupt_file_returns_none(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(heartbeat, "_HEARTBEAT_DIR", tmp_path)
    name = "corrupt_runner"
    (tmp_path / f"{name}.heartbeat").write_text(
        "garbage-not-a-number", encoding="utf-8"
    )
    assert heartbeat.read_heartbeat_epoch(name) is None
    assert heartbeat.heartbeat_age_seconds(name) is None


# ── watchdog 自己修復（ハング検知）────────────────────────────────────────────


def test_watchdog_kills_hung_autopilot(monkeypatch) -> None:
    """生存 PID + stale 鼓動 → 強制終了が呼ばれる（自己修復が発火する）。"""
    from scripts import watchdog

    killed: list[int] = []
    monkeypatch.setattr(watchdog, "_read_auto_runner_pid", lambda: 12345)
    monkeypatch.setattr(watchdog, "_is_pid_alive", lambda pid: True)
    monkeypatch.setattr(watchdog, "_kill_pid", lambda pid: killed.append(pid))
    monkeypatch.setattr(watchdog, "_discord", lambda msg: None)
    monkeypatch.setattr(watchdog, "_last_autopilot_kill_ts", 0.0)
    # 鼓動が閾値超で stale
    monkeypatch.setattr(
        "src.ops.heartbeat.heartbeat_age_seconds",
        lambda name="auto_runner": watchdog._HEARTBEAT_STALE_SEC + 100,
    )
    assert watchdog.check_autopilot_heartbeat() is True
    assert killed == [12345]


def test_watchdog_ignores_healthy_autopilot(monkeypatch) -> None:
    """生存 PID + 新鮮な鼓動 → 何もしない。"""
    from scripts import watchdog

    killed: list[int] = []
    monkeypatch.setattr(watchdog, "_read_auto_runner_pid", lambda: 12345)
    monkeypatch.setattr(watchdog, "_is_pid_alive", lambda pid: True)
    monkeypatch.setattr(watchdog, "_kill_pid", lambda pid: killed.append(pid))
    monkeypatch.setattr(
        "src.ops.heartbeat.heartbeat_age_seconds", lambda name="auto_runner": 5.0
    )
    assert watchdog.check_autopilot_heartbeat() is False
    assert killed == []


def test_watchdog_ignores_when_no_pid(monkeypatch) -> None:
    """PID 不在 → supervisor に委ねる（kill しない）。"""
    from scripts import watchdog

    monkeypatch.setattr(watchdog, "_read_auto_runner_pid", lambda: None)
    assert watchdog.check_autopilot_heartbeat() is False


def test_watchdog_ignores_dead_pid(monkeypatch) -> None:
    """PID は記録されているが既に死亡 → supervisor 再起動に委ねる。"""
    from scripts import watchdog

    killed: list[int] = []
    monkeypatch.setattr(watchdog, "_read_auto_runner_pid", lambda: 999)
    monkeypatch.setattr(watchdog, "_is_pid_alive", lambda pid: False)
    monkeypatch.setattr(watchdog, "_kill_pid", lambda pid: killed.append(pid))
    assert watchdog.check_autopilot_heartbeat() is False
    assert killed == []
