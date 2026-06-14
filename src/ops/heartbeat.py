"""
src/ops/heartbeat.py — 常駐プロセスの生存ハートビート（サイレント・ハング検知の土台）

背景（2026-06-14 障害 / v1.16.0-dev）:
  オートパイロット（today_auto_runner --continuous）が「クラッシュせず生きたまま空転
  （300秒タイムアウトを毎周期くり返す）」状態に陥っても、スーパーバイザー
  `UMALOGI_SCHEDULER.bat` は **プロセス終了時しか再起動しない**ため自己修復が一切
  発火しなかった。＝「土曜から動かない」サイレント停止の主機構。

  本モジュールは「プロセスが生きている」だけでなく「**進捗している**」ことを示す
  ハートビートを軽量なファイルに刻む。watchdog 側がハートビートの鮮度を監視し、
  鮮度が閾値を超えた（＝生きているが進捗していない）場合に強制再起動できる。

設計:
  - 1 名前 = 1 ファイル `data/<name>.heartbeat`。中身は `epoch_seconds\tISO8601` の1行。
  - 書き込みは原子的（tmp → replace）。読み取り失敗・欠損は None を返し、呼び出し側で安全に扱う。
  - 例外は決して送出しない（best-effort）。ハートビートの失敗で本処理を止めてはならない。
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

# data/ ディレクトリ（このファイルは src/ops/heartbeat.py）。
_ROOT: Path = Path(__file__).resolve().parents[2]
_HEARTBEAT_DIR: Path = _ROOT / "data"


def _path(name: str) -> Path:
    return _HEARTBEAT_DIR / f"{name}.heartbeat"


def write_heartbeat(name: str = "auto_runner", *, note: str = "") -> None:
    """name のハートビートを現在時刻で更新する（best-effort・例外を送出しない）。

    Args:
        name: ハートビート名（プロセス識別子）。
        note: 任意の補助情報（直近のフェーズ名など）。改行は除去される。
    """
    try:
        _HEARTBEAT_DIR.mkdir(parents=True, exist_ok=True)
        now = time.time()
        iso = datetime.now().isoformat(timespec="seconds")
        clean_note = note.replace("\n", " ").replace("\t", " ").strip()
        line = f"{now:.0f}\t{iso}\t{clean_note}"
        path = _path(name)
        tmp = path.with_suffix(".heartbeat.tmp")
        tmp.write_text(line, encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        # ハートビート失敗は本処理を絶対に妨げない。
        pass


def read_heartbeat_epoch(name: str = "auto_runner") -> float | None:
    """name のハートビートの最終更新 epoch 秒を返す。欠損・破損時は None。"""
    try:
        path = _path(name)
        if not path.exists():
            return None
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        first = raw.split("\t", 1)[0].strip()
        return float(first)
    except Exception:
        return None


def heartbeat_age_seconds(name: str = "auto_runner") -> float | None:
    """name のハートビート経過秒（now - 最終更新）を返す。欠損・破損時は None。"""
    epoch = read_heartbeat_epoch(name)
    if epoch is None:
        return None
    return max(0.0, time.time() - epoch)


def is_stale(name: str = "auto_runner", *, max_age_seconds: float) -> bool:
    """ハートビートが max_age_seconds より古い（=ハング疑い）なら True。

    欠損時は False を返す（「まだ一度も鼓動していない＝起動直後」を誤検知しない）。
    呼び出し側は別途プロセス存在判定と組み合わせること。
    """
    age = heartbeat_age_seconds(name)
    if age is None:
        return False
    return age > max_age_seconds


def clear_heartbeat(name: str = "auto_runner") -> None:
    """ハートビートファイルを削除する（プロセス正常終了時のクリーンアップ用）。"""
    try:
        _path(name).unlink(missing_ok=True)
    except Exception:
        pass
