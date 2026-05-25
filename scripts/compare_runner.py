"""
/api/compare/<race_id> 用 Python ブリッジスクリプト

Next.js API ルートから subprocess で呼び出され、
compare_predictions() の結果を JSON で stdout に出力する。

使い方:
    py scripts/compare_runner.py <race_id>
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Windows UTF-8 強制
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")


def main() -> None:
    if len(sys.argv) < 2:
        _exit_error("Usage: compare_runner.py <race_id>", code=2)

    race_id: str = sys.argv[1].strip()
    if not race_id:
        _exit_error("race_id が空です", code=2)

    db_path = PROJECT_ROOT / "data" / "umalogi.db"
    if not db_path.exists():
        _exit_error(f"DB が見つかりません: {db_path}", code=3)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        # レースが存在するか確認
        row = conn.execute(
            "SELECT race_id FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        if row is None:
            _exit_error(f"race_id not found: {race_id}", code=404)

        # compare_predictions() を呼び出す
        from src.umasugi_engine.comparator import compare_predictions

        df = compare_predictions(race_id, conn)

        if df.empty:
            # 空 DataFrame → エントリーデータなしとして処理
            result = _build_response(race_id, [])
            print(json.dumps(result, ensure_ascii=False))
            return

        horses: list[dict] = []
        for _, row_data in df.iterrows():
            horses.append(
                {
                    "horse_number": _int_or_none(row_data.get("horse_number")),
                    "horse_name": _str_or_none(row_data.get("horse_name")),
                    "legacy_ev": _float_or_none(row_data.get("legacy_ev")),
                    "umasugi_ev": _float_or_none(row_data.get("umasugi_ev")),
                    "ev_delta": _float_or_none(row_data.get("ev_delta")),
                    "track_style_score": _float_or_none(
                        row_data.get("track_style_score")
                    ),
                    "turf_type_score": _float_or_none(row_data.get("turf_type_score")),
                    "opinion_pressure_score": _float_or_none(
                        row_data.get("opinion_pressure_score")
                    ),
                    "crowd_ev_penalty": _float_or_none(
                        row_data.get("crowd_ev_penalty")
                    ),
                    "umasugi_score": _float_or_none(row_data.get("umasugi_score")),
                    "recommendation": _str_or_none(row_data.get("recommendation")),
                }
            )

        result = _build_response(race_id, horses)
        print(json.dumps(result, ensure_ascii=False))

    finally:
        conn.close()


def _build_response(race_id: str, horses: list[dict]) -> dict:
    """レスポンス JSON を組み立てる。"""
    total = len(horses)
    umasugi_better = sum(1 for h in horses if h.get("recommendation") == "umasugi_better")
    legacy_better = sum(1 for h in horses if h.get("recommendation") == "legacy_better")
    agree = sum(1 for h in horses if h.get("recommendation") == "agree")

    return {
        "race_id": race_id,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "horses": horses,
        "summary": {
            "total_horses": total,
            "umasugi_better": umasugi_better,
            "legacy_better": legacy_better,
            "agree": agree,
        },
    }


def _exit_error(msg: str, code: int = 1) -> None:
    """エラーメッセージを stderr に出力して終了する。"""
    print(json.dumps({"error": msg}, ensure_ascii=False), file=sys.stderr)
    sys.exit(code)


# ── 型変換ヘルパー ──────────────────────────────────────────────────────────

def _float_or_none(v: object) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)  # type: ignore[arg-type]
        import math
        return None if math.isnan(f) or math.isinf(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _int_or_none(v: object) -> int | None:
    if v is None:
        return None
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _str_or_none(v: object) -> str | None:
    if v is None:
        return None
    s = str(v)
    return None if s in ("nan", "None", "") else s


if __name__ == "__main__":
    main()
