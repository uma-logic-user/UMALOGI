"""
U score 統合モデルのドライラン再学習スクリプト
実行: py scripts/dry_run_retrain.py
結果: data/dry_run_result.json に保存
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_ROOT / "data" / "dry_run_retrain.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("dry_run")

result: dict = {"status": "running", "start_time": time.strftime("%Y-%m-%d %H:%M:%S")}
out_path = _ROOT / "data" / "dry_run_result.json"
out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    from src.database.init_db import init_db
    from src.ops.retrain_trigger import weekly_retrain

    logger.info("=== U score ドライラン再学習 開始 ===")
    t0 = time.time()

    conn = init_db()
    try:
        # ① データ量確認
        n_races = conn.execute(
            "SELECT COUNT(DISTINCT rr.race_id) FROM race_results rr WHERE rr.rank IS NOT NULL"
        ).fetchone()[0]
        n_samples = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE rank IS NOT NULL"
        ).fetchone()[0]
        logger.info("学習データ: %d レース / %d サンプル", n_races, n_samples)

        # ② 再学習実行（U score 27列込み）
        versions = weekly_retrain(conn, validate=True)
        elapsed = time.time() - t0

        result.update({
            "status": "success",
            "elapsed_sec": round(elapsed, 1),
            "n_races": n_races,
            "n_samples": n_samples,
            "models": {k: str(v) for k, v in versions.items()},
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        logger.info("=== 再学習完了 (%.0f秒) ===", elapsed)
        logger.info("モデル: %s", result["models"])

    except Exception as e:
        result.update({"status": "error", "error": str(e)})
        logger.error("再学習失敗: %s", e, exc_info=True)
    finally:
        conn.close()
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("結果を %s に保存", out_path)


if __name__ == "__main__":
    main()
