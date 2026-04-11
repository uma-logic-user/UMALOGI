"""
今日（または指定日）の全レースに対して暫定予想を強制生成する。

使用方法:
  python scripts/force_provisional_today.py           # 今日のレース
  python scripts/force_provisional_today.py 20260412  # 指定日のレース

概要:
  - races テーブルから指定日のレース ID を取得
  - prerace_pipeline(race_id, provisional=True) を各レースに実行
  - 馬体重・オッズが未公開（NaN）でも LightGBM が欠損として推論
  - model_type は "本命(暫定)" / "卍(暫定)" として predictions テーブルへ保存

注意:
  - 既に暫定予想が保存済みのレースも再実行される（UPSERT ではなく追記）
  - 直前予想（本命(直前)）が後から生成されると UI 上で両方を比較可能
"""

from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db
from src.main_pipeline import prerace_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main(target_date: str | None = None) -> None:
    if target_date is None:
        target_date = date.today().strftime("%Y%m%d")

    formatted = f"{target_date[:4]}/{target_date[4:6]}/{target_date[6:8]}"
    logger.info("暫定予想強制生成: 対象日=%s (%s)", target_date, formatted)

    conn = init_db()
    race_ids: list[str] = [
        r[0] for r in conn.execute(
            "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
            (formatted,),
        ).fetchall()
    ]

    # 既存の暫定予想を削除（再実行時の重複防止）
    if race_ids:
        placeholders = ",".join("?" * len(race_ids))
        deleted = conn.execute(
            f"DELETE FROM predictions WHERE model_type LIKE '%暫定%'"
            f" AND race_id IN ({placeholders})",
            race_ids,
        )
        conn.commit()
        if deleted.rowcount:
            logger.info("既存の暫定予想を削除: %d 件（再生成前クリーン）", deleted.rowcount)
    conn.close()

    if not race_ids:
        logger.warning(
            "races テーブルに date='%s' のレースが見つかりません。"
            "先に friday_batch を実行してください。",
            formatted,
        )
        sys.exit(1)

    logger.info("対象レース数: %d", len(race_ids))
    succeeded: list[str] = []
    failed: list[str]    = []

    for race_id in race_ids:
        logger.info("暫定予想開始: %s", race_id)
        try:
            result = prerace_pipeline(race_id, provisional=True)
            if result.get("error"):
                logger.error("エラー %s: %s", race_id, result["error"])
                failed.append(race_id)
            elif result.get("skipped"):
                # 暫定モードでは品質チェックを skip しないが、特徴量 0 頭など
                logger.warning("スキップ %s: %s", race_id, result.get("reason"))
                failed.append(race_id)
            else:
                horses = result.get("horses", [])
                ev_recs = result.get("ev_recommend", [])
                logger.info(
                    "完了 %s: %d 頭予想 / EV>=1.0 推奨馬 %d 頭",
                    race_id, len(horses), len(ev_recs),
                )
                succeeded.append(race_id)
        except Exception as exc:
            logger.error("予想失敗 %s: %s", race_id, exc, exc_info=True)
            failed.append(race_id)

    print(f"\n{'=' * 60}")
    print(f"暫定予想強制生成 完了")
    print(f"  対象日  : {formatted}")
    print(f"  成功    : {len(succeeded)} レース")
    print(f"  失敗    : {len(failed)} レース")
    if succeeded:
        print(f"\n成功レース:")
        for r in succeeded:
            print(f"  [OK] {r}")
    if failed:
        print(f"\n失敗レース:")
        for r in failed:
            print(f"  [NG] {r}")
    print("=" * 60)

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    arg_date = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg_date)
