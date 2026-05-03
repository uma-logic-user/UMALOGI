"""
UMALOGI 完全自動化パイプライン — エントリポイント

実装は src/pipeline/ 以下の各モジュールに分割されています。
このファイルは後方互換性のための薄いシムです。

使用例:
  python -m src.main_pipeline friday
  python -m src.main_pipeline prerace <race_id>
  python -m src.main_pipeline provisional --date 20260601
  python -m src.main_pipeline simulate <race_id>
  python -m src.main_pipeline win5
  python -m src.main_pipeline train
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

# パイプライン各モジュールを re-export（既存 import の互換性維持）
from src.pipeline.scraping import friday_batch, save_entries_to_db, fetch_and_save_odds
from src.pipeline.prediction import prerace_pipeline, provisional_batch
from src.pipeline.simulation import simulate_pipeline
from src.pipeline.win5 import win5_batch, try_win5
from src.pipeline.training import train_pipeline

__all__ = [
    "friday_batch",
    "prerace_pipeline",
    "provisional_batch",
    "simulate_pipeline",
    "win5_batch",
    "train_pipeline",
]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="UMALOGI 自動予想パイプライン",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.main_pipeline friday                           # 翌日の出馬表取得
  python -m src.main_pipeline friday --date 20250628           # 指定日の出馬表取得
  python -m src.main_pipeline provisional                      # 翌日の全レース暫定予想
  python -m src.main_pipeline provisional --date 20260411      # 指定日の全レース暫定予想
  python -m src.main_pipeline prerace 202506050811             # 指定レースの直前予想
  python -m src.main_pipeline prerace 202506050811 --provisional  # 指定レースの暫定予想
  python -m src.main_pipeline simulate 202506050811            # 過去レースのシミュレーション
  python -m src.main_pipeline win5                             # WIN5 バッチ予測
  python -m src.main_pipeline train                            # モデル再学習
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_fri = sub.add_parser("friday", help="金曜バッチ: 翌日の出馬表取得")
    p_fri.add_argument("--date", metavar="YYYYMMDD")

    p_prov = sub.add_parser("provisional", help="暫定予想バッチ")
    p_prov.add_argument("--date", metavar="YYYYMMDD")

    p_pre = sub.add_parser("prerace", help="レース直前予想パイプライン")
    p_pre.add_argument("race_id")
    p_pre.add_argument("--provisional", action="store_true")

    p_sim = sub.add_parser("simulate", help="過去レースのシミュレーション")
    p_sim.add_argument("race_id")

    p_win5 = sub.add_parser("win5", help="WIN5 バッチ予測")
    p_win5.add_argument("--date", metavar="YYYYMMDD")

    sub.add_parser("train", help="モデル再学習")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args(argv)

    if args.command == "friday":
        saved = friday_batch(target_date=getattr(args, "date", None))
        print(f"保存レース数: {len(saved)}")
        for r in saved:
            print(f"  {r}")

    elif args.command == "provisional":
        race_ids = provisional_batch(target_date=getattr(args, "date", None))
        print(f"暫定予想完了: {len(race_ids)} レース")
        for r in race_ids:
            print(f"  {r}")

    elif args.command == "prerace":
        prov   = getattr(args, "provisional", False)
        result = prerace_pipeline(args.race_id, provisional=prov)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "simulate":
        result = simulate_pipeline(args.race_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "win5":
        result = win5_batch(target_date=getattr(args, "date", None))
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "train":
        train_pipeline()


if __name__ == "__main__":
    main()
