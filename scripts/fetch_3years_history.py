"""過去3年分（2023〜現在）JRAデータ一括取得（W-077 / フェーズ2）。

既存の堅牢な取得基盤 ``scripts/import_historical.py`` の step 関数を再利用する
薄いオーケストレータ。3年分を年単位に分割し、マスタ→レース(SE)→調教(WOOD)の順で
取得する（FK 制約・OOM 回避）。netkeiba スクレイピングは一切行わず JVLink のみ使用。

設計方針:
  - 重複実装を避け import_historical の step_masters/step_races/step_wood を呼ぶ。
  - mode="stored"（TARGET frontier ローカルキャッシュ優先・高速）を既定とし、
    失敗時は step_races 内で OPT_STORED→不可なら警告。SETUP 全量は --mode setup。
  - 各 step は冪等（UPSERT）なので中断後の再実行で安全に再開できる。

使い方:
  py scripts/fetch_3years_history.py                 # 2023〜現在・stored
  py scripts/fetch_3years_history.py --mode setup    # サーバー全量取得
  py scripts/fetch_3years_history.py --only-races     # レースのみ
"""

from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.import_historical import (  # noqa: E402
    step_masters,
    step_races,
    step_wood,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="過去3年分JRAデータ取得（JVLink）")
    ap.add_argument("--from-year", type=int, default=2023)
    ap.add_argument("--to-year", type=int, default=datetime.date.today().year)
    ap.add_argument(
        "--mode",
        choices=["setup", "stored", "normal"],
        default="stored",
        help="setup=サーバー全量 / stored=ローカルキャッシュ(既定) / normal=差分",
    )
    ap.add_argument("--only-races", action="store_true")
    ap.add_argument("--only-masters", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fy, ty = args.from_year, args.to_year
    print(f"=== 過去データ取得 {fy}〜{ty}年 mode={args.mode} ===")

    ok = True
    if not args.only_races:
        # マスタ（DIFN/BLOD）を先に取得して SE の FK を満たす。
        ok &= step_masters(args.mode, args.dry_run)
    if not args.only_masters:
        ok &= step_races(fy, ty, args.mode, args.dry_run)
        ok &= step_wood(fy, ty, args.dry_run)

    print(f"=== 取得完了: {'全成功' if ok else '一部失敗あり（ログ参照）'} ===")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
