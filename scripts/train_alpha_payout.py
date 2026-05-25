"""
Alpha-Payout 本番モデル訓練スクリプト
=====================================
指定年（デフォルト: 2024 + 2025）のデータで AlphaPayoutModel を訓練し、
data/models/alpha_payout/alpha_payout_model.pkl に保存する。

月次で実行することで EV 閾値を最新データに追従させる。

Usage:
    py scripts/train_alpha_payout.py
    py scripts/train_alpha_payout.py --trials 50
    py scripts/train_alpha_payout.py --years 2024 2025 2026
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from src.ml.alpha_payout_model import AlphaPayoutModel, _MODEL_PATH

_DB_PATH     = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"


def _check_year_data(conn: sqlite3.Connection, year: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM race_results rr "
        "JOIN races r ON rr.race_id = r.race_id "
        "WHERE strftime('%Y', r.date) = ?",
        (str(year),),
    ).fetchone()
    return row[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Alpha-Payout 本番モデル訓練")
    parser.add_argument("--trials", type=int, default=30, help="Optuna トライアル数")
    parser.add_argument(
        "--years", nargs="+", type=int, default=[2024, 2025],
        help="学習データ年 (デフォルト: 2024 2025)",
    )
    args = parser.parse_args()

    research_db: Path | None = None
    if _RESEARCH_DB.exists():
        research_db = _RESEARCH_DB
        print(f"[INFO] research DB 使用: {research_db}", flush=True)
    else:
        print("[INFO] research DB なし — JVLink win_odds のみ使用", flush=True)

    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # 年次データ確認
    print("[INFO] 学習年次データ確認:", flush=True)
    for y in args.years:
        n = _check_year_data(conn, y)
        if n == 0:
            print(f"  {y}: データなし ⚠️ — スキップします", flush=True)
        else:
            print(f"  {y}: {n:,}行 ✓", flush=True)

    valid_years = [y for y in args.years if _check_year_data(conn, y) > 0]
    if not valid_years:
        print("[ERROR] 有効な学習データがありません。", flush=True)
        conn.close()
        sys.exit(1)

    print(f"\n[INFO] 学習開始: years={valid_years}  trials={args.trials}", flush=True)

    model = AlphaPayoutModel()
    df = model.load_training_data(conn, valid_years, research_db)
    conn.close()

    if len(df) < 500:
        print(f"[ERROR] 学習データ不足: {len(df)}行 (最低500行必要)", flush=True)
        sys.exit(1)

    metrics = model.train(df, n_optuna_trials=args.trials)

    model.save(_MODEL_PATH)

    print(f"\n{'='*60}", flush=True)
    print(f"  Alpha-Payout 本番モデル 訓練完了", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  学習年: {valid_years}", flush=True)
    print(f"  学習行: {metrics['n_train']:,}", flush=True)
    print(f"  EV 閾値: pred_ev > {metrics['threshold']:.2f}", flush=True)
    print(f"  検証 ROI: {metrics['val_roi']:.1f}%", flush=True)
    print(f"  訓練 ROI: {metrics['train_roi']:.1f}%", flush=True)
    print(f"  保存先: {_MODEL_PATH}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"\n  prerace_pipeline が自動的にこのモデルを使用します。", flush=True)
    print(f"  月次再訓練推奨: py scripts/train_alpha_payout.py", flush=True)


if __name__ == "__main__":
    main()
