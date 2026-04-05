"""
年間一括シミュレーションバッチ

指定年の全レースを simulate_pipeline で一括処理し、
モデル年間成績を再集計して web 用 JSON を書き出す。

使用例:
  python -m src.simulate_year --year 2024
  python -m src.simulate_year --year 2024 --force    # 既存予想を上書き
  python -m src.simulate_year --year 2024 --workers 1  # シングルスレッド
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)

# プロジェクトルートを sys.path に追加
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db, refresh_model_performance
from src.main_pipeline import simulate_pipeline


# ── レース一覧取得 ────────────────────────────────────────────────

def _get_race_ids(conn, year: int) -> list[str]:
    """
    指定年の race_results が存在するレース ID を日付昇順で返す。
    race_results のない race_id はシミュレーション不能なので除外する。
    """
    rows = conn.execute(
        """
        SELECT DISTINCT r.race_id
        FROM races r
        JOIN race_results rr ON r.race_id = rr.race_id
        WHERE substr(r.date, 1, 4) = ?
        ORDER BY r.date, r.race_id
        """,
        (str(year),),
    ).fetchall()
    return [r[0] for r in rows]


def _get_simulated_ids(conn, year: int) -> set[str]:
    """
    すでに predictions テーブルに予想が存在するレース ID を返す。
    [SIMULATE] notes を持つものだけを対象にする。
    """
    rows = conn.execute(
        """
        SELECT DISTINCT race_id
        FROM predictions
        WHERE substr(race_id, 1, 4) = ?
          AND notes LIKE '[SIMULATE]%'
        """,
        (str(year),),
    ).fetchall()
    return {r[0] for r in rows}


# ── 成績再集計 ────────────────────────────────────────────────────

def _refresh_performance(conn, year: int) -> None:
    """卍・本命モデルの年間成績を再集計する。"""
    for model in ("卍", "本命"):
        refresh_model_performance(conn, model_type=model, year=year)
        # 月別も再集計（1〜12月）
        for month in range(1, 13):
            refresh_model_performance(conn, model_type=model, year=year, month=month)
    logger.info("成績再集計完了: %d年", year)


# ── Web データ書き出し ────────────────────────────────────────────

def _export_web_data() -> None:
    """web/generate_data.py と同等のエクスポート処理を実行する。"""
    import importlib.util

    gen_path = _ROOT / "web" / "generate_data.py"
    if not gen_path.exists():
        logger.warning("generate_data.py が見つかりません: %s", gen_path)
        return

    # 引数なしで main() を呼ぶ（sys.argv を一時退避）
    _orig_argv = sys.argv[:]
    sys.argv = [str(gen_path)]
    try:
        spec = importlib.util.spec_from_file_location("generate_data", gen_path)
        mod  = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        mod.main()
        logger.info("Web データ書き出し完了")
    except Exception as exc:
        logger.error("Web データ書き出し失敗: %s", exc)
    finally:
        sys.argv = _orig_argv


# ── メインバッチ ──────────────────────────────────────────────────

def simulate_year(
    year: int,
    *,
    force: bool = False,
) -> dict[str, int]:
    """
    指定年の全レースを一括シミュレーションする。

    Args:
        year:  対象年（例: 2024）
        force: True のとき既存予想をスキップせず上書きする

    Returns:
        {"total": 総レース数, "simulated": 実行数,
         "skipped": スキップ数, "errors": エラー数}
    """
    conn = init_db()

    race_ids      = _get_race_ids(conn, year)
    simulated_ids = set() if force else _get_simulated_ids(conn, year)
    conn.close()   # simulate_pipeline が内部で connect するため一旦閉じる

    total   = len(race_ids)
    stats   = {"total": total, "simulated": 0, "skipped": 0, "errors": 0}
    errors: list[str] = []

    logger.info(
        "%d年 対象レース: %d件 / 既存スキップ: %d件",
        year, total, len(simulated_ids),
    )

    bar = tqdm(race_ids, desc=f"{year}年 シミュレーション", unit="race", dynamic_ncols=True)

    for race_id in bar:
        # ── スキップ判定 ──────────────────────────────────────────
        if race_id in simulated_ids:
            stats["skipped"] += 1
            bar.set_postfix(
                sim=stats["simulated"],
                skip=stats["skipped"],
                err=stats["errors"],
                refresh=False,
            )
            continue

        # ── シミュレーション実行 ──────────────────────────────────
        try:
            result = simulate_pipeline(race_id)
            if "error" in result:
                logger.warning("simulate エラー race_id=%s: %s", race_id, result["error"])
                stats["errors"] += 1
                errors.append(f"{race_id}: {result['error']}")
            else:
                stats["simulated"] += 1
        except Exception as exc:
            logger.error("simulate 例外 race_id=%s: %s", race_id, exc, exc_info=True)
            stats["errors"] += 1
            errors.append(f"{race_id}: {exc}")

        bar.set_postfix(
            sim=stats["simulated"],
            skip=stats["skipped"],
            err=stats["errors"],
            refresh=False,
        )

    bar.close()

    # ── 成績再集計 ────────────────────────────────────────────────
    if stats["simulated"] > 0 or force:
        logger.info("モデル成績を再集計中...")
        conn = init_db()
        _refresh_performance(conn, year)
        conn.close()
    else:
        logger.info("新規シミュレーションなし — 成績再集計をスキップ")

    # ── Web データ書き出し ────────────────────────────────────────
    logger.info("Web データを書き出し中...")
    _export_web_data()

    # ── 結果サマリー ──────────────────────────────────────────────
    logger.info(
        "%d年 バッチ完了: 実行=%d スキップ=%d エラー=%d",
        year, stats["simulated"], stats["skipped"], stats["errors"],
    )
    if errors:
        logger.warning("エラー一覧（最初の10件）:")
        for e in errors[:10]:
            logger.warning("  %s", e)

    return stats


# ── CLI ──────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="年間一括シミュレーションバッチ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.simulate_year --year 2024          # 2024年を一括シミュレーション
  python -m src.simulate_year --year 2024 --force  # 既存予想も上書き
""",
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="シミュレーション対象年（例: 2024）",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="既存の [SIMULATE] 予想を上書きする",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # tqdm と logging の干渉を抑制
    logging.getLogger("src.scraper").setLevel(logging.WARNING)
    logging.getLogger("src.ml").setLevel(logging.WARNING)
    logging.getLogger("src.database").setLevel(logging.WARNING)
    logging.getLogger("src.main_pipeline").setLevel(logging.WARNING)

    args = _parse_args()
    stats = simulate_year(args.year, force=args.force)

    print(f"\n{'='*50}")
    print(f"  {args.year}年 シミュレーション結果")
    print(f"{'='*50}")
    print(f"  対象レース数  : {stats['total']:5d}")
    print(f"  実行          : {stats['simulated']:5d}")
    print(f"  スキップ      : {stats['skipped']:5d}")
    print(f"  エラー        : {stats['errors']:5d}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
