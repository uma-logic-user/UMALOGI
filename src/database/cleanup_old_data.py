"""
旧予測データクリーンアップスクリプト

prediction_horses.horse_name が "本命" または "卍"（モデル種別名）のみで
構成されている predictions レコードを削除する。

これらは以前のバグ（馬名の代わりにモデル種別名を保存していた）による
不正データであり、照合（reconcile）で使用不能な「ゾンビレコード」。

削除対象の定義:
  ・prediction_horses に 1 行以上存在する
  ・prediction_horses の全行で horse_name IN ('本命', '卍')

カスケード削除:
  predictions を削除すると ON DELETE CASCADE により
  prediction_horses / prediction_results も自動削除される。

削除後: VACUUM を実行してファイルサイズを最適化する。

使用例:
  python -m src.database.cleanup_old_data              # 対話確認あり
  python -m src.database.cleanup_old_data --dry-run    # 削除せず件数確認のみ
  python -m src.database.cleanup_old_data --force      # 確認なしで即削除
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db, get_db_path

# 削除対象となるモデル種別名
_BAD_NAMES = ("本命", "卍")


# ── 対象レコード集計 ──────────────────────────────────────────────

def _count_targets(conn) -> dict[str, int]:
    """
    削除対象の件数を集計して返す。

    Returns:
        {
          "predictions":        削除対象 predictions 数,
          "prediction_horses":  連動削除される prediction_horses 数,
          "prediction_results": 連動削除される prediction_results 数,
        }
    """
    # 削除対象の predictions.id を特定するサブクエリ
    _target_sql = """
        SELECT p.id
        FROM predictions p
        WHERE EXISTS (
            SELECT 1 FROM prediction_horses ph
            WHERE ph.prediction_id = p.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM prediction_horses ph
            WHERE ph.prediction_id = p.id
              AND ph.horse_name NOT IN ('本命', '卍')
        )
    """

    pred_cnt = conn.execute(
        f"SELECT COUNT(*) FROM ({_target_sql})"
    ).fetchone()[0]

    ph_cnt = conn.execute(
        f"""
        SELECT COUNT(*) FROM prediction_horses
        WHERE prediction_id IN ({_target_sql})
        """
    ).fetchone()[0]

    pr_cnt = conn.execute(
        f"""
        SELECT COUNT(*) FROM prediction_results
        WHERE prediction_id IN ({_target_sql})
        """
    ).fetchone()[0]

    return {
        "predictions":        pred_cnt,
        "prediction_horses":  ph_cnt,
        "prediction_results": pr_cnt,
    }


def _count_all(conn) -> dict[str, int]:
    """削除前の全テーブル件数を返す。"""
    return {
        "predictions":        conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0],
        "prediction_horses":  conn.execute("SELECT COUNT(*) FROM prediction_horses").fetchone()[0],
        "prediction_results": conn.execute("SELECT COUNT(*) FROM prediction_results").fetchone()[0],
    }


# ── 削除処理 ─────────────────────────────────────────────────────

def _delete_targets(conn) -> int:
    """
    対象 predictions を削除し、削除件数を返す。
    CASCADE により prediction_horses / prediction_results も自動削除される。
    """
    with conn:
        cur = conn.execute(
            """
            DELETE FROM predictions
            WHERE EXISTS (
                SELECT 1 FROM prediction_horses ph
                WHERE ph.prediction_id = predictions.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM prediction_horses ph
                WHERE ph.prediction_id = predictions.id
                  AND ph.horse_name NOT IN ('本命', '卍')
            )
            """
        )
    return cur.rowcount


def _run_vacuum(conn) -> tuple[int, int]:
    """
    VACUUM を実行してファイルサイズを最適化する。

    Returns:
        (before_bytes, after_bytes)
    """
    db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])

    # VACUUM は autocommit モードで実行する必要がある
    conn.execute("COMMIT")          # 念のため未コミットトランザクションを閉じる
    conn.isolation_level = None     # autocommit に切り替え

    before = db_path.stat().st_size if db_path.exists() else 0

    logger.info("VACUUM 実行中...")
    t0 = time.monotonic()
    conn.execute("VACUUM")
    elapsed = time.monotonic() - t0

    after = db_path.stat().st_size if db_path.exists() else 0

    conn.isolation_level = ""       # デフォルトに戻す
    logger.info("VACUUM 完了: %.1f 秒", elapsed)

    return before, after


# ── メイン ───────────────────────────────────────────────────────

def cleanup(*, dry_run: bool = False, force: bool = False) -> dict:
    """
    旧予測データを削除して DB を最適化する。

    Args:
        dry_run: True なら削除せず件数確認のみ
        force:   True なら確認プロンプトをスキップ

    Returns:
        実行結果を格納した dict
    """
    conn = init_db()

    # ── 削除前の全件数を記録 ──────────────────────────────────────
    before_all = _count_all(conn)
    targets    = _count_targets(conn)

    print()
    print(f"{'='*58}")
    print("  旧予測データ クリーンアップ")
    print(f"{'='*58}")
    print(f"  現在の件数:")
    print(f"    predictions       : {before_all['predictions']:8,} 件")
    print(f"    prediction_horses : {before_all['prediction_horses']:8,} 件")
    print(f"    prediction_results: {before_all['prediction_results']:8,} 件")
    print()
    print(f"  削除対象（horse_name が '本命'/'卍' のみの予想）:")
    print(f"    predictions       : {targets['predictions']:8,} 件")
    print(f"    prediction_horses : {targets['prediction_horses']:8,} 件 (CASCADE)")
    print(f"    prediction_results: {targets['prediction_results']:8,} 件 (CASCADE)")
    print(f"{'='*58}")

    if targets["predictions"] == 0:
        print("  削除対象がありません。処理を終了します。")
        conn.close()
        return {"deleted": 0, "vacuumed": False}

    if dry_run:
        print("  [DRY-RUN] 実際の削除は行いません。")
        conn.close()
        return {"deleted": 0, "vacuumed": False, "targets": targets}

    # ── 確認プロンプト ────────────────────────────────────────────
    if not force:
        print()
        answer = input(
            f"  上記 {targets['predictions']:,} 件の predictions を削除しますか？ [y/N]: "
        ).strip().lower()
        if answer not in ("y", "yes"):
            print("  キャンセルしました。")
            conn.close()
            return {"deleted": 0, "vacuumed": False}

    # ── 削除実行 ──────────────────────────────────────────────────
    print()
    print("  削除中...")
    deleted = _delete_targets(conn)
    logger.info("削除完了: %d 件", deleted)

    after_all = _count_all(conn)
    print(f"  削除後の件数:")
    print(f"    predictions       : {after_all['predictions']:8,} 件 (-{before_all['predictions'] - after_all['predictions']:,})")
    print(f"    prediction_horses : {after_all['prediction_horses']:8,} 件 (-{before_all['prediction_horses'] - after_all['prediction_horses']:,})")
    print(f"    prediction_results: {after_all['prediction_results']:8,} 件 (-{before_all['prediction_results'] - after_all['prediction_results']:,})")

    # ── VACUUM ────────────────────────────────────────────────────
    print()
    print("  VACUUM でファイルを最適化中...")
    try:
        before_bytes, after_bytes = _run_vacuum(conn)
        saved_mb = (before_bytes - after_bytes) / 1024 / 1024
        print(f"  ファイルサイズ: {before_bytes/1024/1024:.1f} MB → {after_bytes/1024/1024:.1f} MB")
        print(f"  削減サイズ    : {saved_mb:.1f} MB")
        vacuumed = True
    except Exception as exc:
        logger.error("VACUUM 失敗: %s", exc)
        print(f"  VACUUM 失敗（削除自体は成功）: {exc}")
        before_bytes = after_bytes = 0
        vacuumed = False

    print(f"{'='*58}")
    print("  クリーンアップ完了")
    print(f"{'='*58}")

    conn.close()
    return {
        "deleted":      deleted,
        "vacuumed":     vacuumed,
        "before_bytes": before_bytes,
        "after_bytes":  after_bytes,
        "targets":      targets,
    }


# ── CLI ──────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="旧予測データ（horse_name が '本命'/'卍' のみ）を削除して DB を最適化する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.database.cleanup_old_data              # 確認後に削除
  python -m src.database.cleanup_old_data --dry-run    # 件数確認のみ（削除なし）
  python -m src.database.cleanup_old_data --force      # 確認スキップで即削除
""",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="削除せず対象件数を表示するだけ",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="確認プロンプトをスキップして即削除",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()
    cleanup(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
