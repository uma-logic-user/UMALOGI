"""
予想結果照合バッチ（単勝特化版）

predictions テーブルの単勝予想と race_results の実結果を突合し、
的中・払戻を prediction_results に保存する。

照合アルゴリズム:
  1. prediction_horses.horse_name に実際の馬名が入っている場合 → 馬名で直接照合
  2. horse_name がモデル種別名 ('本命' / '卍') の場合 → notes の odds=X.X を
     パースして win_odds が一致する馬を特定し照合

払戻計算:
  payout = race_results.win_odds * recommended_bet   (的中時)
  payout = 0                                          (外れ時)

使用例:
  python -m src.ml.reconcile
  python -m src.ml.reconcile --year 2024
  python -m src.ml.reconcile --dry-run   # DB 書き込みなし
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import init_db, record_prediction_result, refresh_model_performance

# モデル種別名（horse_name がこの値の場合は notes で補完）
_MODEL_TYPE_NAMES = {"本命", "卍"}

# notes から odds を抽出する正規表現
_ODDS_RE = re.compile(r"odds=([\d.]+)")


# ── 内部ユーティリティ ────────────────────────────────────────────

def _parse_odds_from_notes(notes: str | None) -> float | None:
    """notes 文字列から 'odds=X.X' を抽出して float で返す。"""
    if not notes:
        return None
    m = _ODDS_RE.search(notes)
    return float(m.group(1)) if m else None


def _find_winner(conn: sqlite3.Connection, race_id: str) -> dict | None:
    """
    race_results から rank=1 の馬情報を取得する。

    Returns:
        {"horse_name": str, "win_odds": float} または None
    """
    row = conn.execute(
        """
        SELECT horse_name, win_odds
        FROM race_results
        WHERE race_id = ? AND rank = 1
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if row is None:
        return None
    return {"horse_name": row[0], "win_odds": row[1]}


def _find_horse_by_odds(
    conn: sqlite3.Connection,
    race_id: str,
    target_odds: float,
) -> str | None:
    """
    race_results から win_odds が最も近い馬名を返す。
    完全一致を優先し、±0.05 以内を許容する。
    """
    rows = conn.execute(
        """
        SELECT horse_name, win_odds
        FROM race_results
        WHERE race_id = ? AND win_odds IS NOT NULL
        ORDER BY ABS(win_odds - ?) ASC
        LIMIT 1
        """,
        (race_id, target_odds),
    ).fetchall()
    if not rows:
        return None
    horse_name, found_odds = rows[0]
    # 0.05 以内の誤差を許容（浮動小数点誤差・更新差を吸収）
    if abs(found_odds - target_odds) <= 0.05:
        return horse_name
    return None


# ── 単勝予想1件を照合 ─────────────────────────────────────────────

def _reconcile_tansho(
    conn: sqlite3.Connection,
    prediction_id: int,
    race_id: str,
    recommended_bet: float,
    notes: str | None,
    ph_horse_name: str | None,
    dry_run: bool,
) -> str:
    """
    単勝1予想を照合して prediction_results に保存する。

    Returns:
        "hit" | "miss" | "skip" | "no_result"
    """
    # ── 勝ち馬を特定 ─────────────────────────────────────────────
    winner = _find_winner(conn, race_id)
    if winner is None:
        logger.debug("race_results に rank=1 なし: race_id=%s", race_id)
        return "no_result"

    winner_name   = winner["horse_name"]
    winner_odds   = winner["win_odds"] or 0.0

    # ── 予想馬を特定 ─────────────────────────────────────────────
    predicted_name: str | None = None

    if ph_horse_name and ph_horse_name not in _MODEL_TYPE_NAMES:
        # 実際の馬名が保存されている（修正後のデータ）
        predicted_name = ph_horse_name
    else:
        # 旧データ: notes から odds でマッピング
        target_odds = _parse_odds_from_notes(notes)
        if target_odds is not None:
            predicted_name = _find_horse_by_odds(conn, race_id, target_odds)

    if predicted_name is None:
        logger.warning(
            "予想馬を特定できませんでした: prediction_id=%d race_id=%s notes=%r",
            prediction_id, race_id, notes,
        )
        return "skip"

    # ── 的中判定 ─────────────────────────────────────────────────
    is_hit  = (predicted_name == winner_name)
    payout  = (winner_odds * recommended_bet) if is_hit else 0.0

    logger.debug(
        "照合: prediction_id=%d race_id=%s 予想=%s 勝馬=%s is_hit=%s payout=%.0f",
        prediction_id, race_id, predicted_name, winner_name, is_hit, payout,
    )

    if not dry_run:
        record_prediction_result(
            conn,
            prediction_id=prediction_id,
            is_hit=is_hit,
            payout=payout,
            recommended_bet=recommended_bet,
        )

    return "hit" if is_hit else "miss"


# ── メイン照合処理 ────────────────────────────────────────────────

def reconcile(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    単勝予想を一括照合して prediction_results に保存する。

    Args:
        conn:    DB コネクション
        year:    対象年（None = 全期間）
        dry_run: True なら DB 書き込みをしない

    Returns:
        {"total": 処理件数, "hit": 的中数, "miss": 外れ数,
         "skip": スキップ数, "no_result": 結果なし数}
    """
    # ── 未処理の単勝予想を取得 ────────────────────────────────────
    year_filter = "AND substr(p.race_id, 1, 4) = ?" if year else ""
    year_param  = [str(year)] if year else []

    rows = conn.execute(
        f"""
        SELECT
            p.id          AS prediction_id,
            p.race_id,
            p.recommended_bet,
            p.notes,
            ph.horse_name AS ph_horse_name
        FROM predictions p
        -- prediction_horses の predicted_rank=1 の行（単勝は1頭）
        LEFT JOIN prediction_horses ph
               ON ph.prediction_id = p.id
              AND ph.predicted_rank = 1
        -- 未処理（prediction_results にまだ行がない）
        WHERE p.bet_type = '単勝'
          AND NOT EXISTS (
              SELECT 1 FROM prediction_results pr
              WHERE pr.prediction_id = p.id
          )
        {year_filter}
        ORDER BY p.race_id, p.id
        """,
        year_param,
    ).fetchall()

    logger.info("単勝予想 照合対象: %d 件 (year=%s)", len(rows), year or "all")

    stats: dict[str, int] = {
        "total": len(rows),
        "hit":  0,
        "miss": 0,
        "skip": 0,
        "no_result": 0,
    }

    for prediction_id, race_id, recommended_bet, notes, ph_horse_name in rows:
        status = _reconcile_tansho(
            conn,
            prediction_id=prediction_id,
            race_id=race_id,
            recommended_bet=recommended_bet or 100.0,
            notes=notes,
            ph_horse_name=ph_horse_name,
            dry_run=dry_run,
        )
        stats[status] += 1

    # ── モデル成績再集計 ──────────────────────────────────────────
    if not dry_run and (stats["hit"] + stats["miss"]) > 0:
        years_to_refresh: set[int] = set()
        if year:
            years_to_refresh.add(year)
        else:
            # 処理したレースの年を全て再集計
            y_rows = conn.execute(
                """
                SELECT DISTINCT substr(race_id, 1, 4) FROM predictions
                WHERE bet_type = '単勝'
                """
            ).fetchall()
            years_to_refresh = {int(r[0]) for r in y_rows if r[0].isdigit()}

        for y in sorted(years_to_refresh):
            for model in ("卍", "本命"):
                refresh_model_performance(conn, model_type=model, year=y)
        logger.info("モデル成績再集計完了: %s年分", sorted(years_to_refresh))

    return stats


# ── CLI ──────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="単勝予想の的中照合バッチ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.ml.reconcile                  # 全期間の未処理単勝を照合
  python -m src.ml.reconcile --year 2024      # 2024年分のみ
  python -m src.ml.reconcile --dry-run        # 書き込みなしで確認
""",
    )
    parser.add_argument("--year",    type=int, help="対象年（省略時=全期間）")
    parser.add_argument("--dry-run", action="store_true", help="DB 書き込みなし（確認用）")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()

    conn = init_db()
    stats = reconcile(conn, year=args.year, dry_run=args.dry_run)
    conn.close()

    hit_rate = (
        stats["hit"] / (stats["hit"] + stats["miss"]) * 100
        if (stats["hit"] + stats["miss"]) > 0
        else 0.0
    )
    invested = 0.0
    payout   = 0.0
    # 簡易ROI表示（集計済み値はここでは使えないため概算）

    mode = "[DRY-RUN]" if args.dry_run else ""
    print(f"\n{'='*55} {mode}")
    print(f"  単勝照合結果 (year={args.year or 'all'})")
    print(f"{'='*55}")
    print(f"  照合対象     : {stats['total']:6d} 件")
    print(f"  的中         : {stats['hit']:6d} 件")
    print(f"  外れ         : {stats['miss']:6d} 件")
    print(f"  特定不能スキップ: {stats['skip']:6d} 件")
    print(f"  結果データなし : {stats['no_result']:6d} 件")
    print(f"  的中率       : {hit_rate:6.1f} %")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
