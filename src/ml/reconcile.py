"""
予想結果照合バッチ（全券種対応版）

predictions / prediction_horses テーブルの予想と
race_payouts テーブルの実際の払戻を突合し、
的中・払戻を prediction_results に保存する。

対象馬券種:
  単勝 / 複勝 / 馬連 / ワイド / 馬単 / 三連複 / 三連単
  ※ WIN5 は別フェーズ対応

【照合アルゴリズム】
  馬番（ゲート番号）ではなく馬名ベースで照合する。
  理由:
    ・simulate 予想の combination_json は人気順インデックス（horse_number=1番人気）を
      格納しており race_payouts の実際の馬番と不一致になる
    ・entries テーブルが未取得の環境では馬番へのマッピングが不可能

  1. prediction_horses.horse_name から予想馬名リストを取得
  2. race_results.rank で各馬の着順を確認して的中判定
  3. race_payouts から払戻額を取得（馬番不要で金額は取得可能）

払戻計算:
  n_combos     = combination_json 内の組み合わせ数（不明なら bet_type の既定値）
  bet_per_combo = recommended_bet / n_combos
  payout        = (race_payouts.payout / 100) * bet_per_combo  (的中組み合わせのみ合算)

  複勝・ワイドは複数払戻あり → 全払戻の平均で近似（的中数が多いほど正確）

使用例:
  python -m src.ml.reconcile
  python -m src.ml.reconcile --year 2024
  python -m src.ml.reconcile --dry-run
"""

from __future__ import annotations

import argparse
import itertools
import json
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

# notes から odds を抽出する正規表現（旧データ互換）
_ODDS_RE = re.compile(r"odds=([\d.]+)")

# モデル種別名（horse_name がこの値の場合は馬名として使えない）
_MODEL_TYPE_NAMES = {"本命", "卍"}

# 三連単ボックス3頭の組み合わせ数
_SANTAN_BOX_3 = len(list(itertools.permutations(range(3))))  # 6


# ── combination_json パーサー ────────────────────────────────────

def _parse_combinations(combination_json: str | None) -> list[list[int]] | None:
    """
    combination_json を解析・正規化して list[list[int]] を返す。

    対応フォーマット:
      フラット    "[4]"              → [[4]]
      ネスト      "[[4]]"            → [[4]]
      多組合せ    "[[4,1,2],[1,4,2]]" → [[4,1,2],[1,4,2]]
      型不定      ["4","1"]          → [[4],[1]] (int キャスト)
      NULL/空     None / "" / "[]"   → None
    """
    if not combination_json:
        return None
    try:
        raw = json.loads(combination_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not raw:
        return None

    # フラット配列 [4] → ネスト [[4]] に正規化
    try:
        if not isinstance(raw[0], (list, tuple)):
            raw = [raw]
    except (IndexError, TypeError):
        return None

    # 各要素を list[int] にキャスト
    result: list[list[int]] = []
    for combo in raw:
        try:
            result.append([int(n) for n in combo])
        except (TypeError, ValueError):
            continue

    return result or None


def _n_combos_from_json(combination_json: str | None, bet_type: str) -> int:
    """
    combination_json または bet_type から組み合わせ数を推定する。

    三連単ボックス3頭 → 6、それ以外の多くは 1。
    複勝は prediction_horses の馬名数に依存するため 0 を返す（呼び出し側で対応）。
    """
    combos = _parse_combinations(combination_json)
    if combos is not None:
        return len(combos)

    # combination_json なし → bet_type の既定値
    defaults = {
        "単勝": 1,
        "複勝": 0,       # 複数馬名ぶん → 呼び出し側で決定
        "馬連": 1,
        "ワイド": 1,
        "馬単": 1,
        "三連複": 1,
        "三連単": _SANTAN_BOX_3,
    }
    return defaults.get(bet_type, 1)


# ── DB 参照ユーティリティ ─────────────────────────────────────────

def _get_payout_map(
    conn: sqlite3.Connection,
    race_id: str,
    bet_type: str,
) -> dict[str, int]:
    """race_payouts から {combination: payout} マップを返す（ゲート番号→払戻）。"""
    rows = conn.execute(
        "SELECT combination, payout FROM race_payouts WHERE race_id = ? AND bet_type = ?",
        (race_id, bet_type),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _get_refund_set(conn: sqlite3.Connection, race_id: str) -> set[str]:
    """
    返還対象の馬名セットを返す。

    race_payouts の bet_type='返還' エントリから馬番を取得し、
    race_results で馬名に変換する。
    返還なしの場合は空セットを返す（クラッシュしない）。
    """
    try:
        refund_rows = conn.execute(
            "SELECT combination FROM race_payouts WHERE race_id = ? AND bet_type = '返還'",
            (race_id,),
        ).fetchall()
    except Exception:
        return set()

    if not refund_rows:
        return set()

    refund_nums: set[int] = set()
    for (comb,) in refund_rows:
        try:
            refund_nums.add(int(comb))
        except (ValueError, TypeError):
            pass

    if not refund_nums:
        return set()

    # 馬番 → 馬名
    result_rows = conn.execute(
        "SELECT horse_name, horse_number FROM race_results WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    return {name for name, num in result_rows if num is not None and int(num) in refund_nums}


def _find_winner(conn: sqlite3.Connection, race_id: str) -> dict | None:
    """race_results から rank=1 の馬情報を返す。"""
    row = conn.execute(
        "SELECT horse_name, win_odds FROM race_results WHERE race_id = ? AND rank = 1 LIMIT 1",
        (race_id,),
    ).fetchone()
    return {"horse_name": row[0], "win_odds": row[1] or 0.0} if row else None


def _find_horse_by_odds(
    conn: sqlite3.Connection,
    race_id: str,
    target_odds: float,
) -> str | None:
    """race_results から win_odds が最も近い馬名を返す（旧データ互換）。"""
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
    return horse_name if abs(found_odds - target_odds) <= 0.05 else None


def _parse_odds_from_notes(notes: str | None) -> float | None:
    """notes 文字列から 'odds=X.X' を抽出して float で返す（旧データ互換）。"""
    if not notes:
        return None
    m = _ODDS_RE.search(notes)
    return float(m.group(1)) if m else None


# ── 馬名ベース照合 ─────────────────────────────────────────────────

def _get_predicted_horse_names(
    conn: sqlite3.Connection,
    prediction_id: int,
    race_id: str,
    bet_type: str,
    notes: str | None,
    ph_horse_name: str | None,
) -> list[str] | None:
    """
    prediction_horses から予想馬名リストを取得する（predicted_rank 昇順）。

    ・MODEL_TYPE_NAMES（"本命"/"卍"）は馬名として使えないため除外する。
    ・全て MODEL_TYPE_NAMES だった旧データの場合:
        単勝のみ notes の odds から馬名を解決する。
        それ以外は None を返す（照合不能）。
    """
    rows = conn.execute(
        """
        SELECT horse_name
        FROM prediction_horses
        WHERE prediction_id = ?
        ORDER BY predicted_rank NULLS LAST, id
        """,
        (prediction_id,),
    ).fetchall()

    names = [r[0] for r in rows if r[0] and r[0] not in _MODEL_TYPE_NAMES]
    if names:
        return names

    # 全て MODEL_TYPE_NAMES （旧データ）
    if bet_type == "単勝":
        name = ph_horse_name
        if not name or name in _MODEL_TYPE_NAMES:
            target_odds = _parse_odds_from_notes(notes)
            if target_odds is not None:
                name = _find_horse_by_odds(conn, race_id, target_odds)
        if name and name not in _MODEL_TYPE_NAMES:
            return [name]

    return None


def _get_race_rank_map(
    conn: sqlite3.Connection,
    race_id: str,
) -> tuple[dict[str, int | None], set[str]] | None:
    """
    race_results から {horse_name: rank} マップと top3 馬名セットを返す。
    race_results が 0 件の場合は None を返す。
    """
    rows = conn.execute(
        "SELECT horse_name, rank FROM race_results WHERE race_id = ? ORDER BY rank NULLS LAST",
        (race_id,),
    ).fetchall()
    if not rows:
        return None
    rank_map = {r[0]: r[1] for r in rows}
    top3 = {r[0] for r in rows if r[1] is not None and 1 <= r[1] <= 3}
    return rank_map, top3


def _payout_for_hit(payout_map: dict[str, int], n_hits: int = 1) -> float:
    """
    payout_map の払戻額を返す（ゲート番号なし版）。

    ・1件のみ: その払戻額を使用
    ・複数件 (複勝/ワイド等): 全払戻の平均を使用（n_hits 分を合算）
    """
    if not payout_map:
        return 0.0
    values = list(payout_map.values())
    avg = sum(values) / len(values)
    return avg * n_hits


def _reconcile_by_names(
    conn: sqlite3.Connection,
    prediction_id: int,
    race_id: str,
    bet_type: str,
    recommended_bet: float,
    notes: str | None,
    ph_horse_name: str | None,
    payout_map: dict[str, int],
    n_combos: int,
    dry_run: bool,
) -> str:
    """
    馬名ベースで的中判定し払戻を算出する。

    ゲート番号（馬番）を使わないため simulate / prerace 両対応。
    """
    # ── 予想馬名リスト取得 ────────────────────────────────────────
    horse_names = _get_predicted_horse_names(
        conn, prediction_id, race_id, bet_type, notes, ph_horse_name
    )
    if not horse_names:
        return "skip"

    # ── race_results から着順情報を取得 ──────────────────────────
    result = _get_race_rank_map(conn, race_id)
    if result is None:
        return "no_result"
    rank_map, top3_names = result

    # ── bet_type 別の的中判定と払戻計算 ──────────────────────────
    is_hit = False
    total_payout = 0.0
    bet_per_combo = recommended_bet / n_combos if n_combos > 0 else recommended_bet

    if bet_type == "単勝":
        # 予想馬が rank=1 か
        is_hit = (rank_map.get(horse_names[0]) == 1)
        if is_hit:
            total_payout = (_payout_for_hit(payout_map) / 100.0) * recommended_bet

    elif bet_type == "複勝":
        # 各予想馬が top3 に入ったか（1頭ずつ独立判定）
        placed_count = sum(
            1 for n in horse_names if rank_map.get(n) is not None and 1 <= rank_map[n] <= 3
        )
        if placed_count > 0:
            is_hit = True
            total_payout = (_payout_for_hit(payout_map, placed_count) / 100.0) * bet_per_combo

    elif bet_type in ("馬連", "枠連"):
        # 予想2頭が rank 1,2 を占めるか（順不同・同着対応）
        # 同着1着（両方rank=1）も的中扱い。同着2着（rank=1+rank=2の組合せ）も的中。
        # rank=2 同士の組み合わせは不可。
        if len(horse_names) >= 2:
            r0 = rank_map.get(horse_names[0])
            r1 = rank_map.get(horse_names[1])
            is_hit = (
                r0 in {1, 2} and r1 in {1, 2}
                and not (r0 == 2 and r1 == 2)
            )
            if is_hit:
                total_payout = (_payout_for_hit(payout_map) / 100.0) * recommended_bet

    elif bet_type == "ワイド":
        # 予想2頭が両方 top3 に入るか
        if len(horse_names) >= 2:
            is_hit = all(n in top3_names for n in horse_names[:2])
            if is_hit:
                total_payout = (_payout_for_hit(payout_map) / 100.0) * recommended_bet

    elif bet_type == "馬単":
        # 予想1着→2着が rank 1→2 か（順序あり）
        if len(horse_names) >= 2:
            is_hit = (rank_map.get(horse_names[0]) == 1 and rank_map.get(horse_names[1]) == 2)
            if is_hit:
                total_payout = (_payout_for_hit(payout_map) / 100.0) * recommended_bet

    elif bet_type == "三連複":
        # 予想3頭が top3 を占めるか（順不同）
        if len(horse_names) >= 3:
            pred_set = set(horse_names[:3])
            is_hit = (len(pred_set) == 3 and pred_set.issubset(top3_names))
            if is_hit:
                total_payout = (_payout_for_hit(payout_map) / 100.0) * recommended_bet

    elif bet_type == "三連単":
        # 予想3頭が top3 に入ったか（ボックス=全順序を購入）
        # どれか1通りが的中 → 1組み合わせ分の払戻
        if len(horse_names) >= 3:
            pred_set = set(horse_names[:3])
            is_hit = (len(pred_set) == 3 and pred_set.issubset(top3_names))
            if is_hit:
                total_payout = (_payout_for_hit(payout_map) / 100.0) * bet_per_combo

    logger.debug(
        "照合: prediction_id=%d race_id=%s bet_type=%s is_hit=%s payout=%.0f",
        prediction_id, race_id, bet_type, is_hit, total_payout,
    )

    if not dry_run:
        record_prediction_result(
            conn,
            prediction_id=prediction_id,
            is_hit=is_hit,
            payout=total_payout,
            recommended_bet=recommended_bet,
        )

    return "hit" if is_hit else "miss"


# ── 払戻データなし時の単勝フォールバック ──────────────────────────

def _reconcile_tansho_fallback(
    conn: sqlite3.Connection,
    prediction_id: int,
    race_id: str,
    recommended_bet: float,
    notes: str | None,
    ph_horse_name: str | None,
    dry_run: bool,
) -> str:
    """
    race_payouts が未取得の場合の単勝フォールバック。
    race_results.win_odds を使って払戻を推定する。
    """
    winner = _find_winner(conn, race_id)
    if winner is None:
        return "no_result"

    winner_name = winner["horse_name"]
    winner_odds = winner["win_odds"]

    predicted_name: str | None = None
    if ph_horse_name and ph_horse_name not in _MODEL_TYPE_NAMES:
        predicted_name = ph_horse_name
    else:
        target_odds = _parse_odds_from_notes(notes)
        if target_odds is not None:
            predicted_name = _find_horse_by_odds(conn, race_id, target_odds)

    if predicted_name is None:
        logger.warning(
            "予想馬を特定できませんでした: prediction_id=%d race_id=%s",
            prediction_id, race_id,
        )
        return "skip"

    is_hit  = (predicted_name == winner_name)
    payout  = (winner_odds * recommended_bet) if is_hit else 0.0

    if not dry_run:
        record_prediction_result(
            conn,
            prediction_id=prediction_id,
            is_hit=is_hit,
            payout=payout,
            recommended_bet=recommended_bet,
        )
    return "hit" if is_hit else "miss"


# ── メイン照合ロジック ────────────────────────────────────────────

def _reconcile_prediction(
    conn: sqlite3.Connection,
    prediction_id: int,
    race_id: str,
    bet_type: str,
    recommended_bet: float,
    notes: str | None,
    combination_json: str | None,
    ph_horse_name: str | None,
    dry_run: bool,
) -> str:
    """
    1 つの予想レコードを照合して prediction_results に保存する。

    Returns:
        "hit" | "miss" | "skip" | "no_result" | "no_payout" | "refund"
    """
    # ── 返還チェック（出走取消・競走除外） ────────────────────────
    # 予想馬が返還対象の場合は払戻 100% として記録し即座にリターン
    refund_names = _get_refund_set(conn, race_id)
    if refund_names:
        horse_names_for_refund = _get_predicted_horse_names(
            conn, prediction_id, race_id, bet_type, notes, ph_horse_name
        )
        if horse_names_for_refund and any(n in refund_names for n in horse_names_for_refund):
            if not dry_run:
                record_prediction_result(
                    conn,
                    prediction_id=prediction_id,
                    is_hit=False,
                    payout=recommended_bet,
                    recommended_bet=recommended_bet,
                )
            return "refund"

    # ── 払戻マップ取得 ─────────────────────────────────────────────
    payout_map = _get_payout_map(conn, race_id, bet_type)

    # 払戻テーブル未取得の場合
    if not payout_map:
        if bet_type == "単勝":
            # 単勝のみ race_results.win_odds で推定
            return _reconcile_tansho_fallback(
                conn, prediction_id, race_id, recommended_bet,
                notes, ph_horse_name, dry_run,
            )
        return "no_payout"

    # ── 組み合わせ数を算出（払戻計算用） ──────────────────────────
    # 複勝は horse_names 数がそのまま n_combos になるため後で確定
    n_combos = _n_combos_from_json(combination_json, bet_type)
    if n_combos == 0:
        # 複勝: 予想馬名リストの長さを使う
        ph_rows = conn.execute(
            "SELECT COUNT(*) FROM prediction_horses WHERE prediction_id = ? AND horse_name NOT IN ('本命', '卍')",
            (prediction_id,),
        ).fetchone()
        n_combos = max((ph_rows[0] if ph_rows else 1), 1)

    # ── 馬名ベースで照合 ──────────────────────────────────────────
    return _reconcile_by_names(
        conn,
        prediction_id=prediction_id,
        race_id=race_id,
        bet_type=bet_type,
        recommended_bet=recommended_bet,
        notes=notes,
        ph_horse_name=ph_horse_name,
        payout_map=payout_map,
        n_combos=n_combos,
        dry_run=dry_run,
    )


# ── バッチ処理 ────────────────────────────────────────────────────

def reconcile(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    race_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    全券種の予想を一括照合して prediction_results に保存する。

    Args:
        conn:    DB コネクション
        year:    対象年（None = 全期間）
        race_id: 特定レース ID のみ照合（None = 制限なし）
        dry_run: True なら DB 書き込みをしない

    Returns:
        {"total": 処理件数, "hit": 的中数, "miss": 外れ数,
         "skip": スキップ数, "no_result": 結果なし数,
         "no_payout": 払戻未取得数}
    """
    params: list = []
    year_filter = ""
    race_filter = ""

    if year:
        year_filter = "AND substr(p.race_id, 1, 4) = ?"
        params.append(str(year))
    if race_id:
        race_filter = "AND p.race_id = ?"
        params.append(race_id)

    rows = conn.execute(
        f"""
        SELECT
            p.id             AS prediction_id,
            p.race_id,
            p.bet_type,
            p.recommended_bet,
            p.notes,
            p.combination_json,
            (
                SELECT horse_name FROM prediction_horses
                WHERE prediction_id = p.id AND predicted_rank = 1
                ORDER BY id LIMIT 1
            )                AS ph_horse_name
        FROM predictions p
        WHERE NOT EXISTS (
            SELECT 1 FROM prediction_results pr
            WHERE pr.prediction_id = p.id
        )
        AND p.bet_type NOT IN ('馬分析', 'WIN5')
        {year_filter}
        {race_filter}
        ORDER BY p.race_id, p.id
        """,
        params,
    ).fetchall()

    logger.info("照合対象: %d 件 (year=%s)", len(rows), year or "all")

    stats: dict[str, int] = {
        "total":     len(rows),
        "hit":       0,
        "miss":      0,
        "skip":      0,
        "no_result": 0,
        "no_payout": 0,
        "refund":    0,
    }

    for row in rows:
        (prediction_id, race_id, bet_type, recommended_bet,
         notes, combination_json, ph_horse_name) = row

        try:
            status = _reconcile_prediction(
                conn,
                prediction_id=prediction_id,
                race_id=race_id,
                bet_type=bet_type,
                recommended_bet=recommended_bet or 100.0,
                notes=notes,
                combination_json=combination_json,
                ph_horse_name=ph_horse_name,
                dry_run=dry_run,
            )
        except Exception as exc:
            logger.error(
                "照合中に例外: prediction_id=%d race_id=%s bet_type=%s: %s",
                prediction_id, race_id, bet_type, exc,
            )
            stats["skip"] += 1
            continue

        stats[status] += 1

    # ── モデル成績再集計 ──────────────────────────────────────────
    if not dry_run and (stats["hit"] + stats["miss"]) > 0:
        years_to_refresh: set[int] = set()
        if race_id:
            years_to_refresh.add(int(race_id[:4]))
        elif year:
            years_to_refresh.add(year)
        else:
            y_rows = conn.execute(
                "SELECT DISTINCT substr(race_id, 1, 4) FROM predictions"
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
        description="全券種予想の的中照合バッチ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python -m src.ml.reconcile                  # 全期間の未処理予想を照合
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

    reconciled = stats["hit"] + stats["miss"]
    hit_rate   = (stats["hit"] / reconciled * 100) if reconciled > 0 else 0.0

    mode = "[DRY-RUN]" if args.dry_run else ""
    print(f"\n{'='*60} {mode}")
    print(f"  全券種照合結果 (year={args.year or 'all'})")
    print(f"{'='*60}")
    print(f"  照合対象          : {stats['total']:6d} 件")
    print(f"  的中              : {stats['hit']:6d} 件")
    print(f"  外れ              : {stats['miss']:6d} 件")
    print(f"  返還              : {stats['refund']:6d} 件")
    print(f"  馬名特定不能(skip): {stats['skip']:6d} 件")
    print(f"  結果データなし    : {stats['no_result']:6d} 件")
    print(f"  払戻データなし    : {stats['no_payout']:6d} 件")
    print(f"  的中率            : {hit_rate:6.1f} %")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
