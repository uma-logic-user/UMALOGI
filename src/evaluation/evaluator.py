"""
予想的中評価エンジン

レース確定後に race_payouts と predictions を突合し、的中/不的中・払戻・ROI を
計算して prediction_results テーブルへ保存する。

例外ケースの取り扱い:
  同着 (dead heat):
    race_results で同じ rank に複数馬が存在する。
    horse_number ベースで払戻を検索し、各馬の払戻を合算して平均をとる。
    例: 複勝 同着 → race_payouts に 2行あり、その平均を使用。
  返還 (refund / scratch):
    出走取消・競走除外の場合 race_payouts に '返還' bet_type が存在する。
    対象馬番を含む買い目は 100円返還として処理する。
  競走中止:
    race_results.rank IS NULL または rank = 0 の馬は未着扱いとし的中対象外。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)

BetType = Literal["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単", "WIN5"]

# 複勝・ワイドの圏内着順（出走頭数によって変わるが JRA は基本3着まで）
_PLACE_RANKS = {1, 2, 3}

# 同着フラグに使う最大着順差（0 = 完全同着のみ判定）
_DEAD_HEAT_TOLERANCE = 0


@dataclass
class BetHitDetail:
    """1買い目ごとの的中結果。"""
    prediction_id: int
    bet_type:        str
    is_hit:          bool
    is_refund:       bool                  # 返還フラグ
    payout:          float                 # 実際の払戻金額（円）
    invested:        float                 # 購入金額（円）
    profit:          float                 # 利益 = payout - invested
    roi:             float                 # 回収率 (%) = payout / invested * 100
    combination:     list[str]             # 予想組み合わせ（馬名）
    actual_winners:  list[str]             # 実際の着順上位馬名


@dataclass
class EvaluationResult:
    """1レースの評価まとめ。"""
    race_id:         str
    race_name:       str
    date:            str
    hits:            list[BetHitDetail]    # 全買い目の結果
    total_invested:  float
    total_payout:    float
    roi:             float                 # レース全体の回収率
    has_manbaken:    bool                  # 払戻 >= 10,000円
    max_single_roi:  float                 # 最大単一買い目 ROI
    is_refund_race:  bool                  # 返還あり
    errors:          list[str] = field(default_factory=list)

    @property
    def net_profit(self) -> float:
        return self.total_payout - self.total_invested

    @property
    def hit_count(self) -> int:
        return sum(1 for h in self.hits if h.is_hit)


# ================================================================
# ヘルパー: DB クエリ
# ================================================================

def _fetch_race_meta(conn: sqlite3.Connection, race_id: str) -> dict:
    row = conn.execute(
        "SELECT race_name, date FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"race_id={race_id} が races テーブルに存在しません")
    return {"race_name": row[0], "date": row[1]}


def _fetch_results(conn: sqlite3.Connection, race_id: str) -> dict[str, int | None]:
    """馬名 → 着順 のマップを返す。rank=NULL は未着(0扱い)。"""
    rows = conn.execute(
        "SELECT horse_name, rank FROM race_results WHERE race_id = ?", (race_id,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _fetch_horse_numbers(conn: sqlite3.Connection, race_id: str) -> dict[str, int]:
    """馬名 → 馬番 のマップ。"""
    rows = conn.execute(
        "SELECT horse_name, horse_number FROM race_results WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _fetch_payouts(conn: sqlite3.Connection, race_id: str) -> dict[tuple[str, str], int]:
    """(bet_type, combination) → payout のマップ。"""
    rows = conn.execute(
        "SELECT bet_type, combination, payout FROM race_payouts WHERE race_id = ?",
        (race_id,),
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def _fetch_refund_numbers(conn: sqlite3.Connection, race_id: str) -> set[int]:
    """返還対象の馬番セット。"""
    rows = conn.execute(
        "SELECT combination FROM race_payouts WHERE race_id = ? AND bet_type = '返還'",
        (race_id,),
    ).fetchall()
    result: set[int] = set()
    for (comb,) in rows:
        try:
            result.add(int(comb))
        except (ValueError, TypeError):
            pass
    return result


def _fetch_predictions(
    conn: sqlite3.Connection, race_id: str
) -> list[dict]:
    """predictions + prediction_horses を結合して返す。"""
    preds = conn.execute(
        """
        SELECT p.id, p.model_type, p.bet_type, p.recommended_bet,
               p.combination_json
        FROM predictions p
        WHERE p.race_id = ?
        """,
        (race_id,),
    ).fetchall()

    result = []
    for pid, model_type, bet_type, rec_bet, combo_json in preds:
        horses = conn.execute(
            """
            SELECT ph.horse_name, ph.predicted_rank, ph.model_score
            FROM prediction_horses ph
            WHERE ph.prediction_id = ?
            ORDER BY ph.predicted_rank
            """,
            (pid,),
        ).fetchall()
        result.append({
            "prediction_id": pid,
            "model_type": model_type,
            "bet_type": bet_type,
            "recommended_bet": rec_bet or 100.0,
            "combination_json": combo_json,
            "horses": [(h[0], h[1], h[2]) for h in horses],
        })
    return result


# ================================================================
# ヘルパー: 払戻検索
# ================================================================

def _build_combination_key(
    bet_type: str,
    horse_names: list[str],
    horse_numbers: dict[str, int],
) -> str | None:
    """
    馬名リスト → race_payouts.combination 文字列へ変換する。

    combination フォーマット（race_payouts の慣習）:
      単勝/複勝  : "7"
      馬連/ワイド: "7-14"       (昇順)
      馬単      : "7→14"
      三連複     : "7-14-16"    (昇順)
      三連単     : "7→14→16"
    """
    nums: list[int] = []
    for name in horse_names:
        n = horse_numbers.get(name)
        if n is None:
            return None
        nums.append(n)

    if bet_type in ("単勝", "複勝"):
        return str(nums[0])
    elif bet_type == "馬連":
        return "-".join(str(n) for n in sorted(nums[:2]))
    elif bet_type == "ワイド":
        return "-".join(str(n) for n in sorted(nums[:2]))
    elif bet_type == "馬単":
        return "→".join(str(n) for n in nums[:2])
    elif bet_type == "三連複":
        return "-".join(str(n) for n in sorted(nums[:3]))
    elif bet_type == "三連単":
        return "→".join(str(n) for n in nums[:3])
    return None


def _lookup_payout(
    bet_type: str,
    combination_key: str | None,
    payouts: dict[tuple[str, str], int],
) -> int:
    """払戻テーブルから金額（円/100円）を返す。見つからなければ 0。"""
    if combination_key is None:
        return 0
    return payouts.get((bet_type, combination_key), 0)


# ================================================================
# ヘルパー: 的中判定
# ================================================================

def _winners_for_bet(
    bet_type: str,
    result_map: dict[str, int | None],
) -> list[str]:
    """bet_type に応じた着順優先馬名リスト（的中判定の基準）を返す。"""
    ranked = sorted(
        [(name, rank) for name, rank in result_map.items() if rank and rank > 0],
        key=lambda x: x[1],
    )
    if bet_type == "単勝":
        return [n for n, r in ranked if r == 1]
    elif bet_type == "複勝":
        return [n for n, r in ranked if r in _PLACE_RANKS]
    elif bet_type in ("馬連", "ワイド"):
        return [n for n, r in ranked if r in {1, 2}] if bet_type == "馬連" else \
               [n for n, r in ranked if r in _PLACE_RANKS]
    elif bet_type == "馬単":
        return [n for n, r in ranked if r in {1, 2}]
    elif bet_type in ("三連複", "三連単"):
        return [n for n, r in ranked if r in {1, 2, 3}]
    return []


def _is_hit(
    bet_type: str,
    predicted_names: list[str],
    result_map: dict[str, int | None],
) -> bool:
    """予想の的中判定。"""
    pset = set(predicted_names)
    if not pset:
        return False

    ranked = sorted(
        [(name, rank) for name, rank in result_map.items() if rank and rank > 0],
        key=lambda x: x[1],
    )

    if bet_type == "単勝":
        top1 = [n for n, r in ranked if r == 1]
        return bool(pset & set(top1))

    elif bet_type == "複勝":
        top3 = [n for n, r in ranked if r in _PLACE_RANKS]
        return bool(pset & set(top3))

    elif bet_type == "馬連":
        # 1着・2着の2頭がセットで予想に含まれているか（同着対応）
        # 同着1着（rank=1が2頭）: 両方rank=1 → 的中
        # 同着2着（rank=2が2頭）: rank=1 + rank=2 の組み合わせのみ的中（rank=2同士は不可）
        if len(predicted_names) < 2:
            return False
        rank_lookup = {n: r for n, r in ranked}
        r0 = rank_lookup.get(predicted_names[0])
        r1 = rank_lookup.get(predicted_names[1])
        if r0 not in {1, 2} or r1 not in {1, 2}:
            return False
        return not (r0 == 2 and r1 == 2)  # 両方rank=2は不可

    elif bet_type == "ワイド":
        # 予想2頭が両方とも3着以内に入っているか
        if len(predicted_names) < 2:
            return False
        top3 = {n for n, r in ranked if r in _PLACE_RANKS}
        return len(pset & top3) >= 2

    elif bet_type == "馬単":
        # 着順まで完全一致（1着=1番目, 2着=2番目）
        if len(predicted_names) < 2:
            return False
        rank_map = {n: r for n, r in ranked}
        r1 = rank_map.get(predicted_names[0])
        r2 = rank_map.get(predicted_names[1])
        return r1 == 1 and r2 == 2

    elif bet_type == "三連複":
        # 同着対応: 3着同着（rank=3が2頭）でも予想3頭がいずれかtop3に入れば的中
        if len(predicted_names) < 3:
            return False
        top3 = {n for n, r in ranked if r in {1, 2, 3}}
        return len(pset) == 3 and pset.issubset(top3)

    elif bet_type == "三連単":
        if len(predicted_names) < 3:
            return False
        rank_map = {n: r for n, r in ranked}
        return (rank_map.get(predicted_names[0]) == 1 and
                rank_map.get(predicted_names[1]) == 2 and
                rank_map.get(predicted_names[2]) == 3)

    return False


def _has_refund(
    horse_names: list[str],
    horse_numbers: dict[str, int],
    refund_numbers: set[int],
) -> bool:
    """予想に含まれる馬番が返還対象かどうかを判定。"""
    for name in horse_names:
        n = horse_numbers.get(name)
        if n is not None and n in refund_numbers:
            return True
    return False


# ================================================================
# Evaluator 本体
# ================================================================

class Evaluator:
    """
    レース確定後の的中・払戻評価クラス。

    Usage:
        evaluator = Evaluator()
        result = evaluator.evaluate_race(conn, "202401010101")
        if result.has_manbaken:
            ...
    """

    def __init__(self) -> None:
        pass

    def evaluate_race(
        self,
        conn: sqlite3.Connection,
        race_id: str,
        *,
        dry_run: bool = False,
    ) -> EvaluationResult:
        """
        指定レースの全予想を評価して EvaluationResult を返す。

        Args:
            conn:    DB コネクション
            race_id: 評価対象レース ID
            dry_run: True の場合 prediction_results への書き込みをスキップ

        Returns:
            EvaluationResult
        """
        errors: list[str] = []

        # ── メタ情報取得 ───────────────────────────────────────
        try:
            meta = _fetch_race_meta(conn, race_id)
        except ValueError as e:
            logger.error("%s", e)
            return EvaluationResult(
                race_id=race_id, race_name="unknown", date="",
                hits=[], total_invested=0, total_payout=0, roi=0,
                has_manbaken=False, max_single_roi=0, is_refund_race=False,
                errors=[str(e)],
            )

        result_map     = _fetch_results(conn, race_id)
        horse_numbers  = _fetch_horse_numbers(conn, race_id)
        payouts        = _fetch_payouts(conn, race_id)
        refund_numbers = _fetch_refund_numbers(conn, race_id)
        predictions    = _fetch_predictions(conn, race_id)
        is_refund_race = bool(refund_numbers)

        if not result_map:
            errors.append(f"race_id={race_id}: race_results にデータがありません")
        if not payouts:
            errors.append(f"race_id={race_id}: race_payouts にデータがありません")

        actual_winners_all = [
            n for n, r in sorted(result_map.items(), key=lambda x: (x[1] or 99))
            if r and r > 0
        ][:3]

        # ── 予想ごとに的中判定 ─────────────────────────────────
        hit_details: list[BetHitDetail] = []
        total_invested = 0.0
        total_payout   = 0.0
        max_single_roi = 0.0

        for pred in predictions:
            pid       = pred["prediction_id"]
            bet_type  = pred["bet_type"]
            invested  = float(pred["recommended_bet"] or 100.0)
            horses    = pred["horses"]            # [(name, pred_rank, score)]
            horse_names = [h[0] for h in horses]

            # 返還チェック
            refund = _has_refund(horse_names, horse_numbers, refund_numbers)
            if refund:
                detail = BetHitDetail(
                    prediction_id=pid,
                    bet_type=bet_type,
                    is_hit=False,
                    is_refund=True,
                    payout=invested,          # 返還 = 投資額返却
                    invested=invested,
                    profit=0.0,
                    roi=100.0,
                    combination=horse_names,
                    actual_winners=actual_winners_all,
                )
                hit_details.append(detail)
                total_invested += invested
                total_payout   += invested
                if not dry_run:
                    self._save_result(conn, pid, False, invested, 0.0, 100.0)
                continue

            # 的中判定
            hit = _is_hit(bet_type, horse_names, result_map)

            # 払戻取得
            payout_per_100 = 0
            if hit:
                combo_key = _build_combination_key(bet_type, horse_names, horse_numbers)
                payout_per_100 = _lookup_payout(bet_type, combo_key, payouts)
                if payout_per_100 == 0 and hit:
                    # combination_key が解決できなかった場合、bet_type だけで検索
                    matching = [
                        v for (bt, _), v in payouts.items() if bt == bet_type
                    ]
                    if matching:
                        payout_per_100 = int(sum(matching) / len(matching))
                        errors.append(
                            f"pid={pid} {bet_type}: combination 解決不可、"
                            f"払戻平均 {payout_per_100} を使用"
                        )

            actual_payout = (payout_per_100 / 100.0) * invested if hit else 0.0
            profit        = actual_payout - invested
            roi           = (actual_payout / invested * 100.0) if invested > 0 else 0.0

            detail = BetHitDetail(
                prediction_id=pid,
                bet_type=bet_type,
                is_hit=hit,
                is_refund=False,
                payout=actual_payout,
                invested=invested,
                profit=profit,
                roi=roi,
                combination=horse_names,
                actual_winners=actual_winners_all,
            )
            hit_details.append(detail)
            total_invested += invested
            total_payout   += actual_payout
            max_single_roi = max(max_single_roi, roi)

            if not dry_run:
                self._save_result(conn, pid, hit, actual_payout, profit, roi)

        # ── サマリ ─────────────────────────────────────────────
        overall_roi = (total_payout / total_invested * 100.0) if total_invested > 0 else 0.0
        has_manbaken = any(d.payout >= 10_000 for d in hit_details if d.is_hit)

        logger.info(
            "[評価] race=%s %s: 予想%d件 的中%d件 投資¥%.0f 払戻¥%.0f ROI=%.1f%%",
            race_id, meta["race_name"],
            len(hit_details),
            sum(1 for d in hit_details if d.is_hit),
            total_invested, total_payout, overall_roi,
        )

        return EvaluationResult(
            race_id=race_id,
            race_name=meta["race_name"],
            date=meta["date"],
            hits=hit_details,
            total_invested=total_invested,
            total_payout=total_payout,
            roi=overall_roi,
            has_manbaken=has_manbaken,
            max_single_roi=max_single_roi,
            is_refund_race=is_refund_race,
            errors=errors,
        )

    def _save_result(
        self,
        conn: sqlite3.Connection,
        prediction_id: int,
        is_hit: bool,
        payout: float,
        profit: float,
        roi: float,
    ) -> None:
        """prediction_results に結果を upsert する。"""
        try:
            with conn:
                updated = conn.execute(
                    """
                    UPDATE prediction_results
                    SET is_hit = ?, payout = ?, profit = ?, roi = ?,
                        recorded_at = datetime('now', 'localtime')
                    WHERE prediction_id = ?
                    """,
                    (int(is_hit), payout, profit, roi, prediction_id),
                ).rowcount
                if updated == 0:
                    conn.execute(
                        """
                        INSERT INTO prediction_results
                            (prediction_id, is_hit, payout, profit, roi)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (prediction_id, int(is_hit), payout, profit, roi),
                    )
        except sqlite3.IntegrityError as e:
            logger.warning("prediction_results 保存失敗 pid=%d: %s", prediction_id, e)

    def evaluate_date(
        self,
        conn: sqlite3.Connection,
        date: str,
        *,
        dry_run: bool = False,
    ) -> list[EvaluationResult]:
        """
        指定日の全レースを評価する。

        Args:
            date: "YYYY-MM-DD" 形式 (ISO 8601)
        """
        race_ids = [
            r[0] for r in conn.execute(
                "SELECT race_id FROM races WHERE date = ? ORDER BY race_id",
                (date,),
            ).fetchall()
        ]
        logger.info("評価対象: %s の %d レース", date, len(race_ids))
        return [self.evaluate_race(conn, rid, dry_run=dry_run) for rid in race_ids]
