"""
scripts/win5_strategy.py — WIN5 累積勝率カバー法 + ウォークフォワードバックテスト

アルゴリズム:
  1. 当日の主会場（最多命名レース・会場優先順位）を特定し R8-R12 を WIN5 対象レースとする
  2. 各レースの単勝オッズ（win_odds）から市場確率を推定
  3. 確率降順に馬を並べ、累積確率が threshold（デフォルト 85%）に達するまで選択
  4. 5レース全馬の直積 = 購入組み合わせ数 × 100 円 = 投資額
  5. バックテスト: 5レース全てで実際の1着馬が選択馬に含まれる場合を「的中」とする
     ※WIN5 実際の払戻データは DB にないため、的中/外れと理論値で評価

データ制約:
  - win_odds は race_results に 2026-04-18〜04-26 のみ存在
  - 勝ち馬は race_payouts.bet_type='単勝' の combination 列から全期間取得可能

Usage:
    py -3 scripts/win5_strategy.py --backtest
    py -3 scripts/win5_strategy.py --next
    py -3 scripts/win5_strategy.py --date 20260510
    py -3 scripts/win5_strategy.py --threshold 0.90
    py -3 scripts/win5_strategy.py --backtest --threshold 0.90 --verbose
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date as dt_date, timedelta
from pathlib import Path
from typing import Optional

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

# ── 定数 ──────────────────────────────────────────────────────────────

# 会場優先順位（小さいほど主要会場）
_VENUE_PRIORITY: dict[str, int] = {
    "東京": 1, "阪神": 2, "京都": 3, "中山": 4,
    "中京": 5, "新潟": 6, "小倉": 7, "福島": 8,
    "函館": 9, "札幌": 10,
}

_DEFAULT_THRESHOLD = 0.85   # 累積確率カバー閾値
_WIN5_RETURN_RATE  = 0.725  # WIN5 払戻率
_MIN_ODDS          = 1.1    # オッズ最小値（0除算防止）
_MAX_VALID_ODDS    = 999.9  # これ以上は汚染データとして除外

# 日曜日 = 0（strftime %w）
_SUNDAY = "0"


# ── データクラス ──────────────────────────────────────────────────────

@dataclass
class Win5RacePicks:
    """1レース分の選択情報。"""
    race_id:      str
    race_number:  int
    race_name:    str
    venue:        str
    field_size:   int           # 出走頭数
    picks:        list[int]     # 選択馬番リスト
    probs:        list[float]   # 選択馬の市場確率
    cumul_prob:   float         # 累積カバー確率
    winner:       Optional[int] = None   # 実際の1着馬番（バックテスト用）
    is_covered:   Optional[bool] = None  # 的中カバー有無

    def __str__(self) -> str:
        picks_str = "・".join(str(p) for p in self.picks)
        covered   = "✓" if self.is_covered else ("✗" if self.is_covered is False else "?")
        return (
            f"  R{self.race_number:02d} {self.race_name[:12]:<12} "
            f"選択{len(self.picks)}頭[{picks_str}]  "
            f"カバー率{self.cumul_prob*100:.1f}%  "
            f"勝馬:{self.winner or '?'}  {covered}"
        )


@dataclass
class Win5WeekResult:
    """1週分の WIN5 バックテスト結果。"""
    date:        str
    venue:       str
    races:       list[Win5RacePicks]
    all_covered: bool          # 5レース全的中カバー
    combinations: int          # 購入組み合わせ数
    cost:        int           # 投資額（円）
    # 理論払戻 = 1/combined_prob × return_rate × 100 円
    theoretical_payout: float
    actual_payout: Optional[int] = None  # 実際の WIN5 払戻（research_db から取得時）

    def report(self) -> str:
        lines = [
            f"【{self.date} {self.venue}】 "
            f"{'★的中カバー' if self.all_covered else '　外れ'}  "
            f"{self.combinations}点 ¥{self.cost:,}",
        ]
        for r in self.races:
            lines.append(str(r))
        if self.all_covered:
            lines.append(
                f"  理論払戻（≈）¥{self.theoretical_payout:,.0f}"
            )
        return "\n".join(lines)


# ── DB ヘルパー ──────────────────────────────────────────────────────

def _get_sundays(conn: sqlite3.Connection,
                 start: str, end: str) -> list[str]:
    """[start, end] の間の全日曜日をリストで返す。"""
    rows = conn.execute(
        """
        SELECT DISTINCT date FROM races
        WHERE date BETWEEN ? AND ?
          AND strftime('%w', date) = ?
        ORDER BY date
        """,
        (start, end, _SUNDAY),
    ).fetchall()
    return [r[0] for r in rows]


def detect_win5_venue(
    conn: sqlite3.Connection,
    date_str: str,
    *,
    prefer_odds: bool = False,
) -> str | None:
    """
    指定日の WIN5 主会場を特定する。

    heuristic:
      1. R8〜R12 に命名レースが最も多い会場
      2. 同数の場合は _VENUE_PRIORITY で優先順位が高い会場
      3. prefer_odds=True の場合: 同順位なら R8-R12 全頭で win_odds が
         最も多く存在する会場を優先（バックテスト用）
    """
    rows = conn.execute(
        """
        SELECT venue,
               SUM(CASE WHEN race_name IS NOT NULL AND race_name != '' THEN 1 ELSE 0 END) AS named_cnt
        FROM races
        WHERE date = ? AND race_number BETWEEN 8 AND 12
        GROUP BY venue
        ORDER BY named_cnt DESC
        """,
        (date_str,),
    ).fetchall()
    if not rows:
        return None

    max_named = rows[0][1]
    candidates = [r[0] for r in rows if r[1] == max_named]

    if prefer_odds and len(candidates) > 1:
        # win_odds の件数が多い会場を優先
        def _odds_count(v: str) -> int:
            cnt = conn.execute(
                """
                SELECT COUNT(DISTINCT rr.horse_number)
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                WHERE r.date = ? AND r.venue = ?
                  AND r.race_number BETWEEN 8 AND 12
                  AND rr.win_odds IS NOT NULL
                  AND rr.win_odds > 0
                  AND rr.win_odds <= ?
                """,
                (date_str, v, _MAX_VALID_ODDS),
            ).fetchone()
            return int(cnt[0]) if cnt else 0

        candidates.sort(
            key=lambda v: (-_odds_count(v), _VENUE_PRIORITY.get(v, 99))
        )
    else:
        candidates.sort(key=lambda v: _VENUE_PRIORITY.get(v, 99))

    return candidates[0]


def detect_win5_races(
    conn: sqlite3.Connection,
    date_str: str,
    *,
    prefer_odds: bool = False,
) -> list[str]:
    """WIN5 対象の 5 race_id（R8〜R12）を返す。"""
    venue = detect_win5_venue(conn, date_str, prefer_odds=prefer_odds)
    if not venue:
        return []
    rows = conn.execute(
        """
        SELECT race_id FROM races
        WHERE date = ? AND venue = ?
          AND race_number BETWEEN 8 AND 12
        ORDER BY race_number
        """,
        (date_str, venue),
    ).fetchall()
    return [r[0] for r in rows]


def _get_race_meta(conn: sqlite3.Connection,
                   race_id: str) -> tuple[str, int, str, str]:
    """(race_name, race_number, date, venue) を返す。"""
    row = conn.execute(
        "SELECT race_name, race_number, date, venue FROM races WHERE race_id=?",
        (race_id,),
    ).fetchone()
    if not row:
        return ("", 0, "", "")
    return (row[0] or "", row[1] or 0, row[2] or "", row[3] or "")


def get_winner(conn: sqlite3.Connection, race_id: str) -> int | None:
    """
    単勝払戻から実際の1着馬番を返す。

    1. popularity IS NOT NULL の行を優先（汚染データの多い日付対策）
    2. 1のデータがない場合: 有効な馬番（1〜28）かつ単一エントリなら採用
    """
    # 優先: popularity 付き
    rows = conn.execute(
        """
        SELECT combination, payout
        FROM race_payouts
        WHERE race_id = ? AND bet_type = '単勝'
          AND popularity IS NOT NULL
        ORDER BY payout ASC
        LIMIT 5
        """,
        (race_id,),
    ).fetchall()
    for combo, payout in rows:
        try:
            hno = int(combo)
            if 1 <= hno <= 28:
                return hno
        except (ValueError, TypeError):
            continue

    # フォールバック: popularity が全 NULL のケース（5/3 等）
    all_rows = conn.execute(
        """
        SELECT combination, payout
        FROM race_payouts
        WHERE race_id = ? AND bet_type = '単勝'
        ORDER BY id
        """,
        (race_id,),
    ).fetchall()
    valid: list[int] = []
    for combo, _ in all_rows:
        try:
            hno = int(combo)
            if 1 <= hno <= 28:
                valid.append(hno)
        except (ValueError, TypeError):
            continue
    # 有効馬番が唯一ならそれを採用（複数あれば汚染の可能性→ None）
    return valid[0] if len(valid) == 1 else None


def _get_horses_with_odds(
    conn: sqlite3.Connection,
    race_id: str,
    research_conn: Optional[sqlite3.Connection] = None,
) -> list[tuple[int, float]]:
    """
    (horse_number, win_odds) のリストを返す。

    取得順: race_results.win_odds → realtime_odds → prediction_horses.model_score
    モデルスコアを使う場合は「擬似オッズ = 1 / normalized_score」として扱う。
    馬番ごとに重複排除し、汚染データ（odds > MAX_VALID_ODDS）を除外する。
    """
    # 1. race_results から取得（汚染行は win_odds が極端に大きい or NULL）
    rows = conn.execute(
        """
        SELECT horse_number, MIN(win_odds) AS odds
        FROM race_results
        WHERE race_id = ?
          AND win_odds IS NOT NULL
          AND win_odds > 0
          AND win_odds <= ?
        GROUP BY horse_number
        ORDER BY horse_number
        """,
        (race_id, _MAX_VALID_ODDS),
    ).fetchall()
    if rows:
        return [(int(r[0]), float(r[1])) for r in rows]

    # 2. research_db (netkeiba スクレイプ済みオッズ) にフォールバック
    if research_conn is not None:
        rows = research_conn.execute(
            """
            SELECT horse_number, win_odds
            FROM horse_odds
            WHERE race_id = ?
              AND win_odds IS NOT NULL
              AND win_odds > 0
              AND win_odds <= ?
            ORDER BY horse_number
            """,
            (race_id, _MAX_VALID_ODDS),
        ).fetchall()
        if rows:
            return [(int(r[0]), float(r[1])) for r in rows]

    # 3. realtime_odds にフォールバック
    rows = conn.execute(
        """
        SELECT horse_number, MIN(win_odds) AS odds
        FROM realtime_odds
        WHERE race_id = ?
          AND win_odds IS NOT NULL
          AND win_odds > 0
          AND win_odds <= ?
        GROUP BY horse_number
        ORDER BY horse_number
        """,
        (race_id, _MAX_VALID_ODDS),
    ).fetchall()
    if rows:
        return [(int(r[0]), float(r[1])) for r in rows]

    # 4. prediction_horses の model_score を擬似オッズとして使用
    #    score → 正規化確率 → pseudo_odds = 1 / prob（_MIN_ODDS 未満にならない）
    ph_rows = conn.execute(
        """
        SELECT DISTINCT ph.horse_name, MAX(ph.model_score) AS score
        FROM prediction_horses ph
        JOIN predictions p ON p.id = ph.prediction_id
        WHERE p.race_id = ?
          AND ph.model_score IS NOT NULL AND ph.model_score > 0
        GROUP BY ph.horse_name
        """,
        (race_id,),
    ).fetchall()
    if not ph_rows:
        return []

    name_score: dict[str, float] = {r[0]: float(r[1]) for r in ph_rows}
    total_score = sum(name_score.values())
    if total_score <= 0:
        return []

    entry_rows = conn.execute(
        "SELECT horse_number, horse_name FROM entries WHERE race_id=?",
        (race_id,),
    ).fetchall()
    if not entry_rows:
        entry_rows = conn.execute(
            """
            SELECT DISTINCT horse_number, horse_name FROM race_results
            WHERE race_id=? AND horse_number IS NOT NULL
            """,
            (race_id,),
        ).fetchall()

    result: list[tuple[int, float]] = []
    for hno, hname in entry_rows:
        score = name_score.get(hname)
        if score and score > 0:
            # 正規化確率 → 擬似オッズ（常に > 1.0 なので _MIN_ODDS クリップ不要）
            prob         = score / total_score
            pseudo_odds  = 1.0 / prob
            result.append((int(hno), pseudo_odds))

    return sorted(result, key=lambda x: x[0])


def _get_field_size(conn: sqlite3.Connection, race_id: str) -> int:
    """出走頭数を entries → race_results の順で返す。"""
    row = conn.execute(
        "SELECT COUNT(*) FROM entries WHERE race_id=?", (race_id,)
    ).fetchone()
    if row and row[0] > 0:
        return int(row[0])
    row = conn.execute(
        "SELECT COUNT(DISTINCT horse_number) FROM race_results WHERE race_id=?",
        (race_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def get_coverage_picks(
    conn: sqlite3.Connection,
    race_id: str,
    threshold: float = _DEFAULT_THRESHOLD,
    research_conn: Optional[sqlite3.Connection] = None,
) -> Win5RacePicks | None:
    """
    累積勝率カバー法で選択馬を決定する。

    勝率降順に馬を並べ、累積確率が threshold に達するまで選択する。
    win_odds データがない場合は None を返す。
    """
    race_name, race_number, _, venue = _get_race_meta(conn, race_id)
    field_size = _get_field_size(conn, race_id)
    horse_odds  = _get_horses_with_odds(conn, race_id, research_conn)

    if not horse_odds:
        return None

    # 市場確率: 1/odds を正規化
    raw  = [(hno, 1.0 / max(o, _MIN_ODDS)) for hno, o in horse_odds]
    total = sum(p for _, p in raw)
    probs = [(hno, p / total) for hno, p in raw]

    # 確率降順にソート
    probs.sort(key=lambda x: x[1], reverse=True)

    # 累積確率が threshold 以上になるまで追加
    selected_hnos:  list[int]   = []
    selected_probs: list[float] = []
    cumul = 0.0
    for hno, prob in probs:
        selected_hnos.append(hno)
        selected_probs.append(prob)
        cumul += prob
        if cumul >= threshold:
            break

    winner = get_winner(conn, race_id)
    is_covered: bool | None = None
    if winner is not None:
        is_covered = winner in selected_hnos

    return Win5RacePicks(
        race_id=race_id,
        race_number=race_number,
        race_name=race_name,
        venue=venue,
        field_size=field_size,
        picks=selected_hnos,
        probs=selected_probs,
        cumul_prob=cumul,
        winner=winner,
        is_covered=is_covered,
    )


# ── バックテスト ─────────────────────────────────────────────────────

def backtest_win5(
    conn: sqlite3.Connection,
    *,
    start_date: str = "2026-01-01",
    end_date:   str | None = None,
    threshold:  float = _DEFAULT_THRESHOLD,
    verbose:    bool = False,
    research_db_path: Optional[Path] = None,
    max_combinations: int = 5000,
) -> list[Win5WeekResult]:
    """
    全日曜日を対象としたウォークフォワードバックテスト。

    research_db_path が指定された場合:
      - netkeiba スクレイプ済みオッズを用いてオッズ不足を補完
      - win5_results テーブルに実際の WIN5 払戻があれば理論値の代わりに使用
    """
    from pathlib import Path as _Path

    today    = dt_date.today().isoformat()
    end_date = end_date or today
    sundays  = _get_sundays(conn, start_date, end_date)
    results: list[Win5WeekResult] = []

    research_conn: Optional[sqlite3.Connection] = None
    if research_db_path and _Path(research_db_path).exists():
        research_conn = sqlite3.connect(str(research_db_path))

    try:
        for date_str in sundays:
            race_ids = detect_win5_races(conn, date_str, prefer_odds=(research_conn is None))
            if len(race_ids) < 5:
                if verbose:
                    print(f"[SKIP] {date_str}: WIN5 対象レース数不足 ({len(race_ids)}件)")
                continue

            venue = detect_win5_venue(conn, date_str, prefer_odds=(research_conn is None)) or ""

            race_results_list: list[Win5RacePicks] = []
            skip = False
            for rid in race_ids:
                pick = get_coverage_picks(conn, rid, threshold, research_conn)
                if pick is None:
                    if verbose:
                        print(f"[SKIP] {date_str} {rid}: odds データなし")
                    skip = True
                    break
                race_results_list.append(pick)

            if skip:
                continue

            # 組み合わせ数チェック（汚染データ週を除外）
            combos = 1
            for r in race_results_list:
                combos *= len(r.picks)

            if combos > max_combinations:
                if verbose:
                    print(f"[SKIP] {date_str}: 組み合わせ数超過 ({combos:,}点 > 上限{max_combinations:,}点)")
                continue

            all_covered = all(r.is_covered is True for r in race_results_list)
            cost = combos * 100

            combined_prob_winner = 1.0
            for r in race_results_list:
                if r.winner and r.winner in r.picks:
                    idx = r.picks.index(r.winner)
                    combined_prob_winner *= r.probs[idx]
                else:
                    combined_prob_winner *= 0.0

            theoretical_payout = (
                (1.0 / max(combined_prob_winner, 1e-10)) * _WIN5_RETURN_RATE * 100
                if combined_prob_winner > 0 else 0.0
            )

            # 実際の WIN5 払戻を research_db から取得（あれば）
            actual_payout: Optional[int] = None
            if research_conn is not None and all_covered:
                week_id = date_str.replace("-", "")
                row = research_conn.execute(
                    "SELECT payout FROM win5_results WHERE week_id = ?",
                    (week_id,),
                ).fetchone()
                if row and row[0]:
                    actual_payout = int(row[0])

            week = Win5WeekResult(
                date=date_str,
                venue=venue,
                races=race_results_list,
                all_covered=all_covered,
                combinations=combos,
                cost=cost,
                theoretical_payout=theoretical_payout,
                actual_payout=actual_payout,
            )
            results.append(week)

            if verbose:
                print(week.report())
                print()

    finally:
        if research_conn is not None:
            research_conn.close()

    return results


def _print_backtest_summary(results: list[Win5WeekResult]) -> None:
    """バックテスト集計結果を出力する。"""
    if not results:
        print("[WIN5] バックテスト対象データなし（win_odds 不足）")
        return

    n_weeks    = len(results)
    n_hit      = sum(1 for r in results if r.all_covered)
    total_cost = sum(r.cost for r in results)
    avg_combos = sum(r.combinations for r in results) / n_weeks

    # 実際の払戻があればそちらを優先
    has_actual = any(r.actual_payout is not None for r in results if r.all_covered)
    if has_actual:
        total_payout = sum(
            (r.actual_payout or r.theoretical_payout)
            for r in results if r.all_covered
        )
        payout_label = "実績払戻合計   "
    else:
        total_payout = sum(r.theoretical_payout for r in results if r.all_covered)
        payout_label = "理論払戻合計（≈）"

    print("=" * 60)
    print("WIN5 バックテスト サマリー")
    print("=" * 60)
    print(f"  対象週数         : {n_weeks} 週")
    print(f"  的中カバー週     : {n_hit} 週 / {n_weeks} 週 "
          f"({n_hit/n_weeks*100:.1f}%)")
    print(f"  平均組み合わせ数  : {avg_combos:.1f} 点/週")
    print(f"  総投資額         : ¥{total_cost:,}")
    print(f"  {payout_label} : ¥{total_payout:,.0f}")
    roi = (total_payout / total_cost * 100) if total_cost > 0 else 0.0
    print(f"  {'実績' if has_actual else '理論'} ROI            : {roi:.1f}%")
    print()

    print("週別結果:")
    print(f"  {'日付':12} {'会場':6} {'結果':10} {'点数':6} {'投資額':8}")
    print(f"  {'-'*52}")
    for r in results:
        result_str = "★的中カバー" if r.all_covered else "　外れ"
        print(
            f"  {r.date}  {r.venue:<4}  {result_str}  "
            f"{r.combinations:5}点  ¥{r.cost:6,}"
        )


# ── 次回 WIN5 予想 ───────────────────────────────────────────────────

def _next_sunday(from_date: str | None = None) -> str:
    """from_date（または今日）の次の日曜日を返す。"""
    base = dt_date.fromisoformat(from_date) if from_date else dt_date.today()
    days_ahead = (6 - base.weekday()) % 7  # 0=月, 6=日
    if days_ahead == 0:
        days_ahead = 7
    return (base + timedelta(days=days_ahead)).isoformat()


def predict_next_win5(
    conn: sqlite3.Connection,
    target_date: str,
    threshold: float = _DEFAULT_THRESHOLD,
) -> None:
    """次回 WIN5 の推奨選択馬を出力する。"""
    race_ids = detect_win5_races(conn, target_date)
    venue    = detect_win5_venue(conn, target_date) or ""

    print("=" * 60)
    print(f"WIN5 推奨選択馬 — {target_date} {venue}")
    print(f"累積確率閾値: {threshold*100:.0f}%")
    print("=" * 60)

    if not race_ids:
        print(f"[ERROR] {target_date} のレースデータが見つかりません")
        return

    total_combos = 1
    race_picks_list: list[Win5RacePicks | None] = []

    for rid in race_ids:
        pick = get_coverage_picks(conn, rid, threshold)
        race_picks_list.append(pick)

    all_ok = all(p is not None for p in race_picks_list)

    for pick in race_picks_list:
        if pick is None:
            print("  [データ不足 — odds データが未取得]")
            continue
        picks_str = "  ".join(
            f"#{hno}({prob*100:.1f}%)" for hno, prob in zip(pick.picks, pick.probs)
        )
        print(
            f"  R{pick.race_number:02d} {pick.race_name[:14]:<14}  "
            f"選択{len(pick.picks)}頭: {picks_str}  "
            f"カバー{pick.cumul_prob*100:.1f}%"
        )
        if pick is not None:
            total_combos *= len(pick.picks)

    if all_ok:
        cost = total_combos * 100
        print()
        print(f"  合計組み合わせ数 : {total_combos} 点")
        print(f"  投資額           : ¥{cost:,}")
        # 理論期待払戻（全馬同確率と仮定した中央値推定）
        avg_probs = []
        for p in race_picks_list:
            if p and p.probs:
                avg_probs.append(sum(p.probs) / len(p.probs))
        if len(avg_probs) == 5:
            median_prob = math.prod(avg_probs)
            est_payout  = (1.0 / max(median_prob, 1e-10)) * _WIN5_RETURN_RATE * 100
            print(f"  理論期待払戻（≈） : ¥{est_payout:,.0f}")
    else:
        print()
        print(f"  ※ odds データが揃い次第、予想は更新されます")


# ── CLI ──────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="WIN5 累積勝率カバー法 + ウォークフォワードバックテスト"
    )
    p.add_argument(
        "--backtest", action="store_true",
        help="2026年全日曜バックテストを実行",
    )
    p.add_argument(
        "--next", action="store_true",
        help="次回日曜の WIN5 推奨馬を出力",
    )
    p.add_argument(
        "--date", default=None,
        help="対象日 YYYYMMDD（--next / --backtest なしで単日予想）",
    )
    p.add_argument(
        "--start", default="2026-01-01",
        help="バックテスト開始日 YYYY-MM-DD（デフォルト 2026-01-01）",
    )
    p.add_argument(
        "--end", default=None,
        help="バックテスト終了日 YYYY-MM-DD（デフォルト: 本日）",
    )
    p.add_argument(
        "--threshold", type=float, default=_DEFAULT_THRESHOLD,
        help=f"累積確率閾値 0〜1（デフォルト {_DEFAULT_THRESHOLD}）",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="バックテスト週別詳細を表示",
    )
    p.add_argument(
        "--research-db", default=None,
        help="Research DB パス（netkeiba オッズ補完・実際の WIN5 払戻用）",
    )
    p.add_argument(
        "--max-combos", type=int, default=5000,
        help="週あたりの最大組み合わせ数（超過週をスキップ）（デフォルト: 5000）",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    from pathlib import Path as _Path
    from src.database.init_db import init_db
    conn = init_db()

    research_db_path = _Path(args.research_db) if args.research_db else None

    try:
        if args.backtest:
            print("[WIN5] バックテスト実行中...")
            results = backtest_win5(
                conn,
                start_date=args.start,
                end_date=args.end,
                threshold=args.threshold,
                verbose=args.verbose,
                research_db_path=research_db_path,
                max_combinations=args.max_combos,
            )
            _print_backtest_summary(results)

        elif args.next:
            target = _next_sunday()
            predict_next_win5(conn, target, threshold=args.threshold)

        elif args.date:
            raw = args.date.replace("-", "")
            target = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            predict_next_win5(conn, target, threshold=args.threshold)

        else:
            # デフォルト: バックテスト + 次回予想
            print("[WIN5] バックテスト実行中...")
            results = backtest_win5(
                conn,
                start_date=args.start,
                end_date=args.end,
                threshold=args.threshold,
                verbose=False,
                research_db_path=research_db_path,
                max_combinations=args.max_combos,
            )
            _print_backtest_summary(results)

            print()
            target = _next_sunday()
            predict_next_win5(conn, target, threshold=args.threshold)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
