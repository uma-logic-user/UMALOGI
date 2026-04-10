"""
UMALOGI AI -- オフライン・バックテスト スクリプト

学習済みモデルをロードし、指定年のデータに対してオフラインで
単勝 / 複勝 / 馬連 / 三連複 の買い目をシミュレートし、
回収率・的中率を報告する。

使用例:
    py scripts/simulate_year.py                     # 2024年全レース
    py scripts/simulate_year.py --year 2023
    py scripts/simulate_year.py --venue 東京 --surface 芝
    py scripts/simulate_year.py --bet honmei_tansho
    py scripts/simulate_year.py --dry-run           # DB統計のみ（予測なし）
    py scripts/simulate_year.py --verbose
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

# ── プロジェクトルートを sys.path に追加 ─────────────────────────
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.database.init_db import get_db_path, init_db
from src.evaluation.evaluator import (
    _build_combination_key,
    _fetch_payouts,
    _fetch_horse_numbers,
    _is_hit,
    _lookup_payout,
)
from src.ml.features import FeatureBuilder
from src.ml.models import load_models


def kelly_fraction(p_win: float, odds: float, multiplier: float = 0.1) -> float:
    """
    フラクショナル・ケリー基準による賭け比率を返す。

    f* = (p * b - q) / b  (b = odds - 1, q = 1 - p)
    返り値 = max(0, f*) × multiplier (デフォルト 1/10 ケリー)
    """
    if odds <= 1.0 or p_win <= 0.0:
        return 0.0
    b = odds - 1.0
    q = 1.0 - p_win
    f_star = (p_win * b - q) / b
    return max(0.0, f_star * multiplier)

logger = logging.getLogger(__name__)

_WIDTH = 62

# 賭け金（固定100円/買い目）
_BET_AMOUNT = 100


# ════════════════════════════════════════════════════════════════
#  買い目戦略定義
# ════════════════════════════════════════════════════════════════

STRATEGIES: dict[str, dict] = {
    # 本命モデル: score 1位の馬の単勝
    "honmei_tansho": {
        "label":    "本命・単勝  (Top1)",
        "model":    "honmei",
        "bet_type": "単勝",
        "n_picks":  1,
    },
    # 本命モデル: score 1-2位の馬連
    "honmei_umaren": {
        "label":    "本命・馬連  (Top2)",
        "model":    "honmei",
        "bet_type": "馬連",
        "n_picks":  2,
    },
    # 本命モデル: score 1-3位の三連複
    "honmei_sanrenpuku": {
        "label":    "本命・三連複 (Top3)",
        "model":    "honmei",
        "bet_type": "三連複",
        "n_picks":  3,
    },
    # 卍モデル: ev_score > 1.0 の馬の単勝（最大1頭）
    "manji_tansho": {
        "label":    "卍・単勝    (EV>1.0)",
        "model":    "manji",
        "bet_type": "単勝",
        "n_picks":  1,
        "ev_filter": True,
    },
    # 卍モデル: ev_score > 1.0 の馬の複勝（最大1頭）
    "manji_fukusho": {
        "label":    "卍・複勝    (EV>1.0)",
        "model":    "manji",
        "bet_type": "複勝",
        "n_picks":  1,
        "ev_filter": True,
    },
    # EV戦略: EV = P(win)×odds >= 1.0 の馬の単勝（1/10ケリー）
    "ev_tansho": {
        "label":    "EV・単勝   (p×odds≥1.0)",
        "model":    "ev_honmei",
        "bet_type": "単勝",
        "n_picks":  1,
        "ev_filter": True,
    },
}


# ════════════════════════════════════════════════════════════════
#  表示ユーティリティ
# ════════════════════════════════════════════════════════════════

def _banner(text: str) -> None:
    inner  = f"  {text}  "
    pad    = max(0, _WIDTH - 2 - len(inner))
    left   = pad // 2
    right  = pad - left
    border = "=" * _WIDTH
    print(f"\n{border}")
    print(f"|{' ' * left}{inner}{' ' * right}|")
    print(f"{border}")


def _section(text: str) -> None:
    print(f"\n{'- ' * (_WIDTH // 2)}")
    print(f"  {text}")
    print(f"{'- ' * (_WIDTH // 2)}")


def _kv_table(rows: list[tuple[str, str]], title: str = "") -> None:
    if not rows:
        return
    if title:
        print(f"\n  {title}")
    col_w = max(len(k) for k, _ in rows) + 2
    val_w = max(len(v) for _, v in rows) + 2
    sep   = f"  +{'-' * col_w}+{'-' * val_w}+"
    print(sep)
    for k, v in rows:
        print(f"  | {k:<{col_w - 2}} | {v:>{val_w - 2}} |")
    print(sep)


def _result_table(
    headers: list[str],
    rows: list[list[str]],
    title: str = "",
) -> None:
    """汎用テーブル表示。"""
    if not rows:
        return
    if title:
        print(f"\n  {title}")
    widths = [max(len(h), max((len(r[i]) for r in rows), default=0)) + 2
              for i, h in enumerate(headers)]
    sep = "  +" + "+".join("-" * w for w in widths) + "+"
    header_line = "  |" + "|".join(
        f" {h:<{w - 2}} " for h, w in zip(headers, widths)
    ) + "|"
    print(sep)
    print(header_line)
    print(sep)
    for row in rows:
        print("  |" + "|".join(
            f" {c:<{w - 2}} " for c, w in zip(row, widths)
        ) + "|")
    print(sep)


# ════════════════════════════════════════════════════════════════
#  集計用データクラス
# ════════════════════════════════════════════════════════════════

class StrategyStats:
    """1戦略の集計状態。"""

    def __init__(self, label: str, bet_type: str) -> None:
        self.label     = label
        self.bet_type  = bet_type
        self.races     = 0      # 買い目が発生したレース数
        self.hits      = 0      # 的中数
        self.invested  = 0.0   # 投資額計（円）
        self.payout    = 0.0   # 回収額計（円）
        self.skipped   = 0     # ev_filter で見送ったレース数
        # ケリー基準シミュレーション（初期資金 100,000円）
        self.kelly_bankroll = 100_000.0
        self.kelly_invested = 0.0
        self.kelly_payout   = 0.0

    def add(self, hit: bool, payout: float, kelly_frac: float = 0.0) -> None:
        self.races    += 1
        self.hits     += int(hit)
        self.invested += _BET_AMOUNT
        self.payout   += payout
        # ケリー基準（実際の賭け額 = kelly_frac × bankroll, 最低100円）
        bet_kelly = max(100.0, round(kelly_frac * self.kelly_bankroll / 100) * 100)
        self.kelly_invested += bet_kelly
        if hit:
            self.kelly_payout   += bet_kelly * (payout / _BET_AMOUNT)
            self.kelly_bankroll += bet_kelly * (payout / _BET_AMOUNT - 1)
        else:
            self.kelly_bankroll -= bet_kelly

    @property
    def roi(self) -> float:
        return (self.payout / self.invested * 100) if self.invested > 0 else 0.0

    @property
    def hit_rate(self) -> float:
        return (self.hits / self.races * 100) if self.races > 0 else 0.0

    def summary_row(self) -> list[str]:
        kelly_roi = (self.kelly_payout / self.kelly_invested * 100) if self.kelly_invested > 0 else 0.0
        return [
            self.label,
            f"{self.races:,}",
            f"{self.hits:,}",
            f"{self.hit_rate:.1f}%",
            f"{int(self.invested):,}",
            f"{int(self.payout):,}",
            f"{self.roi:.1f}%",
            f"{int(self.kelly_bankroll):,}",
            f"{kelly_roi:.1f}%",
        ]


# ════════════════════════════════════════════════════════════════
#  バックテストコア
# ════════════════════════════════════════════════════════════════

def _get_race_ids(conn, year: str, venue: str | None, surface: str | None) -> list[tuple]:
    """フィルタ条件に合うレース一覧を返す。"""
    clauses = ["r.race_id IS NOT NULL"]
    params: list[Any] = []

    if year:
        clauses.append("substr(r.date, 1, 4) = ?")
        params.append(year)
    if venue:
        clauses.append("r.venue = ?")
        params.append(venue)
    if surface:
        clauses.append("r.surface = ?")
        params.append(surface)

    # race_results が 1件以上あるレースのみ
    clauses.append(
        "EXISTS (SELECT 1 FROM race_results rr WHERE rr.race_id = r.race_id AND rr.rank IS NOT NULL)"
    )

    where = " AND ".join(clauses)
    rows = conn.execute(
        f"SELECT r.race_id, r.date, r.venue, r.distance, r.surface "
        f"FROM races r WHERE {where} ORDER BY r.date, r.race_id",
        params,
    ).fetchall()
    return rows


def _distance_band(distance: int) -> str:
    if distance < 1400:
        return "sprint"
    elif distance < 1800:
        return "mile"
    elif distance < 2200:
        return "intermediate"
    else:
        return "long"


def _select_horses(
    df,
    strategy: dict,
    honmei_model,
    manji_model,
) -> tuple[list[str], list[float]]:
    """
    戦略に応じて予想馬名リストとケリー比率リストを返す。

    Returns:
        (picks, kelly_fracs) - 買い目なしは ([], [])
    """
    if df.empty:
        return [], []

    model_key   = strategy["model"]
    n_picks     = strategy["n_picks"]
    ev_filter   = strategy.get("ev_filter", False)

    if model_key == "honmei":
        scores = honmei_model.predict(df)
    elif model_key == "ev_honmei":
        scores = honmei_model.ev_predict(df)   # EV = P(win) × odds
    else:
        scores = manji_model.ev_score(df)

    df2 = df.copy()
    df2["_score"] = scores.values

    if ev_filter:
        # EV > 1.0 のみ対象（回収期待値がプラスの馬）
        df2 = df2[df2["_score"] > 1.0]
        if df2.empty:
            return [], []

    df2 = df2.sort_values("_score", ascending=False)
    top = df2.head(n_picks)
    picks = top["horse_name"].tolist()

    if len(picks) < n_picks and not ev_filter:
        return [], []  # 頭数不足（三連複なのに3頭未満など）

    # ケリー比率計算（単勝戦略のみ意味を持つ）
    kelly_fracs: list[float] = []
    for _, row in top.iterrows():
        try:
            p   = float(honmei_model.predict(df2.loc[[row.name]]).iloc[0]) if model_key != "honmei" else float(row["_score"])
            odds = float(row.get("win_odds") or 0)
            kelly_fracs.append(kelly_fraction(p, odds))
        except Exception:
            kelly_fracs.append(0.0)

    return picks, kelly_fracs


def _run_backtest(
    conn,
    race_rows: list[tuple],
    honmei_model,
    manji_model,
    strategies: dict[str, dict],
    verbose: bool = False,
) -> tuple[
    dict[str, StrategyStats],
    dict[str, dict[str, StrategyStats]],   # monthly
    dict[str, dict[str, StrategyStats]],   # venue
    dict[str, dict[str, StrategyStats]],   # dist_band
]:
    fb = FeatureBuilder(conn)

    # 集計コンテナ
    overall:   dict[str, StrategyStats] = {}
    monthly:   dict[str, dict[str, StrategyStats]] = defaultdict(dict)
    by_venue:  dict[str, dict[str, StrategyStats]] = defaultdict(dict)
    by_dist:   dict[str, dict[str, StrategyStats]] = defaultdict(dict)

    def _make_stats(strat_key: str) -> StrategyStats:
        s = strategies[strat_key]
        return StrategyStats(label=s["label"], bet_type=s["bet_type"])

    for strat_key in strategies:
        overall[strat_key] = _make_stats(strat_key)

    total_races = len(race_rows)
    for i, (race_id, date, venue, distance, surface) in enumerate(race_rows, 1):
        if verbose or i % 100 == 0:
            print(f"  [{i:>4}/{total_races}] {race_id} {date} {venue}", flush=True)

        month    = date[:7].replace("/", "-")  # "2024-01"
        dist_key = _distance_band(distance)

        # 特徴量構築（リーク除外）
        try:
            df = fb.build_race_features_for_simulate(race_id)
        except Exception as exc:
            logger.warning("特徴量生成失敗 race_id=%s: %s", race_id, exc)
            continue

        if df.empty:
            continue

        # 払戻・馬番マップ
        payouts       = _fetch_payouts(conn, race_id)
        horse_numbers = _fetch_horse_numbers(conn, race_id)

        # horse_number が NULL のレース向けに popularity ベースの払戻マップも構築
        # race_payouts.popularity は単勝/複勝では馬固有の人気順を保持する
        payouts_by_pop: dict[tuple[str, int], int] = {}
        for (bt, _comb), pay in payouts.items():
            rows_pop = conn.execute(
                "SELECT popularity FROM race_payouts "
                "WHERE race_id = ? AND bet_type = ? AND combination = ?",
                (race_id, bt, _comb),
            ).fetchone()
            if rows_pop and rows_pop[0] is not None:
                payouts_by_pop[(bt, rows_pop[0])] = pay

        # 馬名 → popularity マップ（df から構築）
        name_to_pop: dict[str, int] = {}
        if "popularity" in df.columns:
            for _, dfrow in df.iterrows():
                pop = dfrow.get("popularity")
                try:
                    if pop is not None and not (isinstance(pop, float) and math.isnan(pop)):
                        name_to_pop[dfrow["horse_name"]] = int(pop)
                except (ValueError, TypeError):
                    pass

        result_map    = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT horse_name, rank FROM race_results WHERE race_id = ?",
                (race_id,),
            ).fetchall()
        }

        for strat_key, strat in strategies.items():
            picks, kelly_fracs = _select_horses(df, strat, honmei_model, manji_model)

            if not picks:
                overall[strat_key].skipped += 1
                continue

            bet_type = strat["bet_type"]
            comb_key = _build_combination_key(bet_type, picks, horse_numbers)
            hit      = _is_hit(bet_type, picks, result_map)

            # 払戻取得: combo_key が None (horse_number 未登録) の場合は
            # 単勝/複勝 は popularity ベースで代替ルックアップ
            if comb_key is not None:
                payout = _lookup_payout(bet_type, comb_key, payouts)
            elif hit and bet_type in ("単勝", "複勝") and len(picks) == 1:
                pop = name_to_pop.get(picks[0])
                payout = payouts_by_pop.get((bet_type, pop), 0) if pop else 0
            else:
                payout = 0

            kf = kelly_fracs[0] if kelly_fracs else 0.0
            overall[strat_key].add(hit, float(payout), kelly_frac=kf)

            # 月別
            if strat_key not in monthly[month]:
                monthly[month][strat_key] = _make_stats(strat_key)
            monthly[month][strat_key].add(hit, float(payout), kf)

            # 会場別
            if strat_key not in by_venue[venue]:
                by_venue[venue][strat_key] = _make_stats(strat_key)
            by_venue[venue][strat_key].add(hit, float(payout), kf)

            # 距離帯別
            if strat_key not in by_dist[dist_key]:
                by_dist[dist_key][strat_key] = _make_stats(strat_key)
            by_dist[dist_key][strat_key].add(hit, float(payout), kf)

    return overall, dict(monthly), dict(by_venue), dict(by_dist)


# ════════════════════════════════════════════════════════════════
#  結果表示
# ════════════════════════════════════════════════════════════════

_SUMMARY_HEADERS = ["戦略", "レース数", "的中", "的中率", "投資(円)", "回収(円)", "ROI", "Kelly残高", "Kelly-ROI"]


def _print_summary(overall: dict[str, StrategyStats]) -> None:
    _section("全体サマリー")
    rows = [s.summary_row() for s in overall.values()]
    _result_table(_SUMMARY_HEADERS, rows, title="")


def _print_breakdown(
    breakdown: dict[str, dict[str, StrategyStats]],
    dimension: str,
    strat_keys: list[str],
) -> None:
    _section(f"{dimension}別・内訳")
    for group_key in sorted(breakdown.keys()):
        stats_map = breakdown[group_key]
        rows = []
        for sk in strat_keys:
            if sk in stats_map:
                rows.append(stats_map[sk].summary_row())
        if rows:
            _result_table(_SUMMARY_HEADERS, rows, title=f"  [{group_key}]")


# ════════════════════════════════════════════════════════════════
#  エントリポイント
# ════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description="UMALOGI AI オフライン・バックテスト",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db",      type=Path, default=None,
                        help="DB ファイルパス（デフォルト: DB_PATH 環境変数 or data/umalogi.db）")
    parser.add_argument("--year",    type=str,  default="2024",
                        help="バックテスト対象年 (default: 2024)")
    parser.add_argument("--venue",   type=str,  default=None,
                        help="会場フィルタ (例: 東京)")
    parser.add_argument("--surface", type=str,  default=None,
                        help="馬場フィルタ (例: 芝 / ダート)")
    parser.add_argument("--bet",     type=str,  default=None,
                        choices=list(STRATEGIES.keys()),
                        help="単一戦略のみ実行（省略時は全戦略）")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run",
                        help="DB 統計のみ表示して終了（予測なし）")
    parser.add_argument("--verbose", action="store_true",
                        help="各レースの進捗を表示する")
    parser.add_argument("--force", action="store_true",
                        help="強制実行フラグ（未学習モデルでも実行を続ける）")
    args = parser.parse_args()

    # Windows UTF-8 対応
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("lightgbm").setLevel(logging.WARNING)

    _banner("UMALOGI AI  --  Offline Backtest")

    # DB 接続
    db_path = args.db or get_db_path()
    print(f"\n  DB   : {db_path}")

    if not Path(db_path).exists():
        print(f"\n  [NG] DB ファイルが見つかりません: {db_path}")
        return 1

    conn = init_db(db_path=Path(db_path))

    # 対象レース取得
    race_rows = _get_race_ids(conn, args.year, args.venue, args.surface)
    n_races   = len(race_rows)
    print(f"  対象 : {args.year}年  会場={args.venue or '全'}  馬場={args.surface or '全'}")
    print(f"  レース数: {n_races:,}")

    if n_races == 0:
        print("\n  [NG] 条件に合うレースが 0 件です。")
        conn.close()
        return 1

    if args.dry_run:
        print("\n  --dry-run: DB 統計のみ表示して終了します。")
        conn.close()
        return 0

    # モデルロード
    print("\n  モデルをロード中 ...")
    honmei_model, manji_model = load_models()
    print(f"  本命モデル: {'学習済み' if honmei_model.is_trained else 'フォールバック(未学習)'}")
    print(f"  卍モデル  : {'学習済み' if manji_model.is_trained else 'フォールバック(未学習)'}")

    # 実行する戦略を絞り込む
    strategies = {args.bet: STRATEGIES[args.bet]} if args.bet else STRATEGIES

    _section("バックテスト実行中 ...")
    wall_start = time.perf_counter()

    overall, monthly, by_venue, by_dist = _run_backtest(
        conn, race_rows, honmei_model, manji_model, strategies, verbose=args.verbose
    )

    conn.close()
    elapsed = time.perf_counter() - wall_start
    mins, secs = divmod(int(elapsed), 60)
    elapsed_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"

    # ── 結果表示 ────────────────────────────────────────────────
    _banner(f"Backtest Results  ({args.year}  elapsed: {elapsed_str})")

    _print_summary(overall)

    strat_keys = list(strategies.keys())
    _print_breakdown(monthly,  "月別",   strat_keys)
    _print_breakdown(by_venue, "会場別", strat_keys)
    _print_breakdown(by_dist,  "距離帯別", strat_keys)

    # 簡易総評
    _section("総評")
    best_roi_key = max(overall, key=lambda k: overall[k].roi)
    best = overall[best_roi_key]
    print(f"  最高ROI戦略: {best.label}")
    print(f"    ROI={best.roi:.1f}%  的中率={best.hit_rate:.1f}%  "
          f"レース数={best.races:,}")
    if best.roi >= 100:
        print("  [OK] 黒字戦略が存在します。")
    else:
        print("  [--] 全戦略が赤字です。さらなるモデル改善を検討してください。")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
