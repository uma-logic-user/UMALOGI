"""
年別ウォークフォワード検証スクリプト

2021-2025年を1年単位でロール:
  Train→Test: 2021→2022, 2022→2023, 2023→2024, 2024→2025

馬券種: 単勝 / 複勝 / 馬連 / 三連単（Harville 確率 × 実払戻）

使い方:
  py scripts/walk_forward_backtest.py
  py scripts/walk_forward_backtest.py --train-window 2  # 直近N年を学習に使用
  py scripts/walk_forward_backtest.py --bet-types 単勝 複勝
  py scripts/walk_forward_backtest.py --save-model       # 最終モデルを保存
"""

from __future__ import annotations

import argparse
import itertools
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_MAIN_DB = _ROOT / "data" / "umalogi.db"
_RESEARCH_DB = _ROOT / "data" / "netkeiba_research.db"
_EV_THRESHOLD_TANSHO = 1.5
_EV_THRESHOLD_FUKUSHO = 1.5
_EV_THRESHOLD_MULTI = 1.0  # 馬連・三連単は EV 1.0 超で buy
_BANKROLL = 100_000


# ── Harville ユーティリティ ──────────────────────────────────────────────


def _normalize(probs: list[float]) -> list[float]:
    total = sum(probs)
    if total <= 0:
        return [1.0 / len(probs)] * len(probs)
    return [p / total for p in probs]


def _harville_quinella(probs: list[float], i: int, j: int) -> float:
    """馬連 (quinella) P(i,j が1・2着、順不同)"""
    p = probs
    pi, pj = p[i], p[j]
    one_minus_pi = max(1.0 - pi, 1e-8)
    one_minus_pj = max(1.0 - pj, 1e-8)
    return pi * (pj / one_minus_pi) + pj * (pi / one_minus_pj)


def _harville_trifecta(probs: list[float], i: int, j: int, k: int) -> float:
    """三連単 P(i→j→k 順番通り)"""
    p = probs
    pi, pj, pk = p[i], p[j], p[k]
    d1 = max(1.0 - pi, 1e-8)
    d2 = max(1.0 - pi - pj, 1e-8)
    return pi * (pj / d1) * (pk / d2)


# ── 馬連バックテスト ─────────────────────────────────────────────────────

# JRA 馬連テイクアウト率 22.5%
_UMAREN_PAYOUT_RATE = 0.775
# JRA 三連単テイクアウト率 27.5%
_SANRENTAN_PAYOUT_RATE = 0.725


def _backtest_umaren(
    model,
    test_df: pd.DataFrame,
    conn: sqlite3.Connection,
    ev_threshold: float = _EV_THRESHOLD_MULTI,
    verbose: bool = True,
) -> dict:
    """
    馬連のウォークフォワードバックテスト（look-ahead バイアス修正版）。

    EV 計算: モデル Harville 確率 / 市場 Harville 確率 × 払戻率
      - 市場確率 = 単勝オッズから導出した Implied Harville 確率
      - 実際の払戻（act_payout > 0）は的中判定にのみ使用
      - EV 計算に実際の払戻を使わないため look-ahead バイアスなし
    """
    win_probs = model.predict_win_prob(test_df).values
    test_df = test_df.copy()
    test_df["win_prob"] = win_probs
    test_df["win_odds"] = (
        pd.to_numeric(test_df["win_odds"], errors="coerce")
        .fillna(50.0)
        .clip(lower=1.01)
    )

    # race_payouts から馬連払戻を取得（的中判定用）
    # DB フォーマット: "7-14"（ハイフン区切り、小番号先）
    race_ids_str = ",".join(f"'{r}'" for r in test_df["race_id"].unique())
    payout_df = pd.read_sql_query(
        f"""SELECT race_id, combination AS combo, payout
            FROM race_payouts
            WHERE bet_type = '馬連'
              AND race_id IN ({race_ids_str})""",
        conn,
    )
    payout_dict: dict[tuple[str, str], float] = {}
    for _, row in payout_df.iterrows():
        parts = str(row["combo"]).split("-")
        if len(parts) == 2:
            a, b = sorted([parts[0].strip(), parts[1].strip()])
            payout_dict[(row["race_id"], f"{a}-{b}")] = float(row["payout"])

    # 馬連払戻が存在するレース ID セット（正常に開催されたレースのみ対象）
    umaren_race_ids: set[str] = {k[0] for k in payout_dict}

    total_invest = 0
    total_payout = 0.0
    n_bets = 0
    n_hits = 0
    pnl_series: list[float] = []

    for race_id, grp in test_df.groupby("race_id"):
        if race_id not in umaren_race_ids:
            continue  # 馬連払戻なし（中止・障害等）
        horses = grp[["horse_number", "win_prob", "win_odds"]].dropna()
        if len(horses) < 2:
            continue

        horse_nums = horses["horse_number"].tolist()
        model_probs = _normalize(horses["win_prob"].tolist())
        # 市場 Implied 確率（単勝オッズの逆数を正規化）
        market_probs = _normalize([1.0 / o for o in horses["win_odds"].tolist()])

        for idx_i, idx_j in itertools.combinations(range(len(horse_nums)), 2):
            model_q = _harville_quinella(model_probs, idx_i, idx_j)
            market_q = max(_harville_quinella(market_probs, idx_i, idx_j), 1e-8)

            # EV = モデル確率 / 市場確率 × 払戻率（実際の払戻不使用）
            ev = model_q / market_q * _UMAREN_PAYOUT_RATE
            if ev < ev_threshold:
                continue

            a_num = str(int(horse_nums[idx_i]))
            b_num = str(int(horse_nums[idx_j]))
            key_str = "-".join(sorted([a_num, b_num]))
            act_payout = payout_dict.get((race_id, key_str), 0.0)

            bet = 100
            hit = 1 if act_payout > 0 else 0
            won = bet * act_payout / 100.0 if hit else 0.0

            total_invest += bet
            total_payout += won
            n_bets += 1
            n_hits += hit
            pnl_series.append(won - bet)

    roi = total_payout / total_invest * 100 if total_invest > 0 else 0.0
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0

    if pnl_series:
        cum = pd.Series(pnl_series).cumsum()
        max_dd = float((cum.cummax() - cum).max())
    else:
        max_dd = 0.0

    result = dict(
        bet_type="馬連",
        n_bets=n_bets,
        n_hits=n_hits,
        hit_rate=hit_rate,
        total_invest=total_invest,
        total_payout=total_payout,
        roi=roi,
        max_drawdown=max_dd,
    )
    if verbose:
        print(
            f"  馬連 : {n_bets:,}点 的中{n_hits}({hit_rate:.1f}%) "
            f"投資¥{total_invest:,} → 払戻¥{total_payout:,.0f} ROI={roi:.1f}%"
        )
    return result


# ── 三連単バックテスト ────────────────────────────────────────────────────


def _backtest_sanrentan(
    model,
    test_df: pd.DataFrame,
    conn: sqlite3.Connection,
    ev_threshold: float = _EV_THRESHOLD_MULTI,
    top_n: int = 8,
    verbose: bool = True,
) -> dict:
    """
    三連単のウォークフォワードバックテスト（look-ahead バイアス修正版）。

    EV 計算: モデル Harville 三連単確率 / 市場 Harville 確率 × 払戻率
    DB フォーマット: "14→7→16"（→区切り）→ 内部キーは "14-7-16" に変換
    top_n: 確率上位 N 頭に限定（組み合わせ爆発防止）
    """
    win_probs = model.predict_win_prob(test_df).values
    test_df = test_df.copy()
    test_df["win_prob"] = win_probs
    test_df["win_odds"] = (
        pd.to_numeric(test_df["win_odds"], errors="coerce")
        .fillna(50.0)
        .clip(lower=1.01)
    )

    race_ids_str = ",".join(f"'{r}'" for r in test_df["race_id"].unique())
    payout_df = pd.read_sql_query(
        f"""SELECT race_id, combination AS combo, payout
            FROM race_payouts
            WHERE bet_type = '三連単'
              AND race_id IN ({race_ids_str})""",
        conn,
    )
    payout_dict: dict[tuple[str, str], float] = {}
    for _, row in payout_df.iterrows():
        # "14→7→16" → "14-7-16"（内部キー形式に統一）
        key = str(row["combo"]).replace("→", "-")
        payout_dict[(row["race_id"], key)] = float(row["payout"])

    sanrentan_race_ids: set[str] = {k[0] for k in payout_dict}

    total_invest = 0
    total_payout = 0.0
    n_bets = 0
    n_hits = 0
    pnl_series: list[float] = []

    for race_id, grp in test_df.groupby("race_id"):
        if race_id not in sanrentan_race_ids:
            continue
        horses = grp[["horse_number", "win_prob", "win_odds"]].dropna()
        if len(horses) < 3:
            continue
        horses = horses.nlargest(top_n, "win_prob")
        horse_nums = horses["horse_number"].tolist()
        model_probs = _normalize(horses["win_prob"].tolist())
        market_probs = _normalize([1.0 / o for o in horses["win_odds"].tolist()])

        for idx_i, idx_j, idx_k in itertools.permutations(range(len(horse_nums)), 3):
            model_tri = _harville_trifecta(model_probs, idx_i, idx_j, idx_k)
            market_tri = max(
                _harville_trifecta(market_probs, idx_i, idx_j, idx_k), 1e-10
            )

            ev = model_tri / market_tri * _SANRENTAN_PAYOUT_RATE
            if ev < ev_threshold:
                continue

            a = str(int(horse_nums[idx_i]))
            b = str(int(horse_nums[idx_j]))
            c = str(int(horse_nums[idx_k]))
            key_str = f"{a}-{b}-{c}"
            act_payout = payout_dict.get((race_id, key_str), 0.0)

            bet = 100
            hit = 1 if act_payout > 0 else 0
            won = bet * act_payout / 100.0 if hit else 0.0

            total_invest += bet
            total_payout += won
            n_bets += 1
            n_hits += hit
            pnl_series.append(won - bet)

    roi = total_payout / total_invest * 100 if total_invest > 0 else 0.0
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0
    if pnl_series:
        cum = pd.Series(pnl_series).cumsum()
        max_dd = float((cum.cummax() - cum).max())
    else:
        max_dd = 0.0

    result = dict(
        bet_type="三連単",
        n_bets=n_bets,
        n_hits=n_hits,
        hit_rate=hit_rate,
        total_invest=total_invest,
        total_payout=total_payout,
        roi=roi,
        max_drawdown=max_dd,
    )
    if verbose:
        print(
            f"  三連単: {n_bets:,}点 的中{n_hits}({hit_rate:.1f}%) "
            f"投資¥{total_invest:,} → 払戻¥{total_payout:,.0f} ROI={roi:.1f}%"
        )
    return result


# ── ウォークフォワードメイン ──────────────────────────────────────────────


def run_walk_forward(
    bet_types: list[str],
    train_window: int,
    save_final_model: bool = False,
) -> list[dict]:
    """
    2021-2025年の年別ウォークフォワード検証を実行する。

    Returns: 全期間・全券種の結果リスト
    """
    from src.ml.alpha_model import AlphaModel, BET_TYPE_FUKUSHO, run_backtest

    conn = sqlite3.connect(str(_MAIN_DB))
    research_db = _RESEARCH_DB if _RESEARCH_DB.exists() else None

    # 利用可能な年を確認
    available_years_row = conn.execute(
        "SELECT DISTINCT strftime('%Y', date) as yr FROM races ORDER BY yr"
    ).fetchall()
    available_years = [int(r[0]) for r in available_years_row if r[0]]

    # ウォークフォワードの期間設定（最小: 2021→2022）
    all_test_years = [y for y in available_years if y >= 2022]
    results: list[dict] = []

    print(f"\n{'=' * 65}")
    print("  UMALOGI ハイブリッド ウォークフォワード検証")
    print(f"  train_window={train_window}年 | 券種: {', '.join(bet_types)}")
    print(f"  Research DB: {'あり' if research_db else 'なし'}")
    print(f"{'=' * 65}\n")

    for test_year in all_test_years:
        # 学習データ: test_year 直前の train_window 年
        train_start = test_year - train_window
        train_years = [y for y in available_years if train_start <= y < test_year]
        if not train_years:
            continue

        print(f"{'─' * 65}")
        print(f"  [{test_year}年テスト]  学習: {train_years}")
        print(f"{'─' * 65}")

        # 単勝・複勝は run_backtest() を再利用
        for bt in bet_types:
            if bt not in ("単勝", "複勝"):
                continue
            try:
                threshold = (
                    _EV_THRESHOLD_TANSHO if bt == "単勝" else _EV_THRESHOLD_FUKUSHO
                )
                r = run_backtest(
                    conn=conn,
                    train_years=train_years,
                    test_years=[test_year],
                    bet_type=bt,
                    ev_threshold=threshold,
                    bankroll=_BANKROLL,
                    verbose=True,
                    research_db_path=research_db,
                )
                results.append(
                    {
                        "test_year": test_year,
                        "train_years": str(train_years),
                        "bet_type": bt,
                        "n_bets": r.num_bets,
                        "n_hits": r.num_hits,
                        "hit_rate": r.hit_rate,
                        "total_invest": r.total_investment,
                        "total_payout": r.total_payout,
                        "roi": r.roi,
                        "max_drawdown": r.max_drawdown,
                    }
                )
            except Exception as e:
                print(f"  [{bt}] エラー: {e}")

        # 馬連・三連単は Harville バックテスト
        multi_types = [bt for bt in bet_types if bt in ("馬連", "三連単")]
        if multi_types:
            # テスト用モデルを再学習（複勝モデルで win_prob を推定）
            model = AlphaModel()
            try:
                train_df = model.load_training_data(
                    conn,
                    train_years,
                    BET_TYPE_FUKUSHO,
                    research_db_path=research_db,
                )
                if len(train_df) < 500:
                    print(
                        f"  [馬連/三連単] 学習データ不足 ({len(train_df)}行) → スキップ"
                    )
                else:
                    model.train(train_df)
                    test_df = model.load_training_data(
                        conn,
                        [test_year],
                        BET_TYPE_FUKUSHO,
                        research_db_path=research_db,
                    )
                    if len(test_df) < 50:
                        print(
                            f"  [馬連/三連単] テストデータ不足 ({len(test_df)}行) → スキップ"
                        )
                    else:
                        if "馬連" in multi_types:
                            r2 = _backtest_umaren(model, test_df, conn, verbose=True)
                            results.append(
                                {
                                    "test_year": test_year,
                                    "train_years": str(train_years),
                                    **r2,
                                }
                            )
                        if "三連単" in multi_types:
                            r3 = _backtest_sanrentan(model, test_df, conn, verbose=True)
                            results.append(
                                {
                                    "test_year": test_year,
                                    "train_years": str(train_years),
                                    **r3,
                                }
                            )
            except Exception as e:
                print(f"  [馬連/三連単] エラー: {e}")

        # 最終年モデルを保存
        if save_final_model and test_year == max(all_test_years):
            model_final = AlphaModel()
            try:
                all_years_so_far = [y for y in available_years if y < test_year]
                df_all = model_final.load_training_data(
                    conn,
                    all_years_so_far,
                    BET_TYPE_FUKUSHO,
                    research_db_path=research_db,
                )
                model_final.train(df_all)
                model_final.save()
                print("\n  [保存] 最終モデル保存: data/models/alpha/alpha_model.pkl")
            except Exception as e:
                print(f"  [保存] エラー: {e}")

    conn.close()
    return results


# ── レポート出力 ─────────────────────────────────────────────────────────


def print_report(results: list[dict]) -> None:
    if not results:
        print("[WARN] 結果なし")
        return

    df = pd.DataFrame(results)

    print(f"\n{'=' * 75}")
    print("  UMALOGI ハイブリッド ウォークフォワード 年別レポート")
    print(f"{'=' * 75}")

    for bet_type in df["bet_type"].unique():
        sub = df[df["bet_type"] == bet_type]
        print(f"\n【{bet_type}】")
        print(
            f"  {'年度':<6} {'学習年':<20} {'買い点':<8} {'的中率':<7} {'投資':>12} {'払戻':>12} {'ROI':>7} {'最大DD':>12}"
        )
        print(f"  {'-' * 72}")
        for _, row in sub.iterrows():
            invest_str = f"¥{int(row['total_invest']):,}"
            payout_str = f"¥{int(row['total_payout']):,}"
            dd_str = (
                f"¥{int(row['max_drawdown']):,}"
                if pd.notna(row.get("max_drawdown"))
                else "—"
            )
            roi_mark = "✅" if row["roi"] >= 100 else "❌"
            print(
                f"  {row['test_year']:<6} "
                f"{str(row['train_years']):<20} "
                f"{int(row['n_bets']):>7,} "
                f"{row['hit_rate']:>6.1f}% "
                f"{invest_str:>12} "
                f"{payout_str:>12} "
                f"{row['roi']:>6.1f}%{roi_mark} "
                f"{dd_str:>12}"
            )

    # 通算サマリー
    print(f"\n{'=' * 75}")
    print("  通算サマリー")
    print(f"{'=' * 75}")
    for bet_type in df["bet_type"].unique():
        sub = df[df["bet_type"] == bet_type]
        tot_inv = sub["total_invest"].sum()
        tot_pay = sub["total_payout"].sum()
        tot_roi = tot_pay / tot_inv * 100 if tot_inv > 0 else 0
        years_ok = (sub["roi"] >= 100).sum()
        total_y = len(sub)
        mark = "✅" if tot_roi >= 110 else ("⚠️ " if tot_roi >= 100 else "❌")
        print(
            f"  {bet_type:<8} 通算ROI={tot_roi:.1f}% {mark}  "
            f"(ROI100%超: {years_ok}/{total_y}年)  "
            f"投資¥{int(tot_inv):,} 払戻¥{int(tot_pay):,}"
        )

    # HYBRID_STRATEGY 推奨
    print(f"\n{'=' * 75}")
    print("  UMALOGI_HYBRID_STRATEGY 推奨判定")
    print(f"{'=' * 75}")
    for bet_type in df["bet_type"].unique():
        sub = df[df["bet_type"] == bet_type]
        tot_inv = sub["total_invest"].sum()
        tot_pay = sub["total_payout"].sum()
        tot_roi = tot_pay / tot_inv * 100 if tot_inv > 0 else 0
        years_ok = int((sub["roi"] >= 100).sum())
        total_y = len(sub)
        if tot_roi >= 110 and years_ok >= total_y // 2:
            print(
                f"  {bet_type}: 【採用】全期間ROI={tot_roi:.1f}% ≥ 110% かつ {years_ok}/{total_y}年でROI100%超"
            )
        elif tot_roi >= 100:
            print(
                f"  {bet_type}: 【条件付き採用】ROI={tot_roi:.1f}% (100-110%帯) — 慎重に運用"
            )
        else:
            print(f"  {bet_type}: 【不採用】ROI={tot_roi:.1f}% < 100% — 改善が必要")


# ── main ─────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="年別ウォークフォワード検証 (2021-2025)"
    )
    parser.add_argument(
        "--bet-types",
        nargs="*",
        default=["単勝", "複勝", "馬連", "三連単"],
        help="検証する馬券種（スペース区切り）",
    )
    parser.add_argument(
        "--train-window",
        type=int,
        default=3,
        help="学習に使う年数（デフォルト: 3年）",
    )
    parser.add_argument(
        "--save-model",
        action="store_true",
        help="最終期間のモデルを data/models/alpha/ に保存する",
    )
    args = parser.parse_args()

    results = run_walk_forward(
        bet_types=args.bet_types,
        train_window=args.train_window,
        save_final_model=args.save_model,
    )
    print_report(results)


if __name__ == "__main__":
    main()
