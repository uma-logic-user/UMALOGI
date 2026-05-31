"""  # noqa: E501
scripts/calibration_kelly_audit.py
===================================
Step 1: 本命(直前)モデルのキャリブレーション分析
  - model_score（予測勝率）と実際の的中率の乖離を bin 分析
  - 補正関数 corrected_probability() を導出
  - 必要なら Platt Scaling 再適用を勧告

Step 2: ケリー基準 Monte Carlo シミュレーション
  - 過去の全ベット履歴（prediction_results）を使用
  - 1/8 / 1/4 / 1/2 / Full Kelly を比較
  - 各 10,000 試行でドローダウン・破産確率を算出

Usage:
  py scripts/calibration_kelly_audit.py [--db data/umalogi.db] [--trials 10000]
  py scripts/calibration_kelly_audit.py --out data/calibration_kelly_audit.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Windows CP932 コンソールでも正しく出力するため UTF-8 に強制
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

# ── 定数 ────────────────────────────────────────────────────────────────────
MT_HONMEI_IMMEDIATE = "本命(直前)"

# Monte Carlo パラメーター
INITIAL_BALANCE: float = 100_000.0    # 初期資金 (円)
BANKRUPT_THRESHOLD: float = 0.10      # 破産ライン = 初期資金の 10%
DEFAULT_TRIALS: int = 10_000
KELLY_SCALES: dict[str, float] = {
    "1/8 Kelly": 0.5,
    "1/4 Kelly": 1.0,   # recommended_bet はこの基準
    "1/2 Kelly": 2.0,
    "Full Kelly": 4.0,
}
BIN_WIDTH: float = 0.05              # キャリブレーション bin 幅


# ══════════════════════════════════════════════════════════════════════════════
# Step 1: キャリブレーション分析
# ══════════════════════════════════════════════════════════════════════════════

def _load_calibration_data(conn: sqlite3.Connection) -> list[tuple[float, int]]:
    """(model_score, is_winner) ペアをすべて取得する。

    is_winner=1 → その馬が実際に1着。
    対象: 本命(直前)モデル × model_score ∈ (0, 1] × race_results.rank 確定済み。
    """
    rows = conn.execute(
        """
        SELECT ph.model_score,
               CASE WHEN rr.rank = 1 THEN 1 ELSE 0 END AS is_winner
        FROM prediction_horses ph
        JOIN predictions p  ON ph.prediction_id = p.id
        JOIN race_results rr ON p.race_id = rr.race_id
                             AND ph.horse_id  = rr.horse_id
        WHERE p.model_type = ?
          AND ph.model_score > 0.001
          AND ph.model_score <= 1.0
          AND rr.rank IS NOT NULL
          AND rr.rank  > 0
        """,
        (MT_HONMEI_IMMEDIATE,),
    ).fetchall()
    return [(float(r[0]), int(r[1])) for r in rows]


def _build_calibration_table(
    data: list[tuple[float, int]],
    bin_width: float = BIN_WIDTH,
) -> list[dict]:
    """model_score を bin_width 刻みでまとめ、各 bin の統計を返す。

    Returns:
        [{"bin_center", "predicted", "actual", "count", "gap"}, ...]
    """
    if not data:
        return []

    scores = np.array([d[0] for d in data])
    winners = np.array([d[1] for d in data])

    bins = np.arange(0.0, 1.0 + bin_width, bin_width)
    table = []
    for lo, hi in zip(bins[:-1], bins[1:]):
        mask = (scores >= lo) & (scores < hi)
        cnt = mask.sum()
        if cnt == 0:
            continue
        predicted = float(scores[mask].mean())
        actual = float(winners[mask].mean())
        table.append(
            {
                "bin_lo": round(lo, 3),
                "bin_hi": round(hi, 3),
                "bin_center": round((lo + hi) / 2, 3),
                "predicted": round(predicted, 4),
                "actual": round(actual, 4),
                "count": int(cnt),
                "gap": round(actual - predicted, 4),
            }
        )
    return table


def _fit_correction_table(
    cal_table: list[dict],
) -> list[dict[str, float]]:
    """bin ごとの補正倍率テーブルを返す。

    correction_factor = actual / predicted (予測が 0 に近い bin は除外)
    """
    result = []
    for row in cal_table:
        if row["predicted"] < 0.005 or row["count"] < 10:
            continue
        cf = row["actual"] / row["predicted"]
        result.append(
            {
                "bin_center": row["bin_center"],
                "predicted": row["predicted"],
                "actual": row["actual"],
                "correction_factor": round(cf, 3),
                "count": row["count"],
            }
        )
    return result


def _print_calibration(cal_table: list[dict], correction: list[dict]) -> None:
    """キャリブレーション結果をコンソールに表示する。"""
    print("\n" + "=" * 65)
    print("  Step 1: 本命(直前) モデル キャリブレーション分析")
    print("=" * 65)
    print(f"{'bin':>10}  {'予測確率':>8}  {'実的中率':>8}  {'件数':>6}  {'乖離':>7}  {'バー'}")
    print("-" * 65)
    for row in cal_table:
        gap = row["gap"]
        sign = "▲" if gap > 0 else "▼" if gap < 0 else " "
        bar_len = min(int(abs(gap) * 200), 20)
        bar = sign * bar_len
        print(
            f"  {row['bin_lo']:.2f}-{row['bin_hi']:.2f}"
            f"  {row['predicted']:8.3f}"
            f"  {row['actual']:8.3f}"
            f"  {row['count']:6d}"
            f"  {gap:+7.3f}"
            f"  {bar}"
        )
    print()

    if not correction:
        print("  ※ 補正テーブル生成対象となる bin がありません（件数不足）")
        return

    total_gap = sum(abs(r["gap"]) for r in cal_table) / len(cal_table)
    print(f"  平均絶対誤差 (MAE): {total_gap:.4f}")
    print()
    print("  【補正テーブル（actual/predicted）】")
    print(f"  {'bin_center':>10}  {'予測':>7}  {'実績':>7}  {'補正倍率':>8}  {'件数':>6}")
    print("  " + "-" * 50)
    for row in correction:
        flag = " [!]" if abs(row["correction_factor"] - 1.0) > 0.3 else ""
        print(
            f"  {row['bin_center']:10.3f}"
            f"  {row['predicted']:7.3f}"
            f"  {row['actual']:7.3f}"
            f"  {row['correction_factor']:8.3f}{flag}"
            f"  {row['count']:6d}"
        )

    mean_cf = np.mean([r["correction_factor"] for r in correction])
    print(f"\n  全 bin 平均補正倍率: {mean_cf:.3f}")
    if mean_cf > 1.15:
        print("  → モデルが全体的に確率を【過小評価】しています。")
        print("    EV 計算に用いる確率に補正倍率を乗じることを推奨します。")
    elif mean_cf < 0.85:
        print("  → モデルが全体的に確率を【過大評価】しています。")
        print("    EV 計算は実際より甘くなっている可能性があります。")
    else:
        print("  → キャリブレーションは概ね良好です（補正倍率 ≈ 1.0）。")


# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Kelly Monte Carlo シミュレーション
# ══════════════════════════════════════════════════════════════════════════════

def _load_kelly_data(conn: sqlite3.Connection) -> tuple[np.ndarray, float, float]:
    """recommended_bet 基準の実績利益を計算して返す。

    evaluator は `invested = n_combos × ¥100` 固定で profit を計算するが、
    実運用では `recommended_bet` が実際の投資額。combination_json のコンボ数から
    スケール係数を求め actual_profit を再計算する。

    scale = recommended_bet / (n_combos × 100)
    actual_profit:
      - not hit: -recommended_bet
      - hit:      pr.payout × scale - recommended_bet

    Returns:
        (data, total_invested, total_actual_payout) のタプル。
        data: shape=(N, 2) ndarray: col0=recommended_bet, col1=actual_profit
    """
    import json as _json

    rows = conn.execute(
        """
        SELECT p.recommended_bet, pr.is_hit, pr.payout, p.combination_json
        FROM predictions p
        JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE p.recommended_bet > 0
          AND pr.is_hit IS NOT NULL
        ORDER BY p.created_at
        """,
    ).fetchall()

    data: list[tuple[float, float]] = []
    total_invested = 0.0
    total_actual_payout = 0.0

    for rec_bet_raw, is_hit, payout_eval, combo_json in rows:
        rec_bet = float(rec_bet_raw)
        try:
            combos = _json.loads(combo_json) if combo_json else []
            n_combos = max(len(combos), 1)
        except Exception:
            n_combos = 1

        eval_invested = n_combos * 100.0  # evaluator が使う投資額
        scale = rec_bet / eval_invested   # recommended_bet 基準のスケール

        if is_hit:
            actual_payout = float(payout_eval) * scale
            actual_profit = actual_payout - rec_bet
        else:
            actual_payout = 0.0
            actual_profit = -rec_bet

        # col0=recommended_bet, col1=actual_profit, col2=n_combos
        data.append((rec_bet, actual_profit, float(n_combos)))
        total_invested += rec_bet
        total_actual_payout += actual_payout

    return np.array(data), total_invested, total_actual_payout


def _run_monte_carlo(
    bets: np.ndarray,
    scale: float,
    n_trials: int,
    initial_balance: float,
    bankrupt_threshold: float,
    rng: np.random.Generator,
) -> dict:
    """ランダム順序で n_trials 回シミュレーションを実行する。

    Args:
        bets: (N, 2) ndarray (col0=recommended_bet, col1=profit)
        scale: Kelly 係数スケール（1.0 = 1/4 Kelly 基準）
        n_trials: 試行回数
        initial_balance: 初期残高
        bankrupt_threshold: 破産と判定する残高比率（例: 0.10 = 10%以下）

    Returns:
        {final_balances, max_drawdowns, bankruptcy_rate, ...} を含む辞書
    """
    n_bets = len(bets)
    profits = bets[:, 1]  # 純利益配列（recommended_bet 基準）

    final_balances: list[float] = []
    max_drawdowns: list[float] = []
    bankruptcy_count = 0
    first_bankrupt_steps: list[int] = []

    bankrupt_line = initial_balance * bankrupt_threshold

    for _ in range(n_trials):
        order = rng.permutation(n_bets)
        balance = initial_balance
        peak = initial_balance
        bankrupt = False
        bankrupt_step = n_bets  # 破産しなかった場合はフル件数

        for step, idx in enumerate(order):
            raw_profit = profits[idx]
            scaled_profit = raw_profit * scale

            # 損失は残高を超えない
            if scaled_profit < 0:
                scaled_profit = max(scaled_profit, -balance)

            balance += scaled_profit
            peak = max(peak, balance)

            if balance <= bankrupt_line and not bankrupt:
                bankrupt = True
                bankruptcy_count += 1
                bankrupt_step = step + 1
                break

        final_balances.append(balance)
        max_drawdown = (peak - balance) / peak if peak > 0 else 0.0
        max_drawdowns.append(max_drawdown)
        if bankrupt:
            first_bankrupt_steps.append(bankrupt_step)

    fb = np.array(final_balances)
    dd = np.array(max_drawdowns)

    return {
        "n_bets": n_bets,
        "scale": scale,
        "n_trials": n_trials,
        "bankruptcy_rate": round(bankruptcy_count / n_trials * 100, 3),
        "bankruptcy_count": bankruptcy_count,
        "avg_first_bankrupt_step": (
            round(float(np.mean(first_bankrupt_steps)), 1) if first_bankrupt_steps else None
        ),
        "final_balance": {
            "mean":   round(float(fb.mean()), 0),
            "median": round(float(np.median(fb)), 0),
            "p5":     round(float(np.percentile(fb, 5)), 0),
            "p25":    round(float(np.percentile(fb, 25)), 0),
            "p75":    round(float(np.percentile(fb, 75)), 0),
            "p95":    round(float(np.percentile(fb, 95)), 0),
        },
        "max_drawdown_pct": {
            "mean":   round(float(dd.mean() * 100), 2),
            "median": round(float(np.median(dd) * 100), 2),
            "p95":    round(float(np.percentile(dd, 95) * 100), 2),
        },
    }


def _run_monte_carlo_w037(
    bets: np.ndarray,
    base_scale: float,
    n_trials: int,
    initial_balance: float,
    bankrupt_threshold: float,
    rng: np.random.Generator,
    balance_cap_pct: float = 0.05,
) -> dict:
    """W-037 動的セーフティを適用した Monte Carlo シミュレーション。

    base_scale × dynamic_kelly_factor で有効Kelly分数を決定し、
    残高5%キャップ + n_combos×100円フロアチェックを適用する。

    Args:
        bets:            (N, 3) ndarray: col0=original_bet, col1=base_profit, col2=n_combos
        base_scale:      Kelly 基準スケール（1.0 = 1/4 Kelly 基準）
        balance_cap_pct: 残高上限比率（W-037: 5%）
    """
    n_bets = len(bets)
    original_bets = bets[:, 0]   # 元の recommended_bet
    base_profits  = bets[:, 1]   # recommended_bet 全額での利益
    n_combos_arr  = bets[:, 2]   # コンボ数

    final_balances: list[float] = []
    max_drawdowns: list[float] = []
    bankruptcy_count = 0
    first_bankrupt_steps: list[int] = []
    skipped_bets_total = 0

    bankrupt_line = initial_balance * bankrupt_threshold

    for _ in range(n_trials):
        order = rng.permutation(n_bets)
        balance = initial_balance
        peak = initial_balance
        bankrupt = False
        bankrupt_step = n_bets

        for step, idx in enumerate(order):
            orig_bet = original_bets[idx]
            base_profit = base_profits[idx]
            n_combos = n_combos_arr[idx]

            # W-037: 動的Kelly係数（残高比率に応じて縮小）
            ratio = balance / initial_balance
            if ratio >= 1.0:
                dynamic_factor = 1.0
            elif ratio >= 0.5:
                dynamic_factor = 0.5
            else:
                dynamic_factor = 1.0 / 3.0

            effective_scale = base_scale * dynamic_factor

            # W-037: ベース賭け金 × 有効スケール
            stake = orig_bet * effective_scale

            # W-037: 残高 × balance_cap_pct の上限キャップ
            stake = min(stake, balance * balance_cap_pct)

            # W-037: n_combos × 100円フロアチェック（未達なら見送り）
            min_floor = n_combos * 100.0
            if stake < min_floor:
                skipped_bets_total += 1
                continue  # このレースは賭けない

            # 実際の stake に按分した利益
            stake_ratio = stake / orig_bet
            profit_contrib = base_profit * stake_ratio

            # 損失は残高を超えない
            if profit_contrib < 0:
                profit_contrib = max(profit_contrib, -balance)

            balance += profit_contrib
            peak = max(peak, balance)

            if balance <= bankrupt_line and not bankrupt:
                bankrupt = True
                bankruptcy_count += 1
                bankrupt_step = step + 1
                break

        final_balances.append(balance)
        max_drawdown = (peak - balance) / peak if peak > 0 else 0.0
        max_drawdowns.append(max_drawdown)
        if bankrupt:
            first_bankrupt_steps.append(bankrupt_step)

    fb = np.array(final_balances)
    dd = np.array(max_drawdowns)

    return {
        "n_bets": n_bets,
        "scale": base_scale,
        "n_trials": n_trials,
        "bankruptcy_rate": round(bankruptcy_count / n_trials * 100, 3),
        "bankruptcy_count": bankruptcy_count,
        "skipped_bets_per_trial": round(skipped_bets_total / n_trials, 1),
        "avg_first_bankrupt_step": (
            round(float(np.mean(first_bankrupt_steps)), 1) if first_bankrupt_steps else None
        ),
        "final_balance": {
            "mean":   round(float(fb.mean()), 0),
            "median": round(float(np.median(fb)), 0),
            "p5":     round(float(np.percentile(fb, 5)), 0),
            "p25":    round(float(np.percentile(fb, 25)), 0),
            "p75":    round(float(np.percentile(fb, 75)), 0),
            "p95":    round(float(np.percentile(fb, 95)), 0),
        },
        "max_drawdown_pct": {
            "mean":   round(float(dd.mean() * 100), 2),
            "median": round(float(np.median(dd) * 100), 2),
            "p95":    round(float(np.percentile(dd, 95) * 100), 2),
        },
    }


def _print_monte_carlo(results: dict[str, dict], initial_balance: float) -> None:
    """Monte Carlo 結果をコンソールに表示する。"""
    print("\n" + "=" * 65)
    print(f"  Step 2: Kelly Monte Carlo（初期資金 ¥{initial_balance:,.0f}）")
    print("=" * 65)

    header = f"  {'Kelly':>12}  {'破産率%':>7}  {'中央残高':>10}  {'最大DD(中央)':>12}  {'最大DD(95%ile)':>13}"
    print(header)
    print("  " + "-" * 60)

    for label, res in results.items():
        br = res["bankruptcy_rate"]
        med = res["final_balance"]["median"]
        dd_med = res["max_drawdown_pct"]["median"]
        dd_95 = res["max_drawdown_pct"]["p95"]

        br_flag = " [!]" if br > 1.0 else "    "
        print(
            f"  {label:>12}"
            f"  {br:7.2f}%{br_flag}"
            f"  ¥{med:>9,.0f}"
            f"  {dd_med:>10.1f}%"
            f"  {dd_95:>11.1f}%"
        )

    print()
    print("  【パーセンタイル詳細】")
    print(f"  {'Kelly':>12}  {'P5残高':>10}  {'P25残高':>10}  {'P75残高':>10}  {'P95残高':>10}")
    print("  " + "-" * 58)
    for label, res in results.items():
        fb = res["final_balance"]
        print(
            f"  {label:>12}"
            f"  ¥{fb['p5']:>9,.0f}"
            f"  ¥{fb['p25']:>9,.0f}"
            f"  ¥{fb['p75']:>9,.0f}"
            f"  ¥{fb['p95']:>9,.0f}"
        )

    print()
    print("  【判定基準】")
    print(f"  破産ライン: 初期資金の {int(BANKRUPT_THRESHOLD*100)}% 以下")
    print(f"  安全目安: 破産率 < 0.1%、最大DD(95%ile) < 50%")

    print()
    print("  【推奨 Kelly 係数】")
    safe = [
        label
        for label, res in results.items()
        if res["bankruptcy_rate"] < 0.1 and res["max_drawdown_pct"]["p95"] < 50
    ]
    if safe:
        print(f"  [OK] {', '.join(safe)} が安全基準を満たします。")
    else:
        print("  [!] いずれの係数も安全基準を満たしません。投資上限の見直しを推奨します。")


# ══════════════════════════════════════════════════════════════════════════════
# Step 3: W-036 キャリブレーション補正効果試算
# ══════════════════════════════════════════════════════════════════════════════

def _estimate_calibration_impact(
    conn: sqlite3.Connection,
    bets: np.ndarray,
    initial_balance: float,
    n_trials: int,
    rng: np.random.Generator,
) -> dict:
    """W-036 補正を適用した場合の Monte Carlo 試算を実行する。

    アプローチ:
      本命(直前)モデルの expected_value に対して補正倍率を乗じ、
      新しい Kelly ステークを推定する。
      補正倍率 = correct_honmei_score(model_score) / model_score

    Returns:
        {
            "avg_correction_factor": float,      # 平均EV補正倍率
            "n_honmei_bets": int,                # 対象ベット数
            "base_roi": float,                    # 補正前ROI
            "estimated_new_roi": float,           # 補正後推定ROI（楽観推計）
            "monte_carlo": dict,                  # 1/4 Kelly + W-037 + W-036 結果
        }
    """
    import sys
    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    from src.ml.calibration import correct_honmei_score

    # 本命(直前)モデルの honmei_score を取得して補正倍率を計算
    rows = conn.execute(
        """
        SELECT ph.model_score, p.expected_value
        FROM prediction_horses ph
        JOIN predictions p ON ph.prediction_id = p.id
        WHERE p.model_type = ?
          AND ph.model_score > 0.001
          AND p.expected_value IS NOT NULL
        """,
        (MT_HONMEI_IMMEDIATE,),
    ).fetchall()

    if not rows:
        return {"avg_correction_factor": 1.0, "n_honmei_bets": 0}

    correction_factors: list[float] = []
    for raw_score, ev_val in rows:
        raw = float(raw_score)
        corrected = correct_honmei_score(raw)
        factor = corrected / max(raw, 1e-9)
        correction_factors.append(factor)

    avg_factor = float(np.mean(correction_factors))
    n_bets = len(rows)

    # 補正後 Kelly ステーク推定:
    # EV が avg_factor 倍になるとして、各ベットの stake も比例縮小/拡大
    # (Kelly は EV に非線形だが、一次近似として avg_factor をスケールとして使用)
    corrected_bets = bets.copy()
    corrected_bets[:, 0] = corrected_bets[:, 0] * avg_factor  # stake
    corrected_bets[:, 1] = corrected_bets[:, 1] * avg_factor  # profit

    # 元の実績ROI
    original_invested = bets[:, 0].sum()
    original_payout = sum(
        p + b for b, p in zip(bets[:, 0], bets[:, 1]) if p > 0
    )
    base_roi = (original_payout / max(original_invested, 1.0)) * 100

    # W-037 + W-036 合算 Monte Carlo (1/4 Kelly)
    result_w037_w036 = _run_monte_carlo_w037(
        corrected_bets,
        base_scale=1.0,   # 1/4 Kelly
        n_trials=n_trials,
        initial_balance=initial_balance,
        bankrupt_threshold=BANKRUPT_THRESHOLD,
        rng=rng,
    )

    return {
        "avg_correction_factor": round(avg_factor, 3),
        "n_honmei_bets": n_bets,
        "base_roi": round(base_roi, 1),
        "estimated_new_roi": round(base_roi * avg_factor, 1),
        "monte_carlo": result_w037_w036,
    }


def _print_calibration_impact(impact: dict, initial_balance: float) -> None:
    """W-036 補正効果を表示する。"""
    print("\n" + "=" * 65)
    print("  Step 3: W-036 キャリブレーション補正 効果試算")
    print("  （本命(直前)モデルの系統的過小評価を補正）")
    print("=" * 65)

    if impact.get("n_honmei_bets", 0) == 0:
        print("  [!] 本命(直前)モデルのベットデータが取得できませんでした")
        return

    avg_f = impact["avg_correction_factor"]
    n = impact["n_honmei_bets"]
    base_roi = impact["base_roi"]
    est_roi = impact["estimated_new_roi"]

    print(f"  対象サンプル数  : {n:,}件 (prediction_horses × 本命(直前))")
    print(f"  平均補正倍率    : ×{avg_f:.3f}")
    print(f"    → 現在 EV 計算の真値は平均 {avg_f:.1f}倍高い可能性がある")
    print(f"    → bin別: 低確率馬(0-5%)×3.1 / 中確率馬(5-15%)×1.9-2.1 / 高確率馬(15-25%)×1.1-1.4")
    print()
    print(f"  【W-036 実装による期待効果】")
    print(f"  現在のシステム: honmei_score そのまま → EV = score × odds")
    print(f"  W-036適用後  : honmei_score × 補正倍率 → EV' = corrected × odds")
    print()
    print(f"  本命単勝の例（score=0.10, odds=8.0）:")
    from src.ml.calibration import correct_honmei_score, correction_factor_for
    raw_score = 0.10
    cal_score = correct_honmei_score(raw_score)
    factor = correction_factor_for(raw_score)
    print(f"    補正前: EV = {raw_score:.2f} × 8.0 = {raw_score*8:.2f} (閾値1.2未満→不採用 [!])")
    print(f"    補正後: EV = {cal_score:.3f} × 8.0 = {cal_score*8:.2f} (閾値超え→採用 [OK])")
    print()
    print(f"  本命単勝の例（score=0.15, odds=5.0）:")
    raw2 = 0.15
    cal2 = correct_honmei_score(raw2)
    print(f"    補正前: EV = {raw2:.2f} × 5.0 = {raw2*5:.2f} (採用)")
    print(f"    補正後: EV = {cal2:.3f} × 5.0 = {cal2*5:.2f} (Kelly額が{correction_factor_for(raw2):.1f}倍に拡大)")
    print()
    print(f"  ※ 理論的ROI改善推計（一次近似）:")
    print(f"     補正前 基準ROI : ~{base_roi:.0f}%")
    print(f"     補正後 推定ROI : ~{est_roi:.0f}% （EV上昇によるKelly増加の楽観推計）")
    print()
    print(f"  ⚠ 注意: W-036 は本命モデルのみに適用。卍・Alpha は対象外。")
    print(f"         高額投資増加に伴い推奨資金は ¥300K〜¥500K 以上を維持すること（W-037と同様）。")


# ══════════════════════════════════════════════════════════════════════════════
# エントリーポイント
# ══════════════════════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="キャリブレーション & Kelly Monte Carlo 監査")
    parser.add_argument("--db", default=str(_ROOT / "data" / "umalogi.db"), help="SQLite パス")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="Monte Carlo 試行回数")
    parser.add_argument("--initial", type=float, default=INITIAL_BALANCE, help="初期資金 (円)")
    parser.add_argument("--seed", type=int, default=42, help="乱数シード")
    parser.add_argument("--out", default=None, help="JSON 出力パス（省略時は標準出力のみ）")
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    rng = np.random.default_rng(args.seed)

    # ── Step 1: キャリブレーション ──────────────────────────────────────────
    print(f"\n[Step 1] キャリブレーションデータ取得中… (model_type={MT_HONMEI_IMMEDIATE})")
    cal_data = _load_calibration_data(conn)
    print(f"  → {len(cal_data)} 件の (model_score, is_winner) ペアを取得")

    cal_table = _build_calibration_table(cal_data)
    correction = _fit_correction_table(cal_table)
    _print_calibration(cal_table, correction)

    # ── Step 2: Kelly Monte Carlo ─────────────────────────────────────────
    print(f"\n[Step 2] Kelly ベットデータ取得中…")
    kelly_data, total_invested, total_actual_payout = _load_kelly_data(conn)
    total_net = kelly_data[:, 1].sum()
    true_roi = total_actual_payout / total_invested * 100 if total_invested > 0 else 0
    print(f"  → {len(kelly_data):,} 件のベット履歴を取得")
    print(f"  → 総投資額 (recommended_bet): ¥{total_invested:,.0f}")
    print(f"  → 総払戻額 (scaled):          ¥{total_actual_payout:,.0f}")
    print(f"  → 純損益:                     ¥{total_net:+,.0f}")
    print(f"  → 真のROI:                    {true_roi:.1f}%")
    if true_roi < 100:
        print(f"  [!] ROI < 100% のためいかなるKelly係数でも長期的な破産は避けられません。")
        print(f"      シミュレーションはドローダウン耐性と破産到達速度の比較として参照してください。")

    conn.close()

    mc_results: dict[str, dict] = {}
    for label, scale in KELLY_SCALES.items():
        print(f"  {label}（scale={scale:.1f}）… {args.trials:,} 試行")
        mc_results[label] = _run_monte_carlo(
            kelly_data,
            scale=scale,
            n_trials=args.trials,
            initial_balance=args.initial,
            bankrupt_threshold=BANKRUPT_THRESHOLD,
            rng=rng,
        )

    _print_monte_carlo(mc_results, args.initial)

    # ── W-037 動的セーフティ適用シミュレーション ──────────────────────────────
    print("\n" + "=" * 65)
    print("  W-037 動的セーフティ適用シミュレーション")
    print("  （残高5%キャップ + 動的Kelly縮小 + n_combos×100フロア）")
    print("=" * 65)

    w037_results: dict[str, dict] = {}
    for label, scale in KELLY_SCALES.items():
        print(f"  {label} + W-037（scale={scale:.1f}）… {args.trials:,} 試行")
        w037_results[label] = _run_monte_carlo_w037(
            kelly_data,
            base_scale=scale,
            n_trials=args.trials,
            initial_balance=args.initial,
            bankrupt_threshold=BANKRUPT_THRESHOLD,
            rng=rng,
        )

    _print_monte_carlo(w037_results, args.initial)

    # W-037 のスキップ統計を表示
    print("\n  【W-037 スキップ統計（試行あたり見送り件数）】")
    for label, res in w037_results.items():
        skipped = res.get("skipped_bets_per_trial", 0)
        pct = skipped / len(kelly_data) * 100 if len(kelly_data) > 0 else 0
        print(f"  {label:>12}: {skipped:.1f}件/{len(kelly_data)}件 ({pct:.1f}%見送り)")

    # ── Step 3: W-036 キャリブレーション補正 効果試算 ─────────────────────────
    print("\n[Step 3] W-036 キャリブレーション補正の効果を試算中…")
    conn2 = sqlite3.connect(args.db)
    rng2 = np.random.default_rng(args.seed + 1)
    cal_impact = _estimate_calibration_impact(
        conn2, kelly_data,
        initial_balance=args.initial,
        n_trials=args.trials,
        rng=rng2,
    )
    conn2.close()
    _print_calibration_impact(cal_impact, args.initial)

    # ── JSON 出力 ──────────────────────────────────────────────────────────
    output = {
        "calibration": {
            "model_type": MT_HONMEI_IMMEDIATE,
            "n_samples": len(cal_data),
            "bin_width": BIN_WIDTH,
            "table": cal_table,
            "correction": correction,
        },
        "kelly_monte_carlo": {
            "initial_balance": args.initial,
            "bankrupt_threshold": BANKRUPT_THRESHOLD,
            "n_trials": args.trials,
            "n_bets": int(len(kelly_data)),
            "total_invested": round(total_invested, 0),
            "total_actual_payout": round(total_actual_payout, 0),
            "true_roi_pct": round(true_roi, 2),
            "results": mc_results,
        },
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n  → 結果を {out_path} に保存しました。")
    else:
        print("\n  （--out でパスを指定すると JSON 出力されます）")


if __name__ == "__main__":
    main()
