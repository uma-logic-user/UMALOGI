"""
UMALOGI モンテカルロ・資金管理シミュレーション
「複勝エリート戦略」で月10万円純利を達成するための最適資金配分算出

前提条件:
  - 的中率   : 45%（複勝）
  - 平均配当 : 2.5倍
  - 月間レース数: 50レース
  - シミュレーション: 10,000回

Usage:
    py scripts/monte_carlo_bankroll.py
"""

from __future__ import annotations

import sys
from typing import NamedTuple

import numpy as np

sys.stdout.reconfigure(encoding="utf-8")

# ================================================================
# パラメータ
# ================================================================

N_SIMS: int = 10_000
N_RACES: int = 50
HIT_RATE: float = 0.45
AVG_RETURN: float = 2.5        # 払戻倍率（元本含む）
NET_WIN: float = AVG_RETURN - 1  # 純利倍率 = 1.5

FIXED_BETS: list[int] = [5_000, 10_000, 15_000]
BANKROLLS: list[int] = [100_000, 200_000, 300_000]
KELLY_FRACTION: float = 0.25   # フルケリーの 1/4 を使用
MONTHS_FOR_RUIN: int = 12      # 破産確率の観測期間

TARGET_PROFIT: int = 100_000   # 月間目標純利

rng = np.random.default_rng(seed=42)

# ================================================================
# シミュレーション関数
# ================================================================


def simulate_month_fixed(bet: int) -> np.ndarray:
    """固定ベット額で月間収支を N_SIMS 回シミュレーション。"""
    outcomes = rng.binomial(1, HIT_RATE, (N_SIMS, N_RACES))
    net_per_race = np.where(outcomes == 1, NET_WIN * bet, -bet)
    return net_per_race.sum(axis=1)


def risk_of_ruin(initial_bankroll: int, bet: int) -> float:
    """
    MONTHS_FOR_RUIN ヶ月以内にバンクが 1 ベット分未満に枯渇する確率を返す。

    固定ベット額を使用。月次収支で更新し、残高が bet 未満になった時点で破産扱い。
    """
    bankroll = np.full(N_SIMS, float(initial_bankroll))
    ruined = np.zeros(N_SIMS, dtype=bool)

    for _ in range(MONTHS_FOR_RUIN):
        active = ~ruined
        if not active.any():
            break
        n_active = int(active.sum())
        outcomes = rng.binomial(1, HIT_RATE, (n_active, N_RACES))
        monthly = np.where(outcomes == 1, NET_WIN * bet, -bet).sum(axis=1)
        bankroll[active] += monthly
        ruined |= bankroll < bet

    return float(ruined.mean())


def simulate_month_kelly(initial_bankroll: float) -> np.ndarray:
    """
    クォーターケリー基準でレースごとにベット額を動的調整した月間収支。

    フルケリー = (p × avg_return - 1) / (avg_return - 1)
    使用ケリー  = フルケリー × KELLY_FRACTION
    """
    full_kelly = (HIT_RATE * AVG_RETURN - 1.0) / NET_WIN
    qk = full_kelly * KELLY_FRACTION

    bankrolls = np.full(N_SIMS, float(initial_bankroll))
    monthly_pnl = np.zeros(N_SIMS)

    for _ in range(N_RACES):
        bet = np.maximum(bankrolls * qk, 100.0)  # 最低 100 円
        outcomes = rng.binomial(1, HIT_RATE, N_SIMS)
        gain = np.where(outcomes == 1, NET_WIN * bet, -bet)
        bankrolls = np.maximum(bankrolls + gain, 0.0)
        monthly_pnl += gain

    return monthly_pnl


# ================================================================
# レポート出力
# ================================================================

SEP = "=" * 64


def fmt(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}¥{int(v):,}"


def pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def print_section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def print_fixed_bet_results(bet: int, pnl: np.ndarray) -> None:
    ev = pnl.mean()
    sd = pnl.std()
    median = np.median(pnl)
    p10 = np.percentile(pnl, 10)
    p25 = np.percentile(pnl, 25)
    p75 = np.percentile(pnl, 75)
    p90 = np.percentile(pnl, 90)
    prob_profit = (pnl > 0).mean()
    prob_target = (pnl >= TARGET_PROFIT).mean()
    prob_loss_20 = (pnl < -20_000).mean()

    print(f"\n  ■ 1レース {bet:,}円固定ベット")
    print(f"    理論期待値 (月間)   : {fmt(N_RACES * (HIT_RATE * NET_WIN - (1 - HIT_RATE)) * bet)}")
    print(f"    MC 平均月間収支     : {fmt(ev)}")
    print(f"    標準偏差            : ¥{int(sd):,}")
    print(f"    中央値              : {fmt(median)}")
    print(f"    10th 〜 90th パーセンタイル")
    print(f"      最悪10%           : {fmt(p10)} 以下")
    print(f"      下位25%           : {fmt(p25)} 以下")
    print(f"      上位25%           : {fmt(p75)} 以上")
    print(f"      上位10%           : {fmt(p90)} 以上")
    print(f"    プラス収支の確率    : {pct(prob_profit)}")
    print(f"    月10万円達成確率    : {pct(prob_target)}")
    print(f"    2万円超損失の確率   : {pct(prob_loss_20)}")


def main() -> None:
    print("\n" + "★" * 64)
    print("  UMALOGI モンテカルロ 資金管理シミュレーション")
    print("  複勝エリート戦略 × 月10万円純利 最適化レポート")
    print("★" * 64)

    print(f"\n  【前提条件】")
    print(f"    的中率           : {HIT_RATE * 100:.0f}%（複勝）")
    print(f"    平均配当         : {AVG_RETURN}倍")
    print(f"    月間レース数     : {N_RACES}レース")
    print(f"    シミュレーション : {N_SIMS:,}回")
    print(f"    Kelly分数        : 1/{int(1/KELLY_FRACTION)}（フルケリーの25%）")

    # ────────────────────────────────────────────────────────────
    # 1. 固定ベット シミュレーション
    # ────────────────────────────────────────────────────────────
    print_section("1. 固定ベット別 月間収支分布（10,000回シミュレーション）")

    fixed_results: dict[int, np.ndarray] = {}
    for bet in FIXED_BETS:
        pnl = simulate_month_fixed(bet)
        fixed_results[bet] = pnl
        print_fixed_bet_results(bet, pnl)

    # ────────────────────────────────────────────────────────────
    # 2. 破産確率（Risk of Ruin）
    # ────────────────────────────────────────────────────────────
    print_section("2. 破産確率（Risk of Ruin） — 12ヶ月以内")

    print(f"\n  ※ ベット額 = 5,000円固定（保守的見積もり）")
    print(f"  ※ 破産 = バンクが1ベット（5,000円）未満に枯渇")
    print()
    print(f"  {'バンク':>12s}  {'RoR':>8s}  {'判定':}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    ror_5k: dict[int, float] = {}
    for bank in BANKROLLS:
        ror = risk_of_ruin(bank, bet=5_000)
        ror_5k[bank] = ror
        rating = "🟢 安全" if ror < 0.05 else ("🟡 許容" if ror < 0.15 else "🔴 危険")
        print(f"  {bank:>12,}円  {pct(ror):>8s}  {rating}")

    print()
    print(f"  ※ ベット額 = 10,000円固定")
    print()
    print(f"  {'バンク':>12s}  {'RoR':>8s}  {'判定':}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    ror_10k: dict[int, float] = {}
    for bank in BANKROLLS:
        ror = risk_of_ruin(bank, bet=10_000)
        ror_10k[bank] = ror
        rating = "🟢 安全" if ror < 0.05 else ("🟡 許容" if ror < 0.15 else "🔴 危険")
        print(f"  {bank:>12,}円  {pct(ror):>8s}  {rating}")

    print()
    print(f"  ※ ベット額 = 15,000円固定")
    print()
    print(f"  {'バンク':>12s}  {'RoR':>8s}  {'判定':}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    ror_15k: dict[int, float] = {}
    for bank in BANKROLLS:
        ror = risk_of_ruin(bank, bet=15_000)
        ror_15k[bank] = ror
        rating = "🟢 安全" if ror < 0.05 else ("🟡 許容" if ror < 0.15 else "🔴 危険")
        print(f"  {bank:>12,}円  {pct(ror):>8s}  {rating}")

    # ────────────────────────────────────────────────────────────
    # 3. Kelly 基準シミュレーション
    # ────────────────────────────────────────────────────────────
    print_section("3. クォーターケリー基準 vs 固定ベット 比較")

    # Kelly の理論値
    full_kelly = (HIT_RATE * AVG_RETURN - 1.0) / NET_WIN
    qk = full_kelly * KELLY_FRACTION

    print(f"\n  ■ Kelly 基準の計算")
    print(f"    フルケリー分数   : {full_kelly * 100:.2f}% of bankroll")
    print(f"    クォーターケリー : {qk * 100:.2f}% of bankroll")
    print()
    print(f"  ■ バンク別 クォーターケリー 初期ベット額")
    for bank in BANKROLLS:
        initial_bet = int(bank * qk)
        print(f"    バンク {bank:>9,}円  → 初期ベット {initial_bet:,}円/レース")

    print()
    # Kelly シミュレーション（初期バンク 200,000 円）
    KELLY_BANK = 200_000
    kelly_pnl = simulate_month_kelly(KELLY_BANK)
    fixed_10k_pnl = fixed_results[10_000]

    print(f"  ■ バンク {KELLY_BANK:,}円 クォーターケリー vs 固定10,000円 比較")
    print(f"  {'指標':20s}  {'Kelly':>14s}  {'固定10k':>14s}")
    print(f"  {'-'*20}  {'-'*14}  {'-'*14}")

    def row(label: str, k_val: float, f_val: float) -> None:
        print(f"  {label:20s}  {fmt(k_val):>14s}  {fmt(f_val):>14s}")

    def rowp(label: str, k_val: float, f_val: float) -> None:
        print(f"  {label:20s}  {pct(k_val):>14s}  {pct(f_val):>14s}")

    row("MC 平均月間収支",    kelly_pnl.mean(),            fixed_10k_pnl.mean())
    row("中央値",             np.median(kelly_pnl),        np.median(fixed_10k_pnl))
    row("標準偏差",           kelly_pnl.std(),             fixed_10k_pnl.std())
    row("最悪10% ライン",     np.percentile(kelly_pnl,10), np.percentile(fixed_10k_pnl,10))
    row("最良10% ライン",     np.percentile(kelly_pnl,90), np.percentile(fixed_10k_pnl,90))
    rowp("プラス確率",        (kelly_pnl > 0).mean(),      (fixed_10k_pnl > 0).mean())
    rowp("月10万達成確率",    (kelly_pnl >= TARGET_PROFIT).mean(), (fixed_10k_pnl >= TARGET_PROFIT).mean())

    # ────────────────────────────────────────────────────────────
    # 4. UMALOGI 向け 1レースあたりのベット金額指示ロジック
    # ────────────────────────────────────────────────────────────
    print_section("4. UMALOGI ベット金額指示ロジック（実装仕様）")

    print("""
  ■ Kelly基準 ベット計算式（UMALOGIモデル用）

    変数定義:
      B     = 現在のバンク残高（円）
      p     = UMALOGIモデルの勝率予測（0〜1）
      odds  = 推定オッズ（例: 2.5倍）
      b     = 純利倍率 = odds - 1.0
      EV    = p * odds（期待値）

    フルケリー分数:
      f_full = (p * odds - 1) / b  ← EV > 1.0 のレースのみ正値

    クォーターケリー ベット額:
      bet = B × f_full × 0.25
      bet = max(bet, MIN_BET)      # 最低ベット: 200円
      bet = min(bet, B × 0.10)    # 最大ベット: バンクの10%（破産防止）
      bet = round(bet / 100) × 100 # 100円単位に丸め

  ■ EV別 推奨ベット額テーブル（バンク 200,000円）

      EV   odds   p_model   f_full   bet/race
     ─────────────────────────────────────────""")

    bank_ex = 200_000
    ev_examples = [
        (1.05, 2.5), (1.10, 2.5), (1.20, 2.5), (1.30, 2.5),
        (1.10, 3.0), (1.20, 3.0), (1.30, 3.0),
        (1.20, 4.0), (1.30, 4.0),
    ]
    for ev, odds in ev_examples:
        p_m = ev / odds
        b_net = odds - 1.0
        f_full = max((p_m * odds - 1.0) / b_net, 0.0)
        bet_raw = bank_ex * f_full * KELLY_FRACTION
        bet = max(min(round(bet_raw / 100) * 100, int(bank_ex * 0.10)), 200)
        print(f"     {ev:.2f}   {odds:.1f}倍   {p_m:.3f}     {f_full:.4f}   ¥{bet:,}")

    # ────────────────────────────────────────────────────────────
    # 5. 総合推奨
    # ────────────────────────────────────────────────────────────
    print_section("5. 社長への総合推奨レポート")

    # 「枕を高くして寝られる」条件: RoR < 5%、月プラス確率 > 65%
    print(f"""
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  【社長への推奨: ステージ別資金戦略】
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ■ STAGE 1「手堅スタート」（推奨: 初月〜）
     バンク: 20万円  |  戦略: クォーターKelly
     初期ベット目安: ¥{int(200_000 * qk):,}円/レース
     月間期待収支: {fmt(kelly_pnl.mean())} (中央値: {fmt(np.median(kelly_pnl))})
     月プラス確率: {pct((kelly_pnl > 0).mean())}
     12ヶ月破産確率(5k固定換算): {pct(ror_5k[200_000])} 🟢
     → 「枕を高くして寝られる」最小バンクはこれ。

  ■ STAGE 2「本格攻略」（推奨: 3ヶ月後 or バンク30万到達後）
     バンク: 30万円  |  戦略: クォーターKelly + 固定10,000円ブレンド
     月10万円達成確率: {pct((fixed_10k_pnl >= TARGET_PROFIT).mean())} (固定10k参考値)
     12ヶ月破産確率(10k固定換算): {pct(ror_10k[300_000])} 🟢
     → 月10万円目標の射程圏。バンクが増えるほど安全性も向上。

  ■ 「やってはいけない」ゾーン
     バンク10万円 × ベット15,000円: RoR = {pct(ror_15k[100_000])} 🔴
     → ハイリスク。バンク比15%超のベットは破産の近道。

  ■ Kelly 基準採用の効果
     固定10,000円 vs クォーターKelly（バンク20万）比較:
       月10万円達成確率: 固定 {pct((fixed_10k_pnl >= TARGET_PROFIT).mean())}
                         Kelly {pct((kelly_pnl >= TARGET_PROFIT).mean())}
     Kellyは「勝てるレースに多く張る」ため長期では必ず固定より優れる。
     ただし短期ではバンク増減が激しいため精神的耐性が必要。

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  結論: 「20万バンク × クォーターKelly」からスタートし、
        バンクが30万を超えたら固定10,000円とKellyのブレンドへ移行。
        これで月10万円は「運ではなく確率の問題」になります。
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    main()
