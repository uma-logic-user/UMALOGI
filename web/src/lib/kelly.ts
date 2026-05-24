/**
 * ケリー基準計算ユーティリティ
 *
 * EV = P(win) × odds なので P(win) = EV / odds
 * ケリー公式: f* = (bp - q) / b = (EV - 1) / (odds - 1)
 *   b = odds - 1 (ネット配当)
 *   p = EV / odds (勝ち確率)
 *   q = 1 - p
 */

/** ケリー最適比率 f* を返す。EV≤1.0 または odds≤1.0 の場合は 0 を返す。 */
export function calcKellyFraction(ev: number, odds: number): number {
  if (odds <= 1 || ev <= 0) return 0
  return (ev - 1) / (odds - 1)
}

/**
 * 推奨購入金額を返す（100円単位切り捨て）。
 * f* ≤ 0 の場合（EV≤1.0）は 0 を返す（購入見送り）。
 *
 * @param ev        期待値（predicted_probability × odds）
 * @param odds      オッズ（倍率）
 * @param bankroll  総資金（円）
 * @param kellyFrac ケリー安全係数（デフォルト 0.25 = 1/4 Kelly）
 */
export function calcKellyStake(
  ev: number,
  odds: number,
  bankroll: number,
  kellyFrac: number = 0.25,
): number {
  const f = calcKellyFraction(ev, odds)
  if (f <= 0) return 0
  return Math.floor((bankroll * kellyFrac * f) / 100) * 100
}
