/**
 * DB 関連の共通ユーティリティ
 * 全 API ルートから import して使用すること。
 */

export const BET_ORDER: Record<string, number> = {
  '単勝': 1, '複勝': 2, '枠連': 3, '馬連': 4,
  'ワイド': 5, '馬単': 6, '三連複': 7, '三連単': 8,
}

/**
 * 制御文字を除去してトリムする。文字列以外はそのまま返す。
 */
export function sanitize(v: unknown): unknown {
  return typeof v === 'string'
    ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim()
    : v
}

/**
 * better-sqlite3 の Row オブジェクトを sanitize 済みプレーン Object に変換する。
 */
export function rowToObj(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row).map(([k, v]) => [k, sanitize(v)]))
}

/**
 * combination_json を正規化してシリアライズする。
 * 無順券種(馬連/ワイド/三連複): 各コンボ内部を昇順ソート → 外側をソート
 * 有順券種(馬単/三連単): 各コンボ内部の順序を保持 → 外側のみソート
 * パース失敗時は元の文字列をそのまま返す。
 */
export function sortedCombinations(json: unknown, betType?: string): string {
  if (!json || typeof json !== 'string') return '[]'
  try {
    const raw: number[][] = JSON.parse(json)
    if (!Array.isArray(raw)) return String(json)
    const isOrdered = betType === '馬単' || betType === '三連単'
    const normalized = raw
      .map(c => isOrdered ? [...c] : [...c].sort((a, b) => a - b))
      .sort((a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          if (a[i] !== b[i]) return a[i] - b[i]
        }
        return a.length - b.length
      })
    return JSON.stringify(normalized)
  } catch {
    return String(json)
  }
}

/**
 * SQLite の変数バインド数上限(999)に収まるよう配列を分割する。
 */
export function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

/**
 * combination_json と bet_type から「買い方ラベル」と「点数」を返す。
 * Python 側の _identify_bet_form() と同一ロジック。
 *
 * @returns [bet_form, n_tickets]
 */
export function identifyBetForm(combinationJson: unknown, betType: string): [string, number] {
  if (!combinationJson || typeof combinationJson !== 'string') return [betType, 0]
  let combos: number[][]
  try {
    const parsed = JSON.parse(combinationJson)
    if (!Array.isArray(parsed) || parsed.length === 0) return [betType, 0]
    combos = Array.isArray(parsed[0]) ? (parsed as number[][]) : [(parsed as number[])]
  } catch {
    return [betType, 0]
  }
  const n = combos.length
  const allHorses = new Set(combos.flatMap(c => c))
  const num = allHorses.size

  if (betType === '三連単') {
    const firsts = new Set(combos.map(c => c[0]))
    // ボックス: N頭の全順列 N*(N-1)*(N-2)
    if (firsts.size === num && num >= 3 && n === num * (num - 1) * (num - 2))
      return [`${num}頭ボックス`, n]
    // 軸馬判定: 全コンボに必ず含まれる馬の数で判定（マルチ = 位置不問）
    const alwaysIn = [...allHorses].filter(h => combos.every(c => c.includes(h)))
    if (alwaysIn.length >= 2) return ['2頭軸マルチ', n]
    if (alwaysIn.length === 1) return ['1頭軸マルチ', n]
    // 1着固定パターン（マルチ不使用）
    if (firsts.size === 2) return ['2頭軸（1着固定）', n]
    if (firsts.size === 1) return ['1頭軸（1着固定）', n]
    return ['フォーメーション', n]
  }
  if (betType === '三連複') {
    if (num >= 3 && n === (num * (num - 1) * (num - 2)) / 6)
      return [`${num}頭ボックス`, n]
    const axes = [...allHorses].filter(h => combos.every(c => c.includes(h)))
    if (axes.length >= 2) return ['軸2頭ながし', n]
    if (axes.length === 1) return ['軸1頭ながし', n]
    return ['フォーメーション', n]
  }
  if (betType === '馬連' || betType === 'ワイド' || betType === '馬単') {
    if (num >= 2 && n === (num * (num - 1)) / 2)
      return [`${num}頭ボックス`, n]
    const axes = [...allHorses].filter(h => combos.every(c => c.includes(h)))
    if (axes.length > 0) return ['軸ながし', n]
    return ['フォーメーション', n]
  }
  return [betType, n]
}
