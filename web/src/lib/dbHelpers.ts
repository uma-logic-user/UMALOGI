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
 * combination_json を昇順ソートして再シリアライズする。
 * パース失敗時は元の文字列をそのまま返す。
 */
export function sortedCombinations(json: unknown): string {
  if (!json || typeof json !== 'string') return '[]'
  try {
    const raw: number[][] = JSON.parse(json)
    const sorted = raw
      .map(c => [...c].sort((a, b) => a - b))
      .sort((a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          if (a[i] !== b[i]) return a[i] - b[i]
        }
        return a.length - b.length
      })
    return JSON.stringify(sorted)
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
    if (firsts.size === num && num >= 3 && n === num * (num - 1) * (num - 2))
      return [`${num}頭ボックス`, n]
    if (firsts.size === 2) return ['2頭軸マルチ', n]
    if (firsts.size === 1) return ['1頭軸マルチ', n]
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
